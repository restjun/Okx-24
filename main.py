from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import schedule, time, requests, threading, uvicorn, logging, pandas as pd, warnings

from datetime import datetime
from zoneinfo import ZoneInfo


# =========================================================
# 기본 설정
# =========================================================

warnings.filterwarnings("ignore", category=FutureWarning)
app = FastAPI()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s"
)
log = logging.getLogger("trading")

VOLUME_HOURS = 24
TOP_N = 30
UPDATE_MINUTES = 1

INITIAL_CANDLE_COUNT = 200
HISTORY_CHUNK = 200
MAX_HISTORY_CHUNKS = 10

BREAKOUT_LOOKBACK = 30
SWING_LEFT = 2
SWING_RIGHT = 2
MIN_CORRECTION_RATE = 0.003

USE_UPBIT = "Y"
USE_OKX = "N"

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10

KST = ZoneInfo("Asia/Seoul")

latest_upbit_data = []
latest_okx_data = []
latest_usdt_krw = 0
latest_upbit_update_time = "-"
latest_okx_update_time = "-"
latest_upbit_markets = []

shown_invalidation_ids = set()
invalidation_lock = threading.Lock()
request_lock = threading.Lock()
update_lock = threading.Lock()
last_request_time = 0


# =========================================================
# 공통
# =========================================================

def kst():
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


def wait_request():
    global last_request_time
    with request_lock:
        gap = time.monotonic() - last_request_time
        if gap < REQUEST_INTERVAL:
            time.sleep(REQUEST_INTERVAL - gap)
        last_request_time = time.monotonic()


def retry(func, *args, **kwargs):
    name = getattr(func, "__name__", str(func))
    url = args[0] if args and isinstance(args[0], str) else kwargs.get("url", "")

    for n in range(MAX_RETRIES):
        try:
            wait_request()
            r = func(*args, **kwargs)

            if not hasattr(r, "status_code"):
                return r

            if r.status_code == 200:
                return r

            if r.status_code == 429:
                wait = min(RATE_LIMIT_WAIT * 2 ** n, 60)
            elif r.status_code >= 500:
                wait = min(2 * 2 ** n, 30)
            else:
                log.warning(f"[HTTP {r.status_code}] {url}")
                return r

            log.warning(f"[API 재시도] {url} {wait}초")
            time.sleep(wait)

        except Exception as e:
            log.error(f"[API 오류] {name} {url}: {e}")
            if n < MAX_RETRIES - 1:
                time.sleep(min(2 * (n + 1), 20))

    log.error(f"[API 최종 실패] {name} {url}")
    return None


# =========================================================
# 거래소 데이터
# =========================================================

def get_upbit_markets():
    global latest_upbit_markets

    r = retry(
        requests.get,
        "https://api.upbit.com/v1/ticker/all",
        params={"quote_currencies": "KRW"},
        timeout=15
    )
    if r is None:
        return []

    try:
        data = r.json()
        result = []

        for x in data:
            market = x.get("market", "")
            if not market.startswith("KRW-"):
                continue

            try:
                volume = float(x["acc_trade_price_24h"])
            except:
                continue

            if volume > 0:
                result.append({
                    "market": market,
                    "volume_24h": volume
                })

        latest_upbit_markets = [x["market"] for x in result]
        return result

    except Exception as e:
        log.error(f"업비트 마켓 오류: {e}")
        return []


def get_usdt_krw():
    r = retry(
        requests.get,
        "https://api.upbit.com/v1/ticker?markets=KRW-USDT",
        timeout=15
    )
    try:
        price = float(r.json()[0]["trade_price"])
        return price if price > 0 else None
    except:
        return None


def get_okx_ohlcv(inst, bar="15m", limit=200, before=None):
    params = {"instId": inst, "bar": bar, "limit": min(max(int(limit), 1), 200)}
    if before is not None:
        params["before"] = str(before)

    r = retry(
        requests.get,
        "https://www.okx.com/api/v5/market/candles",
        params=params,
        timeout=15
    )
    if r is None:
        return None

    try:
        data = r.json().get("data", [])
        if not data:
            return None

        df = pd.DataFrame(data, columns=[
            "ts","o","h","l","c","vol",
            "volCcy","volCcyQuote","confirm"
        ])

        for c in ["ts","o","h","l","c","vol","volCcy","volCcyQuote"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        df = df[df.confirm.astype(str) == "1"]
        return (
            df.sort_values("ts")
            .drop_duplicates("ts")
            .reset_index(drop=True)
            if not df.empty else None
        )

    except Exception as e:
        log.error(f"OKX {inst} {bar} 오류: {e}")
        return None


def get_upbit_minute(market, unit, count=200, to=None):
    params = {"market": market, "count": min(max(int(count), 1), 200)}
    if to:
        params["to"] = to

    r = retry(
        requests.get,
        f"https://api.upbit.com/v1/candles/minutes/{unit}",
        params=params,
        timeout=15
    )
    if r is None:
        return None

    try:
        df = pd.DataFrame(r.json())
        if df.empty:
            return None

        df["o"] = pd.to_numeric(df["opening_price"], errors="coerce")
        df["h"] = pd.to_numeric(df["high_price"], errors="coerce")
        df["l"] = pd.to_numeric(df["low_price"], errors="coerce")
        df["c"] = pd.to_numeric(df["trade_price"], errors="coerce")
        df["volume_krw"] = pd.to_numeric(
            df["candle_acc_trade_price"], errors="coerce"
        )
        df["datetime"] = pd.to_datetime(
            df["candle_date_time_kst"], errors="coerce"
        )

        df = df.dropna(subset=["datetime","o","h","l","c"])
        if df.empty:
            return None

        now = datetime.now(KST)
        block = (now.minute // unit) * unit
        current = now.replace(
            minute=block, second=0, microsecond=0
        ).replace(tzinfo=None)

        df = df[df.datetime < current]

        return (
            df.sort_values("datetime")
            .drop_duplicates("datetime")
            .reset_index(drop=True)
            if not df.empty else None
        )

    except Exception as e:
        log.error(f"업비트 {unit}분봉 오류 {market}: {e}")
        return None


def get_upbit_4h(market, count=200, to=None):
    r = retry(
        requests.get,
        "https://api.upbit.com/v1/candles/minutes/240",
        params={
            "market": market,
            "count": min(max(int(count), 1), 200),
            **({"to": to} if to else {})
        },
        timeout=15
    )
    if r is None:
        return None

    try:
        df = pd.DataFrame(r.json())
        if df.empty:
            return None

        df["o"] = pd.to_numeric(df.opening_price, errors="coerce")
        df["h"] = pd.to_numeric(df.high_price, errors="coerce")
        df["l"] = pd.to_numeric(df.low_price, errors="coerce")
        df["c"] = pd.to_numeric(df.trade_price, errors="coerce")
        df["datetime"] = pd.to_datetime(
            df.candle_date_time_kst, errors="coerce"
        )
        df = df.dropna(subset=["datetime","o","h","l","c"])

        now = datetime.now(KST)
        block = (now.hour // 4) * 4
        current = now.replace(
            hour=block, minute=0, second=0, microsecond=0
        ).replace(tzinfo=None)

        df = df[df.datetime < current]

        return (
            df.sort_values("datetime")
            .drop_duplicates("datetime")
            .reset_index(drop=True)
            if not df.empty else None
        )

    except Exception as e:
        log.error(f"업비트 4H 오류 {market}: {e}")
        return None


# =========================================================
# 과거 데이터
# =========================================================

def history_okx(inst, bar, required=125):
    all_df, before = None, None

    for _ in range(MAX_HISTORY_CHUNKS):
        df = get_okx_ohlcv(inst, bar, HISTORY_CHUNK, before)
        if df is None or df.empty:
            break

        all_df = df.copy() if all_df is None else pd.concat(
            [df, all_df], ignore_index=True
        )
        all_df = (
            all_df.drop_duplicates("ts")
            .sort_values("ts")
            .reset_index(drop=True)
        )

        if len(all_df) >= required:
            return all_df

        before = int(all_df.ts.iloc[0])

    return all_df


def history_upbit(market, unit, required=125):
    all_df, to = None, None

    for _ in range(MAX_HISTORY_CHUNKS):
        df = get_upbit_minute(market, unit, HISTORY_CHUNK, to)
        if df is None or df.empty:
            break

        all_df = df.copy() if all_df is None else pd.concat(
            [df, all_df], ignore_index=True
        )
        all_df = (
            all_df.drop_duplicates("datetime")
            .sort_values("datetime")
            .reset_index(drop=True)
        )

        if len(all_df) >= required:
            return all_df

        to = all_df.datetime.iloc[0].strftime("%Y-%m-%dT%H:%M:%S")

    return all_df


def history_upbit_4h(market):
    all_df, to = None, None

    for _ in range(MAX_HISTORY_CHUNKS):
        df = get_upbit_4h(market, HISTORY_CHUNK, to)
        if df is None or df.empty:
            break

        all_df = df.copy() if all_df is None else pd.concat(
            [df, all_df], ignore_index=True
        )
        all_df = (
            all_df.drop_duplicates("datetime")
            .sort_values("datetime")
            .reset_index(drop=True)
        )

        if len(all_df) >= 125:
            return all_df

        to = all_df.datetime.iloc[0].strftime("%Y-%m-%dT%H:%M:%S")

    return all_df


# =========================================================
# EMA
# =========================================================

def ema(df, period):
    if df is None or df.empty or "c" not in df:
        return None
    return pd.to_numeric(df.c, errors="coerce").ewm(
        span=period,
        adjust=False,
        min_periods=1
    ).mean()


def direction(df):
    if df is None or df.empty:
        return "none"

    e10, e30, e60, e120 = [ema(df, x) for x in (10,30,60,120)]
    if any(x is None for x in (e10,e30,e60,e120)):
        return "none"

    a,b,c,d = [x.iloc[-1] for x in (e10,e30,e60,e120)]

    if any(pd.isna(x) for x in (a,b,c,d)):
        return "none"
    if a > b > c > d:
        return "long"
    if a < b < c < d:
        return "short"
    return "none"


def direction_series(df):
    if df is None or df.empty:
        return []

    es = [ema(df, x) for x in (10,30,60,120)]
    if any(x is None for x in es):
        return []

    result = []
    for i in range(len(df)):
        a,b,c,d = [x.iloc[i] for x in es]

        if any(pd.isna(x) for x in (a,b,c,d)):
            result.append("none")
        elif a > b > c > d:
            result.append("long")
        elif a < b < c < d:
            result.append("short")
        else:
            result.append("none")

    return result


def ema_display(df):
    d = direction(df)
    return {
        "display": "🟢 LONG" if d == "long"
                   else "🔴 SHORT" if d == "short"
                   else "⚪",
        "direction": d
    }


# =========================================================
# 배열 시작점
# =========================================================

def latest_alignment(df, wanted):
    ds = direction_series(df)
    if not ds:
        return None

    latest = None
    for i in range(min(120, len(ds)-1), len(ds)):
        if ds[i] == wanted and (i == 0 or ds[i-1] != wanted):
            latest = i

    return (
        {"direction": wanted, "index": latest}
        if latest is not None else None
    )


def alignment_valid(ds, start, end, wanted):
    if start < 0 or end >= len(ds):
        return False
    return all(x == wanted for x in ds[start:end+1])


# =========================================================
# 스윙
# =========================================================

def swings(df, start, end, high=True):
    col = "h" if high else "l"
    fn = max if high else min
    result = []

    start = max(start, SWING_LEFT)
    end = min(end, len(df)-SWING_RIGHT-1)

    for i in range(start, end+1):
        try:
            value = float(df[col].iloc[i])
            left = pd.to_numeric(
                df[col].iloc[i-SWING_LEFT:i], errors="coerce"
            )
            right = pd.to_numeric(
                df[col].iloc[i+1:i+1+SWING_RIGHT], errors="coerce"
            )

            if not left.empty and not right.empty:
                if high and value >= left.max() and value >= right.max():
                    result.append((i,value))
                elif not high and value <= left.min() and value <= right.min():
                    result.append((i,value))
        except:
            pass

    return result


# =========================================================
# 해지
# =========================================================

def invalidated(df, i, long=True):
    if i <= 0 or i >= len(df):
        return False

    try:
        o = float(df.o.iloc[i])
        c = float(df.c.iloc[i])
        p = float(df.c.iloc[i-1])

        return (
            c < o and c < p
            if long
            else c > o and c > p
        )
    except:
        return False


def breakout_id(exchange, symbol, tf, df, i):
    if i is None or i < 0 or i >= len(df):
        return None

    try:
        cid = int(df.ts.iloc[i]) if "ts" in df else str(df.datetime.iloc[i])
        return f"{exchange}:{symbol}:{tf}:{cid}"
    except:
        return None


def mark_invalidation_once(cid):
    if not cid:
        return False

    with invalidation_lock:
        if cid in shown_invalidation_ids:
            return False
        shown_invalidation_ids.add(cid)
        return True


# =========================================================
# N자 공통 엔진
# =========================================================

def find_n_breakout(df, alignment, long=True):
    empty = {
        "status": "none",
        "direction": "long" if long else "short",
        "breakout_index": None,
        "breakout_level": None,
        "invalidation_index": None
    }

    if (
        alignment is None
        or df is None
        or len(df) < 125
        or alignment.get("direction") != empty["direction"]
    ):
        return empty

    ds = direction_series(df)
    if not ds:
        return empty

    start = alignment["index"]
    current = len(df)-1

    if start >= current or not alignment_valid(
        ds, start, current, empty["direction"]
    ):
        return empty

    sw = swings(
        df,
        start + SWING_RIGHT + 1,
        current,
        high=long
    )

    if not sw:
        return empty

    for anchor_i, anchor in reversed(sw):

        if anchor_i >= current:
            continue

        if long:
            corr = pd.to_numeric(
                df.l.iloc[anchor_i+1:current+1],
                errors="coerce"
            )
            if corr.empty:
                continue

            correction = corr.min()
            if pd.isna(correction):
                continue

            if (anchor-correction)/anchor < MIN_CORRECTION_RATE:
                continue

            attempt = None
            attempt_i = None

            for i in range(anchor_i+1, current+1):

                if ds[i] != "long":
                    break

                try:
                    high = float(df.h.iloc[i])
                    low = float(df.l.iloc[i])
                    close = float(df.c.iloc[i])
                except:
                    continue

                if close > anchor:
                    bi = i
                    break

                correction = min(float(correction), low)

                if i >= SWING_RIGHT:
                    left = pd.to_numeric(
                        df.h.iloc[
                            max(anchor_i+1, i-SWING_LEFT):i
                        ],
                        errors="coerce"
                    )

                    if (
                        not left.empty
                        and high >= left.max()
                        and high < anchor
                    ):
                        attempt = high
                        attempt_i = i

                if attempt is not None and i > attempt_i:
                    if (attempt-low)/attempt >= MIN_CORRECTION_RATE:
                        correction = low
                        attempt = attempt_i = None

            else:
                bi = None

        else:
            corr = pd.to_numeric(
                df.h.iloc[anchor_i+1:current+1],
                errors="coerce"
            )
            if corr.empty:
                continue

            correction = corr.max()
            if pd.isna(correction):
                continue

            if (correction-anchor)/anchor < MIN_CORRECTION_RATE:
                continue

            attempt = None
            attempt_i = None

            for i in range(anchor_i+1, current+1):

                if ds[i] != "short":
                    break

                try:
                    high = float(df.h.iloc[i])
                    low = float(df.l.iloc[i])
                    close = float(df.c.iloc[i])
                except:
                    continue

                if close < anchor:
                    bi = i
                    break

                correction = max(float(correction), high)

                if i >= SWING_RIGHT:
                    left = pd.to_numeric(
                        df.l.iloc[
                            max(anchor_i+1, i-SWING_LEFT):i
                        ],
                        errors="coerce"
                    )

                    if (
                        not left.empty
                        and low <= left.min()
                        and low > anchor
                    ):
                        attempt = low
                        attempt_i = i

                if attempt is not None and i > attempt_i:
                    if (high-attempt)/attempt >= MIN_CORRECTION_RATE:
                        correction = high
                        attempt = attempt_i = None

            else:
                bi = None

        if bi is None:
            continue

        inv = None
        for i in range(bi+1, current+1):
            if invalidated(df, i, long):
                inv = i

        if inv is not None:
            if inv < current:
                continue

            return {
                "status": "invalidated",
                "direction": empty["direction"],
                "breakout_index": bi,
                "breakout_level": anchor,
                "invalidation_index": inv
            }

        return {
            "status": "breakout",
            "direction": empty["direction"],
            "breakout_index": bi,
            "breakout_level": anchor,
            "invalidation_index": None
        }

    return empty


# =========================================================
# 15M N자
# =========================================================

def get_15m_signal(df, exchange, symbol, allow_short=True):
    empty = {
        "signal": "none",
        "direction": "none",
        "breakout_id": None,
        "breakout_index": None,
        "warning_index": None,
        "invalidation_id": None,
        "invalidation_index": None
    }

    if df is None or len(df) < 125:
        return empty

    current_dir = direction(df)

    if current_dir not in ("long", "short"):
        return {**empty, "direction": current_dir}

    if current_dir == "short" and not allow_short:
        return {**empty, "direction": "short"}

    alignment = latest_alignment(df, current_dir)
    result = find_n_breakout(
        df,
        alignment,
        long=current_dir == "long"
    )

    if result["status"] == "invalidated":
        idx = result["invalidation_index"]
        iid = breakout_id(exchange, symbol, "15M", df, idx)

        if mark_invalidation_once(iid):
            return {
                **empty,
                "direction": current_dir,
                "breakout_index": result["breakout_index"],
                "warning_index": idx,
                "invalidation_id": iid,
                "invalidation_index": idx,
                "signal": "invalidated"
            }

        return {
            **empty,
            "direction": current_dir,
            "breakout_index": result["breakout_index"],
            "invalidation_id": iid,
            "invalidation_index": idx
        }

    bi = result.get("breakout_index")
    if bi is None:
        return {**empty, "direction": current_dir}

    bid = breakout_id(exchange, symbol, "15M", df, bi)
    count = len(df)-bi

    if not bid or count < 1:
        return empty

    return {
        **empty,
        "signal": str(count),
        "direction": current_dir,
        "breakout_id": bid,
        "breakout_index": bi
    }


# =========================================================
# 멀티 타임프레임
# =========================================================

def multi_signal(df15, df1h, df4h, exchange, symbol, allow_short):
    w = get_15m_signal(
        df15, exchange, symbol, allow_short
    )

    d15 = direction(df15)
    d1 = direction(df1h)
    d4 = direction(df4h)

    if w["direction"] == "long":
        ok = d15 == d1 == d4 == "long"
    elif w["direction"] == "short":
        ok = d15 == d1 == d4 == "short"
    else:
        ok = False

    if not ok:
        w["signal"] = "none"
        w["warning_index"] = None

    return {"15m": w}


# =========================================================
# 변동률 / 거래대금
# =========================================================

def daily_change_upbit(market):
    r = retry(
        requests.get,
        "https://api.upbit.com/v1/candles/days",
        params={"market": market, "count": 1},
        timeout=15
    )
    try:
        x = r.json()[0].get("change_rate")
        return [round(float(x)*100, 2)] if x is not None else None
    except:
        return None


def daily_changes(df):
    if df is None or df.empty:
        return None

    try:
        x = df.copy()
        x["datetime"] = (
            pd.to_datetime(x.ts, unit="ms", utc=True)
            .dt.tz_convert("Asia/Seoul")
            .dt.tz_localize(None)
        )
        x["c"] = pd.to_numeric(x.c, errors="coerce")
        x = x.dropna(subset=["datetime","c"]).set_index("datetime")

        daily = x.c.resample("1D", offset="9h").last().dropna()
        if len(daily) < 2:
            return None

        result = []
        for i in range(max(1, len(daily)-3), len(daily)):
            if daily.iloc[i-1] != 0:
                result.append(round(
                    (daily.iloc[i]-daily.iloc[i-1])
                    / daily.iloc[i-1] * 100, 2
                ))

        return result[::-1]

    except:
        return None


def format_volume(v):
    if v is None:
        return "-"
    try:
        v = float(v)
    except:
        return "-"

    if v >= 1e12:
        return f"{v/1e12:.2f}조"
    if v >= 1e8:
        return f"{v/1e8:,.0f}억"
    return f"{v/1e4:,.0f}만원"


def format_change(x):
    if not x:
        return '<span class="change-item">⬜ N/A</span>'

    try:
        v = float(x[0])
    except:
        return '<span class="change-item">⬜ N/A</span>'

    icon = "☀️" if v > 0 else "☁️"
    sign = "+" if v > 0 else ""
    cls = "positive" if v > 0 else "negative" if v < 0 else "neutral"

    return (
        f'<span class="change-item {cls}">'
        f'{icon} {sign}{v:.2f}%</span>'
    )


# =========================================================
# 분석
# =========================================================

def analyze(market, okx=False):
    if okx:
        df4 = history_okx(market, "4H")
        df1 = history_okx(market, "1H")
        df15 = history_okx(market, "15m", INITIAL_CANDLE_COUNT)
    else:
        df4 = history_upbit_4h(market)
        df1 = history_upbit(market, 60)
        df15 = history_upbit(market, 15, INITIAL_CANDLE_COUNT)

    if any(x is None or x.empty for x in (df4,df1,df15)):
        return None

    e15 = ema_display(df15)
    e1 = ema_display(df1)
    e4 = ema_display(df4)

    warnings = multi_signal(
        df15,
        df1,
        df4,
        "OKX" if okx else "UPBIT",
        market,
        allow_short=okx
    )

    w = warnings["15m"]
    d = w["direction"]
    signal = w["signal"]

    valid = signal.isdigit() and int(signal) >= 1

    qualified = (
        valid
        and e15["direction"] == d
        and e1["direction"] == d
        and e4["direction"] == d
        and d in ("long","short")
    )

    changes = (
        daily_changes(df15)
        if okx
        else daily_change_upbit(market)
    )

    return {
        "ema": e15,
        "ema_1h": e1,
        "ema_4h": e4,
        "warning": w,
        "warnings": warnings,
        "changes": changes,
        "qualified": qualified
    }


# =========================================================
# 필터
# =========================================================

def pass_filter(a, short=False):
    if not a:
        return False

    d = "short" if short else "long"

    return (
        a["ema"]["direction"] == d
        and a["ema_1h"]["direction"] == d
        and a["ema_4h"]["direction"] == d
        and a["warning"]["direction"] == d
        and a["warning"]["signal"].isdigit()
        and int(a["warning"]["signal"]) >= 1
    )


def empty_analysis():
    e = {"display":"⚪","direction":"none"}
    w = {
        "signal":"none",
        "direction":"none",
        "breakout_id":None,
        "breakout_index":None,
        "warning_index":None,
        "invalidation_id":None,
        "invalidation_index":None
    }

    return {
        "ema":e.copy(),
        "ema_1h":e.copy(),
        "ema_4h":e.copy(),
        "warning":w.copy(),
        "warnings":{"15m":w.copy()},
        "changes":None,
        "qualified":False
    }


# =========================================================
# 업비트 업데이트
# =========================================================

def update_upbit():
    global latest_upbit_data, latest_upbit_update_time

    log.info(f"========== 업비트 TOP{TOP_N} 시작 ==========")

    markets = sorted(
        get_upbit_markets(),
        key=lambda x:x["volume_24h"],
        reverse=True
    )

    rows = []

    for rank,item in enumerate(markets[:TOP_N],1):
        market = item["market"]
        coin = market.replace("KRW-","")

        try:
            a = analyze(market)
            a = a or empty_analysis()

            qualified = pass_filter(a, False)

            rows.append({
                "rank":rank,
                "name":coin,
                "change":format_change(a["changes"]),
                "volume":format_volume(item["volume_24h"]),
                "ema":a["ema"],
                "ema_1h":a["ema_1h"],
                "ema_4h":a["ema_4h"],
                "direction":"long" if qualified else "none",
                "warning":a["warning"],
                "warnings":a["warnings"],
                "qualified":qualified
            })

        except Exception as e:
            log.error(f"업비트 상세 오류 {market}: {e}")

            a = empty_analysis()
            rows.append({
                "rank":rank,
                "name":coin,
                "change":"",
                "volume":format_volume(item["volume_24h"]),
                "ema":a["ema"],
                "ema_1h":a["ema_1h"],
                "ema_4h":a["ema_4h"],
                "direction":"none",
                "warning":a["warning"],
                "warnings":a["warnings"],
                "qualified":False
            })

    latest_upbit_data = rows
    latest_upbit_update_time = kst()

    log.info(
        f"업비트 완료 / 조건 충족 "
        f"{sum(x['qualified'] for x in rows)}개"
    )

    return True


# =========================================================
# OKX
# =========================================================

def get_okx_symbols():
    r = retry(
        requests.get,
        "https://www.okx.com/api/v5/public/instruments",
        params={"instType":"SWAP"},
        timeout=15
    )
    if r is None:
        return []

    try:
        return [
            x["instId"]
            for x in r.json().get("data",[])
            if x.get("instId","").endswith("-USDT-SWAP")
            and x.get("state") == "live"
        ]
    except:
        return []


def get_okx_volume(inst, usdt):
    df = get_okx_ohlcv(inst,"1H",24)
    if df is None or df.empty:
        return None

    try:
        return float(
            pd.to_numeric(
                df.volCcyQuote,
                errors="coerce"
            ).sum()
        ) * float(usdt)
    except:
        return None


def update_okx(usdt):
    global latest_okx_data, latest_okx_update_time

    if not usdt or usdt <= 0:
        return False

    symbols = get_okx_symbols()
    if not symbols:
        return False

    upbit_set = {
        x.replace("KRW-","")
        for x in latest_upbit_markets
    }

    volumes = {}

    for s in symbols:
        v = get_okx_volume(s,usdt)
        if v and v > 0:
            volumes[s] = v

    top = sorted(
        volumes,
        key=volumes.get,
        reverse=True
    )[:TOP_N]

    rows = []

    for rank,symbol in enumerate(top,1):
        coin = symbol.replace("-USDT-SWAP","")
        display = f"{coin}[UP]" if coin in upbit_set else coin

        try:
            a = analyze(symbol,True)
            a = a or empty_analysis()

            d = a["warning"]["direction"]
            qualified = pass_filter(
                a,
                short=(d == "short")
            ) if d in ("long","short") else False

            rows.append({
                "rank":rank,
                "name":display,
                "change":format_change(a["changes"]),
                "volume":format_volume(volumes[symbol]),
                "ema":a["ema"],
                "ema_1h":a["ema_1h"],
                "ema_4h":a["ema_4h"],
                "direction":d if qualified else "none",
                "warning":a["warning"],
                "warnings":a["warnings"],
                "qualified":qualified
            })

        except Exception as e:
            log.error(f"OKX 상세 오류 {symbol}: {e}")

            a = empty_analysis()
            rows.append({
                "rank":rank,
                "name":display,
                "change":"",
                "volume":format_volume(volumes[symbol]),
                "ema":a["ema"],
                "ema_1h":a["ema_1h"],
                "ema_4h":a["ema_4h"],
                "direction":"none",
                "warning":a["warning"],
                "warnings":a["warnings"],
                "qualified":False
            })

    latest_okx_data = rows
    latest_okx_update_time = kst()

    return True


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():
    global latest_usdt_krw, latest_upbit_data, latest_okx_data

    if not update_lock.acquire(False):
        log.warning("이전 조회 진행 중 → 건너뜀")
        return

    try:
        log.info(f"========== 전체 조회 {kst()} ==========")

        if USE_UPBIT == "Y":
            try:
                update_upbit()
            except Exception as e:
                log.exception(f"업비트 업데이트 오류: {e}")
        else:
            latest_upbit_data = []

        if USE_OKX == "Y":
            try:
                usdt = get_usdt_krw()
                if usdt:
                    latest_usdt_krw = usdt
                else:
                    usdt = latest_usdt_krw

                if usdt > 0:
                    update_okx(usdt)
            except Exception as e:
                log.exception(f"OKX 업데이트 오류: {e}")
        else:
            latest_okx_data = []

    finally:
        update_lock.release()


# =========================================================
# HTML
# =========================================================

def warning_html(w):
    if not w:
        return "-"

    s = w.get("signal","none")

    if s == "invalidated":
        return '<span class="inv">🚨</span>'

    if str(s).isdigit() and int(s) >= 1:
        return f'<span class="rocket">🚀({s})</span>'

    return "-"


def direction_html(d):
    if d == "long":
        return '<span class="long">LONG</span>'
    if d == "short":
        return '<span class="short">SHORT</span>'
    return '<span class="none">-</span>'


def rows_html(data):
    out = ""

    for x in data:
        q = x.get("qualified",False)
        dc = x.get("direction","none")
        cls = " qualified" if q else ""

        e15 = x.get("ema",{})
        e1 = x.get("ema_1h",{})
        e4 = x.get("ema_4h",{})

        out += f"""
<tr class="{cls}">
<td>{x.get('rank','-')}</td>

<td>
<div class="coin">{x.get('name','-')}</div>
{x.get('change','')}
</td>

<td>
<div class="vol">{x.get('volume','-')}</div>
{direction_html(dc)}
</td>

<td>
<div class="ema">
<div><b>15M</b>{e15.get('display','⚪')}</div>
<div><b>1H</b>{e1.get('display','⚪')}</div>
<div><b>4H</b>{e4.get('display','⚪')}</div>
</div>
</td>

<td>
<div class="warning">
<span class="label">15M</span>
{warning_html(x.get('warning',{}))}
</div>
</td>
</tr>
"""

    return out


def section(title,data,update_time):
    rows = rows_html(data)

    if not rows:
        rows = """
<tr>
<td colspan="5" class="empty">
현재 조회 데이터 없음
</td>
</tr>
"""

    return f"""
<h2>
🏆 {title} TOP{TOP_N}
<small>조회 {update_time} KST</small>
</h2>

<div class="table-wrap">
<table>
<thead>
<tr>
<th>#</th>
<th>코인</th>
<th>거래대금</th>
<th>EMA</th>
<th>N자</th>
</tr>
</thead>
<tbody>
{rows}
</tbody>
</table>
</div>
"""


CSS = """
*{box-sizing:border-box}
html,body{margin:0;padding:0;width:100%;overflow-x:hidden}
body{
 background:#0f1115;color:#eee;font-family:Arial,sans-serif;
 font-size:9px;padding:4px
}
h1{margin:3px 2px 6px;font-size:14px}
h2{
 margin:10px 2px 5px;font-size:11px
}
h2 small{
 color:#777;font-size:7px;font-weight:normal
}
.info{
 margin:0 2px 6px;padding:5px 6px;
 color:#8b9099;background:#171a1f;
 border:1px solid #252a31;border-radius:7px;
 font-size:7px;line-height:1.5
}
.status{
 display:flex;gap:7px;margin-top:4px;font-weight:bold
}
.y{color:#35e66d}.n{color:#ff4d4d}
.table-wrap{
 width:100%;overflow:hidden;border-radius:8px;
 border:1px solid #252a31
}
table{
 width:100%;table-layout:fixed;
 border-collapse:collapse;background:#181c21
}
th{
 padding:5px 1px;background:#12151a;
 border-bottom:1px solid #2b3037;
 color:#8f949d;font-size:6px
}
td{
 padding:5px 1px;border-bottom:1px solid #272c32;
 text-align:center;vertical-align:middle
}
th:nth-child(1),td:nth-child(1){width:6%}
th:nth-child(2),td:nth-child(2){width:20%}
th:nth-child(3),td:nth-child(3){width:18%}
th:nth-child(4),td:nth-child(4){width:26%}
th:nth-child(5),td:nth-child(5){width:30%}
.coin{font-size:8px;font-weight:bold;white-space:nowrap}
.change-item{
 display:block;font-size:7px;font-weight:bold
}
.positive,.negative{color:#fff}.neutral{color:#aaa}
.vol{font-size:7px;font-weight:bold;margin-bottom:2px}
.long{
 display:block;color:#35e66d;
 font-size:7px;font-weight:800
}
.short{
 display:block;color:#ff4d4d;
 font-size:7px;font-weight:800
}
.none{color:#666;font-size:7px}
.ema{
 display:flex;flex-direction:column;
 text-align:left
}
.ema div{
 height:13px;line-height:13px;
 white-space:nowrap;font-size:7px;font-weight:bold
}
.ema b{
 display:inline-block;width:28px;
 color:#8f949d;font-size:6px;
 text-align:right;margin-right:3px
}
.warning{
 min-height:14px;display:flex;
 justify-content:center;align-items:center;
 gap:2px;white-space:nowrap
}
.label{color:#777;font-size:6px;font-weight:bold}
.rocket,.inv{
 font-size:10px;font-weight:bold
}
.rocket{
 filter:drop-shadow(0 0 4px rgba(50,255,100,.9))
}
.inv{
 filter:drop-shadow(0 0 4px rgba(255,190,50,.95))
}
.qualified{
 animation:blink 1.2s infinite
}
@keyframes blink{
 0%,100%{background:#181c21}
 50%{background:#26352b}
}
.empty{color:#555;padding:12px 4px}
@media(max-width:480px){
 body{padding:3px;font-size:8px}
 h1{font-size:13px}
 h2{font-size:10px}
 .info{font-size:6px;padding:4px 5px}
 th{padding:4px 1px;font-size:5px}
 td{padding:4px 1px}
 .coin{font-size:7px}
 .change-item,.vol{font-size:6px}
 .long,.short{font-size:6px}
 .rocket,.inv{font-size:9px}
 .label{font-size:5px}
 .ema div{height:12px;line-height:12px;font-size:6px}
 .ema b{width:25px;font-size:5px}
}
"""


# =========================================================
# 대시보드
# =========================================================

@app.get("/",response_class=HTMLResponse)
def dashboard():

    status = f"""
<div class="status">
<span class="{'y' if USE_UPBIT=='Y' else 'n'}">
업비트 : {USE_UPBIT}
</span>
<span class="{'y' if USE_OKX=='Y' else 'n'}">
OKX : {USE_OKX}
</span>
</div>
"""

    sections = ""

    if USE_UPBIT == "Y":
        sections += section(
            "업비트",
            latest_upbit_data,
            latest_upbit_update_time
        )

    if USE_OKX == "Y":
        sections += section(
            "OKX",
            latest_okx_data,
            latest_okx_update_time
        )

    return f"""
<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="60">
<meta name="viewport"
content="width=device-width,initial-scale=1.0,
maximum-scale=1.0,user-scalable=no">
<title>15M N Pattern Breakout</title>
<style>{CSS}</style>
</head>

<body>

<h1>📊 15M N Pattern Breakout</h1>

<div class="info">
<div>① 24시간 거래대금 TOP{TOP_N}</div>
<div>② 4H / 1H / 15M EMA 10-30-60-120</div>
<div>③ N자 구조는 15M만 분석</div>
<div>④ LONG : 15M + 1H + 4H 모두 정배열</div>
<div>⑤ SHORT : 15M + 1H + 4H 모두 역배열</div>
<div>⑥ N자 돌파 후 🚀(1)부터 계속 카운터</div>
<div>⑦ 🚀 카운터 제한 없음</div>
<div>⑧ LONG 해지 : 음봉 + 현재 종가 &lt; 이전 종가</div>
<div>⑨ SHORT 해지 : 양봉 + 현재 종가 &gt; 이전 종가</div>
<div>⑩ 해지 순간 🚨 1회 표시 후 새 N자 대기</div>
<div>⑪ 1H / 4H N자 분석 없음</div>
{status}
</div>

{sections}

</body>
</html>
"""


# =========================================================
# 스케줄러
# =========================================================

def scheduler():
    log.info("스케줄러 시작")

    while True:
        try:
            schedule.run_pending()
        except Exception as e:
            log.exception(f"스케줄러 오류: {e}")
        time.sleep(1)


# =========================================================
# 시작
# =========================================================

@app.on_event("startup")
def startup():

    if USE_UPBIT not in ("Y","N"):
        raise ValueError("USE_UPBIT은 Y 또는 N만 가능합니다.")

    if USE_OKX not in ("Y","N"):
        raise ValueError("USE_OKX는 Y 또는 N만 가능합니다.")

    if TOP_N <= 0:
        raise ValueError("TOP_N은 1 이상이어야 합니다.")

    if UPDATE_MINUTES <= 0:
        raise ValueError("UPDATE_MINUTES는 1 이상이어야 합니다.")

    log.info("========================================")
    log.info("서버 시작")
    log.info(f"업비트={USE_UPBIT} / OKX={USE_OKX}")
    log.info(f"TOP={TOP_N} / UPDATE={UPDATE_MINUTES}분")
    log.info("EMA = 10-30-60-120")
    log.info("N자 = 15M만")
    log.info("LONG = 15M + 1H + 4H 정배열")
    log.info("SHORT = 15M + 1H + 4H 역배열")
    log.info("🚀 돌파 후 카운터 제한 없음")
    log.info("🚨 해지봉 1회 표시")
    log.info("현재 진행 중인 봉 제외")
    log.info("========================================")

    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    schedule.every(
        UPDATE_MINUTES
    ).minutes.do(update_dashboard)

    threading.Thread(
        target=scheduler,
        daemon=True
    ).start()


# =========================================================
# 실행
# =========================================================

if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
            )
