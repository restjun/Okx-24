from fastapi import FastAPI
from fastapi.responses import HTMLResponse

import schedule
import time
import requests
import threading
import uvicorn
import logging
import pandas as pd
import warnings

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
TOP_N = 100
UPDATE_MINUTES = 1

INITIAL_CANDLE_COUNT = 200
HISTORY_CHUNK = 200
MAX_HISTORY_CHUNKS = 10

USE_UPBIT = "Y"
USE_OKX = "N"

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10

KST = ZoneInfo("Asia/Seoul")


# =========================================================
# 전역 상태
# =========================================================

latest_upbit_data = []
latest_okx_data = []

latest_usdt_krw = 0

latest_upbit_update_time = "-"
latest_okx_update_time = "-"

latest_upbit_markets = []

request_lock = threading.Lock()
update_lock = threading.Lock()
air_state_lock = threading.Lock()

last_request_time = 0

# 비행기 상태
air_state = {}


# =========================================================
# 공통
# =========================================================

def kst():
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


def empty_air():
    return {
        "active": False,
        "direction": None,
        "count": 0,
        "stopped": False
    }


def empty_ema():
    return {
        "display": "⚪(0)",
        "direction": "none",
        "count": 0
    }


def wait_request():
    global last_request_time

    with request_lock:
        gap = time.monotonic() - last_request_time

        if gap < REQUEST_INTERVAL:
            time.sleep(REQUEST_INTERVAL - gap)

        last_request_time = time.monotonic()


def retry(func, *args, **kwargs):
    name = getattr(func, "__name__", str(func))

    url = (
        args[0]
        if args and isinstance(args[0], str)
        else kwargs.get("url", "")
    )

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
# Upbit 마켓
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
        result = []

        for x in r.json():
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

        latest_upbit_markets = [
            x["market"] for x in result
        ]

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

    if r is None:
        return None

    try:
        price = float(r.json()[0]["trade_price"])
        return price if price > 0 else None
    except:
        return None


# =========================================================
# OKX 캔들
# =========================================================

def get_okx_ohlcv(inst, bar="1H", limit=200, before=None):
    params = {
        "instId": inst,
        "bar": bar,
        "limit": min(max(int(limit), 1), 200)
    }

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

        df = pd.DataFrame(
            data,
            columns=[
                "ts", "o", "h", "l", "c",
                "vol", "volCcy", "volCcyQuote", "confirm"
            ]
        )

        numeric_cols = [
            "ts", "o", "h", "l", "c",
            "vol", "volCcy", "volCcyQuote"
        ]

        for col in numeric_cols:
            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

        df = df[df.confirm.astype(str) == "1"]

        if df.empty:
            return None

        df["datetime"] = (
            pd.to_datetime(
                df["ts"],
                unit="ms",
                utc=True
            )
            .dt.tz_convert(KST)
            .dt.tz_localize(None)
        )

        now = datetime.now(KST)

        if bar == "1H":
            current = now.replace(
                minute=0,
                second=0,
                microsecond=0
            ).replace(tzinfo=None)
        else:
            block = (now.hour // 4) * 4

            current = now.replace(
                hour=block,
                minute=0,
                second=0,
                microsecond=0
            ).replace(tzinfo=None)

        df = df[df.datetime < current]

        if df.empty:
            return None

        return (
            df.sort_values("ts")
            .drop_duplicates("ts")
            .reset_index(drop=True)
        )

    except Exception as e:
        log.error(f"OKX {inst} {bar} 오류: {e}")
        return None


# =========================================================
# Upbit 캔들 공통
# =========================================================

def get_upbit_candle(
    market,
    minutes,
    count=200,
    to=None
):
    params = {
        "market": market,
        "count": min(max(int(count), 1), 200)
    }

    if to:
        params["to"] = to

    r = retry(
        requests.get,
        f"https://api.upbit.com/v1/candles/minutes/{minutes}",
        params=params,
        timeout=15
    )

    if r is None:
        return None

    try:
        df = pd.DataFrame(r.json())

        if df.empty:
            return None

        mapping = {
            "o": "opening_price",
            "h": "high_price",
            "l": "low_price",
            "c": "trade_price"
        }

        for new_col, old_col in mapping.items():
            df[new_col] = pd.to_numeric(
                df[old_col],
                errors="coerce"
            )

        if minutes == 60:
            df["volume_krw"] = pd.to_numeric(
                df.candle_acc_trade_price,
                errors="coerce"
            )

        df["datetime"] = pd.to_datetime(
            df.candle_date_time_kst,
            errors="coerce"
        )

        df = df.dropna(
            subset=["datetime", "o", "h", "l", "c"]
        )

        if df.empty:
            return None

        now = datetime.now(KST)

        if minutes == 60:
            current = now.replace(
                minute=0,
                second=0,
                microsecond=0
            ).replace(tzinfo=None)
        else:
            block = (now.hour // 4) * 4

            current = now.replace(
                hour=block,
                minute=0,
                second=0,
                microsecond=0
            ).replace(tzinfo=None)

        df = df[df.datetime < current]

        if df.empty:
            return None

        return (
            df.sort_values("datetime")
            .drop_duplicates("datetime")
            .reset_index(drop=True)
        )

    except Exception as e:
        log.error(
            f"업비트 {minutes}분 오류 {market}: {e}"
        )
        return None


def get_upbit_1h(market, count=200, to=None):
    return get_upbit_candle(
        market, 60, count, to
    )


def get_upbit_4h(market, count=200, to=None):
    return get_upbit_candle(
        market, 240, count, to
    )


# =========================================================
# History 공통
# =========================================================

def history_okx(inst, bar, required=125):
    all_df = None
    before = None

    for _ in range(MAX_HISTORY_CHUNKS):
        df = get_okx_ohlcv(
            inst,
            bar,
            HISTORY_CHUNK,
            before
        )

        if df is None or df.empty:
            break

        all_df = (
            df.copy()
            if all_df is None
            else pd.concat(
                [df, all_df],
                ignore_index=True
            )
        )

        all_df = (
            all_df
            .drop_duplicates("ts")
            .sort_values("ts")
            .reset_index(drop=True)
        )

        if len(all_df) >= required:
            return all_df

        before = int(all_df.ts.iloc[0])

    return all_df


def history_upbit(market, minutes, required=125):
    all_df = None
    to = None

    for _ in range(MAX_HISTORY_CHUNKS):
        df = get_upbit_candle(
            market,
            minutes,
            HISTORY_CHUNK,
            to
        )

        if df is None or df.empty:
            break

        all_df = (
            df.copy()
            if all_df is None
            else pd.concat(
                [df, all_df],
                ignore_index=True
            )
        )

        all_df = (
            all_df
            .drop_duplicates("datetime")
            .sort_values("datetime")
            .reset_index(drop=True)
        )

        if len(all_df) >= required:
            return all_df

        to = all_df.datetime.iloc[0].strftime(
            "%Y-%m-%dT%H:%M:%S"
        )

    return all_df


def history_upbit_4h(market):
    return history_upbit(
        market,
        240,
        125
    )


# =========================================================
# EMA
# =========================================================

def ema(df, period):
    if (
        df is None
        or df.empty
        or "c" not in df
    ):
        return None

    return pd.to_numeric(
        df.c,
        errors="coerce"
    ).ewm(
        span=period,
        adjust=False,
        min_periods=1
    ).mean()


def ema_values(df):
    if df is None or df.empty:
        return None

    return {
        10: ema(df, 10),
        30: ema(df, 30),
        60: ema(df, 60),
        120: ema(df, 120)
    }


# =========================================================
# EMA 방향
# =========================================================

def direction(df):
    e = ema_values(df)

    if not e:
        return "none"

    try:
        a = float(e[10].iloc[-1])
        b = float(e[30].iloc[-1])
        c = float(e[60].iloc[-1])
        d = float(e[120].iloc[-1])

        if a > b > c > d:
            return "long"

        if a < b < c < d:
            return "short"

    except:
        pass

    return "none"


# =========================================================
# EMA 정배열 연속 카운트
# =========================================================

def ema_alignment_count(df):
    e = ema_values(df)

    if not e:
        return {
            "direction": "none",
            "count": 0
        }

    try:
        current_direction = None
        count = 0

        for i in range(len(df) - 1, -1, -1):

            a = float(e[10].iloc[i])
            b = float(e[30].iloc[i])
            c = float(e[60].iloc[i])
            d = float(e[120].iloc[i])

            if a > b > c > d:
                candle_direction = "long"

            elif a < b < c < d:
                candle_direction = "short"

            else:
                candle_direction = "none"

            if i == len(df) - 1:
                current_direction = candle_direction

                if current_direction == "none":
                    return {
                        "direction": "none",
                        "count": 0
                    }

            if candle_direction == current_direction:
                count += 1
            else:
                break

        return {
            "direction": current_direction,
            "count": count
        }

    except Exception as e:
        log.error(f"EMA 카운팅 오류: {e}")

        return {
            "direction": "none",
            "count": 0
        }


def ema_display(df):
    result = ema_alignment_count(df)

    d = result["direction"]
    count = result["count"]

    icon = {
        "long": "🟢",
        "short": "🔴"
    }.get(d, "⚪")

    return {
        "display": f"{icon}({count if d != 'none' else 0})",
        "direction": d,
        "count": count if d != "none" else 0
    }


# =========================================================
# ★ 10선 종가 카운팅
#
# 아래:
#   🔻(1) → 🔻(2) → 🔻(3) ...
#
# 위:
#   🟢▲
#
# 위로 전환했다고 이전 하락 상태를
# 내부적으로 강제 삭제하지 않는다.
#
# 다시 아래 종가가 발생하면
# 새로운 하락 구간으로 보고 🔻(1)부터 시작.
# =========================================================

def close_vs_ema10_1h(df):
    if df is None or df.empty:
        return {
            "position": "none",
            "display": "-",
            "count": 0
        }

    try:
        e10 = ema(df, 10)

        if e10 is None or e10.empty:
            return {
                "position": "none",
                "display": "-",
                "count": 0
            }

        closes = pd.to_numeric(
            df.c,
            errors="coerce"
        ).reset_index(drop=True)

        emas = pd.to_numeric(
            e10,
            errors="coerce"
        ).reset_index(drop=True)

        close = float(closes.iloc[-1])
        current_ema10 = float(emas.iloc[-1])

        # -------------------------------------------------
        # 10선 위 종가
        # -------------------------------------------------
        if close > current_ema10:
            return {
                "position": "above",
                "display": "▲",
                "count": 0
            }

        # -------------------------------------------------
        # 10선 아래 종가
        # -------------------------------------------------
        if close < current_ema10:

            count = 0

            for i in range(
                len(closes) - 1,
                -1,
                -1
            ):
                try:
                    c = float(closes.iloc[i])
                    e = float(emas.iloc[i])
                except:
                    break

                if c < e:
                    count += 1
                else:
                    break

            return {
                "position": "below",
                "display": f"▼({count})",
                "count": count
            }

        return {
            "position": "equal",
            "display": "＝",
            "count": 0
        }

    except Exception as e:
        log.error(f"10선 카운팅 오류: {e}")

        return {
            "position": "none",
            "display": "-",
            "count": 0
        }


# =========================================================
# 비행기 최초 발생 조건
# =========================================================

def get_air_warning(df1h, df4h):
    if (
        df1h is None
        or df1h.empty
        or df4h is None
        or df4h.empty
        or len(df1h) < 2
    ):
        return None

    # 1H + 4H 모두 정배열
    if direction(df1h) != "long":
        return None

    if direction(df4h) != "long":
        return None

    try:
        e10 = ema(df1h, 10)

        prev_close = float(df1h.c.iloc[-2])
        prev_ema10 = float(e10.iloc[-2])

        current_open = float(df1h.o.iloc[-1])
        current_close = float(df1h.c.iloc[-1])
        current_ema10 = float(e10.iloc[-1])

        # 이전 종가가 10선 아래
        # 현재 양봉
        # 현재 종가가 10선 위
        if (
            prev_close < prev_ema10
            and current_close > current_open
            and current_close > current_ema10
        ):
            return "LONG"

    except:
        pass

    return None


# =========================================================
# ★ 비행기 카운터
#
# 최초 조건:
#   🛩️1
#
# 이후 새로운 1H 캔들:
#   양봉 + EMA10 위 종가
#   → 🛩️2
#   → 🛩️3
#   → 🛩️4 ...
#
# 중요:
# 최초 발생 이후에는 get_air_warning()이
# 다시 발생하지 않아도 계속 카운트한다.
#
# 같은 1H 캔들은 candle_time으로 중복 방지.
# =========================================================

def update_air_counter(
    market,
    df1h,
    new_warning
):
    if (
        df1h is None
        or df1h.empty
        or len(df1h) < 2
    ):
        return empty_air()

    try:
        candle_time = df1h.datetime.iloc[-1]

        current_open = float(
            df1h.o.iloc[-1]
        )

        current_close = float(
            df1h.c.iloc[-1]
        )

        e10 = ema(df1h, 10)

        if e10 is None or e10.empty:
            return empty_air()

        current_ema10 = float(
            e10.iloc[-1]
        )

    except:
        return empty_air()

    with air_state_lock:

        state = air_state.get(market)

        # =================================================
        # ① 최초 비행기 발생
        # =================================================

        if new_warning == "LONG":

            # 같은 캔들에서 반복 발생하는 것을 방지
            if (
                state is None
                or state.get("warning_candle") != candle_time
                or state.get("stopped", False)
            ):

                state = {
                    "active": True,
                    "direction": "LONG",
                    "count": 1,

                    # 최초 발생 캔들
                    "warning_candle": candle_time,

                    # 이 캔들은 이미 1로 계산했음
                    "counted_candle": candle_time,

                    "stopped": False
                }

                air_state[market] = state

                return {
                    "active": True,
                    "direction": "LONG",
                    "count": 1,
                    "stopped": False
                }

        # =================================================
        # ② 기존 비행기가 없는 경우
        # =================================================

        if (
            state is None
            or not state.get("active", False)
        ):
            return {
                "active": False,
                "direction": None,
                "count": 0,
                "stopped": (
                    state.get("stopped", False)
                    if state else False
                )
            }

        # =================================================
        # ③ 같은 1H 캔들 중복 방지
        # =================================================

        if candle_time <= state.get(
            "counted_candle"
        ):
            return {
                "active": True,
                "direction": state.get(
                    "direction",
                    "LONG"
                ),
                "count": state.get(
                    "count",
                    1
                ),
                "stopped": False
            }

        # 새로운 1H 캔들
        state["counted_candle"] = candle_time

        # =================================================
        # ④ 10선 아래 종가
        # =================================================

        if current_close < current_ema10:

            state["active"] = False
            state["stopped"] = True

            return {
                "active": False,
                "direction": state.get(
                    "direction",
                    "LONG"
                ),
                "count": state.get(
                    "count",
                    1
                ),
                "stopped": True
            }

        # =================================================
        # ⑤ 새로운 양봉 + 10선 위 종가
        #
        # ★ 핵심:
        # new_warning이 없어도 카운트 증가
        # =================================================

        if (
            current_close > current_open
            and current_close > current_ema10
        ):

            state["count"] = (
                state.get("count", 1) + 1
            )

            state["active"] = True
            state["stopped"] = False

            return {
                "active": True,
                "direction": state.get(
                    "direction",
                    "LONG"
                ),
                "count": state["count"],
                "stopped": False
            }

        # =================================================
        # ⑥ 10선 위라도 음봉이면 종료
        # =================================================

        if current_close <= current_open:

            state["active"] = False

            return {
                "active": False,
                "direction": state.get(
                    "direction",
                    "LONG"
                ),
                "count": state.get(
                    "count",
                    1
                ),
                "stopped": False
            }

        return {
            "active": state.get(
                "active",
                False
            ),
            "direction": state.get(
                "direction",
                "LONG"
            ),
            "count": state.get(
                "count",
                1
            ),
            "stopped": False
        }


# =========================================================
# 등락률
# =========================================================

def daily_change_upbit(market):
    r = retry(
        requests.get,
        "https://api.upbit.com/v1/candles/days",
        params={
            "market": market,
            "count": 2
        },
        timeout=15
    )

    if r is None:
        return None

    try:
        data = r.json()

        if len(data) < 2:
            return None

        current = float(data[0]["trade_price"])
        previous = float(data[1]["trade_price"])

        if previous == 0:
            return None

        return [
            (current - previous) / previous * 100
        ]

    except:
        return None


def daily_changes(df):
    if df is None or df.empty:
        return None

    try:
        x = df.copy()

        x["datetime"] = pd.to_datetime(
            x["datetime"],
            errors="coerce"
        )

        x["c"] = pd.to_numeric(
            x["c"],
            errors="coerce"
        )

        x = (
            x.dropna(
                subset=["datetime", "c"]
            )
            .set_index("datetime")
        )

        daily = (
            x["c"]
            .resample(
                "1D",
                offset="9h"
            )
            .last()
            .dropna()
        )

        if len(daily) < 2:
            return None

        current = float(daily.iloc[-1])
        previous = float(daily.iloc[-2])

        if previous == 0:
            return None

        return [
            (current - previous) / previous * 100
        ]

    except:
        return None


# =========================================================
# 표시
# =========================================================

def format_change(x):
    if x is None:
        return "-"

    try:
        value = float(
            x[0]
            if isinstance(x, (list, tuple))
            else x
        )

        if value > 0:
            return (
                '<span class="up">'
                f'▲ +{value:.2f}%'
                '</span>'
            )

        if value < 0:
            return (
                '<span class="down">'
                f'▼ {value:.2f}%'
                '</span>'
            )

        return (
            '<span class="zero">'
            '0.00%'
            '</span>'
        )

    except:
        return "-"


def format_volume(v):
    if v is None:
        return "-"

    try:
        v = float(v)
    except:
        return "-"

    if v >= 1e12:
        return f"{v / 1e12:.2f}조"

    if v >= 1e8:
        return f"{v / 1e8:.0f}억"

    if v >= 1e4:
        return f"{v / 1e4:.0f}만"

    return f"{v:,.0f}"


# =========================================================
# 분석
# =========================================================

def empty_analysis():
    return {
        "ema_1h": empty_ema(),
        "ema_4h": empty_ema(),

        "close_ema10_1h": {
            "position": "none",
            "display": "-",
            "count": 0
        },

        "changes": None,

        "air_warning": False,
        "air_direction": None,
        "air_count": 0,
        "air_active": False,
        "air_stopped": False,

        "qualified": False,

        "direction_1h": "none",
        "direction_4h": "none",

        "df1h": None
    }


def analyze(market, okx=False):

    if okx:
        df1 = history_okx(
            market,
            "1H"
        )

        df4 = history_okx(
            market,
            "4H"
        )

    else:
        df1 = history_upbit(
            market,
            60
        )

        df4 = history_upbit_4h(
            market
        )

    if (
        df1 is None
        or df1.empty
        or df4 is None
        or df4.empty
    ):
        return None

    e1 = ema_display(df1)
    e4 = ema_display(df4)

    # ★ 10선 위치 + 하락 연속 카운트
    close_ema10 = close_vs_ema10_1h(df1)

    # 최초 비행기 발생 여부
    new_warning = get_air_warning(
        df1,
        df4
    )

    # 비행기 상태 업데이트
    air = update_air_counter(
        market,
        df1,
        new_warning
    )

    changes = (
        daily_changes(df1)
        if okx
        else daily_change_upbit(market)
    )

    return {
        "ema_1h": e1,
        "ema_4h": e4,

        "close_ema10_1h": close_ema10,

        "changes": changes,

        "air_warning": air["active"],
        "air_direction": air["direction"],
        "air_count": air["count"],
        "air_active": air["active"],
        "air_stopped": air["stopped"],

        "qualified": air["active"],

        "direction_1h": e1["direction"],
        "direction_4h": e4["direction"],

        "df1h": df1
    }


# =========================================================
# 행 데이터 공통
# =========================================================

def make_row(
    rank,
    name,
    volume,
    analysis
):
    a = analysis or empty_analysis()

    return {
        "rank": rank,
        "name": name,
        "change": format_change(
            a["changes"]
        ),
        "volume": format_volume(
            volume
        ),
        "ema_1h": a["ema_1h"],
        "ema_4h": a["ema_4h"],
        "close_ema10_1h": a[
            "close_ema10_1h"
        ],
        "air_warning": a[
            "air_warning"
        ],
        "air_direction": a[
            "air_direction"
        ],
        "air_count": a[
            "air_count"
        ],
        "air_stopped": a.get(
            "air_stopped",
            False
        ),
        "qualified": a[
            "qualified"
        ]
    }


# =========================================================
# Upbit 업데이트
# =========================================================

def update_upbit():
    global latest_upbit_data
    global latest_upbit_update_time

    log.info(
        f"========== 업비트 TOP{TOP_N} 시작 =========="
    )

    markets = get_upbit_markets()

    markets.sort(
        key=lambda x: x["volume_24h"],
        reverse=True
    )

    rows = []

    for rank, item in enumerate(
        markets[:TOP_N],
        1
    ):

        market = item["market"]
        coin = market.replace(
            "KRW-",
            ""
        )

        try:
            a = analyze(market)

            rows.append(
                make_row(
                    rank,
                    coin,
                    item["volume_24h"],
                    a
                )
            )

        except Exception as e:
            log.error(
                f"업비트 상세 오류 {market}: {e}"
            )

            rows.append(
                make_row(
                    rank,
                    coin,
                    item["volume_24h"],
                    None
                )
            )

    latest_upbit_data = rows
    latest_upbit_update_time = kst()

    log.info(
        f"업비트 완료 / 비행기 "
        f"{sum(x['qualified'] for x in rows)}개"
    )


# =========================================================
# OKX 심볼
# =========================================================

def get_okx_symbols():
    r = retry(
        requests.get,
        "https://www.okx.com/api/v5/public/instruments",
        params={"instType": "SWAP"},
        timeout=15
    )

    if r is None:
        return []

    try:
        return [
            x["instId"]
            for x in r.json().get(
                "data",
                []
            )
            if x.get(
                "instId",
                ""
            ).endswith("-USDT-SWAP")
            and x.get("state") == "live"
        ]

    except:
        return []


def get_okx_volume(inst, usdt):
    df = get_okx_ohlcv(
        inst,
        "1H",
        VOLUME_HOURS
    )

    if df is None or df.empty:
        return None

    try:
        volume = float(
            pd.to_numeric(
                df.volCcyQuote,
                errors="coerce"
            ).sum()
        )

        return volume * float(usdt)

    except:
        return None


# =========================================================
# OKX 업데이트
# =========================================================

def update_okx(usdt):
    global latest_okx_data
    global latest_okx_update_time

    if not usdt or usdt <= 0:
        return False

    symbols = get_okx_symbols()

    if not symbols:
        return False

    upbit_set = {
        x.replace(
            "KRW-",
            ""
        )
        for x in latest_upbit_markets
    }

    volumes = {}

    for symbol in symbols:

        v = get_okx_volume(
            symbol,
            usdt
        )

        if v and v > 0:
            volumes[symbol] = v

    top = sorted(
        volumes,
        key=volumes.get,
        reverse=True
    )[:TOP_N]

    rows = []

    for rank, symbol in enumerate(
        top,
        1
    ):

        coin = symbol.replace(
            "-USDT-SWAP",
            ""
        )

        name = (
            f"{coin} (업비트)"
            if coin in upbit_set
            else coin
        )

        try:
            a = analyze(
                symbol,
                True
            )

            rows.append(
                make_row(
                    rank,
                    name,
                    volumes[symbol],
                    a
                )
            )

        except Exception as e:
            log.error(
                f"OKX 상세 오류 {symbol}: {e}"
            )

            rows.append(
                make_row(
                    rank,
                    name,
                    volumes[symbol],
                    None
                )
            )

    latest_okx_data = rows
    latest_okx_update_time = kst()

    log.info(
        f"OKX 완료 / 비행기 "
        f"{sum(x['qualified'] for x in rows)}개"
    )

    return True


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():
    global latest_usdt_krw
    global latest_upbit_data
    global latest_okx_data

    if not update_lock.acquire(False):
        log.warning(
            "이전 조회 진행 중 → 건너뜀"
        )
        return

    try:
        log.info(
            f"========== 전체 조회 {kst()} =========="
        )

        # -------------------------------
        # Upbit
        # -------------------------------

        if USE_UPBIT == "Y":
            try:
                update_upbit()

            except Exception as e:
                log.exception(
                    f"업비트 업데이트 오류: {e}"
                )
        else:
            latest_upbit_data = []

        # -------------------------------
        # OKX
        # -------------------------------

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
                log.exception(
                    f"OKX 업데이트 오류: {e}"
                )
        else:
            latest_okx_data = []

    finally:
        update_lock.release()


# =========================================================
# HTML 표시
# =========================================================

def warning_html(
    air_warning,
    air_direction=None,
    air_count=0,
    air_stopped=False
):
    if air_stopped:
        return '<div class="air-stop">⛔️</div>'

    if not air_warning:
        return "-"

    direction = air_direction or "LONG"

    count_html = (
        f'<div class="air-count">{air_count}</div>'
        if air_count > 0
        else ""
    )

    return (
        '<div class="air-box">'
        '<div class="air-main">'
        '<span class="air-direction long">'
        f'{direction}</span>'
        '<span class="air-icon">🛩 ✈️</span>'
        '</div>'
        f'{count_html}'
        '</div>'
    )


def ema_html(e):
    if not e:
        return "⚪(0)"

    direction = e.get(
        "direction",
        "none"
    )

    display = e.get(
        "display",
        "⚪(0)"
    )

    cls = {
        "long": "ema-long",
        "short": "ema-short",
        "none": "ema-none"
    }.get(
        direction,
        "ema-none"
    )

    return (
        f'<span class="ema-value {cls}">'
        f'{display}'
        '</span>'
    )


def close_ema10_html(data):
    if not data:
        return "-"

    position = data.get(
        "position",
        "none"
    )

    display = data.get(
        "display",
        "-"
    )

    cls = {
        "above": "close-ema-above",
        "below": "close-ema-below",
        "equal": "close-ema-equal"
    }.get(position)

    if not cls:
        return "-"

    return (
        f'<span class="{cls}">'
        f'{display}'
        '</span>'
    )


# =========================================================
# 행 HTML
# =========================================================

def rows_html(data):
    out = []

    for x in data:

        cls = (
            " qualified"
            if x.get(
                "qualified",
                False
            )
            else ""
        )

        out.append(
            f'''
            <tr class="{cls}">

                <td class="rank">
                    {x.get("rank", "-")}
                </td>

                <td class="coin">
                    <div class="coin-name">
                        {x.get("name", "-")}
                    </div>

                    <div class="change">
                        {x.get("change", "-")}
                    </div>
                </td>

                <td class="vol">
                    {x.get("volume", "-")}
                </td>

                <td class="ema-cell">

                    <div class="ema-row">
                        <span class="tf">1H</span>
                        <span class="ema-value-wrap">
                            {ema_html(
                                x.get(
                                    "ema_1h",
                                    {}
                                )
                            )}
                        </span>
                    </div>

                    <div class="ema-row">
                        <span class="tf">4H</span>
                        <span class="ema-value-wrap">
                            {ema_html(
                                x.get(
                                    "ema_4h",
                                    {}
                                )
                            )}
                        </span>
                    </div>

                </td>

                <td class="close-ema10">
                    {close_ema10_html(
                        x.get(
                            "close_ema10_1h",
                            {}
                        )
                    )}
                </td>

                <td class="warning">
                    {warning_html(
                        x.get(
                            "air_warning",
                            False
                        ),
                        x.get(
                            "air_direction"
                        ),
                        x.get(
                            "air_count",
                            0
                        ),
                        x.get(
                            "air_stopped",
                            False
                        )
                    )}
                </td>

            </tr>
            '''
        )

    return "".join(out)


# =========================================================
# Section
# =========================================================

def section(
    title,
    data,
    update_time
):
    rows = rows_html(data)

    if not rows:
        rows = '''
        <tr>
            <td colspan="6" class="empty">
                현재 조회 데이터 없음
            </td>
        </tr>
        '''

    return f'''
    <h2>
        🏆 {title} TOP{TOP_N}
        <small>{update_time} KST</small>
    </h2>

    <div class="table-wrap">

        <table>

            <thead>
                <tr>
                    <th>#</th>
                    <th>코인</th>
                    <th>거래대금</th>
                    <th>EMA</th>
                    <th>10선</th>
                    <th>경고</th>
                </tr>
            </thead>

            <tbody>
                {rows}
            </tbody>

        </table>

    </div>
    '''


def focus_section(
    data,
    update_time
):
    focus_data = [
        x for x in data
        if x.get(
            "qualified",
            False
        )
    ]

    rows = rows_html(focus_data)

    if not rows:
        rows = '''
        <tr>
            <td colspan="6" class="empty">
                현재 경고 발생 코인 없음
            </td>
        </tr>
        '''

    return f'''
    <h2 class="focus-title">
        🚨 집중 리스트
        <small>{update_time} KST</small>
    </h2>

    <div class="table-wrap focus-table">

        <table>

            <thead>
                <tr>
                    <th>#</th>
                    <th>코인</th>
                    <th>거래대금</th>
                    <th>EMA</th>
                    <th>10선</th>
                    <th>경고</th>
                </tr>
            </thead>

            <tbody>
                {rows}
            </tbody>

        </table>

    </div>
    '''


# =========================================================
# CSS
# =========================================================

CSS = r'''
*{
    box-sizing:border-box
}

html,body{
    margin:0;
    padding:0;
    width:100%;
    overflow-x:hidden
}

body{
    background:#0f1115;
    color:#eee;
    font-family:Arial,sans-serif;
    font-size:9px;
    padding:3px
}

h1{
    margin:2px 2px 4px;
    font-size:13px
}

h2{
    margin:7px 2px 3px;
    font-size:10px
}

h2 small{
    color:#777;
    font-size:6px;
    font-weight:normal;
    margin-left:3px
}

.info{
    margin:0 2px 4px;
    padding:3px 5px;
    color:#8b9099;
    background:#171a1f;
    border:1px solid #252a31;
    border-radius:7px;
    font-size:7px;
    line-height:1.25
}

.status{
    display:flex;
    justify-content:center;
    gap:8px;
    margin-top:2px;
    font-weight:bold
}

.y{color:#35e66d}
.n{color:#ff4d4d}

.table-wrap{
    width:100%;
    overflow:hidden;
    border-radius:8px;
    border:1px solid #252a31
}

table{
    width:100%;
    table-layout:fixed;
    border-collapse:collapse;
    background:#181c21
}

th{
    padding:4px 2px;
    background:#12151a;
    border-bottom:1px solid #2b3037;
    color:#8f949d;
    font-size:6px;
    white-space:nowrap;
    text-align:center!important;
    vertical-align:middle
}

td{
    padding:3px 2px;
    border-bottom:1px solid #272c32;
    text-align:center!important;
    vertical-align:middle
}

th:nth-child(1),
td:nth-child(1){width:7%}

th:nth-child(2),
td:nth-child(2){width:23%}

th:nth-child(3),
td:nth-child(3){width:17%}

th:nth-child(4),
td:nth-child(4){width:18%}

th:nth-child(5),
td:nth-child(5){width:10%}

th:nth-child(6),
td:nth-child(6){width:25%}

.rank{
    color:#8f949d;
    font-size:7px
}

.coin{
    overflow:hidden;
    padding:1px 2px
}

.coin-name{
    font-size:8px;
    font-weight:bold;
    line-height:9px;
    height:9px;
    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis
}

.change{
    margin-top:0;
    line-height:7px;
    height:7px;
    font-size:7px;
    white-space:nowrap
}

.up{
    color:#35e66d;
    font-weight:bold
}

.down{
    color:#ff4d4d;
    font-weight:bold
}

.zero{
    color:#999
}

.vol{
    padding:1px 2px!important;
    font-size:7px;
    font-weight:bold;
    line-height:16px;
    height:16px;
    white-space:nowrap
}

.ema-cell{
    overflow:hidden;
    padding:1px!important
}

.ema-row{
    display:flex;
    align-items:center;
    justify-content:center;
    width:100%;
    height:13px;
    line-height:13px;
    white-space:nowrap;
    overflow:hidden;
    font-size:7px;
    font-weight:bold
}

.tf{
    flex:0 0 18px;
    width:18px;
    color:#8f949d;
    font-size:6px;
    font-weight:bold
}

.ema-value-wrap{
    flex:0 0 42px;
    width:42px;
    min-width:42px;
    max-width:42px;
    display:flex;
    align-items:center;
    justify-content:flex-start;
    overflow:hidden
}

.ema-value{
    display:inline-block;
    width:42px;
    min-width:42px;
    max-width:42px;
    text-align:left;
    white-space:nowrap;
    font-size:7px;
    font-weight:bold;
    line-height:13px
}

.ema-long{
    color:#35e66d
}

.ema-short{
    color:#ff4d4d
}

.ema-none{
    color:#eee
}

.close-ema10{
    text-align:center!important;
    vertical-align:middle;
    white-space:nowrap;
    font-size:7px;
    font-weight:bold
}

.close-ema-above{
    color:#35e66d
}

.close-ema-below{
    color:#ff4d4d
}

.close-ema-equal{
    color:#999
}

.warning{
    text-align:center!important;
    vertical-align:middle!important;
    white-space:nowrap;
    padding:0 2px!important
}

.air-box{
    width:100%;
    display:flex;
    flex-direction:column;
    align-items:center;
    justify-content:center;
    text-align:center
}

.air-main{
    display:flex;
    align-items:center;
    justify-content:center;
    gap:4px;
    white-space:nowrap
}

.air-direction{
    font-size:6px;
    font-weight:bold;
    color:#35e66d
}

.air-icon{
    font-size:11px;
    font-weight:bold;
    display:inline-block;
    transform-origin:center center;
    animation:air-pulse .55s infinite;
    filter:
        drop-shadow(0 0 2px currentColor)
        drop-shadow(0 0 4px currentColor)
}

@keyframes air-pulse{

    0%{
        transform:scale(.90);
        opacity:.30
    }

    25%{
        transform:scale(1.15);
        opacity:.75
    }

    50%{
        transform:scale(1.35);
        opacity:1
    }

    75%{
        transform:scale(1.15);
        opacity:.75
    }

    100%{
        transform:scale(.90);
        opacity:.30
    }
}

.air-count{
    margin-top:0;
    font-size:9px;
    line-height:9px;
    font-weight:bold;
    color:#fff;
    text-align:center
}

.air-stop{
    display:flex;
    align-items:center;
    justify-content:center;
    width:100%;
    height:16px;
    font-size:13px;
    line-height:16px;
    font-weight:bold
}

.qualified{
    background:rgba(255,255,255,.06)
}

.focus-title{
    margin-top:5px;
    margin-bottom:3px
}

.focus-table{
    border:1px solid #343a42
}

.empty{
    color:#555;
    padding:10px 4px;
    text-align:center!important
}

@media(max-width:480px){

    body{
        padding:2px;
        font-size:8px
    }

    h1{
        font-size:12px;
        margin:2px 2px 3px
    }

    h2{
        font-size:9px;
        margin:6px 2px 2px
    }

    .info{
        font-size:6px;
        padding:2px 4px;
        line-height:1.2;
        margin-bottom:3px
    }

    th{
        padding:3px 1px;
        font-size:5px
    }

    td{
        padding:2px 1px
    }

    .coin{
        padding:0 1px
    }

    .coin-name{
        font-size:7px;
        line-height:8px;
        height:8px
    }

    .change{
        font-size:6px;
        line-height:6px;
        height:6px
    }

    .vol{
        padding:0 1px!important;
        font-size:6px;
        line-height:14px;
        height:14px
    }

    .ema-cell{
        padding:0!important
    }

    .ema-row{
        height:12px;
        line-height:12px;
        font-size:6px
    }

    .tf{
        flex:0 0 17px;
        width:17px;
        font-size:5px
    }

    .ema-value-wrap{
        flex:0 0 39px;
        width:39px;
        min-width:39px;
        max-width:39px
    }

    .ema-value{
        width:39px;
        min-width:39px;
        max-width:39px;
        font-size:6px;
        line-height:12px
    }

    .close-ema10{
        font-size:6px
    }

    .air-direction{
        font-size:5px
    }

    .air-icon{
        font-size:10px
    }

    .air-count{
        font-size:8px;
        line-height:8px
    }

    .air-stop{
        font-size:12px;
        height:14px;
        line-height:14px
    }
}
'''


# =========================================================
# Dashboard
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def dashboard():

    status = f'''
    <div class="status">

        <span>
            업비트 :
            <b class="y">{USE_UPBIT}</b>
        </span>

        <span>
            OKX :
            <b class="n">{USE_OKX}</b>
        </span>

    </div>
    '''

    sections = ""

    if USE_UPBIT == "Y":
        sections += focus_section(
            latest_upbit_data,
            latest_upbit_update_time
        )

    if USE_OKX == "Y":
        sections += focus_section(
            latest_okx_data,
            latest_okx_update_time
        )

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

    return f'''
<!DOCTYPE html>
<html lang="ko">

<head>

<meta charset="UTF-8">

<meta
    name="viewport"
    content="width=device-width,initial-scale=1"
>

<meta
    http-equiv="refresh"
    content="60"
>

<title>1H EMA 비행기 경고</title>

<style>
{CSS}
</style>

</head>

<body>

<h1>
    📊 매매 전술 눌림 돌파
</h1>

<div class="info">

    ① 거래대금 TOP{TOP_N}<br>

    ② 1H + 4H EMA 10-30-60-120 정배열<br>

    ③ EMA 정배열/역배열 연속 캔들 수 표시<br>

    ④ 10선 아래 종가 → 🔻(연속 횟수)<br>

    ⑤ 10선 위 종가 → 🟢▲<br>

    ⑥ 다시 10선 아래 종가 → 🔻(1)부터 재시작<br>

    ⑦ 이전 1H 종가 &lt; EMA10
    → 현재 양봉 + 종가 &gt; EMA10<br>

    ⑧ 비행기 최초 발생 → 🛩️1<br>

    ⑨ 이후 새 1H 양봉 + EMA10 위 종가
    → 🛩️2 → 🛩️3 → 🛩️4...<br>

    ⑩ 1H 종가 &lt; EMA10
    → ⛔️ 비행기 종료

    {status}

</div>

{sections}

</body>
</html>
'''


# =========================================================
# 스케줄러
# =========================================================

def scheduler():
    log.info("스케줄러 시작")

    while True:

        try:
            schedule.run_pending()

        except Exception as e:
            log.exception(
                f"스케줄러 오류: {e}"
            )

        time.sleep(1)


# =========================================================
# Startup
# =========================================================

@app.on_event("startup")
def startup():

    if USE_UPBIT not in ("Y", "N"):
        raise ValueError(
            "USE_UPBIT은 Y 또는 N만 가능합니다."
        )

    if USE_OKX not in ("Y", "N"):
        raise ValueError(
            "USE_OKX는 Y 또는 N만 가능합니다."
        )

    log.info(
        "========================================"
    )

    log.info(
        "1H EMA 비행기 경고 시스템 시작"
    )

    log.info(
        f"업비트={USE_UPBIT} / OKX={USE_OKX}"
    )

    log.info(
        f"TOP={TOP_N} / UPDATE={UPDATE_MINUTES}분"
    )

    log.info(
        "EMA = 10-30-60-120"
    )

    log.info(
        "EMA 정배열/역배열 연속 카운팅 적용"
    )

    log.info(
        "EMA 표시 = 🟢(N) / 🔴(N) / ⚪(0)"
    )

    log.info(
        "10선 아래 종가 = 🔻(연속 카운트)"
    )

    log.info(
        "10선 위 종가 = 🟢▲"
    )

    log.info(
        "다시 하락 종가 = 🔻(1)부터 재시작"
    )

    log.info(
        "비행기 최초 발생 = 🛩️1"
    )

    log.info(
        "이후 새 1H 양봉 + EMA10 위 = 계속 카운트"
    )

    log.info(
        "같은 1H 캔들 중복 카운트 방지"
    )

    log.info(
        "1H 종가 < EMA10 = ⛔️ 비행기 종료"
    )

    log.info(
        "15M EMA = 완전 삭제"
    )

    log.info(
        "N자 / 로켓 = 완전 삭제"
    )

    log.info(
        "========================================"
    )

    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    schedule.every(
        UPDATE_MINUTES
    ).minutes.do(
        update_dashboard
    )

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
    p
