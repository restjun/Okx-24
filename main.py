from fastapi import FastAPI
from fastapi.responses import HTMLResponse

import requests
import threading
import time
import logging
import schedule
import uvicorn
import pandas as pd
import warnings
import html

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


# =========================================================
# 기본 설정
# =========================================================

warnings.filterwarnings(
    "ignore",
    category=FutureWarning
)

app = FastAPI()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s"
)

log = logging.getLogger("trading")

KST = ZoneInfo("Asia/Seoul")


# =========================================================
# 설정
# =========================================================

TOP_N = 10
UPDATE_MINUTES = 1

HISTORY_CHUNK = 200
MAX_HISTORY_CHUNKS = 10

USE_UPBIT = "Y"
USE_OKX = "N"

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10


# =========================================================
# SIGNAL 패턴
# =========================================================

SIGNAL_CANDLE_PATTERNS = [
    "상승장악",
    "3캔들 상승장악",
    "4캔들 상승장악",
    "상승장악 후 양봉",
    "장대양봉 후 양봉",
    "관통형 후 양봉"
]


# =========================================================
# 전역 데이터
# =========================================================

latest_upbit_data = []

latest_upbit_15m_data = []
latest_upbit_daily_data = []

latest_upbit_update_time = "-"
latest_upbit_15m_update_time = "-"
latest_upbit_daily_update_time = "-"

latest_upbit_markets = []

latest_okx_data = []
latest_okx_update_time = "-"

latest_btc_okx_price = None

latest_btc_15m_periods = []
latest_btc_daily_periods = []

latest_btc_15m_change = None
latest_btc_daily_change = None

request_lock = threading.Lock()
update_lock = threading.Lock()

last_request_time = 0


# =========================================================
# 시간
# =========================================================

def kst():
    return datetime.now(KST).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def timeframe_delta(tf):

    if tf == "15m":
        return timedelta(minutes=15)

    return timedelta(days=1)


def current_tf_start(tf):

    now = datetime.now(KST)

    # -----------------------------------------------------
    # 일봉
    # KST 09:00 기준
    # -----------------------------------------------------

    if tf == "1d":

        x = now.replace(
            hour=9,
            minute=0,
            second=0,
            microsecond=0
        )

        if now < x:
            return x - timedelta(days=1)

        return x

    # -----------------------------------------------------
    # 15분봉
    # -----------------------------------------------------

    return now.replace(
        minute=(now.minute // 15) * 15,
        second=0,
        microsecond=0
    )


def recent_periods(tf, count=6):

    cur = current_tf_start(tf)
    d = timeframe_delta(tf)

    out = []

    for i in range(count - 1, -1, -1):

        s = cur - d * i

        out.append({
            "start": s,
            "end": s + d,
            "active": i == 0,
            "label": (
                s.strftime("%m/%d 09:00")
                if tf == "1d"
                else s.strftime("%m/%d %H:%M")
            )
        })

    return out


# =========================================================
# API 요청 제어
# =========================================================

def wait_request():

    global last_request_time

    with request_lock:

        gap = time.monotonic() - last_request_time

        if gap < REQUEST_INTERVAL:

            time.sleep(
                REQUEST_INTERVAL - gap
            )

        last_request_time = time.monotonic()


def retry(func, *args, **kwargs):

    for n in range(MAX_RETRIES):

        try:

            wait_request()

            r = func(
                *args,
                **kwargs
            )

            if not hasattr(
                r,
                "status_code"
            ):
                return r

            if r.status_code == 200:
                return r

            if r.status_code == 429:

                time.sleep(
                    min(
                        RATE_LIMIT_WAIT * (n + 1),
                        60
                    )
                )

            elif r.status_code >= 500:

                time.sleep(
                    min(
                        2 * (n + 1),
                        30
                    )
                )

            else:

                return r

        except Exception as e:

            log.warning(
                "API 오류 %s/%s: %s",
                n + 1,
                MAX_RETRIES,
                e
            )

            if n < MAX_RETRIES - 1:

                time.sleep(
                    min(
                        2 * (n + 1),
                        20
                    )
                )

    return None


# =========================================================
# 업비트 마켓
# =========================================================

def get_upbit_markets():

    global latest_upbit_markets

    r = retry(
        requests.get,
        "https://api.upbit.com/v1/market/all",
        params={
            "isDetails": "false"
        },
        timeout=15
    )

    if r is None:
        return []

    try:

        markets = [
            x["market"]
            for x in r.json()
            if x.get(
                "market",
                ""
            ).startswith("KRW-")
        ]

    except Exception:

        return []

    result = []

    for i in range(
        0,
        len(markets),
        100
    ):

        rr = retry(
            requests.get,
            "https://api.upbit.com/v1/ticker",
            params={
                "markets": ",".join(
                    markets[i:i + 100]
                )
            },
            timeout=15
        )

        if rr is None:
            continue

        try:
            data = rr.json()
        except Exception:
            continue

        if not isinstance(
            data,
            list
        ):
            continue

        for x in data:

            try:

                result.append({
                    "market": x["market"],
                    "current_price": float(
                        x["trade_price"]
                    ),
                    "volume_24h": float(
                        x["acc_trade_price_24h"]
                    )
                })

            except Exception:
                pass

    latest_upbit_markets = [
        x["market"]
        for x in result
    ]

    return result


# =========================================================
# 업비트 캔들
# =========================================================

def get_upbit_candles(
    market,
    tf,
    count=200
):

    if tf == "1d":

        endpoint = (
            "https://api.upbit.com/v1/candles/days"
        )

        params = {
            "market": market,
            "count": min(
                count,
                200
            )
        }

    else:

        endpoint = (
            "https://api.upbit.com/v1/candles/minutes/15"
        )

        params = {
            "market": market,
            "count": min(
                count,
                200
            )
        }

    r = retry(
        requests.get,
        endpoint,
        params=params,
        timeout=15
    )

    if r is None:
        return pd.DataFrame()

    try:
        data = r.json()
    except Exception:
        return pd.DataFrame()

    if not isinstance(
        data,
        list
    ):
        return pd.DataFrame()

    rows = []

    for x in data:

        try:

            rows.append({
                "datetime":
                    datetime.strptime(
                        x[
                            "candle_date_time_kst"
                        ],
                        "%Y-%m-%dT%H:%M:%S"
                    ).replace(
                        tzinfo=KST
                    ),

                "open":
                    float(
                        x["opening_price"]
                    ),

                "high":
                    float(
                        x["high_price"]
                    ),

                "low":
                    float(
                        x["low_price"]
                    ),

                "close":
                    float(
                        x["trade_price"]
                    )
            })

        except Exception:
            pass

    if not rows:
        return pd.DataFrame()

    return (
        pd.DataFrame(rows)
        .sort_values("datetime")
        .drop_duplicates("datetime")
        .reset_index(drop=True)
    )


# =========================================================
# 캔들 기본 정보
# =========================================================

def candle_parts(c):

    try:

        o, h, l, cl = map(
            float,
            (
                c["open"],
                c["high"],
                c["low"],
                c["close"]
            )
        )

    except Exception:

        return None

    total = h - l

    if total <= 0:
        return None

    body = abs(
        cl - o
    )

    return {
        "open": o,
        "high": h,
        "low": l,
        "close": cl,
        "body": body,
        "total": total,
        "upper": h - max(o, cl),
        "lower": min(o, cl) - l,
        "body_ratio": body / total,
        "bull": cl > o,
        "bear": cl < o
    }


# =========================================================
# 상승장악
# =========================================================

def bullish_engulfing(a, b):

    p1 = candle_parts(a)
    p2 = candle_parts(b)

    return bool(
        p1
        and p2
        and p2["bull"]
        and p2["open"] <= min(
            p1["open"],
            p1["close"]
        )
        and p2["close"] >= max(
            p1["open"],
            p1["close"]
        )
        and p2["body"] > p1["body"]
    )


# =========================================================
# 장대양봉
# =========================================================

def long_bullish(c):

    p = candle_parts(c)

    return bool(
        p
        and p["bull"]
        and p["body_ratio"] >= 0.45
    )


# =========================================================
# 2캔들 패턴
# =========================================================

def two_patterns(a, b):

    p1 = candle_parts(a)
    p2 = candle_parts(b)

    if not p1 or not p2:
        return []

    out = []

    if bullish_engulfing(a, b):
        out.append(
            "상승장악"
        )

    if (
        long_bullish(a)
        and p2["bull"]
    ):
        out.append(
            "장대양봉 후 양봉"
        )

    mid = (
        p1["open"] +
        p1["close"]
    ) / 2

    if (
        p1["bear"]
        and p2["bull"]
        and p2["close"] > mid
        and p2["close"] < p1["open"]
    ):
        out.append(
            "관통형"
        )

    if (
        p1["bull"]
        and p2["bear"]
        and p2["open"] >= p1["close"]
        and p2["close"] <= p1["open"]
        and p2["body"] > p1["body"]
    ):
        out.append(
            "하락장악"
        )

    if (
        p1["bull"]
        and p2["bear"]
        and p2["close"] < mid
        and p2["close"] > p1["open"]
    ):
        out.append(
            "먹구름형"
        )

    return out


# =========================================================
# 3캔들 패턴
# =========================================================

def three_patterns(a, b, c):

    p1, p2, p3 = map(
        candle_parts,
        (a, b, c)
    )

    if not p1 or not p2 or not p3:
        return []

    out = []

    if (
        p1["bear"]
        and p1["body_ratio"] >= 0.45
        and p2["body_ratio"] <= 0.35
        and p3["bull"]
        and p3["body_ratio"] >= 0.45
        and p3["close"]
        > (
            p1["open"] +
            p1["close"]
        ) / 2
    ):
        out.append(
            "모닝스타"
        )

    if (
        p1["bull"]
        and p2["bull"]
        and p3["bull"]
        and p2["close"] > p1["close"]
        and p3["close"] > p2["close"]
    ):
        out.append(
            "3연속양봉"
        )

    if bullish_engulfing(a, c):
        out.append(
            "3캔들 상승장악"
        )

    if (
        bullish_engulfing(a, b)
        and p3["bull"]
    ):
        out.append(
            "상승장악 후 양봉"
        )

    if (
        "관통형" in two_patterns(a, b)
        and p3["bull"]
    ):
        out.append(
            "관통형 후 양봉"
        )

    return out


# =========================================================
# 4캔들 패턴
# =========================================================

def four_patterns(
    a,
    b,
    c,
    d
):

    p1, p2, p3, p4 = map(
        candle_parts,
        (a, b, c, d)
    )

    if not all(
        (p1, p2, p3, p4)
    ):
        return []

    if bullish_engulfing(a, d):

        return [
            "4캔들 상승장악"
        ]

    return []


# =========================================================
# 기간 데이터 생성
# =========================================================

def build_periods(
    df,
    tf,
    current_price=None
):

    if df is None or df.empty:
        return []

    periods = recent_periods(
        tf,
        6
    )

    out = []

    for p in periods:

        part = df[
            (df.datetime >= p["start"])
            &
            (df.datetime < p["end"])
        ]

        if part.empty:

            out.append({
                **p,
                "open": None,
                "high": None,
                "low": None,
                "close": None,
                "change": None,
                "patterns": []
            })

            continue

        o = float(
            part.iloc[0].open
        )

        h = float(
            part.high.max()
        )

        l = float(
            part.low.min()
        )

        c = float(
            part.iloc[-1].close
        )

        # 현재 진행 중인 봉
        if (
            p["active"]
            and current_price is not None
        ):

            c = float(
                current_price
            )

            h = max(
                h,
                c
            )

            l = min(
                l,
                c
            )

        out.append({
            **p,
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "change": (
                (c - o) / o * 100
                if o
                else None
            ),
            "patterns": []
        })

    # -----------------------------------------------------
    # 패턴 계산
    # -----------------------------------------------------

    for i, p in enumerate(out):

        pats = []

        if (
            i >= 1
            and p["open"] is not None
            and out[i - 1]["open"] is not None
        ):

            pats += two_patterns(
                out[i - 1],
                p
            )

        if (
            i >= 2
            and all(
                out[j]["open"] is not None
                for j in (
                    i - 2,
                    i - 1,
                    i
                )
            )
        ):

            pats += three_patterns(
                out[i - 2],
                out[i - 1],
                p
            )

        if (
            i >= 3
            and all(
                out[j]["open"] is not None
                for j in (
                    i - 3,
                    i - 2,
                    i - 1,
                    i
                )
            )
        ):

            pats += four_patterns(
                out[i - 3],
                out[i - 2],
                out[i - 1],
                p
            )

        p["patterns"] = list(
            dict.fromkeys(pats)
        )

    return out


# =========================================================
# SIGNAL 계산
#
# 15분:
#   이전 확정 15분봉
#
# 일봉:
#   현재 진행 중인 일봉
# =========================================================

def calculate_signal(
    periods,
    timeframe
):

    if not periods:

        return {
            "signal_pass": False,
            "change": None,
            "patterns": []
        }

    # -----------------------------------------------------
    # 15분 = 이전 확정봉
    # -----------------------------------------------------

    if timeframe == "15m":

        if len(periods) < 2:

            return {
                "signal_pass": False,
                "change": None,
                "patterns": []
            }

        target = periods[-2]

    # -----------------------------------------------------
    # 일봉 = 현재 진행봉
    # -----------------------------------------------------

    else:

        target = periods[-1]

    change = target.get(
        "change"
    )

    if change is None:

        return {
            "signal_pass": False,
            "change": None,
            "patterns": []
        }

    bullish = (
        target.get("close") is not None
        and target.get("open") is not None
        and target["close"] > target["open"]
    )

    patterns = [
        x
        for x in target.get(
            "patterns",
            []
        )
        if x in SIGNAL_CANDLE_PATTERNS
    ]

    signal_pass = bool(
        change > 0
        and bullish
        and patterns
    )

    return {
        "signal_pass": signal_pass,
        "change": change,
        "patterns": patterns
    }


# =========================================================
# 코인 전체 분석
# =========================================================

def analyze_coin(
    market,
    price
):

    # -----------------------------------------------------
    # 15분
    # -----------------------------------------------------

    df15 = get_upbit_candles(
        market,
        "15m",
        200
    )

    periods_15m = build_periods(
        df15,
        "15m",
        price
    )

    signal_15m = calculate_signal(
        periods_15m,
        "15m"
    )

    # -----------------------------------------------------
    # 일봉
    # -----------------------------------------------------

    df_daily = get_upbit_candles(
        market,
        "1d",
        200
    )

    periods_daily = build_periods(
        df_daily,
        "1d",
        price
    )

    signal_daily = calculate_signal(
        periods_daily,
        "1d"
    )

    # -----------------------------------------------------
    # 동시 SIGNAL
    #
    # 15분 이전 확정봉 SIGNAL
    # +
    # 현재 일봉 SIGNAL
    # -----------------------------------------------------

    simultaneous_signal = bool(
        signal_15m["signal_pass"]
        and signal_daily["signal_pass"]
    )

    return {
        "periods_15m":
            periods_15m,

        "signal_15m":
            signal_15m,

        "periods_daily":
            periods_daily,

        "signal_daily":
            signal_daily,

        "simultaneous_signal":
            simultaneous_signal
    }


# =========================================================
# 행 생성
# =========================================================

def make_row(
    rank,
    market,
    item,
    analysis
):

    coin = market.replace(
        "KRW-",
        ""
    )

    signal_15m = analysis[
        "signal_15m"
    ]

    signal_daily = analysis[
        "signal_daily"
    ]

    return {

        "rank": rank,

        "name": coin,

        "market": market,

        "volume_24h":
            item["volume_24h"],

        "current_price":
            item["current_price"],

        # -----------------------------------------------
        # 15분
        # -----------------------------------------------

        "periods_15m":
            analysis["periods_15m"],

        "signal_15m":
            signal_15m["signal_pass"],

        "signal_15m_change":
            signal_15m["change"],

        "signal_15m_patterns":
            signal_15m["patterns"],

        # -----------------------------------------------
        # 일봉
        # -----------------------------------------------

        "periods_daily":
            analysis["periods_daily"],

        "signal_daily":
            signal_daily["signal_pass"],

        "signal_daily_change":
            signal_daily["change"],

        "signal_daily_patterns":
            signal_daily["patterns"],

        # -----------------------------------------------
        # 동시
        # -----------------------------------------------

        "simultaneous_signal":
            analysis["simultaneous_signal"]
    }


# =========================================================
# 업비트 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_15m_data
    global latest_upbit_daily_data

    global latest_upbit_update_time
    global latest_upbit_15m_update_time
    global latest_upbit_daily_update_time

    markets = sorted(
        get_upbit_markets(),
        key=lambda x: x["volume_24h"],
        reverse=True
    )[:TOP_N]

    rows = []

    for rank, item in enumerate(
        markets,
        1
    ):

        market = item["market"]

        price = item[
            "current_price"
        ]

        try:

            analysis = analyze_coin(
                market,
                price
            )

            row = make_row(
                rank,
                market,
                item,
                analysis
            )

            rows.append(row)

        except Exception as e:

            log.warning(
                "%s 분석 오류: %s",
                market,
                e
            )

    latest_upbit_data = rows

    latest_upbit_15m_data = [
        x
        for x in rows
        if x.get("signal_15m")
    ]

    latest_upbit_daily_data = [
        x
        for x in rows
        if x.get("signal_daily")
    ]

    latest_upbit_15m_update_time = kst()

    latest_upbit_daily_update_time = kst()

    latest_upbit_update_time = (
        latest_upbit_15m_update_time
    )

    log.info(
        "UPBIT | 15분=%s | 일봉=%s | 동시=%s",
        sum(
            x.get(
                "signal_15m",
                False
            )
            for x in rows
        ),
        sum(
            x.get(
                "signal_daily",
                False
            )
            for x in rows
        ),
        sum(
            x.get(
                "simultaneous_signal",
                False
            )
            for x in rows
        )
    )


# =========================================================
# OKX 캔들
# =========================================================

def okx_candles(
    bar,
    limit=200
):

    r = retry(
        requests.get,
        "https://www.okx.com/api/v5/market/candles",
        params={
            "instId":
                "BTC-USDT-SWAP",

            "bar":
                bar,

            "limit":
                str(
                    min(
                        limit,
                        300
                    )
                )
        },
        timeout=15
    )

    if r is None:
        return pd.DataFrame()

    try:

        data = r.json().get(
            "data",
            []
        )

    except Exception:

        return pd.DataFrame()

    rows = []

    for x in data:

        try:

            rows.append({

                "datetime":
                    pd.to_datetime(
                        int(x[0]),
                        unit="ms",
                        utc=True
                    ).tz_convert(KST),

                "open":
                    float(x[1]),

                "high":
                    float(x[2]),

                "low":
                    float(x[3]),

                "close":
                    float(x[4])
            })

        except Exception:
            pass

    if not rows:
        return pd.DataFrame()

    return (
        pd.DataFrame(rows)
        .sort_values("datetime")
        .drop_duplicates("datetime")
        .reset_index(drop=True)
    )


# =========================================================
# OKX 현재가
# =========================================================

def okx_price():

    r = retry(
        requests.get,
        "https://www.okx.com/api/v5/market/ticker",
        params={
            "instId":
                "BTC-USDT-SWAP"
        },
        timeout=15
    )

    try:

        return float(
            r.json()[
                "data"
            ][0]["last"]
        ) if r else None

    except Exception:

        return None


# =========================================================
# BTC 업데이트
# =========================================================

def update_okx_btc():

    global latest_btc_okx_price
    global latest_btc_15m_periods
    global latest_btc_daily_periods
    global latest_btc_15m_change
    global latest_btc_daily_change

    price = okx_price()

    latest_btc_okx_price = price

    if price is None:
        return

    # -----------------------------------------------------
    # 15분
    # -----------------------------------------------------

    d15 = okx_candles(
        "15m",
        200
    )

    latest_btc_15m_periods = build_periods(
        d15,
        "15m",
        price
    )

    # -----------------------------------------------------
    # 1시간 → KST 09:00 일봉
    # -----------------------------------------------------

    d1h = okx_candles(
        "1H",
        200
    )

    if not d1h.empty:

        d1h = d1h.copy()

        d1h["datetime"] = (
            d1h["datetime"]
            .dt
            .tz_convert(KST)
        )

        shifted = d1h.copy()

        shifted["day_start"] = (
            shifted["datetime"]
            - pd.Timedelta(hours=9)
        ).dt.floor("D") + pd.Timedelta(hours=9)

        daily = (
            shifted
            .groupby("day_start")
            .agg(
                open=("open", "first"),
                high=("high", "max"),
                low=("low", "min"),
                close=("close", "last")
            )
            .reset_index()
            .rename(
                columns={
                    "day_start":
                        "datetime"
                }
            )
        )

    else:

        daily = pd.DataFrame()

    latest_btc_daily_periods = build_periods(
        daily,
        "1d",
        price
    )

    # -----------------------------------------------------
    # 15분 = 이전 확정봉
    # -----------------------------------------------------

    if len(
        latest_btc_15m_periods
    ) >= 2:

        latest_btc_15m_change = (
            latest_btc_15m_periods[-2]
            .get("change")
        )

    else:

        latest_btc_15m_change = None

    # -----------------------------------------------------
    # 일봉 = 현재 진행봉
    # -----------------------------------------------------

    if latest_btc_daily_periods:

        latest_btc_daily_change = (
            latest_btc_daily_periods[-1]
            .get("change")
        )

    else:

        latest_btc_daily_change = None

    log.info(
        "BTC | 15분=%s | 일봉=%s",
        latest_btc_15m_change,
        latest_btc_daily_change
    )


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    if not update_lock.acquire(False):
        return

    try:

        update_okx_btc()

        if USE_UPBIT == "Y":

            update_upbit()

    except Exception as e:

        log.exception(
            "업데이트 오류: %s",
            e
        )

    finally:

        update_lock.release()


# =========================================================
# 표시
# =========================================================

def fmt_price(v):

    if v is None:
        return "-"

    try:
        v = float(v)
    except Exception:
        return "-"

    if v >= 100000000:

        return (
            f"{v / 100000000:.2f}억"
        )

    if v >= 10000:

        return f"{v:,.0f}"

    if v >= 1:

        return f"{v:,.2f}"

    return f"{v:.6f}"


def fmt_vol(v):

    try:
        v = float(v)
    except Exception:
        return "-"

    if v >= 1e12:

        return (
            f"{v / 1e12:.1f}조"
        )

    if v >= 1e8:

        return (
            f"{v / 1e8:.0f}억"
        )

    if v >= 1e4:

        return (
            f"{v / 1e4:.0f}만"
        )

    return f"{v:,.0f}"


def fmt_change(v):

    if v is None:

        return (
            '<span class="zero">-</span>'
        )

    try:

        v = float(v)

    except Exception:

        return (
            '<span class="zero">-</span>'
        )

    # =====================================================
    # 양봉 / 상승 = 녹색
    # =====================================================

    if v > 0:

        return (
            f'<span class="up">'
            f'▲ +{v:.2f}%'
            f'</span>'
        )

    # =====================================================
    # 음봉 / 하락 = 빨간색
    # =====================================================

    if v < 0:

        return (
            f'<span class="down">'
            f'▼ {v:.2f}%'
            f'</span>'
        )

    return (
        '<span class="zero">'
        '0.00%'
        '</span>'
    )


# =========================================================
# 기간 표시
# =========================================================

def cells(periods):

    result = []

    for p in periods:

        pattern_text = ""

        if p.get("patterns"):

            pattern_text = (
                "<small>"
                + " · ".join(
                    map(
                        html.escape,
                        p.get(
                            "patterns",
                            []
                        )
                    )
                )
                + "</small>"
            )

        result.append(
            f"""
            <div class="tf-cell {
                'current'
                if p.get('active')
                else ''
            }">

                <div>
                    {html.escape(
                        p.get(
                            "label",
                            "-"
                        )
                    )}
                </div>

                <strong>
                    {fmt_change(
                        p.get("change")
                    )}
                </strong>

                {pattern_text}

            </div>
            """
        )

    return "".join(result)


# =========================================================
# BTC
# =========================================================

def btc_html():

    btc_15m_signal = False
    btc_daily_signal = False

    # -----------------------------------------------------
    # 15분 SIGNAL
    # 이전 확정봉
    # -----------------------------------------------------

    if len(
        latest_btc_15m_periods
    ) >= 2:

        target = (
            latest_btc_15m_periods[-2]
        )

        patterns = [
            x
            for x in target.get(
                "patterns",
                []
            )
            if x in SIGNAL_CANDLE_PATTERNS
        ]

        btc_15m_signal = bool(
            target.get("change") is not None
            and target.get("change") > 0
            and target.get("open") is not None
            and target.get("close") is not None
            and target["close"] > target["open"]
            and patterns
        )

    # -----------------------------------------------------
    # 일봉 SIGNAL
    # 현재 진행봉
    # -----------------------------------------------------

    if latest_btc_daily_periods:

        target = (
            latest_btc_daily_periods[-1]
        )

        patterns = [
            x
            for x in target.get(
                "patterns",
                []
            )
            if x in SIGNAL_CANDLE_PATTERNS
        ]

        btc_daily_signal = bool(
            target.get("change") is not None
            and target.get("change") > 0
            and target.get("open") is not None
            and target.get("close") is not None
            and target["close"] > target["open"]
            and patterns
        )

    # -----------------------------------------------------
    # BTC 상태
    # 화면에는 동시 SIGNAL만 별도 표시
    # -----------------------------------------------------

    if (
        btc_15m_signal
        and btc_daily_signal
    ):

        status = (
            '<span class="btc-status both">'
            '⭐ 동시 SIGNAL'
            '</span>'
        )

    else:

        status = (
            '<span class="btc-status none">'
            '—'
            '</span>'
        )

    return f"""
    <div class="market-card">

        <div class="market-head">

            <b>
                ₿ BTC 시장 시황 · OKX
            </b>

            <span>
                BTC-USDT-SWAP · {kst()}
            </span>

        </div>

        <div class="btc-row">

            <b>BTC</b>

            <strong>
                {fmt_price(
                    latest_btc_okx_price
                )}
            </strong>

            <strong>
                {fmt_change(
                    latest_btc_15m_change
                )}
            </strong>

            <strong>
                {fmt_change(
                    latest_btc_daily_change
                )}
            </strong>

            {status}

        </div>

        <div class="btc-label">
            15분 · 이전 확정봉
        </div>

        <div class="grid">
            {cells(
                latest_btc_15m_periods
            )}
        </div>

        <div class="btc-label">
            일봉 · KST 09:00 · 현재봉
        </div>

        <div class="grid">
            {cells(
                latest_btc_daily_periods
            )}
        </div>

    </div>
    """


# =========================================================
# 업비트 카드
# =========================================================

def card(
    row,
    kind
):

    both = row.get(
        "simultaneous_signal",
        False
    )

    badge = ""

    if both:

        badge = (
            '<span class="both">'
            '⭐ 동시 SIGNAL'
            '</span>'
        )

    if kind == "both":

        title = (
            "⭐ 15분 + 일봉 동시 SIGNAL"
        )

        periods = row.get(
            "periods_15m",
            []
        )

        label = (
            "15분 이전 확정봉 + "
            "일봉 현재봉"
        )

    else:

        title = "🏆 업비트 TOP"

        periods = row.get(
            "periods_15m",
            []
        )

        label = (
            "15분 / 일봉"
        )

    return f"""
    <div class="card {
        'both-card'
        if both
        else ''
    }">

        <div class="card-head">

            <span>
                #{row["rank"]}
            </span>

            <b>
                {html.escape(
                    row["name"]
                )}
            </b>

            {badge}

            <em>
                {title}
            </em>

        </div>

        <div class="main">

            <div>
                현재가

                <strong>
                    {fmt_price(
                        row["current_price"]
                    )}
                </strong>
            </div>

            <div>
                24H 거래대금

                <strong>
                    {fmt_vol(
                        row["volume_24h"]
                    )}
                </strong>
            </div>

            <div>
                15분

                <strong>
                    {fmt_change(
                        row.get(
                            "signal_15m_change"
                        )
                    )}
                </strong>
            </div>

            <div>
                일봉

                <strong>
                    {fmt_change(
                        row.get(
                            "signal_daily_change"
                        )
                    )}
                </strong>
            </div>

        </div>

        <div class="label">
            {label}
        </div>

        <div class="grid">
            {cells(periods)}
        </div>

    </div>
    """


# =========================================================
# 동시 SIGNAL
# =========================================================

def both_section(data):

    rows = [
        r
        for r in data
        if r.get(
            "simultaneous_signal"
        )
    ]

    if rows:

        body = "".join(
            card(
                r,
                "both"
            )
            for r in rows
        )

    else:

        body = (
            '<div class="empty">'
            '현재 동시 SIGNAL 없음'
            '</div>'
        )

    return f"""
    <section>

        <header class="gold">

            <b>
                ⭐ 15분 + 일봉 동시 SIGNAL
            </b>

            <small>
                15분 이전 확정봉 SIGNAL
                +
                현재 일봉 SIGNAL
            </small>

        </header>

        {body}

    </section>
    """


# =========================================================
# CSS
# =========================================================

CSS = """
* {
    box-sizing: border-box;
}

body {
    margin: 0;
    background: #080c11;
    color: #e7ebef;
    font-family: Arial, sans-serif;
    font-size: 11px;
    padding: 8px;
}

h1 {
    font-size: 15px;
    margin: 3px 2px 9px;
}

section {
    margin: 8px 0 12px;
}

section > header {
    padding: 8px 10px;
    border: 2px solid #26313b;
    border-radius: 10px;
    background: #10151b;
    margin-bottom: 6px;
}

section > header b {
    display: block;
    font-size: 11px;
}

section > header small {
    display: block;
    color: #7d8892;
    font-size: 7px;
    margin-top: 2px;
}

section > header.gold {
    border-color: #c9a83d;
    background: #1d190d;
}

.market-card {
    border: 2px solid #26313b;
    border-radius: 11px;
    overflow: hidden;
    margin-bottom: 10px;
}

.market-head {
    padding: 8px;
    background: #111820;
    display: flex;
    justify-content: space-between;
    gap: 8px;
}

.market-head span {
    font-size: 7px;
    color: #74808b;
}

.btc-row {
    display: grid;
    grid-template-columns:
        0.7fr
        1.2fr
        1fr
        1fr
        1.2fr;

    min-height: 48px;
    align-items: center;
    text-align: center;
}

.btc-row > * {
    padding: 5px;
}

.btc-row > * + * {
    border-left: 1px solid #29323c;
}

.btc-status {
    font-size: 8px;
    font-weight: 900;
}

.btc-status.both {
    color: #f0cf67;
}

.btc-status.none {
    color: #66727d;
}

.btc-label,
.label {
    padding: 5px 8px;
    background: #111820;
    color: #aab3ba;
    font-weight: 900;
    font-size: 8px;
}

.grid {
    display: grid;
    grid-template-columns: repeat(6, 1fr);
    gap: 1px;
    background: #29323c;
}

.tf-cell {
    min-height: 47px;
    background: #0d1319;
    padding: 4px;
    text-align: center;
    font-size: 7px;
}

.tf-cell.current {
    background: #173326;
}

.tf-cell strong {
    display: block;
    font-size: 8px;
    margin-top: 2px;
}

.tf-cell small {
    display: block;
    color: #e0bd6d;
    font-size: 5px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}

.card {
    border: 2px solid #26313b;
    border-radius: 10px;
    overflow: hidden;
    margin: 6px 0;
    background: #0f141a;
}

.card.both-card {
    border-color: #d2ae42;
}

.card-head {
    min-height: 38px;
    padding: 6px 8px;
    background: #121820;
    display: flex;
    align-items: center;
    gap: 7px;
}

.card-head > span {
    color: #d8b85c;
    font-weight: 900;
}

.card-head b {
    flex: 1;
    font-size: 10px;
}

.card-head em {
    font-style: normal;
    color: #7e8994;
    font-size: 7px;
}

.both {
    padding: 3px 6px;
    border: 1px solid #d2ae42;
    border-radius: 5px;
    background: #2b230d;
    color: #f0cf67;
    font-size: 6px;
    font-weight: 900;
}

.main {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    min-height: 43px;
}

.main > div {
    text-align: center;
    padding: 5px;
    border-top: 1px solid #29323c;
}

.main > div + div {
    border-left: 1px solid #29323c;
}

.main strong {
    display: block;
    margin-top: 2px;
}

/* =====================================================
   양봉 / 상승 = 녹색
   음봉 / 하락 = 빨간색
   ===================================================== */

.up {
    color: #36d66b;
}

.down {
    color: #ff5c68;
}

.zero {
    color: #66727d;
}

.empty {
    text-align: center;
    padding: 25px;
    border: 1px solid #26313b;
    border-radius: 10px;
    color: #65717c;
}


@media (max-width: 600px) {

    body {
        padding: 5px;
    }

    h1 {
        font-size: 12px;
    }

    .grid {
        grid-template-columns: repeat(3, 1fr);
    }

    .market-head {
        font-size: 9px;
    }

    .market-head span {
        font-size: 5px;
    }

    .btc-row {
        min-height: 40px;
        font-size: 8px;
    }

    .btc-label,
    .label {
        font-size: 6px;
    }

    .tf-cell {
        min-height: 43px;
        font-size: 6px;
    }

    .tf-cell strong {
        font-size: 7px;
    }

    .tf-cell small {
        font-size: 4px;
    }

    .card-head {
        min-height: 33px;
        padding: 5px 6px;
    }

    .card-head b {
        font-size: 8px;
    }

    .card-head em {
        font-size: 5px;
    }

    .both {
        font-size: 4.5px;
    }

    .main {
        min-height: 37px;
        font-size: 6px;
    }

    .main strong {
        font-size: 7px;
    }

    section > header b {
        font-size: 9px;
    }

    section > header small {
        font-size: 5px;
    }
}
"""


# =========================================================
# 대시보드
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def dashboard():

    s = btc_html()

    if USE_UPBIT == "Y":

        # =================================================
        # ⭐ 동시 SIGNAL만 표시
        # =================================================

        s += both_section(
            latest_upbit_data
        )

        # =================================================
        # 🏆 업비트 TOP10
        # =================================================

        top_cards = "".join(
            card(
                r,
                "top"
            )
            for r in latest_upbit_data
        )

        if not top_cards:

            top_cards = (
                '<div class="empty">'
                '현재 데이터 없음'
                '</div>'
            )

        s += f"""
        <section>

            <header>

                <b>
                    🏆 업비트 TOP{TOP_N}
                </b>

                <small>
                    거래대금 순위 · 15분 + 일봉 데이터
                </small>

            </header>

            {top_cards}

        </section>
        """

    return f"""
    <!doctype html>

    <html lang="ko">

    <head>

        <meta charset="utf-8">

        <meta
            name="viewport"
            content="width=device-width,initial-scale=1"
        >

        <meta
            http-equiv="refresh"
            content="60"
        >

        <title>
            TRADING SIGNAL CENTER
        </title>

        <style>
            {CSS}
        </style>

    </head>

    <body>

        <h1>
            📊 TRADING SIGNAL CENTER
        </h1>

        {s}

    </body>

    </html>
    """


# =========================================================
# 스케줄러
# =========================================================

def scheduler():

    while True:

        try:

            schedule.run_pending()

        except Exception as e:

            log.exception(
                "scheduler: %s",
                e
            )

        time.sleep(1)


# =========================================================
# 시작
# =========================================================

@app.on_event("startup")
def startup():

    log.info(
        "START | 동시 SIGNAL → TOP10"
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
    )
