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
# 사용자 설정
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
# SIGNAL 설정
# =========================================================

SIGNAL_TIMEFRAMES = (
    "15m",
    "1d"
)

TIMEFRAME_LABEL = {
    "15m": "15분",
    "1d": "일봉"
}


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


# =========================================================
# BTC
# =========================================================

latest_btc_okx_price = None

latest_btc_15m_periods = []

latest_btc_daily_periods = []

latest_btc_15m_change = None

latest_btc_daily_change = None


# =========================================================
# Lock
# =========================================================

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

        return timedelta(
            minutes=15
        )

    return timedelta(
        days=1
    )


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

            return x - timedelta(
                days=1
            )

        return x

    # -----------------------------------------------------
    # 15분
    # -----------------------------------------------------

    return now.replace(
        minute=(now.minute // 15) * 15,
        second=0,
        microsecond=0
    )


def recent_periods(
    tf,
    count=6
):

    cur = current_tf_start(tf)

    delta = timeframe_delta(tf)

    out = []

    for i in range(
        count - 1,
        -1,
        -1
    ):

        start = cur - delta * i

        end = start + delta

        if tf == "1d":

            label = start.strftime(
                "%m/%d 09:00"
            )

        else:

            label = start.strftime(
                "%m/%d %H:%M"
            )

        out.append({
            "start": start,
            "end": end,
            "active": i == 0,
            "label": label
        })

    return out


# =========================================================
# API 요청
# =========================================================

def wait_request():

    global last_request_time

    with request_lock:

        gap = (
            time.monotonic()
            - last_request_time
        )

        if gap < REQUEST_INTERVAL:

            time.sleep(
                REQUEST_INTERVAL - gap
            )

        last_request_time = (
            time.monotonic()
        )


def retry(
    func,
    *args,
    **kwargs
):

    for n in range(
        MAX_RETRIES
    ):

        try:

            wait_request()

            r = func(
                *args,
                **kwargs
            )

            if (
                not hasattr(
                    r,
                    "status_code"
                )
                or r.status_code == 200
            ):

                return r

            if r.status_code == 429:

                time.sleep(
                    min(
                        RATE_LIMIT_WAIT
                        * (n + 1),
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

                    "market":
                        x["market"],

                    "current_price":
                        float(
                            x["trade_price"]
                        ),

                    "volume_24h":
                        float(
                            x[
                                "acc_trade_price_24h"
                            ]
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
        .drop_duplicates(
            "datetime"
        )
        .reset_index(
            drop=True
        )
    )


# =========================================================
# 캔들 기본값
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

        "upper":
            h - max(
                o,
                cl
            ),

        "lower":
            min(
                o,
                cl
            ) - l,

        "body_ratio":
            body / total,

        "bull":
            cl > o,

        "bear":
            cl < o
    }


# =========================================================
# 상승장악
# =========================================================

def bullish_engulfing(
    a,
    b
):

    p1 = candle_parts(a)

    p2 = candle_parts(b)

    if not p1 or not p2:

        return False

    return bool(

        p2["bull"]

        and p2["open"]
        <= min(
            p1["open"],
            p1["close"]
        )

        and p2["close"]
        >= max(
            p1["open"],
            p1["close"]
        )

        and p2["body"]
        > p1["body"]
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

def two_patterns(
    a,
    b
):

    p1 = candle_parts(a)

    p2 = candle_parts(b)

    if not p1 or not p2:

        return []

    out = []

    # 상승장악
    if bullish_engulfing(
        a,
        b
    ):

        out.append(
            "상승장악"
        )

    # 장대양봉 후 양봉
    if (
        long_bullish(a)
        and p2["bull"]
    ):

        out.append(
            "장대양봉 후 양봉"
        )

    # 관통형
    mid = (
        p1["open"]
        + p1["close"]
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

    # 하락장악
    if (
        p1["bull"]
        and p2["bear"]
        and p2["open"]
        >= p1["close"]
        and p2["close"]
        <= p1["open"]
        and p2["body"]
        > p1["body"]
    ):

        out.append(
            "하락장악"
        )

    # 먹구름형
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
# 3캔들
# =========================================================

def three_patterns(
    a,
    b,
    c
):

    p1 = candle_parts(a)

    p2 = candle_parts(b)

    p3 = candle_parts(c)

    if not p1 or not p2 or not p3:

        return []

    out = []

    # 모닝스타
    if (
        p1["bear"]
        and p1["body_ratio"] >= 0.45
        and p2["body_ratio"] <= 0.35
        and p3["bull"]
        and p3["body_ratio"] >= 0.45
        and p3["close"]
        > (
            p1["open"]
            + p1["close"]
        ) / 2
    ):

        out.append(
            "모닝스타"
        )

    # 3연속 양봉
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

    # 3캔들 상승장악
    if bullish_engulfing(
        a,
        c
    ):

        out.append(
            "3캔들 상승장악"
        )

    # 상승장악 후 양봉
    if (
        bullish_engulfing(
            a,
            b
        )
        and p3["bull"]
    ):

        out.append(
            "상승장악 후 양봉"
        )

    # 관통형 후 양봉
    if (
        "관통형"
        in two_patterns(
            a,
            b
        )
        and p3["bull"]
    ):

        out.append(
            "관통형 후 양봉"
        )

    return out


# =========================================================
# 4캔들
# =========================================================

def four_patterns(
    a,
    b,
    c,
    d
):

    p1 = candle_parts(a)

    p2 = candle_parts(b)

    p3 = candle_parts(c)

    p4 = candle_parts(d)

    if not all(
        (
            p1,
            p2,
            p3,
            p4
        )
    ):

        return []

    if bullish_engulfing(
        a,
        d
    ):

        return [
            "4캔들 상승장악"
        ]

    return []


# =========================================================
# 기간 생성
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

        # 현재 진행봉
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

        change = (
            (c - o) / o * 100
            if o
            else None
        )

        out.append({

            **p,

            "open": o,

            "high": h,

            "low": l,

            "close": c,

            "change": change,

            "patterns": []
        })

    # 패턴 계산
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
# SIGNAL 판정
# =========================================================

def signal_pass(
    periods,
    timeframe
):

    if not periods:

        return False

    # -----------------------------------------------------
    # 15분
    # 직전 완료봉
    # -----------------------------------------------------

    if timeframe == "15m":

        if len(periods) < 2:

            return False

        target = periods[-2]

    # -----------------------------------------------------
    # 일봉
    # 현재 KST 09:00 진행봉
    # -----------------------------------------------------

    else:

        target = periods[-1]

    change = target.get(
        "change"
    )

    open_price = target.get(
        "open"
    )

    close_price = target.get(
        "close"
    )

    patterns = target.get(
        "patterns",
        []
    )

    if change is None:

        return False

    if open_price is None:

        return False

    if close_price is None:

        return False

    # 상승
    if change <= 0:

        return False

    # 양봉
    if close_price <= open_price:

        return False

    # 지정 패턴
    if not any(
        pattern in patterns
        for pattern
        in SIGNAL_CANDLE_PATTERNS
    ):

        return False

    return True


# =========================================================
# 분석
# =========================================================

def analyze(
    market,
    price,
    tf
):

    df = get_upbit_candles(
        market,
        tf,
        200
    )

    periods = build_periods(
        df,
        tf,
        price
    )

    signal = signal_pass(
        periods,
        tf
    )

    if tf == "15m":

        target_index = (
            -2
            if len(periods) >= 2
            else -1
        )

    else:

        target_index = -1

    target = (
        periods[target_index]
        if periods
        else {}
    )

    return {

        "periods":
            periods,

        "signal_pass":
            signal,

        "signal_change":
            target.get(
                "change"
            ),

        "signal_patterns":
            [
                x
                for x
                in SIGNAL_CANDLE_PATTERNS
                if x in target.get(
                    "patterns",
                    []
                )
            ]
    }


# =========================================================
# Row
# =========================================================

def make_row(
    rank,
    market,
    item,
    analysis_15m,
    analysis_daily
):

    coin = market.replace(
        "KRW-",
        ""
    )

    signal_15m = (
        analysis_15m[
            "signal_pass"
        ]
    )

    signal_daily = (
        analysis_daily[
            "signal_pass"
        ]
    )

    simultaneous = (
        signal_15m
        and signal_daily
    )

    return {

        "rank":
            rank,

        "name":
            coin,

        "market":
            market,

        "volume_24h":
            item[
                "volume_24h"
            ],

        "current_price":
            item[
                "current_price"
            ],

        "volume_rank":
            rank,


        # =============================================
        # 15분
        # =============================================

        "periods_15m":
            analysis_15m[
                "periods"
            ],

        "signal_15m":
            signal_15m,

        "signal_15m_change":
            analysis_15m[
                "signal_change"
            ],

        "signal_15m_patterns":
            analysis_15m[
                "signal_patterns"
            ],


        # =============================================
        # 일봉
        # =============================================

        "periods_daily":
            analysis_daily[
                "periods"
            ],

        "signal_daily":
            signal_daily,

        "signal_daily_change":
            analysis_daily[
                "signal_change"
            ],

        "signal_daily_patterns":
            analysis_daily[
                "signal_patterns"
            ],


        # =============================================
        # 동시
        # =============================================

        "simultaneous_signal":
            simultaneous
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
        key=lambda x:
            x["volume_24h"],
        reverse=True
    )[:TOP_N]

    rows = []

    for rank, item in enumerate(
        markets,
        1
    ):

        market = item[
            "market"
        ]

        price = item[
            "current_price"
        ]

        # -----------------------------------------
        # 15분
        # -----------------------------------------

        try:

            analysis_15m = analyze(
                market,
                price,
                "15m"
            )

        except Exception as e:

            log.warning(
                "%s 15분 오류: %s",
                market,
                e
            )

            analysis_15m = {

                "periods": [],

                "signal_pass": False,

                "signal_change": None,

                "signal_patterns": []
            }

        # -----------------------------------------
        # 일봉
        # -----------------------------------------

        try:

            analysis_daily = analyze(
                market,
                price,
                "1d"
            )

        except Exception as e:

            log.warning(
                "%s 일봉 오류: %s",
                market,
                e
            )

            analysis_daily = {

                "periods": [],

                "signal_pass": False,

                "signal_change": None,

                "signal_patterns": []
            }

        row = make_row(
            rank,
            market,
            item,
            analysis_15m,
            analysis_daily
        )

        rows.append(row)

    latest_upbit_data = rows

    latest_upbit_15m_data = rows

    latest_upbit_daily_data = rows

    latest_upbit_15m_update_time = kst()

    latest_upbit_daily_update_time = kst()

    latest_upbit_update_time = (
        latest_upbit_15m_update_time
    )

    log.info(
        "UPBIT | 15분=%s | 일봉=%s | 동시=%s",

        sum(
            x["signal_15m"]
            for x in rows
        ),

        sum(
            x["signal_daily"]
            for x in rows
        ),

        sum(
            x["simultaneous_signal"]
            for x in rows
        )
    )


# =========================================================
# OKX
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
        .drop_duplicates(
            "datetime"
        )
        .reset_index(
            drop=True
        )
    )


# =========================================================
# OKX 가격
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
        )

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

    d15 = okx_candles(
        "15m",
        200
    )

    d1h = okx_candles(
        "1H",
        200
    )

    latest_btc_15m_periods = (
        build_periods(
            d15,
            "15m",
            price
        )
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
            (
                shifted["datetime"]
                - pd.Timedelta(
                    hours=9
                )
            )
            .dt
            .floor("D")
            + pd.Timedelta(
                hours=9
            )
        )

        daily = (
            shifted
            .groupby("day_start")
            .agg(
                open=(
                    "open",
                    "first"
                ),

                high=(
                    "high",
                    "max"
                ),

                low=(
                    "low",
                    "min"
                ),

                close=(
                    "close",
                    "last"
                )
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

    latest_btc_daily_periods = (
        build_periods(
            daily,
            "1d",
            price
        )
    )

    if latest_btc_15m_periods:

        latest_btc_15m_change = (
            latest_btc_15m_periods[-1]
            .get("change")
        )

    else:

        latest_btc_15m_change = None

    if latest_btc_daily_periods:

        latest_btc_daily_change = (
            latest_btc_daily_periods[-1]
            .get("change")
        )

    else:

        latest_btc_daily_change = None


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    if not update_lock.acquire(
        False
    ):

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

    if v > 0:

        return (
            '<span class="up">'
            f'▲ +{v:.2f}%'
            '</span>'
        )

    if v < 0:

        return (
            '<span class="down">'
            f'▼ {v:.2f}%'
            '</span>'
        )

    return (
        '<span class="zero">'
        '0.00%'
        '</span>'
    )


# =========================================================
# 캔들 표시
# =========================================================

def cells(periods):

    result = []

    for p in periods:

        active_class = (
            "current"
            if p.get("active")
            else ""
        )

        patterns = p.get(
            "patterns",
            []
        )

        pattern_html = ""

        if patterns:

            pattern_html = (
                "<small>"
                + " · ".join(
                    html.escape(x)
                    for x in patterns
                )
                + "</small>"
            )

        result.append(
            f"""
            <div class="tf-cell {active_class}">

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
                        p.get(
                            "change"
                        )
                    )}
                </strong>

                {pattern_html}

            </div>
            """
        )

    return "".join(result)


# =========================================================
# BTC HTML
# =========================================================

def btc_html():

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

            <b>
                BTC
            </b>

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
                15분
            </strong>

        </div>


        <div class="btc-label">
            15분
        </div>

        <div class="grid">

            {cells(
                latest_btc_15m_periods
            )}

        </div>


        <div class="btc-label">
            일봉 · KST 09:00
        </div>

        <div class="grid">

            {cells(
                latest_btc_daily_periods
            )}

        </div>

    </div>
    """


# =========================================================
# 카드
# =========================================================

def card(
    row,
    kind
):

    both = row.get(
        "simultaneous_signal",
        False
    )


    # =====================================================
    # TOP10
    # =====================================================

    if kind == "top":

        if both:

            status = (
                '<span class="top-status both">'
                '⭐ 동시'
                '</span>'
            )

        else:

            status = (
                '<span class="top-status none">'
                '—'
                '</span>'
            )


        return f"""

        <div class="card top-card">


            <!-- =========================================
                 종목 헤더
                 ========================================= -->

            <div class="card-head top-head">

                <span>
                    #{row["rank"]}
                </span>

                <b>
                    {html.escape(
                        row["name"]
                    )}
                </b>

                {status}

            </div>


            <!-- =========================================
                 시황
                 ========================================= -->

            <div class="info-section-title">

                📊 시황

            </div>


            <div class="top-summary">


                <div class="top-summary-item">

                    <span>
                        현재가
                    </span>

                    <strong>
                        {fmt_price(
                            row[
                                "current_price"
                            ]
                        )}
                    </strong>

                </div>


                <div class="top-summary-item">

                    <span>
                        24H 거래대금
                    </span>

                    <strong>
                        {fmt_vol(
                            row[
                                "volume_24h"
                            ]
                        )}
                    </strong>

                </div>


                <div class="top-summary-item">

                    <span>
                        15분 변동
                    </span>

                    <strong>
                        {fmt_change(
                            row.get(
                                "signal_15m_change"
                            )
                        )}
                    </strong>

                </div>


                <div class="top-summary-item">

                    <span>
                        일봉 변동
                    </span>

                    <strong>
                        {fmt_change(
                            row.get(
                                "signal_daily_change"
                            )
                        )}
                    </strong>

                </div>


            </div>


            <!-- =========================================
                 시황 / SIGNAL 강한 구분
                 ========================================= -->

            <div class="major-divider">

                <span>
                    ⭐ SIGNAL
                </span>

            </div>


            <!-- =========================================
                 15분 SIGNAL
                 ========================================= -->

            <div class="signal-title">

                <b>
                    15분 SIGNAL
                </b>

                <span>
                    이전 확정봉
                </span>

            </div>


            <div class="grid">

                {cells(
                    row.get(
                        "periods_15m",
                        []
                    )
                )}

            </div>


            <!-- =========================================
                 15분 / 일봉 구분
                 ========================================= -->

            <div class="signal-separator">
            </div>


            <!-- =========================================
                 일봉 SIGNAL
                 ========================================= -->

            <div class="signal-title daily-signal">

                <b>
                    일봉 SIGNAL
                </b>

                <span>
                    KST 09:00 기준
                </span>

            </div>


            <div class="grid">

                {cells(
                    row.get(
                        "periods_daily",
                        []
                    )
                )}

            </div>


        </div>

        """


    # =====================================================
    # 동시 SIGNAL 카드
    # =====================================================

    badge = ""

    if both:

        badge = (
            '<span class="both">'
            '⭐ 동시 SIGNAL'
            '</span>'
        )


    return f"""

    <div class="card both-card">


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
                ⭐ 15분 + 일봉
            </em>

        </div>


        <div class="signal-summary">


            <div>

                <span>
                    15분
                </span>

                <strong>
                    {fmt_change(
                        row.get(
                            "signal_15m_change"
                        )
                    )}
                </strong>

            </div>


            <div>

                <span>
                    일봉
                </span>

                <strong>
                    {fmt_change(
                        row.get(
                            "signal_daily_change"
                        )
                    )}
                </strong>

            </div>


            <div>

                <span>
                    현재가
                </span>

                <strong>
                    {fmt_price(
                        row[
                            "current_price"
                        ]
                    )}
                </strong>

            </div>


        </div>


        <div class="tf-title">

            <b>
                15분
            </b>

            <span>
                이전 확정봉
            </span>

        </div>


        <div class="grid">

            {cells(
                row.get(
                    "periods_15m",
                    []
                )
            )}

        </div>


        <div class="signal-separator">
        </div>


        <div class="tf-title daily-signal">

            <b>
                일봉
            </b>

            <span>
                현재봉 · KST 09:00
            </span>

        </div>


        <div class="grid">

            {cells(
                row.get(
                    "periods_daily",
                    []
                )
            )}

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

        content = "".join(
            card(
                r,
                "both"
            )
            for r in rows
        )

    else:

        content = (
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
                15분 SIGNAL과 일봉 SIGNAL이
                동시에 발생한 종목
            </small>

        </header>

        {content}

    </section>

    """


# =========================================================
# CSS
# =========================================================

CSS = """

* {
    box-sizing:
        border-box;
}


body {

    margin:
        0;

    background:
        #080c11;

    color:
        #e7ebef;

    font-family:
        Arial,
        sans-serif;

    font-size:
        11px;

    padding:
        8px;
}


h1 {

    font-size:
        15px;

    margin:
        3px 2px 9px;
}


section {

    margin:
        8px 0 12px;
}


section > header {

    padding:
        8px 10px;

    border:
        2px solid #26313b;

    border-radius:
        10px;

    background:
        #10151b;

    margin-bottom:
        6px;
}


section > header b {

    display:
        block;

    font-size:
        11px;
}


section > header small {

    display:
        block;

    color:
        #7d8892;

    font-size:
        7px;

    margin-top:
        2px;
}


section > header.gold {

    border-color:
        #c9a83d;

    background:
        #1d190d;
}


/* =====================================================
   상승 / 하락
   ===================================================== */

.up {

    color:
        #36d66b;
}


.down {

    color:
        #ff5c68;
}


.zero {

    color:
        #66727d;
}


/* =====================================================
   BTC
   ===================================================== */

.market-card {

    border:
        2px solid #26313b;

    border-radius:
        11px;

    overflow:
        hidden;

    margin-bottom:
        10px;
}


.market-head {

    padding:
        8px;

    background:
        #111820;

    display:
        flex;

    justify-content:
        space-between;

    gap:
        8px;
}


.market-head span {

    font-size:
        7px;

    color:
        #74808b;
}


.btc-row {

    display:
        grid;

    grid-template-columns:
        1fr 1.3fr 1.2fr 1fr;

    min-height:
        48px;

    align-items:
        center;

    text-align:
        center;
}


.btc-row > * {

    padding:
        5px;
}


.btc-row > * + * {

    border-left:
        1px solid #29323c;
}


.btc-label {

    padding:
        5px 8px;

    background:
        #111820;

    color:
        #aab3ba;

    font-weight:
        900;

    font-size:
        8px;
}


/* =====================================================
   공통 GRID
   ===================================================== */

.grid {

    display:
        grid;

    grid-template-columns:
        repeat(6, 1fr);

    gap:
        1px;

    background:
        #29323c;
}


.tf-cell {

    min-height:
        47px;

    background:
        #0d1319;

    padding:
        4px;

    text-align:
        center;

    font-size:
        7px;
}


.tf-cell.current {

    background:
        #173326;
}


.tf-cell strong {

    display:
        block;

    font-size:
        8px;

    margin-top:
        2px;
}


.tf-cell small {

    display:
        block;

    color:
        #e0bd6d;

    font-size:
        5px;

    white-space:
        nowrap;

    overflow:
        hidden;

    text-overflow:
        ellipsis;
}


/* =====================================================
   카드
   ===================================================== */

.card {

    border:
        2px solid #26313b;

    border-radius:
        10px;

    overflow:
        hidden;

    margin:
        6px 0;

    background:
        #0f141a;
}


.card.both-card {

    border-color:
        #d2ae42;
}


.card-head {

    min-height:
        38px;

    padding:
        6px 8px;

    background:
        #121820;

    display:
        flex;

    align-items:
        center;

    gap:
        7px;
}


.card-head > span {

    color:
        #d8b85c;

    font-weight:
        900;
}


.card-head b {

    flex:
        1;

    font-size:
        10px;
}


.card-head em {

    font-style:
        normal;

    color:
        #7e8994;

    font-size:
        7px;
}


/* =====================================================
   동시 SIGNAL 배지
   ===================================================== */

.both {

    padding:
        3px 6px;

    border:
        1px solid #d2ae42;

    border-radius:
        5px;

    background:
        #2b230d;

    color:
        #f0cf67;

    font-size:
        6px;

    font-weight:
        900;
}


/* =====================================================
   TOP10
   ===================================================== */

.top-card {

    border:
        1px solid #29333d;

    background:
        #0d1319;

    margin-bottom:
        10px;
}


.top-head {

    min-height:
        38px;

    border-bottom:
        1px solid #29333d;
}


.top-status {

    padding:
        3px 7px;

    border-radius:
        5px;

    font-size:
        7px;

    font-weight:
        900;
}


.top-status.both {

    color:
        #f0cf67;

    background:
        #2b230d;

    border:
        1px solid #8d7527;
}


.top-status.none {

    color:
        #66727d;
}


/* =====================================================
   TOP10
   시황 제목
   ===================================================== */

.info-section-title {

    height:
        32px;

    display:
        flex;

    align-items:
        center;

    padding:
        0 10px;

    background:
        #18212a;

    color:
        #e7ebef;

    font-size:
        9px;

    font-weight:
        900;

    border-top:
        2px solid #4b5965;

    border-bottom:
        2px solid #4b5965;
}


/* =====================================================
   TOP10
   시황 내용
   ===================================================== */

.top-summary {

    display:
        grid;

    grid-template-columns:
        repeat(4, 1fr);

    gap:
        1px;

    background:
        #39444f;

    border-bottom:
        1px solid #39444f;
}


.top-summary-item {

    min-height:
        50px;

    padding:
        6px;

    text-align:
        center;

    background:
        #0f151b;
}


.top-summary-item span {

    display:
        block;

    color:
        #77838e;

    font-size:
        7px;

    margin-bottom:
        4px;
}


.top-summary-item strong {

    display:
        block;

    font-size:
        9px;
}


/* =====================================================
   시황 → SIGNAL
   아주 명확한 구분
   ===================================================== */

.major-divider {

    height:
        40px;

    display:
        flex;

    align-items:
        center;

    justify-content:
        center;

    background:
        #080c11;

    border-top:
        4px solid #596773;

    border-bottom:
        4px solid #8d7527;

    margin-top:
        2px;
}


.major-divider span {

    padding:
        5px 16px;

    border:
        1px solid #8d7527;

    border-radius:
        6px;

    background:
        #2b230d;

    color:
        #f0cf67;

    font-size:
        8px;

    font-weight:
        900;
}


/* =====================================================
   SIGNAL 제목
   ===================================================== */

.signal-title {

    height:
        31px;

    display:
        flex;

    align-items:
        center;

    justify-content:
        space-between;

    padding:
        0 10px;

    background:
        #111920;

    border-bottom:
        2px solid #394650;
}


.signal-title b {

    color:
        #e7ebef;

    font-size:
        8px;
}


.signal-title span {

    color:
        #7f8b95;

    font-size:
        6px;
}


/* =====================================================
   15분 → 일봉
   강한 구분
   ===================================================== */

.signal-separator {

    height:
        10px;

    background:
        #080c11;

    border-top:
        3px solid #46525d;

    border-bottom:
        3px solid #46525d;
}


.daily-signal {

    background:
        #151b21;

    border-top:
        1px solid #59636d;

    border-bottom:
        2px solid #59636d;
}


/* =====================================================
   동시 SIGNAL 카드 요약
   ===================================================== */

.signal-summary {

    display:
        grid;

    grid-template-columns:
        repeat(3, 1fr);

    border-bottom:
        1px solid #594a25;
}


.signal-summary > div {

    padding:
        7px;

    text-align:
        center;

    background:
        #151713;
}


.signal-summary > div + div {

    border-left:
        1px solid #594a25;
}


.signal-summary span {

    display:
        block;

    color:
        #8b8b7b;

    font-size:
        7px;
}


.signal-summary strong {

    display:
        block;

    margin-top:
        3px;

    font-size:
        9px;
}


/* =====================================================
   빈 데이터
   ===================================================== */

.empty {

    text-align:
        center;

    padding:
        25px;

    border:
        1px solid #26313b;

    border-radius:
        10px;

    color:
        #65717c;
}


/* =====================================================
   기존 MAIN
   ===================================================== */

.main {

    display:
        grid;

    grid-template-columns:
        repeat(3, 1fr);

    min-height:
        43px;
}


.main > div {

    text-align:
        center;

    padding:
        5px;

    border-top:
        1px solid #29323c;
}


.main > div + div {

    border-left:
        1px solid #29323c;
}


.main strong {

    display:
        block;

    margin-top:
        2px;
}


/* =====================================================
   모바일
   ===================================================== */

@media (max-width: 600px) {

    body {

        padding:
            5px;
    }


    h1 {

        font-size:
            12px;
    }


    .grid {

        grid-template-columns:
            repeat(3, 1fr);
    }


    .market-head {

        font-size:
            9px;
    }


    .market-head span {

        font-size:
            5px;
    }


    .btc-row {

        min-height:
            40px;

        font-size:
            8px;
    }


    .btc-label {

        font-size:
            6px;
    }


    .tf-cell {

        min-height:
            43px;

        font-size:
            6px;
    }


    .tf-cell strong {

        font-size:
            7px;
    }


    .tf-cell small {

        font-size:
            4px;
    }


    .card-head {

        min-height:
            33px;

        padding:
            5px 6px;
    }


    .card-head b {

        font-size:
            8px;
    }


    .card-head em {

        font-size:
            5px;
    }


    .both {

        font-size:
            4.5px;
    }


    .top-summary-item {

        min-height:
            45px;

        padding:
            5px 2px;
    }


    .top-summary-item span {

        font-size:
            6px;
    }


    .top-summary-item strong {

        font-size:
            7px;
    }


    .info-section-title {

        height:
            29px;

        font-size:
            8px;
    }


    .major-divider {

        height:
            36px;

        border-top-width:
            3px;

        border-bottom-width:
            3px;
    }


    .major-divider span {

        font-size:
            7px;

        padding:
            4px 12px;
    }


    .signal-title {

        height:
            27px;
    }


    .signal-title b {

        font-size:
            7px;
    }


    .signal-title span {

        font-size:
            5px;
    }

}
"""


# =========================================================
# DASHBOARD
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def dashboard():

    s = btc_html()

    if USE_UPBIT == "Y":

        # -------------------------------------------------
        # 동시 SIGNAL만 표시
        # -------------------------------------------------

        s += both_section(
            latest_upbit_data
        )


        # -------------------------------------------------
        # TOP10
        # -------------------------------------------------

        if latest_upbit_data:

            top_cards = "".join(
                card(
                    r,
                    "top"
                )
                for r
                in latest_upbit_data
            )

        else:

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
                    거래대금 순위 ·
                    시황 + 15분 SIGNAL + 일봉 SIGNAL
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
            content="width=device-width,
            initial-scale=1"
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
# STARTUP
# =========================================================

@app.on_event("startup")
def startup():

    log.info(
        "START | BTC → 동시 SIGNAL → TOP10"
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
