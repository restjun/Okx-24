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
# 타임프레임
#
# 15분 / 일봉 동시 사용
# =========================================================

TIMEFRAMES = {
    "15m": {
        "label": "15분",
        "upbit_unit": 15,
        "okx_bar": "15m"
    },

    "1d": {
        "label": "일봉",
        "upbit_unit": "day",
        "okx_bar": "1D"
    }
}


# =========================================================
# 전역 데이터
# =========================================================

latest_upbit_data = []

latest_upbit_15m_data = []
latest_upbit_daily_data = []

latest_upbit_update_time = "-"

latest_okx_data = []
latest_okx_update_time = "-"

latest_upbit_markets = []

request_lock = threading.Lock()
update_lock = threading.Lock()

last_request_time = 0


# =========================================================
# OKX BTC
# =========================================================

OKX_BASE_URL = "https://www.okx.com"

OKX_BTC_INST_ID = "BTC-USDT"

latest_btc_okx_price = None

latest_btc_15m_periods = []
latest_btc_daily_periods = []

latest_btc_15m_change = None
latest_btc_daily_change = None

latest_btc_15m_signal = False
latest_btc_daily_signal = False
latest_btc_simultaneous_signal = False


# =========================================================
# 현재 KST
# =========================================================

def kst():

    return datetime.now(
        KST
    ).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


# =========================================================
# 현재 일봉 시작
#
# KST 09:00 기준
# =========================================================

def get_current_daily_start():

    now = datetime.now(KST)

    today_0900 = now.replace(
        hour=9,
        minute=0,
        second=0,
        microsecond=0
    )

    if now < today_0900:

        return (
            today_0900
            -
            timedelta(days=1)
        )

    return today_0900


# =========================================================
# 현재 15분봉 시작
# =========================================================

def get_current_15m_start():

    now = datetime.now(KST)

    minute = (
        now.minute // 15
    ) * 15

    return now.replace(
        minute=minute,
        second=0,
        microsecond=0
    )


# =========================================================
# 기간 생성
# =========================================================

def make_period(
    start,
    timeframe,
    active=False
):

    start = start.astimezone(KST)

    if timeframe == "1d":

        end = (
            start +
            timedelta(days=1)
        )

        label = start.strftime(
            "%m/%d 09:00"
        )

    else:

        end = (
            start +
            timedelta(minutes=15)
        )

        label = start.strftime(
            "%m/%d %H:%M"
        )


    return {

        "key":
            start.strftime(
                "%Y%m%d%H%M"
            ),

        "time_key":
            start.strftime(
                "%Y%m%d%H%M"
            ),

        "label":
            label,

        "display_label":
            (
                f"현재 {label}"
                if active
                else label
            ),

        "start":
            start,

        "end":
            end,

        "active":
            active

    }


# =========================================================
# 최근 기간
# =========================================================

def get_recent_periods(
    timeframe,
    count=6
):

    if timeframe == "1d":

        current_start = (
            get_current_daily_start()
        )

        delta = timedelta(
            days=1
        )

    else:

        current_start = (
            get_current_15m_start()
        )

        delta = timedelta(
            minutes=15
        )


    periods = []


    for i in range(
        count - 1,
        -1,
        -1
    ):

        start = (
            current_start
            -
            delta * i
        )

        periods.append(
            make_period(
                start,
                timeframe,
                active=(i == 0)
            )
        )


    return periods


# =========================================================
# API 요청 제한
# =========================================================

def wait_request():

    global last_request_time

    with request_lock:

        elapsed = (
            time.monotonic()
            -
            last_request_time
        )

        if elapsed < REQUEST_INTERVAL:

            time.sleep(
                REQUEST_INTERVAL -
                elapsed
            )

        last_request_time = (
            time.monotonic()
        )


# =========================================================
# API 재시도
# =========================================================

def retry(
    func,
    *args,
    **kwargs
):

    for attempt in range(
        MAX_RETRIES
    ):

        try:

            wait_request()

            response = func(
                *args,
                **kwargs
            )


            if not hasattr(
                response,
                "status_code"
            ):

                return response


            if response.status_code == 200:

                return response


            if response.status_code == 429:

                time.sleep(
                    min(
                        RATE_LIMIT_WAIT *
                        (attempt + 1),
                        60
                    )
                )

                continue


            if response.status_code >= 500:

                time.sleep(
                    min(
                        2 *
                        (attempt + 1),
                        30
                    )
                )

                continue


            return response


        except Exception as e:

            log.warning(
                f"API 오류 "
                f"{attempt + 1}/"
                f"{MAX_RETRIES}: {e}"
            )

            if attempt < MAX_RETRIES - 1:

                time.sleep(
                    min(
                        2 *
                        (attempt + 1),
                        20
                    )
                )


    return None


# =========================================================
# 업비트 마켓
# =========================================================

def get_upbit_markets():

    global latest_upbit_markets


    response = retry(
        requests.get,
        "https://api.upbit.com/v1/market/all",
        params={
            "isDetails": "false"
        },
        timeout=15
    )


    if response is None:

        return []


    try:

        markets = response.json()

    except Exception:

        return []


    krw_markets = [

        x["market"]

        for x in markets

        if x.get(
            "market",
            ""
        ).startswith("KRW-")

    ]


    result = []


    for i in range(
        0,
        len(krw_markets),
        100
    ):

        chunk = krw_markets[
            i:i + 100
        ]


        response = retry(
            requests.get,
            "https://api.upbit.com/v1/ticker",
            params={
                "markets":
                    ",".join(chunk)
            },
            timeout=15
        )


        if response is None:

            continue


        try:

            data = response.json()

        except Exception:

            continue


        if not isinstance(
            data,
            list
        ):

            continue


        for item in data:

            try:

                result.append({

                    "market":
                        item["market"],

                    "current_price":
                        float(
                            item["trade_price"]
                        ),

                    "volume_24h":
                        float(
                            item[
                                "acc_trade_price_24h"
                            ]
                        )

                })

            except Exception:

                continue


    latest_upbit_markets = [

        x["market"]

        for x in result

    ]


    return result


# =========================================================
# 업비트 일봉
# =========================================================

def get_upbit_daily_candles(
    market,
    count=200
):

    response = retry(
        requests.get,
        "https://api.upbit.com/v1/candles/days",
        params={
            "market": market,
            "count": count
        },
        timeout=15
    )


    if response is None:

        return []


    try:

        data = response.json()

    except Exception:

        return []


    if not isinstance(
        data,
        list
    ):

        return []


    return data


# =========================================================
# 업비트 15분
# =========================================================

def get_upbit_15m_candles(
    market,
    count=200
):

    response = retry(
        requests.get,
        "https://api.upbit.com/v1/candles/minutes/15",
        params={
            "market": market,
            "count": count
        },
        timeout=15
    )


    if response is None:

        return []


    try:

        data = response.json()

    except Exception:

        return []


    if not isinstance(
        data,
        list
    ):

        return []


    return data


# =========================================================
# 캔들 구성요소
# =========================================================

def candle_parts(
    candle
):

    try:

        o = float(
            candle["open"]
        )

        h = float(
            candle["high"]
        )

        l = float(
            candle["low"]
        )

        c = float(
            candle["close"]
        )

    except Exception:

        return None


    total = h - l


    if total <= 0:

        return None


    body = abs(
        c - o
    )

    upper = (
        h -
        max(o, c)
    )

    lower = (
        min(o, c) -
        l
    )

    body_ratio = (
        body /
        total
    )


    return {

        "open": o,
        "high": h,
        "low": l,
        "close": c,

        "body": body,
        "total": total,
        "upper": upper,
        "lower": lower,

        "body_ratio":
            body_ratio,

        "bull":
            c > o,

        "bear":
            c < o

    }


# =========================================================
# 상승장악
# =========================================================

def is_bullish_engulfing(
    first,
    current
):

    p1 = candle_parts(first)
    p2 = candle_parts(current)


    if (
        p1 is None
        or
        p2 is None
    ):

        return False


    if not p2["bull"]:

        return False


    first_body_high = max(
        p1["open"],
        p1["close"]
    )

    first_body_low = min(
        p1["open"],
        p1["close"]
    )


    if p2["open"] > first_body_low:

        return False


    if p2["close"] < first_body_high:

        return False


    if p2["body"] <= p1["body"]:

        return False


    return True


# =========================================================
# 장대양봉
# =========================================================

def is_long_bullish(
    candle
):

    p = candle_parts(
        candle
    )


    if p is None:

        return False


    return (
        p["bull"]
        and
        p["body_ratio"] >= 0.45
    )


# =========================================================
# 1캔들 패턴
# =========================================================

def detect_single_candle_pattern(
    candle
):

    p = candle_parts(
        candle
    )


    if p is None:

        return []


    patterns = []


    if p["body_ratio"] <= 0.10:

        patterns.append(
            "도지"
        )


    if (

        p["body_ratio"] <= 0.40

        and

        p["lower"] >= max(
            p["body"] * 2,
            p["total"] * 0.45
        )

        and

        p["upper"] <= max(
            p["body"],
            p["total"] * 0.15
        )

    ):

        patterns.append(
            "망치형"
        )


    if (

        p["body_ratio"] <= 0.40

        and

        p["upper"] >= max(
            p["body"] * 2,
            p["total"] * 0.45
        )

        and

        p["lower"] <= max(
            p["body"],
            p["total"] * 0.15
        )

    ):

        patterns.append(
            "역망치형"
        )


    return patterns


# =========================================================
# 2캔들 패턴
# =========================================================

def detect_two_candle_pattern(
    previous,
    current
):

    p1 = candle_parts(previous)
    p2 = candle_parts(current)


    if (
        p1 is None
        or
        p2 is None
    ):

        return []


    patterns = []


    if is_bullish_engulfing(
        previous,
        current
    ):

        patterns.append(
            "상승장악"
        )


    if (
        is_long_bullish(
            previous
        )
        and
        p2["bull"]
    ):

        patterns.append(
            "장대양봉 후 양봉"
        )


    midpoint = (
        p1["open"] +
        p1["close"]
    ) / 2


    if (

        p1["bear"]

        and
        p2["bull"]

        and
        p2["close"] > midpoint

        and
        p2["close"] < p1["open"]

    ):

        patterns.append(
            "관통형"
        )


    if (

        p1["bull"]

        and
        p2["bear"]

        and
        p2["open"] >= p1["close"]

        and
        p2["close"] <= p1["open"]

        and
        p2["body"] > p1["body"]

    ):

        patterns.append(
            "하락장악"
        )


    if (

        p1["bull"]

        and
        p2["bear"]

        and
        p2["close"] < midpoint

        and
        p2["close"] > p1["open"]

    ):

        patterns.append(
            "먹구름형"
        )


    return patterns


# =========================================================
# 3캔들 패턴
# =========================================================

def detect_three_candle_pattern(
    c1,
    c2,
    c3
):

    p1 = candle_parts(c1)
    p2 = candle_parts(c2)
    p3 = candle_parts(c3)


    if (
        p1 is None
        or
        p2 is None
        or
        p3 is None
    ):

        return []


    patterns = []


    # 모닝스타

    if (

        p1["bear"]

        and
        p1["body_ratio"] >= 0.45

        and
        p2["body_ratio"] <= 0.35

        and
        p3["bull"]

        and
        p3["body_ratio"] >= 0.45

        and
        p3["close"] >
        (
            p1["open"] +
            p1["close"]
        ) / 2

    ):

        patterns.append(
            "모닝스타"
        )


    # 3연속 양봉

    if (

        p1["bull"]

        and
        p2["bull"]

        and
        p3["bull"]

        and
        p2["close"] > p1["close"]

        and
        p3["close"] > p2["close"]

    ):

        patterns.append(
            "3연속양봉"
        )


    # 3캔들 상승장악

    if is_bullish_engulfing(
        c1,
        c3
    ):

        patterns.append(
            "3캔들 상승장악"
        )


    # 상승장악 후 양봉

    if (

        is_bullish_engulfing(
            c1,
            c2
        )

        and
        p3["bull"]

    ):

        patterns.append(
            "상승장악 후 양봉"
        )


    # 관통형 후 양봉

    if (

        "관통형"
        in
        detect_two_candle_pattern(
            c1,
            c2
        )

        and
        p3["bull"]

    ):

        patterns.append(
            "관통형 후 양봉"
        )


    # 3캔들 관통형

    midpoint = (
        p1["open"] +
        p1["close"]
    ) / 2


    if (

        p1["bear"]

        and
        p3["bull"]

        and
        p3["close"] > midpoint

        and
        p3["close"] < p1["open"]

    ):

        patterns.append(
            "3캔들 관통형"
        )


    return patterns


# =========================================================
# 4캔들 패턴
# =========================================================

def detect_four_candle_pattern(
    c1,
    c2,
    c3,
    c4
):

    p1 = candle_parts(c1)
    p2 = candle_parts(c2)
    p3 = candle_parts(c3)
    p4 = candle_parts(c4)


    if any(
        x is None
        for x in [
            p1,
            p2,
            p3,
            p4
        ]
    ):

        return []


    patterns = []


    if is_bullish_engulfing(
        c1,
        c4
    ):

        patterns.append(
            "4캔들 상승장악"
        )


    midpoint = (
        p1["open"] +
        p1["close"]
    ) / 2


    if (

        p1["bear"]

        and
        p4["bull"]

        and
        p4["close"] > midpoint

        and
        p4["close"] < p1["open"]

    ):

        patterns.append(
            "4캔들 관통형"
        )


    return patterns


# =========================================================
# 전체 패턴 분석
# =========================================================

def detect_patterns(
    periods
):

    result = []


    for i, period in enumerate(
        periods
    ):

        patterns = []


        required = [
            "open",
            "high",
            "low",
            "close"
        ]


        if any(
            period.get(x) is None
            for x in required
        ):

            result.append([])

            continue


        patterns.extend(
            detect_single_candle_pattern(
                period
            )
        )


        if i >= 1:

            previous = periods[
                i - 1
            ]


            if all(
                previous.get(x)
                is not None
                for x in required
            ):

                patterns.extend(
                    detect_two_candle_pattern(
                        previous,
                        period
                    )
                )


        if i >= 2:

            c1 = periods[
                i - 2
            ]

            c2 = periods[
                i - 1
            ]

            c3 = periods[
                i
            ]


            if all(
                x.get(key) is not None
                for x in [
                    c1,
                    c2,
                    c3
                ]
                for key in required
            ):

                patterns.extend(
                    detect_three_candle_pattern(
                        c1,
                        c2,
                        c3
                    )
                )


        if i >= 3:

            c1 = periods[
                i - 3
            ]

            c2 = periods[
                i - 2
            ]

            c3 = periods[
                i - 1
            ]

            c4 = periods[
                i
            ]


            if all(
                x.get(key) is not None
                for x in [
                    c1,
                    c2,
                    c3,
                    c4
                ]
                for key in required
            ):

                patterns.extend(
                    detect_four_candle_pattern(
                        c1,
                        c2,
                        c3,
                        c4
                    )
                )


        result.append(
            list(
                dict.fromkeys(
                    patterns
                )
            )
        )


    return result


# =========================================================
# 업비트 캔들 DataFrame 변환
# =========================================================

def convert_upbit_candles(
    candles
):

    rows = []


    for candle in candles:

        try:

            dt = datetime.strptime(
                candle[
                    "candle_date_time_kst"
                ],
                "%Y-%m-%dT%H:%M:%S"
            ).replace(
                tzinfo=KST
            )


            rows.append({

                "datetime":
                    dt,

                "open":
                    float(
                        candle[
                            "opening_price"
                        ]
                    ),

                "high":
                    float(
                        candle[
                            "high_price"
                        ]
                    ),

                "low":
                    float(
                        candle[
                            "low_price"
                        ]
                    ),

                "close":
                    float(
                        candle[
                            "trade_price"
                        ]
                    )

            })

        except Exception:

            continue


    if not rows:

        return pd.DataFrame()


    return (
        pd.DataFrame(rows)
        .sort_values("datetime")
        .drop_duplicates(
            "datetime"
        )
    )


# =========================================================
# 업비트 특정 타임프레임 구성
# =========================================================

def build_upbit_timeframe(
    market,
    timeframe,
    current_price=None
):

    if timeframe == "15m":

        candles = (
            get_upbit_15m_candles(
                market,
                200
            )
        )

    else:

        candles = (
            get_upbit_daily_candles(
                market,
                200
            )
        )


    df = convert_upbit_candles(
        candles
    )


    if df.empty:

        return []


    periods = get_recent_periods(
        timeframe,
        6
    )


    result = []


    for period in periods:

        part = df[
            (
                df["datetime"]
                >= period["start"]
            )
            &
            (
                df["datetime"]
                < period["end"]
            )
        ].copy()


        if part.empty:

            result.append({

                **period,

                "open": None,
                "high": None,
                "low": None,
                "close": None,

                "change": None,

                "patterns": []

            })

            continue


        part = part.sort_values(
            "datetime"
        )


        open_price = float(
            part.iloc[0]["open"]
        )

        high_price = float(
            part["high"].max()
        )

        low_price = float(
            part["low"].min()
        )

        close_price = float(
            part.iloc[-1]["close"]
        )


        if period["active"]:

            if current_price is not None:

                try:

                    cp = float(
                        current_price
                    )

                    close_price = cp

                    high_price = max(
                        high_price,
                        cp
                    )

                    low_price = min(
                        low_price,
                        cp
                    )

                except Exception:

                    pass


        change = (

            (
                close_price -
                open_price
            )
            /
            open_price
            *
            100

            if open_price != 0

            else None

        )


        result.append({

            **period,

            "open":
                open_price,

            "high":
                high_price,

            "low":
                low_price,

            "close":
                close_price,

            "change":
                change,

            "patterns": []

        })


    pattern_results = (
        detect_patterns(
            result
        )
    )


    for i, patterns in enumerate(
        pattern_results
    ):

        result[i]["patterns"] = (
            patterns
        )


    return result


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
# SIGNAL 판정
# =========================================================

def calculate_signal(
    periods
):

    if not periods:

        return {

            "signal_pass":
                False,

            "change":
                None,

            "patterns":
                []

        }


    current = periods[-1]


    try:

        change = float(
            current["change"]
        )

    except Exception:

        change = None


    bullish = False


    try:

        bullish = (
            float(
                current["close"]
            )
            >
            float(
                current["open"]
            )
        )

    except Exception:

        bullish = False


    patterns = current.get(
        "patterns",
        []
    )


    signal_patterns = [

        p

        for p in SIGNAL_CANDLE_PATTERNS

        if p in patterns

    ]


    positive = (
        change is not None
        and
        change > 0
    )


    signal_pass = (
        positive
        and
        bullish
        and
        bool(signal_patterns)
    )


    return {

        "signal_pass":
            signal_pass,

        "change":
            change,

        "patterns":
            signal_patterns,

        "positive":
            positive,

        "bullish":
            bullish

    }


# =========================================================
# 업비트 한 종목 분석
#
# 15분 + 일봉 동시 계산
# =========================================================

def analyze_coin(
    market,
    current_price
):

    periods_15m = (
        build_upbit_timeframe(
            market,
            "15m",
            current_price
        )
    )


    periods_daily = (
        build_upbit_timeframe(
            market,
            "1d",
            current_price
        )
    )


    signal_15m = calculate_signal(
        periods_15m
    )

    signal_daily = calculate_signal(
        periods_daily
    )


    simultaneous = (

        signal_15m["signal_pass"]
        and
        signal_daily["signal_pass"]

    )


    return {

        "periods_15m":
            periods_15m,

        "periods_daily":
            periods_daily,

        "signal_15m":
            signal_15m,

        "signal_daily":
            signal_daily,

        "simultaneous_signal":
            simultaneous

    }


# =========================================================
# 표시 함수
# =========================================================

def format_change(x):

    try:

        if x is None:

            return (
                '<span class="zero">-</span>'
            )

        x = float(x)

    except Exception:

        return (
            '<span class="zero">-</span>'
        )


    if x > 0:

        return (
            '<span class="up">'
            f'▲ +{x:.2f}%'
            '</span>'
        )


    if x < 0:

        return (
            '<span class="down">'
            f'▼ {x:.2f}%'
            '</span>'
        )


    return (
        '<span class="zero">'
        '0.00%'
        '</span>'
    )


def format_market_price(
    price
):

    if price is None:

        return "-"


    try:

        price = float(price)

    except Exception:

        return "-"


    if price >= 100000000:

        return (
            f"{price / 100000000:.2f}억"
        )


    if price >= 10000:

        return (
            f"{price:,.0f}"
        )


    if price >= 1:

        return (
            f"{price:,.2f}"
        )


    return (
        f"{price:.6f}"
    )


def format_volume(v):

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


    return (
        f"{v:,.0f}"
    )


# =========================================================
# 업비트 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_15m_data
    global latest_upbit_daily_data
    global latest_upbit_update_time


    markets = get_upbit_markets()


    markets = sorted(
        markets,
        key=lambda x:
            x["volume_24h"],
        reverse=True
    )


    volume_rank_map = {

        item["market"]:
            rank

        for rank, item in enumerate(
            markets,
            1
        )

    }


    top_markets = markets[
        :TOP_N
    ]


    rows = []


    for rank, item in enumerate(
        top_markets,
        1
    ):

        market = item[
            "market"
        ]

        coin = market.replace(
            "KRW-",
            ""
        )

        price = item[
            "current_price"
        ]

        volume = item[
            "volume_24h"
        ]


        try:

            analysis = analyze_coin(
                market,
                price
            )

        except Exception as e:

            log.exception(
                f"{market} 분석 오류: {e}"
            )

            analysis = {

                "periods_15m": [],
                "periods_daily": [],

                "signal_15m": {
                    "signal_pass": False,
                    "change": None,
                    "patterns": []
                },

                "signal_daily": {
                    "signal_pass": False,
                    "change": None,
                    "patterns": []
                },

                "simultaneous_signal":
                    False

            }


        s15 = analysis[
            "signal_15m"
        ]

        sd = analysis[
            "signal_daily"
        ]


        row = {

            "rank":
                rank,

            "name":
                coin,

            "market":
                market,

            "volume_24h":
                volume,

            "volume_rank":
                volume_rank_map.get(
                    market
                ),

            "volume":
                format_volume(
                    volume
                ),

            "current_price":
                price,

            # 15분

            "periods_15m":
                analysis[
                    "periods_15m"
                ],

            "change_15m":
                s15[
                    "change"
                ],

            "signal_15m":
                s15[
                    "signal_pass"
                ],

            "signal_patterns_15m":
                s15[
                    "patterns"
                ],

            # 일봉

            "periods_daily":
                analysis[
                    "periods_daily"
                ],

            "change_daily":
                sd[
                    "change"
                ],

            "signal_daily":
                sd[
                    "signal_pass"
                ],

            "signal_patterns_daily":
                sd[
                    "patterns"
                ],

            # 동시

            "simultaneous_signal":
                analysis[
                    "simultaneous_signal"
                ]

        }


        rows.append(
            row
        )


    latest_upbit_data = rows


    latest_upbit_15m_data = [

        row

        for row in rows

        if row.get(
            "signal_15m",
            False
        )

    ]


    latest_upbit_daily_data = [

        row

        for row in rows

        if row.get(
            "signal_daily",
            False
        )

    ]


    latest_upbit_update_time = kst()


    signal_15m_count = sum(

        1

        for row in rows

        if row.get(
            "signal_15m",
            False
        )

    )


    signal_daily_count = sum(

        1

        for row in rows

        if row.get(
            "signal_daily",
            False
        )

    )


    simultaneous_count = sum(

        1

        for row in rows

        if row.get(
            "simultaneous_signal",
            False
        )

    )


    log.info(
        f"UPBIT TOP{TOP_N} | "
        f"15M SIGNAL={signal_15m_count} | "
        f"DAILY SIGNAL={signal_daily_count} | "
        f"SIMULTANEOUS={simultaneous_count}"
    )


# =========================================================
# OKX BTC 현재가
# =========================================================

def get_okx_btc_price():

    response = retry(
        requests.get,
        f"{OKX_BASE_URL}/api/v5/market/ticker",
        params={
            "instId":
                OKX_BTC_INST_ID
        },
        timeout=15
    )


    if response is None:

        return None


    try:

        payload = response.json()

    except Exception:

        return None


    if payload.get(
        "code"
    ) != "0":

        return None


    data = payload.get(
        "data",
        []
    )


    if not data:

        return None


    try:

        return float(
            data[0]["last"]
        )

    except Exception:

        return None


# =========================================================
# OKX 캔들
# =========================================================

def get_okx_candles(
    bar,
    limit=200,
    after=None
):

    params = {

        "instId":
            OKX_BTC_INST_ID,

        "bar":
            bar,

        "limit":
            str(limit)

    }


    if after is not None:

        params["after"] = str(
            after
        )


    response = retry(
        requests.get,
        f"{OKX_BASE_URL}/api/v5/market/candles",
        params=params,
        timeout=15
    )


    if response is None:

        return []


    try:

        payload = response.json()

    except Exception:

        return []


    if payload.get(
        "code"
    ) != "0":

        return []


    return payload.get(
        "data",
        []
    )


# =========================================================
# OKX History
# =========================================================

def get_okx_history(
    bar
):

    rows = []

    after = None


    for _ in range(
        MAX_HISTORY_CHUNKS
    ):

        data = get_okx_candles(
            bar,
            HISTORY_CHUNK,
            after
        )


        if not data:

            break


        rows.extend(
            data
        )


        try:

            oldest = min(
                int(row[0])
                for row in data
            )

        except Exception:

            break


        after = oldest


        if len(data) < HISTORY_CHUNK:

            break


    if not rows:

        return pd.DataFrame()


    unique = {}


    for row in rows:

        try:

            unique[
                int(row[0])
            ] = row

        except Exception:

            continue


    ordered = sorted(
        unique.values(),
        key=lambda x:
            int(x[0])
    )


    converted = []


    for row in ordered:

        try:

            converted.append({

                "timestamp":
                    int(row[0]),

                "open":
                    float(row[1]),

                "high":
                    float(row[2]),

                "low":
                    float(row[3]),

                "close":
                    float(row[4])

            })

        except Exception:

            continue


    if not converted:

        return pd.DataFrame()


    df = pd.DataFrame(
        converted
    )


    df["datetime_utc"] = (
        pd.to_datetime(
            df["timestamp"],
            unit="ms",
            utc=True
        )
    )


    df["datetime_kst"] = (
        df["datetime_utc"]
        .dt
        .tz_convert(KST)
    )


    return df


# =========================================================
# OKX 1시간 History
#
# 일봉 KST 09:00 집계용
# =========================================================

def get_okx_1h_history():

    return get_okx_history(
        "1H"
    )


# =========================================================
# OKX 일봉 집계
#
# KST 09:00 ~ 다음날 09:00
# =========================================================

def aggregate_okx_daily(
    df
):

    if df is None or df.empty:

        return pd.DataFrame()


    temp = df.copy()


    temp["daily_start"] = (

        temp["datetime_kst"]

        -

        pd.Timedelta(
            hours=9
        )

    ).dt.floor(
        "D"
    ) + pd.Timedelta(
        hours=9
    )


    return (

        temp
        .sort_values(
            "datetime_kst"
        )
        .groupby(
            "daily_start"
        )
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last")
        )
        .reset_index()

    )


# =========================================================
# OKX 기간 구성
# =========================================================

def build_okx_periods(
    price,
    timeframe,
    df
):

    if (
        price is None
        or
        df is None
        or
        df.empty
    ):

        return []


    periods = get_recent_periods(
        timeframe,
        6
    )


    result = []


    # =====================================================
    # 일봉
    # =====================================================

    if timeframe == "1d":

        daily = aggregate_okx_daily(
            df
        )


        if daily.empty:

            return []


        for period in periods:

            part = daily[
                (
                    daily["daily_start"]
                    >= period["start"]
                )
                &
                (
                    daily["daily_start"]
                    < period["end"]
                )
            ].copy()


            if part.empty:

                result.append({

                    **period,

                    "open": None,
                    "high": None,
                    "low": None,
                    "close": None,

                    "change": None,

                    "patterns": []

                })

                continue


            part = part.sort_values(
                "daily_start"
            )


            o = float(
                part.iloc[0]["open"]
            )

            h = float(
                part["high"].max()
            )

            l = float(
                part["low"].min()
            )

            c = float(
                part.iloc[-1]["close"]
            )


            if period["active"]:

                c = float(
                    price
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

                (
                    c - o
                )
                /
                o
                *
                100

                if o != 0

                else None

            )


            result.append({

                **period,

                "open": o,
                "high": h,
                "low": l,
                "close": c,

                "change":
                    change,

                "patterns":
                    []

            })


    # =====================================================
    # 15분
    # =====================================================

    else:

        for period in periods:

            part = df[
                (
                    df["datetime_kst"]
                    >= period["start"]
                )
                &
                (
                    df["datetime_kst"]
                    < period["end"]
                )
            ].copy()


            if part.empty:

                result.append({

                    **period,

                    "open": None,
                    "high": None,
                    "low": None,
                    "close": None,

                    "change": None,

                    "patterns": []

                })

                continue


            part = part.sort_values(
                "datetime_kst"
            )


            o = float(
                part.iloc[0]["open"]
            )

            h = float(
                part["high"].max()
            )

            l = float(
                part["low"].min()
            )

            c = float(
                part.iloc[-1]["close"]
            )


            if period["active"]:

                c = float(
                    price
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

                (
                    c - o
                )
                /
                o
                *
                100

                if o != 0

                else None

            )


            result.append({

                **period,

                "open": o,
                "high": h,
                "low": l,
                "close": c,

                "change":
                    change,

                "patterns":
                    []

            })


    pattern_results = detect_patterns(
        result
    )


    for i, patterns in enumerate(
        pattern_results
    ):

        result[i]["patterns"] = (
            patterns
        )


    return result


# =========================================================
# OKX BTC 업데이트
#
# 15분 + 일봉 동시
# =========================================================

def update_btc_market():

    global latest_btc_okx_price
    global latest_btc_15m_periods
    global latest_btc_daily_periods
    global latest_btc_15m_change
    global latest_btc_daily_change
    global latest_btc_15m_signal
    global latest_btc_daily_signal
    global latest_btc_simultaneous_signal


    price = get_okx_btc_price()


    if price is None:

        log.warning(
            "OKX BTC 현재가 조회 실패"
        )

        return


    latest_btc_okx_price = price


    # -----------------------------------------------------
    # 15분
    # -----------------------------------------------------

    df_15m = get_okx_history(
        "15m"
    )


    latest_btc_15m_periods = (
        build_okx_periods(
            price,
            "15m",
            df_15m
        )
    )


    # -----------------------------------------------------
    # 일봉
    # KST 09:00
    # -----------------------------------------------------

    df_1h = get_okx_1h_history()


    latest_btc_daily_periods = (
        build_okx_periods(
            price,
            "1d",
            df_1h
        )
    )


    # -----------------------------------------------------
    # 변동률
    # -----------------------------------------------------

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


    # -----------------------------------------------------
    # BTC SIGNAL
    # -----------------------------------------------------

    signal_15m = calculate_signal(
        latest_btc_15m_periods
    )

    signal_daily = calculate_signal(
        latest_btc_daily_periods
    )


    latest_btc_15m_signal = (
        signal_15m["signal_pass"]
    )

    latest_btc_daily_signal = (
        signal_daily["signal_pass"]
    )


    latest_btc_simultaneous_signal = (

        latest_btc_15m_signal
        and
        latest_btc_daily_signal

    )


    log.info(
        f"OKX BTC | "
        f"15M={latest_btc_15m_signal} | "
        f"1D={latest_btc_daily_signal} | "
        f"BOTH={latest_btc_simultaneous_signal}"
    )


# =========================================================
# OKX 기타
# =========================================================

def update_okx(
    usdt
):

    global latest_okx_data
    global latest_okx_update_time

    latest_okx_data = []

    latest_okx_update_time = kst()


# =========================================================
# USDT
# =========================================================

def get_usdt_krw():

    response = retry(
        requests.get,
        "https://api.upbit.com/v1/ticker",
        params={
            "markets":
                "KRW-USDT"
        },
        timeout=15
    )


    if response is None:

        return None


    try:

        data = response.json()

        return float(
            data[0]["trade_price"]
        )

    except Exception:

        return None


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    if not update_lock.acquire(False):

        return


    try:

        # BTC
        update_btc_market()


        # UPBIT
        if USE_UPBIT == "Y":

            update_upbit()


        # 기타 OKX
        if USE_OKX == "Y":

            usdt = get_usdt_krw()

            if usdt is not None:

                update_okx(
                    usdt
                )


    except Exception as e:

        log.exception(
            f"전체 업데이트 오류: {e}"
        )


    finally:

        update_lock.release()


# =========================================================
# 표시
# =========================================================

def candle_pattern_html(
    patterns
):

    if not patterns:

        return ""


    return (

        '<div class="candle-pattern">'

        +

        " · ".join(
            html.escape(
                str(x)
            )
            for x in patterns
        )

        +

        '</div>'

    )


# =========================================================
# 기간 셀
# =========================================================

def timeframe_cells_html(
    periods
):

    if not periods:

        return (
            '<div class="no-data">-</div>'
        )


    cells = []


    for period in periods:

        cell_class = (

            "daily-cell current-daily"

            if period.get(
                "active",
                False
            )

            else

            "daily-cell"

        )


        cells.append(

            f"""
            <div class="{cell_class}">

                <div class="daily-main">

                    <div class="daily-time">
                        {html.escape(
                            str(
                                period.get(
                                    "label",
                                    "-"
                                )
                            )
                        )}
                    </div>

                    <div class="daily-value">
                        {format_change(
                            period.get(
                                "change"
                            )
                        )}
                    </div>

                </div>

                {candle_pattern_html(
                    period.get(
                        "patterns",
                        []
                    )
                )}

            </div>
            """

        )


    return "".join(
        cells
    )


# =========================================================
# BTC 시장 시황
# =========================================================

def market_summary_html():

    price = format_market_price(
        latest_btc_okx_price
    )


    if latest_btc_simultaneous_signal:

        btc_status = (
            '<span class="btc-both">'
            '⭐ 동시 SIGNAL'
            '</span>'
        )

    elif latest_btc_15m_signal:

        btc_status = (
            '<span class="btc-on">'
            '🚀 15분 SIGNAL'
            '</span>'
        )

    elif latest_btc_daily_signal:

        btc_status = (
            '<span class="btc-daily">'
            '📅 일봉 SIGNAL'
            '</span>'
        )

    else:

        btc_status = (
            '<span class="btc-off">'
            'SIGNAL 없음'
            '</span>'
        )


    return f"""

    <div class="market-card">

        <div class="market-card-header">

            <div class="market-title-block">

                <div class="market-title-main">
                    ₿ BTC 시장 시황
                </div>

                <div class="market-title-sub">
                    OKX BTC-USDT
                    ·
                    15분 + 일봉 동시 분석
                    ·
                    일봉 KST 09:00 기준
                </div>

            </div>

            <div class="market-time">
                {kst()} KST
            </div>

        </div>


        <div class="btc-main-row">

            <div class="btc-name">
                ₿ BTC
            </div>

            <div class="btc-price">
                {price}
            </div>

            <div class="btc-change-box">

                <div>
                    15분
                    {format_change(
                        latest_btc_15m_change
                    )}
                </div>

                <div>
                    일봉
                    {format_change(
                        latest_btc_daily_change
                    )}
                </div>

            </div>


            <div class="btc-signal-box">

                {btc_status}

            </div>

        </div>


        <div class="btc-timeframe-title">
            📊 BTC 15분
        </div>

        <div class="btc-daily-grid">
            {timeframe_cells_html(
                latest_btc_15m_periods
            )}
        </div>


        <div class="btc-timeframe-title">
            📅 BTC 일봉
            <span class="base-text">
                KST 09:00
            </span>
        </div>

        <div class="btc-daily-grid">
            {timeframe_cells_html(
                latest_btc_daily_periods
            )}
        </div>

    </div>

    """


# =========================================================
# 메인 카드
# =========================================================

def get_main_row_class(
    row
):

    change = row.get(
        "change_15m"
    )


    try:

        change = float(
            change
        )

    except Exception:

        change = None


    if change is not None and change > 0:

        return (
            "unified-main-row "
            "daily-positive"
        )


    if change is not None and change < 0:

        return (
            "unified-main-row "
            "daily-negative"
        )


    return (
        "unified-main-row "
        "daily-zero"
    )


# =========================================================
# SIGNAL 카드
# =========================================================

def signal_card_html(
    row,
    signal_type,
    rank
):

    coin = html.escape(
        str(
            row.get(
                "name",
                "-"
            )
        )
    )


    if signal_type == "15m":

        title = "🚀 15분 SIGNAL"

        change = row.get(
            "change_15m"
        )

        patterns = row.get(
            "signal_patterns_15m",
            []
        )

        periods = row.get(
            "periods_15m",
            []
        )

        border = "signal-15m-card"

        badge = ""


    elif signal_type == "both":

        title = "⭐ 15분 + 일봉 동시 SIGNAL"

        change = row.get(
            "change_15m"
        )

        patterns = []

        patterns.extend(
            row.get(
                "signal_patterns_15m",
                []
            )
        )

        patterns.extend(
            row.get(
                "signal_patterns_daily",
                []
            )
        )

        patterns = list(
            dict.fromkeys(
                patterns
            )
        )

        periods = row.get(
            "periods_15m",
            []
        )

        border = "signal-both-card"

        badge = (
            '<span class="both-badge">'
            '15분 ✓ 일봉 ✓'
            '</span>'
        )


    else:

        title = "📅 일봉 SIGNAL"

        change = row.get(
            "change_daily"
        )

        patterns = row.get(
            "signal_patterns_daily",
            []
        )

        periods = row.get(
            "periods_daily",
            []
        )

        border = "signal-daily-card"

        badge = ""


    pattern_text = (

        " / ".join(
            patterns
        )

        if patterns

        else "-"

    )


    return f"""

    <div class="unified-market-card {border}">

        <div class="unified-card-header">

            <div class="unified-card-rank">
                #{rank}
            </div>

            <div class="unified-card-coin">
                {coin}
            </div>

            <div class="unified-card-title">
                {title}
            </div>

            {badge}

        </div>


        <div class="{get_main_row_class(row)}">

            <div class="unified-main-item">

                <div class="unified-label">
                    현재가
                </div>

                <div class="unified-price">
                    {format_market_price(
                        row.get(
                            "current_price"
                        )
                    )}
                </div>

            </div>


            <div class="unified-main-item">

                <div class="unified-label">
                    24H 거래대금
                </div>

                <div class="unified-volume">
                    {row.get(
                        "volume",
                        "-"
                    )}
                </div>

            </div>


            <div class="unified-main-item">

                <div class="unified-label">

                    현재 변동

                </div>

                <div class="unified-daily">

                    {format_change(
                        change
                    )}

                </div>

            </div>

        </div>


        <div class="timeframe-card-title">

            최근 6개
            ·
            {title}

        </div>


        <div class="unified-daily-grid">

            {timeframe_cells_html(
                periods
            )}

        </div>


        <div class="unified-condition-row">

            <div class="unified-condition">

                <div class="condition-label">
                    SIGNAL 패턴
                </div>

                <div class="condition-value">
                    {html.escape(
                        pattern_text
                    )}
                </div>

            </div>

        </div>

    </div>

    """


# =========================================================
# SIGNAL 섹션
# =========================================================

def signal_section(
    data,
    signal_type
):

    if signal_type == "15m":

        title = "🚀 15분 SIGNAL"

        subtitle = (
            "현재 15분봉 양수"
            " · 현재봉 양봉"
            " · 상승장악 계열"
        )

        rows = [

            row for row in data

            if row.get(
                "signal_15m",
                False
            )

        ]


    elif signal_type == "both":

        title = (
            "⭐ 15분 + 일봉 동시 SIGNAL"
        )

        subtitle = (
            "15분 SIGNAL"
            " + "
            "일봉 SIGNAL"
            " 동시 발생"
        )

        rows = [

            row for row in data

            if row.get(
                "simultaneous_signal",
                False
            )

        ]


    else:

        title = "📅 일봉 SIGNAL"

        subtitle = (
            "현재 일봉 양수"
            " · 현재봉 양봉"
            " · 상승장악 계열"
        )

        rows = [

            row for row in data

            if row.get(
                "signal_daily",
                False
            )

        ]


    rows.sort(

        key=lambda row:
            row.get(
                "volume_rank",
                float("inf")
            )

    )


    if not rows:

        body = f"""

        <div class="signal-empty-card">

            <div class="signal-empty-icon">
                🔎
            </div>

            <div class="signal-empty-title">
                현재 {title} 없음
            </div>

            <div class="signal-empty-text">
                {subtitle}
            </div>

        </div>

        """

    else:

        cards = []


        for index, row in enumerate(
            rows
        ):

            cards.append(
                signal_card_html(
                    row,
                    signal_type,
                    index + 1
                )
            )


        body = "".join(
            cards
        )


    if signal_type == "15m":

        number_class = (
            "signal-section-number"
        )

    elif signal_type == "both":

        number_class = (
            "both-section-number"
        )

    else:

        number_class = (
            "daily-section-number"
        )


    return f"""

    <div class="unified-section">

        <div class="section-title-card">

            <div class="section-number {number_class}">
                {title.split(" ")[0]}
            </div>

            <div class="section-heading">

                <div class="section-heading-main">
                    {html.escape(title)}
                </div>

                <div class="section-heading-sub">
                    {html.escape(subtitle)}
                </div>

            </div>

        </div>


        <div class="signal-card-list">

            {body}

        </div>

    </div>

    """


# =========================================================
# TOP10
# =========================================================

def top_section(
    data,
    update_time
):

    if not data:

        cards = """

        <div class="empty-card">
            현재 데이터 없음
        </div>

        """

    else:

        top_cards = []


        for row in data:

            top_cards.append(

                f"""

                <div class="unified-market-card">

                    <div class="unified-card-header">

                        <div class="unified-card-rank">
                            #{row.get("rank","-")}
                        </div>

                        <div class="unified-card-coin">
                            {html.escape(
                                str(
                                    row.get(
                                        "name",
                                        "-"
                                    )
                                )
                            )}
                        </div>

                        <div class="unified-card-title">
                            🏆 업비트 TOP
                        </div>

                    </div>


                    <div class="{get_main_row_class(row)}">

                        <div class="unified-main-item">

                            <div class="unified-label">
                                현재가
                            </div>

                            <div class="unified-price">
                                {format_market_price(
                                    row.get(
                                        "current_price"
                                    )
                                )}
                            </div>

                        </div>


                        <div class="unified-main-item">

                            <div class="unified-label">
                                24H 거래대금
                            </div>

                            <div class="unified-volume">
                                {row.get(
                                    "volume",
                                    "-"
                                )}
                            </div>

                        </div>


                        <div class="unified-main-item">

                            <div class="unified-label">
                                15분
                            </div>

                            <div class="unified-daily">
                                {format_change(
                                    row.get(
                                        "change_15m"
                                    )
                                )}
                            </div>

                        </div>

                    </div>


                    <div class="timeframe-card-title">
                        📊 15분
                    </div>

                    <div class="unified-daily-grid">
                        {timeframe_cells_html(
                            row.get(
                                "periods_15m",
                                []
                            )
                        )}
                    </div>


                    <div class="timeframe-card-title">
                        📅 일봉
                    </div>

                    <div class="unified-daily-grid">
                        {timeframe_cells_html(
                            row.get(
                                "periods_daily",
                                []
                            )
                        )}
                    </div>


                    <div class="top-signal-status">

                        <span>
                            15분:
                        </span>

                        {
                            "🚀 SIGNAL"
                            if row.get(
                                "signal_15m",
                                False
                            )
                            else
                            "—"
                        }

                        <span>
                            일봉:
                        </span>

                        {
                            "📅 SIGNAL"
                            if row.get(
                                "signal_daily",
                                False
                            )
                            else
                            "—"
                        }

                        {
                            '<span class="both-inline">'
                            '⭐ 동시 SIGNAL'
                            '</span>'
                            if row.get(
                                "simultaneous_signal",
                                False
                            )
                            else ""
                        }

                    </div>

                </div>

                """

            )


        cards = "".join(
            top_cards
        )


    return f"""

    <div class="unified-section">

        <div class="section-title-card">

            <div class="section-number top-number">
                🏆
            </div>

            <div class="section-heading">

                <div class="section-heading-main">
                    업비트 TOP{TOP_N}
                </div>

                <div class="section-heading-sub">
                    거래대금 순위
                    ·
                    15분 + 일봉 동시 표시
                </div>

            </div>

        </div>


        <div class="top-update-bar">

            <span>
                TOP {TOP_N}
            </span>

            <span>
                업데이트 {update_time}
            </span>

        </div>


        <div class="top-card-list">

            {cards}

        </div>

    </div>

    """


# =========================================================
# CSS
# =========================================================

CSS = """

*{
    box-sizing:border-box;
    -webkit-tap-highlight-color:transparent;
}

html,
body{
    margin:0;
    padding:0;
    width:100%;
    overflow-x:hidden;
}

body{
    background:#080c11;
    color:#e7ebef;
    font-family:
        -apple-system,
        BlinkMacSystemFont,
        "Segoe UI",
        Arial,
        sans-serif;
    font-size:10px;
    padding:8px;
}

h1{
    margin:3px 4px 9px;
    color:#eef2f5;
    font-size:14px;
    line-height:18px;
    font-weight:900;
}


/* =========================================================
   SECTION
   ========================================================= */

.unified-section{
    width:100%;
    margin:8px 0 10px;
}

.section-title-card{
    display:flex;
    align-items:center;
    width:100%;
    min-height:44px;
    padding:6px 8px;
    background:#10151b;
    border:2px solid #252e38;
    border-radius:11px;
}

.section-number{
    flex:none;
    display:flex;
    align-items:center;
    justify-content:center;
    width:32px;
    height:29px;
    margin-right:7px;
    border-radius:7px;
    background:#18251f;
    border:1px solid #315a48;
    font-size:14px;
    font-weight:900;
}

.signal-section-number{
    background:#2a2413;
    border-color:#d4af37;
}

.both-section-number{
    background:#211a2d;
    border-color:#9b72d1;
}

.daily-section-number{
    background:#152238;
    border-color:#4e79ae;
}

.top-number{
    background:#1d1a13;
    border-color:#665331;
}

.section-heading{
    min-width:0;
    flex:1;
}

.section-heading-main{
    color:#e9edf1;
    font-size:11px;
    line-height:14px;
    font-weight:900;
}

.section-heading-sub{
    margin-top:1px;
    color:#7d8993;
    font-size:6.5px;
    line-height:8px;
    font-weight:700;
}


/* =========================================================
   BTC
   ========================================================= */

.market-card{
    width:100%;
    margin:3px 0 10px;
    background:#0f141a;
    border:2px solid #252e38;
    border-radius:12px;
    overflow:hidden;
}

.market-card-header{
    display:flex;
    align-items:center;
    min-height:38px;
    padding:5px 8px;
    background:#121820;
    border-bottom:1px solid #29323c;
}

.market-title-block{
    min-width:0;
    flex:1;
}

.market-title-main{
    color:#eef2f5;
    font-size:10px;
    font-weight:900;
}

.market-title-sub{
    margin-top:1px;
    color:#7e8994;
    font-size:6px;
    font-weight:700;
}

.market-time{
    flex:none;
    margin-left:6px;
    color:#68737e;
    font-size:6px;
    font-weight:800;
}

.btc-main-row{
    display:grid;
    grid-template-columns:
        .8fr 1.2fr 1.3fr 1.4fr;
    align-items:center;
    min-height:51px;
    background:#11161c;
}

.btc-name{
    padding-left:9px;
    color:#edf1f4;
    font-size:10px;
    font-weight:900;
}

.btc-price{
    color:#f1f4f6;
    font-size:10px;
    font-weight:900;
    text-align:center;
}

.btc-change-box{
    display:flex;
    flex-direction:column;
    align-items:center;
    gap:2px;
    font-size:7px;
    font-weight:900;
}

.btc-signal-box{
    min-height:51px;
    display:flex;
    align-items:center;
    justify-content:center;
    border-left:1px solid #29323c;
    text-align:center;
}

.btc-on{
    color:#78cfa2;
    font-size:7px;
    font-weight:900;
}

.btc-daily{
    color:#6ea5dd;
    font-size:7px;
    font-weight:900;
}

.btc-both{
    color:#e6c65c;
    font-size:8px;
    font-weight:900;
}

.btc-off{
    color:#df8588;
    font-size:7px;
    font-weight:900;
}

.base-text{
    margin-left:5px;
    color:#65717c;
    font-size:5px;
}

.btc-timeframe-title{
    min-height:24px;
    display:flex;
    align-items:center;
    padding:3px 8px;
    background:#111820;
    border-top:1px solid #29323c;
    color:#aeb8c0;
    font-size:7px;
    font-weight:900;
}

.btc-daily-grid,
.unified-daily-grid{
    display:grid;
    grid-template-columns:
        repeat(6,1fr);
    gap:1px;
    background:#29323c;
    border-top:1px solid #29323c;
}

.daily-cell{
    display:flex;
    flex-direction:column;
    align-items:center;
    justify-content:center;
    min-height:42px;
    background:#0d1319;
    gap:1px;
    padding:2px;
}

.daily-main{
    display:flex;
    align-items:center;
    justify-content:center;
    gap:2px;
}

.daily-time{
    color:#b6bec5;
    font-size:6px;
    font-weight:900;
    white-space:nowrap;
}

.daily-value{
    font-size:7px;
    font-weight:900;
    white-space:nowrap;
}

.candle-pattern{
    color:#e0bd6d;
    font-size:5px;
    line-height:6px;
    font-weight:900;
    text-align:center;
    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;
    max-width:100%;
}

.current-daily{
    background:#173326 !important;
}


/* =========================================================
   CARD
   ========================================================= */

.top-card-list,
.signal-card-list{
    display:flex;
    flex-direction:column;
    gap:6px;
}

.unified-market-card{
    width:100%;
    background:#0f141a;
    border:2px solid #252e38;
    border-radius:11px;
    overflow:hidden;
}

.signal-15m-card{
    border-color:#d4af37;
}

.signal-both-card{
    border-color:#9b72d1;
    box-shadow:
        0 0 8px
        rgba(155,114,209,0.18);
}

.signal-daily-card{
    border-color:#4e79ae;
}

.unified-card-header{
    display:flex;
    align-items:center;
    min-height:36px;
    padding:5px 8px;
    background:#121820;
    border-bottom:1px solid #29323c;
}

.signal-15m-card .unified-card-header{
    background:#211d11;
    border-bottom-color:#d4af37;
}

.signal-both-card .unified-card-header{
    background:#1d1727;
    border-bottom-color:#9b72d1;
}

.signal-daily-card .unified-card-header{
    background:#111b2a;
    border-bottom-color:#4e79ae;
}

.unified-card-rank{
    width:31px;
    flex:none;
    color:#e0bd6d;
    font-size:8px;
    font-weight:900;
}

.unified-card-coin{
    flex:1;
    min-width:0;
    color:#eef2f5;
    font-size:10px;
    font-weight:900;
}

.unified-card-title{
    flex:none;
    color:#8a949e;
    font-size:6px;
    font-weight:900;
    margin-right:5px;
}

.both-badge{
    padding:3px 5px;
    border-radius:5px;
    background:#291d3a;
    border:1px solid #9b72d1;
    color:#d4b8f0;
    font-size:5px;
    font-weight:900;
}


/* =========================================================
   MAIN ROW
   ========================================================= */

.unified-main-row{
    display:grid;
    grid-template-columns:
        repeat(3,1fr);
    min-height:43px;
}

.unified-main-item{
    display:flex;
    flex-direction:column;
    align-items:center;
    justify-content:center;
    gap:2px;
    padding:2px;
}

.unified-main-item + .unified-main-item{
    border-left:1px solid #29323c;
}

.unified-label{
    color:#68747e;
    font-size:5.5px;
    font-weight:800;
}

.unified-price{
    color:#f1f4f6;
    font-size:9px;
    font-weight:900;
}

.unified-volume{
    color:#cfd6dc;
    font-size:8px;
    font-weight:900;
}

.unified-daily{
    font-size:8px;
    font-weight:900;
}

.daily-positive .unified-main-item{
    background:#10271c;
}

.daily-negative .unified-main-item{
    background:#2a1518;
}

.daily-zero .unified-main-item{
    background:#15191d;
}

.timeframe-card-title{
    min-height:23px;
    display:flex;
    align-items:center;
    padding:3px 7px;
    background:#111820;
    border-top:1px solid #29323c;
    color:#9da8b1;
    font-size:6.5px;
    font-weight:900;
}


/* =========================================================
   SIGNAL
   ========================================================= */

.unified-condition-row{
    width:100%;
    min-height:39px;
    background:#0f171d;
    border-top:1px solid #29323c;
}

.unified-condition{
    display:flex;
    flex-direction:column;
    align-items:center;
    justify-content:center;
    min-height:39px;
    gap:2px;
    padding:4px 6px;
}

.condition-label{
    color:#7e8994;
    font-size:5px;
    font-weight:800;
}

.condition-value{
    color:#e0bd6d;
    font-size:7px;
    font-weight:900;
    text-align:center;
}

.top-signal-status{
    min-height:27px;
    display:flex;
    align-items:center;
    justify-content:center;
    gap:5px;
    background:#0d1319;
    border-top:1px solid #29323c;
    color:#77828c;
    font-size:6px;
    font-weight:900;
}

.both-inline{
    color:#d4b8f0;
    font-weight:900;
}


/* =========================================================
   EMPTY
   ========================================================= */

.signal-empty-card{
    display:flex;
    flex-direction:column;
    align-items:center;
    justify-content:center;
    min-height:90px;
    margin-top:6px;
    background:#18150e;
    border:2px solid #8f7728;
    border-radius:10px;
    text-align:center;
    padding:10px;
}

.signal-empty-icon{
    font-size:18px;
}

.signal-empty-title{
    color:#f0cf67;
    font-size:9px;
    font-weight:900;
}

.signal-empty-text{
    margin-top:5px;
    color:#a58d4a;
    font-size:6px;
    font-weight:800;
}


/* =========================================================
   COLOR
   ========================================================= */

.up{
    color:#78cfa2 !important;
    font-weight:900;
}

.down{
    color:#df8588 !important;
    font-weight:900;
}

.zero{
    color:#727c86 !important;
    font-weight:900;
}

.empty-card{
    min-height:60px;
    display:flex;
    align-items:center;
    justify-content:center;
    background:#10151b;
    border:1px solid #252e38;
    border-radius:10px;
    color:#59636e;
    font-size:8px;
    font-weight:800;
}

.top-update-bar{
    display:flex;
    justify-content:space-between;
    padding:4px 6px;
    color:#68737e;
    font-size:5.5px;
    font-weight:800;
}


/* =========================================================
   MOBILE
   ========================================================= */

@media(max-width:600px){

    body{
        padding:5px;
    }

    h1{
        margin:2px 3px 7px;
        font-size:11px;
    }

    .section-title-card{
        min-height:37px;
        padding:4px 6px;
        border-radius:8px;
    }

    .section-number{
        width:26px;
        height:25px;
        margin-right:5px;
        font-size:11px;
    }

    .section-heading-main{
        font-size:8px;
        line-height:10px;
    }

    .section-heading-sub{
        font-size:4.5px;
        line-height:6px;
    }

    .market-card{
        border-radius:8px;
    }

    .market-card-header{
        min-height:32px;
        padding:4px 6px;
    }

    .market-title-main{
        font-size:7px;
    }

    .market-title-sub{
        font-size:4px;
    }

    .market-time{
        font-size:4px;
    }

    .btc-main-row{
        min-height:38px;
    }

    .btc-name{
        padding-left:6px;
        font-size:7px;
    }

    .btc-price{
        font-size:7px;
    }

    .btc-change-box{
        font-size:5px;
    }

    .btc-signal-box{
        min-height:38px;
    }

    .btc-on,
    .btc-daily,
    .btc-off{
        font-size:5px;
    }

    .btc-both{
        font-size:6px;
    }

    .btc-timeframe-title{
        min-height:21px;
        font-size:5.5px;
    }

    .btc-daily-grid,
    .unified-daily-grid{
        grid-template-columns:
            repeat(3,1fr);
    }

    .daily-cell{
        min-height:43px;
    }

    .daily-time{
        font-size:4.5px;
    }

    .daily-value{
        font-size:5.5px;
    }

    .candle-pattern{
        font-size:3.8px;
    }

    .unified-card-header{
        min-height:30px;
        padding:4px 6px;
    }

    .unified-card-rank{
        width:24px;
        font-size:6px;
    }

    .unified-card-coin{
        font-size:7px;
    }

    .unified-card-title{
        font-size:4.5px;
    }

    .both-badge{
        font-size:4px;
    }

    .unified-main-row{
        min-height:37px;
    }

    .unified-label{
        font-size:4px;
    }

    .unified-price,
    .unified-volume,
    .unified-daily{
        font-size:6px;
    }

    .timeframe-card-title{
        min-height:20px;
        font-size:5px;
    }

    .unified-condition-row{
        min-height:34px;
    }

    .unified-condition{
        min-height:34px;
    }

    .condition-label{
        font-size:4px;
    }

    .condition-value{
        font-size:5.5px;
    }

    .top-signal-status{
        min-height:24px;
        font-size:4.5px;
    }

}


/* =========================================================
   VERY SMALL
   ========================================================= */

@media(max-width:380px){

    body{
        padding:4px;
    }

    .btc-main-row{
        min-height:34px;
    }

    .btc-name,
    .btc-price{
        font-size:6px;
    }

    .btc-change-box{
        font-size:4px;
    }

    .btc-signal-box{
        min-height:34px;
    }

    .btc-on,
    .btc-daily,
    .btc-off{
        font-size:4px;
    }

    .btc-both{
        font-size:5px;
    }

    .daily-cell{
        min-height:39px;
    }

}


/* =========================================================
   END CSS
   =========================================================
"""


# =========================================================
# Dashboard
#
# 화면 순서
#
# 1. BTC
# 2. 15분 SIGNAL
# 3. 15분 + 일봉 동시 SIGNAL
# 4. 일봉 SIGNAL
# 5. 업비트 TOP10
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def dashboard():

    content = ""


    if USE_UPBIT == "Y":

        # -------------------------------------------------
        # 15분
        # -------------------------------------------------

        content += signal_section(
            latest_upbit_data,
            "15m"
        )


        # -------------------------------------------------
        # 동시
        # -------------------------------------------------

        content += signal_section(
            latest_upbit_data,
            "both"
        )


        # -------------------------------------------------
        # 일봉
        # -------------------------------------------------

        content += signal_section(
            latest_upbit_data,
            "1d"
        )


        # -------------------------------------------------
        # TOP10
        # -------------------------------------------------

        content += top_section(
            latest_upbit_data,
            latest_upbit_update_time
        )


    return f"""

    <!DOCTYPE html>

    <html lang="ko">

    <head>

        <meta charset="UTF-8">

        <meta
            name="viewport"
            content="
                width=device-width,
                initial-scale=1,
                maximum-scale=1,
                user-scalable=no
            "
        >

        <meta
            http-equiv="refresh"
            content="60"
        >

        <meta
            name="theme-color"
            content="#080c11"
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


        {market_summary_html()}


        {content}


    </body>

    </html>

    """


# =========================================================
# Scheduler
# =========================================================

def scheduler():

    log.info(
        "스케줄러 시작"
    )


    while True:

        try:

            schedule.run_pending()

        except Exception as e:

            log.exception(
                f"스케줄러 오류: {e}"
            )

        time.sleep(1)


# =========================================================
# 설정 확인
# =========================================================

def validate_settings():

    if TOP_N < 1:

        raise ValueError(
            "TOP_N은 1 이상이어야 합니다."
        )


    if UPDATE_MINUTES < 1:

        raise ValueError(
            "UPDATE_MINUTES는 1 이상이어야 합니다."
        )


# =========================================================
# Startup
# =========================================================

@app.on_event("startup")
def startup():

    validate_settings()


    log.info(
        "========================================"
    )

    log.info(
        "TRADING SIGNAL SYSTEM START"
    )

    log.info(
        f"UPBIT TOP = {TOP_N}"
    )

    log.info(
        "타임프레임 = 15분 + 일봉 동시"
    )

    log.info(
        "일봉 기준 = KST 09:00"
    )

    log.info(
        "OKX BTC = 15분 + 일봉 동시"
    )

    log.info(
        "화면 순서 = "
        "BTC → 15분 SIGNAL → "
        "동시 SIGNAL → 일봉 SIGNAL → TOP10"
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
    )
