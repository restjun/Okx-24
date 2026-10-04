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
# 최종:
# 15분 / 일봉만 사용
# =========================================================

TIMEFRAME_OPTIONS = {

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


# 기본 타임프레임
SELECTED_TIMEFRAME = "1d"

last_updated_timeframe = None


# =========================================================
# 전역 데이터
# =========================================================

latest_upbit_data = []

latest_okx_data = []

latest_upbit_update_time = "-"
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

latest_btc_daily_change = None
latest_btc_daily_periods = []

latest_btc_current_daily_change = None
latest_btc_current_daily_label = "-"

latest_btc_timeframe = None


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
# 타임프레임
# =========================================================

def get_selected_timeframe():

    return TIMEFRAME_OPTIONS.get(
        SELECTED_TIMEFRAME,
        TIMEFRAME_OPTIONS["1d"]
    )


def get_selected_timeframe_label():

    return get_selected_timeframe()["label"]


def get_timeframe_delta():

    if SELECTED_TIMEFRAME == "15m":

        return timedelta(
            minutes=15
        )

    return timedelta(
        days=1
    )


# =========================================================
# 일봉 시작
# KST 09:00
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
# 현재 선택 시간봉 시작
# =========================================================

def get_current_timeframe_start():

    now = datetime.now(KST)

    # -----------------------------------------------------
    # 일봉
    # -----------------------------------------------------

    if SELECTED_TIMEFRAME == "1d":

        return get_current_daily_start()


    # -----------------------------------------------------
    # 15분
    # -----------------------------------------------------

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

def make_timeframe_period(
    start,
    active=False
):

    start = start.astimezone(KST)

    if SELECTED_TIMEFRAME == "1d":

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
# 최근 6개 기간
# =========================================================

def get_recent_timeframe_periods(
    count=6
):

    current_start = (
        get_current_timeframe_start()
    )

    delta = (
        get_timeframe_delta()
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
            make_timeframe_period(
                start,
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
# 업비트 캔들
# =========================================================

def get_upbit_daily_candles(
    market,
    count=10
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


def get_upbit_timeframe_candles(
    market,
    count=200
):

    unit = (
        get_selected_timeframe()
        ["upbit_unit"]
    )


    if unit == "day":

        return get_upbit_daily_candles(
            market,
            count
        )


    response = retry(
        requests.get,
        f"https://api.upbit.com/v1/candles/minutes/{unit}",
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
# 1캔들
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
# 2캔들
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
# 3캔들
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
# 4캔들
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

def detect_daily_patterns(
    periods
):

    result = []


    for i, period in enumerate(
        periods
    ):

        patterns = []


        if any(
            period.get(x) is None
            for x in [
                "open",
                "high",
                "low",
                "close"
            ]
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
                for x in [
                    "open",
                    "high",
                    "low",
                    "close"
                ]
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

            c3 = periods[i]


            if all(
                x.get(key) is not None
                for x in [
                    c1,
                    c2,
                    c3
                ]
                for key in [
                    "open",
                    "high",
                    "low",
                    "close"
                ]
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

            c4 = periods[i]


            if all(
                x.get(key) is not None
                for x in [
                    c1,
                    c2,
                    c3,
                    c4
                ]
                for key in [
                    "open",
                    "high",
                    "low",
                    "close"
                ]
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
# 업비트 타임프레임 구성
# =========================================================

def build_upbit_timeframe_candles(
    market,
    current_price=None
):

    candles = (
        get_upbit_timeframe_candles(
            market,
            200
        )
    )


    if not candles:

        return []


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

        return []


    df = pd.DataFrame(
        rows
    )


    df = (
        df
        .sort_values(
            "datetime"
        )
        .drop_duplicates(
            "datetime"
        )
    )


    periods = (
        get_recent_timeframe_periods(
            6
        )
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
        detect_daily_patterns(
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
# 업비트 분석
# =========================================================

def analyze(
    market,
    current_price
):

    periods = (
        build_upbit_timeframe_candles(
            market,
            current_price
        )
    )


    current_change = None
    previous_change = None


    if periods:

        current_change = (
            periods[-1].get(
                "change"
            )
        )


        if len(periods) >= 2:

            previous_change = (
                periods[-2].get(
                    "change"
                )
            )


    return {

        "daily_change":
            current_change,

        "daily_periods":
            periods,

        "current_daily_change":
            current_change,

        "previous_daily_change":
            previous_change

    }


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
# SIGNAL 조건
# =========================================================

def calculate_signal_conditions(
    row
):

    current_change = (
        row.get(
            "current_daily_change"
        )
    )


    positive_condition = (

        current_change is not None
        and
        current_change > 0

    )


    periods = row.get(
        "daily_periods",
        []
    )


    current_candle = (
        periods[-1]
        if periods
        else None
    )


    bullish_condition = False


    if current_candle:

        try:

            bullish_condition = (

                float(
                    current_candle["close"]
                )
                >
                float(
                    current_candle["open"]
                )

            )

        except Exception:

            bullish_condition = False


    current_patterns = []


    if current_candle:

        current_patterns = (
            current_candle.get(
                "patterns",
                []
            )
        )


    signal_patterns = [

        pattern

        for pattern
        in SIGNAL_CANDLE_PATTERNS

        if pattern in current_patterns

    ]


    pattern_condition = bool(
        signal_patterns
    )


    signal_pass = (

        positive_condition
        and
        bullish_condition
        and
        pattern_condition

    )


    row["signal_pass"] = (
        signal_pass
    )

    row["signal_conditions"] = {

        "current_positive":
            positive_condition,

        "current_bullish":
            bullish_condition,

        "current_signal_pattern":
            pattern_condition,

        "current_signal_patterns":
            signal_patterns

    }


    row["daily_signal_pass"] = (
        signal_pass
    )

    row["daily_signal_conditions"] = (
        row["signal_conditions"]
    )


    return row


# =========================================================
# 표시
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
# Row 생성
# =========================================================

def make_row(
    rank,
    name,
    volume,
    analysis,
    current_price,
    volume_rank=None
):

    analysis = analysis or {}


    return {

        "rank":
            rank,

        "name":
            name,

        "volume_24h":
            volume,

        "volume_rank":
            volume_rank,

        "volume":
            format_volume(
                volume
            ),

        "current_price":
            current_price,

        "daily_change":
            analysis.get(
                "daily_change"
            ),

        "daily_html":
            format_change(
                analysis.get(
                    "daily_change"
                )
            ),

        "daily_periods":
            analysis.get(
                "daily_periods",
                []
            ),

        "current_daily_change":
            analysis.get(
                "current_daily_change"
            ),

        "previous_daily_change":
            analysis.get(
                "previous_daily_change"
            )

    }


# =========================================================
# 업비트 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update_time
    global last_updated_timeframe


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

            analysis = analyze(
                market,
                price
            )

        except Exception as e:

            log.exception(
                f"{market} 분석 오류: {e}"
            )

            analysis = {}


        row = make_row(
            rank,
            coin,
            volume,
            analysis,
            price,
            volume_rank_map.get(
                market
            )
        )


        row = (
            calculate_signal_conditions(
                row
            )
        )


        rows.append(
            row
        )


    latest_upbit_data = rows

    latest_upbit_update_time = kst()

    last_updated_timeframe = (
        SELECTED_TIMEFRAME
    )


    signal_count = sum(

        1

        for row in rows

        if row.get(
            "signal_pass",
            False
        )

    )


    log.info(
        f"UPBIT TOP{TOP_N} | "
        f"타임프레임="
        f"{get_selected_timeframe_label()} | "
        f"SIGNAL={signal_count}"
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
# OKX 15분
# =========================================================

def get_okx_btc_timeframe_candles(
    limit=200,
    after=None
):

    bar = (
        get_selected_timeframe()
        ["okx_bar"]
    )


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
# OKX 타임프레임 History
# =========================================================

def get_okx_btc_timeframe_history():

    rows = []

    after = None


    for _ in range(
        MAX_HISTORY_CHUNKS
    ):

        data = (
            get_okx_btc_timeframe_candles(
                HISTORY_CHUNK,
                after
            )
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
# OKX 일봉용 1시간 데이터
#
# 일봉은 OKX 1D를 직접 사용하지 않고
# KST 09:00 기준으로 집계
# =========================================================

def get_okx_btc_1h_history_for_daily():

    rows = []

    after = None


    for _ in range(
        MAX_HISTORY_CHUNKS
    ):

        params = {

            "instId":
                OKX_BTC_INST_ID,

            "bar":
                "1H",

            "limit":
                str(HISTORY_CHUNK)

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

            break


        try:

            payload = response.json()

        except Exception:

            break


        if payload.get(
            "code"
        ) != "0":

            break


        data = payload.get(
            "data",
            []
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
# OKX 1시간 → KST 09:00 일봉
# =========================================================

def aggregate_btc_daily(
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
# OKX BTC 기간 구성
# =========================================================

def build_btc_timeframe(
    price,
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


    # =====================================================
    # 일봉
    # =====================================================

    if SELECTED_TIMEFRAME == "1d":

        daily = aggregate_btc_daily(
            df
        )


        if daily.empty:

            return []


        periods = (
            get_recent_timeframe_periods(
                6
            )
        )


        result = []


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

                close_price = float(
                    price
                )

                high_price = max(
                    high_price,
                    close_price
                )

                low_price = min(
                    low_price,
                    close_price
                )


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


    # =====================================================
    # 15분
    # =====================================================

    else:

        periods = (
            get_recent_timeframe_periods(
                6
            )
        )


        result = []


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

                close_price = float(
                    price
                )

                high_price = max(
                    high_price,
                    close_price
                )

                low_price = min(
                    low_price,
                    close_price
                )


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


    patterns = (
        detect_daily_patterns(
            result
        )
    )


    for i, pattern_list in enumerate(
        patterns
    ):

        result[i]["patterns"] = (
            pattern_list
        )


    return result


# =========================================================
# BTC 변동률
# =========================================================

def get_btc_timeframe_change(
    periods
):

    if not periods:

        return None


    current = periods[-1]


    try:

        open_price = float(
            current["open"]
        )

        close_price = float(
            current["close"]
        )

    except Exception:

        return None


    if open_price == 0:

        return None


    return (

        (
            close_price -
            open_price
        )
        /
        open_price
        *
        100

    )


# =========================================================
# OKX BTC 업데이트
#
# 중요:
# SELECTED_TIMEFRAME을 여기서 변경하지 않는다.
# =========================================================

def update_btc_market():

    global latest_btc_okx_price
    global latest_btc_daily_change
    global latest_btc_daily_periods
    global latest_btc_current_daily_change
    global latest_btc_current_daily_label
    global latest_btc_timeframe


    price = (
        get_okx_btc_price()
    )


    if price is None:

        return


    latest_btc_okx_price = price


    # -----------------------------------------------------
    # 일봉
    # -----------------------------------------------------

    if SELECTED_TIMEFRAME == "1d":

        df = (
            get_okx_btc_1h_history_for_daily()
        )


    # -----------------------------------------------------
    # 15분
    # -----------------------------------------------------

    else:

        df = (
            get_okx_btc_timeframe_history()
        )


    periods = build_btc_timeframe(
        price,
        df
    )


    latest_btc_daily_periods = (
        periods
    )


    latest_btc_daily_change = (
        get_btc_timeframe_change(
            periods
        )
    )


    if periods:

        latest_btc_current_daily_label = (
            periods[-1].get(
                "display_label",
                "-"
            )
        )

        latest_btc_current_daily_change = (
            periods[-1].get(
                "change"
            )
        )

    else:

        latest_btc_current_daily_label = "-"
        latest_btc_current_daily_change = None


    latest_btc_timeframe = (
        SELECTED_TIMEFRAME
    )


    log.info(
        f"OKX BTC | "
        f"{get_selected_timeframe_label()} | "
        f"{price:,.2f}"
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

        update_btc_market()


        if USE_UPBIT == "Y":

            update_upbit()


        if USE_OKX == "Y":

            usdt = (
                get_usdt_krw()
            )

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
# 타임프레임 버튼
# =========================================================

def timeframe_selector_html():

    buttons = []


    for key, info in (
        TIMEFRAME_OPTIONS.items()
    ):

        active_class = (

            "tf-button active"

            if key == SELECTED_TIMEFRAME

            else

            "tf-button"

        )


        buttons.append(

            f"""
            <a
                class="{active_class}"
                href="/?tf={key}"
            >
                {html.escape(
                    info["label"]
                )}
            </a>
            """

        )


    return f"""

    <div class="timeframe-selector">

        <div class="timeframe-selector-title">
            TIMEFRAME
        </div>

        <div class="timeframe-buttons">

            {"".join(buttons)}

        </div>

    </div>

    """


# =========================================================
# 캔들 패턴 표시
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
# BTC HTML
# =========================================================

def market_summary_html():

    timeframe_label = (
        get_selected_timeframe_label()
    )


    price = format_market_price(
        latest_btc_okx_price
    )


    change_html = format_change(
        latest_btc_daily_change
    )


    change = (
        latest_btc_current_daily_change
    )


    if change is not None and change > 0:

        status = "상승"
        status_class = "btc-on"

    elif change is not None and change < 0:

        status = "하락"
        status_class = "btc-off"

    else:

        status = "-"
        status_class = "btc-off"


    base_text = (

        "KST 09:00 기준"

        if SELECTED_TIMEFRAME == "1d"

        else

        "현재 15분봉 기준"

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
                    {html.escape(
                        timeframe_label
                    )}
                    ·
                    {base_text}

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

            <div class="btc-change">
                {change_html}
            </div>

            <div class="btc-signal-box">

                <div class="btc-status-item">

                    <div class="btc-info">

                        {html.escape(
                            timeframe_label
                        )}

                    </div>

                    <div class="{status_class}">

                        {status}

                    </div>

                </div>

            </div>

        </div>


        <div class="btc-timeframe-title">

            📅 BTC
            {html.escape(
                timeframe_label
            )}

        </div>


        <div class="btc-daily-grid">

            {timeframe_cells_html(
                latest_btc_daily_periods
            )}

        </div>

    </div>

    """


# =========================================================
# 카드 메인 색상
# =========================================================

def get_main_row_class(
    row
):

    change = row.get(
        "daily_change"
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
# 카드
# =========================================================

def unified_card_html(
    row,
    card_type,
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


    timeframe_label = (
        get_selected_timeframe_label()
    )


    if card_type == "SIGNAL_1":

        title = (
            "🚀 "
            +
            html.escape(
                timeframe_label
            )
            +
            " SIGNAL"
        )

        card_class = (
            "unified-market-card "
            "signal-market-card"
        )

        header_class = (
            "unified-card-header "
            "signal-header"
        )


        volume_rank = row.get(
            "volume_rank"
        )


        badge = (

            f"""
            <span class="signal-badge">
                거래대금 {volume_rank}위
            </span>
            """

            if volume_rank is not None

            else ""

        )


        signal_patterns = (
            row.get(
                "signal_conditions",
                {}
            )
            .get(
                "current_signal_patterns",
                []
            )
        )


        pattern_text = (

            " / ".join(
                signal_patterns
            )

            if signal_patterns

            else "-"

        )


        condition_html = f"""

        <div class="unified-condition-row">

            <div class="unified-condition">

                <div class="condition-label">

                    {html.escape(
                        timeframe_label
                    )} SIGNAL

                </div>

                <div class="condition-period">

                    현재 {html.escape(
                        timeframe_label
                    )} 양수
                    +
                    현재봉 양봉

                </div>

                <div class="condition-value">

                    <span class="up">

                        {html.escape(
                            pattern_text
                        )}

                    </span>

                </div>

            </div>

        </div>

        """


    else:

        title = "🏆 업비트 TOP"

        card_class = (
            "unified-market-card"
        )

        header_class = (
            "unified-card-header"
        )

        badge = ""

        condition_html = ""


    return f"""

    <div class="{card_class}">

        <div class="{header_class}">

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

                    현재
                    {html.escape(
                        timeframe_label
                    )}

                </div>

                <div class="unified-daily">

                    {row.get(
                        "daily_html",
                        "-"
                    )}

                </div>

            </div>

        </div>


        <div class="timeframe-card-title">

            📅
            {html.escape(
                timeframe_label
            )}

        </div>


        <div class="unified-daily-grid">

            {timeframe_cells_html(
                row.get(
                    "daily_periods",
                    []
                )
            )}

        </div>


        {condition_html}

    </div>

    """


# =========================================================
# SIGNAL
# =========================================================

def focus_section(
    data
):

    timeframe_label = (
        get_selected_timeframe_label()
    )


    signal_rows = [

        row

        for row in data

        if row.get(
            "signal_pass",
            False
        )

    ]


    signal_rows.sort(

        key=lambda row:

            row.get(
                "volume_rank",
                float("inf")
            )

    )


    if not signal_rows:

        body = f"""

        <div class="signal-empty-card">

            <div class="signal-empty-icon">
                🔎
            </div>

            <div class="signal-empty-title">

                현재 {html.escape(
                    timeframe_label
                )} SIGNAL 없음

            </div>

            <div class="signal-empty-text">

                현재 {html.escape(
                    timeframe_label
                )} 양수
                +
                현재봉 양봉
                +
                상승장악 계열 조건

            </div>

        </div>

        """


    else:

        cards = []


        for index, row in enumerate(
            signal_rows
        ):

            cards.append(
                unified_card_html(
                    row,
                    "SIGNAL_1",
                    index + 1
                )
            )


        body = "".join(
            cards
        )


    return f"""

    <div class="unified-section">

        <div class="section-title-card">

            <div class="section-number signal-section-number">
                🚀
            </div>

            <div class="section-heading">

                <div class="section-heading-main">

                    {html.escape(
                        timeframe_label
                    )} SIGNAL

                </div>

                <div class="section-heading-sub">

                    {html.escape(
                        timeframe_label
                    )} 양수
                    ·
                    현재봉 양봉
                    ·
                    상승장악
                    ·
                    3캔들 상승장악
                    ·
                    4캔들 상승장악
                    ·
                    상승장악 후 양봉
                    ·
                    장대양봉 후 양봉
                    ·
                    관통형 후 양봉

                </div>

            </div>

        </div>


        <div class="signal-card-list">

            {body}

        </div>

    </div>

    """


# =========================================================
# TOP
# =========================================================

def section(
    data,
    update_time
):

    timeframe_label = (
        get_selected_timeframe_label()
    )


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

                unified_card_html(
                    row,
                    "TOP",
                    row.get(
                        "rank",
                        "-"
                    )
                )

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
                    {html.escape(
                        timeframe_label
                    )} 변동률
                    ·
                    최근 6개
                    {html.escape(
                        timeframe_label
                    )}

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
    padding:10px;
}

h1{
    margin:3px 4px 10px;
    color:#eef2f5;
    font-size:15px;
    line-height:18px;
    font-weight:900;
}


/* =========================================================
   TIMEFRAME
   ========================================================= */

.timeframe-selector{
    display:flex;
    align-items:center;
    width:100%;
    margin:0 0 8px;
    padding:5px 7px;
    background:#10151b;
    border:2px solid #252e38;
    border-radius:10px;
}

.timeframe-selector-title{
    flex:none;
    margin-right:7px;
    color:#68747e;
    font-size:7px;
    font-weight:900;
}

.timeframe-buttons{
    display:flex;
    flex:1;
    gap:4px;
}

.tf-button{
    flex:1;
    display:flex;
    align-items:center;
    justify-content:center;
    min-height:28px;
    padding:4px 5px;
    border-radius:6px;
    background:#151b21;
    border:1px solid #303944;
    color:#8b969f;
    text-decoration:none;
    font-size:8px;
    font-weight:900;
}

.tf-button.active{
    background:#173326;
    border-color:#4f9b73;
    color:#8fe0b2;
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
    min-height:46px;
    padding:6px 9px;
    background:#10151b;
    border:2px solid #252e38;
    border-radius:12px;
}

.section-number{
    flex:none;
    display:flex;
    align-items:center;
    justify-content:center;
    width:35px;
    height:32px;
    margin-right:8px;
    border-radius:8px;
    background:#18251f;
    border:1px solid #315a48;
    color:#82d5a8;
    font-size:16px;
    font-weight:900;
}

.top-number{
    background:#1d1a13;
    border-color:#665331;
    color:#e0bd6d;
}

.signal-section-number{
    background:#2a2413;
    border-color:#d4af37;
    color:#f0cf67;
}

.section-heading{
    min-width:0;
    flex:1;
    overflow:hidden;
}

.section-heading-main{
    color:#e9edf1;
    font-size:12px;
    line-height:15px;
    font-weight:900;
}

.section-heading-sub{
    margin-top:1px;
    color:#87919b;
    font-size:7px;
    line-height:9px;
    font-weight:700;
    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;
}


/* =========================================================
   BTC
   ========================================================= */

.market-card{
    width:100%;
    margin:3px 0 10px;
    background:#0f141a;
    border:2px solid #252e38;
    border-radius:13px;
    overflow:hidden;
}

.market-card-header{
    display:flex;
    align-items:center;
    min-height:40px;
    padding:5px 9px;
    background:#121820;
    border-bottom:1px solid #29323c;
}

.market-title-block{
    min-width:0;
    flex:1;
}

.market-title-main{
    color:#eef2f5;
    font-size:11px;
    font-weight:900;
}

.market-title-sub{
    margin-top:1px;
    color:#7e8994;
    font-size:6.5px;
    font-weight:700;
}

.market-time{
    flex:none;
    margin-left:7px;
    color:#68737e;
    font-size:6.5px;
    font-weight:800;
}

.btc-main-row{
    display:grid;
    grid-template-columns:
        1fr 1.3fr 1fr 1.1fr;
    align-items:center;
    min-height:51px;
    background:#11161c;
}

.btc-name{
    padding-left:12px;
    color:#edf1f4;
    font-size:11px;
    font-weight:900;
}

.btc-price{
    color:#f1f4f6;
    font-size:11px;
    font-weight:900;
    text-align:center;
}

.btc-change{
    font-size:11px;
    font-weight:900;
    text-align:center;
}

.btc-signal-box{
    min-height:51px;
    display:flex;
    align-items:center;
    justify-content:center;
    border-left:1px solid #29323c;
}

.btc-status-item{
    display:flex;
    flex-direction:column;
    align-items:center;
    gap:2px;
}

.btc-info{
    color:#7f8a94;
    font-size:7px;
    font-weight:900;
}

.btc-on{
    color:#78cfa2;
    font-size:8px;
    font-weight:900;
}

.btc-off{
    color:#df8588;
    font-size:8px;
    font-weight:900;
}

.btc-timeframe-title{
    min-height:26px;
    display:flex;
    align-items:center;
    padding:4px 9px;
    background:#111820;
    border-top:1px solid #29323c;
    color:#aeb8c0;
    font-size:8px;
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
    min-height:43px;
    background:#0d1319;
    gap:1px;
    padding:2px;
}

.daily-main{
    display:flex;
    align-items:center;
    justify-content:center;
    gap:3px;
}

.daily-time{
    color:#b6bec5;
    font-size:7px;
    font-weight:900;
    white-space:nowrap;
}

.daily-value{
    font-size:8px;
    font-weight:900;
    white-space:nowrap;
}

.candle-pattern{
    color:#e0bd6d;
    font-size:6px;
    line-height:7px;
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
   카드
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
    border-radius:13px;
    overflow:hidden;
}

.signal-market-card{
    border:2px solid #d4af37;
    box-shadow:
        0 0 8px
        rgba(212,175,55,0.16);
}

.unified-card-header{
    display:flex;
    align-items:center;
    min-height:39px;
    padding:5px 9px;
    background:#121820;
    border-bottom:1px solid #29323c;
}

.signal-header{
    background:#211d11;
    border-bottom:1px solid #d4af37;
}

.unified-card-rank{
    width:38px;
    flex:none;
    color:#e0bd6d;
    font-size:9px;
    font-weight:900;
}

.unified-card-coin{
    flex:1;
    min-width:0;
    color:#eef2f5;
    font-size:11px;
    font-weight:900;
}

.unified-card-title{
    flex:none;
    color:#7f8a94;
    font-size:7px;
    font-weight:900;
    margin-right:7px;
}

.signal-badge{
    padding:3px 6px;
    border-radius:6px;
    background:#2a2413;
    border:1px solid #d4af37;
    color:#f0cf67;
    font-size:6px;
    font-weight:900;
}


/* =========================================================
   MAIN ROW
   ========================================================= */

.unified-main-row{
    display:grid;
    grid-template-columns:
        repeat(3,1fr);
    min-height:45px;
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
    font-size:6px;
    font-weight:800;
}

.unified-price{
    color:#f1f4f6;
    font-size:10px;
    font-weight:900;
}

.unified-volume{
    color:#cfd6dc;
    font-size:9px;
    font-weight:900;
}

.unified-daily{
    font-size:9px;
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
    min-height:26px;
    display:flex;
    align-items:center;
    padding:4px 8px;
    background:#111820;
    border-top:1px solid #29323c;
    color:#9da8b1;
    font-size:7px;
    font-weight:900;
}


/* =========================================================
   SIGNAL
   ========================================================= */

.unified-condition-row{
    width:100%;
    min-height:45px;
    background:#0f171d;
    border-top:1px solid #d4af37;
}

.unified-condition{
    display:flex;
    flex-direction:column;
    align-items:center;
    justify-content:center;
    min-height:45px;
    gap:2px;
    padding:4px 7px;
}

.condition-label{
    color:#b99d46;
    font-size:6px;
    font-weight:800;
}

.condition-period{
    color:#f0cf67;
    font-size:7px;
    font-weight:900;
    text-align:center;
}

.condition-value{
    font-size:8px;
    font-weight:900;
}

.signal-empty-card{
    display:flex;
    flex-direction:column;
    align-items:center;
    justify-content:center;
    min-height:105px;
    margin-top:6px;
    background:#18150e;
    border:2px solid #8f7728;
    border-radius:11px;
    text-align:center;
    padding:12px;
}

.signal-empty-icon{
    font-size:20px;
}

.signal-empty-title{
    color:#f0cf67;
    font-size:10px;
    font-weight:900;
}

.signal-empty-text{
    margin-top:6px;
    color:#a58d4a;
    font-size:7px;
    font-weight:800;
}


/* =========================================================
   색상
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
    border-radius:11px;
    color:#59636e;
    font-size:8px;
    font-weight:800;
}

.top-update-bar{
    display:flex;
    justify-content:space-between;
    padding:4px 7px;
    color:#68737e;
    font-size:6px;
    font-weight:800;
}


/* =========================================================
   MOBILE
   ========================================================= */

@media(max-width:600px){

    body{
        padding:6px;
    }

    h1{
        margin:3px 3px 8px;
        font-size:12px;
    }

    .timeframe-selector{
        padding:4px 5px;
    }

    .timeframe-selector-title{
        margin-right:5px;
        font-size:5px;
    }

    .tf-button{
        min-height:24px;
        font-size:6px;
    }

    .section-title-card{
        min-height:38px;
        padding:4px 6px;
        border-radius:9px;
    }

    .section-number{
        width:27px;
        height:27px;
        margin-right:6px;
        border-radius:6px;
        font-size:12px;
    }

    .section-heading-main{
        font-size:9px;
        line-height:11px;
    }

    .section-heading-sub{
        font-size:5px;
        line-height:7px;
    }

    .market-card{
        border-radius:9px;
    }

    .market-card-header{
        min-height:34px;
        padding:4px 7px;
    }

    .market-title-main{
        font-size:8px;
    }

    .market-title-sub{
        font-size:4.8px;
    }

    .market-time{
        font-size:4.8px;
    }

    .btc-main-row{
        min-height:40px;
    }

    .btc-name{
        padding-left:8px;
        font-size:8px;
    }

    .btc-price{
        font-size:8px;
    }

    .btc-change{
        font-size:8px;
    }

    .btc-signal-box{
        min-height:40px;
    }

    .btc-info{
        font-size:5px;
    }

    .btc-on,
    .btc-off{
        font-size:6px;
    }

    .btc-timeframe-title{
        min-height:22px;
        padding:3px 7px;
        font-size:6px;
    }

    .btc-daily-grid,
    .unified-daily-grid{
        grid-template-columns:
            repeat(3,1fr);
    }

    .daily-cell{
        min-height:47px;
    }

    .daily-time{
        font-size:5px;
    }

    .daily-value{
        font-size:6px;
    }

    .candle-pattern{
        font-size:4.5px;
    }

    .unified-market-card{
        border-radius:9px;
    }

    .unified-card-header{
        min-height:33px;
        padding:4px 7px;
    }

    .unified-card-rank{
        width:27px;
        font-size:7px;
    }

    .unified-card-coin{
        font-size:8px;
    }

    .unified-card-title{
        font-size:5px;
        margin-right:5px;
    }

    .signal-badge{
        font-size:4px;
    }

    .unified-main-row{
        min-height:40px;
    }

    .unified-label{
        font-size:4.5px;
    }

    .unified-price,
    .unified-volume,
    .unified-daily{
        font-size:7px;
    }

    .timeframe-card-title{
        min-height:21px;
        font-size:5px;
    }

    .unified-condition-row{
        min-height:38px;
    }

    .unified-condition{
        min-height:38px;
    }

    .condition-label{
        font-size:4.5px;
    }

    .condition-period{
        font-size:5.5px;
    }

    .condition-value{
        font-size:7px;
    }

}


/* =========================================================
   VERY SMALL
   ========================================================= */

@media(max-width:380px){

    body{
        padding:4px;
    }

    h1{
        font-size:11px;
    }

    .section-title-card{
        min-height:34px;
    }

    .section-number{
        width:23px;
        height:23px;
    }

    .section-heading-main{
        font-size:8px;
    }

    .section-heading-sub{
        font-size:4px;
    }

    .btc-main-row{
        min-height:35px;
    }

    .btc-name,
    .btc-price,
    .btc-change{
        font-size:7px;
    }

    .btc-signal-box{
        min-height:35px;
    }

    .daily-cell{
        min-height:41px;
    }

    .daily-time{
        font-size:4px;
    }

    .daily-value{
        font-size:5px;
    }

    .candle-pattern{
        font-size:3.8px;
    }

}


/* =========================================================
   END CSS
   =========================================================

"""


# =========================================================
# Dashboard
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def dashboard(
    tf: str = "1d"
):

    global SELECTED_TIMEFRAME
    global last_updated_timeframe


    # -----------------------------------------------------
    # 타임프레임 선택
    # -----------------------------------------------------

    if tf in TIMEFRAME_OPTIONS:

        SELECTED_TIMEFRAME = tf

    else:

        SELECTED_TIMEFRAME = "1d"


    # -----------------------------------------------------
    # 선택이 변경되었으면 즉시 업데이트
    # -----------------------------------------------------

    if (
        last_updated_timeframe
        !=
        SELECTED_TIMEFRAME
    ):

        try:

            update_dashboard()

        except Exception as e:

            log.exception(
                f"타임프레임 변경 오류: {e}"
            )


    content = ""


    if USE_UPBIT == "Y":

        content += focus_section(
            latest_upbit_data
        )

        content += section(
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


        {timeframe_selector_html()}


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


    if SELECTED_TIMEFRAME not in TIMEFRAME_OPTIONS:

        raise ValueError(
            "잘못된 타임프레임입니다."
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
        "타임프레임 = 15분 / 일봉"
    )

    log.info(
        "기본 타임프레임 = 일봉"
    )

    log.info(
        "일봉 기준 = KST 09:00"
    )

    log.info(
        "OKX BTC = 선택 타임프레임 연동"
    )

    log.info(
        "OKX 일봉 = KST 09:00 기준"
    )

    log.info(
        "화면 순서 = BTC → SIGNAL → TOP10"
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
