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

TOP_N = 20

UPDATE_MINUTES = 1

HISTORY_CHUNK = 200

MAX_HISTORY_CHUNKS = 10

USE_UPBIT = "Y"

USE_OKX = "N"

REQUEST_INTERVAL = 0.08

RATE_LIMIT_WAIT = 3

MAX_RETRIES = 10


# =========================================================
# 전역
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
# BTC
# =========================================================

OKX_BASE_URL = "https://www.okx.com"

OKX_BTC_INST_ID = "BTC-USDT"

latest_btc_okx_price = None

latest_btc_daily_change = None

latest_btc_4h_periods = []

latest_btc_current_4h_change = None

latest_btc_current_4h_label = "-"


# =========================================================
# 업비트 4H 기준
#
# 01~05
# 05~09
# 09~13
# 13~17
# 17~21
# 21~01
# =========================================================

FOUR_HOUR_DEFINITIONS = [

    {
        "key": "01_05",
        "start_hour": 1,
        "end_hour": 5
    },

    {
        "key": "05_09",
        "start_hour": 5,
        "end_hour": 9
    },

    {
        "key": "09_13",
        "start_hour": 9,
        "end_hour": 13
    },

    {
        "key": "13_17",
        "start_hour": 13,
        "end_hour": 17
    },

    {
        "key": "17_21",
        "start_hour": 17,
        "end_hour": 21
    },

    {
        "key": "21_01",
        "start_hour": 21,
        "end_hour": 1
    }

]


# =========================================================
# 현재 KST 문자열
# =========================================================

def kst():

    return datetime.now(
        KST
    ).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


# =========================================================
# 현재 4H 시작시간
# =========================================================

def get_current_4h_start():

    now = datetime.now(KST)

    hour = now.hour

    if hour < 1:

        return (
            now
            -
            timedelta(days=1)
        ).replace(
            hour=21,
            minute=0,
            second=0,
            microsecond=0
        )

    if hour < 5:

        start_hour = 1

    elif hour < 9:

        start_hour = 5

    elif hour < 13:

        start_hour = 9

    elif hour < 17:

        start_hour = 13

    elif hour < 21:

        start_hour = 17

    else:

        start_hour = 21

    return now.replace(
        hour=start_hour,
        minute=0,
        second=0,
        microsecond=0
    )


# =========================================================
# 4H 기간 만들기
# =========================================================

def make_4h_period(
    start,
    active=False
):

    start = start.astimezone(KST)

    end = (
        start
        +
        timedelta(hours=4)
    )

    if start.date() == datetime.now(KST).date():

        day_label = "오늘"

    elif start.date() == (
        datetime.now(KST).date()
        -
        timedelta(days=1)
    ):

        day_label = "어제"

    else:

        day_label = "전일"


    if active:

        display_label = (
            f"현재 {start:%H}~{end:%H}"
        )

    else:

        display_label = (
            f"{day_label} "
            f"{start:%H}~{end:%H}"
        )


    return {

        "key":
            f"{start:%Y%m%d_%H}",

        "time_key":
            start.strftime("%H"),

        "label":
            f"{start:%H}~{end:%H}",

        "display_label":
            display_label,

        "day_label":
            day_label,

        "start":
            start,

        "end":
            end,

        "active":
            active

    }


# =========================================================
# 최근 6개 4H
# =========================================================

def get_recent_4h_periods(
    count=6
):

    current_start = (
        get_current_4h_start()
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
            timedelta(
                hours=4 * i
            )
        )

        periods.append(
            make_4h_period(
                start,
                active=(i == 0)
            )
        )

    return periods


# =========================================================
# 현재 4H
# =========================================================

def get_current_4h_period():

    periods = get_recent_4h_periods(
        1
    )

    if not periods:

        return None

    return periods[0]


# =========================================================
# 이전 4H
#
# 화면 표시용
#
# SIGNAL 필터에는 사용하지 않음
# =========================================================

def get_previous_4h_period():

    current = get_current_4h_period()

    if current is None:

        return None

    previous_start = (
        current["start"]
        -
        timedelta(hours=4)
    )

    return make_4h_period(
        previous_start,
        active=False
    )


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
                REQUEST_INTERVAL - elapsed
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

    url = ""

    if args and isinstance(
        args[0],
        str
    ):

        url = args[0]

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
                        RATE_LIMIT_WAIT
                        *
                        (attempt + 1),
                        60
                    )
                )

                continue

            if response.status_code >= 500:

                time.sleep(
                    min(
                        2
                        *
                        (attempt + 1),
                        30
                    )
                )

                continue

            return response

        except Exception as e:

            log.warning(
                f"API 오류 "
                f"{url} "
                f"{attempt + 1}/{MAX_RETRIES}: "
                f"{e}"
            )

            if attempt < MAX_RETRIES - 1:

                time.sleep(
                    min(
                        2
                        *
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

                price = float(
                    item["trade_price"]
                )

                volume = float(
                    item["acc_trade_price_24h"]
                )

                result.append({

                    "market":
                        item["market"],

                    "current_price":
                        price,

                    "volume_24h":
                        volume

                })

            except Exception:

                continue


    latest_upbit_markets = [

        x["market"]

        for x in result

    ]


    return result


# =========================================================
# 업비트 당일 변동률
#
# 기준:
# 업비트 일봉
#
# 현재가 - 전일 종가
# =========================================================

def daily_change_upbit(
    market,
    current_price
):

    response = retry(
        requests.get,
        "https://api.upbit.com/v1/candles/days",
        params={
            "market":
                market,
            "count":
                2
        },
        timeout=15
    )


    if response is None:

        return None


    try:

        data = response.json()

    except Exception:

        return None


    if not isinstance(
        data,
        list
    ):

        return None


    if len(data) < 2:

        return None


    try:

        previous_close = float(
            data[1]["trade_price"]
        )

        current_price = float(
            current_price
        )

    except Exception:

        return None


    if previous_close == 0:

        return None


    return (
        (
            current_price
            -
            previous_close
        )
        /
        previous_close
        *
        100
    )


# =========================================================
# 업비트 1H
# =========================================================

def get_upbit_60m_candles(
    market,
    count=300
):

    response = retry(
        requests.get,
        "https://api.upbit.com/v1/candles/minutes/60",
        params={
            "market":
                market,
            "count":
                count
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

def candle_parts(candle):

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
        h
        -
        max(o, c)
    )

    lower = (
        min(o, c)
        -
        l
    )

    body_ratio = (
        body / total
    )


    return {

        "open":
            o,

        "high":
            h,

        "low":
            l,

        "close":
            c,

        "body":
            body,

        "total":
            total,

        "upper":
            upper,

        "lower":
            lower,

        "body_ratio":
            body_ratio,

        "bull":
            c > o,

        "bear":
            c < o

    }


# =========================================================
# 1개 캔들 패턴
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


    body = p["body"]

    total = p["total"]

    upper = p["upper"]

    lower = p["lower"]

    body_ratio = p["body_ratio"]


    # =====================================================
    # 도지
    # =====================================================

    if body_ratio <= 0.10:

        patterns.append(
            "도지"
        )


    # =====================================================
    # 망치형
    #
    # 작은 몸통
    # 긴 아래꼬리
    # 짧은 위꼬리
    # =====================================================

    if (

        body_ratio <= 0.40

        and

        lower >= max(
            body * 2,
            total * 0.45
        )

        and

        upper <= max(
            body,
            total * 0.15
        )

    ):

        patterns.append(
            "망치형"
        )


    # =====================================================
    # 역망치형
    # =====================================================

    if (

        body_ratio <= 0.40

        and

        upper >= max(
            body * 2,
            total * 0.45
        )

        and

        lower <= max(
            body,
            total * 0.15
        )

    ):

        patterns.append(
            "역망치형"
        )


    return patterns


# =========================================================
# 2개 캔들 패턴
# =========================================================

def detect_two_candle_pattern(
    previous,
    current
):

    p1 = candle_parts(
        previous
    )

    p2 = candle_parts(
        current
    )


    if (
        p1 is None
        or
        p2 is None
    ):

        return []


    patterns = []


    # =====================================================
    # 상승 장악형
    # =====================================================

    if (

        p1["bear"]

        and

        p2["bull"]

        and

        p2["open"] <= p1["close"]

        and

        p2["close"] >= p1["open"]

        and

        p2["body"] > p1["body"]

    ):

        patterns.append(
            "상승장악"
        )


    # =====================================================
    # 관통형
    # =====================================================

    midpoint = (
        p1["open"]
        +
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


    return patterns


# =========================================================
# 3개 캔들 패턴
# =========================================================

def detect_three_candle_pattern(
    c1,
    c2,
    c3
):

    p1 = candle_parts(
        c1
    )

    p2 = candle_parts(
        c2
    )

    p3 = candle_parts(
        c3
    )


    if (
        p1 is None
        or
        p2 is None
        or
        p3 is None
    ):

        return []


    patterns = []


    # =====================================================
    # 모닝스타
    # =====================================================

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

        p3["close"]
        >
        (
            p1["open"]
            +
            p1["close"]
        ) / 2

    ):

        patterns.append(
            "모닝스타"
        )


    # =====================================================
    # 3연속 양봉
    # =====================================================

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


    return patterns


# =========================================================
# 4H 전체 캔들 패턴 탐지
# =========================================================

def detect_4h_patterns(
    periods
):

    if not periods:

        return []


    result = []


    for i, period in enumerate(
        periods
    ):

        patterns = []


        # =================================================
        # 현재 캔들 데이터 확인
        # =================================================

        if (

            period.get("open") is None

            or

            period.get("high") is None

            or

            period.get("low") is None

            or

            period.get("close") is None

        ):

            result.append(
                []
            )

            continue


        # =================================================
        # 1개 캔들
        # =================================================

        patterns.extend(

            detect_single_candle_pattern(
                period
            )

        )


        # =================================================
        # 2개 캔들
        # =================================================

        if i >= 1:

            previous = periods[
                i - 1
            ]


            if all(

                previous.get(
                    x
                ) is not None

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


        # =================================================
        # 3개 캔들
        # =================================================

        if i >= 2:

            c1 = periods[
                i - 2
            ]

            c2 = periods[
                i - 1
            ]

            c3 = period


            if all(

                x.get(
                    key
                ) is not None

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


        # =================================================
        # 중복 제거
        # =================================================

        patterns = list(
            dict.fromkeys(
                patterns
            )
        )


        result.append(
            patterns
        )


    return result


# =========================================================
# 업비트 4H 생성
# =========================================================

def build_upbit_4h_candles(
    market,
    current_price=None
):

    candles = get_upbit_60m_candles(
        market,
        300
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


    periods = get_recent_4h_periods(
        6
    )


    result = []


    for period in periods:

        part = df[
            (
                df["datetime"]
                >=
                period["start"]
            )
            &
            (
                df["datetime"]
                <
                period["end"]
            )
        ].copy()


        if part.empty:

            result.append({

                **period,

                "open":
                    None,

                "high":
                    None,

                "low":
                    None,

                "close":
                    None,

                "change":
                    None,

                "patterns":
                    []

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


        # =================================================
        # 현재 진행 중인 4H
        # 현재가 반영
        # =================================================

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


        if open_price == 0:

            change = None

        else:

            change = (

                (
                    close_price
                    -
                    open_price
                )

                /

                open_price

                *

                100

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

            "patterns":
                []

        })


    # =====================================================
    # ★ 4H 캔들 패턴 탐지
    # =====================================================

    pattern_results = (
        detect_4h_patterns(
            result
        )
    )


    for i, pattern_list in enumerate(
        pattern_results
    ):

        result[i]["patterns"] = (
            pattern_list
        )


    return result


# =========================================================
# 코인의 4H 분석
# =========================================================

def analyze_4h(
    market,
    current_price
):

    periods = build_upbit_4h_candles(
        market,
        current_price
    )


    current_period = (
        get_current_4h_period()
    )

    previous_period = (
        get_previous_4h_period()
    )


    current_change = None

    previous_change = None


    for period in periods:

        if (
            period["start"]
            ==
            current_period["start"]
        ):

            current_change = (
                period["change"]
            )


        if (
            period["start"]
            ==
            previous_period["start"]
        ):

            previous_change = (
                period["change"]
            )


    return {

        "periods":
            periods,

        "current_4h_change":
            current_change,

        "previous_4h_change":
            previous_change

    }


# =========================================================
# 코인 전체 분석
# =========================================================

def analyze(
    market,
    current_price
):

    daily_change = daily_change_upbit(
        market,
        current_price
    )


    four_hour = analyze_4h(
        market,
        current_price
    )


    return {

        "daily_change":
            daily_change,

        "four_hour_periods":
            four_hour[
                "periods"
            ],

        "current_4h_change":
            four_hour[
                "current_4h_change"
            ],

        "previous_4h_change":
            four_hour[
                "previous_4h_change"
            ]

    }


# =========================================================
# 값 변환
# =========================================================

def get_change_value(x):

    try:

        if x is None:

            return None

        return float(x)

    except Exception:

        return None


# =========================================================
# 변화율 표시
# =========================================================

def format_change(x):

    x = get_change_value(
        x
    )


    if x is None:

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


# =========================================================
# 가격 표시
# =========================================================

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


# =========================================================
# 거래대금 표시
# =========================================================

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
    current_price
):

    analysis = (
        analysis
        or
        {}
    )


    return {

        "rank":
            rank,

        "name":
            name,

        "volume":
            format_volume(
                volume
            ),

        "current_price":
            current_price,

        # 업비트 당일 변동률
        "daily_change":
            get_change_value(
                analysis.get(
                    "daily_change"
                )
            ),

        "daily_html":
            format_change(
                analysis.get(
                    "daily_change"
                )
            ),

        # 최근 6개 4H
        "four_hour_periods":
            analysis.get(
                "four_hour_periods",
                []
            ),

        # 현재 4H
        "current_4h_change":
            get_change_value(
                analysis.get(
                    "current_4h_change"
                )
            ),

        # 이전 4H
        # 화면 표시용
        # SIGNAL 필터에는 사용하지 않음
        "previous_4h_change":
            get_change_value(
                analysis.get(
                    "previous_4h_change"
                )
            )

    }


# =========================================================
# 업비트 TOP 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data

    global latest_upbit_update_time


    markets = get_upbit_markets()


    markets = sorted(
        markets,
        key=lambda x:
            x["volume_24h"],
        reverse=True
    )


    top_markets = markets[
        :TOP_N
    ]


    rows = []


    # =====================================================
    # BTC 현재 4H 조건
    # =====================================================

    btc_pass = (

        latest_btc_current_4h_change
        is not None

        and

        latest_btc_current_4h_change
        > 0

    )


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
            item[
                "volume_24h"
            ],
            analysis,
            price
        )


        # =================================================
        # SIGNAL 조건 1
        # BTC 현재 4H 양수
        # =================================================

        btc_condition = btc_pass


        # =================================================
        # SIGNAL 조건 2
        # 업비트 당일 변동률 양수
        # =================================================

        daily_condition = (

            row["daily_change"]
            is not None

            and

            row["daily_change"]
            > 0

        )


        # =================================================
        # SIGNAL 조건 3
        # 현재 4H 양수
        # =================================================

        current_4h_condition = (

            row["current_4h_change"]
            is not None

            and

            row["current_4h_change"]
            > 0

        )


        # =================================================
        # 최종 SIGNAL
        #
        # BTC + 당일 + 현재4H
        # =================================================

        row["signal_pass"] = (

            btc_condition

            and

            daily_condition

            and

            current_4h_condition

        )


        # =================================================
        # 디버깅용 조건
        # =================================================

        row["signal_conditions"] = {

            "btc":
                btc_condition,

            "daily":
                daily_condition,

            "current_4h":
                current_4h_condition

        }


        rows.append(
            row
        )


    latest_upbit_data = rows

    latest_upbit_update_time = kst()


    signal_count = sum(

        1

        for row in rows

        if row.get(
            "signal_pass",
            False
        )

    )


    current = get_current_4h_period()

    previous = get_previous_4h_period()


    log.info(

        f"TOP{TOP_N} 업데이트 | "
        f"현재={current['display_label'] if current else '-'} | "
        f"이전={previous['display_label'] if previous else '-'} | "
        f"BTC4H={latest_btc_current_4h_change} | "
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
# BTC 1H
# =========================================================

def get_okx_btc_1h_candles(
    limit=200,
    after=None
):

    params = {

        "instId":
            OKX_BTC_INST_ID,

        "bar":
            "1H",

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
# BTC 1H history
# =========================================================

def get_okx_btc_1h_history():

    rows = []

    after = None


    for _ in range(
        MAX_HISTORY_CHUNKS
    ):

        data = get_okx_btc_1h_candles(
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


    result = []


    for row in ordered:

        try:

            result.append({

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


    if not result:

        return pd.DataFrame()


    df = pd.DataFrame(
        result
    )


    df["datetime_utc"] = pd.to_datetime(
        df["timestamp"],
        unit="ms",
        utc=True
    )


    df["datetime_kst"] = (
        df["datetime_utc"]
        .dt
        .tz_convert(KST)
    )


    return df


# =========================================================
# BTC KST 일봉
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
# BTC 당일 변동률
# =========================================================

def get_btc_daily_change(
    price,
    df
):

    if price is None:

        return None


    daily = aggregate_btc_daily(
        df
    )


    if daily.empty:

        return None


    now = datetime.now(KST)


    today_0900 = now.replace(
        hour=9,
        minute=0,
        second=0,
        microsecond=0
    )


    if now < today_0900:

        current_start = (
            today_0900
            -
            timedelta(days=1)
        )

    else:

        current_start = today_0900


    daily["daily_start"] = (
        daily["daily_start"]
        .dt
        .tz_localize(None)
    )


    target = (
        current_start.replace(
            tzinfo=None
        )
    )


    previous = daily[
        daily["daily_start"]
        <
        target
    ]


    if previous.empty:

        return None


    previous_close = float(
        previous.iloc[-1][
            "close"
        ]
    )


    if previous_close == 0:

        return None


    return (
        (
            price
            -
            previous_close
        )
        /
        previous_close
        *
        100
    )


# =========================================================
# BTC 4H
# =========================================================

def build_btc_4h(
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


    temp = df.copy()


    temp["kst_naive"] = (
        temp["datetime_kst"]
        .dt
        .tz_localize(None)
    )


    periods = get_recent_4h_periods(
        6
    )


    result = []


    for period in periods:

        start = (
            period["start"]
            .replace(
                tzinfo=None
            )
        )

        end = (
            period["end"]
            .replace(
                tzinfo=None
            )
        )


        part = temp[
            (
                temp["kst_naive"]
                >=
                start
            )
            &
            (
                temp["kst_naive"]
                <
                end
            )
        ].copy()


        if part.empty:

            result.append({

                **period,

                "open":
                    None,

                "high":
                    None,

                "low":
                    None,

                "close":
                    None,

                "change":
                    None

            })

            continue


        part = part.sort_values(
            "kst_naive"
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


        if open_price == 0:

            change = None

        else:

            change = (
                (
                    close_price
                    -
                    open_price
                )
                /
                open_price
                *
                100
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

            "patterns":
                []

        })


    # =====================================================
    # BTC 4H 캔들 패턴
    # =====================================================

    pattern_results = (
        detect_4h_patterns(
            result
        )
    )


    for i, pattern_list in enumerate(
        pattern_results
    ):

        result[i]["patterns"] = (
            pattern_list
        )


    return result


# =========================================================
# BTC 업데이트
# =========================================================

def update_btc_market():

    global latest_btc_okx_price

    global latest_btc_daily_change

    global latest_btc_4h_periods

    global latest_btc_current_4h_change

    global latest_btc_current_4h_label


    price = get_okx_btc_price()


    if price is None:

        return


    latest_btc_okx_price = price


    df = get_okx_btc_1h_history()


    latest_btc_daily_change = (
        get_btc_daily_change(
            price,
            df
        )
    )


    latest_btc_4h_periods = (
        build_btc_4h(
            price,
            df
        )
    )


    current = (
        get_current_4h_period()
    )


    if current is None:

        return


    latest_btc_current_4h_label = (
        current[
            "display_label"
        ]
    )


    latest_btc_current_4h_change = None


    for period in latest_btc_4h_periods:

        if (
            period["start"]
            ==
            current["start"]
        ):

            latest_btc_current_4h_change = (
                period["change"]
            )

            break


# =========================================================
# OKX placeholder
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

    if not update_lock.acquire(
        False
    ):

        return


    try:

        # BTC 먼저
        update_btc_market()


        # 업비트
        if USE_UPBIT == "Y":

            update_upbit()


        # OKX
        if USE_OKX == "Y":

            usdt = get_usdt_krw()

            if usdt:

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
# 캔들 패턴 HTML
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
# 4H 셀
# =========================================================

def four_hour_cells_html(
    periods
):

    if not periods:

        return (
            '<div class="no-4h-data">-</div>'
        )


    cells = []


    for period in periods:

        if period.get(
            "active",
            False
        ):

            cell_class = (
                "four-hour-cell current-4h"
            )

            day_text = "현재"

        else:

            cell_class = (
                "four-hour-cell"
            )

            day_text = period.get(
                "day_label",
                ""
            )


        cells.append(

            f"""

            <div class="{cell_class}">

                <div class="four-hour-label">
                    {html.escape(
                        str(day_text)
                    )}
                </div>

                <div class="four-hour-time">
                    {html.escape(
                        str(
                            period.get(
                                "label",
                                "-"
                            )
                        )
                    )}
                </div>

                <div class="four-hour-value">
                    {format_change(
                        period.get(
                            "change"
                        )
                    )}
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
# BTC 4H HTML
# =========================================================

def btc_4h_cells_html():

    return four_hour_cells_html(
        latest_btc_4h_periods
    )


# =========================================================
# BTC 시장 카드
# =========================================================

def market_summary_html():

    period = (
        get_current_4h_period()
    )


    price = format_market_price(
        latest_btc_okx_price
    )


    daily = format_change(
        latest_btc_daily_change
    )


    current = format_change(
        latest_btc_current_4h_change
    )


    signal_on = (

        latest_btc_current_4h_change
        is not None

        and

        latest_btc_current_4h_change
        > 0

    )


    signal_status = (
        "ON"
        if signal_on
        else
        "OFF"
    )


    signal_class = (
        "btc-on"
        if signal_on
        else
        "btc-off"
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
                    · 당일 KST 09:00 기준
                    · 4H 업비트 시간 기준
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
                {daily}
            </div>

            <div class="btc-signal-box">

                <span class="btc-info">
                    SIGNAL
                </span>

                <span class="{signal_class}">
                    {signal_status}
                </span>

            </div>

        </div>


        <div class="btc-current-box">

            <div class="btc-current-title">
                현재 4H
            </div>

            <div class="btc-current-period">

                {period.get(
                    "display_label",
                    "-"
                )}

            </div>

            <div class="btc-current-value">
                {current}
            </div>

        </div>


        <div class="btc-4h-grid">

            {btc_4h_cells_html()}

        </div>

    </div>

    """


# =========================================================
# 공통 SIGNAL / TOP 카드
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


    current_period = (
        get_current_4h_period()
    )

    previous_period = (
        get_previous_4h_period()
    )


    current_change = (
        row.get(
            "current_4h_change"
        )
    )


    previous_change = (
        row.get(
            "previous_4h_change"
        )
    )


    if card_type == "SIGNAL":

        title = "🚀 SIGNAL"

        card_class = (
            "unified-market-card signal-market-card"
        )

        header_class = (
            "unified-card-header signal-header"
        )

        badge = """
        <span class="signal-badge">
            SIGNAL
        </span>
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


    if card_type == "SIGNAL":

        condition_html = f"""

        <div class="unified-condition-row">

            <div class="unified-condition">

                <div class="condition-label">
                    이전 4H
                </div>

                <div class="condition-period">

                    {previous_period.get(
                        "display_label",
                        "-"
                    )}

                </div>

                <div class="condition-value">

                    {format_change(
                        previous_change
                    )}

                </div>

            </div>


            <div class="unified-condition">

                <div class="condition-label">
                    SIGNAL 조건
                </div>

                <div class="condition-period">
                    BTC + 당일 + 현재4H
                </div>

                <div class="condition-value">

                    <span class="up">
                        ON
                    </span>

                </div>

            </div>

        </div>

        """


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


        <div class="unified-main-row">

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
                    당일
                </div>

                <div class="unified-daily">

                    {row.get(
                        "daily_html",
                        "-"
                    )}

                </div>

            </div>

        </div>


        <div class="unified-current-row">

            <div class="unified-current-title">
                현재 4H
            </div>

            <div class="unified-current-period">

                {current_period.get(
                    "display_label",
                    "-"
                )}

            </div>

            <div class="unified-current-value">

                {format_change(
                    current_change
                )}

            </div>

        </div>


        <div class="unified-4h-grid">

            {four_hour_cells_html(
                row.get(
                    "four_hour_periods",
                    []
                )
            )}

        </div>


        {condition_html}

    </div>

    """


# =========================================================
# SIGNAL Section
# =========================================================

def focus_section(data):

    current_period = (
        get_current_4h_period()
    )

    previous_period = (
        get_previous_4h_period()
    )


    btc_change = (
        latest_btc_current_4h_change
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
                "current_4h_change"
            )

            if row.get(
                "current_4h_change"
            ) is not None

            else -999999,

        reverse=True

    )


    if not signal_rows:

        if (
            btc_change is None
            or
            btc_change <= 0
        ):

            message = (
                "BTC 현재 4H가 "
                "양수가 아니므로 SIGNAL 없음"
            )

        else:

            message = (
                "BTC 현재 4H는 양수지만 "
                "당일 변동률 + 현재 4H "
                "조건을 모두 만족하는 종목 없음"
            )


        body = f"""

        <div class="signal-empty-card">

            <div class="signal-empty-icon">
                🔎
            </div>

            <div class="signal-empty-title">
                SIGNAL 없음
            </div>

            <div class="signal-empty-text">
                {message}
            </div>

            <div class="signal-empty-sub">

                현재:
                {current_period.get(
                    "display_label",
                    "-"
                )}

                ·

                이전:
                {previous_period.get(
                    "display_label",
                    "-"
                )}

            </div>

        </div>

        """

    else:

        signal_cards = []


        for index, row in enumerate(
            signal_rows
        ):

            signal_cards.append(

                unified_card_html(
                    row,
                    "SIGNAL",
                    index + 1
                )

            )


        body = "".join(
            signal_cards
        )


    return f"""

    <div class="unified-section">

        <div class="section-title-card">

            <div class="section-number">
                🚀
            </div>

            <div class="section-heading">

                <div class="section-heading-main">
                    SIGNAL
                </div>

                <div class="section-heading-sub">

                    BTC 현재 4H 양수
                    · 업비트 당일 양수
                    · 현재 4H 양수

                </div>

            </div>

            <div class="current-time-badge">

                ▶ {current_period.get(
                    "display_label",
                    "-"
                )}

            </div>

        </div>


        <div class="signal-btc-bar">

            <div class="signal-btc-title">
                BTC 현재 4H
            </div>

            <div class="signal-btc-period">

                {current_period.get(
                    "display_label",
                    "-"
                )}

            </div>

            <div class="signal-btc-value">

                {format_change(
                    btc_change
                )}

            </div>

        </div>


        <div class="signal-card-list">

            {body}

        </div>

    </div>

    """


# =========================================================
# TOP Section
# =========================================================

def top_card_html(
    row
):

    return unified_card_html(
        row,
        "TOP",
        row.get(
            "rank",
            "-"
        )
    )


def section(
    data,
    update_time
):

    current_period = (
        get_current_4h_period()
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

                top_card_html(
                    row
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
                    · 당일 변동률
                    · 최근 6개 4H
                    · 캔들 패턴

                </div>

            </div>

            <div class="current-time-badge">

                ▶ {current_period.get(
                    "display_label",
                    "-"
                )}

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
   공통 SECTION
   ========================================================= */

.unified-section{
width:100%;
margin:10px 0 14px;
}

.section-title-card{
display:flex;
align-items:center;
width:100%;
min-height:50px;
padding:8px 10px;
background:#10151b;
border:2px solid #252e38;
border-radius:12px;
overflow:hidden;
}

.section-number{
flex:none;
display:flex;
align-items:center;
justify-content:center;
width:35px;
height:35px;
margin-right:9px;
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
white-space:nowrap;
}

.section-heading-sub{
margin-top:2px;
color:#87919b;
font-size:7px;
line-height:10px;
font-weight:700;
white-space:nowrap;
overflow:hidden;
text-overflow:ellipsis;
}

.current-time-badge{
flex:none;
display:flex;
align-items:center;
justify-content:center;
min-height:28px;
margin-left:8px;
padding:5px 8px;
border-radius:7px;
background:#173326;
border:1px solid #4f9b73;
color:#8fe0b2;
font-size:7px;
font-weight:900;
white-space:nowrap;
}


/* =========================================================
   BTC
   ========================================================= */

.market-card{
width:100%;
margin:3px 0 14px;
background:#0f141a;
border:2px solid #252e38;
border-radius:13px;
overflow:hidden;
}

.market-card-header{
display:flex;
align-items:center;
min-height:44px;
padding:7px 10px;
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
line-height:14px;
font-weight:900;
}

.market-title-sub{
margin-top:2px;
color:#7e8994;
font-size:6.5px;
font-weight:700;
white-space:nowrap;
overflow:hidden;
text-overflow:ellipsis;
}

.market-time{
flex:none;
margin-left:8px;
color:#68737e;
font-size:6.5px;
font-weight:800;
}

.btc-main-row{
display:grid;
grid-template-columns:
    1fr
    1.3fr
    1fr
    1.2fr;
align-items:center;
min-height:58px;
background:#11161c;
}

.btc-name{
padding-left:13px;
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
min-height:58px;
display:flex;
align-items:center;
justify-content:center;
gap:5px;
border-left:1px solid #29323c;
}

.btc-info{
color:#7f8a94;
font-size:8px;
font-weight:900;
}

.btc-on{
color:#78cfa2;
font-size:10px;
font-weight:900;
}

.btc-off{
color:#df8588;
font-size:10px;
font-weight:900;
}

.btc-current-box{
display:flex;
align-items:center;
justify-content:center;
gap:12px;
min-height:48px;
background:#101820;
border-top:1px solid #29323c;
}

.btc-current-title{
color:#7d8993;
font-size:7px;
font-weight:900;
}

.btc-current-period{
color:#b9f0cf;
font-size:8px;
font-weight:900;
}

.btc-current-value{
font-size:11px;
font-weight:900;
}

.btc-4h-grid{
display:grid;
grid-template-columns:
    repeat(6,1fr);
gap:1px;
background:#29323c;
border-top:1px solid #29323c;
}


/* =========================================================
   공통 4H
   ========================================================= */

.four-hour-cell{
display:flex;
flex-direction:column;
align-items:center;
justify-content:center;
min-height:70px;
background:#0d1319;
gap:3px;
padding:4px 2px;
}

.four-hour-label{
color:#68747e;
font-size:6px;
font-weight:800;
}

.four-hour-time{
color:#b6bec5;
font-size:7px;
font-weight:900;
}

.four-hour-value{
font-size:8px;
font-weight:900;
}

.candle-pattern{
color:#e0bd6d;
font-size:6px;
line-height:8px;
font-weight:900;
text-align:center;
white-space:nowrap;
overflow:hidden;
text-overflow:ellipsis;
max-width:100%;
}

.current-4h{
background:#173326!important;
box-shadow:
    inset 0 0 0 1px rgba(116,213,157,0.18),
    inset 0 0 15px rgba(78,164,111,0.08);
}

.current-4h .four-hour-label{
color:#91dcb0;
}

.current-4h .four-hour-time{
color:#b9f0cf;
}

.current-4h .candle-pattern{
color:#f0d486;
}

.no-4h-data{
display:flex;
align-items:center;
justify-content:center;
min-height:50px;
color:#59636e;
}


/* =========================================================
   SIGNAL / TOP 카드
   ========================================================= */

.top-card-list,
.signal-card-list{
display:flex;
flex-direction:column;
gap:8px;
}

.unified-market-card{
width:100%;
background:#0f141a;
border:2px solid #252e38;
border-radius:13px;
overflow:hidden;
}

.signal-market-card{
border-color:#31513f;
}

.unified-card-header{
display:flex;
align-items:center;
min-height:44px;
padding:7px 10px;
background:#121820;
border-bottom:1px solid #29323c;
}

.signal-header{
background:#14231c;
border-bottom-color:#31513f;
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
padding:4px 7px;
border-radius:6px;
background:#183528;
border:1px solid #4f9b73;
color:#8fe0b2;
font-size:6px;
font-weight:900;
}

.unified-main-row{
display:grid;
grid-template-columns:
    1.2fr
    1fr
    1fr;
min-height:58px;
background:#11161c;
}

.unified-main-item{
display:flex;
flex-direction:column;
align-items:center;
justify-content:center;
gap:4px;
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

.unified-current-row{
display:grid;
grid-template-columns:
    1fr
    1.4fr
    1fr;
align-items:center;
min-height:42px;
background:#101820;
border-top:1px solid #29323c;
border-bottom:1px solid #29323c;
text-align:center;
}

.unified-current-title{
color:#7d8993;
font-size:7px;
font-weight:900;
}

.unified-current-period{
color:#8fe0b2;
font-size:7px;
font-weight:900;
}

.unified-current-value{
font-size:9px;
font-weight:900;
}

.unified-4h-grid{
display:grid;
grid-template-columns:
    repeat(6,1fr);
gap:1px;
background:#29323c;
}

.unified-condition-row{
display:grid;
grid-template-columns:
    1fr
    1fr;
min-height:50px;
background:#0f171d;
border-top:1px solid #29343d;
}

.unified-condition{
display:flex;
flex-direction:column;
align-items:center;
justify-content:center;
gap:3px;
}

.unified-condition + .unified-condition{
border-left:1px solid #29343d;
}

.condition-label{
color:#68747e;
font-size:6px;
font-weight:800;
}

.condition-period{
color:#aeb7be;
font-size:6px;
font-weight:800;
}

.condition-value{
font-size:8px;
font-weight:900;
}


/* =========================================================
   SIGNAL BTC BAR
   ========================================================= */

.signal-btc-bar{
display:grid;
grid-template-columns:
    1fr
    1.4fr
    1fr;
align-items:center;
min-height:42px;
margin-top:8px;
background:#111820;
border:1px solid #29343d;
border-radius:8px;
}

.signal-btc-title{
text-align:center;
color:#78858f;
font-size:7px;
font-weight:900;
}

.signal-btc-period{
text-align:center;
color:#b9f0cf;
font-size:7px;
font-weight:900;
}

.signal-btc-value{
text-align:center;
font-size:9px;
font-weight:900;
}


/* =========================================================
   TOP 업데이트
   ========================================================= */

.top-update-bar{
display:flex;
justify-content:space-between;
align-items:center;
padding:6px 4px;
color:#65717b;
font-size:6.5px;
font-weight:800;
}


/* =========================================================
   SIGNAL 없음
   ========================================================= */

.signal-empty-card{
display:flex;
flex-direction:column;
align-items:center;
justify-content:center;
min-height:120px;
margin-top:8px;
background:#10161c;
border:1px solid #29333d;
border-radius:11px;
text-align:center;
padding:15px;
}

.signal-empty-icon{
font-size:20px;
margin-bottom:5px;
}

.signal-empty-title{
color:#e1e7eb;
font-size:10px;
font-weight:900;
}

.signal-empty-text{
margin-top:7px;
color:#77838d;
font-size:7px;
font-weight:800;
}

.signal-empty-sub{
margin-top:6px;
color:#59646e;
font-size:6px;
font-weight:700;
}


/* =========================================================
   색상
   ========================================================= */

.up{
color:#78cfa2!important;
font-weight:900;
}

.down{
color:#df8588!important;
font-weight:900;
}

.zero{
color:#727c86!important;
font-weight:900;
}

.empty-card{
min-height:70px;
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


/* =========================================================
   모바일
   ========================================================= */

@media(max-width:600px){

body{
    padding:6px;
}

h1{
    margin:3px 3px 8px;
    font-size:12px;
    line-height:15px;
}

.unified-section{
    margin:8px 0 10px;
}

.section-title-card{
    min-height:40px;
    padding:5px 6px;
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

.current-time-badge{
    min-height:22px;
    margin-left:5px;
    padding:3px 5px;
    border-radius:5px;
    font-size:4.8px;
}


/* BTC */

.market-card{
    margin:3px 0 9px;
    border-radius:9px;
}

.market-card-header{
    min-height:36px;
    padding:5px 7px;
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
    min-height:43px;
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
    min-height:43px;
}

.btc-info{
    font-size:6px;
}

.btc-on,
.btc-off{
    font-size:7px;
}

.btc-current-box{
    min-height:38px;
    gap:7px;
}

.btc-current-title{
    font-size:5px;
}

.btc-current-period{
    font-size:5.5px;
}

.btc-current-value{
    font-size:7px;
}

.btc-4h-grid{
    grid-template-columns:
        repeat(3,1fr);
}

.four-hour-cell{
    min-height:52px;
}

.four-hour-label{
    font-size:4px;
}

.four-hour-time{
    font-size:5px;
}

.four-hour-value{
    font-size:6px;
}

.candle-pattern{
    font-size:4.5px;
    line-height:6px;
}


/* TOP / SIGNAL */

.unified-market-card{
    border-radius:9px;
}

.unified-card-header{
    min-height:36px;
    padding:5px 7px;
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
    padding:3px 5px;
}

.unified-main-row{
    min-height:48px;
}

.unified-label{
    font-size:4.5px;
}

.unified-price,
.unified-volume,
.unified-daily{
    font-size:7px;
}

.unified-current-row{
    min-height:34px;
}

.unified-current-title,
.unified-current-period{
    font-size:5px;
}

.unified-current-value{
    font-size:7px;
}

.unified-4h-grid{
    grid-template-columns:
        repeat(3,1fr);
}

.unified-condition-row{
    min-height:42px;
}

.condition-label{
    font-size:4.5px;
}

.condition-period{
    font-size:4px;
}

.condition-value{
    font-size:7px;
}

.top-update-bar{
    font-size:5px;
}

.signal-btc-bar{
    min-height:34px;
}

.signal-btc-title,
.signal-btc-period{
    font-size:5px;
}

.signal-btc-value{
    font-size:7px;
}

.signal-empty-card{
    min-height:90px;
}

.signal-empty-icon{
    font-size:16px;
}

.signal-empty-title{
    font-size:8px;
}

.signal-empty-text{
    font-size:5.5px;
}

.signal-empty-sub{
    font-size:4.5px;
}

}


/* =========================================================
   작은 모바일
   ========================================================= */

@media(max-width:380px){

body{
    padding:4px;
}

h1{
    font-size:11px;
}

.section-title-card{
    min-height:35px;
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

.current-time-badge{
    min-height:19px;
    padding:2px 4px;
    font-size:4px;
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
    font-size:7px;
}

.btc-price{
    font-size:7px;
}

.btc-change{
    font-size:7px;
}

.btc-signal-box{
    min-height:38px;
}

.btc-info{
    font-size:5px;
}

.btc-on,
.btc-off{
    font-size:6px;
}

.btc-current-box{
    min-height:34px;
}

.btc-current-title{
    font-size:4.5px;
}

.btc-current-period{
    font-size:4.5px;
}

.btc-current-value{
    font-size:6px;
}

.four-hour-cell{
    min-height:45px;
}

.four-hour-label{
    font-size:3.7px;
}

.four-hour-time{
    font-size:4px;
}

.four-hour-value{
    font-size:5px;
}

.candle-pattern{
    font-size:3.8px;
    line-height:5px;
}

.unified-card-header{
    min-height:32px;
}

.unified-card-rank{
    font-size:6px;
}

.unified-card-coin{
    font-size:7px;
}

.unified-card-title{
    font-size:4px;
}

.signal-badge{
    font-size:3.5px;
}

.unified-main-row{
    min-height:43px;
}

.unified-label{
    font-size:4px;
}

.unified-price,
.unified-volume,
.unified-daily{
    font-size:6px;
}

.unified-current-row{
    min-height:29px;
}

.unified-current-title,
.unified-current-period{
    font-size:4px;
}

.unified-current-value{
    font-size:6px;
}

.unified-condition-row{
    min-height:37px;
}

.condition-label{
    font-size:4px;
}

.condition-period{
    font-size:3.5px;
}

.condition-value{
    font-size:6px;
}

}


/* =========================================================
   강조
   ========================================================= */

.top-market-card .current-4h{
    background:#173326!important;
}

.signal-market-card .current-4h{
    background:#173326!important;
}

"""


# =========================================================
# Dashboard
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def dashboard():

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
# 설정 검증
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

@app.on_event(
    "startup"
)
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
        "4H 기준 = 01 / 05 / 09 / 13 / 17 / 21"
    )

    log.info(
        "당일 변동률 = 업비트 일봉"
    )

    log.info(
        "SIGNAL 조건:"
    )

    log.info(
        "1. BTC 현재 4H > 0"
    )

    log.info(
        "2. 업비트 당일 변동률 > 0"
    )

    log.info(
        "3. 코인 현재 4H > 0"
    )

    log.info(
        "이전 4H = 화면 표시만 사용"
    )

    log.info(
        "이전 4H는 SIGNAL 필터에서 제외"
    )

    log.info(
        "캔들 패턴 = 화면 표시만 사용"
    )

    log.info(
        "캔들 패턴: 도지 / 망치형 / 역망치형 / 상승장악 / 관통형 / 모닝스타 / 3연속양봉"
    )

    log.info(
        "RSI = 삭제"
    )

    log.info(
        "ROC = 삭제"
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
