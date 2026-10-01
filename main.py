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

TOP_N = 30

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

latest_btc_daily_periods = []

latest_btc_4h_periods = []

latest_btc_current_daily_change = None

latest_btc_current_daily_label = "-"

latest_btc_current_4h_change = None

latest_btc_current_4h_label = "-"


# =========================================================
# 업비트 4H 기준
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

    today = datetime.now(KST).date()

    if start.date() == today:

        day_label = "오늘"

    elif start.date() == (
        today -
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

    periods = get_recent_4h_periods(1)

    if not periods:

        return None

    return periods[0]


# =========================================================
# 이전 4H
# =========================================================

def get_previous_4h_period():

    current = get_current_4h_period()

    if current is None:

        return None

    start = (
        current["start"]
        -
        timedelta(hours=4)
    )

    return make_4h_period(
        start,
        active=False
    )


# =========================================================
# 전전 4H
# =========================================================

def get_pre_previous_4h_period():

    current = get_current_4h_period()

    if current is None:

        return None

    start = (
        current["start"]
        -
        timedelta(hours=8)
    )

    return make_4h_period(
        start,
        active=False
    )


# =========================================================
# 전전전 4H
# =========================================================

def get_pre_pre_previous_4h_period():

    current = get_current_4h_period()

    if current is None:

        return None

    start = (
        current["start"]
        -
        timedelta(hours=12)
    )

    return make_4h_period(
        start,
        active=False
    )


# =========================================================
# 전전전전 4H
# =========================================================

def get_pre_pre_pre_previous_4h_period():

    current = get_current_4h_period()

    if current is None:

        return None

    start = (
        current["start"]
        -
        timedelta(hours=16)
    )

    return make_4h_period(
        start,
        active=False
    )


# =========================================================
# 현재 일봉 시작시간
#
# 업비트 일봉 기준
# KST 09:00 ~ 다음날 09:00
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
# 일봉 기간 만들기
# =========================================================

def make_daily_period(
    start,
    active=False
):

    start = start.astimezone(KST)

    end = (
        start
        +
        timedelta(days=1)
    )

    today_start = (
        get_current_daily_start()
    )

    if start == today_start:

        day_label = "오늘"

    elif start == (
        today_start -
        timedelta(days=1)
    ):

        day_label = "어제"

    else:

        diff_days = (
            today_start.date()
            -
            start.date()
        ).days

        if diff_days == 2:

            day_label = "2일전"

        elif diff_days == 3:

            day_label = "3일전"

        elif diff_days == 4:

            day_label = "4일전"

        elif diff_days == 5:

            day_label = "5일전"

        else:

            day_label = "이전"


    if active:

        display_label = (
            f"현재 {start:%m/%d}"
        )

    else:

        display_label = (
            f"{day_label} "
            f"{start:%m/%d}"
        )


    return {

        "key":
            f"{start:%Y%m%d}",

        "time_key":
            start.strftime("%Y%m%d"),

        "label":
            f"{start:%m/%d} 09:00",

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
# 최근 6개 일봉
# =========================================================

def get_recent_daily_periods(
    count=6
):

    current_start = (
        get_current_daily_start()
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
            timedelta(days=i)
        )

        periods.append(
            make_daily_period(
                start,
                active=(i == 0)
            )
        )

    return periods


# =========================================================
# 현재 일봉
# =========================================================

def get_current_daily_period():

    periods = get_recent_daily_periods(1)

    if not periods:

        return None

    return periods[0]


# =========================================================
# 이전 일봉
# =========================================================

def get_previous_daily_period():

    current = get_current_daily_period()

    if current is None:

        return None

    start = (
        current["start"]
        -
        timedelta(days=1)
    )

    return make_daily_period(
        start,
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
                f"API 오류 {url} "
                f"{attempt + 1}/{MAX_RETRIES}: "
                f"{e}"
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
            current_price -
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
# 업비트 일봉
# =========================================================

def get_upbit_daily_candles(
    market,
    count=10
):

    response = retry(
        requests.get,
        "https://api.upbit.com/v1/candles/days",
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

        o = float(candle["open"])
        h = float(candle["high"])
        l = float(candle["low"])
        c = float(candle["close"])

    except Exception:

        return None


    total = h - l

    if total <= 0:

        return None


    body = abs(c - o)

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
        "body_ratio": body_ratio,
        "bull": c > o,
        "bear": c < o

    }


# =========================================================
# 1개 캔들 패턴
#
# 도지
# 망치형
# 역망치형
# =========================================================

def detect_single_candle_pattern(
    candle
):

    p = candle_parts(candle)

    if p is None:

        return []


    patterns = []

    body = p["body"]
    total = p["total"]
    upper = p["upper"]
    lower = p["lower"]
    body_ratio = p["body_ratio"]


    if body_ratio <= 0.10:

        patterns.append("도지")


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

        patterns.append("망치형")


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

        patterns.append("역망치형")


    return patterns


# =========================================================
# 2개 캔들 패턴
#
# 상승장악
# 관통형
#
# 하락장악
# 먹구름형
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


    # -----------------------------------------------------
    # 상승장악
    # -----------------------------------------------------

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

        patterns.append("상승장악")


    # -----------------------------------------------------
    # 관통형
    #
    # 일반 캔들 패턴 표시용
    # SIGNAL에서는 사용하지 않음
    # -----------------------------------------------------

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

        patterns.append("관통형")


    # -----------------------------------------------------
    # 하락장악
    # -----------------------------------------------------

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

        patterns.append("하락장악")


    # -----------------------------------------------------
    # 먹구름형
    # -----------------------------------------------------

    if (

        p1["bull"]

        and

        p2["bear"]

        and

        p2["close"] < midpoint

        and

        p2["close"] > p1["open"]

    ):

        patterns.append("먹구름형")


    return patterns


# =========================================================
# 3개 캔들 패턴
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


    # -----------------------------------------------------
    # 모닝스타
    # -----------------------------------------------------

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

        patterns.append("모닝스타")


    # -----------------------------------------------------
    # 3연속양봉
    # -----------------------------------------------------

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

        patterns.append("3연속양봉")


    # -----------------------------------------------------
    # 3캔들 상승패턴
    # -----------------------------------------------------

    if (

        p1["bear"]

        and

        p3["bull"]

    ):

        first_body_high = max(
            p1["open"],
            p1["close"]
        )

        first_body_low = min(
            p1["open"],
            p1["close"]
        )

        first_midpoint = (
            p1["open"] +
            p1["close"]
        ) / 2


        # 3캔들 상승장악

        if (

            p3["open"] <= first_body_low

            and

            p3["close"] >= first_body_high

            and

            p3["body"] > p1["body"]

        ):

            patterns.append(
                "3캔들 상승장악"
            )


        # 3캔들 관통형
        #
        # 일반 캔들 패턴 표시용
        # SIGNAL에서는 사용하지 않음

        if (

            p3["close"] > first_midpoint

            and

            p3["close"] < p1["open"]

        ):

            patterns.append(
                "3캔들 관통형"
            )


    # -----------------------------------------------------
    # 3캔들 하락패턴
    # -----------------------------------------------------

    if (

        (
            p1["bull"]
            or
            p1["bear"]
        )

        and

        p3["bear"]

    ):

        first_body_high = max(
            p1["open"],
            p1["close"]
        )

        first_body_low = min(
            p1["open"],
            p1["close"]
        )

        first_midpoint = (
            p1["open"] +
            p1["close"]
        ) / 2


        if (

            p1["bull"]

            and

            p3["open"] >= first_body_high

            and

            p3["close"] <= first_body_low

            and

            p3["body"] > p1["body"]

        ):

            patterns.append(
                "3캔들 하락장악"
            )


        if (

            p1["bull"]

            and

            p3["close"] < first_midpoint

            and

            p3["close"] > first_body_low

        ):

            patterns.append(
                "3캔들 먹구름형"
            )


    return patterns


# =========================================================
# 4개 캔들 패턴
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

    if (
        p1 is None
        or
        p2 is None
        or
        p3 is None
        or
        p4 is None
    ):

        return []


    patterns = []


    if (

        p1["bear"]

        and

        p4["bull"]

    ):

        first_body_high = max(
            p1["open"],
            p1["close"]
        )

        first_body_low = min(
            p1["open"],
            p1["close"]
        )

        first_midpoint = (
            p1["open"] +
            p1["close"]
        ) / 2


        # -------------------------------------------------
        # 4캔들 상승장악
        # -------------------------------------------------

        if (

            p4["open"] <= first_body_low

            and

            p4["close"] >= first_body_high

            and

            p4["body"] > p1["body"]

        ):

            patterns.append(
                "4캔들 상승장악"
            )


        # -------------------------------------------------
        # 4캔들 관통형
        #
        # 일반 캔들 패턴 표시용
        # SIGNAL에서는 사용하지 않음
        # -------------------------------------------------

        if (

            p4["close"] > first_midpoint

            and

            p4["close"] < p1["open"]

        ):

            patterns.append(
                "4캔들 관통형"
            )


    return patterns


# =========================================================
# 전체 캔들 패턴
# =========================================================

def detect_4h_patterns(
    periods
):

    if not periods:

        return []


    result = []


    for i, period in enumerate(periods):

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

            previous = periods[i - 1]

            if all(
                previous.get(x) is not None
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

            c1 = periods[i - 2]
            c2 = periods[i - 1]
            c3 = period

            if all(
                x.get(key) is not None
                for x in [c1, c2, c3]
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

            c1 = periods[i - 3]
            c2 = periods[i - 2]
            c3 = periods[i - 1]
            c4 = period

            if all(
                x.get(key) is not None
                for x in [c1, c2, c3, c4]
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


        patterns = list(
            dict.fromkeys(patterns)
        )

        result.append(patterns)


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

                "datetime": dt,

                "open": float(
                    candle[
                        "opening_price"
                    ]
                ),

                "high": float(
                    candle[
                        "high_price"
                    ]
                ),

                "low": float(
                    candle[
                        "low_price"
                    ]
                ),

                "close": float(
                    candle[
                        "trade_price"
                    ]
                )

            })

        except Exception:

            continue


    if not rows:

        return []


    df = pd.DataFrame(rows)


    df = (
        df
        .sort_values("datetime")
        .drop_duplicates("datetime")
    )


    periods = get_recent_4h_periods(6)

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


        if open_price == 0:

            change = None

        else:

            change = (
                (
                    close_price -
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


    pattern_results = detect_4h_patterns(
        result
    )


    for i, pattern_list in enumerate(
        pattern_results
    ):

        result[i]["patterns"] = (
            pattern_list
        )


    return result


# =========================================================
# 업비트 최근 6개 일봉
# =========================================================

def build_upbit_daily_candles(
    market,
    current_price=None
):

    candles = get_upbit_daily_candles(
        market,
        10
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


    df = pd.DataFrame(rows)


    df = (
        df
        .sort_values("datetime")
        .drop_duplicates("datetime")
    )


    periods = get_recent_daily_periods(6)

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


        if open_price == 0:

            change = None

        else:

            change = (
                (
                    close_price -
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


    pattern_results = detect_4h_patterns(
        result
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

        start = period["start"]


        if (
            current_period is not None
            and
            start ==
            current_period["start"]
        ):

            current_change = (
                period["change"]
            )


        if (
            previous_period is not None
            and
            start ==
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
# 코인의 일봉 분석
# =========================================================

def analyze_daily(
    market,
    current_price
):

    periods = build_upbit_daily_candles(
        market,
        current_price
    )


    current_period = (
        get_current_daily_period()
    )

    previous_period = (
        get_previous_daily_period()
    )


    current_change = None
    previous_change = None


    for period in periods:

        start = period["start"]


        if (
            current_period is not None
            and
            start ==
            current_period["start"]
        ):

            current_change = (
                period["change"]
            )


        if (
            previous_period is not None
            and
            start ==
            previous_period["start"]
        ):

            previous_change = (
                period["change"]
            )


    return {

        "periods":
            periods,

        "current_daily_change":
            current_change,

        "previous_daily_change":
            previous_change

    }


# =========================================================
# 코인 전체 분석
# =========================================================

def analyze(
    market,
    current_price
):

    four_hour = analyze_4h(
        market,
        current_price
    )


    daily = analyze_daily(
        market,
        current_price
    )


    daily_change = (
        daily[
            "current_daily_change"
        ]
    )


    return {

        "daily_change":
            daily_change,

        "daily_periods":
            daily[
                "periods"
            ],

        "current_daily_change":
            daily[
                "current_daily_change"
            ],

        "previous_daily_change":
            daily[
                "previous_daily_change"
            ],

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

    x = get_change_value(x)


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
    current_price,
    volume_rank=None
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

        "daily_periods":
            analysis.get(
                "daily_periods",
                []
            ),

        "current_daily_change":
            get_change_value(
                analysis.get(
                    "current_daily_change"
                )
            ),

        "previous_daily_change":
            get_change_value(
                analysis.get(
                    "previous_daily_change"
                )
            ),

        "four_hour_periods":
            analysis.get(
                "four_hour_periods",
                []
            ),

        "current_4h_change":
            get_change_value(
                analysis.get(
                    "current_4h_change"
                )
            ),

        "previous_4h_change":
            get_change_value(
                analysis.get(
                    "previous_4h_change"
                )
            )

    }


# =========================================================
# SIGNAL 패턴 목록
#
# 관통형 제외
#
# SIGNAL에서는 상승장악 계열만 사용
# =========================================================

SIGNAL_CANDLE_PATTERNS = [

    "상승장악",

    "3캔들 상승장악",

    "4캔들 상승장악"

]


# =========================================================
# SIGNAL 조건 계산
# =========================================================

def calculate_signal_conditions(
    row
):

    # =====================================================
    # 일봉 SIGNAL 조건
    # =====================================================

    daily_change = (
        row.get(
            "current_daily_change"
        )
    )


    daily_condition = (

        daily_change is not None

        and

        daily_change > 0

    )


    daily_periods = row.get(
        "daily_periods",
        []
    )


    current_daily_candle = None


    if daily_periods:

        current_daily_candle = (
            daily_periods[-1]
        )


    daily_current_bullish_pattern = False

    daily_signal_patterns = []


    if current_daily_candle is not None:

        current_patterns = (
            current_daily_candle.get(
                "patterns",
                []
            )
        )


        daily_signal_patterns = [

            pattern

            for pattern in SIGNAL_CANDLE_PATTERNS

            if pattern in current_patterns

        ]


        daily_current_bullish_pattern = bool(
            daily_signal_patterns
        )


    daily_current_positive = (

        daily_change is not None

        and

        daily_change > 0

    )


    row["daily_signal_pass"] = (

        daily_condition

        and

        daily_current_positive

        and

        daily_current_bullish_pattern

    )


    row["daily_signal_conditions"] = {

        "daily":
            daily_condition,

        "current_daily_positive":
            daily_current_positive,

        "current_daily_bullish_pattern":
            daily_current_bullish_pattern,

        "current_signal_patterns":
            daily_signal_patterns,

        "current_daily_used":
            True

    }


    # =====================================================
    # 4H SIGNAL
    # =====================================================

    daily_condition_4h = (

        row.get(
            "daily_change"
        )
        is not None

        and

        row.get(
            "daily_change"
        ) > 0

    )


    periods = row.get(
        "four_hour_periods",
        []
    )


    current_candle = None


    if len(periods) >= 1:

        current_candle = periods[-1]


    current_bullish_pattern = False

    current_signal_patterns = []


    if current_candle is not None:

        current_patterns = (
            current_candle.get(
                "patterns",
                []
            )
        )


        current_signal_patterns = [

            pattern

            for pattern in SIGNAL_CANDLE_PATTERNS

            if pattern in current_patterns

        ]


        current_bullish_pattern = bool(
            current_signal_patterns
        )


    current_4h_positive = (

        row.get(
            "current_4h_change"
        )
        is not None

        and

        row.get(
            "current_4h_change"
        ) > 0

    )


    row["signal_pass"] = (

        daily_condition_4h

        and

        current_4h_positive

        and

        current_bullish_pattern

    )


    row["signal_conditions"] = {

        "daily":
            daily_condition_4h,

        "current_4h_positive":
            current_4h_positive,

        "current_bullish_pattern":
            current_bullish_pattern,

        "current_signal_patterns":
            current_signal_patterns,

        "current_4h_used":
            True

    }


    return row


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


    volume_rank_map = {

        item["market"]:
            rank

        for rank, item in enumerate(
            markets,
            1
        )

    }


    top_markets = markets[:TOP_N]


    rows = []


    for rank, item in enumerate(
        top_markets,
        1
    ):

        market = item["market"]

        coin = market.replace(
            "KRW-",
            ""
        )

        price = item["current_price"]

        volume = item["volume_24h"]


        actual_volume_rank = (
            volume_rank_map.get(
                market
            )
        )


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
            actual_volume_rank
        )


        row = calculate_signal_conditions(
            row
        )


        rows.append(row)


    latest_upbit_data = rows

    latest_upbit_update_time = kst()


    daily_signal_count = sum(

        1

        for row in rows

        if row.get(
            "daily_signal_pass",
            False
        )

    )


    signal_count = sum(

        1

        for row in rows

        if row.get(
            "signal_pass",
            False
        )

    )


    dual_signal_count = sum(

        1

        for row in rows

        if (
            row.get(
                "signal_pass",
                False
            )
            and
            row.get(
                "daily_signal_pass",
                False
            )
        )

    )


    current_daily = (
        get_current_daily_period()
    )

    current_4h = (
        get_current_4h_period()
    )


    log.info(

        f"TOP{TOP_N} 업데이트 | "

        f"일봉="
        f"{current_daily['display_label'] if current_daily else '-'}"
        " | "

        f"4H="
        f"{current_4h['display_label'] if current_4h else '-'}"
        " | "

        f"일봉 SIGNAL="
        f"{daily_signal_count}"
        " | "

        f"4H SIGNAL="
        f"{signal_count}"
        " | "

        f"일봉+4H 동시 SIGNAL="
        f"{dual_signal_count}"

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


    if payload.get("code") != "0":

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

        params["after"] = str(after)


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


    if payload.get("code") != "0":

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


        rows.extend(data)


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


    df = pd.DataFrame(result)


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

def aggregate_btc_daily(df):

    if df is None or df.empty:

        return pd.DataFrame()


    temp = df.copy()


    temp["daily_start"] = (

        temp["datetime_kst"]

        -

        pd.Timedelta(hours=9)

    ).dt.floor("D") + pd.Timedelta(hours=9)


    return (
        temp
        .sort_values("datetime_kst")
        .groupby("daily_start")
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last")
        )
        .reset_index()
    )


# =========================================================
# BTC 일봉 6개
# =========================================================

def build_btc_daily(
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


    daily = aggregate_btc_daily(
        df
    )


    if daily.empty:

        return []


    periods = get_recent_daily_periods(
        6
    )


    temp = daily.copy()


    temp["daily_start"] = (
        temp["daily_start"]
        .dt.tz_convert(KST)
    )


    result = []


    for period in periods:

        start = period["start"]

        end = period["end"]


        part = temp[
            (
                temp["daily_start"]
                >= start
            )
            &
            (
                temp["daily_start"]
                < end
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


        if open_price == 0:

            change = None

        else:

            change = (
                (
                    close_price -
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


    pattern_results = detect_4h_patterns(
        result
    )


    for i, pattern_list in enumerate(
        pattern_results
    ):

        result[i]["patterns"] = (
            pattern_list
        )


    return result


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
            today_0900 -
            timedelta(days=1)
        )

    else:

        current_start = today_0900


    daily["daily_start"] = (
        daily["daily_start"]
        .dt
        .tz_localize(None)
    )


    target = current_start.replace(
        tzinfo=None
    )


    previous = daily[
        daily["daily_start"] < target
    ]


    if previous.empty:

        return None


    previous_close = float(
        previous.iloc[-1]["close"]
    )


    if previous_close == 0:

        return None


    return (
        (
            price -
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


    periods = get_recent_4h_periods(6)


    result = []


    for period in periods:

        start = period["start"].replace(
            tzinfo=None
        )

        end = period["end"].replace(
            tzinfo=None
        )


        part = temp[
            (
                temp["kst_naive"] >= start
            )
            &
            (
                temp["kst_naive"] < end
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

            close_price = float(price)

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
                    close_price -
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


    pattern_results = detect_4h_patterns(
        result
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
    global latest_btc_daily_periods
    global latest_btc_4h_periods
    global latest_btc_current_daily_change
    global latest_btc_current_daily_label
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


    latest_btc_daily_periods = (
        build_btc_daily(
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


    current_daily = (
        get_current_daily_period()
    )


    if current_daily is not None:

        latest_btc_current_daily_label = (
            current_daily[
                "display_label"
            ]
        )


        for period in latest_btc_daily_periods:

            if (
                period["start"]
                ==
                current_daily["start"]
            ):

                latest_btc_current_daily_change = (
                    period["change"]
                )

                break


    current = get_current_4h_period()


    if current is None:

        return


    latest_btc_current_4h_label = (
        current["display_label"]
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

def update_okx(usdt):

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

            usdt = get_usdt_krw()

            if usdt:

                update_okx(usdt)


    except Exception as e:

        log.exception(
            f"전체 업데이트 오류: {e}"
        )


    finally:

        update_lock.release()


# =========================================================
# 캔들 패턴 HTML
# =========================================================

def candle_pattern_html(patterns):

    if not patterns:

        return ""


    return (
        '<div class="candle-pattern">'
        +
        " · ".join(
            html.escape(str(x))
            for x in patterns
        )
        +
        '</div>'
    )


# =========================================================
# 기간 셀 HTML
# =========================================================

def period_cells_html(
    periods,
    period_type="4H"
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


        time_text = (
            period.get(
                "label",
                "-"
            )
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
                        str(time_text)
                    )}
                </div>

                <div class="four-hour-value">
                    {format_change(
                        period.get("change")
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


    return "".join(cells)


# =========================================================
# 기존 4H 셀
# =========================================================

def four_hour_cells_html(periods):

    return period_cells_html(
        periods,
        "4H"
    )


# =========================================================
# 일봉 셀
# =========================================================

def daily_cells_html(periods):

    return period_cells_html(
        periods,
        "일봉"
    )


# =========================================================
# BTC 시장 카드
# =========================================================

def market_summary_html():

    price = format_market_price(
        latest_btc_okx_price
    )

    daily = format_change(
        latest_btc_daily_change
    )


    if (
        latest_btc_current_daily_change
        is not None
        and
        latest_btc_current_daily_change > 0
    ):

        daily_status = "상승"

        daily_status_class = "btc-on"

    elif (
        latest_btc_current_daily_change
        is not None
        and
        latest_btc_current_daily_change < 0
    ):

        daily_status = "하락"

        daily_status_class = "btc-off"

    else:

        daily_status = "-"

        daily_status_class = "btc-off"


    if (
        latest_btc_current_4h_change
        is not None
        and
        latest_btc_current_4h_change > 0
    ):

        market_status = "상승"

        market_status_class = "btc-on"

    elif (
        latest_btc_current_4h_change
        is not None
        and
        latest_btc_current_4h_change < 0
    ):

        market_status = "하락"

        market_status_class = "btc-off"

    else:

        market_status = "-"

        market_status_class = "btc-off"


    return f"""

    <div class="market-card">

        <div class="market-card-header">

            <div class="market-title-block">

                <div class="market-title-main">
                    ₿ BTC 시장 시황
                </div>

                <div class="market-title-sub">
                    OKX BTC-USDT
                    · 일봉 KST 09:00 기준
                    · 4H 업비트 시간 기준
                    · SIGNAL 필터 제외
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
                    MARKET
                </span>

                <span class="{daily_status_class}">
                    일봉 {daily_status}
                </span>

                <span class="{market_status_class}">
                    4H {market_status}
                </span>

            </div>

        </div>


        <div class="btc-timeframe-title">
            📅 BTC 일봉
        </div>

        <div class="btc-4h-grid">

            {daily_cells_html(
                latest_btc_daily_periods
            )}

        </div>


        <div class="btc-timeframe-title">
            ⏱ BTC 4시간
        </div>

        <div class="btc-4h-grid">

            {four_hour_cells_html(
                latest_btc_4h_periods
            )}

        </div>

    </div>

    """


# =========================================================
# 공통 카드
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


    if card_type == "SIGNAL_DAILY":

        title = "📅 일봉 SIGNAL"

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


        if volume_rank is not None:

            volume_rank_text = (
                f"거래대금 {volume_rank}위"
            )

        else:

            volume_rank_text = (
                "거래대금 -"
            )


        badge = f"""
        <span class="signal-badge">
            {volume_rank_text}
        </span>
        """


        signal_patterns = row.get(
            "daily_signal_conditions",
            {}
        ).get(
            "current_signal_patterns",
            []
        )


        if signal_patterns:

            pattern_text = (
                " / ".join(
                    signal_patterns
                )
            )

        else:

            pattern_text = "-"


        condition_html = f"""

        <div class="unified-condition-row signal-condition-only">

            <div class="unified-condition">

                <div class="condition-label">
                    일봉 SIGNAL 조건
                </div>

                <div class="condition-period">
                    당일 양수 + 현재 일봉 양봉
                </div>

                <div class="condition-value">

                    <span class="up">
                        {html.escape(
                            pattern_text
                        )}
                    </span>

                    <span class="condition-note">
                        현재 일봉 실시간 판정
                    </span>

                </div>

            </div>

        </div>

        """


    elif card_type == "SIGNAL_4H":

        daily_signal_pass = row.get(
            "daily_signal_pass",
            False
        )


        if daily_signal_pass:

            title = "🔥 일봉 + 4H SIGNAL"

            card_class = (
                "unified-market-card "
                "signal-market-card "
                "dual-signal-card"
            )

            header_class = (
                "unified-card-header "
                "signal-header "
                "dual-signal-header"
            )

        else:

            title = "🚀 4H SIGNAL"

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


        if volume_rank is not None:

            volume_rank_text = (
                f"거래대금 {volume_rank}위"
            )

        else:

            volume_rank_text = (
                "거래대금 -"
            )


        if daily_signal_pass:

            badge = f"""
            <span class="signal-badge dual-signal-badge">
                {volume_rank_text} · 🔥 일봉+4H
            </span>
            """

        else:

            badge = f"""
            <span class="signal-badge">
                {volume_rank_text}
            </span>
            """


        signal_patterns = row.get(
            "signal_conditions",
            {}
        ).get(
            "current_signal_patterns",
            []
        )


        if signal_patterns:

            pattern_text = (
                " / ".join(
                    signal_patterns
                )
            )

        else:

            pattern_text = "-"


        if daily_signal_pass:

            daily_signal_patterns = row.get(
                "daily_signal_conditions",
                {}
            ).get(
                "current_signal_patterns",
                []
            )


            if daily_signal_patterns:

                daily_pattern_text = (
                    " / ".join(
                        daily_signal_patterns
                    )
                )

            else:

                daily_pattern_text = "-"


            condition_html = f"""

            <div class="
                unified-condition-row
                dual-signal-condition
            ">

                <div class="unified-condition">

                    <div class="condition-label">
                        🔥 일봉 + 4H SIGNAL 동시 충족
                    </div>


                    <div class="condition-period">
                        일봉 SIGNAL
                    </div>


                    <div class="condition-value">

                        <span class="up">
                            {html.escape(
                                daily_pattern_text
                            )}
                        </span>

                    </div>


                    <div class="condition-period">
                        4H SIGNAL
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

            condition_html = f"""

            <div class="
                unified-condition-row
                signal-condition-only
            ">

                <div class="unified-condition">

                    <div class="condition-label">
                        4H SIGNAL 조건
                    </div>

                    <div class="condition-period">
                        당일 양수 + 현재 4H 양봉
                    </div>

                    <div class="condition-value">

                        <span class="up">
                            {html.escape(
                                pattern_text
                            )}
                        </span>

                        <span class="condition-note">
                            현재 4H 실시간 판정
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


        <div class="timeframe-card-title">
            📅 일봉
        </div>

        <div class="unified-4h-grid">

            {daily_cells_html(
                row.get(
                    "daily_periods",
                    []
                )
            )}

        </div>


        <div class="timeframe-card-title">
            ⏱ 4시간
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
# 4H SIGNAL Section
# =========================================================

def focus_section(
    data
):

    current_period = (
        get_current_4h_period()
    )

    previous_period = (
        get_previous_4h_period()
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

        message = (
            "당일 양수 + "
            "현재 4H 양봉 + "
            "2캔들 상승장악 + "
            "3캔들 상승장악 + "
            "4캔들 상승장악 "
            "조건을 만족하는 종목 없음"
        )


        body = f"""

        <div class="signal-empty-card">

            <div class="signal-empty-icon">
                🔎
            </div>

            <div class="signal-empty-title">
                4H SIGNAL 없음
            </div>

            <div class="signal-empty-text">
                {message}
            </div>

            <div class="signal-empty-sub">

                현재 4H:
                {(
                    current_period
                    or {}
                ).get(
                    "display_label",
                    "-"
                )}

                ·

                이전 4H:
                {(
                    previous_period
                    or {}
                ).get(
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
                    "SIGNAL_4H",
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
                    4H SIGNAL
                </div>

                <div class="section-heading-sub">

                    당일 양수
                    · 현재 4H 양봉
                    · 2캔들 상승장악
                    · 3캔들 상승장악
                    · 4캔들 상승장악
                    · 🔥 일봉 SIGNAL 동시 충족 강조
                    · 거래대금 순위

                </div>

            </div>

            <div class="current-time-badge">

                ▶ {(
                    current_period
                    or {}
                ).get(
                    "display_label",
                    "-"
                )}

            </div>

        </div>


        <div class="signal-btc-bar">

            <div class="signal-btc-title">
                4H SIGNAL
            </div>

            <div class="signal-btc-period">
                상승장악 계열
            </div>

            <div class="signal-btc-value">
                거래대금 순위
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

def top_card_html(row):

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

    current_daily = (
        get_current_daily_period()
    )

    current_4h = (
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
                top_card_html(row)
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
                    · 최근 6개 일봉
                    · 최근 6개 4H
                    · 캔들 패턴

                </div>

            </div>

            <div class="current-time-badge">

                ▶ 일봉 {(
                    current_daily
                    or {}
                ).get(
                    "display_label",
                    "-"
                )}

                /

                4H {(
                    current_4h
                    or {}
                ).get(
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
        1.6fr;
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
    min-height:30px;
    display:flex;
    align-items:center;
    padding:5px 10px;
    background:#111820;
    border-top:1px solid #29323c;
    color:#aeb8c0;
    font-size:8px;
    font-weight:900;
}

.btc-4h-grid,
.unified-4h-grid{
    display:grid;
    grid-template-columns:
        repeat(6,1fr);
    gap:1px;
    background:#29323c;
    border-top:1px solid #29323c;
}

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
    background:#173326 !important;
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
    white-space:nowrap;
}


/* =========================================================
   일봉 + 4H 동시 SIGNAL 강조
   ========================================================= */

.dual-signal-card{
    border:2px solid #d9a83f !important;
    background:#15150f !important;
    box-shadow:
        0 0 0 1px rgba(224,189,109,0.25),
        0 0 18px rgba(224,189,109,0.10);
}

.dual-signal-header{
    background:#211d11 !important;
    border-bottom:1px solid #8a6c2e !important;
}

.dual-signal-header .unified-card-title{
    color:#f0d486 !important;
}

.dual-signal-header .unified-card-coin{
    color:#fff1b5 !important;
}

.dual-signal-badge{
    background:#3a2c12 !important;
    border-color:#d9a83f !important;
    color:#f0d486 !important;
}

.dual-signal-condition{
    background:#18160e !important;
    border-top:1px solid #8a6c2e !important;
}

.dual-signal-condition .condition-label{
    color:#f0d486 !important;
}

.dual-signal-condition .condition-period{
    color:#d8c078 !important;
}

.dual-signal-condition .condition-value{
    color:#8fe0b2 !important;
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


/* =========================================================
   당일 시세 양수 / 음수 강조
   ========================================================= */

.unified-main-item:has(.unified-daily .up){
    background:#10271c;
    box-shadow:
        inset 0 0 0 1px rgba(120,207,162,0.25);
}

.unified-main-item:has(.unified-daily .down){
    background:#2a1518;
    box-shadow:
        inset 0 0 0 1px rgba(223,133,136,0.25);
}

.unified-daily .up{
    color:#78cfa2 !important;
    font-size:11px;
    font-weight:900;
}

.unified-daily .down{
    color:#df8588 !important;
    font-size:11px;
    font-weight:900;
}

.unified-daily .zero{
    color:#727c86 !important;
    font-size:10px;
    font-weight:900;
}


/* =========================================================
   시간대 카드
   ========================================================= */

.timeframe-card-title{
    min-height:29px;
    display:flex;
    align-items:center;
    padding:5px 9px;
    background:#111820;
    border-top:1px solid #29323c;
    color:#9da8b1;
    font-size:7px;
    font-weight:900;
}

.unified-condition-row{
    width:100%;
    min-height:50px;
    background:#0f171d;
    border-top:1px solid #29343d;
}

.unified-condition{
    display:flex;
    flex-direction:column;
    align-items:center;
    justify-content:center;
    min-height:50px;
    gap:3px;
    padding:5px 8px;
}

.condition-label{
    color:#68747e;
    font-size:6px;
    font-weight:800;
}

.condition-period{
    color:#b9f0cf;
    font-size:7px;
    font-weight:900;
    text-align:center;
}

.condition-value{
    font-size:8px;
    font-weight:900;
}

.condition-note{
    color:#68747e;
    margin-left:5px;
    font-size:6px;
    font-weight:700;
}

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

.top-update-bar{
    display:flex;
    justify-content:space-between;
    align-items:center;
    padding:6px 4px;
    color:#65717b;
    font-size:6.5px;
    font-weight:800;
}

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
        grid-template-columns:
            0.8fr
            1.1fr
            0.9fr
            1.5fr;
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
        gap:3px;
    }

    .btc-info{
        font-size:5px;
    }

    .btc-on,
    .btc-off{
        font-size:6px;
    }

    .btc-timeframe-title{
        min-height:24px;
        padding:4px 7px;
        font-size:6px;
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

    /* =====================================================
       모바일 당일 시세 강조
       ===================================================== */

    .unified-daily .up,
    .unified-daily .down{
        font-size:8px;
    }

    .unified-daily .zero{
        font-size:7px;
    }

    .timeframe-card-title{
        min-height:23px;
        padding:4px 6px;
        font-size:5px;
    }

    .unified-4h-grid{
        grid-template-columns:
            repeat(3,1fr);
    }

    .unified-condition-row{
        min-height:42px;
    }

    .unified-condition{
        min-height:42px;
        padding:4px 6px;
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

    .condition-note{
        font-size:4.5px;
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

    .dual-signal-card{
        border-width:2px !important;
        box-shadow:
            0 0 0 1px rgba(224,189,109,0.25),
            0 0 12px rgba(224,189,109,0.10);
    }

}


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

    .btc-timeframe-title{
        font-size:5px;
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

    /* =====================================================
       380px 이하 당일 시세 강조
       ===================================================== */

    .unified-daily .up,
    .unified-daily .down{
        font-size:7px;
    }

    .unified-daily .zero{
        font-size:6px;
    }

    .timeframe-card-title{
        font-size:4.5px;
    }

    .unified-condition-row{
        min-height:37px;
    }

    .unified-condition{
        min-height:37px;
    }

    .condition-label{
        font-size:4px;
    }

    .condition-period{
        font-size:4.5px;
    }

    .condition-value{
        font-size:6px;
    }

    .condition-note{
        font-size:4px;
    }

}


.top-market-card .current-4h{
    background:#173326 !important;
}

.signal-market-card .current-4h{
    background:#173326 !important;
}

"""


# =========================================================
# Dashboard
#
# 화면:
# 1. BTC 시장 시황
# 2. 4H SIGNAL
# 3. 업비트 TOP20
#
# 일봉 SIGNAL 전용 리스트는 표시하지 않음
#
# 단,
# 일봉 SIGNAL 판정 자체는 유지하여
# 4H SIGNAL과 동시 충족 시 강조
#
# TOP20 카드 내부의 일봉은 그대로 유지
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
        "일봉 기준 = KST 09:00 ~ 다음날 09:00"
    )

    log.info(
        "4H 기준 = 01 / 05 / 09 / 13 / 17 / 21"
    )

    log.info(
        "일봉 최근 6개 표시"
    )

    log.info(
        "4H 최근 6개 표시"
    )

    log.info(
        "일봉 SIGNAL 전용 리스트 화면 출력 = 삭제"
    )

    log.info(
        "일봉 SIGNAL 판정 = 유지"
    )

    log.info(
        "4H SIGNAL = 상승장악 계열만"
    )

    log.info(
        "SIGNAL 관통형 = 제외"
    )

    log.info(
        "일봉 + 4H SIGNAL 동시 충족 = 카드 강조"
    )

    log.info(
        "SIGNAL 순위 = 거래대금 순위"
    )

    log.info(
        "SIGNAL 패턴 = "
        "상승장악 / "
        "3캔들 상승장악 / "
        "4캔들 상승장악"
    )

    log.info(
        "2캔들 = 첫 번째 음봉 → 두 번째 양봉"
    )

    log.info(
        "3캔들 = 음봉 → 아무 캔들 → 양봉"
    )

    log.info(
        "4캔들 = 음봉 → 아무 캔들 → 아무 캔들 → 양봉"
    )

    log.info(
        "3캔들/4캔들 중간 캔들 = 양봉/음봉/도지 모두 허용"
    )

    log.info(
        "3캔들/4캔들 마지막 양봉이 "
        "첫 번째 음봉을 상승장악해야 SIGNAL"
    )

    log.info(
        "4H SIGNAL 조건:"
    )

    log.info(
        "당일 양수"
    )

    log.info(
        "현재 4H 양봉"
    )

    log.info(
        "2캔들 = 상승장악"
    )

    log.info(
        "3캔들 = 음봉 → 아무 캔들 → 양봉"
    )

    log.info(
        "3캔들 = 마지막 양봉이 첫 음봉을 상승장악"
    )

    log.info(
        "4캔들 = 음봉 → 아무 캔들 → 아무 캔들 → 양봉"
    )

    log.info(
        "4캔들 = 마지막 양봉이 첫 음봉을 상승장악"
    )

    log.info(
        "도지 = body_ratio <= 0.10"
    )

    log.info(
        "장대 캔들 = body_ratio >= 0.50"
    )

    log.info(
        "현재 진행 중인 4H를 4H SIGNAL 마지막 캔들로 사용"
    )

    log.info(
        "BTC 시장 시황 = 일봉 + 4H"
    )

    log.info(
        "화면 순서 = BTC → 4H SIGNAL → TOP20"
    )

    log.info(
        "BTC = SIGNAL 필터에서 제외"
    )

    log.info(
        "캔들 패턴 표시 = 상승 + 하락 모두 표시"
    )

    log.info(
        "상승 패턴: 도지 / 망치형 / 역망치형 / "
        "상승장악 / 관통형 / 모닝스타 / 3연속양봉 / "
        "3캔들 상승장악 / 3캔들 관통형 / "
        "4캔들 상승장악 / 4캔들 관통형"
    )

    log.info(
        "하락 패턴: 하락장악 / 먹구름형 / "
        "3캔들 하락장악 / 3캔들 먹구름형"
    )

    log.info(
        "하락 패턴은 SIGNAL에서 제외"
    )

    log.info(
        "관통형은 일반 캔들 패턴 표시에는 유지"
    )

    log.info(
        "RSI = 삭제"
    )

    log.info(
        "ROC = 삭제"
    )

    log.info(
        "당일 시세 양수 = 초록색 강조"
    )

    log.info(
        "당일 시세 음수 = 빨간색 강조"
    )

    log.info(
        "당일 시세 0% = 회색 표시"
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
