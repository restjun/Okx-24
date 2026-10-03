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

latest_btc_daily_periods = []

latest_btc_current_daily_change = None

latest_btc_current_daily_label = "-"


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
# 상승장악 기준
#
# 첫 번째 캔들은 양봉/음봉 관계없이 허용
#
# 예:
# - 장대음봉
# - 도지
# - 망치형
# - 역망치형
# - 일반 양봉
# - 일반 음봉
#
# 두 번째 캔들은 반드시 양봉
#
# 두 번째 캔들의 몸통이
# 첫 번째 캔들의 몸통 전체를 장악하고
# 몸통 크기도 더 커야 함
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


    # =====================================================
    # 두 번째 캔들은 반드시 양봉
    # =====================================================

    if not p2["bull"]:

        return False


    # =====================================================
    # 첫 번째 캔들의 몸통 상단 / 하단
    #
    # 첫 번째가 양봉이든 음봉이든
    # 동일하게 몸통 범위를 계산
    # =====================================================

    first_body_high = max(
        p1["open"],
        p1["close"]
    )

    first_body_low = min(
        p1["open"],
        p1["close"]
    )


    # =====================================================
    # 두 번째 양봉이
    # 첫 번째 몸통 전체를 장악
    # =====================================================

    if p2["open"] > first_body_low:

        return False


    if p2["close"] < first_body_high:

        return False


    # =====================================================
    # 현재 양봉 몸통이 첫 캔들보다 커야 함
    # =====================================================

    if p2["body"] <= p1["body"]:

        return False


    return True


# =========================================================
# 1개 캔들 패턴
#
# 일반 일봉 패턴 표시용
# SIGNAL에는 사용하지 않음
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


    # 도지

    if body_ratio <= 0.10:

        patterns.append("도지")


    # 망치형

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


    # 역망치형

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
# 일반 일봉 패턴 표시용
#
# 상승장악:
# 첫 캔들 방향 관계없이 허용
# 다음 캔들이 양봉으로 첫 캔들 몸통을 장악
#
# 관통형:
# 일반 패턴 표시에는 유지
# SIGNAL에서는 제외
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


    # =====================================================
    # 상승장악
    #
    # 첫 번째 캔들:
    # 양봉/음봉 관계없음
    #
    # 두 번째 캔들:
    # 반드시 양봉
    # =====================================================

    if is_bullish_engulfing(
        previous,
        current
    ):

        patterns.append(
            "상승장악"
        )


    # =====================================================
    # 관통형
    #
    # 일반 패턴 표시용
    # SIGNAL에서는 제외
    # =====================================================

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


    # =====================================================
    # 하락장악
    # =====================================================

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


    # =====================================================
    # 먹구름형
    # =====================================================

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

        p3["close"] >
        (
            p1["open"] +
            p1["close"]
        ) / 2

    ):

        patterns.append("모닝스타")


    # =====================================================
    # 3연속양봉
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

        patterns.append("3연속양봉")


    # =====================================================
    # 3캔들 상승장악
    #
    # 첫 번째 캔들 방향 관계없음
    # 두 번째 캔들 방향 관계없음
    # 세 번째 캔들이 양봉으로
    # 첫 번째 캔들 몸통 전체를 장악
    # =====================================================

    if is_bullish_engulfing(
        c1,
        c3
    ):

        patterns.append(
            "3캔들 상승장악"
        )


    # =====================================================
    # 3캔들 관통형
    #
    # 일반 패턴 표시용
    # SIGNAL에서는 제외
    # =====================================================

    first_midpoint = (
        p1["open"] +
        p1["close"]
    ) / 2


    if (

        p1["bear"]

        and

        p3["bull"]

        and

        p3["close"] > first_midpoint

        and

        p3["close"] < p1["open"]

    ):

        patterns.append(
            "3캔들 관통형"
        )


    # =====================================================
    # 3캔들 하락패턴
    # =====================================================

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


        # 3캔들 하락장악

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


        # 3캔들 먹구름형

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


    # =====================================================
    # 4캔들 상승장악
    #
    # 첫 번째 캔들 방향 관계없음
    # 중간 2개 캔들 방향 관계없음
    # 네 번째 캔들이 양봉으로
    # 첫 번째 캔들 몸통 전체를 장악
    # =====================================================

    if is_bullish_engulfing(
        c1,
        c4
    ):

        patterns.append(
            "4캔들 상승장악"
        )


    # =====================================================
    # 4캔들 관통형
    #
    # 일반 패턴 표시용
    # SIGNAL에서는 제외
    # =====================================================

    first_midpoint = (
        p1["open"] +
        p1["close"]
    ) / 2


    if (

        p1["bear"]

        and

        p4["bull"]

        and

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
#
# 일반 일봉 패턴 표시용
# =========================================================

def detect_daily_patterns(
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


        # =================================================
        # 1봉
        # =================================================

        patterns.extend(
            detect_single_candle_pattern(
                period
            )
        )


        # =================================================
        # 2봉
        # =================================================

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


        # =================================================
        # 3봉
        # =================================================

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


        # =================================================
        # 4봉
        # =================================================

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


    pattern_results = detect_daily_patterns(
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
            )

    }


# =========================================================
# 일봉 SIGNAL 패턴
#
# 상승장악 계열만 SIGNAL
#
# 첫 번째 캔들:
# 양봉/음봉 관계없음
# 도지/망치형/역망치형/장대음봉 등 모두 가능
#
# 마지막 캔들:
# 반드시 양봉
# 첫 번째 캔들의 몸통 전체를 장악
#
# SIGNAL 제외:
# - 도지 단독
# - 망치형 단독
# - 역망치형 단독
# - 관통형
# - 하락장악
# - 먹구름형
# =========================================================

SIGNAL_CANDLE_PATTERNS = [

    "상승장악",

    "3캔들 상승장악",

    "4캔들 상승장악"

]


# =========================================================
# SIGNAL 조건 계산
#
# 조건:
#
# 1. 당일 변동률 양수
# 2. 현재 일봉 양봉
# 3. 상승장악 계열 패턴
# =========================================================

def calculate_signal_conditions(
    row
):

    daily_change = (
        row.get(
            "current_daily_change"
        )
    )


    # =====================================================
    # 조건 1
    # 당일 변동률 양수
    # =====================================================

    daily_condition = (

        daily_change is not None

        and

        daily_change > 0

    )


    # =====================================================
    # 현재 일봉
    # =====================================================

    daily_periods = row.get(
        "daily_periods",
        []
    )


    current_daily_candle = None


    if daily_periods:

        current_daily_candle = (
            daily_periods[-1]
        )


    # =====================================================
    # 조건 2
    # 현재 일봉 양봉
    # =====================================================

    current_daily_bullish = False


    if current_daily_candle is not None:

        open_price = (
            current_daily_candle.get(
                "open"
            )
        )

        close_price = (
            current_daily_candle.get(
                "close"
            )
        )


        if (
            open_price is not None
            and
            close_price is not None
        ):

            try:

                current_daily_bullish = (

                    float(close_price)
                    >
                    float(open_price)

                )

            except Exception:

                current_daily_bullish = False


    # =====================================================
    # 조건 3
    # 상승장악 계열 SIGNAL 패턴
    # =====================================================

    current_signal_patterns = []


    if current_daily_candle is not None:

        current_patterns = (
            current_daily_candle.get(
                "patterns",
                []
            )
        )


        current_signal_patterns = [

            pattern

            for pattern
            in SIGNAL_CANDLE_PATTERNS

            if pattern in current_patterns

        ]


    current_signal_pattern = bool(
        current_signal_patterns
    )


    # =====================================================
    # 최종 SIGNAL
    # =====================================================

    signal_pass = (

        daily_condition

        and

        current_daily_bullish

        and

        current_signal_pattern

    )


    row["signal_pass"] = signal_pass


    row["signal_conditions"] = {

        "daily":
            daily_condition,

        "current_daily_positive":
            daily_condition,

        "current_daily_bullish":
            current_daily_bullish,

        "current_signal_pattern":
            current_signal_pattern,

        "current_signal_patterns":
            current_signal_patterns,

        "current_daily_used":
            True

    }


    # =====================================================
    # daily_signal도 동일 조건
    # =====================================================

    row["daily_signal_pass"] = signal_pass


    row["daily_signal_conditions"] = {

        "daily":
            daily_condition,

        "current_daily_positive":
            daily_condition,

        "current_daily_bullish":
            current_daily_bullish,

        "current_signal_pattern":
            current_signal_pattern,

        "current_signal_patterns":
            current_signal_patterns,

        "current_daily_used":
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


    signal_count = sum(

        1

        for row in rows

        if row.get(
            "signal_pass",
            False
        )

    )


    current_daily = (
        get_current_daily_period()
    )


    log.info(

        f"TOP{TOP_N} 업데이트 | "

        f"일봉="
        f"{current_daily['display_label'] if current_daily else '-'}"
        " | "

        f"일봉 SIGNAL="
        f"{signal_count}"

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
#
# BTC 일봉 구성용
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


    pattern_results = detect_daily_patterns(
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
# BTC 업데이트
# =========================================================

def update_btc_market():

    global latest_btc_okx_price
    global latest_btc_daily_change
    global latest_btc_daily_periods
    global latest_btc_current_daily_change
    global latest_btc_current_daily_label


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


    current_daily = (
        get_current_daily_period()
    )


    if current_daily is not None:

        latest_btc_current_daily_label = (
            current_daily[
                "display_label"
            ]
        )


        latest_btc_current_daily_change = None


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
# 일봉 셀 HTML
# =========================================================

def daily_cells_html(periods):

    if not periods:

        return (
            '<div class="no-daily-data">-</div>'
        )


    cells = []


    for period in periods:

        if period.get(
            "active",
            False
        ):

            cell_class = (
                "daily-cell current-daily"
            )

        else:

            cell_class = (
                "daily-cell"
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

                <div class="daily-main">

                    <div class="daily-time">
                        {html.escape(
                            str(time_text)
                        )}
                    </div>

                    <div class="daily-value">
                        {format_change(
                            period.get("change")
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


    return "".join(cells)


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

                <div class="btc-status-item">

                    <div class="btc-info">
                        일봉
                    </div>

                    <div class="{daily_status_class}">
                        {daily_status}
                    </div>

                </div>

            </div>

        </div>


        <div class="btc-timeframe-title">
            📅 BTC 일봉
        </div>

        <div class="btc-daily-grid">

            {daily_cells_html(
                latest_btc_daily_periods
            )}

        </div>

    </div>

    """


# =========================================================
# 첫 줄 색상 클래스
#
# 당일 변동률 기준
# =========================================================

def get_main_row_class(row):

    daily_change = get_change_value(
        row.get(
            "daily_change"
        )
    )


    if daily_change is not None:

        if daily_change > 0:

            return "unified-main-row daily-positive"

        if daily_change < 0:

            return "unified-main-row daily-negative"

        return "unified-main-row daily-zero"


    return "unified-main-row daily-zero"


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


    if card_type == "SIGNAL_1":

        title = "🚀 일봉 SIGNAL"

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


        condition_html = f"""

        <div class="unified-condition-row">

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
                        상승장악 계열
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


    main_row_class = get_main_row_class(
        row
    )


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


        <div class="{main_row_class}">

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

        <div class="unified-daily-grid">

            {daily_cells_html(
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
# 일봉 SIGNAL Section
# =========================================================

def focus_section(
    data
):

    current_period = (
        get_current_daily_period()
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
            "현재 일봉 양봉 + "
            "첫 캔들 방향 무관 + "
            "상승장악 / "
            "3캔들 상승장악 / "
            "4캔들 상승장악 "
            "조건을 만족하는 종목 없음"
        )


        body = f"""

        <div class="signal-empty-card">

            <div class="signal-empty-icon">
                🔎
            </div>

            <div class="signal-empty-title">
                일봉 SIGNAL 없음
            </div>

            <div class="signal-empty-text">
                {message}
            </div>

            <div class="signal-empty-sub">

                현재 일봉:
                {(
                    current_period
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
                    "SIGNAL_1",
                    index + 1
                )

            )


        body = "".join(
            signal_cards
        )


    return f"""

    <div class="unified-section">

        <div class="section-title-card">

            <div class="section-number signal-section-number">
                🚀
            </div>

            <div class="section-heading">

                <div class="section-heading-main">
                    일봉 SIGNAL
                </div>

                <div class="section-heading-sub">

                    당일 양수
                    · 현재 일봉 양봉
                    · 첫 캔들 방향 무관
                    · 상승장악
                    · 3캔들 상승장악
                    · 4캔들 상승장악
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
                일봉 SIGNAL
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
    overflow:hidden;
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


/* =========================================================
   SIGNAL SECTION 금빛
   ========================================================= */

.signal-section-number{
    background:#2a2413;
    border-color:#d4af37;
    color:#f0cf67;
    box-shadow:
        0 0 8px rgba(212,175,55,0.18);
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
    margin-top:1px;
    color:#87919b;
    font-size:7px;
    line-height:9px;
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
    min-height:25px;
    margin-left:7px;
    padding:4px 7px;
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
    line-height:14px;
    font-weight:900;
}

.market-title-sub{
    margin-top:1px;
    color:#7e8994;
    font-size:6.5px;
    font-weight:700;
    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;
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
        1fr
        1.3fr
        1fr
        1.1fr;
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
    justify-content:center;
    gap:2px;
    min-width:60px;
    min-height:100%;
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


/* =========================================================
   일봉 6칸
   ========================================================= */

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
    padding:2px 2px;
}

.daily-main{
    display:flex;
    align-items:center;
    justify-content:center;
    gap:3px;
    width:100%;
    min-width:0;
    white-space:nowrap;
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
    box-shadow:
        inset 0 0 0 1px rgba(116,213,157,0.18),
        inset 0 0 15px rgba(78,164,111,0.08);
}

.current-daily .daily-time{
    color:#b9f0cf;
}

.current-daily .candle-pattern{
    color:#f0d486;
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


/* =========================================================
   SIGNAL 카드 금빛 테두리
   ========================================================= */

.signal-market-card{
    border:2px solid #d4af37;
    box-shadow:
        0 0 8px rgba(212,175,55,0.16),
        inset 0 0 0 1px rgba(212,175,55,0.12);
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
    box-shadow:
        inset 0 -1px 0 rgba(240,207,103,0.25);
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

.signal-market-card .unified-card-rank{
    color:#f0cf67;
}

.signal-market-card .unified-card-coin{
    color:#fff2bf;
}

.signal-market-card .unified-card-title{
    color:#c8ae5b;
}

.signal-badge{
    padding:3px 6px;
    border-radius:6px;
    background:#2a2413;
    border:1px solid #d4af37;
    color:#f0cf67;
    font-size:6px;
    font-weight:900;
    white-space:nowrap;
}


/* =========================================================
   첫 번째 줄
   ========================================================= */

.unified-main-row{
    display:grid;
    grid-template-columns:
        repeat(6,1fr);
    min-height:45px;
    background:#11161c;
}

.unified-main-item{
    grid-column:span 2;

    display:flex;
    flex-direction:column;
    align-items:center;
    justify-content:center;
    gap:2px;
    padding:2px 2px;
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
   당일 변동률 색상
   ========================================================= */

.unified-main-row.daily-positive
.unified-main-item{
    background:#10271c;
}

.unified-main-row.daily-negative
.unified-main-item{
    background:#2a1518;
}

.unified-main-row.daily-zero
.unified-main-item{
    background:#15191d;
}

.unified-main-row.daily-positive
.unified-main-item{
    box-shadow:
        inset 0 0 0 1px rgba(120,207,162,0.18);
}

.unified-main-row.daily-positive
.unified-price,
.unified-main-row.daily-positive
.unified-volume,
.unified-main-row.daily-positive
.unified-daily{
    color:#78cfa2 !important;
}

.unified-main-row.daily-negative
.unified-main-item{
    box-shadow:
        inset 0 0 0 1px rgba(223,133,136,0.18);
}

.unified-main-row.daily-negative
.unified-price,
.unified-main-row.daily-negative
.unified-volume,
.unified-main-row.daily-negative
.unified-daily{
    color:#df8588 !important;
}

.unified-main-row.daily-zero
.unified-main-item{
    box-shadow:
        inset 0 0 0 1px rgba(114,124,134,0.12);
}

.unified-main-row.daily-zero
.unified-price,
.unified-main-row.daily-zero
.unified-volume,
.unified-main-row.daily-zero
.unified-daily{
    color:#727c86 !important;
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
   시간대
   ========================================================= */

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

.condition-note{
    color:#b99d46;
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
    min-height:37px;
    margin-top:6px;
    background:#19160e;
    border:1px solid #d4af37;
    border-radius:8px;
    box-shadow:
        0 0 6px rgba(212,175,55,0.12);
}

.signal-btc-title{
    text-align:center;
    color:#c8ae5b;
    font-size:7px;
    font-weight:900;
}

.signal-btc-period{
    text-align:center;
    color:#f0cf67;
    font-size:7px;
    font-weight:900;
}

.signal-btc-value{
    text-align:center;
    color:#e8d083;
    font-size:9px;
    font-weight:900;
}

.top-update-bar{
    display:flex;
    justify-content:space-between;
    align-items:center;
    padding:5px 4px;
    color:#65717b;
    font-size:6.5px;
    font-weight:800;
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
    box-shadow:
        0 0 7px rgba(212,175,55,0.10);
}

.signal-empty-icon{
    font-size:20px;
    margin-bottom:4px;
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

.signal-empty-sub{
    margin-top:5px;
    color:#796a3d;
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
        margin:6px 0 8px;
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

    .current-time-badge{
        min-height:21px;
        margin-left:5px;
        padding:3px 5px;
        border-radius:5px;
        font-size:4.8px;
    }

    .market-card{
        margin:3px 0 8px;
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
        grid-template-columns:
            0.8fr
            1.1fr
            0.9fr
            1.0fr;
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

    .btc-status-item{
        gap:1px;
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

    .btc-daily-grid{
        grid-template-columns:
            repeat(3,1fr);
    }

    .daily-cell{
        min-height:47px;
        padding:2px 2px;
        gap:2px;
    }

    .daily-time{
        font-size:5px;
    }

    .daily-value{
        font-size:6px;
    }

    .candle-pattern{
        font-size:4.5px;
        line-height:6px;
    }

    .unified-market-card{
        border-radius:9px;
    }

    .signal-market-card{
        border-width:2px;
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
        padding:3px 5px;
    }

    .unified-main-row{
        grid-template-columns:
            repeat(3,1fr);
        min-height:40px;
    }

    .unified-main-item{
        grid-column:span 1;
        padding:2px 1px;
    }

    .unified-label{
        font-size:4.5px;
    }

    .unified-price,
    .unified-volume,
    .unified-daily{
        font-size:7px;
    }

    .unified-daily .up,
    .unified-daily .down{
        font-size:8px;
    }

    .unified-daily .zero{
        font-size:7px;
    }

    .timeframe-card-title{
        min-height:21px;
        padding:3px 6px;
        font-size:5px;
    }

    .unified-daily-grid{
        grid-template-columns:
            repeat(3,1fr);
    }

    .unified-condition-row{
        min-height:38px;
        border-top-color:#d4af37;
    }

    .unified-condition{
        min-height:38px;
        padding:3px 6px;
        gap:2px;
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
        padding:4px;
    }

    .signal-btc-bar{
        min-height:31px;
        margin-top:5px;
    }

    .signal-btc-title,
    .signal-btc-period{
        font-size:5px;
    }

    .signal-btc-value{
        font-size:7px;
    }

    .signal-empty-card{
        min-height:85px;
        padding:10px;
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
   380px 이하
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

    .current-time-badge{
        min-height:18px;
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
        min-height:35px;
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
        min-height:35px;
    }

    .btc-info{
        font-size:5px;
    }

    .btc-on,
    .btc-off{
        font-size:6px;
    }

    .btc-timeframe-title{
        min-height:20px;
        font-size:5px;
    }

    .daily-cell{
        min-height:41px;
        padding:2px 1px;
    }

    .daily-time{
        font-size:4px;
    }

    .daily-value{
        font-size:5px;
    }

    .candle-pattern{
        font-size:3.8px;
        line-height:5px;
    }

    .unified-card-header{
        min-height:30px;
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
        grid-template-columns:
            repeat(3,1fr);
        min-height:37px;
    }

    .unified-main-item{
        grid-column:span 1;
        padding:1px;
    }

    .unified-label{
        font-size:4px;
    }

    .unified-price,
    .unified-volume,
    .unified-daily{
        font-size:6px;
    }

    .unified-daily .up,
    .unified-daily .down{
        font-size:7px;
    }

    .unified-daily .zero{
        font-size:6px;
    }

    .timeframe-card-title{
        min-height:19px;
        padding:3px 5px;
        font-size:4.5px;
    }

    .unified-condition-row{
        min-height:34px;
    }

    .unified-condition{
        min-height:34px;
        padding:3px 5px;
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


/* =========================================================
   일봉 SIGNAL 강조
   ========================================================= */

.signal-market-card{
    border:2px solid #d4af37;
    box-shadow:
        0 0 8px rgba(212,175,55,0.16),
        inset 0 0 0 1px rgba(212,175,55,0.12);
}

.signal-header{
    background:#211d11;
    border-bottom:1px solid #d4af37;
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
        "일봉 최근 6개 표시"
    )

    log.info(
        "4H 기능 = 전체 삭제"
    )

    log.info(
        "4H 데이터 표시 = 삭제"
    )

    log.info(
        "4H SIGNAL = 삭제"
    )

    log.info(
        "일봉 SIGNAL = 기존 4H SIGNAL을 일봉으로 변경"
    )

    log.info(
        "일봉 SIGNAL 조건 = 당일 양수"
    )

    log.info(
        "일봉 SIGNAL 조건 = 현재 일봉 양봉"
    )

    log.info(
        "일봉 SIGNAL 패턴 = 첫 캔들 방향 무관"
    )

    log.info(
        "일봉 SIGNAL 패턴 = 다음 캔들 양봉 몸통 장악"
    )

    log.info(
        "일봉 SIGNAL 패턴 = 상승장악"
    )

    log.info(
        "일봉 SIGNAL 패턴 = 3캔들 상승장악"
    )

    log.info(
        "일봉 SIGNAL 패턴 = 4캔들 상승장악"
    )

    log.info(
        "일봉 SIGNAL 제외 = 도지 단독 / 망치형 단독 / 역망치형 단독"
    )

    log.info(
        "일봉 SIGNAL 제외 = 관통형"
    )

    log.info(
        "일봉 SIGNAL 제외 = 하락 패턴"
    )

    log.info(
        "일봉 일반 패턴 = 1봉 / 2봉 / 3봉 / 4봉 표시"
    )

    log.info(
        "SIGNAL 순위 = 거래대금 순위"
    )

    log.info(
        "SIGNAL 테두리 = 금빛"
    )

    log.info(
        "BTC 시장 시황 = 일봉"
    )

    log.info(
        "BTC = SIGNAL 필터에서 제외"
    )

    log.info(
        "화면 순서 = BTC → 일봉 SIGNAL → TOP20"
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
        "RSI = 삭제"
    )

    log.info(
        "ROC = 삭제"
    )

    log.info(
        "당일 시세 양수 = 현재가/거래대금/당일 전체 초록색"
    )

    log.info(
        "당일 시세 음수 = 현재가/거래대금/당일 전체 빨간색"
    )

    log.info(
        "당일 시세 0% = 현재가/거래대금/당일 전체 회색"
    )

    log.info(
        "글자 크기 = 기존 유지"
    )

    log.info(
        "카드 높이 = 세로 여백 및 padding만 축소"
    )

    log.info(
        "카드 내용 = 전체 표시 유지"
    )

    log.info(
        "시간 + 변동률 = 한 줄 표시"
    )

    log.info(
        "시간 형식 = 현재 10/03"
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
