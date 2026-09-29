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

latest_btc_01_05_change = None
latest_btc_05_09_change = None
latest_btc_09_13_change = None
latest_btc_13_17_change = None
latest_btc_17_21_change = None
latest_btc_21_01_change = None

latest_btc_current_4h_change = None

latest_btc_current_4h_label = "-"


# =========================================================
# 현재 KST 시간
# =========================================================

def kst():

    return datetime.now(
        KST
    ).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


# =========================================================
# 4시간 구간
#
# 업비트 기준
#
# 01:00 ~ 05:00
# 05:00 ~ 09:00
# 09:00 ~ 13:00
# 13:00 ~ 17:00
# 17:00 ~ 21:00
# 21:00 ~ 01:00
# =========================================================

FOUR_HOUR_PERIODS = [

    {
        "key": "01_05",
        "label": "01:00 ~ 05:00",
        "start_hour": 1,
        "end_hour": 5
    },

    {
        "key": "05_09",
        "label": "05:00 ~ 09:00",
        "start_hour": 5,
        "end_hour": 9
    },

    {
        "key": "09_13",
        "label": "09:00 ~ 13:00",
        "start_hour": 9,
        "end_hour": 13
    },

    {
        "key": "13_17",
        "label": "13:00 ~ 17:00",
        "start_hour": 13,
        "end_hour": 17
    },

    {
        "key": "17_21",
        "label": "17:00 ~ 21:00",
        "start_hour": 17,
        "end_hour": 21
    },

    {
        "key": "21_01",
        "label": "21:00 ~ 01:00",
        "start_hour": 21,
        "end_hour": 1
    }

]


# =========================================================
# 현재 4시간 구간
# =========================================================

def get_current_4h_period():

    now = datetime.now(KST)

    hour = now.hour

    if 1 <= hour < 5:

        return {
            **FOUR_HOUR_PERIODS[0],
            "active": True
        }

    if 5 <= hour < 9:

        return {
            **FOUR_HOUR_PERIODS[1],
            "active": True
        }

    if 9 <= hour < 13:

        return {
            **FOUR_HOUR_PERIODS[2],
            "active": True
        }

    if 13 <= hour < 17:

        return {
            **FOUR_HOUR_PERIODS[3],
            "active": True
        }

    if 17 <= hour < 21:

        return {
            **FOUR_HOUR_PERIODS[4],
            "active": True
        }

    # 21:00 ~ 00:59

    return {
        **FOUR_HOUR_PERIODS[5],
        "active": True
    }


# =========================================================
# 현재 4시간 구간 시작시간
# =========================================================

def get_current_4h_start():

    now = datetime.now(KST)

    period = get_current_4h_period()

    start_hour = period[
        "start_hour"
    ]

    if start_hour == 21:

        if now.hour >= 21:

            return now.replace(
                hour=21,
                minute=0,
                second=0,
                microsecond=0
            )

    if now.hour >= start_hour:

        return now.replace(
            hour=start_hour,
            minute=0,
            second=0,
            microsecond=0
        )

    return (
        now
        -
        timedelta(days=1)
    ).replace(
        hour=start_hour,
        minute=0,
        second=0,
        microsecond=0
    )


# =========================================================
# 4시간 구간 계산
#
# current
# previous
# =========================================================

def get_4h_periods(now=None):

    if now is None:

        now = datetime.now(KST)

    current_start = get_current_4h_start()

    current_period = get_current_4h_period()

    current_index = next(

        i

        for i, p in enumerate(
            FOUR_HOUR_PERIODS
        )

        if p["key"] == current_period["key"]

    )

    previous_index = (
        current_index - 1
    ) % len(
        FOUR_HOUR_PERIODS
    )

    previous_period = (
        FOUR_HOUR_PERIODS[
            previous_index
        ]
    )

    previous_start = (
        current_start
        -
        timedelta(hours=4)
    )

    current_end = (
        current_start
        +
        timedelta(hours=4)
    )

    return {

        "current": {

            **current_period,

            "start":
                current_start,

            "end":
                current_end,

            "active":
                True

        },

        "previous": {

            **previous_period,

            "start":
                previous_start,

            "end":
                current_start,

            "active":
                False

        }

    }


# =========================================================
# API 요청
# =========================================================

def wait_request():

    global last_request_time

    with request_lock:

        gap = (
            time.monotonic()
            -
            last_request_time
        )

        if gap < REQUEST_INTERVAL:

            time.sleep(
                REQUEST_INTERVAL - gap
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

    url = (

        args[0]

        if (

            args

            and

            isinstance(
                args[0],
                str
            )

        )

        else kwargs.get(
            "url",
            ""
        )

    )

    for n in range(
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

                wait = min(
                    RATE_LIMIT_WAIT * 2 ** n,
                    60
                )

            elif response.status_code >= 500:

                wait = min(
                    2 * 2 ** n,
                    30
                )

            else:

                return response

            time.sleep(wait)

        except Exception as e:

            log.error(
                f"[API 오류] {url}: {e}"
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

        krw_markets = [

            x["market"]

            for x in markets

            if x.get(
                "market",
                ""
            ).startswith("KRW-")

        ]

        ticker_result = []

        for i in range(
            0,
            len(krw_markets),
            100
        ):

            chunk = krw_markets[
                i:i + 100
            ]

            ticker_response = retry(
                requests.get,
                "https://api.upbit.com/v1/ticker",
                params={
                    "markets":
                        ",".join(chunk)
                },
                timeout=15
            )

            if ticker_response is None:

                continue

            try:

                data = (
                    ticker_response.json()
                )

            except Exception:

                continue

            if isinstance(
                data,
                list
            ):

                ticker_result.extend(
                    data
                )

        result = []

        for item in ticker_result:

            market = item.get(
                "market",
                ""
            )

            try:

                volume = float(
                    item.get(
                        "acc_trade_price_24h",
                        0
                    )
                )

                price = float(
                    item.get(
                        "trade_price",
                        0
                    )
                )

            except Exception:

                continue

            if (
                volume > 0
                and
                price > 0
            ):

                result.append({

                    "market":
                        market,

                    "volume_24h":
                        volume,

                    "current_price":
                        price

                })

        latest_upbit_markets = [

            x["market"]

            for x in result

        ]

        return result

    except Exception as e:

        log.error(
            f"업비트 마켓 오류: {e}"
        )

        return []


# =========================================================
# 업비트 일봉 상승률
#
# KST 09:00 기준
# =========================================================

def daily_change_upbit(
    market,
    current_price=None
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

        if not isinstance(
            data,
            list
        ):

            return None

        if len(data) < 2:

            return None

        current_candle = data[0]

        previous_candle = data[1]

        if current_price is None:

            current_price = float(
                current_candle[
                    "trade_price"
                ]
            )

        previous_close = float(
            previous_candle[
                "trade_price"
            ]
        )

        if previous_close == 0:

            return None

        return (

            (
                float(current_price)
                -
                previous_close
            )
            /
            previous_close
            *
            100

        )

    except Exception as e:

        log.warning(
            f"업비트 일봉 변동률 오류 "
            f"{market}: {e}"
        )

        return None


# =========================================================
# 업비트 60분봉
#
# 4시간 계산용
# =========================================================

def get_upbit_60m_candles(
    market,
    count=120
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

        if not isinstance(
            data,
            list
        ):

            return []

        return data

    except Exception:

        return []


# =========================================================
# 업비트 4시간봉 생성
#
# 업비트 기준
#
# 01~05
# 05~09
# 09~13
# 13~17
# 17~21
# 21~01
# =========================================================

def build_upbit_4h_candles(
    market,
    current_price=None
):

    candles = get_upbit_60m_candles(
        market,
        count=120
    )

    if not candles:

        return {}

    rows = []

    for candle in candles:

        try:

            dt_text = candle.get(
                "candle_date_time_kst"
            )

            if not dt_text:

                continue

            dt = datetime.strptime(
                dt_text,
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

        except Exception as e:

            log.warning(
                f"[업비트 4H 변환 오류] "
                f"{market}: {e}"
            )

            continue

    if not rows:

        return {}

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

    now = datetime.now(KST)

    periods = get_4h_periods(
        now
    )

    result = {}

    check_periods = [

        periods["current"],

        periods["previous"]

    ]

    for period in check_periods:

        start = period[
            "start"
        ]

        end = period[
            "end"
        ]

        part = df[

            (
                df["datetime"]
                >=
                start
            )

            &

            (
                df["datetime"]
                <
                end
            )

        ].copy()

        if part.empty:

            result[
                period["key"]
            ] = None

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

        # 현재 진행 중인 4H에만 현재가 적용

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

        result[
            period["key"]
        ] = {

            "key":
                period["key"],

            "label":
                period["label"],

            "start":
                start,

            "end":
                end,

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

            "active":
                period["active"],

            "candle_count":
                len(part)

        }

    return result


# =========================================================
# 업비트 4시간 상승률
# =========================================================

def get_upbit_4h_changes(
    market,
    current_price=None
):

    result = {

        "01_05": None,

        "05_09": None,

        "09_13": None,

        "13_17": None,

        "17_21": None,

        "21_01": None

    }

    candles = build_upbit_4h_candles(
        market,
        current_price
    )

    if not candles:

        return result

    for key in result.keys():

        candle = candles.get(
            key
        )

        if candle is not None:

            result[key] = candle[
                "change"
            ]

    return result


# =========================================================
# BTC 현재가
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

        data = response.json()

        if data.get(
            "code"
        ) != "0":

            return None

        rows = data.get(
            "data",
            []
        )

        if not rows:

            return None

        return float(
            rows[0]["last"]
        )

    except Exception as e:

        log.warning(
            f"OKX BTC 현재가 오류: {e}"
        )

        return None


# =========================================================
# BTC 1시간봉
# =========================================================

def get_okx_btc_1h_candles(
    limit=300,
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

        if payload.get(
            "code"
        ) != "0":

            return []

        return payload.get(
            "data",
            []
        )

    except Exception:

        return []


# =========================================================
# BTC 1시간봉 히스토리
# =========================================================

def get_okx_btc_1h_history():

    rows = []

    after = None

    for _ in range(
        MAX_HISTORY_CHUNKS
    ):

        data = (
            get_okx_btc_1h_candles(
                limit=HISTORY_CHUNK,
                after=after
            )
        )

        if not data:

            break

        rows.extend(data)

        try:

            timestamps = [

                int(
                    x[0]
                )

                for x in data

            ]

            oldest_ts = min(
                timestamps
            )

        except Exception:

            break

        after = oldest_ts

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
# BTC KST 일봉
#
# 09:00 기준
# =========================================================

def aggregate_btc_kst_daily(
    df
):

    if (
        df is None
        or
        df.empty
    ):

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
# BTC 당일 상승률
# =========================================================

def get_okx_btc_daily_change(
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

        return None

    daily = (
        aggregate_btc_kst_daily(
            df
        )
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

    start_naive = (
        current_start.replace(
            tzinfo=None
        )
    )

    previous = daily[
        daily["daily_start"]
        <
        start_naive
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
# BTC 4시간봉
#
# OKX 1H를 KST 기준으로 합성
# =========================================================

def build_okx_btc_4h_candles(
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

        return {}

    temp = df.copy()

    temp["kst_naive"] = (
        temp["datetime_kst"]
        .dt
        .tz_localize(None)
    )

    periods = get_4h_periods()

    result = {}

    check_periods = [

        periods["current"],

        periods["previous"]

    ]

    for period in check_periods:

        start = period[
            "start"
        ].replace(
            tzinfo=None
        )

        end = period[
            "end"
        ].replace(
            tzinfo=None
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

            result[
                period["key"]
            ] = None

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

        # 현재 진행 중인 4H에만 현재가 적용

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

        result[
            period["key"]
        ] = {

            "key":
                period["key"],

            "label":
                period["label"],

            "start":
                period["start"],

            "end":
                period["end"],

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

            "active":
                period["active"],

            "candle_count":
                len(part)

        }

    return result


# =========================================================
# BTC 4시간 상승률
# =========================================================

def get_okx_btc_4h_changes(
    price,
    df
):

    result = {

        "01_05": None,

        "05_09": None,

        "09_13": None,

        "13_17": None,

        "17_21": None,

        "21_01": None

    }

    candles = (
        build_okx_btc_4h_candles(
            price,
            df
        )
    )

    if not candles:

        return result

    for key in result.keys():

        candle = candles.get(
            key
        )

        if candle is not None:

            result[key] = candle[
                "change"
            ]

    return result


# =========================================================
# BTC 시황 업데이트
# =========================================================

def update_btc_market():

    global latest_btc_okx_price

    global latest_btc_daily_change

    global latest_btc_01_05_change
    global latest_btc_05_09_change
    global latest_btc_09_13_change
    global latest_btc_13_17_change
    global latest_btc_17_21_change
    global latest_btc_21_01_change

    global latest_btc_current_4h_change

    global latest_btc_current_4h_label

    price = get_okx_btc_price()

    if price is None:

        return

    latest_btc_okx_price = price

    df = get_okx_btc_1h_history()

    latest_btc_daily_change = (
        get_okx_btc_daily_change(
            price,
            df
        )
    )

    changes = (
        get_okx_btc_4h_changes(
            price,
            df
        )
    )

    latest_btc_01_05_change = (
        changes["01_05"]
    )

    latest_btc_05_09_change = (
        changes["05_09"]
    )

    latest_btc_09_13_change = (
        changes["09_13"]
    )

    latest_btc_13_17_change = (
        changes["13_17"]
    )

    latest_btc_17_21_change = (
        changes["17_21"]
    )

    latest_btc_21_01_change = (
        changes["21_01"]
    )

    period = get_current_4h_period()

    latest_btc_current_4h_label = (
        period["label"]
    )

    latest_btc_current_4h_change = (
        changes.get(
            period["key"]
        )
    )

    log.info(

        f"[BTC 4H] "

        f"01~05={latest_btc_01_05_change} | "

        f"05~09={latest_btc_05_09_change} | "

        f"09~13={latest_btc_09_13_change} | "

        f"13~17={latest_btc_13_17_change} | "

        f"17~21={latest_btc_17_21_change} | "

        f"21~01={latest_btc_21_01_change} | "

        f"현재={latest_btc_current_4h_label} "

        f"{latest_btc_current_4h_change}"

    )


# =========================================================
# 코인 분석
# =========================================================

def analyze(
    market,
    current_price
):

    daily_change = (
        daily_change_upbit(
            market,
            current_price
        )
    )

    four_hour = (
        get_upbit_4h_changes(
            market,
            current_price
        )
    )

    period = get_current_4h_period()

    current_4h = four_hour.get(
        period["key"]
    )

    periods = get_4h_periods()

    previous_key = periods[
        "previous"
    ]["key"]

    previous_4h = four_hour.get(
        previous_key
    )

    return {

        "daily_change":
            daily_change,

        "change_01_05":
            four_hour["01_05"],

        "change_05_09":
            four_hour["05_09"],

        "change_09_13":
            four_hour["09_13"],

        "change_13_17":
            four_hour["13_17"],

        "change_17_21":
            four_hour["17_21"],

        "change_21_01":
            four_hour["21_01"],

        "current_4h_change":
            current_4h,

        "previous_4h_change":
            previous_4h

    }


# =========================================================
# 변화값
# =========================================================

def get_change_value(x):

    try:

        if x is None:

            return None

        if isinstance(
            x,
            (list, tuple)
        ):

            if not x:

                return None

            return float(
                x[0]
            )

        return float(x)

    except Exception:

        return None


# =========================================================
# 변화 HTML
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
# 가격
# =========================================================

def format_market_price(price):

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
# 거래대금
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
# ROW
# =========================================================

def make_row(
    rank,
    name,
    volume,
    analysis,
    current_price
):

    a = analysis or {}

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

        "daily_change":
            get_change_value(
                a.get(
                    "daily_change"
                )
            ),

        "daily_html":
            format_change(
                a.get(
                    "daily_change"
                )
            ),

        "change_01_05":
            get_change_value(
                a.get(
                    "change_01_05"
                )
            ),

        "change_05_09":
            get_change_value(
                a.get(
                    "change_05_09"
                )
            ),

        "change_09_13":
            get_change_value(
                a.get(
                    "change_09_13"
                )
            ),

        "change_13_17":
            get_change_value(
                a.get(
                    "change_13_17"
                )
            ),

        "change_17_21":
            get_change_value(
                a.get(
                    "change_17_21"
                )
            ),

        "change_21_01":
            get_change_value(
                a.get(
                    "change_21_01"
                )
            ),

        "current_4h_change":
            get_change_value(
                a.get(
                    "current_4h_change"
                )
            ),

        "previous_4h_change":
            get_change_value(
                a.get(
                    "previous_4h_change"
                )
            ),

        "analysis":
            analysis

    }


# =========================================================
# TOP 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data

    global latest_upbit_update_time

    markets = sorted(

        get_upbit_markets(),

        key=lambda x:
            x["volume_24h"],

        reverse=True

    )

    top_markets = markets[
        :TOP_N
    ]

    if not top_markets:

        latest_upbit_data = []

        latest_upbit_update_time = (
            kst()
        )

        return

    rows = []

    period = get_current_4h_period()

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
                f"분석 오류 {market}: {e}"
            )

            analysis = None

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
        # BTC 현재 4H 양수
        # =================================================

        btc_pass = (

            latest_btc_current_4h_change
            is not None

            and

            latest_btc_current_4h_change
            > 0

        )

        # =================================================
        # 코인 현재 4H 양수
        # =================================================

        coin_current = (
            row.get(
                "current_4h_change"
            )
        )

        coin_current_pass = (

            coin_current is not None

            and

            coin_current > 0

        )

        # =================================================
        # 코인 이전 4H 양수
        # =================================================

        coin_previous = (
            row.get(
                "previous_4h_change"
            )
        )

        coin_previous_pass = (

            coin_previous is not None

            and

            coin_previous > 0

        )

        # =================================================
        # Signal
        # =================================================

        row["signal_pass"] = (

            btc_pass

            and

            coin_current_pass

            and

            coin_previous_pass

        )

        rows.append(
            row
        )

    latest_upbit_data = rows

    latest_upbit_update_time = (
        kst()
    )

    signal_count = sum(

        1

        for x in rows

        if x.get(
            "signal_pass",
            False
        )

    )

    log.info(

        f"TOP{TOP_N} 업데이트 | "

        f"현재={period['label']} | "

        f"BTC 현재4H="
        f"{latest_btc_current_4h_change} | "

        f"Signal={signal_count}"

    )


# =========================================================
# USDT
# =========================================================

def get_usdt_krw_internal():

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

        if not data:

            return None

        return float(
            data[0]["trade_price"]
        )

    except Exception:

        return None


# =========================================================
# OKX 영역
# =========================================================

def update_okx(
    usdt
):

    global latest_okx_data

    global latest_okx_update_time

    latest_okx_data = []

    latest_okx_update_time = (
        kst()
    )

    return True


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

            usdt = (
                get_usdt_krw_internal()
            )

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
# 4H 카드
# =========================================================

def four_hour_cells_html(
    row
):

    current_period = (
        get_current_4h_period()
    )

    current_key = (
        current_period["key"]
    )

    periods = [

        (
            "01_05",
            "01~05"
        ),

        (
            "05_09",
            "05~09"
        ),

        (
            "09_13",
            "09~13"
        ),

        (
            "13_17",
            "13~17"
        ),

        (
            "17_21",
            "17~21"
        ),

        (
            "21_01",
            "21~01"
        )

    ]

    cells = []

    for key, label in periods:

        value = row.get(
            f"change_{key}"
        )

        if key == current_key:

            class_name = (
                "four-hour-cell current-4h"
            )

            state = "현재"

        else:

            class_name = (
                "four-hour-cell"
            )

            state = ""

        cells.append(

            f"""

            <div class="{class_name}">

                <div class="four-hour-label">

                    {label}

                    {
                        f'<span class="four-hour-now">{state}</span>'
                        if state
                        else ""
                    }

                </div>

                <div class="four-hour-value">

                    {format_change(value)}

                </div>

            </div>

            """

        )

    return "".join(
        cells
    )


# =========================================================
# Signal 카드
# =========================================================

def signal_card_html(
    row,
    signal_rank
):

    coin = html.escape(
        str(
            row.get(
                "name",
                "-"
            )
        )
    )

    current_4h = row.get(
        "current_4h_change"
    )

    previous_4h = row.get(
        "previous_4h_change"
    )

    current_period = (
        get_current_4h_period()
    )

    previous_period = (
        get_4h_periods()
        ["previous"]
    )

    return f"""

    <div class="signal-card">

        <div class="signal-card-top">

            <div class="signal-rank">

                #{signal_rank}

            </div>

            <div class="signal-coin">

                {coin}

            </div>

            <div class="signal-top-rank">

                TOP {row.get("rank", "-")}

            </div>

            <div class="signal-badge">

                SIGNAL

            </div>

        </div>


        <div class="signal-card-main">

            <div class="signal-price-block">

                <div class="signal-price-label">

                    현재가

                </div>

                <div class="signal-price">

                    {format_market_price(
                        row.get(
                            "current_price"
                        )
                    )}

                </div>

            </div>


            <div class="signal-volume-block">

                <div class="signal-price-label">

                    24H 거래대금

                </div>

                <div class="signal-volume">

                    {row.get(
                        "volume",
                        "-"
                    )}

                </div>

            </div>


            <div class="signal-daily-block">

                <div class="signal-price-label">

                    당일

                </div>

                <div class="signal-daily">

                    {row.get(
                        "daily_html",
                        "-"
                    )}

                </div>

            </div>

        </div>


        <div class="signal-condition-row">

            <div class="signal-condition">

                <div class="condition-label">

                    현재 4H

                    <span>

                        {current_period["label"]}

                    </span>

                </div>

                <div class="condition-value">

                    {format_change(
                        current_4h
                    )}

                </div>

            </div>


            <div class="signal-condition">

                <div class="condition-label">

                    이전 4H

                    <span>

                        {previous_period["label"]}

                    </span>

                </div>

                <div class="condition-value">

                    {format_change(
                        previous_4h
                    )}

                </div>

            </div>

        </div>

    </div>

    """


# =========================================================
# Signal Section
# =========================================================

def focus_section(data):

    period = get_current_4h_period()

    btc_change = (
        latest_btc_current_4h_change
    )

    btc_positive = (

        btc_change is not None

        and

        btc_change > 0

    )

    current_signal_rows = [

        x.copy()

        for x in data

        if x.get(
            "signal_pass",
            False
        )

    ]

    current_signal_rows.sort(

        key=lambda x:

            x.get(
                "current_4h_change"
            )

            if x.get(
                "current_4h_change"
            ) is not None

            else -999999,

        reverse=True

    )

    if not btc_positive:

        btc_text = (

            "확인 불가"

            if btc_change is None

            else

            f"{btc_change:+.2f}%"

        )

        body = f"""

        <div class="signal-empty-card">

            <div class="signal-empty-icon">

                ⛔

            </div>

            <div class="signal-empty-title">

                Signal OFF

            </div>

            <div class="signal-empty-text">

                BTC 현재 4H

                <strong>
                    {period["label"]}
                </strong>

                상승률

                <strong>
                    {btc_text}
                </strong>

            </div>

            <div class="signal-empty-sub">

                BTC 현재 4시간 상승률이
                0% 초과일 때 Signal 활성화

            </div>

        </div>

        """

    elif not current_signal_rows:

        body = f"""

        <div class="signal-empty-card">

            <div class="signal-empty-icon">

                🔎

            </div>

            <div class="signal-empty-title">

                Signal 없음

            </div>

            <div class="signal-empty-text">

                BTC 현재 4H

                <strong>
                    {period["label"]}
                </strong>

                양수

            </div>

            <div class="signal-empty-sub">

                현재 4H 양수
                +
                이전 4H 양수

                조건을 만족하는
                TOP{TOP_N} 종목 없음

            </div>

        </div>

        """

    else:

        cards = []

        for signal_rank, row in enumerate(

            current_signal_rows,

            1

        ):

            cards.append(

                signal_card_html(
                    row,
                    signal_rank
                )

            )

        body = "".join(
            cards
        )

    return f"""

    <div class="unified-section">

        <div class="section-title-card signal-title">

            <div class="section-number">

                🚀

            </div>

            <div class="section-heading">

                <div class="section-heading-main">

                    SIGNAL

                </div>

                <div class="section-heading-sub">

                    BTC 현재 4H 양수
                    · 코인 현재 4H 양수
                    · 이전 4H 양수

                </div>

            </div>

            <div class="current-time-badge">

                ▶ {period["label"]}

            </div>

        </div>


        <div class="signal-btc-bar">

            <div class="signal-btc-title">

                BTC 현재 4H

            </div>

            <div class="signal-btc-period">

                {period["label"]}

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
# TOP 카드
# =========================================================

def top_card_html(
    row
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

    current_4h = row.get(
        "current_4h_change"
    )

    signal_pass = row.get(
        "signal_pass",
        False
    )

    if signal_pass:

        signal_mark = """

        <div class="top-signal-mark">

            🚀 SIGNAL

        </div>

        """

    else:

        signal_mark = ""

    return f"""

    <div class="top-card">

        <div class="top-card-header">

            <div class="top-rank">

                #{row.get(
                    "rank",
                    "-"
                )}

            </div>

            <div class="top-coin">

                {coin}

            </div>

            {signal_mark}

        </div>


        <div class="top-main-info">

            <div class="top-price-block">

                <div class="top-label">

                    현재가

                </div>

                <div class="top-price">

                    {format_market_price(
                        row.get(
                            "current_price"
                        )
                    )}

                </div>

            </div>


            <div class="top-volume-block">

                <div class="top-label">

                    24H 거래대금

                </div>

                <div class="top-volume">

                    {row.get(
                        "volume",
                        "-"
                    )}

                </div>

            </div>


            <div class="top-daily-block">

                <div class="top-label">

                    당일

                </div>

                <div class="top-daily">

                    {row.get(
                        "daily_html",
                        "-"
                    )}

                </div>

            </div>

        </div>


        <div class="top-current-row">

            <div class="top-current-title">

                현재 4H

            </div>

            <div class="top-current-period">

                {current_period["label"]}

            </div>

            <div class="top-current-value">

                {format_change(
                    current_4h
                )}

            </div>

        </div>


        <div class="four-hour-grid">

            {four_hour_cells_html(
                row
            )}

        </div>

    </div>

    """


# =========================================================
# TOP Section
# =========================================================

def section(
    data,
    update_time
):

    period = get_current_4h_period()

    if not data:

        cards = """

        <div class="empty-card">

            현재 데이터 없음

        </div>

        """

    else:

        cards = "".join(

            top_card_html(
                row
            )

            for row in data

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

                    거래대금 순위 · 당일 ·
                    4H 전체 구간

                </div>

            </div>

            <div class="current-time-badge">

                ▶ {period["label"]}

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
# BTC 4H HTML
# =========================================================

def btc_4h_cells_html():

    period = get_current_4h_period()

    current_key = period[
        "key"
    ]

    btc_values = {

        "01_05":
            latest_btc_01_05_change,

        "05_09":
            latest_btc_05_09_change,

        "09_13":
            latest_btc_09_13_change,

        "13_17":
            latest_btc_13_17_change,

        "17_21":
            latest_btc_17_21_change,

        "21_01":
            latest_btc_21_01_change

    }

    periods = [

        ("01_05", "01~05"),
        ("05_09", "05~09"),
        ("09_13", "09~13"),
        ("13_17", "13~17"),
        ("17_21", "17~21"),
        ("21_01", "21~01")

    ]

    cells = []

    for key, label in periods:

        if key == current_key:

            class_name = (
                "btc-period-card btc-current-4h"
            )

            state = "현재"

        else:

            class_name = (
                "btc-period-card"
            )

            state = ""

        cells.append(

            f"""

            <div class="{class_name}">

                <div class="btc-period-label">

                    {label}

                    {
                        f'<span>{state}</span>'
                        if state
                        else ""
                    }

                </div>

                <div class="btc-period-value">

                    {format_change(
                        btc_values[key]
                    )}

                </div>

            </div>

            """

        )

    return "".join(
        cells
    )


# =========================================================
# BTC 시장 요약
# =========================================================

def market_summary_html():

    period = get_current_4h_period()

    price = format_market_price(
        latest_btc_okx_price
    )

    daily = format_change(
        latest_btc_daily_change
    )

    current = format_change(
        latest_btc_current_4h_change
    )

    if (

        latest_btc_current_4h_change
        is not None

        and

        latest_btc_current_4h_change
        > 0

    ):

        signal_status = "ON"

        signal_class = "btc-on"

    else:

        signal_status = "OFF"

        signal_class = "btc-off"

    return f"""

    <div class="market-card">

        <div class="market-card-header">

            <div class="market-title-block">

                <div class="market-title-main">

                    ₿ BTC 시장 시황

                </div>

                <div class="market-title-sub">

                    OKX BTC-USDT
                    · 당일 = KST 09:00
                    · 4H = 업비트 기준

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

                {period["label"]}

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
   공통
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


/* =========================================================
   BTC 현재 4H
   ========================================================= */

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


/* =========================================================
   BTC 4H 전체
   ========================================================= */

.btc-4h-grid{
display:grid;
grid-template-columns:
    repeat(6, 1fr);
gap:1px;
background:#29323c;
border-top:1px solid #29323c;
}

.btc-period-card{
display:flex;
flex-direction:column;
align-items:center;
justify-content:center;
min-height:62px;
background:#0d1319;
gap:5px;
}

.btc-period-label{
color:#6f7b85;
font-size:7px;
font-weight:800;
}

.btc-period-label span{
display:block;
margin-top:2px;
color:#8fe0b2;
font-size:5px;
}

.btc-period-value{
font-size:9px;
font-weight:900;
}

.btc-current-4h{
background:#173326;
box-shadow:
    inset 0 0 0 1px rgba(116,213,157,0.18),
    inset 0 0 15px rgba(78,164,111,0.08);
}

.btc-current-4h .btc-period-label{
color:#91dcb0;
}


/* =========================================================
   TOP UPDATE
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
   TOP CARD
   ========================================================= */

.top-card-list{
display:flex;
flex-direction:column;
gap:8px;
}

.top-card{
width:100%;
background:#10161c;
border:1px solid #29333d;
border-radius:11px;
overflow:hidden;
}

.top-card-header{
display:flex;
align-items:center;
min-height:39px;
padding:6px 9px;
background:#141b22;
border-bottom:1px solid #29333d;
}

.top-rank{
width:32px;
color:#e0bd6d;
font-size:9px;
font-weight:900;
}

.top-coin{
flex:1;
color:#eef2f5;
font-size:10px;
font-weight:900;
}

.top-signal-mark{
padding:4px 7px;
border-radius:6px;
background:#183528;
border:1px solid #4f9b73;
color:#8fe0b2;
font-size:6px;
font-weight:900;
}


/* =========================================================
   TOP 기본 정보
   ========================================================= */

.top-main-info{
display:grid;
grid-template-columns:
    1.2fr
    1fr
    1fr;
min-height:58px;
background:#11171d;
}

.top-price-block,
.top-volume-block,
.top-daily-block{
display:flex;
flex-direction:column;
align-items:center;
justify-content:center;
gap:4px;
}

.top-volume-block,
.top-daily-block{
border-left:1px solid #29333d;
}

.top-label{
color:#68747e;
font-size:6px;
font-weight:800;
}

.top-price{
color:#f1f4f6;
font-size:10px;
font-weight:900;
}

.top-volume{
color:#cfd6dc;
font-size:9px;
font-weight:900;
}

.top-daily{
font-size:9px;
font-weight:900;
}


/* =========================================================
   TOP 현재 4H
   ========================================================= */

.top-current-row{
display:grid;
grid-template-columns:
    1fr
    1.4fr
    1fr;
align-items:center;
min-height:38px;
background:#101820;
border-top:1px solid #29333d;
border-bottom:1px solid #29333d;
text-align:center;
}

.top-current-title{
color:#7d8993;
font-size:7px;
font-weight:900;
}

.top-current-period{
color:#8fe0b2;
font-size:7px;
font-weight:900;
}

.top-current-value{
font-size:9px;
font-weight:900;
}


/* =========================================================
   4H 전체 그리드
   ========================================================= */

.four-hour-grid{
display:grid;
grid-template-columns:
    repeat(6, 1fr);
gap:1px;
background:#29333d;
}

.four-hour-cell{
min-height:57px;
display:flex;
flex-direction:column;
align-items:center;
justify-content:center;
gap:4px;
background:#0d1319;
}

.four-hour-label{
color:#66727c;
font-size:6.5px;
font-weight:800;
}

.four-hour-now{
display:block;
margin-top:2px;
color:#8fe0b2;
font-size:4.8px;
}

.four-hour-value{
font-size:7.5px;
font-weight:900;
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


/* =========================================================
   SIGNAL
   ========================================================= */

.signal-btc-bar{
display:grid;
grid-template-columns:
    1fr
    1.3fr
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

.signal-card-list{
display:flex;
flex-direction:column;
gap:8px;
margin-top:8px;
}

.signal-card{
width:100%;
background:#10171d;
border:1px solid #31513f;
border-radius:11px;
overflow:hidden;
}

.signal-card-top{
display:flex;
align-items:center;
min-height:40px;
padding:6px 9px;
background:#14231c;
border-bottom:1px solid #31513f;
}

.signal-rank{
width:32px;
color:#e0bd6d;
font-size:9px;
font-weight:900;
}

.signal-coin{
flex:1;
color:#eef2f5;
font-size:10px;
font-weight:900;
}

.signal-top-rank{
margin-right:7px;
color:#78858f;
font-size:6px;
font-weight:800;
}

.signal-badge{
padding:4px 6px;
border-radius:5px;
background:#183528;
border:1px solid #4f9b73;
color:#8fe0b2;
font-size:5.5px;
font-weight:900;
}

.signal-card-main{
display:grid;
grid-template-columns:
    1.3fr
    1fr
    1fr;
min-height:58px;
background:#11181e;
}

.signal-price-block,
.signal-volume-block,
.signal-daily-block{
display:flex;
flex-direction:column;
align-items:center;
justify-content:center;
gap:4px;
}

.signal-volume-block,
.signal-daily-block{
border-left:1px solid #29343d;
}

.signal-price-label{
color:#69757f;
font-size:6px;
font-weight:800;
}

.signal-price{
color:#f1f4f6;
font-size:10px;
font-weight:900;
}

.signal-volume{
color:#cfd6dc;
font-size:9px;
font-weight:900;
}

.signal-daily{
font-size:9px;
font-weight:900;
}

.signal-condition-row{
display:grid;
grid-template-columns:
    1fr
    1fr;
min-height:48px;
background:#0f171d;
border-top:1px solid #29343d;
}

.signal-condition{
display:flex;
flex-direction:column;
align-items:center;
justify-content:center;
gap:3px;
}

.signal-condition + .signal-condition{
border-left:1px solid #29343d;
}

.condition-label{
color:#75818b;
font-size:6px;
font-weight:800;
}

.condition-label span{
color:#8fe0b2;
margin-left:3px;
}

.condition-value{
font-size:9px;
font-weight:900;
}


/* =========================================================
   SIGNAL EMPTY
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

.signal-empty-text strong{
color:#9be1b8;
}

.signal-empty-sub{
margin-top:6px;
color:#59646e;
font-size:6px;
font-weight:700;
}


/* =========================================================
   상승 / 하락
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

.top-number{
    font-size:11px;
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
        repeat(3, 1fr);
}

.btc-period-card{
    min-height:43px;
}

.btc-period-label{
    font-size:5px;
}

.btc-period-label span{
    font-size:3.8px;
}

.btc-period-value{
    font-size:6px;
}


/* TOP */

.top-update-bar{
    font-size:5px;
}

.top-card{
    border-radius:9px;
}

.top-card-header{
    min-height:35px;
    padding:5px 7px;
}

.top-rank{
    width:25px;
    font-size:7px;
}

.top-coin{
    font-size:8px;
}

.top-signal-mark{
    font-size:4.5px;
    padding:3px 5px;
}

.top-main-info{
    min-height:48px;
}

.top-price,
.top-volume,
.top-daily{
    font-size:7px;
}

.top-label{
    font-size:4.5px;
}

.top-current-row{
    min-height:32px;
}

.top-current-title,
.top-current-period{
    font-size:5px;
}

.top-current-value{
    font-size:7px;
}

.four-hour-grid{
    grid-template-columns:
        repeat(3, 1fr);
}

.four-hour-cell{
    min-height:40px;
}

.four-hour-label{
    font-size:4.5px;
}

.four-hour-now{
    font-size:3.5px;
}

.four-hour-value{
    font-size:5.5px;
}


/* SIGNAL */

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

.signal-card-top{
    min-height:35px;
    padding:5px 7px;
}

.signal-rank{
    width:25px;
    font-size:7px;
}

.signal-coin{
    font-size:8px;
}

.signal-top-rank{
    font-size:4.5px;
}

.signal-badge{
    font-size:4px;
}

.signal-card-main{
    min-height:48px;
}

.signal-price,
.signal-volume,
.signal-daily{
    font-size:7px;
}

.signal-price-label{
    font-size:4.5px;
}

.signal-condition-row{
    min-height:40px;
}

.condition-label{
    font-size:4.5px;
}

.condition-label span{
    display:block;
    margin-top:2px;
    margin-left:0;
    font-size:4px;
}

.condition-value{
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


/* BTC */

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

.btc-period-card{
    min-height:38px;
}

.btc-period-label{
    font-size:4px;
}

.btc-period-value{
    font-size:5px;
}


/* TOP */

.top-card-header{
    min-height:32px;
}

.top-rank{
    font-size:6px;
}

.top-coin{
    font-size:7px;
}

.top-main-info{
    min-height:43px;
}

.top-price,
.top-volume,
.top-daily{
    font-size:6px;
}

.top-label{
    font-size:4px;
}

.top-current-row{
    min-height:29px;
}

.top-current-title,
.top-current-period{
    font-size:4px;
}

.top-current-value{
    font-size:6px;
}

.four-hour-cell{
    min-height:37px;
}

.four-hour-label{
    font-size:4px;
}

.four-hour-value{
    font-size:5px;
}


/* SIGNAL */

.signal-card-top{
    min-height:32px;
}

.signal-rank{
    font-size:6px;
}

.signal-coin{
    font-size:7px;
}

.signal-card-main{
    min-height:43px;
}

.signal-price,
.signal-volume,
.signal-daily{
    font-size:6px;
}

.signal-price-label{
    font-size:4px;
}

.signal-condition-row{
    min-height:37px;
}

.condition-label{
    font-size:4px;
}

.condition-value{
    font-size:6px;
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

    sections = ""

    if USE_UPBIT == "Y":

        # Signal

        sections += (
            focus_section(
                latest_upbit_data
            )
        )

        # TOP20

        sections += (
            section(
                latest_upbit_data,
                latest_upbit_update_time
            )
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

        {sections}

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
# STARTUP
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
        "★ 코인 데이터 = 업비트"
    )

    log.info(
        f"★ 거래대금 TOP = 업비트 TOP {TOP_N}"
    )

    log.info(
        "★ BTC 현재가 = OKX BTC-USDT"
    )

    log.info(
        "★ BTC 당일 = KST 09:00 기준"
    )

    log.info(
        "★ 4H = 업비트 기준"
    )

    log.info(
        "★ 4H = 01~05"
    )

    log.info(
        "★ 4H = 05~09"
    )

    log.info(
        "★ 4H = 09~13"
    )

    log.info(
        "★ 4H = 13~17"
    )

    log.info(
        "★ 4H = 17~21"
    )

    log.info(
        "★ 4H = 21~01"
    )

    log.info(
        "★ TOP = 거래대금 순"
    )

    log.info(
        "★ TOP = 4H 전체 구간 표시"
    )

    log.info(
        "★ Signal = 카드형 전체 표시"
    )

    log.info(
        "★ TOP = 카드형 전체 표시"
    )

    log.info(
        "★ Signal 조건 ① = BTC 현재 4H > 0"
    )

    log.info(
        "★ Signal 조건 ② = 코인 현재 4H > 0"
    )

    log.info(
        "★ Signal 조건 ③ = 코인 이전 4H > 0"
    )

    log.info(
        "★ Signal 정렬 = 현재 4H 상승률 높은 순"
    )

    log.info(
        "★ 현재 4H = 화면에서 녹색 강조"
    )

    log.info(
        "★ RSI = 삭제"
    )

    log.info(
        "★ ROC = 삭제"
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
