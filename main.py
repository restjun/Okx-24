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

VOLUME_HOURS = 24

TOP_N = 15

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

latest_btc_09_21_change = None

latest_btc_21_09_change = None

latest_btc_current_12h_change = None

latest_btc_current_12h_label = "-"


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
# 현재 12시간 구간
#
# 09:00 ~ 20:59
#     → 09:00 ~ 21:00
#
# 21:00 ~ 08:59
#     → 21:00 ~ 09:00
# =========================================================

def get_current_12h_period():

    now = datetime.now(KST)

    if 9 <= now.hour < 21:

        return {
            "label": "09:00 ~ 21:00",
            "key": "09_21"
        }

    return {
        "label": "21:00 ~ 09:00",
        "key": "21_09"
    }


# =========================================================
# 현재 시간대 CSS 클래스
# =========================================================

def get_current_period_classes():

    period = get_current_12h_period()

    if period["key"] == "09_21":

        return {
            "active_09": "current-period",
            "active_21": "inactive-period",
            "badge": "current-badge-09"
        }

    return {
        "active_09": "inactive-period",
        "active_21": "current-period",
        "badge": "current-badge-21"
    }


# =========================================================
# 12시간 구간 계산
# =========================================================

def get_12h_periods(now=None):

    if now is None:

        now = datetime.now(KST)

    today = now.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0
    )

    today_09 = today + timedelta(
        hours=9
    )

    today_21 = today + timedelta(
        hours=21
    )

    yesterday_09 = (
        today_09
        -
        timedelta(days=1)
    )

    yesterday_21 = (
        today_21
        -
        timedelta(days=1)
    )

    tomorrow_09 = (
        today_09
        +
        timedelta(days=1)
    )

    # =====================================================
    # 09:00 ~ 20:59
    # =====================================================

    if 9 <= now.hour < 21:

        current = {
            "key": "09_21",
            "label": "09:00 ~ 21:00",
            "start": today_09,
            "end": today_21,
            "active": True
        }

        previous = {
            "key": "21_09",
            "label": "21:00 ~ 09:00",
            "start": yesterday_21,
            "end": today_09,
            "active": False
        }

    # =====================================================
    # 21:00 ~ 23:59
    # =====================================================

    elif now.hour >= 21:

        current = {
            "key": "21_09",
            "label": "21:00 ~ 09:00",
            "start": today_21,
            "end": tomorrow_09,
            "active": True
        }

        previous = {
            "key": "09_21",
            "label": "09:00 ~ 21:00",
            "start": today_09,
            "end": today_21,
            "active": False
        }

    # =====================================================
    # 00:00 ~ 08:59
    # =====================================================

    else:

        current = {
            "key": "21_09",
            "label": "21:00 ~ 09:00",
            "start": yesterday_21,
            "end": today_09,
            "active": True
        }

        previous = {
            "key": "09_21",
            "label": "09:00 ~ 21:00",
            "start": yesterday_09,
            "end": yesterday_21,
            "active": False
        }

    return {
        "current": current,
        "previous": previous
    }


# =========================================================
# 현재 12시간 시작시간
# =========================================================

def get_current_12h_start():

    periods = get_12h_periods()

    return periods[
        "current"
    ]["start"]


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
# =========================================================

def get_upbit_60m_candles(
    market,
    count=72
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
# 업비트 12시간봉 생성
# =========================================================

def build_upbit_12h_candles(
    market,
    current_price=None
):

    candles = get_upbit_60m_candles(
        market,
        count=72
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
                f"[업비트 12H 변환 오류] "
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

    periods = get_12h_periods(
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

        # =================================================
        # 현재 진행 중인 12H만 현재가 적용
        # =================================================

        if period["active"]:

            if current_price is not None:

                try:

                    current_price_float = (
                        float(current_price)
                    )

                    close_price = (
                        current_price_float
                    )

                    high_price = max(
                        high_price,
                        current_price_float
                    )

                    low_price = min(
                        low_price,
                        current_price_float
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
# 업비트 12시간 상승률
# =========================================================

def get_upbit_12h_changes(
    market,
    current_price=None
):

    result = {

        "09_21":
            None,

        "21_09":
            None

    }

    candles = build_upbit_12h_candles(
        market,
        current_price
    )

    if not candles:

        return result

    candle_09_21 = candles.get(
        "09_21"
    )

    candle_21_09 = candles.get(
        "21_09"
    )

    if candle_09_21 is not None:

        result["09_21"] = (
            candle_09_21[
                "change"
            ]
        )

    if candle_21_09 is not None:

        result["21_09"] = (
            candle_21_09[
                "change"
            ]
        )

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
    ).dt.floor("D") + pd.Timedelta(
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
# BTC 12시간봉 생성
# =========================================================

def build_okx_btc_12h_candles(
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

    now = datetime.now(KST)

    periods = get_12h_periods(
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

        # =================================================
        # 현재 진행 중인 12H에만 현재가 적용
        # =================================================

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
# BTC 12시간 상승률
# =========================================================

def get_okx_btc_12h_changes(
    price,
    df
):

    result = {

        "09_21":
            None,

        "21_09":
            None

    }

    candles = (
        build_okx_btc_12h_candles(
            price,
            df
        )
    )

    if not candles:

        return result

    candle_09_21 = candles.get(
        "09_21"
    )

    candle_21_09 = candles.get(
        "21_09"
    )

    if candle_09_21 is not None:

        result["09_21"] = (
            candle_09_21[
                "change"
            ]
        )

    if candle_21_09 is not None:

        result["21_09"] = (
            candle_21_09[
                "change"
            ]
        )

    return result


# =========================================================
# BTC 시황 업데이트
# =========================================================

def update_btc_market():

    global latest_btc_okx_price

    global latest_btc_daily_change

    global latest_btc_09_21_change

    global latest_btc_21_09_change

    global latest_btc_current_12h_change

    global latest_btc_current_12h_label

    price = get_okx_btc_price()

    if price is None:

        return

    latest_btc_okx_price = price

    df = get_okx_btc_1h_history()

    daily_change = (
        get_okx_btc_daily_change(
            price,
            df
        )
    )

    latest_btc_daily_change = (
        daily_change
    )

    changes = (
        get_okx_btc_12h_changes(
            price,
            df
        )
    )

    latest_btc_09_21_change = (
        changes["09_21"]
    )

    latest_btc_21_09_change = (
        changes["21_09"]
    )

    period = (
        get_current_12h_period()
    )

    latest_btc_current_12h_label = (
        period["label"]
    )

    if period["key"] == "09_21":

        latest_btc_current_12h_change = (
            latest_btc_09_21_change
        )

    else:

        latest_btc_current_12h_change = (
            latest_btc_21_09_change
        )

    log.info(
        f"[BTC 12H] "
        f"09~21={latest_btc_09_21_change} | "
        f"21~09={latest_btc_21_09_change} | "
        f"현재={latest_btc_current_12h_label} "
        f"{latest_btc_current_12h_change}"
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

    twelve = (
        get_upbit_12h_changes(
            market,
            current_price
        )
    )

    period = (
        get_current_12h_period()
    )

    if period["key"] == "09_21":

        current_12h = twelve[
            "09_21"
        ]

    else:

        current_12h = twelve[
            "21_09"
        ]

    return {

        "daily_change":
            daily_change,

        "change_09_21":
            twelve["09_21"],

        "change_21_09":
            twelve["21_09"],

        "current_12h_change":
            current_12h

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
            '<span class="zero">'
            '-'
            '</span>'
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

        price = float(
            price
        )

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

    daily = get_change_value(
        a.get(
            "daily_change"
        )
    )

    c09 = get_change_value(
        a.get(
            "change_09_21"
        )
    )

    c21 = get_change_value(
        a.get(
            "change_21_09"
        )
    )

    current_12h = get_change_value(
        a.get(
            "current_12h_change"
        )
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

        "daily_change":
            daily,

        "daily_html":
            format_change(
                daily
            ),

        "change_09_21":
            c09,

        "change_09_21_html":
            format_change(
                c09
            ),

        "change_21_09":
            c21,

        "change_21_09_html":
            format_change(
                c21
            ),

        "current_12h_change":
            current_12h,

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

    # =====================================================
    # 현재 시간대
    # =====================================================

    period = get_current_12h_period()

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
        # BTC 현재 12H 양수
        # =================================================

        btc_pass = (

            latest_btc_current_12h_change
            is not None

            and

            latest_btc_current_12h_change
            > 0

        )

        # =================================================
        # 코인 현재 12H
        # =================================================

        coin_current = (
            row.get(
                "current_12h_change"
            )
        )

        coin_current_pass = (

            coin_current is not None

            and

            coin_current > 0

        )

        # =================================================
        # 코인 이전 12H
        #
        # 현재 09~21
        # → 이전 21~09
        #
        # 현재 21~09
        # → 이전 09~21
        # =================================================

        if period["key"] == "09_21":

            coin_previous = (
                row.get(
                    "change_21_09"
                )
            )

        else:

            coin_previous = (
                row.get(
                    "change_09_21"
                )
            )

        coin_previous_pass = (

            coin_previous is not None

            and

            coin_previous > 0

        )

        # =================================================
        # 이전 12H 값 저장
        # =================================================

        row["previous_12h_change"] = (
            coin_previous
        )

        # =================================================
        # 최종 Signal
        #
        # ① BTC 현재 12H > 0
        # ② 코인 현재 12H > 0
        # ③ 코인 이전 12H > 0
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
        f"현재구간={period['label']} | "
        f"BTC 현재12H={latest_btc_current_12h_change} | "
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
# Signal 한 줄
# =========================================================

def signal_item_html(
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

    period = get_current_12h_period()

    if period["key"] == "09_21":

        c09_class = (
            "current-period"
        )

        c21_class = (
            "inactive-period"
        )

    else:

        c09_class = (
            "inactive-period"
        )

        c21_class = (
            "current-period"
        )

    return f"""

    <div class="signal-row">

        <div class="signal-rank">
            #{signal_rank}
        </div>

        <div class="signal-top">
            TOP {row.get("rank", "-")}
        </div>

        <div class="signal-coin">
            {coin}
        </div>

        <div class="signal-volume">
            {row.get("volume", "-")}
        </div>

        <div class="signal-price">
            {format_market_price(
                row.get(
                    "current_price"
                )
            )}
        </div>

        <div class="signal-daily">
            {row.get(
                "daily_html",
                "-"
            )}
        </div>

        <div class="signal-09 {c09_class}">
            {row.get(
                "change_09_21_html",
                "-"
            )}
        </div>

        <div class="signal-21 {c21_class}">
            {row.get(
                "change_21_09_html",
                "-"
            )}
        </div>

    </div>

    """


# =========================================================
# Signal
# =========================================================

def focus_section(data):

    period = (
        get_current_12h_period()
    )

    btc_change = (
        latest_btc_current_12h_change
    )

    btc_positive = (

        btc_change is not None

        and

        btc_change > 0

    )

    current_period_text = (
        f'▶ 현재 {period["label"]}'
    )

    # =====================================================
    # 이전 시간대
    # =====================================================

    if period["key"] == "09_21":

        previous_period_label = (
            "21:00 ~ 09:00"
        )

    else:

        previous_period_label = (
            "09:00 ~ 21:00"
        )

    # =====================================================
    # BTC OFF
    # =====================================================

    if not btc_positive:

        btc_text = (

            "확인 불가"

            if btc_change is None

            else

            f"{btc_change:+.2f}%"

        )

        return f"""

        <div class="unified-section">

            <div class="section-title-card">

                <div class="section-number">
                    🚀
                </div>

                <div class="section-heading">

                    <div class="section-heading-main">
                        Signal
                    </div>

                    <div class="section-heading-sub">

                        BTC {period["label"]}
                        양수 필터 OFF

                    </div>

                </div>

                <div class="current-time-badge">
                    {current_period_text}
                </div>

            </div>

            <div class="signal-empty">

                BTC 현재 필터 구간
                <strong>{period["label"]}</strong>

                <br>

                BTC 현재 12시간 상승률
                <strong>{btc_text}</strong>

                <br>

                BTC 현재 12시간 상승률이
                0% 초과일 때만 Signal 통과

            </div>

        </div>

        """

    # =====================================================
    # Signal 필터
    #
    # update_upbit()에서 계산된
    # signal_pass를 사용
    # =====================================================

    signal_rows = [

        x.copy()

        for x in data

        if x.get(
            "signal_pass",
            False
        )

    ]

    # =====================================================
    # 현재 12H 상승률 높은 순
    # =====================================================

    signal_rows.sort(

        key=lambda x:
            x.get(
                "current_12h_change"
            )
            if x.get(
                "current_12h_change"
            ) is not None
            else -999999,

        reverse=True

    )

    # =====================================================
    # Signal 없음
    # =====================================================

    if not signal_rows:

        signal_table = f"""

        <div class="signal-empty">

            BTC 현재 구간
            <strong>{period["label"]}</strong>
            양수

            <br>

            TOP{TOP_N} 중

            <strong>
                현재 12H 양수
            </strong>

            +

            <strong>
                이전 12H 양수 마감
            </strong>

            조건을 모두 만족하는 종목 없음

        </div>

        """

    else:

        # =================================================
        # 현재 시간대 강조
        # =================================================

        if period["key"] == "09_21":

            header_09_class = (
                "current-period"
            )

            header_21_class = (
                "inactive-period"
            )

        else:

            header_09_class = (
                "inactive-period"
            )

            header_21_class = (
                "current-period"
            )

        signal_items = []

        for signal_rank, row in enumerate(
            signal_rows,
            1
        ):

            signal_items.append(
                signal_item_html(
                    row,
                    signal_rank
                )
            )

        signal_table = f"""

        <div class="signal-header">

            <div>Signal</div>

            <div>TOP</div>

            <div>코인</div>

            <div>거래대금</div>

            <div>현재가</div>

            <div>당일</div>

            <div class="{header_09_class}">
                09~21
            </div>

            <div class="{header_21_class}">
                21~09
            </div>

        </div>

        <div class="signal-list">

            {"".join(signal_items)}

        </div>

        """

    return f"""

    <div class="unified-section">

        <div class="section-title-card">

            <div class="section-number">
                🚀
            </div>

            <div class="section-heading">

                <div class="section-heading-main">
                    Signal
                </div>

                <div class="section-heading-sub">

                    BTC {period["label"]} 양수
                    · 현재 12H 양수
                    · 이전 12H 양수 마감
                    · TOP{TOP_N}
                    · 현재 12H 높은 순

                </div>

            </div>

            <div class="current-time-badge">
                {current_period_text}
            </div>

        </div>

        {signal_table}

    </div>

    """


# =========================================================
# TOP 리스트
# =========================================================

def rows_html(data):

    out = []

    period = get_current_12h_period()

    if period["key"] == "09_21":

        class_09 = "current-period"

        class_21 = "inactive-period"

    else:

        class_09 = "inactive-period"

        class_21 = "current-period"

    for x in data:

        coin_name = html.escape(
            str(
                x.get(
                    "name",
                    "-"
                )
            )
        )

        out.append(

            f"""

            <div class="coin-card">

                <div class="coin-main-row">

                    <div class="rank-cell">
                        {x.get("rank", "-")}
                    </div>

                    <div class="coin-cell">

                        <span class="coin-name">

                            {coin_name}

                        </span>

                    </div>

                    <div class="volume-cell">

                        {x.get(
                            "volume",
                            "-"
                        )}

                    </div>

                    <div class="price-cell">

                        {format_market_price(
                            x.get(
                                "current_price"
                            )
                        )}

                    </div>

                    <div class="daily-cell">

                        {x.get(
                            "daily_html",
                            "-"
                        )}

                    </div>

                    <div class="h09-cell {class_09}">

                        {x.get(
                            "change_09_21_html",
                            "-"
                        )}

                    </div>

                    <div class="h21-cell {class_21}">

                        {x.get(
                            "change_21_09_html",
                            "-"
                        )}

                    </div>

                </div>

            </div>

            """

        )

    return "".join(
        out
    )


# =========================================================
# TOP 테이블
# =========================================================

def table_html(data):

    rows = rows_html(
        data
    )

    period = get_current_12h_period()

    if period["key"] == "09_21":

        header_09_class = (
            "current-period"
        )

        header_21_class = (
            "inactive-period"
        )

    else:

        header_09_class = (
            "inactive-period"
        )

        header_21_class = (
            "current-period"
        )

    if not rows:

        rows = """

        <div class="empty-card">

            현재 데이터 없음

        </div>

        """

    return f"""

    <div class="top-header">

        <div>순위</div>

        <div>코인</div>

        <div>거래대금</div>

        <div>현재가</div>

        <div>당일</div>

        <div class="{header_09_class}">
            ▶ 09~21
        </div>

        <div class="{header_21_class}">
            ▶ 21~09
        </div>

    </div>

    <div class="card-list">

        {rows}

    </div>

    """


# =========================================================
# TOP Section
# =========================================================

def section(
    data,
    update_time
):

    period = (
        get_current_12h_period()
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

                    거래대금 · 당일 ·
                    09~21 · 21~09

                </div>

            </div>

            <div class="current-time-badge">

                ▶ 현재 {period["label"]}

            </div>

        </div>

        {table_html(data)}

    </div>

    """


# =========================================================
# BTC 시장 요약
# =========================================================

def market_summary_html():

    period = get_current_12h_period()

    price = format_market_price(
        latest_btc_okx_price
    )

    daily = format_change(
        latest_btc_daily_change
    )

    change_09 = format_change(
        latest_btc_09_21_change
    )

    change_21 = format_change(
        latest_btc_21_09_change
    )

    current = format_change(
        latest_btc_current_12h_change
    )

    # =====================================================
    # BTC Signal 상태
    # =====================================================

    if (

        latest_btc_current_12h_change
        is not None

        and

        latest_btc_current_12h_change
        > 0

    ):

        signal_status = "ON"

        signal_class = "btc-on"

    else:

        signal_status = "OFF"

        signal_class = "btc-off"

    # =====================================================
    # 현재 시간대 강조
    # =====================================================

    if period["key"] == "09_21":

        btc_09_class = (
            "btc-current-period"
        )

        btc_21_class = (
            "btc-inactive-period"
        )

    else:

        btc_09_class = (
            "btc-inactive-period"
        )

        btc_21_class = (
            "btc-current-period"
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
                    당일 = KST 09:00 기준
                    ·
                    12H = 09:00 / 21:00 기준

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


        <div class="btc-12h-row">

            <div class="btc-12h-item {btc_09_class}">

                <div class="btc-12h-label">

                    09:00 ~ 21:00

                </div>

                <div class="btc-12h-value">
                    {change_09}
                </div>

                <div class="btc-period-state">

                    {
                        "현재 시간대"
                        if period["key"] == "09_21"
                        else
                        "이전 시간대"
                    }

                </div>

            </div>


            <div class="btc-12h-divider"></div>


            <div class="btc-12h-item {btc_21_class}">

                <div class="btc-12h-label">

                    21:00 ~ 09:00

                </div>

                <div class="btc-12h-value">
                    {change_21}
                </div>

                <div class="btc-period-state">

                    {
                        "현재 시간대"
                        if period["key"] == "21_09"
                        else
                        "이전 시간대"
                    }

                </div>

            </div>


            <div class="btc-current-filter">

                <div class="btc-filter-label">

                    ▶ 현재 필터

                </div>

                <div class="btc-filter-period">

                    {period["label"]}

                </div>

                <div class="btc-filter-value">
                    {current}
                </div>

            </div>

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
font-size:9px;
padding:12px;
}

h1{
margin:3px 4px 10px;
color:#eef2f5;
font-size:15px;
line-height:18px;
font-weight:900;
}


/* =========================================================
   공통 Section
   ========================================================= */

.unified-section{
width:100%;
margin:10px 0 12px;
}

.section-title-card{
display:flex;
align-items:center;
width:100%;
min-height:48px;
padding:7px 10px;
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
width:34px;
height:34px;
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
font-size:14px;
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

.section-time{
flex:none;
margin-left:8px;
color:#68737e;
font-size:6.5px;
font-weight:800;
white-space:nowrap;
}


/* =========================================================
   현재 시간대 배지
   ========================================================= */

.current-time-badge{
flex:none;
display:flex;
align-items:center;
justify-content:center;
min-height:27px;
margin-left:8px;
padding:4px 8px;
border-radius:7px;
background:#173326;
border:1px solid #4f9b73;
color:#8fe0b2;
font-size:7px;
font-weight:900;
white-space:nowrap;
box-shadow:
    0 0 8px rgba(94,190,135,0.12);
}

.current-badge-09{
background:#173326;
border-color:#4f9b73;
color:#8fe0b2;
}

.current-badge-21{
background:#173326;
border-color:#4f9b73;
color:#8fe0b2;
}


/* =========================================================
   현재 / 이전 시간대
   ========================================================= */

.current-period{
background:#172d22!important;
color:#eafff1!important;
border-color:#4f9b73!important;
box-shadow:
    inset 0 0 0 1px rgba(116,213,157,0.10),
    0 0 7px rgba(94,190,135,0.10);
}

.inactive-period{
background:#0c1116!important;
color:#65717b!important;
opacity:0.72;
}


/* =========================================================
   BTC
   ========================================================= */

.market-card{
width:100%;
margin:3px 0 12px;
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
overflow:hidden;
}

.market-title-main{
color:#eef2f5;
font-size:11px;
line-height:14px;
font-weight:900;
white-space:nowrap;
}

.market-title-sub{
margin-top:2px;
color:#7e8994;
font-size:6.5px;
line-height:9px;
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
white-space:nowrap;
}

.btc-main-row{
display:grid;
grid-template-columns:
    1.1fr
    1.3fr
    1fr
    1.4fr;
align-items:center;
min-height:58px;
background:#11161c;
}

.btc-name{
padding-left:13px;
color:#edf1f4;
font-size:11px;
font-weight:900;
white-space:nowrap;
}

.btc-price{
color:#f1f4f6;
font-size:11px;
font-weight:900;
text-align:center;
white-space:nowrap;
}

.btc-change{
font-size:11px;
font-weight:900;
text-align:center;
white-space:nowrap;
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
   BTC 12H
   ========================================================= */

.btc-12h-row{
display:grid;
grid-template-columns:
    1fr
    1px
    1fr
    1.25fr;
align-items:center;
min-height:66px;
background:#0e141a;
border-top:1px solid #29323c;
}

.btc-12h-item{
display:flex;
flex-direction:column;
align-items:center;
justify-content:center;
gap:3px;
height:66px;
}

.btc-12h-label{
color:#7d8893;
font-size:7px;
font-weight:800;
white-space:nowrap;
}

.btc-12h-value{
font-size:10px;
font-weight:900;
white-space:nowrap;
}

.btc-period-state{
font-size:5.5px;
font-weight:900;
color:#68747e;
white-space:nowrap;
}

.btc-current-period{
background:#173326!important;
box-shadow:
    inset 0 0 0 1px rgba(116,213,157,0.18),
    inset 0 0 15px rgba(78,164,111,0.08);
}

.btc-current-period .btc-12h-label{
color:#91dcb0;
}

.btc-current-period .btc-period-state{
color:#8fe0b2;
}

.btc-inactive-period{
background:#0b1015!important;
opacity:0.55;
}

.btc-current-period .btc-12h-value{
font-size:11px;
}

.btc-12h-divider{
height:36px;
background:#29323c;
}

.btc-current-filter{
display:flex;
flex-direction:column;
align-items:center;
justify-content:center;
gap:2px;
height:66px;
border-left:1px solid #29323c;
background:#111820;
}

.btc-filter-label{
color:#7fa18e;
font-size:6px;
font-weight:900;
}

.btc-filter-period{
color:#b9f0cf;
font-size:7px;
font-weight:900;
white-space:nowrap;
}

.btc-filter-value{
font-size:10px;
font-weight:900;
white-space:nowrap;
}


/* =========================================================
   TOP
   ========================================================= */

.top-header{
display:grid;

grid-template-columns:
    7%
    15%
    16%
    16%
    15%
    16%
    15%;

align-items:center;

min-height:30px;

background:#10151b;

border:1px solid #252e38;

border-radius:8px 8px 0 0;

color:#68737e;

font-size:7px;

font-weight:800;

text-align:center;

margin-top:8px;
}

.card-list{
width:100%;
display:flex;
flex-direction:column;
gap:6px;
}

.coin-card{
width:100%;
background:#0f141a;
border:1px solid #252e38;
border-radius:8px;
overflow:hidden;
}

.coin-main-row{
display:grid;

grid-template-columns:
    7%
    15%
    16%
    16%
    15%
    16%
    15%;

align-items:center;

min-height:52px;

background:#11161c;
}

.coin-main-row > div{
min-width:0;
height:52px;
display:flex;
align-items:center;
justify-content:center;
overflow:hidden;
}

.rank-cell{
justify-content:flex-start!important;
padding-left:9px;
color:#e2e7eb;
font-size:9px;
font-weight:900;
}

.coin-name{
width:100%;
padding:0 2px;
color:#eef2f5;
font-size:9px;
font-weight:900;
white-space:nowrap;
overflow:hidden;
text-overflow:ellipsis;
text-align:center;
}

.volume-cell,
.price-cell,
.daily-cell,
.h09-cell,
.h21-cell{
font-size:8px;
font-weight:900;
text-align:center;
white-space:nowrap;
}


/* =========================================================
   SIGNAL
   ========================================================= */

.signal-header,
.signal-row{
display:grid;

grid-template-columns:
    8%
    8%
    14%
    14%
    14%
    12%
    15%
    15%;

align-items:center;
}

.signal-header{
margin-top:8px;
min-height:30px;
background:#10151b;
border:1px solid #252e38;
border-radius:8px 8px 0 0;
color:#68737e;
font-size:6.5px;
font-weight:800;
text-align:center;
}

.signal-row{
min-height:48px;
background:#10171d;
border-left:1px solid #26333c;
border-right:1px solid #26333c;
border-bottom:1px solid #26333c;
font-size:7px;
font-weight:800;
text-align:center;
}

.signal-row:last-child{
border-radius:0 0 8px 8px;
}

.signal-rank{
color:#e0bd6d;
font-size:9px;
font-weight:900;
}

.signal-top{
color:#78838d;
font-size:7px;
}

.signal-coin{
color:#eef2f5;
font-size:8px;
font-weight:900;
white-space:nowrap;
overflow:hidden;
text-overflow:ellipsis;
}

.signal-volume,
.signal-price,
.signal-daily,
.signal-09,
.signal-21{
white-space:nowrap;
}

.signal-list{
width:100%;
}

.signal-empty{
min-height:58px;
display:flex;
align-items:center;
justify-content:center;
text-align:center;
background:#10151b;
border:2px solid #252e38;
border-radius:10px;
color:#59636e;
font-size:8px;
line-height:13px;
font-weight:800;
margin-top:8px;
}

.signal-empty strong{
color:#9be1b8;
font-weight:900;
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
}

.empty-card{
min-height:56px;
display:flex;
align-items:center;
justify-content:center;
background:#10151b;
border:2px solid #252e38;
border-radius:12px;
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

.section-time{
    font-size:5px;
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

.btc-12h-row{
    min-height:50px;
}

.btc-12h-item{
    height:50px;
    gap:2px;
}

.btc-12h-label{
    font-size:5px;
}

.btc-12h-value{
    font-size:7px;
}

.btc-period-state{
    font-size:4px;
}

.btc-current-period .btc-12h-value{
    font-size:8px;
}

.btc-current-filter{
    height:50px;
}

.btc-filter-label{
    font-size:4.5px;
}

.btc-filter-period{
    font-size:5px;
}

.btc-filter-value{
    font-size:7px;
}


/* TOP */

.top-header{
    min-height:24px;
    font-size:4.5px;
}

.coin-main-row{
    min-height:40px;

    grid-template-columns:
        7%
        15%
        16%
        16%
        15%
        16%
        15%;
}

.coin-main-row > div{
    height:40px;
}

.rank-cell{
    padding-left:4px;
    font-size:6px;
}

.coin-name{
    font-size:6px;
}

.volume-cell,
.price-cell,
.daily-cell,
.h09-cell,
.h21-cell{
    font-size:5.2px;
}


/* Signal */

.signal-header,
.signal-row{
    grid-template-columns:
        8%
        8%
        14%
        14%
        14%
        12%
        15%
        15%;
}

.signal-header{
    min-height:23px;
    font-size:4.2px;
}

.signal-row{
    min-height:38px;
    font-size:5px;
}

.signal-rank{
    font-size:6px;
}

.signal-top{
    font-size:4.5px;
}

.signal-coin{
    font-size:5.5px;
}

.signal-volume,
.signal-price,
.signal-daily,
.signal-09,
.signal-21{
    font-size:4.8px;
}

.signal-empty{
    min-height:44px;
    font-size:5.5px;
    line-height:9px;
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

.section-time{
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

.btc-12h-row{
    min-height:43px;
}

.btc-12h-item{
    height:43px;
}

.btc-12h-label{
    font-size:4px;
}

.btc-12h-value{
    font-size:5.5px;
}

.btc-period-state{
    font-size:3.5px;
}

.btc-current-filter{
    height:43px;
}

.btc-filter-label{
    font-size:3.7px;
}

.btc-filter-period{
    font-size:4px;
}

.btc-filter-value{
    font-size:5px;
}


/* TOP */

.top-header{
    min-height:21px;
    font-size:3.7px;
}

.coin-main-row{
    min-height:35px;
}

.coin-main-row > div{
    height:35px;
}

.rank-cell{
    font-size:5.5px;
    padding-left:2px;
}

.coin-name{
    font-size:5.5px;
}

.volume-cell,
.price-cell,
.daily-cell,
.h09-cell,
.h21-cell{
    font-size:4.4px;
}


/* Signal */

.signal-header{
    min-height:20px;
    font-size:3.7px;
}

.signal-row{
    min-height:32px;
}

.signal-rank{
    font-size:5px;
}

.signal-top{
    font-size:4px;
}

.signal-coin{
    font-size:4.8px;
}

.signal-volume,
.signal-price,
.signal-daily,
.signal-09,
.signal-21{
    font-size:4.1px;
}

.signal-empty{
    min-height:37px;
    font-size:4.7px;
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

        sections += (
            focus_section(
                latest_upbit_data
            )
        )

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
        "★ 12H = 1시간봉 12개를 직접 합성"
    )

    log.info(
        "★ 12H = KST 09:00~21:00"
    )

    log.info(
        "★ 12H = KST 21:00~09:00"
    )

    log.info(
        "★ TOP = 당일 + 09~21 + 21~09"
    )

    log.info(
        "★ Signal = 당일 + 09~21 + 21~09"
    )

    log.info(
        "★ Signal 조건 ① = BTC 현재 12H > 0"
    )

    log.info(
        "★ Signal 조건 ② = 코인 현재 12H > 0"
    )

    log.info(
        "★ Signal 조건 ③ = 코인 이전 12H > 0"
    )

    log.info(
        "★ Signal = 현재 + 이전 12H 모두 양수"
    )

    log.info(
        "★ Signal 정렬 = 현재 12H 상승률 높은 순"
    )

    log.info(
        "★ 현재 시간대 = 화면에서 녹색 강조"
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
