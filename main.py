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
# 기본 설정
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
# 시간
# =========================================================

def kst():

    return datetime.now(
        KST
    ).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


# =========================================================
# 현재 적용 시간대
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
# 현재 12시간 시작시간
# =========================================================

def get_current_12h_start():

    now = datetime.now(KST)

    if 9 <= now.hour < 21:

        return now.replace(
            hour=9,
            minute=0,
            second=0,
            microsecond=0
        )

    if now.hour >= 21:

        return now.replace(
            hour=21,
            minute=0,
            second=0,
            microsecond=0
        )

    return (
        now.replace(
            hour=21,
            minute=0,
            second=0,
            microsecond=0
        )
        -
        timedelta(days=1)
    )


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
# 12시간 계산용
# =========================================================

def get_upbit_60m_candles(
    market,
    count=30
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
# 업비트 12시간 상승률 2개 계산
#
# 반환:
#
# {
#     "09_21": ...,
#     "21_09": ...
# }
#
# 현재 진행 중인 구간:
# 현재가 기준
#
# 완료된 구간:
# 마지막 1시간봉 종가 기준
# =========================================================

def get_upbit_12h_changes(
    market,
    current_price=None
):

    candles = (
        get_upbit_60m_candles(
            market,
            count=30
        )
    )

    if not candles:

        return {
            "09_21": None,
            "21_09": None
        }

    candle_map = {}

    for candle in candles:

        try:

            dt = datetime.fromisoformat(
                candle[
                    "candle_date_time_kst"
                ]
            )

            candle_map[dt] = {
                "open": float(
                    candle[
                        "opening_price"
                    ]
                ),
                "close": float(
                    candle[
                        "trade_price"
                    ]
                )
            }

        except Exception:

            continue

    now = datetime.now(KST)

    today_0900 = now.replace(
        hour=9,
        minute=0,
        second=0,
        microsecond=0
    )

    today_2100 = now.replace(
        hour=21,
        minute=0,
        second=0,
        microsecond=0
    )

    # =====================================================
    # 09:00 시작 가격
    # =====================================================

    start_09 = today_0900

    if start_09.replace(
        tzinfo=None
    ) in candle_map:

        price_09 = candle_map[
            start_09.replace(
                tzinfo=None
            )
        ]["open"]

    else:

        price_09 = None

    # =====================================================
    # 21:00 시작 가격
    # =====================================================

    start_21 = today_2100

    if start_21.replace(
        tzinfo=None
    ) in candle_map:

        price_21 = candle_map[
            start_21.replace(
                tzinfo=None
            )
        ]["open"]

    else:

        price_21 = None

    # =====================================================
    # 전날 21:00 시작 가격
    # =====================================================

    yesterday_2100 = (
        today_2100
        -
        timedelta(days=1)
    )

    y21_key = yesterday_2100.replace(
        tzinfo=None
    )

    if y21_key in candle_map:

        price_y21 = candle_map[
            y21_key
        ]["open"]

    else:

        price_y21 = None

    # =====================================================
    # 09:00 직전 캔들
    #
    # 08:00 캔들 종가
    # =====================================================

    before_09 = (
        today_0900
        -
        timedelta(hours=1)
    )

    b09_key = before_09.replace(
        tzinfo=None
    )

    if b09_key in candle_map:

        close_before_09 = candle_map[
            b09_key
        ]["close"]

    else:

        close_before_09 = None

    # =====================================================
    # 21:00 직전 캔들
    #
    # 20:00 캔들 종가
    # =====================================================

    before_21 = (
        today_2100
        -
        timedelta(hours=1)
    )

    b21_key = before_21.replace(
        tzinfo=None
    )

    if b21_key in candle_map:

        close_before_21 = candle_map[
            b21_key
        ]["close"]

    else:

        close_before_21 = None

    # =====================================================
    # 현재 가격
    # =====================================================

    if current_price is None:

        try:

            current_price = float(
                candles[0][
                    "trade_price"
                ]
            )

        except Exception:

            current_price = None

    else:

        current_price = float(
            current_price
        )

    # =====================================================
    # 09 ~ 21
    # =====================================================

    if now.hour < 21:

        # 현재 진행 중
        if (
            price_09 is not None
            and
            current_price is not None
        ):

            change_09_21 = (
                (
                    current_price
                    -
                    price_09
                )
                /
                price_09
                *
                100
            )

        else:

            change_09_21 = None

    else:

        # 완료된 구간
        if (
            price_09 is not None
            and
            close_before_21 is not None
        ):

            change_09_21 = (
                (
                    close_before_21
                    -
                    price_09
                )
                /
                price_09
                *
                100
            )

        else:

            change_09_21 = None

    # =====================================================
    # 21 ~ 09
    # =====================================================

    if now.hour >= 21:

        # 현재 진행 중
        if (
            price_21 is not None
            and
            current_price is not None
        ):

            change_21_09 = (
                (
                    current_price
                    -
                    price_21
                )
                /
                price_21
                *
                100
            )

        else:

            change_21_09 = None

    else:

        # 완료된 구간
        if (
            price_y21 is not None
            and
            close_before_09 is not None
        ):

            change_21_09 = (
                (
                    close_before_09
                    -
                    price_y21
                )
                /
                price_y21
                *
                100
            )

        else:

            change_21_09 = None

    return {

        "09_21":
            change_09_21,

        "21_09":
            change_21_09

    }


# =========================================================
# 업비트 일봉 RSI
# =========================================================

def daily_rsi_upbit(
    market,
    period=14,
    current_price=None
):

    history_count = max(
        period * 8,
        100
    )

    response = retry(
        requests.get,
        "https://api.upbit.com/v1/candles/days",
        params={
            "market":
                market,
            "count":
                history_count
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

        if len(data) < period + 1:

            return None

        data = list(
            reversed(data)
        )

        closes = [

            float(
                item[
                    "trade_price"
                ]
            )

            for item in data

        ]

        if current_price is not None:

            closes[-1] = float(
                current_price
            )

        series = pd.Series(
            closes,
            dtype="float64"
        )

        delta = series.diff()

        gain = delta.clip(
            lower=0
        )

        loss = -delta.clip(
            upper=0
        )

        def wilder_rma(
            source,
            length
        ):

            values = source.to_numpy(
                dtype="float64"
            )

            result = [
                float("nan")
            ] * len(values)

            if len(values) <= length:

                return pd.Series(
                    result,
                    index=source.index
                )

            first_rma = (
                source.iloc[
                    1:length + 1
                ].sum()
                /
                length
            )

            result[length] = float(
                first_rma
            )

            for i in range(
                length + 1,
                len(values)
            ):

                result[i] = (
                    (
                        result[i - 1]
                        *
                        (length - 1)
                    )
                    +
                    values[i]
                ) / length

            return pd.Series(
                result,
                index=source.index
            )

        avg_gain = wilder_rma(
            gain,
            period
        )

        avg_loss = wilder_rma(
            loss,
            period
        )

        last_gain = avg_gain.iloc[-1]

        last_loss = avg_loss.iloc[-1]

        if pd.isna(
            last_gain
        ) or pd.isna(
            last_loss
        ):

            return None

        if last_loss == 0:

            if last_gain == 0:

                return 50.0

            return 100.0

        rs = (
            last_gain
            /
            last_loss
        )

        rsi = (
            100
            -
            100 / (1 + rs)
        )

        return float(rsi)

    except Exception as e:

        log.warning(
            f"업비트 RSI 오류 "
            f"{market}: {e}"
        )

        return None


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

        if data.get("code") != "0":

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

        if payload.get("code") != "0":

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

        if (
            after is not None
            and
            oldest_ts >= int(after)
        ):

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
# BTC 일봉
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
# BTC 12시간 상승률
#
# 09~21
# 21~09
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

    if (
        price is None
        or
        df is None
        or
        df.empty
    ):

        return result

    temp = df.copy()

    temp["kst_naive"] = (
        temp["datetime_kst"]
        .dt
        .tz_localize(None)
    )

    candle_map = {}

    for _, row in temp.iterrows():

        candle_map[
            row["kst_naive"]
        ] = {

            "open":
                float(row["open"]),

            "close":
                float(row["close"])

        }

    now = datetime.now(KST)

    today_0900 = now.replace(
        hour=9,
        minute=0,
        second=0,
        microsecond=0
    )

    today_2100 = now.replace(
        hour=21,
        minute=0,
        second=0,
        microsecond=0
    )

    key_09 = today_0900.replace(
        tzinfo=None
    )

    key_21 = today_2100.replace(
        tzinfo=None
    )

    key_y21 = (
        today_2100
        -
        timedelta(days=1)
    ).replace(
        tzinfo=None
    )

    key_before_09 = (
        today_0900
        -
        timedelta(hours=1)
    ).replace(
        tzinfo=None
    )

    key_before_21 = (
        today_2100
        -
        timedelta(hours=1)
    ).replace(
        tzinfo=None
    )

    price_09 = None

    price_21 = None

    price_y21 = None

    close_before_09 = None

    close_before_21 = None

    if key_09 in candle_map:

        price_09 = candle_map[
            key_09
        ]["open"]

    if key_21 in candle_map:

        price_21 = candle_map[
            key_21
        ]["open"]

    if key_y21 in candle_map:

        price_y21 = candle_map[
            key_y21
        ]["open"]

    if key_before_09 in candle_map:

        close_before_09 = candle_map[
            key_before_09
        ]["close"]

    if key_before_21 in candle_map:

        close_before_21 = candle_map[
            key_before_21
        ]["close"]

    # =====================================================
    # 09 ~ 21
    # =====================================================

    if now.hour < 21:

        if (
            price_09 is not None
            and
            price is not None
        ):

            result["09_21"] = (
                (
                    price
                    -
                    price_09
                )
                /
                price_09
                *
                100
            )

    else:

        if (
            price_09 is not None
            and
            close_before_21 is not None
        ):

            result["09_21"] = (
                (
                    close_before_21
                    -
                    price_09
                )
                /
                price_09
                *
                100
            )

    # =====================================================
    # 21 ~ 09
    # =====================================================

    if now.hour >= 21:

        if (
            price_21 is not None
            and
            price is not None
        ):

            result["21_09"] = (
                (
                    price
                    -
                    price_21
                )
                /
                price_21
                *
                100
            )

    else:

        if (
            price_y21 is not None
            and
            close_before_09 is not None
        ):

            result["21_09"] = (
                (
                    close_before_09
                    -
                    price_y21
                )
                /
                price_y21
                *
                100
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

    if daily_change is not None:

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
            changes["09_21"]
        )

    else:

        latest_btc_current_12h_change = (
            changes["21_09"]
        )

    log.info(
        f"[BTC] "
        f"현재={price} | "
        f"당일={latest_btc_daily_change} | "
        f"09~21={latest_btc_09_21_change} | "
        f"21~09={latest_btc_21_09_change} | "
        f"현재필터={latest_btc_current_12h_label} "
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

    rsi = (
        daily_rsi_upbit(
            market,
            period=14,
            current_price=current_price
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
            current_12h,

        "rsi":
            rsi

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

        return "-"

    if x > 0:

        return (
            '<span class="up">'
            f'▲ +{x:.1f}%'
            '</span>'
        )

    if x < 0:

        return (
            '<span class="down">'
            f'▼ {x:.1f}%'
            '</span>'
        )

    return (
        '<span class="zero">'
        '0.0%'
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

        "rsi":
            a.get(
                "rsi"
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
        # 현재 시간대 필터값
        # =================================================

        if (
            latest_btc_current_12h_change
            is not None
        ):

            btc_pass = (
                latest_btc_current_12h_change
                > 0
            )

        else:

            btc_pass = False

        coin_current = (
            row.get(
                "current_12h_change"
            )
        )

        coin_pass = (
            coin_current is not None
            and
            coin_current > 0
        )

        row["signal_pass"] = (
            btc_pass
            and
            coin_pass
        )

        rows.append(
            row
        )

    latest_upbit_data = rows

    latest_upbit_update_time = (
        kst()
    )

    log.info(
        f"TOP{TOP_N} 업데이트 | "
        f"현재구간={latest_btc_current_12h_label} | "
        f"BTC 12H={latest_btc_current_12h_change}"
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

        update_btc_market()

        if USE_UPBIT == "Y":

            update_upbit()

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

    current_12h = get_change_value(
        row.get(
            "current_12h_change"
        )
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

        <div class="signal-09">

            {row.get(
                "change_09_21_html",
                "-"
            )}

        </div>

        <div class="signal-21">

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

    # =====================================================
    # BTC 필터 OFF
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

                <div class="section-time">

                    {kst()} KST

                </div>

            </div>

            <div class="signal-empty">

                BTC 현재 필터 구간

                {period["label"]}

                <br>

                BTC 상승률 {btc_text}

                <br>

                BTC 12시간 상승률이 0% 초과일 때만 Signal 통과

            </div>

        </div>

        """

    # =====================================================
    # 코인 양수
    # =====================================================

    signal_rows = [

        x.copy()

        for x in data

        if (
            x.get(
                "current_12h_change"
            ) is not None
            and
            x.get(
                "current_12h_change"
            ) > 0
        )

    ]

    signal_rows.sort(

        key=lambda x:
            x["current_12h_change"],

        reverse=True

    )

    if not signal_rows:

        signal_table = """

        <div class="signal-empty">

            BTC 현재 구간은 양수

            <br>

            TOP15 중 현재 12시간 상승률 양수 종목 없음

        </div>

        """

    else:

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

            <div>09~21</div>

            <div>21~09</div>

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

                    ·

                    TOP{TOP_N}

                    ·

                    현재 12H 상승률 높은 순

                </div>

            </div>

            <div class="section-time">

                {period["label"]}

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

    for x in data:

        coin_name = html.escape(
            str(
                x.get(
                    "name",
                    "-"
                )
            )
        )

        # =================================================
        # RSI
        # =================================================

        rsi_value = x.get(
            "rsi"
        )

        try:

            rsi_value = (
                float(rsi_value)
                if rsi_value is not None
                else None
            )

        except (
            TypeError,
            ValueError
        ):

            rsi_value = None

        if rsi_value is None:

            rsi_html = "-"

        else:

            if rsi_value <= 30:

                rsi_class = "rsi-blue"

            elif 40 <= rsi_value <= 60:

                rsi_class = "rsi-green"

            elif rsi_value >= 70:

                rsi_class = "rsi-red"

            else:

                rsi_class = "rsi-normal"

            rsi_html = (

                f'<span class="{rsi_class}">'
                f'RSI {rsi_value:.1f}'
                '</span>'

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

                        {x.get("volume", "-")}

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

                    <div class="h09-cell">

                        {x.get(
                            "change_09_21_html",
                            "-"
                        )}

                    </div>

                    <div class="h21-cell">

                        {x.get(
                            "change_21_09_html",
                            "-"
                        )}

                    </div>

                    <div class="rsi-cell">

                        {rsi_html}

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

        <div>09~21</div>

        <div>21~09</div>

        <div>RSI</div>

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

                    거래대금

                    ·

                    당일

                    ·

                    09~21

                    ·

                    21~09

                    ·

                    RSI

                    ·

                    Signal 현재구간 = {period["label"]}

                </div>

            </div>

            <div class="section-time">

                {update_time} KST

            </div>

        </div>

        {table_html(data)}

    </div>

    """


# =========================================================
# BTC 시장 요약
# =========================================================

def market_summary_html():

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

    if (
        latest_btc_current_12h_change
        is not None
        and
        latest_btc_current_12h_change > 0
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

            <div class="btc-12h-item">

                <div class="btc-12h-label">

                    09:00 ~ 21:00

                </div>

                <div class="btc-12h-value">

                    {change_09}

                </div>

            </div>


            <div class="btc-12h-divider"></div>


            <div class="btc-12h-item">

                <div class="btc-12h-label">

                    21:00 ~ 09:00

                </div>

                <div class="btc-12h-value">

                    {change_21}

                </div>

            </div>


            <div class="btc-current-filter">

                <div class="btc-filter-label">

                    현재 필터

                </div>

                <div class="btc-filter-period">

                    {latest_btc_current_12h_label}

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
gap:4px;
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
color:#6f7a85;
font-size:6px;
font-weight:800;
}

.btc-filter-period{
color:#e3e8ec;
font-size:7px;
font-weight:900;
white-space:nowrap;
}

.btc-filter-value{
font-size:9px;
font-weight:900;
white-space:nowrap;
}


/* =========================================================
   TOP HEADER
   ========================================================= */

.top-header{
display:grid;

grid-template-columns:
    6%
    13%
    14%
    15%
    12%
    13%
    13%
    14%;

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


/* =========================================================
   TOP COIN
   ========================================================= */

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
    6%
    13%
    14%
    15%
    12%
    13%
    13%
    14%;

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
.h21-cell,
.rsi-cell{
font-size:8px;
font-weight:900;
text-align:center;
white-space:nowrap;
}

.rsi-cell{
border-left:1px solid #29323c;
background:#0e141a;
}


/* =========================================================
   Signal
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


/* =========================================================
   RSI
   ========================================================= */

.rsi-normal,
.rsi-blue,
.rsi-green,
.rsi-red{
font-size:8px;
font-weight:900;
white-space:nowrap;
}

.rsi-blue{
color:#459dff;
}

.rsi-green{
color:#78cfa2;
}

.rsi-red{
color:#ff6b6b;
}


/* =========================================================
   상승/하락
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

.btc-12h-label{
    font-size:5px;
}

.btc-12h-value{
    font-size:7px;
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
    font-size:6px;
}


/* TOP */

.top-header{
    min-height:24px;
    font-size:4.5px;
}

.coin-main-row{
    min-height:40px;

    grid-template-columns:
        6%
        13%
        14%
        15%
        12%
        13%
        13%
        14%;
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
.h21-cell,
.rsi-cell{
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


/* RSI */

.rsi-normal,
.rsi-blue,
.rsi-green,
.rsi-red{
    font-size:5.2px;
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

.btc-12h-label{
    font-size:4px;
}

.btc-12h-value{
    font-size:5.5px;
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
.h21-cell,
.rsi-cell{
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
        "★ 12H = KST 09:00~21:00"
    )

    log.info(
        "★ 12H = KST 21:00~09:00"
    )

    log.info(
        "★ TOP = 당일 + 09~21 + 21~09 표시"
    )

    log.info(
        "★ Signal = 당일 + 09~21 + 21~09 표시"
    )

    log.info(
        "★ Signal 필터 = 현재 시간대 12H"
    )

    log.info(
        "★ Signal = BTC 현재 12H > 0"
    )

    log.info(
        "★ Signal = 코인 현재 12H > 0"
    )

    log.info(
        "★ Signal 정렬 = 현재 12H 상승률 높은 순"
    )

    log.info(
        "★ RSI = 업비트 일봉 RSI(14)"
    )

    log.info(
        "★ RSI = TradingView Wilder RMA 방식"
    )

    log.info(
        "★ RSI = Signal 조건에 사용하지 않음"
    )

    log.info(
        "★ ROC = 완전 삭제"
    )

    log.info(
        "★ ROC COUNT = 완전 삭제"
    )

    log.info(
        "★ 0선 돌파 조건 = 삭제"
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
