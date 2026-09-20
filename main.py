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

from datetime import datetime
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
# 상승 신호 영역 EMA COUNT 필터
# =========================================================

EMA_LONG_MAX_COUNT = 70


# =========================================================
# ROC 필터
# =========================================================

USE_1H_ROC_FILTER = "Y"
USE_4H_ROC_FILTER = "N"

USE_1H_ROC5 = "N"
USE_1H_ROC10 = "Y"
USE_1H_ROC20 = "Y"
USE_1H_ROC50 = "Y"
USE_1H_ROC200 = "N"

USE_4H_ROC5 = "N"
USE_4H_ROC10 = "N"
USE_4H_ROC20 = "N"
USE_4H_ROC50 = "N"
USE_4H_ROC200 = "N"

ROC_FILTER_TIMEFRAME = 60
ROC_FILTER_HIGH_TIMEFRAME = 240
ROC_TIMEFRAME = 60

ROC_PERIOD = 5

ROC_FILTER_PERIODS = [
    5,
    10,
    20,
    50,
    200
]


# =========================================================
# 호가
# =========================================================

ORDERBOOK_RANGE = 0.01
ORDERBOOK_COUNT = 30
ORDERBOOK_DOMINANCE_GAP = 5.0


# =========================================================
# 전역
# =========================================================

latest_upbit_data = []
latest_okx_data = []

latest_upbit_update_time = "-"
latest_okx_update_time = "-"

latest_upbit_markets = []
latest_upbit_orderbook = {}

request_lock = threading.Lock()
update_lock = threading.Lock()

last_request_time = 0
latest_usdt_krw_internal = 0

okx_ticker_cache = {}


# =========================================================
# 신호 상태
# =========================================================

roc_signal_state = {}


# =========================================================
# 눌림 상태
# =========================================================

roc_pullback_state = {}


# =========================================================
# 진행캔들에서 이미 실패한 신호
# =========================================================

roc_signal_failed_candle = {}


# =========================================================
# ROC 설정
# =========================================================

def roc_settings():

    return {
        5: {
            "1H": USE_1H_ROC5,
            "4H": USE_4H_ROC5
        },
        10: {
            "1H": USE_1H_ROC10,
            "4H": USE_4H_ROC10
        },
        20: {
            "1H": USE_1H_ROC20,
            "4H": USE_4H_ROC20
        },
        50: {
            "1H": USE_1H_ROC50,
            "4H": USE_4H_ROC50
        },
        200: {
            "1H": USE_1H_ROC200,
            "4H": USE_4H_ROC200
        }
    }


def get_enabled_periods(timeframe):

    if timeframe == "1H":

        if USE_1H_ROC_FILTER != "Y":
            return []

    elif timeframe == "4H":

        if USE_4H_ROC_FILTER != "Y":
            return []

    else:

        return []

    settings = roc_settings()

    return [
        p
        for p in ROC_FILTER_PERIODS
        if settings[p][timeframe] == "Y"
    ]


def get_all_periods():

    return ROC_FILTER_PERIODS.copy()


def get_enabled_all_filters():

    result = []

    settings = roc_settings()

    if USE_1H_ROC_FILTER == "Y":

        for period in ROC_FILTER_PERIODS:

            if settings[period]["1H"] == "Y":

                result.append(
                    ("1H", period)
                )

    if USE_4H_ROC_FILTER == "Y":

        for period in ROC_FILTER_PERIODS:

            if settings[period]["4H"] == "Y":

                result.append(
                    ("4H", period)
                )

    return result


def get_enabled_filter_text(timeframe):

    periods = get_enabled_periods(
        timeframe
    )

    if not periods:
        return "-"

    return "/".join(
        str(x)
        for x in periods
    )


def get_filter_setting_text():

    h1 = (
        get_enabled_filter_text("1H")
        if USE_1H_ROC_FILTER == "Y"
        else "-"
    )

    h4 = (
        get_enabled_filter_text("4H")
        if USE_4H_ROC_FILTER == "Y"
        else "-"
    )

    return f"1H:{h1} 4H:{h4}"


# =========================================================
# EMA 설정
# =========================================================

def get_enabled_ema_periods(timeframe):

    return get_enabled_periods(
        timeframe
    )


# =========================================================
# 시간
# =========================================================

def kst():

    return datetime.now(
        KST
    ).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def format_timeframe(minutes):

    minutes = int(minutes)

    if minutes >= 1440:
        return f"{minutes // 1440}D"

    if minutes >= 60:
        return f"{minutes // 60}H"

    return f"{minutes}M"


def get_current_candle_start(minutes):

    minutes = int(minutes)

    now = datetime.now(KST)

    total = (
        now.hour * 60
        + now.minute
    )

    block = (
        total // minutes
    ) * minutes

    current = now.replace(
        hour=block // 60,
        minute=block % 60,
        second=0,
        microsecond=0
    )

    return current.replace(
        tzinfo=None
    )


def normalize_datetime(value):

    if value is None:
        return None

    try:

        return (
            pd.Timestamp(value)
            .to_pydatetime()
            .replace(
                tzinfo=None
            )
        )

    except Exception:

        return None


def candle_distance(
    start_time,
    end_time,
    timeframe
):

    start_time = normalize_datetime(
        start_time
    )

    end_time = normalize_datetime(
        end_time
    )

    if (
        start_time is None
        or end_time is None
    ):

        return 0

    try:

        seconds = (
            end_time - start_time
        ).total_seconds()

        return max(
            int(
                seconds
                // (
                    int(timeframe)
                    * 60
                )
            ),
            0
        )

    except Exception:

        return 0


def get_last_completed_candle_time(
    df,
    timeframe
):

    if (
        df is None
        or df.empty
        or "datetime" not in df.columns
    ):

        return None

    try:

        current_start = (
            get_current_candle_start(
                timeframe
            )
        )

        temp = df.copy()

        temp["datetime"] = pd.to_datetime(
            temp["datetime"],
            errors="coerce"
        )

        temp = temp.dropna(
            subset=["datetime"]
        )

        completed = temp[
            temp["datetime"] < current_start
        ]

        if completed.empty:
            return None

        return completed[
            "datetime"
        ].iloc[-1]

    except Exception as e:

        log.warning(
            f"완성캔들 시간 오류: {e}"
        )

        return None


# =========================================================
# 검증
# =========================================================

def validate_timeframe():

    if USE_1H_ROC_FILTER not in (
        "Y",
        "N"
    ):

        raise ValueError(
            "USE_1H_ROC_FILTER는 Y/N만 가능합니다."
        )

    if USE_4H_ROC_FILTER not in (
        "Y",
        "N"
    ):

        raise ValueError(
            "USE_4H_ROC_FILTER는 Y/N만 가능합니다."
        )

    if (
        not isinstance(
            EMA_LONG_MAX_COUNT,
            int
        )
        or EMA_LONG_MAX_COUNT < 1
    ):

        raise ValueError(
            "EMA_LONG_MAX_COUNT는 1 이상의 정수여야 합니다."
        )

    settings = roc_settings()

    for period in ROC_FILTER_PERIODS:

        for timeframe in (
            "1H",
            "4H"
        ):

            value = settings[
                period
            ][
                timeframe
            ]

            if value not in (
                "Y",
                "N"
            ):

                raise ValueError(
                    f"{timeframe} ROC{period} 설정 오류"
                )


# =========================================================
# API
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
                REQUEST_INTERVAL
                - gap
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
            and isinstance(
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
                    RATE_LIMIT_WAIT
                    * 2 ** n,
                    60
                )

            elif response.status_code >= 500:

                wait = min(
                    2 * 2 ** n,
                    30
                )

            else:

                return response

            time.sleep(
                wait
            )

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
                and price > 0
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
# 호가
# =========================================================

def get_upbit_orderbooks(markets):

    if not markets:
        return {}

    result = {}

    market_codes = []

    for x in markets:

        if isinstance(
            x,
            str
        ):

            market = x

        elif isinstance(
            x,
            dict
        ):

            market = x.get(
                "market"
            )

        else:

            market = None

        if (
            isinstance(
                market,
                str
            )
            and market
        ):

            market_codes.append(
                market
            )

    market_codes = list(
        dict.fromkeys(
            market_codes
        )
    )

    if not market_codes:
        return {}

    for i in range(
        0,
        len(market_codes),
        30
    ):

        chunk = market_codes[
            i:i + 30
        ]

        response = retry(
            requests.get,
            "https://api.upbit.com/v1/orderbook",
            params={
                "markets":
                    ",".join(chunk),

                "count":
                    ORDERBOOK_COUNT
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

            if not isinstance(
                item,
                dict
            ):

                continue

            market = item.get(
                "market"
            )

            if market:

                result[
                    market
                ] = item

    return result


def calculate_orderbook_amount(
    orderbook,
    current_price
):

    result = {

        "bid_amount": 0.0,
        "ask_amount": 0.0,
        "total_amount": 0.0,

        "bid_ratio": 0.0,
        "ask_ratio": 0.0,

        "bid_count": 0,
        "ask_count": 0,

        "lower_price": None,
        "upper_price": None,

        "dominance": "balanced",
        "dominance_text": "균형"
    }

    if not orderbook:
        return result

    try:

        current_price = float(
            current_price
        )

    except Exception:

        return result

    if current_price <= 0:
        return result

    lower_price = (
        current_price
        * (1 - ORDERBOOK_RANGE)
    )

    upper_price = (
        current_price
        * (1 + ORDERBOOK_RANGE)
    )

    result["lower_price"] = lower_price
    result["upper_price"] = upper_price

    units = orderbook.get(
        "orderbook_units",
        []
    )

    bid_amount = 0.0
    ask_amount = 0.0

    bid_count = 0
    ask_count = 0

    for unit in units:

        try:

            bid_price = float(
                unit.get(
                    "bid_price",
                    0
                )
            )

            bid_size = float(
                unit.get(
                    "bid_size",
                    0
                )
            )

            ask_price = float(
                unit.get(
                    "ask_price",
                    0
                )
            )

            ask_size = float(
                unit.get(
                    "ask_size",
                    0
                )
            )

        except Exception:

            continue

        if (
            lower_price
            <= bid_price
            <= current_price
        ):

            bid_amount += (
                bid_price
                * bid_size
            )

            bid_count += 1

        if (
            current_price
            <= ask_price
            <= upper_price
        ):

            ask_amount += (
                ask_price
                * ask_size
            )

            ask_count += 1

    total_amount = (
        bid_amount
        + ask_amount
    )

    if total_amount > 0:

        bid_ratio = (
            bid_amount
            / total_amount
            * 100
        )

        ask_ratio = (
            ask_amount
            / total_amount
            * 100
        )

    else:

        bid_ratio = 0
        ask_ratio = 0

    difference = (
        bid_ratio
        - ask_ratio
    )

    if (
        difference
        >= ORDERBOOK_DOMINANCE_GAP
    ):

        dominance = "bid"
        dominance_text = "매수 우세"

    elif (
        difference
        <= -ORDERBOOK_DOMINANCE_GAP
    ):

        dominance = "ask"
        dominance_text = "매도 우세"

    else:

        dominance = "balanced"
        dominance_text = "균형"

    result.update({

        "bid_amount":
            bid_amount,

        "ask_amount":
            ask_amount,

        "total_amount":
            total_amount,

        "bid_ratio":
            bid_ratio,

        "ask_ratio":
            ask_ratio,

        "bid_count":
            bid_count,

        "ask_count":
            ask_count,

        "dominance":
            dominance,

        "dominance_text":
            dominance_text
    })

    return result


# =========================================================
# 업비트 캔들
# =========================================================

def get_upbit_candle(
    market,
    unit,
    count=200,
    to=None,
    include_current=False
):

    params = {

        "market":
            market,

        "count":
            min(
                max(
                    int(count),
                    1
                ),
                200
            )
    }

    if to:
        params["to"] = to

    response = retry(
        requests.get,
        f"https://api.upbit.com/v1/candles/minutes/{unit}",
        params=params,
        timeout=15
    )

    if response is None:
        return None

    try:

        df = pd.DataFrame(
            response.json()
        )

        if df.empty:
            return None

        df["o"] = pd.to_numeric(
            df["opening_price"],
            errors="coerce"
        )

        df["h"] = pd.to_numeric(
            df["high_price"],
            errors="coerce"
        )

        df["l"] = pd.to_numeric(
            df["low_price"],
            errors="coerce"
        )

        df["c"] = pd.to_numeric(
            df["trade_price"],
            errors="coerce"
        )

        df["volume_krw"] = pd.to_numeric(
            df["candle_acc_trade_price"],
            errors="coerce"
        )

        df["datetime"] = pd.to_datetime(
            df["candle_date_time_kst"],
            errors="coerce"
        )

        df = df.dropna(
            subset=[
                "datetime",
                "o",
                "h",
                "l",
                "c"
            ]
        )

        if not include_current:

            current = (
                get_current_candle_start(
                    unit
                )
            )

            df = df[
                df.datetime < current
            ]

        if df.empty:
            return None

        return (
            df
            .sort_values(
                "datetime"
            )
            .drop_duplicates(
                "datetime"
            )
            .reset_index(
                drop=True
            )
        )

    except Exception as e:

        log.error(
            f"업비트 {unit}분 오류 "
            f"{market}: {e}"
        )

        return None


def history_upbit(
    market,
    unit,
    required=200
):

    all_df = None
    to = None

    for _ in range(
        MAX_HISTORY_CHUNKS
    ):

        df = get_upbit_candle(
            market,
            unit,
            HISTORY_CHUNK,
            to
        )

        if (
            df is None
            or df.empty
        ):

            break

        all_df = (
            df.copy()
            if all_df is None
            else pd.concat(
                [
                    df,
                    all_df
                ],
                ignore_index=True
            )
        )

        all_df = (
            all_df
            .drop_duplicates(
                "datetime"
            )
            .sort_values(
                "datetime"
            )
            .reset_index(
                drop=True
            )
        )

        if len(all_df) >= required:
            return all_df

        to = (
            all_df
            .datetime
            .iloc[0]
            .strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        )

    return all_df


# =========================================================
# 현재 1H ROC 데이터
# =========================================================

def get_upbit_current_roc_data(
    market,
    current_price
):

    df = get_upbit_candle(
        market,
        ROC_TIMEFRAME,
        include_current=True
    )

    if (
        df is None
        or df.empty
    ):

        return None

    try:

        start = (
            get_current_candle_start(
                ROC_TIMEFRAME
            )
        )

        price = float(
            current_price
        )

        mask = (
            df.datetime == start
        )

        if mask.any():

            df.loc[
                mask,
                "c"
            ] = price

        else:

            row = (
                df.iloc[-1]
                .copy()
            )

            row["datetime"] = start
            row["c"] = price

            df = pd.concat(
                [
                    df,
                    pd.DataFrame(
                        [row]
                    )
                ],
                ignore_index=True
            )

        return (
            df
            .sort_values(
                "datetime"
            )
            .drop_duplicates(
                "datetime"
            )
            .reset_index(
                drop=True
            )
        )

    except Exception as e:

        log.error(
            f"현재 ROC 오류 "
            f"{market}: {e}"
        )

        return df


# =========================================================
# ROC
# =========================================================

def roc(
    df,
    period
):

    if (
        df is None
        or df.empty
        or "c" not in df.columns
    ):

        return None

    try:

        close = pd.to_numeric(
            df["c"],
            errors="coerce"
        )

        return (
            close
            / close.shift(
                int(period)
            )
            - 1
        ) * 100

    except Exception:

        return None


def roc_filter_analysis(
    df,
    periods=None
):

    if periods is None:
        periods = get_all_periods()

    result = {

        "direction":
            "none",

        "passed":
            False,

        "roc_values":
            {},

        "previous_values":
            {},

        "zero_crosses":
            {},

        "positive_count":
            0,

        "total_count":
            len(periods),

        "enabled_periods":
            periods.copy(),

        "all_periods":
            ROC_FILTER_PERIODS.copy()
    }

    if (
        df is None
        or df.empty
    ):

        return result

    values = {}
    previous_values = {}
    zero_crosses = {}

    positive_count = 0

    for period in periods:

        series = roc(
            df,
            period
        )

        if (
            series is None
            or series.empty
        ):

            continue

        current_value = series.iloc[-1]

        if pd.isna(
            current_value
        ):

            continue

        current_value = float(
            current_value
        )

        previous_value = None

        if len(series) >= 2:

            previous_value = (
                series.iloc[-2]
            )

            if not pd.isna(
                previous_value
            ):

                previous_value = float(
                    previous_value
                )

            else:

                previous_value = None

        values[
            period
        ] = current_value

        previous_values[
            period
        ] = previous_value

        zero_crosses[
            period
        ] = (
            previous_value is not None
            and previous_value <= 0
            and current_value >= 0
        )

        if current_value >= 0:
            positive_count += 1

    passed = (
        len(values)
        == len(periods)
        and positive_count
        == len(periods)
    )

    result.update({

        "direction":
            "long"
            if passed
            else "none",

        "passed":
            passed,

        "roc_values":
            values,

        "previous_values":
            previous_values,

        "zero_crosses":
            zero_crosses,

        "positive_count":
            positive_count,

        "total_count":
            len(periods)
    })

    return result


def roc_filter_display(
    result,
    timeframe
):

    result = dict(
        result or {}
    )

    result["timeframe"] = timeframe

    result[
        "enabled_periods"
    ] = get_enabled_periods(
        timeframe
    )

    result[
        "all_periods"
    ] = ROC_FILTER_PERIODS.copy()

    return result


# =========================================================
# EMA
# =========================================================

def ema_series(
    df,
    period
):

    if (
        df is None
        or df.empty
        or "c" not in df.columns
    ):

        return None

    try:

        close = pd.to_numeric(
            df["c"],
            errors="coerce"
        )

        return (
            close
            .ewm(
                span=int(period),
                adjust=False,
                min_periods=int(period)
            )
            .mean()
        )

    except Exception:

        return None


def ema_alignment_analysis(
    df,
    timeframe
):

    periods = get_enabled_ema_periods(
        timeframe
    )

    result = {

        "timeframe":
            timeframe,

        "periods":
            periods.copy(),

        "values":
            {},

        "direction":
            "none",

        "count":
            0,

        "valid":
            False
    }

    if (
        df is None
        or df.empty
        or not periods
    ):

        return result

    if len(periods) < 2:

        return result

    temp = df.copy()

    temp["datetime"] = pd.to_datetime(
        temp["datetime"],
        errors="coerce"
    )

    temp = (
        temp
        .dropna(
            subset=["datetime"]
        )
        .sort_values(
            "datetime"
        )
        .drop_duplicates(
            "datetime"
        )
        .reset_index(
            drop=True
        )
    )

    if temp.empty:
        return result

    ema_data = {}

    for period in periods:

        series = ema_series(
            temp,
            period
        )

        if (
            series is None
            or series.empty
        ):

            return result

        ema_data[
            period
        ] = series

    current_start = (
        get_current_candle_start(
            60
            if timeframe == "1H"
            else 240
        )
    )

    completed = temp[
        temp["datetime"] < current_start
    ]

    if completed.empty:
        return result

    last_index = completed.index[-1]

    current_values = {}

    for period in periods:

        series = ema_data.get(
            period
        )

        if (
            series is None
            or last_index >= len(series)
        ):

            return result

        value = series.iloc[
            last_index
        ]

        if pd.isna(value):

            return result

        current_values[
            period
        ] = float(value)

    values_in_order = [
        current_values[p]
        for p in periods
    ]

    long_alignment = all(
        values_in_order[i]
        > values_in_order[i + 1]
        for i in range(
            len(values_in_order) - 1
        )
    )

    short_alignment = all(
        values_in_order[i]
        < values_in_order[i + 1]
        for i in range(
            len(values_in_order) - 1
        )
    )

    if long_alignment:

        direction = "long"

    elif short_alignment:

        direction = "short"

    else:

        direction = "none"

    count = 0

    for idx in range(
        last_index,
        -1,
        -1
    ):

        row_values = []

        valid_row = True

        for period in periods:

            series = ema_data.get(
                period
            )

            value = series.iloc[
                idx
            ]

            if pd.isna(value):

                valid_row = False
                break

            row_values.append(
                float(value)
            )

        if not valid_row:
            break

        row_long = all(
            row_values[i]
            > row_values[i + 1]
            for i in range(
                len(row_values) - 1
            )
        )

        row_short = all(
            row_values[i]
            < row_values[i + 1]
            for i in range(
                len(row_values) - 1
            )
        )

        if direction == "long":

            if not row_long:
                break

            count += 1

        elif direction == "short":

            if not row_short:
                break

            count += 1

        else:

            break

    result.update({

        "values":
            current_values,

        "direction":
            direction,

        "count":
            count,

        "valid":
            direction in (
                "long",
                "short"
            )
    })

    return result


# =========================================================
# 활성 필터 상태
# =========================================================

def get_all_active_roc_status(
    filter_1h,
    filter_4h
):

    enabled = (
        get_enabled_all_filters()
    )

    if not enabled:
        return False, False

    current_ok = True
    previous_ok = True

    current_count = 0
    previous_count = 0

    for timeframe, period in enabled:

        info = (
            filter_1h
            if timeframe == "1H"
            else filter_4h
        )

        if not info:
            return False, False

        values = info.get(
            "roc_values",
            {}
        )

        previous = info.get(
            "previous_values",
            {}
        )

        current_value = values.get(
            period
        )

        previous_value = previous.get(
            period
        )

        if current_value is None:

            current_ok = False

        else:

            current_count += 1

            try:

                if float(
                    current_value
                ) < 0:

                    current_ok = False

            except Exception:

                current_ok = False

        if previous_value is None:

            previous_ok = False

        else:

            previous_count += 1

            try:

                if float(
                    previous_value
                ) < 0:

                    previous_ok = False

            except Exception:

                previous_ok = False

    return (

        current_count == len(enabled)
        and current_ok,

        previous_count == len(enabled)
        and previous_ok
    )


def all_active_roc_filters_pass(
    filter_1h,
    filter_4h
):

    current, _ = (
        get_all_active_roc_status(
            filter_1h,
            filter_4h
        )
    )

    return current


# =========================================================
# ROC5 0선 상향 돌파
# =========================================================

def roc5_zero_cross(r):

    if not r:
        return False

    current = r.get(
        "roc5"
    )

    previous = r.get(
        "roc5_previous"
    )

    if (
        current is None
        or previous is None
    ):

        return False

    try:

        current = float(
            current
        )

        previous = float(
            previous
        )

        return (
            previous < 0
            and current >= 0
        )

    except Exception:

        return False


# =========================================================
# ROC5 0선 하향 돌파
# =========================================================

def roc5_pullback_condition(r):

    if not r:
        return False

    current = r.get(
        "roc5"
    )

    previous = r.get(
        "roc5_previous"
    )

    if (
        current is None
        or previous is None
    ):

        return False

    try:

        current = float(
            current
        )

        previous = float(
            previous
        )

        return (
            previous > 0
            and current <= 0
        )

    except Exception:

        return False


# =========================================================
# 과거 ROC5 신호 시작점
# =========================================================

def find_latest_signal_start(
    df1h,
    df4h
):

    if (
        df1h is None
        or df1h.empty
    ):

        return None

    try:

        current_start = (
            get_current_candle_start(
                ROC_TIMEFRAME
            )
        )

        temp = df1h.copy()

        temp["datetime"] = pd.to_datetime(
            temp["datetime"],
            errors="coerce"
        )

        temp = (
            temp
            .dropna(
                subset=["datetime"]
            )
            .sort_values(
                "datetime"
            )
            .reset_index(
                drop=True
            )
        )

        temp = temp[
            temp["datetime"]
            < current_start
        ]

        if len(temp) < 2:
            return None

        roc5_series = roc(
            temp,
            5
        )

        if roc5_series is None:
            return None

        enabled = (
            get_enabled_all_filters()
        )

        if not enabled:
            return None

        for i in range(
            len(temp) - 1,
            0,
            -1
        ):

            current_value = (
                roc5_series.iloc[i]
            )

            previous_value = (
                roc5_series.iloc[i - 1]
            )

            if (
                pd.isna(current_value)
                or pd.isna(previous_value)
            ):

                continue

            if not (
                float(previous_value) < 0
                and float(current_value) >= 0
            ):

                continue

            candle_time = (
                temp[
                    "datetime"
                ].iloc[i]
            )

            filter_ok = True

            for timeframe, period in enabled:

                if timeframe == "1H":

                    series = roc(
                        temp,
                        period
                    )

                    if (
                        series is None
                        or i >= len(series)
                    ):

                        filter_ok = False
                        break

                    value = series.iloc[i]

                else:

                    if (
                        df4h is None
                        or df4h.empty
                    ):

                        filter_ok = False
                        break

                    h4 = df4h.copy()

                    h4["datetime"] = pd.to_datetime(
                        h4["datetime"],
                        errors="coerce"
                    )

                    h4 = (
                        h4
                        .dropna(
                            subset=["datetime"]
                        )
                        .sort_values(
                            "datetime"
                        )
                        .reset_index(
                            drop=True
                        )
                    )

                    candidates = h4[
                        h4["datetime"]
                        <= candle_time
                    ]

                    if candidates.empty:

                        filter_ok = False
                        break

                    h4_index = (
                        candidates.index[-1]
                    )

                    series = roc(
                        h4,
                        period
                    )

                    if (
                        series is None
                        or h4_index >= len(series)
                    ):

                        filter_ok = False
                        break

                    value = series.iloc[
                        h4_index
                    ]

                if (
                    value is None
                    or pd.isna(value)
                    or float(value) < 0
                ):

                    filter_ok = False
                    break

            if filter_ok:

                return normalize_datetime(
                    candle_time
                )

        return None

    except Exception as e:

        log.warning(
            f"ROC5 과거 시작점 오류: {e}"
        )

        return None


# =========================================================
# ★ 과거 ROC5 눌림 시작점
#
# 프로그램이 시작될 때 이미 ROC5가 음수인 경우
# 가장 최근의 0선 하향 돌파 지점을 찾아
# 눌림 COUNT를 복원한다.
# =========================================================

def find_latest_pullback_start(
    df1h
):

    if (
        df1h is None
        or df1h.empty
    ):

        return None

    try:

        current_start = (
            get_current_candle_start(
                ROC_TIMEFRAME
            )
        )

        temp = df1h.copy()

        temp["datetime"] = pd.to_datetime(
            temp["datetime"],
            errors="coerce"
        )

        temp = (
            temp
            .dropna(
                subset=["datetime"]
            )
            .sort_values(
                "datetime"
            )
            .reset_index(
                drop=True
            )
        )

        # 현재 진행 중인 캔들은 제외
        temp = temp[
            temp["datetime"]
            < current_start
        ]

        if len(temp) < 2:
            return None

        roc5_series = roc(
            temp,
            5
        )

        if (
            roc5_series is None
            or roc5_series.empty
        ):

            return None

        # 가장 최근 하향 돌파를 찾는다.
        #
        # 이전 > 0
        # 현재 <= 0
        #
        # 이 지점이 눌림 COUNT 1의 시작점
        for i in range(
            len(temp) - 1,
            0,
            -1
        ):

            current_value = (
                roc5_series.iloc[i]
            )

            previous_value = (
                roc5_series.iloc[i - 1]
            )

            if (
                pd.isna(current_value)
                or pd.isna(previous_value)
            ):

                continue

            try:

                current_value = float(
                    current_value
                )

                previous_value = float(
                    previous_value
                )

            except Exception:

                continue

            if (
                previous_value > 0
                and current_value <= 0
            ):

                return normalize_datetime(
                    temp[
                        "datetime"
                    ].iloc[i]
                )

        return None

    except Exception as e:

        log.warning(
            f"ROC5 과거 눌림 시작점 오류: {e}"
        )

        return None


# =========================================================
# 신호 + 눌림 통합 상태
# =========================================================

def update_signal_and_pullback(
    market,
    r,
    filter_pass,
    progress_candle_time,
    historical_start_candle=None,
    historical_pullback_start_candle=None
):

    market_key = str(
        market
    )

    progress_candle_time = (
        normalize_datetime(
            progress_candle_time
        )
    )

    roc5_current = r.get(
        "roc5"
    )

    roc5_previous = r.get(
        "roc5_previous"
    )

    roc5_cross = (
        roc5_zero_cross(r)
    )

    pullback_condition = (
        roc5_pullback_condition(r)
    )

    signal_state = (
        roc_signal_state.get(
            market_key
        )
    )

    pullback_state = (
        roc_pullback_state.get(
            market_key
        )
    )

    # =====================================================
    # 실시간 ROC5 하향 돌파
    # =====================================================

    if pullback_condition:

        if signal_state is not None:

            old_count = int(
                signal_state.get(
                    "count",
                    0
                )
            )

            log.info(
                f"[ROC5 SIGNAL END] "
                f"{market_key} | "
                f"COUNT={old_count} | "
                f"ROC5 0선 하향 돌파"
            )

            roc_signal_state.pop(
                market_key,
                None
            )

            signal_state = None

        if progress_candle_time is not None:

            roc_pullback_state[
                market_key
            ] = {

                "active":
                    True,

                "start_candle":
                    progress_candle_time,

                "count":
                    1,

                "last_candle":
                    progress_candle_time
            }

            pullback_state = (
                roc_pullback_state[
                    market_key
                ]
            )

            log.info(
                f"[ROC5 PULLBACK START] "
                f"{market_key} "
                f"📉1 | "
                f"ROC5={roc5_current}"
            )

    # =====================================================
    # 실시간 ROC5 상향 돌파
    # =====================================================

    elif roc5_cross:

        if pullback_state is not None:

            old_count = int(
                pullback_state.get(
                    "count",
                    0
                )
            )

            log.info(
                f"[ROC5 PULLBACK END] "
                f"{market_key} | "
                f"COUNT={old_count} | "
                f"ROC5 0선 상향 돌파"
            )

            roc_pullback_state.pop(
                market_key,
                None
            )

            pullback_state = None

        if (
            filter_pass
            and progress_candle_time is not None
        ):

            failed_candle = (
                roc_signal_failed_candle.get(
                    market_key
                )
            )

            if failed_candle != (
                progress_candle_time
            ):

                roc_signal_state[
                    market_key
                ] = {

                    "active":
                        True,

                    "cross_candle":
                        progress_candle_time,

                    "count":
                        1,

                    "last_candle":
                        progress_candle_time
                }

                signal_state = (
                    roc_signal_state[
                        market_key
                    ]
                )

                log.info(
                    f"[ROC5 SIGNAL START] "
                    f"{market_key} "
                    f"🚀1 | "
                    f"ROC5={roc5_current}"
                )

    # =====================================================
    # 과거 상승신호 복원
    # =====================================================

    elif signal_state is None:

        start_candle = None

        if historical_start_candle is not None:

            start_candle = (
                historical_start_candle
            )

        if start_candle is not None:

            roc_signal_state[
                market_key
            ] = {

                "active":
                    True,

                "cross_candle":
                    normalize_datetime(
                        start_candle
                    ),

                "count":
                    1,

                "last_candle":
                    progress_candle_time
            }

            signal_state = (
                roc_signal_state[
                    market_key
                ]
            )

    # =====================================================
    # ★ 과거 눌림 복원
    #
    # 현재 ROC5가 이미 음수이고
    # 과거 하향 돌파 지점이 발견되면
    # 눌림 상태를 생성한다.
    # =====================================================

    if (
        pullback_state is None
        and historical_pullback_start_candle is not None
    ):

        try:

            current_roc5_value = float(
                roc5_current
            )

        except Exception:

            current_roc5_value = None

        if (
            current_roc5_value is not None
            and current_roc5_value <= 0
            and progress_candle_time is not None
        ):

            start_candle = (
                normalize_datetime(
                    historical_pullback_start_candle
                )
            )

            distance = candle_distance(
                start_candle,
                progress_candle_time,
                ROC_TIMEFRAME
            )

            restored_count = (
                distance + 1
            )

            roc_pullback_state[
                market_key
            ] = {

                "active":
                    True,

                "start_candle":
                    start_candle,

                "count":
                    restored_count,

                "last_candle":
                    progress_candle_time
            }

            pullback_state = (
                roc_pullback_state[
                    market_key
                ]
            )

            log.info(
                f"[ROC5 PULLBACK RESTORE] "
                f"{market_key} "
                f"📉({restored_count}) | "
                f"시작={start_candle} | "
                f"ROC5={roc5_current}"
            )

    # =====================================================
    # 상승 신호 상태 확인
    # =====================================================

    signal_state = (
        roc_signal_state.get(
            market_key
        )
    )

    if signal_state is not None:

        roc5_negative = False

        try:

            if (
                roc5_current is not None
                and float(roc5_current) < 0
            ):

                roc5_negative = True

        except Exception:

            pass

        if roc5_negative:

            old_count = int(
                signal_state.get(
                    "count",
                    0
                )
            )

            if progress_candle_time is not None:

                roc_signal_failed_candle[
                    market_key
                ] = progress_candle_time

            log.info(
                f"[ROC5 SIGNAL END] "
                f"{market_key} | "
                f"COUNT={old_count} | "
                f"ROC5 음수"
            )

            roc_signal_state.pop(
                market_key,
                None
            )

            signal_state = None

        elif not filter_pass:

            old_count = int(
                signal_state.get(
                    "count",
                    0
                )
            )

            if progress_candle_time is not None:

                roc_signal_failed_candle[
                    market_key
                ] = progress_candle_time

            log.info(
                f"[ROC5 SIGNAL END] "
                f"{market_key} | "
                f"COUNT={old_count} | "
                f"활성 ROC 필터 미통과"
            )

            roc_signal_state.pop(
                market_key,
                None
            )

            signal_state = None

        else:

            cross_candle = (
                signal_state.get(
                    "cross_candle"
                )
            )

            if (
                cross_candle is not None
                and progress_candle_time is not None
            ):

                distance = candle_distance(
                    cross_candle,
                    progress_candle_time,
                    ROC_TIMEFRAME
                )

                signal_state[
                    "count"
                ] = (
                    distance + 1
                )

                signal_state[
                    "last_candle"
                ] = progress_candle_time

    # =====================================================
    # 눌림 상태 확인
    # =====================================================

    pullback_state = (
        roc_pullback_state.get(
            market_key
        )
    )

    if pullback_state is not None:

        roc5_positive = False

        try:

            if (
                roc5_current is not None
                and float(roc5_current) > 0
            ):

                roc5_positive = True

        except Exception:

            pass

        if roc5_positive:

            old_count = int(
                pullback_state.get(
                    "count",
                    0
                )
            )

            log.info(
                f"[ROC5 PULLBACK END] "
                f"{market_key} | "
                f"COUNT={old_count} | "
                f"ROC5 양수 복귀"
            )

            roc_pullback_state.pop(
                market_key,
                None
            )

            pullback_state = None

        else:

            start_candle = (
                pullback_state.get(
                    "start_candle"
                )
            )

            if (
                start_candle is not None
                and progress_candle_time is not None
            ):

                distance = candle_distance(
                    start_candle,
                    progress_candle_time,
                    ROC_TIMEFRAME
                )

                pullback_state[
                    "count"
                ] = (
                    distance + 1
                )

                pullback_state[
                    "last_candle"
                ] = (
                    progress_candle_time
                )

    # =====================================================
    # 최종 상태
    # =====================================================

    signal_state = (
        roc_signal_state.get(
            market_key
        )
    )

    pullback_state = (
        roc_pullback_state.get(
            market_key
        )
    )

    signal_active = bool(
        signal_state
        and signal_state.get(
            "active",
            True
        )
        and filter_pass
    )

    pullback_active = bool(
        pullback_state
        and pullback_state.get(
            "active",
            True
        )
    )

    signal_count = 0

    if signal_state is not None:

        signal_count = int(
            signal_state.get(
                "count",
                0
            )
        )

    pullback_count = 0

    if pullback_state is not None:

        pullback_count = int(
            pullback_state.get(
                "count",
                0
            )
        )

    return {

        "signal_active":
            signal_active,

        "signal_count":
            signal_count,

        "pullback_active":
            pullback_active,

        "pullback_count":
            pullback_count,

        "roc5_cross":
            roc5_cross,

        "roc5_pullback":
            pullback_condition
    }


# =========================================================
# 일봉 등락
# =========================================================

def daily_change_upbit(
    market
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

        if len(data) < 2:
            return None

        current = float(
            data[0]["trade_price"]
        )

        previous = float(
            data[1]["trade_price"]
        )

        if previous == 0:
            return None

        return [
            (
                current
                - previous
            )
            / previous
            * 100
        ]

    except Exception:

        return None


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


def format_change(x):

    x = get_change_value(
        x
    )

    if x is None:
        return "-"

    if x > 0:

        return (
            '<span class="up">'
            f'▲+{x:.1f}%'
            '</span>'
        )

    if x < 0:

        return (
            '<span class="down">'
            f'▼{x:.1f}%'
            '</span>'
        )

    return (
        '<span class="zero">'
        '0.0%'
        '</span>'
    )


def format_volume(v):

    try:

        v = float(v)

    except Exception:

        return "-"

    if v >= 1e12:
        return f"{v / 1e12:.1f}조"

    if v >= 1e8:
        return f"{v / 1e8:.0f}억"

    if v >= 1e4:
        return f"{v / 1e4:.0f}만"

    return f"{v:,.0f}"


# =========================================================
# 분석
# =========================================================

def analyze(
    market,
    current_price
):

    all_periods = (
        get_all_periods()
    )

    df1h = history_upbit(
        market,
        60,
        required=200
    )

    if (
        df1h is None
        or df1h.empty
    ):

        return None

    df4h = history_upbit(
        market,
        240,
        required=200
    )

    if (
        df4h is None
        or df4h.empty
    ):

        return None

    df_current = (
        get_upbit_current_roc_data(
            market,
            current_price
        )
    )

    if (
        df_current is None
        or df_current.empty
    ):

        return None

    r1_raw = roc_filter_analysis(
        df1h,
        all_periods
    )

    r1 = roc_filter_display(
        r1_raw,
        "1H"
    )

    r4_raw = roc_filter_analysis(
        df4h,
        all_periods
    )

    r4 = roc_filter_display(
        r4_raw,
        "4H"
    )

    ema_1h = ema_alignment_analysis(
        df1h,
        "1H"
    )

    ema_4h = ema_alignment_analysis(
        df4h,
        "4H"
    )

    r1_current_raw = (
        roc_filter_analysis(
            df_current,
            all_periods
        )
    )

    r1_current = (
        roc_filter_display(
            r1_current_raw,
            "1H"
        )
    )

    roc5_current = (
        r1_current
        .get(
            "roc_values",
            {}
        )
        .get(5)
    )

    roc5_previous = (
        r1_current
        .get(
            "previous_values",
            {}
        )
        .get(5)
    )

    r = {

        "roc5":
            roc5_current,

        "roc5_previous":
            roc5_previous,

        "roc5_cross":
            False,

        "roc5_pullback":
            False
    }

    r["roc5_cross"] = (
        roc5_zero_cross(r)
    )

    r["roc5_pullback"] = (
        roc5_pullback_condition(r)
    )

    filter_pass = (
        all_active_roc_filters_pass(
            r1,
            r4
        )
    )

    progress_candle_time = (
        get_current_candle_start(
            ROC_TIMEFRAME
        )
    )

    historical_start_candle = None

    if market not in roc_signal_state:

        historical_start_candle = (
            find_latest_signal_start(
                df1h,
                df4h
            )
        )

    # =====================================================
    # ★ 과거 눌림 시작점 찾기
    #
    # 현재 이미 눌림 상태인 경우에만
    # 과거 하향 돌파를 복원한다.
    # =====================================================

    historical_pullback_start_candle = None

    try:

        current_roc5_value = float(
            roc5_current
        )

    except Exception:

        current_roc5_value = None

    if (
        market not in roc_pullback_state
        and current_roc5_value is not None
        and current_roc5_value <= 0
    ):

        historical_pullback_start_candle = (
            find_latest_pullback_start(
                df1h
            )
        )

    state = update_signal_and_pullback(
        market=market,
        r=r,
        filter_pass=filter_pass,
        progress_candle_time=(
            progress_candle_time
        ),
        historical_start_candle=(
            historical_start_candle
        ),
        historical_pullback_start_candle=(
            historical_pullback_start_candle
        )
    )

    changes = (
        daily_change_upbit(
            market
        )
    )

    change_value = (
        get_change_value(
            changes
        )
    )

    daily_pass = (
        change_value is not None
        and change_value >= 0
    )

    return {

        "roc_filter_1h":
            r1,

        "roc_filter_high":
            r4,

        "ema_1h":
            ema_1h,

        "ema_4h":
            ema_4h,

        "roc":
            r,

        "changes":
            changes,

        "filter_pass":
            filter_pass,

        "daily_pass":
            daily_pass,

        "breakout_qualified":
            (
                (
                    state[
                        "signal_active"
                    ]
                    or
                    state[
                        "pullback_active"
                    ]
                )
                and filter_pass
                and daily_pass
            ),

        "signal_active":
            state[
                "signal_active"
            ],

        "signal_count":
            state[
                "signal_count"
            ],

        "pullback_active":
            state[
                "pullback_active"
            ],

        "pullback_count":
            state[
                "pullback_count"
            ],

        "roc5_cross":
            state[
                "roc5_cross"
            ],

        "roc5_pullback":
            state[
                "roc5_pullback"
            ],

        "df1h":
            df1h
    }


# =========================================================
# ROW
# =========================================================

def make_row(
    rank,
    name,
    volume,
    analysis,
    current_price=None,
    orderbook_info=None
):

    a = analysis or {}
    ob = orderbook_info or {}

    return {

        "rank":
            rank,

        "name":
            name,

        "change":
            format_change(
                a.get(
                    "changes"
                )
            ),

        "change_value":
            get_change_value(
                a.get(
                    "changes"
                )
            ),

        "daily_pass":
            bool(
                a.get(
                    "daily_pass",
                    False
                )
            ),

        "volume":
            format_volume(
                volume
            ),

        "current_price":
            current_price,

        "roc_filter_1h":
            a.get(
                "roc_filter_1h",
                {}
            ),

        "roc_filter_high":
            a.get(
                "roc_filter_high",
                {}
            ),

        "ema_1h":
            a.get(
                "ema_1h",
                {}
            ),

        "ema_4h":
            a.get(
                "ema_4h",
                {}
            ),

        "roc":
            a.get(
                "roc",
                {}
            ),

        "filter_pass":
            bool(
                a.get(
                    "filter_pass",
                    False
                )
            ),

        "breakout_qualified":
            bool(
                a.get(
                    "breakout_qualified",
                    False
                )
            ),

        "signal_active":
            bool(
                a.get(
                    "signal_active",
                    False
                )
            ),

        "signal_count":
            int(
                a.get(
                    "signal_count",
                    0
                )
            ),

        "pullback_active":
            bool(
                a.get(
                    "pullback_active",
                    False
                )
            ),

        "pullback_count":
            int(
                a.get(
                    "pullback_count",
                    0
                )
            ),

        "roc5_cross":
            bool(
                a.get(
                    "roc5_cross",
                    False
                )
            ),

        "roc5_pullback":
            bool(
                a.get(
                    "roc5_pullback",
                    False
                )
            ),

        "bid_amount":
            float(
                ob.get(
                    "bid_amount",
                    0
                )
            ),

        "ask_amount":
            float(
                ob.get(
                    "ask_amount",
                    0
                )
            ),

        "bid_ratio":
            float(
                ob.get(
                    "bid_ratio",
                    0
                )
            ),

        "ask_ratio":
            float(
                ob.get(
                    "ask_ratio",
                    0
                )
            ),

        "orderbook_dominance":
            ob.get(
                "dominance",
                "balanced"
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
    global latest_upbit_orderbook

    markets = sorted(
        get_upbit_markets(),
        key=lambda x:
            x["volume_24h"],
        reverse=True
    )

    top_markets = markets[
        :TOP_N
    ]

    market_codes = [
        x["market"]
        for x in top_markets
    ]

    orderbooks = (
        get_upbit_orderbooks(
            market_codes
        )
    )

    latest_upbit_orderbook = (
        orderbooks.copy()
    )

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

        ob = (
            calculate_orderbook_amount(
                orderbooks.get(
                    market
                ),
                price
            )
        )

        row = make_row(
            rank,
            coin,
            item[
                "volume_24h"
            ],
            analysis,
            price,
            ob
        )

        rows.append(
            row
        )

    latest_upbit_data = rows

    latest_upbit_update_time = (
        kst()
    )

    log.info(
        f"TOP{TOP_N} 업데이트 완료"
    )


# =========================================================
# OKX
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

        price = float(
            data[0]["trade_price"]
        )

        return price

    except Exception:

        return None


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
# 가격
# =========================================================

def format_market_price(
    price
):

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

        return f"{price:,.0f}"

    if price >= 1:

        return f"{price:,.2f}"

    return f"{price:.6f}"


# =========================================================
# ROC 필터 HTML
# =========================================================

def roc_filter_html(
    r,
    timeframe
):

    settings = roc_settings()

    if not r:
        r = {}

    values = r.get(
        "roc_values",
        {}
    )

    parts = []

    for period in ROC_FILTER_PERIODS:

        value = values.get(
            period
        )

        if value is None:

            icon = "⚪"
            cls = "roc-zero"

        else:

            try:

                value = float(
                    value
                )

                if value > 0:

                    icon = "🟢"
                    cls = "roc-up"

                elif value < 0:

                    icon = "🔴"
                    cls = "roc-down"

                else:

                    icon = "⚪"
                    cls = "roc-zero"

            except Exception:

                icon = "⚪"
                cls = "roc-zero"

        setting = settings[
            period
        ][
            timeframe
        ]

        setting_cls = (
            "roc-active"
            if setting == "Y"
            else "roc-disabled"
        )

        parts.append(
            f"""
            <span class="
                roc-item
                {setting_cls}
                {cls}
            ">
                {period}{icon}
            </span>
            """
        )

    return (
        '<div class="roc-filter-all">'
        + "".join(parts)
        + '</div>'
    )


def filter_html(
    r1,
    r4
):

    return f"""
    <div class="filter-detail">

        <div class="filter-line">

            <span class="filter-timeframe">
                1H
            </span>

            {roc_filter_html(
                r1,
                "1H"
            )}

        </div>

        <div class="filter-line">

            <span class="filter-timeframe">
                4H
            </span>

            {roc_filter_html(
                r4,
                "4H"
            )}

        </div>

    </div>
    """


# =========================================================
# ROC COUNT HTML
# =========================================================

def signal_html(
    row
):

    if not row:
        return "-"

    signal_active = row.get(
        "signal_active",
        False
    )

    pullback_active = row.get(
        "pullback_active",
        False
    )

    signal_count = int(
        row.get(
            "signal_count",
            0
        )
    )

    pullback_count = int(
        row.get(
            "pullback_count",
            0
        )
    )

    result = []

    if signal_active:

        result.append(
            f"""
            <span class="signal-item">

                <span class="signal-rocket">
                    🚀
                </span>

                <span class="signal-count">
                    ({signal_count})
                </span>

            </span>
            """
        )

    if pullback_active:

        result.append(
            f"""
            <span class="pullback-item">

                <span class="pullback-icon">
                    📉
                </span>

                <span class="pullback-count">
                    ({pullback_count})
                </span>

            </span>
            """
        )

    if not result:

        return (
            '<span class="muted">'
            '-'
            '</span>'
        )

    return (
        '<div class="signal-wrap">'
        + "".join(result)
        + '</div>'
    )


# =========================================================
# TOP 리스트 전용 ROC COUNT
# =========================================================

def top_signal_count_html(
    row
):

    if not row:
        return "-"

    signal_active = row.get(
        "signal_active",
        False
    )

    pullback_active = row.get(
        "pullback_active",
        False
    )

    signal_count = int(
        row.get(
            "signal_count",
            0
        )
    )

    pullback_count = int(
        row.get(
            "pullback_count",
            0
        )
    )

    if not (
        signal_active
        or pullback_active
    ):

        return (
            '<span class="muted">'
            '-'
            '</span>'
        )

    signal_text = (
        f"""
        <span class="top-signal-count">
            🚀({signal_count})
        </span>
        """
        if signal_active
        else ""
    )

    pullback_text = (
        f"""
        <span class="top-pullback-count">
            📉({pullback_count})
        </span>
        """
        if pullback_active
        else ""
    )

    return f"""
    <div class="top-count-wrap">

        {signal_text}

        {pullback_text}

    </div>
    """


# =========================================================
# EMA HTML
# =========================================================

def ema_signal_html(
    row
):

    if not row:
        return "-"

    ema_1h = row.get(
        "ema_1h",
        {}
    )

    ema_4h = row.get(
        "ema_4h",
        {}
    )

    parts = []

    if USE_1H_ROC_FILTER == "Y":

        direction = ema_1h.get(
            "direction",
            "none"
        )

        count = int(
            ema_1h.get(
                "count",
                0
            )
        )

        if (
            direction == "long"
            and count > 0
        ):

            parts.append(
                f"""
                <span class="ema-signal-long">
                    🟢({count})
                </span>
                """
            )

        elif (
            direction == "short"
            and count > 0
        ):

            parts.append(
                f"""
                <span class="ema-signal-short">
                    🔴({count})
                </span>
                """
            )

    if USE_4H_ROC_FILTER == "Y":

        direction = ema_4h.get(
            "direction",
            "none"
        )

        count = int(
            ema_4h.get(
                "count",
                0
            )
        )

        if (
            direction == "long"
            and count > 0
        ):

            parts.append(
                f"""
                <span class="ema-signal-long">
                    🟢({count})
                </span>
                """
            )

        elif (
            direction == "short"
            and count > 0
        ):

            parts.append(
                f"""
                <span class="ema-signal-short">
                    🔴({count})
                </span>
                """
            )

    if not parts:

        return (
            '<span class="muted">'
            '-'
            '</span>'
        )

    return f"""
    <div class="ema-signal-wrap">

        {"".join(parts)}

    </div>
    """


# =========================================================
# 상승 신호 영역 EMA 필터
# =========================================================

def ema_long_filter_pass(
    row
):

    if not row:
        return False

    ema_1h = row.get(
        "ema_1h",
        {}
    )

    ema_4h = row.get(
        "ema_4h",
        {}
    )

    if USE_1H_ROC_FILTER == "Y":

        direction = ema_1h.get(
            "direction",
            "none"
        )

        count = int(
            ema_1h.get(
                "count",
                0
            )
        )

        if (
            direction == "long"
            and 0 < count <= EMA_LONG_MAX_COUNT
        ):

            return True

    if USE_4H_ROC_FILTER == "Y":

        direction = ema_4h.get(
            "direction",
            "none"
        )

        count = int(
            ema_4h.get(
                "count",
                0
            )
        )

        if (
            direction == "long"
            and 0 < count <= EMA_LONG_MAX_COUNT
        ):

            return True

    return False


# =========================================================
# 호가 HTML
# =========================================================

def orderbook_html(
    row
):

    if not row:
        return "-"

    bid_ratio = float(
        row.get(
            "bid_ratio",
            0
        )
    )

    ask_ratio = float(
        row.get(
            "ask_ratio",
            0
        )
    )

    dominance = row.get(
        "orderbook_dominance",
        "balanced"
    )

    if dominance == "bid":

        dominance_html = (
            '<span class="ob-dominance bid-dominance">'
            '▲ 매수 우세'
            '</span>'
        )

    elif dominance == "ask":

        dominance_html = (
            '<span class="ob-dominance ask-dominance">'
            '▼ 매도 우세'
            '</span>'
        )

    else:

        dominance_html = (
            '<span class="ob-dominance balanced-dominance">'
            '◆ 균형'
            '</span>'
        )

    return f"""
    <div class="orderbook-wrap">

        <div class="orderbook-row">

            <span class="orderbook-label ask-label">
                매도대기
            </span>

            <span class="orderbook-amount ask-amount">
                {format_volume(
                    row.get(
                        "ask_amount",
                        0
                    )
                )}
            </span>

            <div class="orderbook-bar-box">

                <div
                    class="orderbook-bar ask-bar"
                    style="width:{ask_ratio:.1f}%"
                ></div>

            </div>

            <span class="orderbook-ratio ask-ratio">
                {ask_ratio:.1f}%
            </span>

        </div>

        <div class="orderbook-row">

            <span class="orderbook-label bid-label">
                매수대기
            </span>

            <span class="orderbook-amount bid-amount">
                {format_volume(
                    row.get(
                        "bid_amount",
                        0
                    )
                )}
            </span>

            <div class="orderbook-bar-box">

                <div
                    class="orderbook-bar bid-bar"
                    style="width:{bid_ratio:.1f}%"
                ></div>

            </div>

            <span class="orderbook-ratio bid-ratio">
                {bid_ratio:.1f}%
            </span>

        </div>

        <div class="orderbook-bottom">

            ±1%

            {dominance_html}

        </div>

    </div>
    """


# =========================================================
# ROW HTML
# =========================================================

def rows_html(
    data
):

    out = []

    for x in data:

        cls_list = []

        signal_active = x.get(
            "signal_active",
            False
        )

        pullback_active = x.get(
            "pullback_active",
            False
        )

        signal_count = int(
            x.get(
                "signal_count",
                0
            )
        )

        pullback_count = int(
            x.get(
                "pullback_count",
                0
            )
        )

        filter_pass = x.get(
            "filter_pass",
            False
        )

        ema_filter_pass = (
            ema_long_filter_pass(
                x
            )
        )

        # =================================================
        # COUNT 1~5까지 반짝임
        # =================================================

        if (
            filter_pass
            and ema_filter_pass
            and
            (
                (
                    signal_active
                    and signal_count in (
                        1,
                        2,
                        3,
                        4,
                        5
                    )
                )
                or
                (
                    pullback_active
                    and pullback_count in (
                        1,
                        2,
                        3,
                        4,
                        5
                    )
                )
            )
        ):

            cls_list.append(
                "signal-flash-one"
            )

        cls = " ".join(
            cls_list
        )

        filter_content = filter_html(
            x.get(
                "roc_filter_1h"
            ),
            x.get(
                "roc_filter_high"
            )
        )

        top_count_content = (
            top_signal_count_html(
                x
            )
        )

        ema_content = (
            ema_signal_html(
                x
            )
        )

        orderbook_content = (
            orderbook_html(
                x
            )
        )

        out.append(
            f"""
            <tr class="{cls}">

                <td>
                    {x.get("rank", "-")}
                </td>

                <td class="coin">

                    <b>
                        {x.get("name", "-")}
                    </b>

                    <small>
                        {x.get("change", "-")}
                    </small>

                </td>

                <td class="vol">
                    {x.get("volume", "-")}
                </td>

                <td class="ema">
                    {filter_content}
                </td>

                <td class="signal-cell">
                    {top_count_content}
                </td>

                <td class="signal-cell">
                    {ema_content}
                </td>

            </tr>

            <tr class="
                orderbook-subrow
                {cls}
            ">

                <td colspan="6">

                    {orderbook_content}

                </td>

            </tr>
            """
        )

    return "".join(
        out
    )


# =========================================================
# 테이블
# =========================================================

def table_html(
    data
):

    rows = rows_html(
        data
    )

    if not rows:

        rows = """
        <tr>

            <td
                colspan="6"
                class="empty"
            >
                현재 후보 없음
            </td>

        </tr>
        """

    return f"""
    <div class="table-wrap">

        <table>

            <thead>

                <tr>

                    <th>#</th>

                    <th>코인</th>

                    <th>거래대금</th>

                    <th>ROC 필터</th>

                    <th>COUNT</th>

                    <th>EMA</th>

                </tr>

            </thead>

            <tbody>

                {rows}

            </tbody>

        </table>

    </div>
    """


# =========================================================
# 상승 신호
# =========================================================

def focus_section(
    data
):

    rows = [
        x
        for x in data

        if (
            # =============================================
            # ROC 필터 통과
            # =============================================

            x.get(
                "filter_pass",
                False
            )

            # =============================================
            # ROC5 COUNT 1~5
            # =============================================

            and

            (
                (
                    x.get(
                        "signal_active",
                        False
                    )

                    and

                    int(
                        x.get(
                            "signal_count",
                            0
                        )
                    ) in (
                        1,
                        2,
                        3,
                        4,
                        5
                    )
                )

                or

                (
                    x.get(
                        "pullback_active",
                        False
                    )

                    and

                    int(
                        x.get(
                            "pullback_count",
                            0
                        )
                    ) in (
                        1,
                        2,
                        3,
                        4,
                        5
                    )
                )
            )

            # =============================================
            # EMA 상승 COUNT 필터
            # =============================================

            and

            ema_long_filter_pass(
                x
            )

            # =============================================
            # 당일 음수 제외
            # =============================================

            and

            x.get(
                "daily_pass",
                False
            )
        )
    ]

    return f"""
    <div class="section-title long-title">

        <span class="section-title-main">
            🚀 상승 신호
        </span>

        <span class="section-title-sub">
            ROC5 돌파
            · 필터 통과
            · 당일 음수 제외
            · EMA 상승 COUNT ≤ {EMA_LONG_MAX_COUNT}
            · 🚀1~5 📉1~5만 표시
            · {kst()} KST
        </span>

    </div>

    {table_html(rows)}
    """


# =========================================================
# TOP
# =========================================================

def section(
    data,
    update_time
):

    return f"""
    <div class="section-title">

        <span class="section-title-main">
            🏆 업비트 TOP{TOP_N}
        </span>

        <span class="section-title-sub">
            {update_time} KST
        </span>

    </div>

    {table_html(data)}
    """


# =========================================================
# BTC 1H ROC 상태 HTML
# =========================================================

def btc_1h_roc_status_html(
    btc_row
):

    if not btc_row:
        return ""

    analysis = btc_row.get(
        "analysis",
        {}
    )

    if not analysis:
        return ""

    df1h = analysis.get(
        "df1h"
    )

    if (
        df1h is None
        or df1h.empty
    ):
        return ""

    periods = [
        5,
        10,
        20,
        50,
        200
    ]

    items = []

    for period in periods:

        series = roc(
            df1h,
            period
        )

        if (
            series is None
            or series.empty
        ):

            icon = "⚪"
            count = 0

        else:

            valid = series.dropna()

            if valid.empty:

                icon = "⚪"
                count = 0

            else:

                current = float(
                    valid.iloc[-1]
                )

                if current >= 0:

                    icon = "🟢"

                    count = 0

                    for value in reversed(
                        valid.tolist()
                    ):

                        if float(value) >= 0:

                            count += 1

                        else:

                            break

                else:

                    icon = "🔴"

                    count = 0

                    for value in reversed(
                        valid.tolist()
                    ):

                        if float(value) < 0:

                            count += 1

                        else:

                            break

        items.append(
            f"""
            <div class="btc-roc-item">

                <div class="btc-roc-period">
                    {period}
                </div>

                <div class="btc-roc-icon">
                    {icon}
                </div>

                <div class="btc-roc-count">
                    ({count})
                </div>

            </div>
            """
        )

    return f"""
    <div class="btc-roc-section">

        <div class="btc-roc-title">
            1시간 ROC
        </div>

        <div class="btc-roc-grid">

            {"".join(items)}

        </div>

    </div>
    """


# =========================================================
# 시장 요약
# =========================================================

def market_summary_html():

    btc = None

    for row in latest_upbit_data:

        if row.get(
            "name"
        ) == "BTC":

            btc = row

            break

    if btc:

        price = format_market_price(
            btc.get(
                "current_price"
            )
        )

        change = format_change(
            btc.get(
                "change_value"
            )
        )

        signal = signal_html(
            btc
        )

        roc_status = (
            btc_1h_roc_status_html(
                btc
            )
        )

    else:

        price = "-"
        change = "-"
        signal = "-"
        roc_status = ""

    return f"""
    <div class="market-summary">

        <div class="market-title">

            <span class="market-title-main">
                ₿ BTC 시장 시황
            </span>

            <span class="market-title-sub">
                활성 ROC
                {get_filter_setting_text()}
            </span>

        </div>

        <div class="btc-top">

            <span class="btc-name">
                ₿ BTC
            </span>

            <span class="btc-price">
                {price}
            </span>

            <span class="btc-change">
                {change}
            </span>

            <span>
                {signal}
            </span>

        </div>

        {roc_status}

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
background:#0b0e12;
color:#e5e9ed;

font-family:
    -apple-system,
    BlinkMacSystemFont,
    "Segoe UI",
    Arial,
    sans-serif;

font-size:8px;

padding:3px;
}

h1{

margin:2px 3px 5px;

color:#dfe4e8;

font-size:12px;

line-height:15px;

font-weight:900;
}

.market-title,
.section-title{

display:flex;

align-items:center;

gap:6px;

width:100%;

min-height:21px;

padding:4px 6px;

background:#14181d;

border:1px solid #252b32;

border-left:3px solid #59616a;

border-radius:5px;

white-space:nowrap;

overflow:hidden;
}

.section-title{

margin:7px 0 5px;
}

.market-title-main,
.section-title-main{

flex:none;

color:#e7ebef;

font-size:8px;

font-weight:900;
}

.market-title-sub,
.section-title-sub{

min-width:0;

color:#737c86;

font-size:5.5px;

font-weight:700;

overflow:hidden;

text-overflow:ellipsis;
}

.market-summary{

width:100%;

margin:2px 0 4px;

padding:4px;

background:#0f1318;

border-top:1px solid #242a31;

border-bottom:1px solid #242a31;

border-radius:4px;
}

.btc-top{

display:flex;

align-items:center;

gap:5px;

min-height:22px;

white-space:nowrap;

overflow:hidden;
}

.btc-name{

color:#dce1e5;

font-size:6.5px;

font-weight:900;
}

.btc-price{

flex:1;

color:#e5e9ed;

font-size:6px;

font-weight:800;
}

.btc-change{

font-size:8px;

font-weight:900;
}

.btc-roc-section{

width:100%;

margin-top:3px;

padding-top:3px;

border-top:1px solid #20262c;
}

.btc-roc-title{

margin-bottom:2px;

color:#737c86;

font-size:5px;

font-weight:900;

text-align:left;
}

.btc-roc-grid{

display:grid;

grid-template-columns:
    repeat(5, 1fr);

width:100%;

gap:2px;
}

.btc-roc-item{

display:flex;

flex-direction:column;

align-items:center;

justify-content:center;

min-height:25px;

background:#14181d;

border:1px solid #242a31;

border-radius:3px;
}

.btc-roc-period{

color:#737c86;

font-size:4.5px;

font-weight:900;

line-height:6px;
}

.btc-roc-icon{

font-size:8px;

line-height:9px;
}

.btc-roc-count{

color:#cdd3d8;

font-size:5px;

font-weight:900;

line-height:7px;
}

.table-wrap{

width:100%;

overflow:hidden;

border:1px solid #272d34;

border-radius:6px;

background:#15191e;
}

table{

width:100%;

table-layout:fixed;

border-collapse:collapse;
}

thead{

background:#101419;
}

th{

height:18px;

padding:2px 1px;

color:#727b85;

border-bottom:1px solid #292f36;

font-size:5px;

font-weight:800;

text-align:center;
}

td{

height:27px;

padding:1px;

color:#d8dde2;

border-bottom:1px solid #22282e;

text-align:center;

vertical-align:middle;

overflow:hidden;
}

th:nth-child(1),
td:nth-child(1){
width:6%;
}

th:nth-child(2),
td:nth-child(2){
width:15%;
}

th:nth-child(3),
td:nth-child(3){
width:14%;
}

th:nth-child(4),
td:nth-child(4){
width:27%;
}

th:nth-child(5),
td:nth-child(5){
width:17%;
}

th:nth-child(6),
td:nth-child(6){
width:21%;
}

.coin{

text-align:left!important;
}

.coin b{

display:block;

color:#e0e5e9;

font-size:6.5px;

line-height:8px;

font-weight:800;

white-space:nowrap;

overflow:hidden;

text-overflow:ellipsis;
}

.coin small{

display:block;

font-size:4.5px;

line-height:6px;
}

.vol{

color:#cdd3d8;

font-size:6px;

font-weight:800;

white-space:nowrap;
}

.ema{

text-align:center!important;
}

.filter-detail{

display:flex;

flex-direction:column;

gap:1px;

width:100%;
}

.filter-line{

display:flex;

align-items:center;

width:100%;
}

.filter-timeframe{

width:12px;

color:#c4cbd1;

font-size:4.5px;

font-weight:900;

text-align:left;
}

.roc-filter-all{

display:flex;

align-items:center;

gap:2px;

white-space:nowrap;
}

.roc-item{

font-size:4.5px;

line-height:8px;

font-weight:900;
}

.roc-active{

opacity:1;
}

.roc-disabled{

opacity:.38;
}

.roc-up{

color:#62b58a!important;
}

.roc-down{

color:#c97878!important;
}

.roc-zero{

color:#68717b!important;
}

.top-count-wrap{

display:flex;

align-items:center;

justify-content:center;

gap:4px;

white-space:nowrap;
}

.top-signal-count{

color:#62b58a;

font-size:7px;

font-weight:900;
}

.top-pullback-count{

color:#b6a77b;

font-size:7px;

font-weight:900;
}

.ema-signal-wrap{

display:flex;

align-items:center;

justify-content:center;

gap:5px;

min-height:20px;

white-space:nowrap;
}

.ema-signal-long,
.ema-signal-short{

display:inline-flex;

align-items:center;

justify-content:center;

font-size:7px;

font-weight:900;
}

.ema-signal-long{

color:#62b58a;
}

.ema-signal-short{

color:#c97878;
}

.signal-cell{

text-align:center!important;
}

.signal-wrap{

display:flex;

align-items:center;

justify-content:center;

gap:5px;

min-height:20px;

white-space:nowrap;
}

.signal-item,
.pullback-item{

display:inline-flex;

align-items:center;

justify-content:center;

gap:1px;

font-weight:900;
}

.signal-rocket{

font-size:9px;
}

.signal-count{

color:#62b58a;

font-size:7px;

font-weight:900;
}

.pullback-icon{

font-size:8px;
}

.pullback-count{

color:#b6a77b;

font-size:7px;

font-weight:900;
}

@keyframes signalFlashOne{

0%{

    background-color:#15191e;

    box-shadow:
        inset 0 0 0
        rgba(98,181,138,0);
}

30%{

    background-color:#2a3d34;

    box-shadow:
        inset 0 0 11px
        rgba(98,181,138,.32);
}

60%{

    background-color:#19221e;

    box-shadow:
        inset 0 0 3px
        rgba(98,181,138,.12);
}

100%{

    background-color:#15191e;

    box-shadow:
        inset 0 0 0
        rgba(98,181,138,0);
}

}

tr.signal-flash-one td{

animation:
    signalFlashOne
    1.35s
    ease-in-out
    infinite;
}

tr.orderbook-subrow.signal-flash-one td{

animation:
    signalFlashOne
    1.35s
    ease-in-out
    infinite;
}

.orderbook-subrow{

background:#0f1318!important;
}

.orderbook-subrow td{

height:auto!important;

padding:4px 5px!important;
}

.orderbook-wrap{

width:100%;
}

.orderbook-row{

display:grid;

grid-template-columns:
    43px
    43px
    minmax(55px,1fr)
    35px;

align-items:center;

gap:4px;

min-height:13px;
}

.orderbook-label{

font-size:5.5px;

font-weight:900;
}

.ask-label{

color:#a96b6b;
}

.bid-label{

color:#609276;
}

.orderbook-amount{

font-size:5.5px;

font-weight:900;

text-align:right;
}

.ask-amount{

color:#a96b6b;
}

.bid-amount{

color:#609276;
}

.orderbook-bar-box{

height:7px;

background:#252b31;

border-radius:4px;

overflow:hidden;
}

.orderbook-bar{

height:100%;

border-radius:4px;

opacity:.85;
}

.ask-bar{

background:#754747;
}

.bid-bar{

background:#416c58;
}

.orderbook-ratio{

font-size:5.5px;

font-weight:900;

text-align:right;
}

.ask-ratio{

color:#a96b6b;
}

.bid-ratio{

color:#609276;
}

.orderbook-bottom{

display:flex;

align-items:center;

justify-content:flex-end;

gap:7px;

margin-top:2px;

color:#5e6670;

font-size:5px;

font-weight:800;
}

.ob-dominance{

font-size:5.5px;

font-weight:900;
}

.bid-dominance{

color:#609276;
}

.ask-dominance{

color:#a96b6b;
}

.balanced-dominance{

color:#8b939c;
}

.up{

color:#62b58a!important;

font-weight:900;
}

.down{

color:#c97878!important;

font-weight:900;
}

.zero{

color:#68717b!important;
}

.muted{

color:#68717b!important;
}

.empty{

height:30px;

color:#555d67;

font-size:6px;
}

@media(max-width:380px){

body{

    padding:2px;
}

h1{

    font-size:11px;

    line-height:13px;
}

.market-title,
.section-title{

    min-height:18px;

    padding:3px 4px;

    gap:4px;
}

.market-title-main,
.section-title-main{

    font-size:7px;
}

.market-title-sub,
.section-title-sub{

    font-size:4.8px;
}

th{

    height:16px;

    font-size:4.5px;
}

td{

    height:23px;
}

.coin b{

    font-size:6px;
}

.coin small{

    font-size:4px;
}

.vol{

    font-size:5.5px;
}

.filter-timeframe{

    width:11px;

    font-size:4.2px;
}

.roc-item{

    font-size:3.8px;

    line-height:7px;
}

.top-count-wrap{

    gap:2px;
}

.top-signal-count,
.top-pullback-count{

    font-size:5.8px;
}

.ema-signal-wrap{

    gap:3px;
}

.ema-signal-long,
.ema-signal-short{

    font-size:5.8px;
}

.signal-wrap{

    gap:3px;
}

.signal-rocket{

    font-size:8px;
}

.signal-count{

    font-size:6.5px;
}

.pullback-icon{

    font-size:7px;
}

.pullback-count{

    font-size:6.5px;
}

.orderbook-row{

    grid-template-columns:
        37px
        38px
        minmax(42px,1fr)
        31px;

    gap:3px;
}

.orderbook-label,
.orderbook-amount,
.orderbook-ratio{

    font-size:4.8px;
}

.btc-roc-period{

    font-size:4px;
}

.btc-roc-icon{

    font-size:7px;
}

.btc-roc-count{

    font-size:4.8px;
}

.btc-roc-item{

    min-height:23px;
}

}

@media(prefers-reduced-motion:reduce){

tr.signal-flash-one td,
tr.orderbook-subrow.signal-flash-one td{

    animation:none!important;
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
            content="#0b0e12"
        >

        <title>
            ROC5 SIGNAL
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
# STARTUP
# =========================================================

@app.on_event(
    "startup"
)
def startup():

    validate_timeframe()

    log.info(
        "========================================"
    )

    log.info(
        "ROC5 SIGNAL / PULLBACK SYSTEM START"
    )

    log.info(
        f"1H ROC FILTER = "
        f"{USE_1H_ROC_FILTER}"
    )

    log.info(
        f"4H ROC FILTER = "
        f"{USE_4H_ROC_FILTER}"
    )

    log.info(
        f"ACTIVE FILTER = "
        f"{get_filter_setting_text()}"
    )

    log.info(
        "----------------------------------------"
    )

    log.info(
        "ROC5 상향 0선 돌파 = 🚀 COUNT 1"
    )

    log.info(
        "ROC5 하향 0선 돌파 = 📉 눌림 COUNT 1"
    )

    log.info(
        "ROC5 음수 → 상승 신호 종료"
    )

    log.info(
        "활성 ROC 필터 음수 → 상승 신호 종료"
    )

    log.info(
        "신호 COUNT = 1,2,3... 무제한"
    )

    log.info(
        "눌림 COUNT = 1,2,3... 무제한"
    )

    log.info(
        "과거 눌림 상태 = 자동 복원"
    )

    log.info(
        "TOP 리스트 = ROC COUNT 표시"
    )

    log.info(
        "ROC COUNT 표시 = 🚀(N) / 📉(N)"
    )

    log.info(
        "COUNT 1~5 = 반짝임"
    )

    log.info(
        "COUNT 6 이상 = 반짝임 없음"
    )

    log.info(
        f"상승 신호 영역 = "
        f"필터 통과 + 🚀1~5 + 📉1~5 "
        f"+ EMA 상승 COUNT <= {EMA_LONG_MAX_COUNT} "
        f"+ 당일 등락 >= 0%"
    )

    log.info(
        f"반짝임 EMA 필터 = "
        f"상승 EMA COUNT <= {EMA_LONG_MAX_COUNT}"
    )

    log.info(
        "별도 눌림 대시보드 = 삭제"
    )

    log.info(
        "----------------------------------------"
    )

    log.info(
        "EMA 기간 = ROC 필터 Y/N 자동 연동"
    )

    log.info(
        f"1H EMA = "
        f"{get_enabled_filter_text('1H')}"
    )

    log.info(
        f"4H EMA = "
        f"{get_enabled_filter_text('4H')}"
    )

    log.info(
        "EMA 정배열 = 🟢(N) — COUNT 제한 없음"
    )

    log.info(
        "EMA 역배열 = 🔴(N) — COUNT 제한 없음"
    )

    log.info(
        f"상승 신호 영역 EMA COUNT 필터 <= "
        f"{EMA_LONG_MAX_COUNT}"
    )

    log.info(
        f"반짝임 EMA COUNT 필터 <= "
        f"{EMA_LONG_MAX_COUNT}"
    )

    log.info(
        "TOP 리스트 EMA COUNT = 무제한 표시"
    )

    log.info(
        "상승 신호 영역 당일 음수 = 제외"
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
