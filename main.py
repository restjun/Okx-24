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
# 시그널 기준 시간봉
# =========================================================

SIGNAL_TIMEFRAME = 240


# =========================================================
# ROC 기간
# =========================================================

ROC_FILTER_PERIODS = [
    5,
    20,
    50,
    200
]


# =========================================================
# Signal 1
# =========================================================

SIGNAL1_ROC_PERIOD = 50

SIGNAL1_FILTER_COUNT_MIN = 1
SIGNAL1_FILTER_COUNT_MAX = 200

SIGNAL1_DISPLAY_COUNT_MIN = 0
SIGNAL1_DISPLAY_COUNT_MAX = 2

SIGNAL1_FLASH_COUNT_MIN = 0
SIGNAL1_FLASH_COUNT_MAX = 2


# =========================================================
# Signal 2
# =========================================================

SIGNAL2_ROC_PERIOD = 5

SIGNAL2_FILTER_COUNT_MIN = 10
SIGNAL2_FILTER_COUNT_MAX = 30

SIGNAL2_DISPLAY_COUNT_MIN = 0
SIGNAL2_DISPLAY_COUNT_MAX = 2

SIGNAL2_FLASH_COUNT_MIN = 0
SIGNAL2_FLASH_COUNT_MAX = 2


# =========================================================
# 필터에 사용할 ROC
# =========================================================

SIGNAL_FILTER_PERIODS = [
    20,
    50,
    200
]


# =========================================================
# ROC 계산용 최소 데이터
# =========================================================

ROC_COUNT_HISTORY_EXTRA = 100

ROC_HISTORY_REQUIRED = (
    max(ROC_FILTER_PERIODS)
    + max(
        SIGNAL1_DISPLAY_COUNT_MAX,
        SIGNAL1_FLASH_COUNT_MAX,
        SIGNAL2_DISPLAY_COUNT_MAX,
        SIGNAL2_FLASH_COUNT_MAX
    )
    + ROC_COUNT_HISTORY_EXTRA
    + 2
)


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
latest_usdt_krw_internal = 0

okx_ticker_cache = {}


# =========================================================
# Signal 1 상태
# =========================================================

roc_signal1_state = {}


# =========================================================
# Signal 1 종료 캔들
# =========================================================

roc_signal1_failed_candle = {}


# =========================================================
# Signal 2 상태
# =========================================================

roc_signal2_state = {}


# =========================================================
# Signal 2 종료 캔들
# =========================================================

roc_signal2_failed_candle = {}


# =========================================================
# COUNT 표시
# =========================================================

def signal1_display_allowed(count):

    try:
        count = int(count)
    except Exception:
        return False

    return (
        SIGNAL1_DISPLAY_COUNT_MIN
        <= count
        <= SIGNAL1_DISPLAY_COUNT_MAX
    )


def signal2_display_allowed(count):

    try:
        count = int(count)
    except Exception:
        return False

    return (
        SIGNAL2_DISPLAY_COUNT_MIN
        <= count
        <= SIGNAL2_DISPLAY_COUNT_MAX
    )


def signal1_flash_allowed(count):

    try:
        count = int(count)
    except Exception:
        return False

    return (
        SIGNAL1_FLASH_COUNT_MIN
        <= count
        <= SIGNAL1_FLASH_COUNT_MAX
    )


def signal2_flash_allowed(count):

    try:
        count = int(count)
    except Exception:
        return False

    return (
        SIGNAL2_FLASH_COUNT_MIN
        <= count
        <= SIGNAL2_FLASH_COUNT_MAX
    )


def signal1_display_text():

    return (
        f"{SIGNAL1_DISPLAY_COUNT_MIN}~"
        f"{SIGNAL1_DISPLAY_COUNT_MAX}"
    )


def signal2_display_text():

    return (
        f"{SIGNAL2_DISPLAY_COUNT_MIN}~"
        f"{SIGNAL2_DISPLAY_COUNT_MAX}"
    )


def signal1_flash_text():

    return (
        f"{SIGNAL1_FLASH_COUNT_MIN}~"
        f"{SIGNAL1_FLASH_COUNT_MAX}"
    )


def signal2_flash_text():

    return (
        f"{SIGNAL2_FLASH_COUNT_MIN}~"
        f"{SIGNAL2_FLASH_COUNT_MAX}"
    )


# =========================================================
# ROC 설정
# =========================================================

def roc_settings():

    return {

        5: {
            "4H": "Y"
        },

        20: {
            "4H": "Y"
        },

        50: {
            "4H": "Y"
        },

        200: {
            "4H": "Y"
        }

    }


def get_enabled_periods(timeframe):

    settings = roc_settings()

    if timeframe != "4H":
        return []

    return [
        p
        for p in ROC_FILTER_PERIODS
        if settings[p]["4H"] == "Y"
    ]


def get_all_periods():

    return ROC_FILTER_PERIODS.copy()


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


# =========================================================
# 현재 4H 캔들 시작시간
# =========================================================

def get_current_candle_start(minutes):

    minutes = int(minutes)

    now = datetime.now(KST)

    if minutes == 240:

        anchor = now.replace(
            hour=1,
            minute=0,
            second=0,
            microsecond=0
        )

        if now < anchor:

            anchor = (
                anchor
                - timedelta(days=1)
            )

        elapsed = (
            now - anchor
        ).total_seconds()

        blocks = int(
            elapsed
            //
            (
                240 * 60
            )
        )

        current = (
            anchor
            + timedelta(
                minutes=blocks * 240
            )
        )

        return current.replace(
            tzinfo=None
        )

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


# =========================================================
# 날짜 정규화
# =========================================================

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


# =========================================================
# 캔들 거리
# =========================================================

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
                //
                (
                    int(timeframe)
                    * 60
                )
            ),
            0
        )

    except Exception:

        return 0


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


# =========================================================
# ROC 양수 연속 COUNT
# =========================================================

def roc_positive_count(
    df,
    period
):

    if (
        df is None
        or df.empty
    ):

        return 0

    series = roc(
        df,
        period
    )

    if (
        series is None
        or series.empty
    ):

        return 0

    valid = series.dropna()

    if valid.empty:
        return 0

    try:

        current = float(
            valid.iloc[-1]
        )

    except Exception:

        return 0

    if current < 0:
        return 0

    count = 0

    for value in reversed(
        valid.tolist()
    ):

        try:
            value = float(value)
        except Exception:
            break

        if value >= 0:
            count += 1
        else:
            break

    return count


# =========================================================
# ROC 음수 연속 COUNT
# =========================================================

def roc_negative_count(
    df,
    period
):

    if (
        df is None
        or df.empty
    ):

        return 0

    series = roc(
        df,
        period
    )

    if (
        series is None
        or series.empty
    ):

        return 0

    valid = series.dropna()

    if valid.empty:
        return 0

    try:

        current = float(
            valid.iloc[-1]
        )

    except Exception:

        return 0

    if current >= 0:
        return 0

    count = 0

    for value in reversed(
        valid.tolist()
    ):

        try:
            value = float(value)
        except Exception:
            break

        if value < 0:
            count += 1
        else:
            break

    return count


# =========================================================
# 시간봉 검증
# =========================================================

def validate_timeframe():

    if SIGNAL_TIMEFRAME != 240:

        raise ValueError(
            "SIGNAL_TIMEFRAME은 "
            "240만 사용할 수 있습니다."
        )

    if SIGNAL1_ROC_PERIOD not in ROC_FILTER_PERIODS:

        raise ValueError(
            "SIGNAL1_ROC_PERIOD 설정 오류"
        )

    if SIGNAL2_ROC_PERIOD not in ROC_FILTER_PERIODS:

        raise ValueError(
            "SIGNAL2_ROC_PERIOD 설정 오류"
        )

    if (
        SIGNAL1_FILTER_COUNT_MIN < 0
        or SIGNAL1_FILTER_COUNT_MAX
        < SIGNAL1_FILTER_COUNT_MIN
    ):

        raise ValueError(
            "Signal 1 필터 COUNT 설정 오류"
        )

    if (
        SIGNAL2_FILTER_COUNT_MIN < 0
        or SIGNAL2_FILTER_COUNT_MAX
        < SIGNAL2_FILTER_COUNT_MIN
    ):

        raise ValueError(
            "Signal 2 필터 COUNT 설정 오류"
        )

    if (
        SIGNAL1_DISPLAY_COUNT_MIN < 0
        or SIGNAL1_DISPLAY_COUNT_MAX
        < SIGNAL1_DISPLAY_COUNT_MIN
    ):

        raise ValueError(
            "Signal 1 표시 COUNT 설정 오류"
        )

    if (
        SIGNAL2_DISPLAY_COUNT_MIN < 0
        or SIGNAL2_DISPLAY_COUNT_MAX
        < SIGNAL2_DISPLAY_COUNT_MIN
    ):

        raise ValueError(
            "Signal 2 표시 COUNT 설정 오류"
        )


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
# Upbit 마켓
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
# Upbit native 캔들
# =========================================================

def get_upbit_candle(
    market,
    unit,
    count=200,
    to=None,
    include_current=False
):

    unit = int(unit)

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

    endpoint = (
        "https://api.upbit.com/"
        f"v1/candles/minutes/{unit}"
    )

    response = retry(
        requests.get,
        endpoint,
        params=params,
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

        df = pd.DataFrame(
            data
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

        if df.empty:
            return None

        df = (
            df
            .sort_values("datetime")
            .drop_duplicates("datetime")
            .reset_index(drop=True)
        )

        if not include_current:

            current_start = (
                get_current_candle_start(
                    unit
                )
            )

            df = df[
                df["datetime"]
                < current_start
            ]

        if df.empty:
            return None

        return df

    except Exception as e:

        log.error(
            f"업비트 "
            f"{format_timeframe(unit)} 오류 "
            f"{market}: {e}"
        )

        return None


# =========================================================
# 과거 데이터
# =========================================================

def history_upbit(
    market,
    unit,
    required=200
):

    unit = int(unit)
    required = int(required)

    all_df = None
    to = None

    for chunk_index in range(
        MAX_HISTORY_CHUNKS
    ):

        df = get_upbit_candle(
            market=market,
            unit=unit,
            count=HISTORY_CHUNK,
            to=to,
            include_current=False
        )

        if (
            df is None
            or df.empty
        ):
            break

        if all_df is None:

            all_df = df.copy()

        else:

            all_df = pd.concat(
                [
                    df,
                    all_df
                ],
                ignore_index=True
            )

        all_df = (
            all_df
            .drop_duplicates("datetime")
            .sort_values("datetime")
            .reset_index(drop=True)
        )

        if len(all_df) >= required:

            return (
                all_df
                .iloc[-required:]
                .reset_index(drop=True)
            )

        oldest = (
            all_df["datetime"].iloc[0]
        )

        to = oldest.strftime(
            "%Y-%m-%dT%H:%M:%S"
        )

    if all_df is None:
        return None

    return all_df


# =========================================================
# 현재 진행 중 ROC 데이터
# =========================================================

def get_upbit_current_roc_data(
    market,
    current_price,
    timeframe
):

    timeframe = int(timeframe)

    df = get_upbit_candle(
        market=market,
        unit=timeframe,
        count=HISTORY_CHUNK,
        include_current=True
    )

    if (
        df is None
        or df.empty
    ):
        return None

    df = (
        df
        .sort_values("datetime")
        .drop_duplicates("datetime")
        .reset_index(drop=True)
    )

    required = ROC_HISTORY_REQUIRED

    if len(df) < required:

        oldest = (
            df["datetime"].iloc[0]
        )

        to = oldest.strftime(
            "%Y-%m-%dT%H:%M:%S"
        )

        df_old = get_upbit_candle(
            market=market,
            unit=timeframe,
            count=HISTORY_CHUNK,
            to=to,
            include_current=False
        )

        if (
            df_old is not None
            and not df_old.empty
        ):

            df = pd.concat(
                [
                    df_old,
                    df
                ],
                ignore_index=True
            )

            df = (
                df
                .drop_duplicates("datetime")
                .sort_values("datetime")
                .reset_index(drop=True)
            )

    try:

        current_price = float(
            current_price
        )

        current_start = (
            get_current_candle_start(
                timeframe
            )
        )

        mask = (
            df["datetime"]
            == current_start
        )

        if mask.any():

            df.loc[
                mask,
                "c"
            ] = current_price

        else:

            log.warning(
                f"[CURRENT] "
                f"{market} | "
                f"4H 진행봉 없음 | "
                f"기준={current_start}"
            )

    except Exception as e:

        log.error(
            f"현재 ROC 데이터 오류 "
            f"{market}: {e}"
        )

    df = (
        df
        .sort_values("datetime")
        .drop_duplicates("datetime")
        .reset_index(drop=True)
    )

    if len(df) > required:

        df = (
            df
            .iloc[-required:]
            .reset_index(drop=True)
        )

    return df


# =========================================================
# ROC 분석
# =========================================================

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

        "positive_counts":
            {},

        "negative_counts":
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

    positive_counts = {}
    negative_counts = {}

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

        current_value = (
            series.iloc[-1]
        )

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

        values[period] = current_value

        previous_values[
            period
        ] = previous_value

        zero_crosses[
            period
        ] = (
            previous_value is not None
            and previous_value < 0
            and current_value >= 0
        )

        positive_counts[
            period
        ] = roc_positive_count(
            df,
            period
        )

        negative_counts[
            period
        ] = roc_negative_count(
            df,
            period
        )

        if current_value >= 0:
            positive_count += 1

    passed = (
        len(values)
        == len(periods)
        and
        positive_count
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

        "positive_counts":
            positive_counts,

        "negative_counts":
            negative_counts,

        "positive_count":
            positive_count,

        "total_count":
            len(periods)

    })

    return result


# =========================================================
# 0선 상향 돌파
# =========================================================

def roc_signal_zero_cross(
    current,
    previous
):

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
# 최근 시그널 이벤트
# =========================================================

def find_latest_signal_event(
    df_signal,
    signal_period
):

    if (
        df_signal is None
        or df_signal.empty
    ):

        return None, None

    try:

        current_start = (
            get_current_candle_start(
                SIGNAL_TIMEFRAME
            )
        )

        temp = df_signal.copy()

        temp["datetime"] = pd.to_datetime(
            temp["datetime"],
            errors="coerce"
        )

        temp = (
            temp
            .dropna(
                subset=["datetime"]
            )
            .sort_values("datetime")
            .reset_index(drop=True)
        )

        temp = temp[
            temp["datetime"]
            < current_start
        ]

        if len(temp) < 2:
            return None, None

        series = roc(
            temp,
            signal_period
        )

        if (
            series is None
            or series.empty
        ):

            return None, None

        for i in range(
            len(temp) - 1,
            0,
            -1
        ):

            current_value = (
                series.iloc[i]
            )

            previous_value = (
                series.iloc[i - 1]
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
                previous_value < 0
                and current_value >= 0
            ):

                return (
                    "signal",
                    normalize_datetime(
                        temp[
                            "datetime"
                        ].iloc[i]
                    )
                )

        return None, None

    except Exception as e:

        log.warning(
            f"ROC{signal_period} "
            f"최근 이벤트 검색 오류: {e}"
        )

        return None, None


# =========================================================
# Signal 상태 업데이트
# =========================================================

def update_signal(
    market,
    signal_number,
    current_value,
    previous_value,
    progress_candle_time,
    historical_start_candle=None
):

    market_key = str(
        market
    )

    if signal_number == 1:

        state_dict = roc_signal1_state

        failed_dict = (
            roc_signal1_failed_candle
        )

        signal_period = (
            SIGNAL1_ROC_PERIOD
        )

    else:

        state_dict = roc_signal2_state

        failed_dict = (
            roc_signal2_failed_candle
        )

        signal_period = (
            SIGNAL2_ROC_PERIOD
        )

    progress_candle_time = (
        normalize_datetime(
            progress_candle_time
        )
    )

    try:

        current_value = float(
            current_value
        )

    except Exception:

        current_value = None

    try:

        previous_value = float(
            previous_value
        )

    except Exception:

        previous_value = None

    signal_cross = (
        roc_signal_zero_cross(
            current_value,
            previous_value
        )
    )

    signal_state = (
        state_dict.get(
            market_key
        )
    )

    # =====================================================
    # 신규 돌파
    # =====================================================

    if signal_cross:

        if progress_candle_time is not None:

            state_dict[
                market_key
            ] = {

                "active":
                    True,

                "cross_candle":
                    progress_candle_time,

                "count":
                    0,

                "last_candle":
                    progress_candle_time

            }

            signal_state = (
                state_dict[
                    market_key
                ]
            )

            failed_dict.pop(
                market_key,
                None
            )

            log.info(
                f"[Signal {signal_number} START] "
                f"{market_key} "
                f"ROC{signal_period} "
                f"🚀(0)"
            )

    # =====================================================
    # 기존 상태가 없으면 과거 이벤트 복원
    # =====================================================

    else:

        if signal_state is None:

            start_candle = None

            if historical_start_candle is not None:

                start_candle = (
                    normalize_datetime(
                        historical_start_candle
                    )
                )

            if (
                start_candle is not None
                and current_value is not None
                and current_value >= 0
                and progress_candle_time is not None
            ):

                failed_candle = (
                    failed_dict.get(
                        market_key
                    )
                )

                if (
                    failed_candle
                    != progress_candle_time
                ):

                    distance = candle_distance(
                        start_candle,
                        progress_candle_time,
                        SIGNAL_TIMEFRAME
                    )

                    state_dict[
                        market_key
                    ] = {

                        "active":
                            True,

                        "cross_candle":
                            start_candle,

                        "count":
                            distance,

                        "last_candle":
                            progress_candle_time

                    }

                    signal_state = (
                        state_dict[
                            market_key
                        ]
                    )

                    log.info(
                        f"[Signal {signal_number} RESTORE] "
                        f"{market_key} "
                        f"🚀({distance})"
                    )

    # =====================================================
    # 상태 확인
    # =====================================================

    signal_state = (
        state_dict.get(
            market_key
        )
    )

    if signal_state is not None:

        if (
            current_value is not None
            and current_value < 0
        ):

            old_count = int(
                signal_state.get(
                    "count",
                    0
                )
            )

            if progress_candle_time is not None:

                failed_dict[
                    market_key
                ] = progress_candle_time

            log.info(
                f"[Signal {signal_number} END] "
                f"{market_key} | "
                f"COUNT={old_count} | "
                f"ROC 음수"
            )

            state_dict.pop(
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
                    SIGNAL_TIMEFRAME
                )

                signal_state[
                    "count"
                ] = distance

                signal_state[
                    "last_candle"
                ] = progress_candle_time

    signal_state = (
        state_dict.get(
            market_key
        )
    )

    signal_active = bool(
        signal_state
        and signal_state.get(
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

    return {

        "signal_active":
            signal_active,

        "signal_count":
            signal_count,

        "signal_roc_cross":
            signal_cross

    }


# =========================================================
# 일봉 변동률
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

        return (
            current
            - previous
        ) / previous * 100

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
# 거래대금
# =========================================================

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

    df_signal = history_upbit(
        market,
        SIGNAL_TIMEFRAME,
        required=ROC_HISTORY_REQUIRED
    )

    if (
        df_signal is None
        or df_signal.empty
    ):

        return None

    df_current = (
        get_upbit_current_roc_data(
            market,
            current_price,
            SIGNAL_TIMEFRAME
        )
    )

    if (
        df_current is None
        or df_current.empty
    ):

        return None

    # =====================================================
    # 모든 ROC
    # =====================================================

    r_signal = roc_filter_analysis(
        df_current,
        ROC_FILTER_PERIODS
    )

    roc_values = r_signal.get(
        "roc_values",
        {}
    )

    previous_values = r_signal.get(
        "previous_values",
        {}
    )

    positive_counts = r_signal.get(
        "positive_counts",
        {}
    )

    # =====================================================
    # Signal 1 ROC50
    # =====================================================

    signal1_roc_current = (
        roc_values.get(
            SIGNAL1_ROC_PERIOD
        )
    )

    signal1_roc_previous = (
        previous_values.get(
            SIGNAL1_ROC_PERIOD
        )
    )

    # =====================================================
    # Signal 2 ROC5
    # =====================================================

    signal2_roc_current = (
        roc_values.get(
            SIGNAL2_ROC_PERIOD
        )
    )

    signal2_roc_previous = (
        previous_values.get(
            SIGNAL2_ROC_PERIOD
        )
    )

    # =====================================================
    # 과거 Signal 1 이벤트
    # =====================================================

    historical_start_candle1 = None

    if market not in roc_signal1_state:

        event_type1, event_candle1 = (
            find_latest_signal_event(
                df_signal,
                SIGNAL1_ROC_PERIOD
            )
        )

        if event_type1 == "signal":

            historical_start_candle1 = (
                event_candle1
            )

    # =====================================================
    # 과거 Signal 2 이벤트
    # =====================================================

    historical_start_candle2 = None

    if market not in roc_signal2_state:

        event_type2, event_candle2 = (
            find_latest_signal_event(
                df_signal,
                SIGNAL2_ROC_PERIOD
            )
        )

        if event_type2 == "signal":

            historical_start_candle2 = (
                event_candle2
            )

    # =====================================================
    # 현재 진행 4H 캔들
    # =====================================================

    progress_candle_time = (
        get_current_candle_start(
            SIGNAL_TIMEFRAME
        )
    )

    # =====================================================
    # Signal 1
    # =====================================================

    state1 = update_signal(
        market=market,
        signal_number=1,
        current_value=signal1_roc_current,
        previous_value=signal1_roc_previous,
        progress_candle_time=(
            progress_candle_time
        ),
        historical_start_candle=(
            historical_start_candle1
        )
    )

    # =====================================================
    # Signal 2
    # =====================================================

    state2 = update_signal(
        market=market,
        signal_number=2,
        current_value=signal2_roc_current,
        previous_value=signal2_roc_previous,
        progress_candle_time=(
            progress_candle_time
        ),
        historical_start_candle=(
            historical_start_candle2
        )
    )

    # =====================================================
    # 자체 Signal COUNT
    # =====================================================

    signal1_count = int(
        state1[
            "signal_count"
        ]
    )

    signal2_count = int(
        state2[
            "signal_count"
        ]
    )

    # =====================================================
    # 필터 COUNT
    # =====================================================

    roc20_count = int(
        positive_counts.get(
            20,
            0
        )
    )

    roc50_count = int(
        positive_counts.get(
            50,
            0
        )
    )

    roc200_count = int(
        positive_counts.get(
            200,
            0
        )
    )

    # =====================================================
    # Signal 1 필터
    # =====================================================

    signal1_filter_pass = (

        SIGNAL1_FILTER_COUNT_MIN
        <= roc20_count
        <= SIGNAL1_FILTER_COUNT_MAX

        and

        SIGNAL1_FILTER_COUNT_MIN
        <= roc50_count
        <= SIGNAL1_FILTER_COUNT_MAX

        and

        SIGNAL1_FILTER_COUNT_MIN
        <= roc200_count
        <= SIGNAL1_FILTER_COUNT_MAX
    )

    # =====================================================
    # Signal 2 필터
    # =====================================================

    signal2_filter_pass = (

        SIGNAL2_FILTER_COUNT_MIN
        <= roc20_count
        <= SIGNAL2_FILTER_COUNT_MAX

        and

        SIGNAL2_FILTER_COUNT_MIN
        <= roc50_count
        <= SIGNAL2_FILTER_COUNT_MAX

        and

        SIGNAL2_FILTER_COUNT_MIN
        <= roc200_count
        <= SIGNAL2_FILTER_COUNT_MAX
    )

    # =====================================================
    # 일봉
    # =====================================================

    change_value = (
        daily_change_upbit(
            market
        )
    )

    daily_pass = (
        change_value is not None
        and change_value >= 0
    )

    # =====================================================
    # Signal 1 최종
    # =====================================================

    signal1_qualified = (

        state1[
            "signal_active"
        ]

        and

        signal1_filter_pass

        and

        daily_pass
    )

    # =====================================================
    # Signal 2 최종
    # =====================================================

    signal2_qualified = (

        state2[
            "signal_active"
        ]

        and

        signal2_filter_pass

        and

        daily_pass
    )

    return {

        "roc":
            r_signal,

        "changes":
            change_value,

        "daily_pass":
            daily_pass,

        "signal1_active":
            state1[
                "signal_active"
            ],

        "signal1_count":
            signal1_count,

        "signal1_roc":
            signal1_roc_current,

        "signal1_roc_previous":
            signal1_roc_previous,

        "signal1_roc_cross":
            state1[
                "signal_roc_cross"
            ],

        "signal1_filter_pass":
            signal1_filter_pass,

        "signal1_count_pass":
            signal1_filter_pass,

        "signal1_roc20_count":
            roc20_count,

        "signal1_roc50_count":
            roc50_count,

        "signal1_roc200_count":
            roc200_count,

        "signal1_qualified":
            signal1_qualified,

        "signal2_active":
            state2[
                "signal_active"
            ],

        "signal2_count":
            signal2_count,

        "signal2_roc":
            signal2_roc_current,

        "signal2_roc_previous":
            signal2_roc_previous,

        "signal2_roc_cross":
            state2[
                "signal_roc_cross"
            ],

        "signal2_filter_pass":
            signal2_filter_pass,

        "signal2_count_pass":
            signal2_filter_pass,

        "signal2_roc20_count":
            roc20_count,

        "signal2_roc50_count":
            roc50_count,

        "signal2_roc200_count":
            roc200_count,

        "signal2_roc_filter_pass":
            signal2_filter_pass,

        "signal2_qualified":
            signal2_qualified,

        "df_signal":
            df_signal
    }


# =========================================================
# ROW
# =========================================================

def make_row(
    rank,
    name,
    volume,
    analysis,
    current_price=None
):

    a = analysis or {}

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

        "roc":
            a.get(
                "roc",
                {}
            ),

        "signal1_active":
            bool(
                a.get(
                    "signal1_active",
                    False
                )
            ),

        "signal1_count":
            int(
                a.get(
                    "signal1_count",
                    0
                )
            ),

        "signal1_roc":
            a.get(
                "signal1_roc"
            ),

        "signal1_roc_cross":
            bool(
                a.get(
                    "signal1_roc_cross",
                    False
                )
            ),

        "signal1_filter_pass":
            bool(
                a.get(
                    "signal1_filter_pass",
                    False
                )
            ),

        "signal1_count_pass":
            bool(
                a.get(
                    "signal1_count_pass",
                    False
                )
            ),

        "signal1_qualified":
            bool(
                a.get(
                    "signal1_qualified",
                    False
                )
            ),

        "signal2_active":
            bool(
                a.get(
                    "signal2_active",
                    False
                )
            ),

        "signal2_count":
            int(
                a.get(
                    "signal2_count",
                    0
                )
            ),

        "signal2_roc":
            a.get(
                "signal2_roc"
            ),

        "signal2_roc_cross":
            bool(
                a.get(
                    "signal2_roc_cross",
                    False
                )
            ),

        "signal2_filter_pass":
            bool(
                a.get(
                    "signal2_filter_pass",
                    False
                )
            ),

        "signal2_count_pass":
            bool(
                a.get(
                    "signal2_count_pass",
                    False
                )
            ),

        "signal2_qualified":
            bool(
                a.get(
                    "signal2_qualified",
                    False
                )
            ),

        "analysis":
            analysis
    }


# =========================================================
# Upbit TOP 업데이트
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
# USDT / OKX
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
# Signal 표시
# =========================================================

def signal_item_html(
    signal_number,
    active,
    count,
    qualified
):

    if not qualified:
        return "-"

    return f"""
    <span class="signal-item signal-{signal_number}">
        <span class="signal-rocket">
            🚀
        </span>
    </span>
    """


def signal_html(
    row
):

    if not row:
        return "-"

    signal1 = signal_item_html(
        1,
        row.get(
            "signal1_active",
            False
        ),
        row.get(
            "signal1_count",
            0
        ),
        row.get(
            "signal1_qualified",
            False
        )
    )

    signal2 = signal_item_html(
        2,
        row.get(
            "signal2_active",
            False
        ),
        row.get(
            "signal2_count",
            0
        ),
        row.get(
            "signal2_qualified",
            False
        )
    )

    return f"""
    <div class="signal-wrap">

        <div class="signal-row">
            {signal1}
        </div>

        <div class="signal-row">
            {signal2}
        </div>

    </div>
    """


def top_signal1_html(
    row
):

    if not row:
        return "-"

    return signal_item_html(
        1,
        row.get(
            "signal1_active",
            False
        ),
        row.get(
            "signal1_count",
            0
        ),
        row.get(
            "signal1_qualified",
            False
        )
    )


def top_signal2_html(
    row
):

    if not row:
        return "-"

    return signal_item_html(
        2,
        row.get(
            "signal2_active",
            False
        ),
        row.get(
            "signal2_count",
            0
        ),
        row.get(
            "signal2_qualified",
            False
        )
    )


# =========================================================
# ROC HTML
# =========================================================

def roc_filter_html(
    r,
    timeframe
):

    if not r:
        r = {}

    values = r.get(
        "roc_values",
        {}
    )

    positive_counts = r.get(
        "positive_counts",
        {}
    )

    negative_counts = r.get(
        "negative_counts",
        {}
    )

    items = []

    for period in ROC_FILTER_PERIODS:

        value = values.get(
            period
        )

        if value is None:

            icon = "⚪"
            count_text = "0"

        else:

            try:

                value = float(
                    value
                )

                if value > 0:

                    icon = "🟢"

                    count_text = str(
                        int(
                            positive_counts.get(
                                period,
                                0
                            )
                        )
                    )

                elif value < 0:

                    icon = "🔴"

                    count_text = str(
                        int(
                            negative_counts.get(
                                period,
                                0
                            )
                        )
                    )

                else:

                    icon = "⚪"
                    count_text = "0"

            except Exception:

                icon = "⚪"
                count_text = "0"

        items.append(
            f"""
            <div class="roc-card-item">

                <div class="roc-card-period">
                    ROC{period}
                </div>

                <div class="roc-card-value">

                    <span class="roc-dot">
                        {icon}
                    </span>

                    <span class="roc-count">
                        ({count_text})
                    </span>

                </div>

            </div>
            """
        )

    return f"""
    <div class="roc-detail">

        <div class="roc-label">
            {timeframe} ROC
        </div>

        <div class="roc-grid">

            {"".join(items)}

        </div>

        <div class="roc-badge">
            4시간 기준
        </div>

    </div>
    """


def filter_html(
    r
):

    return roc_filter_html(
        r,
        "4H"
    )


# =========================================================
# ROW HTML
# =========================================================

def rows_html(
    data
):

    out = []

    for index, x in enumerate(
        data
    ):

        cls_list = []

        signal1_qualified = x.get(
            "signal1_qualified",
            False
        )

        signal2_qualified = x.get(
            "signal2_qualified",
            False
        )

        signal1_count = int(
            x.get(
                "signal1_count",
                0
            )
        )

        signal2_count = int(
            x.get(
                "signal2_count",
                0
            )
        )

        # =================================================
        # Signal 1 반짝임
        # =================================================

        if (
            signal1_qualified
            and
            signal1_flash_allowed(
                signal1_count
            )
        ):

            cls_list.append(
                "signal-flash-one"
            )

        # =================================================
        # Signal 2 반짝임
        # =================================================

        if (
            signal2_qualified
            and
            signal2_flash_allowed(
                signal2_count
            )
        ):

            cls_list.append(
                "signal-flash-two"
            )

        cls = " ".join(
            cls_list
        )

        roc_content = filter_html(
            x.get(
                "roc",
                {}
            )
        )

        signal1_content = (
            top_signal1_html(
                x
            )
        )

        signal2_content = (
            top_signal2_html(
                x
            )
        )

        price = format_market_price(
            x.get(
                "current_price"
            )
        )

        change = x.get(
            "change",
            "-"
        )

        coin_name = x.get(
            "name",
            "-"
        )

        volume = x.get(
            "volume",
            "-"
        )

        out.append(
            f"""
            <div class="coin-card {cls}">

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

                        <span class="volume-value">
                            {volume}
                        </span>

                    </div>

                    <div class="price-cell">

                        <span class="price-value">
                            {price}
                        </span>

                    </div>

                    <div class="change-cell">
                        {change}
                    </div>

                    <div class="signal-cell">
                        {signal1_content}
                    </div>

                    <div class="signal-cell">
                        {signal2_content}
                    </div>

                </div>

                <div class="coin-roc-row">

                    {roc_content}

                </div>

            </div>
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
        <div class="empty-card">
            현재 후보 없음
        </div>
        """

    return f"""
    <div class="card-list">

        {rows}

    </div>
    """


# =========================================================
# Signal 1 / Signal 2 집중 영역
# =========================================================

def focus_section(
    data
):

    # =====================================================
    # Signal 1
    # =====================================================

    signal1_rows = [

        x

        for x in data

        if (

            x.get(
                "signal1_qualified",
                False
            )

            and

            signal1_display_allowed(
                int(
                    x.get(
                        "signal1_count",
                        0
                    )
                )
            )

        )

    ]

    # =====================================================
    # Signal 2
    # =====================================================

    signal2_rows = [

        x

        for x in data

        if (

            x.get(
                "signal2_qualified",
                False
            )

            and

            signal2_display_allowed(
                int(
                    x.get(
                        "signal2_count",
                        0
                    )
                )
            )

        )

    ]

    signal1_table = table_html(
        signal1_rows
    )

    signal2_table = table_html(
        signal2_rows
    )

    return f"""
    <div class="unified-section">

        <div class="section-title-card">

            <div class="section-number">
                1
            </div>

            <div class="section-heading">

                <div class="section-heading-main">
                    Signal 1
                </div>

                <div class="section-heading-sub">

                    ROC{SIGNAL1_ROC_PERIOD}
                    4H

                    ·

                    0선 상향 돌파

                    ·

                    ROC20/50/200 COUNT
                    {SIGNAL1_FILTER_COUNT_MIN}~
                    {SIGNAL1_FILTER_COUNT_MAX}

                    ·

                    당일 변동 0% 이상

                </div>

            </div>

            <div class="section-time">
                {kst()} KST
            </div>

        </div>

        {signal1_table}

    </div>


    <div class="unified-section">

        <div class="section-title-card">

            <div class="section-number signal-two-number">
                2
            </div>

            <div class="section-heading">

                <div class="section-heading-main">
                    Signal 2
                </div>

                <div class="section-heading-sub">

                    ROC{SIGNAL2_ROC_PERIOD}
                    4H

                    ·

                    0선 상향 돌파

                    ·

                    ROC20/50/200 COUNT
                    {SIGNAL2_FILTER_COUNT_MIN}~
                    {SIGNAL2_FILTER_COUNT_MAX}

                    ·

                    당일 변동 0% 이상

                </div>

            </div>

            <div class="section-time">
                {kst()} KST
            </div>

        </div>

        {signal2_table}

    </div>
    """


# =========================================================
# TOP
# =========================================================

def section(
    data,
    update_time
):

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

                    1 = ROC50 4H

                    ·

                    2 = ROC5 4H

                    ·

                    ROC20/50/200 COUNT
                    10~30

                    ·

                    4H native

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
# BTC ROC 상태
# =========================================================

def btc_roc_status_html(
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

    df_signal = analysis.get(
        "df_signal"
    )

    if (
        df_signal is None
        or df_signal.empty
    ):

        return ""

    items = []

    for period in ROC_FILTER_PERIODS:

        series = roc(
            df_signal,
            period
        )

        if (
            series is None
            or series.empty
        ):

            icon = "⚪"
            count_text = "0"

        else:

            valid = series.dropna()

            if valid.empty:

                icon = "⚪"
                count_text = "0"

            else:

                current = float(
                    valid.iloc[-1]
                )

                if current > 0:

                    icon = "🟢"

                    count_text = str(
                        roc_positive_count(
                            df_signal,
                            period
                        )
                    )

                elif current < 0:

                    icon = "🔴"

                    count_text = str(
                        roc_negative_count(
                            df_signal,
                            period
                        )
                    )

                else:

                    icon = "⚪"
                    count_text = "0"

        items.append(
            f"""
            <div class="roc-card-item">

                <div class="roc-card-period">
                    ROC{period}
                </div>

                <div class="roc-card-value">

                    <span class="roc-dot">
                        {icon}
                    </span>

                    <span class="roc-count">
                        ({count_text})
                    </span>

                </div>

            </div>
            """
        )

    return f"""
    <div class="btc-roc-detail">

        <div class="btc-roc-label">
            4H ROC
        </div>

        <div class="roc-grid btc-roc-grid">

            {"".join(items)}

        </div>

        <div class="roc-badge">
            시그널 기준
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
            btc_roc_status_html(
                btc
            )
        )

    else:

        price = "-"
        change = "-"
        signal = "-"
        roc_status = ""

    return f"""
    <div class="market-card">

        <div class="market-card-header">

            <div class="market-title-block">

                <div class="market-title-main">
                    ₿ BTC 시장 시황
                </div>

                <div class="market-title-sub">

                    1 = ROC50 돌파

                    ·

                    2 = ROC5 돌파

                    ·

                    ROC20/50/200 COUNT 10~30

                    ·

                    자체 COUNT 0~2

                    ·

                    기준 4H native

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
                {change}
            </div>

            <div class="btc-signal-box">
                {signal}
            </div>

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


/* =====================================================
   전체 섹션
   ===================================================== */

.unified-section{
    width:100%;
    margin:10px 0 12px;
}


/* =====================================================
   공통 섹션 헤더
   ===================================================== */

.section-title-card{
    display:flex;
    align-items:center;

    width:100%;
    min-height:48px;

    padding:7px 10px;

    background:#10151b;

    border:2px solid #252e38;

    border-radius:12px;

    box-shadow:
        inset 0 0 18px
        rgba(255,255,255,.018);

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

.signal-two-number{
    background:#17202a;

    border-color:#365266;

    color:#9eb9cd;
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


/* =====================================================
   BTC 시장 카드
   ===================================================== */

.market-card{
    width:100%;

    margin:3px 0 12px;

    background:#0f141a;

    border:2px solid #252e38;

    border-radius:13px;

    overflow:hidden;

    box-shadow:
        inset 0 0 20px
        rgba(255,255,255,.018);
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


/* =====================================================
   BTC 메인 행
   ===================================================== */

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

    border-bottom:1px solid #29323c;
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
    color:#79cda1;

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

    border-left:1px solid #29323c;
}


/* =====================================================
   BTC ROC / 공통 ROC
   ===================================================== */

.btc-roc-detail{
    display:grid;

    grid-template-columns:
        1.1fr
        5fr
        1.1fr;

    align-items:center;

    min-height:69px;

    padding:7px 9px;

    background:#0d1218;
}

.btc-roc-label{
    color:#dce2e7;

    font-size:10px;
    font-weight:900;

    text-align:center;

    white-space:nowrap;
}

.roc-detail{
    display:grid;

    grid-template-columns:
        1.05fr
        5fr
        1.05fr;

    align-items:center;

    min-height:68px;

    padding:6px 8px;

    background:#0d1218;
}

.roc-label{
    color:#dce2e7;

    font-size:10px;
    font-weight:900;

    text-align:center;

    white-space:nowrap;
}

.roc-grid{
    display:grid;

    grid-template-columns:
        repeat(4,minmax(0,1fr));

    gap:6px;

    width:100%;
}

.roc-card-item{
    min-height:52px;

    display:flex;
    flex-direction:column;

    align-items:center;
    justify-content:center;

    background:#11171e;

    border:2px solid #27313c;

    border-radius:9px;

    box-shadow:
        inset 0 0 10px
        rgba(255,255,255,.018);
}

.roc-card-period{
    color:#9aa5b0;

    font-size:8px;
    line-height:11px;

    font-weight:900;

    white-space:nowrap;
}

.roc-card-value{
    display:flex;
    align-items:center;
    justify-content:center;

    gap:5px;

    margin-top:2px;
}

.roc-dot{
    font-size:18px;
    line-height:18px;

    filter:
        drop-shadow(
            0 0 4px
            rgba(120,220,130,.18)
        );
}

.roc-count{
    color:#eef2f5;

    font-size:10px;
    line-height:14px;

    font-weight:900;

    white-space:nowrap;
}

.roc-badge{
    justify-self:end;

    padding:5px 7px;

    border-radius:8px;

    background:#183126;

    border:1px solid #285840;

    color:#78c99d;

    font-size:7px;
    line-height:10px;

    font-weight:900;

    white-space:nowrap;
}


/* =====================================================
   카드 리스트
   ===================================================== */

.card-list{
    width:100%;

    display:flex;
    flex-direction:column;

    gap:9px;

    margin-top:8px;
}


/* =====================================================
   코인 카드
   ===================================================== */

.coin-card{
    width:100%;

    background:#0f141a;

    border:2px solid #252e38;

    border-radius:12px;

    overflow:hidden;

    box-shadow:
        inset 0 0 18px
        rgba(255,255,255,.015);
}


/* =====================================================
   코인 메인 행
   ===================================================== */

.coin-main-row{
    display:grid;

    grid-template-columns:
        6%
        18%
        15%
        21%
        14%
        13%
        13%;

    align-items:center;

    min-height:56px;

    background:#11161c;
}

.coin-main-row > div{
    min-width:0;

    height:56px;

    display:flex;
    align-items:center;
    justify-content:center;

    overflow:hidden;
}

.rank-cell{
    justify-content:flex-start!important;

    padding-left:12px;

    color:#e2e7eb;

    font-size:11px;
    font-weight:900;

    white-space:nowrap;
}

.coin-cell{
    text-align:center;
}

.coin-name{
    display:block;

    width:100%;

    padding:0 3px;

    color:#eef2f5;

    font-size:11px;
    line-height:14px;

    font-weight:900;

    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;

    text-align:center;
}

.volume-cell{
    text-align:center;
}

.volume-value{
    display:block;

    width:100%;

    color:#f1f4f6;

    font-size:10px;
    line-height:13px;

    font-weight:900;

    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;

    text-align:center;
}

.price-cell{
    text-align:center;
}

.price-value{
    display:block;

    width:100%;

    color:#eef2f5;

    font-size:10px;
    line-height:13px;

    font-weight:900;

    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;

    text-align:center;
}

.change-cell{
    text-align:center;

    font-size:10px;
    font-weight:900;

    white-space:nowrap;
}

.signal-cell{
    height:56px!important;

    border-left:1px solid #29323c;

    background:#0e141a;

    text-align:center;

    overflow:hidden!important;
}


/* =====================================================
   코인 ROC 행
   ===================================================== */

.coin-roc-row{
    min-height:68px;

    background:#0d1218;

    border-top:1px solid #29323c;
}


/* =====================================================
   Signal
   ===================================================== */

.signal-wrap{
    display:flex;

    align-items:center;
    justify-content:center;

    width:100%;
    height:100%;

    white-space:nowrap;
}

.signal-row{
    display:flex;

    align-items:center;
    justify-content:center;

    width:100%;
}

.signal-item{
    display:inline-flex;

    align-items:center;
    justify-content:center;

    width:43px;
    min-width:43px;

    height:38px;

    padding:3px;

    border-radius:9px;

    font-weight:900;

    white-space:nowrap;

    overflow:visible;

    background:#20272e;

    border:2px solid #343e47;

    color:#aeb8c1;
}

.signal-rocket{
    font-size:20px;

    line-height:21px;

    flex:none;
}

.signal-1{
    background:#153126;

    border-color:#24724e;

    color:#78c99d;

    box-shadow:
        0 0 10px
        rgba(72,190,130,.12);
}

.signal-2{
    background:#17232e;

    border-color:#31556e;

    color:#9ebbd0;

    box-shadow:
        0 0 10px
        rgba(100,160,200,.10);
}


/* =====================================================
   반짝임
   ===================================================== */

@keyframes signalFlashOne{

    0%{
        background-color:#11161c;

        box-shadow:
            inset 0 0 0
            rgba(114,189,152,0);
    }

    30%{
        background-color:#294238;

        box-shadow:
            inset 0 0 16px
            rgba(114,189,152,.32);
    }

    60%{
        background-color:#18231f;

        box-shadow:
            inset 0 0 5px
            rgba(114,189,152,.12);
    }

    100%{
        background-color:#11161c;

        box-shadow:
            inset 0 0 0
            rgba(114,189,152,0);
    }

}

@keyframes signalFlashTwo{

    0%{
        background-color:#11161c;

        box-shadow:
            inset 0 0 0
            rgba(158,181,201,0);
    }

    30%{
        background-color:#293844;

        box-shadow:
            inset 0 0 16px
            rgba(158,181,201,.30);
    }

    60%{
        background-color:#1b252d;

        box-shadow:
            inset 0 0 5px
            rgba(158,181,201,.12);
    }

    100%{
        background-color:#11161c;

        box-shadow:
            inset 0 0 0
            rgba(158,181,201,0);
    }

}

.coin-card.signal-flash-one .coin-main-row > div,
.coin-card.signal-flash-one .coin-roc-row{
    animation:
        signalFlashOne
        1.35s
        ease-in-out
        infinite;
}

.coin-card.signal-flash-two .coin-main-row > div,
.coin-card.signal-flash-two .coin-roc-row{
    animation:
        signalFlashTwo
        1.35s
        ease-in-out
        infinite;
}


/* =====================================================
   변동률
   ===================================================== */

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


/* =====================================================
   빈 카드
   ===================================================== */

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


/* =====================================================
   MOBILE
   ===================================================== */

@media(max-width:600px){

    body{
        padding:7px;
    }

    h1{
        margin:3px 3px 8px;

        font-size:13px;
        line-height:16px;
    }


    /* ================================================
       공통 섹션
       ================================================ */

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
        margin-top:1px;

        font-size:5px;
        line-height:7px;
    }

    .section-time{
        margin-left:4px;

        font-size:5px;
    }


    /* ================================================
       BTC
       ================================================ */

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
        line-height:10px;
    }

    .market-title-sub{
        margin-top:1px;

        font-size:4.8px;
        line-height:6px;
    }

    .market-time{
        margin-left:4px;

        font-size:4.8px;
    }

    .btc-main-row{
        min-height:43px;

        grid-template-columns:
            1.1fr
            1.3fr
            1fr
            1.3fr;
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


    /* ================================================
       ROC
       ================================================ */

    .btc-roc-detail,
    .roc-detail{
        grid-template-columns:
            .95fr
            5.5fr
            .95fr;

        min-height:52px;

        padding:4px 4px;
    }

    .btc-roc-label,
    .roc-label{
        font-size:7px;
    }

    .roc-grid{
        gap:3px;
    }

    .roc-card-item{
        min-height:39px;

        border-width:1px;

        border-radius:5px;
    }

    .roc-card-period{
        font-size:5.8px;
        line-height:7px;
    }

    .roc-card-value{
        gap:2px;

        margin-top:1px;
    }

    .roc-dot{
        font-size:12px;
        line-height:12px;
    }

    .roc-count{
        font-size:6.5px;
        line-height:8px;
    }

    .roc-badge{
        padding:3px 4px;

        border-radius:5px;

        font-size:5px;
        line-height:7px;
    }


    /* ================================================
       카드
       ================================================ */

    .card-list{
        gap:6px;

        margin-top:6px;
    }

    .coin-card{
        border-width:1px;

        border-radius:8px;
    }

    .coin-main-row{
        min-height:40px;

        grid-template-columns:
            6%
            18%
            15%
            21%
            14%
            13%
            13%;
    }

    .coin-main-row > div{
        height:40px;
    }

    .rank-cell{
        padding-left:5px;

        font-size:6.8px;
    }

    .coin-name{
        padding:0 1px;

        font-size:6.8px;
        line-height:9px;
    }

    .volume-value{
        font-size:5.5px;
        line-height:8px;
    }

    .price-value{
        font-size:5.8px;
        line-height:8px;
    }

    .change-cell{
        font-size:5.8px;
    }

    .signal-cell{
        height:40px!important;

        border-left:1px solid #29323c;
    }

    .signal-item{
        width:28px;
        min-width:28px;

        height:27px;

        border-width:1px;

        border-radius:5px;
    }

    .signal-rocket{
        font-size:13px;

        line-height:14px;
    }

    .coin-roc-row{
        min-height:49px;
    }


    /* ================================================
       모바일 ROC 카드
       ================================================ */

    .coin-roc-row .roc-detail{
        min-height:49px;

        padding:3px 3px;
    }

    .coin-roc-row .roc-card-item{
        min-height:37px;
    }

    .coin-roc-row .roc-card-period{
        font-size:5.2px;
    }

    .coin-roc-row .roc-dot{
        font-size:10px;
    }

    .coin-roc-row .roc-count{
        font-size:5.8px;
    }

    .coin-roc-row .roc-badge{
        font-size:4.7px;

        padding:2px 3px;
    }


    /* ================================================
       빈 카드
       ================================================ */

    .empty-card{
        min-height:43px;

        border-width:1px;

        border-radius:8px;

        font-size:6px;
    }

}


@media(max-width:380px){

    body{
        padding:4px;
    }

    h1{
        font-size:12px;
        line-height:15px;

        margin:2px 2px 6px;
    }

    .section-title-card{
        min-height:35px;

        padding:4px 5px;
    }

    .section-number{
        width:23px;
        height:23px;

        margin-right:4px;

        font-size:10px;
    }

    .section-heading-main{
        font-size:8px;
        line-height:10px;
    }

    .section-heading-sub{
        font-size:4.2px;
        line-height:6px;
    }

    .section-time{
        font-size:4.2px;
    }

    .market-card-header{
        min-height:32px;

        padding:4px 5px;
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

    .btc-change{
        font-size:7px;
    }

    .btc-signal-box{
        min-height:38px;
    }

    .btc-roc-detail,
    .roc-detail{
        min-height:47px;

        grid-template-columns:
            .8fr
            5.8fr
            .8fr;

        padding:3px 2px;
    }

    .btc-roc-label,
    .roc-label{
        font-size:6px;
    }

    .roc-grid{
        gap:2px;
    }

    .roc-card-item{
        min-height:34px;

        border-radius:4px;
    }

    .roc-card-period{
        font-size:4.8px;
    }

    .roc-dot{
        font-size:9px;
    }

    .roc-count{
        font-size:5px;
    }

    .roc-badge{
        font-size:4px;

        padding:2px 3px;
    }

    .card-list{
        gap:5px;
    }

    .coin-main-row{
        min-height:36px;
    }

    .coin-main-row > div{
        height:36px;
    }

    .rank-cell{
        padding-left:3px;

        font-size:6px;
    }

    .coin-name{
        font-size:6px;
    }

    .volume-value{
        font-size:5px;
    }

    .price-value{
        font-size:5.2px;
    }

    .change-cell{
        font-size:5.1px;
    }

    .signal-cell{
        height:36px!important;
    }

    .signal-item{
        width:24px;
        min-width:24px;

        height:23px;

        border-radius:4px;
    }

    .signal-rocket{
        font-size:11px;
        line-height:12px;
    }

    .coin-roc-row{
        min-height:45px;
    }

    .coin-roc-row .roc-detail{
        min-height:45px;
    }

    .coin-roc-row .roc-card-item{
        min-height:32px;
    }

    .coin-roc-row .roc-card-period{
        font-size:4.5px;
    }

    .coin-roc-row .roc-dot{
        font-size:8px;
    }

    .coin-roc-row .roc-count{
        font-size:4.7px;
    }

}


/* =====================================================
   REDUCED MOTION
   ===================================================== */

@media(prefers-reduced-motion:reduce){

    .coin-card.signal-flash-one .coin-main-row > div,
    .coin-card.signal-flash-one .coin-roc-row,
    .coin-card.signal-flash-two .coin-main-row > div,
    .coin-card.signal-flash-two .coin-roc-row{
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
            content="#080c11"
        >

        <title>
            1 / 2
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
        "4H 시그널 시스템 START"
    )

    log.info(
        "★ Signal 1 = ROC50 0선 상향 돌파"
    )

    log.info(
        "★ Signal 1 자체 COUNT = 0,1,2,3..."
    )

    log.info(
        "★ Signal 1 필터 = ROC20/50/200 "
        "COUNT 각각 10~30"
    )

    log.info(
        "★ Signal 1 화면/반짝임 = 자체 COUNT 0~2"
    )

    log.info(
        "★ Signal 2 = ROC5 0선 상향 돌파"
    )

    log.info(
        "★ Signal 2 자체 COUNT = 0,1,2,3..."
    )

    log.info(
        "★ Signal 2 필터 = ROC20/50/200 "
        "COUNT 각각 10~30"
    )

    log.info(
        "★ Signal 2 화면/반짝임 = 자체 COUNT 0~2"
    )

    log.info(
        "★ 일봉 변동률 >= 0%"
    )

    log.info(
        "★ 필터 COUNT가 10~30을 벗어나도 "
        "Signal 자체는 종료하지 않음"
    )

    log.info(
        "★ Signal 종료 = 트리거 ROC가 음수"
    )

    log.info(
        "★ Signal 1 / 2 각각 독립 계산"
    )

    log.info(
        "★ ROC5 / ROC20 / ROC50 / ROC200 표시"
    )

    log.info(
        "★ ROC 상세 괄호 = COUNT"
    )

    log.info(
        "★ 기준 시간봉 = Upbit native 4H"
    )

    log.info(
        "★ 1H → 4H 합성하지 않음"
    )

    log.info(
        "★ 1D 필터 사용하지 않음"
    )

    log.info(
        f"★ ROC HISTORY REQUIRED = "
        f"{ROC_HISTORY_REQUIRED}개"
    )

    log.info(
        f"★ TOP = {TOP_N}"
    )

    log.info(
        "★ 모든 화면을 카드형 디자인으로 통일"
    )

    log.info(
        "★ 메인 행 + ROC 상세 = 하나의 카드"
    )

    log.info(
        "★ 카드 사이에만 간격"
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
