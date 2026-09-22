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
# 시그널 설정
# =========================================================

SIGNAL1_ROC_PERIOD = 50
SIGNAL1_COUNT_MIN = 1
SIGNAL1_COUNT_MAX = 200

SIGNAL2_ROC_PERIOD = 5
SIGNAL2_COUNT_MIN = 10
SIGNAL2_COUNT_MAX = 30


# =========================================================
# 화면 COUNT 기준
# =========================================================

DISPLAY_COUNT_MIN = 0
DISPLAY_COUNT_MAX = 3


# =========================================================
# 반짝임 COUNT
# =========================================================

FLASH_COUNT_MIN = 0
FLASH_COUNT_MAX = 3


# =========================================================
# 시그널 1 ROC 기간별 사용 설정
# =========================================================

USE_SIGNAL1_ROC5 = "N"
USE_SIGNAL1_ROC20 = "N"
USE_SIGNAL1_ROC50 = "N"
USE_SIGNAL1_ROC200 = "Y"


# =========================================================
# 시그널 2 ROC 기간별 사용 설정
# =========================================================

USE_SIGNAL2_ROC5 = "N"
USE_SIGNAL2_ROC20 = "Y"
USE_SIGNAL2_ROC50 = "Y"
USE_SIGNAL2_ROC200 = "Y"


# =========================================================
# ROC 계산용 최소 데이터
# =========================================================

ROC_COUNT_HISTORY_EXTRA = 100

ROC_HISTORY_REQUIRED = (
    max(ROC_FILTER_PERIODS)
    + max(
        SIGNAL1_COUNT_MAX,
        SIGNAL2_COUNT_MAX,
        DISPLAY_COUNT_MAX,
        FLASH_COUNT_MAX
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
# 시그널 1 상태
# =========================================================

roc_signal1_state = {}


# =========================================================
# 시그널 1 종료 캔들
# =========================================================

roc_signal1_failed_candle = {}


# =========================================================
# 시그널 2 상태
# =========================================================

roc_signal2_state = {}


# =========================================================
# 시그널 2 종료 캔들
# =========================================================

roc_signal2_failed_candle = {}


# =========================================================
# COUNT 화면 표시
# =========================================================

def count_display_allowed(count):

    try:
        count = int(count)
    except Exception:
        return False

    return (
        DISPLAY_COUNT_MIN
        <= count
        <= DISPLAY_COUNT_MAX
    )


# =========================================================
# COUNT 반짝임
# =========================================================

def count_flash_allowed(count):

    try:
        count = int(count)
    except Exception:
        return False

    return (
        FLASH_COUNT_MIN
        <= count
        <= FLASH_COUNT_MAX
    )


def count_display_text():

    if DISPLAY_COUNT_MAX >= 999999:
        return "전체"

    if DISPLAY_COUNT_MIN == DISPLAY_COUNT_MAX:
        return str(DISPLAY_COUNT_MIN)

    return (
        f"{DISPLAY_COUNT_MIN}~"
        f"{DISPLAY_COUNT_MAX}"
    )


def count_flash_text():

    if FLASH_COUNT_MIN == FLASH_COUNT_MAX:
        return str(FLASH_COUNT_MIN)

    return (
        f"{FLASH_COUNT_MIN}~"
        f"{FLASH_COUNT_MAX}"
    )


# =========================================================
# ROC 설정
# =========================================================

def roc_settings():

    return {

        5: {
            "4H": "Y",
            "SIGNAL1": USE_SIGNAL1_ROC5,
            "SIGNAL2": USE_SIGNAL2_ROC5
        },

        20: {
            "4H": "Y",
            "SIGNAL1": USE_SIGNAL1_ROC20,
            "SIGNAL2": USE_SIGNAL2_ROC20
        },

        50: {
            "4H": "Y",
            "SIGNAL1": USE_SIGNAL1_ROC50,
            "SIGNAL2": USE_SIGNAL2_ROC50
        },

        200: {
            "4H": "Y",
            "SIGNAL1": USE_SIGNAL1_ROC200,
            "SIGNAL2": USE_SIGNAL2_ROC200
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
# 현재 캔들 시작시간
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
# ROC 0선 이상 연속 COUNT
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

    current = float(
        valid.iloc[-1]
    )

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
# ROC 0선 미만 연속 COUNT
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

    current = float(
        valid.iloc[-1]
    )

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
# 검증
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
        not isinstance(
            SIGNAL1_COUNT_MIN,
            int
        )
        or SIGNAL1_COUNT_MIN < 0
    ):

        raise ValueError(
            "SIGNAL1_COUNT_MIN 설정 오류"
        )

    if (
        not isinstance(
            SIGNAL1_COUNT_MAX,
            int
        )
        or SIGNAL1_COUNT_MAX < SIGNAL1_COUNT_MIN
    ):

        raise ValueError(
            "SIGNAL1_COUNT_MAX 설정 오류"
        )

    if (
        not isinstance(
            SIGNAL2_COUNT_MIN,
            int
        )
        or SIGNAL2_COUNT_MIN < 0
    ):

        raise ValueError(
            "SIGNAL2_COUNT_MIN 설정 오류"
        )

    if (
        not isinstance(
            SIGNAL2_COUNT_MAX,
            int
        )
        or SIGNAL2_COUNT_MAX < SIGNAL2_COUNT_MIN
    ):

        raise ValueError(
            "SIGNAL2_COUNT_MAX 설정 오류"
        )

    if (
        not isinstance(
            DISPLAY_COUNT_MIN,
            int
        )
        or not isinstance(
            DISPLAY_COUNT_MAX,
            int
        )
        or DISPLAY_COUNT_MIN < 0
        or DISPLAY_COUNT_MAX < DISPLAY_COUNT_MIN
    ):

        raise ValueError(
            "DISPLAY_COUNT_MIN/MAX 설정이 올바르지 않습니다."
        )

    if (
        not isinstance(
            FLASH_COUNT_MIN,
            int
        )
        or not isinstance(
            FLASH_COUNT_MAX,
            int
        )
        or FLASH_COUNT_MIN < 0
        or FLASH_COUNT_MAX < FLASH_COUNT_MIN
    ):

        raise ValueError(
            "FLASH_COUNT_MIN/MAX 설정이 올바르지 않습니다."
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
# 업비트 native 캔들
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

            log.warning(
                f"업비트 {format_timeframe(unit)} "
                f"응답 형식 오류: {market}"
            )

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
            f"업비트 {format_timeframe(unit)} 오류 "
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

        if chunk_index == 0:

            log.info(
                f"[추가 데이터 요청] "
                f"{market} "
                f"{format_timeframe(unit)} "
                f"1차={len(all_df)}개 "
                f"/ 필요={required}개 "
                f"→ 2차 요청"
            )

    if all_df is None:
        return None

    log.warning(
        f"[자료 부족] "
        f"{market} "
        f"{format_timeframe(unit)} "
        f"확보={len(all_df)}개 "
        f"/ 필요={required}개"
    )

    return all_df


# =========================================================
# 현재 진행 중인 ROC 데이터
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

        log.info(
            f"[현재 ROC 추가 요청] "
            f"{market} "
            f"{format_timeframe(timeframe)} "
            f"1차={len(df)}개 "
            f"/ 필요={required}개 "
            f"→ 2차 요청"
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
                f"{format_timeframe(timeframe)} "
                f"현재 진행봉 없음 | "
                f"기준={current_start} | "
                f"최근={df['datetime'].iloc[-1]}"
            )

    except Exception as e:

        log.error(
            f"현재 "
            f"{format_timeframe(timeframe)} "
            f"ROC 데이터 오류 "
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

    log.info(
        f"[NATIVE CURRENT] "
        f"{market} | "
        f"{format_timeframe(timeframe)}="
        f"{len(df)}개 | "
        f"필요={required}개"
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

        values[period] = current_value
        previous_values[period] = previous_value

        zero_crosses[period] = (
            previous_value is not None
            and previous_value <= 0
            and current_value >= 0
        )

        positive_counts[period] = (
            roc_positive_count(
                df,
                period
            )
        )

        negative_counts[period] = (
            roc_negative_count(
                df,
                period
            )
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
# ROC 0선 상향 돌파
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
# 최근 시그널 이벤트 찾기
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

        signal_roc_series = roc(
            temp,
            signal_period
        )

        if (
            signal_roc_series is None
            or signal_roc_series.empty
        ):
            return None, None

        for i in range(
            len(temp) - 1,
            0,
            -1
        ):

            current_value = (
                signal_roc_series.iloc[i]
            )

            previous_value = (
                signal_roc_series.iloc[i - 1]
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
            f"최근 시그널 이벤트 검색 오류: {e}"
        )

        return None, None


# =========================================================
# 시그널 상태 업데이트
# =========================================================

def update_signal(
    market,
    signal_number,
    current_value,
    previous_value,
    progress_candle_time,
    historical_start_candle=None
):

    market_key = str(market)

    if signal_number == 1:

        state_dict = roc_signal1_state
        failed_dict = roc_signal1_failed_candle
        signal_period = SIGNAL1_ROC_PERIOD

    else:

        state_dict = roc_signal2_state
        failed_dict = roc_signal2_failed_candle
        signal_period = SIGNAL2_ROC_PERIOD

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

    signal_cross = roc_signal_zero_cross(
        current_value,
        previous_value
    )

    signal_state = (
        state_dict.get(
            market_key
        )
    )

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
                f"[시그널 {signal_number} "
                f"ROC{signal_period} START] "
                f"{market_key} 🚀(0) | "
                f"ROC={current_value} | "
                f"기준=4H"
            )

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
                        f"[시그널 {signal_number} "
                        f"ROC{signal_period} RESTORE] "
                        f"{market_key} "
                        f"🚀({distance})"
                    )

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
                f"[시그널 {signal_number} "
                f"ROC{signal_period} END] "
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

                signal_state["count"] = distance

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
# 시그널 COUNT 범위
# =========================================================

def signal_count_allowed(
    signal_number,
    count
):

    try:

        count = int(count)

    except Exception:

        return False

    if signal_number == 1:

        return (
            SIGNAL1_COUNT_MIN
            <= count
            <= SIGNAL1_COUNT_MAX
        )

    return (
        SIGNAL2_COUNT_MIN
        <= count
        <= SIGNAL2_COUNT_MAX
    )


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

    r_signal_raw = roc_filter_analysis(
        df_current,
        all_periods
    )

    r_signal = r_signal_raw

    signal1_roc_current = (
        r_signal
        .get(
            "roc_values",
            {}
        )
        .get(
            SIGNAL1_ROC_PERIOD
        )
    )

    signal1_roc_previous = (
        r_signal
        .get(
            "previous_values",
            {}
        )
        .get(
            SIGNAL1_ROC_PERIOD
        )
    )

    signal2_roc_current = (
        r_signal
        .get(
            "roc_values",
            {}
        )
        .get(
            SIGNAL2_ROC_PERIOD
        )
    )

    signal2_roc_previous = (
        r_signal
        .get(
            "previous_values",
            {}
        )
        .get(
            SIGNAL2_ROC_PERIOD
        )
    )

    historical_start_candle1 = None

    if market not in roc_signal1_state:

        latest_event_type1, latest_event_candle1 = (
            find_latest_signal_event(
                df_signal,
                SIGNAL1_ROC_PERIOD
            )
        )

        if latest_event_type1 == "signal":

            historical_start_candle1 = (
                latest_event_candle1
            )

    historical_start_candle2 = None

    if market not in roc_signal2_state:

        latest_event_type2, latest_event_candle2 = (
            find_latest_signal_event(
                df_signal,
                SIGNAL2_ROC_PERIOD
            )
        )

        if latest_event_type2 == "signal":

            historical_start_candle2 = (
                latest_event_candle2
            )

    progress_candle_time = (
        get_current_candle_start(
            SIGNAL_TIMEFRAME
        )
    )

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

    signal1_count = int(
        state1[
            "signal_count"
        ]
    )

    signal1_count_pass = (
        state1["signal_active"]
        and
        signal_count_allowed(
            1,
            signal1_count
        )
    )

    signal2_count = int(
        state2[
            "signal_count"
        ]
    )

    signal2_count_pass = (
        state2["signal_active"]
        and
        signal_count_allowed(
            2,
            signal2_count
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

    signal1_qualified = (
        signal1_count_pass
        and
        daily_pass
    )

    signal2_qualified = (
        signal2_count_pass
        and
        daily_pass
    )

    return {

        "roc":
            r_signal,

        "changes":
            changes,

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

        "signal1_count_pass":
            signal1_count_pass,

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

        "signal2_count_pass":
            signal2_count_pass,

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
                update_okx(usdt)

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
        price = float(price)
    except Exception:
        return "-"

    if price >= 100000000:
        return f"{price / 100000000:.2f}억"

    if price >= 10000:
        return f"{price:,.0f}"

    if price >= 1:
        return f"{price:,.2f}"

    return f"{price:.6f}"


# =========================================================
# 개별 시그널 표시
# ★ 최종 조건(qualified)을 만족할 때만 🚀
# ★ 조건 미충족은 무조건 -
# ★ 숫자/COUNT는 표시하지 않음
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


# =========================================================
# BTC / 시장용 시그널
# =========================================================

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


# =========================================================
# TOP 시그널
# =========================================================

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
            count_text = "-"

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
                    count_text = "1"

            except Exception:

                icon = "⚪"
                count_text = "-"

        items.append(
            f"""
            <div class="btc-roc-item">

                <div class="btc-roc-period">
                    ROC{period}
                </div>

                <div class="btc-roc-icon">
                    {icon}
                </div>

                <div class="btc-roc-count">
                    ({count_text})
                </div>

            </div>
            """
        )

    return f"""
    <div class="btc-roc-section">

        <div class="btc-roc-title">

            <span>
                {timeframe} ROC
            </span>

            <span class="filter-status-active">
                4시간 기준
            </span>

        </div>

        <div class="btc-roc-grid">

            {"".join(items)}

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

    for x in data:

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

        if (
            signal1_qualified
            and count_flash_allowed(
                signal1_count
            )
        ):

            cls_list.append(
                "signal-flash-one"
            )

        if (
            signal2_qualified
            and count_flash_allowed(
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

        signal1_content = top_signal1_html(
            x
        )

        signal2_content = top_signal2_html(
            x
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
            <tr class="{cls}">

                <td class="rank-cell">
                    {x.get("rank", "-")}.
                </td>

                <td class="coin-cell">

                    <span class="coin-name">
                        {coin_name}
                    </span>

                </td>

                <td class="volume-cell">

                    <span class="volume-value">
                        {volume}
                    </span>

                </td>

                <td class="price-cell">

                    <span class="price-value">
                        {price}
                    </span>

                </td>

                <td class="change-cell">

                    {change}

                </td>

                <td class="signal-cell">

                    {signal1_content}

                </td>

                <td class="signal-cell">

                    {signal2_content}

                </td>

            </tr>

            <tr class="
                roc-subrow
                {cls}
            ">

                <td colspan="7">

                    {roc_content}

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
                colspan="7"
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

                    <th>
                        코인
                    </th>

                    <th>
                        거래대금
                    </th>

                    <th>
                        가격
                    </th>

                    <th>
                        변동률
                    </th>

                    <th>
                        1
                    </th>

                    <th>
                        2
                    </th>

                </tr>

            </thead>

            <tbody>

                {rows}

            </tbody>

        </table>

    </div>
    """


# =========================================================
# 시그널 1 / 시그널 2
# =========================================================

def focus_section(
    data
):

    signal1_rows = [

        x

        for x in data

        if (

            x.get(
                "signal1_qualified",
                False
            )

            and

            count_display_allowed(
                int(
                    x.get(
                        "signal1_count",
                        0
                    )
                )
            )

        )

    ]

    signal2_rows = [

        x

        for x in data

        if (

            x.get(
                "signal2_qualified",
                False
            )

            and

            count_display_allowed(
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
    <div class="section-title long-title">

        <span class="section-title-main">
            1
        </span>

        <span class="section-title-sub">

            ROC{SIGNAL1_ROC_PERIOD}
            4H

            ·

            COUNT
            {SIGNAL1_COUNT_MIN}~
            {SIGNAL1_COUNT_MAX}

            ·

            0선 상향 돌파

            ·

            당일 변동 0% 이상

            ·

            화면 COUNT
            {count_display_text()}

            ·

            반짝임
            {count_flash_text()}

            ·

            {kst()} KST

        </span>

    </div>

    {signal1_table}

    <div class="section-title long-title">

        <span class="section-title-main">
            2
        </span>

        <span class="section-title-sub">

            ROC{SIGNAL2_ROC_PERIOD}
            4H

            ·

            COUNT
            {SIGNAL2_COUNT_MIN}~
            {SIGNAL2_COUNT_MAX}

            ·

            0선 상향 돌파

            ·

            당일 변동 0% 이상

            ·

            화면 COUNT
            {count_display_text()}

            ·

            반짝임
            {count_flash_text()}

            ·

            {kst()} KST

        </span>

    </div>

    {signal2_table}
    """


# =========================================================
# TOP
# =========================================================

def section(
    data,
    update_time
):

    return f"""
    <div class="section-title top-title">

        <span class="section-title-main">
            🏆 업비트 TOP{TOP_N}
        </span>

        <span class="section-title-sub">

            {update_time} KST

            ·

            1
            ROC{SIGNAL1_ROC_PERIOD}
            4H
            COUNT
            {SIGNAL1_COUNT_MIN}~
            {SIGNAL1_COUNT_MAX}

            ·

            2
            ROC{SIGNAL2_ROC_PERIOD}
            4H
            COUNT
            {SIGNAL2_COUNT_MIN}~
            {SIGNAL2_COUNT_MAX}

            ·

            4H native

        </span>

    </div>

    {table_html(data)}
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

    periods = ROC_FILTER_PERIODS.copy()

    items = []

    for period in periods:

        series = roc(
            df_signal,
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

                    count = (
                        roc_positive_count(
                            df_signal,
                            period
                        )
                    )

                else:

                    icon = "🔴"

                    count = (
                        roc_negative_count(
                            df_signal,
                            period
                        )
                    )

        items.append(
            f"""
            <div class="btc-roc-item">

                <div class="btc-roc-period">
                    ROC{period}
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

            <span>
                4H ROC
            </span>

            <span class="filter-status-active">
                시그널 기준
            </span>

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
    <div class="market-summary">

        <div class="market-title">

            <span class="market-title-main">
                ₿ BTC 시장 시황
            </span>

            <span class="market-title-sub">

                1
                ROC{SIGNAL1_ROC_PERIOD}
                COUNT
                {SIGNAL1_COUNT_MIN}~
                {SIGNAL1_COUNT_MAX}

                ·

                2
                ROC{SIGNAL2_ROC_PERIOD}
                COUNT
                {SIGNAL2_COUNT_MIN}~
                {SIGNAL2_COUNT_MAX}

                ·

                기준 4H native

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

font-size:9px;

padding:5px;
}

h1{
margin:3px 4px 7px;

color:#e5e9ed;

font-size:15px;
line-height:18px;

font-weight:900;
}


/* =====================================================
   공통 제목
   시그널 1 / 2 / TOP 동일
   ===================================================== */

.market-title,
.section-title{
display:flex;
align-items:center;

gap:8px;

width:100%;
min-height:28px;

padding:5px 8px;

background:#14181d;

border:1px solid #292f36;
border-left:3px solid #66717b;

border-radius:6px;

white-space:nowrap;
overflow:hidden;
}

.section-title{
margin:9px 0 6px;
}

.market-title-main,
.section-title-main{
flex:none;

color:#e7ebef;

font-size:10px;
font-weight:900;
}

.market-title-sub,
.section-title-sub{
min-width:0;

color:#858e98;

font-size:7px;
font-weight:700;

overflow:hidden;
text-overflow:ellipsis;
}


/* =====================================================
   TOP 제목
   ===================================================== */

.top-title{
margin-top:9px;
margin-bottom:6px;
}


/* =====================================================
   시장 요약
   ===================================================== */

.market-summary{
width:100%;

margin:3px 0 6px;

padding:6px;

background:#0f1318;

border-top:1px solid #292f36;
border-bottom:1px solid #292f36;

border-radius:5px;
}

.btc-top{
display:flex;
align-items:center;

gap:8px;

min-height:27px;

white-space:nowrap;
overflow:hidden;
}

.btc-name{
color:#e0e5e9;

font-size:8px;
font-weight:900;
}

.btc-price{
color:#edf1f4;

font-size:8px;
font-weight:800;

white-space:nowrap;
}

.btc-change{
font-size:10px;
font-weight:900;

white-space:nowrap;
}

.btc-top > span:last-child{
margin-left:2px;
}


/* =====================================================
   BTC ROC
   ===================================================== */

.btc-roc-section{
width:100%;

margin-top:5px;
padding-top:5px;

border-top:1px solid #252b32;
}

.btc-roc-title{
display:flex;
align-items:center;
justify-content:space-between;

width:100%;

margin-bottom:4px;
padding:0 2px;

color:#aeb6be;

font-size:8px;
font-weight:900;

line-height:11px;
}

.btc-roc-grid{
display:grid;

grid-template-columns:
    repeat(4, minmax(0, 1fr));

width:100%;

gap:4px;
}

.btc-roc-item{
display:flex;
flex-direction:column;
align-items:center;
justify-content:center;

min-height:39px;

background:#151a20;

border:1px solid #293039;
border-radius:5px;

text-align:center;
}

.btc-roc-period{
color:#9aa3ad;

font-size:8px;
font-weight:900;

line-height:11px;

text-align:center;
}

.btc-roc-icon{
font-size:13px;

line-height:15px;

text-align:center;
}

.btc-roc-count{
color:#d5dbe0;

font-size:8px;
font-weight:900;

line-height:11px;

text-align:center;
}

.filter-status-active{
display:inline-flex;
align-items:center;

padding:2px 6px;

border-radius:4px;

background:#1b3027;

color:#72bd98;

font-size:7px;
font-weight:900;

line-height:10px;
}


/* =====================================================
   TABLE
   ★ 열 사이 세로 구분선 없음
   ★ 가로선만 사용
   ===================================================== */

.table-wrap{
width:100%;

overflow:hidden;

border:1px solid #2a3037;

border-radius:7px;

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
height:23px;

padding:3px 2px;

color:#818a94;

border-bottom:1px solid #292f36;

border-left:none!important;
border-right:none!important;

font-size:7px;
font-weight:800;

text-align:center;
vertical-align:middle;
}

td{
height:32px;

padding:2px;

color:#d8dde2;

border-bottom:1px solid #22282e;

border-left:none!important;
border-right:none!important;

text-align:center;
vertical-align:middle;

overflow:hidden;
}


/* =====================================================
   테이블 폭
   ===================================================== */

th:nth-child(1),
td:nth-child(1){
width:5%;
}

th:nth-child(2),
td:nth-child(2){
width:18%;
}

th:nth-child(3),
td:nth-child(3){
width:15%;
}

th:nth-child(4),
td:nth-child(4){
width:22%;
}

th:nth-child(5),
td:nth-child(5){
width:14%;
}

th:nth-child(6),
td:nth-child(6){
width:13%;
}

th:nth-child(7),
td:nth-child(7){
width:13%;
}


/* =====================================================
   순위
   ★ 원형 제거
   ★ 1. / 2. / 3. 형태로 표시
   ===================================================== */

.rank-cell{
color:#cbd2d8;

font-size:8px;
font-weight:900;

text-align:left!important;

padding-left:5px!important;
padding-right:1px!important;

white-space:nowrap;
}


/* =====================================================
   코인
   ===================================================== */

.coin-cell{
text-align:center!important;

padding-left:3px!important;
padding-right:3px!important;
}

.coin-name{
display:block;

color:#e3e8ec;

font-size:8px;
line-height:11px;

font-weight:800;

white-space:nowrap;
overflow:hidden;
text-overflow:ellipsis;

text-align:center;
}


/* =====================================================
   거래대금
   ===================================================== */

.volume-cell{
text-align:center!important;

padding-left:3px!important;
padding-right:3px!important;
}

.volume-value{
display:block;

color:#f1f3f5;

font-size:7px;
line-height:10px;

font-weight:800;

white-space:nowrap;
overflow:hidden;
text-overflow:ellipsis;

text-align:center;
}


/* =====================================================
   가격
   ===================================================== */

.price-cell{
text-align:center!important;

padding-left:3px!important;
padding-right:3px!important;
}

.price-value{
display:block;

color:#edf1f4;

font-size:7.5px;

font-weight:800;

white-space:nowrap;
overflow:hidden;
text-overflow:ellipsis;

text-align:center;
}


/* =====================================================
   변동률
   ===================================================== */

.change-cell{
text-align:center!important;

white-space:nowrap;

font-size:7.5px;
font-weight:900;
}


/* =====================================================
   시그널
   ★ 세로선 없음
   ===================================================== */

.signal-cell{
text-align:center!important;

padding-left:2px!important;
padding-right:2px!important;

white-space:nowrap;
overflow:hidden;

border-left:none!important;
border-right:none!important;
}


/* 시그널 칸은 색상으로만 구분 */

thead th:nth-child(6),
thead th:nth-child(7){

background:#11161b;

border-left:none!important;
border-right:none!important;
}

tbody td:nth-child(6),
tbody td:nth-child(7){

background:#11161b;

border-left:none!important;
border-right:none!important;
}


/* =====================================================
   시그널
   ===================================================== */

.signal-wrap{
display:flex;

flex-direction:column;

align-items:center;
justify-content:center;

gap:2px;

min-height:23px;

white-space:nowrap;
}

.signal-row{
display:flex;

align-items:center;
justify-content:center;

width:100%;

white-space:nowrap;
}

.signal-item{
display:inline-flex;

align-items:center;
justify-content:center;

width:28px;
min-width:28px;

height:20px;

padding:2px;

border-radius:4px;

font-weight:900;

white-space:nowrap;

overflow:visible;

background:#20252b;

border:1px solid #343b43;

color:#aeb6be;
}

.signal-rocket{
font-size:11px;

line-height:12px;

flex:none;
}

.signal-1{
background:#183329;

border-color:#285b45;

color:#72bd98;
}

.signal-2{
background:#222b35;

border-color:#3a4856;

color:#9eb5c9;
}

.signal-normal{
opacity:.9;
}


/* =====================================================
   ROC 상세
   ★ 세로 열 구분선 완전 제거
   ★ 가로 구분선만 유지
   ===================================================== */

.roc-subrow{
background:#0f1318!important;
}

.roc-subrow td{

height:auto!important;

padding:0!important;

border-top:1px solid #252b32!important;

border-bottom:1px solid #22282e!important;

border-left:none!important;
border-right:none!important;

background:#0f1318!important;
}


/* =====================================================
   ROC 상세 내용
   ===================================================== */

.filter-detail{
display:flex;

flex-direction:column;

gap:4px;

width:100%;

padding:5px 6px 6px;
}

.filter-detail .btc-roc-section{
margin-top:0;

padding-top:0;

border-top:0;
}

.filter-detail .btc-roc-title{
margin-bottom:4px;

font-size:7.5px;
}

.filter-detail .btc-roc-grid{
gap:3px;
}

.filter-detail .btc-roc-item{
min-height:36px;
}


/* =====================================================
   반짝임
   ===================================================== */

@keyframes signalFlashOne{

0%{
background-color:#15191e;

box-shadow:
inset 0 0 0
rgba(114,189,152,0);
}

30%{
background-color:#2a3d34;

box-shadow:
inset 0 0 12px
rgba(114,189,152,.32);
}

60%{
background-color:#19221e;

box-shadow:
inset 0 0 3px
rgba(114,189,152,.12);
}

100%{
background-color:#15191e;

box-shadow:
inset 0 0 0
rgba(114,189,152,0);
}

}

@keyframes signalFlashTwo{

0%{
background-color:#15191e;

box-shadow:
inset 0 0 0
rgba(158,181,201,0);
}

30%{
background-color:#29343e;

box-shadow:
inset 0 0 12px
rgba(158,181,201,.30);
}

60%{
background-color:#1b2228;

box-shadow:
inset 0 0 3px
rgba(158,181,201,.12);
}

100%{
background-color:#15191e;

box-shadow:
inset 0 0 0
rgba(158,181,201,0);
}

}

tr.signal-flash-one td{
animation:
signalFlashOne
1.35s
ease-in-out
infinite;
}

tr.signal-flash-two td{
animation:
signalFlashTwo
1.35s
ease-in-out
infinite;
}


/* =====================================================
   변동률 색상
   ===================================================== */

.up{
color:#72bd98!important;
font-weight:900;
}

.down{
color:#cf8585!important;
font-weight:900;
}

.zero{
color:#707a84!important;
}

.muted{
color:#707a84!important;
}

.empty{
height:36px;

color:#59626c;

font-size:7px;

text-align:center!important;
}


/* =====================================================
   MOBILE
   ===================================================== */

@media(max-width:380px){

body{
padding:3px;
}

h1{
font-size:13px;
line-height:16px;

margin:3px 3px 6px;
}

.market-title,
.section-title{
min-height:23px;

padding:4px 6px;

gap:5px;
}

.market-title-main,
.section-title-main{
font-size:8px;
}

.market-title-sub,
.section-title-sub{
font-size:5.5px;
}

.btc-top{
gap:5px;
}

.btc-name{
font-size:7px;
}

.btc-price{
font-size:6.5px;
}

.btc-change{
font-size:8px;
}

.btc-roc-section{
margin-top:4px;

padding-top:4px;
}

.btc-roc-title{
margin-bottom:3px;

padding:0 1px;

font-size:7px;

line-height:10px;
}

.btc-roc-grid{
gap:2px;
}

.btc-roc-item{
min-height:34px;

border-radius:4px;
}

.btc-roc-period{
font-size:6.5px;

line-height:9px;
}

.btc-roc-icon{
font-size:11px;

line-height:13px;
}

.btc-roc-count{
font-size:6.5px;

line-height:9px;
}

.filter-status-active{
padding:1px 4px;

font-size:5.5px;

line-height:8px;
}


/* =====================================================
   모바일 테이블
   ===================================================== */

th{
height:19px;

padding:2px 1px;

font-size:5.5px;

border-left:none!important;
border-right:none!important;
}

td{
height:29px;

border-left:none!important;
border-right:none!important;
}


/* =====================================================
   모바일 테이블 폭
   ===================================================== */

th:nth-child(1),
td:nth-child(1){
width:5%;
}

th:nth-child(2),
td:nth-child(2){
width:18%;
}

th:nth-child(3),
td:nth-child(3){
width:15%;
}

th:nth-child(4),
td:nth-child(4){
width:22%;
}

th:nth-child(5),
td:nth-child(5){
width:14%;
}

th:nth-child(6),
td:nth-child(6){
width:13%;
}

th:nth-child(7),
td:nth-child(7){
width:13%;
}


/* =====================================================
   모바일 순위
   ★ 원형 제거
   ===================================================== */

.rank-cell{
padding-left:3px!important;
padding-right:1px!important;

text-align:left!important;

font-size:6.8px;
font-weight:900;

white-space:nowrap;
}


/* =====================================================
   모바일 코인
   ===================================================== */

.coin-cell{
padding-left:2px!important;
padding-right:2px!important;

text-align:center!important;
}

.coin-name{
font-size:6.8px;

text-align:center;
}


/* =====================================================
   모바일 거래대금
   ===================================================== */

.volume-cell{
padding-left:1px!important;
padding-right:1px!important;

text-align:center!important;
}

.volume-value{
font-size:5.3px;

color:#f1f3f5;

font-weight:800;

text-align:center;
}


/* =====================================================
   모바일 가격
   ===================================================== */

.price-cell{
padding-left:1px!important;
padding-right:1px!important;

text-align:center!important;
}

.price-value{
font-size:5.7px;

text-align:center;
}


/* =====================================================
   모바일 변동률
   ===================================================== */

.change-cell{
font-size:5.6px;

text-align:center!important;
}


/* =====================================================
   모바일 ROC
   ★ 세로선 없음
   ===================================================== */

.roc-subrow td{

border-top:1px solid #252b32!important;

border-bottom:1px solid #22282e!important;

border-left:none!important;
border-right:none!important;

background:#0f1318!important;
}


/* =====================================================
   모바일 시그널
   ===================================================== */

.signal-cell{
padding-left:1px!important;
padding-right:1px!important;

text-align:center!important;

white-space:nowrap;
overflow:hidden;

border-left:none!important;
border-right:none!important;
}

.signal-item{
display:inline-flex;

align-items:center;
justify-content:center;

width:23px;
min-width:23px;

height:18px;

padding:1px;

border-radius:3px;

gap:0;

white-space:nowrap;

overflow:visible;
}

.signal-rocket{
font-size:9px;

line-height:10px;

flex:none;
}


/* =====================================================
   모바일 ROC 상세
   ===================================================== */

.filter-detail{
gap:2px;

padding:3px 4px 4px;
}

.filter-detail .btc-roc-grid{
gap:2px;
}

.filter-detail .btc-roc-item{
min-height:31px;
}

.filter-detail .btc-roc-period{
font-size:5.8px;

text-align:center;
}

.filter-detail .btc-roc-icon{
font-size:9px;

text-align:center;
}

.filter-detail .btc-roc-count{
font-size:5.8px;

text-align:center;
}

.btc-roc-period{
font-size:5.8px;

text-align:center;
}

.btc-roc-icon{
font-size:9px;

text-align:center;
}

.btc-roc-count{
font-size:5.8px;

text-align:center;
}

.btc-roc-item{
min-height:31px;
}

}


/* =====================================================
   REDUCED MOTION
   ===================================================== */

@media(prefers-reduced-motion:reduce){

tr.signal-flash-one td,
tr.signal-flash-two td{
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
        f"★ 1 = "
        f"ROC{SIGNAL1_ROC_PERIOD} / "
        f"COUNT "
        f"{SIGNAL1_COUNT_MIN}~"
        f"{SIGNAL1_COUNT_MAX}"
    )

    log.info(
        f"★ 2 = "
        f"ROC{SIGNAL2_ROC_PERIOD} / "
        f"COUNT "
        f"{SIGNAL2_COUNT_MIN}~"
        f"{SIGNAL2_COUNT_MAX}"
    )

    log.info(
        "★ 기준 시간봉 = 4H"
    )

    log.info(
        "★ 1 / 2 각각 독립 계산"
    )

    log.info(
        "★ 1 = ROC50 0선 상향 돌파"
    )

    log.info(
        "★ 2 = ROC5 0선 상향 돌파"
    )

    log.info(
        "★ 시그널 COUNT는 0선 이상 연속 캔들 수"
    )

    log.info(
        "★ ROC5 / ROC20 / ROC50 / ROC200 사용"
    )

    log.info(
        "★ ROC20/ROC50/ROC200은 0선 이상 연속 COUNT 표시"
    )

    log.info(
        f"★ COUNT 화면 표시 = "
        f"{count_display_text()}"
    )

    log.info(
        f"★ COUNT 반짝임 = "
        f"{count_flash_text()}"
    )

    log.info(
        "★ 시그널만 반짝임"
    )

    log.info(
        "★ 과거 이벤트는 가장 최근 상승 이벤트 하나만 사용"
    )

    log.info(
        f"★ ROC HISTORY REQUIRED = "
        f"{ROC_HISTORY_REQUIRED}개"
    )

    log.info(
        "★ 선택한 시간봉은 "
        "업비트 원본 240분봉 사용"
    )

    log.info(
        "★ 1H → 4H 캔들 합성하지 않음"
    )

    log.info(
        "★ 1D 필터 사용하지 않음"
    )

    log.info(
        "★ 일봉 등락 = "
        "상승 리스트의 기본 표시 기준"
    )

    log.info(
        f"★ ROC200 현재값 + COUNT 계산을 위해 "
        f"{ROC_HISTORY_REQUIRED}개 데이터 확보"
    )

    log.info(
        "★ 1차 요청 = 최대 200개"
    )

    log.info(
        "★ 자료 부족 시 과거 200개 추가 요청"
    )

    log.info(
        "★ 4H 진행봉 기준 = "
        "01:00 / 05:00 / 09:00 / "
        "13:00 / 17:00 / 21:00"
    )

    log.info(
        f"★ 화면 COUNT 표시 = "
        f"{count_display_text()}"
    )

    log.info(
        f"★ 반짝임 COUNT = "
        f"{count_flash_text()}"
    )

    log.info(
        "★ TOP 테이블 = "
        "순위 / 코인 / 거래대금 / 가격 / 변동률 / 1 / 2"
    )

    log.info(
        "★ 신호 칸 = 🚀 그림만 표시"
    )

    log.info(
        "★ COUNT 숫자는 아래 ROC 상세에서 표시"
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
