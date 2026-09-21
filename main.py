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
# 신호 / COUNT 기준 시간봉
# =========================================================

SIGNAL_TIMEFRAME = 240


# =========================================================
# ROC 필터 기간
# =========================================================

ROC_FILTER_PERIODS = [
    5,
    20,
    50,
    200
]


# =========================================================
# ROC 필터 최소 COUNT
# =========================================================

ROC_FILTER_COUNT_MIN = 10


# =========================================================
# 화면 COUNT
# =========================================================

DISPLAY_COUNT_MIN = 0
DISPLAY_COUNT_MAX = 1


# =========================================================
# 반짝임 COUNT
# =========================================================

FLASH_COUNT_MIN = 0
FLASH_COUNT_MAX = 1


# =========================================================
# ROC 필터
#
# 사용할 필터를 Y / N으로 직접 설정
#
# FILTER1_TIMEFRAME
# FILTER2_TIMEFRAME
#
# 60   = 1H
# 240  = 4H
# 1440 = 1D
# =========================================================

USE_FILTER1 = "Y"
USE_FILTER2 = "N"

FILTER1_TIMEFRAME = 240
FILTER2_TIMEFRAME = 1440


# =========================================================
# ROC 기간별 화면 사용 설정
#
# 기존 1H / 4H 설정 유지
# 1D 추가
# =========================================================

USE_1H_ROC5 = "Y"
USE_1H_ROC20 = "Y"
USE_1H_ROC50 = "Y"
USE_1H_ROC200 = "Y"

USE_4H_ROC5 = "Y"
USE_4H_ROC20 = "Y"
USE_4H_ROC50 = "Y"
USE_4H_ROC200 = "Y"

USE_1D_ROC5 = "Y"
USE_1D_ROC20 = "Y"
USE_1D_ROC50 = "Y"
USE_1D_ROC200 = "Y"


# =========================================================
# 신호 ROC
# =========================================================

SIGNAL_ROC_PERIOD = 5


# =========================================================
# ROC 계산용 최소 데이터
# =========================================================

ROC_COUNT_HISTORY_EXTRA = 100

ROC_HISTORY_REQUIRED = (
    max(
        ROC_FILTER_PERIODS
    )
    + max(
        ROC_FILTER_COUNT_MIN,
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
# ROC 상승신호 상태
# =========================================================

roc_signal_state = {}


# =========================================================
# ROC 눌림 상태
# =========================================================

roc_pullback_state = {}


# =========================================================
# 진행 캔들에서 이미 종료된 ROC 신호
# =========================================================

roc_signal_failed_candle = {}


# =========================================================
# 진행 캔들에서 이미 종료된 ROC 눌림
# =========================================================

roc_pullback_failed_candle = {}


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
            "1H": USE_1H_ROC5,
            "4H": USE_4H_ROC5,
            "1D": USE_1D_ROC5
        },

        20: {
            "1H": USE_1H_ROC20,
            "4H": USE_4H_ROC20,
            "1D": USE_1D_ROC20
        },

        50: {
            "1H": USE_1H_ROC50,
            "4H": USE_4H_ROC50,
            "1D": USE_1D_ROC50
        },

        200: {
            "1H": USE_1H_ROC200,
            "4H": USE_4H_ROC200,
            "1D": USE_1D_ROC200
        }

    }


def get_enabled_periods(timeframe):

    settings = roc_settings()

    if timeframe not in (
        "1H",
        "4H",
        "1D"
    ):
        return []

    return [
        p
        for p in ROC_FILTER_PERIODS
        if settings[p][timeframe] == "Y"
    ]


def get_all_periods():

    return ROC_FILTER_PERIODS.copy()


def get_filter_configs():

    result = []

    if USE_FILTER1 == "Y":

        result.append(
            FILTER1_TIMEFRAME
        )

    if USE_FILTER2 == "Y":

        result.append(
            FILTER2_TIMEFRAME
        )

    return result


def get_enabled_all_filters():

    result = []

    for timeframe_minutes in (
        get_filter_configs()
    ):

        timeframe = format_timeframe(
            timeframe_minutes
        )

        periods = get_enabled_periods(
            timeframe
        )

        for period in periods:

            result.append(
                (
                    timeframe,
                    period
                )
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

    labels = []

    if USE_FILTER1 == "Y":

        labels.append(
            format_timeframe(
                FILTER1_TIMEFRAME
            )
        )

    if USE_FILTER2 == "Y":

        labels.append(
            format_timeframe(
                FILTER2_TIMEFRAME
            )
        )

    if not labels:
        return "-"

    return " / ".join(
        labels
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


# =========================================================
# 현재 캔들 시작시간
# =========================================================

def get_current_candle_start(minutes):

    minutes = int(minutes)

    now = datetime.now(KST)

    # =====================================================
    # 1D = 업비트 일봉 기준 09:00 KST
    # =====================================================

    if minutes == 1440:

        anchor = now.replace(
            hour=9,
            minute=0,
            second=0,
            microsecond=0
        )

        if now < anchor:

            anchor = (
                anchor
                - timedelta(days=1)
            )

        return anchor.replace(
            tzinfo=None
        )

    # =====================================================
    # 4H
    # =====================================================

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

    if SIGNAL_TIMEFRAME not in (
        60,
        240
    ):

        raise ValueError(
            "SIGNAL_TIMEFRAME은 "
            "60 또는 240만 사용할 수 있습니다."
        )

    if USE_FILTER1 not in (
        "Y",
        "N"
    ):

        raise ValueError(
            "USE_FILTER1은 Y/N만 가능합니다."
        )

    if USE_FILTER2 not in (
        "Y",
        "N"
    ):

        raise ValueError(
            "USE_FILTER2는 Y/N만 가능합니다."
        )

    valid_filter_timeframes = (
        60,
        240,
        1440
    )

    if FILTER1_TIMEFRAME not in valid_filter_timeframes:

        raise ValueError(
            "FILTER1_TIMEFRAME은 "
            "60, 240, 1440만 사용할 수 있습니다."
        )

    if FILTER2_TIMEFRAME not in valid_filter_timeframes:

        raise ValueError(
            "FILTER2_TIMEFRAME은 "
            "60, 240, 1440만 사용할 수 있습니다."
        )

    if SIGNAL_ROC_PERIOD not in ROC_FILTER_PERIODS:

        raise ValueError(
            "SIGNAL_ROC_PERIOD 설정 오류"
        )

    if (
        not isinstance(
            ROC_FILTER_COUNT_MIN,
            int
        )
        or ROC_FILTER_COUNT_MIN < 0
    ):

        raise ValueError(
            "ROC_FILTER_COUNT_MIN 설정 오류"
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

    # =====================================================
    # 1D = 업비트 days API
    # =====================================================

    if unit == 1440:

        endpoint = (
            "https://api.upbit.com/"
            "v1/candles/days"
        )

    else:

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

        # 일봉도 KST 기준으로 통일
        if unit == 1440:

            df["datetime"] = pd.to_datetime(
                df["candle_date_time_kst"],
                errors="coerce"
            )

        else:

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


def roc_filter_display(
    result,
    timeframe
):

    result = dict(
        result or {}
    )

    result["timeframe"] = timeframe

    result["enabled_periods"] = (
        get_enabled_periods(
            timeframe
        )
    )

    result["all_periods"] = (
        ROC_FILTER_PERIODS.copy()
    )

    return result


# =========================================================
# 활성 ROC 필터
# =========================================================

def get_roc_count_filter_periods():

    return [
        20,
        50,
        200
    ]


def get_roc_count_filter_status(
    info
):

    required_periods = (
        get_roc_count_filter_periods()
    )

    if not info:
        return False

    positive_counts = info.get(
        "positive_counts",
        {}
    )

    for period in required_periods:

        count = positive_counts.get(
            period
        )

        if count is None:
            return False

        if int(count) < ROC_FILTER_COUNT_MIN:
            return False

    return True


def get_all_active_roc_status(
    filters
):

    if not filters:

        return (
            True,
            False
        )

    for item in filters:

        if not item:
            return (
                False,
                False
            )

        if not get_roc_count_filter_status(
            item
        ):

            return (
                False,
                False
            )

    return (
        True,
        False
    )


def all_active_roc_filters_pass(
    filters
):

    current, _ = (
        get_all_active_roc_status(
            filters
        )
    )

    return current


# =========================================================
# ROC 0선 상향 돌파
# =========================================================

def roc_signal_zero_cross(r):

    if not r:
        return False

    current = r.get(
        "signal_roc"
    )

    previous = r.get(
        "signal_roc_previous"
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
# ROC 0선 하향 돌파
# =========================================================

def roc_signal_pullback_condition(r):

    if not r:
        return False

    current = r.get(
        "signal_roc"
    )

    previous = r.get(
        "signal_roc_previous"
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
# 가장 최근 ROC5 이벤트 찾기
# =========================================================

def find_latest_signal_event(
    df_signal
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
            SIGNAL_ROC_PERIOD
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

            if (
                previous_value > 0
                and current_value <= 0
            ):

                return (
                    "pullback",
                    normalize_datetime(
                        temp[
                            "datetime"
                        ].iloc[i]
                    )
                )

        return None, None

    except Exception as e:

        log.warning(
            f"ROC{SIGNAL_ROC_PERIOD} "
            f"최근 이벤트 검색 오류: {e}"
        )

        return None, None


# =========================================================
# 신호 + COUNT
# =========================================================

def update_signal_and_pullback(
    market,
    r,
    filter_pass,
    progress_candle_time,
    historical_start_candle=None,
    historical_pullback_start_candle=None
):

    market_key = str(market)

    progress_candle_time = (
        normalize_datetime(
            progress_candle_time
        )
    )

    signal_roc_current = r.get(
        "signal_roc"
    )

    signal_roc_previous = r.get(
        "signal_roc_previous"
    )

    try:

        current_value = float(
            signal_roc_current
        )

    except Exception:

        current_value = None

    try:

        previous_value = float(
            signal_roc_previous
        )

    except Exception:

        previous_value = None

    signal_cross = False

    if (
        current_value is not None
        and previous_value is not None
    ):

        signal_cross = (
            previous_value < 0
            and current_value >= 0
        )

    pullback_cross = False

    if (
        current_value is not None
        and previous_value is not None
    ):

        pullback_cross = (
            previous_value > 0
            and current_value <= 0
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
    # ① 상승 0선 돌파
    # =====================================================

    if signal_cross:

        if progress_candle_time is not None:

            roc_signal_state[
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
                roc_signal_state[
                    market_key
                ]
            )

            roc_pullback_state.pop(
                market_key,
                None
            )

            roc_signal_failed_candle.pop(
                market_key,
                None
            )

            log.info(
                f"[ROC{SIGNAL_ROC_PERIOD} "
                f"SIGNAL START] "
                f"{market_key} 🚀(0) | "
                f"ROC={current_value} | "
                f"기준="
                f"{format_timeframe(SIGNAL_TIMEFRAME)}"
            )

    # =====================================================
    # ② 하락 0선 돌파
    # =====================================================

    elif pullback_cross:

        if signal_state is not None:

            old_count = int(
                signal_state.get(
                    "count",
                    0
                )
            )

            log.info(
                f"[ROC{SIGNAL_ROC_PERIOD} "
                f"SIGNAL END] "
                f"{market_key} | "
                f"COUNT={old_count} | "
                f"ROC 하락돌파"
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

                "cross_candle":
                    progress_candle_time,

                "count":
                    0,

                "last_candle":
                    progress_candle_time

            }

            pullback_state = (
                roc_pullback_state[
                    market_key
                ]
            )

            roc_pullback_failed_candle.pop(
                market_key,
                None
            )

            log.info(
                f"[ROC{SIGNAL_ROC_PERIOD} "
                f"PULLBACK START] "
                f"{market_key} 📉(0)"
            )

    # =====================================================
    # ③ 현재 상태가 없으면 과거 이벤트 복구
    # =====================================================

    else:

        if (
            signal_state is None
            and pullback_state is None
        ):

            start_candle = None

            if historical_start_candle is not None:

                start_candle = (
                    normalize_datetime(
                        historical_start_candle
                    )
                )

            pullback_start_candle = None

            if historical_pullback_start_candle is not None:

                pullback_start_candle = (
                    normalize_datetime(
                        historical_pullback_start_candle
                    )
                )

            if (
                start_candle is not None
                and current_value is not None
                and current_value >= 0
                and progress_candle_time is not None
            ):

                failed_candle = (
                    roc_signal_failed_candle.get(
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

                    roc_signal_state[
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
                        roc_signal_state[
                            market_key
                        ]
                    )

                    log.info(
                        f"[ROC{SIGNAL_ROC_PERIOD} "
                        f"SIGNAL RESTORE] "
                        f"{market_key} "
                        f"🚀({distance})"
                    )

            elif (
                pullback_start_candle is not None
                and current_value is not None
                and current_value <= 0
                and progress_candle_time is not None
            ):

                failed_candle = (
                    roc_pullback_failed_candle.get(
                        market_key
                    )
                )

                if (
                    failed_candle
                    != progress_candle_time
                ):

                    distance = candle_distance(
                        pullback_start_candle,
                        progress_candle_time,
                        SIGNAL_TIMEFRAME
                    )

                    roc_pullback_state[
                        market_key
                    ] = {

                        "active":
                            True,

                        "cross_candle":
                            pullback_start_candle,

                        "count":
                            distance,

                        "last_candle":
                            progress_candle_time

                    }

                    pullback_state = (
                        roc_pullback_state[
                            market_key
                        ]
                    )

                    log.info(
                        f"[ROC{SIGNAL_ROC_PERIOD} "
                        f"PULLBACK RESTORE] "
                        f"{market_key} "
                        f"📉({distance})"
                    )

    # =====================================================
    # ④ 상승 상태 COUNT
    # =====================================================

    signal_state = (
        roc_signal_state.get(
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

                roc_signal_failed_candle[
                    market_key
                ] = progress_candle_time

            log.info(
                f"[ROC{SIGNAL_ROC_PERIOD} "
                f"SIGNAL END] "
                f"{market_key} | "
                f"COUNT={old_count} | "
                f"ROC 음수"
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
                    SIGNAL_TIMEFRAME
                )

                signal_state["count"] = distance

                signal_state[
                    "last_candle"
                ] = progress_candle_time

    # =====================================================
    # ⑤ 눌림 상태 COUNT
    # =====================================================

    pullback_state = (
        roc_pullback_state.get(
            market_key
        )
    )

    if pullback_state is not None:

        if (
            current_value is not None
            and current_value >= 0
        ):

            old_count = int(
                pullback_state.get(
                    "count",
                    0
                )
            )

            if progress_candle_time is not None:

                roc_pullback_failed_candle[
                    market_key
                ] = progress_candle_time

            log.info(
                f"[ROC{SIGNAL_ROC_PERIOD} "
                f"PULLBACK END] "
                f"{market_key} | "
                f"COUNT={old_count} | "
                f"ROC 양수"
            )

            roc_pullback_state.pop(
                market_key,
                None
            )

            pullback_state = None

        else:

            cross_candle = (
                pullback_state.get(
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

                pullback_state[
                    "count"
                ] = distance

                pullback_state[
                    "last_candle"
                ] = progress_candle_time

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

        "signal_roc_cross":
            signal_cross,

        "signal_roc_pullback":
            pullback_cross

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

    # =====================================================
    # ROC 필터 데이터
    #
    # 활성화된 필터만 가져옴
    # =====================================================

    filter_timeframes = (
        get_filter_configs()
    )

    filter_results = []

    filter_df_cache = {}

    for timeframe_minutes in filter_timeframes:

        if timeframe_minutes not in filter_df_cache:

            filter_df_cache[
                timeframe_minutes
            ] = history_upbit(
                market,
                timeframe_minutes,
                required=ROC_HISTORY_REQUIRED
            )

        df_filter = filter_df_cache[
            timeframe_minutes
        ]

        if (
            df_filter is None
            or df_filter.empty
        ):

            return None

        timeframe_name = (
            format_timeframe(
                timeframe_minutes
            )
        )

        raw_filter = (
            roc_filter_analysis(
                df_filter,
                all_periods
            )
        )

        display_filter = (
            roc_filter_display(
                raw_filter,
                timeframe_name
            )
        )

        filter_results.append(
            display_filter
        )

    # =====================================================
    # 신호 기준 native 데이터
    # =====================================================

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

    # =====================================================
    # 현재 진행 중인 신호 시간봉
    # =====================================================

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
    # SIGNAL ROC
    # =====================================================

    signal_timeframe_name = (
        format_timeframe(
            SIGNAL_TIMEFRAME
        )
    )

    r_signal_raw = roc_filter_analysis(
        df_current,
        all_periods
    )

    r_signal = roc_filter_display(
        r_signal_raw,
        signal_timeframe_name
    )

    signal_roc_current = (
        r_signal
        .get(
            "roc_values",
            {}
        )
        .get(
            SIGNAL_ROC_PERIOD
        )
    )

    signal_roc_previous = (
        r_signal
        .get(
            "previous_values",
            {}
        )
        .get(
            SIGNAL_ROC_PERIOD
        )
    )

    r = {

        "signal_roc":
            signal_roc_current,

        "signal_roc_previous":
            signal_roc_previous,

        "signal_roc_cross":
            False,

        "signal_roc_pullback":
            False

    }

    r["signal_roc_cross"] = (
        roc_signal_zero_cross(r)
    )

    r["signal_roc_pullback"] = (
        roc_signal_pullback_condition(r)
    )

    # =====================================================
    # ROC COUNT 필터
    # =====================================================

    filter_pass = (
        all_active_roc_filters_pass(
            filter_results
        )
    )

    # =====================================================
    # 현재 진행 신호 캔들
    # =====================================================

    progress_candle_time = (
        get_current_candle_start(
            SIGNAL_TIMEFRAME
        )
    )

    # =====================================================
    # 과거 가장 최근 이벤트
    # =====================================================

    historical_start_candle = None
    historical_pullback_start_candle = None

    if (
        market not in roc_signal_state
        and market not in roc_pullback_state
    ):

        latest_event_type, latest_event_candle = (
            find_latest_signal_event(
                df_signal
            )
        )

        if latest_event_type == "signal":

            historical_start_candle = (
                latest_event_candle
            )

        elif latest_event_type == "pullback":

            historical_pullback_start_candle = (
                latest_event_candle
            )

    # =====================================================
    # 신호 / 눌림 / COUNT
    # =====================================================

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

    # =====================================================
    # 일봉
    # =====================================================

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

    # =====================================================
    # 상승신호 자격
    # =====================================================

    breakout_qualified = (
        state["signal_active"]
        and
        filter_pass
    )

    return {

        "roc_filters":
            filter_results,

        "roc":
            r,

        "changes":
            changes,

        "filter_pass":
            filter_pass,

        "daily_pass":
            daily_pass,

        "breakout_qualified":
            breakout_qualified,

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

        "signal_roc_cross":
            state[
                "signal_roc_cross"
            ],

        "signal_roc_pullback":
            state[
                "signal_roc_pullback"
            ],

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

        "roc_filters":
            a.get(
                "roc_filters",
                []
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

        "signal_roc_cross":
            bool(
                a.get(
                    "signal_roc_cross",
                    False
                )
            ),

        "signal_roc_pullback":
            bool(
                a.get(
                    "signal_roc_pullback",
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
# ROC 필터 HTML
#
# 실제 지정한 시간봉만 표시
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
            count = 0

        else:

            try:

                value = float(
                    value
                )

                if value > 0:

                    icon = "🟢"

                    count = int(
                        positive_counts.get(
                            period,
                            0
                        )
                    )

                elif value < 0:

                    icon = "🔴"

                    count = int(
                        negative_counts.get(
                            period,
                            0
                        )
                    )

                else:

                    icon = "⚪"
                    count = 1

            except Exception:

                icon = "⚪"
                count = 0

        setting = settings[
            period
        ].get(
            timeframe,
            "N"
        )

        setting_cls = (
            "roc-active"
            if setting == "Y"
            else "roc-disabled"
        )

        items.append(
            f"""
            <div class="btc-roc-item {setting_cls}">

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

            {timeframe} ROC

        </div>

        <div class="btc-roc-grid">

            {"".join(items)}

        </div>

    </div>
    """


def filter_html(
    filters
):

    sections = []

    for r in filters:

        if not r:
            continue

        timeframe = r.get(
            "timeframe",
            "-"
        )

        sections.append(
            roc_filter_html(
                r,
                timeframe
            )
        )

    if not sections:
        return ""

    return f"""
    <div class="filter-detail">

        {"".join(sections)}

    </div>
    """


# =========================================================
# 신호 HTML
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

    signal_count = int(
        row.get(
            "signal_count",
            0
        )
    )

    pullback_active = row.get(
        "pullback_active",
        False
    )

    pullback_count = int(
        row.get(
            "pullback_count",
            0
        )
    )

    items = []

    if (
        signal_active
        and count_display_allowed(
            signal_count
        )
    ):

        items.append(
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

    if (
        pullback_active
        and count_display_allowed(
            pullback_count
        )
    ):

        items.append(
            f"""
            <span class="signal-item pullback-item">

                <span class="signal-pullback">
                    📉
                </span>

                <span class="pullback-count">
                    ({pullback_count})
                </span>

            </span>
            """
        )

    if not items:

        return (
            '<span class="muted">'
            '-'
            '</span>'
        )

    return f"""
    <div class="signal-wrap">

        {"".join(items)}

    </div>
    """


# =========================================================
# TOP COUNT
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

    signal_count = int(
        row.get(
            "signal_count",
            0
        )
    )

    pullback_active = row.get(
        "pullback_active",
        False
    )

    pullback_count = int(
        row.get(
            "pullback_count",
            0
        )
    )

    items = []

    if (
        signal_active
        and count_display_allowed(
            signal_count
        )
    ):

        items.append(
            f"""
            <span class="top-signal-count">
                🚀 ({signal_count})
            </span>
            """
        )

    if (
        pullback_active
        and count_display_allowed(
            pullback_count
        )
    ):

        items.append(
            f"""
            <span class="top-pullback-count">
                📉 ({pullback_count})
            </span>
            """
        )

    if not items:

        return (
            '<span class="muted">'
            '-'
            '</span>'
        )

    return f"""
    <div class="top-count-wrap">

        {"".join(items)}

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

        signal_count = int(
            x.get(
                "signal_count",
                0
            )
        )

        filter_pass = x.get(
            "filter_pass",
            False
        )

        if (
            filter_pass
            and x.get(
                "daily_pass",
                False
            )
            and signal_active
            and count_flash_allowed(
                signal_count
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
                "roc_filters",
                []
            )
        )

        signal_content = top_signal_count_html(
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
                    {x.get("rank", "-")}
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

                    {signal_content}

                </td>

            </tr>

            <tr class="
                roc-subrow
                {cls}
            ">

                <td colspan="6">

                    {filter_content}

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
                        신호
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
# 상승 / 눌림 신호
# =========================================================

def focus_section(
    data
):

    rows = [

        x

        for x in data

        if (

            x.get(
                "filter_pass",
                False
            )

            and

            (
                (
                    x.get(
                        "signal_active",
                        False
                    )

                    and

                    count_display_allowed(
                        int(
                            x.get(
                                "signal_count",
                                0
                            )
                        )
                    )
                )

                or

                (
                    x.get(
                        "pullback_active",
                        False
                    )

                    and

                    count_display_allowed(
                        int(
                            x.get(
                                "pullback_count",
                                0
                            )
                        )
                    )
                )
            )

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
            🚀 상승 / 📉 눌림 신호
        </span>

        <span class="section-title-sub">

            ROC20 / ROC50 / ROC200 COUNT
            ≥ {ROC_FILTER_COUNT_MIN}

            ·

            ROC{SIGNAL_ROC_PERIOD}
            {format_timeframe(SIGNAL_TIMEFRAME)}
            0선 기준

            ·

            당일 변동 0% 이상

            ·

            COUNT {count_display_text()} 표시

            ·

            반짝임 {count_flash_text()}

            ·

            {kst()} KST

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

            ·

            ROC{SIGNAL_ROC_PERIOD}
            {format_timeframe(SIGNAL_TIMEFRAME)}
            🚀 상승 / 📉 눌림

            ·

            ROC20 / ROC50 / ROC200 COUNT
            ≥ {ROC_FILTER_COUNT_MIN}

            ·

            {get_filter_setting_text()} ROC
            업비트 원본

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

            {format_timeframe(SIGNAL_TIMEFRAME)}
            ROC

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

                ROC20 / ROC50 / ROC200 COUNT
                ≥ {ROC_FILTER_COUNT_MIN}

                ·

                신호 ROC{SIGNAL_ROC_PERIOD}

                ·

                기준
                {format_timeframe(SIGNAL_TIMEFRAME)}
                native

                ·

                4H =
                업비트 원본 240분봉

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


/* =====================================================
   BTC 시황
   기존 정렬 유지
   ===================================================== */

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
color:#e5e9ed;
font-size:6px;
font-weight:800;
white-space:nowrap;
}

.btc-change{
font-size:8px;
font-weight:900;
white-space:nowrap;
}

.btc-top > span:last-child{
margin-left:2px;
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
grid-template-columns:repeat(4,1fr);
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
text-align:center;
}

.btc-roc-period{
color:#737c86;
font-size:5.5px;
font-weight:900;
line-height:7px;
text-align:center;
}

.btc-roc-icon{
font-size:9px;
line-height:10px;
text-align:center;
}

.btc-roc-count{
color:#cdd3d8;
font-size:6px;
font-weight:900;
line-height:8px;
text-align:center;
}

.roc-active{
opacity:1;
}

.roc-disabled{
opacity:.38;
}


/* =====================================================
   TABLE
   ===================================================== */

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
vertical-align:middle;
}

.th-sub{
display:block;
font-size:4px;
font-weight:700;
color:#59616a;
line-height:6px;
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


/* =====================================================
   6열 비율
   # / 코인 / 거래대금 / 가격 / 변동률 / 신호
   ===================================================== */

th:nth-child(1),
td:nth-child(1){
width:7%;
}

th:nth-child(2),
td:nth-child(2){
width:20%;
}

th:nth-child(3),
td:nth-child(3){
width:18%;
}

th:nth-child(4),
td:nth-child(4){
width:25%;
}

th:nth-child(5),
td:nth-child(5){
width:15%;
}

th:nth-child(6),
td:nth-child(6){
width:15%;
}


/* =====================================================
   순위
   ===================================================== */

.rank-cell{
color:#7d858e;
font-size:5.5px;
font-weight:800;
text-align:center!important;
}


/* =====================================================
   코인
   ===================================================== */

.coin-cell{
text-align:center!important;
padding-left:2px!important;
padding-right:2px!important;
}

.coin-name{
display:block;
color:#e0e5e9;
font-size:6.5px;
line-height:9px;
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
padding-left:2px!important;
padding-right:2px!important;
}

.volume-value{
display:block;
color:#737c86;
font-size:5.4px;
line-height:9px;
font-weight:700;
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
padding-left:2px!important;
padding-right:2px!important;
}

.price-value{
display:block;
color:#e5e9ed;
font-size:5.9px;
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
font-size:5.8px;
font-weight:900;
}


/* =====================================================
   신호
   ===================================================== */

.signal-cell{
text-align:center!important;
padding-left:2px!important;
padding-right:2px!important;
}


/* =====================================================
   ROC 상세
   ===================================================== */

.filter-detail{
display:flex;
flex-direction:column;
gap:3px;
width:100%;
padding:3px 4px 4px;
}

.filter-detail .btc-roc-section{
margin-top:0;
padding-top:0;
border-top:0;
}

.filter-detail .btc-roc-title{
margin-bottom:2px;
font-size:5px;
text-align:center;
}

.filter-detail .btc-roc-grid{
gap:2px;
}

.filter-detail .btc-roc-item{
min-height:24px;
}

.filter-detail .btc-roc-period{
font-size:5px;
text-align:center;
}

.filter-detail .btc-roc-icon{
font-size:8px;
text-align:center;
}

.filter-detail .btc-roc-count{
font-size:5.8px;
text-align:center;
}

.roc-subrow{
background:#0f1318!important;
}

.roc-subrow td{
height:auto!important;
padding:0!important;
border-bottom:1px solid #22282e;
}


/* =====================================================
   TOP COUNT
   ===================================================== */

.top-count-wrap{
display:flex;
align-items:center;
justify-content:center;
gap:3px;
white-space:nowrap;
}

.top-signal-count{
color:#62b58a;
font-size:7px;
font-weight:900;
}

.top-pullback-count{
color:#c97878;
font-size:7px;
font-weight:900;
}


/* =====================================================
   SIGNAL
   ===================================================== */

.signal-wrap{
display:flex;
align-items:center;
justify-content:center;
gap:3px;
min-height:20px;
white-space:nowrap;
}

.signal-item{
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

.signal-pullback{
font-size:9px;
}

.pullback-count{
color:#c97878;
font-size:7px;
font-weight:900;
}


/* =====================================================
   FLASH
   ===================================================== */

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


/* =====================================================
   COLORS
   ===================================================== */

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
text-align:center!important;
}


/* =====================================================
   MOBILE
   ===================================================== */

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


/* BTC 시황 기존 정렬 유지 */

.btc-top{
gap:4px;
}

.btc-name{
font-size:6px;
}

.btc-price{
font-size:5.5px;
}

.btc-change{
font-size:7px;
}

th{
height:16px;
font-size:4.5px;
text-align:center;
}

.th-sub{
font-size:3.7px;
line-height:5px;
}

td{
height:27px;
}


/* =====================================================
   모바일 6열
   ===================================================== */

th:nth-child(1),
td:nth-child(1){
width:7%;
}

th:nth-child(2),
td:nth-child(2){
width:20%;
}

th:nth-child(3),
td:nth-child(3){
width:18%;
}

th:nth-child(4),
td:nth-child(4){
width:25%;
}

th:nth-child(5),
td:nth-child(5){
width:15%;
}

th:nth-child(6),
td:nth-child(6){
width:15%;
}


/* 코인 */

.coin-cell{
padding-left:2px!important;
padding-right:2px!important;
text-align:center!important;
}

.coin-name{
font-size:6px;
text-align:center;
}


/* 거래대금 */

.volume-cell{
padding-left:1px!important;
padding-right:1px!important;
text-align:center!important;
}

.volume-value{
font-size:4.8px;
text-align:center;
}


/* 가격 */

.price-cell{
padding-left:1px!important;
padding-right:1px!important;
text-align:center!important;
}

.price-value{
font-size:5.2px;
text-align:center;
}


/* 변동률 */

.change-cell{
font-size:5.1px;
text-align:center!important;
}


/* 신호 */

.signal-cell{
padding-left:1px!important;
padding-right:1px!important;
text-align:center!important;
}

.top-count-wrap{
gap:1px;
justify-content:center;
}

.top-signal-count,
.top-pullback-count{
font-size:6.3px;
}


/* ROC */

.filter-detail{
gap:2px;
padding:2px 3px 3px;
}

.filter-detail .btc-roc-grid{
gap:1px;
}

.filter-detail .btc-roc-item{
min-height:22px;
}

.filter-detail .btc-roc-period{
font-size:4.7px;
text-align:center;
}

.filter-detail .btc-roc-icon{
font-size:7px;
text-align:center;
}

.filter-detail .btc-roc-count{
font-size:5.2px;
text-align:center;
}


/* Signal */

.signal-wrap{
gap:2px;
justify-content:center;
}

.signal-rocket,
.signal-pullback{
font-size:8px;
}

.signal-count,
.pullback-count{
font-size:6.5px;
}


/* BTC ROC */

.btc-roc-period{
font-size:5px;
text-align:center;
}

.btc-roc-icon{
font-size:8px;
text-align:center;
}

.btc-roc-count{
font-size:5.8px;
text-align:center;
}

.btc-roc-item{
min-height:23px;
}

}


/* =====================================================
   REDUCED MOTION
   ===================================================== */

@media(prefers-reduced-motion:reduce){

tr.signal-flash-one td{
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
            ROC{SIGNAL_ROC_PERIOD}
            {format_timeframe(SIGNAL_TIMEFRAME)}
            SIGNAL
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
        f"ROC{SIGNAL_ROC_PERIOD} "
        f"{format_timeframe(SIGNAL_TIMEFRAME)} "
        "SIGNAL SYSTEM START"
    )

    log.info(
        f"상승/눌림 신호 ROC = "
        f"ROC{SIGNAL_ROC_PERIOD} "
        f"{format_timeframe(SIGNAL_TIMEFRAME)}"
    )

    log.info(
        f"ROC 필터 COUNT 최소값 = "
        f"{ROC_FILTER_COUNT_MIN}"
    )

    log.info(
        "★ ROC5 / ROC20 / ROC50 / ROC200 사용"
    )

    log.info(
        "★ ROC20/50/200은 0선 이상 연속 COUNT 사용"
    )

    log.info(
        f"★ ROC20/50/200 COUNT >= "
        f"{ROC_FILTER_COUNT_MIN}"
    )

    log.info(
        f"★ 활성 ROC 필터 = "
        f"{get_filter_setting_text()}"
    )

    log.info(
        f"★ FILTER1 = "
        f"{USE_FILTER1} / "
        f"{format_timeframe(FILTER1_TIMEFRAME)}"
    )

    log.info(
        f"★ FILTER2 = "
        f"{USE_FILTER2} / "
        f"{format_timeframe(FILTER2_TIMEFRAME)}"
    )

    log.info(
        "★ 눌림은 화면에 표시"
    )

    log.info(
        "★ ROC5 0선 상향 돌파 = 🚀"
    )

    log.info(
        "★ ROC5 0선 하향 돌파 = 📉"
    )

    log.info(
        "★ 상승/눌림 COUNT 화면 표시"
    )

    log.info(
        "★ ROC5 / ROC20 / ROC50 / ROC200 COUNT "
        "동일 방식 계산"
    )

    log.info(
        f"★ ROC HISTORY REQUIRED = "
        f"{ROC_HISTORY_REQUIRED}개"
    )

    log.info(
        "★ ROC200 COUNT 2 고정 방지용 "
        "과거 데이터 확장"
    )

    log.info(
        "★ 선택한 ROC 필터 시간봉은 "
        "업비트 원본 데이터 사용"
    )

    log.info(
        "★ 1H → 4H 캔들 합성하지 않음"
    )

    log.info(
        "★ 1D = 업비트 원본 일봉"
    )

    log.info(
        "★ 일봉 등락 = "
        "상승/눌림 리스트의 기본 표시 기준"
    )

    log.info(
        f"★ ROC200 현재값 + 과거 COUNT 계산을 위해 "
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
        "★ 1H 진행봉 기준 = 매 정시"
    )

    log.info(
        "★ 1D 진행봉 기준 = 09:00 KST"
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
        "★ 상승신호만 반짝임"
    )

    log.info(
        "★ 눌림은 표시하지만 반짝이지 않음"
    )

    log.info(
        "★ 과거 이벤트는 가장 최근 이벤트 하나만 사용"
    )

    log.info(
        f"ACTIVE FILTER = "
        f"{get_filter_setting_text()}"
    )

    log.info(
        "★ TOP 테이블 = "
        "순위 / 코인 / 거래대금 / 가격 / 변동률 / 신호"
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
