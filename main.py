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
# ROC / SIGNAL 기준 시간봉
#
# 15  = 15분봉
# 60  = 1시간봉
# 240 = 4시간봉
#
# 이 숫자 하나만 변경하면
# ROC와 Signal 전체 시간봉이 같이 변경됨
# =========================================================

SIGNAL_TIMEFRAME = 240


# =========================================================
# ROC 전체 설정
# =========================================================

ROC_SETTINGS = {
    10: "Y",
    20: "Y",
    50: "Y",
    200: "Y"
}

ROC_PERIODS = [
    10,
    20,
    50,
    200
]


# =========================================================
# Signal 1
#
# ROC10
# ROC20
#
# 두 선 모두 0선 위
#
# ROC10 > 0
# AND
# ROC20 > 0
#
# Signal COUNT
# 조건 시작 봉 = 0
# 다음 봉 = 1
#
# 화면 표시
# COUNT 0~1
# =========================================================

SIGNAL1_ROC10_PERIOD = 10
SIGNAL1_ROC20_PERIOD = 20

SIGNAL1_DISPLAY_COUNT_MIN = 1
SIGNAL1_DISPLAY_COUNT_MAX = 5


# =========================================================
# Signal 1 ROC COUNT
#
# ROC10 양수 COUNT 1~10
# ROC20 양수 COUNT 1~10
# =========================================================

SIGNAL1_ROC10_COUNT_MIN = 1
SIGNAL1_ROC10_COUNT_MAX = 10

SIGNAL1_ROC20_COUNT_MIN = 1
SIGNAL1_ROC20_COUNT_MAX = 10


# =========================================================
# ROC 데이터 최소 필요 개수
# =========================================================

ROC_HISTORY_EXTRA = 120

ROC_HISTORY_REQUIRED = (
    max(ROC_PERIODS)
    + SIGNAL1_DISPLAY_COUNT_MAX
    + ROC_HISTORY_EXTRA
    + 5
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
# ROC Y/N
# =========================================================

def roc_is_enabled(period):

    try:
        period = int(period)
    except Exception:
        return False

    return ROC_SETTINGS.get(
        period,
        "N"
    ) == "Y"


def enabled_roc_periods():

    return [
        period
        for period in ROC_PERIODS
        if roc_is_enabled(period)
    ]


def roc_setting_text():

    return " · ".join(
        f"ROC{period} {ROC_SETTINGS.get(period, 'N')}"
        for period in ROC_PERIODS
    )


# =========================================================
# Signal 표시 COUNT
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
# 현재 진행 중 캔들 시작시간
# =========================================================

def get_current_candle_start(minutes):

    minutes = int(minutes)

    now = datetime.now(KST)

    total_minutes = (
        now.hour * 60
        + now.minute
    )

    block = (
        total_minutes // minutes
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
# Signal 1 트리거
#
# ROC10 > 0
# AND
# ROC20 > 0
# =========================================================

def signal1_trigger_pass(df):

    if (
        df is None
        or df.empty
    ):
        return False

    if not roc_is_enabled(
        SIGNAL1_ROC10_PERIOD
    ):
        return False

    if not roc_is_enabled(
        SIGNAL1_ROC20_PERIOD
    ):
        return False

    series10 = roc(
        df,
        SIGNAL1_ROC10_PERIOD
    )

    series20 = roc(
        df,
        SIGNAL1_ROC20_PERIOD
    )

    if (
        series10 is None
        or series20 is None
        or series10.empty
        or series20.empty
    ):
        return False

    try:

        roc10 = float(
            series10.iloc[-1]
        )

        roc20 = float(
            series20.iloc[-1]
        )

    except Exception:

        return False

    return (
        roc10 > 0
        and
        roc20 > 0
    )


# =========================================================
# Signal 1 COUNT 필터
#
# ROC10 COUNT 1~10
# ROC20 COUNT 1~10
# =========================================================

def signal1_filter_pass(df):

    if not roc_is_enabled(
        SIGNAL1_ROC10_PERIOD
    ):
        return False

    if not roc_is_enabled(
        SIGNAL1_ROC20_PERIOD
    ):
        return False

    count_10 = roc_positive_count(
        df,
        SIGNAL1_ROC10_PERIOD
    )

    count_20 = roc_positive_count(
        df,
        SIGNAL1_ROC20_PERIOD
    )

    return (
        SIGNAL1_ROC10_COUNT_MIN
        <= count_10
        <= SIGNAL1_ROC10_COUNT_MAX

        and

        SIGNAL1_ROC20_COUNT_MIN
        <= count_20
        <= SIGNAL1_ROC20_COUNT_MAX
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
                data = ticker_response.json()
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
                    "market": market,
                    "volume_24h": volume,
                    "current_price": price
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
        "market": market,
        "count": min(
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

        df = pd.DataFrame(data)

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

    for _ in range(
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

    while len(df) < required:

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
            df_old is None
            or df_old.empty
        ):
            break

        old_len = len(df)

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

        if len(df) <= old_len:
            break

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
                f"현재 "
                f"{format_timeframe(timeframe)} "
                f"진행봉 없음 | "
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
# ROC 전체 분석
# =========================================================

def roc_filter_analysis(df):

    result = {
        "direction": "none",
        "passed": False,
        "roc_values": {},
        "previous_values": {},
        "zero_crosses": {},
        "positive_counts": {},
        "negative_counts": {},
        "enabled_periods": enabled_roc_periods(),
        "all_periods": ROC_PERIODS.copy()
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

    for period in ROC_PERIODS:

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
            and previous_value < 0
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

    result.update({
        "roc_values": values,
        "previous_values": previous_values,
        "zero_crosses": zero_crosses,
        "positive_counts": positive_counts,
        "negative_counts": negative_counts
    })

    return result


# =========================================================
# 과거 유효 Signal 이벤트 찾기
#
# ROC10 > 0
# AND
# ROC20 > 0
#
# 동시에 조건을 만족한 가장 최근 봉
# =========================================================

def find_latest_valid_signal_event(
    df_signal,
    filter_function
):

    if (
        df_signal is None
        or df_signal.empty
    ):
        return None

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
        ].reset_index(drop=True)

        if temp.empty:
            return None

        for i in range(
            len(temp) - 1,
            -1,
            -1
        ):

            event_df = (
                temp
                .iloc[:i + 1]
                .copy()
            )

            if filter_function(
                event_df
            ):

                return normalize_datetime(
                    temp[
                        "datetime"
                    ].iloc[i]
                )

        return None

    except Exception as e:

        log.warning(
            f"유효 Signal 이벤트 검색 오류: {e}"
        )

        return None


# =========================================================
# Signal 1 상태 업데이트
#
# ROC10 > 0
# AND ROC20 > 0
#
# 두 조건이 동시에 성립한 시점부터
# COUNT 계산
# =========================================================

def update_signal1(
    market,
    trigger_pass_now,
    progress_candle_time,
    historical_start_candle=None
):

    market_key = str(market)

    progress_candle_time = (
        normalize_datetime(
            progress_candle_time
        )
    )

    signal_state = (
        roc_signal1_state.get(
            market_key
        )
    )


    # =====================================================
    # 현재 조건 통과
    # =====================================================

    if trigger_pass_now:

        if signal_state is None:

            start_candle = None

            if historical_start_candle is not None:

                start_candle = (
                    normalize_datetime(
                        historical_start_candle
                    )
                )

            if start_candle is None:

                start_candle = (
                    progress_candle_time
                )

            if start_candle is not None:

                distance = candle_distance(
                    start_candle,
                    progress_candle_time,
                    SIGNAL_TIMEFRAME
                )

                roc_signal1_state[
                    market_key
                ] = {

                    "active": True,

                    "start_candle":
                        start_candle,

                    "count":
                        distance,

                    "last_candle":
                        progress_candle_time
                }

                signal_state = (
                    roc_signal1_state[
                        market_key
                    ]
                )

                roc_signal1_failed_candle.pop(
                    market_key,
                    None
                )

                if (
                    start_candle
                    == progress_candle_time
                ):

                    log.info(
                        f"[Signal 1 START] "
                        f"{market_key} | "
                        f"ROC10 > 0 | "
                        f"ROC20 > 0 | "
                        f"COUNT=0"
                    )

                else:

                    log.info(
                        f"[Signal 1 RESTORE] "
                        f"{market_key} | "
                        f"시작={start_candle} | "
                        f"COUNT={distance}"
                    )


        signal_state = (
            roc_signal1_state.get(
                market_key
            )
        )

        if signal_state is not None:

            start_candle = (
                signal_state.get(
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
                    SIGNAL_TIMEFRAME
                )

                signal_state["count"] = (
                    distance
                )

                signal_state[
                    "last_candle"
                ] = progress_candle_time


    # =====================================================
    # 현재 조건 불통과
    # =====================================================

    else:

        if signal_state is not None:

            old_count = int(
                signal_state.get(
                    "count",
                    0
                )
            )

            if progress_candle_time is not None:

                roc_signal1_failed_candle[
                    market_key
                ] = progress_candle_time

            log.info(
                f"[Signal 1 END] "
                f"{market_key} | "
                f"COUNT={old_count} | "
                f"ROC10 또는 ROC20이 0 이하"
            )

            roc_signal1_state.pop(
                market_key,
                None
            )

            signal_state = None


    # =====================================================
    # 최종 상태
    # =====================================================

    signal_state = (
        roc_signal1_state.get(
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

        "trigger_pass":
            bool(
                trigger_pass_now
            )
    }


# =========================================================
# 일봉 변동률
# =========================================================

def daily_change_upbit(market):

    response = retry(
        requests.get,
        "https://api.upbit.com/v1/candles/days",
        params={
            "market": market,
            "count": 2
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
            (
                current
                - previous
            )
            / previous
            * 100
        )

    except Exception:
        return None


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
    # 현재 ROC
    # =====================================================

    r_signal = roc_filter_analysis(
        df_current
    )

    roc_values = r_signal.get(
        "roc_values",
        {}
    )

    positive_counts = r_signal.get(
        "positive_counts",
        {}
    )


    # =====================================================
    # Signal 1 현재 ROC
    # =====================================================

    signal1_roc10_current = (
        roc_values.get(
            SIGNAL1_ROC10_PERIOD
        )
    )

    signal1_roc20_current = (
        roc_values.get(
            SIGNAL1_ROC20_PERIOD
        )
    )


    # =====================================================
    # Signal 1
    #
    # ROC10 > 0
    # AND
    # ROC20 > 0
    # =====================================================

    signal1_trigger_now = (
        signal1_trigger_pass(
            df_current
        )
    )


    # =====================================================
    # Signal 1 COUNT 필터
    #
    # ROC10 COUNT 1~10
    # ROC20 COUNT 1~10
    # =====================================================

    signal1_filter_pass_now = (
        signal1_filter_pass(
            df_current
        )
    )


    # =====================================================
    # 과거 Signal 시작점
    # =====================================================

    historical_start_candle1 = None

    if market not in roc_signal1_state:

        if (
            roc_is_enabled(
                SIGNAL1_ROC10_PERIOD
            )
            and
            roc_is_enabled(
                SIGNAL1_ROC20_PERIOD
            )
        ):

            historical_start_candle1 = (
                find_latest_valid_signal_event(
                    df_signal,
                    signal1_trigger_pass
                )
            )


    # =====================================================
    # 현재 진행 시간봉
    # =====================================================

    progress_candle_time = (
        get_current_candle_start(
            SIGNAL_TIMEFRAME
        )
    )


    # =====================================================
    # Signal 1 상태
    # =====================================================

    state1 = update_signal1(

        market=market,

        trigger_pass_now=
            signal1_trigger_now,

        progress_candle_time=
            progress_candle_time,

        historical_start_candle=
            historical_start_candle1
    )


    # =====================================================
    # Signal COUNT
    # =====================================================

    signal1_count = int(
        state1[
            "signal_count"
        ]
    )


    # =====================================================
    # ROC COUNT
    # =====================================================

    roc10_count = int(
        positive_counts.get(
            10,
            0
        )
    )

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
    # 일봉
    #
    # 당일 변동 >= 0%
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
    # Signal 표시 COUNT
    # =====================================================

    signal1_display_count_pass = (
        signal1_display_allowed(
            signal1_count
        )
    )


    # =====================================================
    # Signal 1 최종 조건
    #
    # 1. ROC10 > 0
    # 2. ROC20 > 0
    # 3. ROC10 COUNT 1~10
    # 4. ROC20 COUNT 1~10
    # 5. Signal COUNT 1~5
    # 6. 당일 변동률 >= 0%
    #
    # BTC 필터 없음
    # =====================================================

    signal1_qualified = (

        state1[
            "signal_active"
        ]

        and

        signal1_display_count_pass

        and

        signal1_filter_pass_now

        and

        daily_pass

        and

        roc_is_enabled(
            SIGNAL1_ROC10_PERIOD
        )

        and

        roc_is_enabled(
            SIGNAL1_ROC20_PERIOD
        )
    )


    # =====================================================
    # 결과
    # =====================================================

    return {

        "roc":
            r_signal,

        "changes":
            change_value,

        "daily_pass":
            daily_pass,


        # =================================================
        # Signal 1
        # =================================================

        "signal1_active":
            state1[
                "signal_active"
            ],

        "signal1_count":
            signal1_count,

        "signal1_roc10":
            signal1_roc10_current,

        "signal1_roc20":
            signal1_roc20_current,

        "signal1_trigger":
            signal1_trigger_now,

        "signal1_filter_pass":
            signal1_filter_pass_now,

        "signal1_count_pass":
            signal1_display_count_pass,

        "signal1_roc10_count":
            roc10_count,

        "signal1_roc20_count":
            roc20_count,

        "signal1_roc200_count":
            roc200_count,

        "signal1_qualified":
            signal1_qualified,

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


        # Signal 1

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

        "signal1_roc10":
            a.get(
                "signal1_roc10"
            ),

        "signal1_roc20":
            a.get(
                "signal1_roc20"
            ),

        "signal1_trigger":
            bool(
                a.get(
                    "signal1_trigger",
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

        "signal1_roc10_count":
            int(
                a.get(
                    "signal1_roc10_count",
                    0
                )
            ),

        "signal1_roc20_count":
            int(
                a.get(
                    "signal1_roc20_count",
                    0
                )
            ),

        "signal1_roc200_count":
            int(
                a.get(
                    "signal1_roc200_count",
                    0
                )
            ),

        "signal1_qualified":
            bool(
                a.get(
                    "signal1_qualified",
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

    if not top_markets:

        latest_upbit_data = []

        latest_upbit_update_time = (
            kst()
        )

        return


    # =====================================================
    # TOP 코인 분석
    # =====================================================

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
# OKX
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

def format_market_price(price):

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
# Signal 표시
# =========================================================

def signal_item_html(
    active,
    count,
    qualified
):

    if not qualified:
        return "-"

    return """
    <span class="signal-item signal-1">
        <span class="signal-rocket">
            🚀
        </span>
    </span>
    """


def signal_html(row):

    if not row:
        return "-"

    return signal_item_html(
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


def top_signal1_html(row):

    if not row:
        return "-"

    return signal_item_html(
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

    for period in ROC_PERIODS:

        enabled = (
            roc_is_enabled(
                period
            )
        )

        value = values.get(
            period
        )

        if not enabled:

            icon = "⚪"
            count_text = "N"

        elif value is None:

            icon = "⚪"
            count_text = "0"

        else:

            try:

                value = float(value)

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
            {format_timeframe(SIGNAL_TIMEFRAME)} 기준
        </div>

    </div>
    """


def filter_html(r):

    return roc_filter_html(
        r,
        format_timeframe(
            SIGNAL_TIMEFRAME
        )
    )


# =========================================================
# ROW HTML
# =========================================================

def rows_html(data):

    out = []

    for x in data:

        cls_list = []

        signal1_qualified = x.get(
            "signal1_qualified",
            False
        )

        signal1_count = int(
            x.get(
                "signal1_count",
                0
            )
        )


        # =================================================
        # Signal 1 반짝임
        # =================================================

        if (
            signal1_qualified
            and
            signal1_display_allowed(
                signal1_count
            )
        ):

            cls_list.append(
                "signal-flash-one"
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

        price = format_market_price(
            x.get(
                "current_price"
            )
        )

        change = x.get(
            "change",
            "-"
        )

        coin_name = html.escape(
            str(
                x.get(
                    "name",
                    "-"
                )
            )
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

                </div>

                <div class="coin-roc-row">

                    {roc_content}

                </div>

            </div>
            """
        )

    return "".join(out)


# =========================================================
# 테이블
# =========================================================

def table_html(data):

    rows = rows_html(data)

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
# Signal 집중 영역
# =========================================================

def focus_section(data):

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

    signal1_table = table_html(
        signal1_rows
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

                    ROC10 + ROC20
                    {format_timeframe(SIGNAL_TIMEFRAME)}
                    0선 위

                    ·

                    ROC10 COUNT
                    {SIGNAL1_ROC10_COUNT_MIN}~
                    {SIGNAL1_ROC10_COUNT_MAX}

                    ·

                    ROC20 COUNT
                    {SIGNAL1_ROC20_COUNT_MIN}~
                    {SIGNAL1_ROC20_COUNT_MAX}

                    ·

                    Signal COUNT
                    {SIGNAL1_DISPLAY_COUNT_MIN}~
                    {SIGNAL1_DISPLAY_COUNT_MAX}

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

                    Signal 1 =
                    ROC10 + ROC20
                    0선 위

                    ·

                    ROC10 COUNT
                    {SIGNAL1_ROC10_COUNT_MIN}~
                    {SIGNAL1_ROC10_COUNT_MAX}

                    ·

                    ROC20 COUNT
                    {SIGNAL1_ROC20_COUNT_MIN}~
                    {SIGNAL1_ROC20_COUNT_MAX}

                    ·

                    ROC 설정:
                    {roc_setting_text()}

                    ·

                    기준:
                    {format_timeframe(SIGNAL_TIMEFRAME)}

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

def btc_roc_status_html(btc_row):

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

    for period in ROC_PERIODS:

        if not roc_is_enabled(
            period
        ):

            icon = "⚪"
            count_text = "N"

        else:

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
            {format_timeframe(SIGNAL_TIMEFRAME)} ROC
        </div>

        <div class="roc-grid btc-roc-grid">

            {"".join(items)}

        </div>

        <div class="roc-badge">
            Signal =
            ROC10 + ROC20
            0선 위
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

                    Signal 1 =
                    ROC10 + ROC20
                    {format_timeframe(SIGNAL_TIMEFRAME)}
                    0선 위

                    ·

                    ROC10 COUNT
                    {SIGNAL1_ROC10_COUNT_MIN}~
                    {SIGNAL1_ROC10_COUNT_MAX}

                    ·

                    ROC20 COUNT
                    {SIGNAL1_ROC20_COUNT_MIN}~
                    {SIGNAL1_ROC20_COUNT_MAX}

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

.card-list{
width:100%;
display:flex;
flex-direction:column;
gap:9px;
margin-top:8px;
}

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

.coin-main-row{
display:grid;

grid-template-columns:
    6%
    18%
    15%
    21%
    14%
    26%;

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

.coin-roc-row{
min-height:68px;
background:#0d1218;
border-top:1px solid #29323c;
}

.signal-wrap{
display:flex;
align-items:center;
justify-content:center;
width:100%;
height:100%;
white-space:nowrap;
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
background:#153126;
border:2px solid #24724e;
color:#78c99d;
box-shadow:
0 0 10px
rgba(72,190,130,.12);
}

.signal-rocket{
font-size:20px;
line-height:21px;
flex:none;
}

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

.coin-card.signal-flash-one .coin-main-row > div,
.coin-card.signal-flash-one .coin-roc-row{
animation:
signalFlashOne
1.35s
ease-in-out
infinite;
}

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

@media(max-width:600px){

body{
    padding:7px;
}

h1{
    margin:3px 3px 8px;
    font-size:13px;
    line-height:16px;
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
    margin-top:1px;
    font-size:5px;
    line-height:7px;
}

.section-time{
    margin-left:4px;
    font-size:5px;
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
        26%;
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

    grid-template-columns:
        6%
        18%
        15%
        21%
        14%
        26%;
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

@media(prefers-reduced-motion:reduce){

.coin-card.signal-flash-one .coin-main-row > div,
.coin-card.signal-flash-one .coin-roc-row{
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
            1
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

    valid_values = {
        "Y",
        "N"
    }

    for period in ROC_PERIODS:

        value = ROC_SETTINGS.get(
            period
        )

        if value not in valid_values:

            raise ValueError(
                f"ROC{period} 설정은 Y 또는 N이어야 합니다."
            )


    # =====================================================
    # Signal 1 ROC
    # =====================================================

    if not roc_is_enabled(
        SIGNAL1_ROC10_PERIOD
    ):

        log.warning(
            "Signal 1 ROC10이 N입니다. "
            "Signal 1은 발생하지 않습니다."
        )


    if not roc_is_enabled(
        SIGNAL1_ROC20_PERIOD
    ):

        log.warning(
            "Signal 1 ROC20이 N입니다. "
            "Signal 1은 발생하지 않습니다."
        )


    # =====================================================
    # ROC COUNT
    # =====================================================

    if (
        SIGNAL1_ROC10_COUNT_MIN < 1
        or
        SIGNAL1_ROC10_COUNT_MAX
        < SIGNAL1_ROC10_COUNT_MIN
    ):

        raise ValueError(
            "ROC10 COUNT 설정 오류"
        )


    if (
        SIGNAL1_ROC20_COUNT_MIN < 1
        or
        SIGNAL1_ROC20_COUNT_MAX
        < SIGNAL1_ROC20_COUNT_MIN
    ):

        raise ValueError(
            "ROC20 COUNT 설정 오류"
        )


    # =====================================================
    # 시간봉
    # =====================================================

    if SIGNAL_TIMEFRAME not in (
        1,
        3,
        5,
        15,
        30,
        60,
        120,
        240
    ):

        raise ValueError(
            "SIGNAL_TIMEFRAME이 "
            "Upbit 지원 분봉이 아닙니다."
        )


    # =====================================================
    # 표시 COUNT
    # =====================================================

    if (
        SIGNAL1_DISPLAY_COUNT_MIN < 0
        or
        SIGNAL1_DISPLAY_COUNT_MAX
        < SIGNAL1_DISPLAY_COUNT_MIN
    ):

        raise ValueError(
            "Signal 1 표시 COUNT 설정 오류"
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
        "ROC SIGNAL SYSTEM START"
    )

    log.info(
        f"기준 시간봉 = "
        f"{format_timeframe(SIGNAL_TIMEFRAME)}"
    )

    log.info(
        f"ROC 설정 = {roc_setting_text()}"
    )

    log.info(
        "----------------------------------------"
    )

    log.info(
        "Signal 1"
    )

    log.info(
        f"★ ROC10 "
        f"{format_timeframe(SIGNAL_TIMEFRAME)} "
        f"> 0"
    )

    log.info(
        f"★ ROC20 "
        f"{format_timeframe(SIGNAL_TIMEFRAME)} "
        f"> 0"
    )

    log.info(
        "★ ROC10 AND ROC20 두 선 모두 0선 위"
    )

    log.info(
        f"★ ROC10 COUNT "
        f"{SIGNAL1_ROC10_COUNT_MIN}~"
        f"{SIGNAL1_ROC10_COUNT_MAX}"
    )

    log.info(
        f"★ ROC20 COUNT "
        f"{SIGNAL1_ROC20_COUNT_MIN}~"
        f"{SIGNAL1_ROC20_COUNT_MAX}"
    )

    log.info(
        f"★ Signal COUNT "
        f"{SIGNAL1_DISPLAY_COUNT_MIN}~"
        f"{SIGNAL1_DISPLAY_COUNT_MAX}"
    )

    log.info(
        "★ 당일 변동률 >= 0%"
    )

    log.info(
        "★ BTC 시장 필터 없음"
    )

    log.info(
        "----------------------------------------"
    )

    log.info(
        "★ ROC10 또는 ROC20이 0 이하가 되면 "
        "Signal 1 종료"
    )

    log.info(
        f"★ ROC HISTORY REQUIRED = "
        f"{ROC_HISTORY_REQUIRED}"
    )

    log.info(
        f"★ TOP = {TOP_N}"
    )

    log.info(
        f"★ 기준 시간봉 = "
        f"{format_timeframe(SIGNAL_TIMEFRAME)}"
    )

    log.info(
        "★ 모든 ROC는 개별 Y/N 설정"
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
