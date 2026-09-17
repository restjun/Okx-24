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
# FastAPI
# =========================================================

app = FastAPI()


# =========================================================
# 로그
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

log = logging.getLogger(__name__)


# =========================================================
# 시간
# =========================================================

KST = ZoneInfo("Asia/Seoul")


def kst():
    return datetime.now(KST).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


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
# ROC 설정
#
# Y = 실제 1H 필터 사용
# N = 실제 필터 제외
#
# 단!
# N이어도 현재 ROC 상태는 화면에 표시
# =========================================================

USE_1H_ROC5 = "Y"
USE_1H_ROC10 = "Y"
USE_1H_ROC20 = "Y"
USE_1H_ROC50 = "N"
USE_1H_ROC200 = "N"

USE_4H_ROC5 = "Y"
USE_4H_ROC10 = "Y"
USE_4H_ROC20 = "Y"
USE_4H_ROC50 = "Y"
USE_4H_ROC200 = "Y"


# =========================================================
# ROC 시간봉
# =========================================================

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

latest_upbit_orderbook = {}

latest_usdt_krw_internal = None

request_lock = threading.Lock()
update_lock = threading.Lock()

last_request_time = 0


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


# =========================================================
# 전체 ROC 기간
#
# 화면 표시용
# Y/N 관계없이 항상 5개
# =========================================================

def get_all_roc_periods():

    return ROC_FILTER_PERIODS.copy()


# =========================================================
# 실제 필터에 사용하는 기간
#
# Y만 반환
# =========================================================

def get_enabled_periods(timeframe):

    settings = roc_settings()

    result = []

    for period in ROC_FILTER_PERIODS:

        if settings[period][timeframe] == "Y":

            result.append(period)

    return result


# =========================================================
# 화면 표시용 기간
#
# N이어도 무조건 표시
# =========================================================

def get_display_periods(timeframe):

    return ROC_FILTER_PERIODS.copy()


# =========================================================
# 활성 필터 텍스트
# =========================================================

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


# =========================================================
# 화면 표시 기간
# =========================================================

def get_display_filter_text(timeframe):

    return "/".join(
        str(x)
        for x in get_display_periods(
            timeframe
        )
    )


# =========================================================
# 필터 설정
# =========================================================

def get_filter_setting_text():

    return (
        f"1H 기준:{get_enabled_filter_text('1H')} "
        f"/ 4H 참고:{get_display_filter_text('4H')}"
    )


# =========================================================
# 화면 ROC 명칭
# =========================================================

def get_roc_text():

    return "ROC"


# =========================================================
# 시간봉
# =========================================================

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

    total_minutes = (
        now.hour * 60
        + now.minute
    )

    block = (
        total_minutes // minutes
    ) * minutes

    return now.replace(
        hour=block // 60,
        minute=block % 60,
        second=0,
        microsecond=0
    ).replace(
        tzinfo=None
    )


# =========================================================
# 설정 검증
# =========================================================

def validate_settings():

    if USE_UPBIT not in ("Y", "N"):

        raise ValueError(
            "USE_UPBIT은 Y/N만 가능합니다."
        )

    if USE_OKX not in ("Y", "N"):

        raise ValueError(
            "USE_OKX는 Y/N만 가능합니다."
        )

    settings = roc_settings()

    for period in ROC_FILTER_PERIODS:

        for timeframe in ("1H", "4H"):

            value = settings[
                period
            ][timeframe]

            if value not in ("Y", "N"):

                raise ValueError(
                    f"{timeframe} ROC{period} "
                    f"설정 오류: {value}"
                )


# =========================================================
# API 요청 제한
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
                REQUEST_INTERVAL - gap
            )

        last_request_time = (
            time.monotonic()
        )


# =========================================================
# requests 재시도
# =========================================================

def request_get(
    url,
    params=None,
    timeout=15
):

    for attempt in range(
        MAX_RETRIES
    ):

        try:

            wait_request()

            response = requests.get(
                url,
                params=params,
                timeout=timeout
            )

            if response.status_code == 200:

                return response

            if response.status_code == 429:

                wait = min(
                    RATE_LIMIT_WAIT
                    * (attempt + 1),
                    30
                )

                log.warning(
                    f"429 제한 → "
                    f"{wait}초 대기"
                )

                time.sleep(wait)

                continue

            if response.status_code >= 500:

                wait = min(
                    2 * (attempt + 1),
                    20
                )

                time.sleep(wait)

                continue

            log.warning(
                f"HTTP {response.status_code}: "
                f"{url}"
            )

            return None

        except Exception as e:

            log.warning(
                f"요청 오류 "
                f"{attempt + 1}/{MAX_RETRIES}: "
                f"{e}"
            )

            time.sleep(
                min(
                    attempt + 1,
                    10
                )
            )

    return None


# =========================================================
# 업비트 전체 KRW
# =========================================================

def get_upbit_markets():

    response = request_get(
        "https://api.upbit.com/v1/ticker/all",
        params={
            "quote_currencies":
                "KRW"
        }
    )

    if response is None:

        return []

    try:

        data = response.json()

        result = []

        for item in data:

            market = item.get(
                "market",
                ""
            )

            if not market.startswith(
                "KRW-"
            ):

                continue

            try:

                price = float(
                    item.get(
                        "trade_price",
                        0
                    )
                )

                volume = float(
                    item.get(
                        "acc_trade_price_24h",
                        0
                    )
                )

            except Exception:

                continue

            if (
                price <= 0
                or volume <= 0
            ):

                continue

            result.append({

                "market":
                    market,

                "current_price":
                    price,

                "volume_24h":
                    volume
            })

        return result

    except Exception as e:

        log.error(
            f"업비트 마켓 오류: {e}"
        )

        return []


# =========================================================
# 업비트 호가
# =========================================================

def get_upbit_orderbooks(
    markets
):

    if not markets:

        return {}

    result = {}

    chunk_size = 30

    for start in range(
        0,
        len(markets),
        chunk_size
    ):

        chunk = markets[
            start:
            start + chunk_size
        ]

        response = request_get(
            "https://api.upbit.com/v1/orderbook",
            params={
                "markets":
                    ",".join(chunk),

                "count":
                    ORDERBOOK_COUNT
            }
        )

        if response is None:

            continue

        try:

            data = response.json()

            if not isinstance(
                data,
                list
            ):

                continue

            for item in data:

                market = item.get(
                    "market"
                )

                if market:

                    result[
                        market
                    ] = item

        except Exception as e:

            log.error(
                f"호가 오류: {e}"
            )

    return result


# =========================================================
# 호가 분석
# =========================================================

def calculate_orderbook_amount(
    orderbook,
    current_price
):

    result = {

        "bid_amount":
            0.0,

        "ask_amount":
            0.0,

        "total_amount":
            0.0,

        "bid_ratio":
            0.0,

        "ask_ratio":
            0.0,

        "bid_count":
            0,

        "ask_count":
            0,

        "dominance":
            "balanced",

        "dominance_text":
            "균형"
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

    lower = (
        current_price
        * (1 - ORDERBOOK_RANGE)
    )

    upper = (
        current_price
        * (1 + ORDERBOOK_RANGE)
    )

    bid_amount = 0.0
    ask_amount = 0.0

    bid_count = 0
    ask_count = 0

    units = orderbook.get(
        "orderbook_units",
        []
    )

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
            lower
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
            <= upper
        ):

            ask_amount += (
                ask_price
                * ask_size
            )

            ask_count += 1

    total = (
        bid_amount
        + ask_amount
    )

    if total > 0:

        bid_ratio = (
            bid_amount
            / total
            * 100
        )

        ask_ratio = (
            ask_amount
            / total
            * 100
        )

    else:

        bid_ratio = 0
        ask_ratio = 0

    gap = (
        bid_ratio
        - ask_ratio
    )

    if gap >= ORDERBOOK_DOMINANCE_GAP:

        dominance = "bid"
        dominance_text = "매수 우세"

    elif gap <= -ORDERBOOK_DOMINANCE_GAP:

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
            total,

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

    response = request_get(
        f"https://api.upbit.com/v1/candles/minutes/{unit}",
        params=params
    )

    if response is None:

        return None

    try:

        data = response.json()

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
            df[
                "candle_acc_trade_price"
            ],
            errors="coerce"
        )

        df["datetime"] = pd.to_datetime(
            df[
                "candle_date_time_kst"
            ],
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
            f"캔들 변환 오류 "
            f"{market} {unit}: {e}"
        )

        return None


# =========================================================
# 업비트 과거 데이터
#
# 250개 확보
# ROC200 계산 가능
# =========================================================

def history_upbit(
    market,
    unit,
    required=250
):

    all_df = None
    to = None

    for _ in range(
        MAX_HISTORY_CHUNKS
    ):

        df = get_upbit_candle(
            market,
            unit,
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

        first_time = (
            all_df[
                "datetime"
            ].iloc[0]
        )

        to = first_time.strftime(
            "%Y-%m-%dT%H:%M:%S"
        )

    return all_df


# =========================================================
# 현재 ROC용 데이터
# =========================================================

def get_upbit_current_roc_data(
    market,
    current_price
):

    df = get_upbit_candle(
        market,
        ROC_TIMEFRAME,
        count=200,
        include_current=True
    )

    if (
        df is None
        or df.empty
    ):

        return None

    try:

        current_start = (
            get_current_candle_start(
                ROC_TIMEFRAME
            )
        )

        price = float(
            current_price
        )

        mask = (
            df["datetime"]
            == current_start
        )

        if mask.any():

            df.loc[
                mask,
                "c"
            ] = price

        else:

            row = df.iloc[-1].copy()

            row["datetime"] = (
                current_start
            )

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
            f"현재 ROC 데이터 오류: "
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
        or "c" not in df
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
# ROC 필터 분석
#
# ★ 핵심 ★
#
# 모든 기간은 무조건 계산
#
# Y → 실제 필터
# N → 화면 참고
# =========================================================

def roc_filter_analysis(
    df,
    timeframe
):

    display_periods = (
        get_display_periods(
            timeframe
        )
    )

    enabled_periods = (
        get_enabled_periods(
            timeframe
        )
    )

    result = {

        "direction":
            "none",

        "passed":
            False,

        "roc_values":
            {},

        "enabled_periods":
            enabled_periods.copy(),

        "display_periods":
            display_periods.copy(),

        "positive_count":
            0,

        "total_count":
            len(display_periods),

        "enabled_count":
            len(enabled_periods)
    }

    if (
        df is None
        or df.empty
    ):

        return result

    values = {}

    # =====================================================
    # ★ 모든 ROC 계산
    #
    # N이어도 절대 제외하지 않는다.
    # =====================================================

    for period in display_periods:

        try:

            series = roc(
                df,
                period
            )

            if (
                series is None
                or series.empty
            ):

                continue

            value = float(
                series.iloc[-1]
            )

            if pd.isna(value):

                continue

            values[
                period
            ] = value

        except Exception:

            continue

    result[
        "roc_values"
    ] = values

    # =====================================================
    # ★ 실제 필터
    #
    # Y만 검사
    # =====================================================

    if not enabled_periods:

        result["passed"] = True
        result["direction"] = "long"

        return result

    filter_pass = True
    positive_count = 0

    for period in enabled_periods:

        value = values.get(
            period
        )

        if value is None:

            filter_pass = False

            break

        if value >= 0:

            positive_count += 1

        else:

            filter_pass = False

            break

    result[
        "positive_count"
    ] = positive_count

    result[
        "passed"
    ] = filter_pass

    if filter_pass:

        result[
            "direction"
        ] = "long"

    return result


# =========================================================
# 실제 신호 필터
#
# ★ 1H만 사용
#
# 4H는 완전히 무시
# =========================================================

def all_active_roc_filters_pass(
    filter_1h,
    filter_4h=None
):

    enabled_1h = (
        get_enabled_periods(
            "1H"
        )
    )

    # 1H 필터가 하나도 없으면 통과
    if not enabled_1h:

        return True

    if not filter_1h:

        return False

    values = filter_1h.get(
        "roc_values",
        {}
    )

    for period in enabled_1h:

        value = values.get(
            period
        )

        if value is None:

            return False

        try:

            if float(value) < 0:

                return False

        except Exception:

            return False

    return True


# =========================================================
# 빈 필터
# =========================================================

def empty_roc_filter(
    timeframe
):

    return {

        "direction":
            "none",

        "passed":
            False,

        "roc_values":
            {},

        "enabled_periods":
            get_enabled_periods(
                timeframe
            ),

        "display_periods":
            get_display_periods(
                timeframe
            ),

        "positive_count":
            0,

        "total_count":
            5,

        "enabled_count":
            len(
                get_enabled_periods(
                    timeframe
                )
            )
    }


# =========================================================
# ROC 필터 표시 데이터
# =========================================================

def roc_filter_display(
    data,
    timeframe
):

    if not data:

        return empty_roc_filter(
            timeframe
        )

    return {

        "direction":
            data.get(
                "direction",
                "none"
            ),

        "passed":
            bool(
                data.get(
                    "passed",
                    False
                )
            ),

        "roc_values":
            data.get(
                "roc_values",
                {}
            ),

        "enabled_periods":
            get_enabled_periods(
                timeframe
            ),

        "display_periods":
            get_display_periods(
                timeframe
            ),

        "positive_count":
            data.get(
                "positive_count",
                0
            ),

        "total_count":
            5,

        "enabled_count":
            len(
                get_enabled_periods(
                    timeframe
                )
            )
    }


# =========================================================
# ROC 분석
#
# ROC 자체는 1H 기준
# =========================================================

def roc_analysis(
    df_confirmed,
    df_current,
    filter_1h=None,
    filter_4h=None
):

    result = {

        "roc5":
            None,

        "roc5_previous":
            None,

        "roc5_count":
            0,

        "roc5_negative_count":
            0,

        "long_breakout":
            False,

        "long_breakout_count":
            0,

        "long_breakout_state":
            "none",

        "filter_pass":
            False,

        "state":
            "none",

        "display":
            "⚪ 0"
    }

    if (
        df_confirmed is None
        or df_confirmed.empty
        or df_current is None
        or df_current.empty
    ):

        return result

    try:

        confirmed = roc(
            df_confirmed,
            ROC_PERIOD
        )

        current = roc(
            df_current,
            ROC_PERIOD
        )

        if (
            confirmed is None
            or current is None
        ):

            return result

        confirmed_value = float(
            confirmed.iloc[-1]
        )

        current_value = float(
            current.iloc[-1]
        )

        if (
            pd.isna(
                confirmed_value
            )
            or pd.isna(
                current_value
            )
        ):

            return result

        # =================================================
        # ★ 1H 필터만 사용
        # =================================================

        filter_pass = (
            all_active_roc_filters_pass(
                filter_1h,
                filter_4h
            )
        )

        # =================================================
        # 연속 양수
        # =================================================

        values = [
            float(v)
            for v in current.tolist()
            if not pd.isna(v)
        ]

        positive_count = 0

        for value in reversed(values):

            if value > 0:

                positive_count += 1

            else:

                break

        # =================================================
        # 연속 음수
        # =================================================

        negative_count = 0

        for value in reversed(values):

            if value < 0:

                negative_count += 1

            else:

                break

        # =================================================
        # 0선 상향 돌파
        # =================================================

        is_cross = (
            current_value > 0
            and confirmed_value <= 0
        )

        # =================================================
        # 필터 통과
        # =================================================

        if (
            filter_pass
            and current_value > 0
        ):

            if is_cross:

                result.update({

                    "roc5":
                        current_value,

                    "roc5_previous":
                        confirmed_value,

                    "roc5_count":
                        positive_count,

                    "roc5_negative_count":
                        negative_count,

                    "long_breakout":
                        True,

                    "long_breakout_count":
                        0,

                    "long_breakout_state":
                        "current",

                    "filter_pass":
                        True,

                    "state":
                        "long_breakout",

                    "display":
                        "🚀0"
                })

            else:

                count = max(
                    positive_count - 1,
                    0
                )

                result.update({

                    "roc5":
                        current_value,

                    "roc5_previous":
                        confirmed_value,

                    "roc5_count":
                        positive_count,

                    "roc5_negative_count":
                        negative_count,

                    "long_breakout":
                        True,

                    "long_breakout_count":
                        count,

                    "long_breakout_state":
                        "confirmed",

                    "filter_pass":
                        True,

                    "state":
                        "long_breakout",

                    "display":
                        f"🚀{count}"
                })

        # =================================================
        # ROC 양수지만 필터 탈락
        # =================================================

        elif current_value > 0:

            result.update({

                "roc5":
                    current_value,

                "roc5_previous":
                    confirmed_value,

                "roc5_count":
                    positive_count,

                "roc5_negative_count":
                    negative_count,

                "filter_pass":
                    False,

                "state":
                    "filter_wait",

                "display":
                    f"🟡 ROC +({positive_count})"
            })

        # =================================================
        # ROC 음수
        # =================================================

        elif current_value < 0:

            result.update({

                "roc5":
                    current_value,

                "roc5_previous":
                    confirmed_value,

                "roc5_count":
                    positive_count,

                "roc5_negative_count":
                    negative_count,

                "filter_pass":
                    filter_pass,

                "state":
                    "negative",

                "display":
                    f"🔴 ROC -({negative_count})"
            })

        # =================================================
        # 0
        # =================================================

        else:

            result.update({

                "roc5":
                    current_value,

                "roc5_previous":
                    confirmed_value,

                "roc5_count":
                    positive_count,

                "roc5_negative_count":
                    negative_count,

                "filter_pass":
                    filter_pass,

                "state":
                    "none",

                "display":
                    "⚪ 0"
            })

        return result

    except Exception as e:

        log.error(
            f"ROC 분석 오류: {e}"
        )

        return result


# =========================================================
# 일봉 등락률
# =========================================================

def daily_change_upbit(
    market
):

    response = request_get(
        "https://api.upbit.com/v1/candles/days",
        params={
            "market":
                market,

            "count":
                2
        }
    )

    if response is None:

        return None

    try:

        data = response.json()

        if len(data) < 2:

            return None

        current = float(
            data[0][
                "trade_price"
            ]
        )

        previous = float(
            data[1][
                "trade_price"
            ]
        )

        if previous == 0:

            return None

        return (
            current
            - previous
        ) / previous * 100

    except Exception:

        return None


# =========================================================
# 숫자
# =========================================================

def get_change_value(
    value
):

    if value is None:

        return None

    try:

        return float(value)

    except Exception:

        return None


# =========================================================
# 거래대금 표시
# =========================================================

def format_volume(
    value
):

    try:

        value = float(value)

    except Exception:

        return "-"

    if value >= 1e12:

        return f"{value / 1e12:.1f}조"

    if value >= 1e8:

        return f"{value / 1e8:.0f}억"

    if value >= 1e4:

        return f"{value / 1e4:.0f}만"

    return f"{value:,.0f}"


# =========================================================
# 가격
# =========================================================

def format_price(
    value
):

    if value is None:

        return "-"

    try:

        value = float(value)

    except Exception:

        return "-"

    if value >= 100000000:

        return f"{value / 100000000:.2f}억"

    if value >= 10000:

        return f"{value:,.0f}"

    if value >= 1:

        return f"{value:,.2f}"

    return f"{value:.6f}"


# =========================================================
# 등락률 HTML
# =========================================================

def change_html(
    value
):

    value = get_change_value(
        value
    )

    if value is None:

        return "-"

    if value > 0:

        return (
            '<span class="up">'
            f'▲+{value:.1f}%'
            '</span>'
        )

    if value < 0:

        return (
            '<span class="down">'
            f'▼{value:.1f}%'
            '</span>'
        )

    return (
        '<span class="zero">'
        '0.0%'
        '</span>'
    )


# =========================================================
# ★ 분석
#
# 중요:
# 1H/4H 모두 항상 데이터를 가져온다.
# Y/N 때문에 데이터 자체가 없어지지 않는다.
# =========================================================

def analyze(
    market,
    current_price
):

    # =====================================================
    # 1H
    #
    # 항상 계산
    # =====================================================

    df1h = history_upbit(
        market,
        60,
        required=250
    )

    if (
        df1h is None
        or df1h.empty
    ):

        return None

    # =====================================================
    # 4H
    #
    # 항상 계산
    # 참고용
    # =====================================================

    df4h = history_upbit(
        market,
        240,
        required=250
    )

    if (
        df4h is None
        or df4h.empty
    ):

        return None

    # =====================================================
    # ROC
    # =====================================================

    df_roc_confirmed = history_upbit(
        market,
        ROC_TIMEFRAME,
        required=250
    )

    if (
        df_roc_confirmed is None
        or df_roc_confirmed.empty
    ):

        return None

    df_roc_current = (
        get_upbit_current_roc_data(
            market,
            current_price
        )
    )

    if (
        df_roc_current is None
        or df_roc_current.empty
    ):

        return None

    # =====================================================
    # 1H ROC
    #
    # 5/10/20/50/200 전부 계산
    # =====================================================

    filter1_raw = (
        roc_filter_analysis(
            df1h,
            "1H"
        )
    )

    filter1 = (
        roc_filter_display(
            filter1_raw,
            "1H"
        )
    )

    # =====================================================
    # 4H ROC
    #
    # 5/10/20/50/200 전부 계산
    # =====================================================

    filter4_raw = (
        roc_filter_analysis(
            df4h,
            "4H"
        )
    )

    filter4 = (
        roc_filter_display(
            filter4_raw,
            "4H"
        )
    )

    # =====================================================
    # ROC
    # =====================================================

    r = roc_analysis(
        df_roc_confirmed,
        df_roc_current,
        filter1,
        filter4
    )

    # =====================================================
    # 일봉
    # =====================================================

    daily_change = (
        daily_change_upbit(
            market
        )
    )

    # =====================================================
    # ★ 실제 필터
    #
    # 1H Y만 적용
    # 4H는 적용하지 않음
    # =====================================================

    filter_pass = (
        all_active_roc_filters_pass(
            filter1,
            filter4
        )
    )

    # =====================================================
    # ROC 값
    # =====================================================

    roc_value = r.get(
        "roc5"
    )

    try:

        roc_value = float(
            roc_value
        )

    except Exception:

        roc_value = None

    # =====================================================
    # ROC5 사용 여부
    # =====================================================

    roc_enabled = (
        USE_1H_ROC5 == "Y"
    )

    # =====================================================
    # ROC 0선 상승 돌파
    # =====================================================

    breakout = (

        roc_enabled

        and filter_pass

        and roc_value is not None

        and roc_value > 0

        and r.get(
            "long_breakout",
            False
        )

        and r.get(
            "long_breakout_state",
            "none"
        ) != "none"
    )

    # =====================================================
    # 당일 상승
    # =====================================================

    daily_pass = (

        daily_change is not None

        and daily_change >= 0
    )

    # =====================================================
    # 결과
    # =====================================================

    return {

        "roc_filter_1h":
            filter1,

        "roc_filter_high":
            filter4,

        "roc":
            r,

        "changes":
            daily_change,

        "filter_pass":
            filter_pass,

        "direction_1h":
            (
                "long"
                if filter_pass
                else "none"
            ),

        "breakout_qualified":
            bool(
                breakout
                and daily_pass
            ),

        "df1h":
            df1h,

        "df4h":
            df4h
    }


# =========================================================
# 행 생성
# =========================================================

def make_row(
    rank,
    name,
    volume,
    analysis,
    price,
    orderbook
):

    a = analysis or {}

    r1 = a.get(
        "roc_filter_1h"
    )

    r4 = a.get(
        "roc_filter_high"
    )

    r = a.get(
        "roc",
        {}
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

        "volume_raw":
            volume,

        "current_price":
            price,

        "change_value":
            a.get(
                "changes"
            ),

        "roc_filter_1h":
            r1
            if r1
            else empty_roc_filter(
                "1H"
            ),

        "roc_filter_high":
            r4
            if r4
            else empty_roc_filter(
                "4H"
            ),

        "roc":
            r,

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

        "bid_amount":
            orderbook.get(
                "bid_amount",
                0
            ),

        "ask_amount":
            orderbook.get(
                "ask_amount",
                0
            ),

        "bid_ratio":
            orderbook.get(
                "bid_ratio",
                0
            ),

        "ask_ratio":
            orderbook.get(
                "ask_ratio",
                0
            ),

        "orderbook_dominance":
            orderbook.get(
                "dominance",
                "balanced"
            )
    }


# =========================================================
# 상승 신호
# =========================================================

def is_breakout(row):

    if not row:

        return False

    return bool(
        row.get(
            "breakout_qualified",
            False
        )
    )


# =========================================================
# ROC 필터 표시
#
# ★ N이어도 상태 표시
# =========================================================

def roc_filter_html(
    data,
    timeframe
):

    display_periods = (
        get_display_periods(
            timeframe
        )
    )

    enabled_periods = (
        get_enabled_periods(
            timeframe
        )
    )

    values = {}

    if data:

        values = data.get(
            "roc_values",
            {}
        )

    html = []

    for period in display_periods:

        value = values.get(
            period
        )

        # -----------------------------------------------
        # 현재 상태
        # -----------------------------------------------

        if value is None:

            icon = "⚪"

        else:

            try:

                value = float(value)

                if value > 0:

                    icon = "🟢"

                elif value < 0:

                    icon = "🔴"

                else:

                    icon = "⚪"

            except Exception:

                icon = "⚪"

        # -----------------------------------------------
        # Y/N
        # -----------------------------------------------

        if period in enabled_periods:

            # Y = 실제 필터
            cls = "enabled"

        else:

            # N = 참고
            cls = "disabled"

        html.append(
            f'''
            <span class="roc-item {cls}">
                <b>{period}</b>
                <span>{icon}</span>
            </span>
            '''
        )

    return (
        '<div class="roc-periods">'
        + "".join(html)
        + '</div>'
    )


# =========================================================
# 필터 전체
# =========================================================

def filter_html(
    r1,
    r4
):

    return f'''

    <div class="filter-detail">

        <div class="filter-line">

            <span class="filter-timeframe">
                1H 기준
            </span>

            {roc_filter_html(
                r1,
                "1H"
            )}

        </div>

        <div class="filter-line">

            <span class="filter-timeframe reference">
                4H 참고
            </span>

            {roc_filter_html(
                r4,
                "4H"
            )}

        </div>

    </div>

    '''


# =========================================================
# ROC
# =========================================================

def roc_html(
    r
):

    if not r:

        return (
            '<span class="roc-neutral">'
            '⚪'
            '</span>'
        )

    display = r.get(
        "display",
        "⚪ 0"
    )

    if display.startswith(
        "🚀"
    ):

        return (
            '<span class="roc-rocket">'
            f'{display}'
            '</span>'
        )

    if display.startswith(
        "🟡"
    ):

        return (
            '<span class="roc-yellow">'
            f'{display}'
            '</span>'
        )

    if display.startswith(
        "🔴"
    ):

        return (
            '<span class="roc-red">'
            f'{display}'
            '</span>'
        )

    return (
        '<span class="roc-neutral">'
        f'{display}'
        '</span>'
    )


# =========================================================
# 신호
# =========================================================

def signal_html(
    row
):

    if not row:

        return "-"

    if row.get(
        "breakout_qualified",
        False
    ):

        r = row.get(
            "roc",
            {}
        )

        count = r.get(
            "long_breakout_count",
            0
        )

        return (
            '<span class="signal">'
            f'🚀{count}'
            '</span>'
        )

    return "-"


# =========================================================
# 호가
# =========================================================

def orderbook_html(
    row
):

    if not row:

        return "-"

    bid = float(
        row.get(
            "bid_ratio",
            0
        )
    )

    ask = float(
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

        text = "▲ 매수"

        cls = "bid"

    elif dominance == "ask":

        text = "▼ 매도"

        cls = "ask"

    else:

        text = "◆ 균형"

        cls = "balanced"

    return f'''

    <div class="ob">

        <span>
            매수 {bid:.1f}%
        </span>

        <span>
            매도 {ask:.1f}%
        </span>

        <span class="ob-{cls}">
            {text}
        </span>

    </div>

    '''


# =========================================================
# 테이블 행
# =========================================================

def table_rows(
    data
):

    if not data:

        return '''
        <tr>
            <td
                colspan="6"
                class="empty"
            >
                현재 데이터 없음
            </td>
        </tr>
        '''

    result = []

    for row in data:

        cls = (
            "signal-row"
            if row.get(
                "breakout_qualified",
                False
            )
            else ""
        )

        result.append(
            f'''

            <tr class="{cls}">

                <td>
                    {row.get(
                        "rank",
                        "-"
                    )}
                </td>

                <td class="coin">

                    <b>
                        {row.get(
                            "name",
                            "-"
                        )}
                    </b>

                    <small>
                        {change_html(
                            row.get(
                                "change_value"
                            )
                        )}
                    </small>

                </td>

                <td>
                    {row.get(
                        "volume",
                        "-"
                    )}
                </td>

                <td>

                    {filter_html(
                        row.get(
                            "roc_filter_1h"
                        ),
                        row.get(
                            "roc_filter_high"
                        )
                    )}

                </td>

                <td>

                    {roc_html(
                        row.get(
                            "roc"
                        )
                    )}

                </td>

                <td>

                    {signal_html(
                        row
                    )}

                </td>

            </tr>

            <tr class="ob-row">

                <td colspan="6">

                    {orderbook_html(
                        row
                    )}

                </td>

            </tr>

            '''
        )

    return "".join(
        result
    )


# =========================================================
# 테이블
# =========================================================

def table_html(
    data
):

    return f'''

    <div class="table-wrap">

        <table>

            <thead>

                <tr>

                    <th>#</th>

                    <th>코인</th>

                    <th>거래대금</th>

                    <th>
                        ROC 필터
                    </th>

                    <th>
                        ROC
                    </th>

                    <th>
                        신호
                    </th>

                </tr>

            </thead>

            <tbody>

                {table_rows(data)}

            </tbody>

        </table>

    </div>

    '''


# =========================================================
# 상승 신호 영역
# =========================================================

def focus_section(
    data
):

    signals = [
        x
        for x in data
        if is_breakout(x)
    ]

    return f'''

    <div class="section-title">

        <span class="section-main">
            🚀 상승 신호
        </span>

        <span class="section-sub">
            1H 기준
            {get_enabled_filter_text("1H")}
            ≥0
            · ROC 0선 상승 돌파
            · 4H 참고
            · 당일 ≥0%
        </span>

    </div>

    {table_html(signals)}

    '''


# =========================================================
# TOP 영역
# =========================================================

def top_section(
    data
):

    return f'''

    <div class="section-title">

        <span class="section-main">
            🏆 업비트 TOP{TOP_N}
        </span>

        <span class="section-sub">
            {latest_upbit_update_time}
        </span>

    </div>

    {table_html(data)}

    '''


# =========================================================
# BTC
# =========================================================

def get_btc(
    data
):

    for row in data:

        if row.get(
            "name"
        ) == "BTC":

            return row

    return None


def market_summary_html():

    btc = get_btc(
        latest_upbit_data
    )

    if btc:

        price = format_price(
            btc.get(
                "current_price"
            )
        )

        change = change_html(
            btc.get(
                "change_value"
            )
        )

        if btc.get(
            "filter_pass",
            False
        ):

            filter_icon = "☀️"
            filter_text = "통과"

        else:

            filter_icon = "🌧️"
            filter_text = "탈락"

    else:

        price = "-"
        change = "-"
        filter_icon = "⚪"
        filter_text = "-"

    return f'''

    <div class="market-summary">

        <div class="market-title">

            <span>
                ₿ BTC 시장 시황
            </span>

            <small>
                1H 기준
                {get_enabled_filter_text("1H")}
                · 4H 참고
                · ROC {format_timeframe(ROC_TIMEFRAME)}
            </small>

        </div>

        <div class="btc-row">

            <b>
                ₿ BTC
            </b>

            <strong>
                {price}
            </strong>

            <span>
                {change}
            </span>

        </div>

        <div class="btc-boxes">

            <div class="btc-box">

                <small>
                    1H 기준 필터
                </small>

                <strong>
                    {filter_icon}
                </strong>

                <span>
                    {filter_text}
                </span>

            </div>

            <div class="btc-box">

                <small>
                    1H 실제 설정
                </small>

                <strong>
                    {get_enabled_filter_text("1H")}
                </strong>

                <span>
                    Y 항목
                </span>

            </div>

            <div class="btc-box">

                <small>
                    4H 참고
                </small>

                <strong>
                    {get_display_filter_text("4H")}
                </strong>

                <span>
                    상태 표시
                </span>

            </div>

        </div>

    </div>

    '''


# =========================================================
# CSS
# =========================================================

CSS = r'''

* {
    box-sizing: border-box;
    -webkit-tap-highlight-color: transparent;
}

html,
body {
    margin: 0;
    padding: 0;
    width: 100%;
    overflow-x: hidden;
}

body {
    background: #0d1014;
    color: #eeeeee;
    font-family:
        -apple-system,
        BlinkMacSystemFont,
        "Segoe UI",
        Arial,
        sans-serif;

    font-size: 8px;
    padding: 3px;
}

h1 {
    margin: 2px 3px 4px;
    font-size: 12px;
    line-height: 14px;
}

.status {
    display: flex;
    flex-wrap: wrap;
    gap: 7px;
    padding: 3px 4px;
    margin-bottom: 4px;

    border-top: 1px solid #252a30;
    border-bottom: 1px solid #252a30;

    color: #888f98;
    font-size: 5.5px;
    font-weight: 800;
}

.status-y {
    color: #39e875;
}

.status-n {
    color: #ff5555;
}

.market-summary {
    width: 100%;
    padding: 4px;

    background: #11151a;

    border-top: 1px solid #292f36;
    border-bottom: 1px solid #292f36;

    border-radius: 4px;
}

.market-title {
    display: flex;
    align-items: center;
    gap: 5px;

    height: 18px;

    font-size: 8px;
    font-weight: 900;

    white-space: nowrap;
    overflow: hidden;
}

.market-title small {
    color: #727a84;
    font-size: 5.5px;
    overflow: hidden;
    text-overflow: ellipsis;
}

.btc-row {
    display: flex;
    align-items: center;

    width: 100%;
    height: 20px;

    gap: 5px;

    white-space: nowrap;
}

.btc-row b {
    width: 35px;
    font-size: 7px;
}

.btc-row strong {
    flex: 1;
    min-width: 0;

    font-size: 7px;

    overflow: hidden;
    text-overflow: ellipsis;
}

.btc-row > span {
    width: 65px;

    text-align: right;

    font-size: 9px;
    font-weight: 900;
}

.btc-boxes {
    display: grid;
    grid-template-columns:
        1fr 1fr 1fr;

    gap: 4px;

    margin-top: 4px;
}

.btc-box {
    min-height: 58px;

    display: flex;
    flex-direction: column;

    align-items: center;
    justify-content: center;

    text-align: center;

    border: 1px solid #292f36;
    border-radius: 5px;

    background: #15191e;
}

.btc-box small {
    color: #727a84;
    font-size: 5px;
}

.btc-box strong {
    margin: 2px 0;

    color: #ffffff;

    font-size: 18px;
    line-height: 20px;
}

.btc-box span {
    color: #727a84;
    font-size: 5px;
}

.up {
    color: #39e875;
    font-weight: 900;
}

.down {
    color: #ff5555;
    font-weight: 900;
}

.zero {
    color: #777f89;
}

.section-title {
    display: flex;
    align-items: center;

    gap: 5px;

    width: 100%;

    min-height: 20px;

    margin: 5px 0 3px;
    padding: 3px 5px;

    border-left: 3px solid #39e875;

    background: rgba(
        57,
        232,
        117,
        0.08
    );

    border-radius: 3px;

    white-space: nowrap;
    overflow: hidden;
}

.section-main {
    font-size: 8px;
    font-weight: 900;
}

.section-sub {
    color: #737b85;
    font-size: 5.3px;
    font-weight: 700;

    overflow: hidden;
    text-overflow: ellipsis;
}

.table-wrap {
    width: 100%;

    overflow: hidden;

    border: 1px solid #292f36;
    border-radius: 5px;

    background: #171b20;
}

table {
    width: 100%;

    border-collapse: collapse;
    table-layout: fixed;
}

thead {
    background: #111419;
}

th {
    height: 18px;

    padding: 1px;

    border-bottom: 1px solid #292f36;

    color: #747c86;

    font-size: 5px;
    font-weight: 800;

    text-align: center;
}

td {
    height: 27px;

    padding: 1px;

    border-bottom: 1px solid #22282e;

    text-align: center;
    vertical-align: middle;

    overflow: hidden;
}

th:nth-child(1),
td:nth-child(1) {
    width: 6%;
}

th:nth-child(2),
td:nth-child(2) {
    width: 15%;
}

th:nth-child(3),
td:nth-child(3) {
    width: 14%;
}

th:nth-child(4),
td:nth-child(4) {
    width: 33%;
}

th:nth-child(5),
td:nth-child(5) {
    width: 15%;
}

th:nth-child(6),
td:nth-child(6) {
    width: 17%;
}

.coin {
    text-align: left;
}

.coin b {
    display: block;

    font-size: 6.5px;
    line-height: 8px;

    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}

.coin small {
    display: block;

    font-size: 4.5px;
    line-height: 6px;
}

.roc-filter-column {
    text-align: left;
}

.filter-detail {
    display: flex;
    flex-direction: column;

    gap: 2px;

    width: 100%;

    overflow: hidden;
}

.filter-line {
    display: flex;
    align-items: center;

    width: 100%;

    overflow: hidden;
}

.filter-timeframe {
    width: 30px;
    flex: none;

    color: #e0e5ea;

    font-size: 4.6px;
    font-weight: 900;

    text-align: left;
}

.filter-timeframe.reference {
    color: #747c86;
}

.roc-periods {
    display: flex;
    align-items: center;

    gap: 2px;

    overflow: hidden;
}

.roc-item {
    display: inline-flex;
    align-items: center;
    justify-content: center;

    min-width: 24px;
    height: 14px;

    border-radius: 3px;

    font-size: 6px;
    font-weight: 900;

    white-space: nowrap;
}

.roc-item b {
    margin-right: 1px;

    font-size: 4px;
}

.roc-item.enabled {
    opacity: 1;
}

.roc-item.disabled {
    opacity: 0.45;
}

.roc-rocket {
    color: #39e875;

    font-size: 7px;
    font-weight: 900;
}

.roc-yellow {
    color: #ffc857;

    font-size: 6px;
    font-weight: 900;
}

.roc-red {
    color: #ff5555;

    font-size: 6px;
    font-weight: 900;
}

.roc-neutral {
    color: #747c86;

    font-size: 6px;
}

.signal {
    display: inline-flex;

    align-items: center;
    justify-content: center;

    min-width: 28px;
    min-height: 20px;

    color: #39e875;

    font-size: 14px;
    font-weight: 900;
}

.signal-row {
    background: rgba(
        57,
        232,
        117,
        0.07
    );
}

.ob-row {
    background: #101419;
}

.ob-row td {
    height: 14px;
}

.ob {
    display: flex;
    justify-content: flex-end;

    gap: 8px;

    padding: 2px 4px;

    color: #6f7780;

    font-size: 5px;
    font-weight: 700;
}

.ob-bid {
    color: #39e875;
}

.ob-ask {
    color: #ff5555;
}

.ob-balanced {
    color: #858d96;
}

.empty {
    height: 35px;

    color: #555d67;

    font-size: 6px;
}

@media (max-width: 380px) {

    body {
        padding: 2px;
    }

    .market-title {
        font-size: 7px;
    }

    .market-title small {
        font-size: 4.8px;
    }

    .btc-box {
        min-height: 54px;
    }

    .btc-box strong {
        font-size: 16px;
    }

    .section-main {
        font-size: 7px;
    }

    .section-sub {
        font-size: 4.5px;
    }

    .filter-timeframe {
        width: 27px;
        font-size: 4.1px;
    }

    .roc-periods {
        gap: 1px;
    }

    .roc-item {
        min-width: 19px;
        height: 13px;
    }

    .roc-item b {
        font-size: 3.6px;
    }

    .roc-rocket {
        font-size: 6px;
    }

    .roc-yellow,
    .roc-red,
    .roc-neutral {
        font-size: 5.3px;
    }

    .signal {
        font-size: 12px;
    }

    .ob {
        font-size: 4.5px;
        gap: 5px;
    }
}

@media (min-width: 601px) {

    body {
        max-width: 900px;
        margin: auto;
        padding: 8px;
    }

    h1 {
        font-size: 15px;
    }

    .market-title {
        height: 24px;
        font-size: 10px;
    }

    .market-title small {
        font-size: 6.5px;
    }

    .btc-row {
        height: 25px;
    }

    .btc-row b {
        font-size: 9px;
    }

    .btc-row strong {
        font-size: 9px;
    }

    .btc-boxes {
        gap: 6px;
    }

    .btc-box {
        min-height: 75px;
    }

    .btc-box small {
        font-size: 7px;
    }

    .btc-box strong {
        font-size: 27px;
        line-height: 30px;
    }

    .btc-box span {
        font-size: 7px;
    }

    .section-main {
        font-size: 10px;
    }

    .section-sub {
        font-size: 7px;
    }

    th {
        height: 25px;
        font-size: 7px;
    }

    td {
        height: 38px;
    }

    .coin b {
        font-size: 9px;
    }

    .coin small {
        font-size: 6px;
    }

    .filter-timeframe {
        width: 38px;
        font-size: 6px;
    }

    .roc-item {
        min-width: 31px;
        height: 18px;
        font-size: 8px;
    }

    .roc-item b {
        font-size: 5px;
    }

    .roc-rocket {
        font-size: 9px;
    }

    .roc-yellow,
    .roc-red,
    .roc-neutral {
        font-size: 8px;
    }

    .signal {
        font-size: 20px;
    }

    .ob {
        font-size: 7px;
    }
}

'''


# =========================================================
# Dashboard
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def dashboard():

    status = f'''

    <div class="status">

        <span>
            업비트 :
            <b class="status-y">
                {USE_UPBIT}
            </b>
        </span>

        <span>
            OKX :
            <b class="status-n">
                {USE_OKX}
            </b>
        </span>

        <span>
            1H 기준 :
            <b class="status-y">
                {get_enabled_filter_text("1H")}
            </b>
        </span>

        <span>
            4H 참고 :
            <b class="status-y">
                {get_display_filter_text("4H")}
            </b>
        </span>

        <span>
            ROC :
            <b class="status-y">
                {USE_1H_ROC5}
            </b>
        </span>

    </div>

    '''

    upbit_focus = ""

    upbit_top = ""

    if USE_UPBIT == "Y":

        upbit_focus = focus_section(
            latest_upbit_data
        )

        # ★ 중요
        #
        # TOP10은 필터 결과와 관계없이
        # 항상 전체 표시
        #

        upbit_top = top_section(
            latest_upbit_data
        )

    return f'''

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
            content="#0d1014"
        >

        <title>
            ROC · 1H 기준 · 4H 참고
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

        {status}

        {upbit_focus}

        {upbit_top}

    </body>

    </html>

    '''


# =========================================================
# 업비트 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update_time
    global latest_upbit_orderbook

    log.info(
        "========== 업비트 업데이트 =========="
    )

    markets = get_upbit_markets()

    if not markets:

        log.warning(
            "업비트 마켓 데이터 없음"
        )

        return

    # =====================================================
    # 거래대금 TOP10
    #
    # ★ 필터와 관계없이 여기서 먼저 결정
    # =====================================================

    markets = sorted(
        markets,
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

    # =====================================================
    # 호가
    # =====================================================

    orderbooks = (
        get_upbit_orderbooks(
            market_codes
        )
    )

    latest_upbit_orderbook = (
        orderbooks
    )

    rows = []

    # =====================================================
    # TOP10 전부 분석
    #
    # ★ 필터 탈락해도 rows에서 제거하지 않는다.
    # =====================================================

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
                f"{coin} 분석 오류: {e}"
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
            item["volume_24h"],
            analysis,
            price,
            ob
        )

        # ★ 절대 필터 결과로 삭제하지 않음
        rows.append(
            row
        )

        log.info(
            f"{coin} | "
            f"1H필터="
            f"{row['filter_pass']} | "
            f"ROC="
            f"{row['roc'].get('display', '-')}"
        )

    latest_upbit_data = rows

    latest_upbit_update_time = kst()

    log.info(
        f"업비트 TOP{TOP_N} "
        f"총 {len(rows)}개 표시"
    )


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    global latest_usdt_krw_internal

    if not update_lock.acquire(
        False
    ):

        log.warning(
            "이전 업데이트 진행 중"
        )

        return

    try:

        if USE_UPBIT == "Y":

            update_upbit()

    except Exception as e:

        log.exception(
            f"업데이트 오류: {e}"
        )

    finally:

        update_lock.release()


# =========================================================
# 스케줄러
# =========================================================

def scheduler_loop():

    while True:

        try:

            schedule.run_pending()

        except Exception as e:

            log.exception(
                f"스케줄러 오류: {e}"
            )

        time.sleep(1)


# =========================================================
# 시작
# =========================================================

@app.on_event("startup")
def startup():

    validate_settings()

    log.info(
        "========================================"
    )

    log.info(
        "ROC Dashboard 시작"
    )

    log.info(
        f"1H 실제 필터 = "
        f"{get_enabled_filter_text('1H')}"
    )

    log.info(
        "1H N 항목 = 필터 제외 + 상태 표시"
    )

    log.info(
        "1H 전체 표시 = "
        f"{get_display_filter_text('1H')}"
    )

    log.info(
        "4H 전체 표시 = "
        f"{get_display_filter_text('4H')}"
    )

    log.info(
        "4H = 참고용 / 신호 판정 제외"
    )

    log.info(
        "TOP10 = 필터 탈락과 관계없이 표시"
    )

    log.info(
        "========================================"
    )

    # 최초 업데이트

    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    # 반복 업데이트

    schedule.every(
        UPDATE_MINUTES
    ).minutes.do(
        update_dashboard
    )

    threading.Thread(
        target=scheduler_loop,
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
