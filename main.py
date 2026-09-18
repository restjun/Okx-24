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
# ROC 필터 시간봉 사용 설정
#
# Y = 해당 시간봉을 실제 필터에 사용
# N = 해당 시간봉은 실제 필터에서 제외
#
# ★ N이어도 대시보드에는 계속 표시
# =========================================================

USE_1H_ROC_FILTER = "Y"
USE_4H_ROC_FILTER = "N"


# =========================================================
# ROC 필터 개별 사용 설정
#
# Y = 조건에 사용
# N = 조건에는 사용하지 않음
#
# ★ N이어도 화면에는 항상 표시
#
# ★ 시간봉 자체가 N이면
#   해당 시간봉의 ROC는 필터에서 사용하지 않음
# =========================================================

USE_1H_ROC5 = "Y"
USE_1H_ROC10 = "Y"
USE_1H_ROC20 = "Y"
USE_1H_ROC50 = "Y"
USE_1H_ROC200 = "Y"

USE_4H_ROC5 = "Y"
USE_4H_ROC10 = "Y"
USE_4H_ROC20 = "Y"
USE_4H_ROC50 = "Y"
USE_4H_ROC200 = "Y"


# =========================================================
# ROC 필터 시간봉
# =========================================================

ROC_FILTER_TIMEFRAME = 60
ROC_FILTER_HIGH_TIMEFRAME = 240


# =========================================================
# ROC 로켓 시간봉
#
# 현재는 1H ROC5 기준
#
# ★ ROC5 화면 표시용
# ★ 신호 판정은 전체 활성 ROC 0선 돌파
# =========================================================

ROC_TIMEFRAME = 60
ROC_PERIOD = 5


# =========================================================
# ROC 필터 기간
#
# ★ 화면에는 항상 전체 표시
# =========================================================

ROC_FILTER_PERIODS = [
    5,
    10,
    20,
    50,
    200
]


# =========================================================
# 업비트 호가 설정
# =========================================================

ORDERBOOK_RANGE = 0.01
ORDERBOOK_COUNT = 30
ORDERBOOK_DOMINANCE_GAP = 5.0


# =========================================================
# 지원 시간봉
# =========================================================

SUPPORTED_UPBIT_TIMEFRAMES = {
    5,
    15,
    30,
    60,
    240
}

SUPPORTED_OKX_TIMEFRAMES = {
    5,
    15,
    30,
    60,
    120,
    240,
    360,
    480,
    720,
    1440
}


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
okx_1h_cache = {}
okx_1h_cache_time = "-"


# =========================================================
# ROC 신호 유지 카운팅
#
# 돌파 진행 캔들 = 0
# 다음 캔들 = 1
# 이후 2, 3, 4...
#
# 활성 ROC 조건이 해제될 때까지 유지
# =========================================================

roc_signal_state = {}


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
# 활성화된 ROC 기간
# =========================================================

def get_enabled_periods(timeframe):

    result = []

    if timeframe == "1H":

        if USE_1H_ROC_FILTER != "Y":
            return []

    elif timeframe == "4H":

        if USE_4H_ROC_FILTER != "Y":
            return []

    else:

        return []

    settings = roc_settings()

    for period in ROC_FILTER_PERIODS:

        value = settings.get(
            period,
            {}
        ).get(
            timeframe,
            "N"
        )

        if value == "Y":

            result.append(
                period
            )

    return result


# =========================================================
# 전체 ROC 기간
#
# ★ 화면 표시용
# =========================================================

def get_all_periods():

    return ROC_FILTER_PERIODS.copy()


# =========================================================
# 전체 활성 필터
# =========================================================

def get_enabled_all_filters():

    result = []

    settings = roc_settings()

    if USE_1H_ROC_FILTER == "Y":

        for period in ROC_FILTER_PERIODS:

            setting = settings[
                period
            ]["1H"]

            if setting == "Y":

                result.append(
                    ("1H", period)
                )

    if USE_4H_ROC_FILTER == "Y":

        for period in ROC_FILTER_PERIODS:

            setting = settings[
                period
            ]["4H"]

            if setting == "Y":

                result.append(
                    ("4H", period)
                )

    return result


# =========================================================
# 활성 필터 표시
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
# 필터 설정 표시
# =========================================================

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

    return (
        f"1H:{h1} 4H:{h4}"
    )


def get_roc_filter_period_text():

    return "5/10/20/50/200"


def get_roc_text():

    return (
        f"ROC{ROC_PERIOD}"
        f"({format_timeframe(ROC_TIMEFRAME)})"
    )


# =========================================================
# 공통
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


def get_okx_bar(minutes):

    return {
        5: "5m",
        15: "15m",
        30: "30m",
        60: "1H",
        120: "2H",
        240: "4H",
        360: "6H",
        480: "8H",
        720: "12H",
        1440: "1D"
    }.get(
        int(minutes)
    )


def get_okx_bar_minutes(bar):

    return {
        "1m": 1,
        "3m": 3,
        "5m": 5,
        "15m": 15,
        "30m": 30,
        "1H": 60,
        "2H": 120,
        "4H": 240,
        "6H": 360,
        "8H": 480,
        "12H": 720,
        "1D": 1440
    }.get(
        str(bar)
    )


# =========================================================
# 현재 캔들 시작
# =========================================================

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


# =========================================================
# 검증
# =========================================================

def validate_timeframe():

    global ROC_FILTER_TIMEFRAME
    global ROC_FILTER_HIGH_TIMEFRAME
    global ROC_TIMEFRAME

    ROC_FILTER_TIMEFRAME = int(
        ROC_FILTER_TIMEFRAME
    )

    ROC_FILTER_HIGH_TIMEFRAME = int(
        ROC_FILTER_HIGH_TIMEFRAME
    )

    ROC_TIMEFRAME = int(
        ROC_TIMEFRAME
    )

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
        ROC_FILTER_TIMEFRAME
        not in SUPPORTED_UPBIT_TIMEFRAMES
    ):

        raise ValueError(
            f"1H ROC 필터 시간봉 오류: "
            f"{ROC_FILTER_TIMEFRAME}"
        )

    if (
        ROC_FILTER_HIGH_TIMEFRAME
        not in SUPPORTED_UPBIT_TIMEFRAMES
    ):

        raise ValueError(
            f"4H ROC 필터 시간봉 오류: "
            f"{ROC_FILTER_HIGH_TIMEFRAME}"
        )

    if (
        ROC_TIMEFRAME
        not in SUPPORTED_UPBIT_TIMEFRAMES
    ):

        raise ValueError(
            f"ROC 로켓 시간봉 오류: "
            f"{ROC_TIMEFRAME}"
        )

    settings = roc_settings()

    for period in ROC_FILTER_PERIODS:

        for timeframe in [
            "1H",
            "4H"
        ]:

            value = settings[
                period
            ][timeframe]

            if value not in (
                "Y",
                "N"
            ):

                raise ValueError(
                    f"{timeframe} ROC{period} "
                    f"설정은 Y/N만 가능합니다."
                )

    if int(ROC_PERIOD) < 1:

        raise ValueError(
            "ROC_PERIOD는 1 이상이어야 합니다."
        )

    if int(TOP_N) < 1:

        raise ValueError(
            "TOP_N은 1 이상이어야 합니다."
        )

    if not 0 < float(
        ORDERBOOK_RANGE
    ) <= 1:

        raise ValueError(
            "ORDERBOOK_RANGE 오류"
        )

    if int(ORDERBOOK_COUNT) < 1:

        raise ValueError(
            "ORDERBOOK_COUNT 오류"
        )

    if float(
        ORDERBOOK_DOMINANCE_GAP
    ) < 0:

        raise ValueError(
            "ORDERBOOK_DOMINANCE_GAP 오류"
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
        if args
        and isinstance(
            args[0],
            str
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

            r = func(
                *args,
                **kwargs
            )

            if not hasattr(
                r,
                "status_code"
            ):

                return r

            if r.status_code == 200:

                return r

            if r.status_code == 429:

                wait = min(
                    RATE_LIMIT_WAIT
                    * 2 ** n,
                    60
                )

            elif r.status_code >= 500:

                wait = min(
                    2 * 2 ** n,
                    30
                )

            else:

                log.warning(
                    f"[HTTP {r.status_code}] "
                    f"{url}"
                )

                return r

            log.warning(
                f"[API 재시도] "
                f"{url} {wait}초"
            )

            time.sleep(
                wait
            )

        except Exception as e:

            log.error(
                f"[API 오류] "
                f"{url}: {e}"
            )

            if n < MAX_RETRIES - 1:

                time.sleep(
                    min(
                        2 * (n + 1),
                        20
                    )
                )

    log.error(
        f"[API 최종 실패] {url}"
    )

    return None


# =========================================================
# 업비트 마켓
# =========================================================

def get_upbit_markets():

    global latest_upbit_markets

    r = retry(
        requests.get,
        "https://api.upbit.com/v1/ticker/all",
        params={
            "quote_currencies": "KRW"
        },
        timeout=15
    )

    if r is None:
        return []

    try:

        result = []

        for x in r.json():

            market = x.get(
                "market",
                ""
            )

            if not market.startswith(
                "KRW-"
            ):

                continue

            try:

                volume = float(
                    x[
                        "acc_trade_price_24h"
                    ]
                )

                price = float(
                    x[
                        "trade_price"
                    ]
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
# 업비트 호가
# =========================================================

def get_upbit_orderbooks(
    markets
):

    if not markets:
        return {}

    result = {}

    chunk_size = 30

    for i in range(
        0,
        len(markets),
        chunk_size
    ):

        chunk = markets[
            i:i + chunk_size
        ]

        try:

            r = retry(
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

            if r is None:
                continue

            data = r.json()

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
                f"업비트 호가 오류: {e}"
            )

    return result


# =========================================================
# 호가 계산
# =========================================================

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

    result[
        "lower_price"
    ] = lower_price

    result[
        "upper_price"
    ] = upper_price

    units = orderbook.get(
        "orderbook_units",
        []
    )

    if not isinstance(
        units,
        list
    ):

        return result

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
            and bid_size > 0
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
            and ask_size > 0
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

        bid_ratio = 0.0
        ask_ratio = 0.0

    difference = (
        bid_ratio
        - ask_ratio
    )

    if (
        total_amount > 0
        and difference
        >= ORDERBOOK_DOMINANCE_GAP
    ):

        dominance = "bid"
        dominance_text = "매수 우세"

    elif (
        total_amount > 0
        and difference
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

    unit = int(unit)

    r = retry(
        requests.get,
        f"https://api.upbit.com/v1/candles/minutes/{unit}",
        params={
            "market": market,
            "count": min(
                max(int(count), 1),
                200
            ),
            **(
                {"to": to}
                if to
                else {}
            )
        },
        timeout=15
    )

    if r is None:
        return None

    try:

        df = pd.DataFrame(
            r.json()
        )

        if df.empty:
            return None

        df["o"] = pd.to_numeric(
            df.opening_price,
            errors="coerce"
        )

        df["h"] = pd.to_numeric(
            df.high_price,
            errors="coerce"
        )

        df["l"] = pd.to_numeric(
            df.low_price,
            errors="coerce"
        )

        df["c"] = pd.to_numeric(
            df.trade_price,
            errors="coerce"
        )

        df["volume_krw"] = pd.to_numeric(
            df.candle_acc_trade_price,
            errors="coerce"
        )

        df["datetime"] = pd.to_datetime(
            df.candle_date_time_kst,
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
            .sort_values("datetime")
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
# 현재 ROC 데이터
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

    if df is None or df.empty:
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

        if price <= 0:
            return df

        mask = (
            df.datetime == start
        )

        if mask.any():

            df.loc[
                mask,
                "c"
            ] = price

        else:

            row = df.iloc[-1].copy()

            row["datetime"] = start
            row["c"] = price

            df = pd.concat(
                [
                    df,
                    pd.DataFrame([row])
                ],
                ignore_index=True
            )

        return (
            df
            .sort_values("datetime")
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
# ROC 계산
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

    except Exception as e:

        log.error(
            f"ROC 계산 오류: {e}"
        )

        return None


# =========================================================
# ROC 필터 분석
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

        "positive_count":
            0,

        "total_count":
            len(periods),

        "enabled_periods":
            periods.copy()
    }

    if (
        df is None
        or df.empty
    ):

        return result

    try:

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

            current_value = float(
                series.iloc[-1]
            )

            if pd.isna(
                current_value
            ):

                continue

            values[
                period
            ] = current_value

            previous_value = None

            if len(series) >= 2:

                previous_value = float(
                    series.iloc[-2]
                )

                if pd.isna(
                    previous_value
                ):

                    previous_value = None

            previous_values[
                period
            ] = previous_value

            zero_cross = (
                previous_value is not None
                and previous_value <= 0
                and current_value > 0
            )

            zero_crosses[
                period
            ] = zero_cross

            if current_value >= 0:

                positive_count += 1

        passed = (
            len(values) == len(periods)
            and positive_count == len(periods)
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

    except Exception as e:

        log.error(
            f"ROC 필터 오류: {e}"
        )

        return result


# =========================================================
# 활성 ROC 필터 전체 조건
# =========================================================

def all_active_roc_filters_pass(
    filter_1h,
    filter_4h
):

    enabled = get_enabled_all_filters()

    if not enabled:

        return True

    for timeframe, period in enabled:

        if timeframe == "1H":

            info = filter_1h

        else:

            info = filter_4h

        if not info:

            return False

        values = info.get(
            "roc_values",
            {}
        )

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
# 전체 활성 ROC 0선 상승 돌파
# =========================================================

def any_active_roc_zero_cross(
    filter_1h,
    filter_4h
):

    enabled = get_enabled_all_filters()

    if not enabled:

        return False

    for timeframe, period in enabled:

        if timeframe == "1H":

            info = filter_1h

        else:

            info = filter_4h

        if not info:

            continue

        zero_crosses = info.get(
            "zero_crosses",
            {}
        )

        if zero_crosses.get(
            period,
            False
        ):

            return True

    return False


# =========================================================
# 활성 ROC 0선 돌파 목록
# =========================================================

def get_active_zero_cross_list(
    filter_1h,
    filter_4h
):

    result = []

    enabled = get_enabled_all_filters()

    for timeframe, period in enabled:

        info = (
            filter_1h
            if timeframe == "1H"
            else filter_4h
        )

        if not info:

            continue

        zero_crosses = info.get(
            "zero_crosses",
            {}
        )

        if zero_crosses.get(
            period,
            False
        ):

            result.append(
                f"{timeframe} ROC{period}"
            )

    return result


# =========================================================
# ROC 필터 표시용
# =========================================================

def roc_filter_display(
    x,
    timeframe
):

    values = {}
    previous_values = {}
    zero_crosses = {}

    if x:

        values = x.get(
            "roc_values",
            {}
        )

        previous_values = x.get(
            "previous_values",
            {}
        )

        zero_crosses = x.get(
            "zero_crosses",
            {}
        )

    return {

        "display":
            "🟢"
            if x
            and x.get(
                "passed",
                False
            )
            else "⚪",

        "direction":
            x.get(
                "direction",
                "none"
            )
            if x
            else "none",

        "passed":
            bool(
                x.get(
                    "passed",
                    False
                )
            )
            if x
            else False,

        "positive_count":
            x.get(
                "positive_count",
                0
            )
            if x
            else 0,

        "total_count":
            len(
                ROC_FILTER_PERIODS
            ),

        "roc_values":
            values,

        "previous_values":
            previous_values,

        "zero_crosses":
            zero_crosses,

        "enabled_periods":
            get_enabled_periods(
                timeframe
            ),

        "all_periods":
            ROC_FILTER_PERIODS.copy()

    }


# =========================================================
# ROC5 분석
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

        filter_pass = (
            all_active_roc_filters_pass(
                filter_1h,
                filter_4h
            )
        )

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

        negative_count = 0

        for value in reversed(values):

            if value < 0:

                negative_count += 1

            else:

                break

        is_cross = False

        if (
            current_value > 0
            and confirmed_value <= 0
        ):

            is_cross = True

        breakout_count = 0

        if current_value > 0:

            if is_cross:

                breakout_count = 0

            else:

                breakout_count = max(
                    positive_count - 1,
                    0
                )

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
                        breakout_count,

                    "long_breakout_state":
                        "confirmed",

                    "filter_pass":
                        True,

                    "state":
                        "long_breakout",

                    "display":
                        f"🚀{breakout_count}"

                })

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

                "long_breakout":
                    False,

                "long_breakout_count":
                    0,

                "long_breakout_state":
                    "none",

                "filter_pass":
                    False,

                "state":
                    "filter_wait",

                "display":
                    f"🟡 ROC +({positive_count})"

            })

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

                "long_breakout":
                    False,

                "long_breakout_count":
                    0,

                "long_breakout_state":
                    "none",

                "filter_pass":
                    filter_pass,

                "state":
                    "negative",

                "display":
                    f"🔴 ROC -({negative_count})"

            })

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

                "long_breakout":
                    False,

                "long_breakout_count":
                    0,

                "long_breakout_state":
                    "none",

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
            f"ROC5 분석 오류: {e}"
        )

        return result


# =========================================================
# 등락률
# =========================================================

def daily_change_upbit(
    market
):

    r = retry(
        requests.get,
        "https://api.upbit.com/v1/candles/days",
        params={
            "market": market,
            "count": 2
        },
        timeout=15
    )

    if r is None:
        return None

    try:

        data = r.json()

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

        return [
            (
                current
                - previous
            )
            / previous
            * 100
        ]

    except Exception as e:

        log.error(
            f"일봉 등락률 오류 "
            f"{market}: {e}"
        )

        return None


def get_change_value(x):

    try:

        if x is None:
            return None

        if isinstance(
            x,
            (list, tuple)
        ):

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

    return f"{v:,.0f}"


# =========================================================
# 빈 분석
# =========================================================

def empty_roc_filter(
    timeframe
):

    return {

        "display":
            "⚪",

        "direction":
            "none",

        "passed":
            False,

        "positive_count":
            0,

        "total_count":
            len(
                ROC_FILTER_PERIODS
            ),

        "roc_values":
            {},

        "previous_values":
            {},

        "zero_crosses":
            {},

        "enabled_periods":
            get_enabled_periods(
                timeframe
            ),

        "all_periods":
            ROC_FILTER_PERIODS.copy()
    }


def empty_analysis():

    return {

        "roc_filter_1h":
            empty_roc_filter(
                "1H"
            ),

        "roc_filter_high":
            empty_roc_filter(
                "4H"
            ),

        "roc": {

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
        },

        "changes":
            None,

        "breakout_qualified":
            False,

        "filter_pass":
            False,

        "direction_1h":
            "none",

        "zero_cross":
            False,

        "zero_cross_list":
            [],

        "signal_active":
            False,

        "signal_count":
            0,

        "df1h":
            None
    }


# =========================================================
# ROC 필터 HTML
# =========================================================

def roc_filter_html(
    r,
    timeframe
):

    settings = roc_settings()

    values = {}

    if r:

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

        setting = settings[
            period
        ][timeframe]

        if setting == "Y":

            parts.append(
                f'''
                <span class="roc-item roc-active">
                    {period}{icon}
                </span>
                '''
            )

        else:

            parts.append(
                f'''
                <span class="roc-item roc-disabled">
                    {period}{icon}
                </span>
                '''
            )

    return (
        '<div class="roc-filter-all">'
        + "".join(parts)
        + '</div>'
    )


# =========================================================
# 필터 HTML
# =========================================================

def filter_html(
    r1,
    r4
):

    return (
        '<div class="filter-detail">'

        '<div class="filter-line">'

        '<span class="filter-timeframe">'
        '1H'
        '</span>'

        f'{roc_filter_html(r1, "1H")}'

        '</div>'

        '<div class="filter-line">'

        '<span class="filter-timeframe">'
        '4H'
        '</span>'

        f'{roc_filter_html(r4, "4H")}'

        '</div>'

        '</div>'
    )


# =========================================================
# 신호 자격
#
# ★ 전체 활성 ROC 조건
#
# 돌파 캔들 = 0
# 다음 캔들 = 1
# 이후 2, 3, 4...
#
# ★ 조건이 유지되는 동안 계속 표시
# ★ 조건이 해제되면 신호 종료
# =========================================================

def get_signal_qualified(
    r1,
    r4,
    r,
    daily_change,
    market=None,
    current_candle_time=None
):

    global roc_signal_state

    # -----------------------------------------------------
    # 활성 ROC 필터 전체 통과
    # -----------------------------------------------------

    filter_pass = (
        all_active_roc_filters_pass(
            r1,
            r4
        )
    )

    # -----------------------------------------------------
    # 활성 ROC 중 하나라도 0선 상승 돌파
    # -----------------------------------------------------

    zero_cross = (
        any_active_roc_zero_cross(
            r1,
            r4
        )
    )

    # -----------------------------------------------------
    # 당일 등락률
    # -----------------------------------------------------

    change_value = (
        get_change_value(
            daily_change
        )
    )

    daily_pass = (
        change_value is not None
        and change_value >= 0
    )

    # -----------------------------------------------------
    # 활성 조건
    # -----------------------------------------------------

    active_condition = (
        filter_pass
        and daily_pass
    )

    # -----------------------------------------------------
    # 코인별 상태
    # -----------------------------------------------------

    if market is None:

        market_key = "_default"

    else:

        market_key = str(
            market
        )

    state = roc_signal_state.get(
        market_key
    )

    # -----------------------------------------------------
    # 최초 0선 돌파
    #
    # 현재 진행 중인 캔들 = 0
    # -----------------------------------------------------

    if (
        active_condition
        and zero_cross
    ):

        roc_signal_state[
            market_key
        ] = {

            "active":
                True,

            "count":
                0,

            "cross_candle":
                current_candle_time,

            "last_candle":
                current_candle_time
        }

        state = roc_signal_state[
            market_key
        ]

    # -----------------------------------------------------
    # 기존 신호 유지
    #
    # 새로운 1H 캔들이 시작될 때마다 +1
    # -----------------------------------------------------

    elif (
        active_condition
        and state
        and state.get(
            "active",
            False
        )
    ):

        last_candle = state.get(
            "last_candle"
        )

        if (
            current_candle_time is not None
            and last_candle is not None
            and current_candle_time
            != last_candle
        ):

            state[
                "count"
            ] = int(
                state.get(
                    "count",
                    0
                )
            ) + 1

            state[
                "last_candle"
            ] = current_candle_time

    # -----------------------------------------------------
    # 신호 해제
    #
    # 활성 ROC가 하나라도 음수가 되거나
    # 당일 등락률이 음수가 되면 종료
    # -----------------------------------------------------

    elif not active_condition:

        roc_signal_state.pop(
            market_key,
            None
        )

    # -----------------------------------------------------
    # 상태 확인
    # -----------------------------------------------------

    state = roc_signal_state.get(
        market_key
    )

    signal_active = bool(
        state
        and state.get(
            "active",
            False
        )
    )

    signal_count = 0

    if signal_active:

        signal_count = int(
            state.get(
                "count",
                0
            )
        )

    # -----------------------------------------------------
    # 돌파 목록
    # -----------------------------------------------------

    zero_cross_list = (
        get_active_zero_cross_list(
            r1,
            r4
        )
    )

    return {

        "breakout_qualified":
            bool(
                signal_active
            ),

        "filter_pass":
            filter_pass,

        "direction_1h":
            "long"
            if filter_pass
            else "none",

        "zero_cross":
            bool(
                zero_cross
                or signal_active
            ),

        "zero_cross_list":
            zero_cross_list,

        "signal_active":
            signal_active,

        "signal_count":
            signal_count
    }


# =========================================================
# 업비트 분석
# =========================================================

def analyze(
    market,
    current_price=None
):

    all_periods = (
        get_all_periods()
    )

    need_1h = True
    need_4h = True
    need_roc5 = True

    df1h = None
    df4h = None

    # -----------------------------------------------------
    # 1H
    # -----------------------------------------------------

    if need_1h:

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

    # -----------------------------------------------------
    # 4H
    # -----------------------------------------------------

    if need_4h:

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

    # -----------------------------------------------------
    # ROC5
    # -----------------------------------------------------

    if need_roc5:

        df_roc_confirmed = (
            history_upbit(
                market,
                ROC_TIMEFRAME,
                required=200
            )
        )

        df_roc_current = (
            get_upbit_current_roc_data(
                market,
                current_price
            )
        )

        if (
            df_roc_confirmed is None
            or df_roc_confirmed.empty
        ):

            return None

        if (
            df_roc_current is None
            or df_roc_current.empty
        ):

            return None

    else:

        df_roc_confirmed = None
        df_roc_current = None

    # -----------------------------------------------------
    # 1H ROC
    # -----------------------------------------------------

    r1_raw = roc_filter_analysis(
        df1h,
        all_periods
    )

    r1 = roc_filter_display(
        r1_raw,
        "1H"
    )

    # -----------------------------------------------------
    # 4H ROC
    # -----------------------------------------------------

    r4_raw = roc_filter_analysis(
        df4h,
        all_periods
    )

    r4 = roc_filter_display(
        r4_raw,
        "4H"
    )

    # -----------------------------------------------------
    # ROC5
    # -----------------------------------------------------

    if need_roc5:

        r = roc_analysis(
            df_roc_confirmed,
            df_roc_current,
            r1,
            r4
        )

    else:

        r = {

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
                all_active_roc_filters_pass(
                    r1,
                    r4
                ),

            "state":
                "disabled",

            "display":
                "-"
        }

    # -----------------------------------------------------
    # 일봉 등락률
    # -----------------------------------------------------

    changes = (
        daily_change_upbit(
            market
        )
    )

    # -----------------------------------------------------
    # 최종 신호
    #
    # ★ 여기서 코인별 1H 캔들 기준 카운팅
    # -----------------------------------------------------

    q = get_signal_qualified(
        r1,
        r4,
        r,
        changes,
        market=market,
        current_candle_time=get_current_candle_start(
            ROC_TIMEFRAME
        )
    )

    return {

        "roc_filter_1h":
            r1,

        "roc_filter_high":
            r4,

        "roc":
            r,

        "changes":
            changes,

        **q,

        "df1h":
            df1h
    }


# =========================================================
# 행 생성
# =========================================================

def make_row(
    rank,
    name,
    volume,
    analysis,
    current_price=None,
    orderbook_info=None
):

    a = (
        analysis
        or empty_analysis()
    )

    ob = (
        orderbook_info
        or {}
    )

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

        "volume":
            format_volume(
                volume
            ),

        "current_price":
            current_price,

        "roc_filter_1h":
            a.get(
                "roc_filter_1h",
                empty_roc_filter(
                    "1H"
                )
            ),

        "roc_filter_high":
            a.get(
                "roc_filter_high",
                empty_roc_filter(
                    "4H"
                )
            ),

        "roc":
            a.get(
                "roc",
                {}
            ),

        "breakout_qualified":
            bool(
                a.get(
                    "breakout_qualified",
                    False
                )
            ),

        "filter_pass":
            bool(
                a.get(
                    "filter_pass",
                    False
                )
            ),

        "direction":
            a.get(
                "direction_1h",
                "none"
            ),

        "zero_cross":
            bool(
                a.get(
                    "zero_cross",
                    False
                )
            ),

        "zero_cross_list":
            a.get(
                "zero_cross_list",
                []
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

        "bid_count":
            int(
                ob.get(
                    "bid_count",
                    0
                )
            ),

        "ask_count":
            int(
                ob.get(
                    "ask_count",
                    0
                )
            ),

        "orderbook_dominance":
            ob.get(
                "dominance",
                "balanced"
            ),

        "orderbook_dominance_text":
            ob.get(
                "dominance_text",
                "균형"
            )
    }


# =========================================================
# 상승 후보
# =========================================================

def is_breakout(row):

    if not row:
        return False

    return bool(
        row.get(
            "breakout_qualified",
            False
        )
        and row.get(
            "zero_cross",
            False
        )
    )


def is_long_combined(row):

    return is_breakout(
        row
    )


# =========================================================
# 시장폭
# =========================================================

def top_daily_breadth(
    data
):

    result = {

        "positive": 0,
        "negative": 0,
        "zero": 0,
        "total": 0,

        "ratio": 0.0,

        "icon": "⚪",
        "state": "neutral"
    }

    if not data:
        return result

    for row in data:

        value = row.get(
            "change_value"
        )

        try:

            if value is None:
                continue

            value = float(value)

        except Exception:

            continue

        if pd.isna(value):
            continue

        result[
            "total"
        ] += 1

        if value > 0:

            result[
                "positive"
            ] += 1

        elif value < 0:

            result[
                "negative"
            ] += 1

        else:

            result[
                "zero"
            ] += 1

    total = result[
        "total"
    ]

    if total <= 0:
        return result

    result[
        "ratio"
    ] = (
        result["positive"]
        / total
        * 100
    )

    if (
        result["positive"]
        > result["negative"]
    ):

        result[
            "icon"
        ] = "☀️"

        result[
            "state"
        ] = "up"

    elif (
        result["positive"]
        < result["negative"]
    ):

        result[
            "icon"
        ] = "🌧️"

        result[
            "state"
        ] = "down"

    else:

        result[
            "icon"
        ] = "⚪"

        result[
            "state"
        ] = "neutral"

    return result


# =========================================================
# 업비트 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update_time
    global latest_upbit_orderbook

    log.info(
        f"========== 업비트 TOP{TOP_N} =========="
    )

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

            a = analyze(
                market,
                price
            )

        except Exception as e:

            log.exception(
                f"상세 분석 오류 "
                f"{market}: {e}"
            )

            a = None

        ob = (
            calculate_orderbook_amount(
                orderbooks.get(
                    market
                ),
                price
            )
        )

        rows.append(
            make_row(
                rank,
                coin,
                item[
                    "volume_24h"
                ],
                a,
                price,
                ob
            )
        )

        log.info(
            f"[{coin}] "
            f"ROC={rows[-1]['roc'].get('display', '-')} "
            f"0선돌파="
            f"{rows[-1].get('zero_cross_list', [])} "
            f"신호카운트="
            f"{rows[-1].get('signal_count', 0)}"
        )

    latest_upbit_data = rows

    latest_upbit_update_time = (
        kst()
    )

    breadth = (
        top_daily_breadth(
            latest_upbit_data
        )
    )

    log.info(
        f"업비트 완료 / "
        f"상승신호 "
        f"{sum(is_long_combined(x) for x in rows)}개"
    )

    log.info(
        f"TOP{TOP_N} 시장폭 / "
        f"양수 {breadth['positive']} / "
        f"음수 {breadth['negative']} / "
        f"0 {breadth['zero']} / "
        f"{breadth['icon']}"
    )


# =========================================================
# USDT/KRW
# =========================================================

def get_usdt_krw_internal():

    r = retry(
        requests.get,
        "https://api.upbit.com/v1/ticker?markets=KRW-USDT",
        timeout=15
    )

    if r is None:
        return None

    try:

        data = r.json()

        if not data:
            return None

        price = float(
            data[0][
                "trade_price"
            ]
        )

        return (
            price
            if price > 0
            else None
        )

    except Exception:

        return None


# =========================================================
# OKX
# =========================================================

def get_okx_ohlcv(
    inst,
    bar="1H",
    limit=200,
    before=None,
    include_current=False
):

    params = {

        "instId":
            inst,

        "bar":
            bar,

        "limit":
            min(
                max(
                    int(limit),
                    1
                ),
                200
            )
    }

    if before is not None:

        params[
            "before"
        ] = str(before)

    r = retry(
        requests.get,
        "https://www.okx.com/api/v5/market/candles",
        params=params,
        timeout=15
    )

    if r is None:
        return None

    try:

        data = r.json().get(
            "data",
            []
        )

        if not data:
            return None

        df = pd.DataFrame(
            data,
            columns=[
                "ts",
                "o",
                "h",
                "l",
                "c",
                "vol",
                "volCcy",
                "volCcyQuote",
                "confirm"
            ]
        )

        for col in [
            "ts",
            "o",
            "h",
            "l",
            "c",
            "vol",
            "volCcy",
            "volCcyQuote"
        ]:

            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

        if not include_current:

            df = df[
                df.confirm.astype(
                    str
                ) == "1"
            ]

        df["datetime"] = (
            pd.to_datetime(
                df.ts,
                unit="ms",
                utc=True
            )
            .dt.tz_convert(
                KST
            )
            .dt.tz_localize(
                None
            )
        )

        if not include_current:

            minutes = (
                get_okx_bar_minutes(
                    bar
                )
            )

            if minutes:

                current = (
                    get_current_candle_start(
                        minutes
                    )
                )

                df = df[
                    df.datetime < current
                ]

        if df.empty:
            return None

        return (
            df
            .sort_values("ts")
            .drop_duplicates("ts")
            .reset_index(
                drop=True
            )
        )

    except Exception as e:

        log.error(
            f"OKX 오류 {inst}: {e}"
        )

        return None


def history_okx(
    inst,
    bar,
    required=200
):

    all_df = None
    before = None

    for _ in range(
        MAX_HISTORY_CHUNKS
    ):

        df = get_okx_ohlcv(
            inst,
            bar,
            HISTORY_CHUNK,
            before
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
            .drop_duplicates("ts")
            .sort_values("ts")
            .reset_index(
                drop=True
            )
        )

        if len(all_df) >= required:

            return all_df

        before = int(
            all_df.ts.iloc[0]
        )

    return all_df


def get_okx_tickers():

    global okx_ticker_cache

    r = retry(
        requests.get,
        "https://www.okx.com/api/v5/market/tickers",
        params={
            "instType":
                "SWAP"
        },
        timeout=15
    )

    if r is None:
        return {}

    try:

        result = {}

        for x in r.json().get(
            "data",
            []
        ):

            inst = x.get(
                "instId",
                ""
            )

            if not inst.endswith(
                "-USDT-SWAP"
            ):

                continue

            try:

                last = float(
                    x.get(
                        "last",
                        0
                    )
                )

            except Exception:

                last = 0

            if last > 0:

                result[
                    inst
                ] = {
                    "last":
                        last
                }

        okx_ticker_cache = result

        return result

    except Exception:

        return {}


def get_okx_symbols():

    r = retry(
        requests.get,
        "https://www.okx.com/api/v5/public/instruments",
        params={
            "instType":
                "SWAP"
        },
        timeout=15
    )

    if r is None:
        return []

    try:

        return [

            x[
                "instId"
            ]

            for x in r.json().get(
                "data",
                []
            )

            if x.get(
                "instId",
                ""
            ).endswith(
                "-USDT-SWAP"
            )

            and x.get(
                "state"
            ) == "live"

        ]

    except Exception:

        return []


def get_okx_cached_price(
    inst
):

    try:

        item = (
            okx_ticker_cache.get(
                inst
            )
        )

        if not item:
            return None

        price = float(
            item.get(
                "last",
                0
            )
        )

        return (
            price
            if price > 0
            else None
        )

    except Exception:

        return None


# =========================================================
# OKX 업데이트
# =========================================================

def update_okx(
    usdt
):

    global latest_okx_data
    global latest_okx_update_time

    if not usdt or usdt <= 0:
        return False

    tickers = (
        get_okx_tickers()
    )

    if not tickers:
        return False

    symbols = (
        get_okx_symbols()
    )

    if not symbols:
        return False

    symbols = [
        x
        for x in symbols
        if x in tickers
    ]

    volumes = {}

    for symbol in symbols:

        df = get_okx_ohlcv(
            symbol,
            "1H",
            VOLUME_HOURS
        )

        if (
            df is None
            or df.empty
        ):

            continue

        try:

            volume = (
                pd.to_numeric(
                    df[
                        "volCcyQuote"
                    ],
                    errors="coerce"
                ).sum()
            )

            volume = (
                float(volume)
                * float(usdt)
            )

            if volume > 0:

                volumes[
                    symbol
                ] = volume

        except Exception:

            continue

    top = sorted(
        volumes,
        key=volumes.get,
        reverse=True
    )[:TOP_N]

    rows = []

    for rank, symbol in enumerate(
        top,
        1
    ):

        coin = symbol.replace(
            "-USDT-SWAP",
            ""
        )

        price = (
            get_okx_cached_price(
                symbol
            )
        )

        try:

            a = analyze(
                symbol,
                price
            )

        except Exception:

            a = None

        rows.append(
            make_row(
                rank,
                coin,
                volumes[symbol],
                a,
                price
            )
        )

    latest_okx_data = rows

    latest_okx_update_time = (
        kst()
    )

    return True


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    global latest_upbit_data
    global latest_okx_data
    global latest_usdt_krw_internal

    if not update_lock.acquire(
        False
    ):

        log.warning(
            "이전 조회 진행 중"
        )

        return

    try:

        if USE_UPBIT == "Y":

            try:

                update_upbit()

            except Exception as e:

                log.exception(
                    f"업비트 업데이트 오류: {e}"
                )

        else:

            latest_upbit_data = []

        if USE_OKX == "Y":

            try:

                usdt = (
                    get_usdt_krw_internal()
                )

                if usdt:

                    latest_usdt_krw_internal = (
                        usdt
                    )

                    update_okx(
                        usdt
                    )

            except Exception as e:

                log.exception(
                    f"OKX 업데이트 오류: {e}"
                )

        else:

            latest_okx_data = []

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

        return (
            f"{price / 100000000:.2f}억"
        )

    if price >= 10000:

        return f"{price:,.0f}"

    if price >= 1:

        return f"{price:,.2f}"

    return f"{price:.6f}"


# =========================================================
# 시장 등락률
# =========================================================

def market_change_html(
    value
):

    if value is None:

        return (
            '<span class="market-zero">'
            '-'
            '</span>'
        )

    try:

        value = float(value)

    except Exception:

        return (
            '<span class="market-zero">'
            '-'
            '</span>'
        )

    if value > 0:

        return (
            '<span class="market-up">'
            f'▲+{value:.2f}%'
            '</span>'
        )

    if value < 0:

        return (
            '<span class="market-down">'
            f'▼{value:.2f}%'
            '</span>'
        )

    return (
        '<span class="market-zero">'
        '0.00%'
        '</span>'
    )


# =========================================================
# BTC
# =========================================================

def get_market_row(
    coin
):

    for row in latest_upbit_data:

        if row.get(
            "name"
        ) == coin:

            return row

    return None


# =========================================================
# BTC 시장 시황
# =========================================================

def market_summary_html():

    btc = get_market_row(
        "BTC"
    )

    if btc is None:

        btc_price = "-"
        btc_change = "-"
        btc_filter = "⚪"
        btc_daily = "⚪"
        breadth_icon = "⚪"

    else:

        btc_price = (
            format_market_price(
                btc.get(
                    "current_price"
                )
            )
        )

        btc_change = (
            market_change_html(
                btc.get(
                    "change_value"
                )
            )
        )

        btc_filter = (
            "☀️"
            if btc.get(
                "filter_pass",
                False
            )
            else "⚪"
        )

        change = btc.get(
            "change_value"
        )

        if change is not None:

            btc_daily = (
                "☀️"
                if change > 0
                else
                "🌧️"
                if change < 0
                else
                "⚪"
            )

        else:

            btc_daily = "⚪"

        breadth = (
            top_daily_breadth(
                latest_upbit_data
            )
        )

        breadth_icon = (
            breadth.get(
                "icon",
                "⚪"
            )
        )

    breadth = (
        top_daily_breadth(
            latest_upbit_data
        )
    )

    return f"""

    <div class="market-summary">

        <div class="market-title">

            <span class="market-title-main">
                ₿ BTC 시장 시황
            </span>

            <span class="market-title-sub">
                활성 ROC
                {get_filter_setting_text()}
                ·
                전체 0선 돌파
            </span>

        </div>

        <div class="btc-top">

            <span class="btc-name">
                ₿ BTC
            </span>

            <span class="btc-price">
                {btc_price}
            </span>

            <span class="btc-change">
                {btc_change}
            </span>

        </div>

        <div class="btc-bottom">

            <div class="btc-info-box">

                <div class="btc-info-title">
                    활성 ROC 필터
                </div>

                <div class="btc-info-value">
                    {btc_filter}
                </div>

                <div class="btc-info-sub">
                    {get_filter_setting_text()}
                </div>

            </div>

            <div class="btc-info-box">

                <div class="btc-info-title">
                    BTC · 당일
                </div>

                <div class="btc-info-value">
                    {btc_daily}
                </div>

                <div class="btc-info-sub">
                    {btc.get("change_value", "-")
                    if btc else "-"}%
                </div>

            </div>

            <div class="btc-info-box">

                <div class="btc-info-title">
                    TOP{TOP_N} · 당일
                </div>

                <div class="btc-info-value">
                    {breadth_icon}
                </div>

                <div class="btc-info-sub">
                    양수 {breadth["positive"]}
                    /
                    음수 {breadth["negative"]}
                </div>

            </div>

        </div>

    </div>

    """


# =========================================================
# ROC HTML
# =========================================================

def roc_html(
    r
):

    if not r:

        return (
            '<div class="roc-cell">'
            '⚪ 0'
            '</div>'
        )

    value = r.get(
        "roc5"
    )

    if value is None:

        return (
            '<div class="roc-cell">'
            '⚪ 0'
            '</div>'
        )

    try:

        value = float(value)

    except Exception:

        return (
            '<div class="roc-cell">'
            '⚪ 0'
            '</div>'
        )

    if r.get(
        "long_breakout",
        False
    ):

        count = int(
            r.get(
                "long_breakout_count",
                0
            )
        )

        return (
            '<div class="roc-cell">'
            '<span class="roc-positive">'
            f'🚀{count}'
            '</span>'
            '</div>'
        )

    if (
        r.get(
            "state"
        ) == "filter_wait"
    ):

        return (
            '<div class="roc-cell">'
            '<span class="roc-wait">'
            '🟡'
            '</span>'
            '</div>'
        )

    if value > 0:

        count = int(
            r.get(
                "roc5_count",
                0
            )
        )

        return (
            '<div class="roc-cell">'
            '<span class="roc-positive">'
            f'🟢+({count})'
            '</span>'
            '</div>'
        )

    if value < 0:

        count = int(
            r.get(
                "roc5_negative_count",
                0
            )
        )

        return (
            '<div class="roc-cell">'
            '<span class="roc-negative">'
            f'🔴-({count})'
            '</span>'
            '</div>'
        )

    return (
        '<div class="roc-cell">'
        '⚪ 0'
        '</div>'
    )


# =========================================================
# 신호 HTML
#
# ★ 로켓 작게
# ★ 옆에 카운팅
# ★ 0부터 시작
# ★ 유지되는 동안 계속 표시
# =========================================================

def signal_html(
    row
):

    if not row:

        return (
            '<span class="muted">-</span>'
        )

    if row.get(
        "signal_active",
        False
    ):

        count = int(
            row.get(
                "signal_count",
                0
            )
        )

        return (
            '<span class="signal-active">'
            '<span class="signal-rocket">'
            '🚀'
            '</span>'
            '<span class="signal-count">'
            f'{count}'
            '</span>'
            '</span>'
        )

    return (
        '<span class="muted">-</span>'
    )


# =========================================================
# 호가 HTML
# =========================================================

def orderbook_html(
    row
):

    if not row:

        return (
            '<div class="orderbook-empty">'
            '호가 정보 없음'
            '</div>'
        )

    bid_amount = float(
        row.get(
            "bid_amount",
            0
        )
    )

    ask_amount = float(
        row.get(
            "ask_amount",
            0
        )
    )

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

    if (
        bid_amount <= 0
        and ask_amount <= 0
    ):

        return (
            '<div class="orderbook-empty">'
            '호가 정보 없음'
            '</div>'
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
                {format_volume(ask_amount)}
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
                {format_volume(bid_amount)}
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

            <span class="orderbook-range">
                ±{ORDERBOOK_RANGE * 100:.0f}%
            </span>

            {dominance_html}

        </div>

    </div>

    """


# =========================================================
# 행 HTML
# =========================================================

def rows_html(
    data
):

    out = []

    for x in data:

        cls = (
            "breakout-qualified"
            if is_breakout(x)
            else ""
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

                    {filter_html(
                        x.get(
                            "roc_filter_1h"
                        ),
                        x.get(
                            "roc_filter_high"
                        )
                    )}

                </td>

                <td>

                    {roc_html(
                        x.get(
                            "roc"
                        )
                    )}

                </td>

                <td class="signal-cell">

                    {signal_html(
                        x
                    )}

                </td>

            </tr>

            <tr class="orderbook-subrow">

                <td colspan="6">

                    {orderbook_html(x)}

                </td>

            </tr>

            """
        )

    return "".join(out)


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
        if is_long_combined(x)
    ]

    return f"""

    <div class="section-title long-title">

        <span class="section-title-main">
            🚀 상승 신호
        </span>

        <span class="section-title-sub">
            활성 ROC 전체 ≥0
            · 활성 ROC 0선 상승 돌파
            · 당일 ≥0%
            · {kst()} KST
        </span>

    </div>

    {table_html(rows)}

    """


# =========================================================
# 전체 TOP
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
    background:#0d1014;
    color:#eee;
    font-family:
        -apple-system,
        BlinkMacSystemFont,
        "Segoe UI",
        Arial,
        sans-serif;
    font-size:8px;
    padding:2px 2px 8px;
}

h1{
    margin:1px 2px 2px;
    font-size:12px;
    line-height:14px;
}

.market-title,
.section-title{
    display:flex;
    align-items:center;
    gap:5px;
    width:100%;
    min-height:18px;
    color:#fff;
    font-size:8px;
    line-height:10px;
    font-weight:900;
    padding:3px 5px;
    border-left:3px solid #39e875;
    background:rgba(57,232,117,.08);
    border-radius:3px;
    white-space:nowrap;
    overflow:hidden;
}

.market-title{
    margin-bottom:4px;
}

.section-title{
    margin:5px 0 4px;
}

.market-title-main,
.section-title-main{
    color:#fff;
    font-size:8px;
    line-height:10px;
    font-weight:900;
    flex:none;
}

.market-title-sub,
.section-title-sub{
    color:#7f8791;
    font-size:5.5px;
    line-height:8px;
    font-weight:700;
    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;
}

.long-title{
    border-left-color:#39e875;
}

.market-summary{
    width:100%;
    margin:2px 0 3px;
    padding:3px 4px;
    border-top:1px solid #242a31;
    border-bottom:1px solid #242a31;
    background:#101419;
    overflow:hidden;
}

.btc-top{
    display:flex;
    align-items:center;
    width:100%;
    min-height:16px;
    gap:4px;
    white-space:nowrap;
    overflow:hidden;
}

.btc-name{
    flex:none;
    width:34px;
    font-size:6.5px;
    font-weight:900;
}

.btc-price{
    flex:1;
    min-width:0;
    color:#e8edf2;
    font-size:6px;
    font-weight:800;
    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;
}

.btc-change{
    flex:none;
    width:64px;
    font-size:9.5px;
    font-weight:900;
    text-align:right;
    white-space:nowrap;
}

.btc-bottom{
    display:grid;
    grid-template-columns:1fr 1fr 1fr;
    width:100%;
    min-height:70px;
    gap:5px;
    overflow:hidden;
}

.btc-info-box{
    min-width:0;
    min-height:70px;
    padding:5px 6px;
    border:1px solid #292f36;
    border-radius:6px;
    background:#14181d;
    overflow:hidden;
    display:flex;
    flex-direction:column;
    align-items:center;
    justify-content:center;
    text-align:center;
}

.btc-info-title{
    width:100%;
    color:#7f8791;
    font-size:6px;
    line-height:8px;
    font-weight:800;
    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;
}

.btc-info-value{
    width:100%;
    font-size:28px;
    line-height:30px;
    font-weight:900;
}

.btc-info-sub{
    width:100%;
    color:#737b85;
    font-size:6px;
    line-height:8px;
    font-weight:800;
    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;
}

.market-up,
.roc-positive,
.up{
    color:#39e875!important;
    font-weight:900;
}

.market-down,
.roc-negative,
.down{
    color:#ff5555!important;
    font-weight:900;
}

.roc-wait{
    color:#ffc857!important;
}

.market-zero,
.zero,
.muted{
    color:#68717b!important;
}

.status{
    display:flex;
    justify-content:center;
    gap:9px;
    margin:2px 2px 3px;
    padding:2px 0;
    border-top:1px solid #242a31;
    border-bottom:1px solid #242a31;
    font-size:6px;
    font-weight:800;
}

.y{
    color:#39e875!important;
}

.n{
    color:#ff5555!important;
}

.signal-cell{
    text-align:center!important;
    vertical-align:middle;
}

/* =====================================================
   신호 로켓 + 카운팅
   ===================================================== */

.signal-active{
    display:inline-flex;
    align-items:center;
    justify-content:center;
    gap:2px;
    min-height:21px;
    white-space:nowrap;
}

.signal-rocket{
    font-size:9px;
    line-height:10px;
    font-weight:900;
}

.signal-count{
    font-size:7px;
    line-height:9px;
    font-weight:900;
    color:#39e875;
}

.table-wrap{
    width:100%;
    overflow:hidden;
    border-radius:5px;
    border:1px solid #272d34;
    background:#171b20;
}

table{
    width:100%;
    table-layout:fixed;
    border-collapse:collapse;
    background:#171b20;
}

thead{
    background:#111419;
}

th{
    height:17px;
    padding:1px;
    border-bottom:1px solid #292f36;
    color:#7f8791;
    font-size:5px;
    font-weight:700;
    text-align:center;
}

td{
    height:25px;
    padding:1px;
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
    width:15%;
}

th:nth-child(4),
td:nth-child(4){
    width:29%;
}

th:nth-child(5),
td:nth-child(5){
    width:18%;
}

th:nth-child(6),
td:nth-child(6){
    width:17%;
}

td:nth-child(1){
    color:#8b929b;
    font-size:6px;
    font-weight:700;
}

.coin{
    text-align:left!important;
}

.coin b{
    display:block;
    width:100%;
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
    white-space:nowrap;
    overflow:hidden;
}

.vol{
    font-size:6px;
    font-weight:800;
    white-space:nowrap;
}

.ema{
    text-align:center!important;
    line-height:8px;
    white-space:normal;
    overflow:visible;
}

.filter-detail{
    display:flex;
    flex-direction:column;
    align-items:flex-start;
    justify-content:center;
    gap:1px;
    width:100%;
    white-space:nowrap;
    overflow:visible;
}

.filter-line{
    display:flex;
    align-items:center;
    justify-content:flex-start;
    width:100%;
    white-space:nowrap;
    overflow:visible;
}

.filter-timeframe{
    display:inline-block;
    flex:none;
    width:12px;
    color:#d7dce1;
    font-size:4.5px;
    font-weight:900;
    text-align:left;
}

.roc-filter-all{
    display:flex;
    align-items:center;
    justify-content:flex-start;
    gap:2px;
    width:100%;
    line-height:8px;
    white-space:nowrap;
    overflow:visible;
}

.roc-item{
    display:inline-flex;
    align-items:center;
    justify-content:center;
    flex:none;
    white-space:nowrap;
    font-size:4.5px;
    line-height:8px;
    font-weight:800;
}

.roc-active{
    color:#aeb7c0;
}

.roc-disabled{
    color:#555d67;
    opacity:.55;
}

/* =====================================================
   ROC 카운팅 크기 통일
   ===================================================== */

.roc-cell{
    display:flex;
    align-items:center;
    justify-content:center;
    min-height:21px;
    white-space:nowrap;
    font-size:5.8px;
    line-height:8px;
}

.roc-cell span{
    font-size:5.8px!important;
    line-height:8px;
    font-weight:900;
}

.breakout-qualified{
    background:rgba(57,232,117,.08);
}

.empty{
    height:30px;
    padding:8px;
    color:#555d67;
    font-size:6px;
}

.orderbook-subrow{
    background:#101419!important;
}

.orderbook-subrow td{
    height:auto!important;
    padding:3px 4px 4px!important;
}

.orderbook-wrap{
    width:100%;
    padding:1px 0;
    overflow:hidden;
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
    width:100%;
    min-height:13px;
}

.orderbook-label{
    font-size:5.5px;
    font-weight:900;
    white-space:nowrap;
}

.ask-label{
    color:#ff5555;
}

.bid-label{
    color:#39e875;
}

.orderbook-amount{
    font-size:5.5px;
    font-weight:900;
    text-align:right;
    white-space:nowrap;
}

.ask-amount{
    color:#ff7777;
}

.bid-amount{
    color:#39e875;
}

.orderbook-bar-box{
    position:relative;
    width:100%;
    height:7px;
    background:#252b31;
    border-radius:4px;
    overflow:hidden;
}

.orderbook-bar{
    height:100%;
    min-width:1px;
    border-radius:4px;
}

.ask-bar{
    background:#d94a4a;
}

.bid-bar{
    background:#39e875;
}

.orderbook-ratio{
    font-size:5.5px;
    font-weight:900;
    text-align:right;
    white-space:nowrap;
}

.ask-ratio{
    color:#ff7777;
}

.bid-ratio{
    color:#39e875;
}

.orderbook-bottom{
    display:flex;
    align-items:center;
    justify-content:flex-end;
    gap:7px;
    min-height:11px;
    margin-top:2px;
    font-size:5px;
    font-weight:800;
}

.orderbook-range{
    color:#5e6670;
}

.ob-dominance{
    font-size:5.5px;
    font-weight:900;
}

.bid-dominance{
    color:#39e875;
}

.ask-dominance{
    color:#ff5555;
}

.balanced-dominance{
    color:#9aa1aa;
}

.orderbook-empty{
    width:100%;
    padding:3px 0;
    color:#555d67;
    font-size:5px;
    text-align:center;
}

@media(max-width:380px){

    body{
        padding:1px 1px 6px;
    }

    h1{
        font-size:11px;
        line-height:13px;
    }

    .market-title,
    .section-title{
        min-height:17px;
        gap:4px;
        font-size:7px;
        padding:3px 4px;
    }

    .market-title-main,
    .section-title-main{
        font-size:7px;
    }

    .market-title-sub,
    .section-title-sub{
        font-size:4.8px;
    }

    .btc-name{
        width:30px;
        font-size:5.8px;
    }

    .btc-price{
        font-size:5.4px;
    }

    .btc-change{
        width:56px;
        font-size:8.5px;
    }

    .btc-bottom{
        min-height:58px;
        gap:3px;
    }

    .btc-info-box{
        min-height:58px;
        padding:3px 4px;
    }

    .btc-info-title{
        font-size:4.8px;
    }

    .btc-info-value{
        font-size:23px;
        line-height:25px;
    }

    .btc-info-sub{
        font-size:5px;
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

    .roc-filter-all{
        gap:1px;
        line-height:7px;
        overflow:visible;
    }

    .roc-item{
        font-size:3.8px;
        line-height:7px;
    }

    .roc-cell span{
        font-size:5.8px!important;
        line-height:8px;
    }

    .signal-active{
        gap:1px;
        min-height:19px;
    }

    .signal-rocket{
        font-size:8px;
        line-height:9px;
    }

    .signal-count{
        font-size:6.5px;
        line-height:8px;
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

    .orderbook-bar-box{
        height:6px;
    }

    .orderbook-bottom{
        gap:5px;
        font-size:4.5px;
    }

    .ob-dominance{
        font-size:5px;
    }

}

@media(min-width:601px){

    body{
        max-width:900px;
        margin:auto;
        padding:8px;
        font-size:10px;
    }

    h1{
        font-size:15px;
        line-height:20px;
    }

    .market-title,
    .section-title{
        min-height:23px;
        gap:6px;
        font-size:9px;
        padding:4px 6px;
    }

    .market-title-main,
    .section-title-main{
        font-size:9px;
    }

    .market-title-sub,
    .section-title-sub{
        font-size:6px;
    }

    .btc-name{
        width:42px;
        font-size:8px;
    }

    .btc-price{
        font-size:8px;
    }

    .btc-bottom{
        min-height:75px;
        gap:6px;
    }

    .btc-info-box{
        min-height:75px;
        padding:6px 8px;
    }

    .btc-info-title{
        font-size:7px;
    }

    .btc-info-value{
        font-size:32px;
        line-height:34px;
    }

    .btc-info-sub{
        font-size:7px;
    }

    th{
        height:26px;
        font-size:7px;
    }

    td{
        height:38px;
        padding:3px;
    }

    .coin b{
        font-size:9px;
        line-height:11px;
    }

    .coin small{
        font-size:7px;
    }

    .vol{
        font-size:8px;
    }

    .filter-timeframe{
        width:17px;
        font-size:6px;
    }

    .roc-filter-all{
        gap:3px;
        line-height:10px;
        overflow:visible;
    }

    .roc-item{
        font-size:5.5px;
        line-height:10px;
    }

    .roc-cell span{
        font-size:7px!important;
        line-height:10px;
    }

    .signal-active{
        gap:2px;
        min-height:28px;
    }

    .signal-rocket{
        font-size:12px;
        line-height:13px;
    }

    .signal-count{
        font-size:8px;
        line-height:10px;
    }

    .orderbook-row{
        grid-template-columns:
            55px
            60px
            minmax(80px,1fr)
            45px;
        gap:6px;
    }

    .orderbook-label,
    .orderbook-amount,
    .orderbook-ratio{
        font-size:7px;
    }

    .orderbook-bar-box{
        height:9px;
    }

    .orderbook-bottom{
        gap:9px;
        font-size:6px;
    }

    .ob-dominance{
        font-size:6.5px;
    }

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

    status_1h_class = (
        "y"
        if USE_1H_ROC_FILTER == "Y"
        else "n"
    )

    status_4h_class = (
        "y"
        if USE_4H_ROC_FILTER == "Y"
        else "n"
    )

    status = f"""

    <div class="status">

        <span>
            업비트 :
            <b class="y">
                {USE_UPBIT}
            </b>
        </span>

        <span>
            OKX :
            <b class="n">
                {USE_OKX}
            </b>
        </span>

        <span>
            1H :
            <b class="{status_1h_class}">
                {
                    get_enabled_filter_text("1H")
                    if USE_1H_ROC_FILTER == "Y"
                    else "N"
                }
            </b>
        </span>

        <span>
            4H :
            <b class="{status_4h_class}">
                {
                    get_enabled_filter_text("4H")
                    if USE_4H_ROC_FILTER == "Y"
                    else "N"
                }
            </b>
        </span>

        <span>
            0선 :
            <b class="y">
                전체 활성
            </b>
        </span>

    </div>

    """

    sections = ""

    if USE_UPBIT == "Y":

        sections += focus_section(
            latest_upbit_data
        )

    if USE_UPBIT == "Y":

        sections += section(
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
            content="#0d1014"
        >

        <title>
            ROC
            ·
            {get_filter_setting_text()}
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
# Startup
# =========================================================

@app.on_event("startup")
def startup():

    if USE_UPBIT not in (
        "Y",
        "N"
    ):

        raise ValueError(
            "USE_UPBIT은 Y/N만 가능합니다."
        )

    if USE_OKX not in (
        "Y",
        "N"
    ):

        raise ValueError(
            "USE_OKX는 Y/N만 가능합니다."
        )

    validate_timeframe()

    log.info(
        "========================================"
    )

    log.info(
        "ROC 개별 필터 시스템 시작"
    )

    log.info(
        f"1H 필터 사용 = "
        f"{USE_1H_ROC_FILTER}"
    )

    log.info(
        f"4H 필터 사용 = "
        f"{USE_4H_ROC_FILTER}"
    )

    log.info(
        "----------------------------------------"
    )

    log.info(
        f"1H ROC5 = {USE_1H_ROC5}"
    )

    log.info(
        f"1H ROC10 = {USE_1H_ROC10}"
    )

    log.info(
        f"1H ROC20 = {USE_1H_ROC20}"
    )

    log.info(
        f"1H ROC50 = {USE_1H_ROC50}"
    )

    log.info(
        f"1H ROC200 = {USE_1H_ROC200}"
    )

    log.info(
        "----------------------------------------"
    )

    log.info(
        f"4H ROC5 = {USE_4H_ROC5}"
    )

    log.info(
        f"4H ROC10 = {USE_4H_ROC10}"
    )

    log.info(
        f"4H ROC20 = {USE_4H_ROC20}"
    )

    log.info(
        f"4H ROC50 = {USE_4H_ROC50}"
    )

    log.info(
        f"4H ROC200 = {USE_4H_ROC200}"
    )

    log.info(
        "========================================"
    )

    log.info(
        f"실제 활성 필터: "
        f"{get_filter_setting_text()}"
    )

    log.info(
        "Y 시간봉 + Y ROC만 "
        "실제 필터 조건에 사용"
    )

    log.info(
        "N ROC도 대시보드에는 "
        "항상 전체 상태 표시"
    )

    log.info(
        "1H/4H 시간봉 N이어도 "
        "대시보드 ROC 상태는 계속 계산"
    )

    log.info(
        "신호 기준: "
        "활성 ROC 중 하나라도 "
        "0선 상승 돌파"
    )

    log.info(
        f"ROC 로켓 표시: ROC{ROC_PERIOD} "
        f"{format_timeframe(ROC_TIMEFRAME)}"
    )

    log.info(
        "활성 ROC 전체 >= 0 "
        "+ 활성 ROC 중 하나 0선 상승 돌파 "
        "+ 당일 >= 0% → 🚀"
    )

    log.info(
        "신호 카운팅: "
        "돌파 캔들 0 "
        "→ 다음 캔들 1 "
        "→ 이후 2, 3, 4..."
    )

    log.info(
        "========================================"
    )

    log.info(
        f"호가 ±{ORDERBOOK_RANGE * 100:.0f}%"
    )

    log.info(
        f"호가 최대 {ORDERBOOK_COUNT}호가"
    )

    log.info(
        f"호가 우세 기준 "
        f"{ORDERBOOK_DOMINANCE_GAP:.1f}%p"
    )

    log.info(
        f"TOP{TOP_N} / "
        f"업데이트 {UPDATE_MINUTES}분"
    )

    log.info(
        "========================================"
    )

    # -----------------------------------------------------
    # 최초 업데이트
    # -----------------------------------------------------

    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    # -----------------------------------------------------
    # 스케줄러
    # -----------------------------------------------------

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
