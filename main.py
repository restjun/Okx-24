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

KST = ZoneInfo("Asia/Seoul")


# =========================================================
# 업비트 호가 설정
# =========================================================

ORDERBOOK_RANGE = 0.01
ORDERBOOK_COUNT = 30
ORDERBOOK_DOMINANCE_GAP = 5.0


# =========================================================
# ROC 필터 시간봉
# =========================================================

ROC_FILTER_TIMEFRAME = 60
ROC_FILTER_HIGH_TIMEFRAME = 240


# =========================================================
# ROC 필터 개별 사용 여부
#
# Y = 사용
# N = 사용 안 함
#
# 1H
# =========================================================

USE_1H_ROC5 = "Y"
USE_1H_ROC10 = "Y"
USE_1H_ROC20 = "Y"
USE_1H_ROC50 = "Y"
USE_1H_ROC200 = "Y"


# =========================================================
# 4H
# =========================================================

USE_4H_ROC5 = "N"
USE_4H_ROC10 = "N"
USE_4H_ROC20 = "N"
USE_4H_ROC50 = "N"
USE_4H_ROC200 = "N"


# =========================================================
# ROC 기간
# =========================================================

ROC_FILTER_PERIODS = [
    5,
    10,
    20,
    50,
    200
]


# =========================================================
# ROC 로켓
#
# ROC5를 기준으로 돌파 카운트
# =========================================================

ROC_TIMEFRAME = 60
ROC_PERIOD = 5


# =========================================================
# ROC 로켓 표시 제한
# 사실상 제한 없음
# =========================================================

BREAKOUT_MAX_COUNT = 999999


# =========================================================
# 지원 시간봉
# =========================================================

SUPPORTED_UPBIT_TIMEFRAMES = {
    5, 15, 30, 60, 240
}

SUPPORTED_OKX_TIMEFRAMES = {
    5, 15, 30, 60, 120,
    240, 360, 480, 720, 1440
}


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
# OKX 내부 환산용 USDT/KRW
# =========================================================

latest_usdt_krw_internal = 0


# =========================================================
# 업비트 호가 캐시
# =========================================================

latest_upbit_orderbook = {}


# =========================================================
# OKX 캐시
# =========================================================

okx_ticker_cache = {}
okx_1h_cache = {}
okx_1h_cache_time = "-"


# =========================================================
# ROC 개별 설정
# =========================================================

ROC_USE_SETTINGS_1H = {
    5: USE_1H_ROC5,
    10: USE_1H_ROC10,
    20: USE_1H_ROC20,
    50: USE_1H_ROC50,
    200: USE_1H_ROC200
}


ROC_USE_SETTINGS_4H = {
    5: USE_4H_ROC5,
    10: USE_4H_ROC10,
    20: USE_4H_ROC20,
    50: USE_4H_ROC50,
    200: USE_4H_ROC200
}


# =========================================================
# ROC 사용 여부
# =========================================================

def is_roc_enabled(
    timeframe,
    period
):

    try:

        timeframe = int(timeframe)
        period = int(period)

    except Exception:

        return False

    if timeframe == 60:

        return (
            ROC_USE_SETTINGS_1H
            .get(period, "N")
            == "Y"
        )

    if timeframe == 240:

        return (
            ROC_USE_SETTINGS_4H
            .get(period, "N")
            == "Y"
        )

    return False


# =========================================================
# 활성 ROC 기간
# =========================================================

def get_enabled_periods(
    timeframe
):

    result = []

    for period in ROC_FILTER_PERIODS:

        if is_roc_enabled(
            timeframe,
            period
        ):

            result.append(
                period
            )

    return result


# =========================================================
# 활성 ROC 기간 문자열
# =========================================================

def get_enabled_period_text(
    timeframe
):

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
# 전체 ROC 기간 문자열
# =========================================================

def get_roc_filter_period_text():

    return "/".join(
        str(x)
        for x in ROC_FILTER_PERIODS
    )


def get_roc_filter_period_text_long():

    return "/".join(
        f"ROC{x}"
        for x in ROC_FILTER_PERIODS
    )


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


# =========================================================
# ROC 설정 텍스트
# =========================================================

def get_roc_filter_setting_text():

    return (
        f"{format_timeframe(ROC_FILTER_TIMEFRAME)}:"
        f"{get_enabled_period_text(ROC_FILTER_TIMEFRAME)} / "
        f"{format_timeframe(ROC_FILTER_HIGH_TIMEFRAME)}:"
        f"{get_enabled_period_text(ROC_FILTER_HIGH_TIMEFRAME)}"
    )


def get_roc_text():

    return (
        f"ROC{ROC_PERIOD}"
        f"({format_timeframe(ROC_TIMEFRAME)})"
    )


# =========================================================
# 공통
# =========================================================

def kst():

    return datetime.now(KST).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


# =========================================================
# OKX BAR
# =========================================================

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

def get_current_candle_start(
    minutes
):

    minutes = int(minutes)

    now = datetime.now(KST)

    total = (
        now.hour * 60
        + now.minute
    )

    block = (
        total // minutes
    ) * minutes

    day_offset, block = divmod(
        block,
        1440
    )

    current = now.replace(
        hour=block // 60,
        minute=block % 60,
        second=0,
        microsecond=0
    )

    if day_offset:

        current -= pd.Timedelta(
            days=day_offset
        )

    return current.replace(
        tzinfo=None
    )


# =========================================================
# 설정 검증
# =========================================================

def validate_timeframe():

    global ROC_FILTER_TIMEFRAME
    global ROC_FILTER_HIGH_TIMEFRAME
    global ROC_TIMEFRAME

    try:

        ROC_FILTER_TIMEFRAME = int(
            ROC_FILTER_TIMEFRAME
        )

        ROC_FILTER_HIGH_TIMEFRAME = int(
            ROC_FILTER_HIGH_TIMEFRAME
        )

        ROC_TIMEFRAME = int(
            ROC_TIMEFRAME
        )

    except Exception:

        raise ValueError(
            "ROC 시간봉 설정은 숫자여야 합니다."
        )

    if (
        ROC_FILTER_TIMEFRAME
        not in SUPPORTED_UPBIT_TIMEFRAMES
    ):

        raise ValueError(
            f"ROC_FILTER_TIMEFRAME 오류: "
            f"{ROC_FILTER_TIMEFRAME}"
        )

    if (
        ROC_FILTER_HIGH_TIMEFRAME
        not in SUPPORTED_UPBIT_TIMEFRAMES
    ):

        raise ValueError(
            f"ROC_FILTER_HIGH_TIMEFRAME 오류: "
            f"{ROC_FILTER_HIGH_TIMEFRAME}"
        )

    if (
        ROC_TIMEFRAME
        not in SUPPORTED_UPBIT_TIMEFRAMES
    ):

        raise ValueError(
            f"ROC_TIMEFRAME 오류: "
            f"{ROC_TIMEFRAME}"
        )

    if (
        get_okx_bar(
            ROC_FILTER_TIMEFRAME
        )
        is None
    ):

        raise ValueError(
            "OKX에서 지원하지 않는 ROC 필터 시간봉"
        )

    if (
        get_okx_bar(
            ROC_FILTER_HIGH_TIMEFRAME
        )
        is None
    ):

        raise ValueError(
            "OKX에서 지원하지 않는 ROC 고시간봉"
        )

    if (
        get_okx_bar(
            ROC_TIMEFRAME
        )
        is None
    ):

        raise ValueError(
            "OKX에서 지원하지 않는 ROC 시간봉"
        )

    settings = [

        USE_1H_ROC5,
        USE_1H_ROC10,
        USE_1H_ROC20,
        USE_1H_ROC50,
        USE_1H_ROC200,

        USE_4H_ROC5,
        USE_4H_ROC10,
        USE_4H_ROC20,
        USE_4H_ROC50,
        USE_4H_ROC200

    ]

    for value in settings:

        if value not in ("Y", "N"):

            raise ValueError(
                "ROC 개별 사용 설정은 "
                "Y 또는 N만 가능합니다."
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
                    f"[HTTP {r.status_code}] {url}"
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
                f"[API 오류] {url}: {e}"
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
                    x["acc_trade_price_24h"]
                )

                price = float(
                    x["trade_price"]
                )

            except Exception:

                continue

            if (
                volume > 0
                and price > 0
            ):

                result.append({

                    "market": market,

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

            if r.status_code != 200:
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
# 호가 금액
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

    lower_price = (
        current_price
        * (1.0 - ORDERBOOK_RANGE)
    )

    upper_price = (
        current_price
        * (1.0 + ORDERBOOK_RANGE)
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
            "market":
                market,

            "count":
                min(
                    max(
                        int(count),
                        1
                    ),
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


# =========================================================
# 업비트 히스토리
# =========================================================

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

        if df is None or df.empty:
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
# 업비트 현재 ROC 데이터
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
            f"업비트 현재 ROC 오류 "
            f"{market}: {e}"
        )

        return df


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
            .dt.tz_convert(KST)
            .dt.tz_localize(None)
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
            .reset_index(drop=True)
        )

    except Exception as e:

        log.error(
            f"OKX {inst} {bar} 오류: {e}"
        )

        return None


# =========================================================
# OKX 히스토리
# =========================================================

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

        if df is None or df.empty:
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


# =========================================================
# OKX ticker
# =========================================================

def get_okx_tickers():

    global okx_ticker_cache

    r = retry(
        requests.get,
        "https://www.okx.com/api/v5/market/tickers",
        params={
            "instType": "SWAP"
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

                result[inst] = {
                    "last": last
                }

        okx_ticker_cache = result

        return result

    except Exception as e:

        log.error(
            f"OKX ticker 오류: {e}"
        )

        return {}


# =========================================================
# OKX symbols
# =========================================================

def get_okx_symbols():

    r = retry(
        requests.get,
        "https://www.okx.com/api/v5/public/instruments",
        params={
            "instType": "SWAP"
        },
        timeout=15
    )

    if r is None:
        return []

    try:

        return [

            x["instId"]

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


# =========================================================
# OKX 거래대금
# =========================================================

def get_okx_volume_cached(
    inst,
    usdt
):

    df = get_okx_ohlcv(
        inst,
        "1H",
        VOLUME_HOURS
    )

    if df is None or df.empty:
        return None

    okx_1h_cache[
        inst
    ] = df.copy()

    try:

        volume = pd.to_numeric(
            df.volCcyQuote,
            errors="coerce"
        ).sum()

        return (
            float(volume)
            * float(usdt)
        )

    except Exception:

        return None


# =========================================================
# OKX 가격
# =========================================================

def get_okx_cached_price(
    inst
):

    try:

        item = (
            okx_ticker_cache
            .get(inst)
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
# OKX 현재 ROC
# =========================================================

def get_okx_current_roc_data(
    inst,
    current_price
):

    bar = get_okx_bar(
        ROC_TIMEFRAME
    )

    if not bar:
        return None

    df = get_okx_ohlcv(
        inst,
        bar,
        200,
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
                    pd.DataFrame(
                        [row]
                    )
                ],
                ignore_index=True
            )

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
            f"OKX 현재 ROC 오류 "
            f"{inst}: {e}"
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
#
# Y로 설정된 ROC만 검사
# =========================================================

def roc_filter_analysis(
    df,
    timeframe
):

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

        "positive_count":
            0,

        "total_count":
            len(
                enabled_periods
            ),

        "enabled_periods":
            enabled_periods

    }

    # 필터를 하나도 사용하지 않는 경우
    if not enabled_periods:

        result.update({

            "direction":
                "none",

            "passed":
                True,

            "positive_count":
                0,

            "total_count":
                0

        })

        return result

    if (
        df is None
        or df.empty
    ):

        return result

    try:

        values = {}
        positive_count = 0

        for period in enabled_periods:

            series = roc(
                df,
                period
            )

            if (
                series is None
                or series.empty
            ):

                return result

            value = float(
                series.iloc[-1]
            )

            if pd.isna(value):

                return result

            values[
                period
            ] = value

            if value >= 0:

                positive_count += 1

        passed = (
            positive_count
            == len(
                enabled_periods
            )
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

            "positive_count":
                positive_count,

            "total_count":
                len(
                    enabled_periods
                )

        })

        return result

    except Exception as e:

        log.error(
            f"ROC 필터 오류: {e}"
        )

        return result


# =========================================================
# ROC 필터 표시 데이터
# =========================================================

def roc_filter_display(
    x,
    timeframe
):

    enabled_periods = (
        get_enabled_periods(
            timeframe
        )
    )

    if not x:

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
                    enabled_periods
                ),

            "roc_values":
                {},

            "enabled_periods":
                enabled_periods

        }

    return {

        "display":
            (
                "🟢"
                if x.get(
                    "passed",
                    False
                )
                else "⚪"
            ),

        "direction":
            x.get(
                "direction",
                "none"
            ),

        "passed":
            bool(
                x.get(
                    "passed",
                    False
                )
            ),

        "positive_count":
            x.get(
                "positive_count",
                0
            ),

        "total_count":
            x.get(
                "total_count",
                len(
                    enabled_periods
                )
            ),

        "roc_values":
            x.get(
                "roc_values",
                {}
            ),

        "enabled_periods":
            enabled_periods

    }


# =========================================================
# ROC 필터 통과
#
# 선택된 시간봉의 활성화된 ROC가
# 모두 0 이상인지 검사
# =========================================================

def roc_filter_pass(
    r1,
    r4
):

    enabled_1h = (
        get_enabled_periods(
            ROC_FILTER_TIMEFRAME
        )
    )

    enabled_4h = (
        get_enabled_periods(
            ROC_FILTER_HIGH_TIMEFRAME
        )
    )

    if enabled_1h:

        if not r1.get(
            "passed",
            False
        ):

            return False

    if enabled_4h:

        if not r4.get(
            "passed",
            False
        ):

            return False

    return True


# =========================================================
# ROC5 현재 상태
#
# ROC5는 ROC_TIMEFRAME에서 계산
# =========================================================

def roc5_state(
    df_current
):

    result = {

        "value":
            None,

        "previous":
            None,

        "cross_up":
            False,

        "positive":
            False

    }

    if (
        df_current is None
        or df_current.empty
    ):

        return result

    try:

        series = roc(
            df_current,
            ROC_PERIOD
        )

        if (
            series is None
            or len(series) < 2
        ):

            return result

        current = float(
            series.iloc[-1]
        )

        previous = float(
            series.iloc[-2]
        )

        if (
            pd.isna(current)
            or pd.isna(previous)
        ):

            return result

        result.update({

            "value":
                current,

            "previous":
                previous,

            "cross_up":
                (
                    previous <= 0
                    and current > 0
                ),

            "positive":
                current > 0

        })

        return result

    except Exception as e:

        log.error(
            f"ROC5 상태 오류: {e}"
        )

        return result


# =========================================================
# 모든 활성 ROC가 현재 0 이상인지 검사
#
# ROC5도 활성화되어 있으면 포함
# =========================================================

def all_active_roc_positive(
    df,
    timeframe
):

    enabled_periods = (
        get_enabled_periods(
            timeframe
        )
    )

    if not enabled_periods:

        return True

    if df is None or df.empty:

        return False

    for period in enabled_periods:

        series = roc(
            df,
            period
        )

        if (
            series is None
            or series.empty
        ):

            return False

        try:

            value = float(
                series.iloc[-1]
            )

        except Exception:

            return False

        if pd.isna(value):

            return False

        if value < 0:

            return False

    return True


# =========================================================
# ROC 로켓 분석
#
# 핵심:
#
# 1. ROC5가 Y여야 함
# 2. 활성화된 모든 ROC가 0 이상
# 3. ROC5가 직전 <= 0 에서 현재 > 0
#    → 🚀0
# 4. 이후 ROC5 양수 유지
#    → 🚀1, 🚀2...
#
# 주의:
# 현재봉 기준 카운트
# =========================================================

def roc_analysis(
    df_confirmed,
    df_current,
    filter_df_1h=None,
    filter_df_4h=None
):

    result = {

        "roc10":
            None,

        "roc10_previous":
            None,

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

        "all_filter_positive":
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

        # -------------------------------------------------
        # ROC5
        # -------------------------------------------------

        confirmed_series = roc(
            df_confirmed,
            ROC_PERIOD
        )

        current_series = roc(
            df_current,
            ROC_PERIOD
        )

        if (
            confirmed_series is None
            or current_series is None
        ):

            return result

        if len(current_series) < 2:

            return result

        roc5_previous = float(
            current_series.iloc[-2]
        )

        roc5_current = float(
            current_series.iloc[-1]
        )

        if (
            pd.isna(roc5_previous)
            or pd.isna(roc5_current)
        ):

            return result

        # -------------------------------------------------
        # 활성 ROC 필터
        #
        # 1H와 4H의 활성 선을 모두 확인
        # -------------------------------------------------

        filter_1h_ok = (
            all_active_roc_positive(
                filter_df_1h,
                ROC_FILTER_TIMEFRAME
            )
            if get_enabled_periods(
                ROC_FILTER_TIMEFRAME
            )
            else True
        )

        filter_4h_ok = (
            all_active_roc_positive(
                filter_df_4h,
                ROC_FILTER_HIGH_TIMEFRAME
            )
            if get_enabled_periods(
                ROC_FILTER_HIGH_TIMEFRAME
            )
            else True
        )

        all_filter_positive = (
            filter_1h_ok
            and filter_4h_ok
        )

        # -------------------------------------------------
        # ROC5 활성화 여부
        # -------------------------------------------------

        roc5_enabled = (
            is_roc_enabled(
                ROC_TIMEFRAME,
                ROC_PERIOD
            )
        )

        # -------------------------------------------------
        # ROC5 연속 양수 카운트
        # -------------------------------------------------

        values = [

            float(x)

            for x in current_series.tolist()

            if not pd.isna(x)

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

        # -------------------------------------------------
        # 0선 상승 돌파
        # -------------------------------------------------

        cross_up = (
            roc5_previous <= 0
            and roc5_current > 0
        )

        # -------------------------------------------------
        # 로켓 조건
        #
        # ROC5가 Y이고
        # 모든 활성 필터가 0 이상
        # -------------------------------------------------

        breakout_allowed = (

            roc5_enabled

            and roc5_current > 0

            and all_filter_positive

        )

        # -------------------------------------------------
        # 현재 돌파
        # -------------------------------------------------

        if (
            breakout_allowed
            and cross_up
        ):

            result.update({

                "roc5":
                    roc5_current,

                "roc5_previous":
                    roc5_previous,

                "roc5_count":
                    1,

                "roc5_negative_count":
                    0,

                "long_breakout":
                    True,

                "long_breakout_count":
                    0,

                "long_breakout_state":
                    "current",

                "all_filter_positive":
                    True,

                "state":
                    "long_breakout",

                "display":
                    "🚀0"

            })

            return result

        # -------------------------------------------------
        # 돌파 후 유지
        #
        # 현재 양수이고 모든 활성 필터가
        # 계속 0 이상이면 카운트
        # -------------------------------------------------

        if breakout_allowed:

            # 양수 구간에서
            # 돌파 후 첫 봉 = 1
            #
            # positive_count는 현재봉 포함이므로
            # -1 해서 돌파봉을 0으로 계산

            count = max(
                positive_count - 1,
                0
            )

            result.update({

                "roc5":
                    roc5_current,

                "roc5_previous":
                    roc5_previous,

                "roc5_count":
                    positive_count,

                "roc5_negative_count":
                    0,

                "long_breakout":
                    True,

                "long_breakout_count":
                    count,

                "long_breakout_state":
                    "confirmed",

                "all_filter_positive":
                    True,

                "state":
                    "long_breakout",

                "display":
                    f"🚀{count}"

            })

            return result

        # -------------------------------------------------
        # 필터는 깨졌지만 ROC5가 양수
        # -------------------------------------------------

        if (
            roc5_current > 0
            and not all_filter_positive
        ):

            result.update({

                "roc5":
                    roc5_current,

                "roc5_previous":
                    roc5_previous,

                "roc5_count":
                    positive_count,

                "roc5_negative_count":
                    0,

                "long_breakout":
                    False,

                "long_breakout_count":
                    0,

                "long_breakout_state":
                    "none",

                "all_filter_positive":
                    False,

                "state":
                    "filter_failed",

                "display":
                    f"🟡 ROC +({positive_count})"

            })

            return result

        # -------------------------------------------------
        # ROC5 음수
        # -------------------------------------------------

        if roc5_current < 0:

            result.update({

                "roc5":
                    roc5_current,

                "roc5_previous":
                    roc5_previous,

                "roc5_count":
                    0,

                "roc5_negative_count":
                    negative_count,

                "long_breakout":
                    False,

                "long_breakout_count":
                    0,

                "long_breakout_state":
                    "none",

                "all_filter_positive":
                    all_filter_positive,

                "state":
                    "negative",

                "display":
                    f"🔴 ROC -({negative_count})"

            })

            return result

        # -------------------------------------------------
        # 0
        # -------------------------------------------------

        result.update({

            "roc5":
                roc5_current,

            "roc5_previous":
                roc5_previous,

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

            "all_filter_positive":
                all_filter_positive,

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
# 등락률
# =========================================================

def daily_change_upbit(
    market
):

    r = retry(
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

    if r is None:
        return None

    try:

        data = r.json()

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
                current - previous
            )
            / previous
            * 100
        ]

    except Exception as e:

        log.error(
            f"업비트 일봉 등락률 오류 "
            f"{market}: {e}"
        )

        return None


# =========================================================
# 등락률
# =========================================================

def daily_changes(df):

    if df is None or df.empty:
        return None

    try:

        x = df.copy()

        x["datetime"] = pd.to_datetime(
            x.datetime,
            errors="coerce"
        )

        x["c"] = pd.to_numeric(
            x.c,
            errors="coerce"
        )

        x = (
            x
            .dropna(
                subset=[
                    "datetime",
                    "c"
                ]
            )
            .set_index(
                "datetime"
            )
        )

        daily = (
            x.c
            .resample(
                "1D",
                offset="9h"
            )
            .last()
            .dropna()
        )

        if len(daily) < 2:
            return None

        previous = float(
            daily.iloc[-2]
        )

        current = float(
            daily.iloc[-1]
        )

        if previous == 0:
            return None

        return [
            (
                current - previous
            )
            / previous
            * 100
        ]

    except Exception:

        return None


# =========================================================
# 변화값
# =========================================================

def get_change_value(x):

    try:

        if x is None:
            return None

        return float(
            x[0]
            if isinstance(
                x,
                (list, tuple)
            )
            else x
        )

    except Exception:

        return None


# =========================================================
# 변화 표시
# =========================================================

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
# 빈 ROC 필터
# =========================================================

def empty_roc_filter(
    timeframe
):

    enabled_periods = (
        get_enabled_periods(
            timeframe
        )
    )

    return {

        "display":
            "⚪",

        "direction":
            "none",

        "passed":
            (
                True
                if not enabled_periods
                else False
            ),

        "positive_count":
            0,

        "total_count":
            len(
                enabled_periods
            ),

        "roc_values":
            {},

        "enabled_periods":
            enabled_periods

    }


# =========================================================
# 빈 분석
# =========================================================

def empty_analysis():

    return {

        "roc_filter_1h":
            empty_roc_filter(
                ROC_FILTER_TIMEFRAME
            ),

        "roc_filter_high":
            empty_roc_filter(
                ROC_FILTER_HIGH_TIMEFRAME
            ),

        "roc": {

            "roc10":
                None,

            "roc10_previous":
                None,

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

            "all_filter_positive":
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

        "progress_qualified":
            False,

        "roc3_long_progress_qualified":
            False,

        "direction_1h":
            "none",

        "df1h":
            None

    }


# =========================================================
# ROC 필터 HTML
#
# 사용 Y:
# 5🟢
#
# 사용 N:
# 5-
#
# 음수:
# 5🔴
# =========================================================

def roc_filter_html(
    r,
    timeframe
):

    enabled_periods = (
        get_enabled_periods(
            timeframe
        )
    )

    values = {}

    if r:

        values = r.get(
            "roc_values",
            {}
        )

    parts = []

    for period in ROC_FILTER_PERIODS:

        if not is_roc_enabled(
            timeframe,
            period
        ):

            parts.append(
                f"{period}-"
            )

            continue

        value = values.get(
            period
        )

        if value is None:

            icon = "⚪"

        else:

            try:

                value = float(value)

                if value >= 0:

                    icon = "🟢"

                else:

                    icon = "🔴"

            except Exception:

                icon = "⚪"

        parts.append(
            f"{period}{icon}"
        )

    if not parts:

        return (
            '<span class="roc-filter-none">'
            '⚪'
            '</span>'
        )

    return (
        '<span class="roc-filter-box">'
        '<span class="roc-filter-values">'
        + "/".join(parts)
        + '</span>'
        '</span>'
    )


# =========================================================
# 신호 자격
#
# 모든 활성 필터 >= 0
# ROC5 상승 돌파
# 당일 등락률 >= 0
# =========================================================

def get_signal_qualified(
    r1,
    r_high,
    r,
    filter_df_1h=None,
    filter_df_4h=None
):

    # -----------------------------------------------------
    # 활성 필터 검사
    # -----------------------------------------------------

    filter_pass = (
        roc_filter_pass(
            r1,
            r_high
        )
    )

    # -----------------------------------------------------
    # ROC5 활성화
    # -----------------------------------------------------

    roc5_enabled = (
        is_roc_enabled(
            ROC_TIMEFRAME,
            ROC_PERIOD
        )
    )

    # -----------------------------------------------------
    # ROC5
    # -----------------------------------------------------

    try:

        roc_value = float(
            r.get("roc5")
        )

        roc_previous = float(
            r.get(
                "roc5_previous"
            )
        )

    except Exception:

        roc_value = None
        roc_previous = None

    # -----------------------------------------------------
    # ROC5 상승 돌파
    # -----------------------------------------------------

    cross_up = (

        roc5_enabled

        and roc_value is not None

        and roc_previous is not None

        and roc_previous <= 0

        and roc_value > 0

    )

    # -----------------------------------------------------
    # 현재 모든 활성 ROC 확인
    # -----------------------------------------------------

    all_filter_positive = True

    if get_enabled_periods(
        ROC_FILTER_TIMEFRAME
    ):

        all_filter_positive = (
            all_filter_positive
            and all_active_roc_positive(
                filter_df_1h,
                ROC_FILTER_TIMEFRAME
            )
        )

    if get_enabled_periods(
        ROC_FILTER_HIGH_TIMEFRAME
    ):

        all_filter_positive = (
            all_filter_positive
            and all_active_roc_positive(
                filter_df_4h,
                ROC_FILTER_HIGH_TIMEFRAME
            )
        )

    # -----------------------------------------------------
    # 최종 돌파
    # -----------------------------------------------------

    long_breakout_qualified = (

        filter_pass

        and all_filter_positive

        and roc5_enabled

        and cross_up

        and roc_value is not None

        and roc_value > 0

    )

    return {

        "breakout_qualified":
            long_breakout_qualified,

        "progress_qualified":
            False,

        "roc3_long_progress_qualified":
            False,

        "filter_direction":
            (
                "long"
                if filter_pass
                and all_filter_positive
                else "none"
            )

    }


# =========================================================
# OKX 분석
# =========================================================

def analyze_okx(
    market,
    current_price=None
):

    filter_bar = get_okx_bar(
        ROC_FILTER_TIMEFRAME
    )

    high_filter_bar = get_okx_bar(
        ROC_FILTER_HIGH_TIMEFRAME
    )

    roc_bar = get_okx_bar(
        ROC_TIMEFRAME
    )

    if (
        not filter_bar
        or not high_filter_bar
        or not roc_bar
    ):

        return None

    df_filter = history_okx(
        market,
        filter_bar,
        required=200
    )

    df_filter_high = history_okx(
        market,
        high_filter_bar,
        required=200
    )

    df_roc_confirmed = history_okx(
        market,
        roc_bar,
        required=200
    )

    df_roc_current = (
        get_okx_current_roc_data(
            market,
            current_price
        )
    )

    if (
        df_filter is None
        or df_filter.empty
    ):

        return None

    if (
        df_filter_high is None
        or df_filter_high.empty
    ):

        return None

    if (
        df_roc_confirmed is None
        or df_roc_confirmed.empty
    ):

        return None

    roc_filter_1h_raw = (
        roc_filter_analysis(
            df_filter,
            ROC_FILTER_TIMEFRAME
        )
    )

    roc_filter_high_raw = (
        roc_filter_analysis(
            df_filter_high,
            ROC_FILTER_HIGH_TIMEFRAME
        )
    )

    r = roc_analysis(
        df_roc_confirmed,
        df_roc_current,
        df_filter,
        df_filter_high
    )

    changes = daily_changes(
        df_filter
    )

    q = get_signal_qualified(
        roc_filter_1h_raw,
        roc_filter_high_raw,
        r,
        df_filter,
        df_filter_high
    )

    return {

        "roc_filter_1h":
            roc_filter_display(
                roc_filter_1h_raw,
                ROC_FILTER_TIMEFRAME
            ),

        "roc_filter_high":
            roc_filter_display(
                roc_filter_high_raw,
                ROC_FILTER_HIGH_TIMEFRAME
            ),

        "roc":
            r,

        "changes":
            changes,

        **q,

        "direction_1h":
            q["filter_direction"],

        "df1h":
            df_filter

    }


# =========================================================
# 업비트 분석
# =========================================================

def analyze(
    market,
    okx=False,
    current_price=None
):

    if okx:

        return analyze_okx(
            market,
            current_price
        )

    # -----------------------------------------------------
    # 1H
    # -----------------------------------------------------

    df_filter = history_upbit(
        market,
        ROC_FILTER_TIMEFRAME,
        required=200
    )

    # -----------------------------------------------------
    # 4H
    # -----------------------------------------------------

    df_filter_high = history_upbit(
        market,
        ROC_FILTER_HIGH_TIMEFRAME,
        required=200
    )

    # -----------------------------------------------------
    # ROC5
    # -----------------------------------------------------

    df_roc_confirmed = history_upbit(
        market,
        ROC_TIMEFRAME,
        required=200
    )

    df_roc_current = (
        get_upbit_current_roc_data(
            market,
            current_price
        )
    )

    changes = (
        daily_change_upbit(
            market
        )
    )

    if (
        df_filter is None
        or df_filter.empty
    ):

        return None

    if (
        df_filter_high is None
        or df_filter_high.empty
    ):

        return None

    if (
        df_roc_confirmed is None
        or df_roc_confirmed.empty
    ):

        return None

    # -----------------------------------------------------
    # 필터 분석
    # -----------------------------------------------------

    roc_filter_1h_raw = (
        roc_filter_analysis(
            df_filter,
            ROC_FILTER_TIMEFRAME
        )
    )

    roc_filter_high_raw = (
        roc_filter_analysis(
            df_filter_high,
            ROC_FILTER_HIGH_TIMEFRAME
        )
    )

    # -----------------------------------------------------
    # ROC5 분석
    # -----------------------------------------------------

    r = roc_analysis(
        df_roc_confirmed,
        df_roc_current,
        df_filter,
        df_filter_high
    )

    # -----------------------------------------------------
    # 신호
    # -----------------------------------------------------

    q = get_signal_qualified(
        roc_filter_1h_raw,
        roc_filter_high_raw,
        r,
        df_filter,
        df_filter_high
    )

    return {

        "roc_filter_1h":
            roc_filter_display(
                roc_filter_1h_raw,
                ROC_FILTER_TIMEFRAME
            ),

        "roc_filter_high":
            roc_filter_display(
                roc_filter_high_raw,
                ROC_FILTER_HIGH_TIMEFRAME
            ),

        "roc":
            r,

        "changes":
            changes,

        **q,

        "direction_1h":
            q["filter_direction"],

        "df1h":
            df_filter

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
                a.get("changes")
            ),

        "change_value":
            get_change_value(
                a.get("changes")
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
                    ROC_FILTER_TIMEFRAME
                )
            ),

        "roc_filter_high":
            a.get(
                "roc_filter_high",
                empty_roc_filter(
                    ROC_FILTER_HIGH_TIMEFRAME
                )
            ),

        "roc":
            a["roc"],

        "breakout_qualified":
            a["breakout_qualified"],

        "progress_qualified":
            False,

        "roc3_long_progress_qualified":
            False,

        "direction":
            a["direction_1h"],

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
# 돌파 여부
# =========================================================

def is_breakout(row):

    if not row:
        return False

    r = row.get(
        "roc",
        {}
    )

    return bool(
        row.get(
            "breakout_qualified",
            False
        )
        and r.get(
            "long_breakout_state",
            "none"
        ) == "current"
    )


# =========================================================
# 진행
# =========================================================

def is_progress(row):

    return False


def is_roc3_progress(row):

    return False


# =========================================================
# 상승 통합 후보
# =========================================================

def is_long_combined(row):

    if not row:
        return False

    r = row.get(
        "roc",
        {}
    )

    try:

        roc_value = float(
            r.get("roc5")
        )

        count = int(
            r.get(
                "long_breakout_count",
                0
            )
        )

    except Exception:

        return False

    if roc_value <= 0:
        return False

    if count < 0:
        return False

    daily_change = row.get(
        "change_value"
    )

    if daily_change is None:
        return False

    try:

        daily_change = float(
            daily_change
        )

    except Exception:

        return False

    if daily_change < 0:
        return False

    return bool(
        row.get(
            "breakout_qualified",
            False
        )
        or (
            r.get(
                "long_breakout",
                False
            )
            and r.get(
                "all_filter_positive",
                False
            )
        )
    )


# =========================================================
# TOP 시장폭
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

    positive = 0
    negative = 0
    zero = 0
    total = 0

    for row in data:

        if not row:
            continue

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

        total += 1

        if value > 0:

            positive += 1

        elif value < 0:

            negative += 1

        else:

            zero += 1

    result["positive"] = positive
    result["negative"] = negative
    result["zero"] = zero
    result["total"] = total

    if total <= 0:
        return result

    result["ratio"] = (
        positive
        / total
        * 100
    )

    if positive > negative:

        result["icon"] = "☀️"
        result["state"] = "up"

    elif positive < negative:

        result["icon"] = "🌧️"
        result["state"] = "down"

    else:

        result["icon"] = "⚪"
        result["state"] = "neutral"

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
                current_price=price
            )

        except Exception as e:

            log.error(
                f"업비트 상세 오류 "
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
            f"[호가] "
            f"{coin} | "
            f"매수 "
            f"{format_volume(ob['bid_amount'])} "
            f"({ob['bid_ratio']:.1f}%) / "
            f"매도 "
            f"{format_volume(ob['ask_amount'])} "
            f"({ob['ask_ratio']:.1f}%) / "
            f"{ob['dominance_text']}"
        )

    latest_upbit_data = rows

    breadth = (
        top_daily_breadth(
            latest_upbit_data
        )
    )

    latest_upbit_update_time = (
        kst()
    )

    log.info(
        f"업비트 완료 / "
        f"상승 "
        f"{sum(is_long_combined(x) for x in rows)}개"
    )

    log.info(
        f"TOP{TOP_N} 시장폭 / "
        f"양수 {breadth['positive']} / "
        f"음수 {breadth['negative']} / "
        f"보합 {breadth['zero']} / "
        f"{breadth['icon']}"
    )


# =========================================================
# OKX 업데이트
# =========================================================

def update_okx(
    usdt
):

    global latest_okx_data
    global latest_okx_update_time
    global okx_1h_cache
    global okx_1h_cache_time

    if not usdt or usdt <= 0:
        return False

    okx_1h_cache = {}

    tickers = get_okx_tickers()

    if not tickers:
        return False

    symbols = get_okx_symbols()

    if not symbols:
        return False

    symbols = [
        x
        for x in symbols
        if x in tickers
    ]

    upbit_set = {

        x.replace(
            "KRW-",
            ""
        )

        for x in latest_upbit_markets

    }

    volumes = {}

    for idx, symbol in enumerate(
        symbols,
        1
    ):

        v = (
            get_okx_volume_cached(
                symbol,
                usdt
            )
        )

        if v and v > 0:

            volumes[
                symbol
            ] = v

        if idx % 50 == 0:

            log.info(
                f"OKX 거래대금 "
                f"{idx}/{len(symbols)} 완료"
            )

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

        name = (

            f"{coin} (업비트)"

            if coin in upbit_set

            else coin

        )

        price = (
            get_okx_cached_price(
                symbol
            )
        )

        try:

            a = analyze(
                symbol,
                True,
                price
            )

        except Exception as e:

            log.error(
                f"OKX 상세 오류 "
                f"{symbol}: {e}"
            )

            a = None

        rows.append(
            make_row(
                rank,
                name,
                volumes[symbol],
                a,
                price
            )
        )

    latest_okx_data = rows

    breadth = (
        top_daily_breadth(
            latest_okx_data
        )
    )

    okx_1h_cache_time = kst()
    latest_okx_update_time = kst()

    return True


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    global latest_upbit_data
    global latest_okx_data
    global latest_usdt_krw_internal

    if not update_lock.acquire(False):

        log.warning(
            "이전 조회 진행 중 → 건너뜀"
        )

        return

    try:

        usdt = (
            latest_usdt_krw_internal
        )

        if USE_OKX == "Y":

            try:

                current_usdt = (
                    get_usdt_krw_internal()
                )

                if current_usdt is not None:

                    latest_usdt_krw_internal = (
                        current_usdt
                    )

                    usdt = current_usdt

            except Exception as e:

                log.exception(
                    f"USDT/KRW 오류: {e}"
                )

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

                if usdt and usdt > 0:

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
# 시장 변화 HTML
# =========================================================

def market_change_html(
    value
):

    if value is None:

        return (
            '<span class="market-zero">-</span>'
        )

    try:

        value = float(value)

    except Exception:

        return (
            '<span class="market-zero">-</span>'
        )

    if value > 0:

        return (
            '<span class="market-up">'
            f'▲+{value:.1f}%'
            '</span>'
        )

    if value < 0:

        return (
            '<span class="market-down">'
            f'▼{value:.1f}%'
            '</span>'
        )

    return (
        '<span class="market-zero">'
        '0.0%'
        '</span>'
    )


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

    # -----------------------------------------------------
    # BTC 필터
    # -----------------------------------------------------

    if btc is None:

        btc_filter_icon = "⚪"
        btc_filter_display = "-"
        btc_filter_class = "wait"

    else:

        r1 = btc.get(
            "roc_filter_1h",
            {}
        )

        r4 = btc.get(
            "roc_filter_high",
            {}
        )

        pass_1h = (

            r1.get(
                "passed",
                True
            )

            if get_enabled_periods(
                ROC_FILTER_TIMEFRAME
            )

            else True

        )

        pass_4h = (

            r4.get(
                "passed",
                True
            )

            if get_enabled_periods(
                ROC_FILTER_HIGH_TIMEFRAME
            )

            else True

        )

        if pass_1h and pass_4h:

            btc_filter_icon = "☀️"
            btc_filter_display = "필터 통과"
            btc_filter_class = "up"

        else:

            btc_filter_icon = "⚪"
            btc_filter_display = "필터 미통과"
            btc_filter_class = "wait"

    # -----------------------------------------------------
    # BTC 등락
    # -----------------------------------------------------

    if btc is None:

        btc_daily_icon = "⚪"
        btc_daily_display = "-"
        btc_daily_class = "wait"

    else:

        btc_daily_change = btc.get(
            "change_value"
        )

        if btc_daily_change is None:

            btc_daily_icon = "⚪"
            btc_daily_display = "-"
            btc_daily_class = "wait"

        else:

            try:

                btc_daily_change = float(
                    btc_daily_change
                )

                if btc_daily_change > 0:

                    btc_daily_icon = "☀️"

                    btc_daily_display = (
                        f"+{btc_daily_change:.2f}%"
                    )

                    btc_daily_class = "up"

                elif btc_daily_change < 0:

                    btc_daily_icon = "🌧️"

                    btc_daily_display = (
                        f"{btc_daily_change:.2f}%"
                    )

                    btc_daily_class = "down"

                else:

                    btc_daily_icon = "⚪"
                    btc_daily_display = "0.00%"
                    btc_daily_class = "wait"

            except Exception:

                btc_daily_icon = "⚪"
                btc_daily_display = "-"
                btc_daily_class = "wait"

    # -----------------------------------------------------
    # 시장폭
    # -----------------------------------------------------

    breadth = (
        top_daily_breadth(
            latest_upbit_data
        )
    )

    breadth_icon = breadth.get(
        "icon",
        "⚪"
    )

    breadth_positive = breadth.get(
        "positive",
        0
    )

    breadth_negative = breadth.get(
        "negative",
        0
    )

    breadth_zero = breadth.get(
        "zero",
        0
    )

    breadth_total = breadth.get(
        "total",
        0
    )

    if breadth_total > 0:

        breadth_display = (
            f"양수 {breadth_positive}"
            f" / "
            f"음수 {breadth_negative}"
        )

        if breadth_zero > 0:

            breadth_display += (
                f" · 0 {breadth_zero}"
            )

    else:

        breadth_display = "-"

    if breadth.get(
        "state"
    ) == "up":

        breadth_class = "up"

    elif breadth.get(
        "state"
    ) == "down":

        breadth_class = "down"

    else:

        breadth_class = "wait"

    # -----------------------------------------------------
    # BTC 가격
    # -----------------------------------------------------

    if btc is None:

        btc_price_display = "-"
        btc_change_display = "-"

    else:

        btc_price_display = (
            format_market_price(
                btc.get(
                    "current_price"
                )
            )
        )

        btc_change_display = (
            market_change_html(
                btc.get(
                    "change_value"
                )
            )
        )

    return f"""

    <div class="market-summary">

        <div class="market-title">

            <span class="market-title-main">
                ₿ BTC 시장 시황
            </span>

            <span class="market-title-sub">
                ROC 필터
                {format_timeframe(ROC_FILTER_TIMEFRAME)}
                /
                {format_timeframe(ROC_FILTER_HIGH_TIMEFRAME)}
                ·
                5/10/20/50/200
                ·
                {get_roc_text()}
            </span>

        </div>

        <div class="btc-mobile">

            <div class="btc-top">

                <span class="btc-name">
                    ₿ BTC
                </span>

                <span class="btc-price">
                    {btc_price_display}
                </span>

                <span class="btc-change">
                    {btc_change_display}
                </span>

            </div>

            <div class="btc-bottom">

                <div class="
                    btc-info-box
                    {btc_filter_class}
                ">

                    <div class="btc-info-title">
                        ROC 필터
                        {format_timeframe(ROC_FILTER_TIMEFRAME)}
                        /
                        {format_timeframe(ROC_FILTER_HIGH_TIMEFRAME)}
                    </div>

                    <div class="btc-info-value">
                        {btc_filter_icon}
                    </div>

                    <div class="btc-info-sub">
                        {btc_filter_display}
                    </div>

                </div>

                <div class="
                    btc-info-box
                    {btc_daily_class}
                ">

                    <div class="btc-info-title">
                        BTC · 당일
                    </div>

                    <div class="btc-info-value">
                        {btc_daily_icon}
                    </div>

                    <div class="btc-info-sub">
                        {btc_daily_display}
                    </div>

                </div>

                <div class="
                    btc-info-box
                    {breadth_class}
                ">

                    <div class="btc-info-title">
                        전체 · 당일
                    </div>

                    <div class="btc-info-value">
                        {breadth_icon}
                    </div>

                    <div class="btc-info-sub">
                        {breadth_display}
                    </div>

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
            '<span class="roc-zero">'
            '⚪ 0'
            '</span>'
            '</div>'
        )

    value = r.get(
        "roc5"
    )

    if value is None:

        return (
            '<div class="roc-cell">'
            '<span class="roc-zero">'
            '⚪ 0'
            '</span>'
            '</div>'
        )

    try:

        value = float(value)

    except Exception:

        return (
            '<div class="roc-cell">'
            '<span class="roc-zero">'
            '⚪ 0'
            '</span>'
            '</div>'
        )

    # -----------------------------------------------------
    # 모든 활성 필터 통과 + ROC5 양수
    # -----------------------------------------------------

    if (
        value > 0
        and r.get(
            "long_breakout",
            False
        )
        and r.get(
            "all_filter_positive",
            False
        )
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

    # -----------------------------------------------------
    # ROC5 양수지만 필터 미통과
    # -----------------------------------------------------

    if (
        value > 0
        and not r.get(
            "all_filter_positive",
            False
        )
    ):

        count = int(
            r.get(
                "roc5_count",
                0
            )
        )

        return (
            '<div class="roc-cell">'
            '<span class="roc-filter-failed">'
            f'🟡 ROC +({count})'
            '</span>'
            '</div>'
        )

    # -----------------------------------------------------
    # ROC5 음수
    # -----------------------------------------------------

    if value < 0:

        negative_count = int(
            r.get(
                "roc5_negative_count",
                0
            )
        )

        return (
            '<div class="roc-cell">'
            '<span class="roc-negative">'
            f'🔴 ROC -({negative_count})'
            '</span>'
            '</div>'
        )

    return (
        '<div class="roc-cell">'
        '<span class="roc-zero">'
        '⚪ 0'
        '</span>'
        '</div>'
    )


# =========================================================
# 신호 HTML
# =========================================================

def signal_html(
    row,
    top_list=False
):

    if not row:

        return (
            '<span class="muted">-</span>'
        )

    r = row.get(
        "roc",
        {}
    )

    if (
        r.get(
            "long_breakout",
            False
        )
        and r.get(
            "all_filter_positive",
            False
        )
    ):

        try:

            count = int(
                r.get(
                    "long_breakout_count",
                    0
                )
            )

        except Exception:

            count = 0

        return (
            '<span '
            'class="signal-icon long-breakout" '
            'title="활성 ROC 필터 전체 0 이상 + ROC5 양수">'
            f'🚀{count}'
            '</span>'
        )

    return (
        '<span class="muted">-</span>'
    )


# =========================================================
# 필터 HTML
#
# 1H / 4H
#
# 예:
# 1H 5🟢/10🟢/20-/50🟢/200🟢
# 4H 5-/10-/20-/50-/200-
# =========================================================

def filter_html(
    r1,
    r4
):

    if not r1:

        r1 = empty_roc_filter(
            ROC_FILTER_TIMEFRAME
        )

    if not r4:

        r4 = empty_roc_filter(
            ROC_FILTER_HIGH_TIMEFRAME
        )

    return (

        '<div class="filter-detail">'

        '<div class="filter-line">'

        f'<span class="filter-timeframe">'
        f'{format_timeframe(ROC_FILTER_TIMEFRAME)}'
        f'</span>'

        f'{roc_filter_html(
            r1,
            ROC_FILTER_TIMEFRAME
        )}'

        '</div>'

        '<div class="filter-line">'

        f'<span class="filter-timeframe">'
        f'{format_timeframe(ROC_FILTER_HIGH_TIMEFRAME)}'
        f'</span>'

        f'{roc_filter_html(
            r4,
            ROC_FILTER_HIGH_TIMEFRAME
        )}'

        '</div>'

        '</div>'

    )


# =========================================================
# 행 클래스
# =========================================================

def row_class(
    x
):

    r = x.get(
        "roc",
        {}
    )

    if (
        r.get(
            "long_breakout",
            False
        )
        and r.get(
            "all_filter_positive",
            False
        )
    ):

        return "breakout-qualified"

    return ""


# =========================================================
# 행 HTML
# =========================================================

def rows_html(
    data,
    focus=None,
    top_list=False
):

    out = []

    for x in data:

        if focus == "long_combined":

            cls = (

                "breakout-qualified"

                if is_long_combined(x)

                else ""

            )

        else:

            cls = row_class(
                x
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
                            "roc_filter_1h",
                            {}
                        ),
                        x.get(
                            "roc_filter_high",
                            {}
                        )
                    )}

                </td>

                <td>

                    {roc_html(
                        x.get(
                            "roc",
                            {}
                        )
                    )}

                </td>

                <td class="signal-cell">

                    {signal_html(
                        x,
                        top_list
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

    return "".join(
        out
    )


# =========================================================
# 테이블
# =========================================================

def table_html(
    data,
    focus=None,
    top_list=False
):

    rows = rows_html(
        data,
        focus,
        top_list
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
                        {get_roc_text()}
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
# 후보 섹션
# =========================================================

def focus_section(
    title,
    data,
    update_time,
    checker,
    focus,
    description,
    sort_key=None,
    reverse=False
):

    rows = [

        x

        for x in data

        if checker(x)

    ]

    rows.sort(
        key=lambda x:
            x.get(
                "rank",
                999999
            )
    )

    return f"""

    <div class="
        section-title
        {focus}-section-title
    ">

        <span class="section-title-main">
            {title}
        </span>

        <span class="section-title-sub">
            {description}
            ·
            {update_time} KST
        </span>

    </div>

    {table_html(
        rows,
        focus,
        top_list=False
    )}

    """


# =========================================================
# 전체 TOP
# =========================================================

def section(
    title,
    data,
    update_time
):

    return f"""

    <div class="section-title">

        <span class="section-title-main">
            🏆 {title} TOP{TOP_N}
        </span>

        <span class="section-title-sub">
            {update_time} KST
        </span>

    </div>

    {table_html(
        data,
        top_list=True
    )}

    """


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

    try:

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

    except Exception:

        return (
            '<div class="orderbook-empty">'
            '호가 정보 없음'
            '</div>'
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

    bid_ratio = max(
        0.0,
        min(
            100.0,
            bid_ratio
        )
    )

    ask_ratio = max(
        0.0,
        min(
            100.0,
            ask_ratio
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

            <span class="
                orderbook-label
                ask-label
            ">
                매도대기
            </span>

            <span class="
                orderbook-amount
                ask-amount
            ">
                {format_volume(
                    ask_amount
                )}
            </span>

            <div class="orderbook-bar-box">

                <div
                    class="
                        orderbook-bar
                        ask-bar
                    "
                    style="
                        width:
                        {ask_ratio:.1f}%
                    "
                ></div>

            </div>

            <span class="
                orderbook-ratio
                ask-ratio
            ">
                {ask_ratio:.1f}%
            </span>

        </div>

        <div class="orderbook-row">

            <span class="
                orderbook-label
                bid-label
            ">
                매수대기
            </span>

            <span class="
                orderbook-amount
                bid-amount
            ">
                {format_volume(
                    bid_amount
                )}
            </span>

            <div class="orderbook-bar-box">

                <div
                    class="
                        orderbook-bar
                        bid-bar
                    "
                    style="
                        width:
                        {bid_ratio:.1f}%
                    "
                ></div>

            </div>

            <span class="
                orderbook-ratio
                bid-ratio
            ">
                {bid_ratio:.1f}%
            </span>

        </div>

        <div class="orderbook-bottom">

            <span class="orderbook-range">
                호가 ±
                {ORDERBOOK_RANGE * 100:.0f}%
            </span>

            {dominance_html}

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

    background:
        rgba(57,232,117,.08);

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

.long_combined-section-title{
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

.btc-mobile{
    width:100%;
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
    line-height:8px;
    font-weight:900;
}

.btc-price{

    flex:1;

    min-width:0;

    color:#e8edf2;

    font-size:6px;
    line-height:8px;
    font-weight:800;

    text-align:left;

    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;
}

.btc-change{

    flex:none;

    width:64px;

    font-size:9.5px;
    line-height:12px;

    font-weight:900;

    text-align:right;

    white-space:nowrap;
}

.btc-bottom{

    display:grid;

    grid-template-columns:
        1fr
        1fr
        1fr;

    align-items:stretch;

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

    margin-bottom:2px;

    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;
}

.btc-info-value{

    width:100%;

    color:#eee;

    font-size:28px;
    line-height:30px;

    font-weight:900;

    display:flex;

    align-items:center;
    justify-content:center;

    white-space:nowrap;
    overflow:hidden;
}

.btc-info-sub{

    width:100%;

    color:#737b85;

    font-size:7px;
    line-height:9px;

    margin-top:2px;

    font-weight:800;

    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;
}

.btc-info-box.up{

    color:#39e875!important;

    border-color:
        rgba(57,232,117,.28);

    background:
        rgba(57,232,117,.08);
}

.btc-info-box.down{

    color:#ff5555!important;

    border-color:
        rgba(255,85,85,.28);

    background:
        rgba(255,85,85,.08);
}

.btc-info-box.wait{

    color:#b0b7bf!important;

    border-color:#292f36;

    background:#14181d;
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

.roc-filter-failed{

    color:#e0b44c!important;
    font-weight:900;
}

.market-zero,
.roc-zero,
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
    line-height:7px;

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

.signal-icon{

    display:inline-flex;

    align-items:center;
    justify-content:center;

    width:100%;

    min-height:21px;

    font-size:15px;
    line-height:17px;

    font-weight:900;

    white-space:nowrap;
}

.signal-icon.long-breakout{

    filter:
        drop-shadow(
            0 0 2px
            rgba(57,232,117,.35)
        );
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
    line-height:6px;

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

tr:last-child td{
    border-bottom:none;
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

    line-height:9px;
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

    margin:0;

    font-size:4.5px;
    line-height:6px;

    white-space:nowrap;

    overflow:hidden;
}

.vol{

    font-size:6px;
    line-height:8px;

    font-weight:800;

    white-space:nowrap;
}

.ema{

    text-align:center!important;

    font-weight:800;

    line-height:8px;

    white-space:normal;

    overflow:hidden;
}


/* =======================================================
   ROC 필터
   ======================================================= */

.filter-detail{

    display:flex;

    flex-direction:column;

    align-items:flex-start;

    justify-content:center;

    gap:1px;

    width:100%;

    font-size:4.8px;

    line-height:7px;

    white-space:nowrap;

    overflow:hidden;
}

.filter-line{

    display:flex;

    align-items:center;

    justify-content:flex-start;

    gap:0;

    width:100%;

    min-width:0;

    white-space:nowrap;

    overflow:hidden;
}

.filter-timeframe{

    display:inline-block;

    flex:none;

    width:14px;

    color:#d7dce1;

    font-size:4.8px;
    line-height:7px;

    font-weight:900;

    text-align:left;
}

.roc-filter-box{

    display:inline-flex;

    align-items:center;

    gap:0;

    min-width:0;

    font-weight:900;

    white-space:nowrap;

    overflow:hidden;
}

.roc-filter-values{

    color:#9ca4ad;

    font-size:4.2px;

    font-weight:700;

    letter-spacing:-0.2px;

    white-space:nowrap;

    overflow:hidden;

    text-overflow:clip;
}

.roc-cell{

    display:flex;

    flex-direction:row;

    align-items:center;

    justify-content:center;

    gap:1px;

    min-height:21px;

    line-height:8px;

    white-space:nowrap;
}

.roc-cell span{

    font-size:5.8px;
    line-height:8px;

    font-weight:900;

    white-space:nowrap;
}

.breakout-qualified{

    background:
        rgba(57,232,117,.08);
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

    border-bottom:
        1px solid #252b31!important;
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

    line-height:8px;

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

    line-height:8px;

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

    transition:
        width .25s ease;
}

.ask-bar{
    background:#d94a4a;
}

.bid-bar{
    background:#39e875;
}

.orderbook-ratio{

    font-size:5.5px;

    line-height:8px;

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

    padding-right:1px;

    font-size:5px;

    line-height:7px;

    font-weight:800;
}

.orderbook-range{

    color:#5e6670;

    white-space:nowrap;
}

.ob-dominance{

    font-size:5.5px;

    line-height:8px;

    font-weight:900;

    white-space:nowrap;
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

    line-height:7px;

    font-weight:700;

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

    .market-summary{

        padding:3px 3px;
    }

    .market-title,
    .section-title{

        min-height:17px;

        gap:4px;

        font-size:7px;
        line-height:9px;

        padding:3px 4px;

        margin-bottom:3px;

        border-left-width:3px;
    }

    .section-title{

        margin:4px 0 3px;
    }

    .market-title-main,
    .section-title-main{

        font-size:7px;
        line-height:9px;
    }

    .market-title-sub,
    .section-title-sub{

        font-size:4.8px;
        line-height:7px;
    }

    .btc-top{

        min-height:15px;
        gap:3px;
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
        line-height:11px;
    }

    .btc-bottom{

        grid-template-columns:
            1fr
            1fr
            1fr;

        min-height:58px;

        gap:3px;
    }

    .btc-info-box{

        min-height:58px;

        padding:3px 4px;

        border-radius:5px;
    }

    .btc-info-title{

        font-size:4.8px
