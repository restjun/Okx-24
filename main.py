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
TOP_N = 30
UPDATE_MINUTES = 1
HISTORY_CHUNK = 200
MAX_HISTORY_CHUNKS = 10


# =========================================================
# 거래소
# =========================================================

USE_UPBIT = "Y"
USE_OKX = "N"


# =========================================================
# API
# =========================================================

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10

KST = ZoneInfo("Asia/Seoul")


# =========================================================
# EMA 시간봉
# =========================================================

EMA_TIMEFRAME = 60
EMA_HIGH_TIMEFRAME = 240
EMA_DAILY_TIMEFRAME = 1440

USE_EMA_TIMEFRAME = "Y"
USE_EMA_HIGH_TIMEFRAME = "Y"
USE_EMA_DAILY_TIMEFRAME = "N"


# =========================================================
# ROC 시간봉
# =========================================================

ROC_TIMEFRAME = 60
ROC_HIGH_TIMEFRAME = 240
ROC_DAILY_TIMEFRAME = 1440

# ROC 필터 적용 여부
# Y = 실제 필터
# N = 참고용
USE_ROC_TIMEFRAME = "Y"
USE_ROC_HIGH_TIMEFRAME = "Y"
USE_ROC_DAILY_TIMEFRAME = "N"


# =========================================================
# EMA 기간
# =========================================================

EMA1_FASTEST = 10
EMA1_FAST = 30
EMA1_MID = 60
EMA1_SLOW = 120
EMA1_MAX_COUNT = 200


# =========================================================
# ROC
# =========================================================

ROC_PERIOD = 10

# ROC 0선
ROC_LONG_LEVEL = 0
ROC_SHORT_LEVEL = 0


# =========================================================
# ROC 이평선 기간
#
# ROC10 자체에 EMA를 적용
#
# ROC EMA 10
# ROC EMA 30
# ROC EMA 60
# ROC EMA 120
# =========================================================

ROC_MA_FASTEST = 10
ROC_MA_FAST = 30
ROC_MA_MID = 60
ROC_MA_SLOW = 120

ROC_MA_MAX_COUNT = 200


# =========================================================
# ROC 진행 시작
# =========================================================

ROC_PROGRESS_START_COUNT = 2


# =========================================================
# 전역 데이터
# =========================================================

latest_upbit_data = []
latest_okx_data = []

latest_usdt_krw = 0

latest_upbit_update_time = "-"
latest_okx_update_time = "-"

latest_upbit_markets = []


# =========================================================
# 시장 시황 데이터
# =========================================================

latest_market_source = "-"
latest_market_data = {}


# =========================================================
# OKX 캐시
# =========================================================

okx_ticker_cache = {}
okx_1h_cache = {}
okx_1h_cache_time = "-"


# =========================================================
# 락
# =========================================================

request_lock = threading.Lock()
update_lock = threading.Lock()

last_request_time = 0


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
# EMA 시간봉 표시
# =========================================================

def ema_timeframe_label(
    minutes,
    use_flag
):

    label = format_timeframe(minutes)

    if use_flag == "Y":
        return f"{label}"

    return f"{label} 참고"


# =========================================================
# EMA 필터 설정 표시
# =========================================================

def get_ema_filter_labels():

    result = []

    settings = [
        (
            EMA_TIMEFRAME,
            USE_EMA_TIMEFRAME
        ),
        (
            EMA_HIGH_TIMEFRAME,
            USE_EMA_HIGH_TIMEFRAME
        ),
        (
            EMA_DAILY_TIMEFRAME,
            USE_EMA_DAILY_TIMEFRAME
        )
    ]

    for timeframe, flag in settings:

        label = format_timeframe(timeframe)

        if flag == "Y":

            result.append(
                f"{label} 적용"
            )

        else:

            result.append(
                f"{label} 참고"
            )

    return " / ".join(result)


def get_ema_filter_timeframes():

    result = []

    settings = [
        (
            EMA_TIMEFRAME,
            USE_EMA_TIMEFRAME
        ),
        (
            EMA_HIGH_TIMEFRAME,
            USE_EMA_HIGH_TIMEFRAME
        ),
        (
            EMA_DAILY_TIMEFRAME,
            USE_EMA_DAILY_TIMEFRAME
        )
    ]

    for timeframe, flag in settings:

        if flag == "Y":

            result.append(
                format_timeframe(timeframe)
            )

    if not result:

        return "없음"

    return " / ".join(result)


# =========================================================
# ROC 필터 설정 표시
# =========================================================

def get_roc_filter_labels():

    result = []

    settings = [
        (
            ROC_TIMEFRAME,
            USE_ROC_TIMEFRAME
        ),
        (
            ROC_HIGH_TIMEFRAME,
            USE_ROC_HIGH_TIMEFRAME
        ),
        (
            ROC_DAILY_TIMEFRAME,
            USE_ROC_DAILY_TIMEFRAME
        )
    ]

    for timeframe, flag in settings:

        label = format_timeframe(timeframe)

        if flag == "Y":

            result.append(
                f"{label} 적용"
            )

        else:

            result.append(
                f"{label} 참고"
            )

    return " / ".join(result)


def get_roc_filter_timeframes():

    result = []

    settings = [
        (
            ROC_TIMEFRAME,
            USE_ROC_TIMEFRAME
        ),
        (
            ROC_HIGH_TIMEFRAME,
            USE_ROC_HIGH_TIMEFRAME
        ),
        (
            ROC_DAILY_TIMEFRAME,
            USE_ROC_DAILY_TIMEFRAME
        )
    ]

    for timeframe, flag in settings:

        if flag == "Y":

            result.append(
                format_timeframe(timeframe)
            )

    if not result:

        return "없음"

    return " / ".join(result)


# =========================================================
# OKX 시간봉
# =========================================================

def get_okx_bar(minutes):

    return {
        1: "1m",
        3: "3m",
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

    if minutes == 1440:

        anchor = now.replace(
            hour=9,
            minute=0,
            second=0,
            microsecond=0
        )

        if now < anchor:

            anchor -= pd.Timedelta(
                days=1
            )

        return anchor.replace(
            tzinfo=None
        )

    if minutes == 240:

        anchor = now.replace(
            hour=1,
            minute=0,
            second=0,
            microsecond=0
        )

        if now < anchor:

            anchor -= pd.Timedelta(
                days=1
            )

        elapsed_minutes = int(
            (
                now - anchor
            ).total_seconds()
            // 60
        )

        block = (
            elapsed_minutes // 240
        ) * 240

        current = (
            anchor
            + pd.Timedelta(
                minutes=block
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
# 시간봉 검증
# =========================================================

SUPPORTED_UPBIT_TIMEFRAMES = {
    1,
    3,
    5,
    15,
    30,
    60,
    240,
    1440
}

SUPPORTED_OKX_TIMEFRAMES = {
    1,
    3,
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


def validate_timeframe():

    timeframe_list = [

        ("EMA_TIMEFRAME", EMA_TIMEFRAME),
        ("EMA_HIGH_TIMEFRAME", EMA_HIGH_TIMEFRAME),
        ("EMA_DAILY_TIMEFRAME", EMA_DAILY_TIMEFRAME),

        ("ROC_TIMEFRAME", ROC_TIMEFRAME),
        ("ROC_HIGH_TIMEFRAME", ROC_HIGH_TIMEFRAME),
        ("ROC_DAILY_TIMEFRAME", ROC_DAILY_TIMEFRAME)

    ]

    for name, value in timeframe_list:

        if value not in SUPPORTED_UPBIT_TIMEFRAMES:

            raise ValueError(
                f"{name} 오류: {value}"
            )

        if get_okx_bar(value) is None:

            raise ValueError(
                f"OKX {name} 오류: {value}"
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

    for n in range(MAX_RETRIES):

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
                f"API 오류: {e}"
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
# UPBIT 마켓
# =========================================================

def get_upbit_markets():

    global latest_upbit_markets

    response = retry(
        requests.get,
        "https://api.upbit.com/v1/ticker/all",
        params={
            "quote_currencies": "KRW"
        },
        timeout=15
    )

    if response is None:
        return []

    try:

        result = []

        for item in response.json():

            market = item.get(
                "market",
                ""
            )

            if not market.startswith("KRW-"):
                continue

            try:

                volume = float(
                    item["acc_trade_price_24h"]
                )

                price = float(
                    item["trade_price"]
                )

            except Exception:

                continue

            if volume > 0 and price > 0:

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
            f"UPBIT 마켓 오류: {e}"
        )

        return []


# =========================================================
# USDT/KRW
# =========================================================

def get_usdt_krw():

    response = retry(
        requests.get,
        "https://api.upbit.com/v1/ticker?markets=KRW-USDT",
        timeout=15
    )

    if response is None:
        return None

    try:

        price = float(
            response.json()[0]["trade_price"]
        )

        if price > 0:
            return price

    except Exception:
        pass

    return None


# =========================================================
# UPBIT 캔들
# =========================================================

def get_upbit_candle(
    market,
    unit,
    count=200,
    to=None,
    include_current=False
):

    unit = int(unit)

    if unit == 1440:

        url = (
            "https://api.upbit.com/v1/candles/days"
        )

    else:

        url = (
            "https://api.upbit.com/v1/candles/minutes/"
            f"{unit}"
        )

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

    response = retry(
        requests.get,
        url,
        params=params,
        timeout=15
    )

    if response is None:
        return None

    try:

        data = response.json()

        if not data:
            return None

        df = pd.DataFrame(data)

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

        if not include_current:

            current = get_current_candle_start(
                unit
            )

            df = df[
                df.datetime < current
            ]

        if df.empty:
            return None

        return (
            df
            .sort_values("datetime")
            .drop_duplicates("datetime")
            .reset_index(drop=True)
        )

    except Exception as e:

        log.error(
            f"UPBIT candle 오류 "
            f"{market} {unit}: {e}"
        )

        return None


# =========================================================
# UPBIT HISTORY
# =========================================================

def history_upbit(
    market,
    unit,
    required=200
):

    all_df = None
    to = None

    for _ in range(MAX_HISTORY_CHUNKS):

        df = get_upbit_candle(
            market,
            unit,
            HISTORY_CHUNK,
            to
        )

        if df is None or df.empty:
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
            return all_df

        to = (
            all_df.datetime.iloc[0]
            .strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        )

    return all_df


# =========================================================
# UPBIT 현재 캔들
# =========================================================

def get_upbit_current_candle(
    market,
    timeframe,
    current_price
):

    df = get_upbit_candle(
        market,
        timeframe,
        200,
        include_current=True
    )

    if df is None or df.empty:
        return None

    try:

        start = get_current_candle_start(
            timeframe
        )

        price = float(current_price)

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
            .drop_duplicates("datetime")
            .reset_index(drop=True)
        )

    except Exception:

        return df


# =========================================================
# OKX OHLCV
# =========================================================

def get_okx_ohlcv(
    inst,
    bar="1H",
    limit=200,
    before=None,
    include_current=False
):

    params = {

        "instId": inst,
        "bar": bar,

        "limit": min(
            max(
                int(limit),
                1
            ),
            200
        )

    }

    if before is not None:
        params["before"] = str(before)

    response = retry(
        requests.get,
        "https://www.okx.com/api/v5/market/candles",
        params=params,
        timeout=15
    )

    if response is None:
        return None

    try:

        data = response.json().get(
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
                df.confirm.astype(str) == "1"
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

            minutes = get_okx_bar_minutes(
                bar
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
            f"OKX 오류 "
            f"{inst} {bar}: {e}"
        )

        return None


# =========================================================
# OKX HISTORY
# =========================================================

def history_okx(
    inst,
    bar,
    required=200
):

    all_df = None
    before = None

    for _ in range(MAX_HISTORY_CHUNKS):

        df = get_okx_ohlcv(
            inst,
            bar,
            HISTORY_CHUNK,
            before
        )

        if df is None or df.empty:
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
            .drop_duplicates("ts")
            .sort_values("ts")
            .reset_index(drop=True)
        )

        if len(all_df) >= required:
            return all_df

        before = int(
            all_df.ts.iloc[0]
        )

    return all_df


# =========================================================
# OKX TICKER
# =========================================================

def get_okx_tickers():

    global okx_ticker_cache

    response = retry(
        requests.get,
        "https://www.okx.com/api/v5/market/tickers",
        params={
            "instType": "SWAP"
        },
        timeout=15
    )

    if response is None:
        return {}

    try:

        result = {}

        for item in response.json().get(
            "data",
            []
        ):

            inst = item.get(
                "instId",
                ""
            )

            if not inst.endswith(
                "-USDT-SWAP"
            ):
                continue

            try:

                last = float(
                    item.get(
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

    except Exception:

        return {}


# =========================================================
# OKX SYMBOL
# =========================================================

def get_okx_symbols():

    response = retry(
        requests.get,
        "https://www.okx.com/api/v5/public/instruments",
        params={
            "instType": "SWAP"
        },
        timeout=15
    )

    if response is None:
        return []

    try:

        return [

            x["instId"]

            for x in response.json().get(
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


def get_okx_cached_price(
    inst
):

    try:

        item = okx_ticker_cache.get(
            inst
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
# OKX 현재 캔들
# =========================================================

def get_okx_current_candle(
    inst,
    timeframe,
    current_price
):

    bar = get_okx_bar(
        timeframe
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

        start = get_current_candle_start(
            timeframe
        )

        price = float(current_price)

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
            .sort_values("ts")
            .drop_duplicates("ts")
            .reset_index(drop=True)
        )

    except Exception:

        return df


# =========================================================
# EMA 계산
# =========================================================

def ema(
    df,
    period
):

    if (
        df is None
        or df.empty
        or "c" not in df
    ):

        return None

    return (
        pd.to_numeric(
            df["c"],
            errors="coerce"
        )
        .ewm(
            span=period,
            adjust=False,
            min_periods=1
        )
        .mean()
    )


# =========================================================
# EMA 실제 수치
# =========================================================

def ema_values(df):

    result = {
        "10": None,
        "30": None,
        "60": None,
        "120": None
    }

    if df is None or df.empty:
        return result

    try:

        periods = {
            "10": EMA1_FASTEST,
            "30": EMA1_FAST,
            "60": EMA1_MID,
            "120": EMA1_SLOW
        }

        for key, period in periods.items():

            series = ema(
                df,
                period
            )

            if (
                series is not None
                and not series.empty
            ):

                value = float(
                    series.iloc[-1]
                )

                if not pd.isna(value):

                    result[key] = value

        return result

    except Exception:

        return result


# =========================================================
# EMA 배열
# =========================================================

def ema_alignment_count(df):

    if df is None or df.empty:

        return {
            "direction": "none",
            "count": 0,
            "values": ema_values(df)
        }

    try:

        e10 = ema(
            df,
            EMA1_FASTEST
        )

        e30 = ema(
            df,
            EMA1_FAST
        )

        e60 = ema(
            df,
            EMA1_MID
        )

        e120 = ema(
            df,
            EMA1_SLOW
        )

        def get_dir(i):

            a = float(e10.iloc[i])
            b = float(e30.iloc[i])
            c = float(e60.iloc[i])
            d = float(e120.iloc[i])

            if a > b > c > d:
                return "long"

            if a < b < c < d:
                return "short"

            return "none"

        current = get_dir(-1)

        if current == "none":

            return {
                "direction": "none",
                "count": 0,
                "values": ema_values(df)
            }

        count = 0

        for i in range(
            len(df) - 1,
            -1,
            -1
        ):

            if get_dir(i) == current:

                count += 1

            else:

                break

        return {
            "direction": current,
            "count": count,
            "values": ema_values(df)
        }

    except Exception:

        return {
            "direction": "none",
            "count": 0,
            "values": ema_values(df)
        }


# =========================================================
# EMA 표시
# =========================================================

def ema_display(
    df,
    current_price=None
):

    x = ema_alignment_count(df)

    direction = x["direction"]

    icon = {
        "long": "🟢",
        "short": "🔴"
    }.get(
        direction,
        "⚪"
    )

    return {
        "display":
            f"{icon}({x['count']})",

        "direction":
            direction,

        "count":
            x["count"],

        "values":
            x.get(
                "values",
                {}
            ),

        "current_price":
            current_price
    }


# =========================================================
# EMA 필터 방향
# =========================================================

def ema_filter_direction(
    e1,
    e_high,
    e_daily=None
):

    selected = []

    if USE_EMA_TIMEFRAME == "Y":
        selected.append(e1)

    if USE_EMA_HIGH_TIMEFRAME == "Y":
        selected.append(e_high)

    if USE_EMA_DAILY_TIMEFRAME == "Y":
        selected.append(e_daily)

    if not selected:

        return {
            "direction": "none",
            "valid": True
        }

    directions = [
        x.get(
            "direction",
            "none"
        )
        for x in selected
    ]

    if all(
        d == "long"
        for d in directions
    ):

        return {
            "direction": "long",
            "valid": True
        }

    if all(
        d == "short"
        for d in directions
    ):

        return {
            "direction": "short",
            "valid": True
        }

    return {
        "direction": "none",
        "valid": False
    }


# =========================================================
# EMA 필터 통과
# =========================================================

def ema_filter_pass(
    e1,
    e_high,
    e_daily=None
):

    selected = []

    if USE_EMA_TIMEFRAME == "Y":
        selected.append(e1)

    if USE_EMA_HIGH_TIMEFRAME == "Y":
        selected.append(e_high)

    if USE_EMA_DAILY_TIMEFRAME == "Y":
        selected.append(e_daily)

    if not selected:
        return True

    for e in selected:

        if e.get(
            "direction",
            "none"
        ) not in (
            "long",
            "short"
        ):

            return False

        if e.get(
            "count",
            0
        ) > EMA1_MAX_COUNT:

            return False

    directions = [
        e.get(
            "direction",
            "none"
        )
        for e in selected
    ]

    return (
        len(set(directions)) == 1
    )


# =========================================================
# ROC 계산
# =========================================================

def roc(
    df,
    period=ROC_PERIOD
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

        period = int(period)

        if period <= 0:
            return None

        result = (
            (
                close
                / close.shift(period)
            )
            - 1
        ) * 100

        return result

    except Exception:

        return None


# =========================================================
# ROC EMA 계산
#
# ROC 값에 EMA를 적용
# =========================================================

def roc_ema(
    roc_series,
    period
):

    if (
        roc_series is None
        or len(roc_series) == 0
    ):

        return None

    try:

        return (
            pd.to_numeric(
                roc_series,
                errors="coerce"
            )
            .ewm(
                span=int(period),
                adjust=False,
                min_periods=1
            )
            .mean()
        )

    except Exception:

        return None


# =========================================================
# ROC 이평선 실제 수치
# =========================================================

def roc_ma_values(
    df
):

    result = {
        "10": None,
        "30": None,
        "60": None,
        "120": None
    }

    if df is None or df.empty:
        return result

    try:

        roc_series = roc(
            df,
            ROC_PERIOD
        )

        if roc_series is None:
            return result

        periods = {

            "10":
                ROC_MA_FASTEST,

            "30":
                ROC_MA_FAST,

            "60":
                ROC_MA_MID,

            "120":
                ROC_MA_SLOW

        }

        for key, period in periods.items():

            series = roc_ema(
                roc_series,
                period
            )

            if (
                series is not None
                and not series.empty
            ):

                value = float(
                    series.iloc[-1]
                )

                if not pd.isna(value):

                    result[key] = value

        return result

    except Exception:

        return result


# =========================================================
# ROC 이평선 배열
#
# ROC EMA10
# ROC EMA30
# ROC EMA60
# ROC EMA120
#
# LONG:
# 10 > 30 > 60 > 120
#
# SHORT:
# 10 < 30 < 60 < 120
# =========================================================

def roc_alignment_count(
    df
):

    if df is None or df.empty:

        return {
            "direction": "none",
            "count": 0,
            "values": roc_ma_values(df)
        }

    try:

        roc_series = roc(
            df,
            ROC_PERIOD
        )

        if roc_series is None:
            raise ValueError(
                "ROC 계산 실패"
            )

        r10 = roc_ema(
            roc_series,
            ROC_MA_FASTEST
        )

        r30 = roc_ema(
            roc_series,
            ROC_MA_FAST
        )

        r60 = roc_ema(
            roc_series,
            ROC_MA_MID
        )

        r120 = roc_ema(
            roc_series,
            ROC_MA_SLOW
        )

        def get_dir(i):

            a = float(r10.iloc[i])
            b = float(r30.iloc[i])
            c = float(r60.iloc[i])
            d = float(r120.iloc[i])

            if a > b > c > d:

                return "long"

            if a < b < c < d:

                return "short"

            return "none"

        current = get_dir(-1)

        if current == "none":

            return {
                "direction": "none",
                "count": 0,
                "values": roc_ma_values(df)
            }

        count = 0

        for i in range(
            len(df) - 1,
            -1,
            -1
        ):

            if get_dir(i) == current:

                count += 1

            else:

                break

        return {

            "direction":
                current,

            "count":
                count,

            "values":
                roc_ma_values(df)

        }

    except Exception:

        return {

            "direction":
                "none",

            "count":
                0,

            "values":
                roc_ma_values(df)

        }


# =========================================================
# ROC 필터 방향
# =========================================================

def roc_filter_direction(
    r1,
    r_high,
    r_daily=None
):

    selected = []

    if USE_ROC_TIMEFRAME == "Y":

        selected.append(
            r1
        )

    if USE_ROC_HIGH_TIMEFRAME == "Y":

        selected.append(
            r_high
        )

    if USE_ROC_DAILY_TIMEFRAME == "Y":

        selected.append(
            r_daily
        )

    if not selected:

        return {
            "direction": "none",
            "valid": True
        }

    directions = [

        x.get(
            "direction",
            "none"
        )

        for x in selected

    ]

    if all(
        d == "long"
        for d in directions
    ):

        return {

            "direction":
                "long",

            "valid":
                True

        }

    if all(
        d == "short"
        for d in directions
    ):

        return {

            "direction":
                "short",

            "valid":
                True

        }

    return {

        "direction":
            "none",

        "valid":
            False

    }


# =========================================================
# ROC 필터 통과
# =========================================================

def roc_filter_pass(
    r1,
    r_high,
    r_daily=None
):

    selected = []

    if USE_ROC_TIMEFRAME == "Y":

        selected.append(
            r1
        )

    if USE_ROC_HIGH_TIMEFRAME == "Y":

        selected.append(
            r_high
        )

    if USE_ROC_DAILY_TIMEFRAME == "Y":

        selected.append(
            r_daily
        )

    if not selected:

        return True

    for r in selected:

        if r.get(
            "direction",
            "none"
        ) not in (
            "long",
            "short"
        ):

            return False

        if r.get(
            "count",
            0
        ) > ROC_MA_MAX_COUNT:

            return False

    directions = [

        r.get(
            "direction",
            "none"
        )

        for r in selected

    ]

    return (
        len(set(directions)) == 1
    )


# =========================================================
# ROC 필터 종합
# =========================================================

def roc_filter_analysis(
    r1,
    r_high,
    r_daily=None
):

    direction_data = (
        roc_filter_direction(
            r1,
            r_high,
            r_daily
        )
    )

    passed = (
        direction_data["valid"]
        and
        roc_filter_pass(
            r1,
            r_high,
            r_daily
        )
    )

    return {

        "direction":
            direction_data["direction"],

        "valid":
            passed

    }


# =========================================================
# ROC 카운트
#
# 0선 위 연속 캔들 수
# 0선 아래 연속 캔들 수
# =========================================================

def roc_count(
    series,
    level,
    above=True
):

    if series is None:
        return 0

    try:

        values = list(
            reversed(
                series.tolist()
            )
        )

        count = 0

        for value in values:

            if pd.isna(value):
                break

            value = float(value)

            if above:

                if value > level:

                    count += 1

                else:

                    break

            else:

                if value < level:

                    count += 1

                else:

                    break

        return count

    except Exception:

        return 0


# =========================================================
# ROC 참고 분석
# =========================================================

def roc_reference_analysis(
    df
):

    result = {

        "value":
            None,

        "previous":
            None,

        "long_count":
            0,

        "short_count":
            0,

        "direction":
            "flat",

        "change":
            0.0,

        "state":
            "neutral",

        "display":
            "-",

        "ma_direction":
            "none",

        "ma_count":
            0,

        "ma_values":
            {}

    }

    if df is None or df.empty:
        return result

    try:

        series = roc(df)

        if series is None or series.empty:
            return result

        valid = series.dropna()

        if valid.empty:
            return result

        value = float(
            valid.iloc[-1]
        )

        previous = None

        if len(valid) >= 2:

            previous = float(
                valid.iloc[-2]
            )

        if previous is not None:

            change = (
                value
                - previous
            )

        else:

            change = 0.0

        if change > 0:

            direction = "up"

        elif change < 0:

            direction = "down"

        else:

            direction = "flat"

        long_count = roc_count(
            series,
            ROC_LONG_LEVEL,
            True
        )

        short_count = roc_count(
            series,
            ROC_SHORT_LEVEL,
            False
        )

        if value > ROC_LONG_LEVEL:

            state = "long"

        elif value < ROC_SHORT_LEVEL:

            state = "short"

        else:

            state = "neutral"

        ma = roc_alignment_count(
            df
        )

        result.update({

            "value":
                value,

            "previous":
                previous,

            "long_count":
                long_count,

            "short_count":
                short_count,

            "direction":
                direction,

            "change":
                change,

            "state":
                state,

            "display":
                f"{value:.2f}%",

            "ma_direction":
                ma["direction"],

            "ma_count":
                ma["count"],

            "ma_values":
                ma["values"]

        })

        return result

    except Exception as e:

        log.error(
            f"ROC 참고 분석 오류: {e}"
        )

        return result


# =========================================================
# ROC 신호용 분석
# =========================================================

def roc_analysis(
    df_confirmed,
    df_current
):

    result = {

        "roc10":
            None,

        "roc10_previous":
            None,

        "roc10_count":
            0,

        "roc10_short_count":
            0,

        "long_count":
            0,

        "short_count":
            0,

        "state":
            "none",

        "direction":
            "flat",

        "change":
            0.0,

        "display":
            "-",

        "ma_direction":
            "none",

        "ma_count":
            0,

        "ma_values":
            {}

    }

    if (
        df_confirmed is None
        or df_confirmed.empty
        or df_current is None
        or df_current.empty
    ):

        return result

    try:

        confirmed_roc = roc(
            df_confirmed
        )

        current_roc = roc(
            df_current
        )

        if (
            confirmed_roc is None
            or current_roc is None
        ):

            return result

        confirmed_valid = (
            confirmed_roc.dropna()
        )

        current_valid = (
            current_roc.dropna()
        )

        if (
            confirmed_valid.empty
            or current_valid.empty
        ):

            return result

        previous = float(
            confirmed_valid.iloc[-1]
        )

        current_value = float(
            current_valid.iloc[-1]
        )

        change = (
            current_value
            - previous
        )

        if change > 0:

            direction = "up"

        elif change < 0:

            direction = "down"

        else:

            direction = "flat"

        long_count = roc_count(
            current_roc,
            ROC_LONG_LEVEL,
            True
        )

        short_count = roc_count(
            current_roc,
            ROC_SHORT_LEVEL,
            False
        )

        if current_value > ROC_LONG_LEVEL:

            state = "long"

        elif current_value < ROC_SHORT_LEVEL:

            state = "short"

        else:

            state = "neutral"

        ma = roc_alignment_count(
            df_current
        )

        result.update({

            "roc10":
                current_value,

            "roc10_previous":
                previous,

            "roc10_count":
                long_count,

            "roc10_short_count":
                short_count,

            "long_count":
                long_count,

            "short_count":
                short_count,

            "state":
                state,

            "direction":
                direction,

            "change":
                change,

            "display":
                f"{current_value:.2f}%",

            "ma_direction":
                ma["direction"],

            "ma_count":
                ma["count"],

            "ma_values":
                ma["values"]

        })

        return result

    except Exception as e:

        log.error(
            f"ROC 분석 오류: {e}"
        )

        return result


# =========================================================
# 일간 등락률 UPBIT
# =========================================================

def daily_change_upbit(
    market
):

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
            current - previous
        ) / previous * 100

    except Exception:

        return None


# =========================================================
# OKX 일간 등락률
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
            .set_index("datetime")
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

        return (
            current - previous
        ) / previous * 100

    except Exception:

        return None


# =========================================================
# 등락률
# =========================================================

def get_change_value(x):

    if x is None:
        return None

    try:

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


def is_positive_day(row):

    if not row:
        return False

    value = get_change_value(
        row.get(
            "change_value"
        )
    )

    return (
        value is not None
        and value > 0
    )


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
# EMA + ROC 자격
#
# 최종 LONG:
#
# 1H EMA 정배열
# 4H EMA 정배열
# +
# 1H ROC 이평 정배열
# 4H ROC 이평 정배열
# +
1H ROC10 0선 상방
# =========================================================

def get_signal_qualified(
    e1,
    e_high,
    e_daily,
    r,
    r_high,
    r_daily
):

    # =====================================================
    # EMA 필터
    # =====================================================

    ema_filter = ema_filter_direction(
        e1,
        e_high,
        e_daily
    )

    ema_direction = ema_filter.get(
        "direction",
        "none"
    )

    ema_valid = ema_filter.get(
        "valid",
        False
    )

    ema_filter_count_valid = (
        ema_filter_pass(
            e1,
            e_high,
            e_daily
        )
    )

    ema_ready = (
        ema_valid
        and
        ema_filter_count_valid
    )


    # =====================================================
    # ROC 이평 필터
    # =====================================================

    roc_filter = roc_filter_analysis(
        r,
        r_high,
        r_daily
    )

    roc_direction = roc_filter.get(
        "direction",
        "none"
    )

    roc_ready = roc_filter.get(
        "valid",
        False
    )


    # =====================================================
    # ROC 0선
    # =====================================================

    long_roc = (
        int(
            r.get(
                "long_count",
                0
            )
            or 0
        ) >= 1
    )

    short_roc = (
        int(
            r.get(
                "short_count",
                0
            )
            or 0
        ) >= 1
    )


    # =====================================================
    # 최종
    # =====================================================

    long_ready = (
        ema_ready
        and
        roc_ready
        and
        ema_direction == "long"
        and
        roc_direction == "long"
    )

    short_ready = (
        ema_ready
        and
        roc_ready
        and
        ema_direction == "short"
        and
        roc_direction == "short"
    )

    return {

        "breakout_qualified":
            (
                long_ready
                and
                long_roc
            ),

        "short_breakout_qualified":
            (
                short_ready
                and
                short_roc
            ),

        "filter_direction":
            ema_direction,

        "roc_filter_direction":
            roc_direction,

        "ema_filter_valid":
            ema_ready,

        "roc_filter_valid":
            roc_ready

    }


# =========================================================
# 분석 - OKX
# =========================================================

def analyze_okx(
    market,
    current_price=None
):

    ema_bar = get_okx_bar(
        EMA_TIMEFRAME
    )

    ema_high_bar = get_okx_bar(
        EMA_HIGH_TIMEFRAME
    )

    ema_daily_bar = get_okx_bar(
        EMA_DAILY_TIMEFRAME
    )

    roc_bar = get_okx_bar(
        ROC_TIMEFRAME
    )

    roc_high_bar = get_okx_bar(
        ROC_HIGH_TIMEFRAME
    )

    roc_daily_bar = get_okx_bar(
        ROC_DAILY_TIMEFRAME
    )

    if not all([
        ema_bar,
        ema_high_bar,
        ema_daily_bar,
        roc_bar,
        roc_high_bar,
        roc_daily_bar
    ]):

        return None

    df_ema = history_okx(
        market,
        ema_bar
    )

    df_ema_high = history_okx(
        market,
        ema_high_bar
    )

    df_ema_daily = history_okx(
        market,
        ema_daily_bar
    )

    df_roc = history_okx(
        market,
        roc_bar
    )

    df_roc_current = (
        get_okx_current_candle(
            market,
            ROC_TIMEFRAME,
            current_price
        )
    )

    df_roc_high = history_okx(
        market,
        roc_high_bar
    )

    df_roc_daily = history_okx(
        market,
        roc_daily_bar
    )

    if (
        df_ema is None
        or df_ema.empty
        or df_roc is None
        or df_roc.empty
    ):

        return None

    e1 = ema_display(
        df_ema,
        current_price
    )

    e_high = ema_display(
        df_ema_high,
        current_price
    )

    e_daily = ema_display(
        df_ema_daily,
        current_price
    )

    r = roc_analysis(
        df_roc,
        df_roc_current
    )

    r_high = roc_reference_analysis(
        df_roc_high
    )

    r_daily = roc_reference_analysis(
        df_roc_daily
    )

    changes = daily_changes(
        df_ema
    )

    q = get_signal_qualified(
        e1,
        e_high,
        e_daily,
        r,
        r_high,
        r_daily
    )

    return {

        "ema_1h":
            e1,

        "ema_high":
            e_high,

        "ema_daily":
            e_daily,

        "roc":
            r,

        "roc_high":
            r_high,

        "roc_daily":
            r_daily,

        "changes":
            changes,

        **q,

        "direction_1h":
            q["filter_direction"],

        "df1h":
            df_ema

    }


# =========================================================
# 분석 - UPBIT
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

    df_ema = history_upbit(
        market,
        EMA_TIMEFRAME
    )

    df_ema_high = history_upbit(
        market,
        EMA_HIGH_TIMEFRAME
    )

    df_ema_daily = history_upbit(
        market,
        EMA_DAILY_TIMEFRAME
    )

    df_roc = history_upbit(
        market,
        ROC_TIMEFRAME
    )

    df_roc_current = (
        get_upbit_current_candle(
            market,
            ROC_TIMEFRAME,
            current_price
        )
    )

    df_roc_high = history_upbit(
        market,
        ROC_HIGH_TIMEFRAME
    )

    df_roc_daily = history_upbit(
        market,
        ROC_DAILY_TIMEFRAME
    )

    changes = daily_change_upbit(
        market
    )

    if (
        df_ema is None
        or df_ema.empty
        or df_roc is None
        or df_roc.empty
    ):

        return None

    e1 = ema_display(
        df_ema,
        current_price
    )

    e_high = ema_display(
        df_ema_high,
        current_price
    )

    e_daily = ema_display(
        df_ema_daily,
        current_price
    )

    r = roc_analysis(
        df_roc,
        df_roc_current
    )

    r_high = roc_reference_analysis(
        df_roc_high
    )

    r_daily = roc_reference_analysis(
        df_roc_daily
    )

    q = get_signal_qualified(
        e1,
        e_high,
        e_daily,
        r,
        r_high,
        r_daily
    )

    return {

        "ema_1h":
            e1,

        "ema_high":
            e_high,

        "ema_daily":
            e_daily,

        "roc":
            r,

        "roc_high":
            r_high,

        "roc_daily":
            r_daily,

        "changes":
            changes,

        **q,

        "direction_1h":
            q["filter_direction"],

        "df1h":
            df_ema

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

    if analysis is None:

        analysis = {

            "ema_1h": {},
            "ema_high": {},
            "ema_daily": {},

            "roc": {},
            "roc_high": {},
            "roc_daily": {},

            "changes": None,

            "breakout_qualified":
                False,

            "short_breakout_qualified":
                False,

            "roc_filter_direction":
                "none",

            "roc_filter_valid":
                False

        }

    return {

        "rank":
            rank,

        "name":
            name,

        "change":
            format_change(
                analysis.get(
                    "changes"
                )
            ),

        "change_value":
            get_change_value(
                analysis.get(
                    "changes"
                )
            ),

        "volume":
            format_volume(
                volume
            ),

        "current_price":
            current_price,

        "ema_1h":
            analysis.get(
                "ema_1h",
                {}
            ),

        "ema_high":
            analysis.get(
                "ema_high",
                {}
            ),

        "ema_daily":
            analysis.get(
                "ema_daily",
                {}
            ),

        "roc":
            analysis.get(
                "roc",
                {}
            ),

        "roc_high":
            analysis.get(
                "roc_high",
                {}
            ),

        "roc_daily":
            analysis.get(
                "roc_daily",
                {}
            ),

        "breakout_qualified":
            analysis.get(
                "breakout_qualified",
                False
            ),

        "short_breakout_qualified":
            analysis.get(
                "short_breakout_qualified",
                False
            ),

        "direction":
            analysis.get(
                "direction_1h",
                "none"
            ),

        "roc_filter_direction":
            analysis.get(
                "roc_filter_direction",
                "none"
            ),

        "roc_filter_valid":
            analysis.get(
                "roc_filter_valid",
                False
            )

    }


# =========================================================
# ROC 신호
# =========================================================

def is_roc_signal(row):

    if not row:
        return False

    r = row.get(
        "roc",
        {}
    )

    long_count = int(
        r.get(
            "long_count",
            0
        )
        or 0
    )

    short_count = int(
        r.get(
            "short_count",
            0
        )
        or 0
    )

    return (
        long_count == 1
        or
        short_count == 1
    )


# =========================================================
# 추세 진행
# =========================================================

def is_sun_cloud_signal(row):

    if not row:
        return False

    r = row.get(
        "roc",
        {}
    )

    long_count = int(
        r.get(
            "long_count",
            0
        )
        or 0
    )

    short_count = int(
        r.get(
            "short_count",
            0
        )
        or 0
    )

    return (
        long_count >= ROC_PROGRESS_START_COUNT
        or
        short_count >= ROC_PROGRESS_START_COUNT
    )


# =========================================================
# UPBIT 업데이트
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

    rows = []

    for rank, item in enumerate(
        markets[:TOP_N],
        1
    ):

        market = item["market"]

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
                current_price=price
            )

        except Exception as e:

            log.error(
                f"UPBIT 분석 오류 "
                f"{market}: {e}"
            )

            analysis = None

        rows.append(
            make_row(
                rank,
                coin,
                item["volume_24h"],
                analysis,
                price
            )
        )

    latest_upbit_data = rows

    latest_upbit_update_time = kst()

    log.info(
        "UPBIT 완료 / "
        f"TOP{TOP_N} / "
        f"ROC신호="
        f"{sum(is_roc_signal(x) for x in rows)} / "
        f"진행="
        f"{sum(is_sun_cloud_signal(x) for x in rows)}"
    )


# =========================================================
# OKX 업데이트
# =========================================================

def update_okx(usdt):

    global latest_okx_data
    global latest_okx_update_time
    global okx_1h_cache
    global okx_1h_cache_time

    if (
        not usdt
        or usdt <= 0
    ):

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

    for symbol in symbols:

        volume = (
            get_okx_volume_cached(
                symbol,
                usdt
            )
        )

        if (
            volume
            and volume > 0
        ):

            volumes[
                symbol
            ] = volume

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

            analysis = analyze(
                symbol,
                True,
                price
            )

        except Exception as e:

            log.error(
                f"OKX 분석 오류 "
                f"{symbol}: {e}"
            )

            analysis = None

        rows.append(
            make_row(
                rank,
                name,
                volumes[symbol],
                analysis,
                price
            )
        )

    latest_okx_data = rows

    okx_1h_cache_time = kst()
    latest_okx_update_time = kst()

    return True


# =========================================================
# 시장 시황 데이터 생성
# =========================================================

def update_market_summary():

    global latest_market_source
    global latest_market_data

    latest_market_data = {}

    if USE_OKX == "Y":

        for row in latest_okx_data:

            name = row.get(
                "name",
                ""
            )

            coin = name.split(" ")[0]

            if coin in (
                "BTC",
                "ETH"
            ):

                latest_market_data[
                    coin
                ] = row

        if (
            "BTC" in latest_market_data
            or
            "ETH" in latest_market_data
        ):

            latest_market_source = "OKX"

            return

    for row in latest_upbit_data:

        coin = row.get(
            "name"
        )

        if coin in (
            "BTC",
            "ETH"
        ):

            latest_market_data[
                coin
            ] = row

    latest_market_source = "UPBIT"


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    global latest_usdt_krw
    global latest_upbit_data
    global latest_okx_data

    if not update_lock.acquire(False):

        log.warning(
            "이전 조회 진행 중 → 건너뜀"
        )

        return

    try:

        if USE_UPBIT == "Y":

            try:

                update_upbit()

            except Exception as e:

                log.exception(
                    f"UPBIT 업데이트 오류: {e}"
                )

        else:

            latest_upbit_data = []

        if USE_OKX == "Y":

            try:

                usdt = get_usdt_krw()

                if usdt:

                    latest_usdt_krw = usdt

                else:

                    usdt = latest_usdt_krw

                if usdt > 0:

                    update_okx(
                        usdt
                    )

            except Exception as e:

                log.exception(
                    f"OKX 업데이트 오류: {e}"
                )

        else:

            latest_okx_data = []

        update_market_summary()

    finally:

        update_lock.release()


# =========================================================
# 숫자 표시
# =========================================================

def format_indicator_value(value):

    if value is None:
        return "-"

    try:

        return f"{float(value):,.2f}"

    except Exception:

        return "-"


# =========================================================
# EMA HTML
# =========================================================

def ema_detail_html(
    e,
    timeframe,
    use_flag="Y"
):

    if not e:

        suffix = ""

        if use_flag != "Y":

            suffix = " 참고"

        return (
            '<div class="indicator-line">'
            f'<span class="indicator-label">'
            f'{timeframe}{suffix}'
            '</span>'
            '<span class="ema-direction">'
            '⚪(0)'
            '</span>'
            '</div>'
        )

    direction = e.get(
        "direction",
        "none"
    )

    count = int(
        e.get(
            "count",
            0
        )
        or 0
    )

    if direction == "long":

        icon = "🟢"

    elif direction == "short":

        icon = "🔴"

    else:

        icon = "⚪"

    label = timeframe

    if use_flag != "Y":

        label += " 참고"

    return f"""
    <div class="indicator-line ema-detail-line">

        <span class="indicator-label">
            {label}
        </span>

        <span class="ema-direction">
            {icon}({count})
        </span>

    </div>
    """


# =========================================================
# ROC 방향 HTML
# =========================================================

def roc_direction_html(r):

    if not r:
        return ""

    direction = r.get(
        "direction",
        "flat"
    )

    change = r.get(
        "change",
        0
    )

    try:

        change = float(change)

    except Exception:

        change = 0.0

    if direction == "up":

        return (
            '<span class="roc-up">'
            f'↗ +{change:.2f}'
            '</span>'
        )

    if direction == "down":

        return (
            '<span class="roc-down">'
            f'↘ {change:.2f}'
            '</span>'
        )

    return (
        '<span class="roc-flat">'
        '→ 0.00'
        '</span>'
    )


# =========================================================
# ROC 이평 배열 HTML
# =========================================================

def roc_ma_direction_html(r):

    if not r:

        return ""

    direction = r.get(
        "ma_direction",
        "none"
    )

    count = int(
        r.get(
            "ma_count",
            0
        )
        or 0
    )

    if direction == "long":

        return (
            '<span class="roc-ma-long">'
            f'🟢({count})'
            '</span>'
        )

    if direction == "short":

        return (
            '<span class="roc-ma-short">'
            f'🔴({count})'
            '</span>'
        )

    return (
        '<span class="roc-ma-neutral">'
        '⚪(0)'
        '</span>'
    )


# =========================================================
# ROC HTML
# =========================================================

def roc_html(r):

    if not r:

        return (
            '<span class="roc-zero">-</span>'
        )

    value = r.get(
        "roc10"
    )

    if value is None:

        return (
            '<span class="roc-zero">-</span>'
        )

    try:

        value = float(value)

    except Exception:

        return (
            '<span class="roc-zero">-</span>'
        )

    long_count = int(
        r.get(
            "long_count",
            0
        )
        or 0
    )

    short_count = int(
        r.get(
            "short_count",
            0
        )
        or 0
    )

    slope = roc_direction_html(
        r
    )

    ma_direction = (
        roc_ma_direction_html(
            r
        )
    )

    if value > ROC_LONG_LEVEL:

        return (
            '<span class="roc-long">'
            f'🟢{value:+.2f}% '
            f'{slope}'
            f' ({long_count}) '
            f'{ma_direction}'
            '</span>'
        )

    if value < ROC_SHORT_LEVEL:

        return (
            '<span class="roc-short">'
            f'🔴{value:+.2f}% '
            f'{slope}'
            f' ({short_count}) '
            f'{ma_direction}'
            '</span>'
        )

    return (
        '<span class="roc-neutral">'
        f'{value:+.2f}% '
        f'{slope}'
        ' (0) '
        f'{ma_direction}'
        '</span>'
    )


# =========================================================
# ROC 참고 HTML
# =========================================================

def roc_reference_html(r):

    if not r:

        return (
            '<span class="roc-reference">-</span>'
        )

    value = r.get(
        "value"
    )

    if value is None:

        return (
            '<span class="roc-reference">-</span>'
        )

    try:

        value = float(value)

    except Exception:

        return (
            '<span class="roc-reference">-</span>'
        )

    long_count = int(
        r.get(
            "long_count",
            0
        )
        or 0
    )

    short_count = int(
        r.get(
            "short_count",
            0
        )
        or 0
    )

    slope = roc_direction_html(
        r
    )

    ma_direction = (
        roc_ma_direction_html(
            r
        )
    )

    if value > ROC_LONG_LEVEL:

        return (
            '<span class="roc-reference-long">'
            f'🟢{value:+.2f}% '
            f'{slope}'
            f' ({long_count}) '
            f'{ma_direction}'
            '</span>'
        )

    if value < ROC_SHORT_LEVEL:

        return (
            '<span class="roc-reference-short">'
            f'🔴{value:+.2f}% '
            f'{slope}'
            f' ({short_count}) '
            f'{ma_direction}'
            '</span>'
        )

    return (
        '<span class="roc-reference">'
        f'{value:+.2f}% '
        f'{slope}'
        ' (0) '
        f'{ma_direction}'
        '</span>'
    )


# =========================================================
# ROC 시간봉
# =========================================================

def roc_lines_html(
    r,
    r_high,
    r_daily
):

    return (

        '<div class="indicator-line">'

        '<span class="indicator-label">'
        f'{format_timeframe(ROC_TIMEFRAME)}'
        '</span>'

        f'{roc_html(r)}'

        '</div>'


        '<div class="indicator-line">'

        '<span class="indicator-label">'
        f'{format_timeframe(ROC_HIGH_TIMEFRAME)}'
        '</span>'

        f'{roc_reference_html(r_high)}'

        '</div>'


        '<div class="indicator-line">'

        '<span class="indicator-label">'
        f'{format_timeframe(ROC_DAILY_TIMEFRAME)}'
        '</span>'

        f'{roc_reference_html(r_daily)}'

        '</div>'

    )


# =========================================================
# 신호 HTML
# =========================================================

def signal_html(row):

    r = row.get(
        "roc",
        {}
    )

    long_count = int(
        r.get(
            "long_count",
            0
        )
        or 0
    )

    short_count = int(
        r.get(
            "short_count",
            0
        )
        or 0
    )

    if long_count == 1:

        if row.get(
            "breakout_qualified",
            False
        ):

            return (
                '<span '
                'class="signal-icon long-breakout" '
                'title="ROC10 0선 상방 진입 · EMA 1H/4H 정배열 · ROC 이평 1H/4H 정배열">'
                '🚀①'
                '</span>'
            )

        return (
            '<span '
            'class="signal-icon roc-warning-qualified" '
            'title="ROC10 0선 상방 진입 · EMA 또는 ROC 이평 필터 미충족">'
            '⚠️①'
            '</span>'
        )

    if short_count == 1:

        if row.get(
            "short_breakout_qualified",
            False
        ):

            return (
                '<span '
                'class="signal-icon short-breakout" '
                'title="ROC10 0선 하방 진입 · EMA 1H/4H 역배열 · ROC 이평 1H/4H 역배열">'
                '🔻①'
                '</span>'
            )

        return (
            '<span '
            'class="signal-icon roc-warning-qualified" '
            'title="ROC10 0선 하방 진입 · EMA 또는 ROC 이평 필터 미충족">'
            '⚠️①'
            '</span>'
        )

    return (
        '<span class="muted">-</span>'
    )


# =========================================================
# 추세 진행 HTML
# =========================================================

def progress_signal_html(row):

    r = row.get(
        "roc",
        {}
    )

    long_count = int(
        r.get(
            "long_count",
            0
        )
        or 0
    )

    short_count = int(
        r.get(
            "short_count",
            0
        )
        or 0
    )

    if long_count >= 2:

        return (
            '<span '
            'class="signal-icon long-progress" '
            f'title="ROC10 0선 상방 추세 진행 · {long_count}">'
            '☀️'
            '</span>'
        )

    if short_count >= 2:

        return (
            '<span '
            'class="signal-icon short-progress" '
            f'title="ROC10 0선 하방 추세 진행 · {short_count}">'
            '🌧️'
            '</span>'
        )

    return (
        '<span class="muted">-</span>'
    )


# =========================================================
# ROW CLASS
# =========================================================

def row_class(x):

    r = x.get(
        "roc",
        {}
    )

    long_count = int(
        r.get(
            "long_count",
            0
        )
        or 0
    )

    short_count = int(
        r.get(
            "short_count",
            0
        )
        or 0
    )

    if (
        long_count == 1
        and
        x.get(
            "breakout_qualified",
            False
        )
    ):

        return "breakout-qualified"

    if (
        short_count == 1
        and
        x.get(
            "short_breakout_qualified",
            False
        )
    ):

        return "short-breakout-qualified"

    if long_count >= 2:

        return "progress-qualified"

    if short_count >= 2:

        return "short-progress-qualified"

    if long_count == 1:

        return "roc-warning-row"

    if short_count == 1:

        return "roc-warning-row"

    return ""


# =========================================================
# ROW HTML
# =========================================================

def rows_html(
    data,
    signal_mode="normal"
):

    output = []

    for x in data:

        if signal_mode == "progress":

            signal = progress_signal_html(
                x
            )

        else:

            signal = signal_html(
                x
            )

        ema_content = (

            ema_detail_html(
                x.get(
                    "ema_1h",
                    {}
                ),
                format_timeframe(
                    EMA_TIMEFRAME
                ),
                USE_EMA_TIMEFRAME
            )

            +

            ema_detail_html(
                x.get(
                    "ema_high",
                    {}
                ),
                format_timeframe(
                    EMA_HIGH_TIMEFRAME
                ),
                USE_EMA_HIGH_TIMEFRAME
            )

            +

            ema_detail_html(
                x.get(
                    "ema_daily",
                    {}
                ),
                format_timeframe(
                    EMA_DAILY_TIMEFRAME
                ),
                USE_EMA_DAILY_TIMEFRAME
            )

        )

        roc_content = roc_lines_html(

            x.get(
                "roc",
                {}
            ),

            x.get(
                "roc_high",
                {}
            ),

            x.get(
                "roc_daily",
                {}
            )

        )

        output.append(
            f"""
            <tr class="{row_class(x)}">

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

                <td class="ema-cell">

                    <div class="indicator-title">
                        EMA
                    </div>

                    {ema_content}

                </td>

                <td class="roc-column">

                    <div class="indicator-title">
                        ROC10 / ROC MA
                    </div>

                    {roc_content}

                </td>

                <td class="signal-cell">

                    {signal}

                </td>

            </tr>
            """
        )

    return "".join(
        output
    )


# =========================================================
# TABLE
# =========================================================

def table_html(
    data,
    signal_mode="normal"
):

    rows = rows_html(
        data,
        signal_mode
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

                    <th>
                        EMA<br>
                        {format_timeframe(EMA_TIMEFRAME)}/
                        {format_timeframe(EMA_HIGH_TIMEFRAME)}/
                        {format_timeframe(EMA_DAILY_TIMEFRAME)}
                    </th>

                    <th>
                        ROC10 / ROC MA<br>
                        {format_timeframe(ROC_TIMEFRAME)}/
                        {format_timeframe(ROC_HIGH_TIMEFRAME)}/
                        {format_timeframe(ROC_DAILY_TIMEFRAME)}
                    </th>

                    <th>신호</th>

                </tr>

            </thead>

            <tbody>

                {rows}

            </tbody>

        </table>

    </div>

    """


# =========================================================
# ROC 신호
# =========================================================

def roc_signal_section(
    title,
    data,
    update_time,
    upbit=False
):

    rows = []

    for x in data:

        if upbit:

            if not is_positive_day(x):

                continue

        if is_roc_signal(x):

            rows.append(x)

    return f"""

    <div class="section-title roc-section-title">

        <span class="section-title-main">
            🔥 ROC 추세 신호
        </span>

        <span class="section-title-sub">

            TOP{TOP_N} ·
            {format_timeframe(ROC_TIMEFRAME)}
            ROC10 ·
            0선 진입 ·
            카운트 1 ·
            ROC 이평 {get_roc_filter_timeframes()} 필터 ·
            {update_time} KST

        </span>

    </div>

    {table_html(
        rows,
        "normal"
    )}

    """


# =========================================================
# 추세 진행
# =========================================================

def progress_section(
    data,
    update_time,
    upbit=False
):

    rows = []

    for x in data:

        if upbit:

            if not is_positive_day(x):

                continue

        if is_sun_cloud_signal(x):

            rows.append(x)

    return f"""

    <div class="section-title progress-section-title">

        <span class="section-title-main">
            ☀️🌧️ 추세 진행 리스트
        </span>

        <span class="section-title-sub">

            {format_timeframe(ROC_TIMEFRAME)}
            ROC10 0선 카운트 2 이상 ·
            EMA {get_ema_filter_timeframes()} 적용 ·
            ROC 이평 {get_roc_filter_timeframes()} 적용 ·
            {format_timeframe(ROC_DAILY_TIMEFRAME)} 참고 ·
            {update_time} KST

        </span>

    </div>

    {table_html(
        rows,
        "progress"
    )}

    """


# =========================================================
# 시장 시황
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


def market_change_html(value):

    if value is None:

        return "-"

    try:

        value = float(value)

    except Exception:

        return "-"

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


def market_direction_html(
    direction,
    count
):

    try:

        count = int(count)

    except Exception:

        count = 0

    if direction == "long":

        return (
            '<span class="market-up">'
            f'🟢 {count}'
            '</span>'
        )

    if direction == "short":

        return (
            '<span class="market-down">'
            f'🔴 {count}'
            '</span>'
        )

    return (
        '<span class="market-zero">'
        '⚪ 0'
        '</span>'
    )


# =========================================================
# 시장 ROC
# =========================================================

def market_roc_html(r):

    if not r:

        return "-"

    value = r.get(
        "roc10"
    )

    if value is None:

        return "-"

    try:

        value = float(value)

    except Exception:

        return "-"

    direction = r.get(
        "direction",
        "flat"
    )

    change = r.get(
        "change",
        0
    )

    try:

        change = float(change)

    except Exception:

        change = 0.0

    if direction == "up":

        slope = (
            '<span class="roc-up">'
            f'↗ +{change:.2f}'
            '</span>'
        )

    elif direction == "down":

        slope = (
            '<span class="roc-down">'
            f'↘ {change:.2f}'
            '</span>'
        )

    else:

        slope = (
            '<span class="roc-flat">'
            '→ 0.00'
            '</span>'
        )

    ma = roc_ma_direction_html(
        r
    )

    if value > ROC_LONG_LEVEL:

        count = int(
            r.get(
                "long_count",
                0
            )
            or 0
        )

        return (
            '<span class="market-up">'
            f'🟢 {value:+.2f}% '
            f'{slope} ({count}) '
            f'{ma}'
            '</span>'
        )

    if value < ROC_SHORT_LEVEL:

        count = int(
            r.get(
                "short_count",
                0
            )
            or 0
        )

        return (
            '<span class="market-down">'
            f'🔴 {value:+.2f}% '
            f'{slope} ({count}) '
            f'{ma}'
            '</span>'
        )

    return (
        '<span class="market-zero">'
        f'{value:+.2f}% '
        f'{slope} (0) '
        f'{ma}'
        '</span>'
    )


# =========================================================
# 시장 ROC 참고
# =========================================================

def market_roc_reference_html(r):

    if not r:

        return "-"

    value = r.get(
        "value"
    )

    if value is None:

        return "-"

    try:

        value = float(value)

    except Exception:

        return "-"

    direction = r.get(
        "direction",
        "flat"
    )

    change = r.get(
        "change",
        0
    )

    try:

        change = float(change)

    except Exception:

        change = 0.0

    if direction == "up":

        slope = (
            '<span class="roc-up">'
            f'↗ +{change:.2f}'
            '</span>'
        )

    elif direction == "down":

        slope = (
            '<span class="roc-down">'
            f'↘ {change:.2f}'
            '</span>'
        )

    else:

        slope = (
            '<span class="roc-flat">'
            '→ 0.00'
            '</span>'
        )

    ma = roc_ma_direction_html(
        r
    )

    if value > ROC_LONG_LEVEL:

        count = int(
            r.get(
                "long_count",
                0
            )
            or 0
        )

        return (
            '<span class="market-up">'
            f'🟢 {value:+.2f}% '
            f'{slope} ({count}) '
            f'{ma}'
            '</span>'
        )

    if value < ROC_SHORT_LEVEL:

        count = int(
            r.get(
                "short_count",
                0
            )
            or 0
        )

        return (
            '<span class="market-down">'
            f'🔴 {value:+.2f}% '
            f'{slope} ({count}) '
            f'{ma}'
            '</span>'
        )

    return (
        '<span class="market-zero">'
        f'{value:+.2f}% '
        f'{slope} (0) '
        f'{ma}'
        '</span>'
    )


# =========================================================
# 시장 EMA 실제 수치
# =========================================================

def market_ema_numbers(e):

    return ""


# =========================================================
# 시장 패널
# =========================================================

def market_panel(
    row,
    symbol,
    icon
):

    if row is None:

        return f"""

        <div class="market-panel">

            <div class="market-panel-title">

                <span class="market-panel-name">
                    {icon} {symbol}
                </span>

                <span class="market-panel-sub">
                    데이터 대기
                </span>

            </div>

        </div>

        """

    ema_1 = row.get(
        "ema_1h",
        {}
    )

    ema_high = row.get(
        "ema_high",
        {}
    )

    ema_daily = row.get(
        "ema_daily",
        {}
    )

    roc_data = row.get(
        "roc",
        {}
    )

    roc_high = row.get(
        "roc_high",
        {}
    )

    roc_daily = row.get(
        "roc_daily",
        {}
    )

    return f"""

    <div class="market-panel">

        <div class="market-panel-title">

            <span class="market-panel-name">
                {icon} {symbol}
            </span>

            <span class="market-panel-sub">
                {latest_market_source}
            </span>

        </div>


        <div class="market-top">

            <span class="market-price">

                {format_market_price(
                    row.get(
                        "current_price"
                    )
                )}

            </span>

            <span>

                {market_change_html(
                    row.get(
                        "change_value"
                    )
                )}

            </span>

        </div>


        <div class="market-indicators">


            <!-- EMA -->

            <div class="market-indicator-group">

                <span class="market-label">
                    EMA
                </span>


                <div class="market-indicator-line">

                    <span class="market-timeframe">
                        {format_timeframe(
                            EMA_TIMEFRAME
                        )}
                    </span>

                    {market_direction_html(
                        ema_1.get(
                            "direction",
                            "none"
                        ),
                        ema_1.get(
                            "count",
                            0
                        )
                    )}

                </div>


                <div class="market-indicator-line">

                    <span class="market-timeframe">
                        {format_timeframe(
                            EMA_HIGH_TIMEFRAME
                        )}
                    </span>

                    {market_direction_html(
                        ema_high.get(
                            "direction",
                            "none"
                        ),
                        ema_high.get(
                            "count",
                            0
                        )
                    )}

                </div>


                <div class="market-indicator-line">

                    <span class="market-timeframe">
                        {format_timeframe(
                            EMA_DAILY_TIMEFRAME
                        )}
                    </span>

                    {market_direction_html(
                        ema_daily.get(
                            "direction",
                            "none"
                        ),
                        ema_daily.get(
                            "count",
                            0
                        )
                    )}

                </div>

            </div>


            <!-- ROC -->

            <div class="market-indicator-group">

                <span class="market-label">
                    ROC10 / MA
                </span>


                <div class="market-indicator-line">

                    <span class="market-timeframe">
                        {format_timeframe(
                            ROC_TIMEFRAME
                        )}
                    </span>

                    {market_roc_html(
                        roc_data
                    )}

                </div>


                <div class="market-indicator-line">

                    <span class="market-timeframe">
                        {format_timeframe(
                            ROC_HIGH_TIMEFRAME
                        )}
                    </span>

                    {market_roc_reference_html(
                        roc_high
                    )}

                </div>


                <div class="market-indicator-line">

                    <span class="market-timeframe">
                        {format_timeframe(
                            ROC_DAILY_TIMEFRAME
                        )}
                    </span>

                    {market_roc_reference_html(
                        roc_daily
                    )}

                </div>

            </div>


        </div>

    </div>

    """


# =========================================================
# BTC + ETH 시장 시황
# =========================================================

def market_summary_html():

    btc = latest_market_data.get(
        "BTC"
    )

    eth = latest_market_data.get(
        "ETH"
    )

    return f"""

    <div class="market-summary">


        <div class="market-title">

            <span class="market-title-main">
                📊 시장 시황
            </span>

            <span class="market-title-sub">

                {latest_market_source} ·

                EMA {format_timeframe(EMA_TIMEFRAME)}/
                {format_timeframe(EMA_HIGH_TIMEFRAME)}/
                {format_timeframe(EMA_DAILY_TIMEFRAME)}

                +

                ROC10 {format_timeframe(ROC_TIMEFRAME)}/
                {format_timeframe(ROC_HIGH_TIMEFRAME)}/
                {format_timeframe(ROC_DAILY_TIMEFRAME)}

            </span>

        </div>


        <div class="market-grid">

            {market_panel(
                btc,
                "BTC",
                "₿"
            )}

            {market_panel(
                eth,
                "ETH",
                "◆"
            )}

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


/* =========================================================
   제목
   ========================================================= */

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
    margin-bottom:4px;
    padding:3px 5px;
    border-left:3px solid #39e875;
    background:rgba(57,232,117,.08);
    border-radius:3px;
    white-space:nowrap;
    overflow:hidden;
}

.section-title{
    margin:5px 0 4px;
}

.section-title-main{
    color:#fff;
    font-size:8px;
    line-height:10px;
    font-weight:900;
    flex:none;
}

.section-title-sub{
    color:#7f8791;
    font-size:5.5px;
    line-height:8px;
    font-weight:700;
    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;
}

.roc-section-title{
    border-left-color:#39e875;
    background:rgba(57,232,117,.08);
}

.progress-section-title{
    border-left-color:#ffd166;
    background:rgba(255,209,102,.08);
}


/* =========================================================
   상태
   ========================================================= */

.status{
    display:flex;
    gap:10px;
    margin:3px 2px;
    color:#777f89;
    font-size:6px;
    font-weight:700;
    white-space:nowrap;
    overflow:hidden;
}

.status .y{
    color:#39e875;
}

.status .n{
    color:#ff5555;
}


/* =========================================================
   시장 시황
   ========================================================= */

.market-summary{
    width:100%;
    margin:2px 0 3px;
    padding:3px 4px;
    border-top:1px solid #242a31;
    border-bottom:1px solid #242a31;
    background:#101419;
    overflow:hidden;
}

.market-grid{
    display:grid;
    grid-template-columns:
        minmax(0,1fr)
        minmax(0,1fr);
    gap:8px;
    width:100%;
}

.market-panel{
    min-width:0;
    overflow:hidden;
}

.market-panel + .market-panel{
    border-left:1px solid #242a31;
    padding-left:8px;
}

.market-panel-title{
    display:flex;
    align-items:center;
    gap:4px;
    width:100%;
    margin:1px 0 2px;
    white-space:nowrap;
    overflow:hidden;
}

.market-panel-name{
    color:#fff;
    font-size:7px;
    line-height:9px;
    font-weight:900;
    flex:none;
}

.market-panel-sub{
    color:#737b85;
    font-size:4.8px;
    line-height:7px;
    font-weight:700;
    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;
}

.market-top{
    display:flex;
    align-items:center;
    gap:7px;
    margin:2px 0 3px;
    min-height:10px;
    white-space:nowrap;
    overflow:hidden;
}

.market-price{
    color:#eee;
    font-size:7px;
    line-height:9px;
    font-weight:900;
    white-space:nowrap;
}

.market-indicators{
    display:flex;
    align-items:flex-start;
    gap:10px;
    width:100%;
    color:#89919a;
    font-size:5.3px;
    line-height:9px;
}

.market-indicator-group{
    min-width:0;
    flex:none;
}

.market-label{
    display:block;
    color:#aaa;
    font-size:5px;
    line-height:7px;
    font-weight:900;
    margin-bottom:1px;
}

.market-indicator-line{
    display:flex;
    align-items:center;
    gap:3px;
    min-height:10px;
    padding-left:2px;
    white-space:nowrap;
}

.market-timeframe{
    color:#89919a;
    font-size:5px;
    line-height:8px;
    font-weight:700;
    width:18px;
    min-width:18px;
}

.market-up{
    color:#39e875!important;
    font-weight:900;
}

.market-down{
    color:#ff5555!important;
    font-weight:900;
}

.market-zero{
    color:#68717b!important;
    font-weight:800;
}


/* =========================================================
   TABLE
   ========================================================= */

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
    height:55px;
    padding:1px;
    border-bottom:1px solid #22282e;
    text-align:center;
    vertical-align:middle;
    overflow:hidden;
}

tr:last-child td{
    border-bottom:none;
}


/* =========================================================
   컬럼
   ========================================================= */

th:nth-child(1),
td:nth-child(1){
    width:5%;
}

th:nth-child(2),
td:nth-child(2){
    width:15%;
}

th:nth-child(3),
td:nth-child(3){
    width:13%;
}

th:nth-child(4),
td:nth-child(4){
    width:19%;
}

th:nth-child(5),
td:nth-child(5){
    width:34%;
}

th:nth-child(6),
td:nth-child(6){
    width:14%;
}


/* =========================================================
   코인
   ========================================================= */

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


/* =========================================================
   거래대금
   ========================================================= */

.vol{
    font-size:6px;
    line-height:8px;
    font-weight:800;
    white-space:nowrap;
}


/* =========================================================
   EMA / ROC
   ========================================================= */

.ema-cell,
.roc-column{
    text-align:left!important;
    vertical-align:middle;
}

.indicator-title{
    color:#737b85;
    font-size:5px;
    line-height:5px;
    font-weight:900;
    margin-top:0;
    margin-bottom:0;
    padding-left:4px;
}

.indicator-line{
    display:flex;
    align-items:center;
    justify-content:flex-start;
    gap:3px;
    min-height:12px;
    padding-left:4px;
    white-space:nowrap;
}

.indicator-label{
    color:#9da5ae;
    font-size:5px;
    line-height:6px;
    font-weight:700;
    width:16px;
    min-width:16px;
    text-align:left;
}


/* =========================================================
   EMA / ROC 수치
   ========================================================= */

.ema-direction,
.roc-long,
.roc-short,
.roc-neutral,
.roc-zero,
.roc-reference,
.roc-reference-long,
.roc-reference-short{
    font-size:6px;
    line-height:7px;
    font-weight:900;
    white-space:nowrap;
}

.ema-direction{
    min-width:18px;
}


/* =========================================================
   ROC
   ========================================================= */

.roc-long,
.roc-reference-long{
    color:#39e875!important;
}

.roc-short,
.roc-reference-short{
    color:#ff5555!important;
}

.roc-neutral,
.roc-zero,
.roc-reference{
    color:#68717b!important;
}


/* =========================================================
   ROC 이평선
   ========================================================= */

.roc-ma-long{
    color:#39e875!important;
    font-weight:900;
}

.roc-ma-short{
    color:#ff5555!important;
    font-weight:900;
}

.roc-ma-neutral{
    color:#68717b!important;
    font-weight:800;
}


/* =========================================================
   ROC 상승 / 하락
   ========================================================= */

.roc-up{
    color:#39e875!important;
    font-weight:900;
}

.roc-down{
    color:#ff5555!important;
    font-weight:900;
}

.roc-flat{
    color:#8b929b!important;
    font-weight:800;
}


/* =========================================================
   신호
   ========================================================= */

.signal-cell{
    text-align:center!important;
    vertical-align:middle;
    padding-left:2px;
    padding-right:2px;
}

.signal-icon{
    display:inline-flex;
    align-items:center;
    justify-content:center;
    width:100%;
    min-height:18px;
    font-size:11px;
    line-height:14px;
    font-weight:900;
    white-space:nowrap;
}

.long-breakout{
    color:#39e875;
}

.short-breakout{
    color:#ff5555;
}

.roc-warning-qualified{
    color:#ffd166!important;
    font-size:6px!important;
    line-height:8px;
    font-weight:900;
    white-space:nowrap;
}

.long-progress{
    color:#ffd166;
}

.short-progress{
    color:#91a7ff;
}


/* =========================================================
   등락
   ========================================================= */

.up{
    color:#39e875!important;
    font-weight:900;
}

.down{
    color:#ff5555!important;
    font-weight:900;
}

.zero{
    color:#68717b!important;
}

.muted{
    color:#555d67;
}


/* =========================================================
   행
   ========================================================= */

.breakout-qualified{
    background:rgba(57,232,117,.08);
}

.short-breakout-qualified{
    background:rgba(255,85,85,.05);
}

.progress-qualified{
    background:rgba(255,209,102,.07);
}

.short-progress-qualified{
    background:rgba(120,160,255,.06);
}

.roc-warning-row{
    background:rgba(255,209,102,.04);
}

.empty{
    height:30px;
    padding:8px;
    color:#555d67;
    font-size:6px;
}


/* =========================================================
   모바일
   ========================================================= */

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
        line-height:9px;
        padding:3px 4px;
        margin-bottom:3px;
    }

    .section-title{
        margin:4px 0 3px;
    }

    .section-title-main{
        font-size:7px;
        line-height:9px;
    }

    .section-title-sub{
        font-size:4.8px;
        line-height:7px;
    }

    .market-grid{
        gap:4px;
    }

    .market-panel + .market-panel{
        padding-left:4px;
    }

    .market-panel-title{
        gap:3px;
    }

    .market-panel-name{
        font-size:6px;
        line-height:8px;
    }

    .market-panel-sub{
        font-size:4px;
        line-height:6px;
    }

    .market-top{
        gap:5px;
        margin:1px 0 2px;
    }

    .market-price{
        font-size:6px;
        line-height:8px;
    }

    .market-indicators{
        gap:5px;
        font-size:4.8px;
        line-height:8px;
    }

    .market-label{
        font-size:4.5px;
        line-height:6px;
    }

    .market-indicator-line{
        gap:2px;
        min-height:9px;
    }

    .market-timeframe{
        font-size:4.5px;
        line-height:7px;
        width:15px;
        min-width:15px;
    }

    th{
        height:16px;
        font-size:4.5px;
    }

    td{
        height:55px;
    }

    th:nth-child(1),
    td:nth-child(1){
        width:5%;
    }

    th:nth-child(2),
    td:nth-child(2){
        width:15%;
    }

    th:nth-child(3),
    td:nth-child(3){
        width:13%;
    }

    th:nth-child(4),
    td:nth-child(4){
        width:19%;
    }

    th:nth-child(5),
    td:nth-child(5){
        width:34%;
    }

    th:nth-child(6),
    td:nth-child(6){
        width:14%;
    }

    .coin b{
        font-size:6px;
        line-height:7px;
    }

    .coin small{
        font-size:4px;
        line-height:5px;
    }

    .vol{
        font-size:5.5px;
    }

    .indicator-title{
        font-size:4.5px;
        line-height:5px;
        margin-top:0;
        margin-bottom:0;
    }

    .indicator-line{
        min-height:11px;
        padding-left:2px;
        gap:2px;
    }

    .indicator-label{
        font-size:4.5px;
        line-height:6px;
        width:13px;
        min-width:13px;
    }

    .ema-direction,
    .roc-long,
    .roc-short,
    .roc-neutral,
    .roc-zero,
    .roc-reference,
    .roc-reference-long,
    .roc-reference-short{
        font-size:5px;
        line-height:7px;
    }

    .ema-direction{
        min-width:16px;
    }

    .roc-warning-qualified{
        font-size:5px!important;
        line-height:7px;
    }

    .signal-icon{
        font-size:9px;
        line-height:11px;
        min-height:15px;
    }

}


/* =========================================================
   PC
   ========================================================= */

@media(min-width:601px){

    body{
        max-width:1100px;
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
        margin-bottom:5px;
    }

    .section-title{
        margin:10px 0 5px;
    }

    .section-title-main{
        font-size:9px;
    }

    .section-title-sub{
        font-size:6px;
    }

    .market-grid{
        gap:12px;
    }

    .market-panel + .market-panel{
        padding-left:12px;
    }

    .market-panel-name{
        font-size:9px;
        line-height:11px;
    }

    .market-panel-sub{
        font-size:6px;
        line-height:8px;
    }

    .market-price{
        font-size:9px;
        line-height:11px;
    }

    .market-indicators{
        gap:18px;
        font-size:7px;
        line-height:11px;
    }

    .market-label{
        font-size:7px;
        line-height:9px;
    }

    .market-indicator-line{
        min-height:13px;
        gap:4px;
    }

    .market-timeframe{
        font-size:7px;
        line-height:10px;
        width:24px;
        min-width:24px;
    }

    th{
        height:26px;
        font-size:7px;
    }

    td{
        height:70px;
        padding:3px;
    }

    th:nth-child(1),
    td:nth-child(1){
        width:5%;
    }

    th:nth-child(2),
    td:nth-child(2){
        width:15%;
    }

    th:nth-child(3),
    td:nth-child(3){
        width:13%;
    }

    th:nth-child(4),
    td:nth-child(4){
        width:19%;
    }

    th:nth-child(5),
    td:nth-child(5){
        width:34%;
    }

    th:nth-child(6),
    td:nth-child(6){
        width:14%;
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

    .indicator-title{
        font-size:7px;
        line-height:7px;
        margin-top:0;
        margin-bottom:0;
    }

    .indicator-line{
        min-height:16px;
        padding-left:5px;
    }

    .indicator-label{
        font-size:7px;
        line-height:8px;
        width:16px;
        min-width:16px;
    }

    .ema-direction,
    .roc-long,
    .roc-short,
    .roc-neutral,
    .roc-zero,
    .roc-reference,
    .roc-reference-long,
    .roc-reference-short{
        font-size:8px;
        line-height:10px;
    }

    .ema-direction{
        min-width:25px;
    }

    .roc-warning-qualified{
        font-size:8px!important;
        line-height:10px;
    }

    .signal-icon{
        font-size:17px;
        line-height:19px;
        min-height:25px;
    }

}


/* =========================================================
   ROC 상승 / 하락 색상
   ========================================================= */

.roc-up{
    color:#39e875!important;
    font-weight:900;
}

.roc-down{
    color:#ff5555!important;
    font-weight:900;
}

.roc-flat{
    color:#8b929b!important;
    font-weight:800;
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


    # =====================================================
    # ① ROC 추세 신호
    # =====================================================

    if USE_UPBIT == "Y":

        sections += roc_signal_section(
            "🔥 ROC 추세 신호",
            latest_upbit_data,
            latest_upbit_update_time,
            upbit=True
        )


    if USE_OKX == "Y":

        sections += roc_signal_section(
            "🔥 ROC 추세 신호",
            latest_okx_data,
            latest_okx_update_time,
            upbit=False
        )


    # =====================================================
    # ② 추세 진행
    # =====================================================

    if USE_UPBIT == "Y":

        sections += progress_section(
            latest_upbit_data,
            latest_upbit_update_time,
            upbit=True
        )


    if USE_OKX == "Y":

        sections += progress_section(
            latest_okx_data,
            latest_okx_update_time,
            upbit=False
        )


    # =====================================================
    # ③ UPBIT TOP
    # =====================================================

    if USE_UPBIT == "Y":

        sections += f"""

        <div class="section-title">

            <span class="section-title-main">
                🏆 업비트 TOP{TOP_N}
            </span>

            <span class="section-title-sub">

                거래대금 순위 ·
                EMA {get_ema_filter_labels()} ·
                ROC 이평 {get_roc_filter_labels()} ·
                ROC10 참고 ·
                {latest_upbit_update_time} KST

            </span>

        </div>


        {table_html(
            latest_upbit_data,
            "normal"
        )}

        """


    # =====================================================
    # ④ OKX TOP
    # =====================================================

    if USE_OKX == "Y":

        sections += f"""

        <div class="section-title">

            <span class="section-title-main">
                🏆 OKX TOP{TOP_N}
            </span>

            <span class="section-title-sub">

                거래대금 순위 ·
                EMA {get_ema_filter_labels()} ·
                ROC 이평 {get_roc_filter_labels()} ·
                ROC10 참고 ·
                {latest_okx_update_time} KST

            </span>

        </div>


        {table_html(
            latest_okx_data,
            "normal"
        )}

        """


    # =====================================================
    # 상태
    # =====================================================

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
            <b class="{'y' if USE_OKX == 'Y' else 'n'}">
                {USE_OKX}
            </b>
        </span>

        <span>
            EMA :
            <b class="y">
                {get_ema_filter_labels()}
            </b>
        </span>

        <span>
            ROC MA :
            <b class="y">
                {get_roc_filter_labels()}
            </b>
        </span>

        <span>
            ROC :
            <b class="y">
                {format_timeframe(ROC_TIMEFRAME)} /
                {format_timeframe(ROC_HIGH_TIMEFRAME)} /
                {format_timeframe(ROC_DAILY_TIMEFRAME)}
            </b>
        </span>

        <span>
            기준 :
            <b class="y">
                0선
            </b>
        </span>

        <span>
            진행 :
            <b class="y">
                2+
            </b>
        </span>

        <span>
            시장 :
            <b class="y">
                {latest_market_source}
            </b>
        </span>

        <span>
            TOP :
            <b class="y">
                {TOP_N}
            </b>
        </span>

    </div>

    """


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
            ROC 추세 신호
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
        "TRADING SIGNAL CENTER 시작"
    )

    log.info(
        f"EMA TIMEFRAME = "
        f"{format_timeframe(EMA_TIMEFRAME)} "
        f"[{USE_EMA_TIMEFRAME}]"
    )

    log.info(
        f"EMA HIGH TIMEFRAME = "
        f"{format_timeframe(EMA_HIGH_TIMEFRAME)} "
        f"[{USE_EMA_HIGH_TIMEFRAME}]"
    )

    log.info(
        f"EMA DAILY TIMEFRAME = "
        f"{format_timeframe(EMA_DAILY_TIMEFRAME)} "
        f"[{USE_EMA_DAILY_TIMEFRAME}]"
    )

    log.info(
        f"EMA 실제 필터 = "
        f"{get_ema_filter_timeframes()}"
    )

    log.info(
        f"ROC TIMEFRAME = "
        f"{format_timeframe(ROC_TIMEFRAME)} "
        f"[{USE_ROC_TIMEFRAME}]"
    )

    log.info(
        f"ROC HIGH TIMEFRAME = "
        f"{format_timeframe(ROC_HIGH_TIMEFRAME)} "
        f"[{USE_ROC_HIGH_TIMEFRAME}]"
    )

    log.info(
        f"ROC DAILY TIMEFRAME = "
        f"{format_timeframe(ROC_DAILY_TIMEFRAME)} "
        f"[{USE_ROC_DAILY_TIMEFRAME}]"
    )

    log.info(
        f"ROC 실제 필터 = "
        f"{get_roc_filter_timeframes()}"
    )

    log.info(
        f"EMA = "
        f"{EMA1_FASTEST}/"
        f"{EMA1_FAST}/"
        f"{EMA1_MID}/"
        f"{EMA1_SLOW}"
    )

    log.info(
        f"ROC = {ROC_PERIOD}"
    )

    log.info(
        "ROC 이평 = "
        f"{ROC_MA_FASTEST}/"
        f"{ROC_MA_FAST}/"
        f"{ROC_MA_MID}/"
        f"{ROC_MA_SLOW}"
    )

    log.info(
        "ROC LONG = 0선 상방"
    )

    log.info(
        "ROC SHORT = 0선 하방"
    )

    log.info(
        "========================================"
    )

    log.info(
        "ROC 카운팅 구조:"
    )

    log.info(
        "ROC10 > 0 → 🟢 상승"
    )

    log.info(
        "ROC10 < 0 → 🔴 하락"
    )

    log.info(
        "ROC10 = 0 → ⚪ 중립"
    )

    log.info(
        f"{format_timeframe(ROC_TIMEFRAME)} "
        f"ROC10 0선 진입 카운트 1 → 🔥 ROC 추세 신호"
    )

    log.info(
        f"{format_timeframe(ROC_TIMEFRAME)} "
        f"ROC10 0선 상방/하방 카운트 2 이상 → ☀️ / 🌧️ 추세 진행"
    )

    log.info(
        "ROC 이평 LONG:"
    )

    log.info(
        "ROC EMA10 > ROC EMA30 > ROC EMA60 > ROC EMA120"
    )

    log.info(
        "ROC 이평 SHORT:"
    )

    log.info(
        "ROC EMA10 < ROC EMA30 < ROC EMA60 < ROC EMA120"
    )

    log.info(
        f"{format_timeframe(ROC_HIGH_TIMEFRAME)} "
        f"ROC 이평 → 필터"
    )

    log.info(
        f"{format_timeframe(ROC_DAILY_TIMEFRAME)} "
        f"ROC 이평 → 참고용"
    )

    log.info(
        f"EMA 실제 필터 → "
        f"{get_ema_filter_timeframes()}"
    )

    log.info(
        f"ROC 실제 필터 → "
        f"{get_roc_filter_timeframes()}"
    )

    log.info(
        "========================================"
    )

    log.info(
        "최종 LONG 조건:"
    )

    log.info(
        "EMA 1H 정배열"
    )

    log.info(
        "EMA 4H 정배열"
    )

    log.info(
        "ROC 1H 이평 정배열"
    )

    log.info(
        "ROC 4H 이평 정배열"
    )

    log.info(
        "ROC10 0선 상방 진입"
    )

    log.info(
        "========================================"
    )

    log.info(
        "최종 SHORT 조건:"
    )

    log.info(
        "EMA 1H 역배열"
    )

    log.info(
        "EMA 4H 역배열"
    )

    log.info(
        "ROC 1H 이평 역배열"
    )

    log.info(
        "ROC 4H 이평 역배열"
    )

    log.info(
        "ROC10 0선 하방 진입"
    )

    log.info(
        "========================================"
    )

    log.info(
        "시장 시황 거래소 우선순위:"
    )

    if USE_OKX == "Y":

        log.info(
            "OKX ON → OKX BTC / ETH 우선"
        )

    else:

        log.info(
            "OKX OFF → UPBIT BTC / ETH 사용"
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
