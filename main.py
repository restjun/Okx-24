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

TOP_N = 50

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

EMA_TIMEFRAME = 240

EMA_HIGH_TIMEFRAME = 60

USE_EMA_TIMEFRAME = "Y"

USE_EMA_HIGH_TIMEFRAME = "N"


# =========================================================
# EMA 기간
# =========================================================

EMA1_FASTEST = 10

EMA1_FAST = 30

EMA1_MID = 60

EMA1_SLOW = 120

EMA1_MAX_COUNT = 200


# =========================================================
# RSI
# =========================================================

RSI_PERIOD = 14

RSI_LONG_LEVEL = 70

RSI_SHORT_LEVEL = 30


# =========================================================
# RSI 진행 시작
#
# 1 = 최초 진입
# 2 이상 = 추세 진행
# =========================================================

RSI_PROGRESS_START_COUNT = 2


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
# 캐시
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
# OKX 시간봉
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
#
# 4H KST 기준
#
# 01:00
# 05:00
# 09:00
# 13:00
# 17:00
# 21:00
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

            anchor -= pd.Timedelta(days=1)

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


def validate_timeframe():

    if (
        EMA_TIMEFRAME
        not in SUPPORTED_UPBIT_TIMEFRAMES
    ):

        raise ValueError(
            f"EMA_TIMEFRAME 오류: {EMA_TIMEFRAME}"
        )

    if (
        EMA_HIGH_TIMEFRAME
        not in SUPPORTED_UPBIT_TIMEFRAMES
    ):

        raise ValueError(
            f"EMA_HIGH_TIMEFRAME 오류: "
            f"{EMA_HIGH_TIMEFRAME}"
        )

    if (
        get_okx_bar(
            EMA_TIMEFRAME
        )
        is None
    ):

        raise ValueError(
            f"OKX 시간봉 오류: "
            f"{EMA_TIMEFRAME}"
        )

    if (
        get_okx_bar(
            EMA_HIGH_TIMEFRAME
        )
        is None
    ):

        raise ValueError(
            f"OKX HIGH 시간봉 오류: "
            f"{EMA_HIGH_TIMEFRAME}"
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

            if not market.startswith(
                "KRW-"
            ):

                continue

            try:

                volume = float(
                    item[
                        "acc_trade_price_24h"
                    ]
                )

                price = float(
                    item[
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
            response.json()[0][
                "trade_price"
            ]
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

    response = retry(
        requests.get,
        f"https://api.upbit.com/v1/candles/minutes/{int(unit)}",
        params={

            "market": market,

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

    if response is None:

        return None

    try:

        df = pd.DataFrame(
            response.json()
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
            .drop_duplicates("datetime")
            .reset_index(drop=True)
        )

    except Exception as e:

        log.error(
            f"UPBIT candle 오류 "
            f"{market}: {e}"
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

def get_upbit_current_rsi_data(
    market,
    current_price
):

    df = get_upbit_candle(
        market,
        EMA_TIMEFRAME,
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
                EMA_TIMEFRAME
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

            and x.get("state") == "live"

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

    if (
        df is None
        or df.empty
    ):

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


def get_okx_cached_price(inst):

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
# OKX 현재 1H
# =========================================================

def get_okx_current_1h(
    inst,
    current_price
):

    df = get_okx_ohlcv(
        inst,
        "1H",
        200,
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
                60
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
# EMA
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
# EMA 배열
# =========================================================

def ema_alignment_count(df):

    if (
        df is None
        or df.empty
    ):

        return {
            "direction": "none",
            "count": 0
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
                "count": 0
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
            "count": count
        }

    except Exception:

        return {
            "direction": "none",
            "count": 0
        }


# =========================================================
# EMA 표시
# =========================================================

def ema_display(
    df,
    current_price=None
):

    x = ema_alignment_count(
        df
    )

    direction = x[
        "direction"
    ]

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

        "current_price":
            current_price

    }


# =========================================================
# EMA 필터 방향
# =========================================================

def ema_filter_direction(
    e1,
    e_high
):

    selected = []

    if USE_EMA_TIMEFRAME == "Y":

        selected.append(e1)

    if USE_EMA_HIGH_TIMEFRAME == "Y":

        selected.append(e_high)

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
# EMA 필터
# =========================================================

def ema_filter_pass(
    e1,
    e_high
):

    selected = []

    if USE_EMA_TIMEFRAME == "Y":

        selected.append(e1)

    if USE_EMA_HIGH_TIMEFRAME == "Y":

        selected.append(e_high)

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
# RSI 계산
# =========================================================

def rsi(
    df,
    period=RSI_PERIOD
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

        delta = close.diff()

        gain = delta.clip(
            lower=0
        )

        loss = -delta.clip(
            upper=0
        )

        avg_gain = (
            gain
            .ewm(
                alpha=1 / int(period),
                adjust=False,
                min_periods=int(period)
            )
            .mean()
        )

        avg_loss = (
            loss
            .ewm(
                alpha=1 / int(period),
                adjust=False,
                min_periods=int(period)
            )
            .mean()
        )

        rs = (
            avg_gain
            / avg_loss
        )

        result = (
            100
            -
            (
                100
                / (1 + rs)
            )
        )

        result = result.clip(
            0,
            100
        )

        result = result.where(
            avg_loss != 0,
            100
        )

        result = result.where(
            avg_gain != 0,
            0
        )

        return result

    except Exception:

        return None


# =========================================================
# RSI 연속 카운트
#
# 위에서부터 현재까지가 아니라
# 현재 캔들부터 과거로 확인
#
# 70 이상:
# 1, 2, 3...
#
# 30 이하:
# 1, 2, 3...
# =========================================================

def rsi_count(
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

                if value >= level:

                    count += 1

                else:

                    break

            else:

                if value <= level:

                    count += 1

                else:

                    break

        return count

    except Exception:

        return 0


# =========================================================
# RSI 분석
#
# 새로운 방식
#
# 70 이상:
#   1 = RSI 70 진입
#   2+ = RSI 과매수 진행
#
# 30 이하:
#   1 = RSI 30 진입
#   2+ = RSI 과매도 진행
# =========================================================

def rsi_analysis(
    df_confirmed,
    df_current
):

    result = {

        "rsi14":
            None,

        "rsi14_previous":
            None,

        "rsi14_count":
            0,

        "rsi14_short_count":
            0,

        "long_count":
            0,

        "short_count":
            0,

        "state":
            "none",

        "display":
            "-"

    }

    if (
        df_confirmed is None
        or df_confirmed.empty
        or df_current is None
        or df_current.empty
    ):

        return result

    try:

        confirmed_rsi = rsi(
            df_confirmed
        )

        current_rsi = rsi(
            df_current
        )

        if (
            confirmed_rsi is None
            or current_rsi is None
        ):

            return result

        previous = float(
            confirmed_rsi.iloc[-1]
        )

        current_value = float(
            current_rsi.iloc[-1]
        )

        if (
            pd.isna(previous)
            or pd.isna(current_value)
        ):

            return result

        long_count = rsi_count(
            current_rsi,
            RSI_LONG_LEVEL,
            True
        )

        short_count = rsi_count(
            current_rsi,
            RSI_SHORT_LEVEL,
            False
        )

        # -------------------------------------------------
        # 롱
        # -------------------------------------------------

        if current_value >= RSI_LONG_LEVEL:

            state = "long"

        # -------------------------------------------------
        # 숏
        # -------------------------------------------------

        elif current_value <= RSI_SHORT_LEVEL:

            state = "short"

        # -------------------------------------------------
        # 중립
        # -------------------------------------------------

        else:

            state = "neutral"

        result.update({

            "rsi14":
                current_value,

            "rsi14_previous":
                previous,

            "rsi14_count":
                long_count,

            "rsi14_short_count":
                short_count,

            "long_count":
                long_count,

            "short_count":
                short_count,

            "state":
                state

        })

        if state == "long":

            result["display"] = (
                f"🟢{current_value:.1f}"
                f"({long_count})"
            )

        elif state == "short":

            result["display"] = (
                f"🔴{current_value:.1f}"
                f"({short_count})"
            )

        else:

            result["display"] = (
                f"{current_value:.1f}"
                f"(0)"
            )

        return result

    except Exception as e:

        log.error(
            f"RSI 분석 오류: {e}"
        )

        return result


# =========================================================
# 일간 등락률
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
# 등락률
# =========================================================

def daily_changes(df):

    if (
        df is None
        or df.empty
    ):

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
# 등락률 보조
# =========================================================

def get_change_value(x):

    if x is None:

        return None

    try:

        if isinstance(
            x,
            (list, tuple)
        ):

            return float(x[0])

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
# EMA + RSI 자격
# =========================================================

def get_signal_qualified(
    e1,
    e_high,
    r
):

    filter_info = (
        ema_filter_direction(
            e1,
            e_high
        )
    )

    direction = filter_info[
        "direction"
    ]

    filter_pass = (
        ema_filter_pass(
            e1,
            e_high
        )
    )

    if (
        USE_EMA_TIMEFRAME == "N"
        and USE_EMA_HIGH_TIMEFRAME == "N"
    ):

        long_base = True

        short_base = True

    else:

        long_base = (
            filter_pass
            and direction == "long"
        )

        short_base = (
            filter_pass
            and direction == "short"
        )

    return {

        "breakout_qualified":
            (
                long_base
                and r.get(
                    "long_count",
                    0
                ) >= 1
            ),

        "short_breakout_qualified":
            (
                short_base
                and r.get(
                    "short_count",
                    0
                ) >= 1
            ),

        "filter_direction":
            direction

    }


# =========================================================
# 분석 - OKX
# =========================================================

def analyze_okx(
    market,
    current_price=None
):

    bar = get_okx_bar(
        EMA_TIMEFRAME
    )

    high_bar = get_okx_bar(
        EMA_HIGH_TIMEFRAME
    )

    if (
        not bar
        or not high_bar
    ):

        return None

    df_confirmed = (
        okx_1h_cache.get(
            market
        )
    )

    if (
        df_confirmed is None
        or df_confirmed.empty
    ):

        df_confirmed = history_okx(
            market,
            bar
        )

    df_high = history_okx(
        market,
        high_bar
    )

    df_current = (
        get_okx_current_1h(
            market,
            current_price
        )
    )

    if (
        df_confirmed is None
        or df_confirmed.empty
    ):

        return None

    e1 = ema_display(
        df_confirmed,
        current_price
    )

    e_high = ema_display(
        df_high,
        current_price
    )

    r = rsi_analysis(
        df_confirmed,
        df_current
    )

    changes = daily_changes(
        df_confirmed
    )

    q = get_signal_qualified(
        e1,
        e_high,
        r
    )

    return {

        "ema_1h": e1,

        "ema_high": e_high,

        "rsi": r,

        "changes": changes,

        **q,

        "direction_1h":
            q["filter_direction"],

        "df1h":
            df_confirmed

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

    df_confirmed = history_upbit(
        market,
        EMA_TIMEFRAME
    )

    df_high = history_upbit(
        market,
        EMA_HIGH_TIMEFRAME
    )

    df_current = (
        get_upbit_current_rsi_data(
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
        df_confirmed is None
        or df_confirmed.empty
    ):

        return None

    e1 = ema_display(
        df_confirmed,
        current_price
    )

    e_high = ema_display(
        df_high,
        current_price
    )

    r = rsi_analysis(
        df_confirmed,
        df_current
    )

    q = get_signal_qualified(
        e1,
        e_high,
        r
    )

    return {

        "ema_1h": e1,

        "ema_high": e_high,

        "rsi": r,

        "changes": changes,

        **q,

        "direction_1h":
            q["filter_direction"],

        "df1h":
            df_confirmed

    }


# =========================================================
# ROW 생성
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

            "ema_1h": {
                "direction": "none",
                "count": 0
            },

            "ema_high": {
                "direction": "none",
                "count": 0
            },

            "rsi": {
                "rsi14": None,
                "long_count": 0,
                "short_count": 0
            },

            "changes": None,

            "breakout_qualified":
                False,

            "short_breakout_qualified":
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

        "rsi":
            analysis.get(
                "rsi",
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
            )

    }


# =========================================================
# RSI 추세 신호
#
# 카운트 1만
# =========================================================

def is_rsi_signal(row):

    if not row:

        return False

    r = row.get(
        "rsi",
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
# UPBIT 롱 신호
#
# 기존 양수 조건 유지
# RSI 70 최초 진입
# =========================================================

def is_upbit_long_signal(row):

    if not row:

        return False

    r = row.get(
        "rsi",
        {}
    )

    return (
        is_positive_day(row)
        and
        int(
            r.get(
                "long_count",
                0
            )
            or 0
        ) == 1
    )


# =========================================================
# 추세 진행
#
# 2 이상
# =========================================================

def is_sun_cloud_signal(row):

    if not row:

        return False

    r = row.get(
        "rsi",
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
        long_count >= RSI_PROGRESS_START_COUNT
        or
        short_count >= RSI_PROGRESS_START_COUNT
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
        f"RSI신호="
        f"{sum(is_rsi_signal(x) for x in rows)} / "
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

                    update_okx(usdt)

            except Exception as e:

                log.exception(
                    f"OKX 업데이트 오류: {e}"
                )

        else:

            latest_okx_data = []

    finally:

        update_lock.release()


# =========================================================
# RSI HTML
#
# 숫자 + 괄호 카운트
# =========================================================

def rsi_html(r):

    if not r:

        return (
            '<div class="rsi-cell">'
            '<span class="rsi-zero">-</span>'
            '</div>'
        )

    value = r.get(
        "rsi14"
    )

    if value is None:

        return (
            '<div class="rsi-cell">'
            '<span class="rsi-zero">-</span>'
            '</div>'
        )

    try:

        value = float(value)

    except Exception:

        return (
            '<div class="rsi-cell">'
            '<span class="rsi-zero">-</span>'
            '</div>'
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

    if value >= RSI_LONG_LEVEL:

        return (
            '<div class="rsi-cell">'
            '<span class="rsi-long">'
            f'{value:.1f}({long_count})'
            '</span>'
            '</div>'
        )

    if value <= RSI_SHORT_LEVEL:

        return (
            '<div class="rsi-cell">'
            '<span class="rsi-short">'
            f'{value:.1f}({short_count})'
            '</span>'
            '</div>'
        )

    return (
        '<div class="rsi-cell">'
        '<span class="rsi-neutral">'
        f'{value:.1f}(0)'
        '</span>'
        '</div>'
    )


# =========================================================
# RSI 추세 신호 HTML
#
# 카운트 1
# =========================================================

def signal_html(row):

    r = row.get(
        "rsi",
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

    # -----------------------------------------------------
    # RSI 70 최초 진입
    # -----------------------------------------------------

    if long_count == 1:

        if row.get(
            "breakout_qualified",
            False
        ):

            return (
                '<span '
                'class="signal-icon long-breakout" '
                'title="RSI70 진입 · 카운트 1">'
                '🚀①'
                '</span>'
            )

        return (
            '<span '
            'class="signal-icon rsi-warning-qualified" '
            'title="RSI70 진입 · EMA 미충족">'
            '⚠️①'
            '</span>'
        )

    # -----------------------------------------------------
    # RSI 30 최초 진입
    # -----------------------------------------------------

    if short_count == 1:

        if row.get(
            "short_breakout_qualified",
            False
        ):

            return (
                '<span '
                'class="signal-icon short-breakout" '
                'title="RSI30 진입 · 카운트 1">'
                '🔻①'
                '</span>'
            )

        return (
            '<span '
            'class="signal-icon rsi-warning-qualified" '
            'title="RSI30 진입 · EMA 미충족">'
            '⚠️①'
            '</span>'
        )

    # -----------------------------------------------------
    # 2 이상은 추세 진행으로 이동
    # -----------------------------------------------------

    return (
        '<span class="muted">-</span>'
    )


# =========================================================
# 추세 진행 HTML
#
# 2 이상
# =========================================================

def progress_signal_html(row):

    r = row.get(
        "rsi",
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
            f'title="RSI70 추세 진행 · {long_count}">'
            f'☀️({long_count})'
            '</span>'
        )

    if short_count >= 2:

        return (
            '<span '
            'class="signal-icon short-progress" '
            f'title="RSI30 추세 진행 · {short_count}">'
            f'🌧️({short_count})'
            '</span>'
        )

    return (
        '<span class="muted">-</span>'
    )


# =========================================================
# EMA HTML
# =========================================================

def ema_html(e):

    if not e:

        return "⚪0"

    direction = e.get(
        "direction",
        "none"
    )

    count = e.get(
        "count",
        0
    )

    icon = {

        "long": "🟢",

        "short": "🔴"

    }.get(
        direction,
        "⚪"
    )

    return (
        f"{icon}{count}"
    )


# =========================================================
# ROW CLASS
# =========================================================

def row_class(x):

    r = x.get(
        "rsi",
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

        return "rsi-warning-row"

    if short_count == 1:

        return "rsi-warning-row"

    return ""


# =========================================================
# 공통 ROW
# =========================================================

def rows_html(
    data,
    signal_mode="normal"
):

    output = []

    for x in data:

        if signal_mode == "progress":

            signal = (
                progress_signal_html(
                    x
                )
            )

        else:

            signal = (
                signal_html(
                    x
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

                <td class="ema">

                    <span>
                        {format_timeframe(
                            EMA_TIMEFRAME
                        )}

                        {ema_html(
                            x.get(
                                "ema_1h"
                            )
                        )}

                    </span>

                    <span class="ema-sep">
                        /
                    </span>

                    <span>
                        {format_timeframe(
                            EMA_HIGH_TIMEFRAME
                        )}

                        {ema_html(
                            x.get(
                                "ema_high"
                            )
                        )}

                    </span>

                </td>

                <td>

                    {rsi_html(
                        x.get(
                            "rsi",
                            {}
                        )
                    )}

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
# 공통 TABLE
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

                    <th>EMA</th>

                    <th>RSI14</th>

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
# RSI 추세 신호 섹션
#
# 카운트 1만
# =========================================================

def rsi_signal_section(
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

        if is_rsi_signal(x):

            rows.append(x)

    return f"""

    <div class="section-title rsi-section-title">

        <span class="section-title-main">
            🔥 RSI 추세 신호
        </span>

        <span class="section-title-sub">

            TOP{TOP_N} ·
            {format_timeframe(EMA_TIMEFRAME)}
            RSI14 ·
            카운트 1 ·
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
#
# 카운트 2 이상
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

            RSI 카운트 2 이상 ·
            동일 순서 · 동일 표 ·
            {update_time} KST

        </span>

    </div>

    {table_html(
        rows,
        "progress"
    )}

    """


# =========================================================
# BTC
# =========================================================

def format_market_price(price):

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


def market_rsi_html(r):

    if not r:

        return "-"

    value = r.get(
        "rsi14"
    )

    if value is None:

        return "-"

    try:

        value = float(value)

    except Exception:

        return "-"

    if value >= RSI_LONG_LEVEL:

        count = int(
            r.get(
                "long_count",
                0
            )
            or 0
        )

        return (
            '<span class="market-up">'
            f'🟢 {value:.1f}({count})'
            '</span>'
        )

    if value <= RSI_SHORT_LEVEL:

        count = int(
            r.get(
                "short_count",
                0
            )
            or 0
        )

        return (
            '<span class="market-down">'
            f'🔴 {value:.1f}({count})'
            '</span>'
        )

    return (
        '<span class="market-zero">'
        f'{value:.1f}(0)'
        '</span>'
    )


def get_market_row(coin):

    for row in latest_upbit_data:

        if row.get(
            "name"
        ) == coin:

            return row

    return None


def market_summary_html():

    btc = get_market_row("BTC")

    if btc is None:

        return """

        <div class="market-summary">

            <div class="market-title">

                <span class="market-title-main">
                    ₿ BTC 시장 시황
                </span>

                <span class="market-title-sub">
                    데이터 대기
                </span>

            </div>

        </div>

        """

    ema_1 = btc.get(
        "ema_1h",
        {}
    )

    ema_high = btc.get(
        "ema_high",
        {}
    )

    rsi_data = btc.get(
        "rsi",
        {}
    )

    return f"""

    <div class="market-summary">

        <div class="market-title">

            <span class="market-title-main">
                ₿ BTC 시장 시황
            </span>

            <span class="market-title-sub">
                EMA 배열 + RSI14
            </span>

        </div>

        <div class="btc-top">

            <span class="btc-name">
                ₿ BTC
            </span>

            <span class="btc-price">
                {format_market_price(
                    btc.get(
                        "current_price"
                    )
                )}
            </span>

            <span>
                {market_change_html(
                    btc.get(
                        "change_value"
                    )
                )}
            </span>

        </div>

        <div class="btc-bottom">

            <span>

                {format_timeframe(
                    EMA_TIMEFRAME
                )}

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

            </span>

            <span>

                {format_timeframe(
                    EMA_HIGH_TIMEFRAME
                )}

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

            </span>

            <span>

                RSI

                {market_rsi_html(
                    rsi_data
                )}

            </span>

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

    margin-bottom:4px;

    padding:3px 5px;

    border-left:3px solid #39e875;

    background:
        rgba(57,232,117,.08);

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

.rsi-section-title{

    border-left-color:#39e875;

    background:
        rgba(57,232,117,.08);
}

.progress-section-title{

    border-left-color:#ffd166;

    background:
        rgba(255,209,102,.08);
}

.market-summary{

    width:100%;

    margin:2px 0 3px;

    padding:3px 4px;

    border-top:
        1px solid #242a31;

    border-bottom:
        1px solid #242a31;

    background:#101419;

    overflow:hidden;
}

.status{

    display:flex;

    gap:10px;

    margin:3px 2px;

    color:#777f89;

    font-size:6px;

    font-weight:700;
}

.status .y{

    color:#39e875;
}

.status .n{

    color:#ff5555;
}

.btc-top{

    display:flex;

    align-items:center;

    gap:8px;

    margin:2px 0;
}

.btc-name{

    font-weight:900;
}

.btc-price{

    font-weight:800;
}

.btc-bottom{

    display:flex;

    gap:12px;

    color:#89919a;

    font-size:5.5px;

    line-height:9px;
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

.table-wrap{

    width:100%;

    overflow:hidden;

    border-radius:5px;

    border:
        1px solid #272d34;

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

    border-bottom:
        1px solid #292f36;

    color:#7f8791;

    font-size:5px;

    line-height:6px;

    font-weight:700;

    text-align:center;
}

td{

    height:25px;

    padding:1px;

    border-bottom:
        1px solid #22282e;

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

    width:25%;
}

th:nth-child(5),
td:nth-child(5){

    width:22%;
}

th:nth-child(6),
td:nth-child(6){

    width:17%;
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

    white-space:nowrap;

    overflow:visible;
}

.ema span{

    font-size:5.8px;

    line-height:8px;

    white-space:nowrap;
}

.ema-sep{

    color:#555c65;

    margin:0 1px;
}

.rsi-cell{

    display:flex;

    flex-direction:row;

    align-items:center;

    justify-content:center;

    min-height:21px;

    line-height:8px;

    white-space:nowrap;
}

.rsi-cell span{

    font-size:6px;

    line-height:8px;

    font-weight:900;

    white-space:nowrap;
}

.rsi-long{

    color:#39e875!important;

}

.rsi-short{

    color:#ff5555!important;

}

.rsi-neutral{

    color:#68717b!important;

}

.rsi-zero{

    color:#68717b!important;

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

    min-height:18px;

    font-size:12px;

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

.long-progress{

    color:#ffd166;
}

.short-progress{

    color:#91a7ff;
}

.rsi-warning-qualified{

    color:#ffd166!important;
}

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

.breakout-qualified{

    background:
        rgba(57,232,117,.08);
}

.short-breakout-qualified{

    background:
        rgba(255,85,85,.05);
}

.progress-qualified{

    background:
        rgba(255,209,102,.07);
}

.short-progress-qualified{

    background:
        rgba(120,160,255,.06);
}

.rsi-warning-row{

    background:
        rgba(255,209,102,.04);
}

.empty{

    height:30px;

    padding:8px;

    color:#555d67;

    font-size:6px;
}

@media(max-width:380px){

    body{

        padding:
            1px 1px 6px;
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

    th{

        height:16px;

        font-size:4.5px;
    }

    td{

        height:23px;
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

    .ema span{

        font-size:5.3px;
    }

    .rsi-cell span{

        font-size:5.2px;
    }

    .signal-icon{

        font-size:10px;

        line-height:12px;

        min-height:16px;
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

    .ema span{

        font-size:8px;
    }

    .rsi-cell span{

        font-size:7px;
    }

    .signal-icon{

        font-size:17px;

        line-height:19px;

        min-height:25px;
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

    # =====================================================
    # ① RSI 추세 신호
    #
    # 카운트 1
    # =====================================================

    if USE_UPBIT == "Y":

        sections += rsi_signal_section(
            "🔥 RSI 추세 신호",
            latest_upbit_data,
            latest_upbit_update_time,
            upbit=True
        )

    if USE_OKX == "Y":

        sections += rsi_signal_section(
            "🔥 RSI 추세 신호",
            latest_okx_data,
            latest_okx_update_time,
            upbit=False
        )

    # =====================================================
    # ② 추세 진행
    #
    # 카운트 2+
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
    # ③ TOP50
    # =====================================================

    if USE_UPBIT == "Y":

        sections += f"""

        <div class="section-title">

            <span class="section-title-main">
                🏆 업비트 TOP{TOP_N}
            </span>

            <span class="section-title-sub">

                거래대금 순위 ·
                RSI 돌파 + 추세 진행 포함 ·
                {latest_upbit_update_time} KST

            </span>

        </div>

        {table_html(
            latest_upbit_data,
            "normal"
        )}

        """

    if USE_OKX == "Y":

        sections += f"""

        <div class="section-title">

            <span class="section-title-main">
                🏆 OKX TOP{TOP_N}
            </span>

            <span class="section-title-sub">

                거래대금 순위 ·
                RSI 돌파 + 추세 진행 포함 ·
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
            <b class="n">
                {USE_OKX}
            </b>
        </span>

        <span>
            EMA :
            <b class="y">
                {format_timeframe(
                    EMA_TIMEFRAME
                )}
            </b>
        </span>

        <span>
            RSI :
            <b class="y">
                {RSI_LONG_LEVEL}/
                {RSI_SHORT_LEVEL}
            </b>
        </span>

        <span>
            진행 :
            <b class="y">
                2+
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
            RSI 추세 신호
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
        f"{format_timeframe(EMA_TIMEFRAME)}"
    )

    log.info(
        f"EMA HIGH TIMEFRAME = "
        f"{format_timeframe(EMA_HIGH_TIMEFRAME)}"
    )

    log.info(
        f"EMA = "
        f"{EMA1_FASTEST}/"
        f"{EMA1_FAST}/"
        f"{EMA1_MID}/"
        f"{EMA1_SLOW}"
    )

    log.info(
        f"RSI = {RSI_PERIOD}"
    )

    log.info(
        f"RSI LONG = {RSI_LONG_LEVEL}"
    )

    log.info(
        f"RSI SHORT = {RSI_SHORT_LEVEL}"
    )

    log.info(
        "========================================"
    )

    log.info(
        "RSI 카운팅 구조:"
    )

    log.info(
        "RSI >= 70 → 🟢 숫자(연속개수)"
    )

    log.info(
        "RSI <= 30 → 🔴 숫자(연속개수)"
    )

    log.info(
        "30 < RSI < 70 → 회색 숫자(0)"
    )

    log.info(
        "카운트 1 → 🔥 RSI 추세 신호"
    )

    log.info(
        "카운트 2 이상 → ☀️ / 🌧️ 추세 진행"
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
