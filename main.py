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
import copy

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
# 설정
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

KST = ZoneInfo("Asia/Seoul")


# =========================================================
# 시간봉 설정
# =========================================================

EMA_TIMEFRAME = 60
EMA_HIGH_TIMEFRAME = 240


# =========================================================
# EMA 시간봉 필터
#
# Y / Y → 두 시간봉 모두 필터
# Y / N → 첫 번째 시간봉만 필터
# N / Y → 두 번째 시간봉만 필터
# N / N → 이평 배열 필터 사용 안 함
# =========================================================

USE_EMA_TIMEFRAME = "Y"
USE_EMA_HIGH_TIMEFRAME = "Y"


# =========================================================
# EMA 설정
#
# 정배열
# EMA10 > EMA30 > EMA60 > EMA120
#
# 역배열
# EMA10 < EMA30 < EMA60 < EMA120
# =========================================================

EMA1_FASTEST = 10
EMA1_FAST = 30
EMA1_MID = 60
EMA1_SLOW = 120

EMA1_MAX_COUNT = 200

ROC_PERIOD = 10

BREAKOUT_MAX_COUNT = 2


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
# 전역 변수
# =========================================================

latest_upbit_data = []
latest_okx_data = []

latest_usdt_krw = 0

latest_upbit_update_time = "-"
latest_okx_update_time = "-"

latest_upbit_markets = []

request_lock = threading.Lock()
update_lock = threading.Lock()

last_request_time = 0


# =========================================================
# 돌파 이후 추적
#
# key:
# UPBIT:KRW-BTC
# OKX:BTC-USDT-SWAP
#
# ③부터 등록
# 🔻 하락돌파 발생 시 종료
# =========================================================

tracked_breakout_coins = {}

tracking_lock = threading.Lock()


# =========================================================
# OKX 캐시
# =========================================================

okx_ticker_cache = {}

okx_1h_cache = {}
okx_1h_cache_time = "-"


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

        return (
            f"{minutes // 1440}D"
        )

    if minutes >= 60:

        return (
            f"{minutes // 60}H"
        )

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
# 현재 캔들 시작 시간
# =========================================================

def get_current_candle_start(minutes):

    minutes = int(minutes)

    now = datetime.now(
        KST
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
# 시간봉 검증
# =========================================================

def validate_timeframe():

    global EMA_TIMEFRAME
    global EMA_HIGH_TIMEFRAME

    try:

        EMA_TIMEFRAME = int(
            EMA_TIMEFRAME
        )

        EMA_HIGH_TIMEFRAME = int(
            EMA_HIGH_TIMEFRAME
        )

    except Exception:

        raise ValueError(
            "EMA 시간봉은 숫자여야 합니다."
        )

    if (
        EMA_TIMEFRAME
        not in SUPPORTED_UPBIT_TIMEFRAMES
    ):

        raise ValueError(
            f"EMA_TIMEFRAME 오류: "
            f"{EMA_TIMEFRAME}\n"
            "Upbit 지원값: 5, 15, 30, 60, 240"
        )

    if (
        EMA_HIGH_TIMEFRAME
        not in SUPPORTED_UPBIT_TIMEFRAMES
    ):

        raise ValueError(
            f"EMA_HIGH_TIMEFRAME 오류: "
            f"{EMA_HIGH_TIMEFRAME}\n"
            "Upbit 지원값: 5, 15, 30, 60, 240"
        )

    if (
        get_okx_bar(
            EMA_TIMEFRAME
        )
        is None
    ):

        raise ValueError(
            f"OKX에서 지원하지 않는 시간봉: "
            f"{EMA_TIMEFRAME}"
        )

    if (
        get_okx_bar(
            EMA_HIGH_TIMEFRAME
        )
        is None
    ):

        raise ValueError(
            f"OKX에서 지원하지 않는 HIGH 시간봉: "
            f"{EMA_HIGH_TIMEFRAME}"
        )

    if USE_EMA_TIMEFRAME not in (
        "Y",
        "N"
    ):

        raise ValueError(
            "USE_EMA_TIMEFRAME은 Y 또는 N만 가능합니다."
        )

    if USE_EMA_HIGH_TIMEFRAME not in (
        "Y",
        "N"
    ):

        raise ValueError(
            "USE_EMA_HIGH_TIMEFRAME은 Y 또는 N만 가능합니다."
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
                f"{url} "
                f"{wait}초"
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
        f"[API 최종 실패] "
        f"{url}"
    )

    return None


# =========================================================
# Upbit 마켓
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
# USDT/KRW
# =========================================================

def get_usdt_krw():

    r = retry(
        requests.get,
        "https://api.upbit.com/v1/ticker?markets=KRW-USDT",
        timeout=15
    )

    if r is None:

        return None

    try:

        price = float(
            r.json()[0][
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
# Upbit 캔들
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


# =========================================================
# Upbit History
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
# 현재 ROC용 Upbit
# =========================================================

def get_upbit_current_roc_data(
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

        params["before"] = str(
            before
        )

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
                df.confirm.astype(str)
                == "1"
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
            .sort_values(
                "ts"
            )
            .drop_duplicates(
                "ts"
            )
            .reset_index(
                drop=True
            )
        )

    except Exception as e:

        log.error(
            f"OKX {inst} {bar} 오류: {e}"
        )

        return None


# =========================================================
# OKX History
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
                "ts"
            )
            .sort_values(
                "ts"
            )
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
# OKX Ticker
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

        data = r.json().get(
            "data",
            []
        )

        result = {}

        for x in data:

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

                    "last":
                        last
                }

        okx_ticker_cache = result

        return result

    except Exception as e:

        log.error(
            f"OKX ticker 오류: {e}"
        )

        return {}


# =========================================================
# OKX Symbols
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


# =========================================================
# OKX 현재가
# =========================================================

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
            f"OKX 현재 1H 오류 "
            f"{inst}: {e}"
        )

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

def ema_alignment_count(
    df
):

    if (
        df is None
        or df.empty
    ):

        return {

            "direction":
                "none",

            "count":
                0
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

            a = float(
                e10.iloc[i]
            )

            b = float(
                e30.iloc[i]
            )

            c = float(
                e60.iloc[i]
            )

            d = float(
                e120.iloc[i]
            )

            if a > b > c > d:

                return "long"

            if a < b < c < d:

                return "short"

            return "none"

        current = get_dir(
            -1
        )

        if current == "none":

            return {

                "direction":
                    "none",

                "count":
                    0
            }

        count = 0

        for i in range(
            len(df) - 1,
            -1,
            -1
        ):

            if (
                get_dir(i)
                == current
            ):

                count += 1

            else:

                break

        return {

            "direction":
                current,

            "count":
                count
        }

    except Exception as e:

        log.error(
            f"EMA 배열 오류: {e}"
        )

        return {

            "direction":
                "none",

            "count":
                0
        }


def ema_display(
    df,
    current_price=None
):

    x = ema_alignment_count(
        df
    )

    d = x[
        "direction"
    ]

    icon = {

        "long":
            "🟢",

        "short":
            "🔴"

    }.get(
        d,
        "⚪"
    )

    return {

        "display":
            f"{icon}({x['count']})",

        "direction":
            d,

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

        selected.append(
            e1
        )

    if USE_EMA_HIGH_TIMEFRAME == "Y":

        selected.append(
            e_high
        )

    if not selected:

        return {

            "direction":
                "none",

            "valid":
                True
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
# EMA 필터 통과
# =========================================================

def ema_filter_pass(
    e1,
    e_high
):

    selected = []

    if USE_EMA_TIMEFRAME == "Y":

        selected.append(
            e1
        )

    if USE_EMA_HIGH_TIMEFRAME == "Y":

        selected.append(
            e_high
        )

    if not selected:

        return True

    directions = []

    for e in selected:

        d = e.get(
            "direction",
            "none"
        )

        if d not in (
            "long",
            "short"
        ):

            return False

        directions.append(
            d
        )

    return (
        len(
            set(directions)
        )
        == 1
    )


# =========================================================
# ROC
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
# ROC 양수/음수 연속 카운트
# =========================================================

def roc_count(
    series,
    positive=True
):

    if (
        series is None
        or len(series) == 0
    ):

        return 0

    count = 0

    for value in reversed(
        series.tolist()
    ):

        if pd.isna(value):

            break

        if (
            float(value) > 0
        ) == positive:

            count += 1

        else:

            break

    return count


# =========================================================
# ROC 교차
#
# ⓪ 현재 돌파
# ① 돌파 후 1봉
# ② 돌파 후 2봉
# ③ 돌파 후 3봉
# ...
# =========================================================

def roc_cross_state(
    series,
    cross_type
):

    result = {

        "state":
            "none",

        "count":
            0
    }

    try:

        if (
            series is None
            or len(series) < 2
        ):

            return result

        values = []

        for x in series.tolist():

            if pd.isna(x):

                values.append(
                    None
                )

            else:

                values.append(
                    float(x)
                )

        latest = values[-1]

        if latest is None:

            return result

        # -------------------------------------------------
        # 최근 0선 돌파 위치 검색
        # -------------------------------------------------

        cross_index = None

        for i in range(
            len(values) - 1,
            0,
            -1
        ):

            prev = values[i - 1]
            curr = values[i]

            if (
                prev is None
                or curr is None
            ):

                continue

            if cross_type == "long_breakout":

                if (
                    prev <= 0
                    and curr > 0
                ):

                    cross_index = i
                    break

            elif cross_type == "short_breakout":

                if (
                    prev >= 0
                    and curr < 0
                ):

                    cross_index = i
                    break

        if cross_index is None:

            return result

        # -------------------------------------------------
        # 돌파 후 몇 봉인지
        # -------------------------------------------------

        elapsed = (
            len(values)
            - 1
            - cross_index
        )

        # -------------------------------------------------
        # 현재 방향이 유지되지 않으면 종료
        # -------------------------------------------------

        if cross_type == "long_breakout":

            if latest <= 0:

                return result

        elif cross_type == "short_breakout":

            if latest >= 0:

                return result

        # -------------------------------------------------
        # 현재 돌파
        # -------------------------------------------------

        if elapsed == 0:

            return {

                "state":
                    "current",

                "count":
                    0
            }

        # -------------------------------------------------
        # 1봉
        # -------------------------------------------------

        if elapsed == 1:

            return {

                "state":
                    "confirmed",

                "count":
                    1
            }

        # -------------------------------------------------
        # 2봉
        # -------------------------------------------------

        if elapsed == 2:

            return {

                "state":
                    "next",

                "count":
                    2
            }

        # -------------------------------------------------
        # 3봉 이상
        #
        # 여기부터 추적 단계
        # -------------------------------------------------

        return {

            "state":
                "tracking",

            "count":
                elapsed
        }

    except Exception as e:

        log.error(
            f"ROC 교차 상태 오류: {e}"
        )

        return result


# =========================================================
# ROC 분석
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

        "roc10_negative_count":
            0,

        "long_breakout":
            False,

        "short_breakout":
            False,

        "long_breakout_count":
            0,

        "short_breakout_count":
            0,

        "long_breakout_state":
            "none",

        "short_breakout_state":
            "none",

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

        confirmed = roc(
            df_confirmed
        )

        current = roc(
            df_current
        )

        if (
            confirmed is None
            or current is None
        ):

            return result

        current_clean = (
            current
            .dropna()
        )

        confirmed_clean = (
            confirmed
            .dropna()
        )

        if current_clean.empty:

            return result

        current_value = float(
            current_clean.iloc[-1]
        )

        if len(
            current_clean
        ) >= 2:

            previous = float(
                current_clean.iloc[-2]
            )

        elif not confirmed_clean.empty:

            previous = float(
                confirmed_clean.iloc[-1]
            )

        else:

            previous = current_value

        positive_count = roc_count(
            current,
            True
        )

        negative_count = roc_count(
            current,
            False
        )

        lb = roc_cross_state(
            current,
            "long_breakout"
        )

        sb = roc_cross_state(
            current,
            "short_breakout"
        )

        result.update({

            "roc10":
                current_value,

            "roc10_previous":
                previous,

            "roc10_count":
                positive_count,

            "roc10_negative_count":
                negative_count,

            "long_breakout":
                lb["state"] != "none",

            "short_breakout":
                sb["state"] != "none",

            "long_breakout_count":
                lb["count"],

            "short_breakout_count":
                sb["count"],

            "long_breakout_state":
                lb["state"],

            "short_breakout_state":
                sb["state"]
        })

        if (
            lb["state"]
            != "none"
        ):

            result.update({

                "state":
                    "long_breakout",

                "display":
                    "🚀"
                    + count_icon(
                        lb["count"]
                    )
            })

        elif (
            sb["state"]
            != "none"
        ):

            result.update({

                "state":
                    "short_breakout",

                "display":
                    "🔻"
                    + count_icon(
                        sb["count"]
                    )
            })

        elif (
            current_value > 0
            and positive_count >= 2
        ):

            result.update({

                "state":
                    "progress",

                "display":
                    f"진행 {positive_count}"
            })

        elif (
            current_value < 0
            and negative_count >= 2
        ):

            result.update({

                "state":
                    "short_progress",

                "display":
                    f"숏진행 {negative_count}"
            })

        return result

    except Exception as e:

        log.error(
            f"ROC 분석 오류: {e}"
        )

        return result


# =========================================================
# 카운트 아이콘
# =========================================================

def count_icon(
    count
):

    try:

        count = int(
            count
        )

    except Exception:

        return ""

    icons = {

        0: "⓪",
        1: "①",
        2: "②",
        3: "③",
        4: "④",
        5: "⑤",
        6: "⑥",
        7: "⑦",
        8: "⑧",
        9: "⑨",
        10: "⑩"
    }

    if count in icons:

        return icons[count]

    return f"({count})"


# =========================================================
# 일변동
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

    except Exception:

        return None


def daily_changes(
    df
):

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

def empty_analysis():

    e = {

        "display":
            "⚪(0)",

        "direction":
            "none",

        "count":
            0,

        "current_price":
            None
    }

    return {

        "ema_1h":
            e.copy(),

        "ema_high":
            e.copy(),

        "roc": {

            "roc10":
                None,

            "roc10_previous":
                None,

            "roc10_count":
                0,

            "roc10_negative_count":
                0,

            "long_breakout":
                False,

            "short_breakout":
                False,

            "long_breakout_count":
                0,

            "short_breakout_count":
                0,

            "long_breakout_state":
                "none",

            "short_breakout_state":
                "none",

            "state":
                "none",

            "display":
                "-"
        },

        "changes":
            None,

        "breakout_qualified":
            False,

        "short_breakout_qualified":
            False,

        "progress_qualified":
            False,

        "short_progress_qualified":
            False,

        "direction_1h":
            "none",

        "df1h":
            None
    }


# =========================================================
# 신호 자격
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

    filter_direction = (
        filter_info[
            "direction"
        ]
    )

    filter_pass = (
        ema_filter_pass(
            e1,
            e_high
        )
    )

    # -----------------------------------------------------
    # EMA 필터를 전혀 사용하지 않을 경우
    # ROC만으로 돌파 판단
    # -----------------------------------------------------

    if (
        USE_EMA_TIMEFRAME == "N"
        and
        USE_EMA_HIGH_TIMEFRAME == "N"
    ):

        long_base = True
        short_base = True

    else:

        long_base = (
            filter_pass
            and
            filter_direction
            == "long"
        )

        short_base = (
            filter_pass
            and
            filter_direction
            == "short"
        )

    return {

        "breakout_qualified":
            long_base
            and
            r[
                "long_breakout"
            ],

        "short_breakout_qualified":
            short_base
            and
            r[
                "short_breakout"
            ],

        "progress_qualified":
            long_base
            and
            r[
                "roc10"
            ] is not None
            and
            r[
                "roc10"
            ] > 0
            and
            r[
                "roc10_count"
            ] >= 2,

        "short_progress_qualified":
            short_base
            and
            r[
                "roc10"
            ] is not None
            and
            r[
                "roc10"
            ] < 0
            and
            r[
                "roc10_negative_count"
            ] >= 2,

        "filter_direction":
            filter_direction
    }


# =========================================================
# OKX 분석
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

        df_confirmed = (
            history_okx(
                market,
                bar
            )
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

    r = roc_analysis(
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

        "ema_1h":
            e1,

        "ema_high":
            e_high,

        "roc":
            r,

        "changes":
            changes,

        **q,

        "direction_1h":
            q[
                "filter_direction"
            ],

        "df1h":
            df_confirmed
    }


# =========================================================
# 통합 분석
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

    df_confirmed = (
        history_upbit(
            market,
            EMA_TIMEFRAME
        )
    )

    df_high = (
        history_upbit(
            market,
            EMA_HIGH_TIMEFRAME
        )
    )

    df_current = (
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

    r = roc_analysis(
        df_confirmed,
        df_current
    )

    q = get_signal_qualified(
        e1,
        e_high,
        r
    )

    return {

        "ema_1h":
            e1,

        "ema_high":
            e_high,

        "roc":
            r,

        "changes":
            changes,

        **q,

        "direction_1h":
            q[
                "filter_direction"
            ],

        "df1h":
            df_confirmed
    }


# =========================================================
# 행 생성
# =========================================================

def make_row(
    rank,
    name,
    volume,
    analysis,
    current_price=None
):

    a = (
        analysis
        or empty_analysis()
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

        "ema_1h":
            a[
                "ema_1h"
            ],

        "ema_high":
            a[
                "ema_high"
            ],

        "roc":
            a[
                "roc"
            ],

        "breakout_qualified":
            a[
                "breakout_qualified"
            ],

        "short_breakout_qualified":
            a[
                "short_breakout_qualified"
            ],

        "progress_qualified":
            a[
                "progress_qualified"
            ],

        "short_progress_qualified":
            a[
                "short_progress_qualified"
            ],

        "direction":
            a[
                "direction_1h"
            ]
    }


# =========================================================
# 후보 검사
# =========================================================

def is_breakout(row):

    return bool(
        row
        and row.get(
            "breakout_qualified"
        )
    )


def is_short_breakout(row):

    return bool(
        row
        and row.get(
            "short_breakout_qualified"
        )
    )


def is_progress(row):

    return bool(
        row
        and row.get(
            "progress_qualified"
        )
    )


def is_short_progress(row):

    return bool(
        row
        and row.get(
            "short_progress_qualified"
        )
    )


# =========================================================
# 돌파 추적
#
# 핵심
#
# 🚀⓪
# 🚀①
# 🚀②
# 🚀③ ← 여기서 등록
#
# 등록 이후에는
# EMA 조건이 잠시 깨져도 유지
#
# 🔻 하락돌파 발생
# → 추적 종료
# =========================================================

def update_breakout_tracking(
    source,
    market,
    row
):

    key = (
        f"{source}:{market}"
    )

    r = row.get(
        "roc",
        {}
    )

    # -----------------------------------------------------
    # 하락돌파
    #
    # 추적 중이면 즉시 종료
    # -----------------------------------------------------

    if is_short_breakout(
        row
    ):

        with tracking_lock:

            if key in tracked_breakout_coins:

                tracked_breakout_coins.pop(
                    key,
                    None
                )

                log.info(
                    f"[돌파 추적 종료] "
                    f"{source} / {market} / "
                    f"하락돌파"
                )

        return

    # -----------------------------------------------------
    # 롱 돌파 카운트
    # -----------------------------------------------------

    long_state = r.get(
        "long_breakout_state",
        "none"
    )

    long_count = int(
        r.get(
            "long_breakout_count",
            0
        )
        or 0
    )

    # -----------------------------------------------------
    # ③부터 추적 시작
    # -----------------------------------------------------

    tracking_ready = (

        long_state == "tracking"

        and

        long_count >= 3
    )

    if tracking_ready:

        with tracking_lock:

            # ---------------------------------------------
            # 최초 등록
            # ---------------------------------------------

            if (
                key
                not in tracked_breakout_coins
            ):

                tracked_breakout_coins[
                    key
                ] = {

                    "source":
                        source,

                    "market":
                        market,

                    "name":
                        row.get(
                            "name",
                            market
                        ),

                    "started_at":
                        kst(),

                    "tracking_count":
                        long_count,

                    "row":
                        copy.deepcopy(
                            row
                        )
                }

                log.info(
                    f"[돌파 추적 시작] "
                    f"{source} / "
                    f"{market} / "
                    f"③"
                )

            else:

                item = (
                    tracked_breakout_coins[
                        key
                    ]
                )

                item[
                    "tracking_count"
                ] = max(
                    int(
                        item.get(
                            "tracking_count",
                            3
                        )
                    ),
                    long_count
                )

                item[
                    "row"
                ] = copy.deepcopy(
                    row
                )

        return

    # -----------------------------------------------------
    # 이미 추적 중인 경우
    #
    # 조건이 사라져도 삭제하지 않는다.
    # -----------------------------------------------------

    with tracking_lock:

        if (
            key
            in tracked_breakout_coins
        ):

            item = (
                tracked_breakout_coins[
                    key
                ]
            )

            item[
                "row"
            ] = copy.deepcopy(
                row
            )

            item[
                "tracking_count"
            ] = max(
                int(
                    item.get(
                        "tracking_count",
                        3
                    )
                ),
                long_count
            )


# =========================================================
# 추적 리스트 업데이트
# =========================================================

def update_tracking_from_rows(
    source,
    rows
):

    for row in rows:

        market = row.get(
            "market"
        )

        if not market:

            # name밖에 없는 경우
            # Upbit/OKX 순위 행은 아래에서
            # 별도 처리
            continue

        update_breakout_tracking(
            source,
            market,
            row
        )


# =========================================================
# Upbit 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update_time

    log.info(
        f"========== "
        f"업비트 TOP{TOP_N} "
        f"=========="
    )

    markets = sorted(
        get_upbit_markets(),
        key=lambda x:
            x[
                "volume_24h"
            ],
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

        row = make_row(
            rank,
            coin,
            item[
                "volume_24h"
            ],
            a,
            price
        )

        # 실제 시장코드 저장
        row[
            "market"
        ] = market

        row[
            "source"
        ] = "UPBIT"

        rows.append(
            row
        )

        # ---------------------------------------------
        # 돌파 추적
        # ---------------------------------------------

        update_breakout_tracking(
            "UPBIT",
            market,
            row
        )

    latest_upbit_data = rows

    latest_upbit_update_time = kst()

    log.info(
        f"업비트 완료 / "
        f"롱돌파 "
        f"{sum(is_breakout(x) for x in rows)}개 / "
        f"숏돌파 "
        f"{sum(is_short_breakout(x) for x in rows)}개 / "
        f"롱진행 "
        f"{sum(is_progress(x) for x in rows)}개 / "
        f"숏진행 "
        f"{sum(is_short_progress(x) for x in rows)}개"
    )


# =========================================================
# 추적 중인 Upbit 종목을 TOP30 밖에서도 감시
#
# 중요:
# TOP30에서 빠졌다고 추적 종료하지 않는다.
#
# 하락돌파가 실제 발생해야 종료한다.
# =========================================================

def monitor_tracked_upbit():

    with tracking_lock:

        items = [
            copy.deepcopy(x)
            for x in
            tracked_breakout_coins.values()
            if x.get(
                "source"
            ) == "UPBIT"
        ]

    if not items:

        return

    current_markets = set(
        latest_upbit_markets
    )

    top_markets = {

        x.get(
            "market"
        )

        for x in latest_upbit_data
    }

    for item in items:

        market = item.get(
            "market"
        )

        if not market:

            continue

        # TOP30 안에서 이미 분석됨
        if market in top_markets:

            continue

        # -------------------------------------------------
        # TOP30 밖의 추적 종목도 다시 분석
        # -------------------------------------------------

        if (
            current_markets
            and market
            not in current_markets
        ):

            continue

        try:

            ticker = next(
                (
                    x
                    for x in
                    get_upbit_markets()
                    if x[
                        "market"
                    ] == market
                ),
                None
            )

            if ticker is None:

                continue

            price = ticker[
                "current_price"
            ]

            a = analyze(
                market,
                current_price=price
            )

            if a is None:

                continue

            old_row = (
                item.get(
                    "row",
                    {}
                )
            )

            name = (
                old_row.get(
                    "name"
                )
                or market.replace(
                    "KRW-",
                    ""
                )
            )

            row = make_row(
                old_row.get(
                    "rank",
                    "-"
                ),
                name,
                ticker.get(
                    "volume_24h",
                    0
                ),
                a,
                price
            )

            row[
                "market"
            ] = market

            row[
                "source"
            ] = "UPBIT"

            # 하락돌파이면 여기서 제거
            update_breakout_tracking(
                "UPBIT",
                market,
                row
            )

        except Exception as e:

            log.error(
                f"[추적 재조회 오류] "
                f"{market}: {e}"
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

    if (
        not usdt
        or usdt <= 0
    ):

        return False

    log.info(
        "========== "
        "OKX 거래대금 조회 시작 "
        "=========="
    )

    okx_1h_cache = {}

    tickers = get_okx_tickers()

    if not tickers:

        log.warning(
            "OKX ticker 조회 실패"
        )

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

        v = get_okx_volume_cached(
            symbol,
            usdt
        )

        if (
            v
            and v > 0
        ):

            volumes[
                symbol
            ] = v

        if idx % 50 == 0:

            log.info(
                f"OKX 거래대금 "
                f"{idx}/"
                f"{len(symbols)} 완료"
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

        price = get_okx_cached_price(
            symbol
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

        row = make_row(
            rank,
            name,
            volumes[
                symbol
            ],
            a,
            price
        )

        row[
            "market"
        ] = symbol

        row[
            "source"
        ] = "OKX"

        rows.append(
            row
        )

        # 돌파 추적
        update_breakout_tracking(
            "OKX",
            symbol,
            row
        )

    latest_okx_data = rows

    okx_1h_cache_time = kst()
    latest_okx_update_time = kst()

    log.info(
        f"OKX 완료 / "
        f"롱돌파 "
        f"{sum(is_breakout(x) for x in rows)}개 / "
        f"숏돌파 "
        f"{sum(is_short_breakout(x) for x in rows)}개"
    )

    return True


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    global latest_usdt_krw
    global latest_upbit_data
    global latest_okx_data

    if not update_lock.acquire(
        False
    ):

        log.warning(
            "이전 조회 진행 중 → 건너뜀"
        )

        return

    try:

        log.info(
            f"========== "
            f"전체 조회 {kst()} "
            f"=========="
        )

        # -------------------------------------------------
        # Upbit
        # -------------------------------------------------

        if USE_UPBIT == "Y":

            try:

                update_upbit()

                # TOP30 밖의 추적 종목도 확인
                monitor_tracked_upbit()

            except Exception as e:

                log.exception(
                    f"업비트 업데이트 오류: "
                    f"{e}"
                )

        else:

            latest_upbit_data = []

        # -------------------------------------------------
        # OKX
        # -------------------------------------------------

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
                    f"OKX 업데이트 오류: "
                    f"{e}"
                )

        else:

            latest_okx_data = []

    finally:

        update_lock.release()


# =========================================================
# BTC 시장 방향
# =========================================================

def market_direction_html(
    direction,
    count
):

    try:

        count = int(
            count
        )

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


def market_roc_html(
    r
):

    if not r:

        return (
            '<span class="market-zero">'
            '⚪ -'
            '</span>'
        )

    value = r.get(
        "roc10"
    )

    if value is None:

        return (
            '<span class="market-zero">'
            '⚪ -'
            '</span>'
        )

    try:

        value = float(
            value
        )

    except Exception:

        return (
            '<span class="market-zero">'
            '⚪ -'
            '</span>'
        )

    if value > 0:

        count = max(
            int(
                r.get(
                    "roc10_count",
                    0
                )
            ),
            1
        )

        return (
            '<span class="market-up">'
            f'🟢 상승 {count}'
            '</span>'
        )

    if value < 0:

        count = max(
            int(
                r.get(
                    "roc10_negative_count",
                    0
                )
            ),
            1
        )

        return (
            '<span class="market-down">'
            f'🔴 하락 {count}'
            '</span>'
        )

    return (
        '<span class="market-zero">'
        '⚪ 0'
        '</span>'
    )


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

        value = float(
            value
        )

    except Exception:

        return (
            '<span class="market-zero">'
            '-'
            '</span>'
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
# BTC 방향
# =========================================================

def btc_position_view(
    row
):

    if not row:

        return {

            "text":
                "⚪ 관망",

            "class":
                "wait"
        }

    ema_1 = row.get(
        "ema_1h",
        {}
    )

    ema_high = row.get(
        "ema_high",
        {}
    )

    r = row.get(
        "roc",
        {}
    )

    selected = []

    if USE_EMA_TIMEFRAME == "Y":

        selected.append(
            ema_1
        )

    if USE_EMA_HIGH_TIMEFRAME == "Y":

        selected.append(
            ema_high
        )

    if not selected:

        d = None

    else:

        directions = [

            x.get(
                "direction",
                "none"
            )

            for x in selected
        ]

        if all(
            x == "long"
            for x in directions
        ):

            d = "long"

        elif all(
            x == "short"
            for x in directions
        ):

            d = "short"

        else:

            d = "none"

    roc_value = r.get(
        "roc10"
    )

    long_state = r.get(
        "long_breakout_state",
        "none"
    )

    short_state = r.get(
        "short_breakout_state",
        "none"
    )

    # -----------------------------------------------------
    # 이평 필터 없음
    # -----------------------------------------------------

    if not selected:

        if long_state != "none":

            return {

                "text":
                    "🚀"
                    + count_icon(
                        r.get(
                            "long_breakout_count",
                            0
                        )
                    ),

                "class":
                    "long"
            }

        if short_state != "none":

            return {

                "text":
                    "🔻"
                    + count_icon(
                        r.get(
                            "short_breakout_count",
                            0
                        )
                    ),

                "class":
                    "short"
            }

        if (
            roc_value is not None
            and float(
                roc_value
            ) > 0
        ):

            return {

                "text":
                    "🟢 롱 우세",

                "class":
                    "long"
            }

        if (
            roc_value is not None
            and float(
                roc_value
            ) < 0
        ):

            return {

                "text":
                    "🔴 숏 우세",

                "class":
                    "short"
            }

        return {

            "text":
                "⚪ 관망",

            "class":
                "wait"
        }

    # -----------------------------------------------------
    # 방향 불일치
    # -----------------------------------------------------

    if d == "none":

        return {

            "text":
                "⚪ 관망",

            "class":
                "wait"
        }

    # -----------------------------------------------------
    # 롱
    # -----------------------------------------------------

    if d == "long":

        if long_state != "none":

            return {

                "text":
                    "🚀"
                    + count_icon(
                        r.get(
                            "long_breakout_count",
                            0
                        )
                    ),

                "class":
                    "long"
            }

        if (
            roc_value is not None
            and float(
                roc_value
            ) > 0
        ):

            return {

                "text":
                    "🟢 롱 우세",

                "class":
                    "long"
            }

        return {

            "text":
                "⚪ 롱 대기",

            "class":
                "wait"
        }

    # -----------------------------------------------------
    # 숏
    # -----------------------------------------------------

    if d == "short":

        if short_state != "none":

            return {

                "text":
                    "🔻"
                    + count_icon(
                        r.get(
                            "short_breakout_count",
                            0
                        )
                    ),

                "class":
                    "short"
            }

        if (
            roc_value is not None
            and float(
                roc_value
            ) < 0
        ):

            return {

                "text":
                    "🔴 숏 우세",

                "class":
                    "short"
            }

        return {

            "text":
                "⚪ 숏 대기",

            "class":
                "wait"
        }

    return {

        "text":
            "⚪ 관망",

        "class":
            "wait"
    }


# =========================================================
# BTC 찾기
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
# BTC 시황
# =========================================================

def market_summary_html():

    btc = get_market_row(
        "BTC"
    )

    if btc is None:

        return f"""

        <div class="market-summary">

            <div class="market-title">

                <span class="market-title-main">
                    ₿ BTC 시장 시황
                </span>

                <span class="market-title-sub">
                    롱/숏 방향 참고
                </span>

            </div>

            <div class="btc-mobile">

                <div class="btc-top">

                    <span class="btc-name">
                        ₿ BTC
                    </span>

                    <span class="btc-price">
                        -
                    </span>

                    <span class="btc-change">
                        -
                    </span>

                </div>

                <div class="btc-bottom">

                    <span>
                        {format_timeframe(
                            EMA_TIMEFRAME
                        )} ⚪ 0
                    </span>

                    <span>
                        {format_timeframe(
                            EMA_HIGH_TIMEFRAME
                        )} ⚪ 0
                    </span>

                    <span>
                        ROC ⚪ -
                    </span>

                    <span class="btc-position wait">
                        ⚪ 관망
                    </span>

                </div>

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

    roc_data = btc.get(
        "roc",
        {}
    )

    position = btc_position_view(
        btc
    )

    return f"""

    <div class="market-summary">

        <div class="market-title">

            <span class="market-title-main">
                ₿ BTC 시장 시황
            </span>

            <span class="market-title-sub">
                롱/숏 방향 참고
            </span>

        </div>

        <div class="btc-mobile">

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

                <span class="btc-change">
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
                    ROC
                    {market_roc_html(
                        roc_data
                    )}
                </span>

                <span
                    class="
                        btc-position
                        {position["class"]}
                    "
                >
                    {position["text"]}
                </span>

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
        "roc10"
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

        value = float(
            value
        )

    except Exception:

        return (
            '<div class="roc-cell">'
            '<span class="roc-zero">'
            '⚪ 0'
            '</span>'
            '</div>'
        )

    long_state = r.get(
        "long_breakout_state",
        "none"
    )

    if long_state != "none":

        count = int(
            r.get(
                "long_breakout_count",
                0
            )
        )

        return f"""

        <div class="roc-cell">

            <span class="roc-positive">

                🚀{count_icon(count)}

            </span>

        </div>

        """

    short_state = r.get(
        "short_breakout_state",
        "none"
    )

    if short_state != "none":

        count = int(
            r.get(
                "short_breakout_count",
                0
            )
        )

        return f"""

        <div class="roc-cell">

            <span class="roc-negative">

                🔻{count_icon(count)}

            </span>

        </div>

        """

    if value > 0:

        count = max(
            int(
                r.get(
                    "roc10_count",
                    0
                )
            ),
            1
        )

        return f"""

        <div class="roc-cell">

            <span class="roc-positive">
                🟢 상승 {count}
            </span>

        </div>

        """

    if value < 0:

        count = max(
            int(
                r.get(
                    "roc10_negative_count",
                    0
                )
            ),
            1
        )

        return f"""

        <div class="roc-cell">

            <span class="roc-negative">
                🔴 하락 {count}
            </span>

        </div>

        """

    return """

    <div class="roc-cell">

        <span class="roc-zero">
            ⚪ 0
        </span>

    </div>

    """


# =========================================================
# 신호 HTML
# =========================================================

def signal_html(
    row
):

    r = row.get(
        "roc",
        {}
    )

    long_state = r.get(
        "long_breakout_state",
        "none"
    )

    if (
        row.get(
            "breakout_qualified",
            False
        )
        and
        long_state != "none"
    ):

        count = int(
            r.get(
                "long_breakout_count",
                0
            )
        )

        return (

            '<span '
            'class="signal-icon long-breakout" '
            'title="롱 돌파">'
            f'🚀{count_icon(count)}'
            '</span>'

        )

    short_state = r.get(
        "short_breakout_state",
        "none"
    )

    if (
        row.get(
            "short_breakout_qualified",
            False
        )
        and
        short_state != "none"
    ):

        count = int(
            r.get(
                "short_breakout_count",
                0
            )
        )

        return (

            '<span '
            'class="signal-icon short-breakout" '
            'title="숏 돌파">'
            f'🔻{count_icon(count)}'
            '</span>'

        )

    if row.get(
        "progress_qualified",
        False
    ):

        return (

            '<span '
            'class="signal-icon long-progress" '
            'title="롱 진행">'
            '☀️'
            '</span>'

        )

    if row.get(
        "short_progress_qualified",
        False
    ):

        return (

            '<span '
            'class="signal-icon short-progress" '
            'title="숏 진행">'
            '🌧️'
            '</span>'

        )

    return (
        '<span class="muted">-</span>'
    )


# =========================================================
# EMA HTML
# =========================================================

def ema_html(
    e
):

    if not e:

        return "⚪0"

    d = e.get(
        "direction",
        "none"
    )

    count = e.get(
        "count",
        0
    )

    icon = {

        "long":
            "🟢",

        "short":
            "🔴"

    }.get(
        d,
        "⚪"
    )

    return f"{icon}{count}"


# =========================================================
# 행 클래스
# =========================================================

def row_class(
    x
):

    if x.get(
        "breakout_qualified"
    ):

        return (
            "breakout-qualified"
        )

    if x.get(
        "short_breakout_qualified"
    ):

        return (
            "short-breakout-qualified"
        )

    if x.get(
        "progress_qualified"
    ):

        return (
            "progress-qualified"
        )

    if x.get(
        "short_progress_qualified"
    ):

        return (
            "short-progress-qualified"
        )

    return ""


# =========================================================
# 행 HTML
# =========================================================

def rows_html(
    data,
    focus=None,
    tracking=False
):

    out = []

    for x in data:

        if focus == "breakout":

            cls = (
                "breakout-qualified"
            )

        elif focus == "short_breakout":

            cls = (
                "short-breakout-qualified"
            )

        elif focus == "progress":

            cls = (
                "progress-qualified"
            )

        elif focus == "short_progress":

            cls = (
                "short-progress-qualified"
            )

        else:

            cls = row_class(
                x
            )

        tracking_count = x.get(
            "tracking_count",
            "-"
        )

        tracking_started = x.get(
            "started_at",
            "-"
        )

        if tracking:

            extra = f"""

            <td class="tracking-count">
                ③+
                {tracking_count}
            </td>

            <td class="tracking-start">
                {tracking_started}
            </td>

            """

        else:

            extra = ""

        out.append(
            f"""

            <tr class="{cls}">

                <td>
                    {x.get(
                        "rank",
                        "-"
                    )}
                </td>

                <td class="coin">

                    <b>
                        {x.get(
                            "name",
                            "-"
                        )}
                    </b>

                    <small>
                        {x.get(
                            "change",
                            "-"
                        )}
                    </small>

                </td>

                <td class="vol">
                    {x.get(
                        "volume",
                        "-"
                    )}
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

                    {roc_html(
                        x.get(
                            "roc",
                            {}
                        )
                    )}

                </td>

                <td class="signal-cell">

                    {signal_html(
                        x
                    )}

                </td>

                {extra}

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
    tracking=False
):

    rows = rows_html(
        data,
        focus,
        tracking
    )

    if not rows:

        colspan = (
            8
            if tracking
            else 6
        )

        rows = f"""

        <tr>

            <td
                colspan="{colspan}"
                class="empty"
            >
                현재 후보 없음
            </td>

        </tr>

        """

    if tracking:

        head_extra = """

            <th>
                카운트
            </th>

            <th>
                추적 시작
            </th>

        """

    else:

        head_extra = ""

    return f"""

    <div class="table-wrap">

        <table>

            <thead>

                <tr>

                    <th>#</th>

                    <th>코인</th>

                    <th>거래대금</th>

                    <th>EMA</th>

                    <th>ROC10</th>

                    <th>신호</th>

                    {head_extra}

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
    description
):

    rows = [

        x
        for x in data
        if checker(x)

    ]

    return f"""

    <div class="section-title {focus}-section-title">

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
        focus
    )}

    """


# =========================================================
# 돌파 이후 추적 섹션
# =========================================================

def tracking_section():

    tracked = []

    with tracking_lock:

        for item in (
            tracked_breakout_coins.values()
        ):

            row = copy.deepcopy(
                item.get(
                    "row",
                    {}
                )
            )

            if not row:

                continue

            row[
                "tracking_count"
            ] = item.get(
                "tracking_count",
                3
            )

            row[
                "started_at"
            ] = item.get(
                "started_at",
                "-"
            )

            row[
                "market"
            ] = item.get(
                "market",
                "-"
            )

            row[
                "source"
            ] = item.get(
                "source",
                "-"
            )

            tracked.append(
                row
            )

    # 최신 추적 시작순
    tracked.sort(
        key=lambda x:
            str(
                x.get(
                    "started_at",
                    ""
                )
            ),
        reverse=True
    )

    return f"""

    <div class="section-title tracking-section-title">

        <span class="section-title-main">
            📈 돌파 이후 추적
        </span>

        <span class="section-title-sub">
            🚀③부터 추적 시작 · 🔻 하락돌파 시 종료
        </span>

    </div>

    {table_html(
        tracked,
        tracking=True
    )}

    """


# =========================================================
# 일반 TOP
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
        data
    )}

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

    padding:
        2px
        2px
        8px;
}

h1{
    margin:
        1px
        2px
        2px;

    font-size:12px;
    line-height:14px;
}


/* =====================================================
   제목
   ===================================================== */

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

    padding:
        3px
        5px;

    border-left:
        3px solid
        #39e875;

    background:
        rgba(
            57,
            232,
            117,
            .08
        );

    border-radius:3px;

    white-space:nowrap;

    overflow:hidden;
}

.market-title{
    margin-bottom:4px;
}

.section-title{
    margin:
        5px
        0
        4px;
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


/* =====================================================
   제목 색상
   ===================================================== */

.breakout-section-title{

    border-left-color:
        #39e875;
}

.short_breakout-section-title{

    border-left-color:
        #ff5555;
}

.progress-section-title{

    border-left-color:
        #4cc9ff;
}

.short_progress-section-title{

    border-left-color:
        #ff6666;
}

.tracking-section-title{

    border-left-color:
        #ffd84d;

    background:
        rgba(
            255,
            216,
            77,
            .08
        );
}


/* =====================================================
   BTC
   ===================================================== */

.market-summary{

    width:100%;

    margin:
        2px
        0
        3px;

    padding:
        3px
        4px;

    border-top:
        1px solid
        #242a31;

    border-bottom:
        1px solid
        #242a31;

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

    width:58px;

    font-size:7.5px;

    line-height:10px;

    font-weight:900;

    text-align:right;

    white-space:nowrap;
}

.btc-bottom{

    display:flex;

    align-items:center;

    width:100%;

    min-height:17px;

    gap:6px;

    white-space:nowrap;

    overflow:hidden;

    font-size:5.3px;

    line-height:8px;

    font-weight:800;
}

.btc-bottom > span{

    flex:none;

    white-space:nowrap;
}

.btc-position{

    margin-left:auto;

    min-width:72px;

    padding:
        3px
        5px;

    border-radius:4px;

    text-align:center;

    font-size:8px;

    line-height:12px;

    font-weight:900;

    white-space:nowrap;

    border:
        1px solid
        rgba(
            255,
            255,
            255,
            .08
        );
}

.btc-position.long{

    color:#39e875!important;

    background:
        rgba(
            57,
            232,
            117,
            .12
        );
}

.btc-position.short{

    color:#ff5555!important;

    background:
        rgba(
            255,
            85,
            85,
            .12
        );
}

.btc-position.wait{

    color:#b0b7bf!important;

    background:
        rgba(
            104,
            113,
            123,
            .12
        );
}


/* =====================================================
   색상
   ===================================================== */

.market-up,
.y,
.up,
.roc-positive{

    color:#39e875!important;

    font-weight:900;
}

.market-down,
.n,
.down,
.roc-negative{

    color:#ff5555!important;

    font-weight:900;
}

.market-zero,
.zero,
.roc-zero,
.muted{

    color:#68717b!important;

    font-weight:800;
}


/* =====================================================
   상태
   ===================================================== */

.status{

    display:flex;

    justify-content:center;

    gap:9px;

    margin:
        2px
        2px
        3px;

    padding:
        2px
        0;

    border-top:
        1px solid
        #242a31;

    border-bottom:
        1px solid
        #242a31;

    font-size:6px;

    line-height:7px;

    font-weight:800;
}


/* =====================================================
   신호 아이콘
   기존보다 작게
   ===================================================== */

.signal-icon{

    display:inline-flex;

    align-items:center;

    justify-content:center;

    width:100%;

    min-height:16px;

    font-size:10px;

    line-height:12px;

    font-weight:900;

    white-space:nowrap;
}

.signal-icon.long-breakout{

    filter:
        drop-shadow(
            0 0 2px
            rgba(
                57,
                232,
                117,
                .25
            )
        );
}

.signal-icon.short-breakout{

    filter:
        drop-shadow(
            0 0 2px
            rgba(
                255,
                85,
                85,
                .25
            )
        );
}


/* =====================================================
   테이블
   ===================================================== */

.table-wrap{

    width:100%;

    overflow:hidden;

    border-radius:5px;

    border:
        1px solid
        #272d34;

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
        1px solid
        #292f36;

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
        1px solid
        #22282e;

    text-align:center;

    vertical-align:middle;

    overflow:hidden;
}

tr:last-child td{

    border-bottom:none;
}


/* =====================================================
   일반 테이블 비율
   ===================================================== */

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


/* =====================================================
   추적 테이블
   ===================================================== */

.tracking-count{

    width:8%;

    color:#ffd84d;

    font-size:5px;

    font-weight:900;

    white-space:nowrap;
}

.tracking-start{

    width:14%;

    color:#8d949c;

    font-size:4.5px;

    line-height:6px;

    white-space:nowrap;
}


/* =====================================================
   코인
   ===================================================== */

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

    margin:
        0
        1px;
}


/* =====================================================
   ROC
   ===================================================== */

.roc-cell{

    display:flex;

    flex-direction:row;

    align-items:center;

    justify-content:center;

    gap:1px;

    min-height:17px;

    line-height:8px;

    white-space:nowrap;
}

.roc-cell span{

    font-size:5.8px;

    line-height:8px;

    font-weight:900;

    white-space:nowrap;
}


/* =====================================================
   신호 배경
   ===================================================== */

.breakout-qualified{

    background:
        rgba(
            57,
            232,
            117,
            .08
        );
}

.short-breakout-qualified{

    background:
        rgba(
            255,
            85,
            85,
            .05
        );
}

.progress-qualified{

    background:
        rgba(
            76,
            201,
            255,
            .06
        );
}

.short-progress-qualified{

    background:
        rgba(
            255,
            85,
            85,
            .035
        );
}

.empty{

    height:30px;

    padding:8px;

    color:#555d67;

    font-size:6px;
}


/* =====================================================
   모바일
   ===================================================== */

@media(max-width:380px){

    body{

        padding:
            1px
            1px
            6px;
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

        padding:
            3px
            4px;

        margin-bottom:3px;
    }

    .section-title{

        margin:
            4px
            0
            3px;
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

        width:50px;

        font-size:6.5px;

        line-height:9px;
    }

    .btc-bottom{

        min-height:16px;

        gap:4px;

        font-size:4.8px;
    }

    .btc-position{

        min-width:64px;

        padding:
            2px
            4px;

        font-size:7px;

        line-height:10px;
    }

    .status{

        font-size:5.5px;

        line-height:6px;
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

    .roc-cell span{

        font-size:5.2px;
    }

    .signal-icon{

        font-size:9px;

        line-height:11px;

        min-height:15px;
    }

    .tracking-count{

        font-size:4.5px;
    }

    .tracking-start{

        font-size:3.8px;
    }
}


/* =====================================================
   PC
   ===================================================== */

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

        padding:
            4px
            6px;
    }

    .section-title{

        margin:
            10px
            0
            5px;
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

    .btc-change{

        width:55px;

        font-size:7px;
    }

    .btc-bottom{

        font-size:7px;
    }

    .btc-position{

        min-width:70px;

        font-size:7px;

        line-height:11px;
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

    .roc-cell span{

        font-size:7px;
    }

    .signal-icon{

        font-size:15px;

        line-height:17px;

        min-height:21px;
    }

    .tracking-count{

        font-size:7px;
    }

    .tracking-start{

        font-size:6px;
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
            이평필터 :
            <b class="y">
                {USE_EMA_TIMEFRAME}/
                {USE_EMA_HIGH_TIMEFRAME}
            </b>
        </span>

        <span>
            추적 :
            <b class="y">
                ③부터
            </b>
        </span>

    </div>

    """

    sections = ""

    # -----------------------------------------------------
    # Upbit 롱 돌파
    # -----------------------------------------------------

    if USE_UPBIT == "Y":

        sections += focus_section(

            "🟢 돌파 정배열",

            latest_upbit_data,

            latest_upbit_update_time,

            is_breakout,

            "breakout",

            (
                f"{format_timeframe(
                    EMA_TIMEFRAME
                )}/"
                f"{format_timeframe(
                    EMA_HIGH_TIMEFRAME
                )} "
                f"EMA10>30>60>120 · "
                f"ROC10 음수→양수"
            )
        )

    # -----------------------------------------------------
    # OKX 롱 돌파
    # -----------------------------------------------------

    if USE_OKX == "Y":

        sections += focus_section(

            "🟢 돌파 정배열",

            latest_okx_data,

            latest_okx_update_time,

            is_breakout,

            "breakout",

            (
                f"{format_timeframe(
                    EMA_TIMEFRAME
                )}/"
                f"{format_timeframe(
                    EMA_HIGH_TIMEFRAME
                )} "
                f"EMA10>30>60>120 · "
                f"ROC10 음수→양수"
            )
        )

    # =====================================================
    # ★ 돌파 이후 추적
    #
    # 돌파 정배열 바로 아래
    # =====================================================

    sections += tracking_section()

    # -----------------------------------------------------
    # OKX 숏 돌파
    # -----------------------------------------------------

    if USE_OKX == "Y":

        sections += focus_section(

            "🔴 숏 돌파",

            latest_okx_data,

            latest_okx_update_time,

            is_short_breakout,

            "short_breakout",

            (
                f"{format_timeframe(
                    EMA_TIMEFRAME
                )}/"
                f"{format_timeframe(
                    EMA_HIGH_TIMEFRAME
                )} "
                f"EMA10<30<60<120 · "
                f"ROC10 양수→음수"
            )
        )

    # -----------------------------------------------------
    # Upbit 전체
    # -----------------------------------------------------

    if USE_UPBIT == "Y":

        sections += section(

            "업비트",

            latest_upbit_data,

            latest_upbit_update_time

        )

    # -----------------------------------------------------
    # OKX 전체
    # -----------------------------------------------------

    if USE_OKX == "Y":

        sections += section(

            "OKX",

            latest_okx_data,

            latest_okx_update_time

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

        <!-- 대시보드 5초 자동 갱신 -->

        <meta
            http-equiv="refresh"
            content="5"
        >

        <meta
            name="theme-color"
            content="#0d1014"
        >

        <title>
            {format_timeframe(
                EMA_TIMEFRAME
            )}
            /
            {format_timeframe(
                EMA_HIGH_TIMEFRAME
            )}
            EMA10·30·60·120 · ROC10
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

        time.sleep(
            1
        )


# =========================================================
# Startup
# =========================================================

@app.on_event(
    "startup"
)
def startup():

    if USE_UPBIT not in (
        "Y",
        "N"
    ):

        raise ValueError(
            "USE_UPBIT은 Y 또는 N만 가능합니다."
        )

    if USE_OKX not in (
        "Y",
        "N"
    ):

        raise ValueError(
            "USE_OKX는 Y 또는 N만 가능합니다."
        )

    validate_timeframe()

    tf = format_timeframe(
        EMA_TIMEFRAME
    )

    high_tf = format_timeframe(
        EMA_HIGH_TIMEFRAME
    )

    log.info(
        "========================================"
    )

    log.info(
        f"{tf}/{high_tf} "
        f"EMA10·30·60·120 + ROC10 시작"
    )

    log.info(
        f"업비트={USE_UPBIT} / "
        f"OKX={USE_OKX}"
    )

    log.info(
        f"TOP={TOP_N} / "
        f"UPDATE={UPDATE_MINUTES}분"
    )

    log.info(
        f"EMA1={tf} / "
        f"사용={USE_EMA_TIMEFRAME}"
    )

    log.info(
        f"EMA2={high_tf} / "
        f"사용={USE_EMA_HIGH_TIMEFRAME}"
    )

    log.info(
        "EMA10-30-60-120"
    )

    log.info(
        f"EMA count <= "
        f"{EMA1_MAX_COUNT}"
    )

    log.info(
        "----------------------------------------"
    )

    log.info(
        "이평 필터:"
        f" {USE_EMA_TIMEFRAME}/"
        f"{USE_EMA_HIGH_TIMEFRAME}"
    )

    log.info(
        "Y/Y → 두 시간봉 모두 필터"
    )

    log.info(
        "Y/N → 첫 번째 시간봉만 필터"
    )

    log.info(
        "N/Y → 두 번째 시간봉만 필터"
    )

    log.info(
        "N/N → 이평 필터 사용 안 함"
    )

    log.info(
        "----------------------------------------"
    )

    log.info(
        "ROC 돌파 카운팅:"
    )

    log.info(
        "🚀⓪ = 현재 돌파"
    )

    log.info(
        "🚀① = 돌파 후 1봉"
    )

    log.info(
        "🚀② = 돌파 후 2봉"
    )

    log.info(
        "🚀③ = 돌파 후 3봉 → 추적 시작"
    )

    log.info(
        "📈 돌파 이후 추적 → 하락돌파까지 유지"
    )

    log.info(
        "🔻 하락돌파 → 추적 종료"
    )

    log.info(
        "TOP30 밖으로 이동해도 추적 유지"
    )

    log.info(
        "----------------------------------------"
    )

    log.info(
        "롱 정배열:"
        " EMA10>EMA30>EMA60>EMA120 + "
        "ROC 음수→양수"
    )

    log.info(
        "숏 역배열:"
        " EMA10<EMA30<EMA60<EMA120 + "
        "ROC 양수→음수"
    )

    log.info(
        "----------------------------------------"
    )

    log.info(
        "대시보드 자동 새로고침 = 5초"
    )

    log.info(
        "데이터 분석 주기 = "
        f"{UPDATE_MINUTES}분"
    )

    log.info(
        "========================================"
    )

    # -----------------------------------------------------
    # 최초 데이터 조회
    # -----------------------------------------------------

    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    # -----------------------------------------------------
    # 1분마다 데이터 갱신
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
