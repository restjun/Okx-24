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

warnings.filterwarnings("ignore", category=FutureWarning)

app = FastAPI()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

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
# EMA / ROC 설정
# =========================================================

EMA_TIMEFRAME = 60
EMA_HIGH_TIMEFRAME = 240

EMA1_FAST = 30
EMA1_MID = 60
EMA1_SLOW = 120

EMA1_MAX_COUNT = 100

ROC_PERIOD = 10


# =========================================================
# 지원 시간봉
# =========================================================

UPBIT_TIMEFRAMES = {
    5,
    15,
    30,
    60,
    240,
}

OKX_TIMEFRAMES = {
    5,
    15,
    30,
    60,
    120,
    240,
    360,
    480,
    720,
    1440,
}


# =========================================================
# 전역 데이터
# =========================================================

latest_upbit_data = []
latest_okx_data = []

latest_usdt_krw = None

latest_upbit_update = "-"
latest_okx_update = "-"

latest_upbit_markets = {}

request_lock = threading.Lock()
update_lock = threading.Lock()

last_request_time = 0.0


# =========================================================
# OKX 캐시
# =========================================================

# 전체 ticker 1회 조회 결과
okx_ticker_cache = {}

# 1H 24개 봉 거래대금 캐시
# {inst_id: dataframe}
okx_1h_cache = {}

okx_1h_cache_time = "-"


# =========================================================
# 시간
# =========================================================

def kst():
    return datetime.now(KST)


def format_timeframe(minutes):
    if minutes < 60:
        return f"{minutes}분봉"

    if minutes % 60 == 0:
        hours = minutes // 60

        if hours < 24:
            return f"{hours}시간봉"

        if hours == 24:
            return "일봉"

    return f"{minutes}분봉"


def get_current_candle_start(minutes):
    now = kst()

    if minutes >= 1440:
        return now.replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0
        )

    total_minutes = now.hour * 60 + now.minute

    candle_minutes = (
        total_minutes // minutes
    ) * minutes

    hour = candle_minutes // 60
    minute = candle_minutes % 60

    return now.replace(
        hour=hour,
        minute=minute,
        second=0,
        microsecond=0
    )


# =========================================================
# 시간봉 검증
# =========================================================

def validate_timeframe(minutes, exchange="upbit"):

    if exchange == "upbit":
        if minutes not in UPBIT_TIMEFRAMES:
            raise ValueError(
                f"Upbit 지원 시간봉 오류: {minutes}"
            )

    elif exchange == "okx":
        if minutes not in OKX_TIMEFRAMES:
            raise ValueError(
                f"OKX 지원 시간봉 오류: {minutes}"
            )


# =========================================================
# API 요청 속도 제어
# =========================================================

def wait_request():

    global last_request_time

    with request_lock:

        now = time.time()

        diff = now - last_request_time

        if diff < REQUEST_INTERVAL:
            time.sleep(
                REQUEST_INTERVAL - diff
            )

        last_request_time = time.time()


# =========================================================
# 공통 재시도
# =========================================================

def retry_request(
    method,
    url,
    params=None,
    timeout=10
):

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            wait_request()

            response = requests.request(
                method,
                url,
                params=params,
                timeout=timeout
            )

            if response.status_code == 200:
                return response

            if response.status_code == 429:

                logging.warning(
                    f"429 rate limit "
                    f"{attempt}/{MAX_RETRIES}"
                )

                time.sleep(
                    RATE_LIMIT_WAIT * attempt
                )

                continue

            if 500 <= response.status_code < 600:

                logging.warning(
                    f"server error "
                    f"{response.status_code} "
                    f"{attempt}/{MAX_RETRIES}"
                )

                time.sleep(
                    RATE_LIMIT_WAIT
                )

                continue

            logging.warning(
                f"API error "
                f"{response.status_code}: "
                f"{url}"
            )

        except Exception as e:

            logging.warning(
                f"request error "
                f"{attempt}/{MAX_RETRIES}: {e}"
            )

            time.sleep(
                RATE_LIMIT_WAIT
            )

    return None


# =========================================================
# Upbit 전체 KRW ticker
# =========================================================

def get_upbit_markets():

    global latest_upbit_markets

    url = (
        "https://api.upbit.com/v1/ticker/"
        "all?quote_currencies=KRW"
    )

    response = retry_request(
        "GET",
        url
    )

    if response is None:
        return []

    try:

        data = response.json()

        result = []

        market_map = {}

        for item in data:

            market = item.get("market", "")

            if not market.startswith("KRW-"):
                continue

            turnover = float(
                item.get(
                    "acc_trade_price_24h",
                    0
                ) or 0
            )

            price = float(
                item.get(
                    "trade_price",
                    0
                ) or 0
            )

            coin = market.replace(
                "KRW-",
                ""
            )

            row = {
                "market": market,
                "coin": coin,
                "turnover": turnover,
                "price": price,
            }

            result.append(row)

            market_map[coin] = row

        latest_upbit_markets = market_map

        return result

    except Exception as e:

        logging.error(
            f"Upbit ticker parsing error: {e}"
        )

        return []


# =========================================================
# Upbit USDT/KRW
# =========================================================

def get_usdt_krw():

    url = (
        "https://api.upbit.com/v1/ticker"
        "?markets=KRW-USDT"
    )

    response = retry_request(
        "GET",
        url
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
# Upbit 현재/확정 캔들
# =========================================================

def get_upbit_candle(
    market,
    minutes,
    count=200,
    include_current=False
):

    validate_timeframe(
        minutes,
        "upbit"
    )

    url = (
        f"https://api.upbit.com/v1/candles/"
        f"minutes/{minutes}"
    )

    params = {
        "market": market,
        "count": min(count, 200)
    }

    response = retry_request(
        "GET",
        url,
        params=params
    )

    if response is None:
        return pd.DataFrame()

    try:

        data = response.json()

        if not data:
            return pd.DataFrame()

        rows = []

        for item in data:

            rows.append({
                "timestamp": pd.to_datetime(
                    item["candle_date_time_kst"]
                ).tz_localize(KST),
                "open": float(
                    item["opening_price"]
                ),
                "high": float(
                    item["high_price"]
                ),
                "low": float(
                    item["low_price"]
                ),
                "close": float(
                    item["trade_price"]
                ),
                "volume": float(
                    item["candle_acc_trade_volume"]
                ),
                "value": float(
                    item["candle_acc_trade_price"]
                )
            })

        df = pd.DataFrame(rows)

        df = df.sort_values(
            "timestamp"
        ).reset_index(drop=True)

        if not include_current:

            current_start = (
                get_current_candle_start(
                    minutes
                )
            )

            df = df[
                df["timestamp"]
                < current_start
            ]

        return df.reset_index(drop=True)

    except Exception as e:

        logging.warning(
            f"Upbit candle error "
            f"{market} {minutes}: {e}"
        )

        return pd.DataFrame()


# =========================================================
# Upbit history
# =========================================================

def history_upbit(
    market,
    minutes,
    required=200
):

    result = pd.DataFrame()

    to = None

    for _ in range(MAX_HISTORY_CHUNKS):

        url = (
            f"https://api.upbit.com/v1/candles/"
            f"minutes/{minutes}"
        )

        params = {
            "market": market,
            "count": HISTORY_CHUNK
        }

        if to is not None:
            params["to"] = to

        response = retry_request(
            "GET",
            url,
            params=params
        )

        if response is None:
            break

        try:

            data = response.json()

            if not data:
                break

            rows = []

            for item in data:

                rows.append({
                    "timestamp": pd.to_datetime(
                        item[
                            "candle_date_time_kst"
                        ]
                    ).tz_localize(KST),

                    "open": float(
                        item["opening_price"]
                    ),

                    "high": float(
                        item["high_price"]
                    ),

                    "low": float(
                        item["low_price"]
                    ),

                    "close": float(
                        item["trade_price"]
                    ),

                    "volume": float(
                        item[
                            "candle_acc_trade_volume"
                        ]
                    ),

                    "value": float(
                        item[
                            "candle_acc_trade_price"
                        ]
                    )
                })

            chunk = pd.DataFrame(rows)

            result = pd.concat(
                [chunk, result],
                ignore_index=True
            )

            result = (
                result
                .drop_duplicates(
                    "timestamp"
                )
                .sort_values(
                    "timestamp"
                )
                .reset_index(drop=True)
            )

            if len(result) >= required:
                break

            oldest = chunk[
                "timestamp"
            ].min()

            to = (
                oldest
                .strftime(
                    "%Y-%m-%dT%H:%M:%S"
                )
            )

        except Exception as e:

            logging.warning(
                f"Upbit history error "
                f"{market}: {e}"
            )

            break

    return result.tail(
        required
    ).reset_index(drop=True)


# =========================================================
# Upbit 현재 1H ROC용
# =========================================================

def get_upbit_current_roc_data(
    market,
    minutes=60,
    required=200
):

    df = history_upbit(
        market,
        minutes,
        required
    )

    if df.empty:
        return df

    current_start = (
        get_current_candle_start(
            minutes
        )
    )

    current_price = (
        latest_upbit_markets
        .get(
            market.replace("KRW-", ""),
            {}
        )
        .get("price")
    )

    if current_price is None:
        return df

    current_price = float(
        current_price
    )

    if (
        len(df) > 0
        and df.iloc[-1]["timestamp"]
        == current_start
    ):

        df.loc[
            df.index[-1],
            "close"
        ] = current_price

    else:

        new_row = {
            "timestamp": current_start,
            "open": current_price,
            "high": current_price,
            "low": current_price,
            "close": current_price,
            "volume": 0.0,
            "value": 0.0
        }

        df = pd.concat(
            [
                df,
                pd.DataFrame([new_row])
            ],
            ignore_index=True
        )

    return df.reset_index(drop=True)


# =========================================================
# OKX candle interval
# =========================================================

def get_okx_bar(minutes):

    mapping = {
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
    }

    return mapping.get(minutes)


def get_okx_bar_minutes(minutes):

    return get_okx_bar(minutes)


# =========================================================
# OKX OHLCV
# =========================================================

def get_okx_ohlcv(
    inst_id,
    minutes,
    limit=200,
    include_current=False
):

    validate_timeframe(
        minutes,
        "okx"
    )

    bar = get_okx_bar(
        minutes
    )

    url = (
        "https://www.okx.com/api/v5/market/candles"
    )

    params = {
        "instId": inst_id,
        "bar": bar,
        "limit": min(limit, 200)
    }

    response = retry_request(
        "GET",
        url,
        params=params
    )

    if response is None:
        return pd.DataFrame()

    try:

        data = response.json()

        if data.get("code") != "0":
            return pd.DataFrame()

        rows = []

        for item in data.get(
            "data",
            []
        ):

            if len(item) < 9:
                continue

            ts = pd.to_datetime(
                int(item[0]),
                unit="ms",
                utc=True
            ).tz_convert(KST)

            confirm = str(
                item[8]
            )

            rows.append({
                "timestamp": ts,
                "open": float(item[1]),
                "high": float(item[2]),
                "low": float(item[3]),
                "close": float(item[4]),
                "volume": float(item[5]),
                "volCcyQuote": float(item[7]),
                "confirm": confirm
            })

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)

        df = df.sort_values(
            "timestamp"
        ).reset_index(drop=True)

        # 확정봉만 사용
        df = df[
            df["confirm"] == "1"
        ].copy()

        if not include_current:

            current_start = (
                get_current_candle_start(
                    minutes
                )
            )

            df = df[
                df["timestamp"]
                < current_start
            ]

        return df.reset_index(drop=True)

    except Exception as e:

        logging.warning(
            f"OKX candle error "
            f"{inst_id} {minutes}: {e}"
        )

        return pd.DataFrame()


# =========================================================
# OKX History
# =========================================================

def history_okx(
    inst_id,
    minutes,
    required=200
):

    result = pd.DataFrame()

    after = None

    for _ in range(
        MAX_HISTORY_CHUNKS
    ):

        bar = get_okx_bar(
            minutes
        )

        url = (
            "https://www.okx.com/api/v5/"
            "market/history-candles"
        )

        params = {
            "instId": inst_id,
            "bar": bar,
            "limit": HISTORY_CHUNK
        }

        if after is not None:
            params["after"] = after

        response = retry_request(
            "GET",
            url,
            params=params
        )

        if response is None:
            break

        try:

            data = response.json()

            if data.get("code") != "0":
                break

            rows = []

            for item in data.get(
                "data",
                []
            ):

                if len(item) < 9:
                    continue

                ts = pd.to_datetime(
                    int(item[0]),
                    unit="ms",
                    utc=True
                ).tz_convert(KST)

                if str(item[8]) != "1":
                    continue

                rows.append({
                    "timestamp": ts,
                    "open": float(item[1]),
                    "high": float(item[2]),
                    "low": float(item[3]),
                    "close": float(item[4]),
                    "volume": float(item[5]),
                    "volCcyQuote": float(item[7]),
                    "confirm": str(item[8])
                })

            if not rows:
                break

            chunk = pd.DataFrame(rows)

            result = pd.concat(
                [chunk, result],
                ignore_index=True
            )

            result = (
                result
                .drop_duplicates(
                    "timestamp"
                )
                .sort_values(
                    "timestamp"
                )
                .reset_index(drop=True)
            )

            if len(result) >= required:
                break

            oldest = chunk[
                "timestamp"
            ].min()

            after = str(
                int(
                    oldest.timestamp()
                    * 1000
                )
            )

        except Exception as e:

            logging.warning(
                f"OKX history error "
                f"{inst_id}: {e}"
            )

            break

    return result.tail(
        required
    ).reset_index(drop=True)


# =========================================================
# OKX 전체 ticker
# =========================================================

def get_okx_tickers():

    global okx_ticker_cache

    url = (
        "https://www.okx.com/api/v5/"
        "market/tickers"
    )

    params = {
        "instType": "SWAP"
    }

    response = retry_request(
        "GET",
        url,
        params=params
    )

    if response is None:
        return {}

    try:

        data = response.json()

        if data.get("code") != "0":
            return {}

        result = {}

        for item in data.get(
            "data",
            []
        ):

            inst_id = item.get(
                "instId",
                ""
            )

            if not inst_id.endswith(
                "-USDT-SWAP"
            ):
                continue

            result[inst_id] = float(
                item.get(
                    "last",
                    0
                ) or 0
            )

        okx_ticker_cache = result

        return result

    except Exception as e:

        logging.warning(
            f"OKX ticker error: {e}"
        )

        return {}


# =========================================================
# OKX Symbols
# =========================================================

def get_okx_symbols():

    url = (
        "https://www.okx.com/api/v5/"
        "public/instruments"
    )

    params = {
        "instType": "SWAP"
    }

    response = retry_request(
        "GET",
        url,
        params=params
    )

    if response is None:
        return []

    try:

        data = response.json()

        if data.get("code") != "0":
            return []

        result = []

        for item in data.get(
            "data",
            []
        ):

            inst_id = item.get(
                "instId",
                ""
            )

            if (
                inst_id.endswith(
                    "-USDT-SWAP"
                )
                and item.get("state")
                == "live"
            ):
                result.append(
                    inst_id
                )

        return result

    except Exception as e:

        logging.warning(
            f"OKX instruments error: {e}"
        )

        return []


# =========================================================
# OKX 1H 거래대금 캐시
# =========================================================

def get_okx_volume_cached(
    inst_id,
    usdt_krw
):

    if inst_id in okx_1h_cache:

        df = okx_1h_cache[
            inst_id
        ]

    else:

        df = get_okx_ohlcv(
            inst_id,
            60,
            count if False else 24
        )

        if df.empty:
            return 0.0

        okx_1h_cache[
            inst_id
        ] = df

    if df.empty:
        return 0.0

    quote_volume = df[
        "volCcyQuote"
    ].sum()

    return (
        quote_volume
        * usdt_krw
    )


# =========================================================
# OKX 현재 가격
# =========================================================

def get_okx_cached_price(
    inst_id
):

    return okx_ticker_cache.get(
        inst_id
    )


# =========================================================
# OKX 현재 1H ROC
#
# 핵심:
# 기존처럼 별도의 current 1H API를
# 추가 호출하지 않고,
# 200개 1H 확정봉을 한 번 가져온 뒤
# ticker 현재가를 마지막에 붙인다.
# =========================================================

def get_okx_current_1h(
    inst_id,
    required=200
):

    df = history_okx(
        inst_id,
        60,
        required
    )

    if df.empty:
        return df

    current_start = (
        get_current_candle_start(
            60
        )
    )

    current_price = (
        get_okx_cached_price(
            inst_id
        )
    )

    if current_price is None:
        return df

    current_price = float(
        current_price
    )

    if (
        len(df) > 0
        and df.iloc[-1]["timestamp"]
        == current_start
    ):

        df.loc[
            df.index[-1],
            "close"
        ] = current_price

    else:

        new_row = {
            "timestamp": current_start,
            "open": current_price,
            "high": current_price,
            "low": current_price,
            "close": current_price,
            "volume": 0.0,
            "volCcyQuote": 0.0,
            "confirm": "0"
        }

        df = pd.concat(
            [
                df,
                pd.DataFrame([new_row])
            ],
            ignore_index=True
        )

    return df.reset_index(drop=True)


# =========================================================
# EMA
# =========================================================

def ema(
    df,
    period
):

    if df.empty:
        return pd.Series(
            dtype=float
        )

    return df[
        "close"
    ].ewm(
        span=period,
        adjust=False,
        min_periods=1
    ).mean()


# =========================================================
# EMA 정배열 카운트
# =========================================================

def ema_alignment_count(
    df
):

    if df.empty:
        return 0, None

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

    latest_direction = None

    if (
        e30.iloc[-1]
        > e60.iloc[-1]
        > e120.iloc[-1]
    ):
        latest_direction = "long"

    elif (
        e30.iloc[-1]
        < e60.iloc[-1]
        < e120.iloc[-1]
    ):
        latest_direction = "short"

    else:
        return 0, None

    count = 0

    for i in range(
        len(df) - 1,
        -1,
        -1
    ):

        if (
            e30.iloc[i]
            > e60.iloc[i]
            > e120.iloc[i]
        ):
            direction = "long"

        elif (
            e30.iloc[i]
            < e60.iloc[i]
            < e120.iloc[i]
        ):
            direction = "short"

        else:
            break

        if direction != latest_direction:
            break

        count += 1

    return count, latest_direction


# =========================================================
# EMA 표시
# =========================================================

def ema_display(
    df
):

    count, direction = (
        ema_alignment_count(df)
    )

    if direction == "long":
        return f"🟢({count})"

    if direction == "short":
        return f"🔴({count})"

    return "⚪(0)"


# =========================================================
# ROC
# =========================================================

def roc(
    df,
    period=10
):

    if df.empty:
        return pd.Series(
            dtype=float
        )

    return (
        df["close"]
        / df["close"].shift(period)
        - 1
    ) * 100


# =========================================================
# ROC 진행 카운트
# =========================================================

def roc_count(
    df,
    period=10
):

    if len(df) <= period:
        return 0, None

    r = roc(
        df,
        period
    )

    latest = r.iloc[-1]

    if pd.isna(latest):
        return 0, None

    if latest > 0:
        direction = "long"

    elif latest < 0:
        direction = "short"

    else:
        return 0, None

    count = 0

    for i in range(
        len(r) - 1,
        -1,
        -1
    ):

        value = r.iloc[i]

        if pd.isna(value):
            break

        if direction == "long":
            if value <= 0:
                break

        else:
            if value >= 0:
                break

        count += 1

    return count, direction


# =========================================================
# ROC 분석
#
# 돌파:
# 이전 ROC <= 0
# 현재 ROC > 0
#
# 숏 돌파:
# 이전 ROC >= 0
# 현재 ROC < 0
#
# 즉 "양수 전환"을 롱 돌파로 판단
# =========================================================

def roc_analysis(
    df_confirmed,
    df_current
):

    result = {
        "roc": None,
        "roc_count": 0,
        "roc_direction": None,

        "long_breakout": False,
        "short_breakout": False,

        "long_pullback": False,
        "short_pullback": False,

        "long_progress": False,
        "short_progress": False,
    }

    if (
        df_confirmed.empty
        or df_current.empty
    ):
        return result

    confirmed_roc = roc(
        df_confirmed,
        ROC_PERIOD
    )

    current_roc = roc(
        df_current,
        ROC_PERIOD
    )

    if (
        len(confirmed_roc) < 1
        or len(current_roc) < 1
    ):
        return result

    previous = (
        confirmed_roc.iloc[-1]
    )

    current_value = (
        current_roc.iloc[-1]
    )

    if pd.isna(previous):
        return result

    if pd.isna(current_value):
        return result

    count, direction = (
        roc_count(
            df_current,
            ROC_PERIOD
        )
    )

    result[
        "roc"
    ] = current_value

    result[
        "roc_count"
    ] = count

    result[
        "roc_direction"
    ] = direction

    # -----------------------------------------------------
    # 롱 돌파
    # 음수/0 → 양수
    # -----------------------------------------------------

    if (
        previous <= 0
        and current_value > 0
    ):
        result[
            "long_breakout"
        ] = True

    # -----------------------------------------------------
    # 숏 돌파
    # 양수/0 → 음수
    # -----------------------------------------------------

    if (
        previous >= 0
        and current_value < 0
    ):
        result[
            "short_breakout"
        ] = True

    # -----------------------------------------------------
    # 롱 눌림
    # 양수 → 0 이하
    # -----------------------------------------------------

    if (
        previous > 0
        and current_value <= 0
    ):
        result[
            "long_pullback"
        ] = True

    # -----------------------------------------------------
    # 숏 눌림
    # 음수 → 0 이상
    # -----------------------------------------------------

    if (
        previous < 0
        and current_value >= 0
    ):
        result[
            "short_pullback"
        ] = True

    # -----------------------------------------------------
    # 진행
    # -----------------------------------------------------

    if (
        current_value > 0
        and count >= 2
    ):
        result[
            "long_progress"
        ] = True

    if (
        current_value < 0
        and count >= 2
    ):
        result[
            "short_progress"
        ] = True

    return result


# =========================================================
# 일간 등락률 - Upbit
# =========================================================

def daily_change_upbit(
    market
):

    url = (
        "https://api.upbit.com/v1/candles/days"
    )

    params = {
        "market": market,
        "count": 2
    }

    response = retry_request(
        "GET",
        url,
        params=params
    )

    if response is None:
        return None

    try:

        data = response.json()

        if len(data) < 2:
            return None

        previous = float(
            data[1]["trade_price"]
        )

        current = float(
            data[0]["trade_price"]
        )

        if previous == 0:
            return None

        return (
            current / previous
            - 1
        ) * 100

    except Exception:
        return None


# =========================================================
# 일간 등락률 - OKX
# =========================================================

def daily_changes(
    df
):

    if df.empty:
        return None

    temp = df.copy()

    temp["timestamp"] = (
        pd.to_datetime(
            temp["timestamp"]
        )
    )

    temp = (
        temp
        .set_index("timestamp")
        .resample(
            "1D",
            offset="9h"
        )["close"]
        .last()
        .dropna()
    )

    if len(temp) < 2:
        return None

    previous = temp.iloc[-2]
    current = temp.iloc[-1]

    if previous == 0:
        return None

    return (
        current / previous
        - 1
    ) * 100


# =========================================================
# 포맷
# =========================================================

def format_change(
    value
):

    if value is None:
        return "-"

    if value > 0:
        return f"+{value:.2f}%"

    return f"{value:.2f}%"


def format_volume(
    value
):

    if value is None:
        return "-"

    value = float(value)

    if value >= 100000000000:

        return (
            f"{value / 100000000000:.1f}조"
        )

    if value >= 100000000:

        return (
            f"{value / 100000000:.1f}억"
        )

    if value >= 10000:

        return (
            f"{value / 10000:.0f}만원"
        )

    return f"{value:.0f}원"


# =========================================================
# 빈 분석
# =========================================================

def empty_analysis():

    return {
        "ema": "⚪(0)",
        "ema_high": "⚪(0)",

        "direction": None,
        "direction_high": None,

        "ema_count": 0,

        "roc": None,
        "roc_count": 0,
        "roc_direction": None,

        "long_breakout": False,
        "short_breakout": False,

        "long_pullback": False,
        "short_pullback": False,

        "long_progress": False,
        "short_progress": False,

        "qualified_long_breakout": False,
        "qualified_short_breakout": False,

        "qualified_long_pullback": False,
        "qualified_short_pullback": False,

        "qualified_long_progress": False,
        "qualified_short_progress": False,

        "direction_1h": None,

        "df1h": pd.DataFrame()
    }


# =========================================================
# Upbit 분석
# =========================================================

def analyze(
    market
):

    result = empty_analysis()

    # -----------------------------------------------------
    # 1H 확정봉
    # -----------------------------------------------------

    df_confirmed = history_upbit(
        market,
        EMA_TIMEFRAME,
        200
    )

    if df_confirmed.empty:
        return result

    # -----------------------------------------------------
    # 4H
    # -----------------------------------------------------

    df_high = history_upbit(
        market,
        EMA_HIGH_TIMEFRAME,
        200
    )

    # -----------------------------------------------------
    # 현재 1H
    # -----------------------------------------------------

    df_current = (
        get_upbit_current_roc_data(
            market,
            EMA_TIMEFRAME,
            200
        )
    )

    if df_current.empty:
        return result

    # -----------------------------------------------------
    # EMA
    # -----------------------------------------------------

    ema_count, direction = (
        ema_alignment_count(
            df_confirmed
        )
    )

    _, direction_high = (
        ema_alignment_count(
            df_high
        )
    )

    result[
        "ema"
    ] = ema_display(
        df_confirmed
    )

    result[
        "ema_high"
    ] = ema_display(
        df_high
    )

    result[
        "direction"
    ] = direction

    result[
        "direction_high"
    ] = direction_high

    result[
        "ema_count"
    ] = ema_count

    result[
        "direction_1h"
    ] = direction

    # -----------------------------------------------------
    # ROC
    # -----------------------------------------------------

    roc_result = roc_analysis(
        df_confirmed,
        df_current
    )

    result.update(
        roc_result
    )

    # -----------------------------------------------------
    # 정배열 조건
    # -----------------------------------------------------

    base_long = (
        direction == "long"
        and ema_count <= EMA1_MAX_COUNT
    )

    base_short = (
        direction == "short"
        and ema_count <= EMA1_MAX_COUNT
    )

    result[
        "qualified_long_breakout"
    ] = (
        base_long
        and result["long_breakout"]
    )

    result[
        "qualified_short_breakout"
    ] = (
        base_short
        and result["short_breakout"]
    )

    result[
        "qualified_long_pullback"
    ] = (
        base_long
        and result["long_pullback"]
    )

    result[
        "qualified_short_pullback"
    ] = (
        base_short
        and result["short_pullback"]
    )

    result[
        "qualified_long_progress"
    ] = (
        base_long
        and result["long_progress"]
    )

    result[
        "qualified_short_progress"
    ] = (
        base_short
        and result["short_progress"]
    )

    result[
        "df1h"
    ] = df_confirmed

    return result


# =========================================================
# OKX 분석
# =========================================================

def analyze_okx(
    inst_id
):

    result = empty_analysis()

    # -----------------------------------------------------
    # 1H 확정 데이터
    #
    # EMA 계산 정확도를 위해 200개 확보
    # -----------------------------------------------------

    df_confirmed = history_okx(
        inst_id,
        EMA_TIMEFRAME,
        200
    )

    if df_confirmed.empty:
        return result

    # -----------------------------------------------------
    # 4H
    # -----------------------------------------------------

    df_high = history_okx(
        inst_id,
        EMA_HIGH_TIMEFRAME,
        200
    )

    # -----------------------------------------------------
    # 현재 1H
    # ticker 현재가를 사용
    # -----------------------------------------------------

    current_price = (
        get_okx_cached_price(
            inst_id
        )
    )

    if current_price is None:
        return result

    df_current = (
        df_confirmed.copy()
    )

    current_start = (
        get_current_candle_start(
            EMA_TIMEFRAME
        )
    )

    if (
        len(df_current) > 0
        and df_current.iloc[-1]["timestamp"]
        == current_start
    ):

        df_current.loc[
            df_current.index[-1],
            "close"
        ] = current_price

    else:

        new_row = {
            "timestamp": current_start,
            "open": current_price,
            "high": current_price,
            "low": current_price,
            "close": current_price,
            "volume": 0.0,
            "volCcyQuote": 0.0,
            "confirm": "0"
        }

        df_current = pd.concat(
            [
                df_current,
                pd.DataFrame([new_row])
            ],
            ignore_index=True
        )

    # -----------------------------------------------------
    # EMA
    # -----------------------------------------------------

    ema_count, direction = (
        ema_alignment_count(
            df_confirmed
        )
    )

    _, direction_high = (
        ema_alignment_count(
            df_high
        )
    )

    result[
        "ema"
    ] = ema_display(
        df_confirmed
    )

    result[
        "ema_high"
    ] = ema_display(
        df_high
    )

    result[
        "direction"
    ] = direction

    result[
        "direction_high"
    ] = direction_high

    result[
        "ema_count"
    ] = ema_count

    result[
        "direction_1h"
    ] = direction

    # -----------------------------------------------------
    # ROC
    # -----------------------------------------------------

    roc_result = roc_analysis(
        df_confirmed,
        df_current
    )

    result.update(
        roc_result
    )

    # -----------------------------------------------------
    # 정배열
    # -----------------------------------------------------

    base_long = (
        direction == "long"
        and ema_count <= EMA1_MAX_COUNT
    )

    base_short = (
        direction == "short"
        and ema_count <= EMA1_MAX_COUNT
    )

    result[
        "qualified_long_breakout"
    ] = (
        base_long
        and result["long_breakout"]
    )

    result[
        "qualified_short_breakout"
    ] = (
        base_short
        and result["short_breakout"]
    )

    result[
        "qualified_long_pullback"
    ] = (
        base_long
        and result["long_pullback"]
    )

    result[
        "qualified_short_pullback"
    ] = (
        base_short
        and result["short_pullback"]
    )

    result[
        "qualified_long_progress"
    ] = (
        base_long
        and result["long_progress"]
    )

    result[
        "qualified_short_progress"
    ] = (
        base_short
        and result["short_progress"]
    )

    result[
        "df1h"
    ] = df_confirmed

    return result


# =========================================================
# Row 생성
# =========================================================

def make_row(
    rank,
    name,
    change,
    volume,
    price,
    analysis,
    exchange
):

    return {
        "rank": rank,
        "name": name,
        "change": change,
        "volume": volume,
        "price": price,

        "ema": analysis["ema"],
        "ema_high": analysis["ema_high"],

        "roc": analysis["roc"],
        "roc_count": analysis["roc_count"],

        "long_breakout":
            analysis["qualified_long_breakout"],

        "short_breakout":
            analysis["qualified_short_breakout"],

        "long_pullback":
            analysis["qualified_long_pullback"],

        "short_pullback":
            analysis["qualified_short_pullback"],

        "long_progress":
            analysis["qualified_long_progress"],

        "short_progress":
            analysis["qualified_short_progress"],

        "direction":
            analysis["direction"],

        "exchange": exchange
    }


# =========================================================
# Upbit 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update

    markets = get_upbit_markets()

    if not markets:

        latest_upbit_data = []

        return

    markets = sorted(
        markets,
        key=lambda x: x["turnover"],
        reverse=True
    )

    top_markets = markets[
        :TOP_N
    ]

    result = []

    for rank, item in enumerate(
        top_markets,
        start=1
    ):

        market = item["market"]

        try:

            analysis = analyze(
                market
            )

            daily_change = (
                daily_change_upbit(
                    market
                )
            )

            row = make_row(
                rank,
                item["coin"],
                daily_change,
                item["turnover"],
                item["price"],
                analysis,
                "Upbit"
            )

            result.append(row)

        except Exception as e:

            logging.warning(
                f"Upbit analyze error "
                f"{market}: {e}"
            )

    latest_upbit_data = result

    latest_upbit_update = (
        kst().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )

    logging.info(
        f"Upbit update complete: "
        f"{len(result)} coins"
    )


# =========================================================
# OKX 업데이트
# =========================================================

def update_okx():

    global latest_okx_data
    global latest_okx_update
    global latest_usdt_krw
    global okx_1h_cache
    global okx_1h_cache_time

    usdt_krw = get_usdt_krw()

    if usdt_krw is None:
        latest_okx_data = []
        return

    latest_usdt_krw = usdt_krw

    # -----------------------------------------------------
    # 캐시 초기화
    # -----------------------------------------------------

    okx_1h_cache = {}

    okx_1h_cache_time = (
        kst().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )

    # -----------------------------------------------------
    # 전체 ticker 1회
    # -----------------------------------------------------

    tickers = get_okx_tickers()

    if not tickers:
        latest_okx_data = []
        return

    # -----------------------------------------------------
    # 심볼 1회
    # -----------------------------------------------------

    symbols = get_okx_symbols()

    if not symbols:
        latest_okx_data = []
        return

    symbols = [
        s for s in symbols
        if s in tickers
    ]

    # -----------------------------------------------------
    # Upbit 상장 여부
    # -----------------------------------------------------

    upbit_coins = set(
        latest_upbit_markets.keys()
    )

    # -----------------------------------------------------
    # 24시간 거래대금 계산
    #
    # OKX는 Upbit처럼 KRW 24h 거래대금을
    # 직접 제공하지 않으므로
    # 1H 24개 확정봉의
    # volCcyQuote 합계를 사용
    # -----------------------------------------------------

    volume_rows = []

    for inst_id in symbols:

        try:

            volume = (
                get_okx_volume_cached(
                    inst_id,
                    usdt_krw
                )
            )

            if volume <= 0:
                continue

            coin = (
                inst_id
                .replace(
                    "-USDT-SWAP",
                    ""
                )
            )

            if coin in upbit_coins:
                display_name = (
                    f"{coin} (업비트)"
                )
            else:
                display_name = coin

            volume_rows.append({
                "inst_id": inst_id,
                "coin": coin,
                "name": display_name,
                "volume": volume,
                "price": tickers.get(
                    inst_id,
                    0
                )
            })

        except Exception as e:

            logging.warning(
                f"OKX volume error "
                f"{inst_id}: {e}"
            )

    volume_rows = sorted(
        volume_rows,
        key=lambda x: x["volume"],
        reverse=True
    )

    top_symbols = volume_rows[
        :TOP_N
    ]

    # -----------------------------------------------------
    # TOP N만 상세 분석
    # -----------------------------------------------------

    result = []

    for rank, item in enumerate(
        top_symbols,
        start=1
    ):

        inst_id = item["inst_id"]

        try:

            analysis = analyze_okx(
                inst_id
            )

            change = daily_changes(
                analysis["df1h"]
            )

            row = make_row(
                rank,
                item["name"],
                change,
                item["volume"],
                item["price"],
                analysis,
                "OKX"
            )

            result.append(row)

        except Exception as e:

            logging.warning(
                f"OKX analyze error "
                f"{inst_id}: {e}"
            )

    latest_okx_data = result

    latest_okx_update = (
        kst().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )

    logging.info(
        f"OKX update complete: "
        f"{len(result)} coins"
    )


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    if not update_lock.acquire(
        blocking=False
    ):
        logging.info(
            "Previous update still running"
        )
        return

    try:

        if USE_UPBIT == "Y":
            update_upbit()

        if USE_OKX == "Y":
            update_okx()

    except Exception as e:

        logging.exception(
            f"Dashboard update error: {e}"
        )

    finally:

        update_lock.release()


# =========================================================
# HTML 포맷
# =========================================================

def roc_html(
    value,
    count
):

    if value is None:
        return "⚪0"

    if value > 0:

        return (
            f"🟢 상승 {count}"
        )

    if value < 0:

        return (
            f"🔴 하락 {count}"
        )

    return "⚪0"


def signal_html(
    row
):

    if row["long_breakout"]:
        return "🚀 매수 돌파"

    if row["short_breakout"]:
        return "🔻 숏 돌파"

    if row["long_pullback"]:
        return "🧊 롱 눌림"

    if row["short_pullback"]:
        return "☁️ 숏 눌림"

    if row["long_progress"]:
        return "☀️ 진행"

    if row["short_progress"]:
        return "🌧️ 숏 진행"

    return "-"


def signal_class(
    row
):

    if row["long_breakout"]:
        return "long-breakout"

    if row["short_breakout"]:
        return "short-breakout"

    if row["long_pullback"]:
        return "long-pullback"

    if row["short_pullback"]:
        return "short-pullback"

    if row["long_progress"]:
        return "long-progress"

    if row["short_progress"]:
        return "short-progress"

    return ""


# =========================================================
# 테이블
# =========================================================

def rows_html(
    data
):

    if not data:
        return (
            "<tr>"
            "<td colspan='6'>데이터 없음</td>"
            "</tr>"
        )

    html = ""

    for row in data:

        cls = signal_class(
            row
        )

        html += f"""
        <tr class="{cls}">
            <td>{row['rank']}</td>

            <td class="coin">
                {row['name']}
            </td>

            <td>
                {format_volume(
                    row['volume']
                )}
            </td>

            <td>
                <div>
                    {row['ema']}
                    /
                    {row['ema_high']}
                </div>
            </td>

            <td>
                {roc_html(
                    row['roc'],
                    row['roc_count']
                )}
            </td>

            <td class="signal">
                {signal_html(row)}
            </td>
        </tr>
        """

    return html


# =========================================================
# 집중 표시
# =========================================================

def focus_rows(
    data,
    focus_type
):

    result = []

    for row in data:

        if focus_type == "long_breakout":
            if row["long_breakout"]:
                result.append(row)

        elif focus_type == "short_breakout":
            if row["short_breakout"]:
                result.append(row)

        elif focus_type == "long_pullback":
            if row["long_pullback"]:
                result.append(row)

        elif focus_type == "short_pullback":
            if row["short_pullback"]:
                result.append(row)

    return result


def focus_html(
    data,
    title,
    focus_type
):

    rows = focus_rows(
        data,
        focus_type
    )

    if not rows:
        return ""

    html = f"""
    <div class="focus-box">
        <div class="focus-title">
            {title}
        </div>
        <div class="focus-list">
    """

    for row in rows:

        html += f"""
        <div class="focus-item">
            <span class="focus-coin">
                {row['name']}
            </span>

            <span>
                {row['ema']}
            </span>

            <span>
                {roc_html(
                    row['roc'],
                    row['roc_count']
                )}
            </span>

            <span>
                {signal_html(row)}
            </span>
        </div>
        """

    html += """
        </div>
    </div>
    """

    return html


# =========================================================
# BTC 시장 요약
# =========================================================

def get_market_row():

    if not latest_upbit_markets:
        return None

    btc = latest_upbit_markets.get(
        "BTC"
    )

    if not btc:
        return None

    analysis = analyze(
        "KRW-BTC"
    )

    daily_change = (
        daily_change_upbit(
            "KRW-BTC"
        )
    )

    return {
        "price": btc["price"],
        "change": daily_change,
        "analysis": analysis
    }


def market_direction_html(
    analysis
):

    direction_1h = (
        analysis["direction"]
    )

    direction_4h = (
        analysis["direction_high"]
    )

    if (
        direction_1h == "long"
        and direction_4h == "long"
    ):
        return "🟢 1H·4H 롱 정배열"

    if (
        direction_1h == "short"
        and direction_4h == "short"
    ):
        return "🔴 1H·4H 숏 정배열"

    return "⚪ 1H·4H 방향 불일치"


def market_roc_html(
    analysis
):

    if analysis[
        "long_breakout"
    ]:
        return "🚀 ROC 롱 돌파"

    if analysis[
        "short_breakout"
    ]:
        return "🔻 ROC 숏 돌파"

    if analysis[
        "long_pullback"
    ]:
        return "🧊 ROC 롱 눌림"

    if analysis[
        "short_pullback"
    ]:
        return "☁️ ROC 숏 눌림"

    value = analysis[
        "roc"
    ]

    if value is None:
        return "⚪ ROC -"

    if value > 0:
        return "🟢 ROC 양수"

    if value < 0:
        return "🔴 ROC 음수"

    return "⚪ ROC 0"


def format_market_price(
    price
):

    if price is None:
        return "-"

    if price >= 100000000:
        return f"{price:,.0f}"

    if price >= 10000:
        return f"{price:,.0f}"

    if price >= 1:
        return f"{price:,.2f}"

    return f"{price:.8f}"


def market_change_html(
    change
):

    if change is None:
        return "-"

    if change > 0:
        return f"🟢 +{change:.2f}%"

    if change < 0:
        return f"🔴 {change:.2f}%"

    return "⚪ 0.00%"


def btc_position_view(
    analysis
):

    d1 = analysis[
        "direction"
    ]

    d4 = analysis[
        "direction_high"
    ]

    if (
        d1 == "long"
        and d4 == "long"
    ):

        if analysis[
            "long_breakout"
        ]:
            return "🚀 롱 돌파"

        if analysis[
            "long_pullback"
        ]:
            return "🧊 롱 눌림"

        if analysis[
            "roc"
        ] is not None and analysis[
            "roc"
        ] > 0:
            return "🟢 롱 우세"

        return "⚪ 롱 대기"

    if (
        d1 == "short"
        and d4 == "short"
    ):

        if analysis[
            "short_breakout"
        ]:
            return "🔻 숏 돌파"

        if analysis[
            "short_pullback"
        ]:
            return "☁️ 숏 눌림"

        if analysis[
            "roc"
        ] is not None and analysis[
            "roc"
        ] < 0:
            return "🔴 숏 우세"

        return "⚪ 숏 대기"

    return "⚪ 관망"


def market_summary_html():

    row = get_market_row()

    if row is None:
        return ""

    analysis = row[
        "analysis"
    ]

    return f"""
    <div class="market-box">

        <div class="market-title">
            ₿ BTC 시장 방향
        </div>

        <div class="market-price">
            {format_market_price(
                row["price"]
            )}원
        </div>

        <div class="market-grid">

            <div>
                <span class="label">
                    일간
                </span>

                <span>
                    {market_change_html(
                        row["change"]
                    )}
                </span>
            </div>

            <div>
                <span class="label">
                    EMA
                </span>

                <span>
                    {market_direction_html(
                        analysis
                    )}
                </span>
            </div>

            <div>
                <span class="label">
                    ROC10
                </span>

                <span>
                    {market_roc_html(
                        analysis
                    )}
                </span>
            </div>

            <div>
                <span class="label">
                    포지션
                </span>

                <span>
                    {btc_position_view(
                        analysis
                    )}
                </span>
            </div>

        </div>
    </div>
    """


# =========================================================
# HTML
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def dashboard():

    upbit_focus = ""

    if USE_UPBIT == "Y":

        upbit_focus += focus_html(
            latest_upbit_data,
            "🟢 돌파 정배열",
            "long_breakout"
        )

        upbit_focus += focus_html(
            latest_upbit_data,
            "🟡 눌림 정배열",
            "long_pullback"
        )

    okx_focus = ""

    if USE_OKX == "Y":

        okx_focus += focus_html(
            latest_okx_data,
            "🟢 OKX 롱 돌파",
            "long_breakout"
        )

        okx_focus += focus_html(
            latest_okx_data,
            "🔴 OKX 숏 돌파",
            "short_breakout"
        )

        okx_focus += focus_html(
            latest_okx_data,
            "🧊 OKX 롱 눌림",
            "long_pullback"
        )

        okx_focus += focus_html(
            latest_okx_data,
            "☁️ OKX 숏 눌림",
            "short_pullback"
        )

    upbit_table = ""

    if USE_UPBIT == "Y":

        upbit_table = f"""
        <section>

            <h2>
                📊 Upbit TOP {TOP_N}
                <small>(업비트)</small>
            </h2>

            <div class="table-wrap">

                <table>

                    <thead>
                        <tr>
                            <th>#</th>
                            <th>코인</th>
                            <th>거래대금</th>
                            <th>
                                EMA
                                <small>
                                    {format_timeframe(
                                        EMA_TIMEFRAME
                                    )}
                                    /
                                    {format_timeframe(
                                        EMA_HIGH_TIMEFRAME
                                    )}
                                </small>
                            </th>
                            <th>ROC10</th>
                            <th>신호</th>
                        </tr>
                    </thead>

                    <tbody>
                        {rows_html(
                            latest_upbit_data
                        )}
                    </tbody>

                </table>

            </div>

            <div class="update">
                업데이트:
                {latest_upbit_update}
            </div>

        </section>
        """

    okx_table = ""

    if USE_OKX == "Y":

        okx_table = f"""
        <section>

            <h2>
                📊 OKX TOP {TOP_N}
                <small>
                    24H 거래대금 환산
                </small>
            </h2>

            <div class="table-wrap">

                <table>

                    <thead>
                        <tr>
                            <th>#</th>
                            <th>코인</th>
                            <th>거래대금</th>
                            <th>
                                EMA
                                <small>
                                    {format_timeframe(
                                        EMA_TIMEFRAME
                                    )}
                                    /
                                    {format_timeframe(
                                        EMA_HIGH_TIMEFRAME
                                    )}
                                </small>
                            </th>
                            <th>ROC10</th>
                            <th>신호</th>
                        </tr>
                    </thead>

                    <tbody>
                        {rows_html(
                            latest_okx_data
                        )}
                    </tbody>

                </table>

            </div>

            <div class="update">
                업데이트:
                {latest_okx_update}
                /
                USDT:
                {format_market_price(
                    latest_usdt_krw
                )}원
            </div>

        </section>
        """

    html = f"""
    <!DOCTYPE html>

    <html lang="ko">

    <head>

        <meta charset="UTF-8">

        <meta
            name="viewport"
            content="width=device-width,
                     initial-scale=1.0,
                     maximum-scale=1.0"
        >

        <meta
            http-equiv="refresh"
            content="60"
        >

        <title>
            코인 거래대금 · EMA · ROC 스캐너
        </title>

        <style>

            * {{
                box-sizing: border-box;
            }}

            body {{
                margin: 0;
                padding: 12px;
                background: #111;
                color: #eee;
                font-family:
                    Arial,
                    "Noto Sans KR",
                    sans-serif;
                font-size: 13px;
            }}

            .container {{
                max-width: 1200px;
                margin: auto;
            }}

            header {{
                margin-bottom: 12px;
            }}

            h1 {{
                margin: 0 0 6px 0;
                font-size: 20px;
            }}

            h2 {{
                margin: 16px 0 8px 0;
                font-size: 16px;
            }}

            h2 small {{
                font-size: 11px;
                font-weight: normal;
                color: #aaa;
            }}

            .market-box {{
                border: 1px solid #333;
                border-radius: 10px;
                padding: 12px;
                margin-bottom: 14px;
                background: #181818;
            }}

            .market-title {{
                font-size: 15px;
                font-weight: bold;
                margin-bottom: 5px;
            }}

            .market-price {{
                font-size: 21px;
                font-weight: bold;
                margin-bottom: 10px;
            }}

            .market-grid {{
                display: grid;
                grid-template-columns:
                    repeat(4, 1fr);
                gap: 7px;
            }}

            .market-grid > div {{
                background: #222;
                border-radius: 7px;
                padding: 8px;
                min-height: 45px;
            }}

            .label {{
                display: block;
                color: #888;
                font-size: 10px;
                margin-bottom: 3px;
            }}

            .focus-box {{
                border: 1px solid #333;
                border-radius: 9px;
                background: #181818;
                margin-bottom: 9px;
                overflow: hidden;
            }}

            .focus-title {{
                padding: 8px 10px;
                font-weight: bold;
                background: #202020;
            }}

            .focus-list {{
                display: flex;
                flex-direction: column;
            }}

            .focus-item {{
                display: grid;
                grid-template-columns:
                    minmax(100px, 1fr)
                    70px
                    90px
                    100px;
                gap: 6px;
                padding: 8px 10px;
                border-top: 1px solid #292929;
                align-items: center;
            }}

            .focus-coin {{
                font-weight: bold;
            }}

            .table-wrap {{
                overflow-x: auto;
                border: 1px solid #333;
                border-radius: 9px;
            }}

            table {{
                width: 100%;
                border-collapse: collapse;
                min-width: 620px;
            }}

            th {{
                background: #202020;
                color: #aaa;
                font-weight: normal;
                font-size: 11px;
            }}

            th,
            td {{
                padding: 8px 6px;
                border-bottom: 1px solid #292929;
                text-align: center;
                white-space: nowrap;
            }}

            tr:last-child td {{
                border-bottom: 0;
            }}

            td.coin {{
                text-align: left;
                font-weight: bold;
            }}

            th small {{
                display: block;
                color: #666;
                font-size: 9px;
                margin-top: 2px;
            }}

            .signal {{
                font-weight: bold;
            }}

            .long-breakout {{
                background: rgba(
                    0,
                    150,
                    80,
                    0.14
                );
            }}

            .short-breakout {{
                background: rgba(
                    220,
                    60,
                    60,
                    0.14
                );
            }}

            .long-pullback {{
                background: rgba(
                    220,
                    170,
                    40,
                    0.10
                );
            }}

            .short-pullback {{
                background: rgba(
                    130,
                    100,
                    200,
                    0.10
                );
            }}

            .long-progress {{
                background: rgba(
                    0,
                    130,
                    255,
                    0.08
                );
            }}

            .short-progress {{
                background: rgba(
                    255,
                    100,
                    100,
                    0.08
                );
            }}

            .update {{
                color: #777;
                font-size: 10px;
                text-align: right;
                margin-top: 5px;
            }}

            section {{
                margin-bottom: 20px;
            }}

            @media (
                max-width: 600px
            ) {{

                body {{
                    padding: 7px;
                    font-size: 12px;
                }}

                h1 {{
                    font-size: 17px;
                }}

                h2 {{
                    font-size: 14px;
                }}

                .market-grid {{
                    grid-template-columns:
                        repeat(2, 1fr);
                }}

                .market-price {{
                    font-size: 18px;
                }}

                .focus-item {{
                    grid-template-columns:
                        1fr
                        55px
                        80px
                        80px;
                    font-size: 11px;
                }}

                th,
                td {{
                    padding: 7px 5px;
                }}

            }}

        </style>

    </head>

    <body>

        <div class="container">

            <header>

                <h1>
                    📡 코인 거래대금 · EMA · ROC 스캐너
                </h1>

                <div class="update">
                    1분 자동 업데이트 /
                    EMA {format_timeframe(
                        EMA_TIMEFRAME
                    )} /
                    상위 EMA
                    {format_timeframe(
                        EMA_HIGH_TIMEFRAME
                    )} /
                    ROC{ROC_PERIOD}
                </div>

            </header>

            {market_summary_html()}

            {upbit_focus}

            {okx_focus}

            {upbit_table}

            {okx_table}

        </div>

    </body>

    </html>
    """

    return HTMLResponse(
        content=html
    )


# =========================================================
# 스케줄러
# =========================================================

def scheduler_loop():

    schedule.every(
        UPDATE_MINUTES
    ).minutes.do(
        update_dashboard
    )

    while True:

        try:

            schedule.run_pending()

        except Exception as e:

            logging.error(
                f"Scheduler error: {e}"
            )

        time.sleep(1)


# =========================================================
# 시작
# =========================================================

if __name__ == "__main__":

    validate_timeframe(
        EMA_TIMEFRAME,
        "upbit"
    )

    validate_timeframe(
        EMA_HIGH_TIMEFRAME,
        "upbit"
    )

    if USE_OKX == "Y":

        validate_timeframe(
            EMA_TIMEFRAME,
            "okx"
        )

        validate_timeframe(
            EMA_HIGH_TIMEFRAME,
            "okx"
        )

    logging.info(
        "=========================================="
    )

    logging.info(
        "코인 스캐너 시작"
    )

    logging.info(
        f"Upbit: {USE_UPBIT}"
    )

    logging.info(
        f"OKX: {USE_OKX}"
    )

    logging.info(
        f"TOP_N: {TOP_N}"
    )

    logging.info(
        f"EMA: {EMA_TIMEFRAME}분봉"
    )

    logging.info(
        f"상위 EMA: {EMA_HIGH_TIMEFRAME}분봉"
    )

    logging.info(
        f"EMA: "
        f"{EMA1_FAST}/"
        f"{EMA1_MID}/"
        f"{EMA1_SLOW}"
    )

    logging.info(
        f"ROC: {ROC_PERIOD}"
    )

    logging.info(
        "ROC 0선 돌파 기능 복원"
    )

    logging.info(
        "롱 돌파: 이전 ROC <= 0 "
        "→ 현재 ROC > 0"
    )

    logging.info(
        "숏 돌파: 이전 ROC >= 0 "
        "→ 현재 ROC < 0"
    )

    logging.info(
        "EMA 정배열 + ROC 돌파 조건 사용"
    )

    logging.info(
        "OKX: 전체 ticker 1회 조회"
    )

    logging.info(
        "OKX: 1H 24시간 거래대금 캐시"
    )

    logging.info(
        "=========================================="
    )

    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    threading.Thread(
        target=scheduler_loop,
        daemon=True
    ).start()

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )
