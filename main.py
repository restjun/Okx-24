from fastapi import FastAPI
from fastapi.responses import HTMLResponse

import requests
import threading
import time
import logging
import schedule
import uvicorn
import pandas as pd
import warnings
import html
import numpy as np

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


# =========================================================
# 기본 설정
# =========================================================

warnings.filterwarnings("ignore", category=FutureWarning)

app = FastAPI()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s"
)

log = logging.getLogger("trading")

KST = ZoneInfo("Asia/Seoul")
UTC = ZoneInfo("UTC")


# =========================================================
# 사용자 설정
# =========================================================

TOP_N = 20
SHOW_TOP_LIST = "Y"

UPDATE_MINUTES = 1

HISTORY_CHUNK = 200
MAX_HISTORY_CHUNKS = 10

EMA_HISTORY_CANDLES = 1000

USE_UPBIT = "Y"
USE_OKX = "Y"

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10


# =========================================================
# SIGNAL 설정
# =========================================================

# 선택 가능: "15m", "1h", "4h", "1d"
SIGNAL_TIMEFRAME = "15m"

EMA_FAST = 20
EMA_SLOW = 60

TIMEFRAME_LABEL = {
    "15m": "15분봉",
    "1h": "1시간봉",
    "4h": "4시간봉",
    "1d": "일봉"
}

EMA_REFERENCE_TIMEFRAMES = [
    "15m",
    "1h",
    "4h",
    "1d"
]


# =========================================================
# 전역 데이터
# =========================================================

latest_upbit_data = []
latest_upbit_daily_data = []
latest_upbit_update_time = "-"

latest_signal_data = []

latest_okx_data = []
latest_okx_update_time = "-"

latest_btc_okx_price = None
latest_btc_daily_change = None

request_lock = threading.Lock()
update_lock = threading.Lock()

last_request_time = 0


# =========================================================
# 시간
# =========================================================

def kst():
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


def timeframe_label():
    return TIMEFRAME_LABEL.get(
        SIGNAL_TIMEFRAME,
        SIGNAL_TIMEFRAME
    )


# =========================================================
# API 요청
# =========================================================

def wait_request():
    global last_request_time

    with request_lock:
        elapsed = time.monotonic() - last_request_time

        if elapsed < REQUEST_INTERVAL:
            time.sleep(REQUEST_INTERVAL - elapsed)

        last_request_time = time.monotonic()


def retry(func, *args, **kwargs):
    for n in range(MAX_RETRIES):
        try:
            wait_request()

            response = func(*args, **kwargs)

            if not hasattr(response, "status_code"):
                return response

            if response.status_code == 200:
                return response

            if response.status_code == 429:
                time.sleep(
                    min(RATE_LIMIT_WAIT * (n + 1), 60)
                )

            elif response.status_code >= 500:
                time.sleep(min(2 * (n + 1), 30))

            else:
                log.warning(
                    "API 응답 오류: %s",
                    response.status_code
                )
                return None

        except Exception as e:
            log.warning(
                "API 요청 오류 %s/%s: %s",
                n + 1,
                MAX_RETRIES,
                e
            )

            if n < MAX_RETRIES - 1:
                time.sleep(min(2 * (n + 1), 20))

    return None


# =========================================================
# 업비트 마켓 조회
# =========================================================

def get_upbit_markets():
    url = "https://api.upbit.com/v1/market/all"

    response = retry(
        requests.get,
        url,
        params={"isDetails": "false"},
        timeout=15
    )

    if response is None:
        return []

    try:
        markets = [
            item["market"]
            for item in response.json()
            if item.get("market", "").startswith("KRW-")
        ]
    except Exception:
        return []

    result = []

    for start in range(0, len(markets), 100):
        batch = markets[start:start + 100]

        response = retry(
            requests.get,
            "https://api.upbit.com/v1/ticker",
            params={"markets": ",".join(batch)},
            timeout=15
        )

        if response is None:
            continue

        try:
            data = response.json()
        except Exception:
            continue

        if not isinstance(data, list):
            continue

        for item in data:
            try:
                result.append({
                    "market": item["market"],
                    "current_price": float(item["trade_price"]),
                    "volume_24h": float(
                        item["acc_trade_price_24h"]
                    ),
                    "signed_change_rate": float(
                        item.get("signed_change_rate", 0)
                    ) * 100
                })
            except Exception:
                pass

    return result


# =========================================================
# 업비트 분봉 조회
# =========================================================

def get_upbit_minute_candles(market, unit, count=200):
    endpoint = (
        f"https://api.upbit.com/v1/candles/minutes/{unit}"
    )

    target_count = min(
        max(int(count), 1),
        HISTORY_CHUNK * MAX_HISTORY_CHUNKS
    )

    rows = []
    to_value = None

    while len(rows) < target_count:
        request_count = min(
            HISTORY_CHUNK,
            target_count - len(rows)
        )

        params = {
            "market": market,
            "count": request_count
        }

        if to_value is not None:
            params["to"] = to_value

        response = retry(
            requests.get,
            endpoint,
            params=params,
            timeout=15
        )

        if response is None:
            break

        try:
            data = response.json()
        except Exception:
            break

        if not isinstance(data, list) or not data:
            break

        batch_rows = []

        for item in data:
            try:
                dt = datetime.strptime(
                    item["candle_date_time_kst"],
                    "%Y-%m-%dT%H:%M:%S"
                ).replace(tzinfo=KST)

                batch_rows.append({
                    "datetime": dt,
                    "open": float(item["opening_price"]),
                    "high": float(item["high_price"]),
                    "low": float(item["low_price"]),
                    "close": float(item["trade_price"])
                })

            except Exception:
                pass

        if not batch_rows:
            break

        rows.extend(batch_rows)

        oldest = min(
            batch_rows,
            key=lambda x: x["datetime"]
        )

        next_to = oldest["datetime"].astimezone(
            UTC
        ).strftime("%Y-%m-%dT%H:%M:%S")

        if next_to == to_value:
            break

        to_value = next_to

        if len(data) < request_count:
            break

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    return (
        df.sort_values("datetime")
        .drop_duplicates("datetime")
        .reset_index(drop=True)
        .tail(target_count)
        .reset_index(drop=True)
    )


# =========================================================
# 업비트 일봉 조회
# =========================================================

def get_upbit_daily_candles(market, count=200):
    response = retry(
        requests.get,
        "https://api.upbit.com/v1/candles/days",
        params={
            "market": market,
            "count": min(int(count), 200)
        },
        timeout=15
    )

    if response is None:
        return pd.DataFrame()

    try:
        data = response.json()
    except Exception:
        return pd.DataFrame()

    if not isinstance(data, list):
        return pd.DataFrame()

    rows = []

    for item in data:
        try:
            dt = datetime.strptime(
                item["candle_date_time_kst"],
                "%Y-%m-%dT%H:%M:%S"
            ).replace(tzinfo=KST)

            rows.append({
                "datetime": dt,
                "open": float(item["opening_price"]),
                "high": float(item["high_price"]),
                "low": float(item["low_price"]),
                "close": float(item["trade_price"])
            })

        except Exception:
            pass

    if not rows:
        return pd.DataFrame()

    return (
        pd.DataFrame(rows)
        .sort_values("datetime")
        .drop_duplicates("datetime")
        .reset_index(drop=True)
    )


# =========================================================
# 현재 캔들 가격 반영
# =========================================================

def update_current_candle(
    df,
    current_start,
    price
):
    if df is None or df.empty or price is None:
        return df

    idx = df.index[-1]

    if df.loc[idx, "datetime"] != current_start:
        return df

    price = float(price)

    df.loc[idx, "close"] = price
    df.loc[idx, "high"] = max(
        float(df.loc[idx, "high"]),
        price
    )
    df.loc[idx, "low"] = min(
        float(df.loc[idx, "low"]),
        price
    )

    return df


# =========================================================
# EMA 계산
# =========================================================

def calculate_ema_series(df, period):
    if df is None or df.empty:
        return None

    if "close" not in df.columns:
        return None

    closes = pd.to_numeric(
        df["close"],
        errors="coerce"
    ).dropna()

    if closes.empty:
        return None

    values = closes.to_numpy(dtype=float)
    result = np.full(len(values), np.nan)

    alpha = 2.0 / (period + 1.0)

    if len(values) < period:
        result[0] = values[0]

        for i in range(1, len(values)):
            result[i] = (
                alpha * values[i]
                + (1.0 - alpha) * result[i - 1]
            )

    else:
        result[period - 1] = float(
            np.mean(values[:period])
        )

        for i in range(period, len(values)):
            result[i] = (
                alpha * values[i]
                + (1.0 - alpha) * result[i - 1]
            )

    return pd.Series(
        result,
        index=closes.index,
        dtype=float
    )


def calculate_ema(df, period):
    series = calculate_ema_series(df, period)

    if series is None or series.empty:
        return None

    valid = series.dropna()

    if valid.empty:
        return None

    return float(valid.iloc[-1])


def get_ema_alignment_state(df):
    fast = calculate_ema(df, EMA_FAST)
    slow = calculate_ema(df, EMA_SLOW)

    if fast is None or slow is None:
        return None

    if fast > slow:
        return "정배열"

    if fast < slow:
        return "역배열"

    return "동일"


# =========================================================
# 시간봉 데이터 조회
# =========================================================

def get_timeframe_candles(market, timeframe, price=None):
    now = datetime.now(KST)

    if timeframe == "15m":
        df = get_upbit_minute_candles(
            market,
            15,
            EMA_HISTORY_CANDLES
        )

        if df.empty:
            return df

        current_start = now.replace(
            minute=(now.minute // 15) * 15,
