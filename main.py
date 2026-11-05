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

EMA_HISTORY_CANDLES = 200

USE_UPBIT = "Y"
USE_OKX = "Y"

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10

CANDLE_PATTERN_TIMEFRAME = "1h"


# =========================================================
# 시그널 1 설정 - 15분봉
# =========================================================

SIGNAL_1_TIMEFRAME = "15m"

SIGNAL_1_EMA_FAST = 10
SIGNAL_1_EMA_SLOW = 20

SIGNAL_1_EMA_LONG_FAST = 100
SIGNAL_1_EMA_LONG_SLOW = 200

SIGNAL_1_10_20 = "Y"
SIGNAL_1_100_200 = "Y"


# =========================================================
# 시그널 2 설정 - 1시간봉
# =========================================================

SIGNAL_2_TIMEFRAME = "1h"

SIGNAL_2_EMA_FAST = 10
SIGNAL_2_EMA_SLOW = 20

SIGNAL_2_EMA_LONG_FAST = 100
SIGNAL_2_EMA_LONG_SLOW = 200

SIGNAL_2_10_20 = "Y"
SIGNAL_2_100_200 = "Y"


# =========================================================
# 시그널 3 설정 - 일봉
# =========================================================

SIGNAL_3_TIMEFRAME = "1d"

SIGNAL_3_EMA_FAST = 10
SIGNAL_3_EMA_SLOW = 20

SIGNAL_3_EMA_LONG_FAST = 100
SIGNAL_3_EMA_LONG_SLOW = 200

SIGNAL_3_10_20 = "Y"
SIGNAL_3_100_200 = "Y"


# =========================================================
# 시간봉 라벨
# =========================================================

TIMEFRAME_LABEL = {
    "5m": "5분봉",
    "15m": "15분봉",
    "1h": "1시간봉",
    "4h": "4시간봉",
    "1d": "일봉"
}


# =========================================================
# 전역 데이터
# =========================================================

latest_upbit_data = []
latest_upbit_daily_data = []

latest_upbit_update_time = "-"
latest_upbit_daily_update_time = "-"

latest_upbit_markets = []

latest_okx_data = []
latest_okx_update_time = "-"

latest_signal_data = []

latest_btc_okx_price = None
latest_btc_daily_periods = []
latest_btc_daily_change = None

latest_btc_signal_1_states = {
    "10_20": None,
    "100_200": None
}

latest_btc_signal_2_states = {
    "10_20": None,
    "100_200": None
}

latest_btc_signal_3_states = {
    "10_20": None,
    "100_200": None
}


# =========================================================
# Lock 및 캐시
# =========================================================

request_lock = threading.Lock()
update_lock = threading.Lock()
candle_cache_lock = threading.Lock()

last_request_time = 0

candle_cache = {}


def clear_candle_cache():
    with candle_cache_lock:
        candle_cache.clear()


def get_cached_dataframe(market, timeframe, loader):
    key = (market, timeframe)

    with candle_cache_lock:
        cached = candle_cache.get(key)

        if cached is not None:
            return cached.copy(deep=True)

    df = loader()

    if df is None:
        df = pd.DataFrame()

    with candle_cache_lock:
        if not df.empty:
            existing = candle_cache.get(key)

            if existing is not None:
                return existing.copy(deep=True)

            candle_cache[key] = df.copy(deep=True)

    return df.copy(deep=True)


# =========================================================
# 시간 및 라벨
# =========================================================

def kst():
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


def get_timeframe_label(timeframe):
    return TIMEFRAME_LABEL.get(timeframe, timeframe)


def get_signal_config(signal_number):
    if signal_number == 1:
        return {
            "name": "시그널 1",
            "timeframe": SIGNAL_1_TIMEFRAME,
            "fast": SIGNAL_1_EMA_FAST,
            "slow": SIGNAL_1_EMA_SLOW,
            "long_fast": SIGNAL_1_EMA_LONG_FAST,
            "long_slow": SIGNAL_1_EMA_LONG_SLOW,
            "condition_1": SIGNAL_1_10_20,
            "condition_2": SIGNAL_1_100_200
        }

    if signal_number == 2:
        return {
            "name": "시그널 2",
            "timeframe": SIGNAL_2_TIMEFRAME,
            "fast": SIGNAL_2_EMA_FAST,
            "slow": SIGNAL_2_EMA_SLOW,
            "long_fast": SIGNAL_2_EMA_LONG_FAST,
            "long_slow": SIGNAL_2_EMA_LONG_SLOW,
            "condition_1": SIGNAL_2_10_20,
            "condition_2": SIGNAL_2_100_200
        }

    return {
        "name": "시그널 3",
        "timeframe": SIGNAL_3_TIMEFRAME,
        "fast": SIGNAL_3_EMA_FAST,
        "slow": SIGNAL_3_EMA_SLOW,
        "long_fast": SIGNAL_3_EMA_LONG_FAST,
        "long_slow": SIGNAL_3_EMA_LONG_SLOW,
        "condition_1": SIGNAL_3_10_20,
        "condition_2": SIGNAL_3_100_200
    }


def get_ema_pair_labels(signal_number):
    config = get_signal_config(signal_number)

    return (
        f'{config["fast"]}-{config["slow"]}',
        f'{config["long_fast"]}-{config["long_slow"]}'
    )


def get_signal_description(signal_number):
    config = get_signal_config(signal_number)

    state_1 = {
        "Y": "정배열",
        "N": "역배열",
        "ANY": "ANY"
    }.get(config["condition_1"], "ANY")

    state_2 = {
        "Y": "정배열",
        "N": "역배열",
        "ANY": "ANY"
    }.get(config["condition_2"], "ANY")

    pair_1, pair_2 = get_ema_pair_labels(signal_number)

    return (
        f'EMA {pair_1} {state_1} + '
        f'EMA {pair_2} {state_2}'
    )


# =========================================================
# API 요청
# =========================================================

def wait_request():
    global last_request_time

    with request_lock:
        gap = time.monotonic() - last_request_time

        if gap < REQUEST_INTERVAL:
            time.sleep(REQUEST_INTERVAL - gap)

        last_request_time = time.monotonic()


def retry(func, *args, **kwargs):
    for n in range(MAX_RETRIES):
        try:
            wait_request()
            r = func(*args, **kwargs)

            if not hasattr(r, "status_code") or r.status_code == 200:
                return r

            if r.status_code == 429:
                time.sleep(min(RATE_LIMIT_WAIT * (n + 1), 60))

            elif r.status_code >= 500:
                time.sleep(min(2 * (n + 1), 30))

            else:
                return r

        except Exception as e:
            log.warning(
                "API 오류 %s/%s: %s",
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
    global latest_upbit_markets

    r = retry(
        requests.get,
        "https://api.upbit.com/v1/market/all",
        params={"isDetails": "false"},
        timeout=15
    )

    if r is None:
        return []

    try:
        markets = [
            x["market"]
            for x in r.json()
            if x.get("market", "").startswith("KRW-")
        ]
    except Exception:
        return []

    result = []

    for i in range(0, len(markets), 100):
        rr = retry(
            requests.get,
            "https://api.upbit.com/v1/ticker",
            params={"markets": ",".join(markets[i:i + 100])},
            timeout=15
        )

        if rr is None:
            continue

        try:
            data = rr.json()
        except Exception:
            continue

        if not isinstance(data, list):
            continue

        for x in data:
            try:
                result.append({
                    "market": x["market"],
                    "current_price": float(x["trade_price"]),
                    "volume_24h": float(x["acc_trade_price_24h"])
                })
            except Exception:
                pass

    latest_upbit_markets = [
        x["market"] for x in result
    ]

    return result


# =========================================================
# 업비트 분봉 조회
# =========================================================

def get_upbit_minute_candles(market, unit, count=200):
    endpoint = (
        f"https://api.upbit.com/v1/candles/minutes/{unit}"
    )

    target_count = min(max(int(count), 1), 200)

    r = retry(
        requests.get,
        endpoint,
        params={
            "market": market,
            "count": target_count
        },
        timeout=15
    )

    if r is None:
        return pd.DataFrame()

    try:
        data = r.json()
    except Exception:
        return pd.DataFrame()

    if not isinstance(data, list) or not data:
        return pd.DataFrame()

    rows = []

    for x in data:
        try:
            dt_utc = datetime.strptime(
                x["candle_date_time_utc"],
                "%Y-%m-%dT%H:%M:%S"
            ).replace(tzinfo=UTC)

            dt_kst = datetime.strptime(
                x["candle_date_time_kst"],
                "%Y-%m-%dT%H:%M:%S"
            ).replace(tzinfo=KST)

            rows.append({
                "datetime": dt_kst,
                "datetime_utc": dt_utc,
                "open": float(x["opening_price"]),
                "high": float(x["high_price"]),
                "low": float(x["low_price"]),
                "close": float(x["trade_price"])
            })

        except Exception:
            pass

    if not rows:
        return pd.DataFrame()

    return (
        pd.DataFrame(rows)
        .sort_values("datetime")
        .drop_duplicates("datetime")
        .tail(target_count)
        .reset_index(drop=True)
    )


def get_upbit_5m_candles(market, count=200):
    return get_upbit_minute_candles(market, 5, count)


def get_upbit_15m_candles(market, count=200):
    return get_upbit_minute_candles(market, 15, count)


def get_upbit_1h_candles(market, count=200):
    return get_upbit_minute_candles(market, 60, count)


# =========================================================
# 업비트 일봉 조회
# =========================================================

def get_upbit_daily_candles(market, count=200):
    r = retry(
        requests.get,
        "https://api.upbit.com/v1/candles/days",
        params={
            "market": market,
            "count": min(int(count), 200)
        },
        timeout=15
    )

    if r is None:
        return pd.DataFrame()

    try:
        data = r.json()
    except Exception:
        return pd.DataFrame()

    if not isinstance(data, list):
        return pd.DataFrame()

    rows = []

    for x in data:
        try:
            rows.append({
                "datetime": datetime.strptime(
                    x["candle_date_time_kst"],
                    "%Y-%m-%dT%H:%M:%S"
                ).replace(tzinfo=KST),
                "open": float(x["opening_price"]),
                "high": float(x["high_price"]),
                "low": float(x["low_price"]),
                "close": float(x["trade_price"])
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

def update_current_candle(df, current_start, price):
    if df is None or df.empty or price is None:
        return df

    if df.iloc[-1]["datetime"] != current_start:
        return df

    idx = df.index[-1]
    price = float(price)

    df.loc[idx, "close"] = price
    df.loc[idx, "high"] = max(
        float(df.loc[idx, "high"]), price
    )
    df.loc[idx, "low"] = min(
        float(df.loc[idx, "low"]), price
    )

    return df


# =========================================================
# 시간봉 데이터 조회
# =========================================================

def get_timeframe_dataframe(market, timeframe, price=None):
    if timeframe == "5m":
        df = get_cached_dataframe(
            market,
            "5m",
            lambda: get_upbit_5m_candles(
                market, EMA_HISTORY_CANDLES
            )
        )
        minutes = 5

    elif timeframe == "15m":
        df = get_cached_dataframe(
            market,
            "15m",
            lambda: get_upbit_15m_candles(
                market, EMA_HISTORY_CANDLES
            )
        )
        minutes = 15

    elif timeframe == "1h":
        df = get_cached_dataframe(
            market,
            "1h",
            lambda: get_upbit_1h_candles(
                market, EMA_HISTORY_CANDLES
            )
        )
        minutes = 60

    else:
        return pd.DataFrame()

    if df.empty:
        return df

    now = datetime.now(KST)

    current_start = now.replace(
        minute=(now.minute // minutes) * minutes,
        second=0,
        microsecond=0
    )

    return update_current_candle(
        df, current_start, price
    )


def get_daily_dataframe(market):
    return get_cached_dataframe(
        market,
        "1d",
        lambda: get_upbit_daily_candles(market, 200)
    )


def get_signal_dataframe(market, timeframe, price=None):
    if timeframe == "1d":
        df = get_daily_dataframe(market)

        if df.empty:
            return df

        now = datetime.now(KST)

        current_start = now.replace(
            hour=9,
            minute=0,
            second=0,
            microsecond=0
        )

        if now < current_start:
            current_start -= timedelta(days=1)

        return update_current_candle(
            df, current_start, price
        )

    if timeframe in ("5m", "15m", "1h"):
        return get_timeframe_dataframe(
            market, timeframe, price
        )

    return pd.DataFrame()


# =========================================================
# EMA 계산
# =========================================================

def calculate_ema_series(df, period):
    if (
        df is None
        or df.empty
        or "close" not in df.columns
        or period < 1
    ):
        return None

    closes = pd.to_numeric(
        df["close"], errors="coerce"
    )

    closes = closes.loc[closes.notna()]

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

    return (
        float(valid.iloc[-1])
        if not valid.empty
        else None
    )


# =========================================================
# EMA 배열 상태
# =========================================================

def get_ema_alignment_state(df, fast_period, slow_period):
    fast = calculate_ema(df, fast_period)
    slow = calculate_ema(df, slow_period)

    if fast is None or slow is None:
        return None

    if fast > slow:
        return "정배열"

    if fast < slow:
        return "역배열"

    return "동일"


def check_alignment_condition(state, setting):
    if state is None:
        return False

    if setting == "Y":
        return state == "정배열"

    if setting == "N":
        return state == "역배열"

    if setting == "ANY":
        return state in ("정배열", "역배열")

    return False


# =========================================================
# EMA 10선 터치 후 반등 판정
# =========================================================

def check_ema10_rebound(df):
    if df is None or df.empty:
        return False

    required_columns = {"low", "close"}

    if not required_columns.issubset(df.columns):
        return False

    ema10_series = calculate_ema_series(df, 10)

    if ema10_series is None or ema10_series.empty:
        return False

    ema10_valid = ema10_series.dropna()

    if ema10_valid.empty:
        return False

    try:
        ema10 = float(ema10_valid.iloc[-1])
        current_index = ema10_valid.index[-1]

        current_low = float(
            df.loc[current_index, "low"]
        )

        current_close = float(
            df.loc[current_index, "close"]
        )

    except Exception:
        return False

    if not all(
        np.isfinite(v)
        for v in (ema10, current_low, current_close)
    ):
        return False

    return bool(
        current_low <= ema10
        and current_close > ema10
    )


# =========================================================
# 시그널 EMA 분석
# =========================================================

def analyze_signal_ema(
    market,
    signal_number,
    price=None
):
    config = get_signal_config(signal_number)

    df = get_signal_dataframe(
        market,
        config["timeframe"],
        price
    )

    result = {
        "name": config["name"],
        "timeframe": config["timeframe"],
        "ema_fast": None,
        "ema_slow": None,
        "ema_long_fast": None,
        "ema_long_slow": None,
        "alignment_1": None,
        "alignment_2": None,
        "condition": False
    }

    if df is None or df.empty:
        return result

    fast = calculate_ema(
        df, config["fast"]
    )

    slow = calculate_ema(
        df, config["slow"]
    )

    long_fast = calculate_ema(
        df, config["long_fast"]
    )

    long_slow = calculate_ema(
        df, config["long_slow"]
    )

    if any(
        x is None
        for x in (fast, slow, long_fast, long_slow)
    ):
        return result

    state_1 = (
        "정배열" if fast > slow
        else "역배열" if fast < slow
        else "동일"
    )

    state_2 = (
        "정배열" if long_fast > long_slow
        else "역배열" if long_fast < long_slow
        else "동일"
    )

    condition_1 = check_alignment_condition(
        state_1,
        config["condition_1"]
    )

    condition_2 = check_alignment_condition(
        state_2,
        config["condition_2"]
    )

    result.update({
        "ema_fast": fast,
        "ema_slow": slow,
        "ema_long_fast": long_fast,
        "ema_long_slow": long_slow,
        "alignment_1": state_1,
        "alignment_2": state_2,
        "condition": bool(
            condition_1 and condition_2
        )
    })

    return result


# =========================================================
# 세 가지 시그널 통합 판정
# =========================================================

def analyze_all_signals(market, price=None):
    signal_1 = analyze_signal_ema(
        market, 1, price
    )

    signal_2 = analyze_signal_ema(
        market, 2, price
    )

    signal_3 = analyze_signal_ema(
        market, 3, price
    )

    passed = bool(
        signal_1["condition"]
        and signal_2["condition"]
        and signal_3["condition"]
    )

    return {
        "signal_1": signal_1,
        "signal_2": signal_2,
        "signal_3": signal_3,
        "all_pass": passed
    }


# =========================================================
# 일봉 기간 생성
# =========================================================

def build_daily_periods(df, current_price=None):
    if df is None or df.empty:
        return []

    df = (
        df.sort_values("datetime")
        .drop_duplicates("datetime")
        .reset_index(drop=True)
    )

    now = datetime.now(KST)

    current_start = now.replace(
        hour=9,
        minute=0,
        second=0,
        microsecond=0
    )

    if now < current_start:
        current_start -= timedelta(days=1)

    periods = []

    for _, row in df.iterrows():
        start = row["datetime"]
        end = start + timedelta(days=1)

        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])

        active = start == current_start

        if active and current_price is not None:
            c = float(current_price)
            h = max(h, c)
            l = min(l, c)

        change = (c - o) / o * 100 if o else None

        periods.append({
            "start": start,
            "end": end,
            "active": active,
            "label": start.strftime("%m/%d 09:00"),
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "change": change
        })

    return periods[-6:]


# =========================================================
# 캔들 패턴 판정
# =========================================================

def is_bullish_engulfing(previous, current):
    try:
        po, pc = float(previous["open"]), float(previous["close"])
        co, cc = float(current["open"]), float(current["close"])
    except Exception:
        return False

    return bool(
        pc < po
        and cc > co
        and co <= pc
        and cc >= po
    )


def is_bearish_engulfing(previous, current):
    try:
        po, pc = float(previous["open"]), float(previous["close"])
        co, cc = float(current["open"]), float(current["close"])
    except Exception:
        return False

    return bool(
        pc > po
        and cc < co
        and co >= pc
        and cc <= po
    )


def is_positive_doji(current):
    try:
        o, h, l, c = (
            float(current["open"]),
            float(current["high"]),
            float(current["low"]),
            float(current["close"])
        )
    except Exception:
        return False

    candle_range = h - l

    return bool(
        candle_range > 0
        and abs(c - o) / candle_range <= 0.10
        and c >= o
    )


def is_negative_doji(current):
    try:
        o, h, l, c = (
            float(current["open"]),
            float(current["high"]),
            float(current["low"]),
            float(current["close"])
        )
    except Exception:
        return False

    candle_range = h - l

    return bool(
        candle_range > 0
        and abs(c - o) / candle_range <= 0.10
        and c < o
    )


def is_bullish_piercing(previous, current):
    try:
        po, pc = float(previous["open"]), float(previous["close"])
        co, cc = float(current["open"]), float(current["close"])
    except Exception:
        return False

    if pc >= po or cc <= co:
        return False

    midpoint = (po + pc) / 2

    return bool(
        cc > midpoint
        and cc < po
    )


def is_bearish_piercing(previous, current):
    try:
        po, pc = float(previous["open"]), float(previous["close"])
        co, cc = float(current["open"]), float(current["close"])
    except Exception:
        return False

    if pc <= po or cc >= co:
        return False

    midpoint = (po + pc) / 2

    return bool(
        cc < midpoint
        and cc > po
    )


def is_long_bullish(current):
    try:
        o, h, l, c = (
            float(current["open"]),
            float(current["high"]),
            float(current["low"]),
            float(current["close"])
        )
    except Exception:
        return False

    candle_range = h - l

    return bool(
        candle_range > 0
        and c > o
        and abs(c - o) / candle_range >= 0.70
    )


def is_long_bearish(current):
    try:
        o, h, l, c = (
            float(current["open"]),
            float(current["high"]),
            float(current["low"]),
            float(current["close"])
        )
    except Exception:
        return False

    candle_range = h - l

    return bool(
        candle_range > 0
        and c < o
        and abs(c - o) / candle_range >= 0.70
    )


def get_hammer_parts(current):
    try:
        o, h, l, c = (
            float(current["open"]),
            float(current["high"]),
            float(current["low"]),
            float(current["close"])
        )
    except Exception:
        return None

    if h < max(o, c) or l > min(o, c) or h < l:
        return None

    body = abs(c - o)
    candle_range = h - l

    if candle_range <= 0:
        return None

    effective_body = max(
        body,
        candle_range * 0.01
    )

    return {
        "body": effective_body,
        "upper_wick": h - max(o, c),
        "lower_wick": min(o, c) - l,
        "open": o,
        "close": c
    }


def is_bullish_hammer(previous, current):
    if previous is None:
        return False

    try:
        if float(previous["close"]) >= float(previous["open"]):
            return False
    except Exception:
        return False

    parts = get_hammer_parts(current)

    return bool(
        parts is not None
        and parts["lower_wick"] >= parts["body"] * 2
        and parts["upper_wick"] <= parts["body"]
    )


def is_bullish_inverted_hammer(previous, current):
    if previous is None:
        return False

    try:
        if float(previous["close"]) >= float(previous["open"]):
            return False
    except Exception:
        return False

    parts = get_hammer_parts(current)

    return bool(
        parts is not None
        and parts["upper_wick"] >= parts["body"] * 2
        and parts["lower_wick"] <= parts["body"]
    )


def is_bearish_hammer(previous, current):
    if previous is None:
        return False

    try:
        if float(previous["close"]) <= float(previous["open"]):
            return False
    except Exception:
        return False

    parts = get_hammer_parts(current)

    return bool(
        parts is not None
        and parts["lower_wick"] >= parts["body"] * 2
        and parts["upper_wick"] <= parts["body"]
    )


def is_bearish_inverted_hammer(previous, current):
    if previous is None:
        return False

    try:
        if float(previous["close"]) <= float(previous["open"]):
            return False
    except Exception:
        return False

    parts = get_hammer_parts(current)

    return bool(
        parts is not None
        and parts["upper_wick"] >= parts["body"] * 2
        and parts["lower_wick"] <= parts["body"]
    )


BULLISH_HIGHLIGHT_PATTERNS = [
    "상승장악형",
    "상승관통형",
    "장대양봉",
    "상승망치형",
    "상승역망치형"
]

BEARISH_HIGHLIGHT_PATTERNS = [
    "하락장악형",
    "하락관통형",
    "장대음봉",
    "음수도지",
    "하락망치형",
    "하락역망치형"
]


def get_candle_patterns(previous, current):
    if current is None:
        return []

    patterns = []

    if previous is not None:
        if is_bullish_engulfing(previous, current):
            patterns.append("상승장악형")

        if is_bearish_engulfing(previous, current):
            patterns.append("하락장악형")

        if is_bullish_piercing(previous, current):
            patterns.append("상승관통형")

        if is_bearish_piercing(previous, current):
            patterns.append("하락관통형")

        if is_bullish_hammer(previous, current):
            patterns.append("상승망치형")

        if is_bullish_inverted_hammer(previous, current):
            patterns.append("상승역망치형")

        if is_bearish_hammer(previous, current):
            patterns.append("하락망치형")

        if is_bearish_inverted_hammer(previous, current):
            patterns.append("하락역망치형")

    if is_long_bullish(current):
        patterns.append("장대양봉")

    if is_long_bearish(current):
        patterns.append("장대음봉")

    if is_positive_doji(current):
        patterns.append("양수도지")

    if is_negative_doji(current):
        patterns.append("음수도지")

    return patterns


# =========================================================
# 캔들 패턴 시간봉 분석
# =========================================================

def analyze_pattern_dataframe(df, timeframe):
    if df is None or df.empty:
        return {
            "periods": [],
            "previous_pattern": [],
            "current_pattern": [],
            "previous_bullish_pattern": False,
            "current_bullish_pattern": False,
            "ema10_rebound": False
        }

    if timeframe == "1d":
        periods = build_daily_periods(df)

    else:
        minutes = {
            "5m": 5,
            "15m": 15,
            "1h": 60
        }.get(timeframe, 60)

        periods = []

        for _, row in df.iterrows():
            start = row["datetime"]

            o = float(row["open"])
            h = float(row["high"])
            l = float(row["low"])
            c = float(row["close"])

            change = (c - o) / o * 100 if o else None

            periods.append({
                "start": start,
                "end": start + timedelta(minutes=minutes),
                "active": True,
                "label": start.strftime("%m/%d %H:%M"),
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "change": change
            })

        periods = periods[-6:]

    if not periods:
        return {
            "periods": [],
            "previous_pattern": [],
            "current_pattern": [],
            "previous_bullish_pattern": False,
            "current_bullish_pattern": False,
            "ema10_rebound": False
        }

    current = periods[-1]
    previous = periods[-2] if len(periods) >= 2 else None
    previous_previous = periods[-3] if len(periods) >= 3 else None

    current_patterns = get_candle_patterns(
        previous, current
    )

    previous_patterns = (
        get_candle_patterns(previous_previous, previous)
        if previous is not None
        else []
    )

    return {
        "periods": periods,
        "previous_pattern": previous_patterns,
        "current_pattern": current_patterns,
        "previous_bullish_pattern": any(
            p in BULLISH_HIGHLIGHT_PATTERNS
            for p in previous_patterns
        ),
        "current_bullish_pattern": any(
            p in BULLISH_HIGHLIGHT_PATTERNS
            for p in current_patterns
        ),
        "ema10_rebound": check_ema10_rebound(df)
    }


def analyze_signal_candles(market, price):
    timeframe = CANDLE_PATTERN_TIMEFRAME

    df = get_signal_dataframe(
        market,
        timeframe,
        price
    )

    if df is None or df.empty:
        return {
            "periods": [],
            "previous_pattern": [],
            "current_pattern": [],
            "previous_bullish_pattern": False,
            "current_bullish_pattern": False,
            "ema10_rebound": False,
            "signal_pass": False,
            "signal_reason": None,
            "signal_period": None,
            "signal_change": None
        }

    result = analyze_pattern_dataframe(
        df,
        timeframe
    )

    current_period = (
        result["periods"][-1]
        if result["periods"]
        else None
    )

    signal = bool(
        result["current_bullish_pattern"]
        or result["previous_bullish_pattern"]
    )

    reason = None

    for pattern in result["current_pattern"]:
        if pattern in BULLISH_HIGHLIGHT_PATTERNS:
            reason = pattern
            break

    if reason is None:
        for pattern in result["previous_pattern"]:
            if pattern in BULLISH_HIGHLIGHT_PATTERNS:
                reason = pattern
                break

    result.update({
        "signal_pass": signal,
        "signal_reason": reason,
        "signal_period": (
            current_period.get("label")
            if signal and current_period
            else None
        ),
        "signal_change": (
            current_period.get("change")
            if signal and current_period
            else None
        )
    })

    return result


# =========================================================
# 일봉 변동률
# =========================================================

def analyze_daily_change(market, price):
    df = get_daily_dataframe(market)

    if df.empty:
        return {
            "change": None,
            "periods": []
        }

    periods = build_daily_periods(
        df,
        price
    )

    if not periods:
        return {
            "change": None,
            "periods": []
        }

    return {
        "change": periods[-1].get("change"),
        "periods": periods
    }


# =========================================================
# Row 생성
# =========================================================

def make_row(
    rank,
    market,
    item,
    analysis_daily,
    analysis_pattern,
    signals,
    volume_rank
):
    coin = market.replace("KRW-", "")

    s1 = signals["signal_1"]
    s2 = signals["signal_2"]
    s3 = signals["signal_3"]

    return {
        "rank": rank,
        "name": coin,
        "market": market,
        "volume_24h": item["volume_24h"],
        "current_price": item["current_price"],
        "volume_rank": volume_rank,

        "daily_change": analysis_daily["change"],
        "periods_daily": analysis_daily["periods"],

        "periods_pattern": analysis_pattern["periods"],

        "previous_pattern": analysis_pattern["previous_pattern"],
        "current_pattern": analysis_pattern["current_pattern"],

        "previous_bullish_pattern": analysis_pattern[
            "previous_bullish_pattern"
        ],

        "current_bullish_pattern": analysis_pattern[
            "current_bullish_pattern"
        ],

        "ema10_rebound": analysis_pattern["ema10_rebound"],

        "signal_1": s1,
        "signal_2": s2,
        "signal_3": s3,

        "signal_1_pass": s1["condition"],
        "signal_2_pass": s2["condition"],
        "signal_3_pass": s3["condition"],

        "all_signal_pass": signals["all_pass"]
    }


# =========================================================
# 업비트 업데이트
# =========================================================

def update_upbit():
    global latest_upbit_data
    global latest_upbit_daily_data
    global latest_upbit_update_time
    global latest_upbit_daily_update_time
    global latest_signal_data

    clear_candle_cache()

    all_markets = get_upbit_markets()

    if not all_markets:
        return

    all_markets_sorted = sorted(
        all_markets,
        key=lambda x: x.get("volume_24h", 0),
        reverse=True
    )

    volume_rank_map = {
        item["market"]: rank
        for rank, item in enumerate(
            all_markets_sorted,
            1
        )
    }

    selected_markets = all_markets_sorted[:TOP_N]

    rows = []

    for rank, item in enumerate(selected_markets, 1):
        market = item["market"]
        price = item["current_price"]

        try:
            analysis_daily = analyze_daily_change(
                market,
                price
            )
        except Exception as e:
            log.warning(
                "%s 일봉 오류: %s",
                market,
                e
            )

            analysis_daily = {
                "change": None,
                "periods": []
            }

        try:
            signals = analyze_all_signals(
                market,
                price
            )
        except Exception as e:
            log.warning(
                "%s EMA 시그널 오류: %s",
                market,
                e
            )

            empty_signal = {
                "name": "",
                "timeframe": "",
                "ema_fast": None,
                "ema_slow": None,
                "ema_long_fast": None,
                "ema_long_slow": None,
                "alignment_1": None,
                "alignment_2": None,
                "condition": False
            }

            signals = {
                "signal_1": dict(empty_signal),
                "signal_2": dict(empty_signal),
                "signal_3": dict(empty_signal),
                "all_pass": False
            }

        try:
            analysis_pattern = analyze_signal_candles(
                market,
                price
            )

        except Exception as e:
            log.warning(
                "%s 캔들 패턴 오류: %s",
                market,
                e
            )

            analysis_pattern = {
                "periods": [],
                "previous_pattern": [],
                "current_pattern": [],
                "previous_bullish_pattern": False,
                "current_bullish_pattern": False,
                "ema10_rebound": False,
                "signal_pass": False,
                "signal_reason": None,
                "signal_period": None,
                "signal_change": None
            }

        rows.append(
            make_row(
                rank,
                market,
                item,
                analysis_daily,
                analysis_pattern,
                signals,
                volume_rank_map.get(market)
            )
        )

    latest_upbit_data = rows
    latest_upbit_daily_data = rows

    latest_upbit_daily_update_time = kst()
    latest_upbit_update_time = latest_upbit_daily_update_time

    latest_signal_data = [
        row for row in rows
        if (
            row.get("all_signal_pass", False)
            and row.get("daily_change") is not None
            and row.get("daily_change") > 0
        )
    ]

    latest_signal_data.sort(
        key=lambda x: x.get("volume_rank", 999999)
    )

    for signal_rank, row in enumerate(
        latest_signal_data,
        1
    ):
        row["signal_rank"] = signal_rank

    log.info(
        "UPBIT | 거래대금 TOP%s | "
        "시그널1=%s | 시그널2=%s | 시그널3=%s | "
        "SIGNAL=%s",
        TOP_N,
        get_signal_description(1),
        get_signal_description(2),
        get_signal_description(3),
        len(latest_signal_data)
    )


# =========================================================
# OKX 캔들
# =========================================================

def okx_candles(bar, limit=200):
    r = retry(
        requests.get,
        "https://www.okx.com/api/v5/market/candles",
        params={
            "instId": "BTC-USDT-SWAP",
            "bar": bar,
            "limit": str(min(limit, 300))
        },
        timeout=15
    )

    if r is None:
        return pd.DataFrame()

    try:
        response_data = r.json()
        data = response_data.get("data", [])

        if response_data.get("code") != "0":
            log.warning(
                "OKX 캔들 API 오류: %s",
                response_data.get("msg", "unknown")
            )
            return pd.DataFrame()

    except Exception:
        return pd.DataFrame()

    rows = []

    for x in data:
        try:
            rows.append({
                "datetime": pd.to_datetime(
                    int(x[0]),
                    unit="ms",
                    utc=True
                ).tz_convert(KST),
                "open": float(x[1]),
                "high": float(x[2]),
                "low": float(x[3]),
                "close": float(x[4])
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
# OKX BTC 가격
# =========================================================

def okx_price():
    r = retry(
        requests.get,
        "https://www.okx.com/api/v5/market/ticker",
        params={
            "instId": "BTC-USDT-SWAP"
        },
        timeout=15
    )

    if r is None:
        return None

    try:
        response_data = r.json()

        if response_data.get("code") != "0":
            return None

        return float(
            response_data["data"][0]["last"]
        )

    except Exception:
        return None


# =========================================================
# BTC EMA 상태 계산
# =========================================================

def analyze_btc_ema_states(signal_number):
    config = get_signal_config(signal_number)

    states = {
        "10_20": None,
        "100_200": None
    }

    timeframe_bars = {
        "5m": "5m",
        "15m": "15m",
        "1h": "1H",
        "4h": "4H",
        "1d": "1D"
    }

    bar = timeframe_bars.get(
        config["timeframe"]
    )

    if bar is None:
        return states

    df = okx_candles(bar, 200)

    if df is None or df.empty:
        return states

    price = latest_btc_okx_price

    if price is not None:
        now = datetime.now(KST)

        if config["timeframe"] == "5m":
            current_start = now.replace(
                minute=(now.minute // 5) * 5,
                second=0,
                microsecond=0
            )

        elif config["timeframe"] == "15m":
            current_start = now.replace(
                minute=(now.minute // 15) * 15,
                second=0,
                microsecond=0
            )

        elif config["timeframe"] == "1h":
            current_start = now.replace(
                minute=0,
                second=0,
                microsecond=0
            )

        elif config["timeframe"] == "1d":
            current_start = now.replace(
                hour=0,
                minute=0,
                second=0,
                microsecond=0
            )

        else:
            current_start = now.replace(
                hour=now.hour // 4 * 4,
                minute=0,
                second=0,
                microsecond=0
            )

        df = update_current_candle(
            df,
            current_start,
            price
        )

    state_1 = get_ema_alignment_state(
        df,
        config["fast"],
        config["slow"]
    )

    state_2 = get_ema_alignment_state(
        df,
        config["long_fast"],
        config["long_slow"]
    )

    states["10_20"] = state_1
    states["100_200"] = state_2

    return states


# =========================================================
# BTC KST 09:00 기준 일봉
# =========================================================

def build_okx_kst_daily_periods(df, current_price=None):
    if df is None or df.empty:
        return []

    df = (
        df.sort_values("datetime")
        .drop_duplicates("datetime")
        .reset_index(drop=True)
    )

    now = datetime.now(KST)

    current_day_start = now.replace(
        hour=9,
        minute=0,
        second=0,
        microsecond=0
    )

    if now < current_day_start:
        current_day_start -= timedelta(days=1)

    rows = []

    for _, row in df.iterrows():
        dt = row["datetime"]

        day_start = dt.replace(
            hour=9,
            minute=0,
            second=0,
            microsecond=0
        )

        if dt < day_start:
            day_start -= timedelta(days=1)

        rows.append({
            "day_start": day_start,
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"])
        })

    if not rows:
        return []

    raw = pd.DataFrame(rows)
    periods = []

    for day_start, group in raw.groupby(
        "day_start",
        sort=True
    ):
        o = float(group.iloc[0]["open"])
        h = float(group["high"].max())
        l = float(group["low"].min())
        c = float(group.iloc[-1]["close"])

        active = day_start == current_day_start

        if active and current_price is not None:
            c = float(current_price)
            h = max(h, c)
            l = min(l, c)

        change = (c - o) / o * 100 if o else None

        periods.append({
            "start": day_start,
            "end": day_start + timedelta(days=1),
            "active": active,
            "label": day_start.strftime("%m/%d 09:00"),
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "change": change
        })

    return periods[-6:]


# =========================================================
# BTC 업데이트
# =========================================================

def update_okx_btc():
    global latest_btc_okx_price
    global latest_btc_daily_periods
    global latest_btc_daily_change
    global latest_btc_signal_1_states
    global latest_btc_signal_2_states
    global latest_btc_signal_3_states

    price = okx_price()
    latest_btc_okx_price = price

    if price is None:
        return

    d1h = okx_candles("1H", 200)

    if d1h.empty:
        latest_btc_daily_periods = []
        latest_btc_daily_change = None

    else:
        latest_btc_daily_periods = (
            build_okx_kst_daily_periods(
                d1h,
                price
            )
        )

        if latest_btc_daily_periods:
            latest_btc_daily_change = (
                latest_btc_daily_periods[-1].get("change")
            )
        else:
            latest_btc_daily_change = None

    latest_btc_signal_1_states = (
        analyze_btc_ema_states(1)
    )

    latest_btc_signal_2_states = (
        analyze_btc_ema_states(2)
    )

    latest_btc_signal_3_states = (
        analyze_btc_ema_states(3)
    )


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():
    if not update_lock.acquire(False):
        return

    try:
        if USE_OKX == "Y":
            update_okx_btc()

        if USE_UPBIT == "Y":
            update_upbit()

    except Exception as e:
        log.exception(
            "업데이트 오류: %s",
            e
        )

    finally:
        update_lock.release()


# =========================================================
# 표시 포맷
# =========================================================

def fmt_price(v):
    if v is None:
        return "-"

    try:
        v = float(v)
    except Exception:
        return "-"

    if v >= 100000000:
        return f"{v / 100000000:.2f}억"

    if v >= 10000:
        return f"{v:,.0f}"

    if v >= 1:
        return f"{v:,.2f}"

    return f"{v:.6f}"


def fmt_vol(v):
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


def fmt_change(v):
    if v is None:
        return '<span class="zero">-</span>'

    try:
        v = float(v)
    except Exception:
        return '<span class="zero">-</span>'

    if v > 0:
        return f'<span class="up">▲ +{v:.2f}%</span>'

    if v < 0:
        return f'<span class="down">▼ {v:.2f}%</span>'

    return '<span class="zero">0.00%</span>'


# =========================================================
# EMA 상태 HTML
# =========================================================

def ema_symbol(state):
    if state == "정배열":
        return '<span class="ema-normal">▲</span>'

    if state == "역배열":
        return '<span class="ema-reverse">▼</span>'

    if state == "동일":
        return '<span class="ema-same">—</span>'

    return '<span class="ema-same">-</span>'


def signal_ema_line_html(signal_number, signal_data):
    config = get_signal_config(signal_number)

    label = config["name"]
    timeframe = get_timeframe_label(
        config["timeframe"]
    )

    pair_1, pair_2 = get_ema_pair_labels(
        signal_number
    )

    state_1 = signal_data.get("alignment_1")
    state_2 = signal_data.get("alignment_2")

    return f"""
    <div class="ema-status-line">
        <span class="ema-status-name">
            {html.escape(label)}
        </span>

        <span class="ema-status-time">
            {html.escape(timeframe)}
        </span>

        <span>
            EMA {html.escape(pair_1)}
            {ema_symbol(state_1)}
        </span>

        <span>
            EMA {html.escape(pair_2)}
            {ema_symbol(state_2)}
        </span>
    </div>
    """


def row_ema_status_html(row):
    return (
        '<div class="coin-ema-status">'
        + signal_ema_line_html(
            1, row.get("signal_1") or {}
        )
        + signal_ema_line_html(
            2, row.get("signal_2") or {}
        )
        + signal_ema_line_html(
            3, row.get("signal_3") or {}
        )
        + '</div>'
    )


# =========================================================
# 캔들 그림
# =========================================================

def candle_icon_html(pattern):
    if pattern in ("상승장악형", "상승관통형"):
        return """
        <div class="candle-pattern-visual">
            <span class="mini-candle bearish"></span>
            <span class="mini-candle bullish large"></span>
        </div>
        """

    if pattern in ("하락장악형", "하락관통형"):
        return """
        <div class="candle-pattern-visual">
            <span class="mini-candle bullish"></span>
            <span class="mini-candle bearish large"></span>
        </div>
        """

    if pattern == "장대양봉":
        return """
        <div class="candle-pattern-visual">
            <span class="mini-candle bullish very-large"></span>
        </div>
        """

    if pattern == "장대음봉":
        return """
        <div class="candle-pattern-visual">
            <span class="mini-candle bearish very-large"></span>
        </div>
        """

    if pattern in ("상승망치형", "상승역망치형"):
        return """
        <div class="candle-pattern-visual">
            <span class="mini-candle bullish hammer"></span>
        </div>
        """

    if pattern in ("하락망치형", "하락역망치형"):
        return """
        <div class="candle-pattern-visual">
            <span class="mini-candle bearish hammer"></span>
        </div>
        """

    if pattern == "양수도지":
        return """
        <div class="candle-pattern-visual">
            <span class="mini-candle bullish doji"></span>
        </div>
        """

    if pattern == "음수도지":
        return """
        <div class="candle-pattern-visual">
            <span class="mini-candle bearish doji"></span>
        </div>
        """

    return """
    <div class="candle-pattern-visual empty">—</div>
    """


# =========================================================
# 캔들 패턴 HTML
# =========================================================

def current_pattern_html(pattern):
    if isinstance(pattern, str):
        patterns = [pattern] if pattern else []

    elif isinstance(pattern, (list, tuple, set)):
        patterns = list(pattern)

    else:
        patterns = []

    if not patterns:
        return (
            '<div class="pattern-box pattern-none">'
            + candle_icon_html(None)
            + '<span class="pattern-name">-</span>'
            + '</div>'
        )

    boxes = []

    for item in patterns:
        escaped_pattern = html.escape(str(item))

        if item in BULLISH_HIGHLIGHT_PATTERNS:
            css_class = "bullish-pattern"

        elif item in BEARISH_HIGHLIGHT_PATTERNS:
            css_class = "bearish-pattern"

        else:
            css_class = "neutral-pattern"

        boxes.append(
            f'<div class="pattern-box {css_class}">'
            + candle_icon_html(item)
            + '<span class="pattern-name">'
            + escaped_pattern
            + '</span></div>'
        )

    return (
        '<div class="pattern-multi">'
        + "".join(boxes)
        + '</div>'
    )


# =========================================================
# 상승 / 하락 패턴 배지
# =========================================================

def pattern_badges_html(row):
    previous_patterns = row.get("previous_pattern") or []
    current_patterns = row.get("current_pattern") or []

    all_patterns = previous_patterns + current_patterns

    badges = []

    if any(
        pattern in BULLISH_HIGHLIGHT_PATTERNS
        for pattern in all_patterns
    ):
        badges.append(
            '<span class="signal-type-badge">⭐ 상승패턴</span>'
        )

    if any(
        pattern in BEARISH_HIGHLIGHT_PATTERNS
        for pattern in all_patterns
    ):
        badges.append(
            '<span class="bearish-type-badge">🔻 하락패턴</span>'
        )

    return "".join(badges)


# =========================================================
# BTC HTML
# =========================================================

def btc_ema_line_html(signal_number, states):
    config = get_signal_config(signal_number)

    pair_1, pair_2 = get_ema_pair_labels(
        signal_number
    )

    timeframe = get_timeframe_label(
        config["timeframe"]
    )

    return f"""
    <div class="btc-ema-line">
        <span class="btc-ema-time">
            {html.escape(config["name"])}
            · {html.escape(timeframe)}
        </span>

        <span>
            EMA {html.escape(pair_1)}
            {ema_symbol(states.get("10_20"))}
        </span>

        <span>
            EMA {html.escape(pair_2)}
            {ema_symbol(states.get("100_200"))}
        </span>
    </div>
    """


def btc_html():
    return f"""
    <section class="btc-panel">

        <div class="section-head">
            <div>
                <span class="section-kicker">MARKET</span>
                <b>BTCUSDT</b>
            </div>

            <span class="update-time">
                OKX · KST 09:00 기준 · {kst()}
            </span>
        </div>

        <div class="btc-main">
            <div class="btc-name">BTC</div>

            <div class="btc-price">
                {fmt_price(latest_btc_okx_price)}
            </div>

            <div class="btc-change">
                {fmt_change(latest_btc_daily_change)}
            </div>
        </div>

        <div class="btc-ema-status">
            {btc_ema_line_html(
                1, latest_btc_signal_1_states
            )}

            {btc_ema_line_html(
                2, latest_btc_signal_2_states
            )}

            {btc_ema_line_html(
                3, latest_btc_signal_3_states
            )}
        </div>

    </section>
    """


# =========================================================
# SIGNAL 카드
# =========================================================

def signal_card(row):
    timeframe_label = get_timeframe_label(
        CANDLE_PATTERN_TIMEFRAME
    )

    has_bullish_pattern = (
        row.get("current_bullish_pattern", False)
        and row.get("ema10_rebound", False)
    )

    badge_html = pattern_badges_html(row)

    card_classes = ["signal-card"]

    if has_bullish_pattern:
        card_classes.append("has-bullish-pattern")

    card_class = " ".join(card_classes)

    return f"""
    <div class="{card_class}">

        <div class="signal-card-top">

            <span class="signal-rank">
                거래대금 #{row.get("volume_rank", "-")}
            </span>

            <b class="signal-coin-name">
                {html.escape(row["name"])}
            </b>

            <div class="signal-type-list">
                {badge_html}
            </div>

            {row_ema_status_html(row)}

        </div>

        <div class="signal-data-grid">

            <div class="signal-price">
                <span>현재가격</span>
                <strong>
                    {fmt_price(row.get("current_price"))}
                </strong>
            </div>

            <div class="signal-volume">
                <span>거래대금</span>
                <strong>
                    {fmt_vol(row["volume_24h"])}
                </strong>
            </div>

            <div class="signal-change">
                <span>변동률</span>
                <strong>
                    {fmt_change(row.get("daily_change"))}
                </strong>
            </div>

            <div class="signal-pattern">
                <span>이전봉 {html.escape(timeframe_label)}</span>
                <strong class="pattern-display">
                    {current_pattern_html(
                        row.get("previous_pattern")
                    )}
                </strong>
            </div>

            <div class="signal-pattern">
                <span>현재봉 {html.escape(timeframe_label)}</span>
                <strong class="pattern-display">
                    {current_pattern_html(
                        row.get("current_pattern")
                    )}
                </strong>
            </div>

        </div>

    </div>
    """


# =========================================================
# SIGNAL 조건 표시
# =========================================================

def signal_section():
    pattern_timeframe_label = get_timeframe_label(
        CANDLE_PATTERN_TIMEFRAME
    )

    if not latest_signal_data:
        body = """
        <div class="signal-empty">
            현재 SIGNAL 조건에 해당하는 종목 없음
        </div>
        """

    else:
        body = "".join(
            signal_card(row)
            for row in latest_signal_data
        )

    condition_html = ""

    for signal_number in (1, 2, 3):
        config = get_signal_config(signal_number)

        condition_html += f"""
        <div class="signal-condition">
            <span>{html.escape(config["name"])}</span>

            <b>
                {html.escape(
                    get_timeframe_label(config["timeframe"])
                )}
                ·
                {html.escape(
                    get_signal_description(signal_number)
                )}
            </b>
        </div>
        """

    return f"""
    <section class="signal-panel">

        <div class="section-head signal-panel-head">
            <div>
                <span class="section-kicker">SIGNAL</span>
                <b>🔴 종목 검색</b>
            </div>

            <span class="update-time">
                캔들패턴 {html.escape(pattern_timeframe_label)}
                · {kst()}
            </span>
        </div>

        {condition_html}

        <div class="signal-condition">
            <span>공통 조건</span>
            <b>당일 변동률 양수 · 시그널 1·2·3 모두 충족</b>
        </div>

        <div class="signal-header">
            <div>현재가격</div>
            <div>거래대금</div>
            <div>변동률</div>
            <div>이전봉 {html.escape(pattern_timeframe_label)}</div>
            <div>현재봉 {html.escape(pattern_timeframe_label)}</div>
        </div>

        <div class="signal-list">
            {body}
        </div>

    </section>
    """


# =========================================================
# TOP 카드
# =========================================================

def card(row, kind):
    if kind != "top":
        return ""

    timeframe_label = get_timeframe_label(
        CANDLE_PATTERN_TIMEFRAME
    )

    status = pattern_badges_html(row)

    return f"""
    <article class="coin-card">

        <div class="coin-head">

            <div class="coin-title">
                <span class="rank">#{row["rank"]}</span>
                <b>{html.escape(row["name"])}</b>
            </div>

            <div class="pattern-badge-list">
                {status}
            </div>

        </div>

        {row_ema_status_html(row)}

        <div class="market-summary">

            <div>
                <span>현재가</span>
                <strong>
                    {fmt_price(row["current_price"])}
                </strong>
            </div>

            <div>
                <span>24H 거래대금</span>
                <strong>
                    {fmt_vol(row["volume_24h"])}
                </strong>
            </div>

            <div>
                <span>당일 변동률</span>
                <strong class="daily-change-value">
                    {fmt_change(row.get("daily_change"))}
                </strong>
            </div>

            <div>
                <span>이전봉 {html.escape(timeframe_label)}</span>
                <strong class="pattern-display">
                    {current_pattern_html(
                        row.get("previous_pattern")
                    )}
                </strong>
            </div>

            <div>
                <span>현재봉 {html.escape(timeframe_label)}</span>
                <strong class="pattern-display">
                    {current_pattern_html(
                        row.get("current_pattern")
                    )}
                </strong>
            </div>

        </div>

    </article>
    """


# =========================================================
# CSS
# =========================================================

CSS = """
* {
    box-sizing: border-box;
}

html {
    background: #080b0f;
}

body {
    margin: 0;
    padding: 8px;
    background: #080b0f;
    color: #e8edf2;
    font-family: Arial, "Noto Sans KR", sans-serif;
    font-size: 11px;
}

h1 {
    margin: 4px 2px 12px;
    font-size: 14px;
    font-weight: 800;
    color: #f1f4f7;
}

section {
    margin-bottom: 12px;
}

.up {
    color: #38d878 !important;
    font-weight: 900;
}

.down {
    color: #ff5966 !important;
    font-weight: 900;
}

.zero {
    color: #68737e;
    font-weight: 800;
}

.ema-normal {
    color: #38d878;
    font-size: 7px;
    font-weight: 900;
    white-space: nowrap;
}

.ema-reverse {
    color: #ff5966;
    font-size: 7px;
    font-weight: 900;
    white-space: nowrap;
}

.ema-same {
    color: #68737e;
    font-size: 7px;
    font-weight: 900;
    white-space: nowrap;
}


/* =========================================================
   공통 헤더
   ========================================================= */

.section-head {
    height: 38px;
    padding: 0 10px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    border-bottom: 1px solid #202a33;
}

.section-head > div {
    display: flex;
    align-items: center;
    gap: 7px;
}

.section-head b {
    font-size: 10px;
    font-weight: 800;
}

.section-kicker {
    color: #7d8994;
    font-size: 6px;
    letter-spacing: 1px;
    font-weight: 700;
}

.update-time {
    color: #65717c;
    font-size: 6px;
}


/* =========================================================
   코인별 EMA 상태
   ========================================================= */

.coin-ema-status {
    display: flex;
    flex-direction: column;
    gap: 4px;
    padding: 6px 9px;
    background: #0a1015;
    border-bottom: 1px solid #202a33;
}

.ema-status-line {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 9px;
    min-height: 13px;
    font-size: 7px;
    font-weight: 900;
}

.ema-status-name {
    min-width: 43px;
    color: #d5dce2;
    font-size: 7px;
    font-weight: 900;
}

.ema-status-time {
    min-width: 43px;
    color: #83909b;
    font-size: 6px;
    font-weight: 800;
}


/* =========================================================
   캔들 패턴
   ========================================================= */

.pattern-box {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    min-width: 35px;
    line-height: 1;
}

.pattern-name {
    display: block;
    margin-top: 2px;
    color: #38d878;
    font-size: 5px;
    font-weight: 900;
    white-space: nowrap;
}

.bearish-pattern .pattern-name {
    color: #ff5966;
}

.neutral-pattern .pattern-name {
    color: #e4c45e;
}

.pattern-none .pattern-name {
    color: #68737e;
}

.pattern-multi {
    display: flex;
    align-items: flex-start;
    justify-content: center;
    flex-wrap: wrap;
    gap: 3px;
    width: 100%;
}

.pattern-multi .pattern-box {
    min-width: 29px;
}

.candle-pattern-visual {
    height: 23px;
    min-width: 30px;
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 4px;
}

.mini-candle {
    width: 7px;
    height: 22px;
    position: relative;
    display: inline-block;
    border-left: 1px solid #79848e;
    border-right: 1px solid #79848e;
}

.mini-candle.bullish {
    background: #38d878;
}

.mini-candle.bearish {
    background: #ff5966;
}

.mini-candle.large {
    height: 20px;
}

.mini-candle.very-large {
    height: 22px;
    width: 9px;
}

.mini-candle.hammer {
    height: 7px;
    margin-top: 14px;
}

.mini-candle.doji {
    height: 2px;
    margin-top: 10px;
}

.candle-pattern-visual.empty {
    color: #68737e;
    font-size: 9px;
    height: 23px;
}


/* =========================================================
   SIGNAL 패널
   ========================================================= */

.signal-panel {
    background: #0c1116;
    border: 1px solid #48272c;
    margin-bottom: 13px;
}

.signal-panel-head {
    border-bottom: 1px solid #392126;
}

.signal-condition {
    min-height: 30px;
    padding: 4px 9px;
    display: flex;
    align-items: center;
    gap: 8px;
    background: #120e10;
    border-bottom: 1px solid #302024;
}

.signal-condition span {
    color: #7d6b70;
    font-size: 7px;
    min-width: 52px;
    white-space: nowrap;
}

.signal-condition b {
    color: #ff5966;
    font-size: 8px;
    font-weight: 900;
}


/* =========================================================
   SIGNAL 헤더 및 카드
   ========================================================= */

.signal-header,
.signal-data-grid {
    display: grid;
    grid-template-columns: 1.1fr 0.9fr 0.8fr 1.1fr 1.1fr;
    align-items: center;
}

.signal-header {
    min-height: 27px;
    padding: 0 9px;
    color: #69747e;
    font-size: 6px;
    font-weight: 800;
    background: #0b1015;
    border-bottom: 1px solid #20282f;
}

.signal-header > div {
    text-align: center;
}

.signal-card {
    display: flex;
    flex-direction: column;
    background: #0d1217;
    border-bottom: 1px solid #20282f;
}

.signal-card:last-child {
    border-bottom: 0;
}

.signal-card-top {
    width: 100%;
    min-height: 37px;
    padding: 5px 8px;
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 5px;
    border-bottom: 1px solid #263039;
    background: rgba(255,255,255,0.015);
}

.signal-rank {
    color: #b84d58;
    font-size: 7px;
    font-weight: 900;
    white-space: nowrap;
}

.signal-coin-name {
    color: #e5e9ed;
    font-size: 9px;
    font-weight: 900;
    white-space: nowrap;
}

.signal-type-list {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 3px;
}

.signal-type-badge {
    color: #e4c45e;
    font-size: 6px;
    font-weight: 900;
}

.bearish-type-badge {
    color: #ff5966;
    font-size: 6px;
    font-weight: 900;
}

.signal-card-top .coin-ema-status {
    width: 100%;
    padding: 4px 0;
    background: transparent;
    border: 0;
}

.signal-card-top .ema-status-line {
    gap: 7px;
}

.signal-card.has-bullish-pattern {
    background: #10251d;
    box-shadow:
        inset 4px 0 0 #38d878,
        inset 0 0 0 1px #286b48;
}

.signal-card.has-bullish-pattern .signal-coin-name {
    color: #38d878;
}


/* =========================================================
   SIGNAL 하단 데이터
   ========================================================= */

.signal-data-grid {
    width: 100%;
    min-height: 58px;
}

.signal-data-grid > div {
    min-width: 0;
    min-height: 58px;
    padding: 3px 2px;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    text-align: center;
}

.signal-data-grid > div + div {
    border-left: 1px solid #1c252c;
}

.signal-data-grid span {
    color: #69747e;
    font-size: 6px;
}

.signal-data-grid strong {
    margin-top: 2px;
    color: #dce2e7;
    font-size: 8px;
}

.signal-data-grid .signal-price strong {
    color: #f1f4f7;
    font-size: 8px;
    font-weight: 900;
    white-space: nowrap;
}

.signal-pattern .pattern-box {
    min-width: 28px;
}

.signal-pattern .pattern-name {
    font-size: 5px;
}

.signal-pattern .candle-pattern-visual {
    height: 22px;
    transform: scale(.80);
}

.signal-pattern .pattern-multi {
    gap: 1px;
}

.signal-empty {
    min-height: 46px;
    display: flex;
    align-items: center;
    justify-content: center;
    color: #626e78;
    font-size: 7px;
}


/* =========================================================
   BTC 패널
   ========================================================= */

.btc-panel {
    background: #0c1116;
    border: 1px solid #202a33;
    margin-bottom: 13px;
}

.btc-main {
    min-height: 54px;
    display: grid;
    grid-template-columns: 0.7fr 1.5fr 1fr;
    align-items: center;
}

.btc-main > div {
    text-align: center;
    padding: 5px;
}

.btc-name {
    color: #c4ccd3;
    font-size: 10px;
    font-weight: 800;
}

.btc-price {
    color: #f4f6f8;
    font-size: 16px;
    font-weight: 900;
}

.btc-change {
    font-size: 11px;
    font-weight: 900;
}

.btc-ema-status {
    display: flex;
    flex-direction: column;
    gap: 5px;
    padding: 7px 9px;
    background: #0a1015;
    border-top: 1px solid #202a33;
}

.btc-ema-line {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 12px;
    min-height: 14px;
    font-size: 8px;
    font-weight: 900;
}

.btc-ema-time {
    min-width: 100px;
    color: #83909b;
    font-size: 7px;
    font-weight: 800;
}


/* =========================================================
   TOP 카드
   ========================================================= */

.coin-card {
    background: #0c1116;
    border: 1px solid #202a33;
    margin-bottom: 7px;
}

.coin-head {
    min-height: 38px;
    padding: 0 9px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    border-bottom: 1px solid #202a33;
}

.coin-title {
    display: flex;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
}

.coin-title b {
    font-size: 10px;
    font-weight: 900;
}

.rank {
    color: #c9a83d;
    font-size: 8px;
    font-weight: 800;
}

.pattern-badge-list {
    display: flex;
    align-items: center;
    justify-content: flex-end;
    flex-wrap: wrap;
    gap: 5px;
}

.market-summary {
    display: grid;
    grid-template-columns: repeat(5, minmax(0, 1fr));
    border-bottom: 1px solid #202a33;
}

.market-summary > div {
    min-width: 0;
    min-height: 58px;
    padding: 4px 1px;
    text-align: center;
    background: #0d1318;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
}

.market-summary > div + div {
    border-left: 1px solid #1d262e;
}

.market-summary span {
    display: block;
    color: #65717b;
    font-size: 6px;
    margin-bottom: 3px;
}

.market-summary strong {
    display: block;
    color: #dce2e7;
    font-size: 7px;
}

.market-summary .pattern-box {
    min-width: 25px;
}

.market-summary .pattern-name {
    font-size: 4.5px;
}

.market-summary .candle-pattern-visual {
    height: 21px;
    transform: scale(.68);
}

.market-summary .pattern-multi {
    gap: 0;
}

.market-summary .pattern-multi .pattern-box {
    min-width: 22px;
}


/* =========================================================
   모바일 반응형
   ========================================================= */

@media (max-width: 600px) {

    body {
        padding: 5px;
    }

    h1 {
        margin: 3px 2px 9px;
        font-size: 12px;
    }

    .section-head {
        padding: 0 8px;
    }

    .section-head b {
        font-size: 9px;
    }

    .update-time {
        font-size: 5px;
    }

    .btc-main {
        min-height: 48px;
    }

    .btc-price {
        font-size: 13px;
    }

    .btc-change {
        font-size: 9px;
    }

    .btc-ema-status {
        padding: 6px 7px;
        gap: 4px;
    }

    .btc-ema-line {
        gap: 8px;
        font-size: 6px;
    }

    .btc-ema-time {
        min-width: 88px;
        font-size: 6px;
    }

    .coin-head {
        min-height: 35px;
        padding: 0 7px;
    }

    .coin-title {
        gap: 6px;
    }

    .coin-title b {
        font-size: 9px;
    }

    .ema-status-line {
        gap: 6px;
        font-size: 6px;
    }

    .ema-status-name {
        min-width: 39px;
        font-size: 6px;
    }

    .ema-status-time {
        min-width: 38px;
        font-size: 5px;
    }

    .coin-ema-status {
        padding: 5px 7px;
        gap: 3px;
    }

    .market-summary {
        grid-template-columns: repeat(5, minmax(0, 1fr));
    }

    .market-summary > div {
        min-height: 53px;
        padding: 3px 1px;
    }

    .market-summary span {
        font-size: 5px;
        margin-bottom: 2px;
    }

    .market-summary strong {
        font-size: 5px;
    }

    .market-summary .pattern-box {
        min-width: 20px;
    }

    .market-summary .pattern-name {
        font-size: 3.5px;
    }

    .market-summary .candle-pattern-visual {
        height: 19px;
        transform: scale(.56);
    }

    .signal-condition {
        min-height: 27px;
        padding: 3px 6px;
        gap: 5px;
    }

    .signal-condition span {
        min-width: 43px;
        font-size: 6px;
    }

    .signal-condition b {
        font-size: 6px;
    }

    .signal-header,
    .signal-data-grid {
        grid-template-columns: 1.1fr 0.9fr 0.8fr 1.1fr 1.1fr;
    }

    .signal-header {
        padding: 0 3px;
        font-size: 5px;
    }

    .signal-card-top {
        min-height: 32px;
        padding: 4px 5px;
        gap: 4px;
    }

    .signal-rank {
        font-size: 5px;
    }

    .signal-coin-name {
        font-size: 7px;
    }

    .signal-type-badge,
    .bearish-type-badge {
        font-size: 4px;
    }

    .signal-card-top .ema-status-line {
        gap: 5px;
        font-size: 5px;
    }

    .signal-card-top .ema-status-name {
        min-width: 35px;
        font-size: 5px;
    }

    .signal-card-top .ema-status-time {
        min-width: 35px;
        font-size: 4px;
    }

    .signal-data-grid {
        min-height: 53px;
    }

    .signal-data-grid > div {
        min-height: 53px;
        padding: 2px 1px;
    }

    .signal-data-grid span {
        font-size: 5px;
    }

    .signal-data-grid strong {
        font-size: 5px;
    }

    .signal-data-grid .signal-price strong {
        font-size: 6px;
    }

    .signal-pattern .pattern-box {
        min-width: 20px;
    }

    .signal-pattern .pattern-name {
        font-size: 3.5px;
    }

    .signal-pattern .candle-pattern-visual {
        height: 19px;
        transform: scale(.55);
    }
}
"""


# =========================================================
# DASHBOARD
# =========================================================

@app.get("/", response_class=HTMLResponse)
def dashboard():
    s = ""

    if USE_OKX == "Y":
        s += btc_html()

    if USE_UPBIT == "Y":
        s += signal_section()

        if SHOW_TOP_LIST == "Y":

            if latest_upbit_data:
                top_cards = "".join(
                    card(row, "top")
                    for row in latest_upbit_data
                )

            else:
                top_cards = (
                    '<div class="signal-empty">'
                    f'현재 거래대금 TOP{TOP_N} 데이터 없음'
                    '</div>'
                )

            s += f"""
            <section>

                <div class="section-head">
                    <div>
                        <span class="section-kicker">
                            RANKING
                        </span>

                        <b>TOP{TOP_N} · 거래대금 순</b>
                    </div>

                    <span class="update-time">
                        실제 업비트 거래대금 기준
                    </span>
                </div>

                {top_cards}

            </section>
            """

    return f"""
    <!doctype html>
    <html lang="ko">

    <head>
        <meta charset="utf-8">

        <meta
            name="viewport"
            content="width=device-width, initial-scale=1"
        >

        <meta
            http-equiv="refresh"
            content="60"
        >

        <title>TRADING SIGNAL CENTER</title>

        <style>
            {CSS}
        </style>

    </head>

    <body>

        <h1>📊 TRADING SIGNAL CENTER</h1>

        {s}

    </body>
    </html>
    """


# =========================================================
# 스케줄러
# =========================================================

def scheduler():
    while True:
        try:
            schedule.run_pending()

        except Exception as e:
            log.exception(
                "scheduler: %s",
                e
            )

        time.sleep(1)


# =========================================================
# STARTUP
# =========================================================

@app.on_event("startup")
def startup():

    log.info(
        "START | BTC EMA 3개 시그널 표시 | "
        "UPBIT TOP%s | "
        "SIGNAL1=%s | "
        "SIGNAL2=%s | "
        "SIGNAL3=%s | "
        "캔들패턴 시간봉=%s",
        TOP_N,
        get_signal_description(1),
        get_signal_description(2),
        get_signal_description(3),
        get_timeframe_label(CANDLE_PATTERN_TIMEFRAME)
    )

    log.info(
        "시그널 1 시간봉: %s",
        get_timeframe_label(SIGNAL_1_TIMEFRAME)
    )

    log.info(
        "시그널 2 시간봉: %s",
        get_timeframe_label(SIGNAL_2_TIMEFRAME)
    )

    log.info(
        "시그널 3 시간봉: %s",
        get_timeframe_label(SIGNAL_3_TIMEFRAME)
    )

    log.info(
        "시그널 종목 선정: 시그널 1·2·3 모두 충족 + 당일 변동률 양수"
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
