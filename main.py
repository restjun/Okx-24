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

TOP_N = 10
SHOW_TOP_LIST = "N"
UPDATE_MINUTES = 1

DAILY_CHANGE_MIN = 0.0

USE_BTC_VOLUME_FILTER = "N"

USE_UPBIT = "Y"
USE_OKX = "Y"

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 20

CANDLE_PATTERN_TIMEFRAME = "1h"

RSI_PERIOD = 14

# =========================================================
# RSI 설정
# =========================================================

# RSI 상향돌파 및 하향돌파 기준
RSI_SIGNAL_MIN = 70
RSI_SIGNAL_MAX = 70

# 최종 SIGNAL 최신 RSI 허용 범위
RSI_RANGE_MIN = 50
RSI_RANGE_MAX = 70

# 최근 돌파 확인 캔들 수
RSI_CHECK_CANDLES = 12


# =========================================================
# EMA 설정
# =========================================================

EMA_FAST_PERIOD = 5
EMA_MIDDLE_PERIOD = 10
EMA_SLOW_PERIOD = 20
EMA_LONG_PERIOD = 50

USE_EMA_5 = "N"
USE_EMA_10 = "N"
USE_EMA_20 = "N"
USE_EMA_50 = "N"

DISPLAY_CANDLE_COUNT = 12

CONSECUTIVE_CANDLE_MIN = 3
CONSECUTIVE_BULLISH_MIN = 2

TIMEFRAME_LABEL = {
    "5m": "5분",
    "15m": "15분",
    "1h": "1시간",
    "4h": "4시간",
    "1d": "일봉"
}

TIMEFRAME_MINUTES = {
    "5m": 5,
    "15m": 15,
    "1h": 60,
    "4h": 240,
    "1d": 1440
}


# =========================================================
# EMA 조건 표시
# =========================================================

def get_ema_condition_text():

    enabled = []

    if USE_EMA_5 == "Y":
        enabled.append(f"EMA{EMA_FAST_PERIOD}")

    if USE_EMA_10 == "Y":
        enabled.append(f"EMA{EMA_MIDDLE_PERIOD}")

    if USE_EMA_20 == "Y":
        enabled.append(f"EMA{EMA_SLOW_PERIOD}")

    if USE_EMA_50 == "Y":
        enabled.append(f"EMA{EMA_LONG_PERIOD}")

    if not enabled:
        return "EMA 정배열 조건 사용 안 함"

    if len(enabled) == 1:
        return f"{enabled[0]} 단독 사용"

    return " > ".join(enabled) + " 정배열"


# =========================================================
# 전역 데이터
# =========================================================

latest_upbit_data = []
latest_upbit_daily_data = []

latest_upbit_update_time = "-"
latest_upbit_daily_update_time = "-"

latest_upbit_markets = []

latest_signal_data = []

latest_upbit_btc_volume_24h = None

latest_btc_okx_price = None
latest_btc_daily_periods = []
latest_btc_daily_change = None
latest_btc_last_three_patterns = []


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


# =========================================================
# 캔들 시작 시간 계산
# =========================================================

def get_candle_start(now, timeframe):

    now = now.astimezone(KST)

    if timeframe == "1d":

        start = now.replace(
            hour=9,
            minute=0,
            second=0,
            microsecond=0
        )

        if now < start:
            start -= timedelta(days=1)

        return start

    if timeframe == "4h":

        anchor = now.replace(
            hour=9,
            minute=0,
            second=0,
            microsecond=0
        )

        if now < anchor:
            anchor -= timedelta(days=1)

        elapsed = now - anchor

        periods = int(
            elapsed.total_seconds() // 14400
        )

        return anchor + timedelta(hours=periods * 4)

    minutes = TIMEFRAME_MINUTES.get(timeframe)

    if minutes is None:
        return None

    midnight = now.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0
    )

    elapsed = now - midnight

    periods = int(
        elapsed.total_seconds() // (minutes * 60)
    )

    return midnight + timedelta(minutes=periods * minutes)


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
            params={
                "markets": ",".join(markets[i:i + 100])
            },
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


def get_upbit_4h_candles(market, count=200):
    return get_upbit_minute_candles(market, 240, count)


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
        float(df.loc[idx, "high"]),
        price
    )

    df.loc[idx, "low"] = min(
        float(df.loc[idx, "low"]),
        price
    )

    return df


# =========================================================
# RSI 계산
# =========================================================

def calculate_rsi(close_series, period=14):

    if close_series is None:
        return pd.Series(dtype=float)

    close = pd.to_numeric(
        close_series,
        errors="coerce"
    )

    if len(close) <= period:

        return pd.Series(
            float("nan"),
            index=close.index,
            dtype=float
        )

    delta = close.diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(
        alpha=1 / period,
        min_periods=period,
        adjust=False
    ).mean()

    avg_loss = loss.ewm(
        alpha=1 / period,
        min_periods=period,
        adjust=False
    ).mean()

    rs = avg_gain / avg_loss

    rsi = 100 - (
        100 / (1 + rs)
    )

    rsi = rsi.mask(
        (avg_loss == 0) & (avg_gain > 0),
        100
    )

    rsi = rsi.mask(
        (avg_loss == 0) & (avg_gain == 0),
        50
    )

    rsi = rsi.mask(
        (avg_gain == 0) & (avg_loss > 0),
        0
    )

    return rsi


# =========================================================
# EMA 계산
# =========================================================

def calculate_ema(close_series, period):

    if close_series is None:
        return pd.Series(dtype=float)

    close = pd.to_numeric(
        close_series,
        errors="coerce"
    )

    return close.ewm(
        span=period,
        min_periods=period,
        adjust=False
    ).mean()


# =========================================================
# 업비트 시간봉 데이터 조회
# =========================================================

def get_timeframe_dataframe(market, timeframe, price=None):

    loaders = {
        "5m": lambda: get_upbit_5m_candles(market, 200),
        "15m": lambda: get_upbit_15m_candles(market, 200),
        "1h": lambda: get_upbit_1h_candles(market, 200),
        "4h": lambda: get_upbit_4h_candles(market, 200)
    }

    if timeframe not in loaders:
        return pd.DataFrame()

    df = get_cached_dataframe(
        market,
        timeframe,
        loaders[timeframe]
    )

    if df.empty:
        return df

    current_start = get_candle_start(
        datetime.now(KST),
        timeframe
    )

    return update_current_candle(
        df,
        current_start,
        price
    )


def get_daily_dataframe(market):

    return get_cached_dataframe(
        market,
        "1d",
        lambda: get_upbit_daily_candles(market, 200)
    )


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

    current_start = get_candle_start(
        datetime.now(KST),
        "1d"
    )

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

        change = (
            (c - o) / o * 100
            if o else None
        )

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
# 캔들 패턴 함수
# =========================================================

def is_bullish_engulfing(previous, current):

    try:
        po = float(previous["open"])
        pc = float(previous["close"])
        co = float(current["open"])
        cc = float(current["close"])

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
        po = float(previous["open"])
        pc = float(previous["close"])
        co = float(current["open"])
        cc = float(current["close"])

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
        po = float(previous["open"])
        pc = float(previous["close"])
        co = float(current["open"])
        cc = float(current["close"])

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
        po = float(previous["open"])
        pc = float(previous["close"])
        co = float(current["open"])
        cc = float(current["close"])

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

        if float(current["close"]) <= float(current["open"]):
            return False

    except Exception:
        return False

    parts = get_hammer_parts(current)

    return bool(
        parts is not None
        and parts["upper_wick"] > parts["body"]
        and parts["upper_wick"] < parts["body"] * 2
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


# =========================================================
# 상승 / 하락 패턴 대상
# =========================================================

BULLISH_HIGHLIGHT_PATTERNS = [
    "상승장악형",
    "상승관통형"
]

BEARISH_HIGHLIGHT_PATTERNS = [
    "하락장악형",
    "하락관통형",
    "장대음봉",
    "음수도지",
    "하락망치형",
    "하락역망치형"
]


# =========================================================
# 캔들 패턴 분석
# =========================================================

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
# 최근 캔들 패턴 분석
# =========================================================

def analyze_previous_current_pattern(periods):

    result = {
        "previous_pattern": [],
        "current_pattern": [],
        "last_three_patterns": [],
        "previous_bullish_pattern": False,
        "current_bullish_pattern": False,
        "previous_bullish_engulfing": False,
        "current_bullish_engulfing": False,
        "consecutive_bullish": 0,
        "consecutive_bearish": 0,
        "consecutive_bullish_candles": 0
    }

    if not periods:
        return result

    pattern_history = []

    for i in range(len(periods)):

        current = periods[i]

        previous = (
            periods[i - 1]
            if i >= 1
            else None
        )

        patterns = get_candle_patterns(
            previous,
            current
        )

        pattern_history.append(patterns)

    last_six = pattern_history[-DISPLAY_CANDLE_COUNT:]

    selected_periods = periods[-len(last_six):]

    result["last_three_patterns"] = [
        {
            "start": period.get("start"),
            "label": period["label"],
            "patterns": patterns,
            "open": period["open"],
            "high": period["high"],
            "low": period["low"],
            "close": period["close"],
            "change": period.get("change"),
            "active": period.get("active", False),
            "rsi": period.get("rsi"),
            "rsi_signal_type": period.get("rsi_signal_type")
        }
        for period, patterns in zip(
            selected_periods,
            last_six
        )
    ]

    current_patterns = pattern_history[-1]

    previous_patterns = (
        pattern_history[-2]
        if len(pattern_history) >= 2
        else []
    )

    result["current_pattern"] = current_patterns
    result["previous_pattern"] = previous_patterns

    result["current_bullish_engulfing"] = (
        "상승장악형" in current_patterns
    )

    result["previous_bullish_engulfing"] = (
        "상승장악형" in previous_patterns
    )

    result["previous_bullish_pattern"] = bool(
        len(periods) >= 2
        and float(periods[-2]["close"]) > float(periods[-2]["open"])
    )

    result["current_bullish_pattern"] = bool(
        float(periods[-1]["close"]) > float(periods[-1]["open"])
    )

    bullish_count = 0

    for patterns in reversed(pattern_history):

        if "장대양봉" in patterns:
            bullish_count += 1

        else:
            break

    bearish_count = 0

    for patterns in reversed(pattern_history):

        if "장대음봉" in patterns:
            bearish_count += 1

        else:
            break

    result["consecutive_bullish"] = bullish_count
    result["consecutive_bearish"] = bearish_count

    consecutive_bullish_candles = 0

    for period in reversed(periods):

        try:
            o = float(period["open"])
            c = float(period["close"])

        except Exception:
            break

        if c > o:
            consecutive_bullish_candles += 1

        else:
            break

    result["consecutive_bullish_candles"] = (
        consecutive_bullish_candles
    )

    return result


# =========================================================
# 공통 캔들 기간 생성
# =========================================================

def build_timeframe_periods(df, timeframe, current_price=None):

    if df is None or df.empty:
        return []

    df = (
        df.sort_values("datetime")
        .drop_duplicates("datetime")
        .reset_index(drop=True)
    )

    now = datetime.now(KST)

    current_start = get_candle_start(
        now,
        timeframe
    )

    minutes = TIMEFRAME_MINUTES[timeframe]

    periods = []

    for _, row in df.iterrows():

        start = row["datetime"]

        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])

        active = start == current_start

        if active and current_price is not None:

            c = float(current_price)
            h = max(h, c)
            l = min(l, c)

        change = (
            (c - o) / o * 100
            if o else None
        )

        periods.append({
            "start": start,
            "end": start + timedelta(minutes=minutes),
            "active": active,
            "label": start.strftime("%m/%d %H:%M"),
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "change": change
        })

    return periods


# =========================================================
# RSI 돌파 판정
# =========================================================

def apply_rsi_cross_signals(periods):

    if not periods:
        return periods

    for i, period in enumerate(periods):

        period["rsi_previous"] = None
        period["rsi_signal_type"] = None

        if i == 0:
            continue

        previous = periods[i - 1]

        previous_rsi = previous.get("rsi")
        current_rsi = period.get("rsi")

        if previous_rsi is None or current_rsi is None:
            continue

        try:

            previous_rsi = float(previous_rsi)
            current_rsi = float(current_rsi)

            open_price = float(period["open"])
            close_price = float(period["close"])

        except Exception:
            continue

        period["rsi_previous"] = previous_rsi

        # RSI 70 상향돌파 + 양봉
        if (
            previous_rsi < RSI_SIGNAL_MIN
            and current_rsi >= RSI_SIGNAL_MIN
            and close_price > open_price
        ):

            period["rsi_signal_type"] = "RSI70_UP"

        # RSI 70 하향돌파 + 음봉
        elif (
            previous_rsi > RSI_SIGNAL_MAX
            and current_rsi <= RSI_SIGNAL_MAX
            and close_price < open_price
        ):

            period["rsi_signal_type"] = "RSI70_DOWN"

    return periods


# =========================================================
# 확정봉 EMA 정배열 판정
# =========================================================

def get_ema_alignment_at_index(
    ema_series_list,
    index
):

    if len(ema_series_list) < 2:
        return None

    values = []

    for series in ema_series_list:

        if index < 0 or index >= len(series):
            return None

        value = series.iloc[index]

        if pd.isna(value):
            return None

        values.append(float(value))

    return all(
        values[i] > values[i + 1]
        for i in range(len(values) - 1)
    )


# =========================================================
# 업비트 SIGNAL 시간봉 분석 + RSI + EMA
# =========================================================

def analyze_signal_candles(market, price):

    empty_result = {
        "periods": [],
        "signal_pass": False,
        "pattern_signal": False,
        "current_signal": False,
        "previous_signal": False,
        "signal_change": None,
        "signal_period": None,
        "signal_reason": None,
        "previous_pattern": [],
        "current_pattern": [],
        "last_three_patterns": [],
        "previous_bullish_engulfing": False,
        "current_bullish_engulfing": False,
        "consecutive_bullish": 0,
        "consecutive_bearish": 0,
        "consecutive_bullish_candles": 0,
        "rsi_signal": False,
        "rsi_values": [],
        "rsi_low_count": 0,
        "rsi_high_count": 0,
        "rsi_range_pass": False,
        "ema5": None,
        "ema10": None,
        "ema20": None,
        "ema50": None,
        "ema_alignment": False,
        "realtime_below_ema5": False
    }

    timeframe = CANDLE_PATTERN_TIMEFRAME

    if timeframe == "1d":

        df = get_daily_dataframe(market)

        if df.empty:
            return empty_result

        df = update_current_candle(
            df,
            get_candle_start(datetime.now(KST), "1d"),
            price
        )

    elif timeframe in ("5m", "15m", "1h", "4h"):

        df = get_timeframe_dataframe(
            market,
            timeframe,
            price
        )

        if df.empty:
            return empty_result

    else:
        return empty_result

    periods = build_timeframe_periods(
        df,
        timeframe,
        price
    )

    if not periods:
        return empty_result

    # =====================================================
    # RSI 계산
    # =====================================================

    rsi_series = calculate_rsi(
        df["close"],
        RSI_PERIOD
    )

    rsi_map = {
        dt: (
            float(rsi)
            if pd.notna(rsi)
            else None
        )
        for dt, rsi in zip(
            df["datetime"],
            rsi_series
        )
    }

    for period in periods:

        period["rsi"] = rsi_map.get(
            period["start"]
        )

    periods = apply_rsi_cross_signals(periods)

    last_rsi_periods = periods[-RSI_CHECK_CANDLES:]

    rsi_values = [
        period.get("rsi")
        for period in last_rsi_periods
    ]

    rsi_cross_periods = [
        period
        for period in last_rsi_periods
        if period.get("rsi_signal_type") is not None
    ]

    rsi_50_up_periods = [
        period
        for period in rsi_cross_periods
        if period["rsi_signal_type"] == "RSI70_UP"
    ]

    rsi_70_down_periods = [
        period
        for period in rsi_cross_periods
        if period["rsi_signal_type"] == "RSI70_DOWN"
    ]

    rsi_signal = bool(rsi_cross_periods)

    # =====================================================
    # 최신 RSI 범위 확인
    # =====================================================

    latest_rsi = periods[-1].get("rsi")

    rsi_range_pass = False

    if latest_rsi is not None:

        try:

            latest_rsi = float(latest_rsi)

            rsi_range_pass = bool(
                RSI_RANGE_MIN
                <= latest_rsi
                <= RSI_RANGE_MAX
            )

        except Exception:
            rsi_range_pass = False

    # =====================================================
    # EMA 5 / 10 / 20 / 50 계산
    # =====================================================

    ema5_series = calculate_ema(
        df["close"],
        EMA_FAST_PERIOD
    )

    ema10_series = calculate_ema(
        df["close"],
        EMA_MIDDLE_PERIOD
    )

    ema20_series = calculate_ema(
        df["close"],
        EMA_SLOW_PERIOD
    )

    ema50_series = calculate_ema(
        df["close"],
        EMA_LONG_PERIOD
    )

    ema5 = (
        float(ema5_series.iloc[-1])
        if len(ema5_series) > 0
        and pd.notna(ema5_series.iloc[-1])
        else None
    )

    ema10 = (
        float(ema10_series.iloc[-1])
        if len(ema10_series) > 0
        and pd.notna(ema10_series.iloc[-1])
        else None
    )

    ema20 = (
        float(ema20_series.iloc[-1])
        if len(ema20_series) > 0
        and pd.notna(ema20_series.iloc[-1])
        else None
    )

    ema50 = (
        float(ema50_series.iloc[-1])
        if len(ema50_series) > 0
        and pd.notna(ema50_series.iloc[-1])
        else None
    )

    # =====================================================
    # 선택한 EMA만 정배열 비교
    # =====================================================

    enabled_emas = []
    enabled_ema_series = []

    if USE_EMA_5 == "Y":
        enabled_emas.append(("EMA5", ema5))
        enabled_ema_series.append(ema5_series)

    if USE_EMA_10 == "Y":
        enabled_emas.append(("EMA10", ema10))
        enabled_ema_series.append(ema10_series)

    if USE_EMA_20 == "Y":
        enabled_emas.append(("EMA20", ema20))
        enabled_ema_series.append(ema20_series)

    if USE_EMA_50 == "Y":
        enabled_emas.append(("EMA50", ema50))
        enabled_ema_series.append(ema50_series)

    if not enabled_emas:

        ema_alignment = True

    elif any(
        value is None
        for _, value in enabled_emas
    ):

        ema_alignment = False

    else:

        ema_alignment = all(
            enabled_emas[i][1] > enabled_emas[i + 1][1]
            for i in range(len(enabled_emas) - 1)
        )

    # =====================================================
    # 캔들 패턴 분석
    # =====================================================

    pattern_info = analyze_previous_current_pattern(
        periods
    )

    last_three = pattern_info["last_three_patterns"]

    recent_periods = periods[-len(last_three):]

    for item, period in zip(
        last_three,
        recent_periods
    ):

        item["rsi"] = period.get("rsi")
        item["rsi_previous"] = period.get("rsi_previous")
        item["rsi_signal_type"] = period.get("rsi_signal_type")
        item["change"] = period.get("change")
        item["active"] = period.get("active", False)

    current_signal = pattern_info[
        "current_bullish_pattern"
    ]

    previous_signal = pattern_info[
        "previous_bullish_pattern"
    ]

    pattern_signal = bool(
        current_signal or previous_signal
    )

    # =====================================================
    # RSI SIGNAL 사유 및 발생 캔들
    # =====================================================

    signal_reason = None
    signal_period = None
    signal_change = None

    if rsi_cross_periods:

        latest_cross = rsi_cross_periods[-1]

        if latest_cross["rsi_signal_type"] == "RSI70_UP":

            signal_reason = (
                f"RSI {RSI_SIGNAL_MIN} 상향돌파 + 상승 캔들"
            )

        elif latest_cross["rsi_signal_type"] == "RSI70_DOWN":

            signal_reason = (
                f"RSI {RSI_SIGNAL_MAX} 하향돌파 + 하락 캔들"
            )

        signal_period = latest_cross.get("label")
        signal_change = latest_cross.get("change")

    return {
        "periods": periods[-DISPLAY_CANDLE_COUNT:],

        "signal_pass": rsi_signal,

        "pattern_signal": pattern_signal,
        "current_signal": current_signal,
        "previous_signal": previous_signal,

        "signal_change": signal_change,
        "signal_period": signal_period,
        "signal_reason": signal_reason,

        "previous_pattern": pattern_info["previous_pattern"],
        "current_pattern": pattern_info["current_pattern"],
        "last_three_patterns": last_three,

        "previous_bullish_engulfing": pattern_info["previous_bullish_engulfing"],
        "current_bullish_engulfing": pattern_info["current_bullish_engulfing"],

        "consecutive_bullish": pattern_info["consecutive_bullish"],
        "consecutive_bearish": pattern_info["consecutive_bearish"],

        "consecutive_bullish_candles": pattern_info[
            "consecutive_bullish_candles"
        ],

        "rsi_signal": rsi_signal,
        "rsi_values": rsi_values,

        "rsi_low_count": len(rsi_50_up_periods),
        "rsi_high_count": len(rsi_70_down_periods),

        "rsi_50_up_count": len(rsi_50_up_periods),
        "rsi_70_down_count": len(rsi_70_down_periods),

        "rsi_range_pass": rsi_range_pass,

        "ema5": ema5,
        "ema10": ema10,
        "ema20": ema20,
        "ema50": ema50,

        "ema_alignment": ema_alignment,

        "realtime_below_ema5": False
    }


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
    volume_rank
):

    coin = market.replace("KRW-", "")

    return {
        "rank": rank,
        "name": coin,
        "market": market,
        "volume_24h": item["volume_24h"],
        "current_price": item["current_price"],
        "volume_rank": volume_rank,
        "daily_change": analysis_daily["change"],
        "periods_daily": analysis_daily["periods"],
        "periods_4h": analysis_pattern["periods"],

        "signal_4h": analysis_pattern["signal_pass"],
        "signal_4h_pattern": analysis_pattern["pattern_signal"],
        "signal_4h_current": analysis_pattern["current_signal"],
        "signal_4h_previous": analysis_pattern["previous_signal"],
        "signal_4h_change": analysis_pattern["signal_change"],
        "signal_4h_period": analysis_pattern["signal_period"],
        "signal_4h_reason": analysis_pattern["signal_reason"],

        "previous_pattern": analysis_pattern["previous_pattern"],
        "current_pattern": analysis_pattern["current_pattern"],
        "last_three_patterns": analysis_pattern["last_three_patterns"],

        "consecutive_bullish": analysis_pattern["consecutive_bullish"],
        "consecutive_bearish": analysis_pattern["consecutive_bearish"],

        "previous_bullish_engulfing": analysis_pattern["previous_bullish_engulfing"],
        "current_bullish_engulfing": analysis_pattern["current_bullish_engulfing"],

        "consecutive_bullish_candles": analysis_pattern.get(
            "consecutive_bullish_candles", 0
        ),

        "rsi_signal": analysis_pattern.get("rsi_signal", False),
        "rsi_values": analysis_pattern.get("rsi_values", []),

        "rsi_low_count": analysis_pattern.get("rsi_low_count", 0),
        "rsi_high_count": analysis_pattern.get("rsi_high_count", 0),

        "rsi_50_up_count": analysis_pattern.get("rsi_50_up_count", 0),
        "rsi_70_down_count": analysis_pattern.get("rsi_70_down_count", 0),

        "rsi_range_pass": analysis_pattern.get(
            "rsi_range_pass",
            False
        ),

        "ema5": analysis_pattern.get("ema5"),
        "ema10": analysis_pattern.get("ema10"),
        "ema20": analysis_pattern.get("ema20"),
        "ema50": analysis_pattern.get("ema50"),

        "ema_alignment": analysis_pattern.get(
            "ema_alignment",
            False
        ),

        "realtime_below_ema5": False
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
    global latest_upbit_btc_volume_24h

    clear_candle_cache()

    all_markets = get_upbit_markets()

    if not all_markets:
        return

    btc_item = next(
        (
            item for item in all_markets
            if item.get("market") == "KRW-BTC"
        ),
        None
    )

    if btc_item is not None:

        latest_upbit_btc_volume_24h = float(
            btc_item["volume_24h"]
        )

    else:

        latest_upbit_btc_volume_24h = None

        log.warning(
            "업비트 KRW-BTC 거래대금 조회 실패"
        )

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

            analysis_pattern = analyze_signal_candles(
                market,
                price
            )

        except Exception as e:

            log.warning(
                "%s 캔들 패턴 및 RSI 오류: %s",
                market,
                e
            )

            analysis_pattern = {
                "periods": [],
                "signal_pass": False,
                "pattern_signal": False,
                "current_signal": False,
                "previous_signal": False,
                "signal_change": None,
                "signal_period": None,
                "signal_reason": None,
                "previous_pattern": [],
                "current_pattern": [],
                "last_three_patterns": [],
                "previous_bullish_engulfing": False,
                "current_bullish_engulfing": False,
                "consecutive_bullish": 0,
                "consecutive_bearish": 0,
                "consecutive_bullish_candles": 0,
                "rsi_signal": False,
                "rsi_values": [],
                "rsi_low_count": 0,
                "rsi_high_count": 0,
                "rsi_50_up_count": 0,
                "rsi_70_down_count": 0,
                "rsi_range_pass": False,
                "ema5": None,
                "ema10": None,
                "ema20": None,
                "ema50": None,
                "ema_alignment": False,
                "realtime_below_ema5": False
            }

        rows.append(
            make_row(
                rank,
                market,
                item,
                analysis_daily,
                analysis_pattern,
                volume_rank_map.get(market)
            )
        )

    latest_upbit_data = rows
    latest_upbit_daily_data = rows

    latest_upbit_daily_update_time = kst()
    latest_upbit_update_time = latest_upbit_daily_update_time

    # =====================================================
    # 최종 SIGNAL 선정
    # =====================================================

    latest_signal_data = [
        row for row in rows
        if (
            row.get("daily_change") is not None
            and row.get("daily_change") > DAILY_CHANGE_MIN

            # 최근 RSI 돌파 신호 발생
            and row.get("rsi_signal") is True

            # 최신 RSI가 50~70 범위 안에 있어야 함
            and row.get("rsi_range_pass") is True

            # EMA 정배열
            and row.get("ema_alignment") is True
        )
    ]

    latest_signal_data.sort(
        key=lambda x: (
            x.get("volume_24h")
            if x.get("volume_24h") is not None
            else 0
        ),
        reverse=True
    )

    for signal_rank, row in enumerate(
        latest_signal_data,
        1
    ):
        row["signal_rank"] = signal_rank

    log.info(
        "UPBIT | TOP%s | "
        "SIGNAL=당일 변동률>%s%% AND 최근 %s개 봉 RSI70 돌파 "
        "AND 최신 RSI %s~%s AND %s | "
        "시간봉=%s | SIGNAL=%s | 정렬=24H 거래대금 순",
        TOP_N,
        DAILY_CHANGE_MIN,
        RSI_CHECK_CANDLES,
        RSI_RANGE_MIN,
        RSI_RANGE_MAX,
        get_ema_condition_text(),
        CANDLE_PATTERN_TIMEFRAME,
        len(latest_signal_data)
    )


# =========================================================
# OKX 캔들 조회
# =========================================================

def okx_candles(bar, limit=300, history=False, after=None):

    endpoint = (
        "https://www.okx.com/api/v5/market/history-candles"
        if history
        else "https://www.okx.com/api/v5/market/candles"
    )

    params = {
        "instId": "BTC-USDT-SWAP",
        "bar": bar,
        "limit": str(min(limit, 300))
    }

    if after is not None:
        params["after"] = str(after)

    r = retry(
        requests.get,
        endpoint,
        params=params,
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
# BTC OKX 원본 데이터를 업비트 시간 구간으로 집계
# =========================================================

def resample_okx_to_kst_timeframe(df, timeframe):

    if df is None or df.empty:
        return pd.DataFrame()

    if timeframe not in TIMEFRAME_MINUTES:
        return pd.DataFrame()

    df = (
        df.sort_values("datetime")
        .drop_duplicates("datetime")
        .reset_index(drop=True)
    )

    minutes = TIMEFRAME_MINUTES[timeframe]

    anchor = datetime(
        1970, 1, 1, 9, 0, 0,
        tzinfo=KST
    )

    seconds = minutes * 60

    rows = []

    for _, row in df.iterrows():

        dt = row["datetime"].astimezone(KST)

        elapsed = (
            dt - anchor
        ).total_seconds()

        bucket_number = int(
            elapsed // seconds
        )

        bucket_start = (
            anchor
            + timedelta(seconds=bucket_number * seconds)
        )

        rows.append({
            "datetime": bucket_start,
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"])
        })

    if not rows:
        return pd.DataFrame()

    raw = pd.DataFrame(rows)

    result = []

    for start, group in raw.groupby(
        "datetime",
        sort=True
    ):

        result.append({
            "datetime": start,
            "open": float(group.iloc[0]["open"]),
            "high": float(group["high"].max()),
            "low": float(group["low"].min()),
            "close": float(group.iloc[-1]["close"])
        })

    return (
        pd.DataFrame(result)
        .sort_values("datetime")
        .reset_index(drop=True)
    )


# =========================================================
# BTC 선택 시간봉 데이터 조회
# =========================================================

def get_okx_btc_timeframe(timeframe):

    if timeframe == "5m":

        raw = okx_candles("5m", 300)

    elif timeframe == "15m":

        raw = okx_candles("15m", 300)

    elif timeframe == "1h":

        raw = okx_candles("1H", 300)

    elif timeframe == "4h":

        raw = okx_candles("1H", 300)

    elif timeframe == "1d":

        raw = okx_candles("1H", 300)

        if not raw.empty:

            chunks = [raw]

            oldest_timestamp = int(
                raw.iloc[0]["datetime"].timestamp() * 1000
            )

            for _ in range(2):

                older = okx_candles(
                    "1H",
                    300,
                    history=True,
                    after=oldest_timestamp
                )

                if older.empty:
                    break

                new_oldest_timestamp = int(
                    older.iloc[0]["datetime"].timestamp() * 1000
                )

                if new_oldest_timestamp >= oldest_timestamp:
                    break

                chunks.append(older)

                oldest_timestamp = new_oldest_timestamp

                if len(older) < 300:
                    break

            raw = (
                pd.concat(chunks, ignore_index=True)
                .sort_values("datetime")
                .drop_duplicates("datetime")
                .reset_index(drop=True)
            )

            log.info(
                "BTC 일봉 RSI 데이터 확보 | 1시간봉=%s개",
                len(raw)
            )

    else:
        return pd.DataFrame()

    if raw.empty:
        return raw

    if timeframe in ("4h", "1d"):

        raw = resample_okx_to_kst_timeframe(
            raw,
            timeframe
        )

    return raw


# =========================================================
# BTC 선택 시간봉 패턴 및 RSI 분석
# =========================================================

def analyze_okx_btc_candles(df, timeframe, current_price=None):

    if df is None or df.empty:
        return []

    df = (
        df.sort_values("datetime")
        .drop_duplicates("datetime")
        .reset_index(drop=True)
    )

    current_start = get_candle_start(
        datetime.now(KST),
        timeframe
    )

    df = update_current_candle(
        df,
        current_start,
        current_price
    )

    rsi_series = calculate_rsi(
        df["close"],
        RSI_PERIOD
    )

    df["rsi"] = rsi_series

    periods = build_timeframe_periods(
        df,
        timeframe,
        current_price
    )

    if not periods:
        return []

    rsi_map = {
        dt: (
            float(rsi)
            if pd.notna(rsi)
            else None
        )
        for dt, rsi in zip(
            df["datetime"],
            df["rsi"]
        )
    }

    for period in periods:

        period["rsi"] = rsi_map.get(
            period["start"]
        )

    periods = apply_rsi_cross_signals(periods)

    pattern_info = analyze_previous_current_pattern(
        periods
    )

    result = pattern_info["last_three_patterns"]

    recent_periods = periods[-len(result):]

    for item, period in zip(
        result,
        recent_periods
    ):

        item["rsi"] = period.get("rsi")
        item["rsi_previous"] = period.get("rsi_previous")
        item["rsi_signal_type"] = period.get("rsi_signal_type")
        item["change"] = period.get("change")
        item["active"] = period.get("active", False)

    return result


# =========================================================
# BTC 당일 변동률 계산
# KST 09:00 기준
# =========================================================

def calculate_btc_daily_change(current_price):

    if current_price is None:
        return None

    try:

        current_price = float(current_price)

        now = datetime.now(KST)

        daily_start = get_candle_start(
            now,
            "1d"
        )

        daily_df = get_okx_btc_timeframe("1d")

        if daily_df is None or daily_df.empty:
            log.warning("BTC 당일 변동률 계산 실패: 일봉 데이터 없음")
            return None

        daily_df = (
            daily_df.sort_values("datetime")
            .drop_duplicates("datetime")
            .reset_index(drop=True)
        )

        matched = daily_df[
            daily_df["datetime"] == daily_start
        ]

        if matched.empty:

            log.warning(
                "BTC 당일 변동률 계산 실패: "
                "KST 09시 일봉 없음 | 기준=%s",
                daily_start
            )

            return None

        daily_open = float(
            matched.iloc[-1]["open"]
        )

        if daily_open <= 0:
            return None

        daily_change = (
            (current_price - daily_open)
            / daily_open
            * 100
        )

        log.info(
            "BTC 당일 변동률 | "
            "기준시각=%s | 시가=%s | 현재가=%s | 변동률=%.4f%%",
            daily_start.strftime("%Y-%m-%d %H:%M:%S"),
            daily_open,
            current_price,
            daily_change
        )

        return daily_change

    except Exception as e:

        log.exception(
            "BTC 당일 변동률 계산 오류: %s",
            e
        )

        return None


# =========================================================
# BTC 업데이트
# =========================================================

def update_okx_btc():

    global latest_btc_okx_price
    global latest_btc_daily_periods
    global latest_btc_daily_change
    global latest_btc_last_three_patterns

    price = okx_price()

    latest_btc_okx_price = price

    if price is None:
        return

    timeframe = CANDLE_PATTERN_TIMEFRAME

    df = get_okx_btc_timeframe(timeframe)

    if df.empty:

        latest_btc_daily_periods = []
        latest_btc_daily_change = None
        latest_btc_last_three_patterns = []

        return

    df = update_current_candle(
        df,
        get_candle_start(datetime.now(KST), timeframe),
        price
    )

    periods = build_timeframe_periods(
        df,
        timeframe,
        price
    )

    latest_btc_daily_periods = periods

    latest_btc_daily_change = calculate_btc_daily_change(
        price
    )

    latest_btc_last_three_patterns = (
        analyze_okx_btc_candles(
            df,
            timeframe,
            price
        )
    )

    log.info(
        "BTC OKX | 가격=%s | 캔들시간봉=%s | "
        "당일 변동률 기준=KST 09:00 | 변동률=%s",
        price,
        get_timeframe_label(timeframe),
        latest_btc_daily_change
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
# 캔들별 변동률 HTML
# =========================================================

def candle_change_html(value):

    if value is None:
        return (
            '<div class="candle-change candle-change-none">'
            '-'
            '</div>'
        )

    try:
        value = float(value)

    except Exception:
        return (
            '<div class="candle-change candle-change-none">'
            '-'
            '</div>'
        )

    if value > 0:

        return (
            '<div class="candle-change candle-change-up">'
            f'+{value:.2f}%'
            '</div>'
        )

    if value < 0:

        return (
            '<div class="candle-change candle-change-down">'
            f'{value:.2f}%'
            '</div>'
        )

    return (
        '<div class="candle-change candle-change-zero">'
        '0.00%'
        '</div>'
    )


# =========================================================
# RSI 표시 HTML
# =========================================================

def rsi_html(value, signal_type=None):

    if value is None:
        return (
            '<div class="rsi-value rsi-unavailable">'
            'RSI -'
            '</div>'
        )

    try:
        value = float(value)

    except Exception:
        return (
            '<div class="rsi-value rsi-unavailable">'
            'RSI -'
            '</div>'
        )

    if signal_type == "RSI70_UP":

        return (
            '<div class="rsi-value rsi-cross-up">'
            f'RSI {value:.1f} ↑70'
            '</div>'
        )

    if signal_type == "RSI70_DOWN":

        return (
            '<div class="rsi-value rsi-cross-down">'
            f'RSI {value:.1f} ↓70'
            '</div>'
        )

    return (
        '<div class="rsi-value">'
        f'RSI {value:.1f}'
        '</div>'
    )


# =========================================================
# 캔들 그림
# =========================================================

def candle_icon_html(pattern):

    if pattern == "상승장악형":

        return """
        <div class="candle-pattern-visual">
            <div class="mini-candle bearish">
                <span class="wick"></span><span class="body"></span>
            </div>
            <div class="mini-candle bullish large">
                <span class="wick"></span><span class="body"></span>
            </div>
        </div>
        """

    if pattern == "하락장악형":

        return """
        <div class="candle-pattern-visual">
            <div class="mini-candle bullish">
                <span class="wick"></span><span class="body"></span>
            </div>
            <div class="mini-candle bearish large">
                <span class="wick"></span><span class="body"></span>
            </div>
        </div>
        """

    if pattern == "양수도지":

        return """
        <div class="candle-pattern-visual">
            <div class="mini-candle bullish-doji">
                <span class="wick"></span><span class="body"></span>
            </div>
        </div>
        """

    if pattern == "음수도지":

        return """
        <div class="candle-pattern-visual">
            <div class="mini-candle bearish-doji">
                <span class="wick"></span><span class="body"></span>
            </div>
        </div>
        """

    if pattern == "상승관통형":

        return """
        <div class="candle-pattern-visual">
            <div class="mini-candle bearish">
                <span class="wick"></span><span class="body"></span>
            </div>
            <div class="mini-candle bullish piercing-candle">
                <span class="wick"></span><span class="body"></span>
            </div>
        </div>
        """

    if pattern == "하락관통형":

        return """
        <div class="candle-pattern-visual">
            <div class="mini-candle bullish">
                <span class="wick"></span><span class="body"></span>
            </div>
            <div class="mini-candle bearish piercing-bearish">
                <span class="wick"></span><span class="body"></span>
            </div>
        </div>
        """

    if pattern == "장대양봉":

        return """
        <div class="candle-pattern-visual">
            <div class="mini-candle bullish very-large">
                <span class="wick"></span><span class="body"></span>
            </div>
        </div>
        """

    if pattern == "장대음봉":

        return """
        <div class="candle-pattern-visual">
            <div class="mini-candle bearish very-large">
                <span class="wick"></span><span class="body"></span>
            </div>
        </div>
        """

    if pattern == "일반양봉":

        return """
        <div class="candle-pattern-visual">
            <div class="mini-candle bullish">
                <span class="wick"></span><span class="body"></span>
            </div>
        </div>
        """

    if pattern == "일반음봉":

        return """
        <div class="candle-pattern-visual">
            <div class="mini-candle bearish">
                <span class="wick"></span><span class="body"></span>
            </div>
        </div>
        """

    if pattern in ("상승망치형", "하락망치형"):

        candle_class = (
            "bullish hammer"
            if pattern == "상승망치형"
            else "bearish hammer"
        )

        return f"""
        <div class="candle-pattern-visual">
            <div class="mini-candle {candle_class}">
                <span class="wick"></span><span class="body"></span>
            </div>
        </div>
        """

    if pattern in ("상승역망치형", "하락역망치형"):

        candle_class = (
            "bullish inverted-hammer"
            if pattern == "상승역망치형"
            else "bearish inverted-hammer"
        )

        return f"""
        <div class="candle-pattern-visual">
            <div class="mini-candle {candle_class}">
                <span class="wick"></span><span class="body"></span>
            </div>
        </div>
        """

    return """
    <div class="candle-pattern-visual empty">—</div>
    """


# =========================================================
# 캔들 패턴 HTML
# =========================================================

def current_pattern_html(pattern, candle=None):

    if isinstance(pattern, str):
        patterns = [pattern] if pattern else []

    elif isinstance(pattern, (list, tuple, set)):
        patterns = list(pattern)

    else:
        patterns = []

    if not patterns:

        if candle is not None:

            try:

                o = float(candle["open"])
                c = float(candle["close"])

                if c > o:
                    ordinary_pattern = "일반양봉"
                    ordinary_name = "양봉"

                elif c < o:
                    ordinary_pattern = "일반음봉"
                    ordinary_name = "음봉"

                else:
                    ordinary_pattern = "일반양봉"
                    ordinary_name = "보합"

                return (
                    '<div class="pattern-box neutral-pattern">'
                    + candle_icon_html(ordinary_pattern)
                    + '<span class="pattern-name">'
                    + ordinary_name
                    + '</span></div>'
                )

            except Exception:
                pass

        return (
            '<div class="pattern-box pattern-none">'
            + candle_icon_html(None)
            + '<span class="pattern-name">-</span></div>'
        )

    boxes = []

    for item in patterns:

        escaped_pattern = html.escape(str(item))

        if (
            item in BULLISH_HIGHLIGHT_PATTERNS
            or item == "장대양봉"
        ):
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
# 최근 24개 봉 표시
# =========================================================

def last_three_pattern_html(row):

    last_three = row.get("last_three_patterns") or []

    missing = max(0, DISPLAY_CANDLE_COUNT - len(last_three))

    items = []

    for _ in range(missing):

        items.append({
            "label": "",
            "patterns": [],
            "open": None,
            "high": None,
            "low": None,
            "close": None,
            "change": None,
            "rsi": None,
            "rsi_previous": None,
            "rsi_signal_type": None,
            "active": False
        })

    items.extend(last_three)

    boxes = []

    visible_items = items[-DISPLAY_CANDLE_COUNT:]

    for i, item in enumerate(visible_items):

        patterns = item.get("patterns", [])
        raw_label = item.get("label") or "-"

        if raw_label != "-":

            label_parts = raw_label.split(" ", 1)

            date_label = html.escape(label_parts[0])

            time_label = (
                html.escape(label_parts[1])
                if len(label_parts) > 1
                else ""
            )

        else:

            date_label = "-"
            time_label = ""

        live_html = ""

        if i == len(visible_items) - 1 and raw_label != "-":
            live_html = (
                '<span class="candle-live">진행</span>'
            )

        label = f"""
        <div class="candle-date">
            {date_label}
        </div>

        <div class="candle-time">
            {time_label}
            {live_html}
        </div>
        """

        candle_html = current_pattern_html(
            patterns,
            item
        )

        change_display = candle_change_html(
            item.get("change")
        )

        rsi_value = item.get("rsi")
        rsi_signal_type = item.get("rsi_signal_type")

        rsi_class = ""

        if rsi_signal_type == "RSI70_UP":
            rsi_class = "rsi-cross-up-card"

        elif rsi_signal_type == "RSI70_DOWN":
            rsi_class = "rsi-cross-down-card"

        boxes.append(
            f"""
            <div class="three-candle-item {rsi_class}">

                <div class="three-candle-label">
                    {label}
                </div>

                <div class="three-candle-pattern">
                    {candle_html}
                </div>

                <div class="three-candle-change">
                    {change_display}
                </div>

                <div class="three-candle-rsi">
                    {rsi_html(rsi_value, rsi_signal_type)}
                </div>

            </div>
            """
        )

    return (
        '<div class="three-candle-list">'
        + "".join(boxes)
        + '</div>'
    )


# =========================================================
# 연속 장대봉 배지
# =========================================================

def consecutive_candle_badges_html(row):

    bullish_count = int(
        row.get("consecutive_bullish", 0) or 0
    )

    bearish_count = int(
        row.get("consecutive_bearish", 0) or 0
    )

    badges = []

    if bullish_count >= CONSECUTIVE_CANDLE_MIN:

        badges.append(
            '<span class="consecutive-bullish">'
            f'{bullish_count}연속 장대양봉'
            '</span>'
        )

    if bearish_count >= CONSECUTIVE_CANDLE_MIN:

        badges.append(
            '<span class="consecutive-bearish">'
            f'{bearish_count}연속 장대음봉'
            '</span>'
        )

    return "".join(badges)


# =========================================================
# 패턴 배지
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
            '<span class="signal-type-badge">상승패턴</span>'
        )

    if any(
        pattern in BEARISH_HIGHLIGHT_PATTERNS
        for pattern in all_patterns
    ):

        badges.append(
            '<span class="bearish-type-badge">하락패턴</span>'
        )

    badges.append(
        consecutive_candle_badges_html(row)
    )

    return "".join(badges)


# =========================================================
# BTC HTML
# =========================================================

def btc_html():

    btc_row = {
        "last_three_patterns": latest_btc_last_three_patterns
    }

    three_pattern_html = last_three_pattern_html(
        btc_row
    )

    timeframe_label = get_timeframe_label(
        CANDLE_PATTERN_TIMEFRAME
    )

    return f"""
    <section class="btc-panel">

        <div class="btc-top">

            <div class="btc-label">

                <span class="status-dot"></span>

                BTCUSDT

                <span class="market-tag">OKX</span>

            </div>

            <span class="update-time">
                KST 기준 · {kst()}
            </span>

        </div>

        <div class="btc-main">

            <div class="btc-price">
                {fmt_price(latest_btc_okx_price)}
            </div>

            <div class="btc-change">
                {fmt_change(latest_btc_daily_change)}
            </div>

        </div>

        <div class="btc-candle-section">

            <div class="btc-candle-heading">
                BTC 최근 24개 봉 · {timeframe_label} · RSI({RSI_PERIOD})
                <span class="rsi-condition-label">
                    RSI70 상향돌파 녹색 · RSI70 하향돌파 적색
                </span>
            </div>

            {three_pattern_html}

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

    badge_html = pattern_badges_html(row)

    three_pattern_html = last_three_pattern_html(row)

    return f"""
    <div class="signal-card">

        <div class="signal-card-top">

            <div class="signal-identity">

                <span class="signal-rank">
                    #{row.get("volume_rank", "-")}
                </span>

                <b class="signal-coin-name">
                    {html.escape(row["name"])}
                </b>

            </div>

            <div class="signal-type-list">
                {badge_html}
            </div>

        </div>

        <div class="signal-data-grid">

            <div>
                <span>현재가</span>
                <strong>
                    {fmt_price(row.get("current_price"))}
                </strong>
            </div>

            <div>
                <span>거래대금</span>
                <strong>
                    {fmt_vol(row["volume_24h"])}
                </strong>
            </div>

            <div>
                <span>변동률</span>
                <strong>
                    {fmt_change(row.get("daily_change"))}
                </strong>
            </div>

        </div>

        <div class="three-candle-section">

            <div class="three-candle-heading">
                최근 24개 봉 · {timeframe_label} · RSI({RSI_PERIOD})
                <span class="rsi-condition-label">
                    RSI70 상향돌파 녹색 · RSI70 하향돌파 적색
                </span>
            </div>

            {three_pattern_html}

        </div>

    </div>
    """


# =========================================================
# SIGNAL 영역
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

    condition_html = f"""
    <div class="signal-condition">

        <span>선정 기준: 당일 변동률</span>
        <span>당일 변동률 {DAILY_CHANGE_MIN:.2f}% 초과</span>
        <span>최근 {RSI_CHECK_CANDLES}개 봉 RSI70 상향돌파+상승캔들 또는 RSI70 하향돌파+하락캔들</span>
        <span>최신 RSI {RSI_RANGE_MIN}~{RSI_RANGE_MAX}</span>
        <span>{html.escape(get_ema_condition_text())}</span>
        <span>EMA 및 RSI 시간봉: {pattern_timeframe_label}</span>
        <span>BTC 거래대금 비교 없음</span>
        <span>24H 거래대금 높은 순 정렬</span>

    </div>
    """

    return f"""
    <section class="signal-panel">

        <div class="panel-heading">

            <div class="heading-title">
                <span class="heading-mark signal-mark"></span>
                SIGNAL
                <span class="heading-count">
                    {len(latest_signal_data)}
                </span>
            </div>

            <span class="update-time">
                {pattern_timeframe_label} · {kst()}
            </span>

        </div>

        {condition_html}

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

    three_pattern_html = last_three_pattern_html(row)

    return f"""
    <article class="coin-card">

        <div class="coin-head">

            <div class="coin-title">

                <span class="rank">
                    #{row["rank"]}
                </span>

                <b>{html.escape(row["name"])}</b>

            </div>

            <div class="pattern-badge-list">
                {status}
            </div>

        </div>

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
                <strong>
                    {fmt_change(row.get("daily_change"))}
                </strong>
            </div>

        </div>

        <div class="top-three-candle-section">

            <div class="three-candle-heading">
                최근 24개 봉 · {timeframe_label} · RSI({RSI_PERIOD})
                <span class="rsi-condition-label">
                    RSI70 상향돌파 녹색 · RSI70 하향돌파 적색
                </span>
            </div>

            {three_pattern_html}

        </div>

    </article>
    """
