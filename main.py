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

TOP_N = 50
SHOW_TOP_LIST = "N"
UPDATE_MINUTES = 1

DAILY_CHANGE_MIN = 0.0

USE_BTC_VOLUME_FILTER = "N"

USE_UPBIT = "Y"
USE_OKX = "Y"

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 20

CANDLE_PATTERN_TIMEFRAME = "4h"

RSI_PERIOD = 14


# =========================================================
# RSI 설정
# =========================================================

RSI_SIGNAL_MIN = 30
RSI_SIGNAL_MAX = 70

RSI_RANGE_MIN = 30
RSI_RANGE_MAX = 70

RSI_CHECK_CANDLES = 14


# =========================================================
# EMA 설정
# =========================================================

EMA_FAST_PERIOD = 5
EMA_MIDDLE_PERIOD = 10
EMA_SLOW_PERIOD = 20
EMA_LONG_PERIOD = 50

USE_EMA_5 = "N"
USE_EMA_10 = "Y"
USE_EMA_20 = "Y"
USE_EMA_50 = "Y"

# EMA 배열 방향 사용 설정
#
# Y / N : 정배열만 사용
# N / Y : 역배열만 사용
# Y / Y : 정배열 또는 역배열 중 하나만 완성되어도 통과
# N / N : EMA 배열 조건 사용 안 함

USE_EMA_BULLISH_ALIGNMENT = "Y"
USE_EMA_BEARISH_ALIGNMENT = "Y"


# =========================================================
# SIGNAL 체크리스트 개별 사용 설정
# =========================================================

USE_SIGNAL_DAILY_CHANGE = "Y"
USE_SIGNAL_RSI_RANGE = "Y"
USE_SIGNAL_RSI_CROSS = "N"
USE_SIGNAL_EMA_ALIGNMENT = "Y"
USE_SIGNAL_EMA_BEARISH_BULLISH = "Y"

SHOW_SIGNAL_CHECKLIST = "Y"


# =========================================================
# EMA 1 / 3 데드크로스 설정
# =========================================================

EMA_CROSS_FAST_PERIOD = 1
EMA_CROSS_SLOW_PERIOD = 3

DISPLAY_CANDLE_COUNT = 6

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
        return "EMA 배열 조건 사용 안 함"

    if len(enabled) == 1:
        return f"{enabled[0]} 단독 사용"

    conditions = []

    if USE_EMA_BULLISH_ALIGNMENT == "Y":

        conditions.append(
            f"{' > '.join(enabled)} 정배열"
        )

    if USE_EMA_BEARISH_ALIGNMENT == "Y":

        conditions.append(
            f"{' < '.join(enabled)} 역배열"
        )

    if not conditions:
        return "EMA 배열 조건 사용 안 함"

    return " 또는 ".join(conditions)


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

latest_btc_rsi_values = []

latest_btc_ema5 = None
latest_btc_ema10 = None
latest_btc_ema20 = None
latest_btc_ema50 = None


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
# EMA 배열 방향 판정 공통 함수
# =========================================================

def check_ema_alignment_values(values):

    if USE_EMA_BULLISH_ALIGNMENT != "Y" and USE_EMA_BEARISH_ALIGNMENT != "Y":
        return True

    if values is None or len(values) < 2:
        return True

    try:

        numeric_values = [
            float(value)
            for value in values
        ]

    except (TypeError, ValueError):
        return False

    if any(
        pd.isna(value)
        for value in numeric_values
    ):
        return False

    bullish_alignment = all(
        numeric_values[i] > numeric_values[i + 1]
        for i in range(len(numeric_values) - 1)
    )

    bearish_alignment = all(
        numeric_values[i] < numeric_values[i + 1]
        for i in range(len(numeric_values) - 1)
    )

    allowed_bullish = (
        USE_EMA_BULLISH_ALIGNMENT == "Y"
        and bullish_alignment
    )

    allowed_bearish = (
        USE_EMA_BEARISH_ALIGNMENT == "Y"
        and bearish_alignment
    )

    return bool(
        allowed_bullish or allowed_bearish
    )


# =========================================================
# EMA 1 / 3 데드크로스 및 역배열 유지 판정
# =========================================================

def apply_ema_dead_cross_signals(periods, open_series):

    if not periods or open_series is None:
        return periods

    ema_fast = calculate_ema(
        open_series,
        EMA_CROSS_FAST_PERIOD
    )

    ema_slow = calculate_ema(
        open_series,
        EMA_CROSS_SLOW_PERIOD
    )

    bearish_state = False

    for i, period in enumerate(periods):

        period["ema_dead_cross"] = False
        period["ema_dead_cross_progress"] = False
        period["ema_bearish_alignment"] = False

        if i >= len(ema_fast) or i >= len(ema_slow):
            continue

        current_fast = ema_fast.iloc[i]
        current_slow = ema_slow.iloc[i]

        if (
            pd.isna(current_fast)
            or pd.isna(current_slow)
        ):
            bearish_state = False
            continue

        current_fast = float(current_fast)
        current_slow = float(current_slow)

        if i == 0:
            continue

        previous_fast = ema_fast.iloc[i - 1]
        previous_slow = ema_slow.iloc[i - 1]

        if (
            pd.isna(previous_fast)
            or pd.isna(previous_slow)
        ):
            bearish_state = False
            continue

        previous_fast = float(previous_fast)
        previous_slow = float(previous_slow)

        if (
            previous_fast >= previous_slow
            and current_fast < current_slow
        ):

            bearish_state = True

        elif current_fast >= current_slow:

            bearish_state = False

        if (
            bearish_state
            and current_fast < current_slow
        ):

            period["ema_dead_cross"] = True
            period["ema_bearish_alignment"] = True

            period["ema_dead_cross_progress"] = bool(
                period.get("active", False)
            )

        else:

            period["ema_dead_cross"] = False
            period["ema_bearish_alignment"] = False
            period["ema_dead_cross_progress"] = False

    return periods


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
            "rsi_signal_type": period.get("rsi_signal_type"),
            "ema_dead_cross": period.get("ema_dead_cross", False),
            "ema_dead_cross_progress": period.get(
                "ema_dead_cross_progress",
                False
            ),
            "ema_bearish_alignment": period.get(
                "ema_bearish_alignment",
                False
            )
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

        if (
            previous_rsi < RSI_SIGNAL_MIN
            and current_rsi >= RSI_SIGNAL_MIN
            and close_price > open_price
        ):

            period["rsi_signal_type"] = "RSI70_UP"

        elif (
            previous_rsi > RSI_SIGNAL_MAX
            and current_rsi <= RSI_SIGNAL_MAX
            and close_price < open_price
        ):

            period["rsi_signal_type"] = "RSI70_DOWN"

    return periods


# =========================================================
# 확정봉 EMA 정배열 또는 역배열 판정
# =========================================================

def get_ema_alignment_at_index(
    ema_series_list,
    index
):

    if USE_EMA_BULLISH_ALIGNMENT != "Y" and USE_EMA_BEARISH_ALIGNMENT != "Y":
        return True

    if len(ema_series_list) < 2:
        return True

    values = []

    for series in ema_series_list:

        if index < 0 or index >= len(series):
            return None

        value = series.iloc[index]

        if pd.isna(value):
            return None

        values.append(float(value))

    return check_ema_alignment_values(values)


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

    periods = apply_ema_dead_cross_signals(
        periods,
        df["open"]
    )

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

    last_rsi_periods = periods[-DISPLAY_CANDLE_COUNT:]

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

    enabled_emas = []

    if USE_EMA_5 == "Y":
        enabled_emas.append(ema5)

    if USE_EMA_10 == "Y":
        enabled_emas.append(ema10)

    if USE_EMA_20 == "Y":
        enabled_emas.append(ema20)

    if USE_EMA_50 == "Y":
        enabled_emas.append(ema50)

    if (
        USE_EMA_BULLISH_ALIGNMENT != "Y"
        and USE_EMA_BEARISH_ALIGNMENT != "Y"
    ):

        ema_alignment = True

    elif len(enabled_emas) < 2:

        ema_alignment = True

    elif any(
        value is None
        for value in enabled_emas
    ):

        ema_alignment = False

    else:

        ema_alignment = check_ema_alignment_values(
            enabled_emas
        )

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

        item["ema_dead_cross"] = period.get(
            "ema_dead_cross",
            False
        )

        item["ema_dead_cross_progress"] = period.get(
            "ema_dead_cross_progress",
            False
        )

        item["ema_bearish_alignment"] = period.get(
            "ema_bearish_alignment",
            False
        )

    current_signal = pattern_info[
        "current_bullish_pattern"
    ]

    previous_signal = pattern_info[
        "previous_bullish_pattern"
    ]

    pattern_signal = bool(
        current_signal or previous_signal
    )

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
# SIGNAL 조건 공통 판정
# =========================================================

def get_signal_condition_checks(row):

    checks = {}

    daily_change = row.get("daily_change")

    if daily_change is None:

        checks["daily_change"] = None

    else:

        try:

            daily_change = float(daily_change)

            checks["daily_change"] = (
                daily_change > DAILY_CHANGE_MIN
            )

        except (TypeError, ValueError):

            checks["daily_change"] = None

    rsi_values = row.get("rsi_values") or []

    latest_rsi = None

    if rsi_values:

        try:

            candidate = rsi_values[-1]

            if candidate is not None:
                candidate = float(candidate)

                if pd.notna(candidate):
                    latest_rsi = candidate

        except (TypeError, ValueError):
            latest_rsi = None

    if latest_rsi is None:

        checks["rsi_range"] = None

    else:

        checks["rsi_range"] = bool(
            RSI_RANGE_MIN
            <= latest_rsi
            <= RSI_RANGE_MAX
        )

    enabled_ema_values = []

    if USE_EMA_5 == "Y":
        enabled_ema_values.append(row.get("ema5"))

    if USE_EMA_10 == "Y":
        enabled_ema_values.append(row.get("ema10"))

    if USE_EMA_20 == "Y":
        enabled_ema_values.append(row.get("ema20"))

    if USE_EMA_50 == "Y":
        enabled_ema_values.append(row.get("ema50"))

    if (
        USE_EMA_BULLISH_ALIGNMENT != "Y"
        and USE_EMA_BEARISH_ALIGNMENT != "Y"
    ):

        checks["ema_alignment"] = True

    elif len(enabled_ema_values) < 2:

        checks["ema_alignment"] = True

    elif any(value is None for value in enabled_ema_values):

        checks["ema_alignment"] = None

    else:

        try:

            checks["ema_alignment"] = check_ema_alignment_values(
                enabled_ema_values
            )

        except (TypeError, ValueError):

            checks["ema_alignment"] = None

    candles = row.get("last_three_patterns")

    if not candles:

        checks["ema_bearish_bullish_candle"] = None

    else:

        found = False
        valid_candle_count = 0

        for candle in candles:

            try:

                open_price = float(candle["open"])
                close_price = float(candle["close"])

                bearish_alignment = candle.get(
                    "ema_bearish_alignment"
                )

                if bearish_alignment is None:
                    continue

                valid_candle_count += 1

                if (
                    bearish_alignment is True
                    and close_price > open_price
                ):

                    found = True
                    break

            except (KeyError, TypeError, ValueError):
                continue

        if found:

            checks["ema_bearish_bullish_candle"] = True

        elif valid_candle_count > 0:

            checks["ema_bearish_bullish_candle"] = False

        else:

            checks["ema_bearish_bullish_candle"] = None

    candles = row.get("last_three_patterns") or []

    if not candles:

        checks["rsi_cross_signal"] = None

    else:

        valid_rsi_count = 0
        found_rsi_cross = False

        for candle in candles[-DISPLAY_CANDLE_COUNT:]:

            current_rsi = candle.get("rsi")

            if current_rsi is None:
                continue

            try:

                current_rsi = float(current_rsi)

                if not pd.notna(current_rsi):
                    continue

            except (TypeError, ValueError):
                continue

            valid_rsi_count += 1

            if candle.get("rsi_signal_type") in (
                "RSI70_UP",
                "RSI70_DOWN"
            ):

                found_rsi_cross = True
                break

        if found_rsi_cross:

            checks["rsi_cross_signal"] = True

        elif valid_rsi_count > 0:

            checks["rsi_cross_signal"] = False

        else:

            checks["rsi_cross_signal"] = None

    return checks


# =========================================================
# 체크리스트 HTML
# =========================================================

def signal_checklist_html(row):

    if SHOW_SIGNAL_CHECKLIST != "Y":
        return ""

    checks = get_signal_condition_checks(row)

    checklist_items = [
        (
            "daily_change",
            f"당일 변동률 > {DAILY_CHANGE_MIN:.2f}%",
            USE_SIGNAL_DAILY_CHANGE
        ),
        (
            "rsi_range",
            f"최신 RSI {RSI_RANGE_MIN}~{RSI_RANGE_MAX}",
            USE_SIGNAL_RSI_RANGE
        ),
        (
            "rsi_cross_signal",
            f"최근 표시 {DISPLAY_CANDLE_COUNT}개 봉 중 RSI {RSI_SIGNAL_MIN} 상향 또는 RSI {RSI_SIGNAL_MAX} 하향돌파",
            USE_SIGNAL_RSI_CROSS
        )
    ]

    ema_enabled = any([
        USE_EMA_5 == "Y",
        USE_EMA_10 == "Y",
        USE_EMA_20 == "Y",
        USE_EMA_50 == "Y"
    ])

    if ema_enabled:

        checklist_items.append(
            (
                "ema_alignment",
                get_ema_condition_text(),
                USE_SIGNAL_EMA_ALIGNMENT
            )
        )

    checklist_items.append(
        (
            "ema_bearish_bullish_candle",
            f"최근 {DISPLAY_CANDLE_COUNT}개 봉 중 EMA{EMA_CROSS_FAST_PERIOD}/{EMA_CROSS_SLOW_PERIOD} 역배열 양봉",
            USE_SIGNAL_EMA_BEARISH_BULLISH
        )
    )

    checklist_items = [
        item
        for item in checklist_items
        if item[2] == "Y"
    ]

    if not checklist_items:
        return ""

    items_html = []

    for key, label, enabled in checklist_items:

        passed = checks.get(key)

        if passed is True:

            symbol = "✓"
            status_class = "check-pass"
            status_text = "통과"

        elif passed is False:

            symbol = "✕"
            status_class = "check-fail"
            status_text = "미통과"

        else:

            symbol = "-"
            status_class = "check-unknown"
            status_text = "확인불가"

        items_html.append(
            f"""
            <div class="checklist-item">

                <span class="checklist-symbol {status_class}">
                    {symbol}
                </span>

                <span class="checklist-label">
                    {html.escape(label)}
                </span>

                <span class="checklist-status {status_class}">
                    {status_text}
                </span>

            </div>
            """
        )

    return (
        '<div class="signal-checklist">'
        '<div class="checklist-heading">조건 체크리스트</div>'
        '<div class="checklist-grid">'
        + "".join(items_html)
        + '</div>'
        '</div>'
    )


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

    latest_signal_data = []

    signal_condition_keys = []

    if USE_SIGNAL_DAILY_CHANGE == "Y":
        signal_condition_keys.append("daily_change")

    if USE_SIGNAL_RSI_RANGE == "Y":
        signal_condition_keys.append("rsi_range")

    if USE_SIGNAL_EMA_ALIGNMENT == "Y":
        signal_condition_keys.append("ema_alignment")

    if USE_SIGNAL_EMA_BEARISH_BULLISH == "Y":
        signal_condition_keys.append("ema_bearish_bullish_candle")

    if USE_SIGNAL_RSI_CROSS == "Y":
        signal_condition_keys.append("rsi_cross_signal")

    for row in rows:

        checks = get_signal_condition_checks(row)

        if all(
            checks.get(key) is True
            for key in signal_condition_keys
        ):

            latest_signal_data.append(row)

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

    active_condition_names = []

    if USE_SIGNAL_DAILY_CHANGE == "Y":
        active_condition_names.append(
            f"당일 변동률>{DAILY_CHANGE_MIN}%"
        )

    if USE_SIGNAL_RSI_RANGE == "Y":
        active_condition_names.append(
            f"최신 RSI {RSI_RANGE_MIN}~{RSI_RANGE_MAX}"
        )

    if USE_SIGNAL_EMA_ALIGNMENT == "Y":
        active_condition_names.append(
            get_ema_condition_text()
        )

    if USE_SIGNAL_EMA_BEARISH_BULLISH == "Y":
        active_condition_names.append(
            f"최근 {DISPLAY_CANDLE_COUNT}개 봉 중 EMA{EMA_CROSS_FAST_PERIOD}/{EMA_CROSS_SLOW_PERIOD} 역배열 양봉"
        )

    if USE_SIGNAL_RSI_CROSS == "Y":
        active_condition_names.append(
            f"최근 {DISPLAY_CANDLE_COUNT}개 봉 중 RSI{RSI_SIGNAL_MIN} 상향 또는 RSI{RSI_SIGNAL_MAX} 하향돌파"
        )

    if active_condition_names:

        signal_condition_log = " AND ".join(
            active_condition_names
        )

    else:

        signal_condition_log = "활성화된 SIGNAL 조건 없음 (TOP 종목 전체 선정)"

    log.info(
        "UPBIT | TOP%s | SIGNAL 조건=%s | "
        "시간봉=%s | SIGNAL=%s | 정렬=24H 거래대금 순",
        TOP_N,
        signal_condition_log,
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

    periods = apply_ema_dead_cross_signals(
        periods,
        df["open"]
    )

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

        item["ema_dead_cross"] = period.get(
            "ema_dead_cross",
            False
        )

        item["ema_dead_cross_progress"] = period.get(
            "ema_dead_cross_progress",
            False
        )

        item["ema_bearish_alignment"] = period.get(
            "ema_bearish_alignment",
            False
        )

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

    global latest_btc_rsi_values

    global latest_btc_ema5
    global latest_btc_ema10
    global latest_btc_ema20
    global latest_btc_ema50

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

        latest_btc_rsi_values = []

        latest_btc_ema5 = None
        latest_btc_ema10 = None
        latest_btc_ema20 = None
        latest_btc_ema50 = None

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

    rsi_series = calculate_rsi(
        df["close"],
        RSI_PERIOD
    )

    for i, period in enumerate(periods):

        if i < len(rsi_series):

            value = rsi_series.iloc[i]

            period["rsi"] = (
                float(value)
                if pd.notna(value)
                else None
            )

        else:

            period["rsi"] = None

    periods = apply_rsi_cross_signals(periods)

    rsi_periods = periods[-DISPLAY_CANDLE_COUNT:]

    latest_btc_rsi_values = [
        period.get("rsi")
        for period in rsi_periods
    ]

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

    def get_latest_ema(series):

        if len(series) == 0:
            return None

        value = series.iloc[-1]

        if pd.isna(value):
            return None

        return float(value)

    latest_btc_ema5 = get_latest_ema(
        ema5_series
    )

    latest_btc_ema10 = get_latest_ema(
        ema10_series
    )

    latest_btc_ema20 = get_latest_ema(
        ema20_series
    )

    latest_btc_ema50 = get_latest_ema(
        ema50_series
    )

    btc_pattern_periods = apply_ema_dead_cross_signals(
        periods,
        df["open"]
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
        "당일 변동률=%s | RSI=%s | "
        "EMA5=%s | EMA10=%s | EMA20=%s | EMA50=%s",
        price,
        get_timeframe_label(timeframe),
        latest_btc_daily_change,
        latest_btc_rsi_values[-1] if latest_btc_rsi_values else None,
        latest_btc_ema5,
        latest_btc_ema10,
        latest_btc_ema20,
        latest_btc_ema50
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

    if value <= 50:
        color_class = "rsi-low"

    elif value < 70:
        color_class = "rsi-middle"

    else:
        color_class = "rsi-high"

    signal_text = ""

    if signal_type == "RSI70_UP":
        signal_text = " ↑50"

    elif signal_type == "RSI70_DOWN":
        signal_text = " ↓70"

    return (
        f'<div class="rsi-value {color_class}">'
        f'RSI {value:.1f}{signal_text}'
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
# 최근 캔들 표시
# =========================================================

def last_three_pattern_html(row):

    last_three = row.get("last_three_patterns") or []

    # 실제 데이터가 있는 캔들만 표시
    visible_items = last_three[-DISPLAY_CANDLE_COUNT:]

    if not visible_items:
        return (
            '<div class="three-candle-list" '
            'style="grid-template-columns:repeat(1,minmax(0,1fr));">'
            '</div>'
        )

    boxes = []

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

        card_classes = []

        if rsi_signal_type == "RSI70_UP":
            card_classes.append("rsi-cross-up-card")

        elif rsi_signal_type == "RSI70_DOWN":
            card_classes.append("rsi-cross-down-card")

        try:

            is_positive_candle = (
                float(item.get("close", 0))
                > float(item.get("open", 0))
            )

        except (TypeError, ValueError):
            is_positive_candle = False

        if (
            item.get("ema_bearish_alignment", False)
            and is_positive_candle
        ):

            card_classes.append("ema-dead-cross-card")

        if (
            item.get("ema_dead_cross_progress", False)
            and item.get("ema_bearish_alignment", False)
            and is_positive_candle
        ):

            card_classes.append("ema-dead-cross-progress")

        card_class = " ".join(card_classes)

        boxes.append(
            f"""
            <div class="three-candle-item {card_class}">

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

    # 실제 표시 캔들 수에 맞춰 열 개수를 자동 조정
    candle_count = max(1, len(visible_items))

    return (
        f'<div class="three-candle-list" '
        f'style="grid-template-columns:repeat({candle_count},minmax(0,1fr));">'
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
        "daily_change": latest_btc_daily_change,
        "rsi_values": latest_btc_rsi_values,

        "ema5": latest_btc_ema5,
        "ema10": latest_btc_ema10,
        "ema20": latest_btc_ema20,
        "ema50": latest_btc_ema50,

        "last_three_patterns": latest_btc_last_three_patterns
    }

    three_pattern_html = last_three_pattern_html(
        btc_row
    )

    btc_checklist_html = signal_checklist_html(
        btc_row
    )

    if btc_checklist_html:

        btc_checklist_section_html = f"""
        <div class="btc-checklist-section">

            <div class="btc-checklist-heading">
                BTC 시황 체크리스트
                <span>거래대금 조건 제외</span>
            </div>

            {btc_checklist_html}

        </div>
        """

    else:

        btc_checklist_section_html = ""

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

        {btc_checklist_section_html}

        <div class="btc-candle-section">

            <div class="btc-candle-heading">
                BTC 최근 {DISPLAY_CANDLE_COUNT}개 봉 · {timeframe_label} · RSI({RSI_PERIOD})
                <span class="rsi-condition-label">
                    RSI50 상향돌파 녹색 · RSI70 하향돌파 적색 · EMA{EMA_CROSS_FAST_PERIOD}/{EMA_CROSS_SLOW_PERIOD} 역배열 유지 중 양봉 노란색
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

    checklist_html = signal_checklist_html(row)

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

        {checklist_html}

        <div class="three-candle-section">

            <div class="three-candle-heading">
                최근 {DISPLAY_CANDLE_COUNT}개 봉 · {timeframe_label} · RSI({RSI_PERIOD})
                <span class="rsi-condition-label">
                    RSI50 상향돌파 녹색 · RSI70 하향돌파 적색 · EMA{EMA_CROSS_FAST_PERIOD}/{EMA_CROSS_SLOW_PERIOD} 역배열 유지 중 양봉 노란색
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

    active_conditions = []

    if USE_SIGNAL_DAILY_CHANGE == "Y":
        active_conditions.append(
            f"당일 변동률 {DAILY_CHANGE_MIN:.2f}% 초과"
        )

    if USE_SIGNAL_RSI_RANGE == "Y":
        active_conditions.append(
            f"최신 RSI {RSI_RANGE_MIN}~{RSI_RANGE_MAX}"
        )

    if USE_SIGNAL_EMA_ALIGNMENT == "Y":
        active_conditions.append(
            get_ema_condition_text()
        )

    if USE_SIGNAL_EMA_BEARISH_BULLISH == "Y":
        active_conditions.append(
            f"최근 {DISPLAY_CANDLE_COUNT}개 봉 중 EMA{EMA_CROSS_FAST_PERIOD}/{EMA_CROSS_SLOW_PERIOD} 역배열 양봉 1개 이상"
        )

    if USE_SIGNAL_RSI_CROSS == "Y":
        active_conditions.append(
            f"최근 {DISPLAY_CANDLE_COUNT}개 봉 중 RSI{RSI_SIGNAL_MIN} 상향 또는 RSI{RSI_SIGNAL_MAX} 하향돌파"
        )

    condition_items = []

    if active_conditions:

        condition_items.append(
            "<span>활성화된 조건 모두 통과 시 SIGNAL 선정</span>"
        )

        for condition in active_conditions:

            condition_items.append(
                f"<span>{html.escape(condition)}</span>"
            )

    else:

        condition_items.append(
            "<span>활성화된 SIGNAL 조건 없음 (TOP 종목 전체 선정)</span>"
        )

    condition_items.append(
        f"<span>시간봉: {pattern_timeframe_label}</span>"
    )

    condition_html = (
        '<div class="signal-condition">'
        + "".join(condition_items)
        + '</div>'
    )

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

    checklist_html = signal_checklist_html(row)

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

        {checklist_html}

        <div class="top-three-candle-section">

            <div class="three-candle-heading">
                최근 {DISPLAY_CANDLE_COUNT}개 봉 · {timeframe_label} · RSI({RSI_PERIOD})
                <span class="rsi-condition-label">
                    RSI50 상향돌파 녹색 · RSI70 하향돌파 적색 · EMA{EMA_CROSS_FAST_PERIOD}/{EMA_CROSS_SLOW_PERIOD} 역배열 유지 중 양봉 노란색
                </span>
            </div>

            {three_pattern_html}

        </div>

    </article>
    """


# =========================================================
# CSS
# =========================================================

CSS = """
:root {
color-scheme: dark;
--bg:#080d13;
--panel:#101820;
--panel2:#131e28;
--panel3:#0d151d;
--line:#263542;
--line-soft:#1d2a35;
--text:#e5edf4;
--muted:#8495a5;
--green:#38d996;
--green-soft:rgba(56,217,150,.10);
--red:#ff6575;
--red-soft:rgba(255,101,117,.10);
--yellow:#e8c56e;
--fs-main:17px;
--fs-section:13px;
--fs-coin:14px;
--fs-value:12px;
--fs-content:11px;
--fs-small:10px;
--fs-tiny:9px;
}

* { box-sizing:border-box; }

html {
min-height:100%;
background:var(--bg);
-webkit-text-size-adjust:100%;
}

body {
width:100%;
max-width:1000px;
min-height:100vh;
margin:0 auto;
padding:12px;
background:var(--bg);
color:var(--text);
font-family:-apple-system,BlinkMacSystemFont,"Noto Sans KR","Malgun Gothic",Arial,sans-serif;
font-size:var(--fs-content);
line-height:1.4;
letter-spacing:-.2px;
}

h1 {
margin:2px 0 11px;
padding:0 2px 10px;
font-size:var(--fs-main);
font-weight:900;
letter-spacing:-.5px;
color:#f2f6fa;
border-bottom:1px solid var(--line);
}

section { margin-bottom:11px; }

.up { color:var(--green)!important; font-weight:800; }
.down { color:var(--red)!important; font-weight:800; }
.zero { color:#8795a2; font-weight:700; }

.pattern-box {
min-width:45px;
display:flex;
flex-direction:column;
align-items:center;
justify-content:center;
line-height:1.15;
}

.pattern-name {
display:block;
margin-top:3px;
color:var(--yellow);
font-size:var(--fs-tiny);
font-weight:800;
white-space:nowrap;
}

.bullish-pattern .pattern-name { color:var(--green); }
.bearish-pattern .pattern-name { color:var(--red); }
.neutral-pattern .pattern-name { color:var(--yellow); }
.pattern-none .pattern-name { color:var(--muted); }

.pattern-multi {
width:100%;
display:flex;
align-items:flex-start;
justify-content:center;
flex-wrap:wrap;
gap:5px;
}

.pattern-multi .pattern-box { min-width:43px; }

.candle-pattern-visual {
height:28px;
min-width:36px;
display:flex;
align-items:center;
justify-content:center;
gap:4px;
position:relative;
}

.mini-candle {
width:8px;
height:27px;
position:relative;
display:flex;
justify-content:center;
align-items:center;
}

.mini-candle .wick {
position:absolute;
width:1px;
height:27px;
left:50%;
top:0;
transform:translateX(-50%);
background:#82909d;
}

.mini-candle .body {
position:relative;
z-index:2;
width:8px;
height:12px;
border-radius:1px;
}

.mini-candle.bearish .body { background:var(--red); height:15px; }
.mini-candle.bullish .body { background:var(--green); height:19px; }

.mini-candle.bullish.large .body,
.mini-candle.bearish.large .body {
height:23px;
width:10px;
}

.mini-candle.bullish-doji .body {
background:var(--green);
height:3px;
width:10px;
}

.mini-candle.bearish-doji .body {
background:var(--red);
height:3px;
width:10px;
}

.mini-candle.piercing-candle .body {
background:var(--green);
height:18px;
}

.mini-candle.piercing-bearish .body {
background:var(--red);
height:18px;
}

.mini-candle.bullish.very-large .body {
background:var(--green);
height:24px;
width:10px;
}

.mini-candle.bearish.very-large .body {
background:var(--red);
height:24px;
width:10px;
}

.mini-candle.hammer .wick,
.mini-candle.inverted-hammer .wick {
height:27px;
top:0;
}

.mini-candle.hammer .body {
position:absolute;
top:1px;
height:7px;
width:9px;
}

.mini-candle.bullish.hammer .body,
.mini-candle.bullish.inverted-hammer .body {
background:var(--green);
}

.mini-candle.bearish.hammer .body,
.mini-candle.bearish.inverted-hammer .body {
background:var(--red);
}

.mini-candle.inverted-hammer .body {
position:absolute;
bottom:1px;
height:7px;
width:9px;
}

/* =====================================================
RSI 수치 색상
===================================================== */

.rsi-value {
width:100%;
min-height:18px;
display:flex;
align-items:center;
justify-content:center;
margin:0;
padding:2px;
color:#aab9c6;
background:#1b2935;
border:1px solid #344451;
border-radius:4px;
font-size:9px;
font-weight:800;
white-space:nowrap;
}

.rsi-value.rsi-low {
color:#ff6575;
background:rgba(255,101,117,.16);
border:1px solid #ff6575;
}

.rsi-value.rsi-middle {
color:#38d996;
background:rgba(56,217,150,.16);
border:1px solid #38d996;
}

.rsi-value.rsi-high {
color:#e8c56e;
background:rgba(232,197,110,.16);
border:1px solid #e8c56e;
}

.rsi-value.rsi-unavailable {
color:#71808c;
background:#141e27;
border-color:#26343f;
}

/* =====================================================
RSI 돌파 카드 강조
===================================================== */

.three-candle-item.rsi-cross-up-card {
background:rgba(56,217,150,.16);
border:1px solid var(--green);
box-shadow:
inset 0 0 0 1px rgba(56,217,150,.15),
0 0 7px rgba(56,217,150,.15);
}

.three-candle-item.rsi-cross-up-card .candle-date,
.three-candle-item.rsi-cross-up-card .candle-time {
color:var(--green);
}

.three-candle-item.rsi-cross-down-card {
background:rgba(255,101,117,.16);
border:1px solid var(--red);
box-shadow:
inset 0 0 0 1px rgba(255,101,117,.15),
0 0 7px rgba(255,101,117,.15);
}

.three-candle-item.rsi-cross-down-card .candle-date,
.three-candle-item.rsi-cross-down-card .candle-time {
color:var(--red);
}

/* EMA 1 / 3 역배열 유지 중 양봉 노란색 카드 */

.three-candle-item.ema-dead-cross-card {
background:rgba(232,197,110,.22);
border:1px solid var(--yellow);
box-shadow:
inset 0 0 0 1px rgba(232,197,110,.20),
0 0 8px rgba(232,197,110,.22);
}

.three-candle-item.ema-dead-cross-card .candle-date,
.three-candle-item.ema-dead-cross-card .candle-time {
color:var(--yellow);
}

.three-candle-item.ema-dead-cross-progress {
border-width:2px;
box-shadow:
inset 0 0 0 1px rgba(232,197,110,.28),
0 0 10px rgba(232,197,110,.30);
}

.candle-change {
width:100%;
height:17px;
min-height:17px;
display:flex;
align-items:center;
justify-content:center;
font-size:9px;
font-weight:900;
line-height:1;
white-space:nowrap;
}

.candle-change-up { color:var(--green); }
.candle-change-down { color:var(--red); }
.candle-change-zero { color:#aab9c6; }
.candle-change-none { color:#71808c; }

.three-candle-section,
.top-three-candle-section {
padding:8px 9px;
background:var(--panel3);
border-top:1px solid var(--line-soft);
}

.three-candle-heading {
color:#9eafbe;
font-size:var(--fs-small);
font-weight:800;
text-align:left;
margin-bottom:7px;
}

.three-candle-list {
display:grid;
grid-template-columns:repeat(6,minmax(0,1fr));
width:100%;
gap:5px;
}

.three-candle-item {
min-width:0;
height:120px;
min-height:120px;
display:grid;
grid-template-rows:27px minmax(0,1fr) 17px 18px;
row-gap:3px;
align-items:stretch;
padding:5px 2px;
background:#121e28;
border:1px solid #263744;
border-radius:5px;
overflow:hidden;
}

.three-candle-label {
width:100%;
height:27px;
min-height:0;
display:flex;
flex-direction:column;
align-items:center;
justify-content:center;
margin:0;
padding:0;
line-height:1.25;
text-align:center;
white-space:nowrap;
overflow:hidden;
}

.candle-date {
color:#c2cfda;
font-size:var(--fs-tiny);
font-weight:800;
}

.candle-time {
display:flex;
align-items:center;
justify-content:center;
gap:2px;
margin-top:2px;
color:#8295a5;
font-size:var(--fs-tiny);
font-weight:700;
}

.candle-live {
display:inline-block;
color:var(--yellow);
font-size:7px;
font-weight:800;
}

.three-candle-pattern {
width:100%;
height:100%;
min-width:0;
min-height:0;
display:flex;
align-items:center;
justify-content:center;
margin:0;
padding:0;
overflow:hidden;
}

.three-candle-pattern .pattern-multi {
width:100%;
max-width:100%;
max-height:100%;
min-height:0;
display:flex;
flex-wrap:wrap;
align-items:center;
justify-content:center;
gap:2px;
overflow:hidden;
}

.three-candle-pattern .pattern-box {
min-width:0;
max-width:100%;
flex:0 1 auto;
}

.three-candle-pattern .pattern-name {
font-size:7px;
line-height:1.15;
white-space:normal;
overflow-wrap:anywhere;
text-align:center;
}

.three-candle-pattern .pattern-none { min-height:28px; }

.three-candle-change {
width:100%;
height:17px;
min-width:0;
min-height:0;
display:flex;
align-items:center;
justify-content:center;
margin:0;
padding:0;
overflow:hidden;
}

.three-candle-change .candle-change {
width:100%;
height:17px;
min-height:17px;
margin:0;
padding:0;
}

.three-candle-rsi {
width:100%;
height:18px;
min-width:0;
min-height:0;
display:flex;
align-items:center;
justify-content:center;
margin:0;
padding:0;
overflow:hidden;
}

.three-candle-rsi .rsi-value {
width:100%;
height:18px;
min-height:18px;
margin:0;
padding:1px 2px;
line-height:1;
}

/* =====================================================
조건 체크리스트
===================================================== */

.signal-checklist {
padding:8px 10px;
background:#0d161e;
border-top:1px solid #26343f;
border-bottom:1px solid #26343f;
}

.checklist-heading {
margin-bottom:7px;
color:#b7c5d0;
font-size:var(--fs-small);
font-weight:900;
}

.checklist-grid {
display:grid;
grid-template-columns:repeat(2,minmax(0,1fr));
gap:5px 8px;
}

.checklist-item {
min-width:0;
min-height:27px;
display:flex;
align-items:center;
gap:6px;
padding:4px 6px;
background:#131f29;
border:1px solid #263744;
border-radius:4px;
}

.checklist-symbol {
width:17px;
min-width:17px;
height:17px;
display:flex;
align-items:center;
justify-content:center;
border-radius:4px;
font-size:12px;
font-weight:1000;
line-height:1;
}

.checklist-label {
min-width:0;
flex:1;
color:#bdc9d3;
font-size:var(--fs-tiny);
font-weight:700;
line-height:1.3;
overflow-wrap:anywhere;
}

.checklist-status {
flex:0 0 auto;
font-size:var(--fs-tiny);
font-weight:800;
white-space:nowrap;
}

.check-pass {
color:var(--green);
}

.check-fail {
color:var(--red);
}

.check-unknown {
color:#83919c;
}

.checklist-symbol.check-pass {
background:rgba(56,217,150,.13);
border:1px solid rgba(56,217,150,.35);
}

.checklist-symbol.check-fail {
background:rgba(255,101,117,.13);
border:1px solid rgba(255,101,117,.35);
}

.checklist-symbol.check-unknown {
background:rgba(131,145,156,.12);
border:1px solid rgba(131,145,156,.3);
}

/* =====================================================
BTC 체크리스트 전용 디자인
===================================================== */

.btc-checklist-section {
margin-top:10px;
padding-top:9px;
border-top:1px solid #304252;
}

.btc-checklist-heading {
display:flex;
align-items:center;
justify-content:space-between;
gap:6px;
margin-bottom:7px;
color:#e2eaf1;
font-size:var(--fs-small);
font-weight:900;
}

.btc-checklist-heading > span {
color:var(--yellow);
font-size:var(--fs-tiny);
font-weight:800;
}

.btc-checklist-section .signal-checklist {
padding:0;
background:transparent;
border-top:0;
border-bottom:0;
}

.btc-checklist-section .checklist-heading {
display:none;
}

.btc-checklist-section .checklist-item {
background:rgba(12,23,32,.8);
border-color:#304252;
}

.btc-checklist-section .checklist-label {
color:#c5d1db;
}

.signal-type-list,
.pattern-badge-list {
display:flex;
align-items:center;
flex-wrap:wrap;
gap:5px;
}

.signal-type-badge,
.bearish-type-badge,
.consecutive-bullish,
.consecutive-bearish {
display:inline-flex;
align-items:center;
padding:3px 6px;
border-radius:4px;
font-size:var(--fs-tiny)!important;
font-weight:800;
white-space:nowrap;
}

.signal-type-badge,
.consecutive-bullish {
color:var(--green)!important;
background:var(--green-soft);
border:1px solid rgba(56,217,150,.2);
}

.bearish-type-badge,
.consecutive-bearish {
color:var(--red)!important;
background:var(--red-soft);
border:1px solid rgba(255,101,117,.2);
}

.panel-heading {
min-height:39px;
padding:0 10px;
display:flex;
align-items:center;
justify-content:space-between;
gap:8px;
background:#121c26;
border-bottom:1px solid var(--line);
}

.heading-title {
display:flex;
align-items:center;
gap:7px;
font-size:var(--fs-section);
font-weight:900;
}

.heading-mark {
width:6px;
height:6px;
border-radius:50%;
display:inline-block;
}

.signal-mark {
background:var(--green);
box-shadow:0 0 7px rgba(56,217,150,.6);
}

.heading-count {
min-width:18px;
padding:2px 5px;
text-align:center;
color:var(--green);
background:var(--green-soft);
border-radius:4px;
font-size:var(--fs-tiny);
}

.update-time {
color:var(--muted);
font-size:var(--fs-tiny);
white-space:nowrap;
}

.btc-panel {
overflow:hidden;
padding:10px 12px;
background:linear-gradient(110deg,#152432,#0d151e);
border:1px solid #304252;
border-radius:7px;
}

.btc-top {
display:flex;
align-items:center;
justify-content:space-between;
gap:6px;
margin-bottom:7px;
}

.btc-label {
display:flex;
align-items:center;
gap:6px;
color:#e2eaf1;
font-size:var(--fs-section);
font-weight:900;
}

.status-dot {
width:6px;
height:6px;
display:inline-block;
border-radius:50%;
background:var(--green);
}

.market-tag {
padding:2px 5px;
color:#9fb0bf;
background:#1c2c39;
border-radius:3px;
font-size:9px;
}

.btc-main {
display:flex;
align-items:center;
justify-content:space-between;
gap:10px;
}

.btc-price {
color:#f1f5f8;
font-size:21px;
font-weight:900;
letter-spacing:-.5px;
}

.btc-candle-section {
margin-top:10px;
padding-top:9px;
border-top:1px solid #304252;
}

.btc-candle-heading {
color:#aab9c6;
font-size:var(--fs-small);
font-weight:800;
margin-bottom:7px;
}

/* BTC 캔들 제목 아래 설명 문구만 숨김 */
.btc-candle-heading .rsi-condition-label {
display:none;
}

.btc-candle-section .three-candle-item {
background:#101d28;
border-color:#304252;
}

.btc-candle-section .three-candle-label {
color:#b8c6d2;
}

.signal-panel {
overflow:hidden;
background:var(--panel);
border:1px solid #293844;
border-radius:7px;
}

.signal-condition {
display:flex;
align-items:center;
gap:6px;
padding:7px 9px;
overflow-x:auto;
white-space:nowrap;
background:#111922;
border-bottom:1px solid var(--line-soft);
scrollbar-width:none;
}

.signal-condition::-webkit-scrollbar { display:none; }

.signal-condition span {
flex:0 0 auto;
color:#aab8c4;
font-size:var(--fs-tiny);
font-weight:700;
padding-right:7px;
border-right:1px solid #35434e;
}

.signal-condition span:last-child { border-right:0; }

.signal-list { width:100%; }

.signal-card {
display:flex;
flex-direction:column;
min-height:0;
background:#101820;
border-bottom:1px solid #26343f;
}

.signal-card:last-child { border-bottom:0; }

.signal-card-top {
min-height:39px;
padding:6px 9px;
display:flex;
align-items:center;
justify-content:space-between;
flex-wrap:wrap;
gap:5px;
background:#121c25;
border-bottom:1px solid #273541;
}

.signal-identity {
display:flex;
align-items:center;
gap:8px;
}

.signal-rank {
color:var(--yellow);
font-size:var(--fs-small);
font-weight:800;
}

.signal-coin-name {
color:#e9f0f5;
font-size:var(--fs-coin);
font-weight:900;
}

.signal-data-grid {
display:grid;
grid-template-columns:1.1fr 1fr .9fr;
width:100%;
}

.signal-data-grid > div {
min-width:0;
min-height:48px;
padding:6px 3px;
display:flex;
flex-direction:column;
align-items:center;
justify-content:center;
text-align:center;
}

.signal-data-grid > div + div {
border-left:1px solid #27343f;
}

.signal-data-grid span {
color:#8798a7;
font-size:var(--fs-tiny);
}

.signal-data-grid strong {
margin-top:4px;
color:#e7edf2;
font-size:var(--fs-value);
font-weight:800;
white-space:nowrap;
}

.signal-empty {
min-height:65px;
display:flex;
align-items:center;
justify-content:center;
color:#8c9ba8;
font-size:var(--fs-content);
font-weight:700;
}

.coin-card {
overflow:hidden;
background:var(--panel);
border:1px solid #293844;
border-radius:7px;
margin-top:8px;
}

.coin-head {
min-height:39px;
padding:6px 9px;
display:flex;
align-items:center;
justify-content:space-between;
flex-wrap:wrap;
gap:5px;
background:#121c25;
border-bottom:1px solid var(--line);
}

.coin-title {
display:flex;
align-items:center;
gap:8px;
}

.coin-title b {
color:#e9f0f5;
font-size:var(--fs-coin);
font-weight:900;
}

.rank {
color:var(--yellow);
font-size:var(--fs-small);
font-weight:800;
}

.market-summary {
display:grid;
grid-template-columns:repeat(3,minmax(0,1fr));
}

.market-summary > div {
min-width:0;
min-height:49px;
padding:6px 3px;
display:flex;
flex-direction:column;
align-items:center;
justify-content:center;
text-align:center;
background:#101820;
}

.market-summary > div + div {
border-left:1px solid #26343f;
}

.market-summary span {
color:#8798a7;
font-size:var(--fs-tiny);
margin-bottom:4px;
}

.market-summary strong {
color:#e6edf2;
font-size:var(--fs-value);
font-weight:800;
white-space:nowrap;
}

@media (min-width:900px) {
body { padding:16px; }
.btc-price { font-size:23px; }
.three-candle-item {
height:120px;
min-height:120px;
}
}

@media (max-width:899px) {
body { padding:9px; }
}

@media (max-width:600px) {

:root {
    --fs-main:15px;
    --fs-section:12px;
    --fs-coin:13px;
    --fs-value:11px;
    --fs-content:10px;
    --fs-small:9px;
    --fs-tiny:8px;
}

body { padding:6px; }

h1 {
    margin:2px 1px 9px;
    padding-bottom:8px;
}

section { margin-bottom:8px; }

.btc-panel {
    padding:9px 10px;
    border-radius:6px;
}

.btc-price { font-size:19px; }

.panel-heading {
    min-height:36px;
    padding:0 8px;
}

.signal-condition {
    padding:6px 7px;
    gap:5px;
}

.signal-card-top {
    min-height:36px;
    padding:5px 7px;
}

.signal-data-grid > div {
    min-height:43px;
    padding:5px 2px;
}

.three-candle-section,
.top-three-candle-section {
    padding:7px 6px;
}

.three-candle-heading { margin-bottom:5px; }

.three-candle-list {
    grid-template-columns:repeat(6,minmax(0,1fr));
    gap:3px;
}

.three-candle-item {
    height:116px;
    min-height:116px;
    grid-template-rows:25px minmax(0,1fr) 16px 17px;
    row-gap:3px;
    padding:5px 1px;
    border-radius:4px;
}

.three-candle-label {
    height:25px;
    min-height:0;
    margin:0;
}

.candle-date,
.candle-time { font-size:7px; }

.three-candle-pattern .pattern-box { min-width:0; }

.three-candle-pattern .pattern-name {
    font-size:7px;
    white-space:normal;
    text-align:center;
    overflow-wrap:anywhere;
}

.candle-pattern-visual { transform:scale(.75); }

.pattern-multi { gap:2px; }
.pattern-multi .pattern-box { min-width:0; }

.coin-card {
    margin-top:6px;
    border-radius:6px;
}

.coin-head {
    min-height:36px;
    padding:5px 7px;
}

.market-summary > div {
    min-height:44px;
    padding:5px 2px;
}

.signal-empty { min-height:55px; }

.signal-type-list,
.pattern-badge-list { gap:3px; }

.signal-type-badge,
.bearish-type-badge,
.consecutive-bullish,
.consecutive-bearish {
    padding:2px 4px;
}

.rsi-value {
    height:17px;
    min-height:17px;
    font-size:7px;
    padding:1px;
}

.three-candle-rsi {
    height:17px;
    min-height:0;
}

.three-candle-change { height:16px; }

.three-candle-change .candle-change {
    height:16px;
    min-height:16px;
    font-size:8px;
}

/* 모바일 체크리스트 */

.signal-checklist {
    padding:7px 7px;
}

.checklist-grid {
    grid-template-columns:1fr;
    gap:4px;
}

.checklist-item {
    min-height:25px;
    padding:4px 5px;
}

}

@media (max-width:380px) {

:root {
    --fs-main:14px;
    --fs-section:11px;
    --fs-coin:12px;
    --fs-value:10px;
    --fs-content:9px;
    --fs-small:8px;
    --fs-tiny:7px;
}

body { padding:5px; }

.btc-price { font-size:17px; }
.update-time { font-size:7px; }

.three-candle-item {
    height:112px;
    min-height:112px;
    grid-template-rows:23px minmax(0,1fr) 15px 16px;
    row-gap:3px;
}

.three-candle-label { height:23px; }

.candle-date,
.candle-time { font-size:6px; }

.three-candle-pattern .pattern-box { min-width:0; }
.three-candle-pattern .pattern-name { font-size:6px; }

.signal-card-top { gap:4px; }

.rsi-value {
    height:16px;
    min-height:16px;
    font-size:6px;
}

.three-candle-rsi { height:16px; }
.three-candle-change { height:15px; }

.three-candle-change .candle-change {
    height:15px;
    min-height:15px;
    font-size:7px;
}

}
"""


# =========================================================
# DASHBOARD
# =========================================================

@app.get("/", response_class=HTMLResponse)
def dashboard():

    s = btc_html()

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
            <section class="ranking-panel">

                <div class="panel-heading">

                    <div class="heading-title">
                        <span class="heading-mark"
                              style="background:#e8c56e"></span>
                        RANKING
                        <span>TOP{TOP_N}</span>
                    </div>

                    <span class="update-time">
                        업비트 거래대금 순
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

        <meta name="viewport"
              content="width=device-width, initial-scale=1">

        <meta http-equiv="refresh" content="60">

        <title>TRADING SIGNAL CENTER</title>

        <style>
            {CSS}
        </style>

    </head>

    <body>

        <h1>TRADING SIGNAL CENTER</h1>

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
        "START | BTC 가격 및 최근 %s개 캔들 표시 | "
        "UPBIT TOP%s | 캔들패턴=%s",
        DISPLAY_CANDLE_COUNT,
        TOP_N,
        get_timeframe_label(CANDLE_PATTERN_TIMEFRAME)
    )

    log.info(
        "BTC / SIGNAL / TOP 시간봉 통일: %s",
        CANDLE_PATTERN_TIMEFRAME
    )

    log.info(
        "4시간봉 KST 기준: 09, 13, 17, 21, 01, 05"
    )

    log.info(
        "일봉 KST 기준: 09:00"
    )

    log.info(
        "BTC OKX 캔들 데이터는 업비트 KST 캔들 경계로 재구성"
    )

    log.info(
        "BTC 당일 변동률은 KST 09:00 기준 일봉 시가로 계산"
    )

    log.info(
        "SIGNAL 거래대금 최소 조건 사용 안 함"
    )

    log.info(
        "SIGNAL 당일 변동률 조건 사용 여부=%s | 기준=%s%% 초과",
        USE_SIGNAL_DAILY_CHANGE,
        DAILY_CHANGE_MIN
    )

    log.info(
        "SIGNAL 최신 RSI 범위 조건 사용 여부=%s | 범위=%s~%s",
        USE_SIGNAL_RSI_RANGE,
        RSI_RANGE_MIN,
        RSI_RANGE_MAX
    )

    log.info(
        "SIGNAL EMA 배열 조건 사용 여부=%s | 방향 설정 정배열=%s 역배열=%s | 조건=%s",
        USE_SIGNAL_EMA_ALIGNMENT,
        USE_EMA_BULLISH_ALIGNMENT,
        USE_EMA_BEARISH_ALIGNMENT,
        get_ema_condition_text()
    )

    log.info(
        "SIGNAL EMA 역배열 중 양봉 조건 사용 여부=%s",
        USE_SIGNAL_EMA_BEARISH_BULLISH
    )

    log.info(
        "SIGNAL RSI 돌파 조건 사용 여부=%s",
        USE_SIGNAL_RSI_CROSS
    )

    log.info(
        "SIGNAL 체크리스트 화면 표시 여부=%s",
        SHOW_SIGNAL_CHECKLIST
    )

    log.info(
        "EMA 개별 사용 설정 | EMA%s=%s | EMA%s=%s | EMA%s=%s | EMA%s=%s",
        EMA_FAST_PERIOD,
        USE_EMA_5,
        EMA_MIDDLE_PERIOD,
        USE_EMA_10,
        EMA_SLOW_PERIOD,
        USE_EMA_20,
        EMA_LONG_PERIOD,
        USE_EMA_50
    )

    log.info(
        "EMA 배열 방향 설정 | 정배열=%s | 역배열=%s",
        USE_EMA_BULLISH_ALIGNMENT,
        USE_EMA_BEARISH_ALIGNMENT
    )

    log.info(
        "별도 EMA 데드크로스 설정 | EMA%s / EMA%s | 시가(open) 기준",
        EMA_CROSS_FAST_PERIOD,
        EMA_CROSS_SLOW_PERIOD
    )

    log.info(
        "EMA 데드크로스 시간봉: %s",
        get_timeframe_label(CANDLE_PATTERN_TIMEFRAME)
    )

    log.info(
        "EMA 1/3 데드크로스 이후 역배열 유지 상태의 모든 양봉 노란색 강조"
    )

    log.info(
        "EMA 1/3 역배열이 해소되면 노란색 강조 종료"
    )

    log.info(
        "EMA 1/3 역배열 진행 중인 캔들도 양봉이면 노란색 강조"
    )

    log.info(
        "EMA 데드크로스 계산은 시가(open) 기준"
    )

    log.info(
        "SIGNAL EMA 및 RSI 계산 시간봉: %s",
        get_timeframe_label(CANDLE_PATTERN_TIMEFRAME)
    )

    log.info(
        "SIGNAL BTC 거래대금 비교 조건 사용 안 함"
    )

    log.info(
        "SIGNAL 정렬 기준: 업비트 24시간 거래대금 높은 순"
    )

    log.info(
        "캔들 패턴은 RSI SIGNAL 선정과 무관하게 표시"
    )

    log.info(
        "RSI 50 상향돌파 + 상승캔들: 해당 카드 녹색 강조 유지"
    )

    log.info(
        "RSI 70 하향돌파 + 하락캔들: 해당 카드 적색 강조 유지"
    )

    log.info(
        "RSI 50 상향/70 하향돌파 검사 범위: 최근 표시 %s개 캔들",
        DISPLAY_CANDLE_COUNT
    )

    log.info(
        "RSI 수치 색상: 50 이하 빨강 / 50 초과 70 미만 녹색 / 70 이상 노랑"
    )

    log.info(
        "RSI 수치 색상은 RSI 표시 칸에만 적용"
    )

    log.info(
        "SIGNAL 선정은 개별 활성화된 조건만 적용"
    )

    log.info(
        "최근 %s개 표시 캔들 중 EMA1/3 역배열 유지 양봉 조건은 설정에 따라 적용",
        DISPLAY_CANDLE_COUNT
    )

    log.info(
        "최근 %s개 표시 캔들 중 RSI50 상향 또는 RSI70 하향돌파 조건은 설정에 따라 적용",
        DISPLAY_CANDLE_COUNT
    )

    log.info(
        "SIGNAL 및 TOP 리스트 체크리스트 화면 표시 설정=%s",
        SHOW_SIGNAL_CHECKLIST
    )

    log.info(
        "BTC 시황에도 동일한 체크리스트 표시 설정 적용"
    )

    log.info(
        "BTC 체크리스트는 OKX BTC 데이터 기준"
    )

    log.info(
        "BTC 체크리스트 거래대금 조건 제외"
    )

    log.info(
        "BTC 체크리스트 당일 변동률은 KST 09:00 기준"
    )

    log.info(
        "BTC 체크리스트 RSI 및 EMA는 %s 기준",
        get_timeframe_label(CANDLE_PATTERN_TIMEFRAME)
    )

    log.info(
        "BTC 체크리스트 판정은 업비트 SIGNAL 공통 판정 함수 사용"
    )

    log.info(
        "체크리스트는 Y로 설정된 조건만 표시하며, 표시할 조건이 없으면 영역 전체 숨김"
    )

    log.info(
        "체크리스트 통과=초록색 ✓ / 미통과=빨간색 ✕ / 데이터 부족=회색 -"
    )

    log.info(
        "캔들 날짜와 시간 두 줄 표시 적용"
    )

    log.info(
        "상승장악형 및 상승관통형 패턴 이름은 기존 녹색 표시 유지"
    )

    log.info(
        "녹색 강조: 최근 %s개 봉 중 장대양봉",
        DISPLAY_CANDLE_COUNT
    )

    log.info(
        "%s개 이상 연속 일반 양봉",
        CONSECUTIVE_BULLISH_MIN
    )

    log.info(
        "최근 %s개 봉 캔들패턴 및 RSI 표시",
        DISPLAY_CANDLE_COUNT
    )

    log.info(
        "3연속 이상 장대양봉 및 장대음봉 표시"
    )

    log.info(
        "캔들별 변동률: 해당 캔들 시가 대비 종가 기준"
    )

    log.info(
        "기존 EMA 정배열 연속 카운팅 기능 삭제 상태 유지"
    )

    log.info(
        "EMA 배열 방향 설정에 따라 선택된 정배열/역배열 조건만 SIGNAL 통과"
    )

    log.info(
        "정배열과 역배열 설정이 모두 N이면 EMA 배열 조건 자동 통과"
    )

    log.info(
        "기존 현재가 EMA5 하회 노란색 조건 제거 상태 유지"
    )

    log.info(
        "컴팩트 트레이딩 터미널 디자인 적용"
    )

    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    schedule.every(UPDATE_MINUTES).minutes.do(
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
