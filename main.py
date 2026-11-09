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

TOP_N = 20
SHOW_TOP_LIST = "N"
UPDATE_MINUTES = 1

# SIGNAL 최소 24시간 거래대금 (KRW)
SIGNAL_MIN_VOLUME = 10_000_000_000

USE_UPBIT = "Y"
USE_OKX = "Y"

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 20

# 캔들 패턴 시간봉
CANDLE_PATTERN_TIMEFRAME = "4h"

TIMEFRAME_LABEL = {
    "5m": "5분",
    "15m": "15분",
    "1h": "1시간",
    "4h": "4시간",
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

latest_signal_data = []

latest_btc_okx_price = None
latest_btc_daily_periods = []
latest_btc_daily_change = None


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
# 시간봉 데이터 조회
# =========================================================

def get_timeframe_dataframe(market, timeframe, price=None):

    if timeframe == "5m":

        df = get_cached_dataframe(
            market,
            "5m",
            lambda: get_upbit_5m_candles(market, 200)
        )

        minutes = 5

    elif timeframe == "15m":

        df = get_cached_dataframe(
            market,
            "15m",
            lambda: get_upbit_15m_candles(market, 200)
        )

        minutes = 15

    elif timeframe == "1h":

        df = get_cached_dataframe(
            market,
            "1h",
            lambda: get_upbit_1h_candles(market, 200)
        )

        minutes = 60

    elif timeframe == "4h":

        df = get_cached_dataframe(
            market,
            "4h",
            lambda: get_upbit_4h_candles(market, 200)
        )

        minutes = 240

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

    if timeframe == "4h":

        current_start = now.replace(
            hour=(now.hour // 4) * 4,
            minute=0,
            second=0,
            microsecond=0
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


# =========================================================
# 상승 / 하락 패턴 대상
# 장대양봉 추가
# =========================================================

BULLISH_HIGHLIGHT_PATTERNS = [
    "상승장악형",
    "상승관통형",
    "상승망치형",
    "상승역망치형",
    "장대양봉"
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
# 캔들 패턴 분석
# =========================================================

def analyze_previous_current_pattern(periods):

    result = {
        "previous_pattern": [],
        "current_pattern": [],
        "previous_bullish_pattern": False,
        "current_bullish_pattern": False,
        "previous_bullish_engulfing": False,
        "current_bullish_engulfing": False
    }

    if not periods:
        return result

    current = periods[-1]
    previous = periods[-2] if len(periods) >= 2 else None
    previous_previous = periods[-3] if len(periods) >= 3 else None

    result["current_pattern"] = get_candle_patterns(
        previous,
        current
    )

    if previous is not None:

        result["previous_pattern"] = get_candle_patterns(
            previous_previous,
            previous
        )

    result["current_bullish_engulfing"] = (
        "상승장악형" in result["current_pattern"]
    )

    result["previous_bullish_engulfing"] = (
        "상승장악형" in result["previous_pattern"]
    )

    result["previous_bullish_pattern"] = any(
        p in BULLISH_HIGHLIGHT_PATTERNS
        for p in result["previous_pattern"]
    )

    result["current_bullish_pattern"] = any(
        p in BULLISH_HIGHLIGHT_PATTERNS
        for p in result["current_pattern"]
    )

    return result


# =========================================================
# 캔들 패턴 시간봉 분석
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
        "previous_bullish_engulfing": False,
        "current_bullish_engulfing": False
    }

    timeframe = CANDLE_PATTERN_TIMEFRAME

    if timeframe in ("5m", "15m", "1h", "4h"):

        df = get_timeframe_dataframe(
            market,
            timeframe,
            price
        )

        if df.empty:
            return empty_result

        minutes = {
            "5m": 5,
            "15m": 15,
            "1h": 60,
            "4h": 240
        }[timeframe]

        periods = []

        for _, row in df.iterrows():

            start = row["datetime"]

            o = float(row["open"])
            h = float(row["high"])
            l = float(row["low"])
            c = float(row["close"])

            change = (
                (c - o) / o * 100
                if o else None
            )

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

    elif timeframe == "1d":

        df = get_daily_dataframe(market)

        if df.empty:
            return empty_result

        now = datetime.now(KST)

        current_start = now.replace(
            hour=9,
            minute=0,
            second=0,
            microsecond=0
        )

        if now < current_start:
            current_start -= timedelta(days=1)

        df = update_current_candle(
            df,
            current_start,
            price
        )

        periods = build_daily_periods(
            df,
            price
        )

    else:
        return empty_result

    pattern_info = analyze_previous_current_pattern(
        periods
    )

    current_signal = pattern_info[
        "current_bullish_pattern"
    ]

    previous_signal = pattern_info[
        "previous_bullish_pattern"
    ]

    # EMA 조건 없이 상승패턴만으로 시그널 판정
    pattern_signal = bool(
        current_signal or previous_signal
    )

    signal = pattern_signal

    reason = None

    if current_signal:

        for pattern in pattern_info["current_pattern"]:

            if pattern in BULLISH_HIGHLIGHT_PATTERNS:
                reason = pattern
                break

    if reason is None and previous_signal:

        for pattern in pattern_info["previous_pattern"]:

            if pattern in BULLISH_HIGHLIGHT_PATTERNS:
                reason = pattern
                break

    current_period = periods[-1] if periods else None

    return {
        "periods": periods,

        "signal_pass": signal,
        "pattern_signal": pattern_signal,

        "current_signal": current_signal,
        "previous_signal": previous_signal,

        "signal_change": (
            current_period.get("change")
            if signal and current_period is not None
            else None
        ),

        "signal_period": (
            current_period.get("label")
            if signal and current_period is not None
            else None
        ),

        "signal_reason": reason,

        "previous_pattern": pattern_info["previous_pattern"],
        "current_pattern": pattern_info["current_pattern"],

        "previous_bullish_engulfing": pattern_info[
            "previous_bullish_engulfing"
        ],

        "current_bullish_engulfing": pattern_info[
            "current_bullish_engulfing"
        ]
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

        "previous_bullish_engulfing": analysis_pattern[
            "previous_bullish_engulfing"
        ],

        "current_bullish_engulfing": analysis_pattern[
            "current_bullish_engulfing"
        ]
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
                "signal_pass": False,
                "pattern_signal": False,
                "current_signal": False,
                "previous_signal": False,
                "signal_change": None,
                "signal_period": None,
                "signal_reason": None,
                "previous_pattern": [],
                "current_pattern": [],
                "previous_bullish_engulfing": False,
                "current_bullish_engulfing": False
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
    # SIGNAL 조건
    #
    # 1. 현재봉 또는 이전봉 상승패턴
    # 2. 당일 변동률 양수
    # 3. 24시간 거래대금 100억 이상
    # =====================================================

    latest_signal_data = [
        row for row in rows
        if (
            row.get("signal_4h", False)
            and row.get("daily_change") is not None
            and row.get("daily_change") > 0
            and row.get("volume_24h", 0) >= SIGNAL_MIN_VOLUME
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
        "UPBIT | TOP%s | 캔들패턴=%s | "
        "최소 거래대금=%s원 | SIGNAL=%s",
        TOP_N,
        get_timeframe_label(CANDLE_PATTERN_TIMEFRAME),
        SIGNAL_MIN_VOLUME,
        len(latest_signal_data)
    )


# =========================================================
# OKX BTC 캔들
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
# BTC KST 09:00 기준 일봉 생성
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

        change = (
            (c - o) / o * 100
            if o else None
        )

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
            build_okx_kst_daily_periods(d1h, price)
        )

        if latest_btc_daily_periods:

            latest_btc_daily_change = (
                latest_btc_daily_periods[-1].get("change")
            )

        else:
            latest_btc_daily_change = None


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
# 캔들 그림
# =========================================================

def candle_icon_html(pattern):

    if pattern == "상승장악형":

        return """
        <div class="candle-pattern-visual engulfing">
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
        <div class="candle-pattern-visual engulfing">
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
        <div class="candle-pattern-visual doji">
            <div class="mini-candle bullish-doji">
                <span class="wick"></span><span class="body"></span>
            </div>
        </div>
        """

    if pattern == "음수도지":

        return """
        <div class="candle-pattern-visual doji">
            <div class="mini-candle bearish-doji">
                <span class="wick"></span><span class="body"></span>
            </div>
        </div>
        """

    if pattern == "상승관통형":

        return """
        <div class="candle-pattern-visual piercing">
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
        <div class="candle-pattern-visual piercing">
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
        <div class="candle-pattern-visual single-candle">
            <div class="mini-candle bullish very-large">
                <span class="wick"></span><span class="body"></span>
            </div>
        </div>
        """

    if pattern == "장대음봉":

        return """
        <div class="candle-pattern-visual single-candle">
            <div class="mini-candle bearish very-large">
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
        <div class="candle-pattern-visual single-candle">
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
        <div class="candle-pattern-visual single-candle">
            <div class="mini-candle {candle_class}">
                <span class="wick"></span><span class="body"></span>
            </div>
        </div>
        """

    return """
    <div class="candle-pattern-visual empty">
        <span>—</span>
    </div>
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
            + '<span class="pattern-name">-</span></div>'
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
# EMA 표시 및 EMA 경고 완전 삭제
# =========================================================

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

    </section>
    """


# =========================================================
# SIGNAL 카드
# EMA 및 카운팅 표시 삭제
# =========================================================

def signal_card(row):

    timeframe_label = get_timeframe_label(
        CANDLE_PATTERN_TIMEFRAME
    )

    has_bullish_pattern = bool(
        row.get("signal_4h", False)
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
                <span>이전봉 {timeframe_label}</span>

                <strong class="pattern-display">
                    {current_pattern_html(row.get("previous_pattern"))}
                </strong>
            </div>

            <div class="signal-pattern">
                <span>현재봉 {timeframe_label}</span>

                <strong class="pattern-display">
                    {current_pattern_html(row.get("current_pattern"))}
                </strong>
            </div>

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
    <div class="signal-condition-one-line">

        <span>
            상승패턴 · 현재봉 또는 이전봉
        </span>

        <span>
            장대양봉 포함
        </span>

        <span>
            공통조건 · 당일 변동률 양수
        </span>

        <span>
            최소 거래대금 · {fmt_vol(SIGNAL_MIN_VOLUME)} 이상
        </span>

    </div>
    """

    return f"""
    <section class="signal-panel">

        <div class="section-head signal-panel-head">

            <div>
                <span class="section-kicker">SIGNAL</span>
                <b>🔴 종목 조건</b>
            </div>

            <span class="update-time">
                캔들패턴 {html.escape(pattern_timeframe_label)}
                · {kst()}
            </span>

        </div>

        {condition_html}

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

                <span>이전봉 {timeframe_label}</span>

                <strong class="pattern-display">
                    {current_pattern_html(row.get("previous_pattern"))}
                </strong>

            </div>

            <div>

                <span>현재봉 {timeframe_label}</span>

                <strong class="pattern-display">
                    {current_pattern_html(row.get("current_pattern"))}
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
    letter-spacing: -0.4px;
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


/* 캔들 패턴 */

.pattern-box {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    min-width: 55px;
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
    min-width: 36px;
}

.candle-pattern-visual {
    height: 23px;
    min-width: 35px;
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 4px;
    position: relative;
}

.mini-candle {
    width: 7px;
    height: 22px;
    position: relative;
    display: flex;
    justify-content: center;
    align-items: center;
}

.mini-candle .wick {
    position: absolute;
    width: 1px;
    height: 22px;
    left: 50%;
    top: 0;
    transform: translateX(-50%);
    background: #79848e;
}

.mini-candle .body {
    position: relative;
    z-index: 2;
    width: 7px;
    height: 10px;
    border-radius: 1px;
}

.mini-candle.bearish .body {
    background: #ff5966;
    height: 12px;
}

.mini-candle.bullish .body {
    background: #38d878;
    height: 17px;
}

.mini-candle.bullish.large .body,
.mini-candle.bearish.large .body {
    height: 20px;
    width: 9px;
}

.mini-candle.bullish-doji .body {
    background: #38d878;
    height: 2px;
    width: 10px;
}

.mini-candle.bearish-doji .body {
    background: #ff5966;
    height: 2px;
    width: 10px;
}

.mini-candle.piercing-candle .body {
    background: #38d878;
    height: 15px;
}

.mini-candle.piercing-bearish .body {
    background: #ff5966;
    height: 15px;
}

.mini-candle.bullish.very-large .body {
    background: #38d878;
    height: 20px;
    width: 9px;
}

.mini-candle.bearish.very-large .body {
    background: #ff5966;
    height: 20px;
    width: 9px;
}

.mini-candle.hammer .wick,
.mini-candle.inverted-hammer .wick {
    height: 22px;
    top: 0;
}

.mini-candle.hammer .body {
    position: absolute;
    top: 1px;
    height: 6px;
    width: 8px;
}

.mini-candle.bullish.hammer .body,
.mini-candle.bullish.inverted-hammer .body {
    background: #38d878;
}

.mini-candle.bearish.hammer .body,
.mini-candle.bearish.inverted-hammer .body {
    background: #ff5966;
}

.mini-candle.inverted-hammer .body {
    position: absolute;
    bottom: 1px;
    height: 6px;
    width: 8px;
}

.candle-pattern-visual.empty {
    color: #68737e;
    font-size: 9px;
    height: 23px;
}


/* 공통 패널 */

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


/* BTC */

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
    letter-spacing: -0.5px;
}

.btc-change {
    font-size: 11px;
    font-weight: 900;
}


/* SIGNAL */

.signal-panel {
    background: #0c1116;
    border: 1px solid #48272c;
    margin-bottom: 13px;
}

.signal-panel-head {
    border-bottom: 1px solid #392126;
}

.signal-condition-one-line {
    display: flex;
    align-items: center;
    flex-wrap: nowrap;
    gap: 12px;
    width: 100%;
    overflow-x: auto;
    overflow-y: hidden;
    white-space: nowrap;
    padding: 7px 9px;
    background: #120e10;
    border-bottom: 1px solid #302024;
    scrollbar-width: none;
}

.signal-condition-one-line::-webkit-scrollbar {
    display: none;
}

.signal-condition-one-line span {
    flex: 0 0 auto;
    color: #ff5966;
    font-size: 7px;
    font-weight: 900;
    white-space: nowrap;
}

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
    min-height: 0;
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
    flex-direction: row;
    align-items: center;
    flex-wrap: wrap;
    gap: 6px;
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
    gap: 2px;
}

.signal-type-badge {
    color: #e4c45e !important;
    font-size: 5px !important;
    font-weight: 900;
    white-space: nowrap;
}

.bearish-type-badge {
    color: #ff5966 !important;
    font-size: 5px !important;
    font-weight: 900;
    white-space: nowrap;
}

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
    min-width: 42px;
}

.signal-pattern .pattern-name {
    font-size: 5px;
}

.signal-pattern .candle-pattern-visual {
    height: 22px;
    min-width: 32px;
    transform: scale(.80);
}

.signal-pattern .pattern-multi {
    gap: 1px;
}

.signal-pattern .pattern-multi .pattern-box {
    min-width: 29px;
}


/* 상승패턴 강조 */

.signal-card.has-bullish-pattern {
    background: #10251d;
    box-shadow:
        inset 4px 0 0 #38d878,
        inset 0 0 0 1px #286b48;
}

.signal-card.has-bullish-pattern .signal-card-top {
    background: rgba(56, 216, 120, 0.07);
}

.signal-card.has-bullish-pattern .signal-data-grid > div {
    background: rgba(56, 216, 120, 0.07);
}

.signal-card.has-bullish-pattern .signal-coin-name {
    color: #38d878;
}

.signal-card.has-bullish-pattern .signal-rank {
    color: #e4c45e;
}

.signal-card.has-bullish-pattern .signal-pattern {
    background: rgba(56, 216, 120, 0.12);
}

.signal-empty {
    min-height: 46px;
    display: flex;
    align-items: center;
    justify-content: center;
    color: #626e78;
    font-size: 7px;
}


/* TOP 리스트 */

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

.market-summary .daily-change-value .up {
    color: #38d878 !important;
}

.market-summary .daily-change-value .down {
    color: #ff5966 !important;
}

.market-summary .daily-change-value .zero {
    color: #68737e !important;
}

.market-summary .pattern-box {
    min-width: 35px;
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
    min-width: 24px;
}

.market-summary .pattern-multi .pattern-name {
    font-size: 3.8px;
}


/* 모바일 */

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
        min-width: 24px;
    }

    .market-summary .pattern-name {
        font-size: 3.5px;
    }

    .market-summary .candle-pattern-visual {
        height: 19px;
        transform: scale(.56);
    }

    .signal-condition-one-line {
        gap: 9px;
        padding: 6px 7px;
    }

    .signal-condition-one-line span {
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

    .signal-type-list {
        gap: 1px;
    }

    .signal-type-badge {
        font-size: 4px !important;
    }

    .bearish-type-badge {
        font-size: 4px !important;
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
        min-width: 24px;
    }

    .signal-pattern .pattern-name {
        font-size: 3.5px;
    }

    .signal-pattern .candle-pattern-visual {
        height: 19px;
        transform: scale(.55);
    }

    .signal-pattern .pattern-multi .pattern-box {
        min-width: 22px;
    }
}
"""


# =========================================================
# DASHBOARD
# =========================================================

@app.get("/", response_class=HTMLResponse)
def dashboard():

    # BTC EMA 경고 및 EMA 표시 삭제
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
            <section>

                <div class="section-head">

                    <div>
                        <span class="section-kicker">RANKING</span>
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

        <meta name="viewport"
              content="width=device-width, initial-scale=1">

        <meta http-equiv="refresh" content="60">

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
        "START | BTC 가격 표시 | "
        "UPBIT TOP%s | 캔들패턴=%s",
        TOP_N,
        get_timeframe_label(CANDLE_PATTERN_TIMEFRAME)
    )

    log.info(
        "SIGNAL 최소 거래대금: %s원",
        SIGNAL_MIN_VOLUME
    )

    log.info(
        "EMA 및 EMA 카운팅 기능 사용 안 함"
    )

    log.info(
        "장대양봉 상승패턴 시그널 포함"
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
