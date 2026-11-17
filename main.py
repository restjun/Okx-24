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

TOP_N = 30
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

RSI_THRESHOLD = 50
RSI_LOWER_THRESHOLD = 50

RSI_CHECK_CANDLES = 12


# =========================================================
# EMA 설정
# =========================================================

EMA_FAST_PERIOD = 5
EMA_MIDDLE_PERIOD = 10
EMA_SLOW_PERIOD = 20
EMA_LONG_PERIOD = 50

USE_EMA_5 = "Y"
USE_EMA_10 = "Y"
USE_EMA_20 = "Y"
USE_EMA_50 = "Y"

# EMA 정배열 연속 카운팅 범위
EMA_ALIGNMENT_COUNT_MIN = 1
EMA_ALIGNMENT_COUNT_MAX = 12

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
# EMA 정배열 상태 및 이벤트 기록
# =========================================================

ema_alignment_states = {}
ema_alignment_event_history = {}

ema_alignment_lock = threading.Lock()


def update_ema_alignment_transition(
    market,
    timeframe,
    previous_alignment,
    alignment,
    candle_start
):
    """
    확정봉 기준 EMA 정배열 완성 이벤트 기록.

    이전 확정봉: 비정배열
    최신 확정봉: 정배열

    확정봉 마감 시 정배열이면
    기존 임시 '정배열 시작' 이벤트를
    '정배열 완성'으로 변경한다.

    확정봉 마감 시 정배열이 깨졌다면
    해당 봉의 임시 '정배열 시작' 이벤트를 제거한다.
    """

    if candle_start is None:
        return []

    if previous_alignment is None or alignment is None:
        return []

    event_key = (
        market,
        timeframe,
        candle_start
    )

    current_events = []

    with ema_alignment_lock:

        ema_alignment_states[
            (market, timeframe)
        ] = {
            "candle_start": candle_start,
            "alignment": bool(alignment)
        }

        events = ema_alignment_event_history.setdefault(
            event_key,
            []
        )

        # 확정봉이 되었으므로 임시 이벤트 제거
        events[:] = [
            event
            for event in events
            if event.get("label") != "정배열 시작"
        ]

        # 이전 확정봉 비정배열 → 최신 확정봉 정배열
        if (
            previous_alignment is False
            and alignment is True
        ):

            already_exists = any(
                event.get("label") == "정배열 완성"
                for event in events
            )

            if not already_exists:

                now = datetime.now(KST)

                events.append({
                    "label": "정배열 완성",
                    "time": now.strftime("%H:%M:%S")
                })

                log.info(
                    "%s | %s | EMA 정배열 완성 | 확정봉=%s",
                    market,
                    get_timeframe_label(timeframe),
                    candle_start.strftime("%Y-%m-%d %H:%M")
                )

        # 이벤트가 하나도 없으면 빈 키 제거
        if not events:
            ema_alignment_event_history.pop(
                event_key,
                None
            )

        current_events = list(
            ema_alignment_event_history.get(
                event_key,
                []
            )
        )

        # 오래된 이벤트 정리
        cutoff = candle_start - timedelta(
            minutes=TIMEFRAME_MINUTES[timeframe]
            * DISPLAY_CANDLE_COUNT
        )

        old_keys = []

        for key in ema_alignment_event_history:

            if key[0] != market:
                continue

            if key[1] != timeframe:
                continue

            if key[2] < cutoff:
                old_keys.append(key)

        for key in old_keys:
            del ema_alignment_event_history[key]

    return current_events


def update_ema_alignment_realtime(
    market,
    timeframe,
    previous_alignment,
    alignment,
    candle_start
):
    """
    진행 중인 캔들의 EMA 정배열 실시간 감지.

    직전 확정봉이 비정배열이고
    현재 진행봉이 정배열이면
    '정배열 시작' 임시 이벤트를 기록한다.

    진행 중 정배열이 깨지면
    임시 이벤트를 제거한다.

    확정된 '정배열 완성' 이벤트는 건드리지 않는다.
    """

    if candle_start is None:
        return []

    if previous_alignment is None or alignment is None:
        return []

    event_key = (
        market,
        timeframe,
        candle_start
    )

    current_events = []

    with ema_alignment_lock:

        ema_alignment_states[
            (market, timeframe)
        ] = {
            "candle_start": candle_start,
            "alignment": bool(alignment)
        }

        events = ema_alignment_event_history.setdefault(
            event_key,
            []
        )

        # 직전 확정봉이 비정배열이고
        # 현재 진행봉이 정배열인 경우
        if (
            previous_alignment is False
            and alignment is True
        ):

            already_started = any(
                event.get("label") == "정배열 시작"
                for event in events
            )

            already_completed = any(
                event.get("label") == "정배열 완성"
                for event in events
            )

            # 이미 확정된 이벤트가 아니고
            # 임시 시작 이벤트도 없다면 기록
            if not already_started and not already_completed:

                now = datetime.now(KST)

                events.append({
                    "label": "정배열 시작",
                    "time": now.strftime("%H:%M:%S")
                })

                log.info(
                    "%s | %s | EMA 정배열 실시간 시작 | 진행봉=%s",
                    market,
                    get_timeframe_label(timeframe),
                    candle_start.strftime("%Y-%m-%d %H:%M")
                )

        else:

            # 진행 중 정배열이 깨지면
            # 임시 시작 이벤트만 제거
            events[:] = [
                event
                for event in events
                if event.get("label") != "정배열 시작"
            ]

        # 이벤트가 하나도 없으면 빈 키 제거
        if not events:

            ema_alignment_event_history.pop(
                event_key,
                None
            )

        current_events = list(
            ema_alignment_event_history.get(
                event_key,
                []
            )
        )

    return current_events


def get_ema_alignment_events(
    market,
    timeframe,
    candle_start
):
    """
    특정 캔들에 기록된 EMA 이벤트 조회.
    """

    event_key = (
        market,
        timeframe,
        candle_start
    )

    with ema_alignment_lock:

        return list(
            ema_alignment_event_history.get(
                event_key,
                []
            )
        )


# =========================================================
# EMA 조건 표시
# =========================================================

def get_ema_condition_text():

    selected = []

    if USE_EMA_5 == "Y":
        selected.append(f"EMA{EMA_FAST_PERIOD}")

    if USE_EMA_10 == "Y":
        selected.append(f"EMA{EMA_MIDDLE_PERIOD}")

    if USE_EMA_20 == "Y":
        selected.append(f"EMA{EMA_SLOW_PERIOD}")

    if USE_EMA_50 == "Y":
        selected.append(f"EMA{EMA_LONG_PERIOD}")

    if not selected:
        return "EMA 조건 사용 안 함"

    if len(selected) == 1:
        return f"{selected[0]} 단독 사용 (정배열 비교 없음)"

    return " > ".join(selected) + " 정배열"


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

        if (
            previous_rsi < RSI_LOWER_THRESHOLD
            and current_rsi >= RSI_LOWER_THRESHOLD
            and close_price > open_price
        ):

            period["rsi_signal_type"] = "RSI30_UP"

        elif (
            previous_rsi > RSI_THRESHOLD
            and current_rsi <= RSI_THRESHOLD
            and close_price < open_price
        ):

            period["rsi_signal_type"] = "RSI70_DOWN"

    return periods


# =========================================================
# EMA 정배열 연속 카운팅
# =========================================================

def calculate_ema_alignment_count(
    ema_series_list,
    dataframe_length
):

    if len(ema_series_list) < 2:
        return 0

    if dataframe_length <= 0:
        return 0

    count = 0

    # 가장 최근 봉부터 과거 방향으로 검사
    for index in range(dataframe_length - 1, -1, -1):

        values = []

        for series in ema_series_list:

            if index >= len(series):
                values = []
                break

            value = series.iloc[index]

            if pd.isna(value):
                values = []
                break

            values.append(float(value))

        if len(values) != len(ema_series_list):
            break

        # 선택된 EMA가 모두 정배열이어야 카운트
        aligned = all(
            values[i] > values[i + 1]
            for i in range(len(values) - 1)
        )

        if not aligned:
            break

        count += 1

    return count


# =========================================================
# 확정봉 EMA 정배열 판정
# =========================================================

def get_ema_alignment_at_index(
    ema_series_list,
    index
):
    """
    지정한 캔들의 EMA 정배열 여부를 계산한다.

    선택된 EMA가 모두 존재하고
    빠른 EMA > 느린 EMA 순서여야 정배열이다.
    """

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
