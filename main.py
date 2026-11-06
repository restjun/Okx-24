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

TOP_N = 30
SHOW_TOP_LIST = "N"
UPDATE_MINUTES = 1

HISTORY_CHUNK = 200
MAX_HISTORY_CHUNKS = 1
EMA_HISTORY_CANDLES = 200

USE_UPBIT = "Y"
USE_OKX = "Y"

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10


# =========================================================
# EMA 기간 설정
# =========================================================

EMA_PULLBACK = 10
EMA_FAST = 20

EMA_SLOW = 1
EMA_LONG = 100


# =========================================================
# 시그널 2 카운팅 설정
# =========================================================

# 시그널 2의 두 번째 EMA 조건이
# 연속으로 유지되는 완성된 1시간봉 개수 제한

SIGNAL2_MAX_COUNT = 10


# =========================================================
# 시그널 시간봉 설정
# =========================================================

SIGNAL1_TIMEFRAME = "15m"
SIGNAL2_TIMEFRAME = "1h"
SIGNAL3_TIMEFRAME = "1d"

SIGNAL_TIMEFRAMES = {
    "signal1": SIGNAL1_TIMEFRAME,
    "signal2": SIGNAL2_TIMEFRAME,
    "signal3": SIGNAL3_TIMEFRAME
}

SIGNAL_LABELS = {
    "signal1": "시그널1",
    "signal2": "시그널2",
    "signal3": "시그널3"
}

TIMEFRAME_LABEL = {
    "5m": "5분봉",
    "15m": "15분봉",
    "1h": "1시간봉",
    "4h": "4시간봉",
    "1d": "일봉"
}


# =========================================================
# 시그널별 독립 조건 설정
#
# Y   = 정배열
# N   = 역배열
# ANY = 정배열/역배열 관계없이 통과
#
# 각 시그널의 EMA 10-20 / 1-100을 따로 설정 가능
# =========================================================

SIGNAL1_10_20 = "Y"
SIGNAL1_100_200 = "Y"

SIGNAL2_10_20 = "Y"
SIGNAL2_100_200 = "Y"

SIGNAL3_10_20 = "ANY"
SIGNAL3_100_200 = "ANY"


# =========================================================
# 캔들 패턴 설정
# =========================================================

CANDLE_PATTERN_TIMEFRAME = "1h"


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

latest_btc_signal_ema_states = {
    "signal1": {
        "10_20": None,
        "100_200": None
    },
    "signal2": {
        "10_20": None,
        "100_200": None
    },
    "signal3": {
        "10_20": None,
        "100_200": None
    }
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


def get_ema_pair_labels():
    return (
        f"{EMA_PULLBACK}-{EMA_FAST}",
        f"{EMA_SLOW}-{EMA_LONG}"
    )


def get_ema_full_label():
    return (
        f"{EMA_PULLBACK}-{EMA_FAST}-"
        f"{EMA_SLOW}-{EMA_LONG}"
    )


# =========================================================
# 시그널별 설정 가져오기
# =========================================================

def get_signal_conditions(signal_key):
    conditions = {
        "signal1": (
            SIGNAL1_10_20,
            SIGNAL1_100_200
        ),
        "signal2": (
            SIGNAL2_10_20,
            SIGNAL2_100_200
        ),
        "signal3": (
            SIGNAL3_10_20,
            SIGNAL3_100_200
        )
    }

    return conditions.get(
        signal_key,
        ("ANY", "ANY")
    )


def get_condition_label(value):
    if value == "Y":
        return "정배열"

    if value == "N":
        return "역배열"

    return "ANY"


def get_signal_condition_description(signal_key):
    condition_10_20, condition_100_200 = (
        get_signal_conditions(signal_key)
    )

    return (
        f"EMA {EMA_PULLBACK}-{EMA_FAST} "
        f"{get_condition_label(condition_10_20)}"
        f" + EMA {EMA_SLOW}-{EMA_LONG} "
        f"{get_condition_label(condition_100_200)}"
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
# EMA 계산
# =========================================================

def calculate_ema_series(df, period):
    if df is None or df.empty or "close" not in df.columns:
        return None

    closes = pd.to_numeric(
        df["close"],
        errors="coerce"
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
    if df is None or df.empty:
        return None

    fast = calculate_ema(df, fast_period)
    slow = calculate_ema(df, slow_period)

    if fast is None or slow is None:
        return None

    if fast > slow:
        return "정배열"

    if fast < slow:
        return "역배열"

    return "동일"


def get_ema_pair_state(df, fast_period, slow_period):
    return get_ema_alignment_state(
        df,
        fast_period,
        slow_period
    )


# =========================================================
# EMA 10선 터치 후 반등
# =========================================================

def check_ema10_rebound(df):
    if df is None or df.empty:
        return False

    if not {"low", "close"}.issubset(df.columns):
        return False

    ema10_series = calculate_ema_series(
        df,
        EMA_PULLBACK
    )

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
            df.loc[current_index, "close"
            ]
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
# 지정 시간봉 EMA 분석
# =========================================================

def analyze_ema_timeframe(market, timeframe, price=None):
    if timeframe in ("5m", "15m", "1h"):
        df = get_timeframe_dataframe(
            market,
            timeframe,
            price
        )

    elif timeframe == "1d":
        df = get_daily_dataframe(market)

        if df.empty:
            return {
                "10_20": None,
                "100_200": None
            }

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

    else:
        return {
            "10_20": None,
            "100_200": None
        }

    if df is None or df.empty:
        return {
            "10_20": None,
            "100_200": None
        }

    return {
        "10_20": get_ema_pair_state(
            df,
            EMA_PULLBACK,
            EMA_FAST
        ),
        "100_200": get_ema_pair_state(
            df,
            EMA_SLOW,
            EMA_LONG
        )
    }


# =========================================================
# 시그널 2 EMA 1-100 연속 카운팅
#
# 완성된 1시간봉만 사용
# 조건이 연속 유지된 봉의 개수를 계산
# 조건이 깨지면 0부터 다시 시작
# 10회까지 허용
# =========================================================

def get_signal2_100_200_count(market):
    try:
        df = get_cached_dataframe(
            market,
            "1h",
            lambda: get_upbit_1h_candles(
                market,
                EMA_HISTORY_CANDLES
            )
        )

        if df is None or df.empty:
            return 0

        now = datetime.now(KST)

        current_start = now.replace(
            minute=0,
            second=0,
            microsecond=0
        )

        # 현재 진행 중인 1시간봉 제외
        completed_df = df[
            df["datetime"] < current_start
        ].copy()

        completed_df = (
            completed_df
            .sort_values("datetime")
            .drop_duplicates("datetime")
            .reset_index(drop=True)
        )

        if completed_df.empty:
            return 0

        # 기존 EMA 설정 그대로 사용
        fast_series = calculate_ema_series(
            completed_df,
            EMA_SLOW
        )

        slow_series = calculate_ema_series(
            completed_df,
            EMA_LONG
        )

        if fast_series is None or slow_series is None:
            return 0

        combined = pd.DataFrame({
            "fast": fast_series,
            "slow": slow_series
        }).dropna()

        if combined.empty:
            return 0

        count = 0

        # 가장 최근 완성된 봉부터 과거 방향 확인
        for _, row in combined.iloc[::-1].iterrows():

            fast = float(row["fast"])
            slow = float(row["slow"])

            if fast > slow:
                state = "정배열"

            elif fast < slow:
                state = "역배열"

            else:
                state = "동일"

            if check_one_ema_condition(
                state,
                SIGNAL2_100_200
            ):
                count += 1

            else:
                break

        return count

    except Exception as e:
        log.warning(
            "%s 시그널2 카운팅 오류: %s",
            market,
            e
        )

        return 0


# =========================================================
# 시그널별 EMA 분석
# =========================================================

def analyze_all_signal_emas(market, price=None):
    result = {}

    for signal_key, timeframe in SIGNAL_TIMEFRAMES.items():
        try:
            result[signal_key] = analyze_ema_timeframe(
                market,
                timeframe,
                price
            )

        except Exception as e:
            log.warning(
                "%s %s EMA 오류: %s",
                market,
                signal_key,
                e
            )

            result[signal_key] = {
                "10_20": None,
                "100_200": None
            }

    # 시그널 2에만 연속 카운팅 적용
    result["signal2"]["count_100_200"] = (
        get_signal2_100_200_count(market)
    )

    return result


# =========================================================
# 시그널별 조건 판정
# =========================================================

def check_one_ema_condition(state, setting):
    if setting == "Y":
        return state == "정배열"

    if setting == "N":
        return state == "역배열"

    if setting == "ANY":
        return True

    return False


def check_signal_ema_condition(signal_key, states):
    if not states:
        return False

    condition_10_20, condition_100_200 = (
        get_signal_conditions(signal_key)
    )

    state_10_20 = states.get("10_20")
    state_100_200 = states.get("100_200")

    base_condition = bool(
        check_one_ema_condition(
            state_10_20,
            condition_10_20
        )
        and
        check_one_ema_condition(
            state_100_200,
            condition_100_200
        )
    )

    if not base_condition:
        return False

    # 시그널 2만 카운팅 제한 적용
    if signal_key == "signal2":

        count = states.get(
            "count_100_200",
            0
        )

        try:
            count = int(count)
        except Exception:
            return False

        # 1회 이상 10회 이하만 통과
        if count < 1 or count > SIGNAL2_MAX_COUNT:
            return False

    return True


def check_all_signal_conditions(all_states):
    if not all_states:
        return False

    for signal_key in SIGNAL_TIMEFRAMES:
        states = all_states.get(signal_key)

        if not check_signal_ema_condition(
            signal_key,
            states
        ):
            return False

    return True


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
        pc < po and cc > co
        and co <= pc and cc >= po
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
        pc > po and cc < co
        and co >= pc and cc <= po
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

    return bool(cc > midpoint and cc < po)


def is_bearish_piercing(previous, current):
    try:
        po, pc = float(previous["open"]), float(previous["close"])
        co, cc = float(current["open"]), float(current["close"])
    except Exception:
        return False

    if pc <= po or cc >= co:
        return False

    midpoint = (po + pc) / 2

    return bool(cc < midpoint and cc > po)


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

def analyze_signal_candles(market, price, ema_reverse=False):
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
        "current_bullish_engulfing": False,
        "ema10_rebound": False
    }

    timeframe = CANDLE_PATTERN_TIMEFRAME

    if timeframe in ("5m", "15m", "1h"):
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
            "1h": 60
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

    ema10_rebound = check_ema10_rebound(df)

    pattern_info = analyze_previous_current_pattern(
        periods
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

    signal = bool(
        ema_reverse and pattern_signal
    )

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
        ],
        "ema10_rebound": ema10_rebound
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
    all_ema_states,
    volume_rank
):
    coin = market.replace("KRW-", "")

    signal1 = all_ema_states.get("signal1", {})
    signal2 = all_ema_states.get("signal2", {})
    signal3 = all_ema_states.get("signal3", {})

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

        "ema10_rebound": analysis_pattern["ema10_rebound"],

        "previous_pattern": analysis_pattern["previous_pattern"],
        "current_pattern": analysis_pattern["current_pattern"],

        "previous_bullish_engulfing": analysis_pattern[
            "previous_bullish_engulfing"
        ],
        "current_bullish_engulfing": analysis_pattern[
            "current_bullish_engulfing"
        ],

        "ema_signal_states": all_ema_states,

        "signal1_10_20": signal1.get("10_20"),
        "signal1_100_200": signal1.get("100_200"),

        "signal2_10_20": signal2.get("10_20"),
        "signal2_100_200": signal2.get("100_200"),

        "signal2_100_200_count": signal2.get(
            "count_100_200",
            0
        ),

        "signal1_condition": check_signal_ema_condition(
            "signal1",
            signal1
        ),

        "signal2_condition": check_signal_ema_condition(
            "signal2",
            signal2
        ),

        "signal3_condition": check_signal_ema_condition(
            "signal3",
            signal3
        ),

        "all_signal_conditions": check_all_signal_conditions(
            all_ema_states
        )
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
            all_ema_states = analyze_all_signal_emas(
                market,
                price
            )

        except Exception as e:
            log.warning(
                "%s EMA 오류: %s",
                market,
                e
            )

            all_ema_states = {
                "signal1": {
                    "10_20": None,
                    "100_200": None
                },
                "signal2": {
                    "10_20": None,
                    "100_200": None,
                    "count_100_200": 0
                },
                "signal3": {
                    "10_20": None,
                    "100_200": None
                }
            }

        all_ema_condition = check_all_signal_conditions(
            all_ema_states
        )

        try:
            analysis_pattern = analyze_signal_candles(
                market,
                price,
                all_ema_condition
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
                "current_bullish_engulfing": False,
                "ema10_rebound": False
            }

        rows.append(
            make_row(
                rank,
                market,
                item,
                analysis_daily,
                analysis_pattern,
                all_ema_states,
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
            row.get("all_signal_conditions", False)
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
        "UPBIT | TOP%s | SIGNAL1=%s | SIGNAL2=%s | SIGNAL3=%s | SIGNAL=%s",
        TOP_N,
        get_signal_condition_description("signal1"),
        get_signal_condition_description("signal2"),
        get_signal_condition_description("signal3"),
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
# BTC EMA 상태 계산
# =========================================================

def analyze_btc_ema_states(timeframe):
    states = {
        "10_20": None,
        "100_200": None
    }

    timeframe_bars = {
        "5m": "5m",
        "15m": "15m",
        "1h": "1H",
        "1d": "1D"
    }

    bar = timeframe_bars.get(timeframe)

    if bar is None:
        return states

    df = okx_candles(bar, 200)

    if df is None or df.empty:
        return states

    price = latest_btc_okx_price

    if price is not None:
        now = datetime.now(KST)

        if timeframe == "5m":
            current_start = now.replace(
                minute=(now.minute // 5) * 5,
                second=0,
                microsecond=0
            )

        elif timeframe == "15m":
            current_start = now.replace(
                minute=(now.minute // 15) * 15,
                second=0,
                microsecond=0
            )

        elif timeframe == "1h":
            current_start = now.replace(
                minute=0,
                second=0,
                microsecond=0
            )

        else:
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

    states["10_20"] = get_ema_pair_state(
        df,
        EMA_PULLBACK,
        EMA_FAST
    )

    states["100_200"] = get_ema_pair_state(
        df,
        EMA_SLOW,
        EMA_LONG
    )

    return states


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
    global latest_btc_signal_ema_states

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

    for signal_key, timeframe in SIGNAL_TIMEFRAMES.items():
        latest_btc_signal_ema_states[signal_key] = (
            analyze_btc_ema_states(timeframe)
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


def fmt_ema(v):
    if v is None:
        return "-"

    try:
        v = float(v)
    except Exception:
        return "-"

    return fmt_price(v)


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

    return '<span class="ema-same">—</span>'


def ema_state_text(state):
    if state == "정배열":
        return "정배열"

    if state == "역배열":
        return "역배열"

    if state == "동일":
        return "동일"

    return "-"


# =========================================================
# 공통 EMA 상태 표시
#
# BTC / TOP / SIGNAL 모두 동일한 한 줄 표시
# =========================================================

def signal_ema_status_html(all_states):
    all_states = all_states or {}

    pair_1, pair_2 = get_ema_pair_labels()

    pieces = []

    for signal_key in (
        "signal1",
        "signal2",
        "signal3"
    ):
        states = all_states.get(signal_key) or {}

        timeframe = SIGNAL_TIMEFRAMES[signal_key]
        timeframe_label = get_timeframe_label(timeframe)

        state_10_20 = states.get("10_20")
        state_100_200 = states.get("100_200")

        pieces.append(
            f"""
            <span class="ema-signal-group">
                <span class="ema-signal-name">
                    {SIGNAL_LABELS[signal_key]}
                </span>

                <span class="ema-signal-time">
                    {html.escape(timeframe_label)}
                </span>

                <span class="ema-signal-item">
                    EMA{html.escape(pair_1)}
                    {ema_symbol(state_10_20)}
                </span>

                <span class="ema-signal-item">
                    EMA{html.escape(pair_2)}
                    {ema_symbol(state_100_200)}
                </span>
            </span>
            """
        )

    return (
        '<div class="ema-signal-status">'
        '<div class="ema-signal-one-line">'
        '<div class="ema-signal-content">'
        + "".join(pieces)
        + '</div>'
        '</div>'
        '</div>'
    )


# =========================================================
# Row EMA HTML
# =========================================================

def row_ema_status_html(row):
    return signal_ema_status_html(
        row.get("ema_signal_states")
    )


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
# =========================================================

def btc_html():
    ema_html = signal_ema_status_html(
        latest_btc_signal_ema_states
    )

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

        {ema_html}

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
        row.get("signal_4h_current", False)
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

    # 시그널 3개 + 공통 조건을 한 줄로 표시
    condition_html = f"""
    <div class="signal-condition-one-line">

        <span>
            시그널1 {get_timeframe_label(SIGNAL1_TIMEFRAME)}
            · {html.escape(get_signal_condition_description("signal1"))}
        </span>

        <span>
            시그널2 {get_timeframe_label(SIGNAL2_TIMEFRAME)}
            · {html.escape(get_signal_condition_description("signal2"))}
            · 카운팅 최대 {SIGNAL2_MAX_COUNT}회
        </span>

        <span>
            시그널3 {get_timeframe_label(SIGNAL3_TIMEFRAME)}
            · {html.escape(get_signal_condition_description("signal3"))}
        </span>

        <span>
            공통조건 · 당일 변동률 양수
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

    top_ema_html = row_ema_status_html(row)

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

        {top_ema_html}

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

.ema-normal {
    color: #38d878;
    font-weight: 900;
    white-space: nowrap;
}

.ema-reverse {
    color: #ff5966;
    font-weight: 900;
    white-space: nowrap;
}

.ema-same {
    color: #68737e;
    font-weight: 900;
    white-space: nowrap;
}


/* =========================================================
   공통 EMA 상태 표시
   BTC / TOP / SIGNAL 모두 동일한 한 줄 표시
   ========================================================= */

.ema-signal-status {
    width: 100%;
    display: block;
    overflow-x: auto;
    overflow-y: hidden;
    white-space: nowrap;
    padding: 7px 9px;
    background: #0a1015;
    border-bottom: 1px solid #202a33;
    scrollbar-width: none;
}

.ema-signal-status::-webkit-scrollbar {
    display: none;
}

.ema-signal-one-line {
    display: block;
    width: max-content;
    min-width: 100%;
    white-space: nowrap;
}

.ema-signal-content {
    display: inline-flex;
    align-items: center;
    flex-wrap: nowrap;
    gap: 15px;
    white-space: nowrap;
}

.ema-signal-group {
    display: inline-flex;
    align-items: center;
    flex-wrap: nowrap;
    gap: 5px;
    white-space: nowrap;
    line-height: 1.4;
    font-size: 8px;
    font-weight: 900;
}

.ema-signal-name {
    color: #e4c45e;
    font-size: 7px;
    font-weight: 900;
    white-space: nowrap;
}

.ema-signal-time {
    color: #83909b;
    font-size: 7px;
    font-weight: 800;
    white-space: nowrap;
}

.ema-signal-item {
    display: inline-flex;
    align-items: center;
    gap: 2px;
    color: #cbd3da;
    font-size: 7px;
    white-space: nowrap;
}


/* =========================================================
   캔들 패턴
   ========================================================= */

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

/* SIGNAL 조건 3개 + 공통 조건 한 줄 표시 */

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
    min-width: 48px;
    white-space: nowrap;
}

.signal-condition b {
    color: #ff5966;
    font-size: 8px;
    font-weight: 900;
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

.signal-card-top .ema-signal-status {
    width: 100%;
    padding: 5px 7px;
    border: 0;
    background: #0a1015;
}

.signal-card-top .ema-signal-content {
    gap: 12px;
}

.signal-card-top .ema-signal-group {
    gap: 4px;
    font-size: 6px;
}

.signal-card-top .ema-signal-name,
.signal-card-top .ema-signal-time,
.signal-card-top .ema-signal-item {
    font-size: 6px;
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


/* =========================================================
   상승패턴 + EMA 10선 반등 강조
   ========================================================= */

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


/* =========================================================
   BTC 패널
   ========================================================= */

.btc-panel {
    background: #0c1116;
    border: 1px solid #202a33;
    margin-bottom: 13px;
}

.btc-panel .section-head {
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

.btc-panel .ema-signal-status {
    border-top: 1px solid #202a33;
    border-bottom: 0;
}


/* =========================================================
   TOP 리스트
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

.coin-card > .ema-signal-status {
    border-top: 0;
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

    .ema-signal-status {
        padding: 6px 7px;
    }

    .ema-signal-content {
        gap: 10px;
    }

    .ema-signal-group {
        gap: 4px;
        font-size: 6px;
    }

    .ema-signal-name,
    .ema-signal-time,
    .ema-signal-item {
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

    .signal-card-top .ema-signal-status {
        padding: 4px 5px;
    }

    .signal-card-top .ema-signal-content {
        gap: 9px;
    }

    .signal-card-top .ema-signal-group {
        gap: 3px;
    }

    .signal-card-top .ema-signal-name,
    .signal-card-top .ema-signal-time,
    .signal-card-top .ema-signal-item {
        font-size: 5px;
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
        <meta name="viewport" content="width=device-width, initial-scale=1">
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
        "START | BTC EMA 3개 시그널 표시 | "
        "UPBIT TOP%s | EMA %s | "
        "SIGNAL1=%s | SIGNAL2=%s | SIGNAL3=%s",
        TOP_N,
        get_ema_full_label(),
        get_signal_condition_description("signal1"),
        get_signal_condition_description("signal2"),
        get_signal_condition_description("signal3")
    )

    log.info(
        "시그널 2 카운팅 제한: 최대 %s회",
        SIGNAL2_MAX_COUNT
    )

    log.info(
        "시그널 1 시간봉: %s",
        get_timeframe_label(SIGNAL1_TIMEFRAME)
    )

    log.info(
        "시그널 2 시간봉: %s",
        get_timeframe_label(SIGNAL2_TIMEFRAME)
    )

    log.info(
        "시그널 3 시간봉: %s",
        get_timeframe_label(SIGNAL3_TIMEFRAME)
    )

    log.info(
        "캔들 패턴 시간봉: %s",
        get_timeframe_label(CANDLE_PATTERN_TIMEFRAME)
    )

    log.info(
        "EMA 데이터 최대 %s개 / 캐시 공유 활성화",
        EMA_HISTORY_CANDLES
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
