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

TOP_N = 10
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
# SIGNAL 설정
# =========================================================

SIGNAL_TIMEFRAME = "1h"

TIMEFRAME_LABEL = {
    "5m": "5분봉",
    "15m": "15분봉",
    "1h": "1시간봉",
    "4h": "4시간봉",
    "1d": "일봉"
}


# =========================================================
# EMA 설정
# =========================================================

EMA_PULLBACK = 10
EMA_FAST = 20
EMA_SLOW = 100

SIGNAL_5_10 = "ANY"
SIGNAL_10_50 = "ANY"


# =========================================================
# 참고용 EMA 설정
# =========================================================

EMA_REFERENCE_TIMEFRAME = "15m"
EMA_REFERENCE_TIMEFRAMES = [EMA_REFERENCE_TIMEFRAME]

# 참고용 EMA 배열 조건
# Y   = 정배열
# N   = 역배열
# ANY = 배열 방향 무관
REFERENCE_SIGNAL_5_10 = "ANY"
REFERENCE_SIGNAL_10_20 = "Y"

# Y = 참고 EMA 설정 조건을 연속으로 만족한 캔들 수 제한 사용
# N = 카운팅 제한 조건 사용 안 함
REQUIRE_REFERENCE_FULL_ALIGNMENT = "Y"

EMA_REFERENCE_LABELS = {
    EMA_REFERENCE_TIMEFRAME: TIMEFRAME_LABEL.get(
        EMA_REFERENCE_TIMEFRAME,
        EMA_REFERENCE_TIMEFRAME
    )
}

REFERENCE_ALIGNMENT_MAX_CANDLES = 200


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

# BTC 시그널 EMA 상태
latest_btc_signal_ema_states = {
    "5_10": None,
    "10_20": None
}

# BTC 참고 EMA 상태
latest_btc_reference_ema_states = {
    "5_10": None,
    "10_20": None
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


def get_timeframe_label():
    return TIMEFRAME_LABEL.get(
        SIGNAL_TIMEFRAME,
        SIGNAL_TIMEFRAME
    )


def get_reference_timeframe_label():
    return TIMEFRAME_LABEL.get(
        EMA_REFERENCE_TIMEFRAME,
        EMA_REFERENCE_TIMEFRAME
    )


def get_ema_pair_labels():
    return (
        f"{EMA_PULLBACK}-{EMA_FAST}",
        f"{EMA_FAST}-{EMA_SLOW}"
    )


def get_ema_full_label():
    return f"{EMA_PULLBACK}-{EMA_FAST}-{EMA_SLOW}"


# =========================================================
# SIGNAL EMA 조건 설명
# =========================================================

def get_ema_signal_description():
    if SIGNAL_5_10 == "Y":
        state_5_10 = "정배열"
    elif SIGNAL_5_10 == "N":
        state_5_10 = "역배열"
    else:
        state_5_10 = "ANY"

    if SIGNAL_10_50 == "Y":
        state_10_50 = "정배열"
    elif SIGNAL_10_50 == "N":
        state_10_50 = "역배열"
    else:
        state_10_50 = "ANY"

    return (
        f"EMA {EMA_PULLBACK}-{EMA_FAST} {state_5_10} "
        f"+ EMA {EMA_FAST}-{EMA_SLOW} {state_10_50}"
    )


# =========================================================
# 참고용 EMA 조건 설명
# =========================================================

def get_reference_ema_condition_description():
    if REFERENCE_SIGNAL_5_10 == "Y":
        state_5_10 = "정배열"
    elif REFERENCE_SIGNAL_5_10 == "N":
        state_5_10 = "역배열"
    else:
        state_5_10 = "ANY"

    if REFERENCE_SIGNAL_10_20 == "Y":
        state_10_20 = "정배열"
    elif REFERENCE_SIGNAL_10_20 == "N":
        state_10_20 = "역배열"
    else:
        state_10_20 = "ANY"

    count_limit = (
        f"조건 충족 연속 {REFERENCE_ALIGNMENT_MAX_CANDLES}개 캔들 이내"
        if REQUIRE_REFERENCE_FULL_ALIGNMENT == "Y"
        else "조건 충족 캔들 수 제한 미사용"
    )

    return (
        f"참고 EMA {EMA_PULLBACK}-{EMA_FAST} {state_5_10} "
        f"+ EMA {EMA_FAST}-{EMA_SLOW} {state_10_20} "
        f"+ {count_limit}"
    )


# =========================================================
# SIGNAL EMA 조건 판정
# =========================================================

def check_ema_signal_condition(ema5, ema10, ema50):
    if ema5 is None or ema10 is None or ema50 is None:
        return False

    try:
        ema5, ema10, ema50 = map(
            float, (ema5, ema10, ema50)
        )
    except (TypeError, ValueError):
        return False

    if not all(
        np.isfinite(v) for v in (ema5, ema10, ema50)
    ):
        return False

    if SIGNAL_5_10 == "Y":
        condition_5_10 = ema5 > ema10
    elif SIGNAL_5_10 == "N":
        condition_5_10 = ema5 < ema10
    else:
        condition_5_10 = True

    if SIGNAL_10_50 == "Y":
        condition_10_50 = ema10 > ema50
    elif SIGNAL_10_50 == "N":
        condition_10_50 = ema10 < ema50
    else:
        condition_10_50 = True

    return bool(
        condition_5_10 and condition_10_50
    )


# =========================================================
# 참고용 EMA 조건 판정
# =========================================================

def check_reference_ema_condition(
    state_5_10,
    state_10_20
):
    if REFERENCE_SIGNAL_5_10 == "Y":
        condition_5_10 = state_5_10 == "정배열"
    elif REFERENCE_SIGNAL_5_10 == "N":
        condition_5_10 = state_5_10 == "역배열"
    else:
        condition_5_10 = True

    if REFERENCE_SIGNAL_10_20 == "Y":
        condition_10_20 = state_10_20 == "정배열"
    elif REFERENCE_SIGNAL_10_20 == "N":
        condition_10_20 = state_10_20 == "역배열"
    else:
        condition_10_20 = True

    return bool(condition_5_10 and condition_10_20)


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
    endpoint = f"https://api.upbit.com/v1/candles/minutes/{unit}"

    target_count = min(max(int(count), 1), 200)

    params = {
        "market": market,
        "count": target_count
    }

    r = retry(
        requests.get,
        endpoint,
        params=params,
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


# 기존 함수명 유지: 실제 조회는 1시간봉
def get_upbit_4h_candles(market, count=200):
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
# 업비트 4시간봉 생성 - KST 09:00 기준
# =========================================================

def build_upbit_4h_candles(df, current_price=None):
    if df is None or df.empty:
        return pd.DataFrame()

    df = (
        df.sort_values("datetime")
        .drop_duplicates("datetime")
        .reset_index(drop=True)
    )

    now = datetime.now(KST)

    day_start = now.replace(
        hour=9, minute=0, second=0, microsecond=0
    )

    if now < day_start:
        day_start -= timedelta(days=1)

    elapsed_hours = int(
        (now - day_start).total_seconds() // 3600
    )

    current_start = day_start + timedelta(
        hours=(elapsed_hours // 4) * 4
    )

    rows = []

    for _, row in df.iterrows():
        dt = row["datetime"]
        relative_seconds = (
            dt - day_start
        ).total_seconds()

        group_index = int(
            relative_seconds // (4 * 3600)
        )

        period_start = day_start + timedelta(
            hours=group_index * 4
        )

        rows.append({
            "period_start": period_start,
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"])
        })

    if not rows:
        return pd.DataFrame()

    raw = pd.DataFrame(rows)
    grouped = []

    for period_start, group in raw.groupby(
        "period_start", sort=True
    ):
        o = float(group.iloc[0]["open"])
        h = float(group["high"].max())
        l = float(group["low"].min())
        c = float(group.iloc[-1]["close"])

        active = period_start == current_start

        if active and current_price is not None:
            c = float(current_price)
            h = max(h, c)
            l = min(l, c)

        change = (c - o) / o * 100 if o else None

        grouped.append({
            "datetime": period_start,
            "start": period_start,
            "end": period_start + timedelta(hours=4),
            "active": active,
            "label": period_start.strftime("%m/%d %H:%M"),
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "change": change
        })

    return (
        pd.DataFrame(grouped)
        .sort_values("datetime")
        .drop_duplicates("datetime")
        .reset_index(drop=True)
    )


# =========================================================
# EMA 계산
# =========================================================

def calculate_ema_series(df, period):
    if df is None or df.empty or "close" not in df.columns:
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


def get_ema_alignment_state(
    df,
    fast_period=EMA_FAST,
    slow_period=EMA_SLOW
):
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
        df, fast_period, slow_period
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
# 지정 시간봉 데이터 조회 - 캐시 공유
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
            lambda: get_upbit_4h_candles(
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


# =========================================================
# 선택 시간봉 EMA 분석
# =========================================================

def analyze_ema_4h(market, price=None):
    empty_result = {
        "ema5": None,
        "ema15": None,
        "ema5_short": None,
        "ema20": None,
        "ema60": None,
        "reverse": False,
        "bullish_pullback": False,
        "signal_condition": False,
        "alignment": None,
        "cross_previous": False,
        "cross_current": False,
        "cross_signal": False
    }

    timeframe = SIGNAL_TIMEFRAME

    if timeframe in ("5m", "15m", "1h"):
        df = get_timeframe_dataframe(
            market, timeframe, price
        )

    elif timeframe == "4h":
        df_1h = get_timeframe_dataframe(
            market, "1h", price
        )

        if df_1h.empty:
            return empty_result

        df = build_upbit_4h_candles(df_1h, price)

    elif timeframe == "1d":
        df = get_daily_dataframe(market)

        if df.empty:
            return empty_result

        now = datetime.now(KST)

        current_start = now.replace(
            hour=9, minute=0, second=0, microsecond=0
        )

        if now < current_start:
            current_start -= timedelta(days=1)

        df = update_current_candle(
            df, current_start, price
        )

    else:
        log.warning(
            "지원하지 않는 SIGNAL_TIMEFRAME: %s",
            timeframe
        )
        return empty_result

    if df is None or df.empty:
        return empty_result

    ema5_series = calculate_ema_series(
        df, EMA_PULLBACK
    )
    ema10_series = calculate_ema_series(
        df, EMA_FAST
    )
    ema50_series = calculate_ema_series(
        df, EMA_SLOW
    )

    if any(
        x is None
        for x in (ema5_series, ema10_series, ema50_series)
    ):
        return empty_result

    ema5_valid = ema5_series.dropna()
    ema10_valid = ema10_series.dropna()
    ema50_valid = ema50_series.dropna()

    if any(
        x.empty
        for x in (ema5_valid, ema10_valid, ema50_valid)
    ):
        return empty_result

    ema5 = float(ema5_valid.iloc[-1])
    ema10 = float(ema10_valid.iloc[-1])
    ema50 = float(ema50_valid.iloc[-1])

    reverse = ema5 < ema10
    bullish_pullback = (
        ema10 > ema50 and ema5 < ema10
    )

    signal_condition = check_ema_signal_condition(
        ema5, ema10, ema50
    )

    if ema10 > ema50:
        alignment = "정배열"
    elif ema10 < ema50:
        alignment = "역배열"
    else:
        alignment = "동일"

    return {
        "ema5": ema10,
        "ema15": ema50,
        "ema5_short": ema5,
        "ema20": ema10,
        "ema60": ema50,
        "reverse": reverse,
        "bullish_pullback": bullish_pullback,
        "signal_condition": signal_condition,
        "alignment": alignment,
        "cross_previous": False,
        "cross_current": False,
        "cross_signal": False
    }


# =========================================================
# 참고 EMA 조건 충족 연속 캔들 수
# =========================================================

def get_reference_full_alignment_age(df):
    """
    기존에는 EMA_PULLBACK > EMA_FAST > EMA_SLOW인
    완전 정배열 캔들만 카운팅했다.

    수정 후에는 각 캔들마다 다음 설정을 적용한다.

    REFERENCE_SIGNAL_5_10:
        Y   = EMA_PULLBACK > EMA_FAST
        N   = EMA_PULLBACK < EMA_FAST
        ANY = 방향 무관

    REFERENCE_SIGNAL_10_20:
        Y   = EMA_FAST > EMA_SLOW
        N   = EMA_FAST < EMA_SLOW
        ANY = 방향 무관

    마지막 캔들부터 설정 조건을 연속으로 만족한
    캔들 수를 반환한다.
    """

    if df is None or df.empty:
        return 0

    ema_pullback_series = calculate_ema_series(
        df, EMA_PULLBACK
    )
    ema_fast_series = calculate_ema_series(
        df, EMA_FAST
    )
    ema_slow_series = calculate_ema_series(
        df, EMA_SLOW
    )

    if any(
        series is None
        for series in (
            ema_pullback_series,
            ema_fast_series,
            ema_slow_series
        )
    ):
        return 0

    combined = pd.concat([
        ema_pullback_series.rename("ema_pullback"),
        ema_fast_series.rename("ema_fast"),
        ema_slow_series.rename("ema_slow")
    ], axis=1).dropna()

    if combined.empty:
        return 0

    # 각 캔들에서 두 EMA 쌍의 상태를 계산한다.
    condition_results = []

    for _, row in combined.iterrows():
        ema_pullback = float(row["ema_pullback"])
        ema_fast = float(row["ema_fast"])
        ema_slow = float(row["ema_slow"])

        if ema_pullback > ema_fast:
            state_5_10 = "정배열"
        elif ema_pullback < ema_fast:
            state_5_10 = "역배열"
        else:
            state_5_10 = "동일"

        if ema_fast > ema_slow:
            state_10_20 = "정배열"
        elif ema_fast < ema_slow:
            state_10_20 = "역배열"
        else:
            state_10_20 = "동일"

        condition_met = check_reference_ema_condition(
            state_5_10,
            state_10_20
        )

        condition_results.append(bool(condition_met))

    if not condition_results:
        return 0

    # 최신 캔들이 조건을 만족하지 않으면 카운트는 0이다.
    if not condition_results[-1]:
        return 0

    # 마지막 캔들부터 조건을 연속으로 만족하는 수만 센다.
    age = 0

    for condition_met in reversed(condition_results):
        if not condition_met:
            break

        age += 1

    return age


# =========================================================
# 참고 EMA 시간봉 분석 - 동일 캐시 사용
# =========================================================

def analyze_reference_ema_timeframes(market, price=None):
    states = {
        "signal_5_10": None,
        "signal_10_50": None,
        "1h_5_10": None,
        "1h_10_50": None,
        "full_alignment_age": 0,
        "full_alignment_active": False
    }

    timeframe = EMA_REFERENCE_TIMEFRAME

    try:
        if timeframe in ("5m", "15m", "1h"):
            df = get_timeframe_dataframe(
                market, timeframe, price
            )

        elif timeframe == "4h":
            df_1h = get_timeframe_dataframe(
                market, "1h", price
            )

            if df_1h.empty:
                return states

            df = build_upbit_4h_candles(
                df_1h, price
            )

        elif timeframe == "1d":
            df = get_daily_dataframe(market)

            if df.empty:
                return states

            now = datetime.now(KST)

            current_start = now.replace(
                hour=9, minute=0, second=0, microsecond=0
            )

            if now < current_start:
                current_start -= timedelta(days=1)

            df = update_current_candle(
                df, current_start, price
            )

        else:
            log.warning(
                "지원하지 않는 참고 EMA 시간봉: %s",
                timeframe
            )
            return states

        if df is None or df.empty:
            return states

        states["signal_5_10"] = get_ema_pair_state(
            df, EMA_PULLBACK, EMA_FAST
        )

        states["signal_10_50"] = get_ema_pair_state(
            df, EMA_FAST, EMA_SLOW
        )

        states["1h_5_10"] = states["signal_5_10"]
        states["1h_10_50"] = states["signal_10_50"]

        # 수정: 설정된 참고 EMA 조건을 연속으로 만족하는 캔들 수
        condition_age = get_reference_full_alignment_age(df)

        states["full_alignment_age"] = condition_age

        states["full_alignment_active"] = bool(
            1 <= condition_age <= REFERENCE_ALIGNMENT_MAX_CANDLES
        )

    except Exception as e:
        log.warning(
            "%s 참고 EMA 시간봉 오류: %s",
            market, e
        )

    return states


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
        hour=9, minute=0, second=0, microsecond=0
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
# 캔들 패턴 함수
# =========================================================

def is_bullish_engulfing(previous, current):
    try:
        po, pc = float(previous["open"]), float(previous["close"])
        co, cc = float(current["open"]), float(current["close"])
    except Exception:
        return False

    return bool(
        pc < po and cc > co
        and co <= pc and cc >= po
    )


def is_bearish_engulfing(previous, current):
    try:
        po, pc = float(previous["open"]), float(previous["close"])
        co, cc = float(current["open"]), float(current["close"])
    except Exception:
        return False

    return bool(
        pc > po and cc < co
        and co >= pc and cc <= po
    )


def is_positive_doji(current):
    try:
        o, h, l, c = (
            float(current["open"]), float(current["high"]),
            float(current["low"]), float(current["close"])
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
            float(current["open"]), float(current["high"]),
            float(current["low"]), float(current["close"])
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
            float(current["open"]), float(current["high"]),
            float(current["low"]), float(current["close"])
        )
    except Exception:
        return False

    candle_range = h - l

    return bool(
        candle_range > 0 and c > o
        and abs(c - o) / candle_range >= 0.70
    )


def is_long_bearish(current):
    try:
        o, h, l, c = (
            float(current["open"]), float(current["high"]),
            float(current["low"]), float(current["close"])
        )
    except Exception:
        return False

    candle_range = h - l

    return bool(
        candle_range > 0 and c < o
        and abs(c - o) / candle_range >= 0.70
    )


def get_hammer_parts(current):
    try:
        o, h, l, c = (
            float(current["open"]), float(current["high"]),
            float(current["low"]), float(current["close"])
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
        body, candle_range * 0.01
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


def get_candle_pattern(previous, current):
    patterns = get_candle_patterns(previous, current)
    return patterns[0] if patterns else None


BULLISH_HIGHLIGHT_PATTERNS = [
    "상승장악형",
    "상승관통형",
    "장대양봉",
    "상승망치형",
    "상승역망치형"
]


# =========================================================
# 이전봉 / 현재봉 패턴 분석
# =========================================================

def analyze_previous_current_pattern(periods):
    result = {
        "previous_pattern": [],
        "current_pattern": [],
        "previous_bullish_engulfing": False,
        "current_bullish_engulfing": False,
        "bullish_engulfing_signal": False,
        "previous_bullish_pattern": False,
        "current_bullish_pattern": False,
        "bullish_pattern_signal": False
    }

    if not periods:
        return result

    current = periods[-1]
    previous = periods[-2] if len(periods) >= 2 else None
    previous_previous = periods[-3] if len(periods) >= 3 else None

    result["current_pattern"] = get_candle_patterns(
        previous, current
    )

    if previous is not None:
        result["previous_pattern"] = get_candle_patterns(
            previous_previous, previous
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

    result["bullish_pattern_signal"] = bool(
        result["previous_bullish_pattern"]
        or result["current_bullish_pattern"]
    )

    result["bullish_engulfing_signal"] = bool(
        result["current_bullish_engulfing"]
    )

    return result


# =========================================================
# 4시간봉 기간
# =========================================================

def build_upbit_4h_periods(df, current_price=None):
    if df is None or df.empty:
        return []

    periods_df = build_upbit_4h_candles(
        df, current_price
    )

    if periods_df.empty:
        return []

    periods = []

    for _, row in periods_df.iterrows():
        periods.append({
            "start": row["start"],
            "end": row["end"],
            "active": bool(row["active"]),
            "label": row["label"],
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "change": row["change"],
            "signal": False,
            "signal_reason": None,
            "pattern": None
        })

    return periods[-6:]


# =========================================================
# SIGNAL 상세
# =========================================================

def signal_pass(periods, ema_reverse=False):
    return bool(ema_reverse)


def signal_details(periods, ema_reverse=False):
    pattern_info = analyze_previous_current_pattern(periods)

    current_signal = bool(
        pattern_info["current_bullish_pattern"]
    )

    previous_signal = bool(
        pattern_info["previous_bullish_pattern"]
    )

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
        "signal": signal,
        "pattern_signal": pattern_signal,
        "current_signal": current_signal,
        "previous_signal": previous_signal,
        "previous_previous_signal": False,
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
        "signal_reason": reason
    }


# =========================================================
# 선택 시간봉 캔들 패턴 분석
# =========================================================

def analyze_signal_candles(market, price, ema_reverse=False):
    empty_result = {
        "periods": [],
        "signal_pass": False,
        "pattern_signal": False,
        "current_signal": False,
        "previous_signal": False,
        "previous_previous_signal": False,
        "signal_change": None,
        "signal_period": None,
        "signal_reason": None,
        "previous_previous_pattern": [],
        "previous_pattern": [],
        "current_pattern": [],
        "previous_previous_bullish_engulfing": False,
        "previous_bullish_engulfing": False,
        "current_bullish_engulfing": False
    }

    timeframe = SIGNAL_TIMEFRAME

    if timeframe in ("5m", "15m", "1h"):
        df = get_timeframe_dataframe(
            market, timeframe, price
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
                "change": change,
                "signal": False,
                "signal_reason": None,
                "pattern": None
            })

        periods = periods[-6:]

    elif timeframe == "4h":
        df_1h = get_timeframe_dataframe(
            market, "1h", price
        )

        if df_1h.empty:
            return empty_result

        periods = build_upbit_4h_periods(
            df_1h, price
        )

    elif timeframe == "1d":
        df = get_daily_dataframe(market)

        if df.empty:
            return empty_result

        periods = build_daily_periods(df, price)

        periods = [
            {
                "start": p["start"],
                "end": p["end"],
                "active": p["active"],
                "label": p["label"],
                "open": p["open"],
                "high": p["high"],
                "low": p["low"],
                "close": p["close"],
                "change": p["change"],
                "signal": False,
                "signal_reason": None,
                "pattern": None
            }
            for p in periods
        ]

    else:
        log.warning(
            "지원하지 않는 캔들 시간봉: %s",
            timeframe
        )
        return empty_result

    pattern_info = analyze_previous_current_pattern(
        periods
    )

    details = signal_details(
        periods, ema_reverse
    )

    return {
        "periods": periods,
        "signal_pass": details["signal"],
        "pattern_signal": details["pattern_signal"],
        "current_signal": details["current_signal"],
        "previous_signal": details["previous_signal"],
        "previous_previous_signal": False,
        "signal_change": details["signal_change"],
        "signal_period": details["signal_period"],
        "signal_reason": details["signal_reason"],
        "previous_previous_pattern": [],
        "previous_pattern": pattern_info["previous_pattern"],
        "current_pattern": pattern_info["current_pattern"],
        "previous_previous_bullish_engulfing": False,
        "previous_bullish_engulfing": pattern_info[
            "previous_bullish_engulfing"
        ],
        "current_bullish_engulfing": pattern_info[
            "current_bullish_engulfing"
        ]
    }


def analyze_4h(market, price, ema_reverse=False):
    return analyze_signal_candles(
        market, price, ema_reverse
    )


# =========================================================
# 일봉 변동률
# =========================================================

def analyze_daily_change(market, price):
    df = get_daily_dataframe(market)

    if df.empty:
        return {"change": None, "periods": []}

    periods = build_daily_periods(df, price)

    if not periods:
        return {"change": None, "periods": []}

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
    analysis_4h,
    ema_analysis,
    volume_rank,
    reference_ema_states
):
    coin = market.replace("KRW-", "")

    fast_short = ema_analysis.get("ema5_short")
    fast_long = ema_analysis.get("ema20")

    if fast_short is not None and fast_long is not None:
        if fast_short > fast_long:
            ema_1h_5_10 = "정배열"
        elif fast_short < fast_long:
            ema_1h_5_10 = "역배열"
        else:
            ema_1h_5_10 = "동일"
    else:
        ema_1h_5_10 = None

    ema_1h_10_50 = ema_analysis.get("alignment")

    return {
        "rank": rank,
        "name": coin,
        "market": market,
        "volume_24h": item["volume_24h"],
        "current_price": item["current_price"],
        "volume_rank": volume_rank,
        "daily_change": analysis_daily["change"],
        "periods_daily": analysis_daily["periods"],
        "periods_4h": analysis_4h["periods"],

        "signal_4h": analysis_4h["signal_pass"],
        "signal_4h_pattern": analysis_4h.get(
            "pattern_signal", False
        ),
        "signal_4h_current": analysis_4h["current_signal"],
        "signal_4h_previous": analysis_4h["previous_signal"],
        "signal_4h_previous_previous": False,
        "signal_4h_change": analysis_4h["signal_change"],
        "signal_4h_period": analysis_4h["signal_period"],
        "signal_4h_reason": analysis_4h["signal_reason"],

        "previous_previous_pattern": [],
        "previous_pattern": analysis_4h["previous_pattern"],
        "current_pattern": analysis_4h["current_pattern"],

        "previous_previous_bullish_engulfing": False,
        "previous_bullish_engulfing": analysis_4h[
            "previous_bullish_engulfing"
        ],
        "current_bullish_engulfing": analysis_4h[
            "current_bullish_engulfing"
        ],

        "ema5": ema_analysis["ema5"],
        "ema15": ema_analysis["ema15"],
        "ema5_short": ema_analysis.get("ema5_short"),
        "ema20": ema_analysis.get("ema20"),
        "ema60": ema_analysis.get("ema60"),

        "ema_alignment": ema_analysis["alignment"],
        "ema_reverse": ema_analysis["reverse"],
        "ema_bullish_pullback": ema_analysis.get(
            "bullish_pullback", False
        ),
        "ema_signal_condition": ema_analysis.get(
            "signal_condition", False
        ),

        "ema_1h_5_10": ema_1h_5_10,
        "ema_1h_10_50": ema_1h_10_50,

        "ema_timeframe": SIGNAL_TIMEFRAME,
        "ema_fast_period": EMA_FAST,
        "ema_slow_period": EMA_SLOW,

        "ema_cross_previous": False,
        "ema_cross_current": False,
        "ema_cross_signal": False,

        "ema_reference_states": reference_ema_states,
        "simultaneous_signal": analysis_4h["signal_pass"]
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
            all_markets_sorted, 1
        )
    }

    selected_markets = all_markets_sorted[:TOP_N]

    rows = []

    for rank, item in enumerate(selected_markets, 1):
        market = item["market"]
        price = item["current_price"]

        try:
            analysis_daily = analyze_daily_change(
                market, price
            )
        except Exception as e:
            log.warning("%s 일봉 오류: %s", market, e)
            analysis_daily = {
                "change": None,
                "periods": []
            }

        try:
            ema_analysis = analyze_ema_4h(
                market, price
            )
        except Exception as e:
            log.warning("%s EMA 오류: %s", market, e)

            ema_analysis = {
                "ema5": None,
                "ema15": None,
                "ema5_short": None,
                "ema20": None,
                "ema60": None,
                "reverse": False,
                "bullish_pullback": False,
                "signal_condition": False,
                "alignment": None,
                "cross_previous": False,
                "cross_current": False,
                "cross_signal": False
            }

        ema_signal_condition = check_ema_signal_condition(
            ema_analysis.get("ema5_short"),
            ema_analysis.get("ema20"),
            ema_analysis.get("ema60")
        )

        ema_analysis["signal_condition"] = (
            ema_signal_condition
        )

        try:
            analysis_4h = analyze_signal_candles(
                market, price, ema_signal_condition
            )
        except Exception as e:
            log.warning(
                "%s 캔들 패턴 오류: %s",
                market, e
            )

            analysis_4h = {
                "periods": [],
                "signal_pass": False,
                "pattern_signal": False,
                "current_signal": False,
                "previous_signal": False,
                "previous_previous_signal": False,
                "signal_change": None,
                "signal_period": None,
                "signal_reason": None,
                "previous_previous_pattern": [],
                "previous_pattern": [],
                "current_pattern": [],
                "previous_previous_bullish_engulfing": False,
                "previous_bullish_engulfing": False,
                "current_bullish_engulfing": False
            }

        try:
            reference_ema_states = analyze_reference_ema_timeframes(
                market, price
            )
        except Exception as e:
            log.warning(
                "%s 참고 EMA 상태 오류: %s",
                market, e
            )

            reference_ema_states = {
                "signal_5_10": None,
                "signal_10_50": None,
                "1h_5_10": None,
                "1h_10_50": None,
                "full_alignment_age": 0,
                "full_alignment_active": False
            }

        rows.append(
            make_row(
                rank,
                market,
                item,
                analysis_daily,
                analysis_4h,
                ema_analysis,
                volume_rank_map.get(market),
                reference_ema_states
            )
        )

    latest_upbit_data = rows
    latest_upbit_daily_data = rows

    latest_upbit_daily_update_time = kst()
    latest_upbit_update_time = latest_upbit_daily_update_time

    latest_signal_data = [
        row for row in rows
        if (
            row.get("ema_signal_condition", False)
            and row.get("daily_change") is not None
            and row.get("daily_change") > 0

            and check_reference_ema_condition(
                (row.get("ema_reference_states") or {}).get(
                    "signal_5_10"
                ),
                (row.get("ema_reference_states") or {}).get(
                    "signal_10_50"
                )
            )

            and (
                REQUIRE_REFERENCE_FULL_ALIGNMENT != "Y"
                or (
                    (row.get("ema_reference_states") or {}).get(
                        "full_alignment_active", False
                    )
                    and 1 <= (
                        (row.get("ema_reference_states") or {}).get(
                            "full_alignment_age", 0
                        )
                    ) <= REFERENCE_ALIGNMENT_MAX_CANDLES
                )
            )
        )
    ]

    latest_signal_data.sort(
        key=lambda x: x.get("volume_rank", 999999)
    )

    for signal_rank, row in enumerate(
        latest_signal_data, 1
    ):
        row["signal_rank"] = signal_rank

    log.info(
        "UPBIT | 거래대금 TOP%s | EMA%s | 시간봉=%s | "
        "참고 EMA 시간봉=%s | 참고 조건=%s | SIGNAL=%s",
        TOP_N,
        get_ema_full_label(),
        get_timeframe_label(),
        get_reference_timeframe_label(),
        get_reference_ema_condition_description(),
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
                    int(x[0]), unit="ms", utc=True
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
        params={"instId": "BTC-USDT-SWAP"},
        timeout=15
    )

    if r is None:
        return None

    try:
        response_data = r.json()

        if response_data.get("code") != "0":
            return None

        return float(response_data["data"][0]["last"])

    except Exception:
        return None


# =========================================================
# BTC EMA 상태 계산
# =========================================================

def analyze_btc_ema_states(timeframe):
    states = {
        "5_10": None,
        "10_20": None
    }

    timeframe_bars = {
        "5m": "5m",
        "15m": "15m",
        "1h": "1H",
        "4h": "4H",
        "1d": "1D"
    }

    bar = timeframe_bars.get(timeframe)

    if bar is None:
        log.warning(
            "지원하지 않는 BTC EMA 시간봉: %s",
            timeframe
        )
        return states

    df = okx_candles(bar, 200)

    if df is None or df.empty:
        return states

    # 진행 중인 캔들에 현재 BTC 가격을 반영한다.
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
        elif timeframe == "4h":
            day_start = now.replace(
                hour=9,
                minute=0,
                second=0,
                microsecond=0
            )

            if now < day_start:
                day_start -= timedelta(days=1)

            elapsed_hours = int(
                (now - day_start).total_seconds() // 3600
            )

            current_start = day_start + timedelta(
                hours=(elapsed_hours // 4) * 4
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
            df, current_start, price
        )

    ema_pullback = calculate_ema(
        df, EMA_PULLBACK
    )
    ema_fast = calculate_ema(
        df, EMA_FAST
    )
    ema_slow = calculate_ema(
        df, EMA_SLOW
    )

    if any(
        x is None
        for x in (ema_pullback, ema_fast, ema_slow)
    ):
        return states

    if ema_pullback > ema_fast:
        states["5_10"] = "정배열"
    elif ema_pullback < ema_fast:
        states["5_10"] = "역배열"
    else:
        states["5_10"] = "동일"

    if ema_fast > ema_slow:
        states["10_20"] = "정배열"
    elif ema_fast < ema_slow:
        states["10_20"] = "역배열"
    else:
        states["10_20"] = "동일"

    return states


# =========================================================
# OKX BTC KST 09:00 기준 일봉
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
        "day_start", sort=True
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
    global latest_btc_signal_ema_states
    global latest_btc_reference_ema_states

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

    # 시그널 EMA는 SIGNAL_TIMEFRAME 설정을 사용한다.
    latest_btc_signal_ema_states = (
        analyze_btc_ema_states(SIGNAL_TIMEFRAME)
    )

    # 참고 EMA는 EMA_REFERENCE_TIMEFRAME 설정을 사용한다.
    latest_btc_reference_ema_states = (
        analyze_btc_ema_states(EMA_REFERENCE_TIMEFRAME)
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
        log.exception("업데이트 오류: %s", e)

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


def ema_alignment_html(alignment):
    if alignment == "역배열":
        return '<span class="ema-reverse">▼ 역배열</span>'

    if alignment == "정배열":
        return '<span class="ema-normal">▲ 정배열</span>'

    if alignment == "동일":
        return '<span class="ema-same">— 동일</span>'

    return '<span class="ema-same">—</span>'


# =========================================================
# EMA 배열 기호
# =========================================================

def ema_symbol(state):
    if state == "정배열":
        return '<span class="ema-normal">▲</span>'

    if state == "역배열":
        return '<span class="ema-reverse">▼</span>'

    if state == "동일":
        return '<span class="ema-same">—</span>'

    return '<span class="ema-same">—</span>'


# =========================================================
# 탑리스트 EMA 두 줄 표시
# =========================================================

def top_ema_status_html(row):
    reference_states = (
        row.get("ema_reference_states") or {}
    )

    state_1h_5_10 = row.get("ema_1h_5_10")
    state_1h_10_50 = row.get("ema_1h_10_50")

    state_ref_5_10 = reference_states.get("signal_5_10")
    state_ref_10_50 = reference_states.get("signal_10_50")

    signal_label = get_timeframe_label()
    reference_label = get_reference_timeframe_label()

    pair_1, pair_2 = get_ema_pair_labels()

    return f"""
    <div class="top-ema-status">
        <div class="top-ema-line">
            <span class="top-ema-time">
                {html.escape(signal_label)}
            </span>
            <span>
                {html.escape(pair_1)} {ema_symbol(state_1h_5_10)}
            </span>
            <span>
                {html.escape(pair_2)} {ema_symbol(state_1h_10_50)}
            </span>
        </div>
        <div class="top-ema-line">
            <span class="top-ema-time">
                {html.escape(reference_label)}
            </span>
            <span>
                {html.escape(pair_1)} {ema_symbol(state_ref_5_10)}
            </span>
            <span>
                {html.escape(pair_2)} {ema_symbol(state_ref_10_50)}
            </span>
        </div>
    </div>
    """


# =========================================================
# 참고 EMA 상태 HTML
# =========================================================

def reference_ema_states_html(row):
    states = row.get("ema_reference_states") or {}

    timeframe_label = get_reference_timeframe_label()
    pair_1, pair_2 = get_ema_pair_labels()

    display_items = [
        (pair_1, states.get("signal_5_10")),
        (pair_2, states.get("signal_10_50"))
    ]

    pieces = []

    for pair_label, state in display_items:
        if state == "정배열":
            cls, symbol = "ema-normal", "▲"
        elif state == "역배열":
            cls, symbol = "ema-reverse", "▼"
        elif state == "동일":
            cls, symbol = "ema-same", "—"
        else:
            cls, symbol = "ema-same", "—"

        pieces.append(
            f'<div class="ema-ref-item {cls}">'
            f'<span class="ema-ref-label">'
            f'{html.escape(timeframe_label)} '
            f'{html.escape(pair_label)}'
            f'</span> {symbol} '
            f'{html.escape(state) if state else "-"}'
            f'</div>'
        )

    return (
        '<div class="ema-ref-row" '
        f'title="{html.escape(timeframe_label)} 기준 EMA 상태">'
        + "".join(pieces)
        + '</div>'
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
    <div class="candle-pattern-visual empty"><span>—</span></div>
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
        elif item in [
            "하락장악형", "하락관통형", "장대음봉",
            "음수도지", "하락망치형", "하락역망치형"
        ]:
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


def signal_reason_html(reason):
    if reason in BULLISH_HIGHLIGHT_PATTERNS:
        return (
            f'<span class="pattern-cross">'
            f'▲ {html.escape(reason)}</span>'
        )

    return ""


# =========================================================
# BTC HTML - 시그널 EMA / 참고 EMA 표시
# =========================================================

def btc_html():
    signal_states = (
        latest_btc_signal_ema_states or {}
    )

    reference_states = (
        latest_btc_reference_ema_states or {}
    )

    pair_1, pair_2 = get_ema_pair_labels()

    signal_label = get_timeframe_label()
    reference_label = get_reference_timeframe_label()

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
            <div class="btc-ema-line">
                <span class="btc-ema-time">
                    시그널 {html.escape(signal_label)}
                </span>
                <span>
                    EMA {html.escape(pair_1)}
                    {ema_symbol(signal_states.get("5_10"))}
                </span>
                <span>
                    EMA {html.escape(pair_2)}
                    {ema_symbol(signal_states.get("10_20"))}
                </span>
            </div>

            <div class="btc-ema-line">
                <span class="btc-ema-time">
                    참고 {html.escape(reference_label)}
                </span>
                <span>
                    EMA {html.escape(pair_1)}
                    {ema_symbol(reference_states.get("5_10"))}
                </span>
                <span>
                    EMA {html.escape(pair_2)}
                    {ema_symbol(reference_states.get("10_20"))}
                </span>
            </div>
        </div>
    </section>
    """


# =========================================================
# SIGNAL 카드
# =========================================================

def signal_card(row):
    timeframe_label = get_timeframe_label()

    has_bullish_pattern = row.get(
        "signal_4h_pattern", False
    )

    reference_states = (
        row.get("ema_reference_states") or {}
    )

    reference_5_10 = reference_states.get("signal_5_10")
    reference_10_50 = reference_states.get("signal_10_50")

    has_reference_reverse = (
        reference_5_10 == "역배열"
        or reference_10_50 == "역배열"
    )

    badge_html = (
        '<span class="signal-type-badge">⭐ 상승패턴</span>'
        if has_bullish_pattern else ""
    )

    card_class = (
        "signal-card has-reference-reverse"
        if has_reference_reverse
        else "signal-card"
    )

    condition_age = reference_states.get(
        "full_alignment_age", 0
    )

    return f"""
    <div class="{card_class}">
        <div class="signal-coin">
            <span class="signal-rank">
                거래대금 #{row.get("volume_rank", "-")}
            </span>
            <b>{html.escape(row["name"])}</b>
            <div class="signal-type-list">{badge_html}</div>
            {reference_ema_states_html(row)}
            <span class="alignment-age">
                조건 충족 {condition_age}/{REFERENCE_ALIGNMENT_MAX_CANDLES}
            </span>
        </div>
        <div class="signal-volume">
            <span>거래대금</span>
            <strong>{fmt_vol(row["volume_24h"])}</strong>
        </div>
        <div class="signal-change">
            <span>변동률</span>
            <strong>{fmt_change(row.get("daily_change"))}</strong>
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
    """


# =========================================================
# SIGNAL 영역
# =========================================================

def signal_section():
    timeframe_label = get_timeframe_label()
    reference_timeframe_label = get_reference_timeframe_label()
    ema_full_label = get_ema_full_label()

    signal_description = html.escape(
        get_ema_signal_description()
    )

    reference_description = html.escape(
        get_reference_ema_condition_description()
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

    return f"""
    <section class="signal-panel">
        <div class="section-head signal-panel-head">
            <div>
                <span class="section-kicker">SIGNAL</span>
                <b>🔴 EMA{ema_full_label} · {timeframe_label} SIGNAL</b>
            </div>
            <span class="update-time">
                EMA {timeframe_label} · 캔들패턴 2개 봉 · {kst()}
            </span>
        </div>
        <div class="signal-condition">
            <span>SIGNAL 조건</span>
            <b>{signal_description}</b>
            <small>
                당일 변동률 양수 · 참고
                {html.escape(reference_timeframe_label)}
                조건 적용
            </small>
        </div>
        <div class="signal-condition">
            <span>참고 EMA</span>
            <b>{reference_description}</b>
            <small>
                조건 충족 연속 최대 {REFERENCE_ALIGNMENT_MAX_CANDLES}개 캔들
            </small>
        </div>
        <div class="signal-header">
            <div>종목 / EMA 참고</div>
            <div>거래대금</div>
            <div>변동률</div>
            <div>이전봉</div>
            <div>현재봉</div>
        </div>
        <div class="signal-list">{body}</div>
    </section>
    """


# =========================================================
# TOP 카드
# =========================================================

def card(row, kind):
    timeframe_label = get_timeframe_label()

    if kind != "top":
        return ""

    has_bullish_pattern = row.get(
        "signal_4h_pattern", False
    )

    status = (
        '<span class="signal-badge">⭐ 상승패턴</span>'
        if has_bullish_pattern else ""
    )

    top_ema_html = top_ema_status_html(row)

    return f"""
    <article class="coin-card">
        <div class="coin-head">
            <div class="coin-title">
                <span class="rank">#{row["rank"]}</span>
                <b>{html.escape(row["name"])}</b>
            </div>
            {status}
        </div>
        {top_ema_html}
        <div class="market-summary">
            <div>
                <span>현재가</span>
                <strong>{fmt_price(row["current_price"])}</strong>
            </div>
            <div>
                <span>24H 거래대금</span>
                <strong>{fmt_vol(row["volume_24h"])}</strong>
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
* { box-sizing: border-box; }
html { background: #080b0f; }
body {
    margin: 0; padding: 8px; background: #080b0f; color: #e8edf2;
    font-family: Arial, "Noto Sans KR", sans-serif; font-size: 11px;
}
h1 {
    margin: 4px 2px 12px; font-size: 14px; font-weight: 800;
    letter-spacing: -0.4px; color: #f1f4f7;
}
section { margin-bottom: 12px; }
.up { color: #38d878 !important; font-weight: 900; }
.down { color: #ff5966 !important; font-weight: 900; }
.zero { color: #68737e; font-weight: 800; }
.daily-change-value .up { color: #38d878 !important; font-weight: 900; font-size: 9px; }
.daily-change-value .down { color: #ff5966 !important; font-weight: 900; font-size: 9px; }
.ema-normal { color: #38d878; font-size: 7px; font-weight: 900; white-space: nowrap; }
.ema-reverse { color: #ff5966; font-size: 7px; font-weight: 900; white-space: nowrap; }
.ema-same { color: #68737e; font-size: 7px; font-weight: 900; white-space: nowrap; }

.top-ema-status {
    display: flex; flex-direction: column; align-items: flex-start;
    justify-content: center; gap: 4px; padding: 6px 9px;
    background: #0a1015; border-bottom: 1px solid #202a33;
}
.top-ema-line {
    display: flex; align-items: center; gap: 12px; font-size: 8px;
    font-weight: 900; white-space: nowrap; line-height: 1.4;
}
.top-ema-time { width: 38px; color: #83909b; font-size: 8px; font-weight: 800; }
.top-ema-line > span:not(.top-ema-time) { display: inline-flex; align-items: center; gap: 3px; }

.ema-ref-row {
    display: flex; flex-direction: column; align-items: flex-start;
    justify-content: center; gap: 3px; flex-wrap: nowrap;
    white-space: nowrap; min-width: 0;
}
.ema-ref-item { display: flex; align-items: center; gap: 2px; font-size: 7px; font-weight: 900; line-height: 1.3; white-space: nowrap; }
.ema-ref-label { color: #83909b; font-size: 6px; font-weight: 700; white-space: nowrap; }

.pattern-box { display: flex; flex-direction: column; align-items: center; justify-content: center; min-width: 55px; line-height: 1; }
.pattern-name { display: block; margin-top: 2px; color: #38d878; font-size: 5px; font-weight: 900; white-space: nowrap; }
.bearish-pattern .pattern-name { color: #ff5966; }
.neutral-pattern .pattern-name { color: #e4c45e; }
.pattern-none .pattern-name { color: #68737e; }
.pattern-multi { display: flex; align-items: flex-start; justify-content: center; flex-wrap: wrap; gap: 3px; width: 100%; }
.pattern-multi .pattern-box { min-width: 36px; }

.candle-pattern-visual { height: 23px; min-width: 35px; display: flex; align-items: center; justify-content: center; gap: 4px; position: relative; }
.mini-candle { width: 7px; height: 22px; position: relative; display: flex; justify-content: center; align-items: center; }
.mini-candle .wick { position: absolute; width: 1px; height: 22px; left: 50%; top: 0; transform: translateX(-50%); background: #79848e; }
.mini-candle .body { position: relative; z-index: 2; width: 7px; height: 10px; border-radius: 1px; }
.mini-candle.bearish .body { background: #ff5966; height: 12px; }
.mini-candle.bullish .body { background: #38d878; height: 17px; }
.mini-candle.bullish.large .body, .mini-candle.bearish.large .body { height: 20px; width: 9px; }
.mini-candle.bullish-doji .body { background: #38d878; height: 2px; width: 10px; }
.mini-candle.bearish-doji .body { background: #ff5966; height: 2px; width: 10px; }
.mini-candle.piercing-candle .body { background: #38d878; height: 15px; }
.mini-candle.piercing-bearish .body { background: #ff5966; height: 15px; }
.mini-candle.bullish.very-large .body { background: #38d878; height: 20px; width: 9px; }
.mini-candle.bearish.very-large .body { background: #ff5966; height: 20px; width: 9px; }
.mini-candle.hammer .wick, .mini-candle.inverted-hammer .wick { height: 22px; top: 0; }
.mini-candle.hammer .body { position: absolute; top: 1px; height: 6px; width: 8px; }
.mini-candle.bullish.hammer .body, .mini-candle.bullish.inverted-hammer .body { background: #38d878; }
.mini-candle.bearish.hammer .body, .mini-candle.bearish.inverted-hammer .body { background: #ff5966; }
.mini-candle.inverted-hammer .body { position: absolute; bottom: 1px; height: 6px; width: 8px; }

.candle-pattern-visual.empty { color: #68737e; font-size: 9px; height: 23px; }

.signal-panel { background: #0c1116; border: 1px solid #48272c; margin-bottom: 13px; }
.signal-panel-head { border-bottom: 1px solid #392126; }
.signal-condition { min-height: 32px; padding: 0 9px; display: flex; align-items: center; gap: 8px; background: #120e10; border-bottom: 1px solid #302024; }
.signal-condition span { color: #7d6b70; font-size: 6px; }
.signal-condition b { color: #ff5966; font-size: 8px; font-weight: 900; }
.signal-condition small { color: #626e78; font-size: 6px; margin-left: auto; }

.signal-header, .signal-card { display: grid; grid-template-columns: 1.1fr 0.9fr 0.8fr 1.1fr 1.1fr; align-items: center; }
.signal-header { min-height: 27px; padding: 0 9px; color: #69747e; font-size: 6px; font-weight: 800; background: #0b1015; border-bottom: 1px solid #20282f; }
.signal-header > div { text-align: center; }
.signal-card { min-height: 58px; background: #0d1217; border-bottom: 1px solid #20282f; }
.signal-card.has-reference-reverse { background: #171316; box-shadow: inset 3px 0 0 #ff5966; }
.signal-card.has-reference-reverse .signal-coin b { color: #ff5966; }
.signal-card:last-child { border-bottom: 0; }
.signal-card > div { min-height: 58px; padding: 3px; display: flex; flex-direction: column; align-items: center; justify-content: center; text-align: center; }
.signal-card > div + div { border-left: 1px solid #1c252c; }
.signal-coin { flex-direction: row !important; flex-wrap: wrap; gap: 5px; justify-content: flex-start !important; padding-left: 7px !important; }
.signal-rank { color: #b84d58; font-size: 7px; font-weight: 900; }
.signal-coin b { color: #e5e9ed; font-size: 9px; font-weight: 900; }
.signal-type-list { width: 100%; display: flex; flex-wrap: wrap; align-items: center; justify-content: flex-start; gap: 2px; margin-top: 1px; }
.signal-type-badge { color: #e4c45e !important; font-size: 5px !important; font-weight: 900; }
.signal-card span { color: #69747e; font-size: 6px; }
.signal-card .ema-ref-item.ema-normal { color: #38d878 !important; }
.signal-card .ema-ref-item.ema-reverse { color: #ff5966 !important; }
.signal-card .ema-ref-item.ema-same { color: #68737e !important; }
.signal-card strong { margin-top: 2px; color: #dce2e7; font-size: 8px; }
.signal-card .ema-ref-row { width: 100%; justify-content: center; align-items: flex-start; gap: 3px; }
.signal-card .ema-ref-item { font-size: 7px; }
.signal-card .ema-ref-label { font-size: 6px; }
.signal-pattern .pattern-box { min-width: 42px; }
.signal-pattern .pattern-name { font-size: 5px; }
.signal-pattern .candle-pattern-visual { height: 22px; min-width: 32px; transform: scale(.80); }
.signal-pattern .pattern-multi { gap: 1px; }
.signal-pattern .pattern-multi .pattern-box { min-width: 29px; }
.alignment-age { width: 100%; color: #e4c45e !important; font-size: 6px !important; font-weight: 800; margin-top: 2px; }
.signal-empty { min-height: 46px; display: flex; align-items: center; justify-content: center; color: #626e78; font-size: 7px; }

.btc-panel { background: #0c1116; border: 1px solid #202a33; margin-bottom: 13px; }
.btc-panel .section-head { height: 38px; padding: 0 10px; display: flex; align-items: center; justify-content: space-between; border-bottom: 1px solid #202a33; }
.section-head > div { display: flex; align-items: center; gap: 7px; }
.section-head b { font-size: 10px; font-weight: 800; }
.section-kicker { color: #7d8994; font-size: 6px; letter-spacing: 1px; font-weight: 700; }
.update-time { color: #65717c; font-size: 6px; }
.btc-main { min-height: 54px; display: grid; grid-template-columns: 0.7fr 1.5fr 1fr; align-items: center; }
.btc-main > div { text-align: center; padding: 5px; }
.btc-name { color: #c4ccd3; font-size: 10px; font-weight: 800; }
.btc-price { color: #f4f6f8; font-size: 16px; font-weight: 900; letter-spacing: -0.5px; }
.btc-change { font-size: 11px; font-weight: 900; }

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
    min-width: 75px;
    color: #83909b;
    font-size: 7px;
    font-weight: 800;
}

.coin-card { background: #0c1116; border: 1px solid #202a33; margin-bottom: 7px; }
.coin-head { min-height: 38px; padding: 0 9px; display: flex; align-items: center; justify-content: space-between; border-bottom: 1px solid #202a33; }
.coin-title { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
.coin-title b { font-size: 10px; font-weight: 900; }
.rank { color: #c9a83d; font-size: 8px; font-weight: 800; }
.signal-badge { color: #e4c45e; font-size: 7px; font-weight: 900; }

.market-summary { display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); border-bottom: 1px solid #202a33; }
.market-summary > div { min-width: 0; min-height: 58px; padding: 4px 1px; text-align: center; background: #0d1318; display: flex; flex-direction: column; align-items: center; justify-content: center; }
.market-summary > div + div { border-left: 1px solid #1d262e; }
.market-summary span { display: block; color: #65717b; font-size: 6px; margin-bottom: 3px; }
.market-summary strong { display: block; color: #dce2e7; font-size: 7px; }
.market-summary .daily-change-value .up { color: #38d878 !important; }
.market-summary .daily-change-value .down { color: #ff5966 !important; }
.market-summary .daily-change-value .zero { color: #68737e !important; }
.market-summary .pattern-box { min-width: 35px; }
.market-summary .pattern-name { font-size: 4.5px; }
.market-summary .candle-pattern-visual { height: 21px; transform: scale(.68); }
.market-summary .pattern-multi { gap: 0; }
.market-summary .pattern-multi .pattern-box { min-width: 24px; }
.market-summary .pattern-multi .pattern-name { font-size: 3.8px; }

@media (max-width: 600px) {
    body { padding: 5px; }
    h1 { margin: 3px 2px 9px; font-size: 12px; }
    .section-head { padding: 0 8px; }
    .section-head b { font-size: 9px; }
    .update-time { font-size: 5px; }
    .btc-main { min-height: 48px; }
    .btc-price { font-size: 13px; }
    .btc-change { font-size: 9px; }
    .btc-ema-status { padding: 6px 7px; gap: 4px; }
    .btc-ema-line { gap: 9px; font-size: 7px; }
    .btc-ema-time { min-width: 68px; font-size: 6px; }

    .coin-head { min-height: 35px; padding: 0 7px; }
    .coin-title { gap: 6px; }
    .coin-title b { font-size: 9px; }
    .ema-normal, .ema-reverse, .ema-same { font-size: 6px; }
    .top-ema-status { padding: 5px 7px; gap: 4px; }
    .top-ema-line { gap: 10px; font-size: 7px; }
    .top-ema-time { width: 38px; font-size: 7px; }
    .ema-ref-row { gap: 3px; }
    .ema-ref-item { font-size: 6px; }
    .ema-ref-label { font-size: 5px; }

    .market-summary { grid-template-columns: repeat(5, minmax(0, 1fr)); }
    .market-summary > div { min-height: 53px; padding: 3px 1px; }
    .market-summary span { font-size: 5px; margin-bottom: 2px; }
    .market-summary strong { font-size: 5px; }
    .market-summary .pattern-box { min-width: 24px; }
    .market-summary .pattern-name { font-size: 3.5px; }
    .market-summary .candle-pattern-visual { height: 19px; transform: scale(.56); }

    .signal-condition { min-height: 29px; padding: 0 7px; }
    .signal-condition b { font-size: 7px; }
    .signal-condition small { font-size: 5px; }
    .signal-header, .signal-card { grid-template-columns: 1.1fr 0.9fr 0.8fr 1.1fr 1.1fr; }
    .signal-header { padding: 0 3px; font-size: 5px; }
    .signal-card > div { min-height: 53px; padding: 2px 1px; }
    .signal-coin { gap: 4px; padding-left: 5px !important; }
    .signal-coin b { font-size: 7px; }
    .signal-rank { font-size: 5px; }
    .signal-card span { font-size: 5px; }
    .signal-card strong { font-size: 5px; }
    .signal-type-list { gap: 1px; margin-top: 1px; }
    .signal-type-badge { font-size: 4px !important; }
    .signal-pattern .pattern-box { min-width: 24px; }
    .signal-pattern .pattern-name { font-size: 3.5px; }
    .signal-pattern .candle-pattern-visual { height: 19px; transform: scale(.55); }
    .signal-pattern .pattern-multi .pattern-box { min-width: 22px; }
    .signal-card .ema-ref-row { gap: 2px; }
    .signal-card .ema-ref-item { font-size: 5px; }
    .signal-card .ema-ref-label { font-size: 4px; }
    .alignment-age { font-size: 5px !important; }
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
                    card(r, "top")
                    for r in latest_upbit_data
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
        <style>{CSS}</style>
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
            log.exception("scheduler: %s", e)

        time.sleep(1)


# =========================================================
# STARTUP
# =========================================================

@app.on_event("startup")
def startup():
    pair_1, pair_2 = get_ema_pair_labels()

    log.info(
        "START | BTC KST 09:00 일봉 재구성 + "
        "BTC 시그널 EMA 및 참고 EMA 표시 + "
        "업비트 TOP%s + EMA%s %s + "
        "이전봉/현재봉 캔들 패턴 표시 + "
        "%s SIGNAL + 당일 변동률 양수 조건 + "
        "상승패턴 강조 + 참고 EMA 시간봉 %s 및 %s 표시 + "
        "참고 EMA 조건 충족 연속 최대 %s개 캔들",
        TOP_N,
        get_ema_full_label(),
        get_timeframe_label(),
        get_ema_signal_description(),
        pair_1,
        pair_2,
        REFERENCE_ALIGNMENT_MAX_CANDLES
    )

    log.info(
        "참고 EMA 시간 설정: %s",
        get_reference_timeframe_label()
    )

    log.info(
        "참고 EMA 조건: %s",
        get_reference_ema_condition_description()
    )

    log.info(
        "캔들 설정: 최대 %s개 / 분봉 요청 청크 %s개 / 캐시 공유 활성화",
        EMA_HISTORY_CANDLES,
        HISTORY_CHUNK
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
