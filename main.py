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
SHOW_TOP_LIST = "N"
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

SIGNAL_TIMEFRAME = "15m"

TIMEFRAME_LABEL = {
    "15m": "15분봉",
    "1h": "1시간봉",
    "4h": "4시간봉",
    "1d": "일봉"
}


# =========================================================
# EMA 설정
# =========================================================

EMA_PULLBACK = 5
EMA_FAST = 20
EMA_SLOW = 60

EMA_REFERENCE_TIMEFRAMES = [SIGNAL_TIMEFRAME]

EMA_REFERENCE_LABELS = {
    SIGNAL_TIMEFRAME: TIMEFRAME_LABEL.get(
        SIGNAL_TIMEFRAME,
        SIGNAL_TIMEFRAME
    )
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


# =========================================================
# Lock
# =========================================================

request_lock = threading.Lock()
update_lock = threading.Lock()

last_request_time = 0


# =========================================================
# 시간
# =========================================================

def kst():
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


def get_timeframe_label():
    return TIMEFRAME_LABEL.get(
        SIGNAL_TIMEFRAME,
        SIGNAL_TIMEFRAME
    )


def get_ema_pair_labels():
    return (
        f"{EMA_PULLBACK}/{EMA_FAST}",
        f"{EMA_FAST}/{EMA_SLOW}"
    )


def get_ema_full_label():
    return f"{EMA_PULLBACK}/{EMA_FAST}/{EMA_SLOW}"


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

            if (
                not hasattr(r, "status_code")
                or r.status_code == 200
            ):
                return r

            if r.status_code == 429:
                time.sleep(
                    min(RATE_LIMIT_WAIT * (n + 1), 60)
                )

            elif r.status_code >= 500:
                time.sleep(
                    min(2 * (n + 1), 30)
                )

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
                time.sleep(
                    min(2 * (n + 1), 20)
                )

    return None


# =========================================================
# 업비트 마켓
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
                    "volume_24h": float(
                        x["acc_trade_price_24h"]
                    )
                })
            except Exception:
                pass

    latest_upbit_markets = [
        x["market"] for x in result
    ]

    return result


# =========================================================
# 업비트 공통 분봉 조회
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

        r = retry(
            requests.get,
            endpoint,
            params=params,
            timeout=15
        )

        if r is None:
            break

        try:
            data = r.json()
        except Exception:
            break

        if not isinstance(data, list) or not data:
            break

        batch_rows = []

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

                batch_rows.append({
                    "datetime": dt_kst,
                    "datetime_utc": dt_utc,
                    "open": float(x["opening_price"]),
                    "high": float(x["high_price"]),
                    "low": float(x["low_price"]),
                    "close": float(x["trade_price"])
                })

            except Exception:
                pass

        if not batch_rows:
            break

        rows.extend(batch_rows)

        oldest = min(
            batch_rows,
            key=lambda x: x["datetime_utc"]
        )

        next_to = oldest["datetime_utc"].isoformat()

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
# 업비트 15분봉
# =========================================================

def get_upbit_15m_candles(market, count=200):
    return get_upbit_minute_candles(
        market,
        15,
        count
    )


# =========================================================
# 업비트 1시간봉
# 기존 함수명 유지
# =========================================================

def get_upbit_4h_candles(market, count=200):
    return get_upbit_minute_candles(
        market,
        60,
        count
    )


# =========================================================
# 업비트 일봉
# KST 09:00 기준
# =========================================================

def get_upbit_daily_candles(market, count=200):
    endpoint = "https://api.upbit.com/v1/candles/days"

    r = retry(
        requests.get,
        endpoint,
        params={
            "market": market,
            "count": min(count, 200)
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
# 업비트 09:00 기준 4시간봉 생성
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

    current_start = (
        day_start
        + timedelta(hours=(elapsed_hours // 4) * 4)
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

        period_start = (
            day_start
            + timedelta(hours=group_index * 4)
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
        "period_start",
        sort=True
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

        change = (
            (c - o) / o * 100
            if o
            else None
        )

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
# SMA 초기값 + EMA 재귀 계산
# =========================================================

def calculate_ema_series(df, period):
    if df is None or df.empty:
        return None

    if "close" not in df.columns:
        return None

    closes = pd.to_numeric(
        df["close"],
        errors="coerce"
    )

    valid_mask = closes.notna()
    closes = closes.loc[valid_mask]

    if closes.empty:
        return None

    values = closes.to_numpy(dtype=float)
    result = np.full(len(values), np.nan)

    if len(values) < period:
        result[0] = values[0]
        alpha = 2.0 / (period + 1.0)

        for i in range(1, len(values)):
            result[i] = (
                alpha * values[i]
                + (1.0 - alpha) * result[i - 1]
            )

    else:
        alpha = 2.0 / (period + 1.0)

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

    if timeframe == "15m":
        df = get_upbit_15m_candles(
            market,
            EMA_HISTORY_CANDLES
        )

        if df.empty:
            return empty_result

        now = datetime.now(KST)

        current_start = now.replace(
            minute=(now.minute // 15) * 15,
            second=0,
            microsecond=0
        )

        df = update_current_candle(
            df,
            current_start,
            price
        )

    elif timeframe == "1h":
        df = get_upbit_4h_candles(
            market,
            EMA_HISTORY_CANDLES
        )

        if df.empty:
            return empty_result

        now = datetime.now(KST)

        current_start = now.replace(
            minute=0,
            second=0,
            microsecond=0
        )

        df = update_current_candle(
            df,
            current_start,
            price
        )

    elif timeframe == "4h":
        df_1h = get_upbit_4h_candles(
            market,
            EMA_HISTORY_CANDLES
        )

        if df_1h.empty:
            return empty_result

        df = build_upbit_4h_candles(
            df_1h,
            price
        )

        if df.empty:
            return empty_result

    elif timeframe == "1d":
        df = get_upbit_daily_candles(
            market,
            200
        )

        if df.empty:
            return empty_result

        now = datetime.now(KST)

        current_day_start = now.replace(
            hour=9,
            minute=0,
            second=0,
            microsecond=0
        )

        if now < current_day_start:
            current_day_start -= timedelta(days=1)

        df = update_current_candle(
            df,
            current_day_start,
            price
        )

    else:
        log.warning(
            "지원하지 않는 SIGNAL_TIMEFRAME: %s",
            timeframe
        )
        return empty_result

    ema5_series = calculate_ema_series(
        df,
        EMA_PULLBACK
    )

    ema20_series = calculate_ema_series(
        df,
        EMA_FAST
    )

    ema60_series = calculate_ema_series(
        df,
        EMA_SLOW
    )

    if (
        ema5_series is None
        or ema20_series is None
        or ema60_series is None
    ):
        return empty_result

    ema5_valid = ema5_series.dropna()
    ema20_valid = ema20_series.dropna()
    ema60_valid = ema60_series.dropna()

    if (
        ema5_valid.empty
        or ema20_valid.empty
        or ema60_valid.empty
    ):
        return empty_result

    ema5 = float(ema5_valid.iloc[-1])
    ema20 = float(ema20_valid.iloc[-1])
    ema60 = float(ema60_valid.iloc[-1])

    # EMA 5-20 역배열
    reverse = ema5 < ema20

    # EMA 5-20 역배열 + EMA 20-60 정배열
    bullish_pullback = (
        ema20 > ema60
        and ema5 < ema20
    )

    # 최종 EMA SIGNAL 조건
    signal_condition = (
        ema5 < ema20
        and ema20 > ema60
    )

    # 정배열/역배열 표시는 EMA 20-60 기준
    if ema20 > ema60:
        alignment = "정배열"
    elif ema20 < ema60:
        alignment = "역배열"
    else:
        alignment = "동일"

    return {
        "ema5": ema20,
        "ema15": ema60,
        "ema5_short": ema5,
        "ema20": ema20,
        "ema60": ema60,
        "reverse": reverse,
        "bullish_pullback": bullish_pullback,
        "signal_condition": signal_condition,
        "alignment": alignment,
        "cross_previous": False,
        "cross_current": False,
        "cross_signal": False
    }


# =========================================================
# 참고용 EMA 상태 판정
# =========================================================

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


# =========================================================
# 참고용 EMA 상태 분석
# SIGNAL_TIMEFRAME 기준으로 계산
# =========================================================

def analyze_reference_ema_timeframes(market, price=None):
    states = {
        "signal_5_20": None,
        "signal_20_60": None,

        # 기존 키 호환 유지
        "1h_5_20": None,
        "1h": None
    }

    timeframe = SIGNAL_TIMEFRAME

    try:
        if timeframe == "15m":
            df = get_upbit_15m_candles(
                market,
                EMA_HISTORY_CANDLES
            )

            if df.empty:
                return states

            now = datetime.now(KST)

            current_start = now.replace(
                minute=(now.minute // 15) * 15,
                second=0,
                microsecond=0
            )

            df = update_current_candle(
                df,
                current_start,
                price
            )

        elif timeframe == "1h":
            df = get_upbit_4h_candles(
                market,
                EMA_HISTORY_CANDLES
            )

            if df.empty:
                return states

            now = datetime.now(KST)

            current_start = now.replace(
                minute=0,
                second=0,
                microsecond=0
            )

            df = update_current_candle(
                df,
                current_start,
                price
            )

        elif timeframe == "4h":
            df_1h = get_upbit_4h_candles(
                market,
                EMA_HISTORY_CANDLES
            )

            if df_1h.empty:
                return states

            df = build_upbit_4h_candles(
                df_1h,
                price
            )

            if df.empty:
                return states

        elif timeframe == "1d":
            df = get_upbit_daily_candles(
                market,
                200
            )

            if df.empty:
                return states

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
            log.warning(
                "지원하지 않는 참고 EMA 시간봉: %s",
                timeframe
            )
            return states

        # 두 쌍 모두 시그널 시간봉의 동일한 데이터로 계산
        states["signal_5_20"] = get_ema_alignment_state(
            df,
            EMA_PULLBACK,
            EMA_FAST
        )

        states["signal_20_60"] = get_ema_alignment_state(
            df,
            EMA_FAST,
            EMA_SLOW
        )

        # 기존 표시 키도 유지하되 값은 시그널 시간봉 기준
        states["1h_5_20"] = states["signal_5_20"]
        states["1h"] = states["signal_20_60"]

    except Exception as e:
        log.warning(
            "%s 시그널 시간봉 참고 EMA 오류: %s",
            market,
            e
        )

    return states


# =========================================================
# 일봉 기간
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
            if o
            else None
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
# 상승장악형
# =========================================================

def is_bullish_engulfing(previous, current):
    try:
        prev_open = float(previous["open"])
        prev_close = float(previous["close"])
        curr_open = float(current["open"])
        curr_close = float(current["close"])
    except Exception:
        return False

    if prev_close >= prev_open:
        return False

    if curr_close <= curr_open:
        return False

    return bool(
        curr_open <= prev_close
        and curr_close >= prev_open
    )


# =========================================================
# 하락장악형
# =========================================================

def is_bearish_engulfing(previous, current):
    try:
        prev_open = float(previous["open"])
        prev_close = float(previous["close"])
        curr_open = float(current["open"])
        curr_close = float(current["close"])
    except Exception:
        return False

    if prev_close <= prev_open:
        return False

    if curr_close >= curr_open:
        return False

    return bool(
        curr_open >= prev_close
        and curr_close <= prev_open
    )


# =========================================================
# 양수도지
# =========================================================

def is_positive_doji(current):
    try:
        o = float(current["open"])
        h = float(current["high"])
        l = float(current["low"])
        c = float(current["close"])
    except Exception:
        return False

    total_range = h - l

    if total_range <= 0:
        return False

    body = abs(c - o)

    return bool(
        body / total_range <= 0.10
        and c >= o
    )


# =========================================================
# 음수도지
# =========================================================

def is_negative_doji(current):
    try:
        o = float(current["open"])
        h = float(current["high"])
        l = float(current["low"])
        c = float(current["close"])
    except Exception:
        return False

    total_range = h - l

    if total_range <= 0:
        return False

    body = abs(c - o)

    return bool(
        body / total_range <= 0.10
        and c < o
    )


# =========================================================
# 상승관통형
# =========================================================

def is_bullish_piercing(previous, current):
    try:
        prev_open = float(previous["open"])
        prev_close = float(previous["close"])
        curr_open = float(current["open"])
        curr_close = float(current["close"])
    except Exception:
        return False

    if prev_close >= prev_open:
        return False

    if curr_close <= curr_open:
        return False

    midpoint = (prev_open + prev_close) / 2

    return bool(
        curr_close > midpoint
        and curr_close < prev_open
    )


# =========================================================
# 하락관통형
# =========================================================

def is_bearish_piercing(previous, current):
    try:
        prev_open = float(previous["open"])
        prev_close = float(previous["close"])
        curr_open = float(current["open"])
        curr_close = float(current["close"])
    except Exception:
        return False

    if prev_close <= prev_open:
        return False

    if curr_close >= curr_open:
        return False

    midpoint = (prev_open + prev_close) / 2

    return bool(
        curr_close < midpoint
        and curr_close > prev_open
    )


# =========================================================
# 장대양봉
# =========================================================

def is_long_bullish(current):
    try:
        o = float(current["open"])
        h = float(current["high"])
        l = float(current["low"])
        c = float(current["close"])
    except Exception:
        return False

    total_range = h - l

    if total_range <= 0 or c <= o:
        return False

    return bool(
        abs(c - o) / total_range >= 0.70
    )


# =========================================================
# 장대음봉
# =========================================================

def is_long_bearish(current):
    try:
        o = float(current["open"])
        h = float(current["high"])
        l = float(current["low"])
        c = float(current["close"])
    except Exception:
        return False

    total_range = h - l

    if total_range <= 0 or c >= o:
        return False

    return bool(
        abs(c - o) / total_range >= 0.70
    )


# =========================================================
# 망치형 공통 판정
# 아래꼬리 또는 위꼬리가 몸통의 2배 이상
# 반대쪽 꼬리는 몸통 이하
# =========================================================

def get_hammer_parts(current):
    try:
        o = float(current["open"])
        h = float(current["high"])
        l = float(current["low"])
        c = float(current["close"])
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

    upper_wick = h - max(o, c)
    lower_wick = min(o, c) - l

    return {
        "body": effective_body,
        "upper_wick": upper_wick,
        "lower_wick": lower_wick,
        "open": o,
        "close": c
    }


# =========================================================
# 상승망치형
# =========================================================

def is_bullish_hammer(previous, current):
    if previous is None:
        return False

    try:
        if float(previous["close"]) >= float(previous["open"]):
            return False
    except Exception:
        return False

    parts = get_hammer_parts(current)

    if parts is None:
        return False

    return bool(
        parts["lower_wick"] >= parts["body"] * 2
        and parts["upper_wick"] <= parts["body"]
    )


# =========================================================
# 상승역망치형
# =========================================================

def is_bullish_inverted_hammer(previous, current):
    if previous is None:
        return False

    try:
        if float(previous["close"]) >= float(previous["open"]):
            return False
    except Exception:
        return False

    parts = get_hammer_parts(current)

    if parts is None:
        return False

    return bool(
        parts["upper_wick"] >= parts["body"] * 2
        and parts["lower_wick"] <= parts["body"]
    )


# =========================================================
# 하락망치형
# =========================================================

def is_bearish_hammer(previous, current):
    if previous is None:
        return False

    try:
        if float(previous["close"]) <= float(previous["open"]):
            return False
    except Exception:
        return False

    parts = get_hammer_parts(current)

    if parts is None:
        return False

    return bool(
        parts["lower_wick"] >= parts["body"] * 2
        and parts["upper_wick"] <= parts["body"]
    )


# =========================================================
# 하락역망치형
# =========================================================

def is_bearish_inverted_hammer(previous, current):
    if previous is None:
        return False

    try:
        if float(previous["close"]) <= float(previous["open"]):
            return False
    except Exception:
        return False

    parts = get_hammer_parts(current)

    if parts is None:
        return False

    return bool(
        parts["upper_wick"] >= parts["body"] * 2
        and parts["lower_wick"] <= parts["body"]
    )


# =========================================================
# 캔들 패턴 전체 판정
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


def get_candle_pattern(previous, current):
    patterns = get_candle_patterns(previous, current)

    return patterns[0] if patterns else None


# =========================================================
# 시그널 강조 대상
# =========================================================

BULLISH_HIGHLIGHT_PATTERNS = [
    "상승장악형",
    "상승관통형",
    "장대양봉",
    "상승망치형",
    "상승역망치형"
]


# =========================================================
# 이전봉 / 현재봉 패턴
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
        df,
        current_price
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
# SIGNAL
# =========================================================

def signal_pass(periods, ema_reverse=False):
    return bool(ema_reverse)


# =========================================================
# SIGNAL 상세
# =========================================================

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

    # 기존 signal_pass 값은 EMA 조건과 상승패턴 조건을 모두 만족할 때 True
    # SIGNAL 목록의 표시 여부는 update_upbit()에서 별도로 결정
    signal = bool(
        ema_reverse
        and pattern_signal
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

    if timeframe == "15m":
        df = get_upbit_15m_candles(market, 200)

        if df.empty:
            return empty_result

        now = datetime.now(KST)

        current_start = now.replace(
            minute=(now.minute // 15) * 15,
            second=0,
            microsecond=0
        )

        df = update_current_candle(
            df,
            current_start,
            price
        )

        periods = []

        for _, row in df.iterrows():
            start = row["datetime"]
            o = float(row["open"])
            h = float(row["high"])
            l = float(row["low"])
            c = float(row["close"])

            change = (
                (c - o) / o * 100
                if o
                else None
            )

            periods.append({
                "start": start,
                "end": start + timedelta(minutes=15),
                "active": start == current_start,
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

    elif timeframe == "1h":
        df = get_upbit_4h_candles(market, 200)

        if df.empty:
            return empty_result

        now = datetime.now(KST)

        current_start = now.replace(
            minute=0,
            second=0,
            microsecond=0
        )

        df = update_current_candle(
            df,
            current_start,
            price
        )

        periods = []

        for _, row in df.iterrows():
            start = row["datetime"]
            o = float(row["open"])
            h = float(row["high"])
            l = float(row["low"])
            c = float(row["close"])

            change = (
                (c - o) / o * 100
                if o
                else None
            )

            periods.append({
                "start": start,
                "end": start + timedelta(hours=1),
                "active": start == current_start,
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
        df = get_upbit_4h_candles(market, 200)

        if df.empty:
            return empty_result

        periods = build_upbit_4h_periods(
            df,
            price
        )

    elif timeframe == "1d":
        df = get_upbit_daily_candles(market, 200)

        if df.empty:
            return empty_result

        periods = build_daily_periods(
            df,
            price
        )

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

    pattern_info = analyze_previous_current_pattern(periods)

    details = signal_details(
        periods,
        ema_reverse
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


# =========================================================
# 기존 함수명 호환
# =========================================================

def analyze_4h(market, price, ema_reverse=False):
    return analyze_signal_candles(
        market,
        price,
        ema_reverse
    )


# =========================================================
# 일봉 변동률
# =========================================================

def analyze_daily_change(market, price):
    df = get_upbit_daily_candles(market, 10)

    if df.empty:
        return {
            "change": None,
            "periods": []
        }

    periods = build_daily_periods(df, price)

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
# Row
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
            "pattern_signal",
            False
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
            "bullish_pullback",
            False
        ),
        "ema_signal_condition": ema_analysis.get(
            "signal_condition",
            False
        ),

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
        for rank, item in enumerate(all_markets_sorted, 1)
    }

    candidates = []

    for item in all_markets_sorted:
        market = item["market"]
        price = item["current_price"]

        try:
            analysis_daily = analyze_daily_change(
                market,
                price
            )

            candidates.append({
                "item": item,
                "analysis_daily": analysis_daily
            })

        except Exception as e:
            log.warning(
                "%s 일봉 오류: %s",
                market,
                e
            )

    candidates = candidates[:TOP_N]

    rows = []

    for rank, candidate in enumerate(candidates, 1):
        item = candidate["item"]
        analysis_daily = candidate["analysis_daily"]

        market = item["market"]
        price = item["current_price"]

        try:
            ema_analysis = analyze_ema_4h(
                market,
                price
            )

        except Exception as e:
            log.warning(
                "%s EMA 오류: %s",
                market,
                e
            )

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

        # =================================================
        # SIGNAL EMA 조건
        # EMA 5-20 역배열 + EMA 20-60 정배열 필수
        # =================================================

        ema5_value = ema_analysis.get("ema5_short")
        ema20_value = ema_analysis.get("ema20")
        ema60_value = ema_analysis.get("ema60")

        ema_signal_condition = (
            ema5_value is not None
            and ema20_value is not None
            and ema60_value is not None
            and ema5_value < ema20_value
            and ema20_value > ema60_value
        )

        try:
            analysis_4h = analyze_signal_candles(
                market,
                price,
                ema_signal_condition
            )

        except Exception as e:
            log.warning(
                "%s 캔들 패턴 오류: %s",
                market,
                e
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
                market,
                price
            )

        except Exception as e:
            log.warning(
                "%s 참고 EMA 상태 오류: %s",
                market,
                e
            )

            reference_ema_states = {
                "signal_5_20": None,
                "signal_20_60": None,
                "1h_5_20": None,
                "1h": None
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

    # =====================================================
    # SIGNAL 최종 필터
    # 1. EMA 5 < EMA 20
    # 2. EMA 20 > EMA 60
    # 3. 당일 변동률 양수
    # 4. 상승패턴은 필수 조건이 아니라 강조 조건
    # =====================================================

    latest_signal_data = [
        row
        for row in rows
        if (
            row.get("ema_signal_condition", False)
            and row.get("daily_change") is not None
            and row.get("daily_change") > 0
        )
    ]

    latest_signal_data.sort(
        key=lambda x: x.get("volume_rank", 999999)
    )

    for signal_rank, row in enumerate(latest_signal_data, 1):
        row["signal_rank"] = signal_rank

    pair_1, pair_2 = get_ema_pair_labels()

    log.info(
        "UPBIT | 거래대금 TOP%s | EMA%s | 시간봉=%s | "
        "참고 EMA 시그널 시간봉 %s 및 %s | SIGNAL=%s | "
        "EMA 5-20 역배열 + EMA 20-60 정배열 + "
        "당일 변동률 양수 | 상승패턴은 선택 조건이며 "
        "패턴이 있는 종목만 강조 | "
        "캔들패턴 12종 표시, 상승장악형/관통형/"
        "장대양봉/상승망치형/상승역망치형 강조",
        TOP_N,
        get_ema_full_label(),
        get_timeframe_label(),
        pair_1,
        pair_2,
        len(latest_signal_data)
    )


# =========================================================
# OKX
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
        data = r.json().get("data", [])
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
        params={"instId": "BTC-USDT-SWAP"},
        timeout=15
    )

    if r is None:
        return None

    try:
        return float(r.json()["data"][0]["last"])
    except Exception:
        return None


# =========================================================
# OKX BTC
# KST 09:00 기준 일봉 재구성
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

    for day_start, group in raw.groupby("day_start", sort=True):
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
            if o
            else None
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
        return

    latest_btc_daily_periods = build_okx_kst_daily_periods(
        d1h,
        price
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
# 표시
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
# EMA 표시
# =========================================================

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
# 참고용 EMA 상태 HTML
# 수정: 두 줄 표시
# =========================================================

def reference_ema_states_html(row):
    states = row.get("ema_reference_states") or {}
    pieces = []

    timeframe_label = get_timeframe_label()
    pair_1, pair_2 = get_ema_pair_labels()

    display_items = [
        (
            pair_1,
            states.get(
                "signal_5_20",
                states.get("1h_5_20")
            )
        ),
        (
            pair_2,
            states.get(
                "signal_20_60",
                states.get("1h")
            )
        )
    ]

    for pair_label, state in display_items:
        if state == "정배열":
            cls = "ema-normal"
            symbol = "▲"
        elif state == "역배열":
            cls = "ema-reverse"
            symbol = "▼"
        elif state == "동일":
            cls = "ema-same"
            symbol = "—"
        else:
            cls = "ema-same"
            symbol = "—"

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

        elif item in [
            "하락장악형",
            "하락관통형",
            "장대음봉",
            "음수도지",
            "하락망치형",
            "하락역망치형"
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


# =========================================================
# SIGNAL 이유
# =========================================================

def signal_reason_html(reason):
    if reason == "상승장악형":
        return '<span class="pattern-cross">▲ 상승장악형</span>'

    if reason == "상승관통형":
        return '<span class="pattern-cross">▲ 상승관통형</span>'

    if reason == "장대양봉":
        return '<span class="pattern-cross">▲ 장대양봉</span>'

    if reason == "상승망치형":
        return '<span class="pattern-cross">▲ 상승망치형</span>'

    if reason == "상승역망치형":
        return '<span class="pattern-cross">▲ 상승역망치형</span>'

    return ""


# =========================================================
# BTC HTML
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
# 상승패턴이 있는 종목만 기존 강조 유지
# =========================================================

def signal_card(row):
    timeframe_label = get_timeframe_label()

    has_bullish_pattern = row.get(
        "signal_4h_pattern",
        False
    )

    if has_bullish_pattern:
        badge_html = (
            '<span class="signal-type-badge">'
            '⭐ 상승패턴'
            '</span>'
        )
        card_class = "signal-card has-bullish-pattern"
    else:
        badge_html = ""
        card_class = "signal-card"

    return f"""
    <div class="{card_class}">
        <div class="signal-coin">
            <span class="signal-rank">
                거래대금 #{row.get("volume_rank", "-")}
            </span>

            <b>{html.escape(row["name"])}</b>

            <div class="signal-type-list">{badge_html}</div>

            {reference_ema_states_html(row)}
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
    ema_full_label = get_ema_full_label()

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

            <b>
                EMA 5-20 역배열 + 20-60 정배열
            </b>

            <small>
                당일 변동률 양수 · 상승패턴은 강조 조건
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
    pair_1, pair_2 = get_ema_pair_labels()

    has_bullish_pattern = row.get(
        "signal_4h_pattern",
        False
    )

    previous_pattern = row.get("previous_pattern")
    current_pattern = row.get("current_pattern")

    if kind == "top":
        status = (
            '<span class="signal-badge">⭐ 상승패턴</span>'
            if has_bullish_pattern
            else ""
        )

        alignment_html = ema_alignment_html(
            row.get("ema_alignment")
        )

        return f"""
        <article class="coin-card">
            <div class="coin-head">
                <div class="coin-title">
                    <span class="rank">#{row["rank"]}</span>
                    <b>{html.escape(row["name"])}</b>
                    {alignment_html}
                </div>

                {status}
            </div>

            <div class="ema-ref-topline">
                <span class="ema-ref-title">
                    EMA {pair_1} · {pair_2}
                </span>

                {reference_ema_states_html(row)}
            </div>

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
                        {current_pattern_html(previous_pattern)}
                    </strong>
                </div>

                <div>
                    <span>현재봉 {timeframe_label}</span>
                    <strong class="pattern-display">
                        {current_pattern_html(current_pattern)}
                    </strong>
                </div>
            </div>
        </article>
        """

    return ""


# =========================================================
# CSS
# =========================================================

CSS = """
* { box-sizing: border-box; }

html { background: #080b0f; }

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

section { margin-bottom: 12px; }

.up { color: #38d878 !important; font-weight: 900; }
.down { color: #ff5966 !important; font-weight: 900; }
.zero { color: #68737e; font-weight: 800; }

.daily-change-value .up {
    color: #38d878 !important;
    font-weight: 900;
    font-size: 9px;
}

.daily-change-value .down {
    color: #ff5966 !important;
    font-weight: 900;
    font-size: 9px;
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

/* 수정: EMA 상태를 두 줄로 표시 */
.ema-ref-row {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    justify-content: center;
    gap: 3px;
    flex-wrap: nowrap;
    white-space: nowrap;
    min-width: 0;
}

.ema-ref-item {
    display: flex;
    align-items: center;
    gap: 2px;
    font-size: 7px;
    font-weight: 900;
    line-height: 1.3;
    white-space: nowrap;
}

.ema-ref-label {
    color: #83909b;
    font-size: 6px;
    font-weight: 700;
    white-space: nowrap;
}

.ema-ref-topline {
    display: flex;
    align-items: center;
    justify-content: flex-start;
    gap: 7px;
    min-height: 23px;
    padding: 2px 9px;
    background: #0a1015;
    border-bottom: 1px solid #202a33;
}

.ema-ref-topline .ema-ref-row {
    padding: 3px 0;
}

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

.bearish-pattern .pattern-name { color: #ff5966; }
.neutral-pattern .pattern-name { color: #e4c45e; }
.pattern-none .pattern-name { color: #68737e; }

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

/* 망치형: 긴 아래꼬리 */
.mini-candle.hammer .wick {
    height: 22px;
    top: 0;
}

.mini-candle.hammer .body {
    position: absolute;
    top: 1px;
    height: 6px;
    width: 8px;
}

.mini-candle.bullish.hammer .body {
    background: #38d878;
}

.mini-candle.bearish.hammer .body {
    background: #ff5966;
}

/* 역망치형: 긴 위꼬리 */
.mini-candle.inverted-hammer .wick {
    height: 22px;
    top: 0;
}

.mini-candle.inverted-hammer .body {
    position: absolute;
    bottom: 1px;
    height: 6px;
    width: 8px;
}

.mini-candle.bullish.inverted-hammer .body {
    background: #38d878;
}

.mini-candle.bearish.inverted-hammer .body {
    background: #ff5966;
}

.candle-pattern-visual.empty {
    color: #68737e;
    font-size: 9px;
    height: 23px;
}

.signal-panel {
    background: #0c1116;
    border: 1px solid #48272c;
    margin-bottom: 13px;
}

.signal-panel-head { border-bottom: 1px solid #392126; }

.signal-condition {
    min-height: 32px;
    padding: 0 9px;
    display: flex;
    align-items: center;
    gap: 8px;
    background: #120e10;
    border-bottom: 1px solid #302024;
}

.signal-condition span {
    color: #7d6b70;
    font-size: 6px;
}

.signal-condition b {
    color: #ff5966;
    font-size: 8px;
    font-weight: 900;
}

.signal-condition small {
    color: #626e78;
    font-size: 6px;
    margin-left: auto;
}

.signal-header,
.signal-card {
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

.signal-header > div { text-align: center; }

.signal-card {
    min-height: 58px;
    background: #0d1217;
    border-bottom: 1px solid #20282f;
}

.signal-card.has-bullish-pattern {
    background: #171316;
    box-shadow: inset 3px 0 0 #e4c45e;
}

.signal-card.has-bullish-pattern .signal-coin b {
    color: #e4c45e;
}

.signal-card:last-child { border-bottom: 0; }

.signal-card > div {
    min-height: 58px;
    padding: 3px;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    text-align: center;
}

.signal-card > div + div {
    border-left: 1px solid #1c252c;
}

.signal-coin {
    flex-direction: row !important;
    flex-wrap: wrap;
    gap: 5px;
    justify-content: flex-start !important;
    padding-left: 7px !important;
}

.signal-rank {
    color: #b84d58;
    font-size: 7px;
    font-weight: 900;
}

.signal-coin b {
    color: #e5e9ed;
    font-size: 9px;
    font-weight: 900;
}

.signal-type-list {
    width: 100%;
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    justify-content: flex-start;
    gap: 2px;
    margin-top: 1px;
}

.signal-type-badge {
    color: #e4c45e !important;
    font-size: 5px !important;
    font-weight: 900;
}

.signal-card span {
    color: #69747e;
    font-size: 6px;
}

.signal-card .ema-ref-item.ema-normal {
    color: #38d878 !important;
}

.signal-card .ema-ref-item.ema-reverse {
    color: #ff5966 !important;
}

.signal-card .ema-ref-item.ema-same {
    color: #68737e !important;
}

.signal-card strong {
    margin-top: 2px;
    color: #dce2e7;
    font-size: 8px;
}

.signal-card .ema-ref-row {
    width: 100%;
    justify-content: center;
    align-items: flex-start;
    gap: 3px;
}

.signal-card .ema-ref-item { font-size: 7px; }
.signal-card .ema-ref-label { font-size: 6px; }

.signal-pattern .pattern-box { min-width: 42px; }
.signal-pattern .pattern-name { font-size: 5px; }

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

.signal-empty {
    min-height: 46px;
    display: flex;
    align-items: center;
    justify-content: center;
    color: #626e78;
    font-size: 7px;
}

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

.signal-badge {
    color: #e4c45e;
    font-size: 7px;
    font-weight: 900;
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

.market-summary .pattern-box { min-width: 35px; }
.market-summary .pattern-name { font-size: 4.5px; }

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

@media (max-width: 600px) {
    body { padding: 5px; }

    h1 {
        margin: 3px 2px 9px;
        font-size: 12px;
    }

    .section-head { padding: 0 8px; }
    .section-head b { font-size: 9px; }
    .update-time { font-size: 5px; }

    .btc-main { min-height: 48px; }
    .btc-price { font-size: 13px; }
    .btc-change { font-size: 9px; }

    .coin-head {
        min-height: 35px;
        padding: 0 7px;
    }

    .coin-title { gap: 6px; }
    .coin-title b { font-size: 9px; }

    .ema-normal,
    .ema-reverse,
    .ema-same {
        font-size: 6px;
    }

    .ema-ref-row { gap: 3px; }
    .ema-ref-item { font-size: 6px; }
    .ema-ref-label { font-size: 5px; }

    .ema-ref-topline {
        min-height: 21px;
        padding: 2px 7px;
        gap: 5px;
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

    .market-summary strong { font-size: 5px; }
    .market-summary .pattern-box { min-width: 24px; }
    .market-summary .pattern-name { font-size: 3.5px; }

    .market-summary .candle-pattern-visual {
        height: 19px;
        transform: scale(.56);
    }

    .signal-condition {
        min-height: 29px;
        padding: 0 7px;
    }

    .signal-condition b { font-size: 7px; }
    .signal-condition small { font-size: 5px; }

    .signal-header,
    .signal-card {
        grid-template-columns: 1.1fr 0.9fr 0.8fr 1.1fr 1.1fr;
    }

    .signal-header {
        padding: 0 3px;
        font-size: 5px;
    }

    .signal-card > div {
        min-height: 53px;
        padding: 2px 1px;
    }

    .signal-coin {
        gap: 4px;
        padding-left: 5px !important;
    }

    .signal-coin b { font-size: 7px; }
    .signal-rank { font-size: 5px; }
    .signal-card span { font-size: 5px; }
    .signal-card strong { font-size: 5px; }

    .signal-type-list {
        gap: 1px;
        margin-top: 1px;
    }

    .signal-type-badge { font-size: 4px !important; }
    .signal-pattern .pattern-box { min-width: 24px; }
    .signal-pattern .pattern-name { font-size: 3.5px; }

    .signal-pattern .candle-pattern-visual {
        height: 19px;
        transform: scale(.55);
    }

    .signal-pattern .pattern-multi .pattern-box {
        min-width: 22px;
    }

    .signal-card .ema-ref-row { gap: 2px; }
    .signal-card .ema-ref-item { font-size: 5px; }
    .signal-card .ema-ref-label { font-size: 4px; }
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
        "업비트 TOP%s + EMA%s %s + "
        "이전봉/현재봉 캔들 패턴 12종 표시 + "
        "EMA 5-20 역배열 + EMA 20-60 정배열 SIGNAL + "
        "당일 변동률 양수 조건 + 상승패턴은 강조 조건 + "
        "참고용 EMA 시그널 시간봉 %s 및 %s 표시 + "
        "상승장악형/상승관통형/장대양봉/"
        "상승망치형/상승역망치형 강조",
        TOP_N,
        get_ema_full_label(),
        get_timeframe_label(),
        pair_1,
        pair_2
    )

    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    schedule.every(
        UPDATE_MINUTES
    ).minutes.do(update_dashboard)

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
