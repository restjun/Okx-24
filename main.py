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


# =========================================================
# 시간봉별 사용 설정
# Y = 분석 및 화면 표시
# N = 분석 및 화면 표시 제외
# =========================================================

USE_TIMEFRAME_15M = "N"
USE_TIMEFRAME_1H = "N"
USE_TIMEFRAME_4H = "Y"
USE_TIMEFRAME_1D = "N"


TIMEFRAME_LABEL = {
    "15m": "15분",
    "1h": "1시간",
    "4h": "4시간",
    "1d": "일봉"
}

TIMEFRAME_MINUTES = {
    "15m": 15,
    "1h": 60,
    "4h": 240,
    "1d": 1440
}


def get_active_timeframes():

    result = []

    # 대시보드 표시 순서: 일봉 → 4시간 → 1시간 → 15분

    if USE_TIMEFRAME_1D == "Y":
        result.append("1d")

    if USE_TIMEFRAME_4H == "Y":
        result.append("4h")

    if USE_TIMEFRAME_1H == "Y":
        result.append("1h")

    if USE_TIMEFRAME_15M == "Y":
        result.append("15m")

    return result


# =========================================================
# RSI 설정
# =========================================================

RSI_PERIOD = 14

RSI_RANGE_MIN = 30
RSI_RANGE_MAX = 100

RSI_CHECK_CANDLES = 14


# =========================================================
# EMA 설정
# =========================================================

EMA_FAST_PERIOD = 10
EMA_MIDDLE_PERIOD = 20
EMA_SLOW_PERIOD = 50
EMA_LONG_PERIOD = 200

USE_EMA_5 = "N"
USE_EMA_10 = "N"
USE_EMA_20 = "Y"
USE_EMA_50 = "Y"

USE_EMA_BULLISH_ALIGNMENT = "Y"
USE_EMA_BEARISH_ALIGNMENT = "Y"


# =========================================================
# EMA1 / EMA50 / EMA100 / EMA200 크로스 설정
# =========================================================

EMA_CROSS_FAST_PERIOD = 1

EMA_CROSS_SLOW_PERIODS = [50]

# 기존 변수 호환 유지
EMA_CROSS_SLOW_PERIOD = 50

DISPLAY_CANDLE_COUNT = 6

CONSECUTIVE_CANDLE_MIN = 3
CONSECUTIVE_BULLISH_MIN = 2


# =========================================================
# 캔들 조회 개수
# EMA200 크로스 계산을 위해 400개 확보
# =========================================================

CANDLE_HISTORY_COUNT = 400


# =========================================================
# SIGNAL 체크리스트 개별 사용 설정
# =========================================================

USE_SIGNAL_DAILY_CHANGE = "Y"
USE_SIGNAL_RSI_RANGE = "N"
USE_SIGNAL_EMA_ALIGNMENT = "Y"
USE_SIGNAL_EMA_BEARISH_BULLISH = "Y"

SHOW_SIGNAL_CHECKLIST = "Y"


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
        conditions.append(f"{' > '.join(enabled)} 정배열")

    if USE_EMA_BEARISH_ALIGNMENT == "Y":
        conditions.append(f"{' < '.join(enabled)} 역배열")

    if not conditions:
        return "EMA 배열 조건 사용 안 함"

    return " 또는 ".join(conditions)


# =========================================================
# EMA 배열 방향 배지
# =========================================================

def get_ema_alignment_badge(row):

    if USE_SIGNAL_EMA_ALIGNMENT != "Y":
        return ""

    if (
        USE_EMA_BULLISH_ALIGNMENT != "Y"
        and USE_EMA_BEARISH_ALIGNMENT != "Y"
    ):
        return ""

    values = []

    if USE_EMA_5 == "Y":
        values.append(row.get("ema5"))

    if USE_EMA_10 == "Y":
        values.append(row.get("ema10"))

    if USE_EMA_20 == "Y":
        values.append(row.get("ema20"))

    if USE_EMA_50 == "Y":
        values.append(row.get("ema50"))

    if len(values) < 2 or any(v is None for v in values):
        return ""

    try:
        values = [float(v) for v in values]
    except (TypeError, ValueError):
        return ""

    if any(pd.isna(v) for v in values):
        return ""

    bullish = all(
        values[i] > values[i + 1]
        for i in range(len(values) - 1)
    )

    bearish = all(
        values[i] < values[i + 1]
        for i in range(len(values) - 1)
    )

    if USE_EMA_BULLISH_ALIGNMENT == "Y" and bullish:
        return (
            '<span class="ema-alignment-badge ema-bullish-badge">'
            '정배열 통과</span>'
        )

    if USE_EMA_BEARISH_ALIGNMENT == "Y" and bearish:
        return (
            '<span class="ema-alignment-badge ema-bearish-badge">'
            '역배열 통과</span>'
        )

    return ""


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
latest_btc_daily_change = None

# BTC 시간봉별 분석 결과
latest_btc_timeframe_data = {}


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
        periods = int(elapsed.total_seconds() // 14400)

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
# 최대 400개 캔들
# =========================================================

def get_upbit_minute_candles(market, unit, count=CANDLE_HISTORY_COUNT):

    endpoint = f"https://api.upbit.com/v1/candles/minutes/{unit}"

    target_count = min(max(int(count), 1), 400)

    rows = []
    to = None

    while len(rows) < target_count:

        batch_count = min(
            target_count - len(rows),
            200
        )

        params = {
            "market": market,
            "count": batch_count
        }

        if to is not None:
            params["to"] = to

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

        if len(data) < batch_count:
            break

        oldest = min(
            x["candle_date_time_utc"]
            for x in data
        )

        next_to = oldest + "Z"

        if next_to == to:
            break

        to = next_to

    if not rows:
        return pd.DataFrame()

    return (
        pd.DataFrame(rows)
        .sort_values("datetime")
        .drop_duplicates("datetime")
        .tail(target_count)
        .reset_index(drop=True)
    )


def get_upbit_15m_candles(market, count=CANDLE_HISTORY_COUNT):
    return get_upbit_minute_candles(market, 15, count)


def get_upbit_1h_candles(market, count=CANDLE_HISTORY_COUNT):
    return get_upbit_minute_candles(market, 60, count)


def get_upbit_4h_candles(market, count=CANDLE_HISTORY_COUNT):
    return get_upbit_minute_candles(market, 240, count)


# =========================================================
# 업비트 일봉 조회
# 최대 400개 캔들
# =========================================================

def get_upbit_daily_candles(market, count=CANDLE_HISTORY_COUNT):

    endpoint = "https://api.upbit.com/v1/candles/days"

    target_count = min(max(int(count), 1), 400)

    rows = []
    to = None

    while len(rows) < target_count:

        batch_count = min(
            target_count - len(rows),
            200
        )

        params = {
            "market": market,
            "count": batch_count
        }

        if to is not None:
            params["to"] = to

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

        if len(data) < batch_count:
            break

        oldest = min(
            x["candle_date_time_utc"]
            for x in data
        )

        next_to = oldest + "Z"

        if next_to == to:
            break

        to = next_to

    if not rows:
        return pd.DataFrame()

    return (
        pd.DataFrame(rows)
        .sort_values("datetime")
        .drop_duplicates("datetime")
        .tail(target_count)
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

    rsi = 100 - (100 / (1 + rs))

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
# EMA 배열 방향 판정
# =========================================================

def check_ema_alignment_values(values):

    if (
        USE_EMA_BULLISH_ALIGNMENT != "Y"
        and USE_EMA_BEARISH_ALIGNMENT != "Y"
    ):
        return True

    if values is None or len(values) < 2:
        return True

    try:
        numeric_values = [float(v) for v in values]
    except (TypeError, ValueError):
        return False

    if any(pd.isna(v) for v in numeric_values):
        return False

    bullish = all(
        numeric_values[i] > numeric_values[i + 1]
        for i in range(len(numeric_values) - 1)
    )

    bearish = all(
        numeric_values[i] < numeric_values[i + 1]
        for i in range(len(numeric_values) - 1)
    )

    return bool(
        (
            USE_EMA_BULLISH_ALIGNMENT == "Y"
            and bullish
        )
        or (
            USE_EMA_BEARISH_ALIGNMENT == "Y"
            and bearish
        )
    )


# =========================================================
# EMA1 / EMA50 / EMA100 / EMA200
# 골든크로스 및 정배열 유지 판정
#
# 종가(close) 기준
# 실제 상향 크로스가 발생한 캔들만 골든크로스로 판정
# =========================================================

def apply_ema_bullish_cross_signals(periods, close_series):

    if not periods or close_series is None:
        return periods

    ema_fast = calculate_ema(
        close_series,
        EMA_CROSS_FAST_PERIOD
    )

    for slow_period in EMA_CROSS_SLOW_PERIODS:

        ema_slow = calculate_ema(
            close_series,
            slow_period
        )

        bullish_state = False

        for i, period in enumerate(periods):

            cross_key = f"ema_golden_cross_{slow_period}"

            progress_key = (
                f"ema_golden_cross_progress_{slow_period}"
            )

            alignment_key = (
                f"ema_bullish_alignment_{slow_period}"
            )

            period[cross_key] = False
            period[progress_key] = False
            period[alignment_key] = False

            if i >= len(ema_fast) or i >= len(ema_slow):
                continue

            current_fast = ema_fast.iloc[i]
            current_slow = ema_slow.iloc[i]

            if pd.isna(current_fast) or pd.isna(current_slow):
                bullish_state = False
                continue

            current_fast = float(current_fast)
            current_slow = float(current_slow)

            if i == 0:
                continue

            previous_fast = ema_fast.iloc[i - 1]
            previous_slow = ema_slow.iloc[i - 1]

            if pd.isna(previous_fast) or pd.isna(previous_slow):
                bullish_state = False
                continue

            previous_fast = float(previous_fast)
            previous_slow = float(previous_slow)

            # 실제 상향 크로스
            golden_cross = bool(
                previous_fast <= previous_slow
                and current_fast > current_slow
            )

            if golden_cross:

                bullish_state = True

                period[cross_key] = True

            # EMA1이 해당 EMA 이하로 내려가면 정배열 해제
            elif current_fast <= current_slow:

                bullish_state = False

            # 정배열 유지
            if bullish_state and current_fast > current_slow:

                period[alignment_key] = True

                period[progress_key] = bool(
                    period.get("active", False)
                    and golden_cross
                )

        # 기존 EMA1/EMA50 키 호환 유지
        if slow_period == 50:

            for period in periods:

                period["ema_golden_cross"] = period[
                    "ema_golden_cross_50"
                ]

                period["ema_golden_cross_progress"] = period[
                    "ema_golden_cross_progress_50"
                ]

                period["ema_bullish_alignment"] = period[
                    "ema_bullish_alignment_50"
                ]

    return periods


# =========================================================
# 업비트 시간봉 데이터 조회
# =========================================================

def get_timeframe_dataframe(market, timeframe, price=None):

    loaders = {
        "15m": lambda: get_upbit_15m_candles(
            market,
            CANDLE_HISTORY_COUNT
        ),

        "1h": lambda: get_upbit_1h_candles(
            market,
            CANDLE_HISTORY_COUNT
        ),

        "4h": lambda: get_upbit_4h_candles(
            market,
            CANDLE_HISTORY_COUNT
        )
    }

    if timeframe == "1d":

        df = get_cached_dataframe(
            market,
            "1d",
            lambda: get_upbit_daily_candles(
                market,
                CANDLE_HISTORY_COUNT
            )
        )

    elif timeframe in loaders:

        df = get_cached_dataframe(
            market,
            timeframe,
            loaders[timeframe]
        )

    else:
        return pd.DataFrame()

    if df.empty:
        return df

    return update_current_candle(
        df,
        get_candle_start(datetime.now(KST), timeframe),
        price
    )


def get_daily_dataframe(market):

    return get_cached_dataframe(
        market,
        "1d",
        lambda: get_upbit_daily_candles(
            market,
            CANDLE_HISTORY_COUNT
        )
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
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])

        active = start == current_start

        if active and current_price is not None:
            c = float(current_price)
            h = max(h, c)
            l = min(l, c)

        change = ((c - o) / o * 100) if o else None

        periods.append({
            "start": start,
            "end": start + timedelta(days=1),
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
        pc < po and cc > co and co <= pc and cc >= po
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
        pc > po and cc < co and co >= pc and cc <= po
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

    return bool(cc > (po + pc) / 2 and cc < po)


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

    return bool(cc < (po + pc) / 2 and cc > po)


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

    effective_body = max(body, candle_range * 0.01)

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

    for i, current in enumerate(periods):

        previous = periods[i - 1] if i >= 1 else None

        pattern_history.append(
            get_candle_patterns(previous, current)
        )

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

            # EMA1/EMA50 기존 호환
            "ema_golden_cross": period.get(
                "ema_golden_cross", False
            ),

            "ema_golden_cross_progress": period.get(
                "ema_golden_cross_progress", False
            ),

            "ema_bullish_alignment": period.get(
                "ema_bullish_alignment", False
            ),

            # EMA1/EMA50
            "ema_golden_cross_50": period.get(
                "ema_golden_cross_50", False
            ),

            "ema_bullish_alignment_50": period.get(
                "ema_bullish_alignment_50", False
            ),

            # EMA1/EMA100
            "ema_golden_cross_100": period.get(
                "ema_golden_cross_100", False
            ),

            "ema_golden_cross_progress_100": period.get(
                "ema_golden_cross_progress_100", False
            ),

            "ema_bullish_alignment_100": period.get(
                "ema_bullish_alignment_100", False
            ),

            # EMA1/EMA200
            "ema_golden_cross_200": period.get(
                "ema_golden_cross_200", False
            ),

            "ema_golden_cross_progress_200": period.get(
                "ema_golden_cross_progress_200", False
            ),

            "ema_bullish_alignment_200": period.get(
                "ema_bullish_alignment_200", False
            )
        }
        for period, patterns in zip(selected_periods, last_six)
    ]

    current_patterns = pattern_history[-1]

    previous_patterns = (
        pattern_history[-2] if len(pattern_history) >= 2 else []
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
    current_start = get_candle_start(now, timeframe)

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

        change = ((c - o) / o * 100) if o else None

        if timeframe == "1d":
            label = start.strftime("%m/%d 09:00")
            end = start + timedelta(days=1)
        else:
            label = start.strftime("%m/%d %H:%M")
            end = start + timedelta(minutes=minutes)

        periods.append({
            "start": start,
            "end": end,
            "active": active,
            "label": label,
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "change": change
        })

    return periods


# =========================================================
# 업비트 시간봉별 RSI + EMA + 패턴 분석
# =========================================================

def analyze_signal_candles(market, price, timeframe):

    empty_result = {
        "timeframe": timeframe,
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
        "rsi_values": [],
        "rsi_range_pass": False,
        "ema5": None,
        "ema10": None,
        "ema20": None,
        "ema50": None,
        "ema_alignment": False,
        "realtime_below_ema5": False
    }

    df = get_timeframe_dataframe(
        market,
        timeframe,
        price
    )

    if df.empty:
        return empty_result

    periods = build_timeframe_periods(
        df,
        timeframe,
        price
    )

    if not periods:
        return empty_result

    # EMA1 / EMA50 / EMA100 / EMA200
    # 모두 종가 기준 실제 크로스 계산
    periods = apply_ema_bullish_cross_signals(
        periods,
        df["close"]
    )

    rsi_series = calculate_rsi(
        df["close"],
        RSI_PERIOD
    )

    rsi_map = {
        dt: (float(rsi) if pd.notna(rsi) else None)
        for dt, rsi in zip(df["datetime"], rsi_series)
    }

    for period in periods:
        period["rsi"] = rsi_map.get(period["start"])

    last_rsi_periods = periods[-DISPLAY_CANDLE_COUNT:]

    rsi_values = [
        period.get("rsi")
        for period in last_rsi_periods
    ]

    latest_rsi = periods[-1].get("rsi")
    rsi_range_pass = False

    if latest_rsi is not None:

        try:
            latest_rsi = float(latest_rsi)

            rsi_range_pass = bool(
                RSI_RANGE_MIN <= latest_rsi <= RSI_RANGE_MAX
            )
        except Exception:
            rsi_range_pass = False

    ema5_series = calculate_ema(df["close"], EMA_FAST_PERIOD)
    ema10_series = calculate_ema(df["close"], EMA_MIDDLE_PERIOD)
    ema20_series = calculate_ema(df["close"], EMA_SLOW_PERIOD)
    ema50_series = calculate_ema(df["close"], EMA_LONG_PERIOD)

    def latest_ema(series):

        if len(series) == 0:
            return None

        value = series.iloc[-1]

        if pd.isna(value):
            return None

        return float(value)

    ema5 = latest_ema(ema5_series)
    ema10 = latest_ema(ema10_series)
    ema20 = latest_ema(ema20_series)
    ema50 = latest_ema(ema50_series)

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

    elif any(v is None for v in enabled_emas):
        ema_alignment = False

    else:
        ema_alignment = check_ema_alignment_values(enabled_emas)

    pattern_info = analyze_previous_current_pattern(periods)

    last_three = pattern_info["last_three_patterns"]
    recent_periods = periods[-len(last_three):]

    for item, period in zip(last_three, recent_periods):

        item["rsi"] = period.get("rsi")
        item["change"] = period.get("change")
        item["active"] = period.get("active", False)

        for slow_period in EMA_CROSS_SLOW_PERIODS:

            for prefix in (
                "ema_golden_cross",
                "ema_golden_cross_progress",
                "ema_bullish_alignment"
            ):

                key = f"{prefix}_{slow_period}"

                item[key] = period.get(key, False)

        item["ema_golden_cross"] = period.get(
            "ema_golden_cross", False
        )

        item["ema_golden_cross_progress"] = period.get(
            "ema_golden_cross_progress", False
        )

        item["ema_bullish_alignment"] = period.get(
            "ema_bullish_alignment", False
        )

    current_signal = pattern_info["current_bullish_pattern"]
    previous_signal = pattern_info["previous_bullish_pattern"]

    pattern_signal = bool(current_signal or previous_signal)

    return {
        "timeframe": timeframe,
        "periods": periods[-DISPLAY_CANDLE_COUNT:],

        "signal_pass": False,

        "pattern_signal": pattern_signal,
        "current_signal": current_signal,
        "previous_signal": previous_signal,

        "signal_change": None,
        "signal_period": None,
        "signal_reason": None,

        "previous_pattern": pattern_info["previous_pattern"],
        "current_pattern": pattern_info["current_pattern"],
        "last_three_patterns": last_three,

        "previous_bullish_engulfing": pattern_info[
            "previous_bullish_engulfing"
        ],
        "current_bullish_engulfing": pattern_info[
            "current_bullish_engulfing"
        ],

        "consecutive_bullish": pattern_info["consecutive_bullish"],
        "consecutive_bearish": pattern_info["consecutive_bearish"],
        "consecutive_bullish_candles": pattern_info[
            "consecutive_bullish_candles"
        ],

        "rsi_values": rsi_values,

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
    timeframe_analyses,
    volume_rank
):

    coin = market.replace("KRW-", "")

    row = {
        "rank": rank,
        "name": coin,
        "market": market,
        "volume_24h": item["volume_24h"],
        "current_price": item["current_price"],
        "volume_rank": volume_rank,

        "daily_change": analysis_daily["change"],
        "periods_daily": analysis_daily["periods"],

        "timeframe_analyses": timeframe_analyses,

        "signal_timeframes": [],
        "signal_pass": False
    }

    return row


# =========================================================
# SIGNAL 조건 공통 판정
# =========================================================

def get_signal_condition_checks(row, analysis=None):

    if analysis is None:
        analysis = row

    checks = {}

    daily_change = row.get("daily_change")

    if daily_change is None:
        checks["daily_change"] = None
    else:

        try:
            checks["daily_change"] = (
                float(daily_change) > DAILY_CHANGE_MIN
            )
        except (TypeError, ValueError):
            checks["daily_change"] = None

    rsi_values = analysis.get("rsi_values") or []
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
            RSI_RANGE_MIN <= latest_rsi <= RSI_RANGE_MAX
        )

    enabled_ema_values = []

    if USE_EMA_5 == "Y":
        enabled_ema_values.append(analysis.get("ema5"))

    if USE_EMA_10 == "Y":
        enabled_ema_values.append(analysis.get("ema10"))

    if USE_EMA_20 == "Y":
        enabled_ema_values.append(analysis.get("ema20"))

    if USE_EMA_50 == "Y":
        enabled_ema_values.append(analysis.get("ema50"))

    if (
        USE_EMA_BULLISH_ALIGNMENT != "Y"
        and USE_EMA_BEARISH_ALIGNMENT != "Y"
    ):
        checks["ema_alignment"] = True

    elif len(enabled_ema_values) < 2:
        checks["ema_alignment"] = True

    elif any(v is None for v in enabled_ema_values):
        checks["ema_alignment"] = None

    else:

        try:
            checks["ema_alignment"] = check_ema_alignment_values(
                enabled_ema_values
            )
        except (TypeError, ValueError):
            checks["ema_alignment"] = None

    # =====================================================
    # EMA1 / EMA50 / EMA100 / EMA200 개별 크로스 판정
    # 최근 6개 봉에서 각 EMA의 실제 상향 크로스를 독립 확인
    # =====================================================

    candles = analysis.get("last_three_patterns")

    for period in EMA_CROSS_SLOW_PERIODS:

        check_key = f"ema_cross_{period}"
        cross_key = f"ema_golden_cross_{period}"

        if not candles:
            checks[check_key] = None

        else:
            checks[check_key] = any(
                candle.get(cross_key) is True
                for candle in candles
            )

    # 기존 SIGNAL 판정 방식 유지
    # EMA1/50, EMA1/100, EMA1/200 중 하나라도 발생하면 통과

    if not candles:
        checks["ema_bullish_candle"] = None

    else:
        checks["ema_bullish_candle"] = any(
            checks.get(f"ema_cross_{period}") is True
            for period in EMA_CROSS_SLOW_PERIODS
        )

    return checks


# =========================================================
# 활성 SIGNAL 조건 키
# =========================================================

def get_signal_condition_keys():

    keys = []

    if USE_SIGNAL_DAILY_CHANGE == "Y":
        keys.append("daily_change")

    if USE_SIGNAL_RSI_RANGE == "Y":
        keys.append("rsi_range")

    if USE_SIGNAL_EMA_ALIGNMENT == "Y":
        keys.append("ema_alignment")

    if USE_SIGNAL_EMA_BEARISH_BULLISH == "Y":
        keys.append("ema_bullish_candle")

    return keys


# =========================================================
# 시간봉별 SIGNAL 통과 여부 계산
# =========================================================

def evaluate_row_signal(row):

    active_timeframes = get_active_timeframes()
    condition_keys = get_signal_condition_keys()

    passed_timeframes = []
    timeframe_checks = {}

    for timeframe in active_timeframes:

        analysis = (
            row.get("timeframe_analyses", {}).get(timeframe)
        )

        if not analysis:
            timeframe_checks[timeframe] = {}
            continue

        checks = get_signal_condition_checks(
            row,
            analysis
        )

        timeframe_checks[timeframe] = checks

        if not condition_keys:
            continue

        if all(
            checks.get(key) is True
            for key in condition_keys
        ):
            passed_timeframes.append(timeframe)

    row["timeframe_checks"] = timeframe_checks
    row["signal_timeframes"] = passed_timeframes
    row["signal_pass"] = bool(passed_timeframes)

    return row


# =========================================================
# 체크리스트 HTML
# =========================================================

def signal_checklist_html(row, timeframe, analysis):

    if SHOW_SIGNAL_CHECKLIST != "Y":
        return ""

    checks = get_signal_condition_checks(
        row,
        analysis
    )

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

    # EMA1/50, EMA1/100, EMA1/200을 각각 독립 표시
    for period in EMA_CROSS_SLOW_PERIODS:

        checklist_items.append(
            (
                f"ema_cross_{period}",
                (
                    f"최근 {DISPLAY_CANDLE_COUNT}개 봉 "
                    f"EMA1 → EMA{period} 상향 크로스"
                ),
                USE_SIGNAL_EMA_BEARISH_BULLISH
            )
        )

    checklist_items = [
        item for item in checklist_items
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

    passed = (
        timeframe in row.get("signal_timeframes", [])
    )

    timeframe_status = (
        '<span class="timeframe-pass">이 시간봉 SIGNAL 통과</span>'
        if passed
        else '<span class="timeframe-fail">이 시간봉 SIGNAL 미통과</span>'
    )

    return (
        '<div class="signal-checklist">'
        f'<div class="checklist-heading">'
        f'{get_timeframe_label(timeframe)} 체크리스트'
        f'{timeframe_status}'
        '</div>'
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

    active_timeframes = get_active_timeframes()

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
        log.warning("업비트 KRW-BTC 거래대금 조회 실패")

    all_markets_sorted = sorted(
        all_markets,
        key=lambda x: x.get("volume_24h", 0),
        reverse=True
    )

    volume_rank_map = {
        item["market"]: rank
        for rank, item in enumerate(all_markets_sorted, 1)
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
                "%s 일봉 변동률 오류: %s",
                market,
                e
            )

            analysis_daily = {
                "change": None,
                "periods": []
            }

        timeframe_analyses = {}

        for timeframe in active_timeframes:

            try:

                timeframe_analyses[timeframe] = (
                    analyze_signal_candles(
                        market,
                        price,
                        timeframe
                    )
                )

            except Exception as e:

                log.warning(
                    "%s %s 분석 오류: %s",
                    market,
                    timeframe,
                    e
                )

                timeframe_analyses[timeframe] = {
                    "timeframe": timeframe,
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
                    "rsi_values": [],
                    "rsi_range_pass": False,
                    "ema5": None,
                    "ema10": None,
                    "ema20": None,
                    "ema50": None,
                    "ema_alignment": False,
                    "realtime_below_ema5": False
                }

        row = make_row(
            rank,
            market,
            item,
            analysis_daily,
            timeframe_analyses,
            volume_rank_map.get(market)
        )

        evaluate_row_signal(row)

        rows.append(row)

    latest_upbit_data = rows
    latest_upbit_daily_data = rows

    latest_upbit_daily_update_time = kst()
    latest_upbit_update_time = latest_upbit_daily_update_time

    latest_signal_data = [
        row for row in rows
        if row.get("signal_pass")
    ]

    latest_signal_data.sort(
        key=lambda x: (
            x.get("volume_24h")
            if x.get("volume_24h") is not None
            else 0
        ),
        reverse=True
    )

    for signal_rank, row in enumerate(latest_signal_data, 1):
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
            f"최근 {DISPLAY_CANDLE_COUNT}개 봉 중 "
            "EMA1/50·100·200 실제 상향 크로스"
        )

    signal_condition_log = (
        " AND ".join(active_condition_names)
        if active_condition_names
        else "활성화된 SIGNAL 조건 없음"
    )

    log.info(
        "UPBIT | TOP%s | 시간봉=%s | SIGNAL 조건=%s | "
        "SIGNAL=%s | 정렬=24H 거래대금 순",
        TOP_N,
        ",".join(get_timeframe_label(x) for x in active_timeframes)
        if active_timeframes else "없음",
        signal_condition_log,
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

        elapsed = (dt - anchor).total_seconds()

        bucket_number = int(elapsed // seconds)

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

    for start, group in raw.groupby("datetime", sort=True):

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
# BTC 시간봉 데이터 조회
# EMA200 계산을 위한 과거 데이터 확보
# =========================================================

def get_okx_btc_timeframe(timeframe):

    if timeframe == "15m":
        raw = okx_candles("15m", 300)

    elif timeframe == "1h":
        raw = okx_candles("1H", 300)

    elif timeframe in ("4h", "1d"):

        raw = okx_candles("1H", 300)

        if not raw.empty:

            chunks = [raw]

            oldest_timestamp = int(
                raw.iloc[0]["datetime"].timestamp() * 1000
            )

            # 4시간봉은 3회 이내의 1시간봉 이력으로 확보
            # 일봉은 EMA200 계산을 위해 더 많은 이력 확보
            max_history_rounds = (
                18 if timeframe == "1d" else 2
            )

            for _ in range(max_history_rounds):

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
# BTC 시간봉별 분석
# =========================================================

def analyze_okx_btc_timeframe(df, timeframe, current_price=None):

    empty_result = {
        "timeframe": timeframe,
        "periods": [],
        "last_three_patterns": [],
        "rsi_values": [],
        "ema5": None,
        "ema10": None,
        "ema20": None,
        "ema50": None
    }

    if df is None or df.empty:
        return empty_result

    df = (
        df.sort_values("datetime")
        .drop_duplicates("datetime")
        .reset_index(drop=True)
    )

    df = update_current_candle(
        df,
        get_candle_start(datetime.now(KST), timeframe),
        current_price
    )

    periods = build_timeframe_periods(
        df,
        timeframe,
        current_price
    )

    if not periods:
        return empty_result

    # EMA1 / EMA50 / EMA100 / EMA200
    periods = apply_ema_bullish_cross_signals(
        periods,
        df["close"]
    )

    rsi_series = calculate_rsi(
        df["close"],
        RSI_PERIOD
    )

    rsi_map = {
        dt: (float(v) if pd.notna(v) else None)
        for dt, v in zip(df["datetime"], rsi_series)
    }

    for period in periods:
        period["rsi"] = rsi_map.get(period["start"])

    pattern_info = analyze_previous_current_pattern(periods)

    last_patterns = pattern_info["last_three_patterns"]
    recent_periods = periods[-len(last_patterns):]

    for item, period in zip(last_patterns, recent_periods):

        item["rsi"] = period.get("rsi")
        item["change"] = period.get("change")
        item["active"] = period.get("active", False)

        for slow_period in EMA_CROSS_SLOW_PERIODS:

            for prefix in (
                "ema_golden_cross",
                "ema_golden_cross_progress",
                "ema_bullish_alignment"
            ):

                key = f"{prefix}_{slow_period}"

                item[key] = period.get(key, False)

        item["ema_golden_cross"] = period.get(
            "ema_golden_cross", False
        )

        item["ema_golden_cross_progress"] = period.get(
            "ema_golden_cross_progress", False
        )

        item["ema_bullish_alignment"] = period.get(
            "ema_bullish_alignment", False
        )

    def latest_ema(period):

        series = calculate_ema(df["close"], period)

        if len(series) == 0 or pd.isna(series.iloc[-1]):
            return None

        return float(series.iloc[-1])

    recent_rsi = [
        p.get("rsi")
        for p in periods[-DISPLAY_CANDLE_COUNT:]
    ]

    return {
        "timeframe": timeframe,
        "periods": periods[-DISPLAY_CANDLE_COUNT:],
        "last_three_patterns": last_patterns,
        "rsi_values": recent_rsi,
        "ema5": latest_ema(EMA_FAST_PERIOD),
        "ema10": latest_ema(EMA_MIDDLE_PERIOD),
        "ema20": latest_ema(EMA_SLOW_PERIOD),
        "ema50": latest_ema(EMA_LONG_PERIOD)
    }


# =========================================================
# BTC 당일 변동률 계산
# KST 09:00 기준
# =========================================================

def calculate_btc_daily_change(current_price):

    if current_price is None:
        return None

    try:

        current_price = float(current_price)
        daily_start = get_candle_start(datetime.now(KST), "1d")

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
                "BTC 당일 변동률 계산 실패: KST 09시 일봉 없음 | 기준=%s",
                daily_start
            )

            return None

        daily_open = float(matched.iloc[-1]["open"])

        if daily_open <= 0:
            return None

        daily_change = (
            (current_price - daily_open)
            / daily_open
            * 100
        )

        log.info(
            "BTC 당일 변동률 | 기준시각=%s | 시가=%s | 현재가=%s | 변동률=%.4f%%",
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
    global latest_btc_daily_change
    global latest_btc_timeframe_data

    price = okx_price()
    latest_btc_okx_price = price

    if price is None:
        return

    active_timeframes = get_active_timeframes()

    latest_btc_timeframe_data = {}

    for timeframe in active_timeframes:

        try:

            df = get_okx_btc_timeframe(timeframe)

            latest_btc_timeframe_data[timeframe] = (
                analyze_okx_btc_timeframe(
                    df,
                    timeframe,
                    price
                )
            )

        except Exception as e:

            log.warning(
                "BTC %s 분석 오류: %s",
                timeframe,
                e
            )

            latest_btc_timeframe_data[timeframe] = {
                "timeframe": timeframe,
                "periods": [],
                "last_three_patterns": [],
                "rsi_values": [],
                "ema5": None,
                "ema10": None,
                "ema20": None,
                "ema50": None
            }

    latest_btc_daily_change = calculate_btc_daily_change(
        price
    )

    log.info(
        "BTC OKX | 가격=%s | 분석 시간봉=%s | 당일 변동률=%s",
        price,
        ",".join(get_timeframe_label(x) for x in active_timeframes)
        if active_timeframes else "없음",
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

        log.exception("업데이트 오류: %s", e)

    finally:
        update_lock.release()


# =========================================================
# 가격 및 거래대금 표시
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
            '<div class="candle-change candle-change-none">-</div>'
        )

    try:
        value = float(value)
    except Exception:
        return (
            '<div class="candle-change candle-change-none">-</div>'
        )

    if value > 0:
        return (
            '<div class="candle-change candle-change-up">'
            f'+{value:.2f}%</div>'
        )

    if value < 0:
        return (
            '<div class="candle-change candle-change-down">'
            f'{value:.2f}%</div>'
        )

    return (
        '<div class="candle-change candle-change-zero">0.00%</div>'
    )


# =========================================================
# RSI 표시 HTML
# =========================================================

def rsi_html(value):

    if value is None:
        return (
            '<div class="rsi-value rsi-unavailable">RSI -</div>'
        )

    try:
        value = float(value)
    except Exception:
        return (
            '<div class="rsi-value rsi-unavailable">RSI -</div>'
        )

    if value <= 50:
        color_class = "rsi-low"
    elif value < 70:
        color_class = "rsi-middle"
    else:
        color_class = "rsi-high"

    return (
        f'<div class="rsi-value {color_class}">'
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

    return '<div class="candle-pattern-visual empty">—</div>'


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

    return '<div class="pattern-multi">' + "".join(boxes) + '</div>'


# =========================================================
# EMA 크로스 배지 HTML
# =========================================================

def ema_cross_badges_html(item):

    badges = []

    for period in EMA_CROSS_SLOW_PERIODS:

        if item.get(f"ema_golden_cross_{period}") is True:

            badges.append(
                f'<span class="ema-cross-badge">'
                f'EMA1/{period} GC</span>'
            )

    return "".join(badges)


# =========================================================
# 최근 캔들 표시
# =========================================================

def last_three_pattern_html(row):

    last_three = row.get("last_three_patterns") or []
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

        if i == len(visible_items) - 1 and item.get("active"):
            live_html = '<span class="candle-live">진행</span>'

        label = f"""
        <div class="candle-date">{date_label}</div>
        <div class="candle-time">
            {time_label}
            {live_html}
        </div>
        """

        candle_html = current_pattern_html(patterns, item)

        cross_badges = ema_cross_badges_html(item)

        change_display = candle_change_html(
            item.get("change")
        )

        rsi_value = item.get("rsi")

        card_classes = []

        # 실제 EMA 상향 크로스 발생 여부
        # 정배열 유지 여부와 양봉/음봉 여부는 확인하지 않음
        ema_cross_active = any(
            item.get(
                f"ema_golden_cross_{period}",
                False
            ) is True
            for period in EMA_CROSS_SLOW_PERIODS
        )

        # 현재 진행 중인 캔들에서 발생한 크로스 여부
        ema_progress_active = any(
            item.get(
                f"ema_golden_cross_progress_{period}",
                False
            ) is True
            for period in EMA_CROSS_SLOW_PERIODS
        )

        # 실제 크로스가 발생한 캔들만 노란색 표시
        # 진행 중인 캔들과 완성된 캔들 모두 포함
        if ema_cross_active:
            card_classes.append("ema-golden-cross-card")

        # 진행 중인 크로스는 추가 강조
        if ema_progress_active:
            card_classes.append("ema-golden-cross-progress")

        card_class = " ".join(card_classes)

        boxes.append(
            f"""
            <div class="three-candle-item {card_class}">
                <div class="three-candle-label">
                    {label}
                </div>

                <div class="three-candle-pattern">
                    {candle_html}
                    {cross_badges}
                </div>

                <div class="three-candle-change">
                    {change_display}
                </div>

                <div class="three-candle-rsi">
                    {rsi_html(rsi_value)}
                </div>
            </div>
            """
        )

    candle_count = max(1, len(visible_items))

    return (
        f'<div class="three-candle-list" '
        f'style="grid-template-columns:repeat({candle_count},minmax(0,1fr));">'
        + "".join(boxes)
        + '</div>'
    )


# =========================================================
# 분석 결과를 기존 카드 함수에 전달할 표시용 Row
# =========================================================

def make_display_row(row, analysis):

    display = dict(row)

    display.update({
        "last_three_patterns": analysis.get(
            "last_three_patterns", []
        ),
        "previous_pattern": analysis.get(
            "previous_pattern", []
        ),
        "current_pattern": analysis.get(
            "current_pattern", []
        ),
        "consecutive_bullish": analysis.get(
            "consecutive_bullish", 0
        ),
        "consecutive_bearish": analysis.get(
            "consecutive_bearish", 0
        ),
        "ema5": analysis.get("ema5"),
        "ema10": analysis.get("ema10"),
        "ema20": analysis.get("ema20"),
        "ema50": analysis.get("ema50"),
        "rsi_values": analysis.get("rsi_values", [])
    })

    return display


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
            f'{bullish_count}연속 장대양봉</span>'
        )

    if bearish_count >= CONSECUTIVE_CANDLE_MIN:
        badges.append(
            '<span class="consecutive-bearish">'
            f'{bearish_count}연속 장대음봉</span>'
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
        p in BULLISH_HIGHLIGHT_PATTERNS
        for p in all_patterns
    ):
        badges.append(
            '<span class="signal-type-badge">상승패턴</span>'
        )

    if any(
        p in BEARISH_HIGHLIGHT_PATTERNS
        for p in all_patterns
    ):
        badges.append(
            '<span class="bearish-type-badge">하락패턴</span>'
        )

    badges.append(
        consecutive_candle_badges_html(row)
    )

    return "".join(badges)


# =========================================================
# 시간봉별 캔들 섹션
# =========================================================

def timeframe_candle_section(row, timeframe, analysis):

    display_row = make_display_row(row, analysis)

    label = get_timeframe_label(timeframe)

    badge_html = pattern_badges_html(display_row)

    candle_html = last_three_pattern_html(display_row)

    checklist_html = signal_checklist_html(
        row,
        timeframe,
        analysis
    )

    ema_badge = get_ema_alignment_badge(display_row)

    is_pass = (
        timeframe in row.get("signal_timeframes", [])
    )

    if is_pass:
        pass_badge = (
            '<span class="timeframe-pass">SIGNAL 통과</span>'
        )
    else:
        pass_badge = (
            '<span class="timeframe-fail">SIGNAL 미통과</span>'
        )

    return f"""
    <div class="timeframe-analysis">

        <div class="timeframe-analysis-heading">

            <strong>{label}</strong>

            {pass_badge}

            {ema_badge}

            <span class="timeframe-pattern-badges">
                {badge_html}
            </span>

        </div>

        {checklist_html}

        <div class="three-candle-section">

            <div class="three-candle-heading">
                최근 {DISPLAY_CANDLE_COUNT}개 봉 · {label} · RSI({RSI_PERIOD})
            </div>

            {candle_html}

        </div>

    </div>
    """


# =========================================================
# BTC HTML
# =========================================================

def btc_html():

    active_timeframes = get_active_timeframes()

    timeframe_sections = []

    for timeframe in active_timeframes:

        analysis = latest_btc_timeframe_data.get(timeframe)

        if not analysis:
            continue

        btc_row = {
            "last_three_patterns": analysis.get(
                "last_three_patterns", []
            )
        }

        candle_html = last_three_pattern_html(btc_row)

        timeframe_sections.append(
            f"""
            <div class="btc-candle-section">

                <div class="btc-candle-heading">
                    BTC 최근 {DISPLAY_CANDLE_COUNT}개 봉 ·
                    {get_timeframe_label(timeframe)} · RSI({RSI_PERIOD})
                </div>

                {candle_html}

            </div>
            """
        )

    if not active_timeframes:
        timeframe_sections.append(
            '<div class="signal-empty">BTC 표시 시간봉이 설정되지 않았습니다.</div>'
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

        {''.join(timeframe_sections)}

    </section>
    """


# =========================================================
# SIGNAL 카드
# 조건을 통과한 시간봉만 표시
# =========================================================

def signal_card(row):

    passed_timeframes = row.get("signal_timeframes", [])

    passed_labels = [
        get_timeframe_label(x)
        for x in passed_timeframes
    ]

    passed_badges = "".join(
        f'<span class="timeframe-pass">{html.escape(x)} 통과</span>'
        for x in passed_labels
    )

    timeframe_sections = []

    for timeframe in passed_timeframes:

        analysis = (
            row.get("timeframe_analyses", {}).get(timeframe)
        )

        if not analysis:
            continue

        timeframe_sections.append(
            timeframe_candle_section(
                row,
                timeframe,
                analysis
            )
        )

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
                {passed_badges}
            </div>

        </div>

        <div class="signal-data-grid">

            <div>
                <span>현재가</span>
                <strong>{fmt_price(row.get("current_price"))}</strong>
            </div>

            <div>
                <span>거래대금</span>
                <strong>{fmt_vol(row.get("volume_24h"))}</strong>
            </div>

            <div>
                <span>당일 변동률</span>
                <strong>{fmt_change(row.get("daily_change"))}</strong>
            </div>

        </div>

        {''.join(timeframe_sections)}

    </div>
    """


# =========================================================
# SIGNAL 영역
# =========================================================

def signal_section():

    active_timeframes = get_active_timeframes()

    if not active_timeframes:

        body = """
        <div class="signal-empty">
            SIGNAL 시간봉 설정이 없습니다.
            상단의 USE_TIMEFRAME 설정에서 하나 이상 Y로 변경하세요.
        </div>
        """

    elif not get_signal_condition_keys():

        body = """
        <div class="signal-empty">
            활성화된 SIGNAL 조건이 없습니다.
            SIGNAL 조건 설정에서 하나 이상 Y로 변경하세요.
        </div>
        """

    elif not latest_signal_data:

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
            f"최근 {DISPLAY_CANDLE_COUNT}개 봉 중 "
            "EMA1/50·100·200 실제 상향 크로스 1회 이상"
        )

    condition_items = []

    if active_conditions:

        condition_items.append(
            "<span>시간봉별 활성 조건을 모두 통과하면 해당 시간봉 통과</span>"
        )

        for condition in active_conditions:
            condition_items.append(
                f"<span>{html.escape(condition)}</span>"
            )

    else:
        condition_items.append(
            "<span>활성화된 SIGNAL 조건 없음</span>"
        )

    timeframe_text = ", ".join(
        get_timeframe_label(x)
        for x in active_timeframes
    )

    condition_items.append(
        f"<span>검사 시간봉: {html.escape(timeframe_text)}</span>"
    )

    condition_items.append(
        "<span>활성 시간봉 중 하나라도 통과하면 SIGNAL 선정</span>"
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
                {timeframe_text} · {kst()}
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

    timeframe_sections = []

    for timeframe in get_active_timeframes():

        analysis = (
            row.get("timeframe_analyses", {}).get(timeframe)
        )

        if not analysis:
            continue

        timeframe_sections.append(
            timeframe_candle_section(
                row,
                timeframe,
                analysis
            )
        )

    return f"""
    <article class="coin-card">

        <div class="coin-head">

            <div class="coin-title">
                <span class="rank">#{row["rank"]}</span>
                <b>{html.escape(row["name"])}</b>
            </div>

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
                <strong>{fmt_change(row.get("daily_change"))}</strong>
            </div>

        </div>

        {''.join(timeframe_sections)}

    </article>
    """


# =========================================================
# CSS
# =========================================================

CSS = """
:root {
color-scheme:dark;
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

* {box-sizing:border-box;}

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

section {margin-bottom:11px;}

.up {color:var(--green)!important;font-weight:800;}
.down {color:var(--red)!important;font-weight:800;}
.zero {color:#8795a2;font-weight:700;}

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

.bullish-pattern .pattern-name {color:var(--green);}
.bearish-pattern .pattern-name {color:var(--red);}
.neutral-pattern .pattern-name {color:var(--yellow);}
.pattern-none .pattern-name {color:var(--muted);}

.pattern-multi {
width:100%;
display:flex;
align-items:flex-start;
justify-content:center;
flex-wrap:wrap;
gap:5px;
}

.pattern-multi .pattern-box {min-width:43px;}

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

.mini-candle.bearish .body {background:var(--red);height:15px;}
.mini-candle.bullish .body {background:var(--green);height:19px;}

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

/* RSI */

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

/* 실제 EMA 골든크로스 발생 캔들만 노란색 강조 */

.three-candle-item.ema-golden-cross-card {
background:rgba(232,197,110,.22);
border:1px solid var(--yellow);
box-shadow:inset 0 0 0 1px rgba(232,197,110,.20),0 0 8px rgba(232,197,110,.22);
}

.three-candle-item.ema-golden-cross-card .candle-date,
.three-candle-item.ema-golden-cross-card .candle-time {
color:var(--yellow);
}

.three-candle-item.ema-golden-cross-progress {
border-width:2px;
box-shadow:inset 0 0 0 1px rgba(232,197,110,.28),0 0 10px rgba(232,197,110,.30);
}

/* EMA 크로스 배지 */

.ema-cross-badge {
display:inline-flex;
align-items:center;
justify-content:center;
margin:2px 1px;
padding:2px 3px;
color:var(--yellow);
background:rgba(232,197,110,.15);
border:1px solid rgba(232,197,110,.55);
border-radius:3px;
font-size:7px;
font-weight:900;
line-height:1.2;
white-space:nowrap;
}

/* 캔들 변동률 */

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

.candle-change-up {color:var(--green);}
.candle-change-down {color:var(--red);}
.candle-change-zero {color:#aab9c6;}
.candle-change-none {color:#71808c;}

/* 캔들 영역 */

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
flex-direction:column;
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

.three-candle-pattern .pattern-none {min-height:28px;}

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

/* 체크리스트 */

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
display:flex;
align-items:center;
justify-content:space-between;
gap:6px;
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

.check-pass {color:var(--green);}
.check-fail {color:var(--red);}
.check-unknown {color:#83919c;}

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

/* 시간봉 표시 */

.timeframe-analysis {
border-bottom:1px solid #26343f;
}

.timeframe-analysis:last-child {
border-bottom:0;
}

.timeframe-analysis-heading {
min-height:34px;
padding:6px 9px;
display:flex;
align-items:center;
flex-wrap:wrap;
gap:7px;
background:#14212c;
border-top:1px solid #263744;
}

.timeframe-analysis-heading strong {
color:#e6edf2;
font-size:var(--fs-small);
font-weight:900;
}

.timeframe-pattern-badges {
display:flex;
align-items:center;
gap:4px;
flex-wrap:wrap;
}

.timeframe-pass {
display:inline-flex;
align-items:center;
padding:3px 6px;
border-radius:4px;
color:var(--green);
background:rgba(56,217,150,.12);
border:1px solid rgba(56,217,150,.4);
font-size:var(--fs-tiny);
font-weight:900;
white-space:nowrap;
}

.timeframe-fail {
display:inline-flex;
align-items:center;
padding:3px 6px;
border-radius:4px;
color:#9aa7b2;
background:rgba(131,145,156,.10);
border:1px solid rgba(131,145,156,.25);
font-size:var(--fs-tiny);
font-weight:800;
white-space:nowrap;
}

/* EMA 배열 배지 */

.signal-identity {
display:flex;
align-items:center;
gap:8px;
flex-wrap:wrap;
}

.ema-alignment-badge {
display:inline-flex;
align-items:center;
justify-content:center;
padding:3px 6px;
border-radius:4px;
font-size:var(--fs-tiny);
font-weight:900;
line-height:1.2;
white-space:nowrap;
}

.ema-bullish-badge {
color:var(--green);
background:rgba(56,217,150,.12);
border:1px solid rgba(56,217,150,.45);
}

.ema-bearish-badge {
color:var(--red);
background:rgba(255,101,117,.12);
border:1px solid rgba(255,101,117,.45);
}

/* 배지 */

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

/* 패널 */

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

/* BTC */

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

.btc-candle-section .three-candle-item {
background:#101d28;
border-color:#304252;
}

.btc-candle-section .three-candle-label {
color:#b8c6d2;
}

/* SIGNAL */

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

.signal-condition::-webkit-scrollbar {display:none;}

.signal-condition span {
flex:0 0 auto;
color:#aab8c4;
font-size:var(--fs-tiny);
font-weight:700;
padding-right:7px;
border-right:1px solid #35434e;
}

.signal-condition span:last-child {border-right:0;}

.signal-list {width:100%;}

.signal-card {
display:flex;
flex-direction:column;
min-height:0;
background:#101820;
border-bottom:1px solid #26343f;
}

.signal-card:last-child {border-bottom:0;}

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
text-align:center;
padding:12px;
}

/* TOP */

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
body {padding:16px;}
.btc-price {font-size:23px;}
.three-candle-item {height:120px;min-height:120px;}
}

@media (max-width:899px) {
body {padding:9px;}
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

body {padding:6px;}

h1 {
margin:2px 1px 9px;
padding-bottom:8px;
}

section {margin-bottom:8px;}

.btc-panel {
padding:9px 10px;
border-radius:6px;
}

.btc-price {font-size:19px;}

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

.three-candle-heading {margin-bottom:5px;}

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
.candle-time {font-size:7px;}

.three-candle-pattern .pattern-box {min-width:0;}

.three-candle-pattern .pattern-name {
font-size:7px;
white-space:normal;
text-align:center;
overflow-wrap:anywhere;
}

.candle-pattern-visual {transform:scale(.75);}

.pattern-multi {gap:2px;}
.pattern-multi .pattern-box {min-width:0;}

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

.signal-empty {min-height:55px;}

.signal-type-list,
.pattern-badge-list {gap:3px;}

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

.three-candle-change {height:16px;}

.three-candle-change .candle-change {
height:16px;
min-height:16px;
font-size:8px;
}

.signal-checklist {padding:7px;}

.checklist-grid {
grid-template-columns:1fr;
gap:4px;
}

.checklist-item {
min-height:25px;
padding:4px 5px;
}

.signal-identity {gap:6px;}

.ema-alignment-badge {
padding:2px 5px;
font-size:7px;
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

body {padding:5px;}

.btc-price {font-size:17px;}
.update-time {font-size:7px;}

.three-candle-item {
height:112px;
min-height:112px;
grid-template-rows:23px minmax(0,1fr) 15px 16px;
row-gap:3px;
}

.three-candle-label {height:23px;}

.candle-date,
.candle-time {font-size:6px;}

.three-candle-pattern .pattern-box {min-width:0;}
.three-candle-pattern .pattern-name {font-size:6px;}

.signal-card-top {gap:4px;}

.rsi-value {
height:16px;
min-height:16px;
font-size:6px;
}

.three-candle-rsi {height:16px;}
.three-candle-change {height:15px;}

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
            log.exception("scheduler: %s", e)

        time.sleep(1)


# =========================================================
# STARTUP
# =========================================================

@app.on_event("startup")
def startup():

    active_timeframes = get_active_timeframes()

    timeframe_text = (
        ", ".join(get_timeframe_label(x) for x in active_timeframes)
        if active_timeframes
        else "설정 없음"
    )

    log.info(
        "START | BTC 및 업비트 분석 | TOP%s | 시간봉=%s",
        TOP_N,
        timeframe_text
    )

    log.info(
        "시간봉 사용 설정 | 15분=%s | 1시간=%s | 4시간=%s | 일봉=%s",
        USE_TIMEFRAME_15M,
        USE_TIMEFRAME_1H,
        USE_TIMEFRAME_4H,
        USE_TIMEFRAME_1D
    )

    log.info(
        "SIGNAL은 활성화된 시간봉 중 하나라도 모든 활성 조건을 통과하면 선정"
    )

    log.info(
        "시간봉별 분석은 RSI, EMA, 캔들 패턴을 독립적으로 계산"
    )

    log.info(
        "시간봉별 최근 %s개 캔들 표시",
        DISPLAY_CANDLE_COUNT
    )

    log.info(
        "EMA 크로스 분석용 캔들 이력=%s개",
        CANDLE_HISTORY_COUNT
    )

    log.info(
        "일봉 KST 기준: 09:00"
    )

    log.info(
        "4시간봉 KST 기준: 09, 13, 17, 21, 01, 05"
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
        "SIGNAL EMA 배열 조건 사용 여부=%s | 정배열=%s | 역배열=%s | 조건=%s",
        USE_SIGNAL_EMA_ALIGNMENT,
        USE_EMA_BULLISH_ALIGNMENT,
        USE_EMA_BEARISH_ALIGNMENT,
        get_ema_condition_text()
    )

    log.info(
        "SIGNAL EMA 실제 상향 크로스 조건 사용 여부=%s",
        USE_SIGNAL_EMA_BEARISH_BULLISH
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
        "별도 EMA 골든크로스 설정 | EMA1/50, EMA1/100, EMA1/200 | 종가(close) 기준"
    )

    log.info(
        "EMA 크로스 판정은 최근 %s개 봉 중 실제 상향 크로스 발생 여부",
        DISPLAY_CANDLE_COUNT
    )

    log.info(
        "EMA 정배열 유지 여부는 EMA1이 각 EMA보다 높은 상태로 독립 판정"
    )

    log.info(
        "SIGNAL 정렬 기준: 업비트 24시간 거래대금 높은 순"
    )

    log.info(
        "캔들 패턴은 SIGNAL 선정과 무관하게 표시"
    )

    log.info(
        "실제 EMA 골든크로스 발생(진행 및 완성) 캔들만 노란색 강조"
    )

    log.info(
        "실제 EMA 골든크로스 발생 캔들에는 EMA 크로스 배지 표시"
    )

    log.info(
        "RSI 수치 색상: 50 이하 빨강 / 50 초과 70 미만 녹색 / 70 이상 노랑"
    )

    log.info(
        "기존 EMA 정배열 연속 카운팅 기능 삭제 상태 유지"
    )

    log.info(
        "SIGNAL 코인에 통과한 시간봉만 표시"
    )

    log.info(
        "모든 시간봉이 N이면 SIGNAL 선정하지 않음"
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
