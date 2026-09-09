from fastapi import FastAPI
from fastapi.responses import HTMLResponse

import schedule
import time
import requests
import threading
import uvicorn
import logging
import pandas as pd
import warnings

from datetime import datetime
from zoneinfo import ZoneInfo


# =========================================================
# 기본 설정
# =========================================================

warnings.filterwarnings("ignore", category=FutureWarning)

app = FastAPI()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)


# =========================================================
# 사용자 설정
# =========================================================

VOLUME_HOURS = 24
TOP_N = 10
UPDATE_MINUTES = 1

HISTORY_CHUNK = 200
MAX_HISTORY_CHUNKS = 10

USE_UPBIT = "Y"
USE_OKX = "N"

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10

KST = ZoneInfo("Asia/Seoul")


# =========================================================
# EMA / ROC 설정
# =========================================================

EMA_TIMEFRAME = 60
EMA_HIGH_TIMEFRAME = 240

EMA_FAST = 30
EMA_MID = 60
EMA_SLOW = 120

EMA1_MAX_COUNT = 100

ROC_PERIOD = 10


# =========================================================
# 지원 시간봉
# =========================================================

UPBIT_TIMEFRAMES = {
    5: "minutes5",
    15: "minutes15",
    30: "minutes30",
    60: "minutes60",
    240: "minutes240",
}

OKX_TIMEFRAMES = {
    5: "5m",
    15: "15m",
    30: "30m",
    60: "1H",
    120: "2H",
    240: "4H",
    360: "6H",
    480: "8H",
    720: "12H",
    1440: "1D",
}


# =========================================================
# 전역 데이터
# =========================================================

latest_upbit_data = []
latest_okx_data = []

latest_upbit_update_time = None
latest_okx_update_time = None

latest_usdt_krw = None

latest_upbit_markets = []

data_lock = threading.Lock()
request_lock = threading.Lock()

last_request_time = 0.0


# =========================================================
# OKX 캐시
# =========================================================

okx_ticker_cache = {}
okx_1h_cache = {}
okx_1h_cache_time = {}


# =========================================================
# 시간
# =========================================================

def now_kst():
    return datetime.now(KST)


def format_timeframe(minutes):
    if minutes < 60:
        return f"{minutes}분"
    if minutes == 60:
        return "1시간"
    if minutes % 60 == 0:
        return f"{minutes // 60}시간"
    return f"{minutes}분"


def get_okx_bar(minutes):
    return OKX_TIMEFRAMES.get(minutes)


def get_okx_bar_minutes(minutes):
    return OKX_TIMEFRAMES.get(minutes)


def get_current_candle_start(minutes):
    """
    KST 기준 현재 캔들 시작시간.
    240분봉은 00 / 04 / 08 / 12 / 16 / 20시 기준.
    """

    now = now_kst().replace(second=0, microsecond=0)

    total_minutes = now.hour * 60 + now.minute
    candle_start = (total_minutes // minutes) * minutes

    hour = candle_start // 60
    minute = candle_start % 60

    return now.replace(
        hour=hour,
        minute=minute,
        second=0,
        microsecond=0
    )


# =========================================================
# 요청 제한
# =========================================================

def wait_request():
    global last_request_time

    with request_lock:
        now = time.time()
        elapsed = now - last_request_time

        if elapsed < REQUEST_INTERVAL:
            time.sleep(REQUEST_INTERVAL - elapsed)

        last_request_time = time.time()


def retry(func, *args, **kwargs):

    for attempt in range(1, MAX_RETRIES + 1):

        try:
            wait_request()

            response = func(*args, **kwargs)

            if response.status_code == 200:
                return response

            if response.status_code == 429:
                wait_sec = RATE_LIMIT_WAIT * min(attempt, 5)

                logging.warning(
                    f"429 rate limit - {wait_sec}s 대기 "
                    f"({attempt}/{MAX_RETRIES})"
                )

                time.sleep(wait_sec)
                continue

            if 500 <= response.status_code < 600:

                wait_sec = min(attempt, 5)

                logging.warning(
                    f"{response.status_code} 서버 오류 - "
                    f"{wait_sec}s 대기 "
                    f"({attempt}/{MAX_RETRIES})"
                )

                time.sleep(wait_sec)
                continue

            logging.warning(
                f"API 오류 {response.status_code}: "
                f"{response.text[:200]}"
            )

            return None

        except Exception as e:

            logging.warning(
                f"API 요청 오류 "
                f"({attempt}/{MAX_RETRIES}): {e}"
            )

            time.sleep(min(attempt, 5))

    return None


# =========================================================
# Upbit
# =========================================================

UPBIT_BASE = "https://api.upbit.com"


def get_upbit_markets():

    url = f"{UPBIT_BASE}/v1/ticker/all"

    params = {
        "quote_currencies": "KRW"
    }

    response = retry(
        requests.get,
        url,
        params=params,
        timeout=10
    )

    if response is None:
        return []

    try:
        data = response.json()
    except Exception:
        return []

    result = []

    for item in data:

        market = item.get("market")

        if not market:
            continue

        if not market.startswith("KRW-"):
            continue

        volume_24h = item.get("acc_trade_price_24h", 0)
        price = item.get("trade_price", 0)

        result.append({
            "market": market,
            "price": price,
            "volume": volume_24h
        })

    return result


def get_usdt_krw():

    url = f"{UPBIT_BASE}/v1/ticker"

    params = {
        "markets": "KRW-USDT"
    }

    response = retry(
        requests.get,
        url,
        params=params,
        timeout=10
    )

    if response is None:
        return None

    try:
        data = response.json()

        if data:
            return float(data[0]["trade_price"])

    except Exception:
        pass

    return None


def get_upbit_candle(
    market,
    minutes=60,
    count=200,
    include_current=False
):

    unit = UPBIT_TIMEFRAMES.get(minutes)

    if unit is None:
        return pd.DataFrame()

    url = f"{UPBIT_BASE}/v1/candles/{unit}"

    params = {
        "market": market,
        "count": min(count, 200)
    }

    response = retry(
        requests.get,
        url,
        params=params,
        timeout=10
    )

    if response is None:
        return pd.DataFrame()

    try:
        data = response.json()
    except Exception:
        return pd.DataFrame()

    if not isinstance(data, list) or not data:
        return pd.DataFrame()

    rows = []

    for item in reversed(data):

        rows.append({
            "time": pd.to_datetime(
                item.get("candle_date_time_kst")
            ),
            "open": float(item.get("opening_price", 0)),
            "high": float(item.get("high_price", 0)),
            "low": float(item.get("low_price", 0)),
            "close": float(item.get("trade_price", 0)),
            "volume": float(
                item.get("candle_acc_trade_volume", 0)
            ),
            "volume_krw": float(
                item.get("candle_acc_trade_price", 0)
            )
        })

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df = df.drop_duplicates("time")
    df = df.sort_values("time").reset_index(drop=True)

    if not include_current and len(df) > 0:

        current_start = get_current_candle_start(minutes)

        df = df[
            pd.to_datetime(df["time"]) < current_start.replace(
                tzinfo=None
            )
        ].reset_index(drop=True)

    return df


def get_upbit_current_roc_data(
    market,
    minutes=60,
    count=200
):

    df = get_upbit_candle(
        market,
        minutes,
        count,
        include_current=True
    )

    return df


def history_upbit(
    market,
    minutes=60,
    total=200
):

    unit = UPBIT_TIMEFRAMES.get(minutes)

    if unit is None:
        return pd.DataFrame()

    total = min(
        total,
        HISTORY_CHUNK * MAX_HISTORY_CHUNKS
    )

    all_rows = []

    to = None

    chunks = min(
        (total + HISTORY_CHUNK - 1) // HISTORY_CHUNK,
        MAX_HISTORY_CHUNKS
    )

    for _ in range(chunks):

        url = f"{UPBIT_BASE}/v1/candles/{unit}"

        params = {
            "market": market,
            "count": HISTORY_CHUNK
        }

        if to is not None:
            params["to"] = to

        response = retry(
            requests.get,
            url,
            params=params,
            timeout=10
        )

        if response is None:
            break

        try:
            data = response.json()
        except Exception:
            break

        if not data:
            break

        all_rows.extend(data)

        last_time = data[-1].get(
            "candle_date_time_utc"
        )

        if not last_time:
            break

        to = last_time

        if len(all_rows) >= total:
            break

    if not all_rows:
        return pd.DataFrame()

    rows = []

    for item in all_rows:

        rows.append({
            "time": pd.to_datetime(
                item.get("candle_date_time_kst")
            ),
            "open": float(item.get("opening_price", 0)),
            "high": float(item.get("high_price", 0)),
            "low": float(item.get("low_price", 0)),
            "close": float(item.get("trade_price", 0)),
            "volume": float(
                item.get("candle_acc_trade_volume", 0)
            ),
            "volume_krw": float(
                item.get("candle_acc_trade_price", 0)
            )
        })

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df = (
        df
        .drop_duplicates("time")
        .sort_values("time")
        .tail(total)
        .reset_index(drop=True)
    )

    current_start = get_current_candle_start(minutes)

    df = df[
        pd.to_datetime(df["time"]) <
        current_start.replace(tzinfo=None)
    ].reset_index(drop=True)

    return df


# =========================================================
# OKX
# =========================================================

OKX_BASE = "https://www.okx.com"


def get_okx_ohlcv(
    inst_id,
    bar="1H",
    limit=200,
    after=None
):

    url = f"{OKX_BASE}/api/v5/market/candles"

    params = {
        "instId": inst_id,
        "bar": bar,
        "limit": min(limit, 300)
    }

    if after is not None:
        params["after"] = after

    response = retry(
        requests.get,
        url,
        params=params,
        timeout=10
    )

    if response is None:
        return pd.DataFrame()

    try:
        result = response.json()
    except Exception:
        return pd.DataFrame()

    if result.get("code") != "0":
        return pd.DataFrame()

    data = result.get("data", [])

    if not data:
        return pd.DataFrame()

    rows = []

    for item in reversed(data):

        try:
            rows.append({
                "time": pd.to_datetime(
                    int(item[0]),
                    unit="ms"
                ).tz_localize("UTC").tz_convert(KST),
                "open": float(item[1]),
                "high": float(item[2]),
                "low": float(item[3]),
                "close": float(item[4]),
                "volume": float(item[5]),
                "volume_ccy": float(item[6]),
                "volume_quote": float(item[7]),
                "confirm": str(item[8])
            })

        except Exception:
            continue

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df = (
        df
        .drop_duplicates("time")
        .sort_values("time")
        .reset_index(drop=True)
    )

    return df


def history_okx(
    inst_id,
    minutes=60,
    total=200
):

    bar = get_okx_bar(minutes)

    if bar is None:
        return pd.DataFrame()

    total = min(
        total,
        HISTORY_CHUNK * MAX_HISTORY_CHUNKS
    )

    all_df = []

    after = None

    chunks = min(
        (total + HISTORY_CHUNK - 1) // HISTORY_CHUNK,
        MAX_HISTORY_CHUNKS
    )

    for _ in range(chunks):

        df = get_okx_ohlcv(
            inst_id,
            bar,
            HISTORY_CHUNK,
            after
        )

        if df.empty:
            break

        all_df.append(df)

        oldest = df["time"].min()

        after = int(
            oldest.timestamp() * 1000
        )

        if sum(len(x) for x in all_df) >= total:
            break

    if not all_df:
        return pd.DataFrame()

    result = pd.concat(all_df, ignore_index=True)

    result = (
        result
        .drop_duplicates("time")
        .sort_values("time")
        .tail(total)
        .reset_index(drop=True)
    )

    return result


def get_okx_tickers():

    url = f"{OKX_BASE}/api/v5/market/tickers"

    params = {
        "instType": "SWAP"
    }

    response = retry(
        requests.get,
        url,
        params=params,
        timeout=10
    )

    if response is None:
        return {}

    try:
        result = response.json()
    except Exception:
        return {}

    if result.get("code") != "0":
        return {}

    tickers = {}

    for item in result.get("data", []):

        inst_id = item.get("instId")

        if not inst_id:
            continue

        try:
            tickers[inst_id] = {
                "last": float(item.get("last", 0)),
                "vol24h": float(
                    item.get("vol24h", 0)
                )
            }
        except Exception:
            continue

    global okx_ticker_cache
    okx_ticker_cache = tickers

    return tickers


def get_okx_symbols():

    url = f"{OKX_BASE}/api/v5/public/instruments"

    params = {
        "instType": "SWAP"
    }

    response = retry(
        requests.get,
        url,
        params=params,
        timeout=10
    )

    if response is None:
        return []

    try:
        result = response.json()
    except Exception:
        return []

    if result.get("code") != "0":
        return []

    symbols = []

    for item in result.get("data", []):

        inst_id = item.get("instId")
        state = item.get("state")

        if not inst_id:
            continue

        if state != "live":
            continue

        if not inst_id.endswith("-USDT-SWAP"):
            continue

        symbols.append(inst_id)

    return symbols


def get_okx_volume_cached(inst_id, usdt_krw):

    cached = okx_1h_cache.get(inst_id)

    if cached is None:

        df = get_okx_ohlcv(
            inst_id,
            "1H",
            24
        )

        if df.empty:
            return 0

        # 확정된 1시간봉만 사용
        df = df[
            df["confirm"].astype(str) == "1"
        ].tail(24)

        okx_1h_cache[inst_id] = df
        okx_1h_cache_time[inst_id] = time.time()

    else:
        df = cached

    if df.empty:
        return 0

    volume_quote = pd.to_numeric(
        df["volume_quote"],
        errors="coerce"
    ).fillna(0)

    total_usdt = float(volume_quote.sum())

    if usdt_krw is None:
        return total_usdt

    return total_usdt * usdt_krw


def get_okx_current_1h(inst_id):

    df = get_okx_ohlcv(
        inst_id,
        "1H",
        200
    )

    if df.empty:
        return df

    return df


# =========================================================
# EMA
# =========================================================

def ema(series, period):

    return series.ewm(
        span=period,
        adjust=False
    ).mean()


def ema_alignment_count(df):

    if df.empty or len(df) < EMA_SLOW:
        return {
            "direction": None,
            "count": 0
        }

    e30 = ema(df["close"], EMA_FAST)
    e60 = ema(df["close"], EMA_MID)
    e120 = ema(df["close"], EMA_SLOW)

    directions = []

    for a, b, c in zip(
        e30,
        e60,
        e120
    ):

        if a > b > c:
            directions.append("long")

        elif a < b < c:
            directions.append("short")

        else:
            directions.append("neutral")

    current = directions[-1]

    count = 0

    if current != "neutral":

        for value in reversed(directions):

            if value != current:
                break

            count += 1

    return {
        "direction": current,
        "count": count,
        "ema30": float(e30.iloc[-1]),
        "ema60": float(e60.iloc[-1]),
        "ema120": float(e120.iloc[-1])
    }


def ema_display(df):

    result = ema_alignment_count(df)

    direction = result.get("direction")
    count = result.get("count", 0)

    if direction == "long":
        icon = "🟢"

    elif direction == "short":
        icon = "🔴"

    else:
        icon = "⚪"

    return {
        "direction": direction,
        "count": count,
        "display": f"{icon}({count})",
        "ema30": result.get("ema30"),
        "ema60": result.get("ema60"),
        "ema120": result.get("ema120")
    }


# =========================================================
# ROC
# =========================================================

def roc(df, period=ROC_PERIOD):

    if df.empty:
        return pd.Series(dtype=float)

    return (
        df["close"]
        .pct_change(periods=period) * 100
    )


def roc_count(series):

    if series.empty:
        return {
            "positive": 0,
            "negative": 0
        }

    values = series.dropna()

    if values.empty:
        return {
            "positive": 0,
            "negative": 0
        }

    positive = 0
    negative = 0

    for value in reversed(values.tolist()):

        if value > 0:
            positive += 1
        else:
            break

    for value in reversed(values.tolist()):

        if value < 0:
            negative += 1
        else:
            break

    return {
        "positive": positive,
        "negative": negative
    }


def roc_analysis(df):

    result = {
        "roc10": None,
        "roc10_previous": None,
        "roc10_count": 0,
        "roc10_negative_count": 0,

        "long_breakout": False,
        "short_breakout": False,

        "long_pullback": False,
        "short_pullback": False,

        "state": "-"
    }

    if df.empty:
        return result

    r = roc(df, ROC_PERIOD).dropna()

    if len(r) < 2:
        return result

    current = float(r.iloc[-1])
    previous = float(r.iloc[-2])

    counts = roc_count(r)

    result["roc10"] = current
    result["roc10_previous"] = previous

    result["roc10_count"] = counts["positive"]
    result["roc10_negative_count"] = counts["negative"]

    # =====================================================
    # 🚀 돌파
    # ROC 0선 음수 -> 양수 전환
    # =====================================================

    if previous <= 0 and current > 0:
        result["long_breakout"] = True

    # =====================================================
    # 🔴 숏 돌파
    # ROC 0선 양수 -> 음수 전환
    # =====================================================

    if previous >= 0 and current < 0:
        result["short_breakout"] = True

    # =====================================================
    # 🟡 롱 눌림
    # ROC 양수 -> 0 이하
    # =====================================================

    if previous > 0 and current <= 0:
        result["long_pullback"] = True

    # =====================================================
    # 🟠 숏 눌림
    # ROC 음수 -> 0 이상
    # =====================================================

    if previous < 0 and current >= 0:
        result["short_pullback"] = True

    # =====================================================
    # 표시 상태
    # =====================================================

    if result["long_breakout"]:
        result["state"] = "돌파 ①"

    elif result["short_breakout"]:
        result["state"] = "숏돌파 ①"

    elif result["long_pullback"]:
        result["state"] = "눌림 ①"

    elif result["short_pullback"]:
        result["state"] = "숏 눌림 ①"

    elif counts["positive"] >= 2:
        result["state"] = f"진행 {counts['positive']}"

    elif counts["negative"] >= 2:
        result["state"] = f"숏진행 {counts['negative']}"

    return result


# =========================================================
# 일간 등락
# =========================================================

def daily_change_upbit(market):

    url = f"{UPBIT_BASE}/v1/candles/days"

    params = {
        "market": market,
        "count": 2
    }

    response = retry(
        requests.get,
        url,
        params=params,
        timeout=10
    )

    if response is None:
        return None

    try:
        data = response.json()
    except Exception:
        return None

    if len(data) < 2:
        return None

    try:
        previous = float(
            data[1]["trade_price"]
        )

        current = float(
            data[0]["trade_price"]
        )

        if previous == 0:
            return None

        return (
            (current / previous) - 1
        ) * 100

    except Exception:
        return None


def daily_changes_okx(df):

    if df.empty:
        return None

    try:

        confirmed = df[
            df["confirm"].astype(str) == "1"
        ].copy()

        if len(confirmed) < 24:
            return None

        confirmed["time"] = pd.to_datetime(
            confirmed["time"]
        )

        confirmed = (
            confirmed
            .set_index("time")
            .sort_index()
        )

        daily = confirmed["close"].resample("1D").last()

        daily = daily.dropna()

        if len(daily) < 2:
            return None

        previous = float(daily.iloc[-2])
        current = float(daily.iloc[-1])

        if previous == 0:
            return None

        return (
            (current / previous) - 1
        ) * 100

    except Exception:
        return None


# =========================================================
# 분석 결과 기본값
# =========================================================

def empty_analysis():

    return {
        "ema1": {
            "direction": None,
            "count": 0,
            "display": "⚪(0)"
        },
        "ema4": {
            "direction": None,
            "count": 0,
            "display": "⚪(0)"
        },

        "roc10": None,
        "roc10_previous": None,

        "roc10_count": 0,
        "roc10_negative_count": 0,

        "long_breakout": False,
        "short_breakout": False,

        "long_pullback": False,
        "short_pullback": False,

        "progress_qualified": False,
        "short_progress_qualified": False,

        "pullback_qualified": False,
        "short_pullback_qualified": False,

        "daily_change": None
    }


# =========================================================
# OKX 분석
# =========================================================

def analyze_okx(inst_id, usdt_krw):

    result = empty_analysis()

    try:

        # -------------------------------------------------
        # 1H 확정봉
        # -------------------------------------------------

        df1 = okx_1h_cache.get(inst_id)

        if df1 is None or df1.empty:

            df1 = history_okx(
                inst_id,
                60,
                200
            )

            if df1.empty:
                return result

            df1 = df1[
                df1["confirm"].astype(str) == "1"
            ]

            okx_1h_cache[inst_id] = df1
            okx_1h_cache_time[inst_id] = time.time()

        # -------------------------------------------------
        # 4H
        # -------------------------------------------------

        df4 = history_okx(
            inst_id,
            240,
            200
        )

        if df4.empty:
            return result

        df4 = df4[
            df4["confirm"].astype(str) == "1"
        ]

        if df4.empty:
            return result

        # -------------------------------------------------
        # 현재 1H
        # -------------------------------------------------

        current_1h = get_okx_current_1h(
            inst_id
        )

        if not current_1h.empty:

            current_1h = current_1h[
                current_1h["confirm"].astype(str) == "1"
            ]

            if not current_1h.empty:
                df1 = current_1h

        # -------------------------------------------------
        # EMA
        # -------------------------------------------------

        e1 = ema_display(df1)
        e4 = ema_display(df4)

        result["ema1"] = e1
        result["ema4"] = e4

        # -------------------------------------------------
        # ROC
        # -------------------------------------------------

        r = roc_analysis(df1)

        result.update({
            "roc10": r["roc10"],
            "roc10_previous": r["roc10_previous"],
            "roc10_count": r["roc10_count"],
            "roc10_negative_count":
                r["roc10_negative_count"],

            "long_breakout":
                r["long_breakout"],

            "short_breakout":
                r["short_breakout"],

            "long_pullback":
                r["long_pullback"],

            "short_pullback":
                r["short_pullback"]
        })

        # -------------------------------------------------
        # 정배열 기준
        # -------------------------------------------------

        base_long = (
            e1["direction"] == "long"
            and e1["count"] <= EMA1_MAX_COUNT
        )

        base_short = (
            e1["direction"] == "short"
            and e1["count"] <= EMA1_MAX_COUNT
        )

        # -------------------------------------------------
        # 🚀 돌파 정배열
        # EMA30 > EMA60 > EMA120
        # + ROC 음수 -> 양수
        # -------------------------------------------------

        result["progress_qualified"] = (
            base_long
            and r["long_breakout"]
        )

        # -------------------------------------------------
        # 🟡 눌림 정배열
        # -------------------------------------------------

        result["pullback_qualified"] = (
            base_long
            and r["long_pullback"]
        )

        # -------------------------------------------------
        # 🔴 숏 진행
        # -------------------------------------------------

        result["short_progress_qualified"] = (
            base_short
            and r["short_breakout"]
        )

        # -------------------------------------------------
        # 🟠 숏 눌림
        # -------------------------------------------------

        result["short_pullback_qualified"] = (
            base_short
            and r["short_pullback"]
        )

        # -------------------------------------------------
        # 일간 변화
        # -------------------------------------------------

        result["daily_change"] = daily_changes_okx(df1)

    except Exception as e:

        logging.warning(
            f"OKX 분석 오류 {inst_id}: {e}"
        )

    return result


# =========================================================
# Upbit 분석
# =========================================================

def analyze(market):

    result = empty_analysis()

    try:

        # -------------------------------------------------
        # 1H
        # -------------------------------------------------

        df1 = history_upbit(
            market,
            EMA_TIMEFRAME,
            200
        )

        if df1.empty:
            return result

        # -------------------------------------------------
        # 4H
        # -------------------------------------------------

        df4 = history_upbit(
            market,
            EMA_HIGH_TIMEFRAME,
            200
        )

        if df4.empty:
            return result

        # -------------------------------------------------
        # EMA
        # -------------------------------------------------

        e1 = ema_display(df1)
        e4 = ema_display(df4)

        result["ema1"] = e1
        result["ema4"] = e4

        # -------------------------------------------------
        # ROC
        # -------------------------------------------------

        current_df = get_upbit_current_roc_data(
            market,
            EMA_TIMEFRAME,
            200
        )

        if not current_df.empty:
            df_roc = current_df
        else:
            df_roc = df1

        r = roc_analysis(df_roc)

        result.update({
            "roc10": r["roc10"],
            "roc10_previous": r["roc10_previous"],
            "roc10_count": r["roc10_count"],
            "roc10_negative_count":
                r["roc10_negative_count"],

            "long_breakout":
                r["long_breakout"],

            "short_breakout":
                r["short_breakout"],

            "long_pullback":
                r["long_pullback"],

            "short_pullback":
                r["short_pullback"]
        })

        # -------------------------------------------------
        # 정배열
        # -------------------------------------------------

        base_long = (
            e1["direction"] == "long"
            and e1["count"] <= EMA1_MAX_COUNT
        )

        base_short = (
            e1["direction"] == "short"
            and e1["count"] <= EMA1_MAX_COUNT
        )

        # -------------------------------------------------
        # 🚀 돌파 정배열
        # -------------------------------------------------

        result["progress_qualified"] = (
            base_long
            and r["long_breakout"]
        )

        # -------------------------------------------------
        # 🟡 눌림 정배열
        # -------------------------------------------------

        result["pullback_qualified"] = (
            base_long
            and r["long_pullback"]
        )

        # -------------------------------------------------
        # 🔴 숏 돌파
        # -------------------------------------------------

        result["short_progress_qualified"] = (
            base_short
            and r["short_breakout"]
        )

        # -------------------------------------------------
        # 🟠 숏 눌림
        # -------------------------------------------------

        result["short_pullback_qualified"] = (
            base_short
            and r["short_pullback"]
        )

        # -------------------------------------------------
        # 일간 등락
        # -------------------------------------------------

        result["daily_change"] = daily_change_upbit(
            market
        )

    except Exception as e:

        logging.warning(
            f"Upbit 분석 오류 {market}: {e}"
        )

    return result


# =========================================================
# Row 생성
# =========================================================

def make_row(
    rank,
    name,
    turnover,
    analysis,
    price=None
):

    return {
        "rank": rank,
        "name": name,
        "turnover": turnover,
        "price": price,

        **analysis
    }


# =========================================================
# 후보 판정
# =========================================================

def is_pullback(row):

    return bool(
        row.get("pullback_qualified", False)
    )


def is_short_pullback(row):

    return bool(
        row.get("short_pullback_qualified", False)
    )


def is_progress(row):

    return bool(
        row.get("progress_qualified", False)
    )


def is_short_progress(row):

    return bool(
        row.get("short_progress_qualified", False)
    )


# =========================================================
# Upbit 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update_time
    global latest_upbit_markets
    global latest_usdt_krw

    markets = get_upbit_markets()

    if not markets:
        return

    latest_upbit_markets = markets

    usdt = get_usdt_krw()

    if usdt is not None:
        latest_usdt_krw = usdt

    markets = sorted(
        markets,
        key=lambda x: x["volume"],
        reverse=True
    )

    top = markets[:TOP_N]

    rows = []

    for idx, item in enumerate(top, 1):

        market = item["market"]

        analysis = analyze(market)

        coin = market.replace(
            "KRW-",
            ""
        )

        rows.append(
            make_row(
                idx,
                coin,
                item["volume"],
                analysis,
                item["price"]
            )
        )

    with data_lock:

        latest_upbit_data = rows
        latest_upbit_update_time = now_kst()


# =========================================================
# OKX 업데이트
# =========================================================

def update_okx():

    global latest_okx_data
    global latest_okx_update_time

    tickers = get_okx_tickers()

    if not tickers:
        return

    symbols = get_okx_symbols()

    if not symbols:
        return

    # ticker에 실제 존재하는 live SWAP만 사용
    symbols = [
        x for x in symbols
        if x in tickers
    ]

    usdt_krw = latest_usdt_krw

    if usdt_krw is None:
        usdt_krw = get_usdt_krw()

    volumes = []

    for inst_id in symbols:

        try:

            volume = get_okx_volume_cached(
                inst_id,
                usdt_krw
            )

            if volume <= 0:
                continue

            price = tickers[
                inst_id
            ]["last"]

            volumes.append({
                "inst_id": inst_id,
                "volume": volume,
                "price": price
            })

        except Exception as e:

            logging.warning(
                f"OKX 거래대금 오류 "
                f"{inst_id}: {e}"
            )

    volumes.sort(
        key=lambda x: x["volume"],
        reverse=True
    )

    top = volumes[:TOP_N]

    rows = []

    upbit_names = {
        x["name"]
        for x in latest_upbit_data
    }

    for idx, item in enumerate(top, 1):

        inst_id = item["inst_id"]

        coin = inst_id.replace(
            "-USDT-SWAP",
            ""
        )

        if coin in upbit_names:
            display_name = f"{coin} (업비트)"
        else:
            display_name = coin

        analysis = analyze_okx(
            inst_id,
            usdt_krw
        )

        rows.append(
            make_row(
                idx,
                display_name,
                item["volume"],
                analysis,
                item["price"]
            )
        )

    with data_lock:

        latest_okx_data = rows
        latest_okx_update_time = now_kst()


# =========================================================
# 전체 업데이트
# =========================================================

def update_all():

    start = time.time()

    try:

        if USE_UPBIT == "Y":
            update_upbit()

        if USE_OKX == "Y":
            update_okx()

        elapsed = time.time() - start

        logging.info(
            f"전체 업데이트 완료 "
            f"{elapsed:.2f}s"
        )

    except Exception as e:

        logging.exception(
            f"전체 업데이트 오류: {e}"
        )


# =========================================================
# 숫자 표시
# =========================================================

def format_turnover(value):

    if value is None:
        return "-"

    try:
        value = float(value)
    except Exception:
        return "-"

    if value >= 1_000_000_000_000:
        return f"{value / 1_000_000_000_000:.1f}조"

    if value >= 100_000_000:
        return f"{value / 100_000_000:.1f}억"

    if value >= 10_000:
        return f"{value / 10_000:.0f}만원"

    return f"{value:,.0f}원"


def format_price(value):

    if value is None:
        return "-"

    try:
        value = float(value)

        if value >= 1000:
            return f"{value:,.0f}"

        if value >= 1:
            return f"{value:,.2f}"

        return f"{value:.6f}"

    except Exception:
        return "-"


def format_change(value):

    if value is None:
        return "-"

    try:

        value = float(value)

        if value > 0:
            return f"+{value:.2f}%"

        return f"{value:.2f}%"

    except Exception:
        return "-"


# =========================================================
# ROC HTML
# =========================================================

def roc_html(row):

    value = row.get("roc10")

    if value is None:
        return "-"

    try:
        value = float(value)
    except Exception:
        return "-"

    if value > 0:

        count = row.get(
            "roc10_count",
            0
        )

        return (
            f'<span class="roc-long">'
            f'+{value:.2f}% '
            f'({count})'
            f'</span>'
        )

    if value < 0:

        count = row.get(
            "roc10_negative_count",
            0
        )

        return (
            f'<span class="roc-short">'
            f'{value:.2f}% '
            f'({count})'
            f'</span>'
        )

    return "0.00%"


# =========================================================
# Signal HTML
# =========================================================

def signal_html(row):

    # 가장 우선적으로 실제 0선 전환을 표시
    if row.get("long_breakout"):

        return (
            '<span class="signal-breakout">'
            '🚀 돌파 ①'
            '</span>'
        )

    if row.get("short_breakout"):

        return (
            '<span class="signal-short-breakout">'
            '🔴 숏돌파 ①'
            '</span>'
        )

    if row.get("long_pullback"):

        return (
            '<span class="signal-pullback">'
            '🟡 눌림 ①'
            '</span>'
        )

    if row.get("short_pullback"):

        return (
            '<span class="signal-short-pullback">'
            '🟠 숏 눌림 ①'
            '</span>'
        )

    roc_positive = row.get(
        "roc10_count",
        0
    )

    roc_negative = row.get(
        "roc10_negative_count",
        0
    )

    if roc_positive >= 2:

        return (
            '<span class="signal-progress">'
            f'🚀 진행 {roc_positive}'
            '</span>'
        )

    if roc_negative >= 2:

        return (
            '<span class="signal-short-progress">'
            f'🔴 숏진행 {roc_negative}'
            '</span>'
        )

    return "-"


# =========================================================
# EMA HTML
# =========================================================

def ema_html(row):

    e1 = row.get("ema1", {})
    e4 = row.get("ema4", {})

    d1 = e1.get(
        "display",
        "⚪(0)"
    )

    d4 = e4.get(
        "display",
        "⚪(0)"
    )

    return (
        f'<div class="ema-line">'
        f'1H {d1}'
        f'</div>'
        f'<div class="ema-line">'
        f'4H {d4}'
        f'</div>'
    )


# =========================================================
# Row CSS
# =========================================================

def row_class(row):

    if row.get("long_breakout"):
        return "row-breakout"

    if row.get("short_breakout"):
        return "row-short-breakout"

    if row.get("long_pullback"):
        return "row-pullback"

    if row.get("short_pullback"):
        return "row-short-pullback"

    if row.get("progress_qualified"):
        return "row-progress"

    if row.get("short_progress_qualified"):
        return "row-short-progress"

    return ""


# =========================================================
# Rows HTML
# =========================================================

def rows_html(data):

    if not data:
        return (
            '<div class="empty">'
            '데이터 없음'
            '</div>'
        )

    html = ""

    for row in data:

        cls = row_class(row)

        html += f"""
        <tr class="{cls}">
            <td>{row.get("rank", "-")}</td>

            <td class="coin">
                {row.get("name", "-")}
            </td>

            <td class="turnover">
                {format_turnover(
                    row.get("turnover")
                )}
            </td>

            <td class="ema">
                {ema_html(row)}
            </td>

            <td class="roc">
                {roc_html(row)}
            </td>

            <td class="signal">
                {signal_html(row)}
            </td>
        </tr>
        """

    return html


# =========================================================
# Focus Section
# =========================================================

def focus_section(
    title,
    data,
    update_time,
    checker,
    focus,
    description,
    sort_key=None,
    reverse=True
):

    selected = [
        row for row in data
        if checker(row)
    ]

    if sort_key is not None:

        selected.sort(
            key=lambda x: (
                x.get(sort_key)
                if x.get(sort_key) is not None
                else -999999
            ),
            reverse=reverse
        )

    time_text = "-"

    if update_time is not None:
        time_text = update_time.strftime(
            "%H:%M:%S"
        )

    return f"""
    <section class="section {focus}-section">

        <div class="section-head">

            <div class="section-title">
                {title}
            </div>

            <div class="section-time">
                {time_text}
            </div>

        </div>

        <div class="description">
            {description}
        </div>

        <table>

            <thead>
                <tr>
                    <th>#</th>
                    <th>코인</th>
                    <th>거래대금</th>
                    <th>EMA</th>
                    <th>ROC10</th>
                    <th>신호</th>
                </tr>
            </thead>

            <tbody>
                {rows_html(selected)}
            </tbody>

        </table>

    </section>
    """


# =========================================================
# 전체 Section
# =========================================================

def section(
    title,
    data,
    update_time,
    description=""
):

    time_text = "-"

    if update_time is not None:

        time_text = update_time.strftime(
            "%H:%M:%S"
        )

    return f"""
    <section class="section">

        <div class="section-head">

            <div class="section-title">
                {title}
            </div>

            <div class="section-time">
                {time_text}
            </div>

        </div>

        <div class="description">
            {description}
        </div>

        <table>

            <thead>
                <tr>
                    <th>#</th>
                    <th>코인</th>
                    <th>거래대금</th>
                    <th>EMA</th>
                    <th>ROC10</th>
                    <th>신호</th>
                </tr>
            </thead>

            <tbody>
                {rows_html(data)}
            </tbody>

        </table>

    </section>
    """


# =========================================================
# BTC 요약
# =========================================================

def get_btc_row():

    for row in latest_upbit_data:

        if row.get("name") == "BTC":
            return row

    return None


def btc_position_view(row):

    if row is None:
        return "⚪ 관망"

    e1 = row.get("ema1", {})
    e4 = row.get("ema4", {})

    d1 = e1.get("direction")
    d4 = e4.get("direction")

    roc_value = row.get("roc10")

    if d1 != d4:
        return "⚪ 관망"

    if d1 == "long":

        if row.get("long_pullback"):
            return "🟡 롱 눌림"

        if roc_value is not None and roc_value > 0:
            return "🟢 롱 우세"

        return "⚪ 롱 대기"

    if d1 == "short":

        if row.get("short_pullback"):
            return "🟠 숏 눌림"

        if roc_value is not None and roc_value < 0:
            return "🔴 숏 우세"

        return "⚪ 숏 대기"

    return "⚪ 관망"


def btc_summary():

    row = get_btc_row()

    if row is None:
        return ""

    e1 = row.get("ema1", {})
    e4 = row.get("ema4", {})

    daily = format_change(
        row.get("daily_change")
    )

    price = format_price(
        row.get("price")
    )

    roc_value = row.get("roc10")

    if roc_value is None:
        roc_text = "-"
    else:
        roc_text = f"{roc_value:+.2f}%"

    position = btc_position_view(row)

    return f"""
    <div class="btc-box">

        <div class="btc-title">
            ₿ BTC 시장 요약
        </div>

        <div class="btc-grid">

            <div>
                <span>가격</span>
                <strong>{price}</strong>
            </div>

            <div>
                <span>일간</span>
                <strong>{daily}</strong>
            </div>

            <div>
                <span>1H EMA</span>
                <strong>
                    {e1.get("display", "⚪(0)")}
                </strong>
            </div>

            <div>
                <span>4H EMA</span>
                <strong>
                    {e4.get("display", "⚪(0)")}
                </strong>
            </div>

            <div>
                <span>ROC10</span>
                <strong>{roc_text}</strong>
            </div>

            <div>
                <span>시장상태</span>
                <strong>{position}</strong>
            </div>

        </div>

    </div>
    """


# =========================================================
# Dashboard
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def dashboard():

    with data_lock:

        upbit_data = list(
            latest_upbit_data
        )

        okx_data = list(
            latest_okx_data
        )

        upbit_time = (
            latest_upbit_update_time
        )

        okx_time = (
            latest_okx_update_time
        )

    sections = ""

    # =====================================================
    # UPBIT
    # =====================================================

    if USE_UPBIT == "Y":

        # 🚀 돌파 정배열
        sections += focus_section(
            "🚀 돌파 정배열",
            upbit_data,
            upbit_time,
            is_progress,
            "progress",
            "EMA30>60>120 · ROC10 음수→양수 전환 ①"
        )

        # 🟡 눌림 정배열
        sections += focus_section(
            "🟡 눌림 정배열",
            upbit_data,
            upbit_time,
            is_pullback,
            "pullback",
            "EMA30>60>120 · ROC10 양수→0 이하 전환 ①"
        )

    # =====================================================
    # OKX
    # =====================================================

    if USE_OKX == "Y":

        # 🚀 돌파 정배열
        sections += focus_section(
            "🚀 돌파 정배열",
            okx_data,
            okx_time,
            is_progress,
            "progress",
            "EMA30>60>120 · ROC10 음수→양수 전환 ①"
        )

        # 🟡 눌림 정배열
        sections += focus_section(
            "🟡 눌림 정배열",
            okx_data,
            okx_time,
            is_pullback,
            "pullback",
            "EMA30>60>120 · ROC10 양수→0 이하 전환 ①"
        )

        # 🟠 숏 눌림
        sections += focus_section(
            "🟠 숏 눌림",
            okx_data,
            okx_time,
            is_short_pullback,
            "short_pullback",
            "EMA30<60<120 · ROC10 음수→0 이상 전환 ①"
        )

    # =====================================================
    # 전체 TOP
    # =====================================================

    if USE_UPBIT == "Y":

        sections += section(
            "🏆 업비트 TOP10",
            upbit_data,
            upbit_time,
            "24시간 거래대금 기준"
        )

    if USE_OKX == "Y":

        sections += section(
            "🏆 OKX TOP10",
            okx_data,
            okx_time,
            "최근 24개 확정 1시간봉 거래대금 기준"
        )

    # =====================================================
    # 시간
    # =====================================================

    current_time = now_kst().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    html = f"""
    <!DOCTYPE html>

    <html lang="ko">

    <head>

        <meta charset="UTF-8">

        <meta
            name="viewport"
            content="width=device-width,
            initial-scale=1.0,
            maximum-scale=1.0"
        >

        <meta
            http-equiv="refresh"
            content="60"
        >

        <title>코인 스캐너</title>

        <style>

            * {{
                box-sizing: border-box;
            }}

            html,
            body {{
                margin: 0;
                padding: 0;
                background: #111;
                color: #eee;
                font-family:
                    Arial,
                    "Noto Sans KR",
                    sans-serif;
                font-size: 8px;
            }}

            body {{
                width: 100%;
                overflow-x: hidden;
            }}

            .container {{
                width: 100%;
                max-width: 900px;
                margin: 0 auto;
                padding: 5px;
            }}

            .header {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                padding: 4px 2px 6px;
                border-bottom: 1px solid #333;
                margin-bottom: 5px;
            }}

            .title {{
                font-size: 11px;
                font-weight: 700;
            }}

            .clock {{
                font-size: 7px;
                color: #888;
            }}

            .btc-box {{
                background: #181818;
                border: 1px solid #333;
                border-radius: 5px;
                padding: 6px;
                margin-bottom: 6px;
            }}

            .btc-title {{
                font-size: 9px;
                font-weight: 700;
                margin-bottom: 5px;
            }}

            .btc-grid {{
                display: grid;
                grid-template-columns:
                    repeat(3, 1fr);
                gap: 4px;
            }}

            .btc-grid div {{
                background: #202020;
                border-radius: 4px;
                padding: 4px;
                min-width: 0;
            }}

            .btc-grid span {{
                display: block;
                color: #888;
                font-size: 7px;
                margin-bottom: 2px;
            }}

            .btc-grid strong {{
                display: block;
                font-size: 8px;
                font-weight: 700;
                white-space: nowrap;
            }}

            .section {{
                margin-bottom: 7px;
                background: #151515;
                border: 1px solid #292929;
                border-radius: 5px;
                overflow: hidden;
            }}

            .section-head {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                padding: 5px 6px 2px;
            }}

            .section-title {{
                font-size: 9px;
                font-weight: 700;
            }}

            .section-time {{
                color: #777;
                font-size: 7px;
            }}

            .description {{
                color: #777;
                font-size: 7px;
                padding: 0 6px 4px;
            }}

            table {{
                width: 100%;
                border-collapse: collapse;
                table-layout: fixed;
            }}

            th {{
                color: #777;
                font-size: 7px;
                font-weight: 500;
                padding: 3px 2px;
                border-top: 1px solid #292929;
                border-bottom: 1px solid #292929;
            }}

            td {{
                padding: 4px 2px;
                border-bottom: 1px solid #222;
                text-align: center;
                vertical-align: middle;
                font-size: 8px;
                line-height: 1.15;
            }}

            th:nth-child(1),
            td:nth-child(1) {{
                width: 7%;
            }}

            th:nth-child(2),
            td:nth-child(2) {{
                width: 23%;
            }}

            th:nth-child(3),
            td:nth-child(3) {{
                width: 18%;
            }}

            th:nth-child(4),
            td:nth-child(4) {{
                width: 20%;
            }}

            th:nth-child(5),
            td:nth-child(5) {{
                width: 15%;
            }}

            th:nth-child(6),
            td:nth-child(6) {{
                width: 17%;
            }}

            .coin {{
                text-align: left;
                font-weight: 600;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
            }}

            .turnover {{
                white-space: nowrap;
                font-size: 8px;
            }}

            .ema {{
                line-height: 1.15;
            }}

            .ema-line {{
                white-space: nowrap;
            }}

            .roc {{
                white-space: nowrap;
            }}

            .roc-long {{
                color: #55d66b;
            }}

            .roc-short {{
                color: #ff6666;
            }}

            .signal {{
                white-space: nowrap;
                font-weight: 700;
            }}

            .signal-breakout {{
                color: #4cc9ff;
            }}

            .signal-short-breakout {{
                color: #ff6666;
            }}

            .signal-pullback {{
                color: #ffd84d;
            }}

            .signal-short-pullback {{
                color: #ff9b4a;
            }}

            .signal-progress {{
                color: #4cc9ff;
            }}

            .signal-short-progress {{
                color: #ff6666;
            }}

            .row-breakout {{
                background: rgba(
                    76, 201, 255, 0.08
                );
            }}

            .row-short-breakout {{
                background: rgba(
                    255, 102, 102, 0.08
                );
            }}

            .row-pullback {{
                background: rgba(
                    255, 216, 77, 0.08
                );
            }}

            .row-short-pullback {{
                background: rgba(
                    255, 155, 74, 0.08
                );
            }}

            .row-progress {{
                background: rgba(
                    76, 201, 255, 0.05
                );
            }}

            .row-short-progress {{
                background: rgba(
                    255, 102, 102, 0.05
                );
            }}

            .progress-title {{
                color: #4cc9ff;
            }}

            .short_progress-title {{
                color: #ff6666;
            }}

            .empty {{
                padding: 10px;
                text-align: center;
                color: #666;
                font-size: 8px;
            }}

            @media (max-width: 380px) {{

                body {{
                    font-size: 7px;
                }}

                .container {{
                    padding: 3px;
                }}

                .section-title {{
                    font-size: 8px;
                }}

                th {{
                    font-size: 6px;
                }}

                td {{
                    font-size: 7px;
                    padding: 3px 1px;
                }}

                .turnover {{
                    font-size: 7px;
                }}

                .btc-grid strong {{
                    font-size: 7px;
                }}

            }}

            @media (min-width: 601px) {{

                body {{
                    font-size: 9px;
                }}

                .container {{
                    padding: 8px;
                }}

                td {{
                    font-size: 9px;
                    padding: 5px 3px;
                }}

                th {{
                    font-size: 8px;
                }}

            }}

        </style>

    </head>

    <body>

        <div class="container">

            <div class="header">

                <div class="title">
                    📊 코인 스캐너
                </div>

                <div class="clock">
                    {current_time} KST
                </div>

            </div>

            {btc_summary()}

            {sections}

        </div>

    </body>

    </html>
    """

    return HTMLResponse(
        content=html
    )


# =========================================================
# 스케줄러
# =========================================================

def scheduler_loop():

    schedule.every(
        UPDATE_MINUTES
    ).minutes.do(update_all)

    while True:

        try:
            schedule.run_pending()

        except Exception as e:

            logging.exception(
                f"스케줄러 오류: {e}"
            )

        time.sleep(1)


# =========================================================
# 시작
# =========================================================

@app.on_event("startup")
def startup_event():

    logging.info(
        "========================================"
    )

    logging.info(
        "코인 스캐너 시작"
    )

    logging.info(
        "돌파 정의: ROC10 음수 → 양수 전환"
    )

    logging.info(
        "돌파 정배열: EMA30 > EMA60 > EMA120 "
        "+ ROC10 양수 전환"
    )

    logging.info(
        "눌림 정배열: EMA30 > EMA60 > EMA120 "
        "+ ROC10 양수 → 0 이하"
    )

    logging.info(
        "ROC 양수/음수 진행 카운트 유지"
    )

    logging.info(
        "가격 돌파 기능은 사용하지 않음"
    )

    logging.info(
        "OKX: 전체 ticker 1회 조회 "
        "+ 1H 거래대금 캐시"
    )

    logging.info(
        "BTC 시장상태 표시"
    )

    logging.info(
        "========================================"
    )

    thread = threading.Thread(
        target=update_all,
        daemon=True
    )

    thread.start()

    scheduler_thread = threading.Thread(
        target=scheduler_loop,
        daemon=True
    )

    scheduler_thread.start()


# =========================================================
# 실행
# =========================================================

if __name__ == "__main__":

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )
