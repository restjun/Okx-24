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

warnings.filterwarnings(
    "ignore",
    category=FutureWarning
)

app = FastAPI()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# =========================================================
# CONFIG
# =========================================================

VOLUME_HOURS = 24
TOP_N = 50

UPDATE_MINUTES = 1

HISTORY_CHUNK = 200
MAX_HISTORY_CHUNKS = 10

USE_UPBIT = "Y"
USE_OKX = "N"

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10

KST = ZoneInfo("Asia/Seoul")

# EMA10 이전 진행 카운트 최소값
MIN_EMA10_LONG_COUNT = 1


# =========================================================
# GLOBAL STATE
# =========================================================

latest_upbit_data = []
latest_okx_data = []

latest_usdt_krw = 0.0

latest_upbit_update_time = "-"
latest_okx_update_time = "-"

latest_upbit_markets = set()

request_lock = threading.Lock()
update_lock = threading.Lock()
air_state_lock = threading.Lock()

last_request_time = 0.0

# ---------------------------------------------------------
# 비행기 상태
#
# {
#   "KRW-BTC": {
#       "active": True,
#       "direction": "long",
#       "count": 2,
#       "warning_time": "...",
#       "last_candle_time": ...
#   }
# }
# ---------------------------------------------------------

air_state = {}


# =========================================================
# TIME
# =========================================================

def kst():
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


# =========================================================
# REQUEST CONTROL
# =========================================================

def wait_request():

    global last_request_time

    with request_lock:

        now = time.time()

        diff = now - last_request_time

        if diff < REQUEST_INTERVAL:
            time.sleep(REQUEST_INTERVAL - diff)

        last_request_time = time.time()


def retry(func, *args, **kwargs):

    for attempt in range(MAX_RETRIES):

        try:

            wait_request()

            r = func(*args, **kwargs)

            if r.status_code == 429:

                logging.warning(
                    "429 rate limit -> %s초 대기",
                    RATE_LIMIT_WAIT
                )

                time.sleep(RATE_LIMIT_WAIT)

                continue

            if r.status_code >= 500:

                time.sleep(1 + attempt)

                continue

            r.raise_for_status()

            return r

        except Exception as e:

            if attempt == MAX_RETRIES - 1:

                logging.error(
                    "request failed: %s",
                    e
                )

                return None

            time.sleep(1 + attempt)

    return None


# =========================================================
# UPBIT MARKET
# =========================================================

def get_upbit_markets():

    url = "https://api.upbit.com/v1/ticker/all"

    r = retry(
        requests.get,
        url,
        params={
            "quote_currencies": "KRW"
        },
        timeout=10
    )

    if r is None:
        return []

    try:

        data = r.json()

        result = []

        for x in data:

            market = x.get("market")

            if not market:
                continue

            trade_value = float(
                x.get("acc_trade_price_24h", 0) or 0
            )

            result.append({
                "market": market,
                "trade_value": trade_value,
                "change_rate": float(
                    x.get("signed_change_rate", 0) or 0
                )
            })

        return result

    except Exception as e:

        logging.error(
            "get_upbit_markets error: %s",
            e
        )

        return []


# =========================================================
# USDT/KRW
# =========================================================

def get_usdt_krw():

    url = "https://api.upbit.com/v1/ticker"

    r = retry(
        requests.get,
        url,
        params={
            "markets": "KRW-USDT"
        },
        timeout=10
    )

    if r is None:
        return 0.0

    try:

        data = r.json()

        if not data:
            return 0.0

        return float(
            data[0].get("trade_price", 0) or 0
        )

    except Exception:

        return 0.0


# =========================================================
# UPBIT CANDLE
# =========================================================

def get_upbit_candle(
    market,
    unit,
    count=200,
    to=None
):

    url = f"https://api.upbit.com/v1/candles/minutes/{unit}"

    params = {
        "market": market,
        "count": count
    }

    if to is not None:
        params["to"] = to

    r = retry(
        requests.get,
        url,
        params=params,
        timeout=10
    )

    if r is None:
        return pd.DataFrame()

    try:

        data = r.json()

        if not isinstance(data, list):
            return pd.DataFrame()

        rows = []

        for x in data:

            rows.append({
                "time": pd.to_datetime(
                    x["candle_date_time_kst"]
                ),
                "open": float(x["opening_price"]),
                "high": float(x["high_price"]),
                "low": float(x["low_price"]),
                "close": float(x["trade_price"]),
                "volume": float(x["candle_acc_trade_volume"]),
                "volume_krw": float(
                    x.get("candle_acc_trade_price", 0) or 0
                )
            })

        df = pd.DataFrame(rows)

        if df.empty:
            return df

        df = df.sort_values("time").reset_index(drop=True)

        # -------------------------------------------------
        # 현재 진행 중인 캔들 제거
        # -------------------------------------------------

        now = datetime.now(KST).replace(tzinfo=None)

        if unit == 60:

            current_hour = now.replace(
                minute=0,
                second=0,
                microsecond=0
            )

            df = df[
                df["time"] < current_hour
            ]

        elif unit == 240:

            hour = (now.hour // 4) * 4

            current_block = now.replace(
                hour=hour,
                minute=0,
                second=0,
                microsecond=0
            )

            df = df[
                df["time"] < current_block
            ]

        return df.reset_index(drop=True)

    except Exception as e:

        logging.error(
            "get_upbit_candle error %s %s: %s",
            market,
            unit,
            e
        )

        return pd.DataFrame()


def get_upbit_1h(market, count=200):

    return get_upbit_candle(
        market,
        60,
        count=count
    )


def get_upbit_4h(market, count=200):

    return get_upbit_candle(
        market,
        240,
        count=count
    )


# =========================================================
# OKX CANDLE
# =========================================================

def get_okx_ohlcv(
    inst,
    bar="1H",
    limit=200,
    before=None
):

    url = "https://www.okx.com/api/v5/market/candles"

    params = {
        "instId": inst,
        "bar": bar,
        "limit": str(limit)
    }

    if before is not None:
        params["before"] = str(before)

    r = retry(
        requests.get,
        url,
        params=params,
        timeout=10
    )

    if r is None:
        return pd.DataFrame()

    try:

        js = r.json()

        if js.get("code") != "0":
            return pd.DataFrame()

        data = js.get("data", [])

        rows = []

        for x in data:

            rows.append({
                "time": pd.to_datetime(
                    int(x[0]),
                    unit="ms"
                ).tz_localize("UTC").tz_convert(KST).tz_localize(None),

                "open": float(x[1]),
                "high": float(x[2]),
                "low": float(x[3]),
                "close": float(x[4]),
                "volume": float(x[5]),
                "volume_krw": 0.0,
                "confirm": x[8] if len(x) > 8 else "1"
            })

        df = pd.DataFrame(rows)

        if df.empty:
            return df

        # 확정 캔들만
        if "confirm" in df.columns:

            df = df[
                df["confirm"].astype(str) == "1"
            ]

        df = df.sort_values("time").reset_index(drop=True)

        return df

    except Exception as e:

        logging.error(
            "get_okx_ohlcv error %s: %s",
            inst,
            e
        )

        return pd.DataFrame()


# =========================================================
# HISTORY
# =========================================================

def history_upbit(
    market,
    unit,
    required=200
):

    result = []

    to = None

    for _ in range(MAX_HISTORY_CHUNKS):

        df = get_upbit_candle(
            market,
            unit,
            count=HISTORY_CHUNK,
            to=to
        )

        if df.empty:
            break

        result.append(df)

        if len(pd.concat(result)) >= required:
            break

        oldest = df["time"].min()

        to = (
            oldest.strftime("%Y-%m-%d %H:%M:%S")
        )

        time.sleep(REQUEST_INTERVAL)

    if not result:
        return pd.DataFrame()

    df = pd.concat(
        result,
        ignore_index=True
    )

    df = (
        df.drop_duplicates("time")
          .sort_values("time")
          .tail(required)
          .reset_index(drop=True)
    )

    return df


def history_okx(
    inst,
    bar="1H",
    required=200
):

    result = []

    before = None

    for _ in range(MAX_HISTORY_CHUNKS):

        df = get_okx_ohlcv(
            inst,
            bar=bar,
            limit=HISTORY_CHUNK,
            before=before
        )

        if df.empty:
            break

        result.append(df)

        combined = pd.concat(
            result,
            ignore_index=True
        )

        if len(combined) >= required:
            break

        oldest = df["time"].min()

        before = int(
            pd.Timestamp(oldest)
            .tz_localize(KST)
            .timestamp() * 1000
        )

        time.sleep(REQUEST_INTERVAL)

    if not result:
        return pd.DataFrame()

    df = pd.concat(
        result,
        ignore_index=True
    )

    df = (
        df.drop_duplicates("time")
          .sort_values("time")
          .tail(required)
          .reset_index(drop=True)
    )

    return df


# =========================================================
# EMA
# =========================================================

def ema(df, period):

    if df.empty:
        return pd.Series(dtype=float)

    return df["close"].ewm(
        span=period,
        adjust=False
    ).mean()


# =========================================================
# EMA FULL ALIGNMENT
#
# LONG:
# EMA10 > EMA30 > EMA60 > EMA120
#
# SHORT:
# EMA10 < EMA30 < EMA60 < EMA120
# =========================================================

def direction(df):

    if df.empty or len(df) < 120:
        return "none"

    e10 = ema(df, 10).iloc[-1]
    e30 = ema(df, 30).iloc[-1]
    e60 = ema(df, 60).iloc[-1]
    e120 = ema(df, 120).iloc[-1]

    if (
        e10 > e30
        and e30 > e60
        and e60 > e120
    ):
        return "long"

    if (
        e10 < e30
        and e30 < e60
        and e60 < e120
    ):
        return "short"

    return "none"


# =========================================================
# EMA ALIGNMENT COUNT
# =========================================================

def ema_alignment_count(df):

    if df.empty or len(df) < 120:
        return "none", 0

    d = df.copy()

    d["ema10"] = ema(d, 10)
    d["ema30"] = ema(d, 30)
    d["ema60"] = ema(d, 60)
    d["ema120"] = ema(d, 120)

    states = []

    for _, row in d.iterrows():

        if (
            row["ema10"] > row["ema30"]
            and row["ema30"] > row["ema60"]
            and row["ema60"] > row["ema120"]
        ):
            states.append("long")

        elif (
            row["ema10"] < row["ema30"]
            and row["ema30"] < row["ema60"]
            and row["ema60"] < row["ema120"]
        ):
            states.append("short")

        else:
            states.append("none")

    current = states[-1]

    if current == "none":
        return "none", 0

    count = 0

    for state in reversed(states):

        if state != current:
            break

        count += 1

    return current, count


def ema_display(df):

    d, count = ema_alignment_count(df)

    if d == "long":
        return f"🟢({count})"

    if d == "short":
        return f"🔴({count})"

    return "⚪(0)"


# =========================================================
# EMA10 CLOSE STATE
# =========================================================

def ema10_cross_count(df):

    if df.empty:
        return {
            "state": "none",
            "count": 0,
            "final_count": 0,
            "display": "⚪(0)"
        }

    d = df.copy()

    d["ema10"] = ema(d, 10)

    states = []

    for _, row in d.iterrows():

        if row["close"] > row["ema10"]:
            states.append("long")

        elif row["close"] < row["ema10"]:
            states.append("short")

        else:
            states.append("equal")

    current = states[-1]

    if current == "equal":

        return {
            "state": "equal",
            "count": 0,
            "final_count": 0,
            "display": "⚪(0)"
        }

    count = 0

    for state in reversed(states):

        if state != current:
            break

        count += 1

    final_count = 0

    for state in reversed(
        states[:-count]
    ):

        if state == current or state == "equal":
            break

        final_count += 1

    if current == "long":

        display = f"🟢({count})"

        if final_count > 0:
            display += f" <span class='prev-count'>({final_count})</span>"

    else:

        display = f"🔻({count})"

        if final_count > 0:
            display += f" <span class='prev-count'>({final_count})</span>"

    return {
        "state": current,
        "count": count,
        "final_count": final_count,
        "display": display
    }


# =========================================================
# 비행기 직전 경고
#
# LONG
# ---------------------------------------------------------
# 1H 정배열
# 4H 정배열
# 이전 EMA10 위 종가 진행
# 현재 EMA10 아래 마감
# 현재 고가가 EMA10 터치
# 현재 양봉
#
# SHORT
# ---------------------------------------------------------
# 1H 역배열
# 4H 역배열
# 이전 EMA10 아래 종가 진행
# 현재 EMA10 위 마감
# 현재 저가가 EMA10 터치
# 현재 음봉
# =========================================================

def get_air_warning(df1h, df4h):

    if (
        df1h.empty
        or df4h.empty
        or len(df1h) < 120
        or len(df4h) < 120
    ):
        return None

    # -----------------------------------------------------
    # 반드시 1H + 4H 전체 EMA 배열 확인
    # -----------------------------------------------------

    dir1h = direction(df1h)
    dir4h = direction(df4h)

    e10 = ema(df1h, 10)

    df = df1h.copy()

    df["ema10"] = e10

    row = df.iloc[-1]

    cross = ema10_cross_count(df)

    # =====================================================
    # LONG PRE BREAKOUT
    # =====================================================

    if (
        dir1h == "long"
        and dir4h == "long"
        and cross["state"] == "short"
        and cross["final_count"] >= MIN_EMA10_LONG_COUNT
        and row["close"] < row["ema10"]
        and row["high"] >= row["ema10"]
        and row["close"] > row["open"]
    ):

        return "LONG_PRE_BREAKOUT"

    # =====================================================
    # SHORT PRE BREAKOUT
    # =====================================================

    if (
        dir1h == "short"
        and dir4h == "short"
        and cross["state"] == "long"
        and cross["final_count"] >= MIN_EMA10_LONG_COUNT
        and row["close"] > row["ema10"]
        and row["low"] <= row["ema10"]
        and row["close"] < row["open"]
    ):

        return "SHORT_PRE_BREAKOUT"

    return None


# =========================================================
# 비행기 카운터
#
# 중요:
# 이미 active 상태라면 new_warning이 없어도
# 계속 경고 상태를 유지한다.
# =========================================================

def update_air_counter(
    market,
    df1h,
    new_warning
):

    if df1h.empty:
        return {
            "active": False,
            "direction": None,
            "count": 0
        }

    row = df1h.iloc[-1]

    candle_time = row["time"]

    with air_state_lock:

        state = air_state.get(
            market
        )

        # =================================================
        # 새 비행기 발생
        # =================================================

        if new_warning is not None:

            direction_name = (
                "long"
                if new_warning == "LONG_PRE_BREAKOUT"
                else "short"
            )

            # 같은 캔들에서 중복 발생 방지
            if (
                state is None
                or not state.get("active", False)
                or state.get("last_candle_time") != candle_time
                or state.get("direction") != direction_name
            ):

                air_state[market] = {
                    "active": True,
                    "direction": direction_name,
                    "count": 0,
                    "warning_time": str(candle_time),
                    "last_candle_time": candle_time
                }

                return air_state[market].copy()

        # =================================================
        # 기존 비행기 카운팅 중
        # =================================================

        state = air_state.get(
            market
        )

        if not state or not state.get("active", False):

            return {
                "active": False,
                "direction": None,
                "count": 0
            }

        # 같은 캔들에서는 다시 카운트하지 않음
        if state.get("last_candle_time") == candle_time:

            return state.copy()

        direction_name = state.get(
            "direction"
        )

        # =================================================
        # LONG 카운팅
        # =================================================

        if direction_name == "long":

            # 양봉이면 카운트 증가
            if row["close"] > row["open"]:

                state["count"] += 1
                state["last_candle_time"] = candle_time

                return state.copy()

            # 음봉이면 종료
            else:

                state["active"] = False
                state["last_candle_time"] = candle_time

                return state.copy()

        # =================================================
        # SHORT 카운팅
        # =================================================

        if direction_name == "short":

            # 음봉이면 카운트 증가
            if row["close"] < row["open"]:

                state["count"] += 1
                state["last_candle_time"] = candle_time

                return state.copy()

            # 양봉이면 종료
            else:

                state["active"] = False
                state["last_candle_time"] = candle_time

                return state.copy()

        return state.copy()


# =========================================================
# DAILY CHANGE
# =========================================================

def get_upbit_daily_change(market):

    url = "https://api.upbit.com/v1/candles/days"

    r = retry(
        requests.get,
        url,
        params={
            "market": market,
            "count": 2
        },
        timeout=10
    )

    if r is None:
        return 0.0

    try:

        data = r.json()

        if len(data) < 2:
            return 0.0

        # 현재 진행 중인 일봉 제외
        yesterday = data[1]

        opening = float(
            yesterday["opening_price"]
        )

        trade = float(
            yesterday["trade_price"]
        )

        if opening == 0:
            return 0.0

        return (
            (trade - opening)
            / opening
            * 100
        )

    except Exception:

        return 0.0


def get_okx_daily_change(df1h):

    if df1h.empty:
        return 0.0

    d = df1h.copy()

    d["time"] = pd.to_datetime(
        d["time"]
    )

    d = d.set_index("time")

    daily = d.resample(
        "1D",
        offset="9h"
    ).agg({
        "open": "first",
        "close": "last"
    }).dropna()

    if len(daily) < 2:
        return 0.0

    row = daily.iloc[-1]

    if row["open"] == 0:
        return 0.0

    return (
        (row["close"] - row["open"])
        / row["open"]
        * 100
    )


# =========================================================
# FORMAT
# =========================================================

def format_change(v):

    try:

        v = float(v)

        if v > 0:
            return f"+{v:.2f}%"

        return f"{v:.2f}%"

    except Exception:

        return "-"


def format_volume(v):

    try:

        v = float(v)

        if v >= 1000000000000:
            return f"{v / 1000000000000:.2f}조"

        if v >= 100000000:
            return f"{v / 100000000:.1f}억"

        if v >= 10000:
            return f"{v / 10000:.0f}만"

        return f"{v:,.0f}"

    except Exception:

        return "-"


# =========================================================
# EMPTY ANALYSIS
# =========================================================

def empty_analysis():

    return {
        "ema1h": "⚪(0)",
        "ema4h": "⚪(0)",

        "ema10": "⚪(0)",

        "change": 0.0,

        "air_warning": None,
        "air_active": False,
        "air_direction": None,
        "air_count": 0,

        "qualified": False,

        "direction1h": "none",
        "direction4h": "none",

        "df1h": pd.DataFrame()
    }


# =========================================================
# ANALYZE
# =========================================================

def analyze(
    market,
    okx=False
):

    try:

        # -------------------------------------------------
        # 1H
        # -------------------------------------------------

        if okx:

            df1h = history_okx(
                market,
                "1H",
                required=200
            )

        else:

            df1h = history_upbit(
                market,
                60,
                required=200
            )

        if df1h.empty:
            return empty_analysis()

        # -------------------------------------------------
        # 4H
        # -------------------------------------------------

        if okx:

            df4h = history_okx(
                market,
                "4H",
                required=200
            )

        else:

            df4h = history_upbit(
                market,
                240,
                required=200
            )

        if df4h.empty:
            return empty_analysis()

        # -------------------------------------------------
        # EMA
        # -------------------------------------------------

        direction1h = direction(df1h)
        direction4h = direction(df4h)

        e1 = ema_display(df1h)
        e4 = ema_display(df4h)

        ema10_info = ema10_cross_count(
            df1h
        )

        # -------------------------------------------------
        # 비행기 신규 경고
        # -------------------------------------------------

        new_warning = get_air_warning(
            df1h,
            df4h
        )

        # -------------------------------------------------
        # 비행기 상태 업데이트
        #
        # new_warning이 없어도 기존 active 상태 유지
        # -------------------------------------------------

        air = update_air_counter(
            market,
            df1h,
            new_warning
        )

        air_active = air.get(
            "active",
            False
        )

        air_direction = air.get(
            "direction"
        )

        air_count = air.get(
            "count",
            0
        )

        # -------------------------------------------------
        # 일봉 변화율
        # -------------------------------------------------

        if okx:

            change = get_okx_daily_change(
                df1h
            )

        else:

            change = get_upbit_daily_change(
                market
            )

        # -------------------------------------------------
        # qualified
        #
        # 비행기 신규 발생 또는
        # 기존 비행기 카운팅 중이면 True
        # -------------------------------------------------

        qualified = air_active

        return {
            "ema1h": e1,
            "ema4h": e4,

            "ema10": ema10_info["display"],

            "change": change,

            "air_warning": new_warning,
            "air_active": air_active,
            "air_direction": air_direction,
            "air_count": air_count,

            "qualified": qualified,

            "direction1h": direction1h,
            "direction4h": direction4h,

            "df1h": df1h
        }

    except Exception as e:

        logging.error(
            "analyze error %s: %s",
            market,
            e
        )

        return empty_analysis()


# =========================================================
# MAKE ROW
# =========================================================

def make_row(
    rank,
    market,
    volume,
    analysis,
    okx=False
):

    return {
        "rank": rank,
        "market": market,

        "volume": volume,

        "change": analysis["change"],

        "ema1h": analysis["ema1h"],
        "ema4h": analysis["ema4h"],

        "ema10": analysis["ema10"],

        "air_warning": analysis["air_warning"],
        "air_active": analysis["air_active"],
        "air_direction": analysis["air_direction"],
        "air_count": analysis["air_count"],

        "qualified": analysis["qualified"],

        "direction1h": analysis["direction1h"],
        "direction4h": analysis["direction4h"],

        "okx": okx
    }


# =========================================================
# UPBIT UPDATE
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update_time
    global latest_upbit_markets

    markets = get_upbit_markets()

    if not markets:

        logging.warning(
            "Upbit market data 없음"
        )

        return

    latest_upbit_markets = {
        x["market"]
        for x in markets
    }

    markets = sorted(
        markets,
        key=lambda x: x["trade_value"],
        reverse=True
    )

    top = markets[:TOP_N]

    rows = []

    for rank, item in enumerate(
        top,
        start=1
    ):

        market = item["market"]

        logging.info(
            "Upbit analyze %s",
            market
        )

        analysis = analyze(
            market,
            okx=False
        )

        rows.append(
            make_row(
                rank,
                market,
                item["trade_value"],
                analysis,
                okx=False
            )
        )

    latest_upbit_data = rows

    latest_upbit_update_time = kst()


# =========================================================
# OKX SYMBOLS
# =========================================================

def get_okx_symbols():

    url = "https://www.okx.com/api/v5/public/instruments"

    r = retry(
        requests.get,
        url,
        params={
            "instType": "SWAP"
        },
        timeout=10
    )

    if r is None:
        return []

    try:

        js = r.json()

        if js.get("code") != "0":
            return []

        result = []

        for x in js.get("data", []):

            inst = x.get("instId", "")

            if not inst.endswith("-USDT-SWAP"):
                continue

            result.append(inst)

        return result

    except Exception:

        return []


# =========================================================
# OKX VOLUME
# =========================================================

def get_okx_volume(
    inst,
    usdt_krw
):

    df = history_okx(
        inst,
        "1H",
        required=VOLUME_HOURS
    )

    if df.empty:
        return 0.0

    df = df.tail(
        VOLUME_HOURS
    )

    volume_usdt = (
        df["volume"].astype(float).sum()
    )

    return (
        volume_usdt
        * usdt_krw
    )


# =========================================================
# OKX UPDATE
# =========================================================

def update_okx():

    global latest_okx_data
    global latest_okx_update_time

    usdt_krw = get_usdt_krw()

    if usdt_krw <= 0:

        logging.warning(
            "USDT/KRW 없음"
        )

        return

    symbols = get_okx_symbols()

    if not symbols:
        return

    volumes = []

    for inst in symbols:

        try:

            volume = get_okx_volume(
                inst,
                usdt_krw
            )

            if volume <= 0:
                continue

            volumes.append({
                "inst": inst,
                "volume": volume
            })

        except Exception as e:

            logging.error(
                "OKX volume error %s: %s",
                inst,
                e
            )

    volumes.sort(
        key=lambda x: x["volume"],
        reverse=True
    )

    top = volumes[:TOP_N]

    rows = []

    for rank, item in enumerate(
        top,
        start=1
    ):

        inst = item["inst"]

        base = inst.replace(
            "-USDT-SWAP",
            ""
        )

        # -------------------------------------------------
        # Upbit 상장 여부
        # -------------------------------------------------

        upbit_market = (
            f"KRW-{base}"
        )

        display_market = base

        if upbit_market in latest_upbit_markets:

            display_market += " (업비트)"

        analysis = analyze(
            inst,
            okx=True
        )

        row = make_row(
            rank,
            display_market,
            item["volume"],
            analysis,
            okx=True
        )

        row["raw_market"] = inst

        rows.append(row)

    latest_okx_data = rows

    latest_okx_update_time = kst()


# =========================================================
# DASHBOARD UPDATE
# =========================================================

def update_dashboard():

    if not update_lock.acquire(
        blocking=False
    ):
        logging.info(
            "이전 업데이트 진행 중"
        )

        return

    try:

        logging.info(
            "========== DASHBOARD UPDATE =========="
        )

        if USE_UPBIT == "Y":

            update_upbit()

        if USE_OKX == "Y":

            update_okx()

        logging.info(
            "========== UPDATE COMPLETE =========="
        )

    except Exception as e:

        logging.error(
            "update_dashboard error: %s",
            e
        )

    finally:

        update_lock.release()


# =========================================================
# AIR HTML
# =========================================================

def air_html(row):

    if not row.get("air_active"):

        return ""

    direction_name = row.get(
        "air_direction"
    )

    count = row.get(
        "air_count",
        0
    )

    if direction_name == "long":

        cls = "long"

    else:

        cls = "short"

    return (
        f"<div class='air-warning {cls}'>"
        f"<span>직전</span> "
        f"<span class='air-icon'>✈️</span>"
        f"<span class='air-count'>({count})</span>"
        f"</div>"
    )


# =========================================================
# EMA HTML
# =========================================================

def ema_html(value):

    return (
        f"<span class='ema-cell'>{value}</span>"
    )


# =========================================================
# 10선 HTML
# =========================================================

def ema10_html(value):

    return (
        f"<span class='ema10-cell'>{value}</span>"
    )


# =========================================================
# TABLE
# =========================================================

def table_html(data):

    if not data:

        return (
            "<div class='empty'>"
            "표시할 데이터가 없습니다."
            "</div>"
        )

    html = """
    <div class="table-wrap">
    <table>
        <thead>
            <tr>
                <th>순위</th>
                <th>종목</th>
                <th>등락</th>
                <th>거래대금</th>
                <th>1H EMA</th>
                <th>4H EMA</th>
                <th>10선</th>
                <th>비행기</th>
            </tr>
        </thead>
        <tbody>
    """

    for row in data:

        change = row.get(
            "change",
            0
        )

        change_class = ""

        if change > 0:
            change_class = "up"

        elif change < 0:
            change_class = "down"

        html += f"""
        <tr>
            <td>{row.get("rank", "-")}</td>

            <td class="coin">
                {row.get("market", "-")}
            </td>

            <td class="{change_class}">
                {format_change(change)}
            </td>

            <td>
                {format_volume(row.get("volume", 0))}
            </td>

            <td>
                {ema_html(row.get("ema1h", "⚪(0)"))}
            </td>

            <td>
                {ema_html(row.get("ema4h", "⚪(0)"))}
            </td>

            <td>
                {ema10_html(row.get("ema10", "⚪(0)"))}
            </td>

            <td>
                {air_html(row)}
            </td>
        </tr>
        """

    html += """
        </tbody>
    </table>
    </div>
    """

    return html


# =========================================================
# WARNING ONLY
# =========================================================

def warning_data(data):

    result = []

    for row in data:

        # -------------------------------------------------
        # 중요:
        # 신규 경고뿐 아니라
        # 기존 비행기 카운팅 중인 종목도 포함
        # -------------------------------------------------

        if row.get("air_active"):

            result.append(row)

    return result


# =========================================================
# FOCUS SECTION
# =========================================================

def focus_section(data):

    focus_data = warning_data(data)

    if not focus_data:

        return """
        <div class="focus-box">
            <div class="focus-title">
                ✈️ 비행기 경고
            </div>
            <div class="focus-empty">
                현재 경고 종목 없음
            </div>
        </div>
        """

    return f"""
    <div class="focus-box">
        <div class="focus-title">
            ✈️ 비행기 경고
            <span class="focus-count">
                {len(focus_data)}
            </span>
        </div>

        {table_html(focus_data)}
    </div>
    """


# =========================================================
# HTML
# =========================================================

HTML = """
<!DOCTYPE html>
<html lang="ko">

<head>

<meta charset="UTF-8">

<meta
    name="viewport"
    content="width=device-width, initial-scale=1.0"
>

<meta
    http-equiv="refresh"
    content="60"
>

<title>📊 매매 전술 눌림 돌파</title>

<style>

* {
    box-sizing: border-box;
}

body {
    margin: 0;
    padding: 10px;
    background: #111;
    color: #eee;
    font-family:
        Arial,
        "Noto Sans KR",
        sans-serif;
    font-size: 13px;
}

.container {
    width: 100%;
    max-width: 1200px;
    margin: 0 auto;
}

h1 {
    font-size: 18px;
    margin: 4px 0 10px;
}

.info {
    background: #181818;
    border: 1px solid #333;
    border-radius: 8px;
    padding: 9px;
    margin-bottom: 10px;
    line-height: 1.6;
    color: #bbb;
}

.info strong {
    color: #fff;
}

.section {
    margin-top: 12px;
}

.section-title {
    font-size: 15px;
    font-weight: bold;
    padding: 8px 0;
}

.focus-box {
    background: #161616;
    border: 1px solid #555;
    border-radius: 9px;
    padding: 8px;
    margin-bottom: 12px;
}

.focus-title {
    font-size: 15px;
    font-weight: bold;
    margin-bottom: 7px;
}

.focus-count {
    margin-left: 5px;
    color: #fff;
}

.focus-empty {
    color: #888;
    padding: 8px 2px;
}

.table-wrap {
    width: 100%;
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
}

table {
    width: 100%;
    border-collapse: collapse;
    min-width: 700px;
}

th {
    background: #222;
    color: #aaa;
    font-weight: normal;
    padding: 7px 5px;
    border-bottom: 1px solid #444;
    white-space: nowrap;
}

td {
    padding: 7px 5px;
    text-align: center;
    border-bottom: 1px solid #282828;
    white-space: nowrap;
}

.coin {
    text-align: left;
    font-weight: bold;
}

.up {
    color: #00d084;
}

.down {
    color: #ff5252;
}

.ema-cell {
    display: inline-block;
    min-width: 48px;
}

.ema10-cell {
    display: inline-block;
    min-width: 70px;
}

.prev-count {
    color: #fff;
    margin-left: 2px;
}

.air-warning {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 2px;
    font-weight: bold;
    min-width: 70px;
}

.air-warning.long {
    color: #00d084;
}

.air-warning.short {
    color: #ff5252;
}

.air-warning span:first-child {
    color: #aaa;
    font-size: 11px;
}

.air-icon {
    font-size: 17px;
}

.air-count {
    color: #fff;
    font-size: 12px;
}

.empty {
    color: #777;
    padding: 20px;
    text-align: center;
}

.update-time {
    color: #888;
    font-size: 11px;
    margin-top: 8px;
}

</style>

</head>

<body>

<div class="container">

<h1>📊 매매 전술 눌림 돌파</h1>

<div class="info">

<div>
<strong>EMA 방향:</strong>
1H + 4H EMA 10-30-60-120
정배열 / 역배열이 같은 방향이어야 함
</div>

<div>
<strong>비행기:</strong>
돌파 완료가 아니라
돌파 직전 캔들 포착
</div>

<div>
<strong>카운팅:</strong>
비행기 발생 후 진행 캔들마다 카운트
</div>

<div>
<strong>LONG:</strong>
1H·4H 정배열 + 직전 EMA10 조건
</div>

<div>
<strong>SHORT:</strong>
1H·4H 역배열 + 직전 EMA10 조건
</div>

<div>
<strong>비행기 카운팅 중:</strong>
✈️(1) → ✈️(2) → ✈️(3)
경고 목록 계속 유지
</div>

</div>

{focus}

<div class="section">

<div class="section-title">
🏆 OKX / UPBIT
</div>

{table}

</div>

<div class="update-time">
UPBIT 업데이트: {upbit_time}
</div>

<div class="update-time">
OKX 업데이트: {okx_time}
</div>

</div>

</body>
</html>
"""


# =========================================================
# HOME
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def home():

    # -----------------------------------------------------
    # Upbit 경고
    # -----------------------------------------------------

    upbit_focus = focus_section(
        latest_upbit_data
    )

    # -----------------------------------------------------
    # 현재 사용하는 데이터
    # -----------------------------------------------------

    if USE_UPBIT == "Y":

        data = latest_upbit_data

    elif USE_OKX == "Y":

        data = latest_okx_data

    else:

        data = []

    return HTML.format(
        focus=upbit_focus,
        table=table_html(data),
        upbit_time=latest_upbit_update_time,
        okx_time=latest_okx_update_time
    )


# =========================================================
# SCHEDULER
# =========================================================

def scheduler_loop():

    while True:

        try:

            schedule.run_pending()

        except Exception as e:

            logging.error(
                "scheduler error: %s",
                e
            )

        time.sleep(1)


# =========================================================
# STARTUP
# =========================================================

@app.on_event("startup")
def startup():

    logging.info(
        "========================================"
    )

    logging.info(
        "📊 매매 전술 눌림 돌파 START"
    )

    logging.info(
        "VOLUME_HOURS = %s",
        VOLUME_HOURS
    )

    logging.info(
        "TOP_N = %s",
        TOP_N
    )

    logging.info(
        "UPDATE_MINUTES = %s",
        UPDATE_MINUTES
    )

    logging.info(
        "EMA 조건 = 1H + 4H 정배열/역배열 필수"
    )

    logging.info(
        "✈️ 카운팅 중 종목 = 경고 목록 계속 표시"
    )

    logging.info(
        "========================================"
    )

    # 최초 업데이트
    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    # 스케줄 등록
    schedule.every(
        UPDATE_MINUTES
    ).minutes.do(
        update_dashboard
    )

    threading.Thread(
        target=scheduler_loop,
        daemon=True
    ).start()


# =========================================================
# RUN
# =========================================================

if __name__ == "__main__":

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
                )
