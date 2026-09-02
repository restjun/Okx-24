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
    format="%(asctime)s | %(levelname)s | %(message)s"
)

VOLUME_HOURS = 24
TOP_N = 30
UPDATE_MINUTES = 1

INITIAL_CANDLE_COUNT = 200
HISTORY_CHUNK = 200
MAX_HISTORY_CHUNKS = 10

USE_UPBIT = "Y"
USE_OKX = "N"

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10

KST = ZoneInfo("Asia/Seoul")


# =========================================================
# 전역 변수
# =========================================================

latest_upbit_data = []
latest_okx_data = []

latest_usdt_krw = 0

latest_upbit_update_time = "-"
latest_okx_update_time = "-"

latest_upbit_markets = []

request_lock = threading.Lock()
update_lock = threading.Lock()
air_state_lock = threading.Lock()

last_request_time = 0

air_state = {}


# =========================================================
# 시간
# =========================================================

def kst():
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


# =========================================================
# 요청 제한
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

    for i in range(MAX_RETRIES):

        try:
            wait_request()

            r = func(*args, **kwargs)

            if hasattr(r, "status_code"):

                if r.status_code == 429:
                    logging.warning("429 rate limit")
                    time.sleep(RATE_LIMIT_WAIT)
                    continue

                if r.status_code >= 500:
                    time.sleep(1)
                    continue

            return r

        except Exception as e:

            logging.warning(
                f"request retry {i + 1}/{MAX_RETRIES}: {e}"
            )

            time.sleep(1)

    return None


# =========================================================
# UPBIT
# =========================================================

def get_upbit_markets():

    url = "https://api.upbit.com/v1/ticker/all"

    try:

        r = retry(
            requests.get,
            url,
            params={"quote_currencies": "KRW"},
            timeout=10
        )

        if r is None:
            return []

        data = r.json()

        result = []

        for x in data:

            market = x.get("market", "")

            if not market.startswith("KRW-"):
                continue

            result.append({
                "market": market,
                "trade_price": float(x.get("trade_price", 0)),
                "acc_trade_price_24h": float(
                    x.get("acc_trade_price_24h", 0)
                )
            })

        return result

    except Exception as e:

        logging.error(f"get_upbit_markets: {e}")
        return []


def get_usdt_krw():

    try:

        url = "https://api.upbit.com/v1/ticker"

        r = retry(
            requests.get,
            url,
            params={"markets": "KRW-USDT"},
            timeout=10
        )

        if r is None:
            return 0

        data = r.json()

        if not data:
            return 0

        return float(data[0]["trade_price"])

    except Exception as e:

        logging.error(f"get_usdt_krw: {e}")
        return 0


# =========================================================
# UPBIT 1H
# =========================================================

def get_upbit_1h(market, count=200, to=None):

    url = "https://api.upbit.com/v1/candles/minutes/60"

    params = {
        "market": market,
        "count": count
    }

    if to:
        params["to"] = to

    try:

        r = retry(
            requests.get,
            url,
            params=params,
            timeout=10
        )

        if r is None:
            return pd.DataFrame()

        data = r.json()

        if not isinstance(data, list):
            return pd.DataFrame()

        rows = []

        now = datetime.now(KST)

        for x in data:

            dt = pd.to_datetime(
                x["candle_date_time_kst"]
            )

            # 현재 진행 중인 1시간봉 제외
            if (
                dt.year == now.year
                and dt.month == now.month
                and dt.day == now.day
                and dt.hour == now.hour
            ):
                continue

            rows.append({
                "time": dt,
                "o": float(x["opening_price"]),
                "h": float(x["high_price"]),
                "l": float(x["low_price"]),
                "c": float(x["trade_price"]),
                "volume_krw": float(
                    x["candle_acc_trade_price"]
                )
            })

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)

        df = df.sort_values("time").reset_index(drop=True)

        return df

    except Exception as e:

        logging.error(
            f"get_upbit_1h {market}: {e}"
        )

        return pd.DataFrame()


# =========================================================
# UPBIT 4H
# =========================================================

def get_upbit_4h(market, count=200, to=None):

    url = "https://api.upbit.com/v1/candles/minutes/240"

    params = {
        "market": market,
        "count": count
    }

    if to:
        params["to"] = to

    try:

        r = retry(
            requests.get,
            url,
            params=params,
            timeout=10
        )

        if r is None:
            return pd.DataFrame()

        data = r.json()

        if not isinstance(data, list):
            return pd.DataFrame()

        rows = []

        now = datetime.now(KST)

        current_block = (now.hour // 4) * 4

        for x in data:

            dt = pd.to_datetime(
                x["candle_date_time_kst"]
            )

            # 현재 진행 중인 4시간봉 제외
            if (
                dt.year == now.year
                and dt.month == now.month
                and dt.day == now.day
                and dt.hour == current_block
            ):
                continue

            rows.append({
                "time": dt,
                "o": float(x["opening_price"]),
                "h": float(x["high_price"]),
                "l": float(x["low_price"]),
                "c": float(x["trade_price"]),
                "volume_krw": float(
                    x["candle_acc_trade_price"]
                )
            })

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)

        df = df.sort_values("time").reset_index(drop=True)

        return df

    except Exception as e:

        logging.error(
            f"get_upbit_4h {market}: {e}"
        )

        return pd.DataFrame()


# =========================================================
# UPBIT HISTORY
# =========================================================

def history_upbit(market, unit, required=125):

    result = []

    to = None

    for _ in range(MAX_HISTORY_CHUNKS):

        if unit == 60:

            df = get_upbit_1h(
                market,
                HISTORY_CHUNK,
                to
            )

        else:

            df = get_upbit_4h(
                market,
                HISTORY_CHUNK,
                to
            )

        if df.empty:
            break

        result.append(df)

        if len(pd.concat(result)) >= required:
            break

        oldest = df["time"].min()

        to = oldest.strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        time.sleep(0.05)

    if not result:
        return pd.DataFrame()

    df = pd.concat(
        result,
        ignore_index=True
    )

    df = df.drop_duplicates(
        subset=["time"]
    )

    df = df.sort_values("time")

    return df.tail(required).reset_index(drop=True)


def history_upbit_4h(market):

    return history_upbit(
        market,
        240,
        125
    )


# =========================================================
# OKX
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

    if before:
        params["before"] = before

    try:

        r = retry(
            requests.get,
            url,
            params=params,
            timeout=10
        )

        if r is None:
            return pd.DataFrame()

        js = r.json()

        data = js.get("data", [])

        if not data:
            return pd.DataFrame()

        rows = []

        now = datetime.now(KST).replace(
            tzinfo=None
        )

        for x in data:

            if len(x) < 9:
                continue

            ts = int(x[0])

            dt = datetime.fromtimestamp(
                ts / 1000,
                tz=KST
            ).replace(tzinfo=None)

            confirm = str(x[8])

            if confirm != "1":
                continue

            if bar == "1H":

                if (
                    dt.year == now.year
                    and dt.month == now.month
                    and dt.day == now.day
                    and dt.hour == now.hour
                ):
                    continue

            elif bar == "4H":

                block = (now.hour // 4) * 4

                if (
                    dt.year == now.year
                    and dt.month == now.month
                    and dt.day == now.day
                    and dt.hour == block
                ):
                    continue

            rows.append({
                "time": dt,
                "o": float(x[1]),
                "h": float(x[2]),
                "l": float(x[3]),
                "c": float(x[4]),
                "volume_krw": float(x[6])
            })

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)

        df = df.sort_values("time")

        return df.reset_index(drop=True)

    except Exception as e:

        logging.error(
            f"get_okx_ohlcv {inst}: {e}"
        )

        return pd.DataFrame()


def history_okx(inst, bar, required=125):

    result = []

    before = None

    for _ in range(MAX_HISTORY_CHUNKS):

        df = get_okx_ohlcv(
            inst,
            bar,
            HISTORY_CHUNK,
            before
        )

        if df.empty:
            break

        result.append(df)

        all_df = pd.concat(
            result,
            ignore_index=True
        )

        if len(all_df) >= required:
            break

        oldest = df["time"].min()

        before = str(
            int(
                oldest.replace(
                    tzinfo=KST
                ).timestamp() * 1000
            )
        )

        time.sleep(0.05)

    if not result:
        return pd.DataFrame()

    df = pd.concat(
        result,
        ignore_index=True
    )

    df = df.drop_duplicates(
        subset=["time"]
    )

    df = df.sort_values("time")

    return df.tail(required).reset_index(drop=True)


def get_okx_symbols():

    url = "https://www.okx.com/api/v5/public/instruments"

    try:

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

        js = r.json()

        result = []

        for x in js.get("data", []):

            inst = x.get("instId", "")

            if inst.endswith("-USDT-SWAP"):
                result.append(inst)

        return result

    except Exception as e:

        logging.error(f"get_okx_symbols: {e}")
        return []


def get_okx_volume(inst, usdt):

    try:

        df = get_okx_ohlcv(
            inst,
            "1H",
            VOLUME_HOURS + 5
        )

        if df.empty:
            return 0

        return float(
            df["volume_krw"].tail(
                VOLUME_HOURS
            ).sum()
        ) * usdt

    except Exception:
        return 0


# =========================================================
# EMA
# =========================================================

def ema(df, period):

    return pd.to_numeric(
        df["c"],
        errors="coerce"
    ).ewm(
        span=period,
        adjust=False,
        min_periods=1
    ).mean()


def direction(df):

    if df is None or df.empty:
        return "none"

    e10 = ema(df, 10).iloc[-1]
    e30 = ema(df, 30).iloc[-1]
    e60 = ema(df, 60).iloc[-1]
    e120 = ema(df, 120).iloc[-1]

    if e10 > e30 > e60 > e120:
        return "long"

    if e10 < e30 < e60 < e120:
        return "short"

    return "none"


def ema_alignment_count(df):

    if df is None or len(df) < 2:
        return {
            "direction": "none",
            "count": 0
        }

    e10 = ema(df, 10)
    e30 = ema(df, 30)
    e60 = ema(df, 60)
    e120 = ema(df, 120)

    states = []

    for i in range(len(df)):

        if (
            e10.iloc[i]
            > e30.iloc[i]
            > e60.iloc[i]
            > e120.iloc[i]
        ):
            states.append("long")

        elif (
            e10.iloc[i]
            < e30.iloc[i]
            < e60.iloc[i]
            < e120.iloc[i]
        ):
            states.append("short")

        else:
            states.append("none")

    current = states[-1]

    if current == "none":
        return {
            "direction": "none",
            "count": 0
        }

    count = 0

    for s in reversed(states):

        if s != current:
            break

        count += 1

    return {
        "direction": current,
        "count": count
    }


def ema_display(df):

    result = ema_alignment_count(df)

    if result["direction"] == "long":
        return f"🟢({result['count']})"

    if result["direction"] == "short":
        return f"🔴({result['count']})"

    return "⚪(0)"


# =========================================================
# 1H 종가 ↔ EMA10
#
# 상승 = 녹색 ▲
# 하락 = 빨간 ▼
#
# 화면은 항상 2줄
# =========================================================

def ema10_cross_count(df):

    if df is None or df.empty:

        return {
            "current": "none",
            "current_count": 0,
            "previous": "none",
            "previous_count": 0
        }

    e10 = ema(df, 10)

    states = []

    for i in range(len(df)):

        close = float(df["c"].iloc[i])
        ema10_value = float(e10.iloc[i])

        if close > ema10_value:
            states.append("long")

        elif close < ema10_value:
            states.append("short")

        else:
            states.append("equal")

    current = states[-1]

    if current == "equal":

        return {
            "current": "none",
            "current_count": 0,
            "previous": "none",
            "previous_count": 0
        }

    current_count = 0

    for s in reversed(states):

        if s != current:
            break

        current_count += 1

    previous = "short" if current == "long" else "long"

    previous_count = 0

    # 현재 구간 바로 앞의 반대 구간 최종 카운트
    for i in range(len(states) - current_count - 1, -1, -1):

        if states[i] != previous:
            break

        previous_count += 1

    return {
        "current": current,
        "current_count": current_count,
        "previous": previous,
        "previous_count": previous_count
    }


# =========================================================
# 비행기 경고
# =========================================================

def get_air_warning(df1h, df4h):

    if (
        df1h is None
        or df4h is None
        or df1h.empty
        or df4h.empty
    ):
        return None

    if direction(df1h) != "long":
        return None

    if direction(df4h) != "long":
        return None

    e10 = ema(df1h, 10)

    if len(df1h) < 2:
        return None

    prev_close = float(df1h["c"].iloc[-2])
    prev_ema10 = float(e10.iloc[-2])

    current_close = float(df1h["c"].iloc[-1])
    current_open = float(df1h["o"].iloc[-1])
    current_ema10 = float(e10.iloc[-1])

    # 직전 종가가 EMA10 아래
    if prev_close >= prev_ema10:
        return None

    # 현재 양봉
    if current_close <= current_open:
        return None

    # 현재 종가가 EMA10 위로 확정
    if current_close <= current_ema10:
        return None

    return "LONG"


# =========================================================
# 비행기 카운터
# =========================================================

def update_air_counter(
    market,
    df1h,
    new_warning
):

    if df1h is None or df1h.empty:
        return {
            "active": False,
            "count": 0
        }

    candle_time = str(
        df1h["time"].iloc[-1]
    )

    with air_state_lock:

        state = air_state.get(
            market,
            {
                "active": False,
                "count": 0,
                "last_candle": None
            }
        )

        if new_warning:

            if state["last_candle"] != candle_time:

                if state["active"]:

                    current_open = float(
                        df1h["o"].iloc[-1]
                    )

                    current_close = float(
                        df1h["c"].iloc[-1]
                    )

                    if current_close > current_open:

                        state["count"] += 1

                    else:

                        state["active"] = False
                        state["count"] = 0

                else:

                    state["active"] = True
                    state["count"] = 0

                state["last_candle"] = candle_time

        air_state[market] = state

        return {
            "active": state["active"],
            "count": state["count"]
        }


# =========================================================
# 일간 등락률
# =========================================================

def get_upbit_daily_change(market):

    try:

        url = "https://api.upbit.com/v1/candles/days"

        r = retry(
            requests.get,
            url,
            params={
                "market": market,
                "count": 3
            },
            timeout=10
        )

        if r is None:
            return 0

        data = r.json()

        if len(data) < 2:
            return 0

        current = float(
            data[0]["trade_price"]
        )

        previous = float(
            data[1]["trade_price"]
        )

        if previous == 0:
            return 0

        return (
            current - previous
        ) / previous * 100

    except Exception:
        return 0


# =========================================================
# OKX 일간 등락률
# =========================================================

def get_okx_daily_change(inst):

    try:

        df = get_okx_ohlcv(
            inst,
            "1H",
            72
        )

        if df.empty:
            return 0

        temp = df.copy()

        temp["time"] = pd.to_datetime(
            temp["time"]
        )

        temp = temp.set_index("time")

        daily = temp["c"].resample(
            "1D",
            offset="9h"
        ).last().dropna()

        if len(daily) < 2:
            return 0

        current = float(daily.iloc[-1])
        previous = float(daily.iloc[-2])

        if previous == 0:
            return 0

        return (
            current - previous
        ) / previous * 100

    except Exception:
        return 0


# =========================================================
# 포맷
# =========================================================

def format_change(value):

    if value > 0:

        return (
            f'<span class="up">▲ '
            f'+{value:.2f}%</span>'
        )

    if value < 0:

        return (
            f'<span class="down">▼ '
            f'{value:.2f}%</span>'
        )

    return (
        '<span class="neutral">0.00%</span>'
    )


def format_volume(value):

    value = float(value)

    if value >= 1_0000_0000_0000:

        return f"{value / 1_0000_0000_0000:.1f}조"

    if value >= 1_0000_0000:

        return f"{value / 1_0000_0000:.0f}억"

    if value >= 1_0000:

        return f"{value / 1_0000:.0f}만"

    return f"{value:,.0f}"


# =========================================================
# 빈 분석
# =========================================================

def empty_analysis():

    return {
        "ema1h": "-",
        "ema4h": "-",
        "ema10": {
            "current": "none",
            "current_count": 0,
            "previous": "none",
            "previous_count": 0
        },
        "change": 0,
        "air": None,
        "air_count": 0,
        "qualified": False,
        "direction1h": "none",
        "direction4h": "none",
        "df1h": pd.DataFrame()
    }


# =========================================================
# 분석
# =========================================================

def analyze(market, okx=False):

    result = empty_analysis()

    if okx:

        inst = market

        df1h = history_okx(
            inst,
            "1H",
            125
        )

        df4h = history_okx(
            inst,
            "4H",
            125
        )

        change = get_okx_daily_change(inst)

    else:

        df1h = history_upbit(
            market,
            60,
            125
        )

        df4h = history_upbit_4h(
            market
        )

        change = get_upbit_daily_change(
            market
        )

    if df1h.empty:
        return result

    result["df1h"] = df1h

    result["direction1h"] = direction(
        df1h
    )

    result["direction4h"] = direction(
        df4h
    )

    result["ema1h"] = ema_display(
        df1h
    )

    result["ema4h"] = ema_display(
        df4h
    )

    result["ema10"] = ema10_cross_count(
        df1h
    )

    result["change"] = change

    warning = get_air_warning(
        df1h,
        df4h
    )

    air = update_air_counter(
        market,
        df1h,
        warning
    )

    result["air"] = warning
    result["air_count"] = air["count"]

    result["qualified"] = (
        result["direction1h"] == "long"
        and result["direction4h"] == "long"
    )

    return result


# =========================================================
# OKX 업데이트
# =========================================================

def update_okx(usdt):

    global latest_okx_data
    global latest_okx_update_time

    if not usdt:
        return

    symbols = get_okx_symbols()

    rows = []

    for inst in symbols:

        volume = get_okx_volume(
            inst,
            usdt
        )

        if volume <= 0:
            continue

        coin = inst.replace(
            "-USDT-SWAP",
            ""
        )

        rows.append({
            "market": inst,
            "coin": coin,
            "volume": volume / 10
        })

    rows.sort(
        key=lambda x: x["volume"],
        reverse=True
    )

    rows = rows[:TOP_N]

    result = []

    for rank, row in enumerate(
        rows,
        start=1
    ):

        analysis = analyze(
            row["market"],
            okx=True
        )

        row.update(analysis)

        row["rank"] = rank

        result.append(row)

    latest_okx_data = result

    latest_okx_update_time = kst()


# =========================================================
# UPBIT 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update_time
    global latest_upbit_markets
    global latest_usdt_krw

    markets = get_upbit_markets()

    latest_upbit_markets = [
        x["market"]
        for x in markets
    ]

    markets.sort(
        key=lambda x: x["acc_trade_price_24h"],
        reverse=True
    )

    markets = markets[:TOP_N]

    result = []

    for rank, item in enumerate(
        markets,
        start=1
    ):

        market = item["market"]

        analysis = analyze(
            market,
            okx=False
        )

        result.append({
            "rank": rank,
            "market": market,
            "coin": market.replace(
                "KRW-",
                ""
            ),
            "volume": item[
                "acc_trade_price_24h"
            ],
            **analysis
        })

    latest_upbit_data = result

    latest_upbit_update_time = kst()

    latest_usdt_krw = get_usdt_krw()


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    global latest_usdt_krw

    with update_lock:

        try:

            if USE_UPBIT == "Y":

                update_upbit()

            if USE_OKX == "Y":

                if not latest_usdt_krw:
                    latest_usdt_krw = get_usdt_krw()

                update_okx(
                    latest_usdt_krw
                )

            logging.info(
                "Dashboard update completed"
            )

        except Exception as e:

            logging.exception(
                f"Dashboard update error: {e}"
            )


# =========================================================
# HTML - EMA
# =========================================================

def ema_html(value):

    if "🟢" in value:

        return (
            f'<span class="ema-green">{value}</span>'
        )

    if "🔴" in value:

        return (
            f'<span class="ema-red">{value}</span>'
        )

    return (
        f'<span class="ema-gray">{value}</span>'
    )


# =========================================================
# HTML - 10선
#
# 상승 : 녹색 ▲
# 하락 : 빨간 ▼
# 이전 최종값 : 회색
#
# 항상 2줄
# =========================================================

def ema10_cross_html(data):

    current = data.get(
        "current",
        "none"
    )

    current_count = data.get(
        "current_count",
        0
    )

    previous = data.get(
        "previous",
        "none"
    )

    previous_count = data.get(
        "previous_count",
        0
    )

    # ---------------------------------------------
    # 현재 상승
    # ---------------------------------------------

    if current == "long":

        up_class = "triangle-up"
        down_class = "triangle-down-gray"

        up_count = f"({current_count})"

        if previous == "short" and previous_count > 0:
            down_count = f"({previous_count})"
        else:
            down_count = "(0)"

        return f"""
        <div class="ema10-box">

            <div class="ema10-row">

                <span class="{up_class}"></span>

                <span class="ema10-count-up">
                    {up_count}
                </span>

            </div>

            <div class="ema10-row">

                <span class="{down_class}"></span>

                <span class="ema10-count-gray">
                    {down_count}
                </span>

            </div>

        </div>
        """

    # ---------------------------------------------
    # 현재 하락
    # ---------------------------------------------

    if current == "short":

        up_class = "triangle-up-gray"
        down_class = "triangle-down"

        if previous == "long" and previous_count > 0:
            up_count = f"({previous_count})"
        else:
            up_count = "(0)"

        down_count = f"({current_count})"

        return f"""
        <div class="ema10-box">

            <div class="ema10-row">

                <span class="{up_class}"></span>

                <span class="ema10-count-gray">
                    {up_count}
                </span>

            </div>

            <div class="ema10-row">

                <span class="{down_class}"></span>

                <span class="ema10-count-down">
                    {down_count}
                </span>

            </div>

        </div>
        """

    # ---------------------------------------------
    # 중립
    # ---------------------------------------------

    return """
    <div class="ema10-box">

        <div class="ema10-row">

            <span class="triangle-up-gray"></span>

            <span class="ema10-count-gray">
                (0)
            </span>

        </div>

        <div class="ema10-row">

            <span class="triangle-down-gray"></span>

            <span class="ema10-count-gray">
                (0)
            </span>

        </div>

    </div>
    """


# =========================================================
# HTML - 비행기
# =========================================================

def warning_html(
    warning,
    count
):

    if not warning:
        return '<span class="no-warning">-</span>'

    direction_class = (
        "long"
        if warning == "LONG"
        else "short"
    )

    count_html = ""

    if count > 0:

        count_html = (
            f'<span class="air-count">'
            f'{count}'
            f'</span>'
        )

    return f"""
    <div class="air-box">

        <div class="air-main">

            <span class="air-direction {direction_class}">
                {warning}
            </span>

            <span class="air-icon">
                🛩 ✈️
            </span>

        </div>

        {count_html}

    </div>
    """


# =========================================================
# HTML - 행
# =========================================================

def rows_html(rows):

    if not rows:
        return """
        <tr>
            <td colspan="6">
                데이터 없음
            </td>
        </tr>
        """

    html = ""

    for row in rows:

        market = row["market"]

        coin = row["coin"]

        if market in latest_upbit_markets:
            coin += " <small>(업비트)</small>"

        qualified_class = (
            " qualified"
            if row.get("qualified")
            else ""
        )

        html += f"""
        <tr class="{qualified_class}">

            <td class="rank">
                {row["rank"]}
            </td>

            <td class="coin">
                <b>{coin}</b>
                <div>
                    {format_change(row["change"])}
                </div>
            </td>

            <td class="volume">
                {format_volume(row["volume"])}
            </td>

            <td class="ema-cell">

                <div class="ema-row">

                    <span class="tf">
                        1H
                    </span>

                    <span class="ema-value">
                        {ema_html(row["ema1h"])}
                    </span>

                </div>

                <div class="ema-row">

                    <span class="tf">
                        4H
                    </span>

                    <span class="ema-value">
                        {ema_html(row["ema4h"])}
                    </span>

                </div>

            </td>

            <td class="ema10-cell">

                {ema10_cross_html(
                    row["ema10"]
                )}

            </td>

            <td class="warning-cell">

                {warning_html(
                    row["air"],
                    row["air_count"]
                )}

            </td>

        </tr>
        """

    return html


# =========================================================
# HTML - 섹션
# =========================================================

def section(
    title,
    rows
):

    return f"""
    <div class="section">

        <div class="section-title">
            🏆 {title} TOP{TOP_N}
        </div>

        <div class="table-wrap">

            <table>

                <colgroup>

                    <col class="col-rank">
                    <col class="col-coin">
                    <col class="col-volume">
                    <col class="col-ema">
                    <col class="col-ema10">
                    <col class="col-warning">

                </colgroup>

                <thead>

                    <tr>

                        <th>#</th>
                        <th>코인</th>
                        <th>거래대금</th>
                        <th>EMA</th>
                        <th>10선</th>
                        <th>경고</th>

                    </tr>

                </thead>

                <tbody>

                    {rows_html(rows)}

                </tbody>

            </table>

        </div>

    </div>
    """


# =========================================================
# 집중 리스트
# =========================================================

def focus_section(rows):

    focus = [
        x for x in rows
        if x.get("qualified")
    ]

    if not focus:
        return ""

    return section(
        "🚨 집중 리스트",
        focus
    )


# =========================================================
# CSS
# =========================================================

CSS = """

* {
    box-sizing: border-box;
}

body {

    margin: 0;

    padding: 8px;

    background: #111;

    color: #eee;

    font-family:
        Arial,
        sans-serif;

    font-size: 12px;
}

.container {

    width: 100%;

    max-width: 900px;

    margin: auto;
}

.title {

    text-align: center;

    font-size: 17px;

    font-weight: bold;

    margin-bottom: 5px;
}

.info {

    text-align: center;

    color: #aaa;

    font-size: 10px;

    line-height: 1.35;

    margin-bottom: 7px;
}

.status {

    text-align: center;

    font-size: 10px;

    margin-bottom: 7px;
}

.status .on {
    color: #00d084;
}

.status .off {
    color: #777;
}

.section {

    margin-bottom: 9px;

    border: 1px solid #333;

    border-radius: 5px;

    overflow: hidden;
}

.section-title {

    padding: 4px 6px;

    background: #1b1b1b;

    font-size: 12px;

    font-weight: bold;
}

.table-wrap {

    width: 100%;

    overflow: hidden;
}

table {

    width: 100%;

    table-layout: fixed;

    border-collapse: collapse;
}

.col-rank {
    width: 7%;
}

.col-coin {
    width: 23%;
}

.col-volume {
    width: 17%;
}

.col-ema {
    width: 18%;
}

.col-ema10 {
    width: 12%;
}

.col-warning {
    width: 23%;
}

th {

    padding: 4px 2px;

    background: #222;

    color: #999;

    font-size: 10px;

    font-weight: normal;

    border-bottom: 1px solid #333;
}

td {

    padding: 3px 2px;

    text-align: center;

    border-bottom: 1px solid #252525;

    height: 38px;

    vertical-align: middle;

    overflow: hidden;
}

tr:last-child td {
    border-bottom: 0;
}

.rank {

    color: #888;

    font-size: 10px;
}

.coin {

    text-align: left;

    white-space: nowrap;

    overflow: hidden;

    text-overflow: ellipsis;
}

.coin b {

    font-size: 11px;
}

.coin small {

    color: #777;

    font-size: 8px;
}

.volume {

    color: #ddd;

    font-size: 10px;

    white-space: nowrap;
}

.up {

    color: #00d084;

    font-size: 9px;
}

.down {

    color: #ff4d4d;

    font-size: 9px;
}

.neutral {

    color: #888;

    font-size: 9px;
}

.ema-cell {

    padding-left: 1px;

    padding-right: 1px;
}

.ema-row {

    display: flex;

    align-items: center;

    justify-content: center;

    height: 15px;

    line-height: 15px;

    white-space: nowrap;
}

.tf {

    display: inline-block;

    width: 22px;

    text-align: left;

    color: #777;

    font-size: 8px;
}

.ema-value {

    display: inline-block;

    width: 43px;

    text-align: left;

    font-size: 9px;
}

.ema-green {
    color: #00d084;
}

.ema-red {
    color: #ff4d4d;
}

.ema-gray {
    color: #888;
}


/* =====================================================
   10선 - 2줄 고정
   ===================================================== */

.ema10-cell {

    padding: 2px 1px;

    vertical-align: middle;

    overflow: hidden;
}

.ema10-box {

    width: 100%;

    display: flex;

    flex-direction: column;

    align-items: center;

    justify-content: center;

    gap: 1px;

    line-height: 13px;
}

.ema10-row {

    width: 100%;

    height: 13px;

    display: flex;

    align-items: center;

    justify-content: center;

    gap: 3px;

    white-space: nowrap;
}


/* 상승 녹색 삼각형 */

.triangle-up {

    width: 0;

    height: 0;

    border-left: 5px solid transparent;

    border-right: 5px solid transparent;

    border-bottom: 9px solid #00d084;

    display: inline-block;

    flex-shrink: 0;
}


/* 상승 종료값 - 회색 */

.triangle-up-gray {

    width: 0;

    height: 0;

    border-left: 5px solid transparent;

    border-right: 5px solid transparent;

    border-bottom: 9px solid #666;

    display: inline-block;

    flex-shrink: 0;
}


/* 하락 빨간 삼각형 */

.triangle-down {

    width: 0;

    height: 0;

    border-left: 5px solid transparent;

    border-right: 5px solid transparent;

    border-top: 9px solid #ff4d4d;

    display: inline-block;

    flex-shrink: 0;
}


/* 하락 종료값 - 회색 */

.triangle-down-gray {

    width: 0;

    height: 0;

    border-left: 5px solid transparent;

    border-right: 5px solid transparent;

    border-top: 9px solid #666;

    display: inline-block;

    flex-shrink: 0;
}

.ema10-count-up {

    color: #00d084;

    font-size: 9px;

    font-weight: bold;
}

.ema10-count-down {

    color: #ff4d4d;

    font-size: 9px;

    font-weight: bold;
}

.ema10-count-gray {

    color: #666;

    font-size: 9px;

    font-weight: normal;
}


/* =====================================================
   경고
   ===================================================== */

.warning-cell {

    padding-left: 2px;

    padding-right: 2px;
}

.air-box {

    display: flex;

    align-items: center;

    justify-content: center;

    gap: 4px;

    min-width: 0;
}

.air-main {

    display: flex;

    align-items: center;

    justify-content: center;

    gap: 3px;

    white-space: nowrap;
}

.air-direction {

    font-size: 9px;

    font-weight: bold;
}

.air-direction.long {
    color: #00d084;
}

.air-direction.short {
    color: #ff4d4d;
}

.air-icon {

    display: inline-block;

    font-size: 13px;

    animation: air-pulse 1.2s infinite;
}

.air-count {

    min-width: 15px;

    color: #fff;

    font-size: 10px;

    font-weight: bold;
}

.no-warning {

    color: #555;

    font-size: 10px;
}

.qualified td {

    background: rgba(
        0,
        208,
        132,
        0.04
    );
}

@keyframes air-pulse {

    0% {
        transform: translateX(0);
    }

    50% {
        transform: translateX(2px);
    }

    100% {
        transform: translateX(0);
    }
}


/* =====================================================
   모바일
   ===================================================== */

@media (max-width: 480px) {

    body {

        padding: 4px;

        font-size: 11px;
    }

    .title {

        font-size: 15px;
    }

    .info {

        font-size: 9px;

        line-height: 1.3;
    }

    th {

        font-size: 9px;

        padding: 3px 1px;
    }

    td {

        padding: 2px 1px;

        height: 36px;
    }

    .col-rank {
        width: 7%;
    }

    .col-coin {
        width: 22%;
    }

    .col-volume {
        width: 16%;
    }

    .col-ema {
        width: 18%;
    }

    .col-ema10 {
        width: 13%;
    }

    .col-warning {
        width: 24%;
    }

    .coin b {

        font-size: 10px;
    }

    .volume {

        font-size: 9px;
    }

    .ema-value {

        width: 40px;

        font-size: 8px;
    }

    .tf {

        width: 20px;

        font-size: 7px;
    }

    .ema10-row {

        height: 12px;

        line-height: 12px;

        gap: 2px;
    }

    .triangle-up,
    .triangle-up-gray {

        border-left-width: 4px;

        border-right-width: 4px;

        border-bottom-width: 8px;
    }

    .triangle-down,
    .triangle-down-gray {

        border-left-width: 4px;

        border-right-width: 4px;

        border-top-width: 8px;
    }

    .ema10-count-up,
    .ema10-count-down,
    .ema10-count-gray {

        font-size: 8px;
    }

    .air-direction {

        font-size: 8px;
    }

    .air-icon {

        font-size: 11px;
    }

    .air-count {

        font-size: 9px;
    }
}
"""


# =========================================================
# 메인 페이지
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def home():

    with update_lock:

        upbit_rows = list(
            latest_upbit_data
        )

        okx_rows = list(
            latest_okx_data
        )

        upbit_time = (
            latest_upbit_update_time
        )

        okx_time = (
            latest_okx_update_time
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

        <title>
            매매 전술
        </title>

        <style>
            {CSS}
        </style>

    </head>

    <body>

        <div class="container">

            <div class="title">
                📊 매매 전술 눌림 돌파
            </div>

            <div class="info">

                ① 거래대금 TOP{TOP_N}<br>

                ② 1H + 4H EMA
                10-30-60-120 정배열<br>

                ③ EMA 정배열/역배열
                연속 캔들 수 표시<br>

                ④ 1H 종가 ↔ EMA10<br>

                ⑤ EMA10 아래 종가부터
                🔻 하락 카운팅<br>

                ⑥ EMA10 위 종가부터
                🟢 상승 카운팅<br>

                ⑦ 이전 최종 카운트는
                회색으로 유지<br>

                ⑧ 조건 충족 시
                🛩 이후 양봉마다 카운터 증가

            </div>

            <div class="status">

                UPBIT:
                <span class="{
                    'on' if USE_UPBIT == 'Y'
                    else 'off'
                }">
                    {USE_UPBIT}
                </span>

                &nbsp;&nbsp;

                OKX:
                <span class="{
                    'on' if USE_OKX == 'Y'
                    else 'off'
                }">
                    {USE_OKX}
                </span>

                <br>

                UPBIT:
                {upbit_time}

                &nbsp;&nbsp;

                OKX:
                {okx_time}

            </div>

            {
                focus_section(upbit_rows)
                if USE_UPBIT == "Y"
                else ""
            }

            {
                section(
                    "🏆 UPBIT 실거래대금",
                    upbit_rows
                )
                if USE_UPBIT == "Y"
                else ""
            }

            {
                section(
                    "🏆 OKX 실거래대금",
                    okx_rows
                )
                if USE_OKX == "Y"
                else ""
            }

        </div>

        <script>

            setTimeout(
                function() {{
                    location.reload();
                }},
                60000
            );

        </script>

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
    ).minutes.do(
        update_dashboard
    )

    while True:

        try:

            schedule.run_pending()

        except Exception as e:

            logging.exception(
                f"Scheduler error: {e}"
            )

        time.sleep(1)


# =========================================================
# 시작
# =========================================================

if __name__ == "__main__":

    if USE_UPBIT not in ("Y", "N"):
        raise ValueError(
            "USE_UPBIT must be Y or N"
        )

    if USE_OKX not in ("Y", "N"):
        raise ValueError(
            "USE_OKX must be Y or N"
        )

    logging.info(
        "========================================"
    )

    logging.info(
        "📊 매매 전술 대시보드 시작"
    )

    logging.info(
        f"TOP_N = {TOP_N}"
    )

    logging.info(
        f"UPDATE_MINUTES = {UPDATE_MINUTES}"
    )

    logging.info(
        "10선 = 1H 종가 ↔ EMA10"
    )

    logging.info(
        "상승 = 녹색 ▲ / 하락 = 빨간 ▼"
    )

    logging.info(
        "이전 최종 카운트 = 회색"
    )

    logging.info(
        "10선 2줄 고정 표시"
    )

    logging.info(
        "비행기 = 1H EMA10 돌파 + 4H 정배열"
    )

    logging.info(
        "========================================"
    )

    # 최초 업데이트
    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    # 스케줄러
    threading.Thread(
        target=scheduler_loop,
        daemon=True
    ).start()

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )
