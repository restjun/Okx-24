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

KST = ZoneInfo("Asia/Seoul")

VOLUME_HOURS = 24
TOP_N = 30
UPDATE_MINUTES = 1

INITIAL_CANDLE_COUNT = 200
HISTORY_CHUNK = 200
MAX_HISTORY_CHUNKS = 10

SWING_LEFT = 2
SWING_RIGHT = 2

MIN_CORRECTION_RATE = 0.003

USE_UPBIT = "Y"
USE_OKX = "N"

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10

MIN_N_CANDLES = 20


# =========================================================
# 전역 변수
# =========================================================

latest_upbit_data = []
latest_okx_data = []

latest_usdt_krw = 0.0

latest_upbit_update_time = "-"
latest_okx_update_time = "-"

latest_upbit_markets = []

shown_invalidation_ids = set()

request_lock = threading.Lock()
update_lock = threading.Lock()
invalidation_lock = threading.Lock()

last_request_time = 0.0


# =========================================================
# HTTP
# =========================================================

def wait_request():
    global last_request_time

    with request_lock:
        now = time.time()
        diff = now - last_request_time

        if diff < REQUEST_INTERVAL:
            time.sleep(REQUEST_INTERVAL - diff)

        last_request_time = time.time()


def retry_request(method, url, **kwargs):

    for i in range(MAX_RETRIES):

        try:
            wait_request()

            r = requests.request(
                method,
                url,
                timeout=10,
                **kwargs
            )

            if r.status_code == 200:
                return r

            if r.status_code == 429:
                time.sleep(min(RATE_LIMIT_WAIT * (2 ** i), 60))
                continue

            if 500 <= r.status_code < 600:
                time.sleep(min(2 ** i, 30))
                continue

            return r

        except Exception:
            time.sleep(min(2 ** i, 20))

    return None


# =========================================================
# Upbit
# =========================================================

def get_upbit_markets():

    r = retry_request(
        "GET",
        "https://api.upbit.com/v1/market/all",
        params={"isDetails": "false"}
    )

    if not r:
        return []

    try:
        data = r.json()
        return [
            x["market"]
            for x in data
            if x["market"].startswith("KRW-")
        ]
    except Exception:
        return []


def get_upbit_tickers(markets):

    if not markets:
        return []

    result = []

    for i in range(0, len(markets), 100):
        batch = markets[i:i + 100]

        r = retry_request(
            "GET",
            "https://api.upbit.com/v1/ticker",
            params={"markets": ",".join(batch)}
        )

        if not r:
            continue

        try:
            result.extend(r.json())
        except Exception:
            pass

    return result


def get_upbit_minute(market, unit=15, count=200):

    r = retry_request(
        "GET",
        f"https://api.upbit.com/v1/candles/minutes/{unit}",
        params={
            "market": market,
            "count": count
        }
    )

    if not r:
        return pd.DataFrame()

    try:
        data = r.json()

        df = pd.DataFrame(data)

        if df.empty:
            return df

        df["time"] = pd.to_datetime(
            df["candle_date_time_kst"]
        )

        df = df.sort_values("time").reset_index(drop=True)

        return df[
            ["time", "opening_price", "high_price",
             "low_price", "trade_price", "candle_acc_trade_volume"]
        ].rename(
            columns={
                "opening_price": "open",
                "high_price": "high",
                "low_price": "low",
                "trade_price": "close",
                "candle_acc_trade_volume": "volume"
            }
        )

    except Exception:
        return pd.DataFrame()


def get_upbit_4h(market, count=200):

    r = retry_request(
        "GET",
        "https://api.upbit.com/v1/candles/minutes/240",
        params={
            "market": market,
            "count": count
        }
    )

    if not r:
        return pd.DataFrame()

    try:
        data = r.json()
        df = pd.DataFrame(data)

        if df.empty:
            return df

        df["time"] = pd.to_datetime(
            df["candle_date_time_kst"]
        )

        df = df.sort_values("time").reset_index(drop=True)

        return df[
            ["time", "opening_price", "high_price",
             "low_price", "trade_price"]
        ].rename(
            columns={
                "opening_price": "open",
                "high_price": "high",
                "low_price": "low",
                "trade_price": "close"
            }
        )

    except Exception:
        return pd.DataFrame()


# =========================================================
# OKX
# =========================================================

def get_okx_symbols():

    r = retry_request(
        "GET",
        "https://www.okx.com/api/v5/public/instruments",
        params={"instType": "SWAP"}
    )

    if not r:
        return []

    try:
        data = r.json()["data"]

        return [
            x["instId"]
            for x in data
            if x.get("settleCcy") == "USDT"
            and x.get("state") == "live"
            and x["instId"].endswith("-USDT-SWAP")
        ]

    except Exception:
        return []


def get_okx_ohlcv(inst_id, bar="15m", limit=200):

    r = retry_request(
        "GET",
        "https://www.okx.com/api/v5/market/candles",
        params={
            "instId": inst_id,
            "bar": bar,
            "limit": limit
        }
    )

    if not r:
        return pd.DataFrame()

    try:
        data = r.json()["data"]

        if not data:
            return pd.DataFrame()

        rows = []

        for x in data:
            rows.append({
                "time": pd.to_datetime(
                    int(x[0]), unit="ms"
                ),
                "open": float(x[1]),
                "high": float(x[2]),
                "low": float(x[3]),
                "close": float(x[4]),
                "confirm": x[8]
            })

        df = pd.DataFrame(rows)

        df = df.sort_values("time").reset_index(drop=True)

        # 현재 진행 중인 캔들 제거
        df = df[df["confirm"].astype(str) == "1"]

        return df[
            ["time", "open", "high", "low", "close"]
        ].reset_index(drop=True)

    except Exception:
        return pd.DataFrame()


def get_usdt_krw():

    r = retry_request(
        "GET",
        "https://api.upbit.com/v1/ticker",
        params={"markets": "KRW-USDT"}
    )

    if not r:
        return 0.0

    try:
        return float(r.json()[0]["trade_price"])
    except Exception:
        return 0.0


def get_okx_volume(inst_id):

    r = retry_request(
        "GET",
        "https://www.okx.com/api/v5/market/candles",
        params={
            "instId": inst_id,
            "bar": "1H",
            "limit": VOLUME_HOURS + 2
        }
    )

    if not r:
        return 0.0

    try:
        data = r.json()["data"]

        total = 0.0

        for x in data:
            if len(x) > 7:
                total += float(x[7])

        return total * latest_usdt_krw / 10

    except Exception:
        return 0.0


# =========================================================
# EMA
# =========================================================

def ema(df, period):

    return df["close"].ewm(
        span=period,
        adjust=False
    ).mean()


def direction(df):

    if df.empty or len(df) < 120:
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


def direction_series(df):

    if df.empty or len(df) < 120:
        return []

    e10 = ema(df, 10)
    e30 = ema(df, 30)
    e60 = ema(df, 60)
    e120 = ema(df, 120)

    result = []

    for i in range(len(df)):

        if i < 119:
            result.append("none")
            continue

        if (
            e10.iloc[i] >
            e30.iloc[i] >
            e60.iloc[i] >
            e120.iloc[i]
        ):
            result.append("long")

        elif (
            e10.iloc[i] <
            e30.iloc[i] <
            e60.iloc[i] <
            e120.iloc[i]
        ):
            result.append("short")

        else:
            result.append("none")

    return result


def ema_display(df):

    d = direction(df)

    if d == "long":
        return "🟢 LONG"

    if d == "short":
        return "🔴 SHORT"

    return "⚪"


# =========================================================
# N자 파동
#
# LONG
# 0 < B < A < C
#
# 0 : 상승 시작점
# A : 첫 고점
# B : A 이후 조정 저점
# C : B 이후 상승
#
# C가 A를 종가로 돌파하면 🚀
#
# 돌파 후 종가가 A 아래로 내려가면 🚨
#
# 돌파 후 새로운 조정이 나오면
# B -> 새로운 0
# C -> 새로운 A
# =========================================================

def get_swings(df):

    if len(df) < SWING_LEFT + SWING_RIGHT + 5:
        return [], []

    highs = []
    lows = []

    for i in range(
        SWING_LEFT,
        len(df) - SWING_RIGHT
    ):

        h = df["high"].iloc[i]
        l = df["low"].iloc[i]

        left_h = df["high"].iloc[
            i - SWING_LEFT:i
        ]

        right_h = df["high"].iloc[
            i + 1:i + SWING_RIGHT + 1
        ]

        left_l = df["low"].iloc[
            i - SWING_LEFT:i
        ]

        right_l = df["low"].iloc[
            i + 1:i + SWING_RIGHT + 1
        ]

        if h >= left_h.max() and h >= right_h.max():
            highs.append(i)

        if l <= left_l.min() and l <= right_l.min():
            lows.append(i)

    return highs, lows


def find_long_n(df):

    if df.empty or len(df) < MIN_N_CANDLES:
        return None

    highs, lows = get_swings(df)

    if not highs or not lows:
        return None

    # 모든 파동 후보 탐색
    for zero_idx in reversed(lows):

        zero_price = float(df["low"].iloc[zero_idx])

        # ---------------------------------------------
        # 0 -> A
        # ---------------------------------------------

        a_candidates = [
            x for x in highs
            if x > zero_idx
        ]

        if not a_candidates:
            continue

        for a_idx in a_candidates:

            a_price = float(df["high"].iloc[a_idx])

            if a_price <= zero_price:
                continue

            # -----------------------------------------
            # A -> B
            # B < A
            # B > 0
            # -----------------------------------------

            b_candidates = [
                x for x in lows
                if x > a_idx
                and zero_price < float(df["low"].iloc[x]) < a_price
            ]

            if not b_candidates:
                continue

            # 가장 최근 B부터 확인
            for b_idx in reversed(b_candidates):

                b_price = float(df["low"].iloc[b_idx])

                # 조정폭이 너무 작은 경우 제외
                correction = (
                    a_price - b_price
                ) / a_price

                if correction < MIN_CORRECTION_RATE:
                    continue

                # -------------------------------------
                # B -> C
                # C는 A보다 반드시 높아야 함
                # -------------------------------------

                c_candidates = [
                    x for x in highs
                    if x > b_idx
                    and float(df["high"].iloc[x]) > a_price
                ]

                if not c_candidates:
                    continue

                c_idx = c_candidates[0]

                # C 고점
                c_price = float(
                    df["high"].iloc[c_idx]
                )

                if c_price <= a_price:
                    continue

                # -------------------------------------
                # A 돌파 캔들 확인
                # close > A high
                # -------------------------------------

                breakout_idx = None

                for j in range(b_idx + 1, len(df)):

                    close = float(df["close"].iloc[j])

                    if close > a_price:
                        breakout_idx = j
                        break

                if breakout_idx is None:
                    continue

                # -------------------------------------
                # 돌파 이후 A 아래로 내려갔는지 검사
                #
                # 내려가면 이 N은 무효
                # -------------------------------------

                invalid = False

                for j in range(
                    breakout_idx + 1,
                    len(df)
                ):

                    close = float(
                        df["close"].iloc[j]
                    )

                    if close < a_price:
                        invalid = True
                        break

                if invalid:
                    continue

                # -------------------------------------
                # 현재가 C의 상승구간에 있는지 확인
                # -------------------------------------

                count = len(df) - breakout_idx

                return {
                    "direction": "long",
                    "zero_idx": zero_idx,
                    "a_idx": a_idx,
                    "b_idx": b_idx,
                    "c_idx": c_idx,
                    "breakout_idx": breakout_idx,
                    "a_price": a_price,
                    "b_price": b_price,
                    "c_price": c_price,
                    "count": count
                }

    return None


def find_short_n(df):

    if df.empty or len(df) < MIN_N_CANDLES:
        return None

    highs, lows = get_swings(df)

    if not highs or not lows:
        return None

    # SHORT
    # 0 > B > A > C

    for zero_idx in reversed(highs):

        zero_price = float(
            df["high"].iloc[zero_idx]
        )

        a_candidates = [
            x for x in lows
            if x > zero_idx
        ]

        if not a_candidates:
            continue

        for a_idx in a_candidates:

            a_price = float(
                df["low"].iloc[a_idx]
            )

            if a_price >= zero_price:
                continue

            b_candidates = [
                x for x in highs
                if x > a_idx
                and a_price < float(df["high"].iloc[x]) < zero_price
            ]

            if not b_candidates:
                continue

            for b_idx in reversed(b_candidates):

                b_price = float(
                    df["high"].iloc[b_idx]
                )

                correction = (
                    b_price - a_price
                ) / a_price

                if correction < MIN_CORRECTION_RATE:
                    continue

                c_candidates = [
                    x for x in lows
                    if x > b_idx
                    and float(df["low"].iloc[x]) < a_price
                ]

                if not c_candidates:
                    continue

                c_idx = c_candidates[0]

                c_price = float(
                    df["low"].iloc[c_idx]
                )

                if c_price >= a_price:
                    continue

                breakout_idx = None

                for j in range(
                    b_idx + 1,
                    len(df)
                ):

                    close = float(
                        df["close"].iloc[j]
                    )

                    if close < a_price:
                        breakout_idx = j
                        break

                if breakout_idx is None:
                    continue

                # A 위로 다시 올라오면 무효
                invalid = False

                for j in range(
                    breakout_idx + 1,
                    len(df)
                ):

                    close = float(
                        df["close"].iloc[j]
                    )

                    if close > a_price:
                        invalid = True
                        break

                if invalid:
                    continue

                count = len(df) - breakout_idx

                return {
                    "direction": "short",
                    "zero_idx": zero_idx,
                    "a_idx": a_idx,
                    "b_idx": b_idx,
                    "c_idx": c_idx,
                    "breakout_idx": breakout_idx,
                    "a_price": a_price,
                    "b_price": b_price,
                    "c_price": c_price,
                    "count": count
                }

    return None


# =========================================================
# 해지 경고
# =========================================================

def invalidation_id(symbol, signal):

    if not signal:
        return None

    return (
        f"{symbol}_"
        f"{signal['direction']}_"
        f"{signal['breakout_idx']}_"
        f"{signal['a_price']}"
    )


def check_invalidation(
    symbol,
    df,
    signal
):

    if not signal or df.empty:
        return False

    i = len(df) - 1

    if i < 1:
        return False

    a_price = signal["a_price"]

    current_close = float(
        df["close"].iloc[i]
    )

    previous_close = float(
        df["close"].iloc[i - 1]
    )

    current_open = float(
        df["open"].iloc[i]
    )

    direction_ = signal["direction"]

    # ---------------------------------------------
    # LONG
    #
    # 음봉 마감
    # 현재 종가 < 이전 종가
    # 또는 A 아래로 하락
    # ---------------------------------------------

    if direction_ == "long":

        if current_close < a_price:
            return True

        if (
            current_close < current_open
            and current_close < previous_close
        ):
            return True

    # ---------------------------------------------
    # SHORT
    # ---------------------------------------------

    if direction_ == "short":

        if current_close > a_price:
            return True

        if (
            current_close > current_open
            and current_close > previous_close
        ):
            return True

    return False


def mark_invalidation_once(
    symbol,
    signal
):

    sid = invalidation_id(
        symbol,
        signal
    )

    if sid is None:
        return False

    with invalidation_lock:

        if sid in shown_invalidation_ids:
            return False

        shown_invalidation_ids.add(sid)

        # 너무 커지지 않게 관리
        if len(shown_invalidation_ids) > 10000:
            shown_invalidation_ids.clear()

        return True


# =========================================================
# 15분 N자
# =========================================================

def get_15m_signal(
    symbol,
    df15,
    allow_short=True
):

    if df15.empty:
        return None

    long_signal = find_long_n(df15)

    short_signal = (
        find_short_n(df15)
        if allow_short
        else None
    )

    # 최근 breakout을 선택
    candidates = [
        x for x in
        [long_signal, short_signal]
        if x is not None
    ]

    if not candidates:
        return None

    return max(
        candidates,
        key=lambda x: x["breakout_idx"]
    )


# =========================================================
# 멀티 타임프레임 확인
# =========================================================

def multi_signal(
    df15,
    df1h,
    df4h,
    allow_short=True
):

    if df15.empty or df1h.empty or df4h.empty:
        return None

    n = get_15m_signal(
        "",
        df15,
        allow_short
    )

    if not n:
        return None

    d15 = direction(df15)
    d1h = direction(df1h)
    d4h = direction(df4h)

    nd = n["direction"]

    if nd == "long":

        if (
            d15 == "long"
            and d1h == "long"
            and d4h == "long"
        ):
            return n

    if nd == "short" and allow_short:

        if (
            d15 == "short"
            and d1h == "short"
            and d4h == "short"
        ):
            return n

    return None


# =========================================================
# 일간 변동률
# =========================================================

def daily_change_upbit(market):

    r = retry_request(
        "GET",
        "https://api.upbit.com/v1/ticker",
        params={"markets": market}
    )

    if not r:
        return 0.0

    try:
        return float(
            r.json()[0]["signed_change_rate"]
        ) * 100
    except Exception:
        return 0.0


def format_change(v):

    if v is None:
        return "-"

    return f"{v:+.2f}%"


def format_volume(v):

    if v >= 1_000_000_000_000:
        return f"{v / 1_000_000_000_000:.1f}조"

    if v >= 100_000_000:
        return f"{v / 100_000_000:.0f}억"

    if v >= 10_000_000:
        return f"{v / 10_000_000:.1f}천만"

    return f"{v:,.0f}"


# =========================================================
# 분석
# =========================================================

def analyze_upbit(
    market,
    ticker
):

    df15 = get_upbit_minute(
        market,
        15,
        INITIAL_CANDLE_COUNT
    )

    df1h = get_upbit_minute(
        market,
        60,
        INITIAL_CANDLE_COUNT
    )

    df4h = get_upbit_4h(
        market,
        INITIAL_CANDLE_COUNT
    )

    if (
        df15.empty
        or df1h.empty
        or df4h.empty
    ):
        return None

    d15 = direction(df15)
    d1h = direction(df1h)
    d4h = direction(df4h)

    n = find_long_n(df15)

    signal = None

    if n and (
        d15 == "long"
        and d1h == "long"
        and d4h == "long"
    ):
        signal = n

    result = {
        "symbol": market.replace("KRW-", ""),
        "market": market,
        "change": float(
            ticker.get("signed_change_rate", 0)
        ) * 100,
        "volume": float(
            ticker.get("acc_trade_price_24h", 0)
        ),
        "d15": d15,
        "d1h": d1h,
        "d4h": d4h,
        "signal": signal
    }

    return result


def analyze_okx(
    inst_id,
    volume
):

    df15 = get_okx_ohlcv(
        inst_id,
        "15m",
        INITIAL_CANDLE_COUNT
    )

    df1h = get_okx_ohlcv(
        inst_id,
        "1H",
        INITIAL_CANDLE_COUNT
    )

    df4h = get_okx_ohlcv(
        inst_id,
        "4H",
        INITIAL_CANDLE_COUNT
    )

    if (
        df15.empty
        or df1h.empty
        or df4h.empty
    ):
        return None

    d15 = direction(df15)
    d1h = direction(df1h)
    d4h = direction(df4h)

    n_long = find_long_n(df15)
    n_short = find_short_n(df15)

    candidates = [
        x for x in
        [n_long, n_short]
        if x is not None
    ]

    signal = None

    if candidates:

        n = max(
            candidates,
            key=lambda x: x["breakout_idx"]
        )

        if (
            n["direction"] == "long"
            and d15 == "long"
            and d1h == "long"
            and d4h == "long"
        ):
            signal = n

        elif (
            n["direction"] == "short"
            and d15 == "short"
            and d1h == "short"
            and d4h == "short"
        ):
            signal = n

    return {
        "symbol": inst_id.replace("-USDT-SWAP", ""),
        "market": inst_id,
        "change": 0,
        "volume": volume,
        "d15": d15,
        "d1h": d1h,
        "d4h": d4h,
        "signal": signal
    }


# =========================================================
# Upbit 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_markets
    global latest_upbit_update_time

    if USE_UPBIT != "Y":
        return

    try:

        markets = get_upbit_markets()

        latest_upbit_markets = markets

        tickers = get_upbit_tickers(markets)

        tickers = sorted(
            tickers,
            key=lambda x:
            x.get("acc_trade_price_24h", 0),
            reverse=True
        )[:TOP_N]

        result = []

        for ticker in tickers:

            market = ticker["market"]

            data = analyze_upbit(
                market,
                ticker
            )

            if data:
                result.append(data)

        latest_upbit_data = result

        latest_upbit_update_time = (
            datetime.now(KST).strftime(
                "%m-%d %H:%M:%S"
            )
        )

        logging.info(
            "Upbit update: %d",
            len(result)
        )

    except Exception as e:

        logging.exception(
            "Upbit update error: %s",
            e
        )


# =========================================================
# OKX 업데이트
# =========================================================

def update_okx():

    global latest_okx_data
    global latest_okx_update_time
    global latest_usdt_krw

    if USE_OKX != "Y":
        return

    try:

        latest_usdt_krw = get_usdt_krw()

        symbols = get_okx_symbols()

        rows = []

        for symbol in symbols:

            volume = get_okx_volume(
                symbol
            )

            rows.append(
                (symbol, volume)
            )

        rows.sort(
            key=lambda x: x[1],
            reverse=True
        )

        rows = rows[:TOP_N]

        result = []

        for symbol, volume in rows:

            data = analyze_okx(
                symbol,
                volume
            )

            if data:
                result.append(data)

        latest_okx_data = result

        latest_okx_update_time = (
            datetime.now(KST).strftime(
                "%m-%d %H:%M:%S"
            )
        )

        logging.info(
            "OKX update: %d",
            len(result)
        )

    except Exception as e:

        logging.exception(
            "OKX update error: %s",
            e
        )


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    if not update_lock.acquire(
        blocking=False
    ):
        return

    try:

        logging.info("===== UPDATE START =====")

        if USE_UPBIT == "Y":
            update_upbit()

        if USE_OKX == "Y":
            update_okx()

        logging.info("===== UPDATE END =====")

    finally:
        update_lock.release()


# =========================================================
# HTML
# =========================================================

def direction_html(d):

    if d == "long":
        return '<span class="long">LONG</span>'

    if d == "short":
        return '<span class="short">SHORT</span>'

    return '<span class="none">-</span>'


def n_html(
    symbol,
    df,
    signal
):

    if not signal:
        return "-"

    count = signal["count"]

    if check_invalidation(
        symbol,
        df,
        signal
    ):

        if mark_invalidation_once(
            symbol,
            signal
        ):
            return "🚨"

        return "❌"

    if signal["direction"] == "long":
        return f"🚀({count})"

    return f"🚀({count})"


def make_rows(
    data,
    source
):

    rows = []

    for x in data:

        signal = x.get("signal")

        direction_ = (
            signal["direction"]
            if signal
            else None
        )

        cls = "qualified" if signal else ""

        coin = x["symbol"]

        if source == "upbit":
            coin += " <small>(업비트)</small>"

        rows.append(
            f"""
            <tr class="{cls}">
                <td>{coin}</td>

                <td>
                    {format_change(x["change"])}
                </td>

                <td>
                    {format_volume(x["volume"])}
                </td>

                <td>
                    {direction_html(x["d15"])}
                    /
                    {direction_html(x["d1h"])}
                    /
                    {direction_html(x["d4h"])}
                </td>

                <td>
                    {(
                        n_html(
                            x["market"],
                            x.get("_df15", pd.DataFrame()),
                            signal
                        )
                        if signal
                        else "-"
                    )}
                </td>
            </tr>
            """
        )

    return "".join(rows)


# =========================================================
# 데이터에 15분 DF 연결
# =========================================================
#
# HTML에서 해지 여부를 다시 판단하기 위해
# 분석 결과에 DF를 보관
# =========================================================

def attach_upbit_df():

    for x in latest_upbit_data:

        try:
            x["_df15"] = get_upbit_minute(
                x["market"],
                15,
                INITIAL_CANDLE_COUNT
            )
        except Exception:
            x["_df15"] = pd.DataFrame()


def attach_okx_df():

    for x in latest_okx_data:

        try:
            x["_df15"] = get_okx_ohlcv(
                x["market"],
                "15m",
                INITIAL_CANDLE_COUNT
            )
        except Exception:
            x["_df15"] = pd.DataFrame()


# =========================================================
# FastAPI
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def home():

    attach_upbit_df()
    attach_okx_df()

    upbit_rows = make_rows(
        latest_upbit_data,
        "upbit"
    )

    okx_rows = make_rows(
        latest_okx_data,
        "okx"
    )

    return f"""
<!DOCTYPE html>
<html lang="ko">
<head>

<meta charset="UTF-8">

<meta
 name="viewport"
 content="width=device-width,
 initial-scale=1.0"
>

<title>15M N Pattern</title>

<style>

* {{
    box-sizing: border-box;
}}

body {{
    margin: 0;
    background: #111;
    color: #eee;
    font-family: Arial, sans-serif;
}}

.wrap {{
    width: 100%;
    max-width: 1000px;
    margin: auto;
    padding: 8px;
}}

h2 {{
    margin: 5px 0 10px;
    font-size: 18px;
}}

.info {{
    font-size: 11px;
    color: #aaa;
    margin-bottom: 8px;
}}

table {{
    width: 100%;
    border-collapse: collapse;
    table-layout: fixed;
    font-size: 12px;
}}

th {{
    background: #222;
    padding: 7px 3px;
    border-bottom: 1px solid #444;
}}

td {{
    padding: 7px 3px;
    text-align: center;
    border-bottom: 1px solid #292929;
}}

td:first-child {{
    text-align: left;
    font-weight: bold;
}}

small {{
    color: #888;
    font-weight: normal;
}}

.long {{
    color: #00e676;
    font-weight: bold;
}}

.short {{
    color: #ff5252;
    font-weight: bold;
}}

.none {{
    color: #777;
}}

.qualified {{
    animation: blink 1.2s infinite;
}}

@keyframes blink {{
    50% {{
        background: #263b25;
    }}
}}

@media(max-width:600px) {{

    table {{
        font-size: 10px;
    }}

    td, th {{
        padding: 6px 2px;
    }}

    h2 {{
        font-size: 16px;
    }}
}}

</style>

</head>

<body>

<div class="wrap">

<h2>🚀 15분 N자 돌파 감시</h2>

<div class="info">
Upbit: {latest_upbit_update_time}
&nbsp;&nbsp;
OKX: {latest_okx_update_time}
</div>

<table>

<thead>

<tr>
<th>코인</th>
<th>변동</th>
<th>거래대금</th>
<th>EMA<br>15M/1H/4H</th>
<th>N자</th>
</tr>

</thead>

<tbody>

{upbit_rows}
{okx_rows}

</tbody>

</table>

</div>

</body>
</html>
"""


# =========================================================
# 스케줄러
# =========================================================

def scheduler():

    schedule.every(
        UPDATE_MINUTES
    ).minutes.do(
        update_dashboard
    )

    while True:

        try:
            schedule.run_pending()
        except Exception:
            logging.exception(
                "Scheduler error"
            )

        time.sleep(1)


# =========================================================
# 시작
# =========================================================

if __name__ == "__main__":

    update_dashboard()

    threading.Thread(
        target=scheduler,
        daemon=True
    ).start()

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )
