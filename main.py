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


# =========================================================
# FutureWarning 숨김
# =========================================================

warnings.filterwarnings(
    "ignore",
    category=FutureWarning
)


app = FastAPI()


# =========================================================
# 로그
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s"
)


# =========================================================
# 사용자 설정
# =========================================================

VOLUME_HOURS = 24

TOP_N = 20

UPDATE_MINUTES = 1

# ---------------------------------------------------------
# 돌파 기준
# 최소 10개 확정 캔들
# ---------------------------------------------------------

BREAKOUT_LOOKBACK = 10

# ---------------------------------------------------------
# EMA
# ---------------------------------------------------------

EMA_FAST = 10
EMA_MID = 30
EMA_SLOW = 60


# =========================================================
# 거래소 조회 Y / N
# =========================================================

USE_UPBIT = "Y"

USE_OKX = "N"


# =========================================================
# API 안정화 설정
# =========================================================

REQUEST_INTERVAL = 0.08

RATE_LIMIT_WAIT = 3

MAX_RETRIES = 10


# =========================================================
# OKX 실패 종목 반복
# =========================================================

OKX_RETRY_DELAY = 2

OKX_MAX_RETRY_ROUNDS = 0


# =========================================================
# 전역 데이터
# =========================================================

latest_okx_data = []

latest_upbit_data = []

latest_usdt_krw = 0.0


# =========================================================
# API 요청 간격
# =========================================================

request_lock = threading.Lock()

last_request_time = 0.0


def wait_request_interval():

    global last_request_time

    with request_lock:

        now = time.monotonic()

        elapsed = now - last_request_time

        if elapsed < REQUEST_INTERVAL:

            time.sleep(
                REQUEST_INTERVAL - elapsed
            )

        last_request_time = time.monotonic()


# =========================================================
# API 재시도
# =========================================================

def retry_request(
    func,
    *args,
    **kwargs
):

    for attempt in range(MAX_RETRIES):

        try:

            wait_request_interval()

            result = func(
                *args,
                **kwargs
            )

            if hasattr(
                result,
                "status_code"
            ):

                status = result.status_code

                if status == 429:

                    wait_time = min(
                        RATE_LIMIT_WAIT *
                        (2 ** attempt),
                        60
                    )

                    logging.warning(
                        f"API 429 "
                        f"({attempt + 1}/{MAX_RETRIES}) "
                        f"- {wait_time}초 대기"
                    )

                    time.sleep(
                        wait_time
                    )

                    continue

                if status >= 500:

                    wait_time = min(
                        2 * (2 ** attempt),
                        30
                    )

                    logging.warning(
                        f"API 서버 오류 {status} "
                        f"({attempt + 1}/{MAX_RETRIES}) "
                        f"- {wait_time}초 대기"
                    )

                    time.sleep(
                        wait_time
                    )

                    continue

                if status != 200:

                    logging.warning(
                        f"API HTTP 오류 {status}"
                    )

                    return result

            return result

        except Exception as e:

            wait_time = min(
                2 * (attempt + 1),
                20
            )

            logging.error(
                f"API 실패 "
                f"{attempt + 1}/{MAX_RETRIES} : {e}"
            )

            if attempt < MAX_RETRIES - 1:

                time.sleep(
                    wait_time
                )

    return None


# =========================================================
# 업비트 USDT-KRW
# =========================================================

def get_usdt_krw():

    url = (
        "https://api.upbit.com/v1/ticker"
        "?markets=KRW-USDT"
    )

    response = retry_request(
        requests.get,
        url,
        timeout=15
    )

    if response is None:

        return None

    try:

        data = response.json()

        if not data:

            return None

        price = float(
            data[0]["trade_price"]
        )

        if price <= 0:

            return None

        logging.info(
            f"USDT-KRW = {price:,.2f}원"
        )

        return price

    except Exception as e:

        logging.error(
            f"USDT-KRW 처리 오류 : {e}"
        )

        return None


# =========================================================
# OKX 캔들
#
# confirm=1 확정캔들 + 진행중 캔들도 함께 받을 수 있도록
# 별도 함수에서 처리
# =========================================================

def get_okx_ohlcv(
    inst_id,
    bar="1H",
    limit=200
):

    limit = max(
        1,
        min(int(limit), 200)
    )

    url = (
        "https://www.okx.com/api/v5/market/candles"
        f"?instId={inst_id}"
        f"&bar={bar}"
        f"&limit={limit}"
    )

    response = retry_request(
        requests.get,
        url,
        timeout=15
    )

    if response is None:

        return None

    try:

        data = response.json().get(
            "data",
            []
        )

        if not data:

            return None

        df = pd.DataFrame(
            data,
            columns=[
                "ts",
                "o",
                "h",
                "l",
                "c",
                "vol",
                "volCcy",
                "volCcyQuote",
                "confirm"
            ]
        )

        for column in [
            "o",
            "h",
            "l",
            "c",
            "vol",
            "volCcyQuote"
        ]:

            df[column] = pd.to_numeric(
                df[column],
                errors="coerce"
            )

        df["ts"] = pd.to_numeric(
            df["ts"],
            errors="coerce"
        )

        df = (
            df.iloc[::-1]
            .reset_index(drop=True)
        )

        return df

    except Exception as e:

        logging.error(
            f"OKX 캔들 오류 "
            f"{inst_id} : {e}"
        )

        return None


# =========================================================
# OKX 확정 캔들만
# =========================================================

def get_okx_confirmed_ohlcv(
    inst_id,
    bar="1H",
    limit=200
):

    df = get_okx_ohlcv(
        inst_id,
        bar,
        limit
    )

    if df is None:

        return None

    df = df[
        df["confirm"].astype(str) == "1"
    ].copy()

    if df.empty:

        return None

    return (
        df
        .reset_index(drop=True)
    )


# =========================================================
# 업비트 Ticker 거래대금
# =========================================================

def get_upbit_ticker_volume_map(
    markets
):

    if not markets:

        return {}

    volume_map = {}

    chunk_size = 100

    chunks = [
        markets[i:i + chunk_size]
        for i in range(
            0,
            len(markets),
            chunk_size
        )
    ]

    for chunk_index, chunk in enumerate(
        chunks,
        start=1
    ):

        success = False

        while not success:

            try:

                market_string = ",".join(
                    chunk
                )

                url = (
                    "https://api.upbit.com/v1/ticker"
                    f"?markets={market_string}"
                )

                response = retry_request(
                    requests.get,
                    url,
                    timeout=15
                )

                if response is None:

                    time.sleep(2)

                    continue

                if response.status_code != 200:

                    time.sleep(2)

                    continue

                data = response.json()

                if not data:

                    time.sleep(2)

                    continue

                for item in data:

                    market = item.get(
                        "market"
                    )

                    volume = item.get(
                        "acc_trade_price_24h",
                        0
                    )

                    try:

                        volume = float(volume)

                    except Exception:

                        volume = 0

                    if market:

                        volume_map[
                            market
                        ] = volume

                success = True

            except Exception as e:

                logging.error(
                    f"업비트 Ticker 실패 "
                    f"청크 {chunk_index}: {e}"
                )

                time.sleep(2)

    return volume_map


# =========================================================
# 업비트 캔들
# =========================================================

def get_upbit_ohlcv(
    market,
    unit=60,
    count=200
):

    count = max(
        1,
        min(int(count), 200)
    )

    url = (
        "https://api.upbit.com/v1/candles/minutes/"
        f"{unit}"
        f"?market={market}"
        f"&count={count}"
    )

    response = retry_request(
        requests.get,
        url,
        timeout=15
    )

    if response is None:

        return None

    try:

        data = response.json()

        if not data:

            return None

        df = pd.DataFrame(data)

        df = (
            df.iloc[::-1]
            .reset_index(drop=True)
        )

        df["o"] = pd.to_numeric(
            df["opening_price"],
            errors="coerce"
        )

        df["h"] = pd.to_numeric(
            df["high_price"],
            errors="coerce"
        )

        df["l"] = pd.to_numeric(
            df["low_price"],
            errors="coerce"
        )

        df["c"] = pd.to_numeric(
            df["trade_price"],
            errors="coerce"
        )

        df["datetime"] = pd.to_datetime(
            df["candle_date_time_kst"]
        )

        return df

    except Exception as e:

        logging.error(
            f"업비트 캔들 오류 "
            f"{market} : {e}"
        )

        return None


# =========================================================
# 업비트 현재 진행 캔들 제거
# =========================================================

def remove_upbit_current_candle(
    df,
    unit
):

    if df is None or df.empty:

        return df

    now = pd.Timestamp.now(
        tz="Asia/Seoul"
    ).tz_localize(None)

    last_time = pd.Timestamp(
        df.iloc[-1]["datetime"]
    )

    elapsed_minutes = (
        now - last_time
    ).total_seconds() / 60

    # -----------------------------------------------------
    # 업비트 candle_date_time_kst는 캔들 시작시간
    # -----------------------------------------------------

    if elapsed_minutes < unit:

        return (
            df.iloc[:-1]
            .reset_index(drop=True)
        )

    return df.reset_index(
        drop=True
    )


# =========================================================
# 업비트 전체 목록
# =========================================================

def get_upbit_markets():

    response = retry_request(
        requests.get,
        "https://api.upbit.com/v1/market/all",
        timeout=15
    )

    if response is None:

        return []

    try:

        data = response.json()

        return [
            x["market"]
            for x in data
            if x["market"].startswith("KRW-")
        ]

    except Exception as e:

        logging.error(
            f"업비트 목록 오류 : {e}"
        )

        return []


# =========================================================
# 거래대금 표시
# =========================================================

def format_volume(volume):

    if volume >= 1_000_000_000_000:

        return (
            f"{volume / 1_000_000_000_000:.2f}조"
        )

    if volume >= 100_000_000:

        return (
            f"{volume / 100_000_000:,.0f}억"
        )

    return (
        f"{volume / 10_000:,.0f}만원"
    )


# =========================================================
# EMA
# =========================================================

def get_ema(
    df,
    column,
    period
):

    if (
        df is None
        or column not in df.columns
    ):

        return None

    price = pd.to_numeric(
        df[column],
        errors="coerce"
    )

    if price.notna().sum() < period:

        return None

    return price.ewm(
        span=period,
        adjust=False
    ).mean()


# =========================================================
# EMA 10-30-60
# =========================================================

def get_ema_direction(
    df,
    column="c"
):

    ema10 = get_ema(
        df,
        column,
        EMA_FAST
    )

    ema30 = get_ema(
        df,
        column,
        EMA_MID
    )

    ema60 = get_ema(
        df,
        column,
        EMA_SLOW
    )

    if (
        ema10 is None
        or ema30 is None
        or ema60 is None
    ):

        return "none"

    a = ema10.iloc[-1]
    b = ema30.iloc[-1]
    c = ema60.iloc[-1]

    if any(
        pd.isna(x)
        for x in [a, b, c]
    ):

        return "none"

    if a > b > c:

        return "long"

    if a < b < c:

        return "short"

    return "none"


# =========================================================
# EMA 카운트
# =========================================================

def get_ema_alignment_count(
    df,
    column="c"
):

    if (
        df is None
        or len(df) < EMA_SLOW
    ):

        return "none", 0

    ema10 = get_ema(
        df,
        column,
        EMA_FAST
    )

    ema30 = get_ema(
        df,
        column,
        EMA_MID
    )

    ema60 = get_ema(
        df,
        column,
        EMA_SLOW
    )

    if (
        ema10 is None
        or ema30 is None
        or ema60 is None
    ):

        return "none", 0

    direction = None

    count = 0

    for i in range(
        len(df) - 1,
        -1,
        -1
    ):

        a = ema10.iloc[i]
        b = ema30.iloc[i]
        c = ema60.iloc[i]

        if any(
            pd.isna(x)
            for x in [a, b, c]
        ):

            break

        if a > b > c:

            current = "long"

        elif a < b < c:

            current = "short"

        else:

            current = "none"

        if direction is None:

            if current == "none":

                break

            direction = current

            count = 1

        elif current == direction:

            count += 1

        else:

            break

    if direction is None:

        return "none", 0

    return direction, count


# =========================================================
# EMA 표시
# =========================================================

def check_ema(df):

    direction, count = (
        get_ema_alignment_count(
            df,
            "c"
        )
    )

    if direction == "long":

        display = f"🟢({count})"

    elif direction == "short":

        display = f"🔴({count})"

    else:

        display = "⚪"

    return {
        "display": display,
        "direction": direction,
        "count": count
    }


# =========================================================
# 당일 상승 / 하락
#
# KST 09:00 기준
# =========================================================

def get_today_change_upbit(
    market
):

    df = get_upbit_ohlcv(
        market,
        60,
        150
    )

    if df is None or df.empty:

        return None

    df = df.copy()

    df["datetime"] = pd.to_datetime(
        df["datetime"]
    )

    now = pd.Timestamp.now(
        tz="Asia/Seoul"
    ).tz_localize(None)

    today_09 = now.normalize() + pd.Timedelta(
        hours=9
    )

    if now < today_09:

        today_09 -= pd.Timedelta(
            days=1
        )

    before = df[
        df["datetime"] < today_09
    ]

    if before.empty:

        return None

    base = before.iloc[-1]["c"]

    current = df.iloc[-1]["c"]

    if base <= 0:

        return None

    change = (
        (current - base)
        /
        base
        *
        100
    )

    return float(change)


# =========================================================
# OKX 당일 변동률
# =========================================================

def get_today_change_okx(
    inst_id
):

    df = get_okx_ohlcv(
        inst_id,
        "1H",
        150
    )

    if df is None or df.empty:

        return None

    df = df.copy()

    df["datetime"] = (
        pd.to_datetime(
            df["ts"],
            unit="ms"
        )
        +
        pd.Timedelta(hours=9)
    )

    now = pd.Timestamp.now(
        tz="Asia/Seoul"
    ).tz_localize(None)

    today_09 = now.normalize() + pd.Timedelta(
        hours=9
    )

    if now < today_09:

        today_09 -= pd.Timedelta(
            days=1
        )

    before = df[
        df["datetime"] < today_09
    ]

    if before.empty:

        return None

    base = before.iloc[-1]["c"]

    current = df.iloc[-1]["c"]

    if base <= 0:

        return None

    return float(
        (current - base)
        /
        base
        *
        100
    )


# =========================================================
# 당일 표시
# =========================================================

def today_status(change):

    if change is None:

        return {
            "icon": "⬜",
            "direction": "none"
        }

    if change > 0:

        return {
            "icon": "☀️",
            "direction": "long"
        }

    if change < 0:

        return {
            "icon": "☁️",
            "direction": "short"
        }

    return {
        "icon": "⬜",
        "direction": "none"
    }


# =========================================================
# 최근 고점 돌파 양봉
#
# 상승:
# 현재 양봉 + 최근 N개 고점 돌파
#
# 하락:
# 현재 음봉 + 최근 N개 저점 이탈
# =========================================================

def is_long_breakout(
    df,
    index
):

    if index < BREAKOUT_LOOKBACK:

        return False

    row = df.iloc[index]

    previous = df.iloc[
        index - BREAKOUT_LOOKBACK:index
    ]

    if previous.empty:

        return False

    previous_high = pd.to_numeric(
        previous["h"],
        errors="coerce"
    ).max()

    if pd.isna(previous_high):

        return False

    return (
        row["c"] > row["o"]
        and
        row["h"] > previous_high
        and
        row["c"] > previous_high
    )


def is_short_breakout(
    df,
    index
):

    if index < BREAKOUT_LOOKBACK:

        return False

    row = df.iloc[index]

    previous = df.iloc[
        index - BREAKOUT_LOOKBACK:index
    ]

    if previous.empty:

        return False

    previous_low = pd.to_numeric(
        previous["l"],
        errors="coerce"
    ).min()

    if pd.isna(previous_low):

        return False

    return (
        row["c"] < row["o"]
        and
        row["l"] < previous_low
        and
        row["c"] < previous_low
    )


# =========================================================
# 돌파전
#
# 현재 고가가 최근 고점에 접근했지만
# 아직 종가 돌파는 안한 상태
# =========================================================

def is_long_pre_breakout(
    df,
    index
):

    if index < BREAKOUT_LOOKBACK:

        return False

    row = df.iloc[index]

    previous = df.iloc[
        index - BREAKOUT_LOOKBACK:index
    ]

    previous_high = pd.to_numeric(
        previous["h"],
        errors="coerce"
    ).max()

    if pd.isna(previous_high):

        return False

    return (
        row["c"] > row["o"]
        and
        row["h"] >= previous_high
        and
        row["c"] <= previous_high
    )


def is_short_pre_breakout(
    df,
    index
):

    if index < BREAKOUT_LOOKBACK:

        return False

    row = df.iloc[index]

    previous = df.iloc[
        index - BREAKOUT_LOOKBACK:index
    ]

    previous_low = pd.to_numeric(
        previous["l"],
        errors="coerce"
    ).min()

    if pd.isna(previous_low):

        return False

    return (
        row["c"] < row["o"]
        and
        row["l"] <= previous_low
        and
        row["c"] >= previous_low
    )


# =========================================================
# 상승 1파 → 조정 → 재돌파
#
# 단순히 최근 최고가를 한 번 찍은 것보다
# 직전 돌파 이후 조정이 존재하고
# 다시 이전 고점을 양봉으로 돌파하는 구조를 우선
# =========================================================

def detect_long_wave_breakout(
    df
):

    if (
        df is None
        or
        len(df) < EMA_SLOW + BREAKOUT_LOOKBACK + 5
    ):

        return False

    ema10 = get_ema(
        df,
        "c",
        EMA_FAST
    )

    ema30 = get_ema(
        df,
        "c",
        EMA_MID
    )

    ema60 = get_ema(
        df,
        "c",
        EMA_SLOW
    )

    if (
        ema10 is None
        or ema30 is None
        or ema60 is None
    ):

        return False

    i = len(df) - 1

    # -----------------------------------------------------
    # 현재 10-30-60 정배열
    # -----------------------------------------------------

    if not (
        ema10.iloc[i]
        >
        ema30.iloc[i]
        >
        ema60.iloc[i]
    ):

        return False

    # -----------------------------------------------------
    # 현재 양봉
    # -----------------------------------------------------

    current = df.iloc[i]

    if current["c"] <= current["o"]:

        return False

    # -----------------------------------------------------
    # 최근 고점
    # -----------------------------------------------------

    previous = df.iloc[
        i - BREAKOUT_LOOKBACK:i
    ]

    previous_high = pd.to_numeric(
        previous["h"],
        errors="coerce"
    ).max()

    if pd.isna(previous_high):

        return False

    if current["c"] <= previous_high:

        return False

    # -----------------------------------------------------
    # 과거 구간에서 조정 확인
    # -----------------------------------------------------

    look_start = max(
        EMA_SLOW,
        i - 15
    )

    pullback_found = False

    for j in range(
        look_start,
        i
    ):

        if (
            df.iloc[j]["c"]
            <
            df.iloc[j]["o"]
        ):

            pullback_found = True

            break

        if (
            j > look_start
            and
            df.iloc[j]["c"]
            <
            df.iloc[j - 1]["c"]
        ):

            pullback_found = True

            break

    return pullback_found


# =========================================================
# 숏 구조
#
# 하락 1파 → 반등 → 재하락
# =========================================================

def detect_short_wave_breakout(
    df
):

    if (
        df is None
        or
        len(df) < EMA_SLOW + BREAKOUT_LOOKBACK + 5
    ):

        return False

    ema10 = get_ema(
        df,
        "c",
        EMA_FAST
    )

    ema30 = get_ema(
        df,
        "c",
        EMA_MID
    )

    ema60 = get_ema(
        df,
        "c",
        EMA_SLOW
    )

    if (
        ema10 is None
        or ema30 is None
        or ema60 is None
    ):

        return False

    i = len(df) - 1

    if not (
        ema10.iloc[i]
        <
        ema30.iloc[i]
        <
        ema60.iloc[i]
    ):

        return False

    current = df.iloc[i]

    if current["c"] >= current["o"]:

        return False

    previous = df.iloc[
        i - BREAKOUT_LOOKBACK:i
    ]

    previous_low = pd.to_numeric(
        previous["l"],
        errors="coerce"
    ).min()

    if pd.isna(previous_low):

        return False

    if current["c"] >= previous_low:

        return False

    rebound_found = False

    look_start = max(
        EMA_SLOW,
        i - 15
    )

    for j in range(
        look_start,
        i
    ):

        if (
            df.iloc[j]["c"]
            >
            df.iloc[j]["o"]
        ):

            rebound_found = True

            break

        if (
            j > look_start
            and
            df.iloc[j]["c"]
            >
            df.iloc[j - 1]["c"]
        ):

            rebound_found = True

            break

    return rebound_found


# =========================================================
# 시간봉 돌파 상태
#
# pre = 돌파전
# 1   = 현재 최초 돌파
# none = 표시 안함
# =========================================================

def get_timeframe_breakout(
    df,
    direction
):

    if (
        df is None
        or
        len(df)
        <
        EMA_SLOW
        +
        BREAKOUT_LOOKBACK
        +
        2
    ):

        return "none"

    df = df.copy().reset_index(
        drop=True
    )

    # -----------------------------------------------------
    # 현재 캔들
    # -----------------------------------------------------

    current_index = len(df) - 1

    current = df.iloc[
        current_index
    ]

    # -----------------------------------------------------
    # EMA
    # -----------------------------------------------------

    ema10 = get_ema(
        df,
        "c",
        EMA_FAST
    )

    ema30 = get_ema(
        df,
        "c",
        EMA_MID
    )

    ema60 = get_ema(
        df,
        "c",
        EMA_SLOW
    )

    if (
        ema10 is None
        or ema30 is None
        or ema60 is None
    ):

        return "none"

    e10 = ema10.iloc[
        current_index
    ]

    e30 = ema30.iloc[
        current_index
    ]

    e60 = ema60.iloc[
        current_index
    ]

    if any(
        pd.isna(x)
        for x in [e10, e30, e60]
    ):

        return "none"

    # -----------------------------------------------------
    # LONG
    # -----------------------------------------------------

    if direction == "long":

        if not (
            e10 > e30 > e60
        ):

            return "none"

        # 현재 캔들 최초 돌파
        if is_long_breakout(
            df,
            current_index
        ):

            return "1"

        # 돌파전
        if is_long_pre_breakout(
            df,
            current_index
        ):

            return "pre"

        return "none"

    # -----------------------------------------------------
    # SHORT
    # -----------------------------------------------------

    if direction == "short":

        if not (
            e10 < e30 < e60
        ):

            return "none"

        if is_short_breakout(
            df,
            current_index
        ):

            return "1"

        if is_short_pre_breakout(
            df,
            current_index
        ):

            return "pre"

        return "none"

    return "none"


# =========================================================
# 1H + 4H 돌파 상태
# =========================================================

def get_combined_breakout_warning(
    df1h,
    df4h
):

    direction_1h = get_ema_direction(
        df1h,
        "c"
    )

    direction_4h = get_ema_direction(
        df4h,
        "c"
    )

    warning_1h = get_timeframe_breakout(
        df1h,
        direction_1h
    )

    warning_4h = get_timeframe_breakout(
        df4h,
        direction_4h
    )

    return {
        "1h": warning_1h,
        "4h": warning_4h,
        "direction_1h": direction_1h,
        "direction_4h": direction_4h
    }


# =========================================================
# 현재 돌파 경고 여부
# =========================================================

def is_visible_combined_warning(
    warning,
    today_direction,
    exchange
):

    if not warning:

        return False

    w1 = warning.get(
        "1h",
        "none"
    )

    w4 = warning.get(
        "4h",
        "none"
    )

    # -----------------------------------------------------
    # 업비트
    # 양수 + LONG만
    # -----------------------------------------------------

    if exchange == "upbit":

        if today_direction != "long":

            return False

        return (
            w1 in ("pre", "1")
            and
            warning.get(
                "direction_1h"
            ) == "long"
        ) or (
            w4 in ("pre", "1")
            and
            warning.get(
                "direction_4h"
            ) == "long"
        )

    # -----------------------------------------------------
    # OKX
    # 양수 LONG
    # 음수 SHORT
    # -----------------------------------------------------

    if exchange == "okx":

        if today_direction == "long":

            return (
                w1 in ("pre", "1")
                and
                warning.get(
                    "direction_1h"
                ) == "long"
            ) or (
                w4 in ("pre", "1")
                and
                warning.get(
                    "direction_4h"
                ) == "long"
            )

        if today_direction == "short":

            return (
                w1 in ("pre", "1")
                and
                warning.get(
                    "direction_1h"
                ) == "short"
            ) or (
                w4 in ("pre", "1")
                and
                warning.get(
                    "direction_4h"
                ) == "short"
            )

    return False


# =========================================================
# 경고 HTML
#
# 🚨만 표시
# 로켓 제거
# =========================================================

def combined_warning_html(
    warning,
    today_direction,
    exchange
):

    if not warning:

        return ""

    w1 = warning.get(
        "1h",
        "none"
    )

    w4 = warning.get(
        "4h",
        "none"
    )

    result = []

    # -----------------------------------------------------
    # LONG
    # -----------------------------------------------------

    if today_direction == "long":

        if (
            w1 in ("pre", "1")
            and
            warning.get(
                "direction_1h"
            ) == "long"
        ):

            result.append(
                '<span class="alarm long-alarm">'
                '🚨1H'
                '</span>'
            )

        if (
            w4 in ("pre", "1")
            and
            warning.get(
                "direction_4h"
            ) == "long"
        ):

            result.append(
                '<span class="alarm long-alarm">'
                '🚨4H'
                '</span>'
            )

    # -----------------------------------------------------
    # SHORT
    # -----------------------------------------------------

    elif today_direction == "short":

        if (
            w1 in ("pre", "1")
            and
            warning.get(
                "direction_1h"
            ) == "short"
        ):

            result.append(
                '<span class="alarm short-alarm">'
                '🚨1H'
                '</span>'
            )

        if (
            w4 in ("pre", "1")
            and
            warning.get(
                "direction_4h"
            ) == "short"
        ):

            result.append(
                '<span class="alarm short-alarm">'
                '🚨4H'
                '</span>'
            )

    if not result:

        return ""

    return " ".join(result)


# =========================================================
# 오늘 변동률 표시
# =========================================================

def format_today_change(
    change
):

    if change is None:

        return "⬜ N/A"

    if change > 0:

        return (
            f"☀️ +{change:.2f}%"
        )

    if change < 0:

        return (
            f"☁️ {change:.2f}%"
        )

    return (
        f"⬜ {change:.2f}%"
    )


# =========================================================
# LONG / SHORT 표시
# =========================================================

def direction_html(
    direction
):

    if direction == "long":

        return (
            '<span class="direction-long">'
            'LONG'
            '</span>'
        )

    if direction == "short":

        return (
            '<span class="direction-short">'
            'SHORT'
            '</span>'
        )

    return (
        '<span class="direction-none">'
        '-'
        '</span>'
    )


# =========================================================
# EMA HTML
#
# 모바일에서 잘리지 않도록 두 줄
# =========================================================

def ema_html(
    ema_1h,
    ema_4h
):

    return f"""
    <div class="ema-box">

        <div class="ema-line">
            <span class="ema-label">
                1H
            </span>
            <span>
                {ema_1h}
            </span>
        </div>

        <div class="ema-line">
            <span class="ema-label">
                4H
            </span>
            <span>
                {ema_4h}
            </span>
        </div>

    </div>
    """


# =========================================================
# 빈 EMA
# =========================================================

def empty_ema():

    return {
        "display": "⚪",
        "direction": "none",
        "count": 0
    }


# =========================================================
# 업비트 EMA / 돌파
#
# 현재 진행 1H / 4H 캔들 사용
# 1분 조회 시 현재 상태 반영
# =========================================================

def get_upbit_ema(
    market
):

    raw1h = get_upbit_ohlcv(
        market,
        60,
        200
    )

    raw4h = get_upbit_ohlcv(
        market,
        240,
        200
    )

    if (
        raw1h is None
        or raw4h is None
    ):

        return {
            "1h_ema": empty_ema(),
            "4h_ema": empty_ema(),
            "warning": {
                "1h": "none",
                "4h": "none"
            }
        }

    df1h = raw1h.copy()

    df4h = raw4h.copy()

    # -----------------------------------------------------
    # EMA는 확정봉 기준
    # -----------------------------------------------------

    ema_df1h = (
        remove_upbit_current_candle(
            df1h,
            60
        )
    )

    ema_df4h = (
        remove_upbit_current_candle(
            df4h,
            240
        )
    )

    ema1h = check_ema(
        ema_df1h
    )

    ema4h = check_ema(
        ema_df4h
    )

    # -----------------------------------------------------
    # 돌파는 현재 진행봉 포함
    # -----------------------------------------------------

    warning = get_combined_breakout_warning(
        df1h,
        df4h
    )

    return {
        "1h_ema": ema1h,
        "4h_ema": ema4h,
        "warning": warning
    }


# =========================================================
# OKX EMA
# =========================================================

def get_okx_ema(
    inst_id
):

    df1h = get_okx_ohlcv(
        inst_id,
        "1H",
        200
    )

    df4h = get_okx_ohlcv(
        inst_id,
        "4H",
        200
    )

    if (
        df1h is None
        or df4h is None
    ):

        return {
            "1h_ema": empty_ema(),
            "4h_ema": empty_ema(),
            "warning": {
                "1h": "none",
                "4h": "none"
            }
        }

    confirmed1h = (
        get_okx_confirmed_ohlcv(
            inst_id,
            "1H",
            200
        )
    )

    confirmed4h = (
        get_okx_confirmed_ohlcv(
            inst_id,
            "4H",
            200
        )
    )

    if confirmed1h is None:

        confirmed1h = df1h.copy()

    if confirmed4h is None:

        confirmed4h = df4h.copy()

    ema1h = check_ema(
        confirmed1h
    )

    ema4h = check_ema(
        confirmed4h
    )

    # -----------------------------------------------------
    # 돌파는 현재 진행봉 포함
    # -----------------------------------------------------

    warning = get_combined_breakout_warning(
        df1h,
        df4h
    )

    return {
        "1h_ema": ema1h,
        "4h_ema": ema4h,
        "warning": warning
    }


# =========================================================
# OKX 거래대금
# =========================================================

def get_okx_volume(
    inst_id,
    usdt_krw
):

    hours = max(
        1,
        min(
            int(VOLUME_HOURS),
            200
        )
    )

    while True:

        try:

            df = get_okx_confirmed_ohlcv(
                inst_id,
                "1H",
                hours + 1
            )

            if (
                df is None
                or
                df.empty
            ):

                time.sleep(
                    OKX_RETRY_DELAY
                )

                continue

            if len(df) < hours:

                time.sleep(
                    OKX_RETRY_DELAY
                )

                continue

            volume_usdt = float(
                df["volCcyQuote"]
                .tail(hours)
                .sum()
            )

            # OKX volCcyQuote 구조에 맞춘 기존 환산 유지
            volume_usdt = (
                volume_usdt / 10
            )

            if volume_usdt <= 0:

                time.sleep(
                    OKX_RETRY_DELAY
                )

                continue

            volume_krw = (
                volume_usdt
                *
                usdt_krw
            )

            if volume_krw <= 0:

                time.sleep(
                    OKX_RETRY_DELAY
                )

                continue

            return volume_krw

        except Exception as e:

            logging.error(
                f"OKX 거래대금 오류 "
                f"{inst_id} : {e}"
            )

            time.sleep(
                OKX_RETRY_DELAY
            )


# =========================================================
# OKX 전체 목록
# =========================================================

def get_all_okx_swap_symbols():

    url = (
        "https://www.okx.com/api/v5/"
        "public/instruments?instType=SWAP"
    )

    response = retry_request(
        requests.get,
        url,
        timeout=15
    )

    if response is None:

        return []

    try:

        data = response.json().get(
            "data",
            []
        )

        return [
            x["instId"]
            for x in data
            if (
                x["instId"].endswith(
                    "-USDT-SWAP"
                )
                and
                x.get("state") == "live"
            )
        ]

    except Exception as e:

        logging.error(
            f"OKX 목록 오류 : {e}"
        )

        return []


# =========================================================
# 업비트 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data

    logging.info(
        f"========== 업비트 TOP{TOP_N} 시작 =========="
    )

    markets = get_upbit_markets()

    if not markets:

        return False

    volume_map = (
        get_upbit_ticker_volume_map(
            markets
        )
    )

    if not volume_map:

        return False

    top_markets = sorted(
        volume_map,
        key=volume_map.get,
        reverse=True
    )[:TOP_N]

    rows = []

    for rank, market in enumerate(
        top_markets,
        start=1
    ):

        coin = market.replace(
            "KRW-",
            ""
        )

        try:

            change = (
                get_today_change_upbit(
                    market
                )
            )

            today = today_status(
                change
            )

            ema = get_upbit_ema(
                market
            )

            warning = ema.get(
                "warning",
                {}
            )

            # -------------------------------------------------
            # 업비트:
            # 당일 양수 + LONG 조건만
            # -------------------------------------------------

            if not is_visible_combined_warning(
                warning,
                today["direction"],
                "upbit"
            ):

                continue

            warning_html = (
                combined_warning_html(
                    warning,
                    today["direction"],
                    "upbit"
                )
            )

            rows.append(
                {
                    "rank": rank,

                    "name": coin,

                    "change": (
                        format_today_change(
                            change
                        )
                    ),

                    "change_percent": change,

                    "volume": (
                        format_volume(
                            volume_map[market]
                        )
                    ),

                    "direction": "long",

                    "direction_html": (
                        direction_html(
                            "long"
                        )
                    ),

                    "ema_1h": ema[
                        "1h_ema"
                    ],

                    "ema_4h": ema[
                        "4h_ema"
                    ],

                    "warning": warning,

                    "warning_html":
                        warning_html
                }
            )

        except Exception as e:

            logging.error(
                f"업비트 상세 오류 "
                f"{market} : {e}"
            )

    latest_upbit_data = rows

    logging.info(
        f"업비트 경고 종목 "
        f"{len(rows)}개"
    )

    logging.info(
        f"========== 업비트 TOP{TOP_N} 완료 =========="
    )

    return True


# =========================================================
# OKX 업데이트
# =========================================================

def update_okx(
    usdt_krw
):

    global latest_okx_data

    if (
        usdt_krw is None
        or
        usdt_krw <= 0
    ):

        return False

    symbols = (
        get_all_okx_swap_symbols()
    )

    if not symbols:

        return False

    upbit_markets = (
        get_upbit_markets()
    )

    upbit_coin_set = {
        market.replace(
            "KRW-",
            ""
        )
        for market in upbit_markets
    }

    volume_map = {}

    for symbol in symbols:

        while True:

            try:

                volume = get_okx_volume(
                    symbol,
                    usdt_krw
                )

                if volume > 0:

                    volume_map[
                        symbol
                    ] = volume

                    break

            except Exception as e:

                logging.error(
                    f"OKX 거래대금 실패 "
                    f"{symbol} : {e}"
                )

            time.sleep(
                OKX_RETRY_DELAY
            )

    if not volume_map:

        return False

    top_symbols = sorted(
        volume_map,
        key=volume_map.get,
        reverse=True
    )[:TOP_N]

    rows = []

    for rank, symbol in enumerate(
        top_symbols,
        start=1
    ):

        coin = symbol.replace(
            "-USDT-SWAP",
            ""
        )

        if coin in upbit_coin_set:

            coin_display = (
                f"{coin}[UP]"
            )

        else:

            coin_display = coin

        try:

            change = (
                get_today_change_okx(
                    symbol
                )
            )

            today = today_status(
                change
            )

            ema = get_okx_ema(
                symbol
            )

            warning = ema.get(
                "warning",
                {}
            )

            # -------------------------------------------------
            # OKX:
            # 양수 + LONG
            # 음수 + SHORT
            # -------------------------------------------------

            if not is_visible_combined_warning(
                warning,
                today["direction"],
                "okx"
            ):

                continue

            direction = (
                "long"
                if today["direction"] == "long"
                else
                "short"
            )

            warning_html = (
                combined_warning_html(
                    warning,
                    today["direction"],
                    "okx"
                )
            )

            rows.append(
                {
                    "rank": rank,

                    "name": coin_display,

                    "change": (
                        format_today_change(
                            change
                        )
                    ),

                    "change_percent": change,

                    "volume": (
                        format_volume(
                            volume_map[symbol]
                        )
                    ),

                    "direction": direction,

                    "direction_html": (
                        direction_html(
                            direction
                        )
                    ),

                    "ema_1h": ema[
                        "1h_ema"
                    ],

                    "ema_4h": ema[
                        "4h_ema"
                    ],

                    "warning": warning,

                    "warning_html":
                        warning_html
                }
            )

        except Exception as e:

            logging.error(
                f"OKX 상세 오류 "
                f"{symbol} : {e}"
            )

    latest_okx_data = rows

    logging.info(
        f"OKX 경고 종목 "
        f"{len(rows)}개"
    )

    return True


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    global latest_usdt_krw
    global latest_upbit_data
    global latest_okx_data

    logging.info(
        "========================================"
    )

    logging.info(
        "전체 조회 시작"
    )

    # =====================================================
    # 업비트
    # =====================================================

    if USE_UPBIT == "Y":

        try:

            update_upbit()

        except Exception as e:

            logging.exception(
                f"업비트 업데이트 오류 : {e}"
            )

    else:

        latest_upbit_data = []

    # =====================================================
    # OKX
    # =====================================================

    if USE_OKX == "Y":

        try:

            usdt_krw = get_usdt_krw()

            if usdt_krw is not None:

                latest_usdt_krw = usdt_krw

            else:

                usdt_krw = latest_usdt_krw

            update_okx(
                usdt_krw
            )

        except Exception as e:

            logging.exception(
                f"OKX 업데이트 오류 : {e}"
            )

    else:

        latest_okx_data = []

    logging.info(
        "전체 조회 완료"
    )


# =========================================================
# 스케줄러
# =========================================================

def scheduler():

    while True:

        try:

            schedule.run_pending()

        except Exception as e:

            logging.exception(
                f"스케줄러 오류 : {e}"
            )

        time.sleep(1)


# =========================================================
# CSS
# =========================================================

DASHBOARD_CSS = """

* {
    box-sizing: border-box;
}

html,
body {

    margin: 0;
    padding: 0;
    width: 100%;
    max-width: 100%;
    overflow-x: hidden;
}

body {

    background: #0f1115;
    color: #eeeeee;

    font-family:
        Arial,
        "Noto Sans KR",
        sans-serif;

    font-size: 9px;

    padding: 5px;
}

h1 {

    margin: 2px 2px 5px 2px;

    font-size: 14px;

    line-height: 1.2;
}

h2 {

    margin: 10px 2px 5px 2px;

    font-size: 11px;

    line-height: 1.2;
}

.info {

    margin: 0 2px 6px 2px;

    padding: 5px 6px;

    color: #858b94;

    background: #171a1f;

    border: 1px solid #252a31;

    border-radius: 7px;

    font-size: 7px;

    line-height: 1.5;
}

.exchange-status {

    display: flex;

    gap: 8px;

    margin-top: 4px;

    font-size: 7px;

    font-weight: 700;
}

.status-y {

    color: #35e66d;
}

.status-n {

    color: #ff4d4d;
}

.table-wrap {

    width: 100%;

    overflow: hidden;

    border-radius: 8px;

    border: 1px solid #252a31;
}

table {

    width: 100%;

    table-layout: fixed;

    border-collapse: collapse;

    background: #181c21;
}

th {

    padding: 5px 1px;

    background: #12151a;

    border-bottom:
        1px solid #2b3037;

    color: #8f949d;

    font-size: 7px;

    text-align: center;
}

td {

    padding: 5px 1px;

    border-bottom:
        1px solid #272c32;

    text-align: center;

    vertical-align: middle;
}


/* =====================================================
   컬럼
   ===================================================== */

th:nth-child(1),
td:nth-child(1) {

    width: 7%;
}

th:nth-child(2),
td:nth-child(2) {

    width: 20%;
}

th:nth-child(3),
td:nth-child(3) {

    width: 15%;
}

th:nth-child(4),
td:nth-child(4) {

    width: 20%;
}

th:nth-child(5),
td:nth-child(5) {

    width: 20%;
}

th:nth-child(6),
td:nth-child(6) {

    width: 18%;
}


/* =====================================================
   코인
   ===================================================== */

.coin {

    display: block;

    font-size: 9px;

    font-weight: 700;

    line-height: 1.2;
}

.today-status {

    margin-top: 3px;

    font-size: 8px;

    line-height: 1.2;

    white-space: nowrap;
}


/* =====================================================
   거래대금
   ===================================================== */

.volume-value {

    display: block;

    font-size: 8px;

    font-weight: 600;

    line-height: 1.2;
}

.direction-wrap {

    margin-top: 3px;

    line-height: 1.2;
}

.direction-long {

    color: #35e66d;

    font-size: 8px;

    font-weight: 800;
}

.direction-short {

    color: #ff4d4d;

    font-size: 8px;

    font-weight: 800;
}

.direction-none {

    color: #777;

    font-size: 8px;
}


/* =====================================================
   오늘
   ===================================================== */

.today-wrap {

    display: flex;

    flex-direction: column;

    align-items: center;

    justify-content: center;

    gap: 4px;

    width: 100%;
}


/* =====================================================
   🚨 알람
   ===================================================== */

.breakout-warning {

    display: flex;

    justify-content: center;

    align-items: center;

    gap: 4px;

    width: 100%;

    min-height: 14px;

    white-space: nowrap;
}

.alarm {

    display: inline-block;

    font-size: 9px;

    font-weight: 800;

    line-height: 1;

    transform-origin: center;
}


/* =====================================================
   LONG 반짝임
   ===================================================== */

.long-alarm {

    color: #35e66d;

    animation:
        longFlash
        0.9s
        ease-in-out
        infinite;
}

@keyframes longFlash {

    0% {

        opacity: 0.35;

        transform:
            scale(0.95);
    }

    50% {

        opacity: 1;

        transform:
            scale(1.08);
    }

    100% {

        opacity: 0.35;

        transform:
            scale(0.95);
    }
}


/* =====================================================
   SHORT 반짝임
   ===================================================== */

.short-alarm {

    color: #ff4d4d;

    animation:
        shortFlash
        0.9s
        ease-in-out
        infinite;
}

@keyframes shortFlash {

    0% {

        opacity: 0.35;

        transform:
            scale(0.95);
    }

    50% {

        opacity: 1;

        transform:
            scale(1.08);
    }

    100% {

        opacity: 0.35;

        transform:
            scale(0.95);
    }
}


/* =====================================================
   EMA
   ===================================================== */

.ema-box {

    width: 100%;

    display: flex;

    flex-direction: column;

    align-items: center;

    justify-content: center;

    gap: 3px;
}

.ema-line {

    width: 100%;

    display: flex;

    align-items: center;

    justify-content: center;

    gap: 3px;

    font-size: 8px;

    font-weight: 700;

    line-height: 1.2;

    white-space: nowrap;
}

.ema-label {

    color: #777e88;

    font-size: 7px;

    font-weight: 700;
}


/* =====================================================
   설명
   ===================================================== */

.note {

    color: #626872;

    font-size: 7px;

    line-height: 1.45;

    margin:
        4px 2px 7px 2px;
}


/* =====================================================
   모바일
   ===================================================== */

@media (max-width: 480px) {

    body {

        padding: 3px;

        font-size: 8px;
    }

    h1 {

        font-size: 13px;
    }

    h2 {

        font-size: 10px;
    }

    .info {

        font-size: 6px;

        padding: 4px 5px;
    }

    th {

        padding: 4px 1px;

        font-size: 6px;
    }

    td {

        padding: 4px 1px;
    }

    .coin {

        font-size: 8px;
    }

    .today-status {

        font-size: 7px;
    }

    .volume-value {

        font-size: 7px;
    }

    .direction-long,
    .direction-short,
    .direction-none {

        font-size: 7px;
    }

    .alarm {

        font-size: 8px;
    }

    .ema-line {

        font-size: 7px;
    }

    .ema-label {

        font-size: 6px;
    }

    .note {

        font-size: 6px;
    }
}

"""


# =========================================================
# 테이블 행
# =========================================================

def make_table_rows(
    data
):

    rows_html = ""

    for item in data:

        rows_html += f"""

<tr>

<td>
{item.get("rank", "-")}
</td>


<td>

<span class="coin">
{item["name"]}
</span>

<div class="today-status">
{item["change"]}
</div>

</td>


<td>

<span class="volume-value">
{item["volume"]}
</span>

<div class="direction-wrap">
{item["direction_html"]}
</div>

</td>


<td>

<div class="today-wrap">

<div class="breakout-warning">

{item.get("warning_html", "")}

</div>

</div>

</td>


<td>

{ema_html(
    item["ema_1h"].get(
        "display",
        "⚪"
    ),
    item["ema_4h"].get(
        "display",
        "⚪"
    )
)}

</td>


</tr>

"""

    return rows_html


# =========================================================
# 거래소 섹션
# =========================================================

def make_exchange_section(
    title,
    data
):

    rows = make_table_rows(
        data
    )

    if not rows:

        rows = """

<tr>

<td
    colspan="5"
    style="
        color:#555;
        padding:12px 4px;
    "
>

현재 🚨 종목 없음

</td>

</tr>

"""

    return f"""

<div class="section">

<h2>
{title} TOP{TOP_N} 경고
</h2>


<div class="table-wrap">

<table>

<thead>

<tr>

<th>#</th>

<th>코인</th>

<th>거래대금</th>

<th>돌파</th>

<th>EMA</th>

</tr>

</thead>


<tbody>

{rows}

</tbody>

</table>

</div>


<div class="note">

※ TOP{TOP_N} 실제 거래대금 순위 기준<br>

※ 코인명 아래 ☀️ = 당일 양수 / ☁️ = 당일 음수<br>

※ 거래대금 아래 LONG = 상승 조건 / SHORT = 하락 조건<br>

※ EMA = 10-30-60 정배열 / 역배열<br>

※ EMA 순서 = 1H → 4H<br>

※ 🚨1H = 1시간 돌파 조건<br>

※ 🚨4H = 4시간 돌파 조건<br>

※ 1H 또는 4H 중 하나라도 조건이 맞으면 표시<br>

※ 업비트 = 당일 양수 + LONG만 표시<br>

※ OKX = 당일 양수 LONG / 당일 음수 SHORT 표시<br>

※ 돌파 = 최근 {BREAKOUT_LOOKBACK}개 캔들 고가/저가 돌파<br>

※ 현재 진행 캔들은 1분마다 상태 확인<br>

※ 추가 돌파 캔들은 🚀 없이 경고 리스트에서 제외

</div>

</div>

"""


# =========================================================
# 대시보드
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def dashboard():

    upbit_status_class = (
        "status-y"
        if USE_UPBIT == "Y"
        else "status-n"
    )

    okx_status_class = (
        "status-y"
        if USE_OKX == "Y"
        else "status-n"
    )

    exchange_sections = ""

    if USE_UPBIT == "Y":

        exchange_sections += (
            make_exchange_section(
                "🏆 업비트",
                latest_upbit_data
            )
        )

    if USE_OKX == "Y":

        exchange_sections += (
            make_exchange_section(
                "🏆 OKX",
                latest_okx_data
            )
        )

    html = f"""

<!DOCTYPE html>

<html lang="ko">

<head>

<meta charset="UTF-8">

<meta name="viewport"
      content="
        width=device-width,
        initial-scale=1.0,
        maximum-scale=1.0,
        user-scalable=no">

<meta http-equiv="refresh"
      content="60">

<title>
Breakout Trading
</title>

<style>

{DASHBOARD_CSS}

</style>

</head>


<body>


<h1>
📊 Breakout Trading
</h1>


<div class="info">

<div>
1H + 4H 추세 · 1H + 4H 돌파
</div>

<div>
10-30-60 EMA 정배열 / 역배열
</div>

<div>
최근 {BREAKOUT_LOOKBACK}개 캔들 고가 / 저가 돌파
</div>

<div>
1분마다 현재 상태 조회 · 🚨 돌파 경고
</div>


<div class="exchange-status">

<span class="{upbit_status_class}">
업비트 : {USE_UPBIT}
</span>

<span class="{okx_status_class}">
OKX : {USE_OKX}
</span>

</div>

</div>


{exchange_sections}


</body>

</html>

"""

    return html


# =========================================================
# 시작
# =========================================================

@app.on_event(
    "startup"
)
def startup():

    logging.info(
        "서버 시작"
    )

    logging.info(
        f"조회 설정 "
        f"업비트={USE_UPBIT} "
        f"OKX={USE_OKX}"
    )

    logging.info(
        f"EMA = "
        f"{EMA_FAST}-"
        f"{EMA_MID}-"
        f"{EMA_SLOW}"
    )

    logging.info(
        f"BREAKOUT_LOOKBACK = "
        f"{BREAKOUT_LOOKBACK}"
    )

    if USE_UPBIT not in (
        "Y",
        "N"
    ):

        raise ValueError(
            "USE_UPBIT은 Y 또는 N만 사용할 수 있습니다."
        )

    if USE_OKX not in (
        "Y",
        "N"
    ):

        raise ValueError(
            "USE_OKX는 Y 또는 N만 사용할 수 있습니다."
        )

    # =====================================================
    # 최초 즉시 조회
    # =====================================================

    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    # =====================================================
    # 1분마다 조회
    # =====================================================

    schedule.every(
        UPDATE_MINUTES
    ).minutes.do(
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
