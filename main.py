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
# 핵심 돌파 설정
# ---------------------------------------------------------

# 첫 번째 상승 고점을 찾을 때 사용하는 최소 과거 캔들 수
BREAKOUT_LOOKBACK = 10

# 첫 번째 고점 이후 최소 조정 캔들 수
PULLBACK_MIN_CANDLES = 1

# 30-60-120 EMA 기준
EMA_FAST = 30
EMA_MID = 60
EMA_SLOW = 120


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
# OKX 실패 종목 반복 설정
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
# include_current=True
# 현재 진행 중인 캔들도 사용
# =========================================================

def get_okx_ohlcv(
    inst_id,
    bar="1H",
    limit=200,
    include_current=True
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

        if not include_current:

            df = df[
                df["confirm"].astype(str) == "1"
            ]

        if df.empty:
            return None

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

                        volume = float(
                            volume
                        )

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
#
# 현재 진행 중인 캔들 포함
# =========================================================

def get_upbit_ohlcv(
    market,
    unit=240,
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

        return df

    except Exception as e:

        logging.error(
            f"업비트 캔들 오류 "
            f"{market} : {e}"
        )

        return None


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

    elif volume >= 100_000_000:

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
# EMA 30-60-120 방향
# =========================================================

def get_ema_30_60_120_direction(
    df,
    column="c"
):

    if df is None:

        return "none"

    if len(df) < EMA_SLOW:

        return "none"

    ema30 = get_ema(
        df,
        column,
        EMA_FAST
    )

    ema60 = get_ema(
        df,
        column,
        EMA_MID
    )

    ema120 = get_ema(
        df,
        column,
        EMA_SLOW
    )

    if (
        ema30 is None
        or ema60 is None
        or ema120 is None
    ):

        return "none"

    a = ema30.iloc[-1]
    b = ema60.iloc[-1]
    c = ema120.iloc[-1]

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
# EMA 정렬 연속 캔들 수
# =========================================================

def get_ema_alignment_count(
    df,
    column="c"
):

    if (
        df is None
        or
        len(df) < EMA_SLOW
    ):

        return "none", 0

    ema30 = get_ema(
        df,
        column,
        EMA_FAST
    )

    ema60 = get_ema(
        df,
        column,
        EMA_MID
    )

    ema120 = get_ema(
        df,
        column,
        EMA_SLOW
    )

    if (
        ema30 is None
        or ema60 is None
        or ema120 is None
    ):

        return "none", 0

    direction = None

    count = 0

    for i in range(
        len(df) - 1,
        -1,
        -1
    ):

        a = ema30.iloc[i]
        b = ema60.iloc[i]
        c = ema120.iloc[i]

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

def check_ema(
    df
):

    direction, count = (
        get_ema_alignment_count(
            df,
            "c"
        )
    )

    if direction == "long":

        display = (
            f"🟢({count})"
        )

    elif direction == "short":

        display = (
            f"🔴({count})"
        )

    else:

        display = "⚪"

    return {
        "display": display,
        "direction": direction,
        "count": count
    }


# =========================================================
# 양봉 / 음봉
# =========================================================

def is_bullish(row):

    try:

        return float(row["c"]) > float(row["o"])

    except Exception:

        return False


def is_bearish(row):

    try:

        return float(row["c"]) < float(row["o"])

    except Exception:

        return False


# =========================================================
# 상승 1파의 첫 번째 고점 찾기
#
# 현재 진행 캔들을 제외한 과거에서
# 가장 최근에 만들어진 의미 있는 상승 고점을 찾는다.
#
# 고점 이후 최소 1개 이상 조정 캔들이 있어야 한다.
# =========================================================

def find_first_up_high(
    df,
    direction
):

    if (
        df is None
        or
        len(df) < BREAKOUT_LOOKBACK + 5
    ):

        return None

    if direction != "long":

        return None

    highs = pd.to_numeric(
        df["h"],
        errors="coerce"
    )

    lows = pd.to_numeric(
        df["l"],
        errors="coerce"
    )

    closes = pd.to_numeric(
        df["c"],
        errors="coerce"
    )

    opens = pd.to_numeric(
        df["o"],
        errors="coerce"
    )

    # -----------------------------------------------------
    # 현재 캔들은 돌파 검사에 사용하지만
    # 기준 고점 탐색에서는 제외
    # -----------------------------------------------------

    last_index = len(df) - 2

    if last_index < BREAKOUT_LOOKBACK:

        return None

    # -----------------------------------------------------
    # 최근 고점부터 과거 방향으로 탐색
    # -----------------------------------------------------

    for pivot in range(
        last_index - 1,
        BREAKOUT_LOOKBACK - 1,
        -1
    ):

        pivot_high = highs.iloc[pivot]

        if pd.isna(pivot_high):

            continue

        left = highs.iloc[
            pivot - BREAKOUT_LOOKBACK:
            pivot
        ]

        right_end = min(
            pivot + BREAKOUT_LOOKBACK + 1,
            len(df) - 1
        )

        right = highs.iloc[
            pivot + 1:
            right_end
        ]

        if left.empty or right.empty:

            continue

        left_max = left.max()

        right_max = right.max()

        # -------------------------------------------------
        # 주변보다 높은 고점
        # -------------------------------------------------

        if pivot_high < left_max:

            continue

        if pivot_high < right_max:

            continue

        # -------------------------------------------------
        # 고점 전 상승 확인
        # -------------------------------------------------

        pre_start = max(
            0,
            pivot - 5
        )

        pre_closes = closes.iloc[
            pre_start:pivot
        ]

        if len(pre_closes) < 2:

            continue

        if (
            pre_closes.iloc[-1]
            <=
            pre_closes.iloc[0]
        ):

            continue

        # -------------------------------------------------
        # 고점 이후 조정 확인
        # -------------------------------------------------

        pullback_end = min(
            len(df) - 1,
            pivot + PULLBACK_MIN_CANDLES + 1
        )

        pullback = lows.iloc[
            pivot + 1:
            pullback_end
        ]

        if len(pullback) < PULLBACK_MIN_CANDLES:

            continue

        if pullback.empty:

            continue

        # 고점보다 낮은 저점이 있어야 조정
        if pullback.min() >= pivot_high:

            continue

        return {
            "index": pivot,
            "high": float(pivot_high)
        }

    return None


# =========================================================
# 상승 돌파
#
# 조건
# 1. 30-60-120 정배열
# 2. 상승 1파 첫 고점 존재
# 3. 그 후 조정 발생
# 4. 현재 캔들이 첫 고점 돌파
# 5. 현재 캔들은 양봉
# =========================================================

def detect_long_breakout(
    df
):

    if (
        df is None
        or
        len(df) < EMA_SLOW + BREAKOUT_LOOKBACK
    ):

        return False

    direction = (
        get_ema_30_60_120_direction(
            df,
            "c"
        )
    )

    if direction != "long":

        return False

    current = df.iloc[-1]

    if not is_bullish(current):

        return False

    pivot = find_first_up_high(
        df,
        "long"
    )

    if pivot is None:

        return False

    pivot_high = pivot["high"]

    current_close = float(
        current["c"]
    )

    current_high = float(
        current["h"]
    )

    # -----------------------------------------------------
    # 현재 캔들이 첫 상승 고점을 돌파
    # -----------------------------------------------------

    if current_high <= pivot_high:

        return False

    if current_close <= pivot_high:

        return False

    return True


# =========================================================
# 하락 1파 첫 저점
# =========================================================

def find_first_down_low(
    df,
    direction
):

    if (
        df is None
        or
        len(df) < BREAKOUT_LOOKBACK + 5
    ):

        return None

    if direction != "short":

        return None

    lows = pd.to_numeric(
        df["l"],
        errors="coerce"
    )

    closes = pd.to_numeric(
        df["c"],
        errors="coerce"
    )

    last_index = len(df) - 2

    if last_index < BREAKOUT_LOOKBACK:

        return None

    for pivot in range(
        last_index - 1,
        BREAKOUT_LOOKBACK - 1,
        -1
    ):

        pivot_low = lows.iloc[pivot]

        if pd.isna(pivot_low):

            continue

        left = lows.iloc[
            pivot - BREAKOUT_LOOKBACK:
            pivot
        ]

        right_end = min(
            pivot + BREAKOUT_LOOKBACK + 1,
            len(df) - 1
        )

        right = lows.iloc[
            pivot + 1:
            right_end
        ]

        if left.empty or right.empty:

            continue

        if pivot_low > left.min():

            continue

        if pivot_low > right.min():

            continue

        pre_start = max(
            0,
            pivot - 5
        )

        pre_closes = closes.iloc[
            pre_start:pivot
        ]

        if len(pre_closes) < 2:

            continue

        if (
            pre_closes.iloc[-1]
            >=
            pre_closes.iloc[0]
        ):

            continue

        pullback_end = min(
            len(df) - 1,
            pivot + PULLBACK_MIN_CANDLES + 1
        )

        pullback = lows.iloc[
            pivot + 1:
            pullback_end
        ]

        # 하락 후 반등 확인을 위해
        # 이후 고점이 저점보다 높아야 함
        highs = pd.to_numeric(
            df["h"],
            errors="coerce"
        )

        if pullback.empty:

            continue

        future_highs = highs.iloc[
            pivot + 1:
            pullback_end
        ]

        if future_highs.empty:

            continue

        if future_highs.max() <= pivot_low:

            continue

        return {
            "index": pivot,
            "low": float(pivot_low)
        }

    return None


# =========================================================
# 하락 돌파
#
# 조건
# 1. 30-60-120 역배열
# 2. 하락 1파 첫 저점
# 3. 이후 반등
# 4. 현재 캔들이 첫 저점 이탈
# 5. 현재 캔들은 음봉
# =========================================================

def detect_short_breakout(
    df
):

    if (
        df is None
        or
        len(df) < EMA_SLOW + BREAKOUT_LOOKBACK
    ):

        return False

    direction = (
        get_ema_30_60_120_direction(
            df,
            "c"
        )
    )

    if direction != "short":

        return False

    current = df.iloc[-1]

    if not is_bearish(current):

        return False

    pivot = find_first_down_low(
        df,
        "short"
    )

    if pivot is None:

        return False

    pivot_low = pivot["low"]

    current_close = float(
        current["c"]
    )

    current_low = float(
        current["l"]
    )

    if current_low >= pivot_low:

        return False

    if current_close >= pivot_low:

        return False

    return True


# =========================================================
# 돌파전 상태
#
# 롱:
# 현재 고점이 기준 고점 근처까지 접근
#
# 숏:
# 현재 저점이 기준 저점 근처까지 접근
# =========================================================

def detect_long_pre_breakout(
    df
):

    if (
        df is None
        or
        len(df) < EMA_SLOW + BREAKOUT_LOOKBACK
    ):

        return False

    direction = (
        get_ema_30_60_120_direction(
            df,
            "c"
        )
    )

    if direction != "long":

        return False

    current = df.iloc[-1]

    pivot = find_first_up_high(
        df,
        "long"
    )

    if pivot is None:

        return False

    pivot_high = pivot["high"]

    current_high = float(
        current["h"]
    )

    current_close = float(
        current["c"]
    )

    if current_high >= pivot_high:

        if current_close < pivot_high:

            return True

    # 0.5% 이내 접근
    distance = (
        (
            pivot_high
            -
            current_close
        )
        /
        pivot_high
        *
        100
    )

    return (
        distance >= 0
        and
        distance <= 0.5
        and
        is_bullish(current)
    )


def detect_short_pre_breakout(
    df
):

    if (
        df is None
        or
        len(df) < EMA_SLOW + BREAKOUT_LOOKBACK
    ):

        return False

    direction = (
        get_ema_30_60_120_direction(
            df,
            "c"
        )
    )

    if direction != "short":

        return False

    current = df.iloc[-1]

    pivot = find_first_down_low(
        df,
        "short"
    )

    if pivot is None:

        return False

    pivot_low = pivot["low"]

    current_low = float(
        current["l"]
    )

    current_close = float(
        current["c"]
    )

    if current_low <= pivot_low:

        if current_close > pivot_low:

            return True

    distance = (
        (
            current_close
            -
            pivot_low
        )
        /
        pivot_low
        *
        100
    )

    return (
        distance >= 0
        and
        distance <= 0.5
        and
        is_bearish(current)
    )


# =========================================================
# 시간봉 돌파 상태
#
# 반환:
# pre  = 돌파전
# 1    = 현재 최초 돌파
# none = 없음
# =========================================================

def get_timeframe_breakout(
    df
):

    if (
        df is None
        or
        len(df) < EMA_SLOW + BREAKOUT_LOOKBACK
    ):

        return {
            "status": "none",
            "direction": "none"
        }

    direction = (
        get_ema_30_60_120_direction(
            df,
            "c"
        )
    )

    if direction == "long":

        if detect_long_breakout(df):

            return {
                "status": "1",
                "direction": "long"
            }

        if detect_long_pre_breakout(df):

            return {
                "status": "pre",
                "direction": "long"
            }

    elif direction == "short":

        if detect_short_breakout(df):

            return {
                "status": "1",
                "direction": "short"
            }

        if detect_short_pre_breakout(df):

            return {
                "status": "pre",
                "direction": "short"
            }

    return {
        "status": "none",
        "direction": direction
    }


# =========================================================
# 당일 방향
#
# 양수 = ☀️ LONG
# 음수 = ☁️ SHORT
# =========================================================

def get_day_direction(
    change_percent
):

    if change_percent is None:

        return "none"

    if change_percent > 0:

        return "long"

    if change_percent < 0:

        return "short"

    return "none"


# =========================================================
# 당일 아이콘
# =========================================================

def day_icon(
    change_percent
):

    if change_percent is None:

        return "—"

    if change_percent > 0:

        return "☀️"

    if change_percent < 0:

        return "☁️"

    return "—"


# =========================================================
# 방향 표시
# =========================================================

def direction_label(
    direction
):

    if direction == "long":

        return "LONG"

    if direction == "short":

        return "SHORT"

    return ""


# =========================================================
# 1H + 4H 돌파 상태
#
# 둘 중 하나라도 발생하되
# 당일 방향과 돌파 방향이 일치해야 함
# =========================================================

def get_combined_breakout_warning(
    df1h,
    df4h,
    day_direction,
    exchange="UPBIT"
):

    result = {
        "1h": {
            "status": "none",
            "direction": "none"
        },
        "4h": {
            "status": "none",
            "direction": "none"
        },
        "visible": False,
        "direction": "none"
    }

    state_1h = get_timeframe_breakout(
        df1h
    )

    state_4h = get_timeframe_breakout(
        df4h
    )

    result["1h"] = state_1h
    result["4h"] = state_4h

    # -----------------------------------------------------
    # 업비트는 LONG만
    # -----------------------------------------------------

    if exchange == "UPBIT":

        if day_direction != "long":

            return result

        for state in [
            state_1h,
            state_4h
        ]:

            if (
                state["direction"] == "long"
                and
                state["status"] in ("pre", "1")
            ):

                result["visible"] = True
                result["direction"] = "long"

                return result

        return result

    # -----------------------------------------------------
    # OKX는 LONG / SHORT 모두
    # -----------------------------------------------------

    for state in [
        state_1h,
        state_4h
    ]:

        if (
            state["direction"] == day_direction
            and
            state["status"] in ("pre", "1")
        ):

            result["visible"] = True
            result["direction"] = day_direction

            return result

    return result


# =========================================================
# 경고 HTML
#
# 🚨만 표시
# 로켓 제거
# =========================================================

def combined_warning_html(
    warning
):

    if not warning:

        return ""

    if not warning.get(
        "visible",
        False
    ):

        return ""

    direction = warning.get(
        "direction",
        "none"
    )

    state_1h = warning.get(
        "1h",
        {}
    )

    state_4h = warning.get(
        "4h",
        {}
    )

    result = []

    if (
        state_1h.get("direction")
        == direction
        and
        state_1h.get("status")
        in ("pre", "1")
    ):

        result.append(
            '<span class="warning-item">🚨1H</span>'
        )

    if (
        state_4h.get("direction")
        == direction
        and
        state_4h.get("status")
        in ("pre", "1")
    ):

        result.append(
            '<span class="warning-item">🚨4H</span>'
        )

    return " ".join(result)


# =========================================================
# 변동률
# =========================================================

def get_upbit_change(market):

    df = get_upbit_ohlcv(
        market,
        60,
        120
    )

    if df is None or len(df) < 50:

        return None

    df = df.copy()

    df["datetime"] = pd.to_datetime(
        df["candle_date_time_kst"]
    )

    df.set_index(
        "datetime",
        inplace=True
    )

    daily = (
        df["trade_price"]
        .resample(
            "1D",
            offset="9h"
        )
        .last()
    )

    if len(daily) < 5:

        return None

    result = []

    for i in [-1, -2, -3]:

        if daily.iloc[i - 1] == 0:

            result.append(0)

            continue

        change = (
            (
                daily.iloc[i]
                -
                daily.iloc[i - 1]
            )
            /
            daily.iloc[i - 1]
            *
            100
        )

        result.append(
            round(change, 2)
        )

    return result


# =========================================================
# OKX 변동률
# =========================================================

def get_okx_change(inst_id):

    df = get_okx_ohlcv(
        inst_id,
        "1H",
        120,
        include_current=True
    )

    if df is None or len(df) < 50:

        return None

    df = df.copy()

    df["ts"] = pd.to_numeric(
        df["ts"],
        errors="coerce"
    )

    df["datetime"] = (
        pd.to_datetime(
            df["ts"],
            unit="ms"
        )
        +
        pd.Timedelta(hours=9)
    )

    df.set_index(
        "datetime",
        inplace=True
    )

    daily = (
        df["c"]
        .resample(
            "1D",
            offset="9h"
        )
        .last()
    )

    if len(daily) < 5:

        return None

    result = []

    for i in [-1, -2, -3]:

        if daily.iloc[i - 1] == 0:

            result.append(0)

            continue

        change = (
            (
                daily.iloc[i]
                -
                daily.iloc[i - 1]
            )
            /
            daily.iloc[i - 1]
            *
            100
        )

        result.append(
            round(change, 2)
        )

    return result


# =========================================================
# 변동률 HTML
# =========================================================

def format_change(changes):

    if (
        changes is None
        or
        len(changes) == 0
    ):

        return "N/A"

    x = changes[0]

    if x > 0:

        icon = "🟩"
        sign = "+"

    elif x < 0:

        icon = "🟥"
        sign = ""

    else:

        icon = "⬜"
        sign = ""

    return (
        '<span class="change-item">'
        f'{icon} {sign}{x:.2f}%'
        '</span>'
    )


# =========================================================
# EMA HTML
#
# 휴대폰에서 안 잘리도록 두 줄
# =========================================================

def ema_html(
    ema_1h,
    ema_4h
):

    return f"""
    <div class="ema-value">

        <div class="ema-row">
            <span class="ema-time">
                1H
            </span>
            <span class="ema-status">
                {ema_1h}
            </span>
        </div>

        <div class="ema-row">
            <span class="ema-time">
                4H
            </span>
            <span class="ema-status">
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
# 업비트 EMA
#
# 현재 진행 1H / 4H 사용
# =========================================================

def get_upbit_ema(market):

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

    if raw1h is None or raw4h is None:

        return {
            "1h_ema": empty_ema(),
            "4h_ema": empty_ema(),
            "warning": {
                "visible": False
            }
        }

    df1h = raw1h.copy()

    df4h = raw4h.copy()

    ema1h = check_ema(
        df1h
    )

    ema4h = check_ema(
        df4h
    )

    return {
        "1h_df": df1h,
        "4h_df": df4h,
        "1h_ema": ema1h,
        "4h_ema": ema4h
    }


# =========================================================
# OKX EMA
#
# 현재 진행 캔들 사용
# =========================================================

def get_okx_ema(inst_id):

    df1h = get_okx_ohlcv(
        inst_id,
        "1H",
        200,
        include_current=True
    )

    df4h = get_okx_ohlcv(
        inst_id,
        "4H",
        200,
        include_current=True
    )

    if df1h is None or df4h is None:

        return {
            "1h_ema": empty_ema(),
            "4h_ema": empty_ema(),
            "warning": {
                "visible": False
            }
        }

    ema1h = check_ema(
        df1h
    )

    ema4h = check_ema(
        df4h
    )

    return {
        "1h_df": df1h,
        "4h_df": df4h,
        "1h_ema": ema1h,
        "4h_ema": ema4h
    }


# =========================================================
# OKX 거래대금
#
# 거래대금은 완료된 1H 기준
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

            df = get_okx_ohlcv(
                inst_id,
                "1H",
                hours + 1,
                include_current=False
            )

            if df is None or df.empty:

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

            changes = get_upbit_change(
                market
            )

            change_percent = (
                changes[0]
                if (
                    changes is not None
                    and
                    len(changes) > 0
                )
                else None
            )

            day_direction = (
                get_day_direction(
                    change_percent
                )
            )

            ema = get_upbit_ema(
                market
            )

            df1h = ema.get(
                "1h_df"
            )

            df4h = ema.get(
                "4h_df"
            )

            if df1h is None or df4h is None:

                continue

            warning = (
                get_combined_breakout_warning(
                    df1h,
                    df4h,
                    day_direction,
                    exchange="UPBIT"
                )
            )

            # -------------------------------------------------
            # 업비트는 LONG 조건만
            # -------------------------------------------------

            if not warning["visible"]:

                continue

            rows.append(
                {
                    "rank": rank,

                    "name": coin,

                    "day_icon":
                        day_icon(
                            change_percent
                        ),

                    "direction":
                        warning.get(
                            "direction",
                            "none"
                        ),

                    "change":
                        format_change(
                            changes
                        ),

                    "change_percent":
                        change_percent,

                    "volume":
                        format_volume(
                            volume_map[market]
                        ),

                    "ema_1h":
                        ema["1h_ema"],

                    "ema_4h":
                        ema["4h_ema"],

                    "warning":
                        warning
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

    symbols = get_all_okx_swap_symbols()

    if not symbols:

        return False

    upbit_markets = get_upbit_markets()

    upbit_coin_set = {
        market.replace(
            "KRW-",
            ""
        )
        for market in upbit_markets
    }

    volume_map = {}

    for index, symbol in enumerate(
        symbols,
        start=1
    ):

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

            coin = f"{coin}[UP]"

        try:

            changes = get_okx_change(
                symbol
            )

            change_percent = (
                changes[0]
                if (
                    changes is not None
                    and
                    len(changes) > 0
                )
                else None
            )

            day_direction = (
                get_day_direction(
                    change_percent
                )
            )

            ema = get_okx_ema(
                symbol
            )

            df1h = ema.get(
                "1h_df"
            )

            df4h = ema.get(
                "4h_df"
            )

            if df1h is None or df4h is None:

                continue

            warning = (
                get_combined_breakout_warning(
                    df1h,
                    df4h,
                    day_direction,
                    exchange="OKX"
                )
            )

            if not warning["visible"]:

                continue

            rows.append(
                {
                    "rank": rank,

                    "name": coin,

                    "day_icon":
                        day_icon(
                            change_percent
                        ),

                    "direction":
                        warning.get(
                            "direction",
                            "none"
                        ),

                    "change":
                        format_change(
                            changes
                        ),

                    "change_percent":
                        change_percent,

                    "volume":
                        format_volume(
                            volume_map[symbol]
                        ),

                    "ema_1h":
                        ema["1h_ema"],

                    "ema_4h":
                        ema["4h_ema"],

                    "warning":
                        warning
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
        "1분 현재상태 조회 시작"
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

                latest_usdt_krw = (
                    usdt_krw
                )

            else:

                usdt_krw = (
                    latest_usdt_krw
                )

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
        "현재상태 조회 완료"
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
    font-family: Arial, sans-serif;
    font-size: 9px;
    padding: 5px;
}

h1 {

    margin: 3px 2px 5px 2px;
    font-size: 14px;
}

h2 {

    margin: 10px 2px 5px 2px;
    font-size: 11px;
}

.info {

    margin: 0 2px 6px 2px;
    padding: 5px 6px;
    color: #8b9099;
    background: #171a1f;
    border: 1px solid #252a31;
    border-radius: 7px;
    font-size: 7px;
    line-height: 1.45;
}

.exchange-status {

    display: flex;
    gap: 7px;
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
    border-bottom: 1px solid #2b3037;
    color: #8f949d;
    font-size: 7px;
    text-align: center;
}

td {

    padding: 5px 1px;
    border-bottom: 1px solid #272c32;
    text-align: center;
    vertical-align: middle;
}


/* =====================================================
   컬럼
   ===================================================== */

th:nth-child(1),
td:nth-child(1) {

    width: 6%;
}

th:nth-child(2),
td:nth-child(2) {

    width: 18%;
}

th:nth-child(3),
td:nth-child(3) {

    width: 18%;
}

th:nth-child(4),
td:nth-child(4) {

    width: 33%;
}

th:nth-child(5),
td:nth-child(5) {

    width: 25%;
}


/* =====================================================
   코인
   ===================================================== */

.coin {

    display: block;
    font-size: 8px;
    font-weight: bold;
    line-height: 1.2;
}

.day-icon {

    display: block;
    margin-top: 3px;
    font-size: 9px;
    line-height: 1;
}


/* =====================================================
   거래대금
   ===================================================== */

.volume-value {

    font-size: 7px;
    font-weight: 600;
    line-height: 1.2;
}

.direction-label {

    display: block;
    margin-top: 3px;
    font-size: 8px;
    font-weight: 800;
    line-height: 1;
}

.direction-long {

    color: #35e66d;
}

.direction-short {

    color: #ff4d4d;
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

.change-item {

    display: block;
    width: 100%;
    font-size: 8px;
    text-align: center;
    white-space: nowrap;
}


/* =====================================================
   경고
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

.warning-item {

    display: inline-block;
    font-size: 9px;
    font-weight: 800;
    line-height: 1;
    animation: warningBlink 0.9s infinite;
}


/* =====================================================
   조건이 맞는 랭크 전체 반짝임
   ===================================================== */

.warning-row {

    animation: rowGlow 1.2s infinite;
}


@keyframes warningBlink {

    0%,
    100% {

        opacity: 1;
        transform: scale(1);
    }

    50% {

        opacity: 0.35;
        transform: scale(0.94);
    }
}


@keyframes rowGlow {

    0%,
    100% {

        background: #181c21;
    }

    50% {

        background: #20252b;
    }
}


/* =====================================================
   EMA
   ===================================================== */

.ema-value {

    width: 100%;
    font-size: 8px;
    font-weight: bold;
    line-height: 1.25;
}

.ema-row {

    display: flex;
    align-items: center;
    justify-content: center;
    gap: 4px;
    width: 100%;
}

.ema-time {

    width: 20px;
    color: #858b94;
    font-size: 8px;
    font-weight: 700;
    text-align: right;
}

.ema-status {

    min-width: 40px;
    font-size: 8px;
    font-weight: 700;
    text-align: left;
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

    .day-icon {

        font-size: 8px;
    }

    .volume-value {

        font-size: 6px;
    }

    .direction-label {

        font-size: 7px;
    }

    .change-item {

        font-size: 7px;
    }

    .warning-item {

        font-size: 9px;
    }

    .ema-value {

        font-size: 8px;
    }

    .ema-time {

        font-size: 7px;
    }

    .ema-status {

        font-size: 8px;
    }

}


/* =====================================================
   아주 작은 화면
   ===================================================== */

@media (max-width: 360px) {

    th:nth-child(2),
    td:nth-child(2) {

        width: 17%;
    }

    th:nth-child(3),
    td:nth-child(3) {

        width: 18%;
    }

    th:nth-child(4),
    td:nth-child(4) {

        width: 34%;
    }

    th:nth-child(5),
    td:nth-child(5) {

        width: 25%;
    }

    .ema-time {

        width: 18px;
        font-size: 6px;
    }

    .ema-status {

        font-size: 7px;
    }

}

"""


# =========================================================
# 테이블 행 생성
# =========================================================

def make_table_rows(data):

    rows_html = ""

    for item in data:

        warning = item.get(
            "warning",
            {}
        )

        warning_text = (
            combined_warning_html(
                warning
            )
        )

        direction = item.get(
            "direction",
            "none"
        )

        row_class = (
            "warning-row"
            if warning.get(
                "visible",
                False
            )
            else ""
        )

        if direction == "long":

            direction_html = (
                '<span class="direction-label '
                'direction-long">'
                'LONG'
                '</span>'
            )

        elif direction == "short":

            direction_html = (
                '<span class="direction-label '
                'direction-short">'
                'SHORT'
                '</span>'
            )

        else:

            direction_html = ""

        rows_html += f"""

<tr class="{row_class}">

<td>
{item.get("rank", "-")}
</td>


<td>

<span class="coin">
{item["name"]}
</span>

<span class="day-icon">
{item.get("day_icon", "—")}
</span>

</td>


<td>

<span class="volume-value">
{item["volume"]}
</span>

{direction_html}

</td>


<td>

<div class="today-wrap">

<div>
{item["change"]}
</div>

<div class="breakout-warning">

{warning_text}

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

<td colspan="5"
    style="
        color:#555;
        padding:12px 4px;
    ">

현재 🚨 조건 종목 없음

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

<th>오늘</th>

<th>EMA</th>

</tr>

</thead>

<tbody>

{rows}

</tbody>

</table>

</div>

<div class="note"
     style="
        color:#666;
        font-size:6px;
        line-height:1.45;
        margin:4px 2px 7px 2px;
     ">

※ TOP{TOP_N} 거래대금 실제 순위 기준<br>

※ 코인명 아래 ☀️ = 당일 양수 / ☁️ = 당일 음수<br>

※ 거래대금 아래 LONG / SHORT = 당일 방향<br>

※ EMA = 30-60-120 정배열 / 역배열<br>

※ 1H / 4H 현재 진행 캔들 상태를 1분마다 조회<br>

※ 🚨1H = 1시간 첫 고점 재돌파 조건<br>

※ 🚨4H = 4시간 첫 고점 재돌파 조건<br>

※ LONG은 30-60-120 정배열 후 상승 1파 고점 재돌파<br>

※ SHORT은 30-60-120 역배열 후 하락 1파 저점 재이탈<br>

※ 당일 방향과 돌파 방향이 일치할 때만 표시<br>

※ 업비트 = LONG만 표시<br>

※ OKX = LONG / SHORT 표시<br>

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

<meta http-equiv="refresh"
      content="60">

<meta name="viewport"
      content="
        width=device-width,
        initial-scale=1.0,
        maximum-scale=1.0,
        user-scalable=no">

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
30-60-120 정배열 · 상승 1파 고점 재돌파
</div>

<div>
1H + 4H 현재 상태 · 1분마다 조회
</div>

<div>
TOP{TOP_N} · 당일 방향 일치 · 🚨 돌파 조건
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
        f"돌파 LOOKBACK = "
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
    # 1분마다 현재 상태 조회
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
