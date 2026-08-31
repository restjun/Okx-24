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

MAX_WARNING_COUNT = 3

BREAKOUT_LOOKBACK = 10


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
# 돌파 상태 저장
#
# key:
#   (거래소, 종목, timeframe)
#
# 상태:
#
# idle
# pre
# breakout
# stopped
#
# stopped 상태가 되면 다음 조회에서도 다시 경고하지 않음
# =========================================================

breakout_states = {}

breakout_state_lock = threading.Lock()


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
                        volume = float(volume)
                    except Exception:
                        volume = 0

                    if market:
                        volume_map[market] = volume

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
# 현재 진행 캔들 포함
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

        df["volume"] = pd.to_numeric(
            df.get("candle_acc_trade_volume"),
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
# EMA 10-30
# =========================================================

def get_ema_10_30_direction(
    df,
    column
):

    ema10 = get_ema(df, column, 10)

    ema30 = get_ema(df, column, 30)

    if ema10 is None or ema30 is None:
        return "none"

    a = ema10.iloc[-1]
    b = ema30.iloc[-1]

    if pd.isna(a) or pd.isna(b):
        return "none"

    if a > b:
        return "long"

    if a < b:
        return "short"

    return "none"


# =========================================================
# EMA 10-30-60
# =========================================================

def get_ema_10_30_60_direction(
    df,
    column
):

    ema10 = get_ema(df, column, 10)
    ema30 = get_ema(df, column, 30)
    ema60 = get_ema(df, column, 60)

    if (
        ema10 is None
        or ema30 is None
        or ema60 is None
    ):

        return "none"

    values = [
        ema10.iloc[-1],
        ema30.iloc[-1],
        ema60.iloc[-1]
    ]

    if any(pd.isna(x) for x in values):
        return "none"

    if values[0] > values[1] > values[2]:
        return "long"

    if values[0] < values[1] < values[2]:
        return "short"

    return "none"


# =========================================================
# EMA 정렬 카운트
# =========================================================

def get_ema_10_30_60_alignment_count(
    df,
    column
):

    if df is None or len(df) < 60:

        return "none", 0

    ema10 = get_ema(df, column, 10)
    ema30 = get_ema(df, column, 30)
    ema60 = get_ema(df, column, 60)

    if any(
        x is None
        for x in [
            ema10,
            ema30,
            ema60
        ]
    ):

        return "none", 0

    current_direction = None
    count = 0

    for index in range(
        len(df) - 1,
        -1,
        -1
    ):

        values = [
            ema10.iloc[index],
            ema30.iloc[index],
            ema60.iloc[index]
        ]

        if any(
            pd.isna(x)
            for x in values
        ):

            break

        if (
            values[0]
            >
            values[1]
            >
            values[2]
        ):

            direction = "long"

        elif (
            values[0]
            <
            values[1]
            <
            values[2]
        ):

            direction = "short"

        else:

            direction = "none"

        if current_direction is None:

            if direction == "none":
                break

            current_direction = direction
            count = 1

        elif direction == current_direction:

            count += 1

        else:

            break

    if current_direction is None:

        return "none", 0

    return current_direction, count


# =========================================================
# 메인 EMA 방향
# =========================================================

def get_main_direction(
    df,
    column
):

    if df is not None and len(df) >= 60:

        direction = (
            get_ema_10_30_60_direction(
                df,
                column
            )
        )

        if direction != "none":
            return direction

    return get_ema_10_30_direction(
        df,
        column
    )


# =========================================================
# EMA 표시
# =========================================================

def check_ema(
    df
):

    if (
        df is not None
        and
        len(df) >= 60
    ):

        direction, count = (
            get_ema_10_30_60_alignment_count(
                df,
                "c"
            )
        )

    else:

        direction = (
            get_ema_10_30_direction(
                df,
                "c"
            )
        )

        count = 0

        if direction != "none":

            ema10 = get_ema(
                df,
                "c",
                10
            )

            ema30 = get_ema(
                df,
                "c",
                30
            )

            if (
                ema10 is not None
                and
                ema30 is not None
            ):

                for i in range(
                    len(df) - 1,
                    -1,
                    -1
                ):

                    a = ema10.iloc[i]
                    b = ema30.iloc[i]

                    if pd.isna(a) or pd.isna(b):
                        break

                    current = (
                        "long"
                        if a > b
                        else
                        "short"
                        if a < b
                        else
                        "none"
                    )

                    if current != direction:
                        break

                    count += 1

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
# 일목 해구름
#
# 표준 일목:
#
# 전환선 = 9
# 기준선 = 26
# 선행스팬 A
# 선행스팬 B = 52
#
# 여기서는 "현재 가격 + 구름 방향"을 같이 사용
#
# 롱:
#   현재가 > 구름 상단
#   Span A > Span B
#
# 숏:
#   현재가 < 구름 하단
#   Span A < Span B
# =========================================================

def get_ichimoku_cloud_direction(
    df
):

    if df is None or len(df) < 60:

        return {
            "direction": "none",
            "cloud_top": None,
            "cloud_bottom": None,
            "span_a": None,
            "span_b": None
        }

    high = pd.to_numeric(
        df["h"],
        errors="coerce"
    )

    low = pd.to_numeric(
        df["l"],
        errors="coerce"
    )

    close = pd.to_numeric(
        df["c"],
        errors="coerce"
    )

    conversion = (
        high.rolling(9).max()
        +
        low.rolling(9).min()
    ) / 2

    base = (
        high.rolling(26).max()
        +
        low.rolling(26).min()
    ) / 2

    span_a = (
        conversion
        +
        base
    ) / 2

    span_b = (
        high.rolling(52).max()
        +
        low.rolling(52).min()
    ) / 2

    a = span_a.iloc[-1]
    b = span_b.iloc[-1]
    price = close.iloc[-1]

    if any(
        pd.isna(x)
        for x in [a, b, price]
    ):

        return {
            "direction": "none",
            "cloud_top": None,
            "cloud_bottom": None,
            "span_a": None,
            "span_b": None
        }

    cloud_top = max(a, b)

    cloud_bottom = min(a, b)

    if (
        price > cloud_top
        and
        a > b
    ):

        direction = "long"

    elif (
        price < cloud_bottom
        and
        a < b
    ):

        direction = "short"

    else:

        direction = "none"

    return {
        "direction": direction,
        "cloud_top": cloud_top,
        "cloud_bottom": cloud_bottom,
        "span_a": a,
        "span_b": b
    }


# =========================================================
# 당일 방향
#
# 양수 = long
# 음수 = short
# =========================================================

def get_day_direction(
    change_percent
):

    if change_percent is None:

        return "none"

    try:

        value = float(
            change_percent
        )

    except Exception:

        return "none"

    if value > 0:
        return "long"

    if value < 0:
        return "short"

    return "none"


# =========================================================
# 돌파 판정
# =========================================================

def is_long_breakout(
    row,
    previous
):

    if previous.empty:
        return False

    previous_high = pd.to_numeric(
        previous["h"],
        errors="coerce"
    ).max()

    try:

        return (
            float(row["c"])
            >
            previous_high
            and
            float(row["c"])
            >
            float(row["o"])
        )

    except Exception:

        return False


def is_short_breakout(
    row,
    previous
):

    if previous.empty:
        return False

    previous_low = pd.to_numeric(
        previous["l"],
        errors="coerce"
    ).min()

    try:

        return (
            float(row["c"])
            <
            previous_low
            and
            float(row["c"])
            <
            float(row["o"])
        )

    except Exception:

        return False


# =========================================================
# 돌파 전
# =========================================================

def is_long_pre_breakout(
    row,
    previous
):

    if previous.empty:
        return False

    previous_high = pd.to_numeric(
        previous["h"],
        errors="coerce"
    ).max()

    try:

        return (
            float(row["h"])
            >=
            previous_high
            and
            float(row["c"])
            <=
            previous_high
            and
            float(row["c"])
            >=
            float(row["o"])
        )

    except Exception:

        return False


def is_short_pre_breakout(
    row,
    previous
):

    if previous.empty:
        return False

    previous_low = pd.to_numeric(
        previous["l"],
        errors="coerce"
    ).min()

    try:

        return (
            float(row["l"])
            <=
            previous_low
            and
            float(row["c"])
            >=
            previous_low
            and
            float(row["c"])
            <=
            float(row["o"])
        )

    except Exception:

        return False


# =========================================================
# 현재 캔들 ID
# =========================================================

def get_candle_id(
    df,
    timeframe
):

    if df is None or df.empty:

        return None

    row = df.iloc[-1]

    if "candle_date_time_kst" in df.columns:

        return str(
            row["candle_date_time_kst"]
        )

    if "ts" in df.columns:

        try:

            ts = int(
                row["ts"]
            )

            return f"{timeframe}_{ts}"

        except Exception:

            return None

    return None


# =========================================================
# 돌파 기준값
# =========================================================

def get_breakout_levels(
    df
):

    if (
        df is None
        or
        len(df) < BREAKOUT_LOOKBACK + 1
    ):

        return None, None

    previous = df.iloc[
        -BREAKOUT_LOOKBACK - 1:
        -1
    ]

    high = pd.to_numeric(
        previous["h"],
        errors="coerce"
    ).max()

    low = pd.to_numeric(
        previous["l"],
        errors="coerce"
    ).min()

    if pd.isna(high) or pd.isna(low):

        return None, None

    return float(high), float(low)


# =========================================================
# 상태 초기화
#
# 새로운 캔들이 시작되면
# 이전 stopped 상태를 그대로 유지한다.
#
# 단, 새로운 실제 돌파가 발생하면
# 새로운 사이클로 인정할 수 있도록 한다.
# =========================================================

def get_state(
    exchange,
    coin,
    timeframe
):

    key = (
        exchange,
        coin,
        timeframe
    )

    with breakout_state_lock:

        if key not in breakout_states:

            breakout_states[key] = {
                "status": "idle",
                "candle_id": None,
                "breakout_high": None,
                "breakout_low": None,
                "direction": None,
                "stopped_once": False
            }

        return breakout_states[key]


# =========================================================
# 방향 일치 돌파 상태
#
# return:
#
# none
# pre
# 1
# stopped
#
# 중요한 부분:
#
# 1. 당일 방향
# 2. 해구름 방향
# 3. EMA 방향
# 4. 돌파 방향
#
# 모두 같은 방향이어야 한다.
# =========================================================

def get_confirmed_breakout_state(
    df,
    timeframe,
    exchange,
    coin,
    day_direction,
    cloud_direction,
    ema_direction
):

    if (
        df is None
        or
        len(df)
        <
        BREAKOUT_LOOKBACK + 30
    ):

        return "none"

    # -----------------------------------------------------
    # 방향 일치
    # -----------------------------------------------------

    if (
        day_direction == "none"
        or
        cloud_direction == "none"
        or
        ema_direction == "none"
    ):

        return "none"

    if not (
        day_direction
        ==
        cloud_direction
        ==
        ema_direction
    ):

        return "none"

    direction = day_direction

    state = get_state(
        exchange,
        coin,
        timeframe
    )

    current_id = get_candle_id(
        df,
        timeframe
    )

    current = df.iloc[-1]

    previous = df.iloc[
        -BREAKOUT_LOOKBACK - 1:
        -1
    ]

    high_level = pd.to_numeric(
        previous["h"],
        errors="coerce"
    ).max()

    low_level = pd.to_numeric(
        previous["l"],
        errors="coerce"
    ).min()

    # -----------------------------------------------------
    # 기존 돌파 상태가 있으면 손절 확인
    # -----------------------------------------------------

    if (
        state["status"] == "breakout"
        and
        state["direction"] == direction
    ):

        if direction == "long":

            breakout_low = (
                state["breakout_low"]
            )

            try:

                if (
                    breakout_low is not None
                    and
                    float(current["l"])
                    <
                    float(breakout_low)
                ):

                    state["status"] = "stopped"

                    state["stopped_once"] = True

                    logging.info(
                        f"[{exchange}] "
                        f"{coin} {timeframe} "
                        f"롱 돌파 후 저점 이탈 "
                        f"→ ⛔️ 중지"
                    )

                    return "stopped"

            except Exception:
                pass

        elif direction == "short":

            breakout_high = (
                state["breakout_high"]
            )

            try:

                if (
                    breakout_high is not None
                    and
                    float(current["h"])
                    >
                    float(breakout_high)
                ):

                    state["status"] = "stopped"

                    state["stopped_once"] = True

                    logging.info(
                        f"[{exchange}] "
                        f"{coin} {timeframe} "
                        f"숏 돌파 후 고점 이탈 "
                        f"→ ⛔️ 중지"
                    )

                    return "stopped"

            except Exception:
                pass

        # -------------------------------------------------
        # 돌파 후에는 최초 1만 유지
        # -------------------------------------------------

        return "1"

    # -----------------------------------------------------
    # stopped 상태
    #
    # 이 상태에서는 다시 경고하지 않는다.
    # -----------------------------------------------------

    if state["status"] == "stopped":

        return "none"

    # -----------------------------------------------------
    # 현재 돌파
    # -----------------------------------------------------

    breakout = False

    if direction == "long":

        breakout = is_long_breakout(
            current,
            previous
        )

    elif direction == "short":

        breakout = is_short_breakout(
            current,
            previous
        )

    if breakout:

        state["status"] = "breakout"

        state["candle_id"] = current_id

        state["direction"] = direction

        try:

            state["breakout_high"] = float(
                current["h"]
            )

            state["breakout_low"] = float(
                current["l"]
            )

        except Exception:

            state["breakout_high"] = None
            state["breakout_low"] = None

        state["stopped_once"] = False

        logging.info(
            f"[{exchange}] "
            f"{coin} {timeframe} "
            f"{direction} 최초 돌파 → 🚀1"
        )

        return "1"

    # -----------------------------------------------------
    # 돌파 전
    # -----------------------------------------------------

    pre = False

    if direction == "long":

        pre = is_long_pre_breakout(
            current,
            previous
        )

    elif direction == "short":

        pre = is_short_pre_breakout(
            current,
            previous
        )

    if pre:

        state["status"] = "pre"

        state["candle_id"] = current_id

        state["direction"] = direction

        return "pre"

    # -----------------------------------------------------
    # 아무 조건도 없음
    # -----------------------------------------------------

    return "none"


# =========================================================
# 변동률
# =========================================================

def get_upbit_change(
    market
):

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

def get_okx_change(
    inst_id
):

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

def format_change(
    changes
):

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
# 1H / 4H
# =========================================================

def ema_html(
    ema_1h,
    ema_4h
):

    return f"""
    <div class="ema-value">
        <span class="ema-1h">
            1H {ema_1h}
        </span>
        <span class="ema-divider"> / </span>
        <span class="ema-4h">
            4H {ema_4h}
        </span>
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
# 시간봉 분석
#
# 현재 진행 중인 캔들 사용
# =========================================================

def analyze_timeframe(
    df,
    timeframe,
    exchange,
    coin,
    day_direction
):

    if (
        df is None
        or
        len(df) < 70
    ):

        return {
            "ema": empty_ema(),
            "cloud": {
                "direction": "none"
            },
            "breakout": "none",
            "direction": "none"
        }

    ema = check_ema(
        df
    )

    cloud = get_ichimoku_cloud_direction(
        df
    )

    breakout = get_confirmed_breakout_state(
        df=df,
        timeframe=timeframe,
        exchange=exchange,
        coin=coin,
        day_direction=day_direction,
        cloud_direction=cloud["direction"],
        ema_direction=ema["direction"]
    )

    return {
        "ema": ema,
        "cloud": cloud,
        "breakout": breakout,
        "direction": ema["direction"]
    }


# =========================================================
# 최종 경고
#
# 1H / 4H 각각 독립적으로 판단
#
# 당일 + = 롱
# 당일 - = 숏
#
# 해구름 + EMA + 돌파 방향이 모두 일치
# =========================================================

def get_combined_warning(
    df1h,
    df4h,
    change_percent,
    exchange,
    coin
):

    day_direction = get_day_direction(
        change_percent
    )

    result_1h = analyze_timeframe(
        df1h,
        "1H",
        exchange,
        coin,
        day_direction
    )

    result_4h = analyze_timeframe(
        df4h,
        "4H",
        exchange,
        coin,
        day_direction
    )

    return {
        "1h": result_1h,
        "4h": result_4h,
        "day_direction": day_direction
    }


# =========================================================
# 최종 표시 가능 여부
# =========================================================

def is_visible_combined_warning(
    warning
):

    if not warning:

        return False

    one = warning.get(
        "1h",
        {}
    )

    four = warning.get(
        "4h",
        {}
    )

    b1 = one.get(
        "breakout",
        "none"
    )

    b4 = four.get(
        "breakout",
        "none"
    )

    return (
        b1 in ("pre", "1")
        or
        b4 in ("pre", "1")
    )


# =========================================================
# 경고 HTML
#
# 1H → 4H
# =========================================================

def combined_warning_html(
    warning
):

    if not warning:
        return ""

    result = []

    one = warning.get(
        "1h",
        {}
    )

    four = warning.get(
        "4h",
        {}
    )

    b1 = one.get(
        "breakout",
        "none"
    )

    b4 = four.get(
        "breakout",
        "none"
    )

    # -----------------------------------------------------
    # 1H
    # -----------------------------------------------------

    if b1 == "pre":

        result.append(
            '<span class="warning-pre">'
            '🚨1H'
            '</span>'
        )

    elif b1 == "1":

        result.append(
            '<span class="warning-rocket">'
            '🚀1H(1)'
            '</span>'
        )

    # -----------------------------------------------------
    # 4H
    # -----------------------------------------------------

    if b4 == "pre":

        result.append(
            '<span class="warning-pre">'
            '🚨4H'
            '</span>'
        )

    elif b4 == "1":

        result.append(
            '<span class="warning-rocket">'
            '🚀4H(1)'
            '</span>'
        )

    return " ".join(
        result
    )


# =========================================================
# 업비트 EMA + 해구름 + 돌파
# =========================================================

def get_upbit_ema(
    market,
    change_percent
):

    df1h = get_upbit_ohlcv(
        market,
        60,
        200
    )

    df4h = get_upbit_ohlcv(
        market,
        240,
        200
    )

    if (
        df1h is None
        or
        df4h is None
    ):

        return {
            "1h_ema": empty_ema(),
            "4h_ema": empty_ema(),
            "warning": {}
        }

    result = get_combined_warning(
        df1h,
        df4h,
        change_percent,
        "UPBIT",
        market
    )

    return {
        "1h_ema":
            result["1h"]["ema"],

        "4h_ema":
            result["4h"]["ema"],

        "warning":
            result
    }


# =========================================================
# OKX EMA + 해구름 + 돌파
# =========================================================

def get_okx_ema(
    inst_id,
    change_percent
):

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

    if (
        df1h is None
        or
        df4h is None
    ):

        return {
            "1h_ema": empty_ema(),
            "4h_ema": empty_ema(),
            "warning": {}
        }

    result = get_combined_warning(
        df1h,
        df4h,
        change_percent,
        "OKX",
        inst_id
    )

    return {
        "1h_ema":
            result["1h"]["ema"],

        "4h_ema":
            result["4h"]["ema"],

        "warning":
            result
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

            ema = get_upbit_ema(
                market,
                change_percent
            )

            warning = ema.get(
                "warning",
                {}
            )

            if not is_visible_combined_warning(
                warning
            ):

                continue

            rows.append(
                {
                    "rank": rank,

                    "name": coin,

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

                    volume_map[symbol] = volume

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

            ema = get_okx_ema(
                symbol,
                change_percent
            )

            warning = ema.get(
                "warning",
                {}
            )

            if not is_visible_combined_warning(
                warning
            ):

                continue

            rows.append(
                {
                    "rank": rank,

                    "name": coin,

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
    font-family: Arial, sans-serif;
    font-size: 10px;
    padding: 6px;
}

h1 {

    margin: 3px 2px 6px 2px;
    font-size: 15px;
}

h2 {

    margin: 12px 2px 6px 2px;
    font-size: 12px;
}

.info {

    margin: 0 2px 7px 2px;
    padding: 6px 7px;
    color: #8b9099;
    background: #171a1f;
    border: 1px solid #252a31;
    border-radius: 8px;
    font-size: 8px;
    line-height: 1.5;
}

.exchange-status {

    display: flex;
    gap: 8px;
    margin-top: 5px;
    font-size: 8px;
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
    border-radius: 9px;
    border: 1px solid #252a31;
}

table {

    width: 100%;
    table-layout: fixed;
    border-collapse: collapse;
    background: #181c21;
}

th {

    padding: 6px 2px;
    background: #12151a;
    border-bottom: 1px solid #2b3037;
    color: #8f949d;
    font-size: 8px;
    text-align: center;
}

td {

    padding: 6px 2px;
    border-bottom: 1px solid #272c32;
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

    width: 18%;
}

th:nth-child(3),
td:nth-child(3) {

    width: 15%;
}

th:nth-child(4),
td:nth-child(4) {

    width: 38%;
}

th:nth-child(5),
td:nth-child(5) {

    width: 22%;
}


/* =====================================================
   코인
   ===================================================== */

.coin {

    display: block;
    font-size: 9px;
    font-weight: bold;
    line-height: 1.2;
}


/* =====================================================
   거래대금
   ===================================================== */

.volume-value {

    font-size: 8px;
    font-weight: 600;
}


/* =====================================================
   오늘
   ===================================================== */

.today-wrap {

    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 5px;
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
    gap: 5px;
    width: 100%;
    min-height: 15px;
    white-space: nowrap;
}

.warning-pre {

    font-size: 10px;
    font-weight: bold;
    filter: drop-shadow(
        0 0 5px rgba(255,180,0,0.8)
    );
}

.warning-rocket {

    font-size: 10px;
    font-weight: bold;
    filter: drop-shadow(
        0 0 5px rgba(50,255,100,0.8)
    );
}


/* =====================================================
   EMA
   ===================================================== */

.ema-value {

    width: 100%;
    font-size: 7px;
    font-weight: bold;
    white-space: nowrap;
}

.ema-divider {

    color: #555;
}

.ema-1h {

    display: inline;
}

.ema-4h {

    display: inline;
}


/* =====================================================
   모바일
   ===================================================== */

@media (max-width: 480px) {

    body {

        padding: 4px;
        font-size: 9px;
    }

    h1 {

        font-size: 14px;
    }

    h2 {

        font-size: 12px;
    }

    .info {

        font-size: 7px;
    }

    th {

        padding: 5px 1px;
        font-size: 7px;
    }

    td {

        padding: 5px 1px;
    }

    .coin {

        font-size: 8px;
    }

    .volume-value {

        font-size: 7px;
    }

    .change-item {

        font-size: 8px;
    }

    .warning-pre,
    .warning-rocket {

        font-size: 9px;
    }

    .ema-value {

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

        warning = item.get(
            "warning",
            {}
        )

        warning_text = (
            combined_warning_html(
                warning
            )
        )

        rows_html += f"""

<tr>

<td>
{item.get("rank", "-")}
</td>

<td>

<span class="coin">
{item["name"]}
</span>

</td>

<td>

<span class="volume-value">
{item["volume"]}
</span>

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

현재 🚨 / 🚀 종목 없음

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
        font-size:7px;
        line-height:1.5;
        margin:5px 2px 8px 2px;
     ">

※ TOP{TOP_N} 거래대금 실제 순위 기준<br>

※ 오늘 1줄 = 당일 변동률<br>

※ 오늘 2줄 = 1H / 4H 돌파 상태<br>

※ 당일 양수 + 해구름 롱 + EMA 롱 + 롱 돌파만 표시<br>

※ 당일 음수 + 해구름 숏 + EMA 숏 + 숏 돌파만 표시<br>

※ 🚨1H = 1시간 돌파 전<br>

※ 🚀1H(1) = 1시간 최초 돌파<br>

※ 🚨4H = 4시간 돌파 전<br>

※ 🚀4H(1) = 4시간 최초 돌파<br>

※ 🚀(2) 이상 추가 카운팅 없음<br>

※ 돌파 후 돌파 캔들 반대쪽 이탈 시 ⛔️ 중지 처리<br>

※ ⛔️ 처리 후 해당 돌파 사이클은 경고 리스트에서 제거<br>

※ 현재 진행 중인 1H / 4H 캔들 기준으로 1분마다 조회<br>

※ EMA는 1H / 4H 순서<br>

※ EMA 60개 이상 = 10-30-60 정렬<br>

※ EMA 60개 미만 = 10-30 정렬<br>

※ 해구름 = 현재가가 구름 위/아래이고 Span A/B 방향까지 일치

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
1H + 4H 추세 · 해구름 · 돌파
</div>

<div>
당일 방향 + 해구름 방향 + EMA 방향 + 돌파 방향 일치
</div>

<div>
TOP{TOP_N} · 🚨 돌파 전 · 🚀 최초 돌파
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
        f"OKX={USE_OKX} "
        f"조회주기={UPDATE_MINUTES}분"
    )

    if USE_UPBIT not in ("Y", "N"):

        raise ValueError(
            "USE_UPBIT은 Y 또는 N만 사용할 수 있습니다."
        )

    if USE_OKX not in ("Y", "N"):

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
