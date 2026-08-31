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

MAX_WARNING_COUNT = 3

# 최근 몇 개의 확정 캔들 고점/저점을 기준으로
# 돌파를 판단할지
BREAKOUT_LOOKBACK = 10

# 돌파 후 너무 많이 올라간 지점에서
# 늦게 경고가 발생하는 것을 방지
MAX_BREAKOUT_DISTANCE_PERCENT = 3.0


# =========================================================
# 거래소 조회
# =========================================================

USE_UPBIT = "Y"

USE_OKX = "N"


# =========================================================
# API 안정화
# =========================================================

REQUEST_INTERVAL = 0.08

RATE_LIMIT_WAIT = 3

MAX_RETRIES = 10


# =========================================================
# OKX 실패 재시도
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
# 1H 돌파 상태 저장
#
# 종목별로 이전 상태를 기억
# =========================================================

breakout_states = {}

breakout_states_lock = threading.Lock()


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

        if not isinstance(
            data,
            list
        ):

            return None

        if not data:
            return None

        df = pd.DataFrame(
            data
        )

        if df.empty:
            return None

        required = [
            "opening_price",
            "high_price",
            "low_price",
            "trade_price"
        ]

        for column in required:

            if column not in df.columns:

                return None

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

        df = df.dropna(
            subset=[
                "o",
                "h",
                "l",
                "c"
            ]
        )

        if df.empty:
            return None

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
        or
        df.empty
        or
        column not in df.columns
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
# EMA 30-60-120
# =========================================================

def get_ema_30_60_120_direction(
    df,
    column="c"
):

    ema30 = get_ema(
        df,
        column,
        30
    )

    ema60 = get_ema(
        df,
        column,
        60
    )

    ema120 = get_ema(
        df,
        column,
        120
    )

    if (
        ema30 is None
        or ema60 is None
        or ema120 is None
    ):

        return "none"

    if (
        len(ema30) == 0
        or len(ema60) == 0
        or len(ema120) == 0
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
# EMA 30-60-120 정배열 유지 캔들 수
# =========================================================

def get_alignment_count(
    df,
    column="c"
):

    if (
        df is None
        or
        len(df) < 120
    ):

        return "none", 0

    ema30 = get_ema(
        df,
        column,
        30
    )

    ema60 = get_ema(
        df,
        column,
        60
    )

    ema120 = get_ema(
        df,
        column,
        120
    )

    if (
        ema30 is None
        or ema60 is None
        or ema120 is None
    ):

        return "none", 0

    direction = None

    count = 0

    for index in range(
        len(df) - 1,
        -1,
        -1
    ):

        a = ema30.iloc[index]
        b = ema60.iloc[index]
        c = ema120.iloc[index]

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
        get_alignment_count(
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
# 당일 양수 / 음수
#
# KST 09:00 기준
# =========================================================

def get_today_change_from_1h(
    df
):

    if (
        df is None
        or
        df.empty
    ):

        return None

    df = df.copy()

    try:

        if "candle_date_time_kst" in df.columns:

            df["datetime"] = pd.to_datetime(
                df["candle_date_time_kst"],
                errors="coerce"
            )

        elif "ts" in df.columns:

            df["ts"] = pd.to_numeric(
                df["ts"],
                errors="coerce"
            )

            df["datetime"] = (
                pd.to_datetime(
                    df["ts"],
                    unit="ms",
                    errors="coerce"
                )
                +
                pd.Timedelta(hours=9)
            )

        else:

            return None

        df["c"] = pd.to_numeric(
            df["c"],
            errors="coerce"
        )

        df = df.dropna(
            subset=[
                "datetime",
                "c"
            ]
        )

        if df.empty:
            return None

        df = (
            df.sort_values("datetime")
            .reset_index(drop=True)
        )

        latest = df.iloc[-1]

        current_date = (
            latest["datetime"].date()
        )

        today_start = pd.Timestamp(
            current_date
        ) + pd.Timedelta(
            hours=9
        )

        today_data = df[
            df["datetime"] >= today_start
        ]

        if today_data.empty:

            return None

        current_price = float(
            today_data["c"].iloc[-1]
        )

        previous_data = df[
            df["datetime"] < today_start
        ]

        if previous_data.empty:

            return None

        previous_close = float(
            previous_data["c"].iloc[-1]
        )

        if previous_close <= 0:

            return None

        change = (
            (
                current_price
                -
                previous_close
            )
            /
            previous_close
            *
            100
        )

        return round(
            float(change),
            2
        )

    except Exception as e:

        logging.error(
            f"당일 변동률 오류 : {e}"
        )

        return None


# =========================================================
# 당일 해 / 구름 표시
# =========================================================

def get_today_weather(
    change
):

    if change is None:

        return {
            "type": "none",
            "text": "⚪"
        }

    if change > 0:

        return {
            "type": "long",
            "text": "☀️"
        }

    if change < 0:

        return {
            "type": "short",
            "text": "☁️"
        }

    return {
        "type": "none",
        "text": "⚪"
    }


# =========================================================
# 고점 / 저점 추적
#
# 상승 후 조정 뒤 다시 고점을 넘는 구조
# =========================================================

def find_long_breakout(
    df,
    lookback=10
):

    if (
        df is None
        or
        len(df) < lookback + 1
    ):

        return None

    current_index = len(df) - 1

    current = df.iloc[
        current_index
    ]

    previous = df.iloc[
        max(
            0,
            current_index - lookback
        ):
        current_index
    ]

    if previous.empty:

        return None

    previous_high = float(
        previous["h"].max()
    )

    current_close = float(
        current["c"]
    )

    current_open = float(
        current["o"]
    )

    current_high = float(
        current["h"]
    )

    # -----------------------------------------------------
    # 양봉이면서 직전 고점을 실제 종가로 돌파
    # -----------------------------------------------------

    if (
        current_close > previous_high
        and
        current_close > current_open
    ):

        return {
            "type": "breakout",
            "index": current_index,
            "level": previous_high,
            "price": current_close
        }

    # -----------------------------------------------------
    # 아직 돌파 전
    # 고점에 접근한 양봉
    # -----------------------------------------------------

    if (
        current_high >= previous_high
        and
        current_close <= previous_high
        and
        current_close >= current_open
    ):

        return {
            "type": "pre",
            "index": current_index,
            "level": previous_high,
            "price": current_close
        }

    return None


# =========================================================
# SHORT 돌파
# =========================================================

def find_short_breakout(
    df,
    lookback=10
):

    if (
        df is None
        or
        len(df) < lookback + 1
    ):

        return None

    current_index = len(df) - 1

    current = df.iloc[
        current_index
    ]

    previous = df.iloc[
        max(
            0,
            current_index - lookback
        ):
        current_index
    ]

    if previous.empty:

        return None

    previous_low = float(
        previous["l"].min()
    )

    current_close = float(
        current["c"]
    )

    current_open = float(
        current["o"]
    )

    current_low = float(
        current["l"]
    )

    # -----------------------------------------------------
    # 음봉이면서 직전 저점을 종가로 이탈
    # -----------------------------------------------------

    if (
        current_close < previous_low
        and
        current_close < current_open
    ):

        return {
            "type": "breakout",
            "index": current_index,
            "level": previous_low,
            "price": current_close
        }

    # -----------------------------------------------------
    # 돌파 전
    # -----------------------------------------------------

    if (
        current_low <= previous_low
        and
        current_close >= previous_low
        and
        current_close <= current_open
    ):

        return {
            "type": "pre",
            "index": current_index,
            "level": previous_low,
            "price": current_close
        }

    return None


# =========================================================
# 1H 돌파 판단
#
# 30-60-120 정배열 유지 상태에서만
# =========================================================

def get_1h_breakout_signal(
    symbol,
    df,
    weather_type
):

    if (
        df is None
        or
        len(df) < 130
    ):

        return "none"

    direction = (
        get_ema_30_60_120_direction(
            df,
            "c"
        )
    )

    # -----------------------------------------------------
    # LONG
    # 당일 양수 + EMA 정배열
    # -----------------------------------------------------

    if (
        weather_type == "long"
        and
        direction == "long"
    ):

        result = find_long_breakout(
            df,
            BREAKOUT_LOOKBACK
        )

        if result is None:

            return "none"

        return result["type"]

    # -----------------------------------------------------
    # SHORT
    # 당일 음수 + EMA 역배열
    # -----------------------------------------------------

    if (
        weather_type == "short"
        and
        direction == "short"
    ):

        result = find_short_breakout(
            df,
            BREAKOUT_LOOKBACK
        )

        if result is None:

            return "none"

        return result["type"]

    return "none"


# =========================================================
# 상태 저장
#
# 🚀 최초 돌파 1회만
# =========================================================

def apply_breakout_state(
    symbol,
    signal
):

    now = time.time()

    with breakout_states_lock:

        state = breakout_states.get(
            symbol
        )

        if state is None:

            state = {
                "status": "none",
                "last_signal": "none",
                "last_time": 0
            }

            breakout_states[
                symbol
            ] = state

        # -------------------------------------------------
        # 돌파 전
        # -------------------------------------------------

        if signal == "pre":

            if state["status"] != "breakout":

                state["status"] = "pre"

                state["last_signal"] = "pre"

            return "pre"

        # -------------------------------------------------
        # 최초 돌파
        # -------------------------------------------------

        if signal == "breakout":

            if state["status"] == "breakout":

                # 이미 돌파한 종목
                # 🚀 중복 표시 금지
                return "none"

            state["status"] = "breakout"

            state["last_signal"] = "breakout"

            state["last_time"] = now

            return "1"

        # -------------------------------------------------
        # 아무 조건도 없으면 제거
        # -------------------------------------------------

        if signal == "none":

            # 정배열 자체가 깨졌거나
            # 방향 조건이 사라진 경우
            if state["status"] == "pre":

                state["status"] = "none"

                state["last_signal"] = "none"

            return "none"

        return "none"


# =========================================================
# 업비트 변동률
#
# out-of-bounds 방지 버전
# =========================================================

def get_upbit_change(
    market
):

    try:

        df = get_upbit_ohlcv(
            market,
            60,
            200
        )

        if (
            df is None
            or
            df.empty
        ):

            return None

        if "candle_date_time_kst" not in df.columns:

            return None

        if "c" not in df.columns:

            return None

        df = df.copy()

        df["datetime"] = pd.to_datetime(
            df["candle_date_time_kst"],
            errors="coerce"
        )

        df["c"] = pd.to_numeric(
            df["c"],
            errors="coerce"
        )

        df = df.dropna(
            subset=[
                "datetime",
                "c"
            ]
        )

        if df.empty:

            return None

        df = (
            df.sort_values("datetime")
            .reset_index(drop=True)
        )

        daily = (
            df.set_index("datetime")["c"]
            .resample(
                "1D",
                offset="9h"
            )
            .last()
            .dropna()
        )

        # -------------------------------------------------
        # 반드시 충분한 일봉이 있어야 함
        # -------------------------------------------------

        if len(daily) < 4:

            return None

        result = []

        positions = [
            len(daily) - 1,
            len(daily) - 2,
            len(daily) - 3
        ]

        for current_pos in positions:

            previous_pos = current_pos - 1

            if current_pos < 0:

                return None

            if previous_pos < 0:

                return None

            if current_pos >= len(daily):

                return None

            if previous_pos >= len(daily):

                return None

            current_price = float(
                daily.iloc[
                    current_pos
                ]
            )

            previous_price = float(
                daily.iloc[
                    previous_pos
                ]
            )

            if previous_price <= 0:

                result.append(0.0)

                continue

            change = (
                (
                    current_price
                    -
                    previous_price
                )
                /
                previous_price
                *
                100
            )

            result.append(
                round(
                    change,
                    2
                )
            )

        return result

    except Exception as e:

        logging.error(
            f"업비트 변동률 처리 오류 "
            f"{market} : {e}"
        )

        return None


# =========================================================
# OKX 변동률
# =========================================================

def get_okx_change(
    inst_id
):

    try:

        df = get_okx_ohlcv(
            inst_id,
            "1H",
            200
        )

        if (
            df is None
            or
            df.empty
        ):

            return None

        if "ts" not in df.columns:

            return None

        if "c" not in df.columns:

            return None

        df = df.copy()

        df["ts"] = pd.to_numeric(
            df["ts"],
            errors="coerce"
        )

        df["c"] = pd.to_numeric(
            df["c"],
            errors="coerce"
        )

        df["datetime"] = (
            pd.to_datetime(
                df["ts"],
                unit="ms",
                errors="coerce"
            )
            +
            pd.Timedelta(hours=9)
        )

        df = df.dropna(
            subset=[
                "datetime",
                "c"
            ]
        )

        if df.empty:

            return None

        df = (
            df.sort_values("datetime")
            .reset_index(drop=True)
        )

        daily = (
            df.set_index("datetime")["c"]
            .resample(
                "1D",
                offset="9h"
            )
            .last()
            .dropna()
        )

        if len(daily) < 4:

            return None

        result = []

        positions = [
            len(daily) - 1,
            len(daily) - 2,
            len(daily) - 3
        ]

        for current_pos in positions:

            previous_pos = current_pos - 1

            if (
                current_pos < 0
                or
                previous_pos < 0
                or
                current_pos >= len(daily)
                or
                previous_pos >= len(daily)
            ):

                return None

            current_price = float(
                daily.iloc[
                    current_pos
                ]
            )

            previous_price = float(
                daily.iloc[
                    previous_pos
                ]
            )

            if previous_price <= 0:

                result.append(0.0)

                continue

            change = (
                (
                    current_price
                    -
                    previous_price
                )
                /
                previous_price
                *
                100
            )

            result.append(
                round(
                    change,
                    2
                )
            )

        return result

    except Exception as e:

        logging.error(
            f"OKX 변동률 처리 오류 "
            f"{inst_id} : {e}"
        )

        return None


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

        return "⚪ N/A"

    x = changes[0]

    if x > 0:

        return (
            f"🟩 +{x:.2f}%"
        )

    if x < 0:

        return (
            f"🟥 {x:.2f}%"
        )

    return (
        f"⬜ {x:.2f}%"
    )


# =========================================================
# 당일 해 / 구름
# =========================================================

def weather_html(
    change
):

    weather = get_today_weather(
        change
    )

    if weather["type"] == "long":

        return (
            '<span class="weather-long">'
            '☀️'
            '</span>'
        )

    if weather["type"] == "short":

        return (
            '<span class="weather-short">'
            '☁️'
            '</span>'
        )

    return (
        '<span class="weather-none">'
        '⚪'
        '</span>'
    )


# =========================================================
# LONG / SHORT 표시
# =========================================================

def direction_html(
    direction,
    warning
):

    if direction == "long":

        if warning in (
            "pre",
            "1"
        ):

            return (
                '<span class="signal-long signal-blink">'
                'LONG'
                '</span>'
            )

        return (
            '<span class="signal-long">'
            'LONG'
            '</span>'
        )

    if direction == "short":

        if warning in (
            "pre",
            "1"
        ):

            return (
                '<span class="signal-short signal-blink">'
                'SHORT'
                '</span>'
            )

        return (
            '<span class="signal-short">'
            'SHORT'
            '</span>'
        )

    return ""


# =========================================================
# 경고 표시
#
# 🚨만 표시
# 🚀 제거
# =========================================================

def warning_html(
    warning
):

    if warning == "pre":

        return (
            '<span class="warning-pre">'
            '🚨'
            '</span>'
        )

    return ""


# =========================================================
# EMA HTML
#
# 1H / 4H 두 줄
# =========================================================

def ema_html(
    ema_1h,
    ema_4h
):

    return f"""
    <div class="ema-value">

        <div class="ema-line">
            <span class="ema-label">1H</span>
            <span>{ema_1h}</span>
        </div>

        <div class="ema-line">
            <span class="ema-label">4H</span>
            <span>{ema_4h}</span>
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
# 1H 현재 진행 캔들 포함
# 4H는 표시용만 유지
#
# 돌파 판단은 1H만
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

    if raw1h is None:

        return {
            "1h_ema": empty_ema(),
            "4h_ema": empty_ema(),
            "change": None,
            "weather": "none",
            "warning": "none"
        }

    # -----------------------------------------------------
    # 1H EMA
    # -----------------------------------------------------

    ema1h = check_ema(
        raw1h
    )

    # -----------------------------------------------------
    # 4H EMA는 화면 표시용
    # -----------------------------------------------------

    if raw4h is not None:

        ema4h = check_ema(
            raw4h
        )

    else:

        ema4h = empty_ema()

    # -----------------------------------------------------
    # 당일 변동률
    # -----------------------------------------------------

    change = get_today_change_from_1h(
        raw1h
    )

    weather = get_today_weather(
        change
    )

    # -----------------------------------------------------
    # 1H 돌파
    # -----------------------------------------------------

    raw1h_for_signal = raw1h.copy()

    signal = get_1h_breakout_signal(
        market,
        raw1h_for_signal,
        weather["type"]
    )

    warning = apply_breakout_state(
        market,
        signal
    )

    return {
        "1h_ema": ema1h,
        "4h_ema": ema4h,
        "change": change,
        "weather": weather["type"],
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

    if df1h is None:

        return {
            "1h_ema": empty_ema(),
            "4h_ema": empty_ema(),
            "change": None,
            "weather": "none",
            "warning": "none"
        }

    ema1h = check_ema(
        df1h
    )

    if df4h is not None:

        ema4h = check_ema(
            df4h
        )

    else:

        ema4h = empty_ema()

    # -----------------------------------------------------
    # 당일 변동률
    # -----------------------------------------------------

    change = get_today_change_from_1h(
        df1h
    )

    weather = get_today_weather(
        change
    )

    # -----------------------------------------------------
    # 1H 돌파만 사용
    # -----------------------------------------------------

    signal = get_1h_breakout_signal(
        inst_id,
        df1h,
        weather["type"]
    )

    warning = apply_breakout_state(
        inst_id,
        signal
    )

    return {
        "1h_ema": ema1h,
        "4h_ema": ema4h,
        "change": change,
        "weather": weather["type"],
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

            df = get_okx_ohlcv(
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
# 경고 표시 여부
#
# 업비트:
# LONG만
#
# OKX:
# LONG / SHORT
# =========================================================

def is_upbit_visible(
    weather,
    warning
):

    return (
        weather == "long"
        and
        warning == "pre"
    )


def is_okx_visible(
    weather,
    warning
):

    return (
        weather in (
            "long",
            "short"
        )
        and
        warning == "pre"
    )


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

            ema = get_upbit_ema(
                market
            )

            if ema is None:

                continue

            warning = ema.get(
                "warning",
                "none"
            )

            weather = ema.get(
                "weather",
                "none"
            )

            # -------------------------------------------------
            # 업비트는 LONG만
            # -------------------------------------------------

            if not is_upbit_visible(
                weather,
                warning
            ):

                continue

            change = ema.get(
                "change"
            )

            rows.append(
                {
                    "rank": rank,

                    "name": coin,

                    "change": (
                        format_change(
                            [change]
                        )
                        if change is not None
                        else "⚪ N/A"
                    ),

                    "change_percent":
                        change,

                    "weather":
                        weather,

                    "volume":
                        format_volume(
                            volume_map[market]
                        ),

                    "direction":
                        "long",

                    "warning":
                        warning,

                    "ema_1h":
                        ema["1h_ema"],

                    "ema_4h":
                        ema["4h_ema"]
                }
            )

        except Exception as e:

            logging.error(
                f"업비트 상세 오류 "
                f"{market} : {e}"
            )

            # 해당 종목만 건너뜀
            continue

    latest_upbit_data = rows

    logging.info(
        f"업비트 LONG 돌파 종목 "
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

            coin = f"{coin}[UP]"

        try:

            ema = get_okx_ema(
                symbol
            )

            if ema is None:

                continue

            warning = ema.get(
                "warning",
                "none"
            )

            weather = ema.get(
                "weather",
                "none"
            )

            # -------------------------------------------------
            # OKX는 LONG / SHORT 모두
            # -------------------------------------------------

            if not is_okx_visible(
                weather,
                warning
            ):

                continue

            change = ema.get(
                "change"
            )

            direction = (
                "long"
                if weather == "long"
                else
                "short"
            )

            rows.append(
                {
                    "rank": rank,

                    "name": coin,

                    "change": (
                        format_change(
                            [change]
                        )
                        if change is not None
                        else "⚪ N/A"
                    ),

                    "change_percent":
                        change,

                    "weather":
                        weather,

                    "volume":
                        format_volume(
                            volume_map[symbol]
                        ),

                    "direction":
                        direction,

                    "warning":
                        warning,

                    "ema_1h":
                        ema["1h_ema"],

                    "ema_4h":
                        ema["4h_ema"]
                }
            )

        except Exception as e:

            logging.error(
                f"OKX 상세 오류 "
                f"{symbol} : {e}"
            )

            continue

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

    logging.info(
        "========================================"
    )

    logging.info(
        "1분 현재상태 조회 시작"
    )

    # -----------------------------------------------------
    # 업비트
    # -----------------------------------------------------

    if USE_UPBIT == "Y":

        try:

            update_upbit()

        except Exception as e:

            logging.exception(
                f"업비트 업데이트 오류 : {e}"
            )

    else:

        global latest_upbit_data

        latest_upbit_data = []

    # -----------------------------------------------------
    # OKX
    # -----------------------------------------------------

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

        global latest_okx_data

        latest_okx_data = []


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

    margin: 3px 2px 6px 2px;
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

    width: 7%;
}

th:nth-child(2),
td:nth-child(2) {

    width: 18%;
}

th:nth-child(3),
td:nth-child(3) {

    width: 17%;
}

th:nth-child(4),
td:nth-child(4) {

    width: 35%;
}

th:nth-child(5),
td:nth-child(5) {

    width: 23%;
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

.weather {

    margin-top: 3px;
    font-size: 10px;
    line-height: 1;
}


/* =====================================================
   거래대금
   ===================================================== */

.volume-value {

    font-size: 7px;
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
    gap: 3px;
    width: 100%;
}

.change-item {

    display: block;
    width: 100%;
    font-size: 7px;
    text-align: center;
    white-space: nowrap;
}


/* =====================================================
   LONG / SHORT
   ===================================================== */

.direction-wrap {

    min-height: 13px;
    display: flex;
    justify-content: center;
    align-items: center;
}

.signal-long,
.signal-short {

    display: inline-block;
    font-size: 8px;
    font-weight: 900;
    letter-spacing: 0.3px;
    white-space: nowrap;
}

.signal-long {

    color: #35e66d;
    text-shadow:
        0 0 4px rgba(53,230,109,0.55);
}

.signal-short {

    color: #ff4d4d;
    text-shadow:
        0 0 4px rgba(255,77,77,0.55);
}


/* =====================================================
   반짝임
   ===================================================== */

.signal-blink {

    animation: signalBlink 1s infinite;
}

@keyframes signalBlink {

    0%,
    100% {

        opacity: 1;

    }

    50% {

        opacity: 0.35;

    }
}


/* =====================================================
   경고
   ===================================================== */

.breakout-warning {

    display: flex;
    justify-content: center;
    align-items: center;
    min-height: 13px;
}

.warning-pre {

    font-size: 9px;
    font-weight: bold;

    animation: warningBlink 0.9s infinite;

    filter:
        drop-shadow(
            0 0 4px
            rgba(255,180,0,0.85)
        );
}

@keyframes warningBlink {

    0%,
    100% {

        opacity: 1;

    }

    50% {

        opacity: 0.25;

    }
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

.ema-line {

    display: flex;
    justify-content: center;
    align-items: center;
    gap: 3px;
    height: 15px;
}

.ema-label {

    color: #8f949d;
    font-size: 7px;
    font-weight: bold;
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

        font-size: 7px;
    }

    .weather {

        font-size: 9px;
    }

    .volume-value {

        font-size: 6px;
    }

    .change-item {

        font-size: 7px;
    }

    .signal-long,
    .signal-short {

        font-size: 7px;
    }

    .warning-pre {

        font-size: 8px;
    }

    .ema-value {

        font-size: 7px;
    }

    .ema-line {

        height: 16px;
    }

    .ema-label {

        font-size: 7px;
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
            "none"
        )

        weather = item.get(
            "weather",
            "none"
        )

        direction = item.get(
            "direction",
            "none"
        )

        change = item.get(
            "change_percent"
        )

        weather_icon = weather_html(
            change
        )

        signal = direction_html(
            direction,
            warning
        )

        warning_text = warning_html(
            warning
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

<div class="weather">
{weather_icon}
</div>

</td>


<td>

<div class="volume-value">
{item["volume"]}
</div>

<div class="direction-wrap">
{signal}
</div>

</td>


<td>

<div class="today-wrap">

<div class="change-item">
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

※ TOP{TOP_N} 거래대금 순위<br>

※ 코인명 아래 ☀️ = 당일 양수 / ☁️ = 당일 음수<br>

※ 거래대금 아래 LONG = 상승 정배열 / SHORT = 하락 역배열<br>

※ LONG은 당일 양수 + 30-60-120 정배열 조건<br>

※ SHORT는 당일 음수 + 30-60-120 역배열 조건<br>

※ 🚨 = 최근 {BREAKOUT_LOOKBACK}개 캔들 고점/저점 돌파 전<br>

※ 돌파 조건은 1H에서만 판단<br>

※ 정배열이 유지되는 상태에서 직전 고점을 다시 돌파할 때 신호<br>

※ 조정 중 생성된 낮은 고점도 이후 돌파 기준으로 추적<br>

※ 이미 돌파한 종목은 같은 돌파 사이클에서 중복 경고하지 않음<br>

※ EMA는 1H / 4H 두 줄 표시<br>

※ EMA 기준은 30-60-120

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
1H 파동 돌파 · 30-60-120 정배열
</div>

<div>
최근 {BREAKOUT_LOOKBACK}개 캔들 고점/저점 기준
</div>

<div>
☀️ 양수 LONG · ☁️ 음수 SHORT · 🚨 돌파 전
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

    # -----------------------------------------------------
    # 최초 즉시 조회
    # -----------------------------------------------------

    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    # -----------------------------------------------------
    # 1분마다 조회
    # -----------------------------------------------------

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
