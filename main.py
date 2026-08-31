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

# 기존 돌파 범위
BREAKOUT_LOOKBACK = 10

# =========================================================
# 스윙 고점 설정
# =========================================================

# 스윙 고점을 찾기 위한 좌우 캔들 수
SWING_LEFT = 2
SWING_RIGHT = 2

# 최소 분석 캔들 수
MIN_SWING_CANDLES = 10

# 고점 이후 최소 조정 캔들
MIN_PULLBACK_CANDLES = 2


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
# 경고 상태 저장
#
# 같은 스윙 고점을 계속 돌파하고 있는 동안
# 매 1분마다 중복 경고가 발생하지 않도록 사용
# =========================================================

breakout_state_lock = threading.Lock()

breakout_states = {}


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
# 기존처럼 확정 캔들만 사용
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
# EMA 10-30
# =========================================================

def get_ema_10_30_direction(
    df,
    column
):

    ema10 = get_ema(
        df,
        column,
        10
    )

    ema30 = get_ema(
        df,
        column,
        30
    )

    if (
        ema10 is None
        or
        ema30 is None
    ):

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
# EMA 30-60-120
#
# 이번 조건의 핵심 정배열
# =========================================================

def get_ema_30_60_120_direction(
    df,
    column
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
        or
        ema60 is None
        or
        ema120 is None
    ):

        return "none"

    values = [
        ema30.iloc[-1],
        ema60.iloc[-1],
        ema120.iloc[-1]
    ]

    if any(
        pd.isna(x)
        for x in values
    ):

        return "none"

    if (
        values[0]
        >
        values[1]
        >
        values[2]
    ):

        return "long"

    if (
        values[0]
        <
        values[1]
        <
        values[2]
    ):

        return "short"

    return "none"


# =========================================================
# EMA 10-30-60 표시
#
# 기존 화면 표시 유지
# =========================================================

def get_ema_10_30_60_direction(
    df,
    column
):

    ema10 = get_ema(
        df,
        column,
        10
    )

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

    if (
        ema10 is None
        or
        ema30 is None
        or
        ema60 is None
    ):

        return "none"

    values = [
        ema10.iloc[-1],
        ema30.iloc[-1],
        ema60.iloc[-1]
    ]

    if any(
        pd.isna(x)
        for x in values
    ):

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

    if (
        df is None
        or
        len(df) < 60
    ):

        return "none", 0

    ema10 = get_ema(
        df,
        column,
        10
    )

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
# 메인 방향
#
# 화면 EMA는 10-30-60
# =========================================================

def get_main_direction(
    df,
    column
):

    if (
        df is not None
        and
        len(df) >= 60
    ):

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
# 스윙 고점 판정
#
# 중앙 캔들의 고가가 좌우 고가보다 높은 경우
# =========================================================

def is_swing_high(
    df,
    index
):

    if df is None:

        return False

    if index < SWING_LEFT:

        return False

    if (
        index + SWING_RIGHT
        >= len(df)
    ):

        return False

    current_high = float(
        df.iloc[index]["h"]
    )

    if pd.isna(current_high):

        return False

    left = pd.to_numeric(
        df.iloc[
            index - SWING_LEFT:index
        ]["h"],
        errors="coerce"
    )

    right = pd.to_numeric(
        df.iloc[
            index + 1:
            index + SWING_RIGHT + 1
        ]["h"],
        errors="coerce"
    )

    if left.isna().any() or right.isna().any():

        return False

    return (
        current_high > left.max()
        and
        current_high >= right.max()
    )


# =========================================================
# 스윙 저점 판정
#
# 숏 조건용
# =========================================================

def is_swing_low(
    df,
    index
):

    if df is None:

        return False

    if index < SWING_LEFT:

        return False

    if (
        index + SWING_RIGHT
        >= len(df)
    ):

        return False

    current_low = float(
        df.iloc[index]["l"]
    )

    if pd.isna(current_low):

        return False

    left = pd.to_numeric(
        df.iloc[
            index - SWING_LEFT:index
        ]["l"],
        errors="coerce"
    )

    right = pd.to_numeric(
        df.iloc[
            index + 1:
            index + SWING_RIGHT + 1
        ]["l"],
        errors="coerce"
    )

    if left.isna().any() or right.isna().any():

        return False

    return (
        current_low < left.min()
        and
        current_low <= right.min()
    )


# =========================================================
# 최근 유효 스윙 고점 찾기
#
# 중요한 부분
#
# 첫 번째 고점 A
#      ↓
# 조정
#      ↓
# 두 번째 고점 B
#
# B가 A보다 낮아도 B를 새로운 기준 고점으로 사용
# =========================================================

def find_latest_swing_high(
    df,
    direction
):

    if (
        df is None
        or
        len(df) < MIN_SWING_CANDLES
    ):

        return None

    end_index = (
        len(df)
        -
        SWING_RIGHT
        -
        1
    )

    if end_index <= SWING_LEFT:

        return None

    ema30 = get_ema(
        df,
        "c",
        30
    )

    ema60 = get_ema(
        df,
        "c",
        60
    )

    ema120 = get_ema(
        df,
        "c",
        120
    )

    # =====================================================
    # LONG
    # =====================================================

    if direction == "long":

        for index in range(
            end_index,
            SWING_LEFT - 1,
            -1
        ):

            if not is_swing_high(
                df,
                index
            ):

                continue

            # -------------------------------------------------
            # 스윙 고점 생성 당시 정배열 확인
            # -------------------------------------------------

            if (
                ema30 is not None
                and
                ema60 is not None
                and
                ema120 is not None
            ):

                values = [
                    ema30.iloc[index],
                    ema60.iloc[index],
                    ema120.iloc[index]
                ]

                if any(
                    pd.isna(x)
                    for x in values
                ):

                    continue

                if not (
                    values[0]
                    >
                    values[1]
                    >
                    values[2]
                ):

                    continue

            # -------------------------------------------------
            # 고점 이후 조정 캔들이 존재해야 함
            # -------------------------------------------------

            correction_start = index + 1

            correction_end = (
                len(df) - 1
            )

            correction_count = (
                correction_end
                -
                correction_start
                +
                1
            )

            if (
                correction_count
                <
                MIN_PULLBACK_CANDLES
            ):

                continue

            # -------------------------------------------------
            # 고점 이후 현재까지
            # 고점을 이미 돌파했다면
            # 현재 후보로 사용하지 않음
            #
            # 현재 진행 중인 최초 돌파만 찾기 위해
            # 기존 고점보다 높은 종가가 없는지 확인
            # -------------------------------------------------

            after_high = df.iloc[
                correction_start:
                correction_end + 1
            ]

            if after_high.empty:

                continue

            previous_close_max = pd.to_numeric(
                after_high["c"],
                errors="coerce"
            ).max()

            high_value = float(
                df.iloc[index]["h"]
            )

            if (
                pd.notna(previous_close_max)
                and
                previous_close_max > high_value
            ):

                continue

            return {
                "index": index,
                "price": high_value
            }

    # =====================================================
    # SHORT
    # =====================================================

    if direction == "short":

        for index in range(
            end_index,
            SWING_LEFT - 1,
            -1
        ):

            if not is_swing_low(
                df,
                index
            ):

                continue

            if (
                ema30 is not None
                and
                ema60 is not None
                and
                ema120 is not None
            ):

                values = [
                    ema30.iloc[index],
                    ema60.iloc[index],
                    ema120.iloc[index]
                ]

                if any(
                    pd.isna(x)
                    for x in values
                ):

                    continue

                if not (
                    values[0]
                    <
                    values[1]
                    <
                    values[2]
                ):

                    continue

            correction_start = index + 1

            correction_end = (
                len(df) - 1
            )

            correction_count = (
                correction_end
                -
                correction_start
                +
                1
            )

            if (
                correction_count
                <
                MIN_PULLBACK_CANDLES
            ):

                continue

            after_low = df.iloc[
                correction_start:
                correction_end + 1
            ]

            if after_low.empty:

                continue

            previous_close_min = pd.to_numeric(
                after_low["c"],
                errors="coerce"
            ).min()

            low_value = float(
                df.iloc[index]["l"]
            )

            if (
                pd.notna(previous_close_min)
                and
                previous_close_min < low_value
            ):

                continue

            return {
                "index": index,
                "price": low_value
            }

    return None


# =========================================================
# 현재 정배열 확인
#
# 돌파 시점에도 반드시 30-60-120 유지
# =========================================================

def is_current_ema_aligned(
    df,
    direction
):

    if (
        df is None
        or
        len(df) < 120
    ):

        return False

    ema30 = get_ema(
        df,
        "c",
        30
    )

    ema60 = get_ema(
        df,
        "c",
        60
    )

    ema120 = get_ema(
        df,
        "c",
        120
    )

    if (
        ema30 is None
        or
        ema60 is None
        or
        ema120 is None
    ):

        return False

    a = ema30.iloc[-1]

    b = ema60.iloc[-1]

    c = ema120.iloc[-1]

    if (
        pd.isna(a)
        or
        pd.isna(b)
        or
        pd.isna(c)
    ):

        return False

    if direction == "long":

        return (
            a > b > c
        )

    if direction == "short":

        return (
            a < b < c
        )

    return False


# =========================================================
# 스윙 돌파 신호
#
# 반환
#
# none = 없음
# pre  = 돌파 전
# 1    = 최초 돌파
# =========================================================

def get_swing_breakout_signal(
    df,
    direction
):

    if (
        df is None
        or
        len(df) < MIN_SWING_CANDLES
    ):

        return {
            "state": "none",
            "level": None,
            "index": None
        }

    # =====================================================
    # 현재 정배열이 아니면 즉시 무효
    # =====================================================

    if not is_current_ema_aligned(
        df,
        direction
    ):

        return {
            "state": "none",
            "level": None,
            "index": None
        }

    candidate = find_latest_swing_high(
        df,
        direction
    )

    if candidate is None:

        return {
            "state": "none",
            "level": None,
            "index": None
        }

    level = float(
        candidate["price"]
    )

    current = df.iloc[-1]

    current_open = float(
        current["o"]
    )

    current_high = float(
        current["h"]
    )

    current_close = float(
        current["c"]
    )

    # =====================================================
    # LONG
    #
    # 양봉으로 스윙 고점 돌파
    # =====================================================

    if direction == "long":

        # 돌파 완료
        if (
            current_close > level
            and
            current_close > current_open
        ):

            return {
                "state": "1",
                "level": level,
                "index": candidate["index"]
            }

        # 돌파 직전
        if (
            current_high >= level
            and
            current_close <= level
            and
            current_close >= current_open
        ):

            return {
                "state": "pre",
                "level": level,
                "index": candidate["index"]
            }

    # =====================================================
    # SHORT
    #
    # 음봉으로 스윙 저점 이탈
    # =====================================================

    if direction == "short":

        if (
            current_close < level
            and
            current_close < current_open
        ):

            return {
                "state": "1",
                "level": level,
                "index": candidate["index"]
            }

        if (
            current_low <= level
            and
            current_close >= level
            and
            current_close <= current_open
        ):

            return {
                "state": "pre",
                "level": level,
                "index": candidate["index"]
            }

    return {
        "state": "none",
        "level": level,
        "index": candidate["index"]
    }


# =========================================================
# 상태 저장
#
# 같은 스윙 고점에 대해
# 최초 1회만 돌파 신호 허용
# =========================================================

def apply_breakout_state(
    symbol,
    timeframe,
    signal
):

    state = signal.get(
        "state",
        "none"
    )

    level = signal.get(
        "level"
    )

    index = signal.get(
        "index"
    )

    key = (
        f"{symbol}|"
        f"{timeframe}"
    )

    with breakout_state_lock:

        previous = breakout_states.get(
            key
        )

        # =================================================
        # 현재 후보 자체가 없으면 상태 제거
        # =================================================

        if (
            level is None
            or
            index is None
        ):

            breakout_states.pop(
                key,
                None
            )

            return "none"

        # =================================================
        # 새로운 스윙 고점이면
        # 새로운 돌파 사이클 시작
        # =================================================

        if (
            previous is None
            or
            previous.get("index") != index
            or
            previous.get("level") != level
        ):

            breakout_states[key] = {
                "index": index,
                "level": level,
                "triggered": False
            }

            previous = breakout_states[key]

        # =================================================
        # 돌파 전
        # =================================================

        if state == "pre":

            return "pre"

        # =================================================
        # 최초 돌파
        # =================================================

        if state == "1":

            if previous.get(
                "triggered",
                False
            ):

                return "none"

            previous["triggered"] = True

            return "1"

        # =================================================
        # 그 외
        # =================================================

        return "none"


# =========================================================
# 최종 시간봉 돌파
#
# 30-60-120 정배열 유지
# 스윙 고점/저점 돌파
# =========================================================

def get_timeframe_breakout(
    df,
    direction,
    symbol,
    timeframe
):

    signal = get_swing_breakout_signal(
        df,
        direction
    )

    return apply_breakout_state(
        symbol,
        timeframe,
        signal
    )


# =========================================================
# 1H + 4H 경고
#
# 둘 중 하나라도 경고이면 표시
# =========================================================

def get_combined_breakout_warning(
    df1h,
    df4h,
    symbol
):

    direction_1h = get_main_direction(
        df1h,
        "c"
    )

    direction_4h = get_main_direction(
        df4h,
        "c"
    )

    warning_1h = get_timeframe_breakout(
        df1h,
        direction_1h,
        symbol,
        "1H"
    )

    warning_4h = get_timeframe_breakout(
        df4h,
        direction_4h,
        symbol,
        "4H"
    )

    return {
        "1h": warning_1h,
        "4h": warning_4h
    }


# =========================================================
# 경고 표시 여부
#
# 🚨 돌파 전만 표시
# 🚀는 현재 요청에서 제외
# =========================================================

def is_visible_combined_warning(
    warning
):

    if not warning:

        return False

    return (
        warning.get("1h")
        ==
        "pre"
        or
        warning.get("4h")
        ==
        "pre"
    )


# =========================================================
# 경고 HTML
#
# 현재 요청:
# 🚨만 표시
# =========================================================

def combined_warning_html(
    warning
):

    if not warning:

        return ""

    result = []

    warning_1h = warning.get(
        "1h",
        "none"
    )

    warning_4h = warning.get(
        "4h",
        "none"
    )

    if warning_1h == "pre":

        result.append(
            '<span class="warning-pre">🚨1H</span>'
        )

    if warning_4h == "pre":

        result.append(
            '<span class="warning-pre">🚨4H</span>'
        )

    if not result:

        return ""

    return " ".join(result)


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
        120
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
# 1시간
# 4시간
# 두 줄 표시
# =========================================================

def ema_html(
    ema_1h,
    ema_4h
):

    return f"""
    <div class="ema-value">

        <div class="ema-row">
            <span class="ema-label">1H</span>
            <span>{ema_1h}</span>
        </div>

        <div class="ema-row">
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
# 현재 진행 중인 캔들을 포함
#
# 1분마다 조회하므로
# 현재 상태를 즉시 반영
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
        or
        raw4h is None
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

    # =====================================================
    # EMA
    # =====================================================

    ema1h = check_ema(
        df1h
    )

    ema4h = check_ema(
        df4h
    )

    # =====================================================
    # 돌파 경고
    # =====================================================

    warning = get_combined_breakout_warning(
        df1h,
        df4h,
        market
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
        or
        df4h is None
    ):

        return {
            "1h_ema": empty_ema(),
            "4h_ema": empty_ema(),
            "warning": {
                "1h": "none",
                "4h": "none"
            }
        }

    ema1h = check_ema(
        df1h
    )

    ema4h = check_ema(
        df4h
    )

    warning = get_combined_breakout_warning(
        df1h,
        df4h,
        inst_id
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

            df = get_okx_ohlcv(
                inst_id,
                "1H",
                hours + 1
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

            ema = get_upbit_ema(
                market
            )

            warning = ema.get(
                "warning",
                {}
            )

            # =================================================
            # 🚨가 있는 종목만 표시
            # =================================================

            if not is_visible_combined_warning(
                warning
            ):

                continue

            change_percent = (
                changes[0]
                if (
                    changes is not None
                    and
                    len(changes) > 0
                )
                else None
            )

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

            ema = get_okx_ema(
                symbol
            )

            warning = ema.get(
                "warning",
                {}
            )

            if not is_visible_combined_warning(
                warning
            ):

                continue

            change_percent = (
                changes[0]
                if (
                    changes is not None
                    and
                    len(changes) > 0
                )
                else None
            )

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

    width: 17%;
}

th:nth-child(3),
td:nth-child(3) {

    width: 15%;
}

th:nth-child(4),
td:nth-child(4) {

    width: 29%;
}

th:nth-child(5),
td:nth-child(5) {

    width: 32%;
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
    gap: 4px;
    width: 100%;
    min-height: 15px;
    white-space: nowrap;
}

.warning-pre {

    font-size: 10px;
    font-weight: bold;

    filter:
        drop-shadow(
            0 0 5px
            rgba(255,180,0,0.8)
        );

    animation:
        warningBlink
        0.9s
        infinite;
}


/* =====================================================
   반짝임
   ===================================================== */

@keyframes warningBlink {

    0% {

        opacity: 1;

        transform:
            scale(1);
    }

    50% {

        opacity: 0.35;

        transform:
            scale(0.94);
    }

    100% {

        opacity: 1;

        transform:
            scale(1);
    }
}


/* =====================================================
   EMA
   ===================================================== */

.ema-value {

    width: 100%;
    font-size: 9px;
    font-weight: bold;
    line-height: 1.5;
    white-space: nowrap;
}

.ema-row {

    display: flex;
    align-items: center;
    justify-content: center;
    gap: 4px;
    min-height: 18px;
}

.ema-label {

    color: #8f949d;
    font-size: 8px;
    font-weight: bold;
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

    .warning-pre {

        font-size: 9px;
    }

    .ema-value {

        font-size: 8px;
        line-height: 1.5;
    }

    .ema-row {

        min-height: 18px;
    }

    .ema-label {

        font-size: 7px;
    }

}

"""


# =========================================================
# 테이블 행 생성
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
        font-size:7px;
        line-height:1.5;
        margin:5px 2px 8px 2px;
     ">

※ TOP{TOP_N} 거래대금 실제 순위 기준<br>

※ 오늘 1줄 = 당일 변동률<br>

※ 오늘 2줄 = 1H / 4H 돌파 경고<br>

※ 🚨1H = 1시간 스윙 고점 돌파 전<br>

※ 🚨4H = 4시간 스윙 고점 돌파 전<br>

※ 30-60-120 정배열 유지 조건<br>

※ 상승 → 고점 → 조정 → 재반등 구조 확인<br>

※ 첫 고점보다 낮은 두 번째 고점도 새로운 돌파 기준으로 인정<br>

※ 정배열이 깨지면 해당 돌파 조건 무효<br>

※ EMA는 1H / 4H 두 줄 표시<br>

※ EMA 화면 표시는 10-30-60 정렬 기준

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
1H + 4H 추세 · 스윙 고점 돌파
</div>

<div>
30-60-120 정배열 유지
</div>

<div>
상승 → 고점 → 조정 → 재반등 → 고점 돌파
</div>

<div>
TOP{TOP_N} · 🚨 돌파 전
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
        "돌파 기준 = 30-60-120 정배열"
    )

    logging.info(
        "스윙 고점 추적 = 활성화"
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
