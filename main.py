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

BREAKOUT_LOOKBACK = 10

# 고점과 저점 사이 최소 캔들
MIN_SWING_BARS = 2

# 파동 추적에 사용할 최대 과거 캔들
WAVE_LOOKBACK = 100


# =========================================================
# 거래소
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
# OKX 재시도
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
# EMA 30-60-120
# =========================================================

def get_ema_30_60_120_direction(
    df
):

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

        return "none"

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

        return "none"

    if a > b > c:

        return "long"

    if a < b < c:

        return "short"

    return "none"


# =========================================================
# EMA 정렬 카운트
# =========================================================

def get_ema_30_60_120_alignment_count(
    df
):

    if df is None or len(df) < 120:

        return "none", 0

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

        return "none", 0

    current_direction = None

    count = 0

    for index in range(
        len(df) - 1,
        -1,
        -1
    ):

        a = ema30.iloc[index]
        b = ema60.iloc[index]
        c = ema120.iloc[index]

        if (
            pd.isna(a)
            or
            pd.isna(b)
            or
            pd.isna(c)
        ):

            break

        if a > b > c:

            direction = "long"

        elif a < b < c:

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
# EMA 표시
# =========================================================

def check_ema(df):

    if df is None:

        return {
            "display": "⚪",
            "direction": "none",
            "count": 0
        }

    direction, count = (
        get_ema_30_60_120_alignment_count(
            df
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
# 파동용 고점/저점
# =========================================================

def is_swing_high(
    df,
    index
):

    if index <= 0:

        return False

    if index >= len(df) - 1:

        return False

    high = float(
        df.iloc[index]["h"]
    )

    left_high = float(
        df.iloc[index - 1]["h"]
    )

    right_high = float(
        df.iloc[index + 1]["h"]
    )

    return (
        high >= left_high
        and
        high >= right_high
    )


def is_swing_low(
    df,
    index
):

    if index <= 0:

        return False

    if index >= len(df) - 1:

        return False

    low = float(
        df.iloc[index]["l"]
    )

    left_low = float(
        df.iloc[index - 1]["l"]
    )

    right_low = float(
        df.iloc[index + 1]["l"]
    )

    return (
        low <= left_low
        and
        low <= right_low
    )


# =========================================================
# LONG 파동 돌파
#
# 상승
#   ↓
# 고점
#   ↓
# 조정
#   ↓
# 낮은 고점
#   ↓
# 재상승
#   ↓
# 이전 고점 돌파
#
# EMA 30-60-120 정배열 유지 시 인정
# =========================================================

def find_long_wave_breakout(
    df
):

    if (
        df is None
        or
        len(df) < 125
    ):

        return False

    df = df.copy().reset_index(drop=True)

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

    current_index = len(df) - 1

    current = df.iloc[current_index]

    # -----------------------------------------------------
    # 현재 캔들
    # 양봉 조건
    # -----------------------------------------------------

    current_open = float(
        current["o"]
    )

    current_close = float(
        current["c"]
    )

    current_high = float(
        current["h"]
    )

    if current_close <= current_open:

        return False

    # -----------------------------------------------------
    # 현재 EMA 정배열
    # -----------------------------------------------------

    e30 = ema30.iloc[current_index]
    e60 = ema60.iloc[current_index]
    e120 = ema120.iloc[current_index]

    if (
        pd.isna(e30)
        or
        pd.isna(e60)
        or
        pd.isna(e120)
    ):

        return False

    if not (
        e30 > e60 > e120
    ):

        return False

    # -----------------------------------------------------
    # 최근 구간에서 고점 후보 수집
    # -----------------------------------------------------

    start = max(
        2,
        current_index - WAVE_LOOKBACK
    )

    swing_highs = []

    for i in range(
        start,
        current_index - 1
    ):

        if is_swing_high(
            df,
            i
        ):

            high_value = float(
                df.iloc[i]["h"]
            )

            swing_highs.append(
                (
                    i,
                    high_value
                )
            )

    if not swing_highs:

        return False

    # -----------------------------------------------------
    # 가장 최근 고점부터 확인
    #
    # 최근 고점보다 현재가가 높아야 함
    # -----------------------------------------------------

    for high_index, high_value in reversed(
        swing_highs
    ):

        if current_high <= high_value:

            continue

        # -------------------------------------------------
        # 고점 이후 조정 구간 확인
        # -------------------------------------------------

        if (
            current_index
            -
            high_index
            <
            MIN_SWING_BARS + 1
        ):

            continue

        correction_low = None

        for j in range(
            high_index + 1,
            current_index
        ):

            low_value = float(
                df.iloc[j]["l"]
            )

            if (
                correction_low is None
                or
                low_value < correction_low
            ):

                correction_low = low_value

        if correction_low is None:

            continue

        # -------------------------------------------------
        # 고점 이후 조정이 실제로 있었는지
        # -------------------------------------------------

        if correction_low >= high_value:

            continue

        # -------------------------------------------------
        # 조정 이후 재상승 확인
        # -------------------------------------------------

        rebound_exists = False

        for j in range(
            high_index + 1,
            current_index
        ):

            close_value = float(
                df.iloc[j]["c"]
            )

            if close_value > correction_low:

                rebound_exists = True

                break

        if not rebound_exists:

            continue

        # -------------------------------------------------
        # 돌파 직전까지 EMA 정배열 유지
        # -------------------------------------------------

        alignment_ok = True

        for j in range(
            high_index,
            current_index + 1
        ):

            a = ema30.iloc[j]
            b = ema60.iloc[j]
            c = ema120.iloc[j]

            if (
                pd.isna(a)
                or
                pd.isna(b)
                or
                pd.isna(c)
            ):

                alignment_ok = False

                break

            if not (
                a > b > c
            ):

                alignment_ok = False

                break

        if not alignment_ok:

            continue

        # -------------------------------------------------
        # 현재 양봉이 해당 고점을 돌파
        # -------------------------------------------------

        if current_close > high_value:

            return True

    return False


# =========================================================
# SHORT 파동 돌파
#
# 하락
#   ↓
# 저점
#   ↓
# 반등
#   ↓
# 높은 저점
#   ↓
# 재하락
#   ↓
# 이전 저점 이탈
#
# EMA 30-60-120 역배열 유지
# =========================================================

def find_short_wave_breakout(
    df
):

    if (
        df is None
        or
        len(df) < 125
    ):

        return False

    df = df.copy().reset_index(drop=True)

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

    current_index = len(df) - 1

    current = df.iloc[current_index]

    current_open = float(
        current["o"]
    )

    current_close = float(
        current["c"]
    )

    current_low = float(
        current["l"]
    )

    # -----------------------------------------------------
    # 음봉
    # -----------------------------------------------------

    if current_close >= current_open:

        return False

    # -----------------------------------------------------
    # 현재 EMA 역배열
    # -----------------------------------------------------

    e30 = ema30.iloc[current_index]
    e60 = ema60.iloc[current_index]
    e120 = ema120.iloc[current_index]

    if (
        pd.isna(e30)
        or
        pd.isna(e60)
        or
        pd.isna(e120)
    ):

        return False

    if not (
        e30 < e60 < e120
    ):

        return False

    # -----------------------------------------------------
    # 최근 저점 수집
    # -----------------------------------------------------

    start = max(
        2,
        current_index - WAVE_LOOKBACK
    )

    swing_lows = []

    for i in range(
        start,
        current_index - 1
    ):

        if is_swing_low(
            df,
            i
        ):

            low_value = float(
                df.iloc[i]["l"]
            )

            swing_lows.append(
                (
                    i,
                    low_value
                )
            )

    if not swing_lows:

        return False

    # -----------------------------------------------------
    # 최근 저점부터 확인
    # -----------------------------------------------------

    for low_index, low_value in reversed(
        swing_lows
    ):

        if current_low >= low_value:

            continue

        if (
            current_index
            -
            low_index
            <
            MIN_SWING_BARS + 1
        ):

            continue

        correction_high = None

        for j in range(
            low_index + 1,
            current_index
        ):

            high_value = float(
                df.iloc[j]["h"]
            )

            if (
                correction_high is None
                or
                high_value > correction_high
            ):

                correction_high = high_value

        if correction_high is None:

            continue

        if correction_high <= low_value:

            continue

        rebound_exists = False

        for j in range(
            low_index + 1,
            current_index
        ):

            close_value = float(
                df.iloc[j]["c"]
            )

            if close_value < correction_high:

                rebound_exists = True

                break

        if not rebound_exists:

            continue

        # -------------------------------------------------
        # 역배열 유지
        # -------------------------------------------------

        alignment_ok = True

        for j in range(
            low_index,
            current_index + 1
        ):

            a = ema30.iloc[j]
            b = ema60.iloc[j]
            c = ema120.iloc[j]

            if (
                pd.isna(a)
                or
                pd.isna(b)
                or
                pd.isna(c)
            ):

                alignment_ok = False

                break

            if not (
                a < b < c
            ):

                alignment_ok = False

                break

        if not alignment_ok:

            continue

        # -------------------------------------------------
        # 현재 음봉이 이전 저점 이탈
        # -------------------------------------------------

        if current_close < low_value:

            return True

    return False


# =========================================================
# 1H 돌파 상태
#
# LONG / SHORT 중 하나만 반환
# =========================================================

def get_1h_breakout_signal(
    df,
    day_change
):

    if (
        df is None
        or
        len(df) < 125
    ):

        return {
            "signal": "none",
            "direction": "none"
        }

    # =====================================================
    # 당일 양수 → LONG만
    # =====================================================

    if day_change is not None and day_change > 0:

        if find_long_wave_breakout(df):

            return {
                "signal": "warning",
                "direction": "long"
            }

        return {
            "signal": "none",
            "direction": "long"
        }

    # =====================================================
    # 당일 음수 → SHORT만
    # =====================================================

    if day_change is not None and day_change < 0:

        if find_short_wave_breakout(df):

            return {
                "signal": "warning",
                "direction": "short"
            }

        return {
            "signal": "none",
            "direction": "short"
        }

    # =====================================================
    # 변동 없음
    # =====================================================

    return {
        "signal": "none",
        "direction": "none"
    }


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
# 1H EMA HTML
# =========================================================

def ema_html(
    ema_1h
):

    return f"""
    <div class="ema-value">
        <span class="ema-title">1H</span>
        <span class="ema-data">
            {ema_1h}
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
# 업비트 EMA
#
# 현재 진행 중인 1H 캔들 제외
# =========================================================

def get_upbit_ema(market):

    raw1h = get_upbit_ohlcv(
        market,
        60,
        200
    )

    if raw1h is None:

        return {
            "1h_ema": empty_ema(),
            "signal": {
                "signal": "none",
                "direction": "none"
            }
        }

    df1h = raw1h.copy()

    # -----------------------------------------------------
    # 현재 진행 중인 캔들 제외
    # -----------------------------------------------------

    if len(df1h) > 1:

        df1h = (
            df1h
            .iloc[:-1]
            .reset_index(drop=True)
        )

    ema1h = check_ema(
        df1h
    )

    return {
        "1h_ema": ema1h
    }


# =========================================================
# OKX EMA
# =========================================================

def get_okx_ema(inst_id):

    df1h = get_okx_ohlcv(
        inst_id,
        "1H",
        200
    )

    if df1h is None:

        return {
            "1h_ema": empty_ema()
        }

    ema1h = check_ema(
        df1h
    )

    return {
        "1h_ema": ema1h
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

            if (
                changes is None
                or
                len(changes) == 0
            ):

                continue

            day_change = changes[0]

            # -------------------------------------------------
            # 1H 캔들
            # -------------------------------------------------

            raw1h = get_upbit_ohlcv(
                market,
                60,
                200
            )

            if raw1h is None:

                continue

            df1h = raw1h.copy()

            # 현재 진행 캔들 제외
            if len(df1h) > 1:

                df1h = (
                    df1h
                    .iloc[:-1]
                    .reset_index(drop=True)
                )

            # -------------------------------------------------
            # EMA
            # -------------------------------------------------

            ema = check_ema(
                df1h
            )

            # -------------------------------------------------
            # 당일 방향 + 1H 돌파
            # -------------------------------------------------

            signal = get_1h_breakout_signal(
                df1h,
                day_change
            )

            # -------------------------------------------------
            # 🚨 조건이 없으면 리스트 제외
            # -------------------------------------------------

            if signal["signal"] != "warning":

                continue

            # -------------------------------------------------
            # 당일 방향과 EMA 방향도 일치해야 함
            # -------------------------------------------------

            if (
                signal["direction"]
                !=
                ema["direction"]
            ):

                continue

            if signal["direction"] == "long":

                direction_html = (
                    '<span class="direction-long">'
                    'LONG'
                    '</span>'
                )

            elif signal["direction"] == "short":

                direction_html = (
                    '<span class="direction-short">'
                    'SHORT'
                    '</span>'
                )

            else:

                continue

            rows.append(
                {
                    "rank": rank,
                    "name": coin,
                    "change": format_change(
                        changes
                    ),
                    "change_percent": day_change,
                    "volume": format_volume(
                        volume_map[market]
                    ),
                    "ema_1h": ema,
                    "direction": direction_html,
                    "signal": "warning"
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

            if (
                changes is None
                or
                len(changes) == 0
            ):

                continue

            day_change = changes[0]

            # -------------------------------------------------
            # 1H
            # -------------------------------------------------

            df1h = get_okx_ohlcv(
                symbol,
                "1H",
                200
            )

            if df1h is None:

                continue

            # -------------------------------------------------
            # EMA
            # -------------------------------------------------

            ema = check_ema(
                df1h
            )

            # -------------------------------------------------
            # 돌파
            # -------------------------------------------------

            signal = get_1h_breakout_signal(
                df1h,
                day_change
            )

            if signal["signal"] != "warning":

                continue

            if (
                signal["direction"]
                !=
                ema["direction"]
            ):

                continue

            if signal["direction"] == "long":

                direction_html = (
                    '<span class="direction-long">'
                    'LONG'
                    '</span>'
                )

            elif signal["direction"] == "short":

                direction_html = (
                    '<span class="direction-short">'
                    'SHORT'
                    '</span>'
                )

            else:

                continue

            rows.append(
                {
                    "rank": rank,
                    "name": coin,
                    "change": format_change(
                        changes
                    ),
                    "change_percent": day_change,
                    "volume": format_volume(
                        volume_map[symbol]
                    ),
                    "ema_1h": ema,
                    "direction": direction_html,
                    "signal": "warning"
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
        "1H 전체 조회 시작"
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

    width: 19%;
}

th:nth-child(3),
td:nth-child(3) {

    width: 20%;
}

th:nth-child(4),
td:nth-child(4) {

    width: 29%;
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


/* =====================================================
   거래대금
   ===================================================== */

.volume-value {

    display: block;
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
    font-size: 8px;
    text-align: center;
    white-space: nowrap;
}


/* =====================================================
   방향
   ===================================================== */

.direction-long,
.direction-short {

    display: inline-flex;
    align-items: center;
    justify-content: center;

    font-size: 8px;
    font-weight: 800;

    min-width: 43px;
    height: 15px;

    border-radius: 4px;

    animation: alarmBlink 1s infinite;
}

.direction-long {

    color: #39ef75;
    border: 1px solid rgba(
        57,
        239,
        117,
        0.45
    );
}

.direction-short {

    color: #ff4f5f;
    border: 1px solid rgba(
        255,
        79,
        95,
        0.45
    );
}


/* =====================================================
   🚨
   ===================================================== */

.breakout-warning {

    display: flex;
    align-items: center;
    justify-content: center;
    gap: 3px;
    min-height: 14px;
}

.warning {

    font-size: 10px;
    line-height: 1;

    animation: alarmBlink 0.9s infinite;
}


/* =====================================================
   반짝임
   ===================================================== */

@keyframes alarmBlink {

    0% {

        opacity: 1;
        transform: scale(1);
    }

    50% {

        opacity: 0.35;
        transform: scale(0.96);
    }

    100% {

        opacity: 1;
        transform: scale(1);
    }
}


/* =====================================================
   EMA
   ===================================================== */

.ema-value {

    width: 100%;

    display: flex;
    align-items: center;
    justify-content: center;

    gap: 3px;

    white-space: nowrap;

    font-size: 9px;
    font-weight: bold;
}

.ema-title {

    color: #8f949d;
    font-size: 8px;
}

.ema-data {

    font-size: 9px;
}


/* =====================================================
   모바일
   ===================================================== */

@media (max-width: 480px) {

    body {

        padding: 3px;
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

    .volume-value {

        font-size: 6px;
    }

    .change-item {

        font-size: 7px;
    }

    .direction-long,
    .direction-short {

        font-size: 7px;
        min-width: 39px;
        height: 14px;
    }

    .warning {

        font-size: 9px;
    }

    .ema-value {

        font-size: 8px;
    }

    .ema-title {

        font-size: 7px;
    }

    .ema-data {

        font-size: 8px;
    }

}

"""


# =========================================================
# 테이블 행
# =========================================================

def make_table_rows(data):

    rows_html = ""

    for item in data:

        warning_html = (
            '<span class="warning">🚨</span>'
            if item.get("signal") == "warning"
            else ""
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

<div style="
    margin-top:3px;
">
{item["direction"]}
</div>

</td>

<td>

<div class="today-wrap">

<div>
{item["change"]}
</div>

<div class="breakout-warning">

{warning_html}

</div>

</div>

</td>

<td>

{ema_html(
    item["ema_1h"].get(
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
        line-height:1.5;
        margin:4px 2px 7px 2px;
     ">

※ 1H 기준<br>

※ EMA 30-60-120 정배열 / 역배열<br>

※ ☀️ 당일 양수 = LONG 조건<br>

※ ☁️ 당일 음수 = SHORT 조건<br>

※ LONG은 당일 양수일 때만 인정<br>

※ SHORT는 당일 음수일 때만 인정<br>

※ 상승 후 조정 → 재상승 → 이전 고점 양봉 돌파<br>

※ 첫 고점보다 낮은 고점도 정배열 유지 시 돌파 대상으로 인정<br>

※ 🚨 조건 발생 종목만 표시<br>

※ 4H 조건은 사용하지 않음

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
1H Wave Breakout
</title>

<style>

{DASHBOARD_CSS}

</style>

</head>

<body>

<h1>
📊 1H Wave Breakout
</h1>

<div class="info">

<div>
1H 파동 + 30-60-120 EMA
</div>

<div>
상승 → 조정 → 재상승 → 이전 고점 돌파
</div>

<div>
☀️ LONG / ☁️ SHORT · 🚨 돌파
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
        "4H 조건 제거"
    )

    logging.info(
        "1H 30-60-120 파동 돌파 모드"
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
    # 최초 조회
    # =====================================================

    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    # =====================================================
    # 1분마다
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
