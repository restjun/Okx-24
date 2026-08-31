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

TOP_N = 100

UPDATE_MINUTES = 1

# 돌파를 판단할 최소 과거 캔들
BREAKOUT_LOOKBACK = 10

# 스윙 고점 판단 좌우 캔들
SWING_LEFT = 2
SWING_RIGHT = 2

# 전고점에 접근했다고 판단하는 거리
# 0.30% 이내면 🚨
PRE_BREAKOUT_DISTANCE = 0.003


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
                        f"({attempt + 1}/{MAX_RETRIES})"
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

        if df.empty:
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
            subset=["o", "h", "l", "c"]
        ).reset_index(drop=True)

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
# EMA 정렬 카운트
# =========================================================

def get_ema_alignment_count(
    df,
    column="c"
):

    if (
        df is None
        or len(df) < 120
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

    if any(
        x is None
        for x in [
            ema30,
            ema60,
            ema120
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

        a = ema30.iloc[index]
        b = ema60.iloc[index]
        c = ema120.iloc[index]

        if any(
            pd.isna(x)
            for x in [a, b, c]
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
# 스윙 고점
#
# 실제 캔들 구조에서 의미 있는 고점을 찾는다.
# 단순 최근 N개 최고가 방식이 아니다.
# =========================================================

def find_swing_highs(
    df,
    left=SWING_LEFT,
    right=SWING_RIGHT
):

    if (
        df is None
        or len(df) < left + right + 1
    ):

        return []

    highs = pd.to_numeric(
        df["h"],
        errors="coerce"
    )

    swing_highs = []

    for i in range(
        left,
        len(df) - right
    ):

        current = highs.iloc[i]

        if pd.isna(current):
            continue

        left_values = highs.iloc[
            i - left:i
        ]

        right_values = highs.iloc[
            i + 1:i + right + 1
        ]

        if (
            current >= left_values.max()
            and
            current >= right_values.max()
        ):

            swing_highs.append(
                {
                    "index": i,
                    "price": float(current)
                }
            )

    return swing_highs


# =========================================================
# 상승 파동 전고점 후보
#
# 가장 최근의 의미 있는 고점을 사용하되
# 현재 캔들보다 과거에 존재하는 고점만 사용한다.
# =========================================================

def get_previous_swing_high(
    df
):

    if (
        df is None
        or len(df) < 10
    ):

        return None

    swings = find_swing_highs(df)

    if not swings:
        return None

    current_index = len(df) - 1

    candidates = [
        x
        for x in swings
        if x["index"] < current_index
    ]

    if not candidates:
        return None

    return candidates[-1]


# =========================================================
# 전고점 돌파 감지
#
# 반환
#
# none = 없음
# pre  = 전고점 직전
# 1    = 첫 돌파 캔들
# =========================================================

def get_swing_breakout_signal(
    df
):

    if (
        df is None
        or len(df) < 130
    ):

        return {
            "signal": "none",
            "level": None,
            "index": None
        }

    df = (
        df
        .copy()
        .reset_index(drop=True)
    )

    direction = (
        get_ema_30_60_120_direction(
            df,
            "c"
        )
    )

    # -----------------------------------------------------
    # LONG만 상승 돌파 감지
    # -----------------------------------------------------

    if direction != "long":

        return {
            "signal": "none",
            "level": None,
            "index": None
        }

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

    if any(
        x is None
        for x in [
            ema30,
            ema60,
            ema120
        ]
    ):

        return {
            "signal": "none",
            "level": None,
            "index": None
        }

    current_index = len(df) - 1

    # -----------------------------------------------------
    # 최근 스윙 고점 검색
    # -----------------------------------------------------

    swing_highs = find_swing_highs(df)

    if not swing_highs:

        return {
            "signal": "none",
            "level": None,
            "index": None
        }

    # 현재 캔들 이전의 고점만 사용
    candidates = [
        x
        for x in swing_highs
        if x["index"] < current_index
    ]

    if not candidates:

        return {
            "signal": "none",
            "level": None,
            "index": None
        }

    # -----------------------------------------------------
    # 가장 최근 스윙 고점
    # -----------------------------------------------------

    previous_swing = candidates[-1]

    high_price = previous_swing["price"]

    # -----------------------------------------------------
    # 최근 캔들
    # -----------------------------------------------------

    row = df.iloc[current_index]

    close = float(row["c"])
    open_price = float(row["o"])
    high = float(row["h"])

    # -----------------------------------------------------
    # 첫 돌파
    #
    # 반드시 양봉
    # 종가가 전고점보다 높아야 함
    # -----------------------------------------------------

    if (
        close > high_price
        and
        close > open_price
    ):

        # 이전 캔들들이 이미 전고점 위에서
        # 계속 머물렀던 경우는 최초 돌파로 보지 않는다.
        prior_closes = pd.to_numeric(
            df["c"].iloc[
                max(
                    0,
                    current_index - BREAKOUT_LOOKBACK
                ):
                current_index
            ],
            errors="coerce"
        )

        already_above = (
            not prior_closes.empty
            and
            (prior_closes > high_price).any()
        )

        if not already_above:

            return {
                "signal": "1",
                "level": high_price,
                "index": current_index
            }

    # -----------------------------------------------------
    # 전고점 직전
    #
    # 현재 고가가 전고점에 접근했지만
    # 아직 종가 돌파는 하지 않은 상태
    # -----------------------------------------------------

    distance = (
        high_price - high
    ) / high_price

    if (
        high < high_price
        and
        distance <= PRE_BREAKOUT_DISTANCE
        and
        close <= high_price
        and
        close >= open_price
    ):

        return {
            "signal": "pre",
            "level": high_price,
            "index": current_index
        }

    return {
        "signal": "none",
        "level": high_price,
        "index": previous_swing["index"]
    }


# =========================================================
# 1H 돌파 경고
# =========================================================

def get_1h_breakout_warning(
    df
):

    result = get_swing_breakout_signal(
        df
    )

    return result


# =========================================================
# 당일 변동률
#
# 기존 out-of-bounds 방어
# =========================================================

def get_upbit_change(market):

    df = get_upbit_ohlcv(
        market,
        60,
        120
    )

    if df is None or len(df) < 24:

        return None

    try:

        df = df.copy()

        if "candle_date_time_kst" not in df.columns:

            return None

        df["datetime"] = pd.to_datetime(
            df["candle_date_time_kst"],
            errors="coerce"
        )

        df = df.dropna(
            subset=["datetime"]
        )

        if df.empty:

            return None

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
            .dropna()
        )

        if len(daily) < 2:

            return None

        result = []

        for i in [-1, -2, -3]:

            try:

                if abs(i) > len(daily):

                    continue

                current_value = float(
                    daily.iloc[i]
                )

                previous_position = i - 1

                if abs(previous_position) > len(daily):

                    continue

                previous_value = float(
                    daily.iloc[
                        previous_position
                    ]
                )

                if previous_value == 0:

                    result.append(0.0)

                    continue

                change = (
                    (
                        current_value
                        -
                        previous_value
                    )
                    /
                    previous_value
                    *
                    100
                )

                result.append(
                    round(change, 2)
                )

            except (
                IndexError,
                KeyError
            ):

                continue

        if not result:

            return None

        return result

    except Exception as e:

        logging.error(
            f"업비트 변동률 오류 "
            f"{market} : {e}"
        )

        return None


# =========================================================
# OKX 변동률
# =========================================================

def get_okx_change(inst_id):

    df = get_okx_ohlcv(
        inst_id,
        "1H",
        120
    )

    if df is None or len(df) < 24:

        return None

    try:

        df = df.copy()

        df["ts"] = pd.to_numeric(
            df["ts"],
            errors="coerce"
        )

        df = df.dropna(
            subset=["ts"]
        )

        if df.empty:
            return None

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
            .dropna()
        )

        if len(daily) < 2:
            return None

        result = []

        for i in [-1, -2, -3]:

            try:

                if abs(i) > len(daily):
                    continue

                current_value = float(
                    daily.iloc[i]
                )

                previous_position = i - 1

                if abs(previous_position) > len(daily):
                    continue

                previous_value = float(
                    daily.iloc[
                        previous_position
                    ]
                )

                if previous_value == 0:

                    result.append(0.0)
                    continue

                change = (
                    (
                        current_value
                        -
                        previous_value
                    )
                    /
                    previous_value
                    *
                    100
                )

                result.append(
                    round(change, 2)
                )

            except IndexError:

                continue

        if not result:
            return None

        return result

    except Exception as e:

        logging.error(
            f"OKX 변동률 오류 "
            f"{inst_id} : {e}"
        )

        return None


# =========================================================
# 변동률 HTML
# =========================================================

def format_change(changes):

    if (
        changes is None
        or
        len(changes) == 0
    ):

        return "⬜ N/A"

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
# LONG / SHORT
#
# 당일 양수 = LONG
# 당일 음수 = SHORT
# =========================================================

def get_day_side(changes):

    if (
        changes is None
        or
        len(changes) == 0
    ):

        return "none"

    value = changes[0]

    if value > 0:

        return "LONG"

    if value < 0:

        return "SHORT"

    return "none"


# =========================================================
# EMA HTML
#
# 모바일에서 1H / 4H 두 줄
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
# 1H 현재 진행 캔들 제외
# 4H는 표시용만 유지
# 돌파 조건은 1H만 사용
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

    if raw1h is None:

        return {
            "1h_ema": empty_ema(),
            "4h_ema": empty_ema(),
            "warning": {
                "signal": "none",
                "level": None
            }
        }

    df1h = raw1h.copy()

    df4h = (
        raw4h.copy()
        if raw4h is not None
        else None
    )

    # 현재 진행 캔들 제외
    if len(df1h) > 1:

        df1h = (
            df1h
            .iloc[:-1]
            .reset_index(drop=True)
        )

    if (
        df4h is not None
        and
        len(df4h) > 1
    ):

        df4h = (
            df4h
            .iloc[:-1]
            .reset_index(drop=True)
        )

    ema1h = check_ema(
        df1h
    )

    ema4h = (
        check_ema(df4h)
        if df4h is not None
        else empty_ema()
    )

    warning = get_1h_breakout_warning(
        df1h
    )

    return {
        "1h_ema": ema1h,
        "4h_ema": ema4h,
        "warning": warning
    }


# =========================================================
# OKX EMA
#
# 돌파 조건은 1H만 사용
# =========================================================

def get_okx_ema(inst_id):

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
            "warning": {
                "signal": "none",
                "level": None
            }
        }

    ema1h = check_ema(
        df1h
    )

    ema4h = (
        check_ema(df4h)
        if df4h is not None
        else empty_ema()
    )

    warning = get_1h_breakout_warning(
        df1h
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
# 경고 표시 여부
#
# 업비트:
# 당일 양수 + LONG + 🚨/🚀
#
# OKX:
# LONG / SHORT 모두 허용
# =========================================================

def is_visible_warning(
    warning,
    side,
    exchange
):

    if not warning:

        return False

    signal = warning.get(
        "signal",
        "none"
    )

    if signal not in (
        "pre",
        "1"
    ):

        return False

    # -----------------------------------------------------
    # 업비트는 LONG만
    # -----------------------------------------------------

    if exchange == "UPBIT":

        return side == "LONG"

    # -----------------------------------------------------
    # OKX는 LONG / SHORT 모두
    # -----------------------------------------------------

    return side in (
        "LONG",
        "SHORT"
    )


# =========================================================
# 경고 HTML
# =========================================================

def warning_html(
    warning,
    side
):

    if not warning:

        return ""

    signal = warning.get(
        "signal",
        "none"
    )

    if signal == "pre":

        icon = "🚨"

    elif signal == "1":

        icon = "🚀"

    else:

        return ""

    if side == "LONG":

        side_class = "side-long"

    elif side == "SHORT":

        side_class = "side-short"

    else:

        return ""

    return f"""
    <div class="signal-row {side_class}">
        <span class="signal-side">{side}</span>
        <span class="signal-icon">{icon}</span>
    </div>
    """


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

        latest_upbit_data = []

        return False

    volume_map = (
        get_upbit_ticker_volume_map(
            markets
        )
    )

    if not volume_map:

        latest_upbit_data = []

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

            side = get_day_side(
                changes
            )

            ema = get_upbit_ema(
                market
            )

            warning = ema.get(
                "warning",
                {}
            )

            # -------------------------------------------------
            # 업비트는 당일 양수 LONG만
            # -------------------------------------------------

            if not is_visible_warning(
                warning,
                side,
                "UPBIT"
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

                    "side":
                        side,

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

def update_okx(usdt_krw):

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

    for symbol in symbols:

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

            side = get_day_side(
                changes
            )

            ema = get_okx_ema(
                symbol
            )

            warning = ema.get(
                "warning",
                {}
            )

            if not is_visible_warning(
                warning,
                side,
                "OKX"
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

                    "side":
                        side,

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

    if USE_UPBIT == "Y":

        try:

            update_upbit()

        except Exception as e:

            logging.exception(
                f"업비트 업데이트 오류 : {e}"
            )

    else:

        latest_upbit_data = []

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
    margin: 3px 2px 6px;
    font-size: 14px;
}

h2 {
    margin: 10px 2px 5px;
    font-size: 11px;
}

.info {
    margin: 0 2px 6px;
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
    width: 17%;
}

th:nth-child(3),
td:nth-child(3) {
    width: 18%;
}

th:nth-child(4),
td:nth-child(4) {
    width: 28%;
}

th:nth-child(5),
td:nth-child(5) {
    width: 30%;
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
   LONG / SHORT
   ===================================================== */

.signal-row {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 5px;
    width: 100%;
    min-height: 17px;
    font-weight: 800;
}

.signal-side {
    font-size: 8px;
    letter-spacing: 0.2px;
}

.side-long .signal-side {
    color: #35e66d;
}

.side-short .signal-side {
    color: #ff4d4d;
}


/* =====================================================
   경고 그림
   ===================================================== */

.signal-icon {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    font-size: 10px;
    line-height: 1;
}

.side-long .signal-icon {
    filter: drop-shadow(
        0 0 4px rgba(50,255,100,0.75)
    );
}

.side-short .signal-icon {
    filter: drop-shadow(
        0 0 4px rgba(255,60,60,0.75)
    );
}


/* =====================================================
   EMA
   ===================================================== */

.ema-value {
    width: 100%;
    font-size: 8px;
    font-weight: bold;
    line-height: 1.55;
    white-space: nowrap;
}

.ema-line {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 4px;
}

.ema-label {
    color: #8d939d;
    font-size: 7px;
    font-weight: 700;
}


/* =====================================================
   모바일
   ===================================================== */

@media (max-width: 480px) {

    body {
        padding: 4px;
    }

    h1 {
        font-size: 13px;
    }

    h2 {
        font-size: 11px;
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

    .signal-side {
        font-size: 8px;
    }

    .signal-icon {
        font-size: 10px;
    }

    .ema-value {
        font-size: 8px;
        line-height: 1.6;
    }

    .ema-label {
        font-size: 7px;
    }

}
"""


# =========================================================
# 테이블 행
# =========================================================

def make_table_rows(data):

    rows_html = ""

    for item in data:

        warning_text = warning_html(
            item.get(
                "warning",
                {}
            ),
            item.get(
                "side",
                "none"
            )
        )

        rows_html += f"""

<tr>

<td>
{item.get("rank", "-")}
</td>

<td>

<span class="coin">
{item.get("name", "-")}
</span>

</td>

<td>

<span class="volume-value">
{item.get("volume", "-")}
</span>

</td>

<td>

<div class="today-wrap">

<div>
{item.get("change", "⬜ N/A")}
</div>

<div>
{warning_text}
</div>

</div>

</td>

<td>

{ema_html(
    item.get(
        "ema_1h",
        empty_ema()
    ).get(
        "display",
        "⚪"
    ),
    item.get(
        "ema_4h",
        empty_ema()
    ).get(
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
        margin:5px 2px 8px;
     ">

※ TOP{TOP_N} 거래대금 실제 순위 기준<br>

※ 당일 양수 = LONG / 당일 음수 = SHORT<br>

※ EMA 기준 = 30-60-120<br>

※ 1H 정배열 유지 상태에서 전고점 추적<br>

※ 🚨 = 전고점 직전 캔들<br>

※ 🚀 = 전고점을 처음 돌파한 양봉 캔들<br>

※ 🚀 이후 추가 상승 캔들은 표시하지 않음<br>

※ 조정 후 만들어진 낮은 고점도 새로운 전고점으로 추적<br>

※ 정배열이 깨지면 돌파 추적 종료<br>

※ 업비트 = LONG만 표시<br>

※ OKX = LONG / SHORT 표시<br>

※ EMA는 1H / 4H 두 줄 표시<br>

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
1H EMA 30-60-120 정배열 · 전고점 돌파 추적
</div>

<div>
🚨 전고점 직전 · 🚀 최초 돌파 양봉
</div>

<div>
TOP{TOP_N} · 당일 양수 LONG · 당일 음수 SHORT
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
        "돌파 기준 = 1H EMA 30-60-120"
    )

    logging.info(
        "돌파 방식 = 스윙 고점 → 조정 → 재반등 → 전고점 돌파"
    )

    if USE_UPBIT not in ("Y", "N"):

        raise ValueError(
            "USE_UPBIT은 Y 또는 N만 사용할 수 있습니다."
        )

    if USE_OKX not in ("Y", "N"):

        raise ValueError(
            "USE_OKX는 Y 또는 N만 사용할 수 있습니다."
        )

    # 최초 조회

    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    # 1분마다

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
