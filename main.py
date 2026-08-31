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
# FutureWarning
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
# 돌파 패턴 설정
# ---------------------------------------------------------

# 패턴을 찾을 최대 캔들 범위
PATTERN_LOOKBACK = 60

# 상승 1파 최소 캔들 수
MIN_RISE_CANDLES = 2

# 조정 최소 캔들 수
MIN_PULLBACK_CANDLES = 1

# EMA 최소 필요 캔들
EMA_MIN_CANDLES = 60


# =========================================================
# 거래소 조회 Y / N
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
# OKX 실패 종목
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

                    time.sleep(wait_time)

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

                    time.sleep(wait_time)

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

                time.sleep(wait_time)

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
            f"USDT-KRW 오류 : {e}"
        )

        return None


# =========================================================
# OKX 캔들
#
# include_current=True
# → 현재 진행 중인 캔들도 포함
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

                market_string = ",".join(chunk)

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
        len(df) < EMA_MIN_CANDLES
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

        if values[0] > values[1] > values[2]:

            direction = "long"

        elif values[0] < values[1] < values[2]:

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
# =========================================================

def get_main_direction(
    df,
    column
):

    if (
        df is not None
        and
        len(df) >= EMA_MIN_CANDLES
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

def check_ema(df):

    if (
        df is not None
        and
        len(df) >= EMA_MIN_CANDLES
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
# =========================================================
# 핵심 신규 로직
#
# EMA 10-30-60 정배열
# → 상승 1파
# → 조정
# → 첫 상승 고점 재돌파
# → 양봉
#
# SHORT는 반대
# =========================================================
# =========================================================

def detect_long_first_high_breakout(df):

    if (
        df is None
        or
        len(df) < EMA_MIN_CANDLES + 5
    ):

        return False

    df = (
        df.copy()
        .reset_index(drop=True)
    )

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

    ema60 = get_ema(
        df,
        "c",
        60
    )

    if (
        ema10 is None
        or
        ema30 is None
        or
        ema60 is None
    ):

        return False

    start = max(
        EMA_MIN_CANDLES,
        len(df) - PATTERN_LOOKBACK
    )

    current_index = len(df) - 1

    # -----------------------------------------------------
    # 가장 최근의 완성 가능한 구조를 찾는다.
    # -----------------------------------------------------

    for alignment_start in range(
        start,
        current_index - 4
    ):

        if (
            pd.isna(ema10.iloc[alignment_start])
            or
            pd.isna(ema30.iloc[alignment_start])
            or
            pd.isna(ema60.iloc[alignment_start])
        ):

            continue

        # 정배열 시작점
        if not (
            ema10.iloc[alignment_start]
            >
            ema30.iloc[alignment_start]
            >
            ema60.iloc[alignment_start]
        ):

            continue

        rise_start = alignment_start

        peak_index = None

        peak_high = None

        rise_count = 0

        # -------------------------------------------------
        # 상승 1파 탐색
        # -------------------------------------------------

        for i in range(
            rise_start + 1,
            current_index
        ):

            if not (
                ema10.iloc[i]
                >
                ema30.iloc[i]
                >
                ema60.iloc[i]
            ):

                break

            previous_close = float(
                df.iloc[i - 1]["c"]
            )

            current_close = float(
                df.iloc[i]["c"]
            )

            current_high = float(
                df.iloc[i]["h"]
            )

            if current_close >= previous_close:

                rise_count += 1

                if (
                    peak_high is None
                    or
                    current_high > peak_high
                ):

                    peak_high = current_high

                    peak_index = i

            else:

                # -----------------------------------------
                # 상승이 끝나면 고점 확정
                # -----------------------------------------

                if (
                    rise_count >= MIN_RISE_CANDLES
                    and
                    peak_index is not None
                ):

                    break

        if (
            peak_index is None
            or
            rise_count < MIN_RISE_CANDLES
        ):

            continue

        # -------------------------------------------------
        # 고점 이후 조정 확인
        # -------------------------------------------------

        pullback_start = peak_index + 1

        if pullback_start >= current_index:
            continue

        pullback_count = 0

        pullback_confirmed = False

        for j in range(
            pullback_start,
            current_index
        ):

            if j == current_index:
                break

            # 정배열이 완전히 깨지면 이 패턴 폐기
            if not (
                ema10.iloc[j]
                >
                ema30.iloc[j]
                >
                ema60.iloc[j]
            ):

                break

            if float(df.iloc[j]["h"]) < peak_high:

                pullback_count += 1

            if (
                float(df.iloc[j]["c"])
                <
                float(df.iloc[j - 1]["c"])
            ):

                pullback_confirmed = True

        if (
            pullback_count
            <
            MIN_PULLBACK_CANDLES
        ):

            continue

        # 조정 확인을 조금 더 엄격하게
        if not pullback_confirmed:

            continue

        # -------------------------------------------------
        # 현재 캔들 재돌파
        # -------------------------------------------------

        current = df.iloc[current_index]

        if not (
            ema10.iloc[current_index]
            >
            ema30.iloc[current_index]
            >
            ema60.iloc[current_index]
        ):

            continue

        current_open = float(
            current["o"]
        )

        current_high = float(
            current["h"]
        )

        current_close = float(
            current["c"]
        )

        # 양봉
        bullish = (
            current_close > current_open
        )

        # 첫 상승 고점 재돌파
        breakout = (
            current_high > peak_high
            and
            current_close > peak_high
        )

        if bullish and breakout:

            return True

    return False


# =========================================================
# SHORT 첫 하락 저점 재이탈
# =========================================================

def detect_short_first_low_breakout(df):

    if (
        df is None
        or
        len(df) < EMA_MIN_CANDLES + 5
    ):

        return False

    df = (
        df.copy()
        .reset_index(drop=True)
    )

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

    ema60 = get_ema(
        df,
        "c",
        60
    )

    if (
        ema10 is None
        or
        ema30 is None
        or
        ema60 is None
    ):

        return False

    start = max(
        EMA_MIN_CANDLES,
        len(df) - PATTERN_LOOKBACK
    )

    current_index = len(df) - 1

    for alignment_start in range(
        start,
        current_index - 4
    ):

        if (
            pd.isna(ema10.iloc[alignment_start])
            or
            pd.isna(ema30.iloc[alignment_start])
            or
            pd.isna(ema60.iloc[alignment_start])
        ):

            continue

        # 역배열 시작
        if not (
            ema10.iloc[alignment_start]
            <
            ema30.iloc[alignment_start]
            <
            ema60.iloc[alignment_start]
        ):

            continue

        fall_start = alignment_start

        low_index = None

        low_price = None

        fall_count = 0

        # -------------------------------------------------
        # 하락 1파
        # -------------------------------------------------

        for i in range(
            fall_start + 1,
            current_index
        ):

            if not (
                ema10.iloc[i]
                <
                ema30.iloc[i]
                <
                ema60.iloc[i]
            ):

                break

            previous_close = float(
                df.iloc[i - 1]["c"]
            )

            current_close = float(
                df.iloc[i]["c"]
            )

            current_low = float(
                df.iloc[i]["l"]
            )

            if current_close <= previous_close:

                fall_count += 1

                if (
                    low_price is None
                    or
                    current_low < low_price
                ):

                    low_price = current_low

                    low_index = i

            else:

                if (
                    fall_count >= MIN_RISE_CANDLES
                    and
                    low_index is not None
                ):

                    break

        if (
            low_index is None
            or
            fall_count < MIN_RISE_CANDLES
        ):

            continue

        # -------------------------------------------------
        # 반등 / 조정
        # -------------------------------------------------

        pullback_start = low_index + 1

        if pullback_start >= current_index:
            continue

        pullback_count = 0

        pullback_confirmed = False

        for j in range(
            pullback_start,
            current_index
        ):

            if not (
                ema10.iloc[j]
                <
                ema30.iloc[j]
                <
                ema60.iloc[j]
            ):

                break

            if float(df.iloc[j]["l"]) > low_price:

                pullback_count += 1

            if (
                float(df.iloc[j]["c"])
                >
                float(df.iloc[j - 1]["c"])
            ):

                pullback_confirmed = True

        if (
            pullback_count
            <
            MIN_PULLBACK_CANDLES
        ):

            continue

        if not pullback_confirmed:

            continue

        # -------------------------------------------------
        # 현재 캔들 첫 저점 재이탈
        # -------------------------------------------------

        current = df.iloc[current_index]

        if not (
            ema10.iloc[current_index]
            <
            ema30.iloc[current_index]
            <
            ema60.iloc[current_index]
        ):

            continue

        current_open = float(
            current["o"]
        )

        current_low = float(
            current["l"]
        )

        current_close = float(
            current["c"]
        )

        # 음봉
        bearish = (
            current_close < current_open
        )

        # 첫 하락 저점 재이탈
        breakout = (
            current_low < low_price
            and
            current_close < low_price
        )

        if bearish and breakout:

            return True

    return False


# =========================================================
# 시간봉 돌파
#
# 반환:
# long
# short
# none
# =========================================================

def get_timeframe_breakout(
    df,
    allowed_direction=None
):

    if (
        df is None
        or
        len(df) < EMA_MIN_CANDLES + 5
    ):

        return "none"

    # -----------------------------------------------------
    # LONG
    # -----------------------------------------------------

    long_signal = detect_long_first_high_breakout(
        df
    )

    # -----------------------------------------------------
    # SHORT
    # -----------------------------------------------------

    short_signal = detect_short_first_low_breakout(
        df
    )

    if allowed_direction == "long":

        if long_signal:
            return "long"

        return "none"

    if allowed_direction == "short":

        if short_signal:
            return "short"

        return "none"

    if long_signal:
        return "long"

    if short_signal:
        return "short"

    return "none"


# =========================================================
# 당일 해 / 구름
#
# 양수 = ☀️ LONG
# 음수 = ☁️ SHORT
# =========================================================

def get_market_weather(
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
# 해 / 구름 HTML
# =========================================================

def weather_html(
    change_percent
):

    direction = get_market_weather(
        change_percent
    )

    if direction == "long":

        return (
            '<span class="weather-long">'
            '☀️ LONG'
            '</span>'
        )

    if direction == "short":

        return (
            '<span class="weather-short">'
            '☁️ SHORT'
            '</span>'
        )

    return (
        '<span class="weather-none">'
        '➖'
        '</span>'
    )


# =========================================================
# LONG / SHORT HTML
#
# 조건이 일치한 랭크 전체 반짝임
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

    return ""


# =========================================================
# 돌파 방향 + 당일 방향 일치 확인
# =========================================================

def get_matching_breakout(
    warning,
    weather,
    exchange
):

    if not warning:
        return None

    # -----------------------------------------------------
    # 업비트는 LONG만
    # -----------------------------------------------------

    if exchange == "upbit":

        if (
            weather == "long"
            and
            (
                warning.get("1h") == "long"
                or
                warning.get("4h") == "long"
            )
        ):

            return "long"

        return None

    # -----------------------------------------------------
    # OKX LONG
    # -----------------------------------------------------

    if (
        weather == "long"
        and
        (
            warning.get("1h") == "long"
            or
            warning.get("4h") == "long"
        )
    ):

        return "long"

    # -----------------------------------------------------
    # OKX SHORT
    # -----------------------------------------------------

    if (
        weather == "short"
        and
        (
            warning.get("1h") == "short"
            or
            warning.get("4h") == "short"
        )
    ):

        return "short"

    return None


# =========================================================
# 경고 HTML
#
# 현재는 🚨만 표시
# =========================================================

def combined_warning_html(
    warning,
    matching_direction
):

    if (
        not warning
        or
        matching_direction is None
    ):

        return ""

    result = []

    if (
        matching_direction == "long"
        and
        warning.get("1h") == "long"
    ):

        result.append(
            '<span class="warning-long">'
            '🚨1H'
            '</span>'
        )

    if (
        matching_direction == "long"
        and
        warning.get("4h") == "long"
    ):

        result.append(
            '<span class="warning-long">'
            '🚨4H'
            '</span>'
        )

    if (
        matching_direction == "short"
        and
        warning.get("1h") == "short"
    ):

        result.append(
            '<span class="warning-short">'
            '🚨1H'
            '</span>'
        )

    if (
        matching_direction == "short"
        and
        warning.get("4h") == "short"
    ):

        result.append(
            '<span class="warning-short">'
            '🚨4H'
            '</span>'
        )

    if not result:
        return ""

    return " ".join(result)


# =========================================================
# 표시 여부
# =========================================================

def is_visible_matching_warning(
    warning,
    weather,
    exchange
):

    return (
        get_matching_breakout(
            warning,
            weather,
            exchange
        )
        is not None
    )


# =========================================================
# 업비트 변동률
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
# 모바일에서 두 줄
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
            <span>
                {ema_1h}
            </span>
        </div>

        <div class="ema-row">
            <span class="ema-time">
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
# 업비트 EMA
#
# 현재 진행 캔들 포함
# =========================================================

def get_upbit_ema(market):

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

    # -----------------------------------------------------
    # 현재 진행 캔들 포함하여 돌파 검사
    # -----------------------------------------------------

    warning = {
        "1h":
            get_timeframe_breakout(
                df1h
            ),

        "4h":
            get_timeframe_breakout(
                df4h
            )
    }

    return {
        "1h_ema": ema1h,
        "4h_ema": ema4h,
        "warning": warning
    }


# =========================================================
# OKX EMA
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

    warning = {
        "1h":
            get_timeframe_breakout(
                df1h
            ),

        "4h":
            get_timeframe_breakout(
                df4h
            )
    }

    return {
        "1h_ema": ema1h,
        "4h_ema": ema4h,
        "warning": warning
    }


# =========================================================
# OKX 거래대금
#
# 거래대금은 확정 1H 캔들 기준
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

            weather = get_market_weather(
                change_percent
            )

            # 업비트는 LONG만
            if weather != "long":

                continue

            ema = get_upbit_ema(
                market
            )

            warning = ema.get(
                "warning",
                {}
            )

            matching_direction = (
                get_matching_breakout(
                    warning,
                    weather,
                    "upbit"
                )
            )

            if matching_direction != "long":

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

                    "weather":
                        weather_html(
                            change_percent
                        ),

                    "direction":
                        direction_html(
                            matching_direction
                        ),

                    "volume":
                        format_volume(
                            volume_map[market]
                        ),

                    "ema_1h":
                        ema["1h_ema"],

                    "ema_4h":
                        ema["4h_ema"],

                    "warning":
                        warning,

                    "matching_direction":
                        matching_direction
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

            change_percent = (
                changes[0]
                if (
                    changes is not None
                    and
                    len(changes) > 0
                )
                else None
            )

            weather = get_market_weather(
                change_percent
            )

            # -------------------------------------------------
            # OKX EMA + 돌파
            # -------------------------------------------------

            ema = get_okx_ema(
                symbol
            )

            warning = ema.get(
                "warning",
                {}
            )

            matching_direction = (
                get_matching_breakout(
                    warning,
                    weather,
                    "okx"
                )
            )

            # -------------------------------------------------
            # 방향과 돌파가 일치할 때만 표시
            # -------------------------------------------------

            if matching_direction is None:

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

                    "weather":
                        weather_html(
                            change_percent
                        ),

                    "direction":
                        direction_html(
                            matching_direction
                        ),

                    "volume":
                        format_volume(
                            volume_map[symbol]
                        ),

                    "ema_1h":
                        ema["1h_ema"],

                    "ema_4h":
                        ema["4h_ema"],

                    "warning":
                        warning,

                    "matching_direction":
                        matching_direction
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

        global latest_upbit_data

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

    font-family:
        Arial,
        "Noto Sans KR",
        sans-serif;

    font-size: 9px;

    padding: 5px;
}

h1 {

    margin: 3px 2px 6px 2px;

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

    width: 6%;
}

th:nth-child(2),
td:nth-child(2) {

    width: 19%;
}

th:nth-child(3),
td:nth-child(3) {

    width: 16%;
}

th:nth-child(4),
td:nth-child(4) {

    width: 20%;
}

th:nth-child(5),
td:nth-child(5) {

    width: 22%;
}

th:nth-child(6),
td:nth-child(6) {

    width: 17%;
}


/* =====================================================
   코인
   ===================================================== */

.coin {

    display: block;

    font-size: 8px;

    font-weight: 700;

    line-height: 1.2;

    white-space: nowrap;
}


/* =====================================================
   해 / 구름
   ===================================================== */

.weather {

    display: block;

    margin-top: 3px;

    font-size: 7px;

    font-weight: 700;

    line-height: 1;
}

.weather-long {

    color: #54e879;
}

.weather-short {

    color: #ff5b5b;
}

.weather-none {

    color: #777;
}


/* =====================================================
   거래대금
   ===================================================== */

.volume-value {

    display: block;

    font-size: 7px;

    font-weight: 600;

    line-height: 1.1;
}


/* =====================================================
   LONG / SHORT
   ===================================================== */

.direction {

    display: block;

    margin-top: 3px;

    font-size: 7px;

    font-weight: 800;

    line-height: 1;
}

.direction-long {

    color: #3fe76b;

    animation:
        alarmPulse 0.9s infinite;
}

.direction-short {

    color: #ff4d4d;

    animation:
        alarmPulse 0.9s infinite;
}


/* =====================================================
   반짝임
   조건이 된 랭크 전체
   ===================================================== */

@keyframes alarmPulse {

    0% {

        opacity: 1;

        transform:
            scale(1);
    }

    50% {

        opacity: 0.35;

        transform:
            scale(1.04);
    }

    100% {

        opacity: 1;

        transform:
            scale(1);
    }
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

    font-weight: 700;

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

.warning-long {

    color: #45ed73;

    font-size: 9px;

    font-weight: 800;

    animation:
        alarmPulse 0.9s infinite;
}

.warning-short {

    color: #ff4d4d;

    font-size: 9px;

    font-weight: 800;

    animation:
        alarmPulse 0.9s infinite;
}


/* =====================================================
   EMA
   ===================================================== */

.ema-value {

    display: flex;

    flex-direction: column;

    align-items: center;

    justify-content: center;

    gap: 3px;

    width: 100%;

    font-size: 8px;

    font-weight: 700;

    line-height: 1.1;

    white-space: nowrap;
}

.ema-row {

    display: flex;

    align-items: center;

    justify-content: center;

    gap: 4px;

    width: 100%;
}

.ema-time {

    color: #8c929b;

    font-size: 7px;

    font-weight: 800;
}


/* =====================================================
   설명
   ===================================================== */

.note {

    color: #666;

    font-size: 6px;

    line-height: 1.5;

    margin:
        4px 2px 7px 2px;
}


/* =====================================================
   모바일
   ===================================================== */

@media (max-width: 480px) {

    body {

        padding: 4px;

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

    .weather {

        font-size: 7px;
    }

    .volume-value {

        font-size: 6px;
    }

    .change-item {

        font-size: 7px;
    }

    .direction {

        font-size: 7px;
    }

    .warning-long,
    .warning-short {

        font-size: 9px;
    }

    .ema-value {

        font-size: 8px;

        gap: 3px;
    }

    .ema-time {

        font-size: 7px;
    }

}


/* =====================================================
   아주 작은 화면
   ===================================================== */

@media (max-width: 360px) {

    .coin {

        font-size: 7px;
    }

    .change-item {

        font-size: 6px;
    }

    .volume-value {

        font-size: 6px;
    }

    .weather {

        font-size: 6px;
    }

    .direction {

        font-size: 6px;
    }

    .warning-long,
    .warning-short {

        font-size: 8px;
    }

    .ema-value {

        font-size: 7px;
    }

    .ema-time {

        font-size: 6px;
    }
}

"""


# =========================================================
# 테이블 행
# =========================================================

def make_table_rows(data):

    rows_html = ""

    for item in data:

        warning = item.get(
            "warning",
            {}
        )

        matching_direction = item.get(
            "matching_direction"
        )

        warning_text = (
            combined_warning_html(
                warning,
                matching_direction
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

<div class="weather">
{item.get("weather", "")}
</div>

</td>


<td>

<span class="volume-value">
{item["volume"]}
</span>

<div class="direction">
{item.get("direction", "")}
</div>

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


<td>

<div class="breakout-warning">

{warning_text}

</div>

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

<td colspan="6"
    style="
        color:#555;
        padding:12px 4px;
    ">

현재 조건 일치 종목 없음

</td>

</tr>

"""

    return f"""

<div class="section">

<h2>
{title} TOP{TOP_N}
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

<th>돌파</th>

</tr>

</thead>

<tbody>

{rows}

</tbody>

</table>

</div>

<div class="note">

※ TOP{TOP_N} 거래대금 실제 순위<br>

※ 코인명 아래 ☀️ LONG / ☁️ SHORT<br>

※ 거래대금 아래 LONG / SHORT 표시<br>

※ EMA 1H / 4H 두 줄 표시<br>

※ EMA 기준 10-30-60 정배열 / 역배열<br>

※ LONG = EMA 10 > 30 > 60<br>

※ SHORT = EMA 10 < 30 < 60<br>

※ 돌파 = 정배열 후 첫 상승 1파 → 조정 → 첫 고점 재돌파<br>

※ LONG은 재돌파 양봉일 때 🚨<br>

※ SHORT는 재이탈 음봉일 때 🚨<br>

※ 당일 방향과 돌파 방향이 일치할 때만 표시<br>

※ 업비트는 LONG만 표시<br>

※ OKX는 LONG / SHORT 표시<br>

※ 1H / 4H 중 하나라도 조건 일치하면 표시<br>

※ 현재 진행 캔들을 포함하여 1분마다 확인

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
1H + 4H EMA 10-30-60
</div>

<div>
정배열 → 상승 1파 → 조정 → 첫 고점 재돌파
</div>

<div>
1분마다 현재 상태 확인
</div>

<div>
TOP{TOP_N} · 방향 일치 🚨만 표시
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
        f"패턴 설정 "
        f"LOOKBACK={PATTERN_LOOKBACK} "
        f"MIN_RISE={MIN_RISE_CANDLES} "
        f"MIN_PULLBACK={MIN_PULLBACK_CANDLES}"
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
