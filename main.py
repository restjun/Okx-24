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

# 최근 최소 10개 이상
BREAKOUT_LOOKBACK = 20

# 돌파 전 조정으로 인정할 최소 캔들 수
MIN_PULLBACK_CANDLES = 2

# 돌파 캔들 몸통 최소 비율
MIN_BODY_RATIO = 0.30


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
# OKX
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

        return price

    except Exception as e:

        logging.error(
            f"USDT-KRW 처리 오류 : {e}"
        )

        return None


# =========================================================
# OKX 캔들
#
# 현재 진행 캔들도 포함
# confirm 여부와 관계없이 최신 캔들 사용
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

    for chunk in chunks:

        while True:

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

                break

            except Exception as e:

                logging.error(
                    f"업비트 Ticker 실패 : {e}"
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
# EMA 10-30-60
#
# 기존 표시용
# =========================================================

def get_ema_10_30_60_alignment_count(
    df,
    column
):

    if df is None or len(df) < 60:

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

    direction = None
    count = 0

    for index in range(
        len(df) - 1,
        -1,
        -1
    ):

        a = ema10.iloc[index]
        b = ema30.iloc[index]
        c = ema60.iloc[index]

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

    if df is None or df.empty:

        return {
            "display": "⚪",
            "direction": "none",
            "count": 0
        }

    if len(df) >= 60:

        direction, count = (
            get_ema_10_30_60_alignment_count(
                df,
                "c"
            )
        )

    else:

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
            ema10 is None
            or
            ema30 is None
        ):

            return {
                "display": "⚪",
                "direction": "none",
                "count": 0
            }

        a = ema10.iloc[-1]
        b = ema30.iloc[-1]

        if a > b:

            direction = "long"

        elif a < b:

            direction = "short"

        else:

            direction = "none"

        count = 1

        for i in range(
            len(df) - 2,
            -1,
            -1
        ):

            x = ema10.iloc[i]
            y = ema30.iloc[i]

            if pd.isna(x) or pd.isna(y):

                break

            current = (
                "long"
                if x > y
                else
                "short"
                if x < y
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
# EMA 30-60-120
#
# 돌파 조건용
# =========================================================

def get_ema_30_60_120_direction(
    df
):

    if df is None or len(df) < 120:

        return "none"

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
# 캔들 방향
# =========================================================

def candle_is_bullish(row):

    return float(row["c"]) > float(row["o"])


def candle_is_bearish(row):

    return float(row["c"]) < float(row["o"])


# =========================================================
# 몸통 비율
# =========================================================

def body_ratio(row):

    high = float(row["h"])
    low = float(row["l"])
    opening = float(row["o"])
    close = float(row["c"])

    total = high - low

    if total <= 0:

        return 0

    return abs(
        close - opening
    ) / total


# =========================================================
# 당일 방향
#
# 한국시간 09:00 기준
#
# 양수 = ☀️ LONG
# 음수 = ☁️ SHORT
# =========================================================

def get_today_direction_from_upbit(
    df
):

    if df is None or df.empty:

        return "none", None

    try:

        now = datetime.now(
            ZoneInfo("Asia/Seoul")
        )

        today_9 = now.replace(
            hour=9,
            minute=0,
            second=0,
            microsecond=0
        )

        if now < today_9:

            today_9 = (
                today_9
                -
                pd.Timedelta(days=1)
            )

        candle_times = pd.to_datetime(
            df["datetime"]
        )

        candidates = df[
            candle_times >= today_9
        ]

        if candidates.empty:

            return "none", None

        day_open = float(
            candidates.iloc[0]["o"]
        )

        current_price = float(
            df.iloc[-1]["c"]
        )

        if day_open <= 0:

            return "none", None

        change = (
            (
                current_price
                -
                day_open
            )
            /
            day_open
            *
            100
        )

        if change > 0:

            return "long", change

        if change < 0:

            return "short", change

        return "none", change

    except Exception:

        return "none", None


# =========================================================
# OKX 당일 방향
#
# 한국시간 09:00 기준
# =========================================================

def get_today_direction_from_okx(
    df
):

    if df is None or df.empty:

        return "none", None

    try:

        temp = df.copy()

        temp["datetime"] = (
            pd.to_datetime(
                pd.to_numeric(
                    temp["ts"],
                    errors="coerce"
                ),
                unit="ms",
                utc=True
            )
            .dt.tz_convert(
                "Asia/Seoul"
            )
            .dt.tz_localize(None)
        )

        now = datetime.now(
            ZoneInfo("Asia/Seoul")
        )

        today_9 = now.replace(
            hour=9,
            minute=0,
            second=0,
            microsecond=0
        )

        if now < today_9:

            today_9 = (
                today_9
                -
                pd.Timedelta(days=1)
            )

        candidates = temp[
            temp["datetime"] >= today_9
        ]

        if candidates.empty:

            return "none", None

        day_open = float(
            candidates.iloc[0]["o"]
        )

        current_price = float(
            temp.iloc[-1]["c"]
        )

        if day_open <= 0:

            return "none", None

        change = (
            (
                current_price
                -
                day_open
            )
            /
            day_open
            *
            100
        )

        if change > 0:

            return "long", change

        if change < 0:

            return "short", change

        return "none", change

    except Exception:

        return "none", None


# =========================================================
# 상승 1파 → 조정 → 재상승 → 고점 돌파
#
# LONG
#
# 1. 30-60-120 정배열
# 2. 최근 LOOKBACK 구간 안에서 이전 고점 존재
# 3. 고점 이후 최소 2개 조정
# 4. 현재 캔들이 양봉
# 5. 현재 종가가 이전 고점 돌파
#
# SHORT
#
# 반대 구조
# =========================================================

def detect_impulse_pullback_breakout(
    df,
    direction
):

    if (
        df is None
        or
        len(df) < 130
    ):

        return False

    ema_direction = (
        get_ema_30_60_120_direction(
            df
        )
    )

    if ema_direction != direction:

        return False

    current_index = len(df) - 1

    current = df.iloc[
        current_index
    ]

    start = max(
        120,
        current_index - BREAKOUT_LOOKBACK
    )

    previous_start = start

    previous_end = current_index

    previous = df.iloc[
        previous_start:
        previous_end
    ]

    if len(previous) < 10:

        return False

    # =====================================================
    # LONG
    # =====================================================

    if direction == "long":

        previous_high = float(
            previous["h"].max()
        )

        # 현재 캔들이 반드시 양봉
        if not candle_is_bullish(
            current
        ):

            return False

        # 몸통이 너무 작은 캔들은 제외
        if body_ratio(current) < MIN_BODY_RATIO:

            return False

        # 이전 고점 돌파
        if float(current["c"]) <= previous_high:

            return False

        # 고점 위치
        high_index = (
            previous["h"]
            .astype(float)
            .idxmax()
        )

        # 고점 이후 조정 캔들 수
        pullback_count = (
            current_index
            -
            high_index
            -
            1
        )

        if pullback_count < MIN_PULLBACK_CANDLES:

            return False

        # 고점 이후 실제 조정이 있었는지
        after_high = df.loc[
            high_index + 1:
            current_index - 1
        ]

        if after_high.empty:

            return False

        pullback_low = float(
            after_high["l"].min()
        )

        # 조정 저점이 이전 고점보다 낮아야 함
        if pullback_low >= previous_high:

            return False

        return True

    # =====================================================
    # SHORT
    # =====================================================

    if direction == "short":

        previous_low = float(
            previous["l"].min()
        )

        if not candle_is_bearish(
            current
        ):

            return False

        if body_ratio(current) < MIN_BODY_RATIO:

            return False

        if float(current["c"]) >= previous_low:

            return False

        low_index = (
            previous["l"]
            .astype(float)
            .idxmin()
        )

        rebound_count = (
            current_index
            -
            low_index
            -
            1
        )

        if rebound_count < MIN_PULLBACK_CANDLES:

            return False

        after_low = df.loc[
            low_index + 1:
            current_index - 1
        ]

        if after_low.empty:

            return False

        rebound_high = float(
            after_low["h"].max()
        )

        if rebound_high <= previous_low:

            return False

        return True

    return False


# =========================================================
# 기존 단순 돌파전
#
# 현재 진행 캔들의 상태를 사용
# =========================================================

def detect_pre_breakout(
    df,
    direction
):

    if (
        df is None
        or
        len(df) < 130
    ):

        return False

    if (
        get_ema_30_60_120_direction(df)
        != direction
    ):

        return False

    current = df.iloc[-1]

    previous = df.iloc[
        -BREAKOUT_LOOKBACK - 1:
        -1
    ]

    if previous.empty:

        return False

    if direction == "long":

        previous_high = float(
            previous["h"].max()
        )

        return (
            float(current["h"])
            >= previous_high
            and
            float(current["c"])
            <= previous_high
            and
            candle_is_bullish(current)
        )

    if direction == "short":

        previous_low = float(
            previous["l"].min()
        )

        return (
            float(current["l"])
            <= previous_low
            and
            float(current["c"])
            >= previous_low
            and
            candle_is_bearish(current)
        )

    return False


# =========================================================
# 1H / 4H 돌파 상태
#
# breakout = 실제 재돌파
# pre      = 돌파 직전
# none     = 없음
# =========================================================

def get_breakout_state(
    df,
    direction
):

    if (
        df is None
        or
        len(df) < 130
    ):

        return "none"

    # -----------------------------------------------------
    # 실제 상승 1파 → 조정 → 재돌파
    # -----------------------------------------------------

    if detect_impulse_pullback_breakout(
        df,
        direction
    ):

        return "breakout"

    # -----------------------------------------------------
    # 돌파 직전
    # -----------------------------------------------------

    if detect_pre_breakout(
        df,
        direction
    ):

        return "pre"

    return "none"


# =========================================================
# 해 / 구름 + 돌파 방향 일치
#
# 당일 양수 + LONG 돌파
# 당일 음수 + SHORT 돌파
# =========================================================

def get_aligned_warning(
    df,
    exchange,
    allowed_directions
):

    if df is None:

        return {
            "direction": "none",
            "state": "none",
            "today_direction": "none",
            "today_change": None
        }

    if exchange == "upbit":

        today_direction, today_change = (
            get_today_direction_from_upbit(
                df
            )
        )

    else:

        today_direction, today_change = (
            get_today_direction_from_okx(
                df
            )
        )

    # 당일 방향이 없으면 표시하지 않음
    if today_direction == "none":

        return {
            "direction": "none",
            "state": "none",
            "today_direction": today_direction,
            "today_change": today_change
        }

    # 업비트는 LONG만
    if (
        today_direction
        not in allowed_directions
    ):

        return {
            "direction": "none",
            "state": "none",
            "today_direction": today_direction,
            "today_change": today_change
        }

    state = get_breakout_state(
        df,
        today_direction
    )

    return {
        "direction": today_direction,
        "state": state,
        "today_direction": today_direction,
        "today_change": today_change
    }


# =========================================================
# 1H + 4H 통합 경고
#
# 둘 중 하나라도 조건 만족하면 표시
# =========================================================

def get_combined_warning(
    df1h,
    df4h,
    exchange
):

    if exchange == "upbit":

        allowed = ["long"]

    else:

        allowed = [
            "long",
            "short"
        ]

    result_1h = get_aligned_warning(
        df1h,
        exchange,
        allowed
    )

    result_4h = get_aligned_warning(
        df4h,
        exchange,
        allowed
    )

    visible = (
        result_1h["state"]
        in ["pre", "breakout"]
        or
        result_4h["state"]
        in ["pre", "breakout"]
    )

    # 표시 방향
    direction = "none"

    if result_1h["state"] in [
        "pre",
        "breakout"
    ]:

        direction = result_1h[
            "direction"
        ]

    elif result_4h["state"] in [
        "pre",
        "breakout"
    ]:

        direction = result_4h[
            "direction"
        ]

    return {
        "1h": result_1h,
        "4h": result_4h,
        "visible": visible,
        "direction": direction
    }


# =========================================================
# 경고 HTML
#
# 🚨만 표시
# =========================================================

def combined_warning_html(
    warning
):

    if not warning:

        return ""

    result = []

    one_h = warning.get(
        "1h",
        {}
    )

    four_h = warning.get(
        "4h",
        {}
    )

    if one_h.get("state") in [
        "pre",
        "breakout"
    ]:

        result.append(
            '<span class="warning-item">'
            '🚨1H'
            '</span>'
        )

    if four_h.get("state") in [
        "pre",
        "breakout"
    ]:

        result.append(
            '<span class="warning-item">'
            '🚨4H'
            '</span>'
        )

    return " ".join(
        result
    )


# =========================================================
# 당일 해 / 구름
# =========================================================

def today_weather_html(
    today_direction,
    today_change
):

    if today_direction == "long":

        change_text = (
            f"+{today_change:.2f}%"
            if today_change is not None
            else ""
        )

        return (
            '<span class="sun">'
            f'☀️ {change_text}'
            '</span>'
        )

    if today_direction == "short":

        change_text = (
            f"{today_change:.2f}%"
            if today_change is not None
            else ""
        )

        return (
            '<span class="cloud">'
            f'☁️ {change_text}'
            '</span>'
        )

    return (
        '<span class="neutral-weather">'
        '➖'
        '</span>'
    )


# =========================================================
# LONG / SHORT
# =========================================================

def direction_html(
    direction,
    warning_visible
):

    if direction == "long":

        if warning_visible:

            return (
                '<span class="direction-long blink">'
                'LONG'
                '</span>'
            )

        return (
            '<span class="direction-long">'
            'LONG'
            '</span>'
        )

    if direction == "short":

        if warning_visible:

            return (
                '<span class="direction-short blink">'
                'SHORT'
                '</span>'
            )

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
# 모바일에서 2줄
# =========================================================

def ema_html(
    ema_1h,
    ema_4h
):

    return f"""
    <div class="ema-box">

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
# 업비트 EMA + 경고
# =========================================================

def get_upbit_ema(
    market
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

    warning = get_combined_warning(
        df1h,
        df4h,
        "upbit"
    )

    return {
        "1h_ema": ema1h,
        "4h_ema": ema4h,
        "warning": warning
    }


# =========================================================
# OKX EMA + 경고
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
                "visible": False
            }
        }

    ema1h = check_ema(
        df1h
    )

    ema4h = check_ema(
        df4h
    )

    warning = get_combined_warning(
        df1h,
        df4h,
        "okx"
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
# 업비트 변동률
# =========================================================

def get_upbit_today(
    market
):

    df = get_upbit_ohlcv(
        market,
        60,
        120
    )

    if df is None:

        return None, "none"

    direction, change = (
        get_today_direction_from_upbit(
            df
        )
    )

    return change, direction


# =========================================================
# OKX 변동률
# =========================================================

def get_okx_today(
    inst_id
):

    df = get_okx_ohlcv(
        inst_id,
        "1H",
        120
    )

    if df is None:

        return None, "none"

    direction, change = (
        get_today_direction_from_okx(
            df
        )
    )

    return change, direction


# =========================================================
# 변동률 HTML
# =========================================================

def format_change(
    change,
    direction
):

    if change is None:

        return "N/A"

    if direction == "long":

        return (
            '<span class="change-positive">'
            f'🟩 +{change:.2f}%'
            '</span>'
        )

    if direction == "short":

        return (
            '<span class="change-negative">'
            f'🟥 {change:.2f}%'
            '</span>'
        )

    return (
        '<span class="change-neutral">'
        f'⬜ {change:.2f}%'
        '</span>'
    )


# =========================================================
# 업비트 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data

    logging.info(
        f"========== 업비트 TOP{TOP_N} =========="
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

        try:

            ema = get_upbit_ema(
                market
            )

            warning = ema.get(
                "warning",
                {}
            )

            # 업비트는 LONG 경고만
            if not warning.get(
                "visible",
                False
            ):

                continue

            if warning.get(
                "direction"
            ) != "long":

                continue

            change, today_direction = (
                get_upbit_today(
                    market
                )
            )

            weather = today_weather_html(
                today_direction,
                change
            )

            warning_text = (
                combined_warning_html(
                    warning
                )
            )

            rows.append(
                {
                    "rank": rank,

                    "name":
                        market.replace(
                            "KRW-",
                            ""
                        ),

                    "change":
                        format_change(
                            change,
                            today_direction
                        ),

                    "weather":
                        weather,

                    "volume":
                        format_volume(
                            volume_map[market]
                        ),

                    "direction":
                        direction_html(
                            warning[
                                "direction"
                            ],
                            True
                        ),

                    "warning":
                        warning_text,

                    "ema_1h":
                        ema[
                            "1h_ema"
                        ],

                    "ema_4h":
                        ema[
                            "4h_ema"
                        ]
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

            warning = ema.get(
                "warning",
                {}
            )

            if not warning.get(
                "visible",
                False
            ):

                continue

            direction = warning.get(
                "direction",
                "none"
            )

            # OKX는 LONG / SHORT 모두
            if direction not in [
                "long",
                "short"
            ]:

                continue

            change, today_direction = (
                get_okx_today(
                    symbol
                )
            )

            weather = today_weather_html(
                today_direction,
                change
            )

            warning_text = (
                combined_warning_html(
                    warning
                )
            )

            rows.append(
                {
                    "rank": rank,

                    "name": coin,

                    "change":
                        format_change(
                            change,
                            today_direction
                        ),

                    "weather":
                        weather,

                    "volume":
                        format_volume(
                            volume_map[symbol]
                        ),

                    "direction":
                        direction_html(
                            direction,
                            True
                        ),

                    "warning":
                        warning_text,

                    "ema_1h":
                        ema[
                            "1h_ema"
                        ],

                    "ema_4h":
                        ema[
                            "4h_ema"
                        ]
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
        "1분 실시간 조회 시작"
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

    font-family:
        Arial,
        "Noto Sans KR",
        sans-serif;

    font-size: 10px;

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

    margin: 0 2px 7px;

    padding: 6px;

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

    width: 18%;
}

th:nth-child(4),
td:nth-child(4) {

    width: 25%;
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

    font-weight: 700;

    line-height: 1.2;

    margin-bottom: 3px;
}


/* =====================================================
   해 / 구름
   ===================================================== */

.weather {

    display: block;

    font-size: 8px;

    line-height: 1.2;

    white-space: nowrap;
}

.sun {

    color: #ffd84d;

    font-weight: 700;
}

.cloud {

    color: #aab0bb;

    font-weight: 700;
}

.neutral-weather {

    color: #777;

    font-weight: 700;
}


/* =====================================================
   거래대금
   ===================================================== */

.volume-value {

    display: block;

    font-size: 8px;

    font-weight: 600;

    margin-bottom: 3px;
}


/* =====================================================
   방향
   ===================================================== */

.direction {

    display: block;

    font-size: 8px;

    font-weight: 800;

    line-height: 1.2;
}

.direction-long {

    color: #35e66d;

    font-weight: 800;
}

.direction-short {

    color: #ff4d4d;

    font-weight: 800;
}

.direction-none {

    color: #777;
}


/* =====================================================
   반짝임
   ===================================================== */

.blink {

    animation:
        blink-alert
        0.8s
        ease-in-out
        infinite;
}

@keyframes blink-alert {

    0%,
    100% {

        opacity: 1;

        transform:
            scale(1);
    }

    50% {

        opacity: 0.35;

        transform:
            scale(1.08);
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

    gap: 3px;

    width: 100%;
}

.change-value {

    font-size: 8px;

    font-weight: 700;

    white-space: nowrap;
}

.change-positive {

    color: #eeeeee;
}

.change-negative {

    color: #eeeeee;
}

.change-neutral {

    color: #999;
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

    min-height: 13px;

    white-space: nowrap;
}

.warning-item {

    font-size: 10px;

    font-weight: 800;

    line-height: 1;

    animation:
        warning-blink
        0.75s
        ease-in-out
        infinite;
}

@keyframes warning-blink {

    0%,
    100% {

        opacity: 1;
    }

    50% {

        opacity: 0.35;
    }
}


/* =====================================================
   EMA
   ===================================================== */

.ema-box {

    display: flex;

    flex-direction: column;

    align-items: center;

    justify-content: center;

    gap: 3px;

    width: 100%;
}

.ema-line {

    display: flex;

    align-items: center;

    justify-content: center;

    gap: 3px;

    width: 100%;

    font-size: 8px;

    font-weight: 700;

    line-height: 1.1;

    white-space: nowrap;
}

.ema-label {

    color: #8b9099;

    font-size: 7px;

    font-weight: 700;
}


/* =====================================================
   설명
   ===================================================== */

.note {

    color: #666;

    font-size: 7px;

    line-height: 1.5;

    margin:
        5px 2px 8px;
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

        font-size: 8px;
    }

    .weather {

        font-size: 7px;
    }

    .volume-value {

        font-size: 7px;
    }

    .direction {

        font-size: 7px;
    }

    .change-value {

        font-size: 7px;
    }

    .warning-item {

        font-size: 9px;
    }

    .ema-line {

        font-size: 8px;
    }

    .ema-label {

        font-size: 7px;
    }

    .note {

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

        rows_html += f"""

<tr>

<td>
{item.get("rank", "-")}
</td>


<td>

<span class="coin">
{item["name"]}
</span>

<span class="weather">
{item["weather"]}
</span>

</td>


<td>

<span class="volume-value">
{item["volume"]}
</span>

<div class="direction">
{item["direction"]}
</div>

</td>


<td>

<div class="today-wrap">

<div class="change-value">
{item["change"]}
</div>

<div class="breakout-warning">
{item["warning"]}
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

현재 조건 일치 종목 없음

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

<div class="note">

※ TOP{TOP_N} 거래대금 순위 기준<br>

※ 코인명 아래 ☀️ = 당일 양수 / ☁️ = 당일 음수<br>

※ 거래대금 아래 LONG / SHORT = 현재 방향<br>

※ 🚨1H = 1시간 돌파전 또는 재돌파 조건<br>

※ 🚨4H = 4시간 돌파전 또는 재돌파 조건<br>

※ 1H / 4H 중 하나라도 조건 충족하면 표시<br>

※ 해/구름 방향과 돌파 방향이 일치해야 표시<br>

※ LONG = 30 > 60 > 120 EMA 정배열<br>

※ SHORT = 30 < 60 < 120 EMA 역배열<br>

※ 돌파는 상승 1파 → 조정 → 재상승 구조 기준<br>

※ 돌파 기준은 최근 {BREAKOUT_LOOKBACK}개 캔들<br>

※ 최소 10개 이상 구간을 사용<br>

※ 현재 진행 중인 1H / 4H 캔들도 실시간 감시<br>

※ 🚀 표시는 사용하지 않음<br>

※ 조건이 사라지면 경고 리스트에서 즉시 제거

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
1H + 4H 실시간 추세 · 돌파 감시
</div>

<div>
상승 1파 → 조정 → 재상승 → 전 고점 돌파
</div>

<div>
EMA 30-60-120 정배열 기준
</div>

<div>
TOP{TOP_N} · 1분 조회 · 현재 진행 캔들 포함
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
        "========================================"
    )

    logging.info(
        "Breakout Trading 서버 시작"
    )

    logging.info(
        f"업비트={USE_UPBIT} "
        f"OKX={USE_OKX}"
    )

    logging.info(
        f"TOP_N={TOP_N} "
        f"조회={UPDATE_MINUTES}분"
    )

    logging.info(
        f"BREAKOUT_LOOKBACK="
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
