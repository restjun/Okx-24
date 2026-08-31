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

# 전고점 근처를 돌파 직전으로 판단하는 허용 거리
# 0.5% 이내
PRE_BREAKOUT_DISTANCE = 0.005

# 전고점 후보를 찾기 위한 좌우 캔들 수
SWING_LEFT = 2
SWING_RIGHT = 2


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
# OKX 확정 캔들
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

        if not data:
            return None

        df = pd.DataFrame(
            data
        )

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

    if len(ema10) == 0 or len(ema30) == 0:
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
# =========================================================

def get_ema_30_60_120_direction(
    df,
    column="c"
):

    if (
        df is None
        or
        len(df) < 120
    ):

        return "none"

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

    if (
        len(ema30) == 0
        or
        len(ema60) == 0
        or
        len(ema120) == 0
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
# EMA 10-30-60
# =========================================================

def get_ema_10_30_60_direction(
    df,
    column
):

    if (
        df is None
        or
        len(df) < 60
    ):

        return "none"

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
# EMA 표시
# =========================================================

def check_ema(
    df
):

    if (
        df is None
        or
        df.empty
    ):

        return {
            "display": "⚪",
            "direction": "none",
            "count": 0
        }

    # -----------------------------------------------------
    # 4H / 장기 기준으로 사용할 때
    # 30-60-120
    # -----------------------------------------------------

    if len(df) >= 120:

        direction = (
            get_ema_30_60_120_direction(
                df,
                "c"
            )
        )

        if direction == "long":

            return {
                "display": "🟢 L",
                "direction": "long",
                "count": 0
            }

        if direction == "short":

            return {
                "display": "🔴 S",
                "direction": "short",
                "count": 0
            }

    # -----------------------------------------------------
    # 캔들 부족
    # -----------------------------------------------------

    direction = (
        get_ema_10_30_direction(
            df,
            "c"
        )
    )

    if direction == "long":

        return {
            "display": "🟢 L",
            "direction": "long",
            "count": 0
        }

    if direction == "short":

        return {
            "display": "🔴 S",
            "direction": "short",
            "count": 0
        }

    return {
        "display": "⚪",
        "direction": "none",
        "count": 0
    }


# =========================================================
# 4H 30-60-120 정배열 여부
# =========================================================

def is_4h_long_alignment(
    df4h
):

    if (
        df4h is None
        or
        len(df4h) < 120
    ):

        return False

    return (
        get_ema_30_60_120_direction(
            df4h,
            "c"
        )
        == "long"
    )


# =========================================================
# 스윙 고점 찾기
#
# 최근에 만들어진 확정 고점 중
# 현재 가격보다 위에 있는 고점을 찾는다.
# =========================================================

def find_previous_swing_high(
    df,
    end_index
):

    if (
        df is None
        or
        df.empty
    ):

        return None

    start = max(
        SWING_LEFT,
        1
    )

    end = min(
        end_index - SWING_RIGHT,
        len(df) - SWING_RIGHT - 1
    )

    if end < start:

        return None

    candidates = []

    for i in range(
        start,
        end + 1
    ):

        try:

            high = float(
                df["h"].iloc[i]
            )

            left_highs = pd.to_numeric(
                df["h"].iloc[
                    i - SWING_LEFT:i
                ],
                errors="coerce"
            )

            right_highs = pd.to_numeric(
                df["h"].iloc[
                    i + 1:
                    i + 1 + SWING_RIGHT
                ],
                errors="coerce"
            )

            if (
                left_highs.empty
                or
                right_highs.empty
            ):

                continue

            if (
                high >= left_highs.max()
                and
                high >= right_highs.max()
            ):

                candidates.append(
                    (
                        i,
                        high
                    )
                )

        except Exception:

            continue

    if not candidates:
        return None

    return candidates[-1]


# =========================================================
# 4H 상승 돌파 추적
#
# 상태:
#
# none
# pre
# 1
#
# pre:
# 전고점 바로 아래
#
# 1:
# 전고점을 첫 양봉으로 돌파
# =========================================================

def get_4h_breakout_signal(
    df4h
):

    if (
        df4h is None
        or
        df4h.empty
    ):

        return "none"

    if len(df4h) < 125:

        return "none"

    # -----------------------------------------------------
    # 30-60-120 정배열이 아니면 신호 없음
    # -----------------------------------------------------

    if not is_4h_long_alignment(
        df4h
    ):

        return "none"

    df = (
        df4h
        .copy()
        .reset_index(drop=True)
    )

    current_index = len(df) - 1

    current = df.iloc[
        current_index
    ]

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
    # 최근 확정 스윙 고점
    #
    # 현재 캔들에서 너무 먼 과거가 아닌
    # 최근 10~30개 정도 범위 안에서 탐색
    # -----------------------------------------------------

    search_start = max(
        0,
        current_index - 40
    )

    swing_candidates = []

    for i in range(
        search_start,
        current_index - SWING_RIGHT
    ):

        if i < SWING_LEFT:
            continue

        try:

            high = float(
                df["h"].iloc[i]
            )

            left = pd.to_numeric(
                df["h"].iloc[
                    i - SWING_LEFT:i
                ],
                errors="coerce"
            )

            right = pd.to_numeric(
                df["h"].iloc[
                    i + 1:
                    i + 1 + SWING_RIGHT
                ],
                errors="coerce"
            )

            if (
                left.empty
                or
                right.empty
            ):

                continue

            if (
                high >= left.max()
                and
                high >= right.max()
            ):

                swing_candidates.append(
                    (
                        i,
                        high
                    )
                )

        except Exception:

            continue

    if not swing_candidates:

        return "none"

    # -----------------------------------------------------
    # 가장 최근 스윙 고점
    # -----------------------------------------------------

    swing_index, swing_high = (
        swing_candidates[-1]
    )

    # -----------------------------------------------------
    # 너무 오래된 고점이면 무효
    # -----------------------------------------------------

    if (
        current_index
        -
        swing_index
        >
        BREAKOUT_LOOKBACK + 15
    ):

        return "none"

    # -----------------------------------------------------
    # 고점 돌파 이전의 조정이 있었는지 확인
    #
    # 단순히 현재 가격이 고점 아래에 있다고
    # 바로 경고하지 않는다.
    # 고점 이후 저점이 낮아졌다가 다시 올라오는
    # 구조를 요구한다.
    # -----------------------------------------------------

    if current_index <= swing_index + 2:

        return "none"

    after_swing = df.iloc[
        swing_index + 1:
        current_index
    ]

    if after_swing.empty:

        return "none"

    correction_low = pd.to_numeric(
        after_swing["l"],
        errors="coerce"
    ).min()

    if pd.isna(correction_low):

        return "none"

    # 고점 대비 최소 0.3% 이상 조정
    correction_rate = (
        (
            swing_high
            -
            float(correction_low)
        )
        /
        swing_high
    )

    if correction_rate < 0.003:

        return "none"

    # =====================================================
    # 이미 이전 캔들에서 고점을 돌파했다면
    # 현재 시점에서 뒤늦게 신호를 만들지 않는다.
    # =====================================================

    if current_index >= 1:

        previous = df.iloc[
            current_index - 1
        ]

        previous_close = float(
            previous["c"]
        )

        if (
            previous_close > swing_high
            and
            float(previous["c"])
            >
            float(previous["o"])
        ):

            return "none"

    # =====================================================
    # 1. 현재 캔들이 전고점 돌파
    #
    # 첫 양봉 돌파만 🚀1
    # =====================================================

    if (
        current_close > swing_high
        and
        current_close > current_open
    ):

        return "1"

    # =====================================================
    # 2. 돌파 직전
    #
    # 전고점 아래에 있으면서
    # 고점에 매우 가까워지고 있는 양봉
    # =====================================================

    distance = (
        swing_high
        -
        current_close
    ) / swing_high

    if (
        current_close < swing_high
        and
        current_close >= current_open
        and
        distance <= PRE_BREAKOUT_DISTANCE
    ):

        return "pre"

    return "none"


# =========================================================
# 4H 돌파 경고
# =========================================================

def get_breakout_warning(
    df4h
):

    signal = get_4h_breakout_signal(
        df4h
    )

    return {
        "4h": signal
    }


# =========================================================
# 경고 표시 여부
# =========================================================

def is_visible_warning(
    warning
):

    if not warning:
        return False

    return (
        warning.get("4h")
        in
        ("pre", "1")
    )


# =========================================================
# 경고 HTML
# =========================================================

def combined_warning_html(
    warning
):

    if not warning:
        return ""

    signal = warning.get(
        "4h",
        "none"
    )

    if signal == "pre":

        return (
            '<span class="warning-pre">'
            '🚨'
            '</span>'
        )

    if signal == "1":

        return (
            '<span class="warning-rocket">'
            '🚀1'
            '</span>'
        )

    return ""


# =========================================================
# 변동률
# =========================================================

def calculate_daily_changes(
    df,
    time_column,
    price_column,
    is_okx=False
):

    if (
        df is None
        or
        df.empty
        or
        len(df) < 50
    ):

        return None

    try:

        temp = df.copy()

        if is_okx:

            temp["ts"] = pd.to_numeric(
                temp["ts"],
                errors="coerce"
            )

            temp = temp.dropna(
                subset=["ts"]
            )

            if temp.empty:
                return None

            temp["datetime"] = (
                pd.to_datetime(
                    temp["ts"],
                    unit="ms"
                )
                +
                pd.Timedelta(hours=9)
            )

        else:

            if time_column not in temp.columns:
                return None

            temp["datetime"] = pd.to_datetime(
                temp[time_column],
                errors="coerce"
            )

        temp[price_column] = pd.to_numeric(
            temp[price_column],
            errors="coerce"
        )

        temp = temp.dropna(
            subset=[
                "datetime",
                price_column
            ]
        )

        if temp.empty:
            return None

        temp = temp.set_index(
            "datetime"
        )

        daily = (
            temp[price_column]
            .resample(
                "1D",
                offset="9h"
            )
            .last()
            .dropna()
        )

        # 최소 4개 일봉 필요
        if len(daily) < 4:

            return None

        result = []

        # 현재일
        current_price = daily.iloc[-1]

        previous_price = daily.iloc[-2]

        if previous_price == 0:

            current_change = 0.0

        else:

            current_change = (
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
                float(current_change),
                2
            )
        )

        # 이전 2일
        if len(daily) >= 3:

            p1 = daily.iloc[-2]
            p2 = daily.iloc[-3]

            if p2 != 0:

                result.append(
                    round(
                        float(
                            (
                                p1 - p2
                            )
                            /
                            p2
                            *
                            100
                        ),
                        2
                    )
                )

        if len(daily) >= 4:

            p1 = daily.iloc[-3]
            p2 = daily.iloc[-4]

            if p2 != 0:

                result.append(
                    round(
                        float(
                            (
                                p1 - p2
                            )
                            /
                            p2
                            *
                            100
                        ),
                        2
                    )
                )

        return result

    except Exception as e:

        logging.error(
            f"일간 변동률 계산 오류 : {e}"
        )

        return None


# =========================================================
# 업비트 변동률
# =========================================================

def get_upbit_change(
    market
):

    df = get_upbit_ohlcv(
        market,
        60,
        200
    )

    if df is None:

        return None

    return calculate_daily_changes(
        df,
        "candle_date_time_kst",
        "trade_price",
        False
    )


# =========================================================
# OKX 변동률
# =========================================================

def get_okx_change(
    inst_id
):

    df = get_okx_ohlcv(
        inst_id,
        "1H",
        200
    )

    if df is None:

        return None

    return calculate_daily_changes(
        df,
        "ts",
        "c",
        True
    )


# =========================================================
# 당일 표시
#
# 양수 = ☀️
# 음수 = ☁️
# =========================================================

def format_change(
    changes
):

    if (
        changes is None
        or
        len(changes) == 0
    ):

        return (
            '<span class="change-item">'
            '⬜ N/A'
            '</span>'
        )

    try:

        x = float(
            changes[0]
        )

    except Exception:

        return (
            '<span class="change-item">'
            '⬜ N/A'
            '</span>'
        )

    if x > 0:

        icon = "☀️"

        sign = "+"

        cls = "positive"

    elif x < 0:

        icon = "☁️"

        sign = ""

        cls = "negative"

    else:

        icon = "☁️"

        sign = ""

        cls = "neutral"

    return (
        '<span class="change-item '
        f'{cls}">'
        f'{icon} {sign}{x:.2f}%'
        '</span>'
    )


# =========================================================
# EMA HTML
#
# 1시간
# 4시간
# 두 줄
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
# 1H 표시
# 4H 표시
#
# 돌파 판단은 4H만 사용
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
                "4h": "none"
            }
        }

    df1h = raw1h.copy()

    df4h = raw4h.copy()

    # -----------------------------------------------------
    # 현재 진행 캔들 제외
    # -----------------------------------------------------

    if len(df1h) > 1:

        df1h = (
            df1h
            .iloc[:-1]
            .reset_index(drop=True)
        )

    if len(df4h) > 1:

        df4h = (
            df4h
            .iloc[:-1]
            .reset_index(drop=True)
        )

    # -----------------------------------------------------
    # EMA
    # -----------------------------------------------------

    ema1h = check_ema(
        df1h
    )

    ema4h = check_ema(
        df4h
    )

    # -----------------------------------------------------
    # 4H 돌파
    # -----------------------------------------------------

    warning = get_breakout_warning(
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
        or
        df4h is None
    ):

        return {
            "1h_ema": empty_ema(),
            "4h_ema": empty_ema(),
            "warning": {
                "4h": "none"
            }
        }

    ema1h = check_ema(
        df1h
    )

    ema4h = check_ema(
        df4h
    )

    warning = get_breakout_warning(
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

            # -------------------------------------------------
            # 업비트는 4H LONG 돌파만 표시
            # -------------------------------------------------

            signal = warning.get(
                "4h",
                "none"
            )

            if signal not in (
                "pre",
                "1"
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
                        ema[
                            "1h_ema"
                        ],

                    "ema_4h":
                        ema[
                            "4h_ema"
                        ],

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

            # -------------------------------------------------
            # OKX는 4H LONG / SHORT 모두 표시 가능
            # 단 현재 돌파 알고리즘은 상승 돌파 중심
            # -------------------------------------------------

            if not is_visible_warning(
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
                        ema[
                            "1h_ema"
                        ],

                    "ema_4h":
                        ema[
                            "4h_ema"
                        ],

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

        global latest_upbit_data

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
    padding: 4px;
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

    width: 17%;
}

th:nth-child(3),
td:nth-child(3) {

    width: 17%;
}

th:nth-child(4),
td:nth-child(4) {

    width: 31%;
}

th:nth-child(5),
td:nth-child(5) {

    width: 28%;
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

    font-weight: 700;

    text-align: center;

    white-space: nowrap;
}

.positive {

    color: #ffffff;
}

.negative {

    color: #ffffff;
}

.neutral {

    color: #aaaaaa;
}


/* =====================================================
   경고
   ===================================================== */

.breakout-warning {

    display: flex;

    justify-content: center;

    align-items: center;

    width: 100%;

    min-height: 14px;

    white-space: nowrap;
}

.warning-pre {

    font-size: 10px;

    font-weight: bold;

    animation:
        warning-blink
        0.9s
        infinite;

    filter:
        drop-shadow(
            0 0 4px
            rgba(255,180,0,0.9)
        );
}

.warning-rocket {

    font-size: 10px;

    font-weight: bold;

    filter:
        drop-shadow(
            0 0 4px
            rgba(50,255,100,0.9)
        );
}


/* =====================================================
   반짝임
   ===================================================== */

@keyframes warning-blink {

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

    font-size: 8px;

    font-weight: bold;

    line-height: 1.5;

    white-space: nowrap;
}

.ema-line {

    display: flex;

    justify-content: center;

    align-items: center;

    gap: 4px;

    width: 100%;
}

.ema-label {

    color: #777d86;

    font-size: 7px;
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

        font-size: 7px;
    }

    .volume-value {

        font-size: 6px;
    }

    .change-item {

        font-size: 7px;
    }

    .warning-pre,
    .warning-rocket {

        font-size: 9px;
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
        font-size:6px;
        line-height:1.5;
        margin:4px 2px 7px 2px;
     ">

※ TOP{TOP_N} 거래대금 순위 기준<br>

※ ☀️ 양수 / ☁️ 음수<br>

※ 🚨 = 4H 전고점 돌파 직전<br>

※ 🚨는 전고점 0.5% 이내 접근한 양봉<br>

※ 🚀1 = 전고점을 처음 돌파한 양봉<br>

※ 4H EMA 30-60-120 정배열 유지 조건<br>

※ 고점 형성 → 조정 → 재상승 구조 확인<br>

※ 이미 크게 돌파한 종목은 뒤늦게 경고하지 않음<br>

※ EMA는 1H / 4H 두 줄 표시

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
4H 30-60-120 정배열 + 전고점 재돌파
</div>

<div>
고점 형성 → 조정 → 재상승 → 전고점 돌파
</div>

<div>
🚨 돌파 직전 · 🚀1 첫 돌파
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
