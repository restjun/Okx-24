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

TOP_N = 50

UPDATE_MINUTES = 1

BREAKOUT_LOOKBACK = 10

# 전고점 근처를 돌파 직전으로 판단하는 허용 거리
PRE_BREAKOUT_DISTANCE = 0.005

# 스윙 고점 판단
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

OKX_MAX_RETRY_ROUNDS = 3


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

    logging.info(
        "OKX 환산용 USDT-KRW API 요청 시작"
    )

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

        logging.error(
            "USDT-KRW API 응답 없음"
        )

        return None

    try:

        data = response.json()

        if not data:

            logging.error(
                "USDT-KRW API 데이터 없음"
            )

            return None

        price = float(
            data[0]["trade_price"]
        )

        if price <= 0:

            logging.error(
                f"USDT-KRW 잘못된 가격 : {price}"
            )

            return None

        logging.info(
            f"USDT-KRW 조회 성공 : {price}"
        )

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

        logging.warning(
            f"OKX 캔들 응답 없음 : {inst_id}"
        )

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

        # 확정 캔들만 사용
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
# EMA 표시
#
# 4H는 화면 표시용
# 경고 조건에는 사용하지 않음
# =========================================================

def check_ema(
    df
):

    if (
        df is None
        or
        df.empty
    ):

        return empty_ema()

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

    return empty_ema()


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
# 1H 최초 정배열 시작점 찾기
#
# 조건:
#
# 이전:
# 30 <= 60 또는 60 <= 120
#
# 현재:
# 30 > 60 > 120
#
# 가장 최근에 발생한 정배열 전환점
# =========================================================

def find_first_1h_alignment_start(
    df
):

    if (
        df is None
        or
        len(df) < 125
    ):

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

    if (
        ema30 is None
        or
        ema60 is None
        or
        ema120 is None
    ):

        return None

    start_index = None

    # 너무 과거까지 가지 않고
    # 최근 60개 확정봉에서 탐색
    search_start = max(
        120,
        len(df) - 60
    )

    search_end = len(df) - 1

    for i in range(
        search_start,
        search_end + 1
    ):

        if i <= 0:

            continue

        p30 = ema30.iloc[i - 1]
        p60 = ema60.iloc[i - 1]
        p120 = ema120.iloc[i - 1]

        c30 = ema30.iloc[i]
        c60 = ema60.iloc[i]
        c120 = ema120.iloc[i]

        if any(
            pd.isna(x)
            for x in [
                p30,
                p60,
                p120,
                c30,
                c60,
                c120
            ]
        ):

            continue

        was_not_long = not (
            p30 > p60 > p120
        )

        now_long = (
            c30 > c60 > c120
        )

        if (
            was_not_long
            and
            now_long
        ):

            start_index = i

    return start_index


# =========================================================
# 최초 정배열 시작 이후
# 다시 역배열로 전환됐는지 확인
# =========================================================

def alignment_still_valid(
    df,
    start_index
):

    if (
        df is None
        or
        start_index is None
        or
        start_index >= len(df)
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

    for i in range(
        start_index,
        len(df)
    ):

        a = ema30.iloc[i]
        b = ema60.iloc[i]
        c = ema120.iloc[i]

        if any(
            pd.isna(x)
            for x in [a, b, c]
        ):

            continue

        # 완전 역배열이 나오면
        # 기존 정배열 구조 종료
        if a < b < c:

            return False

    return True


# =========================================================
# 최초 정배열 시작 구간의 기준 고점 찾기
#
# 정배열 시작점부터
# 현재까지의 고점 중
# 의미 있는 최근 스윙 고점 선택
# =========================================================

def find_alignment_reference_high(
    df,
    alignment_start
):

    if (
        df is None
        or
        df.empty
        or
        alignment_start is None
    ):

        return None

    current_index = len(df) - 1

    if alignment_start >= current_index:

        return None

    # 정배열 시작 후 최소 몇 개 캔들이 필요
    if current_index <= alignment_start + 3:

        return None

    candidates = []

    # 정배열 시작 이후 스윙 고점 탐색
    for i in range(
        alignment_start + 1,
        current_index - SWING_RIGHT + 1
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

    # 최초 정배열 시작 이후 만들어진
    # 고점들 중 가장 높은 고점을 기준 고점으로 사용
    reference = max(
        candidates,
        key=lambda x: x[1]
    )

    return reference


# =========================================================
# 1H 상승 돌파 신호
#
# 핵심:
#
# 1H 30-60-120 최초 정배열 전환
# ↓
# 그 정배열 구간에서 형성된 고점
# ↓
# 조정
# ↓
# 재상승
# ↓
# 🚨
# ↓
# 🚀
# ↓
# 〽️
# =========================================================

def get_1h_breakout_signal(
    df1h
):

    if (
        df1h is None
        or
        df1h.empty
        or
        len(df1h) < 125
    ):

        return "none"

    df = (
        df1h
        .copy()
        .reset_index(drop=True)
    )

    for col in [
        "o",
        "h",
        "l",
        "c"
    ]:

        df[col] = pd.to_numeric(
            df[col],
            errors="coerce"
        )

    df = df.dropna(
        subset=[
            "o",
            "h",
            "l",
            "c"
        ]
    ).reset_index(
        drop=True
    )

    if len(df) < 125:

        return "none"

    # =====================================================
    # 현재 1H 상태
    # =====================================================

    current_direction = (
        get_ema_30_60_120_direction(
            df,
            "c"
        )
    )

    # 현재가 정배열이어야 함
    if current_direction != "long":

        return "none"

    # =====================================================
    # 최초 정배열 시작점
    # =====================================================

    alignment_start = (
        find_first_1h_alignment_start(
            df
        )
    )

    if alignment_start is None:

        return "none"

    # =====================================================
    # 정배열이 이후 계속 유지되어야 함
    # =====================================================

    if not alignment_still_valid(
        df,
        alignment_start
    ):

        return "none"

    current_index = len(df) - 1

    # =====================================================
    # 정배열 시작 후 기준 고점
    # =====================================================

    reference = (
        find_alignment_reference_high(
            df,
            alignment_start
        )
    )

    if reference is None:

        return "none"

    reference_index, reference_high = (
        reference
    )

    # 기준 고점이 너무 오래되면 무효
    if (
        current_index
        -
        reference_index
        >
        BREAKOUT_LOOKBACK + 15
    ):

        return "none"

    # =====================================================
    # 기준 고점 이후 조정 확인
    # =====================================================

    if current_index <= reference_index + 1:

        return "none"

    after_reference = df.iloc[
        reference_index + 1:
        current_index + 1
    ]

    if after_reference.empty:

        return "none"

    correction_low = pd.to_numeric(
        after_reference["l"],
        errors="coerce"
    ).min()

    if pd.isna(correction_low):

        return "none"

    correction_rate = (
        (
            reference_high
            -
            float(correction_low)
        )
        /
        reference_high
    )

    # 최소 0.3% 조정
    if correction_rate < 0.003:

        return "none"

    # =====================================================
    # 🚨 찾기
    #
    # 기준 고점 아래
    # 0.5% 이내
    # 양봉
    # =====================================================

    pre_index = None

    for i in range(
        reference_index + 1,
        current_index + 1
    ):

        candle = df.iloc[i]

        o = float(
            candle["o"]
        )

        c = float(
            candle["c"]
        )

        if c < o:

            continue

        # 아직 돌파하지 않은 캔들
        if c >= reference_high:

            continue

        distance = (
            reference_high - c
        ) / reference_high

        if distance <= PRE_BREAKOUT_DISTANCE:

            pre_index = i

    # =====================================================
    # 🚨 없음
    # =====================================================

    if pre_index is None:

        current = df.iloc[
            current_index
        ]

        current_open = float(
            current["o"]
        )

        current_close = float(
            current["c"]
        )

        distance = (
            reference_high
            -
            current_close
        ) / reference_high

        if (
            current_close < reference_high
            and
            current_close >= current_open
            and
            distance <= PRE_BREAKOUT_DISTANCE
        ):

            return "pre"

        return "none"

    # =====================================================
    # 🚨 기준 캔들 저점
    # =====================================================

    pre_low = float(
        df["l"].iloc[
            pre_index
        ]
    )

    # =====================================================
    # 🚀 최초 돌파 캔들
    # =====================================================

    breakout_index = None

    for i in range(
        pre_index + 1,
        current_index + 1
    ):

        candle = df.iloc[i]

        o = float(
            candle["o"]
        )

        c = float(
            candle["c"]
        )

        # 돌파 캔들은 양봉
        if c <= o:

            continue

        # 기준 고점 돌파
        if c > reference_high:

            breakout_index = i

            break

    # =====================================================
    # 아직 돌파하지 않음
    # =====================================================

    if breakout_index is None:

        if current_index == pre_index:

            return "pre"

        current = df.iloc[
            current_index
        ]

        current_open = float(
            current["o"]
        )

        current_close = float(
            current["c"]
        )

        distance = (
            reference_high
            -
            current_close
        ) / reference_high

        if (
            current_close < reference_high
            and
            current_close >= current_open
            and
            distance <= PRE_BREAKOUT_DISTANCE
        ):

            return "pre"

        return "none"

    # =====================================================
    # 🚀 현재 캔들이 최초 돌파 캔들
    # =====================================================

    if breakout_index == current_index:

        return "1"

    # =====================================================
    # 🚀 다음 캔들
    # =====================================================

    after_breakout_index = (
        breakout_index + 1
    )

    if current_index == after_breakout_index:

        current = df.iloc[
            current_index
        ]

        current_low = float(
            current["l"]
        )

        current_close = float(
            current["c"]
        )

        current_open = float(
            current["o"]
        )

        # =================================================
        # 🚨 기준 캔들 저점 이탈
        #
        # 돌파 실패
        # =================================================

        if current_low < pre_low:

            return "none"

        # =================================================
        # 🚀 직후 음봉
        # =================================================

        if current_close < current_open:

            return "pullback"

        return "none"

    # =====================================================
    # 두 번째 캔들부터는 표시하지 않음
    # =====================================================

    return "none"


# =========================================================
# 돌파 경고
#
# ★ 4H 완전 제거
# =========================================================

def get_breakout_warning(
    df1h
):

    signal = get_1h_breakout_signal(
        df1h
    )

    return {
        "1h": signal
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
        warning.get("1h")
        in
        (
            "pre",
            "1",
            "pullback"
        )
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
        "1h",
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
            '🚀'
            '</span>'
        )

    if signal == "pullback":

        return (
            '<span class="warning-pullback">'
            '〽️'
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

        if len(daily) < 4:

            return None

        result = []

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
# 업비트 EMA
#
# ★ 경고는 1H만 사용
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
                "1h": "none"
            }
        }

    df1h = raw1h.copy()

    df4h = raw4h.copy()

    # 현재 진행 캔들 제외
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

    ema1h = check_ema(
        df1h
    )

    ema4h = check_ema(
        df4h
    )

    # ★ 경고는 1H
    warning = get_breakout_warning(
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
# ★ 경고는 1H만 사용
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
                "1h": "none"
            }
        }

    ema1h = check_ema(
        df1h
    )

    ema4h = check_ema(
        df4h
    )

    # ★ 4H 사용 안 함
    warning = get_breakout_warning(
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

    for retry_round in range(
        1,
        OKX_MAX_RETRY_ROUNDS + 1
    ):

        try:

            logging.info(
                f"OKX 거래대금 API 조회 "
                f"{inst_id} "
                f"({retry_round}/{OKX_MAX_RETRY_ROUNDS})"
            )

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

    return None


# =========================================================
# OKX 전체 목록
# =========================================================

def get_all_okx_swap_symbols():

    logging.info(
        "OKX SWAP 종목 목록 API 요청 시작"
    )

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

        symbols = [
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

        logging.info(
            f"OKX SWAP 종목 목록 조회 성공 : "
            f"{len(symbols)}개"
        )

        return symbols

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
        f"업비트 1H 돌파 종목 "
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

    total_symbols = len(symbols)

    for index, symbol in enumerate(
        symbols,
        start=1
    ):

        logging.info(
            f"[OKX 거래대금] "
            f"{index}/{total_symbols} "
            f"{symbol}"
        )

        volume = get_okx_volume(
            symbol,
            usdt_krw
        )

        if (
            volume is not None
            and
            volume > 0
        ):

            volume_map[
                symbol
            ] = volume

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

    logging.info(
        f"OKX 1H 돌파 종목 "
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

            if (
                usdt_krw is not None
                and
                usdt_krw > 0
            ):

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

    logging.info(
        "1분 현재상태 조회 종료"
    )

    logging.info(
        "========================================"
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

.coin {

    display: block;
    font-size: 8px;
    font-weight: bold;
    line-height: 1.2;
}

.volume-value {

    font-size: 7px;
    font-weight: 600;
}

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

.positive,
.negative {
    color: #ffffff;
}

.neutral {
    color: #aaaaaa;
}

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

.warning-pullback {

    font-size: 10px;
    font-weight: bold;

    filter:
        drop-shadow(
            0 0 4px
            rgba(255,200,50,0.9)
        );
}

@keyframes warning-blink {

    0%,
    100% {
        opacity: 1;
    }

    50% {
        opacity: 0.25;
    }
}

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
    .warning-rocket,
    .warning-pullback {
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

현재 🚨 / 🚀 / 〽️ 종목 없음

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

※ 🚨 = 1H 최초 정배열 이후 기준 고점 돌파 직전<br>

※ 🚨 = 기준 고점 0.5% 이내 접근한 양봉<br>

※ 🚀 = 기준 고점을 처음 돌파한 양봉<br>

※ 〽️ = 🚀 직후 음봉 눌림<br>

※ 〽️는 🚨 캔들의 저점을 이탈하지 않을 때만 표시<br>

※ 🚨 캔들 저점 이탈 시 돌파 실패<br>

※ 경고 판단은 1H 30-60-120 정배열만 사용<br>

※ 4H EMA는 화면 표시용이며 경고 조건에 사용하지 않음<br>

※ 역배열 → 최초 정배열 전환 → 고점 → 조정 → 재돌파 구조<br>

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
1H 30-60-120 최초 정배열 전환 + 기준 고점 재돌파
</div>

<div>
역배열 → 최초 정배열 → 고점 형성 → 조정 → 재상승
</div>

<div>
🚨 돌파 직전 · 🚀 첫 돌파 · 〽️ 돌파 직후 눌림
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

    # 최초 즉시 조회
    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    # 1분마다 조회
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
