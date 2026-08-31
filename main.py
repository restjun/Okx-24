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

TOP_N = 30

UPDATE_MINUTES = 1

BREAKOUT_LOOKBACK = 10

# 전고점 근처를 돌파 직전으로 판단하는 허용 거리
PRE_BREAKOUT_DISTANCE = 0.005

# 최소 조정폭
MIN_CORRECTION_RATE = 0.003

# 스윙 고점 판단용
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

            return None

        price = float(
            data[0]["trade_price"]
        )

        if price <= 0:

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

        # OKX는 현재 진행 중인 캔들이 섞일 수 있으므로
        # 확정 캔들만 사용하지 않고 아래에서 별도 처리하지 않는다.
        #
        # 이번 로직에서는 현재 진행 캔들을 활용하기 위해
        # confirm 여부와 관계없이 가격 데이터를 사용한다.

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

        df = df.dropna(
            subset=[
                "o",
                "h",
                "l",
                "c"
            ]
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
# 1H 60-120 방향
#
# 핵심 기준
#
# 60 > 120 = 정배열
# 60 < 120 = 역배열
#
# 30 EMA는 사용하지 않음
# 4H 조건도 사용하지 않음
# =========================================================

def get_ema_60_120_direction(
    df,
    column="c"
):

    if (
        df is None
        or
        len(df) < 120
    ):

        return "none"

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
        ema60 is None
        or
        ema120 is None
    ):

        return "none"

    a = ema60.iloc[-1]

    b = ema120.iloc[-1]

    if pd.isna(a) or pd.isna(b):

        return "none"

    if a > b:

        return "long"

    if a < b:

        return "short"

    return "none"


# =========================================================
# 1H 60-120 정배열 시작점
# =========================================================

def find_alignment_start(
    df,
    direction
):

    if (
        df is None
        or
        len(df) < 125
    ):

        return None

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
        ema60 is None
        or
        ema120 is None
    ):

        return None

    start_index = None

    for i in range(
        121,
        len(df)
    ):

        a = ema60.iloc[i]

        b = ema120.iloc[i]

        pa = ema60.iloc[i - 1]

        pb = ema120.iloc[i - 1]

        if any(
            pd.isna(x)
            for x in [
                a,
                b,
                pa,
                pb
            ]
        ):

            continue

        if direction == "long":

            # 60이 120 아래/같았다가
            # 처음 위로 올라온 지점
            if (
                pa <= pb
                and
                a > b
            ):

                start_index = i

        elif direction == "short":

            # 60이 120 위/같았다가
            # 처음 아래로 내려온 지점
            if (
                pa >= pb
                and
                a < b
            ):

                start_index = i

    return start_index


# =========================================================
# 고점 후보
# =========================================================

def is_swing_high(
    df,
    index
):

    if (
        index < SWING_LEFT
        or
        index + SWING_RIGHT >= len(df)
    ):

        return False

    try:

        high = float(
            df["h"].iloc[index]
        )

        left = pd.to_numeric(
            df["h"].iloc[
                index - SWING_LEFT:index
            ],
            errors="coerce"
        )

        right = pd.to_numeric(
            df["h"].iloc[
                index + 1:
                index + 1 + SWING_RIGHT
            ],
            errors="coerce"
        )

        if (
            left.empty
            or
            right.empty
        ):

            return False

        return (
            high >= left.max()
            and
            high >= right.max()
        )

    except Exception:

        return False


# =========================================================
# 상승 구조 분석
#
# 핵심 변경
#
# ① 1H 60-120 정배열 상태
# ② 정배열 최초 시작점 이후 고점을 찾음
# ③ 고점 → 눌림
# ④ 반등
# ⑤ 전고점 돌파 실패 시
#    반등 고점을 새로운 고점으로 변경
# ⑥ 다시 눌림 → 반등
# ⑦ 현재 진행 캔들까지 활용
# =========================================================

def analyze_long_structure(
    df
):

    if (
        df is None
        or
        len(df) < 125
    ):

        return None

    direction = get_ema_60_120_direction(
        df
    )

    if direction != "long":

        return None

    alignment_start = (
        find_alignment_start(
            df,
            "long"
        )
    )

    if alignment_start is None:

        # 과거 시작점을 못 찾는 경우
        # 현재 데이터 안에서 충분히 오래된 시점부터 분석
        alignment_start = max(
            120,
            len(df) - 60
        )

    current_index = len(df) - 1

    if current_index <= alignment_start:

        return None

    # -----------------------------------------------------
    # 정배열 시작 이후의 데이터
    # -----------------------------------------------------

    work = df.iloc[
        alignment_start:
    ].copy().reset_index(
        drop=True
    )

    if len(work) < 5:

        return None

    # -----------------------------------------------------
    # 고점 → 눌림 → 반등 구조를 순차적으로 추적
    # -----------------------------------------------------

    anchor_index = None

    anchor_high = None

    pullback_low = None

    rebound_index = None

    rebound_high = None

    state = "search_high"

    # -----------------------------------------------------
    # 현재까지 모든 캔들을 순차 분석
    # -----------------------------------------------------

    for i in range(
        0,
        len(work)
    ):

        row = work.iloc[i]

        high = float(row["h"])

        low = float(row["l"])

        close = float(row["c"])

        open_price = float(row["o"])

        # -------------------------------------------------
        # 최초 고점 탐색
        # -------------------------------------------------

        if state == "search_high":

            # 현재까지의 최고가를 후보로 사용
            if (
                anchor_high is None
                or
                high > anchor_high
            ):

                anchor_index = i

                anchor_high = high

            # 충분한 눌림이 발생하면
            # 고점 확정
            if (
                anchor_high is not None
                and
                i > anchor_index
            ):

                correction = (
                    anchor_high - low
                ) / anchor_high

                if correction >= MIN_CORRECTION_RATE:

                    pullback_low = low

                    state = "pullback"

            continue

        # -------------------------------------------------
        # 눌림 확인
        # -------------------------------------------------

        if state == "pullback":

            if (
                pullback_low is None
                or
                low < pullback_low
            ):

                pullback_low = low

            # 고점보다 높은 가격이 나오면
            # 돌파
            if high > anchor_high:

                anchor_index = i

                anchor_high = high

                pullback_low = None

                rebound_index = None

                rebound_high = None

                state = "search_high"

                continue

            # -------------------------------------------------
            # 반등 시작
            # -------------------------------------------------

            if (
                close > open_price
                and
                i > anchor_index
            ):

                rebound_index = i

                rebound_high = high

                state = "rebound"

            continue

        # -------------------------------------------------
        # 반등 확인
        # -------------------------------------------------

        if state == "rebound":

            # 반등 중 더 높은 고점이 나오면
            # 반등 고점 갱신
            if (
                rebound_high is None
                or
                high > rebound_high
            ):

                rebound_high = high

                rebound_index = i

            # -------------------------------------------------
            # 기존 전고점 돌파
            # -------------------------------------------------

            if high > anchor_high:

                anchor_index = i

                anchor_high = high

                pullback_low = None

                rebound_index = None

                rebound_high = None

                state = "search_high"

                continue

            # -------------------------------------------------
            # 전고점 돌파 실패 후 다시 눌림
            #
            # ★ 핵심 변경
            #
            # 반등 고점이 기존 전고점을 넘지 못했다면
            # 그 반등 고점을 새로운 고점으로 간주
            # -------------------------------------------------

            if (
                rebound_high is not None
                and
                i > rebound_index
            ):

                correction = (
                    rebound_high
                    -
                    low
                ) / rebound_high

                if correction >= MIN_CORRECTION_RATE:

                    # ★ 반등 고점을 새 기준 고점으로 변경
                    anchor_index = rebound_index

                    anchor_high = rebound_high

                    pullback_low = low

                    rebound_index = None

                    rebound_high = None

                    state = "pullback"

            continue

    # =====================================================
    # 최종 상태 판단
    # =====================================================

    if anchor_high is None:

        return None

    current = work.iloc[-1]

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
    # 현재 진행 캔들이 고점을 돌파
    # -----------------------------------------------------

    if current_close > anchor_high:

        return {
            "signal": "1",
            "anchor_high": anchor_high,
            "anchor_index": anchor_index,
            "pre_low": pullback_low
        }

    # -----------------------------------------------------
    # 전고점 바로 아래
    # -----------------------------------------------------

    distance = (
        anchor_high
        -
        current_close
    ) / anchor_high

    if (
        current_close < anchor_high
        and
        current_close >= current_open
        and
        distance <= PRE_BREAKOUT_DISTANCE
    ):

        return {
            "signal": "pre",
            "anchor_high": anchor_high,
            "anchor_index": anchor_index,
            "pre_low": pullback_low
        }

    return None


# =========================================================
# 하락 구조 분석
#
# 60 < 120 역배열
#
# 고점 → 하락 → 반등 실패
# 반등 고점을 새로운 기준점으로 갱신
# =========================================================

def analyze_short_structure(
    df
):

    if (
        df is None
        or
        len(df) < 125
    ):

        return None

    direction = get_ema_60_120_direction(
        df
    )

    if direction != "short":

        return None

    alignment_start = (
        find_alignment_start(
            df,
            "short"
        )
    )

    if alignment_start is None:

        alignment_start = max(
            120,
            len(df) - 60
        )

    current_index = len(df) - 1

    if current_index <= alignment_start:

        return None

    work = df.iloc[
        alignment_start:
    ].copy().reset_index(
        drop=True
    )

    if len(work) < 5:

        return None

    anchor_index = None

    anchor_low = None

    rebound_high = None

    state = "search_low"

    for i in range(
        len(work)
    ):

        row = work.iloc[i]

        high = float(row["h"])

        low = float(row["l"])

        close = float(row["c"])

        open_price = float(row["o"])

        # -------------------------------------------------
        # 최초 저점 탐색
        # -------------------------------------------------

        if state == "search_low":

            if (
                anchor_low is None
                or
                low < anchor_low
            ):

                anchor_index = i

                anchor_low = low

            if (
                anchor_low is not None
                and
                i > anchor_index
            ):

                correction = (
                    high - anchor_low
                ) / anchor_low

                if correction >= MIN_CORRECTION_RATE:

                    rebound_high = high

                    state = "rebound"

            continue

        # -------------------------------------------------
        # 반등
        # -------------------------------------------------

        if state == "rebound":

            if (
                rebound_high is None
                or
                high > rebound_high
            ):

                rebound_high = high

            # 기존 저점 하향 돌파
            if low < anchor_low:

                anchor_index = i

                anchor_low = low

                rebound_high = None

                state = "search_low"

                continue

            # -------------------------------------------------
            # 다시 하락
            # -------------------------------------------------

            if (
                close < open_price
                and
                i > anchor_index
            ):

                # 현재 반등 고점을 새로운 기준 고점으로
                # 취급하면서 다시 구조를 시작
                if rebound_high is not None:

                    anchor_index = i

                    anchor_low = low

                    rebound_high = None

                    state = "search_low"

            continue

    if anchor_low is None:

        return None

    current = work.iloc[-1]

    current_close = float(
        current["c"]
    )

    current_open = float(
        current["o"]
    )

    distance = (
        current_close
        -
        anchor_low
    ) / anchor_low

    # -----------------------------------------------------
    # 하락 돌파
    # -----------------------------------------------------

    if current_close < anchor_low:

        return {
            "signal": "short_1",
            "anchor_low": anchor_low,
            "anchor_index": anchor_index
        }

    # -----------------------------------------------------
    # 하락 돌파 직전
    # -----------------------------------------------------

    if (
        current_close > anchor_low
        and
        current_close <= current_open
        and
        distance <= PRE_BREAKOUT_DISTANCE
    ):

        return {
            "signal": "short_pre",
            "anchor_low": anchor_low,
            "anchor_index": anchor_index
        }

    return None


# =========================================================
# 1H 돌파 신호
#
# 4H 조건 없음
# 30 EMA 없음
# 60-120만 사용
# =========================================================

def get_1h_breakout_signal(
    df1h
):

    if (
        df1h is None
        or
        df1h.empty
    ):

        return "none"

    if len(df1h) < 125:

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

    direction = get_ema_60_120_direction(
        df
    )

    # =====================================================
    # 정배열
    # =====================================================

    if direction == "long":

        result = analyze_long_structure(
            df
        )

        if result is None:

            return "none"

        signal = result.get(
            "signal"
        )

        # -------------------------------------------------
        # 🚨
        # -------------------------------------------------

        if signal == "pre":

            return "pre"

        # -------------------------------------------------
        # 🚀
        # -------------------------------------------------

        if signal == "1":

            return "1"

        return "none"

    # =====================================================
    # 역배열
    # =====================================================

    if direction == "short":

        result = analyze_short_structure(
            df
        )

        if result is None:

            return "none"

        signal = result.get(
            "signal"
        )

        if signal == "short_pre":

            return "short_pre"

        if signal == "short_1":

            return "short_1"

        return "none"

    return "none"


# =========================================================
# 🚀 직후 눌림
#
# 현재 진행 중인 캔들까지 사용
#
# 단, 돌파 캔들의 저점을 기준으로 함
# =========================================================

def check_long_pullback(
    df
):

    if (
        df is None
        or
        len(df) < 3
    ):

        return False

    current_index = len(df) - 1

    # 직전 캔들이 돌파 캔들인지 확인
    breakout = df.iloc[
        current_index - 1
    ]

    current = df.iloc[
        current_index
    ]

    breakout_open = float(
        breakout["o"]
    )

    breakout_close = float(
        breakout["c"]
    )

    breakout_low = float(
        breakout["l"]
    )

    current_open = float(
        current["o"]
    )

    current_close = float(
        current["c"]
    )

    current_low = float(
        current["l"]
    )

    # 직전 캔들이 양봉이어야 함
    if breakout_close <= breakout_open:

        return False

    # 현재 캔들 저점이 돌파 캔들 저점 이탈
    if current_low < breakout_low:

        return False

    # 현재 캔들이 음봉이면 〽️
    if current_close < current_open:

        return True

    return False


# =========================================================
# 전체 1H 경고
# =========================================================

def get_breakout_warning(
    df1h
):

    signal = get_1h_breakout_signal(
        df1h
    )

    # -----------------------------------------------------
    # 현재가 🚨/🚀가 아니라
    # 바로 직전 캔들이 🚀였는지 확인
    # -----------------------------------------------------

    if signal == "none":

        if (
            df1h is not None
            and
            len(df1h) >= 3
        ):

            previous_df = (
                df1h
                .iloc[:-1]
                .reset_index(drop=True)
            )

            previous_signal = (
                get_1h_breakout_signal(
                    previous_df
                )
            )

            if previous_signal == "1":

                if check_long_pullback(
                    df1h
                ):

                    return {
                        "1h": "pullback"
                    }

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
            "pullback",
            "short_pre",
            "short_1"
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

    if signal in (
        "pre",
        "short_pre"
    ):

        return (
            '<span class="warning-pre">'
            '🚨'
            '</span>'
        )

    if signal in (
        "1",
        "short_1"
    ):

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
# EMA 표시
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
# EMA 표시용
#
# ★ 1H는 60-120
# ★ 4H도 표시만 함
# ★ 4H는 경고 조건에 사용하지 않음
# =========================================================

def get_display_ema_1h(
    df
):

    direction = get_ema_60_120_direction(
        df
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


def get_display_ema_4h(
    df
):

    if (
        df is None
        or
        len(df) < 120
    ):

        return empty_ema()

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
        ema60 is None
        or
        ema120 is None
    ):

        return empty_ema()

    a = ema60.iloc[-1]

    b = ema120.iloc[-1]

    if pd.isna(a) or pd.isna(b):

        return empty_ema()

    if a > b:

        return {
            "display": "🟢 L",
            "direction": "long",
            "count": 0
        }

    if a < b:

        return {
            "display": "🔴 S",
            "direction": "short",
            "count": 0
        }

    return empty_ema()


# =========================================================
# 업비트 EMA
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
            "warning": {
                "1h": "none"
            }
        }

    df1h = raw1h.copy()

    df4h = (
        raw4h.copy()
        if raw4h is not None
        else None
    )

    # ★ 현재 진행 중인 1H 캔들을 제외하지 않는다.
    # ★ 현재 캔들까지 활용

    ema1h = get_display_ema_1h(
        df1h
    )

    ema4h = get_display_ema_4h(
        df4h
    )

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
            "warning": {
                "1h": "none"
            }
        }

    ema1h = get_display_ema_1h(
        df1h
    )

    ema4h = get_display_ema_4h(
        df4h
    )

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
        "========== 현재상태 조회 시작 =========="
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

        latest_okx_data = []

    logging.info(
        "========== 현재상태 조회 종료 =========="
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

.positive {
    color: #ffffff;
}

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

※ 🚨 = 1H 60-120 정배열/역배열 상태에서 전고점 돌파 직전<br>

※ 🚨 = 전고점 0.5% 이내 접근한 양봉<br>

※ 🚀 = 전고점을 처음 돌파한 캔들<br>

※ 〽️ = 🚀 직후 음봉 눌림<br>

※ 🚨 캔들의 저점 이탈 시 돌파 실패<br>

※ 1H 60-120 정배열/역배열만 경고 판단에 사용<br>

※ 4H EMA는 화면 표시만 하며 경고 필수조건이 아님<br>

※ 정배열 시작 이후 최초 고점 → 눌림 → 반등 구조<br>

※ 반등이 전고점을 넘지 못하면 반등 고점을 새로운 고점으로 갱신<br>

※ 이후 다시 눌림 → 반등 구조를 반복<br>

※ 현재 진행 중인 1H 캔들까지 판단에 활용

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
1H 60-120 정배열/역배열 + 전고점 재돌파
</div>

<div>
정배열 시작 → 최초 고점 → 눌림 → 반등
</div>

<div>
전고점 미돌파 시 반등 고점을 새로운 고점으로 갱신
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

    # =====================================================
    # 최초 즉시 조회
    # =====================================================

    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    # =====================================================
    # 주기 조회
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
