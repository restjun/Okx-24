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
# 기본 설정
# =========================================================

warnings.filterwarnings(
    "ignore",
    category=FutureWarning
)

app = FastAPI()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s"
)

logger = logging.getLogger("trading")


# =========================================================
# 사용자 설정
# =========================================================

VOLUME_HOURS = 24

TOP_N = 20

UPDATE_MINUTES = 1

INITIAL_CANDLE_COUNT = 200

HISTORY_CHUNK = 200

MAX_HISTORY_CHUNKS = 10

# 정배열 시작점 이후 구조를 확인할 최대 범위
BREAKOUT_LOOKBACK = 30

# 직전 고점/저점 접근 허용거리
PRE_BREAKOUT_DISTANCE = 0.005

# 스윙 판정
SWING_LEFT = 2
SWING_RIGHT = 2

# 최소 눌림/반등폭
MIN_CORRECTION_RATE = 0.003


# =========================================================
# 거래소 사용 여부
# =========================================================

USE_UPBIT = "Y"
USE_OKX = "N"


# =========================================================
# API 설정
# =========================================================

REQUEST_INTERVAL = 0.08

RATE_LIMIT_WAIT = 3

MAX_RETRIES = 10

OKX_RETRY_DELAY = 2

OKX_MAX_RETRY_ROUNDS = 3


# =========================================================
# 시간
# =========================================================

KST = ZoneInfo("Asia/Seoul")


def get_kst_time():

    return datetime.now(KST).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


# =========================================================
# 전역 데이터
# =========================================================

latest_okx_data = []

latest_upbit_data = []

latest_usdt_krw = 0.0

latest_upbit_update_time = "-"

latest_okx_update_time = "-"

latest_upbit_markets = []

request_lock = threading.Lock()

last_request_time = 0.0

update_lock = threading.Lock()


# =========================================================
# API 요청 간격
# =========================================================

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
# API 요청 이름
# =========================================================

def get_request_name(func):

    try:

        return getattr(
            func,
            "__name__",
            str(func)
        )

    except Exception:

        return str(func)


# =========================================================
# API 재시도
# =========================================================

def retry_request(func, *args, **kwargs):

    function_name = get_request_name(func)

    url = ""

    if args:

        try:

            if isinstance(args[0], str):

                url = args[0]

        except Exception:

            pass

    if not url:

        url = kwargs.get(
            "url",
            ""
        )

    for attempt in range(MAX_RETRIES):

        try:

            wait_request_interval()

            logging.info(
                f"[API 요청] "
                f"{function_name} "
                f"{url} "
                f"시도={attempt + 1}/{MAX_RETRIES}"
            )

            result = func(
                *args,
                **kwargs
            )

            if hasattr(
                result,
                "status_code"
            ):

                status = result.status_code

                if status == 200:

                    logging.info(
                        f"[API 성공] "
                        f"HTTP {status} "
                        f"{url}"
                    )

                    return result

                if status == 429:

                    wait_time = min(
                        RATE_LIMIT_WAIT *
                        (2 ** attempt),
                        60
                    )

                    logging.warning(
                        f"[API 429] "
                        f"{url} "
                        f"시도={attempt + 1}/{MAX_RETRIES} "
                        f"{wait_time}초 대기"
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
                        f"[API 서버 오류] "
                        f"HTTP {status} "
                        f"{url} "
                        f"{wait_time}초 대기"
                    )

                    time.sleep(
                        wait_time
                    )

                    continue

                logging.warning(
                    f"[API HTTP 오류] "
                    f"HTTP {status} "
                    f"{url}"
                )

                return result

            return result

        except Exception as e:

            wait_time = min(
                2 * (attempt + 1),
                20
            )

            logging.error(
                f"[API 예외] "
                f"{function_name} "
                f"{url} "
                f"시도={attempt + 1}/{MAX_RETRIES} "
                f"오류={e}"
            )

            if attempt < MAX_RETRIES - 1:

                time.sleep(
                    wait_time
                )

    logging.error(
        f"[API 최종 실패] "
        f"{function_name} "
        f"{url}"
    )

    return None


# =========================================================
# 업비트 USDT-KRW
# =========================================================

def get_usdt_krw():

    logging.info(
        "[업비트 API] USDT-KRW 조회 시작"
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
# OKX 15분봉
# 확정봉만 사용
# =========================================================

def get_okx_ohlcv(
    inst_id,
    bar="15m",
    limit=200,
    before=None
):

    limit = max(
        1,
        min(int(limit), 200)
    )

    url = (
        "https://www.okx.com/api/v5/"
        "market/candles"
    )

    params = {
        "instId": inst_id,
        "bar": bar,
        "limit": limit
    }

    if before is not None:

        params["before"] = str(before)

    response = retry_request(
        requests.get,
        url,
        params=params,
        timeout=15
    )

    if response is None:

        return None

    try:

        payload = response.json()

        data = payload.get(
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

        for col in [
            "ts",
            "o",
            "h",
            "l",
            "c",
            "vol",
            "volCcy",
            "volCcyQuote"
        ]:

            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

        df = df[
            df["confirm"].astype(str) == "1"
        ]

        if df.empty:

            return None

        return (
            df
            .sort_values("ts")
            .drop_duplicates("ts")
            .reset_index(drop=True)
        )

    except Exception as e:

        logging.error(
            f"OKX 15분봉 처리 오류 "
            f"{inst_id}: {e}"
        )

        return None


# =========================================================
# 업비트 15분봉
# 현재 진행 중인 봉 제외
# =========================================================

def get_upbit_ohlcv(
    market,
    unit=15,
    count=200,
    to=None
):

    count = max(
        1,
        min(int(count), 200)
    )

    url = (
        "https://api.upbit.com/v1/"
        "candles/minutes/"
        f"{unit}"
    )

    params = {
        "market": market,
        "count": count
    }

    if to is not None:

        params["to"] = to

    response = retry_request(
        requests.get,
        url,
        params=params,
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

        df["volume_krw"] = pd.to_numeric(
            df["candle_acc_trade_price"],
            errors="coerce"
        )

        df["datetime"] = pd.to_datetime(
            df["candle_date_time_kst"],
            errors="coerce"
        )

        df = df.dropna(
            subset=[
                "datetime",
                "o",
                "h",
                "l",
                "c"
            ]
        )

        if df.empty:

            return None

        now = datetime.now(KST)

        minute_block = (
            now.minute // 15
        ) * 15

        current_candle_start = now.replace(
            minute=minute_block,
            second=0,
            microsecond=0
        )

        current_candle_start_naive = (
            current_candle_start
            .replace(tzinfo=None)
        )

        df = df[
            df["datetime"]
            <
            current_candle_start_naive
        ]

        if df.empty:

            return None

        return (
            df
            .sort_values("datetime")
            .drop_duplicates("datetime")
            .reset_index(drop=True)
        )

    except Exception as e:

        logging.error(
            f"업비트 15분봉 처리 오류 "
            f"{market}: {e}"
        )

        return None


# =========================================================
# 업비트 일봉 변동률
# =========================================================

def get_upbit_daily_change(
    market
):

    url = (
        "https://api.upbit.com/v1/"
        "candles/days"
    )

    params = {
        "market": market,
        "count": 1
    }

    response = retry_request(
        requests.get,
        url,
        params=params,
        timeout=15
    )

    if response is None:

        return None

    try:

        data = response.json()

        if not data:

            return None

        change_rate = data[0].get(
            "change_rate"
        )

        if change_rate is None:

            return None

        return [
            round(
                float(change_rate) * 100,
                2
            )
        ]

    except Exception as e:

        logging.error(
            f"업비트 일봉 처리 오류 : {e}"
        )

        return None


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
        or df.empty
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
# EMA 30-60-120 방향
# =========================================================

def get_ema_30_60_120_direction(
    df
):

    if (
        df is None
        or len(df) < 120
    ):

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
# 방향 시계열
# =========================================================

def get_direction_series(df):

    if (
        df is None
        or len(df) < 120
    ):

        return []

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
        or ema60 is None
        or ema120 is None
    ):

        return []

    result = []

    for i in range(len(df)):

        a = ema30.iloc[i]
        b = ema60.iloc[i]
        c = ema120.iloc[i]

        if any(
            pd.isna(x)
            for x in [a, b, c]
        ):

            result.append("none")

        elif a > b > c:

            result.append("long")

        elif a < b < c:

            result.append("short")

        else:

            result.append("none")

    return result


# =========================================================
# 최초 정배열 시작점
# =========================================================

def find_first_alignment_start(df):

    directions = get_direction_series(df)

    if not directions:

        return None

    for i in range(
        120,
        len(directions)
    ):

        current = directions[i]

        previous = directions[i - 1]

        if (
            current == "long"
            and previous != "long"
        ):

            return {
                "direction": "long",
                "index": i
            }

        if (
            current == "short"
            and previous != "short"
        ):

            return {
                "direction": "short",
                "index": i
            }

    return None


# =========================================================
# 스윙 고점
# =========================================================

def find_swing_highs(
    df,
    start_index,
    end_index
):

    result = []

    start = max(
        start_index,
        SWING_LEFT
    )

    end = min(
        end_index,
        len(df) - SWING_RIGHT - 1
    )

    for i in range(
        start,
        end + 1
    ):

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

            if left.empty or right.empty:

                continue

            if (
                high >= left.max()
                and
                high >= right.max()
            ):

                result.append(
                    (i, high)
                )

        except Exception:

            continue

    return result


# =========================================================
# 스윙 저점
# =========================================================

def find_swing_lows(
    df,
    start_index,
    end_index
):

    result = []

    start = max(
        start_index,
        SWING_LEFT
    )

    end = min(
        end_index,
        len(df) - SWING_RIGHT - 1
    )

    for i in range(
        start,
        end + 1
    ):

        try:

            low = float(
                df["l"].iloc[i]
            )

            left = pd.to_numeric(
                df["l"].iloc[
                    i - SWING_LEFT:i
                ],
                errors="coerce"
            )

            right = pd.to_numeric(
                df["l"].iloc[
                    i + 1:
                    i + 1 + SWING_RIGHT
                ],
                errors="coerce"
            )

            if left.empty or right.empty:

                continue

            if (
                low <= left.min()
                and
                low <= right.min()
            ):

                result.append(
                    (i, low)
                )

        except Exception:

            continue

    return result


# =========================================================
# LONG 구조 추적
#
# 핵심 로직
#
# 정배열 시작점
#       ↓
# 최초 확정 최고점
#       ↓
# 눌림
#       ↓
# 반등 고점
#       ↓
# 기준 고점 돌파 실패
#       ↓
# 다시 눌림
#       ↓
# 다시 반등 고점
#       ↓
# 반복
#       ↓
# 기준 고점 돌파
#       ↓
# 🚀(1)
#
# "최근 N개 고점" 방식이 아님.
# 정배열 시작점 이후의 구조를 시간순으로 추적.
# =========================================================

def get_long_breakout_signal(
    df,
    alignment_start
):

    if alignment_start is None:

        return "none"

    if alignment_start.get(
        "direction"
    ) != "long":

        return "none"

    start = alignment_start.get(
        "index"
    )

    current_index = len(df) - 1

    if start is None:

        return "none"

    if start >= current_index - 5:

        return "none"

    # -----------------------------------------------------
    # 정배열 시작 이후 확정된 스윙 고점 목록
    # -----------------------------------------------------

    swing_highs = find_swing_highs(
        df,
        start + 1,
        current_index
    )

    if not swing_highs:

        return "none"

    # -----------------------------------------------------
    # 첫 번째 확정 고점을 최초 기준 고점으로 설정
    # -----------------------------------------------------

    first_high = None

    for index, value in swing_highs:

        if index > start:

            first_high = (
                index,
                value
            )

            break

    if first_high is None:

        return "none"

    reference_index = first_high[0]

    reference_high = first_high[1]

    # -----------------------------------------------------
    # 정배열 시작 → 최초 고점 이후 구조를 순차적으로 추적
    # -----------------------------------------------------

    correction_started = False

    correction_low = None

    failed_rebounds = []

    candidate_rebound = None

    breakout_index = None

    i = reference_index + 1

    while i <= current_index:

        try:

            high = float(
                df["h"].iloc[i]
            )

            low = float(
                df["l"].iloc[i]
            )

            close = float(
                df["c"].iloc[i]
            )

        except Exception:

            i += 1
            continue

        # =================================================
        # 1. 기준 고점보다 높은 가격이 나왔는지
        # =================================================

        if (
            close > reference_high
        ):

            breakout_index = i

            break

        # =================================================
        # 2. 기준 고점 아래에서 눌림 확인
        # =================================================

        if low < reference_high:

            if not correction_started:

                correction_started = True

                correction_low = low

            else:

                if (
                    correction_low is None
                    or low < correction_low
                ):

                    correction_low = low

        # =================================================
        # 3. 충분한 눌림이 발생했는지
        # =================================================

        if (
            correction_started
            and
            correction_low is not None
        ):

            correction_rate = (
                reference_high -
                correction_low
            ) / reference_high

            if (
                correction_rate
                >=
                MIN_CORRECTION_RATE
            ):

                # -----------------------------------------
                # 현재 시점까지 확정된 스윙 고점 확인
                # -----------------------------------------

                confirmed_highs = [
                    x
                    for x in swing_highs
                    if (
                        x[0] >
                        reference_index
                        and
                        x[0] <= i
                    )
                ]

                # -----------------------------------------
                # 반등 고점 확인
                # -----------------------------------------

                if confirmed_highs:

                    candidate_rebound = (
                        confirmed_highs[-1]
                    )

                    rebound_index = (
                        candidate_rebound[0]
                    )

                    rebound_high = (
                        candidate_rebound[1]
                    )

                    # -------------------------------------
                    # 반등 고점이 기존 기준 고점보다 낮으면
                    # 돌파 실패
                    # -------------------------------------

                    if (
                        rebound_index > reference_index
                        and
                        rebound_high < reference_high
                    ):

                        if not failed_rebounds:

                            failed_rebounds.append(
                                (
                                    rebound_index,
                                    rebound_high
                                )
                            )

                        elif (
                            failed_rebounds[-1][0]
                            !=
                            rebound_index
                        ):

                            failed_rebounds.append(
                                (
                                    rebound_index,
                                    rebound_high
                                )
                            )

                        # ---------------------------------
                        # 새로운 실패 고점 이후 다시 눌림
                        # ---------------------------------

                        correction_started = False

                        correction_low = None

                        candidate_rebound = None

                        # ---------------------------------
                        # 다음 구조를 계속 추적
                        # 기준 고점은 유지
                        # ---------------------------------

        # =================================================
        # 4. 캔들의 고가가 기준 고점을 넘었지만
        #    종가가 넘지 못했다면 돌파 실패 후보
        # =================================================

        if (
            high > reference_high
            and
            close <= reference_high
        ):

            # 아직 종가 돌파가 아니므로 실패 구조로 유지
            if not failed_rebounds:

                failed_rebounds.append(
                    (
                        i,
                        high
                    )
                )

            elif (
                failed_rebounds[-1][0] != i
            ):

                failed_rebounds.append(
                    (
                        i,
                        high
                    )
                )

        i += 1

    # =====================================================
    # 종가 기준 돌파가 없으면 신호 없음
    # =====================================================

    if breakout_index is None:

        return "none"

    # =====================================================
    # 돌파 기준봉
    # =====================================================

    breakout_open = float(
        df["o"].iloc[
            breakout_index
        ]
    )

    breakout_close = float(
        df["c"].iloc[
            breakout_index
        ]
    )

    breakout_low = float(
        df["l"].iloc[
            breakout_index
        ]
    )

    # 안전 확인
    if breakout_close <= breakout_open:

        return "none"

    if breakout_close <= reference_high:

        return "none"

    # =====================================================
    # 돌파 이후 카운팅
    # =====================================================

    count = (
        current_index
        -
        breakout_index
        +
        1
    )

    # =====================================================
    # 돌파 기준봉 저점 이탈
    #
    # 한 번이라도 이탈하면 구조 폐기
    # =====================================================

    after_section = df.iloc[
        breakout_index:
        current_index + 1
    ]

    lows_after = pd.to_numeric(
        after_section["l"],
        errors="coerce"
    )

    if lows_after.empty:

        return "none"

    if (
        lows_after.min()
        <
        breakout_low
    ):

        return "none"

    # =====================================================
    # 1~3까지만 표시
    # =====================================================

    if 1 <= count <= 3:

        return str(count)

    return "none"


# =========================================================
# SHORT 구조 추적
#
# 정배열 시작점
#       ↓
# 최초 확정 최저점
#       ↓
# 반등
#       ↓
# 반락 저점
#       ↓
# 기존 최저점 이탈 실패
#       ↓
# 반복
#       ↓
# 기준 저점 이탈
#       ↓
# 🚀(1)
# =========================================================

def get_short_breakout_signal(
    df,
    alignment_start
):

    if alignment_start is None:

        return "none"

    if alignment_start.get(
        "direction"
    ) != "short":

        return "none"

    start = alignment_start.get(
        "index"
    )

    current_index = len(df) - 1

    if start is None:

        return "none"

    if start >= current_index - 5:

        return "none"

    # -----------------------------------------------------
    # 정배열 시작 이후 스윙 저점
    # -----------------------------------------------------

    swing_lows = find_swing_lows(
        df,
        start + 1,
        current_index
    )

    if not swing_lows:

        return "none"

    # -----------------------------------------------------
    # 최초 확정 저점
    # -----------------------------------------------------

    first_low = None

    for index, value in swing_lows:

        if index > start:

            first_low = (
                index,
                value
            )

            break

    if first_low is None:

        return "none"

    reference_index = first_low[0]

    reference_low = first_low[1]

    correction_started = False

    correction_high = None

    failed_rebounds = []

    candidate_rebound = None

    breakout_index = None

    i = reference_index + 1

    while i <= current_index:

        try:

            high = float(
                df["h"].iloc[i]
            )

            low = float(
                df["l"].iloc[i]
            )

            close = float(
                df["c"].iloc[i]
            )

        except Exception:

            i += 1
            continue

        # =================================================
        # 1. 기준 저점 아래 종가 이탈
        # =================================================

        if (
            close < reference_low
        ):

            breakout_index = i

            break

        # =================================================
        # 2. 기준 저점 위 반등
        # =================================================

        if high > reference_low:

            if not correction_started:

                correction_started = True

                correction_high = high

            else:

                if (
                    correction_high is None
                    or high > correction_high
                ):

                    correction_high = high

        # =================================================
        # 3. 충분한 반등 확인
        # =================================================

        if (
            correction_started
            and
            correction_high is not None
        ):

            correction_rate = (
                correction_high -
                reference_low
            ) / reference_low

            if (
                correction_rate
                >=
                MIN_CORRECTION_RATE
            ):

                confirmed_lows = [
                    x
                    for x in swing_lows
                    if (
                        x[0] >
                        reference_index
                        and
                        x[0] <= i
                    )
                ]

                if confirmed_lows:

                    candidate_rebound = (
                        confirmed_lows[-1]
                    )

                    rebound_index = (
                        candidate_rebound[0]
                    )

                    rebound_low = (
                        candidate_rebound[1]
                    )

                    # -------------------------------------
                    # 새로운 반락 저점이 기존 기준 저점보다
                    # 높다면 이탈 실패
                    # -------------------------------------

                    if (
                        rebound_index > reference_index
                        and
                        rebound_low > reference_low
                    ):

                        if not failed_rebounds:

                            failed_rebounds.append(
                                (
                                    rebound_index,
                                    rebound_low
                                )
                            )

                        elif (
                            failed_rebounds[-1][0]
                            !=
                            rebound_index
                        ):

                            failed_rebounds.append(
                                (
                                    rebound_index,
                                    rebound_low
                                )
                            )

                        correction_started = False

                        correction_high = None

                        candidate_rebound = None

        # =================================================
        # 4. 저가가 기준 저점 밑으로 내려갔지만
        #    종가가 기준 저점 위라면 이탈 실패 후보
        # =================================================

        if (
            low < reference_low
            and
            close >= reference_low
        ):

            if not failed_rebounds:

                failed_rebounds.append(
                    (
                        i,
                        low
                    )
                )

            elif (
                failed_rebounds[-1][0] != i
            ):

                failed_rebounds.append(
                    (
                        i,
                        low
                    )
                )

        i += 1

    # =====================================================
    # 종가 기준 이탈이 없으면 신호 없음
    # =====================================================

    if breakout_index is None:

        return "none"

    breakout_open = float(
        df["o"].iloc[
            breakout_index
        ]
    )

    breakout_close = float(
        df["c"].iloc[
            breakout_index
        ]
    )

    breakout_high = float(
        df["h"].iloc[
            breakout_index
        ]
    )

    if breakout_close >= breakout_open:

        return "none"

    if breakout_close >= reference_low:

        return "none"

    # =====================================================
    # 돌파 이후 카운팅
    # =====================================================

    count = (
        current_index
        -
        breakout_index
        +
        1
    )

    # =====================================================
    # 돌파 기준봉 고점 돌파 시 무효
    # =====================================================

    after_section = df.iloc[
        breakout_index:
        current_index + 1
    ]

    highs_after = pd.to_numeric(
        after_section["h"],
        errors="coerce"
    )

    if highs_after.empty:

        return "none"

    if (
        highs_after.max()
        >
        breakout_high
    ):

        return "none"

    # =====================================================
    # 1~3까지만 표시
    # =====================================================

    if 1 <= count <= 3:

        return str(count)

    return "none"


# =========================================================
# 돌파 통합
# =========================================================

def get_breakout_signal(
    df,
    allow_short=True
):

    if (
        df is None
        or len(df) < 125
    ):

        return {
            "signal": "none",
            "direction": "none"
        }

    alignment = find_first_alignment_start(
        df
    )

    if alignment is None:

        return {
            "signal": "none",
            "direction": "none"
        }

    direction = alignment[
        "direction"
    ]

    if direction == "long":

        signal = get_long_breakout_signal(
            df,
            alignment
        )

        return {
            "signal": signal,
            "direction": "long"
        }

    if (
        direction == "short"
        and
        allow_short
    ):

        signal = get_short_breakout_signal(
            df,
            alignment
        )

        return {
            "signal": signal,
            "direction": "short"
        }

    return {
        "signal": "none",
        "direction": direction
    }


# =========================================================
# 변동률
# =========================================================

def calculate_daily_changes(
    df,
    is_okx=False
):

    if (
        df is None
        or df.empty
    ):

        return None

    try:

        temp = df.copy()

        if is_okx:

            temp["datetime"] = (
                pd.to_datetime(
                    temp["ts"],
                    unit="ms",
                    utc=True
                )
                .dt.tz_convert(
                    "Asia/Seoul"
                )
                .dt.tz_localize(None)
            )

        elif "datetime" not in temp.columns:

            return None

        temp["c"] = pd.to_numeric(
            temp["c"],
            errors="coerce"
        )

        temp = temp.dropna(
            subset=[
                "datetime",
                "c"
            ]
        )

        if temp.empty:

            return None

        temp = temp.set_index(
            "datetime"
        )

        daily = (
            temp["c"]
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

        start_index = max(
            1,
            len(daily) - 3
        )

        for i in range(
            start_index,
            len(daily)
        ):

            previous = daily.iloc[
                i - 1
            ]

            current = daily.iloc[
                i
            ]

            if previous == 0:

                continue

            change = (
                (
                    current -
                    previous
                )
                /
                previous
                *
                100
            )

            result.append(
                round(
                    float(change),
                    2
                )
            )

        return result[::-1]

    except Exception as e:

        logging.error(
            f"변동률 계산 오류 : {e}"
        )

        return None


# =========================================================
# EMA 표시
# =========================================================

def check_ema(df):

    direction = get_ema_30_60_120_direction(
        df
    )

    if direction == "long":

        return {
            "display": "🟢 LONG",
            "direction": "long"
        }

    if direction == "short":

        return {
            "display": "🔴 SHORT",
            "direction": "short"
        }

    return {
        "display": "⚪",
        "direction": "none"
    }


# =========================================================
# 거래대금 표시
# =========================================================

def format_volume(volume):

    if volume is None:

        return "-"

    try:

        volume = float(volume)

    except Exception:

        return "-"

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
# 변동률 HTML
# =========================================================

def format_change(changes):

    if (
        changes is None
        or len(changes) == 0
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
# 표시 여부
# =========================================================

def is_visible_warning(
    warning
):

    if not warning:

        return False

    return warning.get(
        "signal",
        "none"
    ) in (
        "1",
        "2",
        "3"
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
        "signal",
        "none"
    )

    if signal in (
        "1",
        "2",
        "3"
    ):

        return (
            '<span class="warning-rocket">'
            f'🚀({signal})'
            '</span>'
        )

    return ""


# =========================================================
# 방향 HTML
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

    return (
        '<span class="direction-none">'
        '-'
        '</span>'
    )


# =========================================================
# 업비트 마켓 + 24시간 거래대금
# =========================================================

def get_upbit_markets():

    global latest_upbit_markets

    logging.info(
        "[업비트 API] "
        "KRW 마켓 + 24시간 거래대금 조회 시작"
    )

    url = (
        "https://api.upbit.com/v1/ticker/all"
    )

    params = {
        "quote_currencies": "KRW"
    }

    response = retry_request(
        requests.get,
        url,
        params=params,
        timeout=15
    )

    if response is None:

        return []

    try:

        data = response.json()

        if not data:

            return []

        markets = []

        for item in data:

            market = item.get(
                "market",
                ""
            )

            if not market.startswith(
                "KRW-"
            ):

                continue

            volume_24h = item.get(
                "acc_trade_price_24h"
            )

            try:

                volume_24h = float(
                    volume_24h
                )

            except Exception:

                continue

            if volume_24h <= 0:

                continue

            markets.append(
                {
                    "market": market,
                    "volume_24h": volume_24h
                }
            )

        latest_upbit_markets = [
            item["market"]
            for item in markets
        ]

        return markets

    except Exception as e:

        logging.error(
            f"업비트 마켓 처리 오류 : {e}"
        )

        return []


# =========================================================
# OKX 목록
# =========================================================

def get_all_okx_swap_symbols():

    response = retry_request(
        requests.get,
        "https://www.okx.com/api/v5/"
        "public/instruments",
        params={
            "instType": "SWAP"
        },
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
                x.get(
                    "instId",
                    ""
                ).endswith(
                    "-USDT-SWAP"
                )
                and
                x.get("state") == "live"
            )
        ]

    except Exception as e:

        logging.error(
            f"OKX 목록 처리 오류 : {e}"
        )

        return []


# =========================================================
# OKX 거래대금
# =========================================================

def get_okx_volume(
    inst_id,
    usdt_krw
):

    df = get_okx_ohlcv(
        inst_id,
        "1H",
        24
    )

    if (
        df is None
        or df.empty
    ):

        return None

    try:

        volume = pd.to_numeric(
            df["volCcyQuote"],
            errors="coerce"
        ).sum()

        if pd.isna(volume):

            return None

        return (
            float(volume)
            *
            float(usdt_krw)
        )

    except Exception:

        return None


# =========================================================
# 업비트 분석
# =========================================================

def get_upbit_analysis(
    market
):

    df = get_upbit_history(
        market
    )

    if (
        df is None
        or len(df) < 125
    ):

        return None

    ema = check_ema(
        df
    )

    warning = get_breakout_signal(
        df,
        allow_short=False
    )

    changes = get_upbit_daily_change(
        market
    )

    if (
        changes is None
        or len(changes) == 0
    ):

        return None

    return {
        "ema": ema,
        "warning": warning,
        "changes": changes
    }


# =========================================================
# OKX 분석
# =========================================================

def get_okx_analysis(
    inst_id
):

    df = get_okx_history(
        inst_id,
        "15m"
    )

    if (
        df is None
        or len(df) < 125
    ):

        return None

    ema = check_ema(
        df
    )

    warning = get_breakout_signal(
        df,
        allow_short=True
    )

    changes = calculate_daily_changes(
        df,
        True
    )

    return {
        "ema": ema,
        "warning": warning,
        "changes": changes
    }


# =========================================================
# LONG 필터
# =========================================================

def pass_long_filter(
    analysis
):

    if analysis is None:

        return False

    ema = analysis.get(
        "ema",
        {}
    )

    warning = analysis.get(
        "warning",
        {}
    )

    changes = analysis.get(
        "changes"
    )

    if ema.get(
        "direction"
    ) != "long":

        return False

    if warning.get(
        "direction"
    ) != "long":

        return False

    if not changes:

        return False

    try:

        today_change = float(
            changes[0]
        )

    except Exception:

        return False

    if today_change <= 0:

        return False

    return True


# =========================================================
# SHORT 필터
# =========================================================

def pass_short_filter(
    analysis
):

    if analysis is None:

        return False

    ema = analysis.get(
        "ema",
        {}
    )

    warning = analysis.get(
        "warning",
        {}
    )

    changes = analysis.get(
        "changes"
    )

    if ema.get(
        "direction"
    ) != "short":

        return False

    if warning.get(
        "direction"
    ) != "short":

        return False

    if not changes:

        return False

    try:

        today_change = float(
            changes[0]
        )

    except Exception:

        return False

    if today_change >= 0:

        return False

    return True


# =========================================================
# 업비트 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update_time

    start_time = get_kst_time()

    logging.info(
        f"========== 업비트 TOP{TOP_N} 시작 "
        f"{start_time} KST =========="
    )

    market_data = get_upbit_markets()

    if not market_data:

        return False

    market_data = sorted(
        market_data,
        key=lambda x: x["volume_24h"],
        reverse=True
    )

    top_markets = market_data[
        :TOP_N
    ]

    volume_map = {
        item["market"]: item["volume_24h"]
        for item in top_markets
    }

    rows = []

    for rank, item in enumerate(
        top_markets,
        start=1
    ):

        market = item["market"]

        coin = market.replace(
            "KRW-",
            ""
        )

        logging.info(
            f"[업비트 15분 분석] "
            f"{rank}/{len(top_markets)} "
            f"{market}"
        )

        try:

            analysis = get_upbit_analysis(
                market
            )

            if analysis is None:

                continue

            warning = analysis[
                "warning"
            ]

            if not is_visible_warning(
                warning
            ):

                continue

            if not pass_long_filter(
                analysis
            ):

                continue

            rows.append(
                {
                    "rank": rank,
                    "name": coin,
                    "change": format_change(
                        analysis["changes"]
                    ),
                    "volume": format_volume(
                        volume_map[market]
                    ),
                    "ema": analysis["ema"],
                    "direction": "long",
                    "warning": warning
                }
            )

        except Exception as e:

            logging.error(
                f"업비트 상세 오류 "
                f"{market}: {e}"
            )

    latest_upbit_data = rows

    latest_upbit_update_time = (
        get_kst_time()
    )

    logging.info(
        f"업비트 LONG 돌파 "
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
    global latest_okx_update_time

    if (
        usdt_krw is None
        or usdt_krw <= 0
    ):

        return False

    symbols = get_all_okx_swap_symbols()

    if not symbols:

        return False

    upbit_coin_set = {
        market.replace(
            "KRW-",
            ""
        )
        for market in latest_upbit_markets
    }

    volume_map = {}

    for index, symbol in enumerate(
        symbols,
        start=1
    ):

        logging.info(
            f"[OKX 거래대금 진행] "
            f"{index}/{len(symbols)} "
            f"{symbol}"
        )

        volume = get_okx_volume(
            symbol,
            usdt_krw
        )

        if (
            volume is not None
            and volume > 0
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

        display_coin = coin

        if coin in upbit_coin_set:

            display_coin = (
                f"{coin}[UP]"
            )

        try:

            analysis = get_okx_analysis(
                symbol
            )

            if analysis is None:

                continue

            warning = analysis[
                "warning"
            ]

            if not is_visible_warning(
                warning
            ):

                continue

            direction = warning.get(
                "direction",
                "none"
            )

            if direction == "long":

                if not pass_long_filter(
                    analysis
                ):

                    continue

            elif direction == "short":

                if not pass_short_filter(
                    analysis
                ):

                    continue

            else:

                continue

            rows.append(
                {
                    "rank": rank,
                    "name": display_coin,
                    "change": format_change(
                        analysis["changes"]
                    ),
                    "volume": format_volume(
                        volume_map[symbol]
                    ),
                    "ema": analysis["ema"],
                    "direction": direction,
                    "warning": warning
                }
            )

        except Exception as e:

            logging.error(
                f"OKX 상세 오류 "
                f"{symbol}: {e}"
            )

    latest_okx_data = rows

    latest_okx_update_time = (
        get_kst_time()
    )

    return True


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    global latest_usdt_krw
    global latest_upbit_data
    global latest_okx_data

    if not update_lock.acquire(
        blocking=False
    ):

        logging.warning(
            "이전 전체 조회가 진행 중입니다."
        )

        return

    try:

        logging.info(
            "========================================"
        )

        logging.info(
            f"전체 조회 시작 "
            f"{get_kst_time()} KST"
        )

        # =================================================
        # 업비트
        # =================================================

        if USE_UPBIT == "Y":

            try:

                update_upbit()

            except Exception as e:

                logging.exception(
                    f"업비트 업데이트 오류 : {e}"
                )

        else:

            latest_upbit_data = []

            latest_upbit_markets = []

        # =================================================
        # OKX
        # =================================================

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
            f"전체 조회 종료 "
            f"{get_kst_time()} KST"
        )

    finally:

        update_lock.release()


# =========================================================
# 스케줄러
# =========================================================

def scheduler():

    logging.info(
        "스케줄러 스레드 시작"
    )

    while True:

        try:

            schedule.run_pending()

        except Exception as e:

            logging.exception(
                f"스케줄러 오류 : {e}"
            )

        time.sleep(1)


# =========================================================
# HTML CSS
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
    width: 19%;
}

th:nth-child(3),
td:nth-child(3) {
    width: 22%;
}

th:nth-child(4),
td:nth-child(4) {
    width: 25%;
}

th:nth-child(5),
td:nth-child(5) {
    width: 27%;
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

.direction-long {
    display: block;
    color: #35e66d;
    font-size: 7px;
    font-weight: 800;
    margin-top: 2px;
}

.direction-short {
    display: block;
    color: #ff4d4d;
    font-size: 7px;
    font-weight: 800;
    margin-top: 2px;
}

.direction-none {
    display: block;
    color: #666;
    font-size: 7px;
}

.breakout-warning {
    display: flex;
    justify-content: center;
    align-items: center;
    width: 100%;
    min-height: 14px;
    white-space: nowrap;
}

.warning-rocket {
    font-size: 10px;
    font-weight: bold;
    filter: drop-shadow(
        0 0 4px
        rgba(50, 255, 100, 0.9)
    );
}

.ema-value {
    width: 100%;
    font-size: 8px;
    font-weight: bold;
    line-height: 1.5;
    white-space: nowrap;
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

    .direction-long,
    .direction-short {
        font-size: 6px;
    }

    .warning-rocket {
        font-size: 9px;
    }

    .ema-value {
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

        warning_text = combined_warning_html(
            item.get(
                "warning",
                {}
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

{direction_html(
    item.get(
        "direction",
        "none"
    )
)}

</td>

<td>

<div class="today-wrap">

<div>
{item.get("change", "")}
</div>

<div class="breakout-warning">
{warning_text}
</div>

</div>

</td>

<td>

<div class="ema-value">
{item.get("ema", {}).get(
    "display",
    "⚪"
)}
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
    data,
    is_okx=False
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
현재 🚀(1~3) 종목 없음
</td>
</tr>
"""

    if is_okx:

        direction_note = (
            "※ OKX = LONG / SHORT 모두 표시<br>"
        )

        change_note = (
            "※ 변동률 = OKX 15분봉을 이용해 한국시간 09:00 기준 계산<br>"
        )

        update_time = (
            latest_okx_update_time
        )

    else:

        direction_note = (
            "※ 업비트 = LONG만 표시<br>"
        )

        change_note = (
            "※ 변동률 = 업비트 일봉 API의 change_rate 사용<br>"
        )

        update_time = (
            latest_upbit_update_time
        )

    return f"""
<div class="section">

<h2>
🏆 {title} TOP{TOP_N} 돌파
<span style="
    color:#777;
    font-size:7px;
    font-weight:normal;
">
&nbsp;조회 {update_time} KST
</span>
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

※ TOP{TOP_N} 거래대금 순위<br>
※ 업비트 거래대금 = 24시간 누적 거래대금<br>
{direction_note}
{change_note}
※ EMA = 15분봉 30-60-120<br>
※ LONG = 15분 EMA 30 > 60 > 120 + 당일 변동률 양수<br>
※ SHORT = 15분 EMA 30 < 60 < 120 + 당일 변동률 음수<br>
※ 현재 진행 중인 15분봉 제외<br>
※ 정배열 시작점부터 고점/저점 구조를 순차적으로 추적<br>
※ 최초 확정 고점/저점을 기준점으로 설정<br>
※ 눌림/반등 후 돌파 실패 구조를 반복 확인<br>
※ LONG = 이전 기준 고점 종가 돌파<br>
※ SHORT = 이전 기준 저점 종가 이탈<br>
※ 🚀(1) = 돌파 첫 확정봉<br>
※ 🚀(2) = 돌파 후 두 번째 확정봉<br>
※ 🚀(3) = 돌파 후 세 번째 확정봉<br>
※ 🚀는 돌파 후 3개 확정봉까지만 표시<br>
※ 돌파 직전 🚨는 표시하지 않음<br>
※ 돌파 직후 〽️는 표시하지 않음<br>
※ LONG 돌파 기준봉 저점 이탈 시 신호 제거<br>
※ SHORT 돌파 기준봉 고점 돌파 시 신호 제거<br>
※ 1시간봉 조건 사용하지 않음

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
                "업비트",
                latest_upbit_data,
                False
            )
        )

    if USE_OKX == "Y":

        exchange_sections += (
            make_exchange_section(
                "OKX",
                latest_okx_data,
                True
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
15M Breakout Trading
</title>

<style>

{DASHBOARD_CSS}

</style>

</head>

<body>

<h1>
📊 15M Breakout Trading
</h1>

<div class="info">

<div>
15분봉 30-60-120 정배열 / 역배열
</div>

<div>
정배열 시작점 → 최초 최고점/최저점
→ 눌림/반등 → 돌파 실패 반복
→ 이전 기준점 돌파
</div>

<div>
🚀(1) 첫 돌파 · 🚀(2) 두 번째 봉 · 🚀(3) 세 번째 봉
</div>

<div>
돌파 직전 🚨 및 돌파 직후 〽️ 미표시
</div>

<div>
LONG = 15분 EMA 정배열 + 당일 변동률 양수
</div>

<div>
SHORT = 15분 EMA 역배열 + 당일 변동률 음수
</div>

<div>
확정 15분봉 기준
</div>

<div>
업비트 거래대금 = 24시간 누적 거래대금
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

@app.on_event("startup")
def startup():

    logging.info(
        "========================================"
    )

    logging.info(
        "서버 시작"
    )

    logging.info(
        f"업비트={USE_UPBIT} "
        f"OKX={USE_OKX}"
    )

    logging.info(
        f"TOP={TOP_N}"
    )

    logging.info(
        f"UPDATE={UPDATE_MINUTES}분"
    )

    logging.info(
        "기준 : 15분 확정봉"
    )

    logging.info(
        "EMA : 15분 30-60-120"
    )

    logging.info(
        "돌파 구조 : 정배열 시작점부터 순차 추적"
    )

    logging.info(
        "최초 확정 최고점/최저점을 기준점으로 사용"
    )

    logging.info(
        "돌파 실패 구조 반복 확인"
    )

    logging.info(
        "🚨 돌파 직전 표시 안 함"
    )

    logging.info(
        "🚀 돌파 후 1~3까지만 표시"
    )

    logging.info(
        "〽️ 돌파 직후 표시 안 함"
    )

    logging.info(
        "LONG : 기준 고점 종가 돌파"
    )

    logging.info(
        "SHORT : 기준 저점 종가 이탈"
    )

    logging.info(
        "LONG : 돌파 기준봉 저점 이탈 시 제거"
    )

    logging.info(
        "SHORT : 돌파 기준봉 고점 돌파 시 제거"
    )

    logging.info(
        "1H 조건 : 사용 안 함"
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

    if TOP_N <= 0:

        raise ValueError(
            "TOP_N은 1 이상이어야 합니다."
        )

    if UPDATE_MINUTES <= 0:

        raise ValueError(
            "UPDATE_MINUTES는 1 이상이어야 합니다."
        )

    # -----------------------------------------------------
    # 최초 즉시 조회
    # -----------------------------------------------------

    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    # -----------------------------------------------------
    # 주기 등록
    # -----------------------------------------------------

    schedule.every(
        UPDATE_MINUTES
    ).minutes.do(
        update_dashboard
    )

    # -----------------------------------------------------
    # 스케줄러
    # -----------------------------------------------------

    threading.Thread(
        target=scheduler,
        daemon=True
    ).start()

    logging.info(
        "서버 시작 완료"
    )


# =========================================================
# 실행
# =========================================================

if __name__ == "__main__":

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
                    )
