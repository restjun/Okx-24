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


# =========================================================
# N자 구조 설정
# =========================================================

BREAKOUT_LOOKBACK = 30

# 돌파 직전 경고 거리
# LONG  : 이전 고점의 0.5% 이내 접근
# SHORT : 이전 저점의 0.5% 이내 접근
PRE_BREAKOUT_DISTANCE = 0.005

SWING_LEFT = 2

SWING_RIGHT = 2

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


# =========================================================
# 업비트 마켓 목록 캐시
# =========================================================

latest_upbit_markets = []


# =========================================================
# API 요청 동시 제어
# =========================================================

request_lock = threading.Lock()

last_request_time = 0.0


# =========================================================
# 전체 업데이트 중복 실행 방지
# =========================================================

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
                        RATE_LIMIT_WAIT * (2 ** attempt),
                        60
                    )

                    logging.warning(
                        f"[API 429] "
                        f"{url} "
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
            f"[업비트 USDT-KRW 오류] {e}"
        )

        return None


# =========================================================
# OKX OHLCV
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

        numeric_columns = [
            "ts",
            "o",
            "h",
            "l",
            "c",
            "vol",
            "volCcy",
            "volCcyQuote"
        ]

        for col in numeric_columns:

            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

        df = df[
            df["confirm"].astype(str) == "1"
        ]

        if df.empty:

            return None

        df = (
            df
            .sort_values("ts")
            .drop_duplicates("ts")
            .reset_index(drop=True)
        )

        return df

    except Exception as e:

        logging.error(
            f"OKX {bar} 처리 오류 "
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

        df = (
            df
            .sort_values("datetime")
            .drop_duplicates("datetime")
            .reset_index(drop=True)
        )

        return df

    except Exception as e:

        logging.error(
            f"업비트 {unit}분봉 처리 오류 "
            f"{market}: {e}"
        )

        return None


# =========================================================
# 업비트 4시간봉
# 현재 진행 중인 봉 제외
# =========================================================

def get_upbit_4h_ohlcv(
    market,
    count=200,
    to=None
):

    count = max(
        1,
        min(int(count), 200)
    )

    url = (
        "https://api.upbit.com/v1/"
        "candles/minutes/240"
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

        hour_block = (
            now.hour // 4
        ) * 4

        current_candle_start = now.replace(
            hour=hour_block,
            minute=0,
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

        df = (
            df
            .sort_values("datetime")
            .drop_duplicates("datetime")
            .reset_index(drop=True)
        )

        return df

    except Exception as e:

        logging.error(
            f"업비트 4시간봉 처리 오류 "
            f"{market}: {e}"
        )

        return None


# =========================================================
# 업비트 일봉 변동률
# =========================================================

def get_upbit_daily_change(market):

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
            f"[업비트 일봉] "
            f"{market} 처리 오류 : {e}"
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

def get_ema_30_60_120_direction(df):

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
# 정배열/역배열 시작점
# =========================================================

def find_latest_alignment_start(
    df,
    direction
):

    directions = get_direction_series(
        df
    )

    if not directions:

        return None

    latest = None

    for i in range(
        120,
        len(directions)
    ):

        if (
            directions[i] == direction
            and
            directions[i - 1] != direction
        ):

            latest = {
                "direction": direction,
                "index": i
            }

    return latest


# =========================================================
# 정배열 유지 여부
# =========================================================

def alignment_is_valid(
    directions,
    start_index,
    end_index,
    direction
):

    if (
        start_index < 0
        or end_index >= len(directions)
    ):

        return False

    for i in range(
        start_index,
        end_index + 1
    ):

        if directions[i] != direction:

            return False

    return True


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

            if (
                left.empty
                or right.empty
            ):

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

            if (
                left.empty
                or right.empty
            ):

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
# LONG N자 구조
#
# 반환 상태
# none        : 구조 없음
# prebreakout : 돌파 직전
# breakout    : 돌파 확정
# =========================================================

def find_long_breakout(
    df,
    alignment_start
):

    result = {
        "status": "none",
        "direction": "long",
        "breakout_index": None,
        "breakout_level": None,
        "warning_index": None
    }

    if alignment_start is None:

        return result

    if alignment_start.get(
        "direction"
    ) != "long":

        return result

    if (
        df is None
        or len(df) < 125
    ):

        return result

    directions = get_direction_series(
        df
    )

    if not directions:

        return result

    start = alignment_start["index"]

    current_index = len(df) - 1

    if start >= current_index:

        return result

    if not alignment_is_valid(
        directions,
        start,
        current_index,
        "long"
    ):

        return result

    swing_highs = find_swing_highs(
        df,
        start + SWING_RIGHT + 1,
        current_index
    )

    if not swing_highs:

        return result

    # 가장 최근 유효 고점부터 탐색
    for anchor_pos in range(
        len(swing_highs) - 1,
        -1,
        -1
    ):

        anchor_index, anchor_high = (
            swing_highs[anchor_pos]
        )

        if anchor_index >= current_index:

            continue

        correction_start = (
            anchor_index + 1
        )

        correction_end = current_index

        if correction_start > correction_end:

            continue

        correction_lows = pd.to_numeric(
            df["l"].iloc[
                correction_start:
                correction_end + 1
            ],
            errors="coerce"
        )

        if correction_lows.empty:

            continue

        correction_low = correction_lows.min()

        if pd.isna(correction_low):

            continue

        correction_rate = (
            anchor_high -
            float(correction_low)
        ) / anchor_high

        if correction_rate < MIN_CORRECTION_RATE:

            continue

        search_start = anchor_index + 1

        attempt_high = None
        attempt_high_index = None

        breakout_index = None

        # ---------------------------------------------
        # N자 구조 추적
        # ---------------------------------------------

        for i in range(
            search_start,
            current_index + 1
        ):

            if directions[i] != "long":

                breakout_index = None

                break

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

                continue

            # -----------------------------------------
            # 이전 고점 종가 돌파
            # -----------------------------------------

            if close > anchor_high:

                breakout_index = i

                break

            # -----------------------------------------
            # 조정 저점 갱신
            # -----------------------------------------

            if low < float(correction_low):

                correction_low = low

            # -----------------------------------------
            # 반등 고점 탐색
            # -----------------------------------------

            if i >= SWING_RIGHT:

                left_start = max(
                    search_start,
                    i - SWING_LEFT
                )

                left_values = pd.to_numeric(
                    df["h"].iloc[
                        left_start:i
                    ],
                    errors="coerce"
                )

                if not left_values.empty:

                    if high >= left_values.max():

                        if high < anchor_high:

                            attempt_high = high

                            attempt_high_index = i

            # -----------------------------------------
            # 반등 후 재조정
            # -----------------------------------------

            if (
                attempt_high is not None
                and
                attempt_high_index is not None
                and
                i > attempt_high_index
            ):

                decline_rate = (
                    attempt_high - low
                ) / attempt_high

                if decline_rate >= MIN_CORRECTION_RATE:

                    correction_low = low

                    attempt_high = None

                    attempt_high_index = None

        # ---------------------------------------------
        # 돌파 발생
        # ---------------------------------------------

        if breakout_index is not None:

            result["status"] = "breakout"

            result["breakout_index"] = (
                breakout_index
            )

            result["breakout_level"] = (
                anchor_high
            )

            return result

        # ---------------------------------------------
        # 돌파 직전 판단
        #
        # 최신 확정봉의 고가가
        # 이전 고점의 0.5% 이내까지 접근
        # 하지만 아직 종가 돌파하지 않은 경우
        # ---------------------------------------------

        try:

            latest_high = float(
                df["h"].iloc[current_index]
            )

            latest_close = float(
                df["c"].iloc[current_index]
            )

        except Exception:

            continue

        pre_breakout_level = (
            anchor_high *
            (1 - PRE_BREAKOUT_DISTANCE)
        )

        if (
            latest_high >= pre_breakout_level
            and
            latest_close <= anchor_high
        ):

            result["status"] = "prebreakout"

            result["warning_index"] = (
                current_index
            )

            result["breakout_level"] = (
                anchor_high
            )

            return result

    return result


# =========================================================
# SHORT N자 구조
# =========================================================

def find_short_breakout(
    df,
    alignment_start
):

    result = {
        "status": "none",
        "direction": "short",
        "breakout_index": None,
        "breakout_level": None,
        "warning_index": None
    }

    if alignment_start is None:

        return result

    if alignment_start.get(
        "direction"
    ) != "short":

        return result

    if (
        df is None
        or len(df) < 125
    ):

        return result

    directions = get_direction_series(
        df
    )

    if not directions:

        return result

    start = alignment_start["index"]

    current_index = len(df) - 1

    if start >= current_index:

        return result

    if not alignment_is_valid(
        directions,
        start,
        current_index,
        "short"
    ):

        return result

    swing_lows = find_swing_lows(
        df,
        start + SWING_RIGHT + 1,
        current_index
    )

    if not swing_lows:

        return result

    for anchor_pos in range(
        len(swing_lows) - 1,
        -1,
        -1
    ):

        anchor_index, anchor_low = (
            swing_lows[anchor_pos]
        )

        if anchor_index >= current_index:

            continue

        correction_start = (
            anchor_index + 1
        )

        correction_end = current_index

        if correction_start > correction_end:

            continue

        correction_highs = pd.to_numeric(
            df["h"].iloc[
                correction_start:
                correction_end + 1
            ],
            errors="coerce"
        )

        if correction_highs.empty:

            continue

        correction_high = correction_highs.max()

        if pd.isna(correction_high):

            continue

        correction_rate = (
            float(correction_high) -
            anchor_low
        ) / anchor_low

        if correction_rate < MIN_CORRECTION_RATE:

            continue

        search_start = anchor_index + 1

        attempt_low = None
        attempt_low_index = None

        breakout_index = None

        for i in range(
            search_start,
            current_index + 1
        ):

            if directions[i] != "short":

                breakout_index = None

                break

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

                continue

            # -----------------------------------------
            # 이전 저점 종가 이탈
            # -----------------------------------------

            if close < anchor_low:

                breakout_index = i

                break

            # -----------------------------------------
            # 반등 고점 갱신
            # -----------------------------------------

            if high > float(correction_high):

                correction_high = high

            # -----------------------------------------
            # 재하락 저점 탐색
            # -----------------------------------------

            if i >= SWING_RIGHT:

                left_start = max(
                    search_start,
                    i - SWING_LEFT
                )

                left_values = pd.to_numeric(
                    df["l"].iloc[
                        left_start:i
                    ],
                    errors="coerce"
                )

                if not left_values.empty:

                    if low <= left_values.min():

                        if low > anchor_low:

                            attempt_low = low

                            attempt_low_index = i

            # -----------------------------------------
            # 재하락 후 반등
            # -----------------------------------------

            if (
                attempt_low is not None
                and
                attempt_low_index is not None
                and
                i > attempt_low_index
            ):

                rise_rate = (
                    high - attempt_low
                ) / attempt_low

                if rise_rate >= MIN_CORRECTION_RATE:

                    correction_high = high

                    attempt_low = None

                    attempt_low_index = None

        # ---------------------------------------------
        # 돌파 발생
        # ---------------------------------------------

        if breakout_index is not None:

            result["status"] = "breakout"

            result["breakout_index"] = (
                breakout_index
            )

            result["breakout_level"] = (
                anchor_low
            )

            return result

        # ---------------------------------------------
        # 돌파 직전 판단
        # ---------------------------------------------

        try:

            latest_low = float(
                df["l"].iloc[current_index]
            )

            latest_close = float(
                df["c"].iloc[current_index]
            )

        except Exception:

            continue

        pre_breakout_level = (
            anchor_low *
            (1 + PRE_BREAKOUT_DISTANCE)
        )

        if (
            latest_low <= pre_breakout_level
            and
            latest_close >= anchor_low
        ):

            result["status"] = "prebreakout"

            result["warning_index"] = (
                current_index
            )

            result["breakout_level"] = (
                anchor_low
            )

            return result

    return result


# =========================================================
# 돌파 ID
# =========================================================

def make_breakout_id(
    exchange,
    symbol,
    df,
    index
):

    if (
        df is None
        or index is None
        or index < 0
        or index >= len(df)
    ):

        return None

    try:

        if "ts" in df.columns:

            candle_id = int(
                df["ts"].iloc[index]
            )

        elif "datetime" in df.columns:

            candle_id = str(
                df["datetime"].iloc[index]
            )

        else:

            candle_id = int(index)

        return (
            f"{exchange}:"
            f"{symbol}:"
            f"{candle_id}"
        )

    except Exception:

        return None


# =========================================================
# 돌파 후 카운팅
# =========================================================

def get_breakout_count(
    df,
    breakout_index
):

    if breakout_index is None:

        return None

    current_index = len(df) - 1

    count = (
        current_index -
        breakout_index +
        1
    )

    if count < 1:

        return None

    return count


# =========================================================
# 돌파 통합
#
# signal
# prebreakout = 🚨
# 1~3         = 🚀(1~3)
# 4 이상      = 내부 상태만 유지
# none        = 표시 없음
#
# 기존 invalid / 해지 기능 완전 삭제
# =========================================================

def get_breakout_signal(
    df,
    exchange,
    symbol,
    allow_short=True
):

    empty_result = {
        "signal": "none",
        "direction": "none",
        "breakout_id": None,
        "breakout_index": None,
        "warning_index": None
    }

    if (
        df is None
        or len(df) < 125
    ):

        return empty_result

    directions = get_direction_series(
        df
    )

    if not directions:

        return empty_result

    current_direction = directions[-1]

    # =====================================================
    # LONG
    # =====================================================

    if current_direction == "long":

        alignment = find_latest_alignment_start(
            df,
            "long"
        )

        result = find_long_breakout(
            df,
            alignment
        )

        status = result.get(
            "status",
            "none"
        )

        # ---------------------------------------------
        # 돌파 직전
        # ---------------------------------------------

        if status == "prebreakout":

            warning_index = result.get(
                "warning_index"
            )

            warning_id = make_breakout_id(
                exchange,
                symbol,
                df,
                warning_index
            )

            return {
                "signal": "prebreakout",
                "direction": "long",
                "breakout_id": warning_id,
                "breakout_index": None,
                "warning_index": warning_index
            }

        # ---------------------------------------------
        # 돌파
        # ---------------------------------------------

        breakout_index = result.get(
            "breakout_index"
        )

        if breakout_index is None:

            return {
                "signal": "none",
                "direction": "long",
                "breakout_id": None,
                "breakout_index": None,
                "warning_index": None
            }

        breakout_id = make_breakout_id(
            exchange,
            symbol,
            df,
            breakout_index
        )

        if breakout_id is None:

            return empty_result

        count = get_breakout_count(
            df,
            breakout_index
        )

        if count is None:

            return empty_result

        return {
            "signal": str(count),
            "direction": "long",
            "breakout_id": breakout_id,
            "breakout_index": breakout_index,
            "warning_index": None
        }

    # =====================================================
    # SHORT
    # =====================================================

    if (
        current_direction == "short"
        and
        allow_short
    ):

        alignment = find_latest_alignment_start(
            df,
            "short"
        )

        result = find_short_breakout(
            df,
            alignment
        )

        status = result.get(
            "status",
            "none"
        )

        # ---------------------------------------------
        # 돌파 직전
        # ---------------------------------------------

        if status == "prebreakout":

            warning_index = result.get(
                "warning_index"
            )

            warning_id = make_breakout_id(
                exchange,
                symbol,
                df,
                warning_index
            )

            return {
                "signal": "prebreakout",
                "direction": "short",
                "breakout_id": warning_id,
                "breakout_index": None,
                "warning_index": warning_index
            }

        # ---------------------------------------------
        # 돌파
        # ---------------------------------------------

        breakout_index = result.get(
            "breakout_index"
        )

        if breakout_index is None:

            return {
                "signal": "none",
                "direction": "short",
                "breakout_id": None,
                "breakout_index": None,
                "warning_index": None
            }

        breakout_id = make_breakout_id(
            exchange,
            symbol,
            df,
            breakout_index
        )

        if breakout_id is None:

            return empty_result

        count = get_breakout_count(
            df,
            breakout_index
        )

        if count is None:

            return empty_result

        return {
            "signal": str(count),
            "direction": "short",
            "breakout_id": breakout_id,
            "breakout_index": breakout_index,
            "warning_index": None
        }

    return {
        "signal": "none",
        "direction": current_direction,
        "breakout_id": None,
        "breakout_index": None,
        "warning_index": None
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

            previous = daily.iloc[i - 1]

            current = daily.iloc[i]

            if previous == 0:

                continue

            change = (
                (current - previous)
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
# 4H EMA 필터
# =========================================================

def check_4h_filter(df4h):

    direction = get_ema_30_60_120_direction(
        df4h
    )

    if direction == "long":

        return {
            "direction": "long",
            "display": "🟢 LONG"
        }

    if direction == "short":

        return {
            "direction": "short",
            "display": "🔴 SHORT"
        }

    return {
        "direction": "none",
        "display": "⚪"
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

        x = float(changes[0])

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
# 경고 HTML
#
# 🚨 = 돌파 직전
# 🚀 = 돌파 후 1~3
# =========================================================

def combined_warning_html(
    warning,
    qualified=False
):

    if not warning:

        return ""

    if not qualified:

        return ""

    signal = warning.get(
        "signal",
        "none"
    )

    # ---------------------------------------------
    # 돌파 직전
    # ---------------------------------------------

    if signal == "prebreakout":

        return (
            '<span class="prebreakout-warning">'
            '🚨'
            '</span>'
        )

    # ---------------------------------------------
    # 돌파 후 1~3
    # ---------------------------------------------

    if signal.isdigit():

        count = int(signal)

        if 1 <= count <= 3:

            return (
                '<span class="warning-rocket">'
                f'🚀({count})'
                '</span>'
            )

    return ""


# =========================================================
# 방향 표시
#
# 경고 조건을 모두 만족한 경우에만 표시
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
        "KRW 마켓 + 24시간 거래대금 조회"
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

        if not markets:

            return []

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

    except Exception as e:

        logging.error(
            f"OKX 거래대금 오류 "
            f"{inst_id}: {e}"
        )

        return None


# =========================================================
# OKX 4H 과거 데이터
# =========================================================

def get_okx_4h_history(inst_id):

    all_df = None

    before = None

    for chunk_index in range(
        MAX_HISTORY_CHUNKS
    ):

        df = get_okx_ohlcv(
            inst_id,
            "4H",
            HISTORY_CHUNK,
            before
        )

        if (
            df is None
            or df.empty
        ):

            break

        if all_df is None:

            all_df = df.copy()

        else:

            all_df = pd.concat(
                [
                    df,
                    all_df
                ],
                ignore_index=True
            )

        all_df = (
            all_df
            .drop_duplicates("ts")
            .sort_values("ts")
            .reset_index(drop=True)
        )

        if len(all_df) >= 125:

            return all_df

        oldest_ts = int(
            all_df["ts"].iloc[0]
        )

        before = oldest_ts

    return all_df


# =========================================================
# OKX 15M 과거 데이터
# =========================================================

def get_okx_history(
    inst_id,
    bar="15m"
):

    all_df = None

    before = None

    for chunk_index in range(
        MAX_HISTORY_CHUNKS
    ):

        df = get_okx_ohlcv(
            inst_id,
            bar,
            HISTORY_CHUNK,
            before
        )

        if (
            df is None
            or df.empty
        ):

            break

        if all_df is None:

            all_df = df.copy()

        else:

            all_df = pd.concat(
                [
                    df,
                    all_df
                ],
                ignore_index=True
            )

        all_df = (
            all_df
            .drop_duplicates("ts")
            .sort_values("ts")
            .reset_index(drop=True)
        )

        if len(all_df) >= INITIAL_CANDLE_COUNT:

            return all_df

        oldest_ts = int(
            all_df["ts"].iloc[0]
        )

        before = oldest_ts

    return all_df


# =========================================================
# 업비트 4H 과거 데이터
# =========================================================

def get_upbit_4h_history(market):

    all_df = None

    to = None

    for chunk_index in range(
        MAX_HISTORY_CHUNKS
    ):

        df = get_upbit_4h_ohlcv(
            market,
            HISTORY_CHUNK,
            to
        )

        if (
            df is None
            or df.empty
        ):

            break

        if all_df is None:

            all_df = df.copy()

        else:

            all_df = pd.concat(
                [
                    df,
                    all_df
                ],
                ignore_index=True
            )

        all_df = (
            all_df
            .drop_duplicates("datetime")
            .sort_values("datetime")
            .reset_index(drop=True)
        )

        if len(all_df) >= 125:

            return all_df

        oldest = all_df[
            "datetime"
        ].iloc[0]

        to = oldest.strftime(
            "%Y-%m-%dT%H:%M:%S"
        )

    return all_df


# =========================================================
# 업비트 15M 과거 데이터
# =========================================================

def get_upbit_history(market):

    all_df = None

    to = None

    for chunk_index in range(
        MAX_HISTORY_CHUNKS
    ):

        df = get_upbit_ohlcv(
            market,
            15,
            HISTORY_CHUNK,
            to
        )

        if (
            df is None
            or df.empty
        ):

            break

        if all_df is None:

            all_df = df.copy()

        else:

            all_df = pd.concat(
                [
                    df,
                    all_df
                ],
                ignore_index=True
            )

        all_df = (
            all_df
            .drop_duplicates("datetime")
            .sort_values("datetime")
            .reset_index(drop=True)
        )

        if len(all_df) >= INITIAL_CANDLE_COUNT:

            return all_df

        oldest = all_df[
            "datetime"
        ].iloc[0]

        to = oldest.strftime(
            "%Y-%m-%dT%H:%M:%S"
        )

    return all_df


# =========================================================
# 업비트 분석
# =========================================================

def get_upbit_analysis(market):

    df4h = get_upbit_4h_history(
        market
    )

    if (
        df4h is None
        or len(df4h) < 125
    ):

        return None

    filter4h = check_4h_filter(
        df4h
    )

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
        "UPBIT",
        market,
        allow_short=False
    )

    changes = get_upbit_daily_change(
        market
    )

    return {
        "ema": ema,
        "ema_4h": filter4h,
        "warning": warning,
        "changes": changes
    }


# =========================================================
# OKX 분석
# =========================================================

def get_okx_analysis(inst_id):

    df4h = get_okx_4h_history(
        inst_id
    )

    if (
        df4h is None
        or len(df4h) < 125
    ):

        return None

    filter4h = check_4h_filter(
        df4h
    )

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
        "OKX",
        inst_id,
        allow_short=True
    )

    changes = calculate_daily_changes(
        df,
        True
    )

    return {
        "ema": ema,
        "ema_4h": filter4h,
        "warning": warning,
        "changes": changes
    }


# =========================================================
# LONG 필터
# =========================================================

def pass_long_filter(analysis):

    if analysis is None:

        return False

    ema = analysis.get(
        "ema",
        {}
    )

    ema4h = analysis.get(
        "ema_4h",
        {}
    )

    warning = analysis.get(
        "warning",
        {}
    )

    if ema4h.get(
        "direction"
    ) != "long":

        return False

    if ema.get(
        "direction"
    ) != "long":

        return False

    if warning.get(
        "direction"
    ) != "long":

        return False

    signal = warning.get(
        "signal",
        "none"
    )

    if signal == "none":

        return False

    return True


# =========================================================
# SHORT 필터
# =========================================================

def pass_short_filter(analysis):

    if analysis is None:

        return False

    ema = analysis.get(
        "ema",
        {}
    )

    ema4h = analysis.get(
        "ema_4h",
        {}
    )

    warning = analysis.get(
        "warning",
        {}
    )

    if ema4h.get(
        "direction"
    ) != "short":

        return False

    if ema.get(
        "direction"
    ) != "short":

        return False

    if warning.get(
        "direction"
    ) != "short":

        return False

    signal = warning.get(
        "signal",
        "none"
    )

    if signal == "none":

        return False

    return True


# =========================================================
# 빈 분석
# =========================================================

def make_empty_analysis():

    return {
        "ema": {
            "display": "⚪",
            "direction": "none"
        },
        "ema_4h": {
            "display": "⚪",
            "direction": "none"
        },
        "warning": {
            "signal": "none",
            "direction": "none",
            "breakout_id": None,
            "breakout_index": None,
            "warning_index": None
        },
        "changes": None
    }


# =========================================================
# 업비트 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update_time
    global latest_upbit_markets

    logging.info(
        f"========== 업비트 TOP{TOP_N} 시작 =========="
    )

    market_data = get_upbit_markets()

    if not market_data:

        return False

    market_data = sorted(
        market_data,
        key=lambda x: x["volume_24h"],
        reverse=True
    )

    top_markets = market_data[:TOP_N]

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

        volume = format_volume(
            item["volume_24h"]
        )

        try:

            analysis = get_upbit_analysis(
                market
            )

            if analysis is None:

                empty = make_empty_analysis()

                rows.append(
                    {
                        "rank": rank,
                        "name": coin,
                        "change": "",
                        "volume": volume,
                        "ema": empty["ema"],
                        "ema_4h": empty["ema_4h"],
                        "direction": "none",
                        "warning": empty["warning"],
                        "qualified": False
                    }
                )

                continue

            ema = analysis["ema"]

            ema4h = analysis["ema_4h"]

            warning = analysis["warning"]

            # 업비트는 LONG만
            qualified = pass_long_filter(
                analysis
            )

            warning_signal = warning.get(
                "signal",
                "none"
            )

            # ---------------------------------------------
            # 핵심
            # 실제 경고가 있을 때만 LONG 표시
            #
            # prebreakout도 실제 경고이므로 표시
            # ---------------------------------------------

            if (
                qualified
                and
                warning_signal != "none"
                and
                warning.get(
                    "direction"
                ) == "long"
            ):

                display_direction = "long"

            else:

                display_direction = "none"

            rows.append(
                {
                    "rank": rank,
                    "name": coin,
                    "change": format_change(
                        analysis["changes"]
                    ),
                    "volume": volume,
                    "ema": ema,
                    "ema_4h": ema4h,
                    "direction": display_direction,
                    "warning": warning,
                    "qualified": qualified
                }
            )

        except Exception as e:

            logging.error(
                f"업비트 상세 오류 "
                f"{market}: {e}"
            )

            empty = make_empty_analysis()

            rows.append(
                {
                    "rank": rank,
                    "name": coin,
                    "change": "",
                    "volume": volume,
                    "ema": empty["ema"],
                    "ema_4h": empty["ema_4h"],
                    "direction": "none",
                    "warning": empty["warning"],
                    "qualified": False
                }
            )

    latest_upbit_data = rows

    latest_upbit_update_time = (
        get_kst_time()
    )

    logging.info(
        f"업비트 TOP{TOP_N} 완료 / "
        f"조건 충족 "
        f"{sum(1 for x in rows if x.get('qualified'))}개"
    )

    return True


# =========================================================
# OKX 업데이트
# =========================================================

def update_okx(usdt_krw):

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

        volume = format_volume(
            volume_map[symbol]
        )

        try:

            analysis = get_okx_analysis(
                symbol
            )

            if analysis is None:

                empty = make_empty_analysis()

                rows.append(
                    {
                        "rank": rank,
                        "name": display_coin,
                        "change": "",
                        "volume": volume,
                        "ema": empty["ema"],
                        "ema_4h": empty["ema_4h"],
                        "direction": "none",
                        "warning": empty["warning"],
                        "qualified": False
                    }
                )

                continue

            ema = analysis["ema"]

            ema4h = analysis["ema_4h"]

            warning = analysis["warning"]

            warning_direction = warning.get(
                "direction",
                "none"
            )

            if warning_direction == "long":

                qualified = pass_long_filter(
                    analysis
                )

            elif warning_direction == "short":

                qualified = pass_short_filter(
                    analysis
                )

            else:

                qualified = False

            warning_signal = warning.get(
                "signal",
                "none"
            )

            # ---------------------------------------------
            # 핵심
            #
            # 경고가 없는 코인은 LONG / SHORT 없음
            # ---------------------------------------------

            if (
                qualified
                and
                warning_signal != "none"
                and
                warning_direction in (
                    "long",
                    "short"
                )
            ):

                display_direction = (
                    warning_direction
                )

            else:

                display_direction = "none"

            rows.append(
                {
                    "rank": rank,
                    "name": display_coin,
                    "change": format_change(
                        analysis["changes"]
                    ),
                    "volume": volume,
                    "ema": ema,
                    "ema_4h": ema4h,
                    "direction": display_direction,
                    "warning": warning,
                    "qualified": qualified
                }
            )

        except Exception as e:

            logging.error(
                f"OKX 상세 오류 "
                f"{symbol}: {e}"
            )

            empty = make_empty_analysis()

            rows.append(
                {
                    "rank": rank,
                    "name": display_coin,
                    "change": "",
                    "volume": volume,
                    "ema": empty["ema"],
                    "ema_4h": empty["ema_4h"],
                    "direction": "none",
                    "warning": empty["warning"],
                    "qualified": False
                }
            )

    latest_okx_data = rows

    latest_okx_update_time = (
        get_kst_time()
    )

    logging.info(
        f"OKX TOP{TOP_N} 완료 / "
        f"조건 충족 "
        f"{sum(1 for x in rows if x.get('qualified'))}개"
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
            "이전 전체 조회 진행 중 → 건너뜀"
        )

        return

    try:

        logging.info(
            "========================================"
        )

        logging.info(
            f"전체 조회 시작 {get_kst_time()} KST"
        )

        logging.info(
            "조회 순서 : "
            "24H 거래대금 → 4H EMA → 15M N자"
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
            f"전체 조회 종료 {get_kst_time()} KST"
        )

        logging.info(
            "========================================"
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
    width: 18%;
}

th:nth-child(3),
td:nth-child(3) {
    width: 21%;
}

th:nth-child(4),
td:nth-child(4) {
    width: 22%;
}

th:nth-child(5),
td:nth-child(5) {
    width: 32%;
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

.prebreakout-warning {
    font-size: 10px;
    font-weight: bold;
    filter: drop-shadow(
        0 0 4px
        rgba(255, 190, 50, 0.95)
    );
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

.ema4h-value {
    width: 100%;
    font-size: 7px;
    color: #8f949d;
    margin-top: 2px;
    white-space: nowrap;
}

.qualified-row {
    animation:
        qualifiedBlink
        1.2s
        infinite;
}

.qualified-row td {
    animation:
        qualifiedCellBlink
        1.2s
        infinite;
}

@keyframes qualifiedBlink {

    0%,
    100% {
        background: #181c21;
    }

    50% {
        background: #26352b;
    }
}

@keyframes qualifiedCellBlink {

    0%,
    100% {
        opacity: 1;
    }

    50% {
        opacity: 0.55;
    }
}

.qualified-long {
    text-shadow:
        0 0 3px
        rgba(53, 230, 109, 0.95),
        0 0 7px
        rgba(53, 230, 109, 0.75);
}

.qualified-short {
    text-shadow:
        0 0 3px
        rgba(255, 77, 77, 0.95),
        0 0 7px
        rgba(255, 77, 77, 0.75);
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

    .prebreakout-warning,
    .warning-rocket {
        font-size: 9px;
    }

    .ema-value {
        font-size: 7px;
    }

    .ema4h-value {
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

        qualified = item.get(
            "qualified",
            False
        )

        warning_text = combined_warning_html(
            item.get(
                "warning",
                {}
            ),
            qualified
        )

        ema4h = item.get(
            "ema_4h",
            {}
        )

        direction = item.get(
            "direction",
            "none"
        )

        row_class = ""

        direction_class = ""

        if qualified:

            row_class = "qualified-row"

            if direction == "long":

                direction_class = (
                    "qualified-long"
                )

            elif direction == "short":

                direction_class = (
                    "qualified-short"
                )

        rows_html += f"""
<tr class="{row_class}">

<td class="{direction_class}">
{item.get("rank", "-")}
</td>

<td class="{direction_class}">
<span class="coin">
{item.get("name", "-")}
</span>
</td>

<td class="{direction_class}">

<span class="volume-value">
{item.get("volume", "-")}
</span>

{direction_html(
    direction
)}

</td>

<td class="{direction_class}">

<div class="today-wrap">

<div>
{item.get("change", "")}
</div>

<div class="breakout-warning">
{warning_text}
</div>

</div>

</td>

<td class="{direction_class}">

<div class="ema-value">
15M {item.get("ema", {}).get(
    "display",
    "⚪"
)}
</div>

<div class="ema4h-value">
4H {ema4h.get(
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
현재 조회 데이터 없음
</td>
</tr>
"""

    if is_okx:

        direction_note = (
            "※ OKX = 4H + 15M + 실제 N자 LONG/SHORT 조건 충족 시 표시<br>"
        )

        change_note = (
            "※ 변동률 = OKX 15분봉 한국시간 09:00 기준<br>"
        )

        update_time = latest_okx_update_time

    else:

        direction_note = (
            "※ 업비트 = 4H + 15M + 실제 N자 LONG 조건 충족 시 표시<br>"
        )

        change_note = (
            "※ 변동률 = 업비트 일봉 API change_rate<br>"
        )

        update_time = latest_upbit_update_time

    return f"""
<div class="section">

<h2>
🏆 {title} TOP{TOP_N}
<span style="
    color:#777;
    font-size:7px;
    font-weight:normal;
">
&nbsp;전체 랭크 / 조건 종목 반짝임
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

<div
    style="
        color:#666;
        font-size:6px;
        line-height:1.5;
        margin:4px 2px 7px 2px;
    ">

※ 1차 = 24시간 거래대금 TOP{TOP_N}<br>

※ TOP{TOP_N} 전체 랭크 항상 표시<br>

※ 2차 = 4시간봉 EMA 30-60-120 방향 필터<br>

※ 3차 = 15분봉 EMA 30-60-120 + N자 구조 분석<br>

{direction_note}

{change_note}

※ LONG 필수 = 4H LONG + 15M LONG + N자 LONG<br>

※ SHORT 필수 = 4H SHORT + 15M SHORT + N자 SHORT<br>

※ 4H LONG = EMA 30 > 60 > 120<br>

※ 4H SHORT = EMA 30 < 60 < 120<br>

※ 15M LONG = EMA 30 > 60 > 120<br>

※ 15M SHORT = EMA 30 < 60 < 120<br>

※ 4H와 15M 방향이 다르면 LONG / SHORT 표시하지 않음<br>

※ 실제 N자 경고가 없으면 LONG / SHORT 표시하지 않음<br>

※ 거래대금 아래 LONG / SHORT는 실제 경고 조건 충족 종목만 표시<br>

※ 🚨 = 돌파 직전 가능성이 높은 확정봉<br>

※ 🚨는 이전 고점/저점에 설정 거리 이내 접근한 경우 표시<br>

※ LONG 🚨 = 이전 주요 고점의 0.5% 이내 접근 + 아직 종가 돌파 전<br>

※ SHORT 🚨 = 이전 주요 저점의 0.5% 이내 접근 + 아직 종가 이탈 전<br>

※ 🚀(1) = 최초 돌파 확정봉<br>

※ 🚀(2) = 돌파 후 두 번째 확정봉<br>

※ 🚀(3) = 돌파 후 세 번째 확정봉<br>

※ 🚀는 3개까지만 표시<br>

※ 🚀(4) 이후에도 내부적으로 돌파 상태를 계산하지만 화면에는 표시하지 않음<br>

※ 기존 ⛔️ 해지 경고는 사용하지 않음<br>

※ 기존 돌파 후 기준봉 저가/고가에 의한 해지 판정도 사용하지 않음<br>

※ 돌파 직전 🚨는 돌파가 확정되면 🚀(1)로 변경<br>

※ N자 추적 중 15분 EMA 배열이 깨지면 구조 폐기<br>

※ 돌파는 15분 확정봉 종가 기준<br>

※ 현재 진행 중인 15분봉 제외<br>

※ 당일 변동률 양수/음수는 필터에 사용하지 않음<br>

※ 1시간봉 조건 사용하지 않음<br>

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
15M N Pattern Breakout
</title>

<style>

{DASHBOARD_CSS}

</style>

</head>

<body>

<h1>
📊 15M N Pattern Breakout
</h1>

<div class="info">

<div>
① 24시간 거래대금 TOP{TOP_N}
</div>

<div>
② 4H EMA 30-60-120 방향 필터
</div>

<div>
③ 15M EMA 30-60-120 + N자 돌파
</div>

<div>
④ 4H와 15M 방향 일치 + 실제 N자 경고 시 표시
</div>

<div>
🚨 돌파 직전 / 🚀 돌파 후 1~3
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
        f"설정 업비트={USE_UPBIT} "
        f"OKX={USE_OKX}"
    )

    logging.info(
        f"TOP={TOP_N}"
    )

    logging.info(
        f"UPDATE={UPDATE_MINUTES}분"
    )

    logging.info(
        "1차 필터 : 24시간 거래대금"
    )

    logging.info(
        "2차 필터 : 4시간 EMA 30-60-120"
    )

    logging.info(
        "3차 분석 : 15분 EMA 30-60-120 + N자"
    )

    logging.info(
        "LONG : 4H LONG + 15M LONG + N자 LONG"
    )

    logging.info(
        "SHORT : 4H SHORT + 15M SHORT + N자 SHORT"
    )

    logging.info(
        "🚨 : 돌파 직전 확정봉"
    )

    logging.info(
        "🚀 : 돌파 후 1~3"
    )

    logging.info(
        "🚀 4 이상 : 화면 표시 안 함"
    )

    logging.info(
        "⛔️ 해지 경고 : 사용 안 함"
    )

    logging.info(
        "돌파 후 해지 판정 : 사용 안 함"
    )

    logging.info(
        "거래대금 아래 LONG/SHORT : "
        "실제 경고 조건 충족 시에만 표시"
    )

    logging.info(
        "기준 : 15분 확정봉"
    )

    logging.info(
        "현재 진행 중인 15분봉 제외"
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

    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    schedule.every(
        UPDATE_MINUTES
    ).minutes.do(
        update_dashboard
    )

    threading.Thread(
        target=scheduler,
        daemon=True
    ).start()

    logging.info(
        "스케줄러 시작 완료"
    )

    logging.info(
        "========================================"
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
