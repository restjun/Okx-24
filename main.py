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

PRE_BREAKOUT_DISTANCE = 0.005

SWING_LEFT = 2

SWING_RIGHT = 2

# 최소 조정폭
MIN_CORRECTION_RATE = 0.003

# ---------------------------------------------------------
# N자 되돌림 기준
#
# 이전 상승폭/하락폭의 50% 이상 되돌림되어야
# 다음 N자 구조로 인정
#
# LONG
# 100 → 120 상승
# 상승폭 = 20
# 50% 되돌림 = 10
# 따라서 110 이하까지 내려와야 인정
#
# SHORT
# 120 → 100 하락
# 하락폭 = 20
# 50% 되돌림 = 10
# 따라서 110 이상까지 반등해야 인정
# ---------------------------------------------------------

N_PATTERN_RETRACE = 0.50


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
                        f"{url}"
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

        df = (
            df
            .sort_values("ts")
            .drop_duplicates("ts")
            .reset_index(drop=True)
        )

        return df

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

        df = (
            df
            .sort_values("datetime")
            .drop_duplicates("datetime")
            .reset_index(drop=True)
        )

        return df

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
            f"업비트 일봉 처리 오류 "
            f"{market}: {e}"
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
# 최근 정배열/역배열 시작점
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
# 배열 유지 확인
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
        or start_index > end_index
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
# LONG N자 추적
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

    if (
        df is None
        or len(df) < 125
    ):

        return "none"

    directions = get_direction_series(
        df
    )

    if not directions:

        return "none"

    start = alignment_start[
        "index"
    ]

    current_index = len(df) - 1

    if start >= current_index:

        return "none"

    # -----------------------------------------------------
    # 현재까지 정배열 유지
    # -----------------------------------------------------

    if not alignment_is_valid(
        directions,
        start,
        current_index,
        "long"
    ):

        return "none"

    swing_highs = find_swing_highs(
        df,
        start + SWING_RIGHT + 1,
        current_index
    )

    swing_lows = find_swing_lows(
        df,
        start + SWING_RIGHT + 1,
        current_index
    )

    if not swing_highs:

        return "none"

    if not swing_lows:

        return "none"

    # -----------------------------------------------------
    # 기준 고점 후보
    # 최근 후보부터 검사
    # -----------------------------------------------------

    for anchor_index, anchor_high in reversed(
        swing_highs
    ):

        if anchor_index >= current_index - 2:

            continue

        anchor_high = float(
            anchor_high
        )

        # -------------------------------------------------
        # 기준 고점 직전의 가장 가까운 스윙 저점
        # -------------------------------------------------

        previous_lows = [
            (idx, low)
            for idx, low in swing_lows
            if idx < anchor_index
        ]

        if not previous_lows:

            continue

        previous_low_index, previous_low = (
            previous_lows[-1]
        )

        previous_low = float(
            previous_low
        )

        if anchor_high <= previous_low:

            continue

        # -------------------------------------------------
        # 기준 상승폭
        # -------------------------------------------------

        rise_range = (
            anchor_high -
            previous_low
        )

        if rise_range <= 0:

            continue

        # -------------------------------------------------
        # 50% 되돌림 가격
        # -------------------------------------------------

        retracement_price = (
            anchor_high
            -
            (
                rise_range
                *
                N_PATTERN_RETRACE
            )
        )

        # -------------------------------------------------
        # 기준 고점 이후 실제 저점
        # -------------------------------------------------

        post_anchor = df.iloc[
            anchor_index + 1:
            current_index + 1
        ]

        if post_anchor.empty:

            continue

        lows = pd.to_numeric(
            post_anchor["l"],
            errors="coerce"
        ).dropna()

        if lows.empty:

            continue

        correction_low = float(
            lows.min()
        )

        # -------------------------------------------------
        # 반드시 50% 이상 되돌림
        # -------------------------------------------------

        if correction_low > retracement_price:

            continue

        # -------------------------------------------------
        # 기준 고점 이후에도 정배열 유지
        # -------------------------------------------------

        if not alignment_is_valid(
            directions,
            anchor_index,
            current_index,
            "long"
        ):

            continue

        # -------------------------------------------------
        # N자 구조 확인
        # -------------------------------------------------

        correction_confirmed = False

        rebound_high = None

        rebound_high_index = None

        breakout_index = None

        search_start = (
            anchor_index + 1
        )

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

            # -------------------------------------------------
            # 50% 되돌림 완료
            # -------------------------------------------------

            if low <= retracement_price:

                correction_confirmed = True

            # -------------------------------------------------
            # 되돌림이 확인된 이후에만
            # 반등 고점 추적
            # -------------------------------------------------

            if correction_confirmed:

                if i >= search_start + SWING_LEFT:

                    left_start = max(
                        search_start,
                        i - SWING_LEFT
                    )

                    left_values = pd.to_numeric(
                        df["h"].iloc[
                            left_start:i
                        ],
                        errors="coerce"
                    ).dropna()

                    if not left_values.empty:

                        if high >= left_values.max():

                            if high < anchor_high:

                                rebound_high = high

                                rebound_high_index = i

            # -------------------------------------------------
            # 반등 후 다시 조정
            # -------------------------------------------------

            if (
                rebound_high is not None
                and
                rebound_high_index is not None
                and
                i > rebound_high_index
            ):

                decline_rate = (
                    rebound_high -
                    low
                ) / rebound_high

                if (
                    decline_rate
                    >=
                    MIN_CORRECTION_RATE
                ):

                    # 새로운 N자 조정이 발생했지만
                    # 기준 고점은 절대 변경하지 않음
                    rebound_high = None

                    rebound_high_index = None

            # -------------------------------------------------
            # 최종 기준 고점 돌파
            #
            # 반드시 50% 되돌림이 먼저 발생하고
            # 그 후 기준 고점을 종가로 돌파해야 함
            # -------------------------------------------------

            if (
                correction_confirmed
                and
                close > anchor_high
            ):

                breakout_index = i

                break

        if breakout_index is None:

            continue

        # -----------------------------------------------------
        # 돌파 후 카운팅
        # -----------------------------------------------------

        count = (
            current_index
            -
            breakout_index
            +
            1
        )

        # -----------------------------------------------------
        # 돌파 기준봉 저점
        # -----------------------------------------------------

        breakout_low = float(
            df["l"].iloc[
                breakout_index
            ]
        )

        # -----------------------------------------------------
        # 돌파 이후 기준봉 저점 이탈
        # -----------------------------------------------------

        after_section = df.iloc[
            breakout_index:
            current_index + 1
        ]

        lows_after = pd.to_numeric(
            after_section["l"],
            errors="coerce"
        ).dropna()

        if lows_after.empty:

            continue

        if lows_after.min() < breakout_low:

            return "none"

        # -----------------------------------------------------
        # 🚀 1~3
        # -----------------------------------------------------

        if 1 <= count <= 3:

            return str(count)

        return "none"

    return "none"


# =========================================================
# SHORT N자 추적
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

    if (
        df is None
        or len(df) < 125
    ):

        return "none"

    directions = get_direction_series(
        df
    )

    if not directions:

        return "none"

    start = alignment_start[
        "index"
    ]

    current_index = len(df) - 1

    if start >= current_index:

        return "none"

    if not alignment_is_valid(
        directions,
        start,
        current_index,
        "short"
    ):

        return "none"

    swing_lows = find_swing_lows(
        df,
        start + SWING_RIGHT + 1,
        current_index
    )

    swing_highs = find_swing_highs(
        df,
        start + SWING_RIGHT + 1,
        current_index
    )

    if not swing_lows:

        return "none"

    if not swing_highs:

        return "none"

    # -----------------------------------------------------
    # 기준 저점 후보
    # -----------------------------------------------------

    for anchor_index, anchor_low in reversed(
        swing_lows
    ):

        if anchor_index >= current_index - 2:

            continue

        anchor_low = float(
            anchor_low
        )

        # -------------------------------------------------
        # 기준 저점 직전 스윙 고점
        # -------------------------------------------------

        previous_highs = [
            (idx, high)
            for idx, high in swing_highs
            if idx < anchor_index
        ]

        if not previous_highs:

            continue

        previous_high_index, previous_high = (
            previous_highs[-1]
        )

        previous_high = float(
            previous_high
        )

        if previous_high <= anchor_low:

            continue

        # -------------------------------------------------
        # 기준 하락폭
        # -------------------------------------------------

        fall_range = (
            previous_high -
            anchor_low
        )

        if fall_range <= 0:

            continue

        # -------------------------------------------------
        # 50% 되돌림 가격
        # -------------------------------------------------

        retracement_price = (
            anchor_low
            +
            (
                fall_range
                *
                N_PATTERN_RETRACE
            )
        )

        # -------------------------------------------------
        # 기준 저점 이후 실제 반등
        # -------------------------------------------------

        post_anchor = df.iloc[
            anchor_index + 1:
            current_index + 1
        ]

        if post_anchor.empty:

            continue

        highs = pd.to_numeric(
            post_anchor["h"],
            errors="coerce"
        ).dropna()

        if highs.empty:

            continue

        correction_high = float(
            highs.max()
        )

        # -------------------------------------------------
        # 반드시 50% 이상 반등
        # -------------------------------------------------

        if correction_high < retracement_price:

            continue

        # -------------------------------------------------
        # 역배열 유지
        # -------------------------------------------------

        if not alignment_is_valid(
            directions,
            anchor_index,
            current_index,
            "short"
        ):

            continue

        correction_confirmed = False

        rebound_low = None

        rebound_low_index = None

        breakout_index = None

        search_start = (
            anchor_index + 1
        )

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

            # -------------------------------------------------
            # 50% 반등 확인
            # -------------------------------------------------

            if high >= retracement_price:

                correction_confirmed = True

            # -------------------------------------------------
            # 반등 이후 재하락 저점 추적
            # -------------------------------------------------

            if correction_confirmed:

                if i >= search_start + SWING_LEFT:

                    left_start = max(
                        search_start,
                        i - SWING_LEFT
                    )

                    left_values = pd.to_numeric(
                        df["l"].iloc[
                            left_start:i
                        ],
                        errors="coerce"
                    ).dropna()

                    if not left_values.empty:

                        if low <= left_values.min():

                            if low > anchor_low:

                                rebound_low = low

                                rebound_low_index = i

            # -------------------------------------------------
            # 재하락 후 반등
            # -------------------------------------------------

            if (
                rebound_low is not None
                and
                rebound_low_index is not None
                and
                i > rebound_low_index
            ):

                rise_rate = (
                    high -
                    rebound_low
                ) / rebound_low

                if (
                    rise_rate
                    >=
                    MIN_CORRECTION_RATE
                ):

                    rebound_low = None

                    rebound_low_index = None

            # -------------------------------------------------
            # 기준 저점 최종 이탈
            # -------------------------------------------------

            if (
                correction_confirmed
                and
                close < anchor_low
            ):

                breakout_index = i

                break

        if breakout_index is None:

            continue

        # -----------------------------------------------------
        # 카운팅
        # -----------------------------------------------------

        count = (
            current_index
            -
            breakout_index
            +
            1
        )

        # -----------------------------------------------------
        # 돌파 기준봉 고점
        # -----------------------------------------------------

        breakout_high = float(
            df["h"].iloc[
                breakout_index
            ]
        )

        # -----------------------------------------------------
        # 돌파 기준봉 고점 돌파 시
        # SHORT 신호 제거
        # -----------------------------------------------------

        after_section = df.iloc[
            breakout_index:
            current_index + 1
        ]

        highs_after = pd.to_numeric(
            after_section["h"],
            errors="coerce"
        ).dropna()

        if highs_after.empty:

            continue

        if highs_after.max() > breakout_high:

            return "none"

        # -----------------------------------------------------
        # 🚀 1~3
        # -----------------------------------------------------

        if 1 <= count <= 3:

            return str(count)

        return "none"

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

    directions = get_direction_series(
        df
    )

    if not directions:

        return {
            "signal": "none",
            "direction": "none"
        }

    current_direction = directions[-1]

    # -----------------------------------------------------
    # LONG
    # -----------------------------------------------------

    if current_direction == "long":

        alignment = (
            find_latest_alignment_start(
                df,
                "long"
            )
        )

        signal = (
            get_long_breakout_signal(
                df,
                alignment
            )
        )

        return {
            "signal": signal,
            "direction": "long"
        }

    # -----------------------------------------------------
    # SHORT
    # -----------------------------------------------------

    if (
        current_direction == "short"
        and
        allow_short
    ):

        alignment = (
            find_latest_alignment_start(
                df,
                "short"
            )
        )

        signal = (
            get_short_breakout_signal(
                df,
                alignment
            )
        )

        return {
            "signal": signal,
            "direction": "short"
        }

    return {
        "signal": "none",
        "direction": current_direction
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

    direction = (
        get_ema_30_60_120_direction(
            df
        )
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
# 거래대금
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
# 경고 표시
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
# 방향 표시
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
                x.get(
                    "state"
                ) == "live"
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
            f"OKX 거래대금 계산 오류 "
            f"{inst_id}: {e}"
        )

        return None


# =========================================================
# OKX 과거 데이터
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
# 업비트 과거 데이터
# =========================================================

def get_upbit_history(
    market
):

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
        f"업비트 LONG N자 돌파 "
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
            "이전 전체 조회 진행 중 → 이번 주기 건너뜀"
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

        logging.info(
            "조회 순서 : 업비트 → OKX"
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

            latest_upbit_markets = []

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
            "※ 변동률 = OKX 15분봉 한국시간 09:00 기준<br>"
        )

        update_time = (
            latest_okx_update_time
        )

    else:

        direction_note = (
            "※ 업비트 = LONG만 표시<br>"
        )

        change_note = (
            "※ 변동률 = 업비트 일봉 API change_rate<br>"
        )

        update_time = (
            latest_upbit_update_time
        )

    return f"""
<div class="section">

<h2>
🏆 {title} TOP{TOP_N} N자 돌파
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

<div
    style="
        color:#666;
        font-size:6px;
        line-height:1.5;
        margin:4px 2px 7px 2px;
    ">

※ TOP{TOP_N} 거래대금 순위<br>
※ 업비트 거래대금 = API 24시간 누적 거래대금<br>
{direction_note}
{change_note}
※ EMA = 15분봉 30-60-120<br>
※ LONG = 30 > 60 > 120 정배열 유지<br>
※ SHORT = 30 < 60 < 120 역배열 유지<br>
※ 정배열/역배열 시작점부터 가격 구조 추적<br>
※ LONG = 상승 → 주요 고점 → 50% 이상 되돌림 → 반등 → 재조정 → 기준 고점 돌파<br>
※ SHORT = 하락 → 주요 저점 → 50% 이상 되돌림 → 재하락 → 재반등 → 기준 저점 이탈<br>
※ 50% 되돌림 기준은 기준 고점/저점 직전 상승폭/하락폭 기준<br>
※ 중간 반등 고점/반락 저점 때문에 기준 고점/저점을 변경하지 않음<br>
※ N자 추적 중 이평 배열이 깨지면 구조 폐기<br>
※ 돌파는 15분 확정봉 종가 기준<br>
※ 현재 진행 중인 15분봉 제외<br>
※ 🚀(1) = 최초 돌파 확정봉<br>
※ 🚀(2) = 돌파 후 두 번째 확정봉<br>
※ 🚀(3) = 돌파 후 세 번째 확정봉<br>
※ 🚀 표시는 3개 확정봉까지만 표시<br>
※ 돌파 직전 🚨 미표시<br>
※ 돌파 직후 별도 표시 없음<br>
※ LONG 돌파 기준봉 저가 이탈 시 신호 제거<br>
※ SHORT 돌파 기준봉 고가 돌파 시 신호 제거<br>
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
15분봉 30-60-120 정배열 / 역배열
</div>

<div>
정배열 시작 → 상승 → 주요 고점
→ 50% 이상 되돌림 → 반등
→ 재조정 → 재반등 → 기준 고점 돌파
</div>

<div>
기준 고점/저점은 중간 반등/반락 때문에 변경하지 않음
</div>

<div>
🚀(1) 첫 돌파 · 🚀(2) 두 번째 봉 · 🚀(3) 세 번째 봉
</div>

<div>
돌파 직전 🚨 미표시
</div>

<div>
LONG = 가격 조정 중에도 EMA 30 > 60 > 120 유지
</div>

<div>
SHORT = 가격 반등 중에도 EMA 30 < 60 < 120 유지
</div>

<div>
기준 상승폭/하락폭의 50% 이상 되돌림 발생 필요
</div>

<div>
돌파는 15분 확정봉 종가 기준
</div>

<div>
업비트 변동률 = 일봉 API change_rate
</div>

<div>
OKX 변동률 = 15분봉 한국시간 09:00 기준
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
        f"설정 "
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
        "N자형 가격 구조 추적"
    )

    logging.info(
        f"N자 최소 되돌림 = "
        f"{N_PATTERN_RETRACE * 100:.0f}%"
    )

    logging.info(
        "정배열/역배열 유지 중 가격 조정 추적"
    )

    logging.info(
        "기준 고점/저점 돌파 시 🚀"
    )

    logging.info(
        "🚨 돌파 직전 표시 안 함"
    )

    logging.info(
        "🚀 돌파 후 1~3까지만 표시"
    )

    logging.info(
        "1H 조건 : 사용 안 함"
    )

    logging.info(
        "조회 순서 : 업비트 → OKX"
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
    # 최초 조회
    # -----------------------------------------------------

    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    # -----------------------------------------------------
    # 주기
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
