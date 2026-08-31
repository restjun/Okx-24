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

# 거래대금 계산 시간
VOLUME_HOURS = 24

# 거래대금 TOP
TOP_N = 100

# 업데이트 주기
UPDATE_MINUTES = 1

# 최초 캔들 요청
INITIAL_CANDLE_COUNT = 200

# 과거 추가 요청
HISTORY_CHUNK = 200

# 최대 과거 요청 횟수
MAX_HISTORY_CHUNKS = 10

# 구조 탐색 범위
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


# =========================================================
# 업비트 마켓 목록 캐시
#
# 업비트 조회가 끝난 뒤 OKX에서
# 다시 업비트 API를 호출하지 않도록 사용
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

            wait_time = (
                REQUEST_INTERVAL - elapsed
            )

            time.sleep(wait_time)

        last_request_time = time.monotonic()


# =========================================================
# API URL 표시
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

            logging.info(
                f"[API 응답] "
                f"{function_name} "
                f"{url}"
            )

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

        logging.error(
            "[업비트 API] USDT-KRW 조회 실패"
        )

        return None

    try:

        data = response.json()

        if not data:

            logging.warning(
                "[업비트 API] USDT-KRW 데이터 없음"
            )

            return None

        price = float(
            data[0]["trade_price"]
        )

        if price <= 0:

            return None

        logging.info(
            f"[업비트 API] "
            f"USDT-KRW 성공 "
            f"{price:,.2f}"
        )

        return price

    except Exception as e:

        logging.error(
            f"[업비트 API] "
            f"USDT-KRW 처리 오류 : {e}"
        )

        return None


# =========================================================
# OKX 1H 캔들
# 확정봉만 사용
# =========================================================

def get_okx_ohlcv(
    inst_id,
    bar="1H",
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
            f"OKX 캔들 처리 오류 "
            f"{inst_id}: {e}"
        )

        return None


# =========================================================
# 업비트 1H 캔들
# 현재 진행 중인 봉 제외
# =========================================================

def get_upbit_ohlcv(
    market,
    unit=60,
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

        # -------------------------------------------------
        # 현재 진행 중인 1시간봉 제거
        # -------------------------------------------------

        now = datetime.now(
            KST
        )

        current_hour = now.replace(
            minute=0,
            second=0,
            microsecond=0
        )

        df = df[
            df["datetime"] < current_hour.replace(
                tzinfo=None
            )
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
            f"업비트 캔들 처리 오류 "
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
# 최초 배열 시작점
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
# OKX 과거 데이터
# =========================================================

def get_okx_history(
    inst_id,
    bar="1H"
):

    all_df = None

    before = None

    for chunk_index in range(
        MAX_HISTORY_CHUNKS
    ):

        logging.info(
            f"[OKX 과거조회] "
            f"{inst_id} "
            f"{chunk_index + 1}/"
            f"{MAX_HISTORY_CHUNKS}"
        )

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

        if find_first_alignment_start(
            all_df
        ) is not None:

            return all_df

        oldest_ts = int(
            all_df["ts"].iloc[0]
        )

        before = oldest_ts

    return all_df


# =========================================================
# 업비트 과거 데이터
# =========================================================

def get_upbit_history(market):

    all_df = None

    to = None

    for chunk_index in range(
        MAX_HISTORY_CHUNKS
    ):

        logging.info(
            f"[업비트 과거조회] "
            f"{market} "
            f"{chunk_index + 1}/"
            f"{MAX_HISTORY_CHUNKS}"
        )

        df = get_upbit_ohlcv(
            market,
            60,
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

        if find_first_alignment_start(
            all_df
        ) is not None:

            return all_df

        oldest = all_df[
            "datetime"
        ].iloc[0]

        to = oldest.strftime(
            "%Y-%m-%dT%H:%M:%S"
        )

    return all_df


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
# LONG 돌파
# =========================================================

def get_long_breakout_signal(
    df,
    alignment_start
):

    if alignment_start is None:

        return "none"

    if alignment_start[
        "direction"
    ] != "long":

        return "none"

    start = alignment_start[
        "index"
    ]

    current_index = len(df) - 1

    if start >= current_index - 2:

        return "none"

    highs = find_swing_highs(
        df,
        start + 1,
        current_index
    )

    if not highs:

        return "none"

    for swing_index, swing_high in reversed(
        highs
    ):

        if (
            current_index - swing_index
            >
            BREAKOUT_LOOKBACK + 15
        ):

            continue

        if swing_index + 2 >= len(df):

            continue

        section = df.iloc[
            swing_index + 1:
            current_index + 1
        ]

        lows = pd.to_numeric(
            section["l"],
            errors="coerce"
        )

        if lows.empty:

            continue

        correction_low = lows.min()

        if pd.isna(
            correction_low
        ):

            continue

        correction_rate = (
            swing_high -
            float(correction_low)
        ) / swing_high

        if (
            correction_rate
            <
            MIN_CORRECTION_RATE
        ):

            continue

        new_highs = find_swing_highs(
            df,
            swing_index + 1,
            current_index
        )

        effective_high = swing_high

        effective_high_index = swing_index

        for (
            nh_index,
            nh_value
        ) in new_highs:

            if nh_index <= swing_index:

                continue

            if nh_value < effective_high:

                effective_high = nh_value

                effective_high_index = nh_index

        if effective_high_index >= current_index:

            continue

        correction_section = df.iloc[
            effective_high_index + 1:
            current_index + 1
        ]

        if correction_section.empty:

            continue

        correction_low = pd.to_numeric(
            correction_section["l"],
            errors="coerce"
        ).min()

        if pd.isna(
            correction_low
        ):

            continue

        correction_rate = (
            effective_high -
            float(correction_low)
        ) / effective_high

        if (
            correction_rate
            <
            MIN_CORRECTION_RATE
        ):

            continue

        # -------------------------------------------------
        # 🚨 직전 고점 접근
        # -------------------------------------------------

        pre_index = None

        for i in range(
            effective_high_index + 1,
            current_index + 1
        ):

            try:

                o = float(
                    df["o"].iloc[i]
                )

                c = float(
                    df["c"].iloc[i]
                )

                # LONG 접근봉은 양봉
                if c < o:

                    continue

                # 이미 돌파한 봉 제외
                if c >= effective_high:

                    continue

                distance = (
                    effective_high - c
                ) / effective_high

                if (
                    distance
                    <=
                    PRE_BREAKOUT_DISTANCE
                ):

                    pre_index = i

            except Exception:

                continue

        if pre_index is None:

            current = df.iloc[
                current_index
            ]

            o = float(
                current["o"]
            )

            c = float(
                current["c"]
            )

            if (
                c >= o
                and
                c < effective_high
                and
                (
                    effective_high - c
                ) / effective_high
                <= PRE_BREAKOUT_DISTANCE
            ):

                return "pre"

            continue

        # 🚨 기준봉 저점
        pre_low = float(
            df["l"].iloc[
                pre_index
            ]
        )

        # -------------------------------------------------
        # 🚀 직전 고점 돌파
        # -------------------------------------------------

        breakout_index = None

        for i in range(
            pre_index + 1,
            current_index + 1
        ):

            try:

                o = float(
                    df["o"].iloc[i]
                )

                c = float(
                    df["c"].iloc[i]
                )

                # LONG 돌파봉은 양봉
                if c <= o:

                    continue

                if c > effective_high:

                    breakout_index = i

                    break

            except Exception:

                continue

        if breakout_index is None:

            if current_index == pre_index:

                return "pre"

            current = df.iloc[
                current_index
            ]

            o = float(
                current["o"]
            )

            c = float(
                current["c"]
            )

            if (
                c >= o
                and
                c < effective_high
                and
                (
                    effective_high - c
                ) / effective_high
                <= PRE_BREAKOUT_DISTANCE
            ):

                return "pre"

            continue

        # 최초 돌파봉
        if breakout_index == current_index:

            return "1"

        # -------------------------------------------------
        # 돌파 직후 봉
        # -------------------------------------------------

        after_index = (
            breakout_index + 1
        )

        if current_index == after_index:

            current_low = float(
                df["l"].iloc[
                    current_index
                ]
            )

            current_close = float(
                df["c"].iloc[
                    current_index
                ]
            )

            current_open = float(
                df["o"].iloc[
                    current_index
                ]
            )

            # 돌파 기준봉 저점 이탈
            if current_low < pre_low:

                return "none"

            # 돌파 직후 음봉
            if current_close < current_open:

                return "pullback"

            return "none"

        return "none"

    return "none"


# =========================================================
# SHORT 돌파
# =========================================================

def get_short_breakout_signal(
    df,
    alignment_start
):

    if alignment_start is None:

        return "none"

    if alignment_start[
        "direction"
    ] != "short":

        return "none"

    start = alignment_start[
        "index"
    ]

    current_index = len(df) - 1

    if start >= current_index - 2:

        return "none"

    lows = find_swing_lows(
        df,
        start + 1,
        current_index
    )

    if not lows:

        return "none"

    for swing_index, swing_low in reversed(
        lows
    ):

        if (
            current_index - swing_index
            >
            BREAKOUT_LOOKBACK + 15
        ):

            continue

        if swing_index + 2 >= len(df):

            continue

        section = df.iloc[
            swing_index + 1:
            current_index + 1
        ]

        highs = pd.to_numeric(
            section["h"],
            errors="coerce"
        )

        if highs.empty:

            continue

        correction_high = highs.max()

        if pd.isna(
            correction_high
        ):

            continue

        correction_rate = (
            float(correction_high) -
            swing_low
        ) / swing_low

        if (
            correction_rate
            <
            MIN_CORRECTION_RATE
        ):

            continue

        new_lows = find_swing_lows(
            df,
            swing_index + 1,
            current_index
        )

        effective_low = swing_low

        effective_low_index = swing_index

        for (
            nl_index,
            nl_value
        ) in new_lows:

            if nl_index <= swing_index:

                continue

            if nl_value > effective_low:

                effective_low = nl_value

                effective_low_index = nl_index

        if effective_low_index >= current_index:

            continue

        correction_section = df.iloc[
            effective_low_index + 1:
            current_index + 1
        ]

        if correction_section.empty:

            continue

        correction_high = pd.to_numeric(
            correction_section["h"],
            errors="coerce"
        ).max()

        if pd.isna(
            correction_high
        ):

            continue

        correction_rate = (
            float(correction_high) -
            effective_low
        ) / effective_low

        if (
            correction_rate
            <
            MIN_CORRECTION_RATE
        ):

            continue

        # -------------------------------------------------
        # 🚨 직전 저점 접근
        # -------------------------------------------------

        pre_index = None

        for i in range(
            effective_low_index + 1,
            current_index + 1
        ):

            try:

                o = float(
                    df["o"].iloc[i]
                )

                c = float(
                    df["c"].iloc[i]
                )

                # SHORT 접근봉은 음봉
                if c > o:

                    continue

                if c <= effective_low:

                    continue

                distance = (
                    c - effective_low
                ) / effective_low

                if (
                    distance
                    <=
                    PRE_BREAKOUT_DISTANCE
                ):

                    pre_index = i

            except Exception:

                continue

        if pre_index is None:

            current = df.iloc[
                current_index
            ]

            o = float(
                current["o"]
            )

            c = float(
                current["c"]
            )

            if (
                c <= o
                and
                c > effective_low
                and
                (
                    c - effective_low
                ) / effective_low
                <= PRE_BREAKOUT_DISTANCE
            ):

                return "pre"

            continue

        # 🚨 기준봉 고점
        pre_high = float(
            df["h"].iloc[
                pre_index
            ]
        )

        # -------------------------------------------------
        # 🚀 직전 저점 이탈
        # -------------------------------------------------

        breakout_index = None

        for i in range(
            pre_index + 1,
            current_index + 1
        ):

            try:

                o = float(
                    df["o"].iloc[i]
                )

                c = float(
                    df["c"].iloc[i]
                )

                # SHORT 돌파봉은 음봉
                if c >= o:

                    continue

                if c < effective_low:

                    breakout_index = i

                    break

            except Exception:

                continue

        if breakout_index is None:

            if current_index == pre_index:

                return "pre"

            current = df.iloc[
                current_index
            ]

            o = float(
                current["o"]
            )

            c = float(
                current["c"]
            )

            if (
                c <= o
                and
                c > effective_low
                and
                (
                    c - effective_low
                ) / effective_low
                <= PRE_BREAKOUT_DISTANCE
            ):

                return "pre"

            continue

        # 최초 돌파봉
        if breakout_index == current_index:

            return "1"

        # -------------------------------------------------
        # 돌파 직후
        # -------------------------------------------------

        after_index = (
            breakout_index + 1
        )

        if current_index == after_index:

            current_high = float(
                df["h"].iloc[
                    current_index
                ]
            )

            current_close = float(
                df["c"].iloc[
                    current_index
                ]
            )

            current_open = float(
                df["o"].iloc[
                    current_index
                ]
            )

            # 돌파 기준봉 고점 돌파
            if current_high > pre_high:

                return "none"

            # SHORT 직후 양봉
            if current_close > current_open:

                return "pullback"

            return "none"

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
#
# 한국시간 09:00 기준
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

        # -------------------------------------------------
        # 한국시간 09:00 기준 일봉
        # -------------------------------------------------

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
        "pre",
        "1",
        "pullback"
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
# 업비트 거래대금
# =========================================================

def get_upbit_volume(
    market
):

    logging.info(
        f"[업비트 거래대금] "
        f"{market} 조회"
    )

    df = get_upbit_ohlcv(
        market,
        60,
        VOLUME_HOURS + 1
    )

    if (
        df is None
        or df.empty
    ):

        logging.warning(
            f"[업비트 거래대금] "
            f"{market} 데이터 없음"
        )

        return None

    volume = pd.to_numeric(
        df["volume_krw"],
        errors="coerce"
    ).tail(
        VOLUME_HOURS
    ).sum()

    if pd.isna(volume):

        return None

    volume = float(volume)

    logging.info(
        f"[업비트 거래대금] "
        f"{market} = "
        f"{format_volume(volume)}"
    )

    return volume


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

    for round_index in range(
        OKX_MAX_RETRY_ROUNDS
    ):

        try:

            df = get_okx_ohlcv(
                inst_id,
                "1H",
                hours + 1
            )

            if (
                df is None
                or len(df) < hours
            ):

                time.sleep(
                    OKX_RETRY_DELAY
                )

                continue

            volume_usdt = float(
                df[
                    "volCcyQuote"
                ]
                .tail(hours)
                .sum()
            )

            if volume_usdt <= 0:

                time.sleep(
                    OKX_RETRY_DELAY
                )

                continue

            volume_krw = (
                volume_usdt *
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
                f"{inst_id}: {e} "
                f"round={round_index + 1}"
            )

            time.sleep(
                OKX_RETRY_DELAY
            )

    return None


# =========================================================
# 업비트 마켓 목록
#
# 이 함수에서 조회한 결과를 캐시
# =========================================================

def get_upbit_markets():

    global latest_upbit_markets

    logging.info(
        "[업비트 API] 마켓 목록 조회 시작"
    )

    response = retry_request(
        requests.get,
        "https://api.upbit.com/v1/market/all",
        timeout=15
    )

    if response is None:

        logging.error(
            "[업비트 API] 마켓 목록 조회 실패"
        )

        return []

    try:

        data = response.json()

        markets = [
            x["market"]
            for x in data
            if x.get(
                "market",
                ""
            ).startswith("KRW-")
        ]

        latest_upbit_markets = markets.copy()

        logging.info(
            "[업비트 API] "
            f"마켓 목록 완료 "
            f"KRW={len(markets)}개"
        )

        return markets

    except Exception as e:

        logging.error(
            f"업비트 마켓 목록 처리 오류 : {e}"
        )

        return []


# =========================================================
# OKX 목록
# =========================================================

def get_all_okx_swap_symbols():

    logging.info(
        "[OKX API] SWAP 목록 조회 시작"
    )

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

        symbols = [
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

        logging.info(
            f"[OKX API] "
            f"SWAP 목록 완료 "
            f"{len(symbols)}개"
        )

        return symbols

    except Exception as e:

        logging.error(
            f"OKX 목록 처리 오류 : {e}"
        )

        return []


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

    changes = calculate_daily_changes(
        df,
        False
    )

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
        "1H"
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
# 최종 LONG 필터
#
# 1. 직전 고점 돌파 구조 LONG
# 2. EMA 30 > 60 > 120
# 3. 당일 변동률 양수
#
# 변동률 기준이 최종 필터
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

    # 30-60-120 정배열
    if ema.get(
        "direction"
    ) != "long":

        return False

    # 직전 고점 돌파 구조
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

    # 당일 변동률 양수
    if today_change <= 0:

        return False

    return True


# =========================================================
# 최종 SHORT 필터
#
# 1. 직전 저점 이탈 구조 SHORT
# 2. EMA 30 < 60 < 120
# 3. 당일 변동률 음수
#
# 변동률 기준이 최종 필터
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

    # 30-60-120 역배열
    if ema.get(
        "direction"
    ) != "short":

        return False

    # 직전 저점 이탈 구조
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

    # 당일 변동률 음수
    if today_change >= 0:

        return False

    return True


# =========================================================
# 업비트 업데이트
#
# LONG만 표시
#
# 중요:
# 여기서 latest_upbit_markets를 만들고
# 이후 OKX에서는 이것을 재사용
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update_time
    global latest_upbit_markets

    start_time = get_kst_time()

    logging.info(
        "========================================"
    )

    logging.info(
        f"========== 업비트 TOP{TOP_N} 시작 "
        f"{start_time} KST =========="
    )

    # -----------------------------------------------------
    # 1. 업비트 마켓 목록
    # -----------------------------------------------------

    markets = get_upbit_markets()

    if not markets:

        logging.error(
            "업비트 마켓 목록 조회 실패"
        )

        return False

    logging.info(
        f"[업비트 진행] "
        f"KRW 마켓 {len(markets)}개"
    )

    # -----------------------------------------------------
    # 2. 거래대금 조회
    # -----------------------------------------------------

    volume_map = {}

    for index, market in enumerate(
        markets,
        start=1
    ):

        logging.info(
            f"[업비트 거래대금 진행] "
            f"{index}/{len(markets)} "
            f"{market}"
        )

        try:

            volume = get_upbit_volume(
                market
            )

            if (
                volume is not None
                and volume > 0
            ):

                volume_map[
                    market
                ] = volume

        except Exception as e:

            logging.error(
                f"업비트 거래대금 오류 "
                f"{market}: {e}"
            )

    if not volume_map:

        logging.error(
            "업비트 거래대금 데이터 없음"
        )

        return False

    # -----------------------------------------------------
    # 3. TOP30
    # -----------------------------------------------------

    top_markets = sorted(
        volume_map,
        key=volume_map.get,
        reverse=True
    )[:TOP_N]

    logging.info(
        f"[업비트 진행] "
        f"거래대금 TOP{TOP_N} 선정 완료"
    )

    # -----------------------------------------------------
    # 4. TOP30 분석
    # -----------------------------------------------------

    rows = []

    for rank, market in enumerate(
        top_markets,
        start=1
    ):

        coin = market.replace(
            "KRW-",
            ""
        )

        logging.info(
            f"[업비트 분석] "
            f"{rank}/{len(top_markets)} "
            f"{market}"
        )

        try:

            analysis = get_upbit_analysis(
                market
            )

            if analysis is None:

                logging.info(
                    f"[업비트 분석] "
                    f"{market} 데이터 부족"
                )

                continue

            warning = analysis[
                "warning"
            ]

            if not is_visible_warning(
                warning
            ):

                continue

            # 업비트 LONG만
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
        f"업비트 LONG 경고 "
        f"{len(rows)}개"
    )

    logging.info(
        f"업비트 조회 종료 "
        f"{latest_upbit_update_time} KST"
    )

    logging.info(
        "========== 업비트 완전 종료 =========="
    )

    return True


# =========================================================
# OKX 업데이트
#
# LONG / SHORT
#
# 중요:
# 업비트 마켓을 API로 다시 조회하지 않는다.
# latest_upbit_markets 사용
# =========================================================

def update_okx(
    usdt_krw
):

    global latest_okx_data
    global latest_okx_update_time

    start_time = get_kst_time()

    logging.info(
        "========================================"
    )

    logging.info(
        f"========== OKX TOP{TOP_N} 시작 "
        f"{start_time} KST =========="
    )

    if (
        usdt_krw is None
        or usdt_krw <= 0
    ):

        logging.error(
            "OKX USDT-KRW 환율 없음"
        )

        return False

    # -----------------------------------------------------
    # OKX 목록
    # -----------------------------------------------------

    symbols = get_all_okx_swap_symbols()

    if not symbols:

        logging.error(
            "OKX SWAP 목록 없음"
        )

        return False

    # -----------------------------------------------------
    # 중요
    #
    # 업비트 API를 다시 호출하지 않는다.
    # 직전 업비트 단계에서 저장한 캐시 사용
    # -----------------------------------------------------

    upbit_coin_set = {
        market.replace(
            "KRW-",
            ""
        )
        for market in latest_upbit_markets
    }

    logging.info(
        f"[OKX] "
        f"업비트 상장 캐시 사용 "
        f"{len(upbit_coin_set)}개"
    )

    # -----------------------------------------------------
    # OKX 거래대금
    # -----------------------------------------------------

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

        logging.error(
            "OKX 거래대금 데이터 없음"
        )

        return False

    top_symbols = sorted(
        volume_map,
        key=volume_map.get,
        reverse=True
    )[:TOP_N]

    logging.info(
        f"[OKX 진행] "
        f"거래대금 TOP{TOP_N} 선정 완료"
    )

    rows = []

    # -----------------------------------------------------
    # TOP30 분석
    # -----------------------------------------------------

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

        logging.info(
            f"[OKX 분석] "
            f"{rank}/{len(top_symbols)} "
            f"{symbol}"
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

            # -------------------------------------------------
            # LONG
            # -------------------------------------------------

            if direction == "long":

                if not pass_long_filter(
                    analysis
                ):

                    continue

            # -------------------------------------------------
            # SHORT
            # -------------------------------------------------

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

    logging.info(
        f"OKX LONG/SHORT 경고 "
        f"{len(rows)}개"
    )

    logging.info(
        f"OKX 조회 종료 "
        f"{latest_okx_update_time} KST"
    )

    logging.info(
        "========== OKX 완전 종료 =========="
    )

    return True


# =========================================================
# 전체 업데이트
#
# 반드시
#
# 업비트
# ↓
# 업비트 완전 종료
# ↓
# OKX
# ↓
# OKX 완전 종료
#
# =========================================================

def update_dashboard():

    global latest_usdt_krw
    global latest_upbit_data
    global latest_okx_data

    # -----------------------------------------------------
    # 이전 업데이트가 아직 끝나지 않았다면
    # 중복 실행 방지
    # -----------------------------------------------------

    if not update_lock.acquire(
        blocking=False
    ):

        logging.warning(
            "이전 전체 조회가 아직 진행 중입니다. "
            "이번 주기는 건너뜁니다."
        )

        return

    try:

        cycle_start = get_kst_time()

        logging.info(
            ""
        )

        logging.info(
            "========================================"
        )

        logging.info(
            f"전체 조회 시작 "
            f"{cycle_start} KST"
        )

        logging.info(
            "조회 순서 : 업비트 → OKX"
        )

        # =================================================
        # 1. 업비트
        # =================================================

        if USE_UPBIT == "Y":

            try:

                upbit_result = update_upbit()

                if upbit_result:

                    logging.info(
                        "업비트 단계 성공"
                    )

                else:

                    logging.warning(
                        "업비트 단계 실패"
                    )

            except Exception as e:

                logging.exception(
                    f"업비트 업데이트 오류 : {e}"
                )

        else:

            latest_upbit_data = []

            latest_upbit_markets = []

            logging.info(
                "업비트 사용 안 함"
            )

        # =================================================
        # 2. 업비트가 완전히 끝난 뒤 OKX
        # =================================================

        if USE_OKX == "Y":

            logging.info(
                "업비트 종료 확인 → OKX 시작"
            )

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

                else:

                    logging.error(
                        "OKX 환율을 사용할 수 없습니다."
                    )

            except Exception as e:

                logging.exception(
                    f"OKX 업데이트 오류 : {e}"
                )

        else:

            latest_okx_data = []

            logging.info(
                "OKX 사용 안 함 "
                "(USE_OKX=N)"
            )

        # =================================================
        # 전체 종료
        # =================================================

        cycle_end = get_kst_time()

        logging.info(
            f"전체 조회 종료 "
            f"{cycle_end} KST"
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

.warning-pre {
    font-size: 10px;
    font-weight: bold;
    animation: warning-blink 0.9s infinite;
    filter: drop-shadow(
        0 0 4px
        rgba(255, 180, 0, 0.9)
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

.warning-pullback {
    font-size: 10px;
    font-weight: bold;
    filter: drop-shadow(
        0 0 4px
        rgba(255, 200, 50, 0.9)
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

    .warning-pre,
    .warning-rocket,
    .warning-pullback {
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
현재 🚨 / 🚀 / 〽️ 종목 없음
</td>
</tr>
"""

    if is_okx:

        direction_note = (
            "※ OKX = LONG / SHORT 모두 표시<br>"
        )

        update_time = (
            latest_okx_update_time
        )

    else:

        direction_note = (
            "※ 업비트 = LONG만 표시<br>"
        )

        update_time = (
            latest_upbit_update_time
        )

    return f"""
<div class="section">

<h2>
🏆 {title} TOP{TOP_N} 경고
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
※ 거래대금 = 확정 1시간봉 기준<br>
{direction_note}
※ LONG = EMA 30 > 60 > 120 + 당일 변동률 양수<br>
※ SHORT = EMA 30 < 60 < 120 + 당일 변동률 음수<br>
※ 당일 변동률 = 한국시간 09:00 기준<br>
※ 현재 진행 중인 1시간봉 제외<br>
※ 🚨 = 직전 고점/저점 돌파 직전<br>
※ 🚀 = 최초 돌파 확정봉<br>
※ 〽️ = 🚀 직후 눌림<br>
※ LONG은 직전 고점 돌파 기준<br>
※ SHORT는 직전 저점 이탈 기준<br>
※ 🚨 기준봉 저점/고점 이탈 시 구조 폐기<br>
※ 돌파 실패 후 반등 고점/반락 저점은 새 기준점<br>
※ 최초 30-60-120 배열을 찾을 때까지 과거 캔들 추가 조회<br>
※ 4H 조건 사용하지 않음<br>

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
1H Breakout Trading
</title>

<style>

{DASHBOARD_CSS}

</style>

</head>

<body>

<h1>
📊 1H Breakout Trading
</h1>

<div class="info">

<div>
1H 30-60-120 정배열 / 역배열
</div>

<div>
최초 배열 시작 → 고점/저점 → 눌림/반등
→ 직전 고점/저점 돌파
</div>

<div>
🚨 돌파 직전 · 🚀 첫 돌파 · 〽️ 돌파 직후
</div>

<div>
LONG = 정배열 + 당일 변동률 양수
</div>

<div>
SHORT = 역배열 + 당일 변동률 음수
</div>

<div>
변동률 = 한국시간 09:00 기준
</div>

<div>
확정 1시간봉 기준
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
        "기준 : 1H 확정봉"
    )

    logging.info(
        "EMA : 30-60-120"
    )

    logging.info(
        "LONG : EMA 30 > 60 > 120 + 당일 변동률 양수"
    )

    logging.info(
        "SHORT : EMA 30 < 60 < 120 + 당일 변동률 음수"
    )

    logging.info(
        "변동률 : 한국시간 09:00 기준"
    )

    logging.info(
        "4H 조건 : 사용 안 함"
    )

    logging.info(
        "조회 순서 : 업비트 → 업비트 종료 → OKX"
    )

    logging.info(
        "OKX 단계에서 업비트 마켓 API 재조회 안 함"
    )

    # -----------------------------------------------------
    # 설정 검증
    # -----------------------------------------------------

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

    logging.info(
        "최초 즉시 조회 스레드 시작"
    )

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

    logging.info(
        f"{UPDATE_MINUTES}분 주기 등록 완료"
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
