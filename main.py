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

from datetime import datetime, timezone


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

# 거래대금 계산에 사용할 확정 1시간봉 개수
VOLUME_HOURS = 24

# 거래대금 TOP
TOP_N = 30

# 업데이트 주기
UPDATE_MINUTES = 1

# ---------------------------------------------------------
# 최초 조회 개수
# ---------------------------------------------------------

INITIAL_CANDLE_COUNT = 200

# ---------------------------------------------------------
# 과거 캔들 추가 조회 개수
# ---------------------------------------------------------

HISTORY_CHUNK = 200

# ---------------------------------------------------------
# 최초 정배열/역배열 시작점을 찾기 위한
# 최대 추가 조회 횟수
#
# 필요하면 계속 늘릴 수 있음
# ---------------------------------------------------------

MAX_HISTORY_CHUNKS = 10

# ---------------------------------------------------------
# 돌파 구조 탐색 범위
# ---------------------------------------------------------

BREAKOUT_LOOKBACK = 30

# 전고점/전저점 접근 허용거리
PRE_BREAKOUT_DISTANCE = 0.005

# 스윙 판정
SWING_LEFT = 2
SWING_RIGHT = 2

# 최소 조정폭
MIN_CORRECTION_RATE = 0.003


# =========================================================
# 거래소 조회
# =========================================================

USE_UPBIT = "Y"

# OKX
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

        elapsed = (
            now -
            last_request_time
        )

        if elapsed < REQUEST_INTERVAL:

            time.sleep(
                REQUEST_INTERVAL -
                elapsed
            )

        last_request_time = (
            time.monotonic()
        )


# =========================================================
# API 재시도
# =========================================================

def retry_request(
    func,
    *args,
    **kwargs
):

    for attempt in range(
        MAX_RETRIES
    ):

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

                status = (
                    result.status_code
                )

                if status == 429:

                    wait_time = min(
                        RATE_LIMIT_WAIT *
                        (
                            2 ** attempt
                        ),
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
                        2 *
                        (
                            2 ** attempt
                        ),
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

            if attempt < (
                MAX_RETRIES - 1
            ):

                time.sleep(
                    wait_time
                )

    return None


# =========================================================
# 업비트 USDT-KRW
# =========================================================

def get_usdt_krw():

    logging.info(
        "USDT-KRW API 요청 시작"
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

        logging.info(
            f"USDT-KRW : {price}"
        )

        return price

    except Exception as e:

        logging.error(
            f"USDT-KRW 처리 오류 : {e}"
        )

        return None


# =========================================================
# OKX 1H 확정 캔들
#
# after 사용:
# 더 과거 데이터 확보
# =========================================================

def get_okx_ohlcv(
    inst_id,
    bar="1H",
    limit=200,
    before=None
):

    limit = max(
        1,
        min(
            int(limit),
            200
        )
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

        params["before"] = str(
            before
        )

    response = retry_request(
        requests.get,
        url,
        params=params,
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

        # 확정봉만
        df = df[
            df["confirm"].astype(str) == "1"
        ]

        if df.empty:

            return None

        df = (
            df.sort_values("ts")
            .drop_duplicates(
                subset=["ts"]
            )
            .reset_index(drop=True)
        )

        return df

    except Exception as e:

        logging.error(
            f"OKX 캔들 오류 "
            f"{inst_id}: {e}"
        )

        return None


# =========================================================
# OKX 과거 1H 캔들 확보
#
# 200개 → 부족하면 200개 추가
# 최초 정배열/역배열 시작점이 나올 때까지
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

        df = get_okx_ohlcv(
            inst_id,
            bar,
            HISTORY_CHUNK,
            before
        )

        if df is None or df.empty:

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
            .drop_duplicates(
                subset=["ts"]
            )
            .sort_values("ts")
            .reset_index(drop=True)
        )

        # 최초 배열 시작점 확인
        direction_info = (
            find_first_alignment_start(
                all_df
            )
        )

        if direction_info is not None:

            logging.info(
                f"OKX {inst_id} "
                f"최초 배열 시작점 확인 "
                f"{len(all_df)}개"
            )

            return all_df

        # 가장 오래된 캔들의 timestamp
        oldest_ts = int(
            all_df["ts"].iloc[0]
        )

        # OKX before는 해당 시각 이전
        before = oldest_ts

        logging.info(
            f"OKX {inst_id} "
            f"과거 캔들 추가 확보 "
            f"{len(all_df)}개"
        )

    return all_df


# =========================================================
# 업비트 1H 캔들
#
# to 사용하여 과거 데이터 추가 확보
# =========================================================

def get_upbit_ohlcv(
    market,
    unit=60,
    count=200,
    to=None
):

    count = max(
        1,
        min(
            int(count),
            200
        )
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

        df = pd.DataFrame(
            data
        )

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
            df.get(
                "candle_acc_trade_price",
                0
            ),
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

        # 현재 진행 중인 1H 봉 제외
        now = pd.Timestamp.now(
            tz="Asia/Seoul"
        ).tz_localize(None)

        current_hour = now.replace(
            minute=0,
            second=0,
            microsecond=0
        )

        df = df[
            df["datetime"] <
            current_hour
        ]

        df = (
            df.sort_values("datetime")
            .drop_duplicates(
                subset=["datetime"]
            )
            .reset_index(drop=True)
        )

        return df

    except Exception as e:

        logging.error(
            f"업비트 캔들 오류 "
            f"{market}: {e}"
        )

        return None


# =========================================================
# 업비트 과거 캔들 추가 확보
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
            60,
            HISTORY_CHUNK,
            to
        )

        if df is None or df.empty:

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
            .drop_duplicates(
                subset=["datetime"]
            )
            .sort_values("datetime")
            .reset_index(drop=True)
        )

        direction_info = (
            find_first_alignment_start(
                all_df
            )
        )

        if direction_info is not None:

            logging.info(
                f"업비트 {market} "
                f"최초 배열 시작점 확인 "
                f"{len(all_df)}개"
            )

            return all_df

        oldest = (
            all_df["datetime"].iloc[0]
        )

        # 업비트 to는 해당 시각 이전
        to = (
            oldest
            .strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        )

        logging.info(
            f"업비트 {market} "
            f"과거 캔들 추가 확보 "
            f"{len(all_df)}개"
        )

    return all_df


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
            if x["market"].startswith(
                "KRW-"
            )
        ]

    except Exception as e:

        logging.error(
            f"업비트 목록 오류 : {e}"
        )

        return []


# =========================================================
# OKX 전체 SWAP 목록
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
# 거래대금 표시
# =========================================================

def format_volume(
    volume
):

    if volume >= 1_000_000_000_000:

        return (
            f"{volume / "
            f"1_000_000_000_000:.2f}조"
        )

    if volume >= 100_000_000:

        return (
            f"{volume / "
            f"100_000_000:,.0f}억"
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
# 30-60-120 방향
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
# 전체 시점별 EMA 방향
# =========================================================

def get_direction_series(
    df
):

    if (
        df is None
        or
        len(df) < 120
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
        or
        ema60 is None
        or
        ema120 is None
    ):

        return []

    result = []

    for i in range(
        len(df)
    ):

        a = ema30.iloc[i]
        b = ema60.iloc[i]
        c = ema120.iloc[i]

        if any(
            pd.isna(x)
            for x in [a, b, c]
        ):

            result.append(
                "none"
            )

        elif a > b > c:

            result.append(
                "long"
            )

        elif a < b < c:

            result.append(
                "short"
            )

        else:

            result.append(
                "none"
            )

    return result


# =========================================================
# 최초 정배열/역배열 시작점
#
# long:
# 이전 none → 현재 long
#
# short:
# 이전 none → 현재 short
# =========================================================

def find_first_alignment_start(
    df
):

    directions = (
        get_direction_series(
            df
        )
    )

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
            and
            previous != "long"
        ):

            return {
                "direction": "long",
                "index": i
            }

        if (
            current == "short"
            and
            previous != "short"
        ):

            return {
                "direction": "short",
                "index": i
            }

    return None


# =========================================================
# EMA 표시
# =========================================================

def check_ema(
    df
):

    direction = (
        get_ema_30_60_120_direction(
            df,
            "c"
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
        len(df) -
        SWING_RIGHT -
        1
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
                    i + 1 +
                    SWING_RIGHT
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

                result.append(
                    (
                        i,
                        high
                    )
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
        len(df) -
        SWING_RIGHT -
        1
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
                    i + 1 +
                    SWING_RIGHT
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
                low <= left.min()
                and
                low <= right.min()
            ):

                result.append(
                    (
                        i,
                        low
                    )
                )

        except Exception:

            continue

    return result


# =========================================================
# LONG 구조
#
# 최초 LONG 정배열 시작
# → 고점
# → 눌림
# → 재상승
# → 전고점 접근 🚨
# → 전고점 돌파 🚀
# → 직후 음봉 〽️
#
# 돌파하지 못한 반등 고점은
# 새로운 고점으로 갱신
# =========================================================

def get_long_breakout_signal(
    df,
    alignment_start
):

    if alignment_start is None:

        return "none"

    start = (
        alignment_start["index"]
    )

    if (
        alignment_start["direction"]
        != "long"
    ):

        return "none"

    if start >= len(df) - 3:

        return "none"

    # -----------------------------------------------------
    # 정배열 시작 이후 고점 후보
    # -----------------------------------------------------

    highs = find_swing_highs(
        df,
        start + 1,
        len(df) - 1
    )

    if not highs:

        return "none"

    # 가장 최근 구조부터 검사
    for swing_index, swing_high in reversed(
        highs
    ):

        if (
            len(df) -
            swing_index
            >
            BREAKOUT_LOOKBACK + 15
        ):

            continue

        if swing_index + 2 >= len(df):

            continue

        # -------------------------------------------------
        # 고점 이후 저점
        # -------------------------------------------------

        after_high = df.iloc[
            swing_index + 1:
        ]

        correction_low = pd.to_numeric(
            after_high["l"],
            errors="coerce"
        ).min()

        if pd.isna(
            correction_low
        ):

            continue

        correction_rate = (
            (
                swing_high -
                float(correction_low)
            )
            /
            swing_high
        )

        if (
            correction_rate
            <
            MIN_CORRECTION_RATE
        ):

            continue

        # -------------------------------------------------
        # 고점 이후 재반등
        # -------------------------------------------------

        current_index = (
            len(df) - 1
        )

        # 최근 반등 고점이 전고점을
        # 넘지 못했다면 새로운 고점으로 갱신
        new_highs = find_swing_highs(
            df,
            swing_index + 1,
            current_index
        )

        effective_high = swing_high
        effective_high_index = swing_index

        for nh_index, nh_value in new_highs:

            if nh_index <= swing_index:

                continue

            # 전고점 돌파가 아닌 반등 고점
            if nh_value < effective_high:

                effective_high = nh_value
                effective_high_index = (
                    nh_index
                )

        # -------------------------------------------------
        # 새로운 기준 고점 이후 조정 확인
        # -------------------------------------------------

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
            (
                effective_high -
                float(correction_low)
            )
            /
            effective_high
        )

        if (
            correction_rate
            <
            MIN_CORRECTION_RATE
        ):

            continue

        # -------------------------------------------------
        # 🚨 캔들 탐색
        # -------------------------------------------------

        pre_index = None

        for i in range(
            effective_high_index + 1,
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

        pre_low = float(
            df["l"].iloc[
                pre_index
            ]
        )

        # -------------------------------------------------
        # 🚀 돌파 캔들
        # -------------------------------------------------

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

            if c <= o:

                continue

            if c > effective_high:

                breakout_index = i

                break

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

        # -------------------------------------------------
        # 현재 봉이 첫 돌파봉
        # -------------------------------------------------

        if (
            breakout_index
            ==
            current_index
        ):

            return "1"

        # -------------------------------------------------
        # 돌파 다음 봉
        # -------------------------------------------------

        after_index = (
            breakout_index + 1
        )

        if (
            current_index
            ==
            after_index
        ):

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

            # 🚨 기준봉 저점 이탈
            if current_low < pre_low:

                return "none"

            if (
                current_close
                <
                current_open
            ):

                return "pullback"

            return "none"

        # -------------------------------------------------
        # 두 번째 이후 캔들에서는
        # 눌림 표시하지 않음
        # -------------------------------------------------

        return "none"

    return "none"


# =========================================================
# SHORT 구조
#
# 최초 SHORT 역배열 시작
# → 저점
# → 반등
# → 재하락
# → 전저점 접근 🚨
# → 전저점 이탈 🚀
# → 직후 양봉 〽️
#
# 업비트에서는 사용하지 않음
# OKX에서만 사용
# =========================================================

def get_short_breakout_signal(
    df,
    alignment_start
):

    if alignment_start is None:

        return "none"

    start = (
        alignment_start["index"]
    )

    if (
        alignment_start["direction"]
        != "short"
    ):

        return "none"

    if start >= len(df) - 3:

        return "none"

    lows = find_swing_lows(
        df,
        start + 1,
        len(df) - 1
    )

    if not lows:

        return "none"

    for swing_index, swing_low in reversed(
        lows
    ):

        if (
            len(df) -
            swing_index
            >
            BREAKOUT_LOOKBACK + 15
        ):

            continue

        if swing_index + 2 >= len(df):

            continue

        after_low = df.iloc[
            swing_index + 1:
        ]

        correction_high = pd.to_numeric(
            after_low["h"],
            errors="coerce"
        ).max()

        if pd.isna(
            correction_high
        ):

            continue

        correction_rate = (
            (
                float(correction_high) -
                swing_low
            )
            /
            swing_low
        )

        if (
            correction_rate
            <
            MIN_CORRECTION_RATE
        ):

            continue

        current_index = (
            len(df) - 1
        )

        new_lows = find_swing_lows(
            df,
            swing_index + 1,
            current_index
        )

        effective_low = swing_low
        effective_low_index = swing_index

        for nl_index, nl_value in new_lows:

            if nl_index <= swing_index:

                continue

            # 이전 저점을 깨지 못한
            # 반등 후 새로운 저점
            if nl_value > effective_low:

                effective_low = nl_value
                effective_low_index = (
                    nl_index
                )

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
            (
                float(correction_high) -
                effective_low
            )
            /
            effective_low
        )

        if (
            correction_rate
            <
            MIN_CORRECTION_RATE
        ):

            continue

        # -------------------------------------------------
        # 🚨
        # 전저점 바로 위의 음봉
        # -------------------------------------------------

        pre_index = None

        for i in range(
            effective_low_index + 1,
            current_index + 1
        ):

            candle = df.iloc[i]

            o = float(
                candle["o"]
            )

            c = float(
                candle["c"]
            )

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

        pre_high = float(
            df["h"].iloc[
                pre_index
            ]
        )

        # -------------------------------------------------
        # 🚀 첫 하락 돌파
        # -------------------------------------------------

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

            if c >= o:

                continue

            if c < effective_low:

                breakout_index = i

                break

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

        if (
            breakout_index
            ==
            current_index
        ):

            return "1"

        # -------------------------------------------------
        # 돌파 직후
        # -------------------------------------------------

        after_index = (
            breakout_index + 1
        )

        if (
            current_index
            ==
            after_index
        ):

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

            # 🚨 기준봉 고점 이탈
            if current_high > pre_high:

                return "none"

            # SHORT 돌파 직후 양봉 눌림
            if (
                current_close
                >
                current_open
            ):

                return "pullback"

            return "none"

        return "none"

    return "none"


# =========================================================
# 통합 돌파 신호
# =========================================================

def get_breakout_signal(
    df,
    allow_short=True
):

    if (
        df is None
        or
        len(df) < 125
    ):

        return {
            "signal": "none",
            "direction": "none"
        }

    alignment = (
        find_first_alignment_start(
            df
        )
    )

    if alignment is None:

        return {
            "signal": "none",
            "direction": "none"
        }

    direction = (
        alignment["direction"]
    )

    if direction == "long":

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

    if (
        direction == "short"
        and
        allow_short
    ):

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
        "direction": direction
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
        warning.get("signal")
        in (
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
# 방향 HTML
#
# 업비트 LONG만
# OKX LONG/SHORT
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
# 변동률 계산
# =========================================================

def calculate_daily_changes(
    df,
    is_okx=False
):

    if (
        df is None
        or
        df.empty
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

            price_col = "c"

        else:

            if "datetime" not in temp.columns:

                return None

            price_col = "c"

        temp[price_col] = pd.to_numeric(
            temp[price_col],
            errors="coerce"
        )

        temp = temp.dropna(
            subset=[
                "datetime",
                price_col
            ]
        )

        if temp.empty:

            return None

        temp = temp.set_index(
            "datetime"
        )

        daily = (
            temp[price_col]
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

        for i in range(
            max(1, len(daily) - 3),
            len(daily)
        ):

            if i <= 0:

                continue

            previous = daily.iloc[
                i - 1
            ]

            current = daily.iloc[
                i
            ]

            if previous == 0:

                continue

            result.append(
                round(
                    float(
                        (
                            current -
                            previous
                        )
                        /
                        previous
                        *
                        100
                    ),
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
# 변동률 표시
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
# 1H만 표시
# =========================================================

def ema_html(
    ema
):

    return f"""
    <div class="ema-value">
        {ema}
    </div>
    """


# =========================================================
# 1H 거래대금
#
# 확정봉만 사용
# =========================================================

def get_upbit_volume(
    market
):

    df = get_upbit_ohlcv(
        market,
        60,
        VOLUME_HOURS + 1
    )

    if (
        df is None
        or
        df.empty
    ):

        return None

    volume = pd.to_numeric(
        df["volume_krw"],
        errors="coerce"
    ).tail(
        VOLUME_HOURS
    ).sum()

    if pd.isna(volume):

        return None

    return float(
        volume
    )


# =========================================================
# OKX 거래대금
#
# 확정 1H봉
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

            df = get_okx_ohlcv(
                inst_id,
                "1H",
                hours + 1
            )

            if (
                df is None
                or
                len(df) < hours
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
                f"{inst_id}: {e}"
            )

            time.sleep(
                OKX_RETRY_DELAY
            )

    return None


# =========================================================
# 업비트 EMA + 돌파
# =========================================================

def get_upbit_analysis(
    market
):

    df = get_upbit_history(
        market
    )

    if (
        df is None
        or
        len(df) < 125
    ):

        return {
            "ema": {
                "display": "⚪",
                "direction": "none"
            },
            "warning": {
                "signal": "none",
                "direction": "none"
            },
            "changes": None
        }

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
# OKX EMA + 돌파
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
        or
        len(df) < 125
    ):

        return {
            "ema": {
                "display": "⚪",
                "direction": "none"
            },
            "warning": {
                "signal": "none",
                "direction": "none"
            },
            "changes": None
        }

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

    volume_map = {}

    # -----------------------------------------------------
    # 확정 1H봉 거래대금
    # -----------------------------------------------------

    for index, market in enumerate(
        markets,
        start=1
    ):

        try:

            volume = get_upbit_volume(
                market
            )

            if (
                volume is not None
                and
                volume > 0
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

            analysis = (
                get_upbit_analysis(
                    market
                )
            )

            warning = analysis[
                "warning"
            ]

            # 업비트는 LONG만
            if (
                warning.get(
                    "direction"
                )
                != "long"
            ):

                continue

            if not is_visible_warning(
                warning
            ):

                continue

            rows.append(
                {
                    "rank": rank,

                    "name": coin,

                    "change":
                        format_change(
                            analysis[
                                "changes"
                            ]
                        ),

                    "volume":
                        format_volume(
                            volume_map[
                                market
                            ]
                        ),

                    "ema":
                        analysis[
                            "ema"
                        ],

                    "direction":
                        "long",

                    "warning":
                        warning
                }
            )

        except Exception as e:

            logging.error(
                f"업비트 상세 오류 "
                f"{market}: {e}"
            )

    latest_upbit_data = rows

    logging.info(
        f"업비트 LONG 경고 "
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

    logging.info(
        "========== OKX 업데이트 시작 =========="
    )

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

    # -----------------------------------------------------
    # 업비트 상장 여부
    # -----------------------------------------------------

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

    # -----------------------------------------------------
    # 거래대금
    # -----------------------------------------------------

    volume_map = {}

    for index, symbol in enumerate(
        symbols,
        start=1
    ):

        logging.info(
            f"[OKX 거래대금] "
            f"{index}/{len(symbols)} "
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

            analysis = (
                get_okx_analysis(
                    symbol
                )
            )

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

            if direction not in (
                "long",
                "short"
            ):

                continue

            rows.append(
                {
                    "rank": rank,

                    "name": coin,

                    "change":
                        format_change(
                            analysis[
                                "changes"
                            ]
                        ),

                    "volume":
                        format_volume(
                            volume_map[
                                symbol
                            ]
                        ),

                    "ema":
                        analysis[
                            "ema"
                        ],

                    "direction":
                        direction,

                    "warning":
                        warning
                }
            )

        except Exception as e:

            logging.error(
                f"OKX 상세 오류 "
                f"{symbol}: {e}"
            )

    latest_okx_data = rows

    logging.info(
        f"OKX LONG/SHORT 경고 "
        f"{len(rows)}개"
    )

    logging.info(
        "========== OKX 업데이트 완료 =========="
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
        "1시간 확정봉 기준 조회 시작"
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
        "1시간 확정봉 조회 종료"
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

    font-family:
        Arial,
        sans-serif;

    font-size: 9px;

    padding: 4px;
}

h1 {

    margin:
        3px
        2px
        6px
        2px;

    font-size: 14px;
}

h2 {

    margin:
        10px
        2px
        5px
        2px;

    font-size: 11px;
}

.info {

    margin:
        0
        2px
        6px
        2px;

    padding: 5px 6px;

    color: #8b9099;

    background: #171a1f;

    border:
        1px solid
        #252a31;

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

    border:
        1px solid
        #252a31;
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
        1px solid
        #2b3037;

    color: #8f949d;

    font-size: 7px;

    text-align: center;
}

td {

    padding: 5px 1px;

    border-bottom:
        1px solid
        #272c32;

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

    animation:
        warning-blink
        0.9s
        infinite;

    filter:
        drop-shadow(
            0 0 4px
            rgba(
                255,
                180,
                0,
                0.9
            )
        );
}

.warning-rocket {

    font-size: 10px;

    font-weight: bold;

    filter:
        drop-shadow(
            0 0 4px
            rgba(
                50,
                255,
                100,
                0.9
            )
        );
}

.warning-pullback {

    font-size: 10px;

    font-weight: bold;

    filter:
        drop-shadow(
            0 0 4px
            rgba(
                255,
                200,
                50,
                0.9
            )
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
{item["change"]}
</div>

<div class="breakout-warning">

{warning_text}

</div>

</div>

</td>

<td>

{ema_html(
    item["ema"].get(
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

    else:

        direction_note = (
            "※ 업비트 = LONG만 표시<br>"
        )

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
        margin:
            4px
            2px
            7px
            2px;
     ">

※ TOP{TOP_N} 거래대금 순위<br>

※ 거래대금 = 확정 1시간봉 기준
<br>

{direction_note}

※ LONG = EMA 30 > 60 > 120
<br>

※ SHORT = EMA 30 < 60 < 120
<br>

※ 현재 진행 중인 1시간봉 제외
<br>

※ 🚨 = 전고점/전저점 돌파 직전
<br>

※ 🚀 = 최초 돌파 확정봉
<br>

※ 〽️ = 🚀 직후 눌림
<br>

※ 🚨 기준봉 저점/고점 이탈 시
돌파 구조 폐기
<br>

※ 정배열/역배열 최초 시작점을 찾기 위해
필요하면 과거 캔들을 추가 조회
<br>

※ 최초 배열 시작 이후 고점/저점 형성
<br>

※ 돌파 실패 후 반등/반락 고점/저점은
새로운 기준점으로 갱신
<br>

※ 4H 조건 사용하지 않음
<br>

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
                latest_upbit_data,
                False
            )
        )

    if USE_OKX == "Y":

        exchange_sections += (
            make_exchange_section(
                "🏆 OKX",
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
Breakout Trading
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
→ 재돌파
</div>

<div>
🚨 돌파 직전 · 🚀 첫 돌파 · 〽️ 돌파 직후
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

@app.on_event(
    "startup"
)
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
        "기준 : 1H 확정봉"
    )

    logging.info(
        "EMA : 30-60-120"
    )

    logging.info(
        "4H 조건 : 사용 안 함"
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

    # -----------------------------------------------------
    # 최초 즉시 조회
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

    threading.Thread(
        target=scheduler,
        daemon=True
    ).start()

    logging.info(
        f"{UPDATE_MINUTES}분 주기 등록 완료"
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
