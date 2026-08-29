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


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s"
)


# =========================================================
# 사용자 설정
# =========================================================

VOLUME_HOURS = 24

TOP_N = 20

UPDATE_MINUTES = 5

MAX_WARNING_COUNT = 3

PULLBACK_DISTANCE = 0.02

BREAKOUT_LOOKBACK = 5


# =========================================================
# API 안정화 설정
# =========================================================

REQUEST_INTERVAL = 0.08

RATE_LIMIT_WAIT = 3

MAX_RETRIES = 10


# =========================================================
# 전역 데이터
# =========================================================

latest_okx_data = []

latest_upbit_data = []


# =========================================================
# 마지막 요청 시간
# =========================================================

request_lock = threading.Lock()

last_request_time = 0.0


# =========================================================
# API 요청 간격 조절
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

                status = result.status_code

                # -----------------------------------------
                # 429
                # -----------------------------------------

                if status == 429:

                    wait_time = min(
                        RATE_LIMIT_WAIT
                        *
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

                # -----------------------------------------
                # 서버 오류
                # -----------------------------------------

                if status >= 500:

                    wait_time = min(
                        2
                        *
                        (2 ** attempt),
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

                # -----------------------------------------
                # 기타 HTTP 오류
                # -----------------------------------------

                if status != 200:

                    logging.warning(
                        f"API HTTP 오류 "
                        f"{status}"
                    )

                    return result

            return result

        except Exception as e:

            wait_time = min(
                2
                *
                (attempt + 1),
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
# OKX 캔들
# ★ 기존 확정 캔들 함수 유지
# ★ confirm = 1만 사용
# =========================================================

def get_okx_ohlcv(
    inst_id,
    bar="1H",
    limit=200
):

    limit = max(
        1,
        min(
            int(limit),
            200
        )
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
            df["confirm"]
            .astype(str)
            ==
            "1"
        ]

        if df.empty:
            return None

        df = (
            df
            .iloc[::-1]
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
# ★ OKX 현재 진행 중 캔들
# ★ confirm 여부와 관계없이 마지막 캔들 사용
# ★ 0 판단 전용
# =========================================================

def get_okx_current_ohlcv(
    inst_id,
    bar="1H",
    limit=200
):

    limit = max(
        1,
        min(
            int(limit),
            200
        )
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
            df
            .iloc[::-1]
            .reset_index(drop=True)
        )

        return df

    except Exception as e:

        logging.error(
            f"OKX 진행 캔들 오류 "
            f"{inst_id} : {e}"
        )

        return None


# =========================================================
# 업비트 분봉
# =========================================================

def get_upbit_ohlcv(
    market,
    unit=60,
    count=200
):

    count = max(
        1,
        min(
            int(count),
            200
        )
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
            df
            .iloc[::-1]
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

        df["candle_acc_trade_volume"] = (
            pd.to_numeric(
                df["candle_acc_trade_volume"],
                errors="coerce"
            )
        )

        df["candle_acc_trade_price"] = (
            pd.to_numeric(
                df["candle_acc_trade_price"],
                errors="coerce"
            )
        )

        return df

    except Exception as e:

        logging.error(
            f"업비트 캔들 오류 "
            f"{market} : {e}"
        )

        return None


# =========================================================
# 업비트 일봉
# =========================================================

def get_upbit_daily_ohlcv(
    market,
    count=200
):

    count = max(
        1,
        min(
            int(count),
            200
        )
    )

    url = (
        "https://api.upbit.com/v1/candles/days"
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
            df
            .iloc[::-1]
            .reset_index(drop=True)
        )

        df["trade_price"] = pd.to_numeric(
            df["trade_price"],
            errors="coerce"
        )

        df["c"] = df["trade_price"]

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

        # 진행 중인 일봉 제거
        if len(df) > 1:

            df = (
                df
                .iloc[:-1]
                .reset_index(drop=True)
            )

        return df

    except Exception as e:

        logging.error(
            f"업비트 일봉 오류 "
            f"{market} : {e}"
        )

        return None


# =========================================================
# USDT/KRW
# =========================================================

def get_usdt_krw():

    response = retry_request(
        requests.get,
        "https://api.upbit.com/v1/ticker?markets=KRW-USDT",
        timeout=15
    )

    if response is None:

        logging.warning(
            "USDT/KRW 조회 실패 - 1400 사용"
        )

        return 1400

    try:

        return float(
            response.json()[0]["trade_price"]
        )

    except Exception:

        return 1400


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
            f"OKX 전체 USDT-SWAP "
            f"{len(symbols)}개 확인"
        )

        return symbols

    except Exception as e:

        logging.error(
            f"OKX 목록 오류 : {e}"
        )

        return []


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

        markets = [
            x["market"]
            for x in data
            if x["market"].startswith(
                "KRW-"
            )
        ]

        logging.info(
            f"업비트 전체 KRW 마켓 "
            f"{len(markets)}개 확인"
        )

        return markets

    except Exception as e:

        logging.error(
            f"업비트 목록 오류 : {e}"
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
            f"{volume / 1_000_000_000_000:.2f}조"
        )

    elif volume >= 100_000_000:

        return (
            f"{volume / 100_000_000:,.0f}억"
        )

    else:

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
        or column not in df.columns
    ):

        return None

    price = pd.to_numeric(
        df[column],
        errors="coerce"
    )

    valid_count = price.notna().sum()

    if valid_count < period:

        return None

    return price.ewm(
        span=period,
        adjust=False
    ).mean()


# =========================================================
# EMA 10-30 현재 방향
# =========================================================

def get_ema_10_30_direction(
    df,
    column
):

    if (
        df is None
        or column not in df.columns
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

    if (
        ema10 is None
        or ema30 is None
    ):

        return "none"

    if (
        pd.isna(ema10.iloc[-1])
        or
        pd.isna(ema30.iloc[-1])
    ):

        return "none"

    if (
        ema10.iloc[-1]
        >
        ema30.iloc[-1]
    ):

        return "long"

    if (
        ema10.iloc[-1]
        <
        ema30.iloc[-1]
    ):

        return "short"

    return "none"


# =========================================================
# EMA 10-30 연속 카운트
# =========================================================

def get_10_30_count(
    df,
    column
):

    if (
        df is None
        or column not in df.columns
        or len(df) < 30
    ):

        return 0, "none"

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
        or ema30 is None
    ):

        return 0, "none"

    states = []

    for i in range(
        len(df)
    ):

        if (
            pd.isna(ema10.iloc[i])
            or
            pd.isna(ema30.iloc[i])
        ):

            states.append("none")

        elif (
            ema10.iloc[i]
            >
            ema30.iloc[i]
        ):

            states.append("long")

        elif (
            ema10.iloc[i]
            <
            ema30.iloc[i]
        ):

            states.append("short")

        else:

            states.append("none")

    current_state = states[-1]

    if current_state == "none":

        return 0, "none"

    count = 0

    for state in reversed(
        states
    ):

        if state == current_state:

            count += 1

        else:

            break

    return count, current_state


# =========================================================
# EMA 30-60-120
# =========================================================

def get_ema_30_60_120_direction(
    df,
    column
):

    if (
        df is None
        or column not in df.columns
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
        or ema60 is None
        or ema120 is None
    ):

        return "none"

    if (
        ema30.iloc[-1]
        >
        ema60.iloc[-1]
        >
        ema120.iloc[-1]
    ):

        return "long"

    if (
        ema30.iloc[-1]
        <
        ema60.iloc[-1]
        <
        ema120.iloc[-1]
    ):

        return "short"

    return "none"


# =========================================================
# 30-60-120 연속 카운트
# =========================================================

def get_30_60_120_count(
    df,
    column
):

    if (
        df is None
        or column not in df.columns
        or len(df) < 120
    ):

        return 0, "none"

    df = df.copy()

    df["ema30"] = get_ema(
        df,
        column,
        30
    )

    df["ema60"] = get_ema(
        df,
        column,
        60
    )

    df["ema120"] = get_ema(
        df,
        column,
        120
    )

    states = []

    for _, row in df.iterrows():

        if (
            pd.isna(row["ema30"])
            or
            pd.isna(row["ema60"])
            or
            pd.isna(row["ema120"])
        ):

            states.append("none")

        elif (
            row["ema30"]
            >
            row["ema60"]
            >
            row["ema120"]
        ):

            states.append("long")

        elif (
            row["ema30"]
            <
            row["ema60"]
            <
            row["ema120"]
        ):

            states.append("short")

        else:

            states.append("none")

    current_state = states[-1]

    if current_state == "none":

        return 0, "none"

    count = 0

    for state in reversed(
        states
    ):

        if state == current_state:

            count += 1

        else:

            break

    return count, current_state


# =========================================================
# EMA 표시
# =========================================================

def check_ema_10_30(
    df,
    column
):

    count, direction = get_10_30_count(
        df,
        column
    )

    if direction == "long":

        return f"🟢({count})"

    if direction == "short":

        return f"🔴({count})"

    return "⚪"


def check_ema(
    df,
    column
):

    count, direction = (
        get_30_60_120_count(
            df,
            column
        )
    )

    if direction == "long":

        return f"🟢({count})"

    if direction == "short":

        return f"🔴({count})"

    return "⚪"


# =========================================================
# 메인 방향
# ★ 1H 30-60-120만 사용
# =========================================================

def get_main_direction(
    df1h,
    df4h,
    column
):

    h1_direction = (
        get_ema_30_60_120_direction(
            df1h,
            column
        )
    )

    if h1_direction == "long":

        return "long"

    if h1_direction == "short":

        return "short"

    return "none"


# =========================================================
# 🚀 1H 확정 돌파
# ★ 기존 로직 그대로 유지
# =========================================================

def check_breakout(
    df1h,
    column
):

    if (
        df1h is None
        or len(df1h)
        <
        120 + BREAKOUT_LOOKBACK
    ):

        return "none"

    df = df1h.copy()

    df["ema10"] = get_ema(
        df,
        column,
        10
    )

    df["ema30"] = get_ema(
        df,
        column,
        30
    )

    df["ema60"] = get_ema(
        df,
        column,
        60
    )

    df["ema120"] = get_ema(
        df,
        column,
        120
    )

    def get_breakout_state(index):

        if index < BREAKOUT_LOOKBACK:

            return "none"

        cur = df.iloc[index]

        previous = df.iloc[
            index - BREAKOUT_LOOKBACK:
            index
        ]

        if previous.empty:

            return "none"

        previous_high = (
            pd.to_numeric(
                previous["h"],
                errors="coerce"
            ).max()
        )

        previous_low = (
            pd.to_numeric(
                previous["l"],
                errors="coerce"
            ).min()
        )

        # LONG
        long_10_30 = (
            cur["ema10"]
            >
            cur["ema30"]
        )

        long_30_60_120 = (
            cur["ema30"]
            >
            cur["ema60"]
            >
            cur["ema120"]
        )

        long_break = (
            cur["c"]
            >
            previous_high
        )

        long_candle = (
            cur["c"]
            >
            cur["o"]
        )

        if (
            long_10_30
            and
            long_30_60_120
            and
            long_break
            and
            long_candle
        ):

            return "long"

        # SHORT
        short_10_30 = (
            cur["ema10"]
            <
            cur["ema30"]
        )

        short_30_60_120 = (
            cur["ema30"]
            <
            cur["ema60"]
            <
            cur["ema120"]
        )

        short_break = (
            cur["c"]
            <
            previous_low
        )

        short_candle = (
            cur["c"]
            <
            cur["o"]
        )

        if (
            short_10_30
            and
            short_30_60_120
            and
            short_break
            and
            short_candle
        ):

            return "short"

        return "none"

    current_index = len(df) - 1

    current_state = get_breakout_state(
        current_index
    )

    if current_state == "none":

        return "none"

    count = 0

    for index in range(
        current_index,
        -1,
        -1
    ):

        state = get_breakout_state(
            index
        )

        if state == current_state:

            count += 1

        else:

            break

    if current_state == "long":

        return f"long_breakout_{count}"

    if current_state == "short":

        return f"short_breakout_{count}"

    return "none"


# =========================================================
# 🚀 1H 진행 중 캔들 0
# ★ 확정 돌파와 완전히 별도
# =========================================================

def check_breakout_0(
    df1h,
    current1h,
    column
):

    if (
        df1h is None
        or current1h is None
        or current1h.empty
        or len(df1h) < 120 + BREAKOUT_LOOKBACK
    ):

        return "none"

    df = df1h.copy()

    current = current1h.iloc[-1]

    # -----------------------------------------------------
    # 현재 진행 캔들의 가격
    # -----------------------------------------------------

    current_open = pd.to_numeric(
        current["o"],
        errors="coerce"
    )

    current_high = pd.to_numeric(
        current["h"],
        errors="coerce"
    )

    current_low = pd.to_numeric(
        current["l"],
        errors="coerce"
    )

    current_close = pd.to_numeric(
        current["c"],
        errors="coerce"
    )

    if (
        pd.isna(current_open)
        or
        pd.isna(current_high)
        or
        pd.isna(current_low)
        or
        pd.isna(current_close)
    ):

        return "none"

    # -----------------------------------------------------
    # 이전 5개 확정 캔들
    # -----------------------------------------------------

    previous = df.tail(
        BREAKOUT_LOOKBACK
    )

    if len(previous) < BREAKOUT_LOOKBACK:

        return "none"

    previous_high = (
        pd.to_numeric(
            previous["h"],
            errors="coerce"
        ).max()
    )

    previous_low = (
        pd.to_numeric(
            previous["l"],
            errors="coerce"
        ).min()
    )

    if (
        pd.isna(previous_high)
        or
        pd.isna(previous_low)
    ):

        return "none"

    # -----------------------------------------------------
    # 현재 진행 캔들을 포함하여 EMA 계산
    # -----------------------------------------------------

    temp = pd.concat(
        [
            df[[column]],
            pd.DataFrame(
                [
                    {
                        column:
                            current_close
                    }
                ]
            )
        ],
        ignore_index=True
    )

    ema10 = get_ema(
        temp,
        column,
        10
    )

    ema30 = get_ema(
        temp,
        column,
        30
    )

    ema60 = get_ema(
        temp,
        column,
        60
    )

    ema120 = get_ema(
        temp,
        column,
        120
    )

    if (
        ema10 is None
        or
        ema30 is None
        or
        ema60 is None
        or
        ema120 is None
    ):

        return "none"

    cur_ema10 = ema10.iloc[-1]
    cur_ema30 = ema30.iloc[-1]
    cur_ema60 = ema60.iloc[-1]
    cur_ema120 = ema120.iloc[-1]

    # -----------------------------------------------------
    # LONG 0
    # -----------------------------------------------------

    long_10_30 = (
        cur_ema10
        >
        cur_ema30
    )

    long_30_60_120 = (
        cur_ema30
        >
        cur_ema60
        >
        cur_ema120
    )

    long_candle = (
        current_close
        >
        current_open
    )

    # 현재 진행 캔들의 고가가
    # 이전 5개 캔들 최고가에 도달
    long_possible = (
        current_high
        >=
        previous_high
    )

    if (
        long_10_30
        and
        long_30_60_120
        and
        long_candle
        and
        long_possible
    ):

        return "long_breakout_0"

    # -----------------------------------------------------
    # SHORT 0
    # -----------------------------------------------------

    short_10_30 = (
        cur_ema10
        <
        cur_ema30
    )

    short_30_60_120 = (
        cur_ema30
        <
        cur_ema60
        <
        cur_ema120
    )

    short_candle = (
        current_close
        <
        current_open
    )

    # 현재 진행 캔들의 저가가
    # 이전 5개 캔들 최저가에 도달
    short_possible = (
        current_low
        <=
        previous_low
    )

    if (
        short_10_30
        and
        short_30_60_120
        and
        short_candle
        and
        short_possible
    ):

        return "short_breakout_0"

    return "none"


# =========================================================
# ⚡ 4H 확정 돌파
# ★ 기존 로직 그대로 유지
# =========================================================

def check_breakout_4h(
    df4h,
    column
):

    if (
        df4h is None
        or len(df4h)
        <
        120 + BREAKOUT_LOOKBACK
    ):

        return "none"

    df = df4h.copy()

    df["ema10"] = get_ema(
        df,
        column,
        10
    )

    df["ema30"] = get_ema(
        df,
        column,
        30
    )

    df["ema60"] = get_ema(
        df,
        column,
        60
    )

    df["ema120"] = get_ema(
        df,
        column,
        120
    )

    def get_breakout_state_4h(index):

        if index < BREAKOUT_LOOKBACK:

            return "none"

        cur = df.iloc[index]

        previous = df.iloc[
            index - BREAKOUT_LOOKBACK:
            index
        ]

        if previous.empty:

            return "none"

        previous_high = (
            pd.to_numeric(
                previous["h"],
                errors="coerce"
            ).max()
        )

        previous_low = (
            pd.to_numeric(
                previous["l"],
                errors="coerce"
            ).min()
        )

        # LONG
        long_10_30 = (
            cur["ema10"]
            >
            cur["ema30"]
        )

        long_30_60_120 = (
            cur["ema30"]
            >
            cur["ema60"]
            >
            cur["ema120"]
        )

        long_break = (
            cur["c"]
            >
            previous_high
        )

        long_candle = (
            cur["c"]
            >
            cur["o"]
        )

        if (
            long_10_30
            and
            long_30_60_120
            and
            long_break
            and
            long_candle
        ):

            return "long"

        # SHORT
        short_10_30 = (
            cur["ema10"]
            <
            cur["ema30"]
        )

        short_30_60_120 = (
            cur["ema30"]
            <
            cur["ema60"]
            <
            cur["ema120"]
        )

        short_break = (
            cur["c"]
            <
            previous_low
        )

        short_candle = (
            cur["c"]
            <
            cur["o"]
        )

        if (
            short_10_30
            and
            short_30_60_120
            and
            short_break
            and
            short_candle
        ):

            return "short"

        return "none"

    current_index = len(df) - 1

    current_state = get_breakout_state_4h(
        current_index
    )

    if current_state == "none":

        return "none"

    count = 0

    for index in range(
        current_index,
        -1,
        -1
    ):

        state = get_breakout_state_4h(
            index
        )

        if state == current_state:

            count += 1

        else:

            break

    if current_state == "long":

        return f"long_breakout_4h_{count}"

    if current_state == "short":

        return f"short_breakout_4h_{count}"

    return "none"


# =========================================================
# ⚡ 4H 진행 중 캔들 0
# =========================================================

def check_breakout_4h_0(
    df4h,
    current4h,
    column
):

    if (
        df4h is None
        or current4h is None
        or current4h.empty
        or len(df4h) < 120 + BREAKOUT_LOOKBACK
    ):

        return "none"

    df = df4h.copy()

    current = current4h.iloc[-1]

    # -----------------------------------------------------
    # 현재 진행 캔들
    # -----------------------------------------------------

    current_open = pd.to_numeric(
        current["o"],
        errors="coerce"
    )

    current_high = pd.to_numeric(
        current["h"],
        errors="coerce"
    )

    current_low = pd.to_numeric(
        current["l"],
        errors="coerce"
    )

    current_close = pd.to_numeric(
        current["c"],
        errors="coerce"
    )

    if (
        pd.isna(current_open)
        or
        pd.isna(current_high)
        or
        pd.isna(current_low)
        or
        pd.isna(current_close)
    ):

        return "none"

    # -----------------------------------------------------
    # 이전 5개 확정 4H 캔들
    # -----------------------------------------------------

    previous = df.tail(
        BREAKOUT_LOOKBACK
    )

    if len(previous) < BREAKOUT_LOOKBACK:

        return "none"

    previous_high = (
        pd.to_numeric(
            previous["h"],
            errors="coerce"
        ).max()
    )

    previous_low = (
        pd.to_numeric(
            previous["l"],
            errors="coerce"
        ).min()
    )

    if (
        pd.isna(previous_high)
        or
        pd.isna(previous_low)
    ):

        return "none"

    # -----------------------------------------------------
    # 현재 진행 가격을 포함한 EMA
    # -----------------------------------------------------

    temp = pd.concat(
        [
            df[[column]],
            pd.DataFrame(
                [
                    {
                        column:
                            current_close
                    }
                ]
            )
        ],
        ignore_index=True
    )

    ema10 = get_ema(
        temp,
        column,
        10
    )

    ema30 = get_ema(
        temp,
        column,
        30
    )

    ema60 = get_ema(
        temp,
        column,
        60
    )

    ema120 = get_ema(
        temp,
        column,
        120
    )

    if (
        ema10 is None
        or
        ema30 is None
        or
        ema60 is None
        or
        ema120 is None
    ):

        return "none"

    cur_ema10 = ema10.iloc[-1]
    cur_ema30 = ema30.iloc[-1]
    cur_ema60 = ema60.iloc[-1]
    cur_ema120 = ema120.iloc[-1]

    # -----------------------------------------------------
    # LONG 0
    # -----------------------------------------------------

    long_10_30 = (
        cur_ema10
        >
        cur_ema30
    )

    long_30_60_120 = (
        cur_ema30
        >
        cur_ema60
        >
        cur_ema120
    )

    long_candle = (
        current_close
        >
        current_open
    )

    long_possible = (
        current_high
        >=
        previous_high
    )

    if (
        long_10_30
        and
        long_30_60_120
        and
        long_candle
        and
        long_possible
    ):

        return "long_breakout_4h_0"

    # -----------------------------------------------------
    # SHORT 0
    # -----------------------------------------------------

    short_10_30 = (
        cur_ema10
        <
        cur_ema30
    )

    short_30_60_120 = (
        cur_ema30
        <
        cur_ema60
        <
        cur_ema120
    )

    short_candle = (
        current_close
        <
        current_open
    )

    short_possible = (
        current_low
        <=
        previous_low
    )

    if (
        short_10_30
        and
        short_30_60_120
        and
        short_candle
        and
        short_possible
    ):

        return "short_breakout_4h_0"

    return "none"


# =========================================================
# 최종 경고
# =========================================================

def check_entry_warning(
    df1h,
    df4h,
    current1h,
    current4h,
    column
):

    breakout_1h = check_breakout(
        df1h,
        column
    )

    breakout_4h = check_breakout_4h(
        df4h,
        column
    )

    # -----------------------------------------------------
    # 확정 돌파가 없을 때만 0 검사
    # -----------------------------------------------------

    if breakout_1h == "none":

        breakout_1h_0 = check_breakout_0(
            df1h,
            current1h,
            column
        )

        if breakout_1h_0 != "none":

            breakout_1h = breakout_1h_0

    if breakout_4h == "none":

        breakout_4h_0 = check_breakout_4h_0(
            df4h,
            current4h,
            column
        )

        if breakout_4h_0 != "none":

            breakout_4h = breakout_4h_0

    return (
        breakout_1h,
        breakout_4h
    )


# =========================================================
# LONG / SHORT
# ★ 0은 신호로 사용하지 않음
# =========================================================

def get_trade_signal(
    df1h,
    df4h,
    current1h,
    current4h,
    column
):

    main_direction = get_main_direction(
        df1h,
        df4h,
        column
    )

    if main_direction == "none":

        return "", "none", "none"

    breakout_1h, breakout_4h = (
        check_entry_warning(
            df1h,
            df4h,
            current1h,
            current4h,
            column
        )
    )

    signal = ""

    # -----------------------------------------------------
    # 0은 아직 확정이 아니므로 신호 발생 안 함
    # -----------------------------------------------------

    if (
        main_direction == "long"
        and
        breakout_1h.startswith(
            "long_breakout_"
        )
        and
        breakout_1h != "long_breakout_0"
    ):

        signal = "LONG"

    elif (
        main_direction == "short"
        and
        breakout_1h.startswith(
            "short_breakout_"
        )
        and
        breakout_1h != "short_breakout_0"
    ):

        signal = "SHORT"

    return (
        signal,
        breakout_1h,
        breakout_4h
    )


# =========================================================
# OKX EMA
# =========================================================

def get_okx_ema(
    inst_id
):

    # -----------------------------------------------------
    # 확정 캔들
    # -----------------------------------------------------

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

    # -----------------------------------------------------
    # 현재 진행 캔들
    # -----------------------------------------------------

    current1h = get_okx_current_ohlcv(
        inst_id,
        "1H",
        2
    )

    current4h = get_okx_current_ohlcv(
        inst_id,
        "4H",
        2
    )

    if (
        df1h is None
        or df4h is None
    ):

        return {
            "1h_10_30": "⚪",
            "1h_30_60_120": "⚪",
            "4h_10_30": "⚪",
            "4h_30_60_120": "⚪",
            "signal": "",
            "warning": "none",
            "warning_1h": "none",
            "warning_4h": "none",
            "direction": "none"
        }

    signal, warning_1h, warning_4h = (
        get_trade_signal(
            df1h,
            df4h,
            current1h,
            current4h,
            "c"
        )
    )

    direction = get_main_direction(
        df1h,
        df4h,
        "c"
    )

    return {

        "1h_10_30":
            check_ema_10_30(
                df1h,
                "c"
            ),

        "1h_30_60_120":
            check_ema(
                df1h,
                "c"
            ),

        "4h_10_30":
            check_ema_10_30(
                df4h,
                "c"
            ),

        "4h_30_60_120":
            check_ema(
                df4h,
                "c"
            ),

        "signal":
            signal,

        "warning":
            warning_1h,

        "warning_1h":
            warning_1h,

        "warning_4h":
            warning_4h,

        "direction":
            direction
    }


# =========================================================
# 업비트 EMA
# =========================================================

def get_upbit_ema(
    market
):

    # -----------------------------------------------------
    # 현재 포함 원본
    # -----------------------------------------------------

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
        or raw4h is None
    ):

        return {
            "1h_10_30": "⚪",
            "1h_30_60_120": "⚪",
            "4h_10_30": "⚪",
            "4h_30_60_120": "⚪",
            "signal": "",
            "warning": "none",
            "warning_1h": "none",
            "warning_4h": "none",
            "direction": "none"
        }

    # -----------------------------------------------------
    # 현재 진행 캔들
    # -----------------------------------------------------

    current1h = raw1h.tail(
        1
    ).copy()

    current4h = raw4h.tail(
        1
    ).copy()

    # -----------------------------------------------------
    # 확정 캔들
    # -----------------------------------------------------

    df1h = raw1h.copy()

    df4h = raw4h.copy()

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

    signal, warning_1h, warning_4h = (
        get_trade_signal(
            df1h,
            df4h,
            current1h,
            current4h,
            "c"
        )
    )

    direction = get_main_direction(
        df1h,
        df4h,
        "c"
    )

    return {

        "1h_10_30":
            check_ema_10_30(
                df1h,
                "c"
            ),

        "1h_30_60_120":
            check_ema(
                df1h,
                "c"
            ),

        "4h_10_30":
            check_ema_10_30(
                df4h,
                "c"
            ),

        "4h_30_60_120":
            check_ema(
                df4h,
                "c"
            ),

        "signal":
            signal,

        "warning":
            warning_1h,

        "warning_1h":
            warning_1h,

        "warning_4h":
            warning_4h,

        "direction":
            direction
    }


# =========================================================
# OKX 거래대금
# =========================================================

def get_okx_volume(
    inst_id
):

    hours = max(
        1,
        min(
            int(VOLUME_HOURS),
            200
        )
    )

    if hours == 1:

        df = get_okx_ohlcv(
            inst_id,
            "1m",
            61
        )

        if (
            df is None
            or df.empty
        ):

            return 0

        volume = float(
            df["volCcyQuote"]
            .tail(60)
            .sum()
        )

        return volume / 10

    df = get_okx_ohlcv(
        inst_id,
        "1H",
        hours + 1
    )

    if (
        df is None
        or df.empty
    ):

        return 0

    volume = float(
        df["volCcyQuote"]
        .tail(hours)
        .sum()
    )

    return volume / 10


# =========================================================
# 업비트 거래대금
# =========================================================

def get_upbit_volume(
    market
):

    hours = max(
        1,
        min(
            int(VOLUME_HOURS),
            200
        )
    )

    if hours == 1:

        df = get_upbit_ohlcv(
            market,
            1,
            60
        )

        if (
            df is None
            or df.empty
        ):

            return 0

        return float(
            df[
                "candle_acc_trade_price"
            ]
            .fillna(0)
            .tail(60)
            .sum()
        )

    df = get_upbit_ohlcv(
        market,
        60,
        hours
    )

    if (
        df is None
        or df.empty
    ):

        return 0

    return float(
        df[
            "candle_acc_trade_price"
        ]
        .fillna(0)
        .tail(hours)
        .sum()
    )


# =========================================================
# 업비트 거래대금 MAP
# =========================================================

def get_upbit_volume_map(
    markets
):

    if not markets:

        return {}

    volume_map = {}

    total = len(markets)

    logging.info(
        f"업비트 거래대금 전체 "
        f"{total}개 처리 시작"
    )

    success = 0
    failed = 0

    for index, market in enumerate(
        markets,
        start=1
    ):

        try:

            volume = get_upbit_volume(
                market
            )

            volume_map[market] = volume

            if volume > 0:

                success += 1

            else:

                failed += 1

        except Exception as e:

            failed += 1

            volume_map[market] = 0

            logging.error(
                f"업비트 거래대금 실패 "
                f"{market} : {e}"
            )

        if (
            index % 25 == 0
            or
            index == total
        ):

            logging.info(
                f"업비트 거래대금 "
                f"{index}/{total} "
                f"(성공 {success} / 실패 {failed})"
            )

    logging.info(
        f"업비트 거래대금 전체 처리 완료 "
        f"{total}/{total}"
    )

    return volume_map


# =========================================================
# OKX 변동률
# =========================================================

def get_okx_change(
    inst_id
):

    df = get_okx_ohlcv(
        inst_id,
        "1H",
        120
    )

    if (
        df is None
        or len(df) < 50
    ):

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

    for i in [
        -1,
        -2,
        -3
    ]:

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
            round(
                change,
                2
            )
        )

    return result


# =========================================================
# 업비트 변동률
# =========================================================

def get_upbit_change(
    market
):

    df = get_upbit_ohlcv(
        market,
        60,
        120
    )

    if (
        df is None
        or len(df) < 50
    ):

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

    for i in [
        -1,
        -2,
        -3
    ]:

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
            round(
                change,
                2
            )
        )

    return result


# =========================================================
# 변동률
# =========================================================

def format_change(
    changes
):

    if (
        changes is None
        or len(changes) == 0
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
        f'<span class="change-item">'
        f'<span class="change-icon">{icon}</span>'
        f'<span class="change-value">'
        f'{sign}{x:.2f}%'
        f'</span>'
        f'</span>'
    )


# =========================================================
# LONG / SHORT
# =========================================================

def signal_html(
    signal,
    warning,
    change_percent
):

    if (
        signal == "LONG"
        and
        change_percent is not None
        and
        change_percent > 0
    ):

        return (
            '<span class="signal-text long-text">'
            'LONG'
            '</span>'
        )

    if (
        signal == "SHORT"
        and
        change_percent is not None
        and
        change_percent < 0
    ):

        return (
            '<span class="signal-text short-text">'
            'SHORT'
            '</span>'
        )

    return (
        '<span class="signal-none">'
        '—'
        '</span>'
    )


# =========================================================
# 방향 HTML
# =========================================================

def direction_html(
    direction,
    change_percent
):

    if (
        direction == "long"
        and
        change_percent is not None
        and
        change_percent > 0
    ):

        return (
            '<span class="direction-long">'
            '☀️'
            '</span>'
        )

    if (
        direction == "short"
        and
        change_percent is not None
        and
        change_percent < 0
    ):

        return (
            '<span class="direction-short">'
            '🌧'
            '</span>'
        )

    return (
        '<span class="direction-none">'
        '—'
        '</span>'
    )


# =========================================================
# 경고 HTML
# ★ 0 포함
# =========================================================

def warning_html(
    warning_1h,
    warning_4h,
    change_percent
):

    html = (
        '<div class="warning-wrap">'
    )

    # =====================================================
    # 1H 🚀
    # =====================================================

    html += (
        '<div class="warning-row">'
        '<span class="warning-period">1H</span>'
    )

    if (
        warning_1h.startswith(
            "long_breakout_"
        )
        or
        warning_1h.startswith(
            "short_breakout_"
        )
    ):

        valid_1h = False

        # -------------------------------------------------
        # 0
        # -------------------------------------------------

        if warning_1h in (
            "long_breakout_0",
            "short_breakout_0"
        ):

            valid_1h = True

        # -------------------------------------------------
        # 기존 확정 LONG
        # -------------------------------------------------

        elif (
            warning_1h.startswith(
                "long_breakout_"
            )
            and
            change_percent is not None
            and
            change_percent > 0
        ):

            valid_1h = True

        # -------------------------------------------------
        # 기존 확정 SHORT
        # -------------------------------------------------

        elif (
            warning_1h.startswith(
                "short_breakout_"
            )
            and
            change_percent is not None
            and
            change_percent < 0
        ):

            valid_1h = True

        if valid_1h:

            try:

                count = int(
                    warning_1h.split("_")[-1]
                )

            except Exception:

                count = 0

            html += (
                '<span class="warning-icon rocket">'
                f'🚀({count})'
                '</span>'
            )

        else:

            html += (
                '<span class="warning-empty">'
                '—'
                '</span>'
            )

    else:

        html += (
            '<span class="warning-empty">'
            '—'
            '</span>'
        )

    html += '</div>'


    # =====================================================
    # 4H ⚡
    # =====================================================

    html += (
        '<div class="warning-row">'
        '<span class="warning-period">4H</span>'
    )

    if (
        warning_4h.startswith(
            "long_breakout_4h_"
        )
        or
        warning_4h.startswith(
            "short_breakout_4h_"
        )
    ):

        valid_4h = False

        # -------------------------------------------------
        # 0
        # -------------------------------------------------

        if warning_4h in (
            "long_breakout_4h_0",
            "short_breakout_4h_0"
        ):

            valid_4h = True

        # -------------------------------------------------
        # 기존 확정 LONG
        # -------------------------------------------------

        elif (
            warning_4h.startswith(
                "long_breakout_4h_"
            )
            and
            change_percent is not None
            and
            change_percent > 0
        ):

            valid_4h = True

        # -------------------------------------------------
        # 기존 확정 SHORT
        # -------------------------------------------------

        elif (
            warning_4h.startswith(
                "short_breakout_4h_"
            )
            and
            change_percent is not None
            and
            change_percent < 0
        ):

            valid_4h = True

        if valid_4h:

            try:

                count = int(
                    warning_4h.split("_")[-1]
                )

            except Exception:

                count = 0

            html += (
                '<span class="warning-icon lightning">'
                f'⚡({count})'
                '</span>'
            )

        else:

            html += (
                '<span class="warning-empty">'
                '—'
                '</span>'
            )

    else:

        html += (
            '<span class="warning-empty">'
            '—'
            '</span>'
        )

    html += '</div>'

    html += '</div>'

    return html


# =========================================================
# EMA HTML
# =========================================================

def ema_html(
    ema
):

    return f"""

<div class="ema-box">

    <div class="ema-row">

        <span class="ema-period">
            1H
        </span>

        <span class="ema-value">
            {ema.get("1h_10_30", "⚪")}
        </span>

        <span class="ema-value">
            {ema.get("1h_30_60_120", "⚪")}
        </span>

    </div>

    <div class="ema-row ema-day">

        <span class="ema-period">
            4H
        </span>

        <span class="ema-value">
            {ema.get("4h_10_30", "⚪")}
        </span>

        <span class="ema-value">
            {ema.get("4h_30_60_120", "⚪")}
        </span>

    </div>

</div>

"""


# =========================================================
# 업비트 TOP
# =========================================================

def update_upbit():

    global latest_upbit_data

    logging.info(
        f"업비트 TOP{TOP_N} 시작 "
        f"(거래대금 {VOLUME_HOURS}시간)"
    )

    markets = get_upbit_markets()

    if not markets:

        logging.error(
            "업비트 마켓 목록 조회 실패"
        )

        return

    total_markets = len(markets)

    logging.info(
        f"업비트 전체 {total_markets}개 "
        f"거래대금 계산 시작"
    )

    volume_map = get_upbit_volume_map(
        markets
    )

    if not volume_map:

        logging.error(
            "업비트 거래대금 계산 실패"
        )

        return

    top_markets = sorted(
        volume_map,
        key=volume_map.get,
        reverse=True
    )[:TOP_N]

    rows = []

    total_top = len(top_markets)

    logging.info(
        f"업비트 TOP{total_top} 상세 조회 시작"
    )

    success_detail = 0
    failed_detail = 0

    for rank, market in enumerate(
        top_markets,
        start=1
    ):

        try:

            coin = market.replace(
                "KRW-",
                ""
            )

            changes = get_upbit_change(
                market
            )

            ema = get_upbit_ema(
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

            rows.append({

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

                "ema": ema

            })

            success_detail += 1

        except Exception as e:

            failed_detail += 1

            logging.error(
                f"업비트 TOP 상세 오류 "
                f"{market} : {e}"
            )

            rows.append({

                "rank": rank,

                "name": coin,

                "change": "N/A",

                "change_percent": None,

                "volume":
                    format_volume(
                        volume_map.get(
                            market,
                            0
                        )
                    ),

                "ema": {
                    "1h_10_30": "⚪",
                    "1h_30_60_120": "⚪",
                    "4h_10_30": "⚪",
                    "4h_30_60_120": "⚪",
                    "signal": "",
                    "warning": "none",
                    "warning_1h": "none",
                    "warning_4h": "none",
                    "direction": "none"
                }

            })

        if (
            rank % 5 == 0
            or
            rank == total_top
        ):

            logging.info(
                f"업비트 TOP 상세 "
                f"{rank}/{total_top} "
                f"(성공 {success_detail} / "
                f"실패 {failed_detail})"
            )

    latest_upbit_data = rows

    logging.info(
        f"업비트 TOP{TOP_N} 상세 조회 완료 "
        f"({total_top}/{total_top})"
    )

    logging.info(
        f"업비트 TOP{TOP_N} 완료 "
        f"(전체 {total_markets}/{total_markets} 처리)"
    )


# =========================================================
# OKX TOP
# =========================================================

def update_okx():

    global latest_okx_data

    logging.info(
        f"OKX TOP{TOP_N} 시작 "
        f"(거래대금 {VOLUME_HOURS}시간 / 최종 ÷10)"
    )

    symbols = get_all_okx_swap_symbols()

    if not symbols:

        logging.error(
            "OKX 종목 목록 조회 실패"
        )

        return

    total_symbols = len(symbols)

    logging.info(
        f"OKX 전체 {total_symbols}개 "
        f"거래대금 계산 시작"
    )

    usdt_krw = get_usdt_krw()

    upbit_markets = get_upbit_markets()

    upbit_coin_set = {

        market.replace(
            "KRW-",
            ""
        )

        for market in upbit_markets

    }

    volume_map = {}

    success = 0
    failed = 0

    for index, symbol in enumerate(
        symbols,
        start=1
    ):

        try:

            volume_usdt = get_okx_volume(
                symbol
            )

            volume_krw = (
                volume_usdt
                *
                usdt_krw
            )

            volume_map[symbol] = volume_krw

            if volume_usdt > 0:

                success += 1

            else:

                failed += 1

        except Exception as e:

            failed += 1

            volume_map[symbol] = 0

            logging.error(
                f"OKX 거래대금 실패 "
                f"{symbol} : {e}"
            )

        if (
            index % 25 == 0
            or
            index == total_symbols
        ):

            logging.info(
                f"OKX 거래대금 "
                f"{index}/{total_symbols} "
                f"(성공 {success} / 실패 {failed})"
            )

    logging.info(
        f"OKX 거래대금 전체 처리 완료 "
        f"{total_symbols}/{total_symbols}"
    )

    top_symbols = sorted(
        volume_map,
        key=volume_map.get,
        reverse=True
    )[:TOP_N]

    rows = []

    total_top = len(top_symbols)

    logging.info(
        f"OKX TOP{total_top} 상세 조회 시작"
    )

    success_detail = 0
    failed_detail = 0

    for rank, symbol in enumerate(
        top_symbols,
        start=1
    ):

        coin = symbol.replace(
            "-USDT-SWAP",
            ""
        )

        if coin in upbit_coin_set:

            coin = f"{coin}(업비트)"

        try:

            changes = get_okx_change(
                symbol
            )

            ema = get_okx_ema(
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

            rows.append({

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

                "ema": ema

            })

            success_detail += 1

        except Exception as e:

            failed_detail += 1

            logging.error(
                f"OKX TOP 상세 오류 "
                f"{symbol} : {e}"
            )

            rows.append({

                "rank": rank,

                "name": coin,

                "change": "N/A",

                "change_percent": None,

                "volume":
                    format_volume(
                        volume_map.get(
                            symbol,
                            0
                        )
                    ),

                "ema": {
                    "1h_10_30": "⚪",
                    "1h_30_60_120": "⚪",
                    "4h_10_30": "⚪",
                    "4h_30_60_120": "⚪",
                    "signal": "",
                    "warning": "none",
                    "warning_1h": "none",
                    "warning_4h": "none",
                    "direction": "none"
                }

            })

        if (
            rank % 5 == 0
            or
            rank == total_top
        ):

            logging.info(
                f"OKX TOP 상세 "
                f"{rank}/{total_top} "
                f"(성공 {success_detail} / "
                f"실패 {failed_detail})"
            )

    latest_okx_data = rows

    logging.info(
        f"OKX TOP{total_top} 상세 조회 완료 "
        f"({total_top}/{total_top})"
    )

    logging.info(
        f"OKX TOP{TOP_N} 완료 "
        f"(전체 {total_symbols}/{total_symbols} 처리)"
    )


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    logging.info(
        "========================================"
    )

    logging.info(
        "전체 조회 시작"
    )

    try:

        update_upbit()

    except Exception as e:

        logging.exception(
            f"업비트 업데이트 오류 : {e}"
        )

    try:

        update_okx()

    except Exception as e:

        logging.exception(
            f"OKX 업데이트 오류 : {e}"
        )

    logging.info(
        "전체 업데이트 완료"
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
# 웹 대시보드
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def dashboard():

    html = """

<html>

<head>

<meta
    http-equiv="refresh"
    content="300"
>

<meta
    name="viewport"
    content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no"
>

<title>
1H / 4H 차트 집중
</title>

<style>

*{

    box-sizing:border-box;

    -webkit-tap-highlight-color:
        transparent;

}

html,
body{

    width:100%;

    margin:0;

    padding:0;

    overflow-x:hidden;

}

body{

    background:#101010;

    color:#e8e8e8;

    font-family:
        Arial,
        sans-serif;

    padding:2px;

    font-size:8px;

}


.main-title{

    margin:1px 0 2px;

    font-size:12px;

    line-height:13px;

    font-weight:bold;

}

.title-time{

    color:#777;

    font-size:8px;

    font-weight:normal;

}


.description{

    color:#777;

    font-size:6px;

    line-height:7px;

    white-space:nowrap;

    overflow:hidden;

    text-overflow:ellipsis;

    margin-bottom:2px;

}


.setting-row{

    display:flex;

    gap:2px;

    margin-bottom:3px;

}

.volume-setting{

    padding:1px 3px;

    background:#1b1b1b;

    border:1px solid #303030;

    border-radius:3px;

    color:#888;

    font-size:6px;

    line-height:7px;

    white-space:nowrap;

}


.section-title{

    margin:4px 0 2px;

    padding:2px 4px;

    background:#1d1d1d;

    border-left:2px solid #777;

    color:#ddd;

    font-size:8px;

    line-height:9px;

    font-weight:bold;

}


.table-wrap{

    width:100%;

    overflow:hidden;

}

table{

    width:100%;

    table-layout:fixed;

    border-collapse:collapse;

    border:1px solid #292929;

}


th{

    background:#252525;

    color:#aaa;

    padding:1px;

    height:16px;

    border-right:1px solid #333;

    border-bottom:1px solid #333;

    white-space:nowrap;

    font-size:6px;

    line-height:7px;

    font-weight:bold;

}


td{

    padding:0;

    height:25px;

    border-bottom:1px solid #292929;

    border-right:1px solid #252525;

    text-align:center;

    vertical-align:middle;

    white-space:nowrap;

    font-size:7px;

}


.rank-cell{

    width:5%;

    color:#777;

    font-size:6px;

}


.coin-cell{

    width:20%;

    text-align:left;

    padding-left:2px;

    color:#eee;

    font-weight:bold;

    font-size:7px;

    overflow:hidden;

    text-overflow:ellipsis;

}


.volume-cell{

    width:20%;

    text-align:center;

    color:#bbb;

    font-size:6.5px;

}


.change-cell{

    width:17%;

    font-size:6.5px;

}


.ema-cell{

    width:38%;

    padding:0;

}


.coin-wrap{

    width:100%;

    min-height:25px;

    display:flex;

    flex-direction:column;

    justify-content:center;

    line-height:1;

}

.coin-name{

    height:12px;

    display:flex;

    align-items:center;

    overflow:hidden;

    white-space:nowrap;

}

.coin-sub{

    height:12px;

    display:flex;

    align-items:center;

    overflow:hidden;

    white-space:nowrap;

}


.volume-wrap{

    width:100%;

    min-height:25px;

    display:flex;

    flex-direction:column;

    justify-content:center;

}

.volume-main{

    height:12px;

    display:flex;

    align-items:center;

    justify-content:center;

    color:#bbb;

    font-size:6.5px;

    font-family:monospace;

}

.volume-sub{

    height:12px;

    display:flex;

    align-items:center;

    justify-content:center;

}


.change-wrap{

    width:100%;

    min-height:25px;

    display:flex;

    flex-direction:column;

    justify-content:center;

}

.change-main{

    height:12px;

    display:flex;

    align-items:center;

    justify-content:center;

}

.change-sub{

    height:12px;

    display:flex;

    align-items:center;

    justify-content:center;

}


.change-item{

    display:flex;

    align-items:center;

    justify-content:center;

    gap:2px;

    width:100%;

}

.change-icon{

    font-size:5.5px;

    line-height:7px;

}

.change-value{

    font-family:monospace;

    font-size:6.5px;

    line-height:8px;

}


.signal-text{

    display:inline-flex;

    align-items:center;

    justify-content:center;

    min-width:30px;

    padding:0 3px;

    border-radius:3px;

    font-size:6.5px;

    line-height:9px;

    font-weight:900;

    letter-spacing:.2px;

}

.long-text{

    color:#00ff7f;

    background:rgba(0,230,118,.12);

    border:1px solid rgba(0,230,118,.28);

}

.short-text{

    color:#ff5252;

    background:rgba(255,82,82,.12);

    border:1px solid rgba(255,82,82,.28);

}


.warning-wrap{

    width:100%;

    display:flex;

    flex-direction:column;

    justify-content:center;

}

.warning-row{

    height:10px;

    display:flex;

    align-items:center;

    justify-content:center;

    gap:1px;

    white-space:nowrap;

}

.warning-period{

    color:#666;

    font-size:5px;

    line-height:7px;

    min-width:9px;

}

.warning-icon{

    font-size:6.5px;

    line-height:9px;

    font-weight:bold;

}

.warning-icon.rocket{

    color:#fff;

}

.warning-icon.lightning{

    color:#fff;

}

.warning-empty{

    color:#444;

    font-size:6px;

    line-height:8px;

}


.signal-none{

    color:#444;

    font-size:7px;

}


.direction-long{

    font-size:8px;

    font-weight:bold;

    filter:
        drop-shadow(
            0 0 3px
            rgba(255,213,79,.35)
        );

}

.direction-short{

    font-size:8px;

    font-weight:bold;

    filter:
        drop-shadow(
            0 0 3px
            rgba(144,202,249,.35)
        );

}

.direction-none{

    color:#444;

    font-size:7px;

}


.ema-box{

    width:100%;

    min-height:25px;

    display:flex;

    flex-direction:column;

    justify-content:center;

}

.ema-row{

    width:100%;

    height:12px;

    display:flex;

    align-items:center;

    overflow:hidden;

    white-space:nowrap;

}

.ema-day{

    border-top:1px solid #242424;

}


.ema-period{

    width:17px;

    min-width:17px;

    color:#777;

    text-align:left;

    font-size:5.8px;

    font-weight:bold;

}


.ema-value{

    flex:1;

    min-width:0;

    color:#aaa;

    text-align:left;

    font-size:5.3px;

    line-height:7px;

    overflow:hidden;

    white-space:nowrap;

}


@media(
    max-width:600px
){

    body{

        padding:2px;

    }

    .main-title{

        font-size:12px;

        line-height:13px;

        margin:1px 0 2px;

    }

    .title-time{

        font-size:8px;

    }

    .description{

        font-size:6px;

        line-height:7px;

        margin-bottom:2px;

    }

    .setting-row{

        gap:2px;

        margin-bottom:3px;

    }

    .volume-setting{

        padding:1px 3px;

        font-size:6px;

        line-height:7px;

    }

    .section-title{

        margin:4px 0 2px;

        padding:2px 4px;

        font-size:8px;

        line-height:9px;

    }

    th{

        height:16px;

        padding:1px;

        font-size:6px;

        line-height:7px;

    }

    td{

        padding:0;

        height:25px;

    }

    .rank-cell{

        font-size:6px;

    }

    .coin-cell{

        width:20%;

        font-size:7px;

        padding-left:2px;

    }

    .volume-cell{

        width:20%;

        font-size:6.5px;

    }

    .change-cell{

        width:17%;

        font-size:6.5px;

    }

    .ema-cell{

        width:38%;

        padding:0;

    }

    .coin-wrap,
    .volume-wrap,
    .change-wrap,
    .ema-box{

        min-height:25px;

    }

    .coin-name,
    .volume-main,
    .change-main,
    .ema-row{

        height:12px;

    }

    .coin-sub,
    .volume-sub,
    .change-sub{

        height:12px;

    }

    .volume-main{

        font-size:6.5px;

    }

    .change-icon{

        font-size:5.5px;

    }

    .change-value{

        font-size:6.5px;

    }

    .signal-text{

        min-width:30px;

        padding:0 3px;

        font-size:6.5px;

        line-height:9px;

    }

    .warning-icon{

        font-size:6.5px;

        line-height:9px;

    }

    .warning-row{

        height:10px;

    }

    .warning-period{

        font-size:5px;

        min-width:9px;

    }

    .direction-long,
    .direction-short{

        font-size:8px;

    }

    .ema-period{

        width:17px;

        min-width:17px;

        font-size:5.8px;

    }

    .ema-value{

        font-size:5.3px;

        line-height:7px;

    }

}


@media(
    max-width:380px
){

    .coin-cell{

        width:20%;

        font-size:6.7px;

    }

    .volume-cell{

        width:20%;

    }

    .volume-main{

        font-size:6px;

    }

    .change-cell{

        width:17%;

    }

    .change-value{

        font-size:6px;

    }

    .ema-cell{

        width:38%;

    }

    .ema-period{

        width:16px;

        min-width:16px;

        font-size:5.5px;

    }

    .ema-value{

        font-size:5px;

    }

    .signal-text{

        font-size:6px;

        min-width:29px;

    }

    .warning-icon{

        font-size:6px;

    }

    .warning-period{

        font-size:4.8px;

        min-width:8px;

    }

}

</style>

</head>


<body>


<div class="main-title">
📊 1H / 4H 차트 집중
</div>


<div class="description">
1H 추세 방향 + 1H 🚀 돌파 + 4H ⚡ 돌파 | 0=진행중 가능성 | 1+=확정 돌파
</div>


<div class="setting-row">

<span class="volume-setting">
거래대금 """ + str(VOLUME_HOURS) + """H
</span>

<span class="volume-setting">
OKX ÷10
</span>

<span class="volume-setting">
TOP""" + str(TOP_N) + """
</span>

<span class="volume-setting">
10-30-60-120
</span>

</div>


<!-- =====================================================
     업비트
     ===================================================== -->

<div class="section-title">

🏆 업비트 돌파 TOP""" + str(TOP_N) + """

</div>


<div class="table-wrap">

<table>

<colgroup>

<col style="width:5%">
<col style="width:20%">
<col style="width:20%">
<col style="width:17%">
<col style="width:38%">

</colgroup>


<tr>

<th>
#
</th>

<th>
코인
</th>

<th>
거래대금
</th>

<th>
오늘
</th>

<th>
1시간 / 4시간
</th>

</tr>

"""

    # =====================================================
    # 업비트 화면
    # =====================================================

    for item in latest_upbit_data:

        ema = item["ema"]

        direction = ema.get(
            "direction",
            "none"
        )

        change_percent = item.get(
            "change_percent",
            None
        )

        warning_1h = ema.get(
            "warning_1h",
            ema.get(
                "warning",
                "none"
            )
        )

        warning_4h = ema.get(
            "warning_4h",
            "none"
        )

        # -------------------------------------------------
        # 기존 확정 1H
        # -------------------------------------------------

        valid_1h = (

            (
                direction == "long"
                and
                change_percent is not None
                and
                change_percent > 0
                and
                warning_1h.startswith(
                    "long_breakout_"
                )
            )

            or

            (
                direction == "short"
                and
                change_percent is not None
                and
                change_percent < 0
                and
                warning_1h.startswith(
                    "short_breakout_"
                )
            )

            or

            # 진행 중 0
            warning_1h in (
                "long_breakout_0",
                "short_breakout_0"
            )

        )

        # -------------------------------------------------
        # 기존 확정 4H
        # -------------------------------------------------

        valid_4h = (

            (
                warning_4h.startswith(
                    "long_breakout_4h_"
                )
                and
                change_percent is not None
                and
                change_percent > 0
            )

            or

            (
                warning_4h.startswith(
                    "short_breakout_4h_"
                )
                and
                change_percent is not None
                and
                change_percent < 0
            )

            or

            # 진행 중 0
            warning_4h in (
                "long_breakout_4h_0",
                "short_breakout_4h_0"
            )

        )

        show_item = (
            valid_1h
            or
            valid_4h
        )

        if not show_item:

            continue


        html += f"""

<tr>

<td class="rank-cell">

<div class="coin-wrap">

<div class="coin-name">
{item['rank']}
</div>

<div class="coin-sub">
</div>

</div>

</td>


<td class="coin-cell">

<div class="coin-wrap">

<div class="coin-name">
{item['name']}
</div>

<div class="coin-sub">

{direction_html(
    ema.get(
        "direction",
        "none"
    ),
    item.get(
        "change_percent",
        None
    )
)}

</div>

</div>

</td>


<td class="volume-cell">

<div class="volume-wrap">

<div class="volume-main">
{item['volume']}
</div>

<div class="volume-sub">

{signal_html(
    ema.get(
        "signal",
        ""
    ),
    warning_1h,
    item.get(
        "change_percent",
        None
    )
)}

</div>

</div>

</td>


<td class="change-cell">

<div class="change-wrap">

<div class="change-main">
{item['change']}
</div>

<div class="change-sub">

"""

        html += warning_html(
            warning_1h,
            warning_4h,
            item.get(
                "change_percent",
                None
            )
        )

        html += """

</div>

</div>

</td>


<td class="ema-cell">

"""

        html += ema_html(
            ema
        )

        html += """

</td>

</tr>

"""

    html += """

</table>

</div>


<!-- =====================================================
     OKX
     ===================================================== -->

<div class="section-title">

🏆 OKX 돌파 TOP""" + str(TOP_N) + """

</div>


<div class="table-wrap">

<table>

<colgroup>

<col style="width:5%">
<col style="width:20%">
<col style="width:20%">
<col style="width:17%">
<col style="width:38%">

</colgroup>


<tr>

<th>
#
</th>

<th>
코인
</th>

<th>
거래대금
</th>

<th>
오늘
</th>

<th>
1시간 / 4시간
</th>

</tr>

"""

    # =====================================================
    # OKX 화면
    # =====================================================

    for item in latest_okx_data:

        ema = item["ema"]

        direction = ema.get(
            "direction",
            "none"
        )

        change_percent = item.get(
            "change_percent",
            None
        )

        warning_1h = ema.get(
            "warning_1h",
            ema.get(
                "warning",
                "none"
            )
        )

        warning_4h = ema.get(
            "warning_4h",
            "none"
        )

        valid_1h = (

            (
                direction == "long"
                and
                change_percent is not None
                and
                change_percent > 0
                and
                warning_1h.startswith(
                    "long_breakout_"
                )
            )

            or

            (
                direction == "short"
                and
                change_percent is not None
                and
                change_percent < 0
                and
                warning_1h.startswith(
                    "short_breakout_"
                )
            )

            or

            warning_1h in (
                "long_breakout_0",
                "short_breakout_0"
            )

        )

        valid_4h = (

            (
                warning_4h.startswith(
                    "long_breakout_4h_"
                )
                and
                change_percent is not None
                and
                change_percent > 0
            )

            or

            (
                warning_4h.startswith(
                    "short_breakout_4h_"
                )
                and
                change_percent is not None
                and
                change_percent < 0
            )

            or

            warning_4h in (
                "long_breakout_4h_0",
                "short_breakout_4h_0"
            )

        )

        show_item = (
            valid_1h
            or
            valid_4h
        )

        if not show_item:

            continue


        html += f"""

<tr>

<td class="rank-cell">

<div class="coin-wrap">

<div class="coin-name">
{item['rank']}
</div>

<div class="coin-sub">
</div>

</div>

</td>


<td class="coin-cell">

<div class="coin-wrap">

<div class="coin-name">
{item['name']}
</div>

<div class="coin-sub">

{direction_html(
    ema.get(
        "direction",
        "none"
    ),
    item.get(
        "change_percent",
        None
    )
)}

</div>

</div>

</td>


<td class="volume-cell">

<div class="volume-wrap">

<div class="volume-main">
{item['volume']}
</div>

<div class="volume-sub">

{signal_html(
    ema.get(
        "signal",
        ""
    ),
    warning_1h,
    item.get(
        "change_percent",
        None
    )
)}

</div>

</div>

</td>


<td class="change-cell">

<div class="change-wrap">

<div class="change-main">
{item['change']}
</div>

<div class="change-sub">

"""

        html += warning_html(
            warning_1h,
            warning_4h,
            item.get(
                "change_percent",
                None
            )
        )

        html += """

</div>

</div>

</td>


<td class="ema-cell">

"""

        html += ema_html(
            ema
        )

        html += """

</td>

</tr>

"""

    html += """

</table>

</div>


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
        "서버 시작"
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


# =========================================================
# 실행
# =========================================================

if __name__ == "__main__":

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
                    )
