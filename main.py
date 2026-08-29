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
                        2 *
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

                if status != 200:

                    logging.warning(
                        f"API HTTP 오류 {status}"
                    )

                    return result

            return result

        except Exception as e:

            wait_time = min(
                2 *
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
# OKX 확정 캔들
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
            == "1"
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
# OKX 현재 진행 중 캔들
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
# OKX 일봉
# =========================================================

def get_okx_daily_ohlcv(
    inst_id,
    limit=200
):

    return get_okx_ohlcv(
        inst_id,
        "1D",
        limit
    )


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

def format_volume(volume):

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
# EMA 10-30-60-120 방향
# =========================================================

def get_ema_10_30_60_120_direction(
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
        ema10 is None
        or ema30 is None
        or ema60 is None
        or ema120 is None
    ):

        return "none"

    if (
        pd.isna(ema10.iloc[-1])
        or
        pd.isna(ema30.iloc[-1])
        or
        pd.isna(ema60.iloc[-1])
        or
        pd.isna(ema120.iloc[-1])
    ):

        return "none"

    if (
        ema10.iloc[-1]
        >
        ema30.iloc[-1]
        >
        ema60.iloc[-1]
        >
        ema120.iloc[-1]
    ):

        return "long"

    if (
        ema10.iloc[-1]
        <
        ema30.iloc[-1]
        <
        ema60.iloc[-1]
        <
        ema120.iloc[-1]
    ):

        return "short"

    return "none"


# =========================================================
# EMA 표시
# =========================================================

def check_ema_10_30_60_120(
    df,
    column
):

    direction = (
        get_ema_10_30_60_120_direction(
            df,
            column
        )
    )

    if direction == "long":

        return "🟢"

    if direction == "short":

        return "🔴"

    return "⚪"


# =========================================================
# 메인 방향
# =========================================================

def get_main_direction(
    df1h,
    df4h,
    column
):

    h1_direction = (
        get_ema_10_30_60_120_direction(
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
# 1H 확정 돌파
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

        long_10_30_60_120 = (
            cur["ema10"]
            >
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
            long_10_30_60_120
            and
            long_break
            and
            long_candle
        ):

            return "long"

        short_10_30_60_120 = (
            cur["ema10"]
            <
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
            short_10_30_60_120
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
# 1H 진행 중 캔들 0
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
        or len(df1h)
        <
        120 + BREAKOUT_LOOKBACK
    ):

        return "none"

    df = df1h.copy()

    current = current1h.iloc[-1]

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

    long_10_30_60_120 = (
        cur_ema10
        >
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
        long_10_30_60_120
        and
        long_candle
        and
        long_possible
    ):

        return "long_breakout_0"

    short_10_30_60_120 = (
        cur_ema10
        <
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
        short_10_30_60_120
        and
        short_candle
        and
        short_possible
    ):

        return "short_breakout_0"

    return "none"


# =========================================================
# 메인 진입 경고
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

    if breakout_1h == "none":

        breakout_1h_0 = check_breakout_0(
            df1h,
            current1h,
            column
        )

        if breakout_1h_0 != "none":

            breakout_1h = breakout_1h_0

    breakout_4h = "none"

    return (
        breakout_1h,
        breakout_4h
    )


# =========================================================
# LONG / SHORT
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

    df1d = get_okx_daily_ohlcv(
        inst_id,
        200
    )

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
        or
        df4h is None
        or
        df1d is None
    ):

        return {
            "1h_10_30_60_120": "⚪",
            "4h_10_30_60_120": "⚪",
            "1d_10_30_60_120": "⚪",
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

        "1h_10_30_60_120":
            check_ema_10_30_60_120(
                df1h,
                "c"
            ),

        "4h_10_30_60_120":
            check_ema_10_30_60_120(
                df4h,
                "c"
            ),

        "1d_10_30_60_120":
            check_ema_10_30_60_120(
                df1d,
                "c"
            ),

        "signal":
            signal,

        "warning":
            warning_1h,

        "warning_1h":
            warning_1h,

        "warning_4h":
            "none",

        "direction":
            direction
    }


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

    raw1d = get_upbit_daily_ohlcv(
        market,
        200
    )

    if (
        raw1h is None
        or
        raw4h is None
        or
        raw1d is None
    ):

        return {
            "1h_10_30_60_120": "⚪",
            "4h_10_30_60_120": "⚪",
            "1d_10_30_60_120": "⚪",
            "signal": "",
            "warning": "none",
            "warning_1h": "none",
            "warning_4h": "none",
            "direction": "none"
        }

    current1h = raw1h.tail(
        1
    ).copy()

    current4h = raw4h.tail(
        1
    ).copy()

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

    df1d = raw1d.copy()

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

        "1h_10_30_60_120":
            check_ema_10_30_60_120(
                df1h,
                "c"
            ),

        "4h_10_30_60_120":
            check_ema_10_30_60_120(
                df4h,
                "c"
            ),

        "1d_10_30_60_120":
            check_ema_10_30_60_120(
                df1d,
                "c"
            ),

        "signal":
            signal,

        "warning":
            warning_1h,

        "warning_1h":
            warning_1h,

        "warning_4h":
            "none",

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
        '<span class="change-item">'
        f'<span class="change-icon">{icon}</span>'
        f'<span class="change-value">{sign}{x:.2f}%</span>'
        '</span>'
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
# =========================================================

def warning_html(
    warning_1h,
    warning_4h,
    change_percent
):

    html = (
        '<div class="warning-wrap">'
    )

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

        if warning_1h in (
            "long_breakout_0",
            "short_breakout_0"
        ):

            valid_1h = True

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

    html += '</div>'

    return html


# =========================================================
# EMA HTML
# ★ 사진 스타일에 맞게 축소
# ★ 전고점 매물 확인 삭제
# =========================================================

def ema_html(
    ema
):

    ema_1h = ema.get(
        "1h_10_30_60_120",
        "⚪"
    )

    ema_4h = ema.get(
        "4h_10_30_60_120",
        "⚪"
    )

    ema_1d = ema.get(
        "1d_10_30_60_120",
        "⚪"
    )

    return f"""
    <div class="ema-container">

        <div class="ema-line">

            <div class="ema-item">
                <span class="ema-period">1H</span>
                <span class="ema-value">
                    {ema_1h}
                </span>
            </div>

            <div class="ema-item">
                <span class="ema-period">4H</span>
                <span class="ema-value">
                    {ema_4h}
                </span>
            </div>

            <div class="ema-item">
                <span class="ema-period">1D</span>
                <span class="ema-value">
                    {ema_1d}
                </span>
            </div>

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
                    "ema":
                        ema
                }
            )

            success_detail += 1

        except Exception as e:

            failed_detail += 1

            logging.error(
                f"업비트 TOP 상세 오류 "
                f"{market} : {e}"
            )

            rows.append(
                {
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
                    "ema":
                        {
                            "1h_10_30_60_120": "⚪",
                            "4h_10_30_60_120": "⚪",
                            "1d_10_30_60_120": "⚪",
                            "signal": "",
                            "warning": "none",
                            "warning_1h": "none",
                            "warning_4h": "none",
                            "direction": "none"
                        }
                }
            )

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

    top_symbols = sorted(
        volume_map,
        key=volume_map.get,
        reverse=True
    )[:TOP_N]

    rows = []

    total_top = len(top_symbols)

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
                    "ema":
                        ema
                }
            )

        except Exception as e:

            logging.error(
                f"OKX TOP 상세 오류 "
                f"{symbol} : {e}"
            )

            rows.append(
                {
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
                    "ema":
                        {
                            "1h_10_30_60_120": "⚪",
                            "4h_10_30_60_120": "⚪",
                            "1d_10_30_60_120": "⚪",
                            "signal": "",
                            "warning": "none",
                            "warning_1h": "none",
                            "warning_4h": "none",
                            "direction": "none"
                        }
                }
            )

    latest_okx_data = rows


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
# ★ 사진 스타일 다크 카드형으로 수정
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def dashboard():

    html = """
<!DOCTYPE html>

<html lang="ko">

<head>

<meta charset="UTF-8">

<meta http-equiv="refresh" content="300">

<meta name="viewport"
      content="width=device-width,
               initial-scale=1.0,
               maximum-scale=1.0,
               user-scalable=no">

<title>돌파 TOP</title>

<style>

/* =====================================================
   기본
   ===================================================== */

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

    background:
        #0d1117;

    color:
        #f1f3f5;

    font-family:
        Arial,
        "Noto Sans KR",
        sans-serif;

    font-size: 10px;

    padding: 8px;

    overflow-wrap: anywhere;
}


/* =====================================================
   전체 컨테이너
   ===================================================== */

.dashboard {

    width: 100%;

    max-width: 100%;

    margin: 0 auto;

    overflow: hidden;
}


/* =====================================================
   상단 헤더
   사진과 비슷한 카드형
   ===================================================== */

.header-card {

    width: 100%;

    background:
        #171c23;

    border:
        1px solid #292f38;

    border-radius:
        14px;

    padding:
        12px 13px;

    margin-bottom:
        9px;

    box-shadow:
        0 2px 8px rgba(0,0,0,0.25);
}

.header-title {

    margin: 0;

    font-size: 17px;

    font-weight: 800;

    color:
        #f5f7fa;

    line-height:
        1.2;
}

.header-subtitle {

    margin-top: 5px;

    color:
        #7f8792;

    font-size: 9px;

    line-height:
        1.4;
}

.header-info {

    margin-top: 9px;

    display: flex;

    flex-wrap: wrap;

    gap: 5px;
}

.info-badge {

    background:
        #10151b;

    border:
        1px solid #292f38;

    border-radius:
        7px;

    padding:
        4px 7px;

    color:
        #aeb5bf;

    font-size: 8px;

    white-space:
        nowrap;
}


/* =====================================================
   섹션
   ===================================================== */

.section {

    width: 100%;

    margin-top: 9px;

    background:
        #11161c;

    border:
        1px solid #252b33;

    border-radius:
        14px;

    overflow: hidden;

    box-shadow:
        0 2px 8px rgba(0,0,0,0.22);
}


/* =====================================================
   섹션 제목
   ===================================================== */

.section-title {

    display: flex;

    align-items: center;

    justify-content: space-between;

    width: 100%;

    padding:
        11px 12px 9px 12px;

    background:
        #171c23;

    border-bottom:
        1px solid #292f38;
}

.section-title-main {

    font-size: 13px;

    font-weight: 800;

    color:
        #f2f4f7;
}

.section-title-sub {

    color:
        #737b87;

    font-size: 7px;

    font-weight: normal;
}


/* =====================================================
   테이블
   ===================================================== */

.table-wrap {

    width: 100%;

    max-width: 100%;

    overflow: hidden;
}

table {

    width: 100%;

    max-width: 100%;

    table-layout: fixed;

    border-collapse: collapse;

    background:
        #171c22;
}


/* =====================================================
   5칸
   # | 코인 | 거래대금 | 오늘 | EMA

   번호 확대
   오늘 확대
   EMA 축소
   ===================================================== */

th:nth-child(1),
td:nth-child(1) {

    width: 8%;
}

th:nth-child(2),
td:nth-child(2) {

    width: 21%;
}

th:nth-child(3),
td:nth-child(3) {

    width: 17%;
}

th:nth-child(4),
td:nth-child(4) {

    width: 22%;
}

th:nth-child(5),
td:nth-child(5) {

    width: 32%;
}


/* =====================================================
   헤더
   ===================================================== */

th {

    padding:
        7px 3px;

    background:
        #10151b;

    border-bottom:
        1px solid #2a3038;

    color:
        #858d98;

    font-size:
        8px;

    font-weight:
        600;

    white-space:
        nowrap;

    overflow:
        hidden;

    text-overflow:
        clip;

    text-align:
        center;
}


/* =====================================================
   셀
   ===================================================== */

td {

    padding:
        7px 3px;

    border-bottom:
        1px solid #252b32;

    vertical-align:
        middle;

    text-align:
        center;

    overflow:
        hidden;

    overflow-wrap:
        anywhere;

    word-break:
        break-word;

    min-width:
        0;
}

tbody tr:last-child td {

    border-bottom:
        none;
}

tbody tr {

    transition:
        background 0.15s;
}

tbody tr:hover {

    background:
        #1b2128;
}


/* =====================================================
   순위
   ===================================================== */

td:first-child {

    color:
        #b7bec8;

    font-size:
        9px;

    font-weight:
        700;

    text-align:
        center;
}

td:first-child::before {

    content:
        "";
}


/* =====================================================
   코인
   ===================================================== */

.coin-wrap {

    display:
        flex;

    flex-direction:
        column;

    align-items:
        flex-start;

    justify-content:
        center;

    gap:
        3px;

    width:
        100%;

    max-width:
        100%;

    overflow:
        hidden;
}

.coin {

    display:
        block;

    width:
        100%;

    max-width:
        100%;

    font-weight:
        800;

    text-align:
        left;

    font-size:
        10px;

    line-height:
        1.15;

    color:
        #f2f4f7;

    overflow:
        hidden;

    overflow-wrap:
        anywhere;

    word-break:
        break-word;
}


/* =====================================================
   방향
   ===================================================== */

.direction-long,
.direction-short,
.direction-none {

    display:
        block;

    font-size:
        9px;

    line-height:
        1;
}

.direction-long {

    filter:
        drop-shadow(0 0 2px rgba(255,210,70,0.25));
}

.direction-short {

    opacity:
        0.9;
}

.direction-none {

    color:
        #505862;
}


/* =====================================================
   거래대금
   ===================================================== */

.volume-wrap {

    display:
        flex;

    flex-direction:
        column;

    align-items:
        center;

    justify-content:
        center;

    gap:
        4px;

    width:
        100%;

    max-width:
        100%;

    overflow:
        hidden;
}

.volume-value {

    display:
        block;

    max-width:
        100%;

    color:
        #e6e9ed;

    font-size:
        8px;

    font-weight:
        700;

    line-height:
        1.2;

    overflow:
        hidden;

    overflow-wrap:
        anywhere;

    word-break:
        break-word;
}


/* =====================================================
   LONG / SHORT
   ===================================================== */

.signal-text {

    font-weight:
        800;

    font-size:
        8px;

    line-height:
        1;
}

.long-text {

    color:
        #35e66d;
}

.short-text {

    color:
        #ff5555;
}

.signal-none {

    color:
        #444b54;

    font-size:
        8px;
}


/* =====================================================
   오늘
   ★ 기존보다 넓게
   ===================================================== */

.today-wrap {

    display:
        flex;

    flex-direction:
        column;

    align-items:
        flex-start;

    justify-content:
        center;

    gap:
        4px;

    width:
        100%;

    max-width:
        100%;

    overflow:
        hidden;
}

.change-item {

    display:
        inline-flex;

    align-items:
        center;

    gap:
        2px;

    max-width:
        100%;

    color:
        #e5e8ec;

    font-size:
        8px;

    font-weight:
        700;

    line-height:
        1.1;

    overflow:
        hidden;
}

.change-icon {

    flex-shrink:
        0;
}

.change-value {

    overflow:
        hidden;

    white-space:
        nowrap;

    text-overflow:
        clip;
}


/* =====================================================
   돌파
   ===================================================== */

.breakout-wrap {

    display:
        flex;

    flex-direction:
        column;

    align-items:
        flex-start;

    justify-content:
        center;

    max-width:
        100%;

    overflow:
        hidden;
}

.warning-wrap {

    width:
        100%;

    max-width:
        100%;

    overflow:
        hidden;
}

.warning-row {

    display:
        flex;

    align-items:
        center;

    justify-content:
        flex-start;

    gap:
        3px;

    max-width:
        100%;

    overflow:
        hidden;
}

.rocket {

    font-size:
        9px;

    font-weight:
        700;

    white-space:
        nowrap;
}

.warning-empty {

    color:
        #414850;

    font-size:
        8px;

    white-space:
        nowrap;
}

.warning-period {

    color:
        #707984;

    font-size:
        7px;

    white-space:
        nowrap;
}


/* =====================================================
   EMA
   ★ 기존보다 좁게
   ★ 모바일에서 한 줄 유지
   ===================================================== */

.ema-container {

    width:
        100%;

    max-width:
        100%;

    overflow:
        hidden;
}

.ema-line {

    display:
        flex;

    align-items:
        center;

    justify-content:
        space-between;

    gap:
        2px;

    width:
        100%;

    max-width:
        100%;

    overflow:
        hidden;
}

.ema-item {

    display:
        flex;

    align-items:
        center;

    justify-content:
        center;

    gap:
        1px;

    min-width:
        0;

    width:
        33.33%;

    max-width:
        33.33%;

    overflow:
        hidden;

    white-space:
        nowrap;
}

.ema-period {

    color:
        #737b86;

    font-size:
        6px;

    flex-shrink:
        0;
}

.ema-value {

    font-size:
        8px;

    font-weight:
        800;

    flex-shrink:
        0;
}


/* =====================================================
   설명
   ===================================================== */

.note {

    color:
        #626a75;

    font-size:
        7px;

    line-height:
        1.5;

    padding:
        7px 11px 9px 11px;

    background:
        #11161c;

    border-top:
        1px solid #242a31;

    max-width:
        100%;

    overflow-wrap:
        anywhere;

    word-break:
        break-word;
}


/* =====================================================
   모바일
   ===================================================== */

@media (max-width: 480px) {

    body {

        padding:
            5px;

        font-size:
            9px;
    }

    .header-card {

        padding:
            11px 11px;

        border-radius:
            13px;

        margin-bottom:
            7px;
    }

    .header-title {

        font-size:
            15px;
    }

    .header-subtitle {

        font-size:
            8px;

        margin-top:
            4px;
    }

    .header-info {

        margin-top:
            7px;

        gap:
            4px;
    }

    .info-badge {

        font-size:
            7px;

        padding:
            4px 6px;
    }

    .section {

        margin-top:
            7px;

        border-radius:
            12px;
    }

    .section-title {

        padding:
            9px 10px 8px 10px;
    }

    .section-title-main {

        font-size:
            12px;
    }

    .section-title-sub {

        font-size:
            6px;
    }

    th {

        padding:
            6px 2px;

        font-size:
            7px;
    }

    td {

        padding:
            6px 2px;
    }

    td:first-child {

        font-size:
            9px;
    }

    .coin {

        font-size:
            9px;
    }

    .direction-long,
    .direction-short,
    .direction-none {

        font-size:
            8px;
    }

    .volume-value {

        font-size:
            7px;
    }

    .signal-text {

        font-size:
            7px;
    }

    .signal-none {

        font-size:
            7px;
    }

    .change-item {

        font-size:
            8px;

        gap:
            1px;
    }

    .warning-row {

        gap:
            2px;
    }

    .rocket {

        font-size:
            8px;
    }

    .warning-period {

        font-size:
            6px;
    }

    .ema-line {

        gap:
            1px;
    }

    .ema-item {

        gap:
            1px;
    }

    .ema-period {

        font-size:
            5px;
    }

    .ema-value {

        font-size:
            7px;
    }

    .note {

        padding:
            6px 9px 8px 9px;

        font-size:
            6px;
    }
}


/* =====================================================
   아주 작은 모바일
   ===================================================== */

@media (max-width: 360px) {

    body {

        padding:
            3px;
    }

    .header-title {

        font-size:
            14px;
    }

    .section-title-main {

        font-size:
            11px;
    }

    th {

        font-size:
            6px;

        padding:
            5px 1px;
    }

    td {

        padding:
            5px 1px;
    }

    .coin {

        font-size:
            8px;
    }

    .volume-value {

        font-size:
            6px;
    }

    .change-item {

        font-size:
            7px;
    }

    .rocket {

        font-size:
            7px;
    }

    .ema-period {

        font-size:
            5px;
    }

    .ema-value {

        font-size:
            6px;
    }
}

</style>

</head>

<body>

<div class="dashboard">


<!-- =====================================================
     상단
     ===================================================== -->

<div class="header-card">

    <div class="header-title">
        📊 돌파 TOP
    </div>

    <div class="header-subtitle">
        1H 추세 + 1H 🚀 돌파 기준 실시간 조회
    </div>

    <div class="header-info">

        <span class="info-badge">
            거래대금 {VOLUME_HOURS}H
        </span>

        <span class="info-badge">
            OKX ÷10
        </span>

        <span class="info-badge">
            TOP {TOP_N}
        </span>

        <span class="info-badge">
            EMA 10-30-60-120
        </span>

        <span class="info-badge">
            0 진행 중
        </span>

        <span class="info-badge">
            1+ 확정
        </span>

    </div>

</div>


<!-- =====================================================
     업비트
     ===================================================== -->

<div class="section">

    <div class="section-title">

        <span class="section-title-main">
            🏆 업비트 TOP {TOP_N}
        </span>

        <span class="section-title-sub">
            1H 돌파
        </span>

    </div>

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
"""


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

        if not valid_1h:

            continue

        html += f"""

                <tr>

                    <td>
                        {item['rank']}
                    </td>


                    <td>

                        <div class="coin-wrap">

                            <span class="coin">
                                {item['name']}
                            </span>

                            {direction_html(
                                direction,
                                change_percent
                            )}

                        </div>

                    </td>


                    <td>

                        <div class="volume-wrap">

                            <span class="volume-value">
                                {item['volume']}
                            </span>

                            {signal_html(
                                ema.get(
                                    "signal",
                                    ""
                                ),
                                warning_1h,
                                change_percent
                            )}

                        </div>

                    </td>


                    <td>

                        <div class="today-wrap">

                            {item['change']}

                            <div class="breakout-wrap">

                                {warning_html(
                                    warning_1h,
                                    "none",
                                    change_percent
                                )}

                            </div>

                        </div>

                    </td>


                    <td>

                        {ema_html(ema)}

                    </td>

                </tr>

"""


    html += """

            </tbody>

        </table>

    </div>

    <div class="note">

        ※ 🟢 = EMA10 &gt; EMA30 &gt; EMA60 &gt; EMA120<br>
        ※ 🔴 = EMA10 &lt; EMA30 &lt; EMA60 &lt; EMA120<br>
        ※ 🚀(0) = 현재 진행 중인 1H 캔들의 돌파 가능성<br>
        ※ 1+ = 확정 돌파

    </div>

</div>


<!-- =====================================================
     OKX
     ===================================================== -->

<div class="section">

    <div class="section-title">

        <span class="section-title-main">
            🏆 OKX TOP {TOP_N}
        </span>

        <span class="section-title-sub">
            1H 돌파
        </span>

    </div>

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
"""


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

        if not valid_1h:

            continue

        html += f"""

                <tr>

                    <td>
                        {item['rank']}
                    </td>


                    <td>

                        <div class="coin-wrap">

                            <span class="coin">
                                {item['name']}
                            </span>

                            {direction_html(
                                direction,
                                change_percent
                            )}

                        </div>

                    </td>


                    <td>

                        <div class="volume-wrap">

                            <span class="volume-value">
                                {item['volume']}
                            </span>

                            {signal_html(
                                ema.get(
                                    "signal",
                                    ""
                                ),
                                warning_1h,
                                change_percent
                            )}

                        </div>

                    </td>


                    <td>

                        <div class="today-wrap">

                            {item['change']}

                            <div class="breakout-wrap">

                                {warning_html(
                                    warning_1h,
                                    "none",
                                    change_percent
                                )}

                            </div>

                        </div>

                    </td>


                    <td>

                        {ema_html(ema)}

                    </td>

                </tr>

"""


    html += """

            </tbody>

        </table>

    </div>

    <div class="note">

        ※ 1H 돌파 기준: 직전 """

    html += str(BREAKOUT_LOOKBACK)

    html += """개 확정 캔들의 고가/저가 돌파<br>
        ※ 🚀(0) = 현재 진행 중인 1H 캔들의 돌파 가능성<br>
        ※ LONG / SHORT 실제 신호는 확정 돌파만 사용<br>
        ※ EMA = 1H → 4H → 1D

    </div>

</div>


</div>

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
