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
# API 안정화
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
# OKX 현재 진행 캔들
# =========================================================

def get_okx_current_ohlcv(
    inst_id,
    bar="1H",
    limit=2
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
# 업비트 거래대금
# =========================================================
# 업비트 ticker API에서 24시간 누적 거래대금을 직접 사용
# =========================================================

def get_upbit_ticker_all():

    response = retry_request(
        requests.get,
        "https://api.upbit.com/v1/ticker/all",
        timeout=15
    )

    if response is None:
        return {}

    try:

        data = response.json()

        if not isinstance(
            data,
            list
        ):

            return {}

        result = {}

        for item in data:

            market = item.get(
                "market"
            )

            if not market:
                continue

            if not market.startswith(
                "KRW-"
            ):

                continue

            trade_price = float(
                item.get(
                    "trade_price",
                    0
                ) or 0
            )

            acc_trade_price_24h = float(
                item.get(
                    "acc_trade_price_24h",
                    0
                ) or 0
            )

            result[market] = {
                "price": trade_price,
                "volume_24h": acc_trade_price_24h
            }

        logging.info(
            f"업비트 거래대금 "
            f"{len(result)}개 수신"
        )

        return result

    except Exception as e:

        logging.error(
            f"업비트 ticker 오류 : {e}"
        )

        return {}


# =========================================================
# 업비트 마켓 목록
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
# OKX 전체 SWAP 목록
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
# OKX 거래대금용 계약정보
# =========================================================

def get_okx_swap_instruments():

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
        return {}

    try:

        data = response.json().get(
            "data",
            []
        )

        result = {}

        for item in data:

            inst_id = item.get(
                "instId"
            )

            if not inst_id:
                continue

            if not inst_id.endswith(
                "-USDT-SWAP"
            ):

                continue

            if item.get(
                "state"
            ) != "live":

                continue

            try:

                ct_val = float(
                    item.get(
                        "ctVal",
                        0
                    ) or 0
                )

            except Exception:

                ct_val = 0

            result[inst_id] = {
                "ctVal": ct_val,
                "ctValCcy":
                    item.get(
                        "ctValCcy",
                        ""
                    ),
                "settleCcy":
                    item.get(
                        "settleCcy",
                        ""
                    )
            }

        logging.info(
            f"OKX SWAP 계약정보 "
            f"{len(result)}개 수신"
        )

        return result

    except Exception as e:

        logging.error(
            f"OKX 계약정보 오류 : {e}"
        )

        return {}


# =========================================================
# OKX TICKERS
# =========================================================
# 핵심 변경 부분
#
# 기존:
# 종목 하나마다 1H 캔들 조회
#
# 변경:
# /market/tickers?instType=SWAP
# 한 번에 전체 SWAP 24시간 데이터 조회
# =========================================================

def get_okx_tickers():

    url = (
        "https://www.okx.com/api/v5/"
        "market/tickers?instType=SWAP"
    )

    response = retry_request(
        requests.get,
        url,
        timeout=15
    )

    if response is None:
        return []

    try:

        result = response.json()

        if result.get(
            "code"
        ) != "0":

            logging.error(
                f"OKX ticker API 오류 "
                f"{result}"
            )

            return []

        data = result.get(
            "data",
            []
        )

        logging.info(
            f"OKX TICKERS "
            f"{len(data)}개 수신"
        )

        return data

    except Exception as e:

        logging.error(
            f"OKX TICKERS 오류 : {e}"
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

    if price.notna().sum() < period:

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

    if any(
        x is None
        for x in [
            ema10,
            ema30,
            ema60,
            ema120
        ]
    ):

        return "none"

    a = ema10.iloc[-1]
    b = ema30.iloc[-1]
    c = ema60.iloc[-1]
    d = ema120.iloc[-1]

    if any(
        pd.isna(x)
        for x in [
            a,
            b,
            c,
            d
        ]
    ):

        return "none"

    if (
        a > b
        and
        b > c
        and
        c > d
    ):

        return "long"

    if (
        a < b
        and
        b < c
        and
        c < d
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

    direction = (
        get_ema_10_30_60_120_direction(
            df1h,
            column
        )
    )

    return direction


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

    def get_breakout_state(
        index
    ):

        if index < BREAKOUT_LOOKBACK:

            return "none"

        cur = df.iloc[index]

        previous = df.iloc[
            index - BREAKOUT_LOOKBACK:
            index
        ]

        previous_high = pd.to_numeric(
            previous["h"],
            errors="coerce"
        ).max()

        previous_low = pd.to_numeric(
            previous["l"],
            errors="coerce"
        ).min()

        long_alignment = (
            cur["ema10"]
            >
            cur["ema30"]
            >
            cur["ema60"]
            >
            cur["ema120"]
        )

        if (
            long_alignment
            and
            cur["c"] > previous_high
            and
            cur["c"] > cur["o"]
        ):

            return "long"

        short_alignment = (
            cur["ema10"]
            <
            cur["ema30"]
            <
            cur["ema60"]
            <
            cur["ema120"]
        )

        if (
            short_alignment
            and
            cur["c"] < previous_low
            and
            cur["c"] < cur["o"]
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

    return (
        f"{current_state}_breakout_{count}"
    )


# =========================================================
# 진행 중 1H 돌파
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

    current = current1h.iloc[-1]

    current_open = float(
        current["o"]
    )

    current_high = float(
        current["h"]
    )

    current_low = float(
        current["l"]
    )

    current_close = float(
        current["c"]
    )

    previous = df1h.tail(
        BREAKOUT_LOOKBACK
    )

    previous_high = pd.to_numeric(
        previous["h"],
        errors="coerce"
    ).max()

    previous_low = pd.to_numeric(
        previous["l"],
        errors="coerce"
    ).min()

    temp = pd.concat(
        [
            df1h[[column]],

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

    if any(
        x is None
        for x in [
            ema10,
            ema30,
            ema60,
            ema120
        ]
    ):

        return "none"

    a = ema10.iloc[-1]
    b = ema30.iloc[-1]
    c = ema60.iloc[-1]
    d = ema120.iloc[-1]

    if (
        a > b > c > d
        and
        current_close > current_open
        and
        current_high >= previous_high
    ):

        return "long_breakout_0"

    if (
        a < b < c < d
        and
        current_close < current_open
        and
        current_low <= previous_low
    ):

        return "short_breakout_0"

    return "none"


# =========================================================
# 진입 경고
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

    return (
        breakout_1h,
        "none"
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
    ):

        return {
            "1h_10_30_60_120": "⚪",
            "4h_10_30_60_120": "⚪",
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
            "1h_10_30_60_120": "⚪",
            "4h_10_30_60_120": "⚪",
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

    df1h = raw1h.iloc[
        :-1
    ].reset_index(
        drop=True
    )

    df4h = raw4h.iloc[
        :-1
    ].reset_index(
        drop=True
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

        return df

    except Exception as e:

        logging.error(
            f"업비트 캔들 오류 "
            f"{market} : {e}"
        )

        return None


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
# 변동률 표시
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
# LONG / SHORT HTML
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

        valid = False

        if warning_1h in (
            "long_breakout_0",
            "short_breakout_0"
        ):

            valid = True

        elif (
            warning_1h.startswith(
                "long_breakout_"
            )
            and
            change_percent is not None
            and
            change_percent > 0
        ):

            valid = True

        elif (
            warning_1h.startswith(
                "short_breakout_"
            )
            and
            change_percent is not None
            and
            change_percent < 0
        ):

            valid = True

        if valid:

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

        </div>

    </div>
    """


# =========================================================
# 업비트 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data

    logging.info(
        f"업비트 TOP{TOP_N} 시작"
    )

    ticker_map = get_upbit_ticker_all()

    if not ticker_map:

        logging.error(
            "업비트 거래대금 조회 실패"
        )

        return

    sorted_markets = sorted(
        ticker_map.keys(),
        key=lambda x:
        ticker_map[x]["volume_24h"],
        reverse=True
    )

    top_markets = sorted_markets[
        :TOP_N
    ]

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

            change_percent = (
                changes[0]
                if changes
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
                            ticker_map[
                                market
                            ][
                                "volume_24h"
                            ]
                        ),
                    "ema": ema
                }
            )

        except Exception as e:

            logging.error(
                f"업비트 상세 오류 "
                f"{market} : {e}"
            )

    latest_upbit_data = rows

    logging.info(
        f"업비트 TOP{TOP_N} 완료"
    )


# =========================================================
# OKX 거래대금 계산
# =========================================================

def calculate_okx_volume(
    ticker,
    instrument
):

    try:

        last = float(
            ticker.get(
                "last",
                0
            ) or 0
        )

        vol_ccy = float(
            ticker.get(
                "volCcy24h",
                0
            ) or 0
        )

        ct_val = float(
            instrument.get(
                "ctVal",
                0
            ) or 0
        )

        if (
            last <= 0
            or
            vol_ccy <= 0
            or
            ct_val <= 0
        ):

            return 0

        # USDT SWAP의 경우
        # 계약수 × 계약가치 × 가격
        volume_usdt = (
            vol_ccy
            *
            ct_val
            *
            last
        )

        return volume_usdt

    except Exception:

        return 0


# =========================================================
# OKX 업데이트
# =========================================================

def update_okx():

    global latest_okx_data

    logging.info(
        f"OKX TOP{TOP_N} 시작 "
        f"(TICKERS 24H 거래대금)"
    )

    tickers = get_okx_tickers()

    if not tickers:

        logging.error(
            "OKX TICKERS 조회 실패"
        )

        return

    instruments = (
        get_okx_swap_instruments()
    )

    if not instruments:

        logging.error(
            "OKX 계약정보 조회 실패"
        )

        return

    upbit_tickers = (
        get_upbit_ticker_all()
    )

    upbit_coin_set = {
        market.replace(
            "KRW-",
            ""
        )
        for market in upbit_tickers
    }

    volume_rows = []

    for ticker in tickers:

        inst_id = ticker.get(
            "instId",
            ""
        )

        if not inst_id.endswith(
            "-USDT-SWAP"
        ):

            continue

        instrument = instruments.get(
            inst_id
        )

        if instrument is None:

            continue

        volume_usdt = (
            calculate_okx_volume(
                ticker,
                instrument
            )
        )

        if volume_usdt <= 0:

            continue

        volume_rows.append(
            {
                "inst_id": inst_id,
                "ticker": ticker,
                "volume_usdt":
                    volume_usdt
            }
        )

    volume_rows.sort(
        key=lambda x:
        x["volume_usdt"],
        reverse=True
    )

    top_rows = volume_rows[
        :TOP_N
    ]

    logging.info(
        f"OKX TICKERS 기준 "
        f"TOP{len(top_rows)} 선정"
    )

    # -----------------------------------------------------
    # USDT/KRW
    # -----------------------------------------------------

    usdt_krw = 1400

    try:

        if "KRW-USDT" in upbit_tickers:

            usdt_krw = (
                upbit_tickers[
                    "KRW-USDT"
                ][
                    "price"
                ]
            )

    except Exception:

        pass

    rows = []

    for rank, item in enumerate(
        top_rows,
        start=1
    ):

        symbol = item[
            "inst_id"
        ]

        coin = symbol.replace(
            "-USDT-SWAP",
            ""
        )

        if coin in upbit_coin_set:

            coin = (
                f"{coin}(업비트)"
            )

        try:

            changes = get_okx_change(
                symbol
            )

            ema = get_okx_ema(
                symbol
            )

            change_percent = (
                changes[0]
                if changes
                else None
            )

            volume_krw = (
                item[
                    "volume_usdt"
                ]
                *
                usdt_krw
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
                            volume_krw
                        ),
                    "ema": ema
                }
            )

        except Exception as e:

            logging.error(
                f"OKX TOP 상세 오류 "
                f"{symbol} : {e}"
            )

    latest_okx_data = rows

    logging.info(
        f"OKX TOP{TOP_N} 완료"
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
# 대시보드
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
    font-size: 10px;
    padding: 6px;
}

h1 {
    margin: 3px 2px 6px 2px;
    font-size: 15px;
}

h2 {
    margin: 12px 2px 6px 2px;
    font-size: 12px;
}

.info {
    margin: 0 2px 7px 2px;
    padding: 6px 7px;
    color: #8b9099;
    background: #171a1f;
    border: 1px solid #252a31;
    border-radius: 8px;
    font-size: 8px;
    line-height: 1.5;
}

.table-wrap {
    width: 100%;
    overflow: hidden;
    border-radius: 9px;
    border: 1px solid #252a31;
}

table {
    width: 100%;
    table-layout: fixed;
    border-collapse: collapse;
    background: #181c21;
}

th {
    padding: 6px 2px;
    background: #12151a;
    border-bottom: 1px solid #2b3037;
    color: #8f949d;
    font-size: 8px;
    text-align: center;
}

td {
    padding: 6px 2px;
    border-bottom: 1px solid #272c32;
    text-align: center;
    vertical-align: middle;
    overflow: hidden;
}

tbody tr:last-child td {
    border-bottom: none;
}

th:nth-child(1),
td:nth-child(1) {
    width: 8%;
}

th:nth-child(2),
td:nth-child(2) {
    width: 20%;
}

th:nth-child(3),
td:nth-child(3) {
    width: 17%;
}

th:nth-child(4),
td:nth-child(4) {
    width: 25%;
}

th:nth-child(5),
td:nth-child(5) {
    width: 30%;
}

.coin-wrap {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 2px;
}

.coin {
    font-weight: bold;
    font-size: 9px;
}

.volume-wrap,
.today-wrap,
.breakout-wrap {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 3px;
}

.volume-value {
    font-size: 8px;
    font-weight: 600;
}

.signal-text {
    font-weight: bold;
    font-size: 8px;
}

.long-text {
    color: #35e66d;
}

.short-text {
    color: #ff4d4d;
}

.signal-none {
    color: #555;
}

.change-item {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 2px;
    font-size: 8px;
}

.direction-long,
.direction-short,
.direction-none {
    font-size: 9px;
}

.warning-wrap {
    width: 100%;
    text-align: center;
}

.warning-row {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 2px;
}

.warning-period {
    color: #888;
    font-size: 7px;
}

.rocket {
    font-size: 8px;
}

.warning-empty {
    color: #555;
}

.ema-container {
    width: 100%;
    overflow: hidden;
}

.ema-line {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 1px;
}

.ema-item {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 1px;
    width: 50%;
    min-width: 0;
}

.ema-period {
    color: #888;
    font-size: 6px;
}

.ema-value {
    font-size: 7px;
    font-weight: bold;
}

.note {
    color: #666;
    font-size: 7px;
    line-height: 1.5;
    margin: 5px 2px 8px 2px;
}

@media (max-width: 480px) {

    body {
        padding: 4px;
    }

    h1 {
        font-size: 14px;
    }

    h2 {
        font-size: 12px;
    }

    .info {
        font-size: 7px;
    }

    th {
        font-size: 7px;
        padding: 5px 1px;
    }

    td {
        padding: 5px 1px;
    }

    .coin {
        font-size: 8px;
    }

    .volume-value,
    .signal-text,
    .signal-none {
        font-size: 7px;
    }

    .change-item {
        font-size: 7px;
    }

    .ema-period {
        font-size: 5px;
    }

    .ema-value {
        font-size: 6px;
    }

    .rocket {
        font-size: 7px;
    }

    .warning-period {
        font-size: 5px;
    }
}

</style>

</head>

<body>

<h1>
📊 돌파 TOP
</h1>

<div class="info">

1H 추세 + 1H 🚀 돌파 |
0 = 진행 중 가능성 |
1+ = 확정 돌파

<br>

업비트 = 거래소 24H 거래대금 |
OKX = TICKERS 24H 거래대금 |
TOP"""

    html += str(TOP_N)

    html += """

<br>

EMA 10-30-60-120

</div>


<h2>
🏆 업비트 TOP"""

    html += str(TOP_N)

    html += """
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

        if not valid_1h:
            continue

        html += f"""

<tr>

<td>{item['rank']}</td>

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

{warning_html(
    warning_1h,
    "none",
    change_percent
)}

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

※ 🟢 = EMA10 > EMA30 > EMA60 > EMA120<br>
※ 🔴 = EMA10 < EMA30 < EMA60 < EMA120<br>
※ 일봉 조회 제거

</div>


<h2>
🏆 OKX TOP"""

    html += str(TOP_N)

    html += """
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

        if not valid_1h:
            continue

        html += f"""

<tr>

<td>{item['rank']}</td>

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

{warning_html(
    warning_1h,
    "none",
    change_percent
)}

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

※ OKX 거래대금 = TICKERS 24H 데이터 기준<br>
※ 🚀(0) = 현재 진행 중인 1H 캔들의 돌파 가능성<br>
※ LONG / SHORT 실제 신호는 확정 돌파만 사용<br>
※ EMA = 1H → 4H

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
