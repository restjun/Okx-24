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

from concurrent.futures import ThreadPoolExecutor, as_completed


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

TOP_N = 20

UPDATE_MINUTES = 1

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
# 병렬 처리
# =========================================================

MAX_WORKERS = 10


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
    limit=2
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
# 업비트 1H 캔들
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

    if (
        ema10 is None
        or ema30 is None
        or ema60 is None
        or ema120 is None
    ):

        return "none"

    values = [
        ema10.iloc[-1],
        ema30.iloc[-1],
        ema60.iloc[-1],
        ema120.iloc[-1]
    ]

    if any(pd.isna(x) for x in values):

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

    direction = (
        get_ema_10_30_60_120_direction(
            df1h,
            column
        )
    )

    if direction == "long":

        return "long"

    if direction == "short":

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
        < 120 + BREAKOUT_LOOKBACK
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
            or pd.isna(previous_low)
        ):

            return "none"

        long_ema = (
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
            long_ema
            and
            long_break
            and
            long_candle
        ):

            return "long"

        short_ema = (
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
            short_ema
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

    return (
        f"{current_state}_breakout_{count}"
    )


# =========================================================
# 1H 진행 중 돌파
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
        < 120 + BREAKOUT_LOOKBACK
    ):

        return "none"

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

    if any(
        pd.isna(x)
        for x in [
            current_open,
            current_high,
            current_low,
            current_close
        ]
    ):

        return "none"

    previous = df1h.tail(
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
        or pd.isna(previous_low)
    ):

        return "none"

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

    cur10 = ema10.iloc[-1]
    cur30 = ema30.iloc[-1]
    cur60 = ema60.iloc[-1]
    cur120 = ema120.iloc[-1]

    if (
        cur10
        >
        cur30
        >
        cur60
        >
        cur120
        and
        current_close
        >
        current_open
        and
        current_high
        >=
        previous_high
    ):

        return "long_breakout_0"

    if (
        cur10
        <
        cur30
        <
        cur60
        <
        cur120
        and
        current_close
        <
        current_open
        and
        current_low
        <=
        previous_low
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
# 기본 EMA 데이터
# =========================================================

def empty_ema():

    return {
        "1h_10_30_60_120": "⚪",
        "4h_10_30_60_120": "⚪",
        "signal": "",
        "warning": "none",
        "warning_1h": "none",
        "warning_4h": "none",
        "direction": "none"
    }


# =========================================================
# OKX EMA
# 일봉 완전 제거
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
        or df4h is None
    ):

        return empty_ema()

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

        "signal": signal,

        "warning": warning_1h,

        "warning_1h": warning_1h,

        "warning_4h": warning_4h,

        "direction": direction
    }


# =========================================================
# 업비트 EMA
# 일봉 완전 제거
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
        or raw4h is None
    ):

        return empty_ema()

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

        "signal": signal,

        "warning": warning_1h,

        "warning_1h": warning_1h,

        "warning_4h": warning_4h,

        "direction": direction
    }


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
            f"{len(symbols)}개"
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
            f"{len(markets)}개"
        )

        return markets

    except Exception as e:

        logging.error(
            f"업비트 목록 오류 : {e}"
        )

        return []


# =========================================================
# 업비트 ticker 거래대금
# ★ 거래소 제공 24시간 거래대금
# =========================================================

def get_upbit_ticker_volumes(
    markets
):

    if not markets:

        return {}

    volume_map = {}

    # 업비트 ticker API는 여러 마켓을 한 번에 조회
    chunk_size = 100

    logging.info(
        f"업비트 거래소 제공 "
        f"24시간 거래대금 조회 시작 "
        f"({len(markets)}개)"
    )

    for i in range(
        0,
        len(markets),
        chunk_size
    ):

        chunk = markets[
            i:i + chunk_size
        ]

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

            logging.error(
                f"업비트 ticker 조회 실패 "
                f"{i + 1}~{i + len(chunk)}"
            )

            continue

        try:

            data = response.json()

            for item in data:

                market = item.get(
                    "market"
                )

                volume = float(
                    item.get(
                        "acc_trade_price_24h",
                        0
                    )
                )

                if market:

                    volume_map[
                        market
                    ] = volume

        except Exception as e:

            logging.error(
                f"업비트 ticker 처리 오류 : {e}"
            )

    valid_count = sum(
        1
        for value in volume_map.values()
        if value > 0
    )

    logging.info(
        f"업비트 거래대금 조회 완료 "
        f"정상 {valid_count}/{len(markets)}"
    )

    return volume_map


# =========================================================
# OKX tickers
# ★ OKX 전체 SWAP 거래대금
# =========================================================

def get_okx_ticker_volumes():

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

        logging.error(
            "OKX tickers 거래대금 조회 실패"
        )

        return {}

    try:

        data = response.json().get(
            "data",
            []
        )

        volume_map = {}

        for item in data:

            inst_id = item.get(
                "instId",
                ""
            )

            if not inst_id.endswith(
                "-USDT-SWAP"
            ):

                continue

            # OKX 파생상품의 volCcy24h 사용
            volume_usdt = float(
                item.get(
                    "volCcy24h",
                    0
                )
                or 0
            )

            if volume_usdt > 0:

                volume_map[
                    inst_id
                ] = volume_usdt

        logging.info(
            f"OKX tickers 거래대금 "
            f"{len(volume_map)}개 정상"
        )

        return volume_map

    except Exception as e:

        logging.error(
            f"OKX tickers 처리 오류 : {e}"
        )

        return {}


# =========================================================
# 변동률 - OKX
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

    if len(daily) < 2:

        return None

    current = daily.iloc[-1]

    previous = daily.iloc[-2]

    if previous == 0:

        return [0]

    change = (
        (
            current
            -
            previous
        )
        /
        previous
        *
        100
    )

    return [
        round(
            change,
            2
        )
    ]


# =========================================================
# 변동률 - 업비트
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

    if len(daily) < 2:

        return None

    current = daily.iloc[-1]

    previous = daily.iloc[-2]

    if previous == 0:

        return [0]

    change = (
        (
            current
            -
            previous
        )
        /
        previous
        *
        100
    )

    return [
        round(
            change,
            2
        )
    ]


# =========================================================
# 변동률 HTML
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

    html += '</div></div>'

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
                <span class="ema-value">{ema_1h}</span>
            </div>

            <div class="ema-item">
                <span class="ema-period">4H</span>
                <span class="ema-value">{ema_4h}</span>
            </div>

        </div>
    </div>
    """


# =========================================================
# 업비트 상세 1개
# =========================================================

def process_upbit_detail(
    rank,
    market,
    volume
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

        return {
            "rank": rank,
            "name": coin,
            "change":
                format_change(changes),
            "change_percent":
                change_percent,
            "volume":
                format_volume(volume),
            "ema":
                ema
        }

    except Exception as e:

        logging.error(
            f"업비트 상세 오류 "
            f"{market} : {e}"
        )

        return {
            "rank": rank,
            "name": coin,
            "change": "N/A",
            "change_percent": None,
            "volume":
                format_volume(volume),
            "ema":
                empty_ema()
        }


# =========================================================
# OKX 상세 1개
# =========================================================

def process_okx_detail(
    rank,
    symbol,
    volume_krw,
    upbit_coin_set
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
            if changes
            else None
        )

        return {
            "rank": rank,
            "name": coin,
            "change":
                format_change(changes),
            "change_percent":
                change_percent,
            "volume":
                format_volume(
                    volume_krw
                ),
            "ema":
                ema
        }

    except Exception as e:

        logging.error(
            f"OKX 상세 오류 "
            f"{symbol} : {e}"
        )

        return {
            "rank": rank,
            "name": coin,
            "change": "N/A",
            "change_percent": None,
            "volume":
                format_volume(
                    volume_krw
                ),
            "ema":
                empty_ema()
        }


# =========================================================
# 업비트 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data

    logging.info(
        f"업비트 TOP{TOP_N} 시작"
    )

    markets = get_upbit_markets()

    if not markets:

        logging.error(
            "업비트 마켓 목록 실패"
        )

        return False

    volume_map = (
        get_upbit_ticker_volumes(
            markets
        )
    )

    valid_volume_count = sum(
        1
        for value in volume_map.values()
        if value > 0
    )

    if valid_volume_count == 0:

        logging.error(
            "업비트 거래대금 정상 데이터 0개 "
            "→ 다음 1분에 재시도"
        )

        return False

    top_markets = sorted(
        volume_map,
        key=volume_map.get,
        reverse=True
    )[:TOP_N]

    rows = []

    logging.info(
        f"업비트 TOP{len(top_markets)} "
        f"상세 병렬 조회"
    )

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        futures = {
            executor.submit(
                process_upbit_detail,
                rank,
                market,
                volume_map[market]
            ): rank
            for rank, market
            in enumerate(
                top_markets,
                start=1
            )
        }

        for future in as_completed(
            futures
        ):

            try:

                rows.append(
                    future.result()
                )

            except Exception as e:

                logging.error(
                    f"업비트 병렬 처리 오류 : {e}"
                )

    rows.sort(
        key=lambda x: x["rank"]
    )

    if not rows:

        logging.error(
            "업비트 상세 데이터 없음 "
            "→ 기존 데이터 유지"
        )

        return False

    latest_upbit_data = rows

    logging.info(
        f"업비트 TOP{len(rows)} 완료"
    )

    return True


# =========================================================
# OKX 업데이트
# =========================================================

def update_okx():

    global latest_okx_data

    logging.info(
        f"OKX TOP{TOP_N} 시작 "
        f"(tickers 거래대금)"
    )

    symbols = get_all_okx_swap_symbols()

    if not symbols:

        logging.error(
            "OKX 종목 목록 실패 "
            "→ 다음 1분에 재시도"
        )

        return False

    volume_usdt_map = (
        get_okx_ticker_volumes()
    )

    if not volume_usdt_map:

        logging.error(
            "OKX tickers 거래대금 조회 실패 "
            "→ 다음 1분에 재시도"
        )

        return False

    usdt_krw = get_usdt_krw()

    volume_map = {}

    for symbol in symbols:

        volume_usdt = (
            volume_usdt_map.get(
                symbol,
                0
            )
        )

        if volume_usdt > 0:

            volume_map[symbol] = (
                volume_usdt
                *
                usdt_krw
            )

    if not volume_map:

        logging.error(
            "OKX 거래대금 정상 데이터 0개 "
            "→ 다음 1분에 재시도"
        )

        return False

    top_symbols = sorted(
        volume_map,
        key=volume_map.get,
        reverse=True
    )[:TOP_N]

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

    rows = []

    logging.info(
        f"OKX TOP{len(top_symbols)} "
        f"상세 병렬 조회"
    )

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        futures = {
            executor.submit(
                process_okx_detail,
                rank,
                symbol,
                volume_map[symbol],
                upbit_coin_set
            ): rank
            for rank, symbol
            in enumerate(
                top_symbols,
                start=1
            )
        }

        for future in as_completed(
            futures
        ):

            try:

                rows.append(
                    future.result()
                )

            except Exception as e:

                logging.error(
                    f"OKX 병렬 처리 오류 : {e}"
                )

    rows.sort(
        key=lambda x: x["rank"]
    )

    if not rows:

        logging.error(
            "OKX 상세 데이터 없음 "
            "→ 기존 데이터 유지"
        )

        return False

    latest_okx_data = rows

    logging.info(
        f"OKX TOP{len(rows)} 완료"
    )

    return True


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    logging.info(
        "========================================"
    )

    logging.info(
        "전체 업데이트 시작"
    )

    upbit_success = False
    okx_success = False

    try:

        upbit_success = update_upbit()

    except Exception as e:

        logging.exception(
            f"업비트 업데이트 오류 : {e}"
        )

    try:

        okx_success = update_okx()

    except Exception as e:

        logging.exception(
            f"OKX 업데이트 오류 : {e}"
        )

    logging.info(
        f"전체 업데이트 완료 "
        f"(업비트={upbit_success}, "
        f"OKX={okx_success})"
    )

    logging.info(
        "========================================"
    )


# =========================================================
# 스케줄러
# ★ 1분마다 실행
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
# HTML
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

<meta http-equiv="refresh" content="60">

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

    overflow-x: hidden;
}

body {

    background: #0f1115;

    color: #eeeeee;

    font-family: Arial, sans-serif;

    font-size: 10px;

    padding: 6px;
}


/* 제목 */

h1 {

    margin: 3px 2px 6px 2px;

    font-size: 15px;

    font-weight: 700;
}

h2 {

    margin: 12px 2px 6px 2px;

    font-size: 12px;

    font-weight: 700;
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


/* 테이블 */

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

    vertical-align: middle;
}

td {

    padding: 6px 2px;

    border-bottom: 1px solid #272c32;

    text-align: center;

    vertical-align: middle;

    overflow: hidden;

    word-break: break-word;
}

tbody tr:last-child td {

    border-bottom: none;
}


/* 5칸 */

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


/* 코인 */

.coin-wrap {

    display: flex;

    flex-direction: column;

    align-items: center;

    justify-content: center;

    gap: 2px;
}

.coin {

    font-weight: bold;

    font-size: 9px;

    text-align: center;
}


/* 방향 */

.direction-long,
.direction-short,
.direction-none {

    display: block;

    font-size: 9px;

    text-align: center;
}


/* 거래대금 */

.volume-wrap {

    display: flex;

    flex-direction: column;

    align-items: center;

    justify-content: center;

    gap: 3px;
}

.volume-value {

    font-size: 8px;

    font-weight: 600;

    text-align: center;
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

    font-size: 8px;
}


/* 오늘 */

.today-wrap {

    display: flex;

    flex-direction: column;

    align-items: center;

    justify-content: center;

    gap: 3px;
}

.change-item {

    display: inline-flex;

    align-items: center;

    justify-content: center;

    gap: 2px;

    font-size: 8px;
}


/* 돌파 */

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

.rocket {

    font-size: 8px;

    white-space: nowrap;
}

.warning-empty {

    color: #555;
}

.warning-period {

    color: #888;

    font-size: 7px;
}


/* EMA */

.ema-container {

    width: 100%;

    overflow: hidden;
}

.ema-line {

    display: flex;

    align-items: center;

    justify-content: space-between;

    width: 100%;
}

.ema-item {

    display: flex;

    align-items: center;

    justify-content: center;

    gap: 1px;

    width: 50%;
}

.ema-period {

    color: #888;

    font-size: 6px;
}

.ema-value {

    font-size: 7px;

    font-weight: bold;
}


/* 설명 */

.note {

    color: #666;

    font-size: 7px;

    line-height: 1.5;

    margin: 5px 2px 8px 2px;
}


/* 모바일 */

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

        padding: 5px 1px;

        font-size: 7px;
    }

    td {

        padding: 5px 1px;
    }

    .coin {

        font-size: 8px;
    }

    .volume-value {

        font-size: 7px;
    }

    .signal-text {

        font-size: 7px;
    }

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

    .direction-long,
    .direction-short,
    .direction-none {

        font-size: 8px;
    }

    .note {

        font-size: 6px;
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

업비트 = 거래소 제공 24H 거래대금 |
OKX = Tickers 24H 거래대금 |
TOP"""

    html += str(TOP_N)

    html += """ |
EMA 10-30-60-120 |
1분마다 갱신

</div>


<!-- 업비트 -->

<div class="section">

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
    ema.get("signal", ""),
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

※ 🟢 = EMA10 &gt; EMA30 &gt; EMA60 &gt; EMA120<br>
※ 🔴 = EMA10 &lt; EMA30 &lt; EMA60 &lt; EMA120<br>
※ 🚀(0) = 현재 진행 중인 1H 캔들 돌파 가능성<br>
※ LONG / SHORT = 확정 돌파 기준

</div>

</div>


<!-- OKX -->

<div class="section">

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
    ema.get("signal", ""),
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

※ OKX 거래대금 = OKX Tickers 24시간 거래대금<br>
※ 1H 돌파 = 직전 5개 확정 캔들의 고가/저가 돌파<br>
※ 🚀(0) = 현재 진행 중인 1H 캔들의 돌파 가능성<br>
※ EMA = 1H / 4H<br>
※ 일봉 조회 없음<br>
※ 데이터 조회 실패 시 기존 데이터 유지 후 1분 뒤 재시도

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

    # 최초 실행
    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    # ★ 1분마다 실행
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
