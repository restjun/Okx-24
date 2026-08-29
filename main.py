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

VOLUME_HOURS = 24

TOP_N = 20

UPDATE_MINUTES = 1

MAX_WARNING_COUNT = 3

PULLBACK_DISTANCE = 0.02

BREAKOUT_LOOKBACK = 5


# =========================================================
# OKX 병렬 요청 설정
# =========================================================

OKX_WORKERS = 35

OKX_MAX_ROUNDS = 10

OKX_RETRY_WAIT = 1.0


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
# 업데이트 Lock
# =========================================================

update_lock = threading.Lock()


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

        result = response.json()

        data = result.get(
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
        or
        ema30 is None
        or
        ema60 is None
        or
        ema120 is None
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
        or
        len(df1h)
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

    if current_state == "long":

        return f"long_breakout_{count}"

    if current_state == "short":

        return f"short_breakout_{count}"

    return "none"


# =========================================================
# 메인 진입 경고
# 돌파 0 삭제
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
    ):

        signal = "LONG"

    elif (
        main_direction == "short"
        and
        breakout_1h.startswith(
            "short_breakout_"
        )
    ):

        signal = "SHORT"

    return (
        signal,
        breakout_1h,
        breakout_4h
    )


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
            None,
            None,
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
            "none",

        "direction":
            direction
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
            None,
            None,
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
            "none",

        "direction":
            direction
    }


# =========================================================
# OKX 거래대금
# 기존 방식 유지
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

        return 0

    volume = float(
        df["volCcyQuote"]
        .tail(hours)
        .sum()
    )

    return volume / 10


# =========================================================
# 업비트 Ticker 거래대금
# 거래소 제공 24시간 누적 거래대금
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

    logging.info(
        f"업비트 Ticker 거래대금 조회 "
        f"{len(markets)}개"
    )

    for chunk in chunks:

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
                "업비트 Ticker 조회 실패"
            )

            continue

        try:

            data = response.json()

            for item in data:

                market = item.get(
                    "market"
                )

                volume = item.get(
                    "acc_trade_price_24h",
                    0
                )

                try:

                    volume_map[market] = float(
                        volume
                    )

                except Exception:

                    volume_map[market] = 0

        except Exception as e:

            logging.error(
                f"업비트 Ticker 파싱 오류 : {e}"
            )

    logging.info(
        f"업비트 Ticker 거래대금 "
        f"{len(volume_map)}/{len(markets)} 완료"
    )

    return volume_map


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
# OKX 거래대금 병렬 1개 요청
# =========================================================

def fetch_okx_volume_worker(
    symbol
):

    try:

        volume = get_okx_volume(
            symbol
        )

        if volume > 0:

            return symbol, volume

        return symbol, 0

    except Exception as e:

        logging.warning(
            f"OKX 거래대금 실패 "
            f"{symbol} : {e}"
        )

        return symbol, 0


# =========================================================
# OKX 거래대금 35개 병렬
# 실패 종목 반복 재요청
# =========================================================

def get_okx_volume_map(
    symbols
):

    if not symbols:

        return {}

    result_map = {}

    pending = list(
        dict.fromkeys(symbols)
    )

    total = len(pending)

    logging.info(
        f"OKX 거래대금 시작 "
        f"전체 {total}개 / "
        f"병렬 {OKX_WORKERS}개"
    )

    for round_no in range(
        1,
        OKX_MAX_ROUNDS + 1
    ):

        if not pending:

            break

        logging.info(
            f"OKX 거래대금 "
            f"{round_no}차 시도 "
            f"남은 {len(pending)}개"
        )

        next_pending = []

        with ThreadPoolExecutor(
            max_workers=OKX_WORKERS
        ) as executor:

            futures = {
                executor.submit(
                    fetch_okx_volume_worker,
                    symbol
                ): symbol
                for symbol in pending
            }

            for future in as_completed(
                futures
            ):

                symbol = futures[
                    future
                ]

                try:

                    returned_symbol, volume = (
                        future.result()
                    )

                    if volume > 0:

                        result_map[
                            returned_symbol
                        ] = volume

                    else:

                        next_pending.append(
                            symbol
                        )

                except Exception as e:

                    logging.warning(
                        f"OKX 병렬 요청 오류 "
                        f"{symbol} : {e}"
                    )

                    next_pending.append(
                        symbol
                    )

        success_count = len(
            result_map
        )

        failed_count = len(
            next_pending
        )

        logging.info(
            f"OKX {round_no}차 완료 "
            f"성공 {success_count} / "
            f"실패 {failed_count}"
        )

        pending = next_pending

        if pending:

            time.sleep(
                OKX_RETRY_WAIT
            )

    if pending:

        logging.warning(
            f"OKX 최종 실패 "
            f"{len(pending)}개"
        )

    logging.info(
        f"OKX 거래대금 최종 완료 "
        f"{len(result_map)}/{total}"
    )

    return result_map


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
        or
        len(df) < 50
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
        or
        len(df) < 50
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
# 변동률 HTML
# =========================================================

def format_change(
    changes
):

    if (
        changes is None
        or
        len(changes) == 0
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
# 돌파 0 제거
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

        if (
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

                count = 1

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
# 1H / 4H만
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
# 업비트 TOP
# =========================================================

def update_upbit():

    global latest_upbit_data

    logging.info(
        f"업비트 TOP{TOP_N} 시작 "
        f"(거래소 제공 24H 거래대금)"
    )

    markets = get_upbit_markets()

    if not markets:

        logging.error(
            "업비트 마켓 목록 조회 실패"
        )

        return

    volume_map = (
        get_upbit_ticker_volume_map(
            markets
        )
    )

    if not volume_map:

        logging.error(
            "업비트 거래대금 조회 실패"
        )

        return

    top_markets = sorted(
        volume_map,
        key=volume_map.get,
        reverse=True
    )[:TOP_N]

    rows = []

    total_top = len(
        top_markets
    )

    logging.info(
        f"업비트 TOP{total_top} 상세 조회 시작"
    )

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
                            volume_map[
                                market
                            ]
                        ),
                    "ema":
                        ema
                }
            )

        except Exception as e:

            logging.error(
                f"업비트 상세 오류 "
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
                        empty_ema()
                }
            )

    latest_upbit_data = rows

    logging.info(
        f"업비트 TOP{total_top} 완료"
    )


# =========================================================
# OKX TOP
# 업비트 완료 후 실행
# =========================================================

def update_okx():

    global latest_okx_data

    logging.info(
        f"OKX TOP{TOP_N} 시작 "
        f"(거래대금 {VOLUME_HOURS}H / ÷10)"
    )

    symbols = get_all_okx_swap_symbols()

    if not symbols:

        logging.error(
            "OKX 종목 목록 조회 실패"
        )

        return

    volume_map = get_okx_volume_map(
        symbols
    )

    if not volume_map:

        logging.error(
            "OKX 거래대금 조회 실패"
        )

        return

    top_symbols = sorted(
        volume_map,
        key=volume_map.get,
        reverse=True
    )[:TOP_N]

    upbit_markets = get_upbit_markets()

    upbit_coin_set = {
        market.replace(
            "KRW-",
            ""
        )
        for market in upbit_markets
    }

    rows = []

    total_top = len(
        top_symbols
    )

    logging.info(
        f"OKX TOP{total_top} 상세 조회 시작"
    )

    for rank, symbol in enumerate(
        top_symbols,
        start=1
    ):

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
                            volume_map[
                                symbol
                            ]
                        ),
                    "ema":
                        ema
                }
            )

        except Exception as e:

            logging.error(
                f"OKX 상세 오류 "
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
                        empty_ema()
                }
            )

    latest_okx_data = rows

    logging.info(
        f"OKX TOP{total_top} 완료"
    )


# =========================================================
# 전체 업데이트
# 중요:
# 업비트 완료 → OKX 시작
# =========================================================

def update_dashboard():

    if not update_lock.acquire(
        blocking=False
    ):

        logging.warning(
            "이전 업데이트가 아직 실행 중 "
            "이번 실행은 건너뜁니다."
        )

        return

    try:

        logging.info(
            "========================================"
        )

        logging.info(
            "전체 조회 시작"
        )

        # -------------------------------------------------
        # 1. 업비트 먼저
        # -------------------------------------------------

        try:

            update_upbit()

        except Exception as e:

            logging.exception(
                f"업비트 업데이트 오류 : {e}"
            )

        # -------------------------------------------------
        # 2. 업비트 완료 후 OKX
        # -------------------------------------------------

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

    finally:

        update_lock.release()


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

    max-width: 100%;

    overflow-x: hidden;
}

body {

    background: #0f1115;

    color: #eeeeee;

    font-family:
        Arial,
        sans-serif;

    font-size: 10px;

    padding: 6px;

    overflow-wrap: anywhere;
}


/* =====================================================
   제목
   ===================================================== */

h1 {

    margin: 3px 2px 6px 2px;

    font-size: 15px;

    font-weight: 700;

    letter-spacing: -0.3px;
}

h2 {

    margin: 12px 2px 6px 2px;

    font-size: 12px;

    font-weight: 700;

    letter-spacing: -0.2px;
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

    overflow-wrap: anywhere;
}


/* =====================================================
   테이블
   ===================================================== */

.table-wrap {

    width: 100%;

    max-width: 100%;

    overflow: hidden;

    border-radius: 9px;

    border: 1px solid #252a31;
}

table {

    width: 100%;

    max-width: 100%;

    table-layout: fixed;

    border-collapse: collapse;

    background: #181c21;
}

th {

    padding: 6px 2px;

    background: #12151a;

    border-bottom:
        1px solid #2b3037;

    color: #8f949d;

    font-size: 8px;

    font-weight: 600;

    white-space: normal;

    overflow: hidden;

    overflow-wrap: anywhere;

    word-break: break-word;

    text-align: center;

    vertical-align: middle;
}

td {

    padding: 6px 2px;

    border-bottom:
        1px solid #272c32;

    vertical-align: middle;

    text-align: center;

    overflow: hidden;

    overflow-wrap: anywhere;

    word-break: break-word;

    min-width: 0;
}

tbody tr:last-child td {

    border-bottom: none;
}


/* =====================================================
   5칸
   ===================================================== */

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


/* =====================================================
   코인
   ===================================================== */

.coin {

    display: block;

    width: 100%;

    max-width: 100%;

    font-weight: bold;

    text-align: center;

    font-size: 9px;

    line-height: 1.2;

    overflow: hidden;

    overflow-wrap: anywhere;

    word-break: break-word;
}

.coin-wrap {

    display: flex;

    flex-direction: column;

    align-items: center;

    justify-content: center;

    gap: 2px;

    width: 100%;

    max-width: 100%;

    overflow: hidden;

    text-align: center;
}


/* =====================================================
   방향
   ===================================================== */

.direction-long,
.direction-short,
.direction-none {

    display: block;

    width: 100%;

    font-size: 9px;

    text-align: center;
}


/* =====================================================
   거래대금
   ===================================================== */

.volume-wrap {

    display: flex;

    flex-direction: column;

    align-items: center;

    justify-content: center;

    gap: 3px;

    width: 100%;

    max-width: 100%;

    overflow: hidden;

    text-align: center;
}

.volume-value {

    display: block;

    width: 100%;

    max-width: 100%;

    font-size: 8px;

    font-weight: 600;

    text-align: center;

    overflow: hidden;

    overflow-wrap: anywhere;

    word-break: break-word;
}

.signal-text {

    display: block;

    font-weight: bold;

    font-size: 8px;

    text-align: center;
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

    text-align: center;
}


/* =====================================================
   오늘
   ===================================================== */

.today-wrap {

    display: flex;

    flex-direction: column;

    align-items: center;

    justify-content: center;

    gap: 3px;

    width: 100%;

    max-width: 100%;

    overflow: hidden;

    text-align: center;
}

.change-item {

    display: inline-flex;

    gap: 2px;

    align-items: center;

    justify-content: center;

    max-width: 100%;

    font-size: 8px;

    overflow: hidden;

    text-align: center;
}

.change-icon {

    flex-shrink: 0;
}

.change-value {

    overflow: hidden;

    overflow-wrap: anywhere;

    word-break: break-word;

    text-align: center;
}


/* =====================================================
   돌파
   ===================================================== */

.breakout-wrap {

    display: flex;

    flex-direction: column;

    align-items: center;

    justify-content: center;

    gap: 2px;

    width: 100%;

    max-width: 100%;

    overflow: hidden;

    text-align: center;
}

.warning-wrap {

    width: 100%;

    max-width: 100%;

    overflow: hidden;

    text-align: center;
}

.warning-row {

    display: flex;

    align-items: center;

    justify-content: center;

    gap: 2px;

    width: 100%;

    max-width: 100%;

    overflow: hidden;

    text-align: center;
}

.rocket {

    font-size: 8px;

    white-space: nowrap;

    text-align: center;
}

.warning-empty {

    color: #555;

    white-space: nowrap;

    text-align: center;
}

.warning-period {

    color: #888;

    font-size: 7px;

    white-space: nowrap;
}


/* =====================================================
   EMA
   ===================================================== */

.ema-container {

    width: 100%;

    max-width: 100%;

    overflow: hidden;

    text-align: center;
}

.ema-line {

    display: flex;

    align-items: center;

    justify-content: space-between;

    gap: 1px;

    width: 100%;

    max-width: 100%;

    overflow: hidden;

    text-align: center;
}

.ema-item {

    position: relative;

    display: flex;

    align-items: center;

    justify-content: center;

    gap: 1px;

    width: 50%;

    min-width: 0;

    max-width: 50%;

    overflow: hidden;

    flex-shrink: 1;

    text-align: center;
}

.ema-period {

    color: #888;

    font-size: 6px;

    flex-shrink: 0;

    text-align: center;
}

.ema-value {

    font-size: 7px;

    font-weight: bold;

    flex-shrink: 0;

    text-align: center;
}


/* =====================================================
   설명
   ===================================================== */

.note {

    color: #666;

    font-size: 7px;

    line-height: 1.5;

    margin: 5px 2px 8px 2px;

    padding: 0 2px;

    max-width: 100%;

    overflow-wrap: anywhere;

    word-break: break-word;
}


/* =====================================================
   모바일
   ===================================================== */

@media (max-width: 480px) {

    body {

        padding: 4px;

        font-size: 9px;

        overflow-x: hidden;
    }

    h1 {

        font-size: 14px;

        margin: 2px 2px 5px 2px;
    }

    h2 {

        font-size: 12px;

        margin: 9px 2px 5px 2px;
    }

    .info {

        font-size: 7px;

        line-height: 1.4;

        padding: 5px 6px;

        margin-bottom: 6px;
    }

    .table-wrap {

        border-radius: 8px;
    }

    th {

        padding: 5px 1px;

        font-size: 7px;

        overflow: hidden;

        overflow-wrap: anywhere;

        word-break: break-word;

        text-align: center;
    }

    td {

        padding: 5px 1px;

        overflow: hidden;

        overflow-wrap: anywhere;

        word-break: break-word;

        text-align: center;
    }

    .coin {

        font-size: 8px;

        text-align: center;
    }

    .volume-value {

        font-size: 7px;

        text-align: center;
    }

    .signal-text {

        font-size: 7px;

        text-align: center;
    }

    .signal-none {

        font-size: 7px;

        text-align: center;
    }

    .change-item {

        font-size: 7px;

        gap: 1px;

        justify-content: center;
    }

    .ema-line {

        gap: 1px;

        justify-content: space-between;

        overflow: hidden;
    }

    .ema-item {

        gap: 1px;

        width: 50%;

        max-width: 50%;

        overflow: hidden;

        justify-content: center;
    }

    .ema-period {

        font-size: 5px;

        text-align: center;
    }

    .ema-value {

        font-size: 6px;

        text-align: center;
    }

    .rocket {

        font-size: 7px;

        text-align: center;
    }

    .warning-period {

        font-size: 5px;

        text-align: center;
    }

    .direction-long,
    .direction-short,
    .direction-none {

        font-size: 8px;

        text-align: center;
    }

    .note {

        font-size: 6px;

        line-height: 1.4;

        margin-top: 4px;
    }
}

</style>

</head>

<body>

<h1>
📊 돌파 TOP
</h1>

<div class="info">

1H 추세 + 1H 🚀 확정 돌파

<br>

업비트 거래대금 = 거래소 제공 24H 누적 거래대금

&nbsp;|&nbsp;

OKX 거래대금 = 1H 확정 캔들 거래대금 ÷10

&nbsp;|&nbsp;

TOP"""

    html += str(TOP_N)

    html += """&nbsp;|&nbsp;

EMA 10-30-60-120

<br>

※ 진행 중인 돌파(0)는 표시하지 않음

</div>


<!-- =====================================================
     업비트
     ===================================================== -->

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
※ 업비트 거래대금은 거래소 제공 24시간 누적 거래대금 기준<br>
※ 확정된 1H 돌파만 표시

</div>

</div>


<!-- =====================================================
     OKX
     ===================================================== -->

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
※ 🚀(1+) = 확정된 1H 돌파<br>
※ 진행 중 캔들의 돌파 가능성은 표시하지 않음<br>
※ LONG / SHORT 실제 신호는 확정 돌파만 사용<br>
※ EMA = 1H → 4H

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
