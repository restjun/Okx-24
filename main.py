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

VOLUME_HOURS = 24

TOP_N = 20

UPDATE_MINUTES = 1

MAX_WARNING_COUNT = 3

BREAKOUT_LOOKBACK = 5


# =========================================================
# 거래소 조회 Y / N
#
# Y = 조회
# N = 조회 안 함
# =========================================================

USE_UPBIT = "Y"

USE_OKX = "N"


# =========================================================
# API 안정화 설정
# =========================================================

REQUEST_INTERVAL = 0.08

RATE_LIMIT_WAIT = 3

MAX_RETRIES = 10


# =========================================================
# OKX 실패 종목 반복 설정
# =========================================================

OKX_RETRY_DELAY = 2

OKX_MAX_RETRY_ROUNDS = 0
# 0 = 성공할 때까지 계속 재시도


# =========================================================
# 전역 데이터
# =========================================================

latest_okx_data = []

latest_upbit_data = []

latest_usdt_krw = 0.0


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

        logging.error(
            "USDT-KRW 조회 실패"
        )

        return None

    try:

        data = response.json()

        if not data:

            logging.error(
                "USDT-KRW 데이터 없음"
            )

            return None

        price = float(
            data[0]["trade_price"]
        )

        if price <= 0:

            return None

        logging.info(
            f"USDT-KRW = {price:,.2f}원"
        )

        return price

    except Exception as e:

        logging.error(
            f"USDT-KRW 처리 오류 : {e}"
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
# 업비트 Ticker 거래대금
# =========================================================

def get_upbit_ticker_volume_map(
    markets
):

    if not markets:
        return {}

    volume_map = {}

    chunk_size = 100

    total = len(markets)

    chunks = [
        markets[i:i + chunk_size]
        for i in range(
            0,
            total,
            chunk_size
        )
    ]

    logging.info(
        f"업비트 거래대금 Ticker 조회 "
        f"{total}개 / {len(chunks)}회"
    )

    for chunk_index, chunk in enumerate(
        chunks,
        start=1
    ):

        success = False

        while not success:

            try:

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

                    logging.warning(
                        f"업비트 Ticker 응답 없음 "
                        f"청크 {chunk_index}"
                    )

                    time.sleep(2)

                    continue

                if response.status_code != 200:

                    logging.warning(
                        f"업비트 Ticker HTTP "
                        f"{response.status_code}"
                    )

                    time.sleep(2)

                    continue

                data = response.json()

                if not data:

                    logging.warning(
                        f"업비트 Ticker 데이터 없음 "
                        f"청크 {chunk_index}"
                    )

                    time.sleep(2)

                    continue

                for item in data:

                    market = item.get(
                        "market"
                    )

                    volume = item.get(
                        "acc_trade_price_24h",
                        0
                    )

                    try:

                        volume = float(
                            volume
                        )

                    except Exception:

                        volume = 0

                    if market:

                        volume_map[
                            market
                        ] = volume

                success = True

                logging.info(
                    f"업비트 거래대금 "
                    f"{chunk_index}/{len(chunks)} 완료 "
                    f"({len(data)}개)"
                )

            except Exception as e:

                logging.error(
                    f"업비트 Ticker 실패 "
                    f"청크 {chunk_index}: {e}"
                )

                time.sleep(2)

    logging.info(
        f"업비트 거래대금 조회 완료 "
        f"{len(volume_map)}/{total}"
    )

    return volume_map


# =========================================================
# 업비트 캔들
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

    ema10 = get_ema(df, column, 10)
    ema30 = get_ema(df, column, 30)
    ema60 = get_ema(df, column, 60)
    ema120 = get_ema(df, column, 120)

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

    values = [
        ema10.iloc[-1],
        ema30.iloc[-1],
        ema60.iloc[-1],
        ema120.iloc[-1]
    ]

    if any(
        pd.isna(x)
        for x in values
    ):

        return "none"

    if (
        values[0]
        >
        values[1]
        >
        values[2]
        >
        values[3]
    ):

        return "long"

    if (
        values[0]
        <
        values[1]
        <
        values[2]
        <
        values[3]
    ):

        return "short"

    return "none"


# =========================================================
# EMA 정렬 카운팅
# =========================================================

def get_ema_alignment_count(
    df,
    column
):

    if (
        df is None
        or len(df) < 120
    ):

        return (
            "none",
            0
        )

    ema10 = get_ema(df, column, 10)
    ema30 = get_ema(df, column, 30)
    ema60 = get_ema(df, column, 60)
    ema120 = get_ema(df, column, 120)

    if any(
        x is None
        for x in [
            ema10,
            ema30,
            ema60,
            ema120
        ]
    ):

        return (
            "none",
            0
        )

    count = 0
    current_direction = None

    for index in range(
        len(df) - 1,
        -1,
        -1
    ):

        values = [
            ema10.iloc[index],
            ema30.iloc[index],
            ema60.iloc[index],
            ema120.iloc[index]
        ]

        if any(
            pd.isna(x)
            for x in values
        ):

            break

        if (
            values[0]
            >
            values[1]
            >
            values[2]
            >
            values[3]
        ):

            direction = "long"

        elif (
            values[0]
            <
            values[1]
            <
            values[2]
            <
            values[3]
        ):

            direction = "short"

        else:

            direction = "none"

        if current_direction is None:

            if direction == "none":
                break

            current_direction = direction
            count = 1

        else:

            if direction == current_direction:

                count += 1

            else:

                break

    if current_direction is None:

        return (
            "none",
            0
        )

    return (
        current_direction,
        count
    )


# =========================================================
# EMA 표시
# =========================================================

def check_ema_10_30_60_120(
    df,
    column
):

    direction, count = (
        get_ema_alignment_count(
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
# =========================================================

def get_main_direction(
    df1h,
    df4h,
    column
):

    return (
        get_ema_10_30_60_120_direction(
            df1h,
            column
        )
    )


# =========================================================
# LONG 돌파
# =========================================================

def is_long_breakout(
    row,
    previous
):

    if previous.empty:
        return False

    previous_high = pd.to_numeric(
        previous["h"],
        errors="coerce"
    ).max()

    return (
        row["c"] > previous_high
        and
        row["c"] > row["o"]
    )


# =========================================================
# SHORT 돌파
# =========================================================

def is_short_breakout(
    row,
    previous
):

    if previous.empty:
        return False

    previous_low = pd.to_numeric(
        previous["l"],
        errors="coerce"
    ).min()

    return (
        row["c"] < previous_low
        and
        row["c"] < row["o"]
    )


# =========================================================
# 🚨 LONG 돌파 직전
#
# 현재 캔들의 고가가 직전 5개 확정 캔들 고가를
# 테스트했지만 종가는 아직 돌파하지 않은 경우
# =========================================================

def is_long_prebreakout(
    row,
    previous
):

    if previous.empty:
        return False

    previous_high = pd.to_numeric(
        previous["h"],
        errors="coerce"
    ).max()

    if pd.isna(previous_high):
        return False

    return (
        row["h"] >= previous_high
        and
        row["c"] <= previous_high
        and
        row["c"] > row["o"]
    )


# =========================================================
# 🚨 SHORT 돌파 직전
# =========================================================

def is_short_prebreakout(
    row,
    previous
):

    if previous.empty:
        return False

    previous_low = pd.to_numeric(
        previous["l"],
        errors="coerce"
    ).min()

    if pd.isna(previous_low):
        return False

    return (
        row["l"] <= previous_low
        and
        row["c"] >= previous_low
        and
        row["c"] < row["o"]
    )


# =========================================================
# 확정 돌파 + 돌파 직전 + -1/-2
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

    current_index = len(df) - 1

    cur = df.iloc[
        current_index
    ]

    previous = df.iloc[
        current_index - BREAKOUT_LOOKBACK:
        current_index
    ]

    long_ema = (
        cur["ema10"]
        >
        cur["ema30"]
        >
        cur["ema60"]
        >
        cur["ema120"]
    )

    short_ema = (
        cur["ema10"]
        <
        cur["ema30"]
        <
        cur["ema60"]
        <
        cur["ema120"]
    )

    # =====================================================
    # 현재 확정 돌파
    # =====================================================

    if long_ema:

        if is_long_breakout(
            cur,
            previous
        ):

            count = 0

            for index in range(
                current_index,
                -1,
                -1
            ):

                if index < BREAKOUT_LOOKBACK:
                    break

                row = df.iloc[index]

                prev = df.iloc[
                    index - BREAKOUT_LOOKBACK:
                    index
                ]

                row_long_ema = (
                    row["ema10"]
                    >
                    row["ema30"]
                    >
                    row["ema60"]
                    >
                    row["ema120"]
                )

                state = (
                    row_long_ema
                    and
                    is_long_breakout(
                        row,
                        prev
                    )
                )

                if state:

                    count += 1

                else:

                    break

            if count > 0:

                return (
                    f"long_breakout_{count}"
                )


    if short_ema:

        if is_short_breakout(
            cur,
            previous
        ):

            count = 0

            for index in range(
                current_index,
                -1,
                -1
            ):

                if index < BREAKOUT_LOOKBACK:
                    break

                row = df.iloc[index]

                prev = df.iloc[
                    index - BREAKOUT_LOOKBACK:
                    index
                ]

                row_short_ema = (
                    row["ema10"]
                    <
                    row["ema30"]
                    <
                    row["ema60"]
                    <
                    row["ema120"]
                )

                state = (
                    row_short_ema
                    and
                    is_short_breakout(
                        row,
                        prev
                    )
                )

                if state:

                    count += 1

                else:

                    break

            if count > 0:

                return (
                    f"short_breakout_{count}"
                )


    # =====================================================
    # 돌파 후 -1 / -2
    # =====================================================

    for age in [1, 2]:

        breakout_index = (
            current_index - age
        )

        if breakout_index < BREAKOUT_LOOKBACK:
            continue

        breakout_row = df.iloc[
            breakout_index
        ]

        breakout_prev = df.iloc[
            breakout_index - BREAKOUT_LOOKBACK:
            breakout_index
        ]

        breakout_long_ema = (
            breakout_row["ema10"]
            >
            breakout_row["ema30"]
            >
            breakout_row["ema60"]
            >
            breakout_row["ema120"]
        )

        breakout_short_ema = (
            breakout_row["ema10"]
            <
            breakout_row["ema30"]
            <
            breakout_row["ema60"]
            <
            breakout_row["ema120"]
        )

        # -------------------------------------------------
        # LONG -1 / -2
        # -------------------------------------------------

        if (
            breakout_long_ema
            and
            is_long_breakout(
                breakout_row,
                breakout_prev
            )
        ):

            breakout_high = float(
                breakout_row["h"]
            )

            after = df.iloc[
                breakout_index + 1:
                current_index + 1
            ]

            if not after.empty:

                new_high = pd.to_numeric(
                    after["h"],
                    errors="coerce"
                ).max()

                if new_high <= breakout_high:

                    return (
                        f"long_breakout_-{age}"
                    )

        # -------------------------------------------------
        # SHORT -1 / -2
        # -------------------------------------------------

        if (
            breakout_short_ema
            and
            is_short_breakout(
                breakout_row,
                breakout_prev
            )
        ):

            breakout_low = float(
                breakout_row["l"]
            )

            after = df.iloc[
                breakout_index + 1:
                current_index + 1
            ]

            if not after.empty:

                new_low = pd.to_numeric(
                    after["l"],
                    errors="coerce"
                ).min()

                if new_low >= breakout_low:

                    return (
                        f"short_breakout_-{age}"
                    )


    # =====================================================
    # 🚨 돌파 직전
    #
    # 확정 돌파가 아니면 마지막에 검사
    # =====================================================

    if long_ema:

        if is_long_prebreakout(
            cur,
            previous
        ):

            return "long_prebreakout"


    if short_ema:

        if is_short_prebreakout(
            cur,
            previous
        ):

            return "short_prebreakout"


    return "none"


# =========================================================
# 메인 진입 경고
# =========================================================

def check_entry_warning(
    df1h,
    df4h,
    column
):

    breakout_1h = check_breakout(
        df1h,
        column
    )

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
    column
):

    main_direction = get_main_direction(
        df1h,
        df4h,
        column
    )

    if main_direction == "none":

        return (
            "",
            "none",
            "none"
        )

    breakout_1h, breakout_4h = (
        check_entry_warning(
            df1h,
            df4h,
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

        try:

            count = int(
                breakout_1h.split("_")[-1]
            )

            if count > 0:

                signal = "LONG"

        except Exception:

            pass

    elif (
        main_direction == "short"
        and
        breakout_1h.startswith(
            "short_breakout_"
        )
    ):

        try:

            count = int(
                breakout_1h.split("_")[-1]
            )

            if count > 0:

                signal = "SHORT"

        except Exception:

            pass

    return (
        signal,
        breakout_1h,
        breakout_4h
    )


# =========================================================
# 빈 EMA
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

        return empty_ema()

    signal, warning_1h, warning_4h = (
        get_trade_signal(
            df1h,
            df4h,
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

        return empty_ema()

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

    while True:

        try:

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

                time.sleep(
                    OKX_RETRY_DELAY
                )

                continue

            if len(df) < hours:

                time.sleep(
                    OKX_RETRY_DELAY
                )

                continue

            volume_usdt = float(
                df["volCcyQuote"]
                .tail(hours)
                .sum()
            )

            volume_usdt = (
                volume_usdt / 10
            )

            if volume_usdt <= 0:

                time.sleep(
                    OKX_RETRY_DELAY
                )

                continue

            volume_krw = (
                volume_usdt
                *
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
                f"{inst_id} : {e}"
            )

            time.sleep(
                OKX_RETRY_DELAY
            )


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

        return symbols

    except Exception as e:

        logging.error(
            f"OKX 목록 오류 : {e}"
        )

        return []


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

    for i in [-1, -2, -3]:

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

    for i in [-1, -2, -3]:

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
        '<span class="signal-none">—</span>'
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
        '<span class="direction-none">—</span>'
    )


# =========================================================
# 경고 HTML
# =========================================================

def warning_html(
    warning_1h,
    change_percent
):

    if warning_1h == "long_prebreakout":

        return (
            '<span class="warning-icon prebreak-long">'
            '🚨'
            '</span>'
        )

    if warning_1h == "short_prebreakout":

        return (
            '<span class="warning-icon prebreak-short">'
            '🚨'
            '</span>'
        )

    if (
        not warning_1h.startswith(
            "long_breakout_"
        )
        and
        not warning_1h.startswith(
            "short_breakout_"
        )
    ):

        return (
            '<span class="warning-empty">—</span>'
        )

    if warning_1h.startswith(
        "long_breakout_"
    ):

        if (
            change_percent is None
            or
            change_percent <= 0
        ):

            return (
                '<span class="warning-empty">—</span>'
            )

    if warning_1h.startswith(
        "short_breakout_"
    ):

        if (
            change_percent is None
            or
            change_percent >= 0
        ):

            return (
                '<span class="warning-empty">—</span>'
            )

    try:

        count = int(
            warning_1h.split("_")[-1]
        )

    except Exception:

        return (
            '<span class="warning-empty">—</span>'
        )

    if count == 0:

        return (
            '<span class="warning-empty">—</span>'
        )

    if count < 0:

        return (
            '<span class="warning-icon rocket">'
            f'🚀({count})'
            '</span>'
        )

    return (
        '<span class="warning-icon rocket">'
        f'🚀({count})'
        '</span>'
    )


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

                <span class="ema-period">
                    1H
                </span>

                <span class="ema-value">
                    {ema_1h}
                </span>

            </div>

            <div class="ema-item">

                <span class="ema-period">
                    4H
                </span>

                <span class="ema-value">
                    {ema_4h}
                </span>

            </div>

        </div>

    </div>
    """


# =========================================================
# 경고 종류
# =========================================================

def get_warning_type(
    warning
):

    if warning in (
        "long_prebreakout",
        "short_prebreakout"
    ):

        return "prebreak"

    if warning.startswith(
        "long_breakout_"
    ):

        try:

            count = int(
                warning.split("_")[-1]
            )

            if count > 0:
                return "breakout"

            if count < 0:
                return "failed"

        except Exception:

            pass

    if warning.startswith(
        "short_breakout_"
    ):

        try:

            count = int(
                warning.split("_")[-1]
            )

            if count > 0:
                return "breakout"

            if count < 0:
                return "failed"

        except Exception:

            pass

    return "none"


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

    volume_map = get_upbit_ticker_volume_map(
        markets
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

        except Exception as e:

            logging.error(
                f"업비트 상세 오류 "
                f"{market} : {e}"
            )

    latest_upbit_data = rows

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

    if (
        usdt_krw is None
        or
        usdt_krw <= 0
    ):

        return False

    symbols = get_all_okx_swap_symbols()

    if not symbols:
        return False

    upbit_markets = get_upbit_markets()

    upbit_coin_set = {
        market.replace(
            "KRW-",
            ""
        )
        for market in upbit_markets
    }

    volume_map = {}

    for index, symbol in enumerate(
        symbols,
        start=1
    ):

        while True:

            try:

                volume = get_okx_volume(
                    symbol,
                    usdt_krw
                )

                if volume > 0:

                    volume_map[
                        symbol
                    ] = volume

                    logging.info(
                        f"OKX 거래대금 "
                        f"{index}/{len(symbols)} "
                        f"{symbol} 완료"
                    )

                    break

            except Exception as e:

                logging.error(
                    f"OKX 거래대금 실패 "
                    f"{symbol} : {e}"
                )

            time.sleep(
                OKX_RETRY_DELAY
            )

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
                f"OKX 상세 오류 "
                f"{symbol} : {e}"
            )

    latest_okx_data = rows

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
        "전체 조회 시작"
    )

    if USE_UPBIT not in (
        "Y",
        "N"
    ):

        logging.error(
            f"USE_UPBIT 설정 오류 : {USE_UPBIT}"
        )

        return

    if USE_OKX not in (
        "Y",
        "N"
    ):

        logging.error(
            f"USE_OKX 설정 오류 : {USE_OKX}"
        )

        return

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

        logging.info(
            "업비트 조회 N - 조회하지 않음"
        )

    # =====================================================
    # OKX
    # =====================================================

    if USE_OKX == "Y":

        try:

            usdt_krw = get_usdt_krw()

            if usdt_krw is not None:

                latest_usdt_krw = usdt_krw

            else:

                usdt_krw = latest_usdt_krw

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
            "OKX 조회 N - 조회하지 않음"
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
# 대시보드 CSS
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
    font-size: 10px;
    padding: 6px;
    overflow-wrap: anywhere;
}

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

.exchange-status {
    display: flex;
    gap: 8px;
    margin-top: 5px;
    font-size: 8px;
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
    vertical-align: middle;
    text-align: center;
    overflow: hidden;
    overflow-wrap: anywhere;
    word-break: break-word;
}

tbody tr:last-child td {
    border-bottom: none;
}


/* =======================================================
   경고 행 전체 반짝임
   ======================================================= */

.warning-row {
    animation:
        warningFlash 1.1s infinite;
}

.prebreak-row {
    animation:
        prebreakFlash 1.0s infinite;
}

.breakout-row {
    animation:
        breakoutFlash 1.0s infinite;
}

.failed-row {
    animation:
        failedFlash 1.3s infinite;
}


/* =======================================================
   LONG / SHORT 행
   ======================================================= */

.long-row td {
    background: rgba(30, 180, 80, 0.10);
}

.short-row td {
    background: rgba(220, 50, 50, 0.10);
}


/* =======================================================
   🚨 돌파 직전
   ======================================================= */

.prebreak-long td {
    box-shadow:
        inset 0 0 12px rgba(255, 190, 0, 0.35);
}

.prebreak-short td {
    box-shadow:
        inset 0 0 12px rgba(255, 80, 80, 0.35);
}


/* =======================================================
   🚀 확정 돌파
   ======================================================= */

.breakout-row td {
    box-shadow:
        inset 0 0 12px rgba(255, 255, 255, 0.18);
}


/* =======================================================
   🚀 -1 / -2
   ======================================================= */

.failed-row td {
    box-shadow:
        inset 0 0 10px rgba(255, 150, 0, 0.22);
}


/* =======================================================
   애니메이션
   ======================================================= */

@keyframes warningFlash {

    0% {
        filter: brightness(1);
    }

    50% {
        filter: brightness(1.55);
    }

    100% {
        filter: brightness(1);
    }
}

@keyframes prebreakFlash {

    0% {
        filter: brightness(1);
    }

    50% {
        filter: brightness(1.7);
    }

    100% {
        filter: brightness(1);
    }
}

@keyframes breakoutFlash {

    0% {
        filter: brightness(1);
    }

    50% {
        filter: brightness(1.65);
    }

    100% {
        filter: brightness(1);
    }
}

@keyframes failedFlash {

    0% {
        filter: brightness(1);
    }

    50% {
        filter: brightness(1.45);
    }

    100% {
        filter: brightness(1);
    }
}


/* =======================================================
   기본 컬럼
   ======================================================= */

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
    justify-content: center;
    gap: 2px;
    width: 100%;
}

.coin {
    display: block;
    width: 100%;
    font-weight: bold;
    text-align: center;
    font-size: 9px;
    line-height: 1.2;
}

.direction-long,
.direction-short,
.direction-none {
    display: block;
    width: 100%;
    font-size: 9px;
    text-align: center;
}

.direction-long {
    filter: drop-shadow(
        0 0 4px rgba(255, 220, 0, 0.7)
    );
}

.direction-short {
    filter: drop-shadow(
        0 0 4px rgba(100, 150, 255, 0.7)
    );
}

.volume-wrap {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 3px;
    width: 100%;
}

.volume-value {
    display: block;
    width: 100%;
    font-size: 8px;
    font-weight: 600;
    text-align: center;
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

.today-wrap {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 3px;
    width: 100%;
}

.change-item {
    display: inline-flex;
    gap: 2px;
    align-items: center;
    justify-content: center;
    max-width: 100%;
    font-size: 8px;
    text-align: center;
}

.change-icon {
    flex-shrink: 0;
}

.breakout-wrap {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 2px;
    width: 100%;
}

.warning-empty {
    color: #555;
    white-space: nowrap;
}

.warning-icon {
    font-size: 9px;
    white-space: nowrap;
    font-weight: bold;
}

.prebreak-long {
    filter: drop-shadow(
        0 0 5px rgba(255, 190, 0, 0.9)
    );
}

.prebreak-short {
    filter: drop-shadow(
        0 0 5px rgba(255, 60, 60, 0.9)
    );
}

.rocket {
    filter: drop-shadow(
        0 0 4px rgba(255, 255, 255, 0.7)
    );
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
    width: 100%;
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
    flex-shrink: 0;
}

.ema-value {
    font-size: 7px;
    font-weight: bold;
    flex-shrink: 0;
}

.note {
    color: #666;
    font-size: 7px;
    line-height: 1.5;
    margin: 5px 2px 8px 2px;
}


/* =======================================================
   모바일
   ======================================================= */

@media (max-width: 480px) {

    body {
        padding: 4px;
        font-size: 9px;
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
    }

    .exchange-status {
        font-size: 7px;
        gap: 6px;
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

    .signal-text,
    .signal-none {
        font-size: 7px;
    }

    .change-item {
        font-size: 7px;
        gap: 1px;
    }

    .ema-period {
        font-size: 5px;
    }

    .ema-value {
        font-size: 6px;
    }

    .rocket,
    .warning-icon {
        font-size: 8px;
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
"""


# =========================================================
# 테이블 행 생성
#
# 정상 종목은 표시하지 않음
# 경고 종목만 표시
# =========================================================

def make_table_rows(
    data
):

    rows_html = ""

    for item in data:

        ema = item.get(
            "ema",
            empty_ema()
        )

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

        warning_type = get_warning_type(
            warning_1h
        )

        # -------------------------------------------------
        # 정상 종목은 표시하지 않음
        # -------------------------------------------------

        if warning_type == "none":

            continue

        # -------------------------------------------------
        # 돌파 0은 표시하지 않음
        # -------------------------------------------------

        if warning_1h in (
            "long_breakout_0",
            "short_breakout_0"
        ):

            continue

        # -------------------------------------------------
        # 현재 확정 돌파
        # -------------------------------------------------

        valid_breakout = (

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
                and
                not warning_1h.startswith(
                    "long_breakout_-"
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
                and
                not warning_1h.startswith(
                    "short_breakout_-"
                )
            )
        )

        # -------------------------------------------------
        # 돌파 후 -1 / -2
        # -------------------------------------------------

        valid_failed = False

        if warning_1h.startswith(
            "long_breakout_-"
        ):

            valid_failed = (
                direction == "long"
                and
                change_percent is not None
                and
                change_percent > 0
            )

        elif warning_1h.startswith(
            "short_breakout_-"
        ):

            valid_failed = (
                direction == "short"
                and
                change_percent is not None
                and
                change_percent < 0
            )

        # -------------------------------------------------
        # 🚨 돌파 직전
        # -------------------------------------------------

        valid_prebreakout = (
            warning_1h in (
                "long_prebreakout",
                "short_prebreakout"
            )
        )

        # -------------------------------------------------
        # 최종 표시 여부
        # -------------------------------------------------

        if not (
            valid_breakout
            or
            valid_failed
            or
            valid_prebreakout
        ):

            continue

        # -------------------------------------------------
        # 행 클래스
        # -------------------------------------------------

        row_classes = [
            "warning-row"
        ]

        if (
            direction == "long"
        ):

            row_classes.append(
                "long-row"
            )

        elif (
            direction == "short"
        ):

            row_classes.append(
                "short-row"
            )

        if valid_prebreakout:

            row_classes.append(
                "prebreak-row"
            )

            if warning_1h == "long_prebreakout":

                row_classes.append(
                    "prebreak-long"
                )

            else:

                row_classes.append(
                    "prebreak-short"
                )

        elif valid_breakout:

            row_classes.append(
                "breakout-row"
            )

        elif valid_failed:

            row_classes.append(
                "failed-row"
            )

        row_class_string = " ".join(
            row_classes
        )

        # -------------------------------------------------
        # 행
        # -------------------------------------------------

        rows_html += f"""
<tr class="{row_class_string}">

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
    change_percent
)}

</div>

</div>

</td>

<td>

{ema_html(
    ema
)}

</td>

</tr>
"""

    return rows_html


# =========================================================
# 거래소 테이블
# =========================================================

def make_exchange_section(
    title,
    data
):

    rows = make_table_rows(
        data
    )

    if not rows:

        rows = """
<tr>
<td colspan="5" style="
    color:#555;
    padding:12px 4px;
">
현재 경고 종목 없음
</td>
</tr>
"""

    return f"""
<div class="section">

<h2>
{title} 경고
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

<div class="note">

※ 🚨 = EMA 정렬 상태에서 직전 {BREAKOUT_LOOKBACK}개 확정 캔들
고가/저가를 테스트했지만 아직 종가 확정 돌파 전<br>

※ 🚀(1), 🚀(2) = 확정 돌파<br>

※ 🚀(-1), 🚀(-2) = 돌파 후 다음 고점/저점 갱신 실패<br>

※ 돌파 0은 표시하지 않음<br>

※ 정상 종목은 표시하지 않음

</div>

</div>
"""


# =========================================================
# 웹 대시보드
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

    # -----------------------------------------------------
    # 조회 Y인 거래소만 표시
    # 단, 데이터는 경고 종목만 표시
    # -----------------------------------------------------

    exchange_sections = ""

    if USE_UPBIT == "Y":

        exchange_sections += (
            make_exchange_section(
                "🏆 업비트",
                latest_upbit_data
            )
        )

    if USE_OKX == "Y":

        exchange_sections += (
            make_exchange_section(
                "🏆 OKX",
                latest_okx_data
            )
        )

    html = f"""<!DOCTYPE html>

<html lang="ko">

<head>

<meta charset="UTF-8">

<meta http-equiv="refresh" content="60">

<meta name="viewport"
      content="width=device-width,
               initial-scale=1.0,
               maximum-scale=1.0,
               user-scalable=no">

<title>돌파 경고</title>

<style>

{DASHBOARD_CSS}

</style>

</head>

<body>

<h1>
🚨 돌파 경고
</h1>

<div class="info">

<div>
1H 추세 + 1H 확정 돌파 |
직전 {BREAKOUT_LOOKBACK}개 확정 캔들 고가/저가 기준
</div>

<div>
업비트 거래대금 = 거래소 24시간 누적 거래대금
&nbsp;|&nbsp;
OKX 거래대금 = 1H 확정 캔들 {VOLUME_HOURS}개 × USDT-KRW
</div>

<div>
TOP{TOP_N}
&nbsp;|&nbsp;
EMA 10-30-60-120 정렬 카운팅
</div>

<div class="exchange-status">

<span class="{upbit_status_class}">
업비트 조회 : {USE_UPBIT}
</span>

<span class="{okx_status_class}">
OKX 조회 : {USE_OKX}
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
        "서버 시작"
    )

    logging.info(
        f"조회 설정 "
        f"업비트={USE_UPBIT} "
        f"OKX={USE_OKX}"
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
    # 최초 1회 즉시 실행
    # -----------------------------------------------------

    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    # -----------------------------------------------------
    # 주기 실행
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


# =========================================================
# 실행
# =========================================================

if __name__ == "__main__":

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
        )9s
