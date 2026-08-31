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

TOP_N = 30

UPDATE_MINUTES = 1

MAX_WARNING_COUNT = 3

BREAKOUT_LOOKBACK = 10


# =========================================================
# 거래소 조회 Y / N
# =========================================================

USE_UPBIT = "Y"

USE_OKX = "Y"


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
                        2 * (2 ** attempt),
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
            df["confirm"].astype(str) == "1"
        ]

        if df.empty:
            return None

        df = (
            df.iloc[::-1]
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

    chunks = [
        markets[i:i + chunk_size]
        for i in range(
            0,
            len(markets),
            chunk_size
        )
    ]

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

                    time.sleep(2)
                    continue

                if response.status_code != 200:

                    time.sleep(2)
                    continue

                data = response.json()

                if not data:

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

                        volume = float(volume)

                    except Exception:

                        volume = 0

                    if market:

                        volume_map[market] = volume

                success = True

            except Exception as e:

                logging.error(
                    f"업비트 Ticker 실패 "
                    f"청크 {chunk_index}: {e}"
                )

                time.sleep(2)

    return volume_map


# =========================================================
# 업비트 캔들
# =========================================================

def get_upbit_ohlcv(
    market,
    unit=240,
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
            df.iloc[::-1]
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

        return [
            x["market"]
            for x in data
            if x["market"].startswith("KRW-")
        ]

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
# EMA 10-30
# =========================================================

def get_ema_10_30_direction(
    df,
    column
):

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
        or
        ema30 is None
    ):

        return "none"

    a = ema10.iloc[-1]
    b = ema30.iloc[-1]

    if pd.isna(a) or pd.isna(b):

        return "none"

    if a > b:

        return "long"

    if a < b:

        return "short"

    return "none"


# =========================================================
# EMA 10-30-60
# =========================================================

def get_ema_10_30_60_direction(
    df,
    column
):

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

    if (
        ema10 is None
        or
        ema30 is None
        or
        ema60 is None
    ):

        return "none"

    values = [
        ema10.iloc[-1],
        ema30.iloc[-1],
        ema60.iloc[-1]
    ]

    if any(
        pd.isna(x)
        for x in values
    ):

        return "none"

    if values[0] > values[1] > values[2]:

        return "long"

    if values[0] < values[1] < values[2]:

        return "short"

    return "none"


# =========================================================
# EMA 정렬 카운트
# =========================================================

def get_ema_10_30_60_alignment_count(
    df,
    column
):

    if (
        df is None
        or
        len(df) < 60
    ):

        return "none", 0

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

    if any(
        x is None
        for x in [
            ema10,
            ema30,
            ema60
        ]
    ):

        return "none", 0

    current_direction = None

    count = 0

    for index in range(
        len(df) - 1,
        -1,
        -1
    ):

        values = [
            ema10.iloc[index],
            ema30.iloc[index],
            ema60.iloc[index]
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
        ):

            direction = "long"

        elif (
            values[0]
            <
            values[1]
            <
            values[2]
        ):

            direction = "short"

        else:

            direction = "none"

        if current_direction is None:

            if direction == "none":

                break

            current_direction = direction

            count = 1

        elif direction == current_direction:

            count += 1

        else:

            break

    if current_direction is None:

        return "none", 0

    return (
        current_direction,
        count
    )


# =========================================================
# 메인 방향
# =========================================================

def get_main_direction(
    df,
    column
):

    if (
        df is not None
        and
        len(df) >= 60
    ):

        direction = (
            get_ema_10_30_60_direction(
                df,
                column
            )
        )

        if direction != "none":

            return direction

    return get_ema_10_30_direction(
        df,
        column
    )


# =========================================================
# EMA 표시
# =========================================================

def check_ema(df):

    if (
        df is not None
        and
        len(df) >= 60
    ):

        direction, count = (
            get_ema_10_30_60_alignment_count(
                df,
                "c"
            )
        )

    else:

        direction = (
            get_ema_10_30_direction(
                df,
                "c"
            )
        )

        count = 0

        if direction != "none":

            ema10 = get_ema(
                df,
                "c",
                10
            )

            ema30 = get_ema(
                df,
                "c",
                30
            )

            if (
                ema10 is not None
                and
                ema30 is not None
            ):

                for i in range(
                    len(df) - 1,
                    -1,
                    -1
                ):

                    a = ema10.iloc[i]
                    b = ema30.iloc[i]

                    if (
                        pd.isna(a)
                        or
                        pd.isna(b)
                    ):

                        break

                    current = (
                        "long"
                        if a > b
                        else
                        "short"
                        if a < b
                        else
                        "none"
                    )

                    if current != direction:

                        break

                    count += 1

    if direction == "long":

        display = f"🟢({count})"

    elif direction == "short":

        display = f"🔴({count})"

    else:

        display = "⚪"

    return {
        "display": display,
        "direction": direction,
        "count": count
    }


# =========================================================
# 최초 돌파
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
# 돌파 직전
# =========================================================

def is_long_pre_breakout(
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
        row["h"] >= previous_high
        and
        row["c"] <= previous_high
        and
        row["c"] >= row["o"]
    )


def is_short_pre_breakout(
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
        row["l"] <= previous_low
        and
        row["c"] >= previous_low
        and
        row["c"] <= row["o"]
    )


# =========================================================
# 시간봉 돌파 상태
#
# 이번 버전에서는 🚨 돌파전만 사용
#
# 반환:
# pre  = 돌파전
# none = 표시하지 않음
#
# 🚀 1 / 2 / 3 등은 사용하지 않음
# =========================================================

def get_timeframe_breakout(
    df,
    direction
):

    if (
        df is None
        or
        len(df) < BREAKOUT_LOOKBACK + 30
    ):

        return "none"

    df = (
        df.copy()
        .reset_index(drop=True)
    )

    ema10 = get_ema(
        df,
        "c",
        10
    )

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

    if (
        ema10 is None
        or
        ema30 is None
    ):

        return "none"

    current_index = len(df) - 1

    row = df.iloc[current_index]

    previous = df.iloc[
        current_index - BREAKOUT_LOOKBACK:
        current_index
    ]

    e10 = ema10.iloc[current_index]

    e30 = ema30.iloc[current_index]

    if (
        pd.isna(e10)
        or
        pd.isna(e30)
    ):

        return "none"

    if ema60 is not None:

        e60 = ema60.iloc[current_index]

        if pd.isna(e60):

            long_ema = e10 > e30

            short_ema = e10 < e30

        else:

            long_ema = (
                e10 > e30 > e60
            )

            short_ema = (
                e10 < e30 < e60
            )

    else:

        long_ema = e10 > e30

        short_ema = e10 < e30

    # =====================================================
    # LONG 돌파전
    # =====================================================

    if (
        direction == "long"
        and
        long_ema
        and
        is_long_pre_breakout(
            row,
            previous
        )
    ):

        return "pre"

    # =====================================================
    # SHORT 돌파전
    # =====================================================

    if (
        direction == "short"
        and
        short_ema
        and
        is_short_pre_breakout(
            row,
            previous
        )
    ):

        return "pre"

    return "none"


# =========================================================
# 당일 방향
#
# 양수 = long
# 음수 = short
# 0    = none
# =========================================================

def get_today_direction(
    change_percent
):

    if change_percent is None:

        return "none"

    try:

        value = float(
            change_percent
        )

    except Exception:

        return "none"

    if value > 0:

        return "long"

    if value < 0:

        return "short"

    return "none"


# =========================================================
# 해 / 구름 표시
#
# 당일 양수 = ☀️
# 당일 음수 = ☁️
# =========================================================

def weather_html(
    change_percent
):

    direction = get_today_direction(
        change_percent
    )

    if direction == "long":

        return (
            '<span class="weather-sun">'
            '☀️ LONG'
            '</span>'
        )

    if direction == "short":

        return (
            '<span class="weather-cloud">'
            '☁️ SHORT'
            '</span>'
        )

    return (
        '<span class="weather-none">'
        '—'
        '</span>'
    )


# =========================================================
# 1H + 4H 돌파 상태
# =========================================================

def get_combined_breakout_warning(
    df1h,
    df4h
):

    direction_1h = get_main_direction(
        df1h,
        "c"
    )

    direction_4h = get_main_direction(
        df4h,
        "c"
    )

    warning_1h = get_timeframe_breakout(
        df1h,
        direction_1h
    )

    warning_4h = get_timeframe_breakout(
        df4h,
        direction_4h
    )

    return {
        "1h": warning_1h,
        "4h": warning_4h,

        "1h_direction":
            direction_1h,

        "4h_direction":
            direction_4h
    }


# =========================================================
# 최종 경고 조건
#
# change 양수:
#     LONG 돌파전만 허용
#
# change 음수:
#     SHORT 돌파전만 허용
#
# 업비트:
#     LONG만 허용
#
# OKX:
#     LONG / SHORT 허용
# =========================================================

def get_final_warning(
    warning,
    change_percent,
    exchange
):

    if not warning:

        return {
            "show": False,
            "direction": "none"
        }

    today_direction = (
        get_today_direction(
            change_percent
        )
    )

    if today_direction == "none":

        return {
            "show": False,
            "direction": "none"
        }

    # -----------------------------------------------------
    # 업비트는 LONG만 표시
    # -----------------------------------------------------

    if (
        exchange == "UPBIT"
        and
        today_direction != "long"
    ):

        return {
            "show": False,
            "direction": "none"
        }

    # -----------------------------------------------------
    # 당일 양수 → LONG
    # -----------------------------------------------------

    if today_direction == "long":

        if (
            warning.get("1h") == "pre"
            and
            warning.get("1h_direction")
            == "long"
        ):

            return {
                "show": True,
                "direction": "long",
                "timeframe": "1H"
            }

        if (
            warning.get("4h") == "pre"
            and
            warning.get("4h_direction")
            == "long"
        ):

            return {
                "show": True,
                "direction": "long",
                "timeframe": "4H"
            }

    # -----------------------------------------------------
    # 당일 음수 → SHORT
    # -----------------------------------------------------

    if today_direction == "short":

        if (
            warning.get("1h") == "pre"
            and
            warning.get("1h_direction")
            == "short"
        ):

            return {
                "show": True,
                "direction": "short",
                "timeframe": "1H"
            }

        if (
            warning.get("4h") == "pre"
            and
            warning.get("4h_direction")
            == "short"
        ):

            return {
                "show": True,
                "direction": "short",
                "timeframe": "4H"
            }

    return {
        "show": False,
        "direction": "none"
    }


# =========================================================
# 경고 HTML
#
# 🚨만 표시
# =========================================================

def combined_warning_html(
    warning,
    final_warning
):

    if (
        not final_warning
        or
        not final_warning.get("show")
    ):

        return ""

    direction = final_warning.get(
        "direction"
    )

    timeframe = final_warning.get(
        "timeframe"
    )

    if direction == "long":

        return (
            '<span class="warning-box warning-long">'
            f'🚨 LONG {timeframe}'
            '</span>'
        )

    if direction == "short":

        return (
            '<span class="warning-box warning-short">'
            f'🚨 SHORT {timeframe}'
            '</span>'
        )

    return ""


# =========================================================
# 변동률
# =========================================================

def get_upbit_change(market):

    df = get_upbit_ohlcv(
        market,
        60,
        120
    )

    if df is None or len(df) < 50:

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
            round(change, 2)
        )

    return result


# =========================================================
# OKX 변동률
# =========================================================

def get_okx_change(inst_id):

    df = get_okx_ohlcv(
        inst_id,
        "1H",
        120
    )

    if df is None or len(df) < 50:

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
            round(change, 2)
        )

    return result


# =========================================================
# 변동률 HTML
# =========================================================

def format_change(changes):

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
        f'{icon} {sign}{x:.2f}%'
        '</span>'
    )


# =========================================================
# EMA HTML
#
# 모바일에서 잘리지 않도록
# 1H / 4H 두 줄
# =========================================================

def ema_html(
    ema_1h,
    ema_4h
):

    return f"""
    <div class="ema-value">

        <div class="ema-line ema-1h">
            1H&nbsp;&nbsp;{ema_1h}
        </div>

        <div class="ema-line ema-4h">
            4H&nbsp;&nbsp;{ema_4h}
        </div>

    </div>
    """


# =========================================================
# 빈 EMA
# =========================================================

def empty_ema():

    return {
        "display": "⚪",
        "direction": "none",
        "count": 0
    }


# =========================================================
# 업비트 EMA
#
# 현재 진행 중인 캔들 제외
# =========================================================

def get_upbit_ema(market):

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
            "1h_ema": empty_ema(),
            "4h_ema": empty_ema(),
            "warning": {
                "1h": "none",
                "4h": "none",
                "1h_direction": "none",
                "4h_direction": "none"
            }
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

    ema1h = check_ema(
        df1h
    )

    ema4h = check_ema(
        df4h
    )

    warning = (
        get_combined_breakout_warning(
            df1h,
            df4h
        )
    )

    return {
        "1h_ema": ema1h,
        "4h_ema": ema4h,
        "warning": warning
    }


# =========================================================
# OKX EMA
# =========================================================

def get_okx_ema(inst_id):

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
            "1h_ema": empty_ema(),
            "4h_ema": empty_ema(),
            "warning": {
                "1h": "none",
                "4h": "none",
                "1h_direction": "none",
                "4h_direction": "none"
            }
        }

    ema1h = check_ema(
        df1h
    )

    ema4h = check_ema(
        df4h
    )

    warning = (
        get_combined_breakout_warning(
            df1h,
            df4h
        )
    )

    return {
        "1h_ema": ema1h,
        "4h_ema": ema4h,
        "warning": warning
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

            if df is None or df.empty:

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

    volume_map = (
        get_upbit_ticker_volume_map(
            markets
        )
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

            change_percent = (
                changes[0]
                if (
                    changes is not None
                    and
                    len(changes) > 0
                )
                else None
            )

            ema = get_upbit_ema(
                market
            )

            warning = ema.get(
                "warning",
                {}
            )

            final_warning = (
                get_final_warning(
                    warning,
                    change_percent,
                    "UPBIT"
                )
            )

            # =================================================
            # 최종 경고 조건에 맞는 종목만 표시
            # =================================================

            if not final_warning.get(
                "show",
                False
            ):

                continue

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

                    "weather":
                        weather_html(
                            change_percent
                        ),

                    "volume":
                        format_volume(
                            volume_map[market]
                        ),

                    "ema_1h":
                        ema["1h_ema"],

                    "ema_4h":
                        ema["4h_ema"],

                    "warning":
                        warning,

                    "final_warning":
                        final_warning
                }
            )

        except Exception as e:

            logging.error(
                f"업비트 상세 오류 "
                f"{market} : {e}"
            )

    latest_upbit_data = rows

    logging.info(
        f"업비트 경고 종목 "
        f"{len(rows)}개"
    )

    logging.info(
        f"========== 업비트 TOP{TOP_N} 완료 =========="
    )

    return True


# =========================================================
# OKX 업데이트
# =========================================================

def update_okx(usdt_krw):

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

                    volume_map[symbol] = volume

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

            coin = f"{coin}[UP]"

        try:

            changes = get_okx_change(
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

            ema = get_okx_ema(
                symbol
            )

            warning = ema.get(
                "warning",
                {}
            )

            # =================================================
            # OKX는 LONG / SHORT 모두 허용
            # =================================================

            final_warning = (
                get_final_warning(
                    warning,
                    change_percent,
                    "OKX"
                )
            )

            if not final_warning.get(
                "show",
                False
            ):

                continue

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

                    "weather":
                        weather_html(
                            change_percent
                        ),

                    "volume":
                        format_volume(
                            volume_map[symbol]
                        ),

                    "ema_1h":
                        ema["1h_ema"],

                    "ema_4h":
                        ema["4h_ema"],

                    "warning":
                        warning,

                    "final_warning":
                        final_warning
                }
            )

        except Exception as e:

            logging.error(
                f"OKX 상세 오류 "
                f"{symbol} : {e}"
            )

    latest_okx_data = rows

    logging.info(
        f"OKX 경고 종목 "
        f"{len(rows)}개"
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
        "전체 조회 시작"
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
    font-family: Arial, sans-serif;
    font-size: 9px;
    padding: 5px;
}

h1 {

    margin: 3px 2px 5px 2px;
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
    line-height: 1.4;
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


/* =====================================================
   컬럼 폭
   ===================================================== */

th:nth-child(1),
td:nth-child(1) {

    width: 6%;
}

th:nth-child(2),
td:nth-child(2) {

    width: 19%;
}

th:nth-child(3),
td:nth-child(3) {

    width: 18%;
}

th:nth-child(4),
td:nth-child(4) {

    width: 29%;
}

th:nth-child(5),
td:nth-child(5) {

    width: 28%;
}


/* =====================================================
   코인
   ===================================================== */

.coin {

    display: block;
    font-size: 8px;
    font-weight: bold;
    line-height: 1.2;
}


/* =====================================================
   거래대금
   ===================================================== */

.volume-value {

    display: block;
    font-size: 7px;
    font-weight: 600;
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
}

.change-item {

    display: block;
    width: 100%;
    font-size: 7px;
    text-align: center;
    white-space: nowrap;
}


/* =====================================================
   해 / 구름
   ===================================================== */

.weather {

    display: block;
    font-size: 7px;
    font-weight: bold;
    line-height: 1.2;
    white-space: nowrap;
}

.weather-sun {

    color: #ffd84d;
    font-weight: bold;
}

.weather-cloud {

    color: #b9c1cc;
    font-weight: bold;
}

.weather-none {

    color: #555;
}


/* =====================================================
   경고
   ===================================================== */

.breakout-warning {

    display: flex;
    justify-content: center;
    align-items: center;
    width: 100%;
    min-height: 14px;
    white-space: nowrap;
}


/* =====================================================
   LONG / SHORT 경고
   ===================================================== */

.warning-box {

    display: inline-flex;
    align-items: center;
    justify-content: center;

    padding: 2px 4px;

    border-radius: 4px;

    font-size: 8px;

    font-weight: 800;

    line-height: 1;

    white-space: nowrap;
}


/* =====================================================
   LONG
   ===================================================== */

.warning-long {

    color: #45ff78;

    text-shadow:
        0 0 4px rgba(50,255,100,0.9);

    animation:
        warningBlink 0.9s infinite;
}


/* =====================================================
   SHORT
   ===================================================== */

.warning-short {

    color: #ff5b5b;

    text-shadow:
        0 0 4px rgba(255,50,50,0.9);

    animation:
        warningBlink 0.9s infinite;
}


/* =====================================================
   조건 종목 전체 반짝임
   ===================================================== */

@keyframes warningBlink {

    0% {

        opacity: 1;

        transform: scale(1);
    }

    50% {

        opacity: 0.35;

        transform: scale(0.97);
    }

    100% {

        opacity: 1;

        transform: scale(1);
    }
}


/* =====================================================
   EMA
   ===================================================== */

.ema-value {

    width: 100%;

    display: flex;

    flex-direction: column;

    align-items: center;

    justify-content: center;

    gap: 3px;

    font-size: 8px;

    font-weight: bold;

    line-height: 1.15;

    white-space: nowrap;
}

.ema-line {

    width: 100%;

    text-align: center;

    white-space: nowrap;
}

.ema-1h {

    color: #eeeeee;
}

.ema-4h {

    color: #eeeeee;
}


/* =====================================================
   모바일
   ===================================================== */

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

    .weather {

        font-size: 7px;
    }

    .warning-box {

        font-size: 7px;
        padding: 2px 3px;
    }

    .ema-value {

        font-size: 7px;
        gap: 3px;
    }

}


/* =====================================================
   더 작은 화면
   ===================================================== */

@media (max-width: 380px) {

    .ema-value {

        font-size: 6px;
    }

    .warning-box {

        font-size: 6px;
    }

    .weather {

        font-size: 6px;
    }

    .coin {

        font-size: 6px;
    }

}

"""


# =========================================================
# 테이블 행 생성
# =========================================================

def make_table_rows(data):

    rows_html = ""

    for item in data:

        warning = item.get(
            "warning",
            {}
        )

        final_warning = item.get(
            "final_warning",
            {}
        )

        warning_text = (
            combined_warning_html(
                warning,
                final_warning
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

<div class="weather">
{item.get("weather", "")}
</div>

</td>


<td>

<span class="volume-value">
{item["volume"]}
</span>

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
    item["ema_1h"].get(
        "display",
        "⚪"
    ),
    item["ema_4h"].get(
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
    data
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

현재 조건 일치 종목 없음

</td>

</tr>

"""

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
        margin:4px 2px 7px 2px;
     ">

※ TOP{TOP_N} 거래대금 순위 기준<br>

※ 코인 아래 ☀️ LONG / ☁️ SHORT = 당일 방향<br>

※ 당일 양수 = ☀️ LONG<br>

※ 당일 음수 = ☁️ SHORT<br>

※ 업비트 = LONG 조건만 표시<br>

※ OKX = LONG / SHORT 모두 표시<br>

※ 당일 방향과 돌파 방향이 일치할 때만 경고<br>

※ 🚨 = 돌파 전 상태만 표시<br>

※ 🚀 돌파 후 표시는 삭제<br>

※ 1H / 4H 중 조건이 맞는 시간봉만 🚨 표시<br>

※ 경고 조건 종목은 LONG / SHORT 글자가 반짝임<br>

※ EMA는 1H / 4H 두 줄 표시<br>

※ EMA 60개 이상 = 10-30-60 정렬<br>

※ EMA 60개 미만 = 10-30 정렬<br>

※ 1분마다 재조회

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
📊 Breakout Trading
</h1>


<div class="info">

<div>
1H + 4H 추세 · 1H + 4H 돌파
</div>

<div>
최근 {BREAKOUT_LOOKBACK}개 확정 캔들 고가/저가 기준
</div>

<div>
☀️ LONG · ☁️ SHORT · 🚨 돌파 전
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


    # =====================================================
    # 최초 즉시 조회
    # =====================================================

    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()


    # =====================================================
    # 1분마다 조회
    # =====================================================

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
