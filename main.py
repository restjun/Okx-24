from fastapi import FastAPI
from fastapi.responses import HTMLResponse

import schedule
import time
import requests
import threading
import uvicorn
import logging
import pandas as pd


app = FastAPI()


logging.basicConfig(
    level=logging.INFO
)


# =========================================================
# 사용자 설정
# =========================================================

VOLUME_HOURS = 4

TOP_N = 10

UPDATE_MINUTES = 5

MAX_WARNING_COUNT = 3

PULLBACK_DISTANCE = 0.01

BREAKOUT_LOOKBACK = 5


# =========================================================
# 전역 데이터
# =========================================================

latest_okx_data = []

latest_upbit_data = []


# =========================================================
# API 재시도
# =========================================================

def retry_request(
    func,
    *args,
    **kwargs
):

    for attempt in range(10):

        try:

            result = func(
                *args,
                **kwargs
            )

            if hasattr(
                result,
                "status_code"
            ):

                if result.status_code == 429:

                    logging.warning(
                        "API 요청 제한(429) - 2초 대기"
                    )

                    time.sleep(2)

                    continue

            return result

        except Exception as e:

            logging.error(
                f"API 실패 "
                f"{attempt + 1}/10 : {e}"
            )

            time.sleep(3)

    return None


# =========================================================
# OKX 캔들
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

        data = response.json()["data"]

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
            f"OKX 오류 {inst_id} : {e}"
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

        df["candle_acc_trade_volume"] = pd.to_numeric(
            df["candle_acc_trade_volume"],
            errors="coerce"
        )

        df["candle_acc_trade_price"] = pd.to_numeric(
            df["candle_acc_trade_price"],
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
        return 1400

    try:

        return float(
            response.json()[0]["trade_price"]
        )

    except Exception:

        return 1400


# =========================================================
# OKX 목록
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

        return [
            x["instId"]
            for x in response.json()["data"]
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
# 업비트 목록
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

        return [
            x["market"]
            for x in response.json()
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
# EMA 10-30
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

    for state in reversed(states):

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

    direction = get_ema_10_30_direction(
        df,
        column
    )

    if direction == "long":
        return "🟢"

    if direction == "short":
        return "🔴"

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
# =========================================================

def get_main_direction(
    df4h,
    df1d,
    column
):

    h4_direction = (
        get_ema_30_60_120_direction(
            df4h,
            column
        )
    )

    d1_direction = (
        get_ema_10_30_direction(
            df1d,
            column
        )
    )

    if (
        h4_direction == "long"
        and
        d1_direction == "long"
    ):

        return "long"

    if (
        h4_direction == "short"
        and
        d1_direction == "short"
    ):

        return "short"

    return "none"


# =========================================================
# ⚡ 추세전환
# =========================================================

def check_lightning(
    df4h,
    column
):

    count, direction = (
        get_30_60_120_count(
            df4h,
            column
        )
    )

    if not (
        1 <= count <= MAX_WARNING_COUNT
    ):

        return "none"

    if direction == "long":

        return f"long_lightning_{count}"

    if direction == "short":

        return f"short_lightning_{count}"

    return "none"


# =========================================================
# 🔥 눌림목
# =========================================================

def check_pullback(
    df4h,
    column
):

    if (
        df4h is None
        or len(df4h) < 125
    ):

        return "none"

    df = df4h.copy()

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

    cur = df.iloc[-1]

    prev = df.iloc[-2]

    if (
        pd.isna(cur["ema30"])
        or
        pd.isna(cur["ema60"])
        or
        pd.isna(cur["ema120"])
        or
        pd.isna(prev["ema30"])
        or
        pd.isna(prev["ema60"])
        or
        pd.isna(prev["ema120"])
    ):

        return "none"

    long_trend = (
        cur["ema30"]
        >
        cur["ema60"]
        >
        cur["ema120"]
    )

    long_touch = (
        cur["l"]
        <=
        cur["ema30"] *
        (1 + PULLBACK_DISTANCE)
    )

    long_close = (
        cur[column]
        >
        cur["ema30"]
    )

    long_candle = (
        cur["c"]
        >
        cur["o"]
    )

    current_long = (
        long_trend
        and
        long_touch
        and
        long_close
        and
        long_candle
    )

    prev_long_trend = (
        prev["ema30"]
        >
        prev["ema60"]
        >
        prev["ema120"]
    )

    prev_long_touch = (
        prev["l"]
        <=
        prev["ema30"] *
        (1 + PULLBACK_DISTANCE)
    )

    prev_long_close = (
        prev[column]
        >
        prev["ema30"]
    )

    prev_long_candle = (
        prev["c"]
        >
        prev["o"]
    )

    previous_long = (
        prev_long_trend
        and
        prev_long_touch
        and
        prev_long_close
        and
        prev_long_candle
    )

    if (
        current_long
        and
        not previous_long
    ):

        return "long_pullback"

    short_trend = (
        cur["ema30"]
        <
        cur["ema60"]
        <
        cur["ema120"]
    )

    short_touch = (
        cur["h"]
        >=
        cur["ema30"] *
        (1 - PULLBACK_DISTANCE)
    )

    short_close = (
        cur[column]
        <
        cur["ema30"]
    )

    short_candle = (
        cur["c"]
        <
        cur["o"]
    )

    current_short = (
        short_trend
        and
        short_touch
        and
        short_close
        and
        short_candle
    )

    prev_short_trend = (
        prev["ema30"]
        <
        prev["ema60"]
        <
        prev["ema120"]
    )

    prev_short_touch = (
        prev["h"]
        >=
        prev["ema30"] *
        (1 - PULLBACK_DISTANCE)
    )

    prev_short_close = (
        prev[column]
        <
        prev["ema30"]
    )

    prev_short_candle = (
        prev["c"]
        <
        prev["o"]
    )

    previous_short = (
        prev_short_trend
        and
        prev_short_touch
        and
        prev_short_close
        and
        prev_short_candle
    )

    if (
        current_short
        and
        not previous_short
    ):

        return "short_pullback"

    return "none"


# =========================================================
# 🚀 돌파
# =========================================================

def check_breakout(
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

    cur = df.iloc[-1]

    previous = df.iloc[
        -(BREAKOUT_LOOKBACK + 1):
        -1
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

    long_trend = (
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
        long_trend
        and
        long_break
        and
        long_candle
    ):

        return "long_breakout"

    short_trend = (
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
        short_trend
        and
        short_break
        and
        short_candle
    ):

        return "short_breakout"

    return "none"


# =========================================================
# 최종 경고
# =========================================================

def check_entry_warning(
    df4h,
    column
):

    lightning = check_lightning(
        df4h,
        column
    )

    if lightning != "none":

        return lightning

    pullback = check_pullback(
        df4h,
        column
    )

    if pullback != "none":

        return pullback

    breakout = check_breakout(
        df4h,
        column
    )

    if breakout != "none":

        return breakout

    return "none"


# =========================================================
# LONG / SHORT
# =========================================================

def get_trade_signal(
    df4h,
    df1d,
    column
):

    main_direction = get_main_direction(
        df4h,
        df1d,
        column
    )

    if main_direction == "none":

        return "", "none"

    warning = check_entry_warning(
        df4h,
        column
    )

    if warning == "none":

        return "", "none"

    if (
        main_direction == "long"
        and
        warning.startswith("long_")
    ):

        return "LONG", warning

    if (
        main_direction == "short"
        and
        warning.startswith("short_")
    ):

        return "SHORT", warning

    return "", "none"


# =========================================================
# OKX EMA
# =========================================================

def get_okx_ema(
    inst_id
):

    df4h = get_okx_ohlcv(
        inst_id,
        "4H",
        200
    )

    df1d = get_okx_ohlcv(
        inst_id,
        "1D",
        200
    )

    if (
        df4h is None
        or df1d is None
    ):

        return {
            "4h_10_30": "⚪",
            "4h_30_60_120": "⚪",
            "1d_10_30": "⚪",
            "signal": "",
            "warning": "none",
            "direction": "none"
        }

    signal, warning = get_trade_signal(
        df4h,
        df1d,
        "c"
    )

    direction = get_main_direction(
        df4h,
        df1d,
        "c"
    )

    return {

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

        "1d_10_30":
            check_ema_10_30(
                df1d,
                "c"
            ),

        "signal":
            signal,

        "warning":
            warning,

        "direction":
            direction
    }


# =========================================================
# 업비트 EMA
# =========================================================

def get_upbit_ema(
    market
):

    df4h = get_upbit_ohlcv(
        market,
        240,
        200
    )

    df1d = get_upbit_daily_ohlcv(
        market,
        200
    )

    if (
        df4h is None
        or df1d is None
    ):

        return {
            "4h_10_30": "⚪",
            "4h_30_60_120": "⚪",
            "1d_10_30": "⚪",
            "signal": "",
            "warning": "none",
            "direction": "none"
        }

    if len(df4h) > 1:

        df4h = (
            df4h
            .iloc[:-1]
            .reset_index(drop=True)
        )

    signal, warning = get_trade_signal(
        df4h,
        df1d,
        "c"
    )

    direction = get_main_direction(
        df4h,
        df1d,
        "c"
    )

    return {

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

        "1d_10_30":
            check_ema_10_30(
                df1d,
                "trade_price"
            ),

        "signal":
            signal,

        "warning":
            warning,

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

    for index, market in enumerate(
        markets,
        start=1
    ):

        volume_map[market] = (
            get_upbit_volume(
                market
            )
        )

        time.sleep(0.03)

        if index % 50 == 0:

            logging.info(
                f"업비트 거래대금 "
                f"{index}/{total}"
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

        color = "🟩"
        sign = "+"

    elif x < 0:

        color = "🟥"
        sign = ""

    else:

        color = "⬜"
        sign = ""

    return (
        f'<span class="change-item">'
        f'<span class="change-icon">{color}</span>'
        f'<span class="change-value">'
        f'{sign}{x:.2f}%'
        f'</span>'
        f'</span>'
    )


# =========================================================
# 신호
# =========================================================

def signal_html(
    signal,
    warning
):

    if signal == "LONG":

        if warning.startswith(
            "long_lightning_"
        ):

            count = warning.split(
                "_"
            )[-1]

            return (
                '<span class="long-text">'
                'LONG'
                '</span>'
                f' ⚡{count}'
            )

        if warning == "long_pullback":

            return (
                '<span class="long-text">'
                'LONG'
                '</span> 🔥'
            )

        if warning == "long_breakout":

            return (
                '<span class="long-text">'
                'LONG'
                '</span> 🚀'
            )

    if signal == "SHORT":

        if warning.startswith(
            "short_lightning_"
        ):

            count = warning.split(
                "_"
            )[-1]

            return (
                '<span class="short-text">'
                'SHORT'
                '</span>'
                f' 💥{count}'
            )

        if warning == "short_pullback":

            return (
                '<span class="short-text">'
                'SHORT'
                '</span> 🔥'
            )

        if warning == "short_breakout":

            return (
                '<span class="short-text">'
                'SHORT'
                '</span> 🚀'
            )

    return ""


# =========================================================
# EMA HTML
# =========================================================

def ema_html(
    ema
):

    display_signal = signal_html(
        ema["signal"],
        ema["warning"]
    )

    direction = ema.get(
        "direction",
        "none"
    )

    if direction == "long":

        direction_html = (
            '<span class="direction-long">'
            '☀️'
            '</span>'
        )

    elif direction == "short":

        direction_html = (
            '<span class="direction-short">'
            '🌧'
            '</span>'
        )

    else:

        direction_html = (
            '<span class="direction-none">'
            '—'
            '</span>'
        )

    return f"""

<div class="ema-display">

    <div class="signal-period">
        {display_signal}
    </div>

    <div class="direction-period">
        {direction_html}
    </div>

    <div class="ema-period">

        <span class="ema-time">
            4H
        </span>

        <span class="ema-status">
            10-30 {ema["4h_10_30"]}
        </span>

        <span class="ema-status">
            30-60-120 {ema["4h_30_60_120"]}
        </span>

    </div>

    <div class="ema-period last">

        <span class="ema-time">
            1D
        </span>

        <span class="ema-status">
            10-30 {ema["1d_10_30"]}
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

        return

    volume_map = (
        get_upbit_volume_map(
            markets
        )
    )

    if not volume_map:

        return

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

        changes = get_upbit_change(
            market
        )

        ema = get_upbit_ema(
            market
        )

        rows.append({

            "rank": rank,

            "name": coin,

            "change":
                format_change(
                    changes
                ),

            "volume":
                format_volume(
                    volume_map[market]
                ),

            "ema": ema

        })

    latest_upbit_data = rows

    logging.info(
        f"업비트 TOP{TOP_N} 완료"
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

    symbols = (
        get_all_okx_swap_symbols()
    )

    if not symbols:

        return

    usdt_krw = get_usdt_krw()

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

    volume_map = {}

    total = len(symbols)

    for index, symbol in enumerate(
        symbols,
        start=1
    ):

        volume_usdt = get_okx_volume(
            symbol
        )

        volume_krw = (
            volume_usdt
            *
            usdt_krw
        )

        volume_map[symbol] = volume_krw

        time.sleep(0.03)

        if index % 50 == 0:

            logging.info(
                f"OKX 거래대금 "
                f"{index}/{total}"
            )

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

        changes = get_okx_change(
            symbol
        )

        ema = get_okx_ema(
            symbol
        )

        rows.append({

            "rank": rank,

            "name": coin,

            "change":
                format_change(
                    changes
                ),

            "volume":
                format_volume(
                    volume_map[symbol]
                ),

            "ema": ema

        })

    latest_okx_data = rows

    logging.info(
        f"OKX TOP{TOP_N} 완료"
    )


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

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
4H 종가매매
</title>

<style>

/* =====================================================
   기본
   ===================================================== */

*{
    box-sizing:border-box;
}

html,
body{

    width:100%;
    margin:0;
    padding:0;

    overflow-x:hidden;

}

body{

    background:#0d0d0f;
    color:#e8e8e8;

    font-family:
        Arial,
        "Noto Sans KR",
        sans-serif;

    padding:4px;

    font-size:9px;

}

/* =====================================================
   전체 컨테이너
   ===================================================== */

.page{

    width:100%;
    max-width:720px;
    margin:0 auto;

}

/* =====================================================
   상단
   ===================================================== */

.top-header{

    display:flex;
    align-items:center;
    justify-content:space-between;

    padding:4px 5px 3px;

}

.title{

    font-size:13px;
    font-weight:700;

    letter-spacing:-0.5px;

}

.update-time{

    color:#666;
    font-size:7px;

}

/* =====================================================
   설명
   ===================================================== */

.description{

    padding:2px 5px 4px;

    color:#777;

    font-size:7px;

    line-height:1.2;

    white-space:nowrap;

    overflow:hidden;

    text-overflow:ellipsis;

}

/* =====================================================
   설정
   ===================================================== */

.setting-row{

    display:flex;
    gap:3px;

    padding:0 4px 5px;

}

.volume-setting{

    display:inline-flex;
    align-items:center;

    padding:2px 5px;

    background:#18181b;

    border:1px solid #29292d;

    border-radius:3px;

    color:#888;

    font-size:7px;

    white-space:nowrap;

}

/* =====================================================
   섹션
   ===================================================== */

.section-title{

    display:flex;
    align-items:center;

    margin:5px 0 2px;

    padding:4px 5px;

    background:#19191c;

    border:1px solid #29292d;

    border-left:2px solid #777;

    border-radius:3px;

    font-size:10px;

    font-weight:700;

}

/* =====================================================
   테이블
   ===================================================== */

.table-wrap{

    width:100%;

    overflow:hidden;

    border-radius:3px;

}

table{

    width:100%;

    table-layout:fixed;

    border-collapse:collapse;

    background:#111113;

    border:1px solid #29292d;

}

/* =====================================================
   열 비율
   ===================================================== */

.rank-cell{
    width:5%;
}

.coin-cell{
    width:15%;
}

.volume-cell{
    width:14%;
}

.change-cell{
    width:13%;
}

.ema-cell{
    width:53%;
}

/* =====================================================
   헤더
   ===================================================== */

th{

    height:22px;

    padding:2px 1px;

    background:#202023;

    border-right:1px solid #303034;
    border-bottom:1px solid #353539;

    color:#8f8f95;

    white-space:nowrap;

    font-size:7px;

    font-weight:600;

    text-align:center;

}

/* =====================================================
   셀
   ===================================================== */

td{

    height:31px;

    padding:1px;

    border-right:1px solid #222225;
    border-bottom:1px solid #222225;

    text-align:center;

    white-space:nowrap;

    overflow:hidden;

    font-size:8px;

}

/* =====================================================
   순위
   ===================================================== */

.rank-cell{

    color:#777;

    font-size:7px;

}

/* =====================================================
   코인
   ===================================================== */

.coin-cell{

    text-align:left;

    padding-left:4px;

    color:#eee;

    font-weight:700;

    font-size:8px;

    overflow:hidden;

    text-overflow:ellipsis;

}

/* =====================================================
   거래대금
   ===================================================== */

.volume-cell{

    text-align:right;

    padding-right:3px;

    color:#bbb;

    font-family:monospace;

    font-size:7px;

}

/* =====================================================
   변동률
   ===================================================== */

.change-cell{

    font-size:7px;

}

.change-item{

    display:flex;

    align-items:center;
    justify-content:center;

    width:100%;

}

.change-icon{

    width:9px;
    min-width:9px;

    font-size:5px;

}

.change-value{

    width:35px;
    min-width:0;

    text-align:right;

    font-family:monospace;

    font-size:7px;

}

/* =====================================================
   EMA
   ===================================================== */

.ema-cell{

    padding:0 1px;

}

.ema-display{

    display:flex;

    align-items:center;

    width:100%;
    height:29px;

    white-space:nowrap;

}

/* =====================================================
   신호
   ===================================================== */

.signal-period{

    width:19%;
    min-width:19%;

    height:100%;

    display:flex;

    align-items:center;
    justify-content:center;

    overflow:hidden;

    font-size:7px;

}

/* =====================================================
   방향
   ===================================================== */

.direction-period{

    width:9%;
    min-width:9%;

    height:100%;

    display:flex;

    align-items:center;
    justify-content:center;

    font-size:8px;

}

/* =====================================================
   방향
   ===================================================== */

.direction-long{

    color:#00e676;

    font-weight:bold;

}

.direction-short{

    color:#ff5252;

    font-weight:bold;

}

.direction-none{

    color:#555;

}

/* =====================================================
   EMA 구역
   ===================================================== */

.ema-period{

    display:flex;
    align-items:center;

    height:24px;

    padding:0 2px;

    border-right:1px solid #303034;

    overflow:hidden;

}

.ema-period.last{

    border-right:none;

}

/* =====================================================
   시간
   ===================================================== */

.ema-time{

    display:inline-block;

    width:16px;
    min-width:16px;

    color:#777;

    text-align:left;

    font-size:7px;
    font-weight:bold;

}

/* =====================================================
   EMA 상태
   ===================================================== */

.ema-status{

    display:inline-block;

    width:52px;
    min-width:52px;

    color:#aaa;

    text-align:left;

    font-size:6.5px;

}

/* =====================================================
   LONG / SHORT
   ===================================================== */

.long-text{

    color:#00e676;

    font-weight:700;

    font-size:7px;

}

.short-text{

    color:#ff5252;

    font-weight:700;

    font-size:7px;

}

/* =====================================================
   TOP 강조
   ===================================================== */

tr:nth-child(2) td{

    background:#151518;

}

/* =====================================================
   모바일
   ===================================================== */

@media(
    max-width:600px
){

    body{

        padding:3px;

    }

    .top-header{

        padding:3px 4px;

    }

    .title{

        font-size:12px;

    }

    .update-time{

        font-size:6px;

    }

    .description{

        padding:1px 4px 3px;

        font-size:6.5px;

    }

    .setting-row{

        padding:0 3px 4px;

    }

    .volume-setting{

        padding:2px 4px;

        font-size:6.5px;

    }

    .section-title{

        margin:4px 0 2px;

        padding:3px 4px;

        font-size:9px;

    }

    th{

        height:20px;

        font-size:6.5px;

    }

    td{

        height:29px;

        font-size:7px;

    }

    .coin-cell{

        font-size:7.5px;

        padding-left:3px;

    }

    .volume-cell{

        font-size:6.5px;

    }

    .change-value{

        width:32px;

        font-size:6.5px;

    }

    .change-icon{

        width:8px;
        min-width:8px;

        font-size:5px;

    }

    .ema-display{

        height:27px;

    }

    .signal-period{

        width:18%;

        min-width:18%;

        font-size:6.5px;

    }

    .direction-period{

        width:8%;

        min-width:8%;

        font-size:7px;

    }

    .ema-time{

        width:14px;
        min-width:14px;

        font-size:6.5px;

    }

    .ema-status{

        width:47px;
        min-width:47px;

        font-size:6px;

    }

    .long-text,
    .short-text{

        font-size:6.5px;

    }

}

/* =====================================================
   아주 작은 화면
   ===================================================== */

@media(
    max-width:380px
){

    body{

        padding:2px;

    }

    th{

        font-size:6px;

    }

    td{

        height:28px;

    }

    .coin-cell{

        font-size:7px;

    }

    .volume-cell{

        font-size:6px;

    }

    .change-value{

        width:29px;

        font-size:6px;

    }

    .signal-period{

        width:17%;

        min-width:17%;

    }

    .direction-period{

        width:7%;

        min-width:7%;

    }

    .ema-status{

        width:43px;
        min-width:43px;

        font-size:5.7px;

    }

}

</style>

</head>

<body>

<div class="page">

<!-- =====================================================
     상단
     ===================================================== -->

<div class="top-header">

    <div class="title">
        📊 4H 종가매매
    </div>

    <div class="update-time">
        5분 갱신
    </div>

</div>


<div class="description">

일봉 방향 + 4H 추세 일치 · ⚡추세전환 · 🔥눌림목 · 🚀돌파 · 완료된 4H 종가

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

</div>


<!-- =====================================================
     업비트
     ===================================================== -->

<h2 class="section-title">

🏆 업비트 현물 TOP""" + str(TOP_N) + """

</h2>


<div class="table-wrap">

<table>

<colgroup>

<col style="width:5%">
<col style="width:15%">
<col style="width:14%">
<col style="width:13%">
<col style="width:53%">

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
4H
</th>

</tr>

"""

    for item in latest_upbit_data:

        html += f"""

<tr>

<td class="rank-cell">
{item['rank']}
</td>

<td class="coin-cell">
{item['name']}
</td>

<td class="volume-cell">
{item['volume']}
</td>

<td class="change-cell">
{item['change']}
</td>

<td class="ema-cell">
{ema_html(item["ema"])}
</td>

</tr>

"""

    html += """

</table>

</div>


<!-- =====================================================
     OKX
     ===================================================== -->

<h2 class="section-title">

🏆 OKX 선물 TOP""" + str(TOP_N) + """

</h2>


<div class="table-wrap">

<table>

<colgroup>

<col style="width:5%">
<col style="width:15%">
<col style="width:14%">
<col style="width:13%">
<col style="width:53%">

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
4H
</th>

</tr>

"""

    for item in latest_okx_data:

        html += f"""

<tr>

<td class="rank-cell">
{item['rank']}
</td>

<td class="coin-cell">
{item['name']}
</td>

<td class="volume-cell">
{item['volume']}
</td>

<td class="change-cell">
{item['change']}
</td>

<td class="ema-cell">
{ema_html(item["ema"])}
</td>

</tr>

"""

    html += """

</table>

</div>

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
