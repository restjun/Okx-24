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
            "1d_30_60_120": "⚪",
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

        "1d_30_60_120":
            check_ema(
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
            "1d_30_60_120": "⚪",
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
                "c"
            ),

        "1d_30_60_120":
            check_ema(
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
                '<span class="signal-long">'
                'LONG'
                '</span>'
                f'<span class="warning-badge lightning">'
                f'⚡{count}'
                f'</span>'
            )

        if warning == "long_pullback":

            return (
                '<span class="signal-long">'
                'LONG'
                '</span>'
                '<span class="warning-badge pullback">'
                '🔥'
                '</span>'
            )

        if warning == "long_breakout":

            return (
                '<span class="signal-long">'
                'LONG'
                '</span>'
                '<span class="warning-badge breakout">'
                '🚀'
                '</span>'
            )

    if signal == "SHORT":

        if warning.startswith(
            "short_lightning_"
        ):

            count = warning.split(
                "_"
            )[-1]

            return (
                '<span class="signal-short">'
                'SHORT'
                '</span>'
                f'<span class="warning-badge explosion">'
                f'💥{count}'
                f'</span>'
            )

        if warning == "short_pullback":

            return (
                '<span class="signal-short">'
                'SHORT'
                '</span>'
                '<span class="warning-badge pullback">'
                '🔥'
                '</span>'
            )

        if warning == "short_breakout":

            return (
                '<span class="signal-short">'
                'SHORT'
                '</span>'
                '<span class="warning-badge breakout">'
                '🚀'
                '</span>'
            )

    return (
        '<span class="signal-none">'
        '—'
        '</span>'
    )


# =========================================================
# EMA HTML
#
# 1줄 : LONG / SHORT + 경고 + 방향
# 2줄 : 4H 10-30 / 30-60-120
# 3줄 : 1D 10-30 / 30-60-120
# =========================================================

def ema_html(
    ema
):

    display_signal = signal_html(
        ema.get(
            "signal",
            ""
        ),
        ema.get(
            "warning",
            "none"
        )
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

<div class="ema-box">

    <div class="signal-line">

        <div class="signal-main">

            {display_signal}

        </div>

        <div class="direction-main">

            {direction_html}

        </div>

    </div>


    <div class="ema-line">

        <span class="time-label">
            4H
        </span>

        <span class="ema-item">
            10-30
            <b>
                {ema.get("4h_10_30", "⚪")}
            </b>
        </span>

        <span class="ema-item ema-wide">
            30-60-120
            <b>
                {ema.get("4h_30_60_120", "⚪")}
            </b>
        </span>

    </div>


    <div class="ema-line">

        <span class="time-label">
            1D
        </span>

        <span class="ema-item">
            10-30
            <b>
                {ema.get("1d_10_30", "⚪")}
            </b>
        </span>

        <span class="ema-item ema-wide">
            30-60-120
            <b>
                {ema.get("1d_30_60_120", "⚪")}
            </b>
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

    padding:4px;

    font-size:8px;

}


/* =====================================================
   제목
   ===================================================== */

.main-title{

    margin:2px 0 3px;

    font-size:13px;

    line-height:16px;

    font-weight:bold;

}


/* =====================================================
   설명
   ===================================================== */

.description{

    color:#777;

    font-size:7px;

    line-height:10px;

    white-space:nowrap;

    overflow:hidden;

    text-overflow:ellipsis;

    margin-bottom:4px;

}


/* =====================================================
   설정
   ===================================================== */

.setting-row{

    display:flex;

    gap:3px;

    margin-bottom:5px;

}

.volume-setting{

    padding:2px 4px;

    background:#1b1b1b;

    border:1px solid #303030;

    border-radius:3px;

    color:#888;

    font-size:7px;

    line-height:10px;

    white-space:nowrap;

}


/* =====================================================
   섹션
   ===================================================== */

.section-title{

    margin:7px 0 3px;

    padding:4px 5px;

    background:#1d1d1d;

    border-left:3px solid #777;

    color:#ddd;

    font-size:10px;

    line-height:12px;

    font-weight:bold;

}


/* =====================================================
   테이블
   ===================================================== */

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


/* =====================================================
   헤더
   ===================================================== */

th{

    background:#252525;

    color:#aaa;

    padding:3px 1px;

    height:20px;

    border-right:1px solid #333;

    border-bottom:1px solid #333;

    white-space:nowrap;

    font-size:7px;

    line-height:9px;

    font-weight:bold;

}


/* =====================================================
   기본 셀
   ===================================================== */

td{

    padding:3px 1px;

    border-bottom:1px solid #292929;

    border-right:1px solid #252525;

    text-align:center;

    vertical-align:middle;

    white-space:nowrap;

    font-size:8px;

}


/* =====================================================
   순위
   ===================================================== */

.rank-cell{

    width:6%;

    color:#777;

    font-size:7px;

}


/* =====================================================
   코인
   ===================================================== */

.coin-cell{

    width:18%;

    text-align:left;

    padding-left:4px;

    color:#eee;

    font-weight:bold;

    font-size:8px;

    overflow:hidden;

    text-overflow:ellipsis;

}


/* =====================================================
   거래대금
   ===================================================== */

.volume-cell{

    width:15%;

    text-align:right;

    padding-right:3px;

    color:#ccc;

    font-size:7px;

    font-weight:bold;

}


/* =====================================================
   변동률
   ===================================================== */

.change-cell{

    width:15%;

    font-size:7px;

}


/* =====================================================
   EMA 셀
   ===================================================== */

.ema-cell{

    width:46%;

    padding:2px;

}


/* =====================================================
   변동률
   ===================================================== */

.change-item{

    display:flex;

    align-items:center;

    justify-content:center;

    width:100%;

    gap:2px;

}

.change-icon{

    font-size:6px;

    line-height:9px;

}

.change-value{

    text-align:right;

    font-family:monospace;

    font-size:7px;

    line-height:9px;

    font-weight:bold;

}


/* =====================================================
   EMA 전체
   ===================================================== */

.ema-box{

    width:100%;

    display:flex;

    flex-direction:column;

    justify-content:center;

    gap:1px;

    padding:2px 1px;

}


/* =====================================================
   신호
   ===================================================== */

.signal-line{

    width:100%;

    min-height:18px;

    display:flex;

    align-items:center;

    justify-content:space-between;

    border-bottom:1px solid #292929;

}

.signal-main{

    display:flex;

    align-items:center;

    gap:4px;

    min-width:0;

}


/* =====================================================
   LONG
   ===================================================== */

.signal-long{

    display:inline-flex;

    align-items:center;

    justify-content:center;

    padding:2px 5px;

    border-radius:3px;

    background:#123d29;

    color:#39f58a;

    font-size:8px;

    line-height:11px;

    font-weight:900;

}


/* =====================================================
   SHORT
   ===================================================== */

.signal-short{

    display:inline-flex;

    align-items:center;

    justify-content:center;

    padding:2px 5px;

    border-radius:3px;

    background:#431b22;

    color:#ff6675;

    font-size:8px;

    line-height:11px;

    font-weight:900;

}


/* =====================================================
   신호 없음
   ===================================================== */

.signal-none{

    color:#444;

    font-size:8px;

}


/* =====================================================
   경고 배지
   ===================================================== */

.warning-badge{

    display:inline-flex;

    align-items:center;

    justify-content:center;

    min-width:21px;

    height:15px;

    padding:1px 4px;

    border-radius:3px;

    font-size:8px;

    line-height:11px;

    font-weight:bold;

}


.warning-badge.lightning{

    background:#4b3908;

    border:1px solid #725a12;

}


.warning-badge.explosion{

    background:#451820;

    border:1px solid #70242e;

}


.warning-badge.pullback{

    background:#432d0a;

    border:1px solid #6b4810;

}


.warning-badge.breakout{

    background:#102d43;

    border:1px solid #1d5275;

}


/* =====================================================
   방향
   ===================================================== */

.direction-main{

    flex-shrink:0;

    padding-right:3px;

}

.direction-long{

    display:inline-flex;

    align-items:center;

    justify-content:center;

    padding:2px 3px;

    border-radius:3px;

    background:#40370c;

    color:#ffd84d;

    font-size:9px;

    line-height:11px;

    font-weight:bold;

}


.direction-short{

    display:inline-flex;

    align-items:center;

    justify-content:center;

    padding:2px 3px;

    border-radius:3px;

    background:#122d42;

    color:#8ecbff;

    font-size:9px;

    line-height:11px;

    font-weight:bold;

}


.direction-none{

    color:#444;

    font-size:8px;

}


/* =====================================================
   EMA 행
   ===================================================== */

.ema-line{

    width:100%;

    min-height:14px;

    display:flex;

    align-items:center;

    overflow:hidden;

    white-space:nowrap;

}


/* =====================================================
   시간
   ===================================================== */

.time-label{

    width:19px;

    min-width:19px;

    color:#777;

    text-align:left;

    font-size:6.5px;

    font-weight:900;

}


/* =====================================================
   EMA
   ===================================================== */

.ema-item{

    display:inline-flex;

    align-items:center;

    gap:2px;

    height:13px;

    min-width:58px;

    padding:1px 3px;

    margin-right:3px;

    border-radius:2px;

    background:#1c1c1c;

    color:#999;

    text-align:left;

    font-size:6px;

    line-height:9px;

}


/* =====================================================
   30-60-120
   ===================================================== */

.ema-wide{

    min-width:78px;

}


/* =====================================================
   EMA 아이콘
   ===================================================== */

.ema-item b{

    color:#ccc;

    font-size:7px;

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


    .main-title{

        font-size:12px;

        line-height:15px;

        margin:1px 0 2px;

    }


    .description{

        font-size:6.5px;

        line-height:9px;

        margin-bottom:3px;

    }


    .setting-row{

        gap:2px;

        margin-bottom:4px;

    }


    .volume-setting{

        padding:2px 3px;

        font-size:6.5px;

        line-height:9px;

    }


    .section-title{

        margin:6px 0 2px;

        padding:4px;

        font-size:9px;

        line-height:11px;

    }


    th{

        height:19px;

        padding:3px 1px;

        font-size:6.5px;

        line-height:8px;

    }


    td{

        padding:3px 1px;

    }


    .rank-cell{

        font-size:6.5px;

    }


    .coin-cell{

        font-size:7.5px;

        padding-left:3px;

    }


    .volume-cell{

        font-size:6.5px;

        padding-right:2px;

    }


    .change-value{

        font-size:6.5px;

    }


    .change-icon{

        font-size:5.5px;

    }


    .ema-cell{

        padding:2px 1px;

    }


    .ema-box{

        padding:2px 1px;

    }


    .signal-line{

        min-height:18px;

    }


    .signal-long,
    .signal-short{

        font-size:7.5px;

        padding:2px 4px;

    }


    .warning-badge{

        min-width:19px;

        height:14px;

        font-size:7px;

    }


    .direction-long,
    .direction-short{

        font-size:8px;

        padding:2px;

    }


    .ema-line{

        min-height:13px;

    }


    .time-label{

        width:18px;

        min-width:18px;

        font-size:6px;

    }


    .ema-item{

        height:12px;

        min-width:55px;

        padding:1px 2px;

        margin-right:2px;

        font-size:5.7px;

    }


    .ema-wide{

        min-width:72px;

    }


    .ema-item b{

        font-size:6.5px;

    }

}


/* =====================================================
   아주 작은 화면
   ===================================================== */

@media(
    max-width:380px
){

    .coin-cell{

        font-size:7px;

    }


    .volume-cell{

        font-size:6px;

    }


    .change-value{

        font-size:6px;

    }


    .ema-item{

        min-width:51px;

        font-size:5.4px;

    }


    .ema-wide{

        min-width:66px;

    }


    .time-label{

        width:16px;

        min-width:16px;

    }

}

</style>

</head>


<body>


<div class="main-title">
📊 4H 종가매매
</div>


<div class="description">
일봉 방향 + 4H 추세 일치 | ⚡ 추세전환 | 🔥 눌림목 | 🚀 돌파 | 완료된 4H 종가
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

<div class="section-title">

🏆 업비트 현물 TOP""" + str(TOP_N) + """

</div>


<div class="table-wrap">

<table>

<colgroup>

<col style="width:6%">
<col style="width:18%">
<col style="width:15%">
<col style="width:15%">
<col style="width:46%">

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
신호 · EMA
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

<div class="section-title">

🏆 OKX 선물 TOP""" + str(TOP_N) + """

</div>


<div class="table-wrap">

<table>

<colgroup>

<col style="width:6%">
<col style="width:18%">
<col style="width:15%">
<col style="width:15%">
<col style="width:46%">

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
신호 · EMA
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
