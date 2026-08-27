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
# EMA 방향 표시
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
#
# 중요:
# LONG / SHORT 글자만 표시
# ⚡ 🔥 🚀는 별도 위치에 표시
# =========================================================

def signal_html(
    signal
):

    if signal == "LONG":

        return (
            '<span class="long-text">'
            'LONG'
            '</span>'
        )

    if signal == "SHORT":

        return (
            '<span class="short-text">'
            'SHORT'
            '</span>'
        )

    return (
        '<span class="signal-none">'
        '—'
        '</span>'
    )


# =========================================================
# 경고 아이콘
# =========================================================

def warning_html(
    warning
):

    if warning.startswith(
        "long_lightning_"
    ):

        count = warning.split(
            "_"
        )[-1]

        return (
            '<span class="warning-lightning">'
            f'⚡{count}'
            '</span>'
        )

    if warning.startswith(
        "short_lightning_"
    ):

        count = warning.split(
            "_"
        )[-1]

        return (
            '<span class="warning-lightning">'
            f'💥{count}'
            '</span>'
        )

    if warning == "long_pullback":

        return (
            '<span class="warning-pullback">'
            '🔥'
            '</span>'
        )

    if warning == "short_pullback":

        return (
            '<span class="warning-pullback">'
            '🔥'
            '</span>'
        )

    if warning == "long_breakout":

        return (
            '<span class="warning-breakout">'
            '🚀'
            '</span>'
        )

    if warning == "short_breakout":

        return (
            '<span class="warning-breakout">'
            '🚀'
            '</span>'
        )

    return (
        '<span class="warning-none">'
        '—'
        '</span>'
    )


# =========================================================
# 방향
# =========================================================

def direction_html(
    direction
):

    if direction == "long":

        return (
            '<span class="direction-long">'
            '☀️'
            '</span>'
        )

    if direction == "short":

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
# EMA HTML
#
# 최종 표시 구조
#
# 1줄
# 4H  🟢(25)  /  🟢
#
# 2줄
# 1D  🟢(11)
#
# 10-30 / 30-60-120 글자는 삭제
# =========================================================

def ema_html(
    ema
):

    signal = ema.get(
        "signal",
        ""
    )

    warning = ema.get(
        "warning",
        "none"
    )

    direction = ema.get(
        "direction",
        "none"
    )

    h4_10_30 = ema.get(
        "4h_10_30",
        "⚪"
    )

    h4_30_60_120 = ema.get(
        "4h_30_60_120",
        "⚪"
    )

    d1_10_30 = ema.get(
        "1d_10_30",
        "⚪"
    )

    signal_display = signal_html(
        signal
    )

    warning_display = warning_html(
        warning
    )

    direction_display = direction_html(
        direction
    )

    return f"""

<div class="ema-box">

    <div class="ema-row-1">

        <span class="signal-under">
            {signal_display}
        </span>

        <span class="direction-under">
            {direction_display}
        </span>

        <span class="warning-under">
            {warning_display}
        </span>

        <span class="time-label">
            4H
        </span>

        <span class="ema-icon">
            {h4_10_30}
        </span>

        <span class="ema-icon">
            {h4_30_60_120}
        </span>

    </div>


    <div class="ema-row-2">

        <span class="time-label">
            1D
        </span>

        <span class="ema-icon">
            {d1_10_30}
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

    font-size:9px;

}


/* =====================================================
   제목
   ===================================================== */

.main-title{

    margin:2px 0 2px;

    font-size:13px;

    line-height:15px;

    font-weight:bold;

}


/* =====================================================
   설명
   ===================================================== */

.description{

    color:#777;

    font-size:6.5px;

    line-height:9px;

    white-space:nowrap;

    overflow:hidden;

    text-overflow:ellipsis;

    margin-bottom:3px;

}


/* =====================================================
   설정
   ===================================================== */

.setting-row{

    display:flex;

    align-items:center;

    gap:3px;

    margin-bottom:4px;

}

.volume-setting{

    padding:2px 4px;

    background:#1b1b1b;

    border:1px solid #303030;

    border-radius:3px;

    color:#888;

    font-size:6.5px;

    line-height:9px;

    white-space:nowrap;

}


/* =====================================================
   신호 설명
   ===================================================== */

.signal-guide{

    margin-left:auto;

    color:#777;

    font-size:6.5px;

    white-space:nowrap;

}


/* =====================================================
   섹션
   ===================================================== */

.section-title{

    margin:5px 0 2px;

    padding:3px 5px;

    background:#1d1d1d;

    border-left:2px solid #777;

    color:#ddd;

    font-size:9px;

    line-height:11px;

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

    height:18px;

    border-right:1px solid #333;

    border-bottom:1px solid #333;

    white-space:nowrap;

    font-size:7px;

    line-height:9px;

    font-weight:bold;

}


/* =====================================================
   셀
   ===================================================== */

td{

    padding:1px;

    height:37px;

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

    font-size:6.5px;

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

    color:#bbb;

    font-size:7px;

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

    padding:1px 2px;

}


/* =====================================================
   변동률
   ===================================================== */

.change-item{

    display:flex;

    align-items:center;

    justify-content:center;

    width:100%;

    gap:1px;

}

.change-icon{

    font-size:6px;

    line-height:8px;

}

.change-value{

    text-align:right;

    font-family:monospace;

    font-size:7px;

    line-height:9px;

}


/* =====================================================
   EMA 박스
   ===================================================== */

.ema-box{

    width:100%;

    height:35px;

    display:flex;

    flex-direction:column;

    justify-content:center;

}


/* =====================================================
   1줄
   ===================================================== */

.ema-row-1{

    width:100%;

    height:17px;

    display:flex;

    align-items:center;

    overflow:hidden;

    white-space:nowrap;

    border-bottom:1px solid #242424;

}


/* =====================================================
   2줄
   ===================================================== */

.ema-row-2{

    width:100%;

    height:17px;

    display:flex;

    align-items:center;

    overflow:hidden;

    white-space:nowrap;

}


/* =====================================================
   롱 / 숏
   ===================================================== */

.signal-under{

    width:48px;

    min-width:48px;

    text-align:left;

}


.long-text{

    color:#00e676;

    font-size:8px;

    font-weight:900;

    letter-spacing:.2px;

}


.short-text{

    color:#ff5252;

    font-size:8px;

    font-weight:900;

    letter-spacing:.2px;

}


.signal-none{

    color:#333;

    font-size:7px;

}


/* =====================================================
   방향
   ===================================================== */

.direction-under{

    width:25px;

    min-width:25px;

    text-align:center;

}


.direction-long{

    color:#ffd54f;

    font-size:9px;

    font-weight:bold;

}


.direction-short{

    color:#90caf9;

    font-size:9px;

    font-weight:bold;

}


.direction-none{

    color:#333;

}


/* =====================================================
   경고
   ===================================================== */

.warning-under{

    width:30px;

    min-width:30px;

    text-align:center;

}


.warning-lightning{

    font-size:8px;

    font-weight:bold;

}


.warning-pullback{

    font-size:8px;

}


.warning-breakout{

    font-size:8px;

}


.warning-none{

    color:#333;

}


/* =====================================================
   시간
   ===================================================== */

.time-label{

    width:22px;

    min-width:22px;

    color:#777;

    text-align:left;

    font-size:6.5px;

    font-weight:bold;

}


/* =====================================================
   방향 아이콘
   숫자 카운터만 유지
   ===================================================== */

.ema-icon{

    width:38px;

    min-width:38px;

    text-align:left;

    color:#aaa;

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

        line-height:14px;

    }


    .description{

        font-size:6px;

        line-height:8px;

    }


    .volume-setting{

        padding:2px 3px;

        font-size:6px;

    }


    .signal-guide{

        font-size:6px;

    }


    .section-title{

        font-size:8.5px;

        line-height:10px;

        padding:3px 4px;

    }


    th{

        height:17px;

        padding:2px 1px;

        font-size:6.5px;

    }


    td{

        height:36px;

    }


    .rank-cell{

        font-size:6px;

    }


    .coin-cell{

        font-size:7.5px;

        padding-left:3px;

    }


    .volume-cell{

        font-size:6.5px;

    }


    .change-value{

        font-size:6.5px;

    }


    .ema-box{

        height:34px;

    }


    .ema-row-1,
    .ema-row-2{

        height:17px;

    }


    .signal-under{

        width:44px;

        min-width:44px;

    }


    .long-text,
    .short-text{

        font-size:7.5px;

    }


    .direction-under{

        width:23px;

        min-width:23px;

    }


    .warning-under{

        width:28px;

        min-width:28px;

    }


    .time-label{

        width:20px;

        min-width:20px;

        font-size:6px;

    }


    .ema-icon{

        width:34px;

        min-width:34px;

        font-size:6.5px;

    }

}


/* =====================================================
   아주 작은 화면
   ===================================================== */

@media(
    max-width:380px
){

    .signal-under{

        width:40px;

        min-width:40px;

    }


    .direction-under{

        width:21px;

        min-width:21px;

    }


    .warning-under{

        width:25px;

        min-width:25px;

    }


    .ema-icon{

        width:30px;

        min-width:30px;

        font-size:6px;

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

<span class="signal-guide">
🟢 상승　🔴 하락　☀️ LONG　🌧 SHORT
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
신호 / 방향
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
신호 / 방향
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
