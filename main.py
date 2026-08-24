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

# =========================================================
# 거래대금 집계 시간
#
# 1  = 최근 1시간
#      → 1분봉 60개 사용
#
# 2 이상
#      → 완료된 1시간봉 N개 사용
#
# =========================================================

VOLUME_HOURS = 24


# =========================================================
# 표시할 순위
# =========================================================

TOP_N = 15


# =========================================================
# 자동 새로고침
# =========================================================

UPDATE_MINUTES = 5


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
#
# 미완성 캔들 제외
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

        df["c"] = (
            df["c"]
            .astype(float)
        )

        df["volCcyQuote"] = (
            df["volCcyQuote"]
            .astype(float)
        )

        # =====================================================
        # 미완성 캔들 제외
        # =====================================================

        df = df[
            df["confirm"]
            .astype(str)
            == "1"
        ]

        if df.empty:

            return None

        # =====================================================
        # 오래된 캔들 → 최신 캔들
        # =====================================================

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

        # =====================================================
        # 오래된 캔들 → 최신 캔들
        # =====================================================

        df = (
            df
            .iloc[::-1]
            .reset_index(drop=True)
        )

        df["trade_price"] = (
            df["trade_price"]
            .astype(float)
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

def get_upbit_day_ohlcv(
    market,
    count=200
):

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

        df["trade_price"] = (
            df["trade_price"]
            .astype(float)
        )

        return df

    except Exception as e:

        logging.error(
            f"업비트 일봉 오류 "
            f"{market} : {e}"
        )

        return None


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
# EMA 10-20 방향
# =========================================================

def get_ema_10_20_direction(
    df,
    column
):

    if (
        df is None
        or len(df) < 20
    ):

        return "none"

    ema10 = (
        df[column]
        .ewm(
            span=10,
            adjust=False
        )
        .mean()
    )

    ema20 = (
        df[column]
        .ewm(
            span=20,
            adjust=False
        )
        .mean()
    )

    if ema10.iloc[-1] > ema20.iloc[-1]:

        return "long"

    elif ema10.iloc[-1] < ema20.iloc[-1]:

        return "short"

    return "none"


# =========================================================
# EMA 20-60-120 방향
# =========================================================

def get_ema_20_60_120_direction(
    df,
    column
):

    if (
        df is None
        or len(df) < 120
    ):

        return "none"

    ema20 = (
        df[column]
        .ewm(
            span=20,
            adjust=False
        )
        .mean()
    )

    ema60 = (
        df[column]
        .ewm(
            span=60,
            adjust=False
        )
        .mean()
    )

    ema120 = (
        df[column]
        .ewm(
            span=120,
            adjust=False
        )
        .mean()
    )

    if (
        ema20.iloc[-1]
        >
        ema60.iloc[-1]
        >
        ema120.iloc[-1]
    ):

        return "long"

    elif (
        ema20.iloc[-1]
        <
        ema60.iloc[-1]
        <
        ema120.iloc[-1]
    ):

        return "short"

    return "none"


# =========================================================
# EMA 10-20 상태
# =========================================================

def check_ema_10_20(
    df,
    column
):

    if (
        df is None
        or len(df) < 20
    ):

        return "⚪(0)"

    df = df.copy()

    df["ema10"] = (
        df[column]
        .ewm(
            span=10,
            adjust=False
        )
        .mean()
    )

    df["ema20"] = (
        df[column]
        .ewm(
            span=20,
            adjust=False
        )
        .mean()
    )

    states = []

    for _, row in df.iterrows():

        if row["ema10"] > row["ema20"]:

            states.append("long")

        elif row["ema10"] < row["ema20"]:

            states.append("short")

        else:

            states.append("none")

    current_state = states[-1]

    if current_state == "none":

        return "⚪(0)"

    count = 0

    for state in reversed(states):

        if state == current_state:

            count += 1

        else:

            break

    if current_state == "long":

        return f"🟢({count})"

    elif current_state == "short":

        return f"🔴({count})"

    return "⚪(0)"


# =========================================================
# EMA 20-60-120 상태
# =========================================================

def check_ema(
    df,
    column
):

    if (
        df is None
        or len(df) < 120
    ):

        return "⚪(0)"

    df = df.copy()

    df["ema20"] = (
        df[column]
        .ewm(
            span=20,
            adjust=False
        )
        .mean()
    )

    df["ema60"] = (
        df[column]
        .ewm(
            span=60,
            adjust=False
        )
        .mean()
    )

    df["ema120"] = (
        df[column]
        .ewm(
            span=120,
            adjust=False
        )
        .mean()
    )

    states = []

    for _, row in df.iterrows():

        if (
            row["ema20"]
            >
            row["ema60"]
            >
            row["ema120"]
        ):

            states.append("long")

        elif (
            row["ema20"]
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

        return "⚪(0)"

    count = 0

    for state in reversed(states):

        if state == current_state:

            count += 1

        else:

            break

    if current_state == "long":

        return f"🟢({count})"

    elif current_state == "short":

        return f"🔴({count})"

    return "⚪(0)"


# =========================================================
# EMA 20-60-120 지속 캔들 수
# =========================================================

def get_ema_20_60_120_count(
    df,
    column
):

    if (
        df is None
        or len(df) < 120
    ):

        return 0, "none"

    df = df.copy()

    df["ema20"] = (
        df[column]
        .ewm(
            span=20,
            adjust=False
        )
        .mean()
    )

    df["ema60"] = (
        df[column]
        .ewm(
            span=60,
            adjust=False
        )
        .mean()
    )

    df["ema120"] = (
        df[column]
        .ewm(
            span=120,
            adjust=False
        )
        .mean()
    )

    states = []

    for _, row in df.iterrows():

        if (
            row["ema20"]
            >
            row["ema60"]
            >
            row["ema120"]
        ):

            states.append("long")

        elif (
            row["ema20"]
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
# 15분 눌림 조건
# =========================================================

def check_15m_warning(
    df15m,
    df1h,
    column
):

    if (
        df15m is None
        or df1h is None
        or len(df15m) < 120
        or len(df1h) < 120
    ):

        return "none"

    ema15m_10_20 = (
        get_ema_10_20_direction(
            df15m,
            column
        )
    )

    ema15m_20_60_120 = (
        get_ema_20_60_120_direction(
            df15m,
            column
        )
    )

    ema1h_10_20 = (
        get_ema_10_20_direction(
            df1h,
            column
        )
    )

    ema1h_20_60_120 = (
        get_ema_20_60_120_direction(
            df1h,
            column
        )
    )

    count1h, direction1h = (
        get_ema_20_60_120_count(
            df1h,
            column
        )
    )

    # =====================================================
    # LONG 〽️
    # =====================================================

    if (
        ema15m_10_20 == "long"
        and
        ema1h_10_20 == "short"
        and
        ema1h_20_60_120 == "long"
    ):

        return "long_special"

    # =====================================================
    # SHORT 〽️
    # =====================================================

    if (
        ema15m_10_20 == "short"
        and
        ema1h_10_20 == "long"
        and
        ema1h_20_60_120 == "short"
    ):

        return "short_special"

    # =====================================================
    # 기존 LONG 눌림
    # =====================================================

    if (
        ema15m_10_20 == "short"
        and
        ema15m_20_60_120 == "long"
        and
        ema1h_10_20 == "long"
        and
        ema1h_20_60_120 == "long"
        and
        direction1h == "long"
        and
        count1h >= 1
        and
        count1h <= 20
    ):

        return f"long_warning_{count1h}"

    # =====================================================
    # 기존 SHORT 눌림
    # =====================================================

    if (
        ema15m_10_20 == "long"
        and
        ema15m_20_60_120 == "short"
        and
        ema1h_10_20 == "short"
        and
        ema1h_20_60_120 == "short"
        and
        direction1h == "short"
        and
        count1h >= 1
        and
        count1h <= 20
    ):

        return f"short_warning_{count1h}"

    return "none"


# =========================================================
# 15분 돌파 조건
# =========================================================

def check_15m_breakout_warning(
    df15m,
    df1h,
    column
):

    if (
        df15m is None
        or df1h is None
        or len(df15m) < 120
        or len(df1h) < 120
    ):

        return "none"

    ema15m_10_20 = (
        get_ema_10_20_direction(
            df15m,
            column
        )
    )

    ema15m_20_60_120 = (
        get_ema_20_60_120_direction(
            df15m,
            column
        )
    )

    ema1h_10_20 = (
        get_ema_10_20_direction(
            df1h,
            column
        )
    )

    ema1h_20_60_120 = (
        get_ema_20_60_120_direction(
            df1h,
            column
        )
    )

    count1h, direction1h = (
        get_ema_20_60_120_count(
            df1h,
            column
        )
    )

    # =====================================================
    # LONG 돌파
    # =====================================================

    if (
        ema15m_10_20 == "long"
        and
        ema15m_20_60_120 == "long"
        and
        ema1h_10_20 == "long"
        and
        ema1h_20_60_120 == "long"
        and
        direction1h == "long"
        and
        count1h >= 1
        and
        count1h <= 20
    ):

        return f"long_breakout_{count1h}"

    # =====================================================
    # SHORT 돌파
    # =====================================================

    if (
        ema15m_10_20 == "short"
        and
        ema15m_20_60_120 == "short"
        and
        ema1h_10_20 == "short"
        and
        ema1h_20_60_120 == "short"
        and
        direction1h == "short"
        and
        count1h >= 1
        and
        count1h <= 20
    ):

        return f"short_breakout_{count1h}"

    return "none"


# =========================================================
# 최종 LONG / SHORT
# =========================================================

def get_trade_signal(
    warning,
    breakout
):

    if warning == "long_special":

        return "LONG"

    if warning == "short_special":

        return "SHORT"

    if breakout.startswith(
        "long_breakout_"
    ):

        return "LONG"

    if breakout.startswith(
        "short_breakout_"
    ):

        return "SHORT"

    if warning.startswith(
        "long_warning_"
    ):

        return "LONG"

    if warning.startswith(
        "short_warning_"
    ):

        return "SHORT"

    return ""


# =========================================================
# OKX 15M + 1H EMA
# =========================================================

def get_okx_ema(
    inst_id
):

    df15m = get_okx_ohlcv(
        inst_id,
        "15m",
        200
    )

    df1h = get_okx_ohlcv(
        inst_id,
        "1H",
        200
    )

    warning = check_15m_warning(
        df15m,
        df1h,
        "c"
    )

    breakout = check_15m_breakout_warning(
        df15m,
        df1h,
        "c"
    )

    signal = get_trade_signal(
        warning,
        breakout
    )

    return {

        "15m_10_20":
            check_ema_10_20(
                df15m,
                "c"
            ),

        "15m_20_60_120":
            check_ema(
                df15m,
                "c"
            ),

        "1h_10_20":
            check_ema_10_20(
                df1h,
                "c"
            ),

        "1h_20_60_120":
            check_ema(
                df1h,
                "c"
            ),

        "warning":
            warning,

        "breakout":
            breakout,

        "signal":
            signal
    }


# =========================================================
# 업비트 15M + 1H EMA
# =========================================================

def get_upbit_ema(
    market
):

    df15m = get_upbit_ohlcv(
        market,
        15,
        200
    )

    df1h = get_upbit_ohlcv(
        market,
        60,
        200
    )

    warning = check_15m_warning(
        df15m,
        df1h,
        "trade_price"
    )

    breakout = check_15m_breakout_warning(
        df15m,
        df1h,
        "trade_price"
    )

    signal = get_trade_signal(
        warning,
        breakout
    )

    return {

        "15m_10_20":
            check_ema_10_20(
                df15m,
                "trade_price"
            ),

        "15m_20_60_120":
            check_ema(
                df15m,
                "trade_price"
            ),

        "1h_10_20":
            check_ema_10_20(
                df1h,
                "trade_price"
            ),

        "1h_20_60_120":
            check_ema(
                df1h,
                "trade_price"
            ),

        "warning":
            warning,

        "breakout":
            breakout,

        "signal":
            signal
    }


# =========================================================
# OKX 거래대금
#
# VOLUME_HOURS = 1
# → 1분봉 60개 사용
#
# VOLUME_HOURS >= 2
# → 완료된 1시간봉 N개 사용
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

    # =====================================================
    # 1시간 거래대금
    # → 1분봉 60개
    # =====================================================

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

        df = df.tail(60)

        volume = (
            df["volCcyQuote"]
            .sum()
        )

        return float(volume)

    # =====================================================
    # 2시간 이상
    # → 1시간봉 N개
    # =====================================================

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

    volume = (
        df["volCcyQuote"]
        .tail(hours)
        .sum()
    )

    return float(volume)


# =========================================================
# 업비트 거래대금
#
# VOLUME_HOURS = 1
# → 1분봉 60개 사용
#
# VOLUME_HOURS >= 2
# → 1시간봉 N개 사용
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

    # =====================================================
    # 1시간 거래대금
    # → 1분봉 60개
    # =====================================================

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

        if "candle_acc_trade_price" not in df.columns:

            return 0

        volume = (
            pd.to_numeric(
                df["candle_acc_trade_price"],
                errors="coerce"
            )
            .fillna(0)
            .tail(60)
            .sum()
        )

        return float(volume)

    # =====================================================
    # 2시간 이상
    # → 1시간봉 N개
    # =====================================================

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

    if "candle_acc_trade_price" not in df.columns:

        return 0

    volume = (
        pd.to_numeric(
            df["candle_acc_trade_price"],
            errors="coerce"
        )
        .fillna(0)
        .tail(hours)
        .sum()
    )

    return float(volume)


# =========================================================
# 업비트 전체 거래대금
#
# VOLUME_HOURS 기준으로 계산
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

        volume = get_upbit_volume(
            market
        )

        volume_map[market] = volume

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

        color = "🟩"
        sign = "+"

    elif x < 0:

        color = "🟥"
        sign = ""

    else:

        color = "⬜️"
        sign = ""

    return f"""
    <span class="change-item">

        <span class="change-icon">
            {color}
        </span>

        <span class="change-value">
            {sign}{x:.2f}%
        </span>

    </span>
    """


# =========================================================
# 눌림 경고 HTML
# =========================================================

def warning_html(
    warning
):

    if warning == "long_special":

        return "〽️"

    elif warning == "short_special":

        return "〽️"

    if warning.startswith(
        "long_warning_"
    ):

        try:

            count = int(
                warning.split("_")[-1]
            )

        except Exception:

            count = 0

        return f"🚀({count})"

    elif warning.startswith(
        "short_warning_"
    ):

        try:

            count = int(
                warning.split("_")[-1]
            )

        except Exception:

            count = 0

        return f"🚨({count})"

    return ""


# =========================================================
# 돌파 경고 HTML
# =========================================================

def breakout_html(
    breakout
):

    if breakout.startswith(
        "long_breakout_"
    ):

        try:

            count = int(
                breakout.split("_")[-1]
            )

        except Exception:

            count = 0

        return f"⚡({count})"

    elif breakout.startswith(
        "short_breakout_"
    ):

        try:

            count = int(
                breakout.split("_")[-1]
            )

        except Exception:

            count = 0

        return f"💥({count})"

    return ""


# =========================================================
# LONG / SHORT HTML
# =========================================================

def signal_html(
    signal
):

    if signal == "LONG":

        return """
        <span class="signal long-signal">
            LONG
        </span>
        """

    elif signal == "SHORT":

        return """
        <span class="signal short-signal">
            SHORT
        </span>
        """

    return ""


# =========================================================
# EMA HTML
# =========================================================

def ema_html(
    ema
):

    warning = warning_html(
        ema["warning"]
    )

    breakout = breakout_html(
        ema["breakout"]
    )

    signal = signal_html(
        ema["signal"]
    )

    alert_html = ""

    if warning:

        alert_html += f"""
        <span class="alert-warning">
            {warning}
        </span>
        """

    if breakout:

        alert_html += f"""
        <span class="alert-breakout">
            {breakout}
        </span>
        """

    return f"""

<div class="ema-display">

    <div class="signal-period">

        {signal}

    </div>


    <div class="alert-period">

        {alert_html}

    </div>


    <div class="ema-period">

        <span class="ema-time">
            15M
        </span>

        <span class="ema-status">
            {ema["15m_10_20"]}
        </span>

        <span class="ema-status">
            {ema["15m_20_60_120"]}
        </span>

    </div>


    <div class="ema-period last">

        <span class="ema-time">
            1H
        </span>

        <span class="ema-status">
            {ema["1h_10_20"]}
        </span>

        <span class="ema-status">
            {ema["1h_20_60_120"]}
        </span>

    </div>


</div>

"""


# =========================================================
# OKX TOP15
# =========================================================

def update_okx():

    global latest_okx_data

    logging.info(
        f"OKX TOP{TOP_N} 시작 "
        f"(거래대금 {VOLUME_HOURS}시간)"
    )

    symbols = (
        get_all_okx_swap_symbols()
    )

    if not symbols:

        logging.error(
            "OKX 심볼을 가져오지 못했습니다."
        )

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

    # =====================================================
    # 모든 OKX USDT-SWAP 거래대금 계산
    # =====================================================

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

        volume_map[symbol] = (
            volume_krw
        )

        time.sleep(0.03)

        if index % 50 == 0:

            logging.info(
                f"OKX 거래대금 "
                f"{index}/{total}"
            )

    # =====================================================
    # TOP15
    # =====================================================

    top_symbols = sorted(
        volume_map,
        key=volume_map.get,
        reverse=True
    )[:TOP_N]

    rows = []

    rank = 1

    for symbol in top_symbols:

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

            "rank":
                rank,

            "name":
                coin,

            "change":
                format_change(
                    changes
                ),

            "volume":
                format_volume(
                    volume_map[symbol]
                ),

            "ema":
                ema

        })

        rank += 1

    latest_okx_data = rows

    logging.info(
        f"OKX TOP{TOP_N} 완료"
    )


# =========================================================
# 업비트 TOP15
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
            "업비트 마켓을 가져오지 못했습니다."
        )

        return

    # =====================================================
    # 거래대금 계산
    # =====================================================

    volume_map = (
        get_upbit_volume_map(
            markets
        )
    )

    if not volume_map:

        logging.error(
            "업비트 거래대금을 가져오지 못했습니다."
        )

        return

    # =====================================================
    # TOP15
    # =====================================================

    top_markets = sorted(
        volume_map,
        key=volume_map.get,
        reverse=True
    )[:TOP_N]

    rows = []

    rank = 1

    for market in top_markets:

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

            "rank":
                rank,

            "name":
                coin,

            "change":
                format_change(
                    changes
                ),

            "volume":
                format_volume(
                    volume_map[market]
                ),

            "ema":
                ema

        })

        rank += 1

    latest_upbit_data = rows

    logging.info(
        f"업비트 TOP{TOP_N} 완료"
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

    logging.info(
        f"거래대금 기준 : 최근 "
        f"{VOLUME_HOURS}시간"
    )

    if VOLUME_HOURS == 1:

        logging.info(
            "1시간 거래대금 = 완료된 1분봉 60개"
        )

    else:

        logging.info(
            f"{VOLUME_HOURS}시간 거래대금 = "
            f"완료된 1시간봉 {VOLUME_HOURS}개"
        )

    logging.info(
        f"표시 순위 : TOP{TOP_N}"
    )

    logging.info(
        "========================================"
    )

    try:

        update_okx()

    except Exception as e:

        logging.exception(
            f"OKX 업데이트 오류 : {e}"
        )

    try:

        update_upbit()

    except Exception as e:

        logging.exception(
            f"업비트 업데이트 오류 : {e}"
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

<title>
OKX + UPBIT
</title>


<style>

body{

    background:#111;

    color:white;

    font-family:Arial;

    padding:20px;

}


table{

    width:auto;

    border-collapse:collapse;

    border:1px solid #444;

}


th{

    background:#333;

    padding:10px 12px;

    border-right:2px solid #555;

    white-space:nowrap;

}


th:last-child{

    border-right:none;

}


td{

    padding:8px 12px;

    border-bottom:1px solid #444;

    border-right:2px solid #333;

    text-align:center;

    white-space:nowrap;

}


td:last-child{

    border-right:none;

}


.rank-cell{

    width:45px;

    min-width:45px;

}


.coin-cell{

    min-width:90px;

    text-align:left;

    font-weight:bold;

}


.volume-cell{

    min-width:100px;

    padding-left:15px;

    padding-right:15px;

    text-align:right;

    white-space:nowrap;

}


.change-cell{

    min-width:105px;

    padding-left:12px;

    padding-right:12px;

    white-space:nowrap;

}


.change-item{

    display:inline-flex;

    align-items:center;

    width:95px;

    min-width:95px;

    box-sizing:border-box;

}


.change-icon{

    display:inline-block;

    width:28px;

    min-width:28px;

    text-align:center;

}


.change-value{

    display:inline-block;

    width:67px;

    min-width:67px;

    text-align:right;

    font-family:monospace;

}


.ema-display{

    display:flex;

    align-items:center;

    height:44px;

    white-space:nowrap;

    font-family:monospace;

    padding:0 5px;

}


.signal-period{

    width:75px;

    min-width:75px;

    text-align:center;

    display:flex;

    align-items:center;

    justify-content:center;

}


.signal{

    display:inline-block;

    font-weight:bold;

    font-size:16px;

    padding:4px 7px;

    border-radius:4px;

}


.long-signal{

    color:#00ff66;

    border:1px solid #00ff66;

}


.short-signal{

    color:#ff4444;

    border:1px solid #ff4444;

}


.alert-period{

    width:75px;

    min-width:75px;

    height:40px;

    display:flex;

    align-items:center;

    justify-content:center;

    gap:5px;

    padding:0 5px;

    border-right:2px solid #555;

    box-sizing:border-box;

}


.alert-warning{

    font-size:24px;

    line-height:30px;

    display:inline-block;

}


.alert-breakout{

    font-size:24px;

    line-height:30px;

    display:inline-block;

}


.ema-period{

    display:flex;

    align-items:center;

    height:40px;

    padding:0 10px;

    border-right:2px solid #555;

}


.ema-period.last{

    border-right:none;

}


.ema-time{

    display:inline-block;

    width:45px;

    min-width:45px;

    text-align:left;

    font-weight:bold;

}


.ema-status{

    display:inline-block;

    width:70px;

    min-width:70px;

    text-align:left;

}


.section-title{

    margin-top:25px;

    padding:10px 12px;

    background:#222;

    border-left:5px solid #666;

}


.volume-setting{

    display:inline-block;

    margin-left:10px;

    padding:6px 10px;

    background:#222;

    border:1px solid #444;

    border-radius:5px;

    color:#ddd;

}


tr:hover{

    background:#1d1d1d;

}

</style>

</head>


<body>


<h2>
📊 암호화폐 실시간 분석
</h2>


<p>
15분 EMA · 1시간 추세 · 눌림 · 돌파 · LONG / SHORT
</p>


<p>

거래대금 기준:
<span class="volume-setting">
최근 """ + str(VOLUME_HOURS) + """시간
</span>

&nbsp;&nbsp;

표시:
<span class="volume-setting">
TOP""" + str(TOP_N) + """
</span>

</p>


<h2 class="section-title">
🏆 OKX 선물 거래대금 TOP""" + str(TOP_N) + """
</h2>


<table>

<tr>

<th class="rank-cell">
순위
</th>

<th>
코인
</th>

<th>
최근 """ + str(VOLUME_HOURS) + """시간 거래대금
</th>

<th>
오늘
</th>

<th>
EMA 상태
</th>

</tr>

"""


    # =====================================================
    # OKX
    # =====================================================

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

<td>

{ema_html(
    item["ema"]
)}

</td>

</tr>

"""


    html += """

</table>


<h2 class="section-title">
🏆 업비트 현물 거래대금 TOP""" + str(TOP_N) + """
</h2>


<table>

<tr>

<th class="rank-cell">
순위
</th>

<th>
코인
</th>

<th>
최근 """ + str(VOLUME_HOURS) + """시간 거래대금
</th>

<th>
오늘
</th>

<th>
EMA 상태
</th>

</tr>

"""


    # =====================================================
    # 업비트
    # =====================================================

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

<td>

{ema_html(
    item["ema"]
)}

</td>

</tr>

"""


    html += """

</table>


</body>

</html>

"""

    return html


# =========================================================
# 시작
# =========================================================

@app.on_event("startup")
def startup():

    # =====================================================
    # 최초 1회 업데이트
    # =====================================================

    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    # =====================================================
    # 5분마다 업데이트
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
