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

VOLUME_HOURS = 24

TOP_N = 20

UPDATE_MINUTES = 5

# ⚡ / 💥 번개 최대 캔들
MAX_WARNING_COUNT = 10


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
            df["c"].astype(float)
        )

        df["vol"] = (
            df["vol"].astype(float)
        )

        df["volCcyQuote"] = (
            df["volCcyQuote"].astype(float)
        )

        # 미완성 캔들 제외
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

        df["trade_price"] = (
            df["trade_price"].astype(float)
        )

        df["candle_acc_trade_volume"] = (
            df["candle_acc_trade_volume"]
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
        or len(df) < period
        or column not in df.columns
    ):
        return None

    price = pd.to_numeric(
        df[column],
        errors="coerce"
    )

    return price.ewm(
        span=period,
        adjust=False
    ).mean()


# =========================================================
# EMA 10-30 방향
# =========================================================

def get_ema_10_30_direction(
    df,
    column
):

    if (
        df is None
        or len(df) < 30
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
# EMA 30-60-120 방향
# =========================================================

def get_ema_30_60_120_direction(
    df,
    column
):

    if (
        df is None
        or len(df) < 120
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
# EMA 10-30-60-120 방향
# =========================================================

def get_ema_10_30_60_120_direction(
    df,
    column
):

    if (
        df is None
        or len(df) < 60
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

    if (
        ema10 is None
        or ema30 is None
        or ema60 is None
    ):
        return "none"

    if len(df) >= 120:

        ema120 = get_ema(
            df,
            column,
            120
        )

        if ema120 is not None:

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

    if (
        ema10.iloc[-1]
        >
        ema30.iloc[-1]
        >
        ema60.iloc[-1]
    ):
        return "long"

    if (
        ema10.iloc[-1]
        <
        ema30.iloc[-1]
        <
        ema60.iloc[-1]
    ):
        return "short"

    return "none"


# =========================================================
# 1H 10-30 연속 캔들 카운트
#
# 번개 카운트는 이것을 사용
# =========================================================

def get_10_30_count(
    df,
    column
):

    if (
        df is None
        or len(df) < 30
    ):
        return 0, "none"

    df = df.copy()

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

    states = []

    for _, row in df.iterrows():

        if (
            pd.isna(row["ema10"])
            or
            pd.isna(row["ema30"])
        ):

            states.append("none")

        elif (
            row["ema10"]
            >
            row["ema30"]
        ):

            states.append("long")

        elif (
            row["ema10"]
            <
            row["ema30"]
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
# EMA 10-30 상태 + 카운팅
# =========================================================

def check_ema_10_30(
    df,
    column
):

    count, direction = (
        get_10_30_count(
            df,
            column
        )
    )

    if direction == "long":

        return f"🟢({count})"

    if direction == "short":

        return f"🔴({count})"

    return "⚪(0)"


# =========================================================
# EMA 30-60-120 상태 + 카운팅
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
        return "⚪(0)"

    count = 0

    for state in reversed(states):

        if state == current_state:

            count += 1

        else:

            break

    if current_state == "long":

        return f"🟢({count})"

    if current_state == "short":

        return f"🔴({count})"

    return "⚪(0)"


# =========================================================
# 전체 정배열 / 역배열
#
# 1H + 4H
# =========================================================

def check_all_alignment(
    df1h,
    df4h,
    column
):

    if (
        df1h is None
        or df4h is None
    ):
        return "none"

    h1_10_30 = (
        get_ema_10_30_direction(
            df1h,
            column
        )
    )

    h1_30_60_120 = (
        get_ema_30_60_120_direction(
            df1h,
            column
        )
    )

    h4_all = (
        get_ema_10_30_60_120_direction(
            df4h,
            column
        )
    )

    if (
        h1_10_30 == "long"
        and
        h1_30_60_120 == "long"
        and
        h4_all == "long"
    ):

        return "long_alignment"

    if (
        h1_10_30 == "short"
        and
        h1_30_60_120 == "short"
        and
        h4_all == "short"
    ):

        return "short_alignment"

    return "none"


# =========================================================
# ⚡ / 💥 번개
#
# 눌림 / 특수구간 조건 모두 삭제
#
# LONG
# 1H 10-30 정배열
# 1H 30-60-120 정배열
# 1H 10-30 정배열 1~10개
# 4H 전체 정배열
#
# SHORT
# 1H 10-30 역배열
# 1H 30-60-120 역배열
# 1H 10-30 역배열 1~10개
# 4H 전체 역배열
# =========================================================

def check_breakout_warning(
    df1h,
    df4h,
    column
):

    if (
        df1h is None
        or df4h is None
    ):
        return "none"

    # =====================================================
    # 1H 방향
    # =====================================================

    h1_10_30 = (
        get_ema_10_30_direction(
            df1h,
            column
        )
    )

    h1_30_60_120 = (
        get_ema_30_60_120_direction(
            df1h,
            column
        )
    )

    # =====================================================
    # ★ 번개 카운트
    #
    # 10-30 연속 캔들 수
    # =====================================================

    count1h, direction1h = (
        get_10_30_count(
            df1h,
            column
        )
    )

    # =====================================================
    # 4H 전체 정배열 / 역배열
    # =====================================================

    h4_all = (
        get_ema_10_30_60_120_direction(
            df4h,
            column
        )
    )

    valid_count = (
        1 <= count1h <= MAX_WARNING_COUNT
    )

    # =====================================================
    # ⚡ LONG 번개
    #
    # 1H 10-30 정배열
    # 1H 30-60-120 정배열
    # 10-30 정배열 1~10개
    # 4H 전체 정배열
    # =====================================================

    if (
        valid_count
        and
        direction1h == "long"
        and
        h1_10_30 == "long"
        and
        h1_30_60_120 == "long"
        and
        h4_all == "long"
    ):

        return f"long_lightning_{count1h}"

    # =====================================================
    # 💥 SHORT 번개
    #
    # 1H 10-30 역배열
    # 1H 30-60-120 역배열
    # 10-30 역배열 1~10개
    # 4H 전체 역배열
    # =====================================================

    if (
        valid_count
        and
        direction1h == "short"
        and
        h1_10_30 == "short"
        and
        h1_30_60_120 == "short"
        and
        h4_all == "short"
    ):

        return f"short_lightning_{count1h}"

    return "none"


# =========================================================
# 최종 경고
# =========================================================

def check_final_warning(
    df1h,
    df4h,
    column
):

    return check_breakout_warning(
        df1h,
        df4h,
        column
    )


# =========================================================
# 당일 변동률 방향 필터
# =========================================================

def filter_warning_by_change(
    warning,
    alignment,
    daily_change
):

    if daily_change is None:

        return (
            "none",
            "none"
        )

    if daily_change > 0:

        if warning.startswith("short_"):

            warning = "none"

        if alignment == "short_alignment":

            alignment = "none"

    elif daily_change < 0:

        if warning.startswith("long_"):

            warning = "none"

        if alignment == "long_alignment":

            alignment = "none"

    else:

        warning = "none"
        alignment = "none"

    return (
        warning,
        alignment
    )


# =========================================================
# LONG / SHORT
# =========================================================

def get_trade_signal(
    warning,
    daily_change
):

    if daily_change is None:

        return ""

    if daily_change > 0:

        if warning.startswith("long_"):

            return "LONG"

    if daily_change < 0:

        if warning.startswith("short_"):

            return "SHORT"

    return ""


# =========================================================
# OKX 1H + 4H EMA
# =========================================================

def get_okx_ema(
    inst_id,
    daily_change
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

    warning = check_final_warning(
        df1h,
        df4h,
        "c"
    )

    alignment = check_all_alignment(
        df1h,
        df4h,
        "c"
    )

    warning, alignment = (
        filter_warning_by_change(
            warning,
            alignment,
            daily_change
        )
    )

    signal = get_trade_signal(
        warning,
        daily_change
    )

    return {

        "1h_10_30":
            check_ema_10_30(
                df1h,
                "c"
            ),

        "1h_30_60_120":
            check_ema(
                df1h,
                "c"
            ),

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

        "warning":
            warning,

        "signal":
            signal,

        "alignment":
            alignment
    }


# =========================================================
# 업비트 1H + 4H EMA
# =========================================================

def get_upbit_ema(
    market,
    daily_change
):

    df1h = get_upbit_ohlcv(
        market,
        60,
        200
    )

    df4h = get_upbit_ohlcv(
        market,
        240,
        200
    )

    warning = check_final_warning(
        df1h,
        df4h,
        "trade_price"
    )

    alignment = check_all_alignment(
        df1h,
        df4h,
        "trade_price"
    )

    warning, alignment = (
        filter_warning_by_change(
            warning,
            alignment,
            daily_change
        )
    )

    signal = get_trade_signal(
        warning,
        daily_change
    )

    return {

        "1h_10_30":
            check_ema_10_30(
                df1h,
                "trade_price"
            ),

        "1h_30_60_120":
            check_ema(
                df1h,
                "trade_price"
            ),

        "4h_10_30":
            check_ema_10_30(
                df4h,
                "trade_price"
            ),

        "4h_30_60_120":
            check_ema(
                df4h,
                "trade_price"
            ),

        "warning":
            warning,

        "signal":
            signal,

        "alignment":
            alignment
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

        if df is None or df.empty:

            return 0

        return float(
            df["volCcyQuote"]
            .tail(60)
            .sum()
        )

    df = get_okx_ohlcv(
        inst_id,
        "1H",
        hours + 1
    )

    if df is None or df.empty:

        return 0

    return float(
        df["volCcyQuote"]
        .tail(hours)
        .sum()
    )


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

        if df is None or df.empty:

            return 0

        if "candle_acc_trade_price" not in df.columns:

            return 0

        return float(
            pd.to_numeric(
                df["candle_acc_trade_price"],
                errors="coerce"
            )
            .fillna(0)
            .tail(60)
            .sum()
        )

    df = get_upbit_ohlcv(
        market,
        60,
        hours
    )

    if df is None or df.empty:

        return 0

    if "candle_acc_trade_price" not in df.columns:

        return 0

    return float(
        pd.to_numeric(
            df["candle_acc_trade_price"],
            errors="coerce"
        )
        .fillna(0)
        .tail(hours)
        .sum()
    )


# =========================================================
# 업비트 전체 거래대금
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
# 경고 HTML
#
# 〽️ 삭제
# =========================================================

def warning_html(
    warning
):

    if warning.startswith(
        "long_lightning_"
    ):

        try:

            count = int(
                warning.split("_")[-1]
            )

        except Exception:

            count = 0

        return f"⚡({count})"

    if warning.startswith(
        "short_lightning_"
    ):

        try:

            count = int(
                warning.split("_")[-1]
            )

        except Exception:

            count = 0

        return f"💥({count})"

    return ""


# =========================================================
# 해 / 구름 HTML
# =========================================================

def alignment_html(
    alignment
):

    if alignment == "long_alignment":

        return "☀️"

    if alignment == "short_alignment":

        return "🌧"

    return ""


# =========================================================
# LONG / SHORT + 경고 + 해/구름
#
# 〽️ 삭제
# =========================================================

def signal_html(
    signal,
    warning,
    alignment
):

    # =====================================================
    # 정배열
    # =====================================================

    if alignment == "long_alignment":

        if warning.startswith(
            "long_lightning_"
        ):

            try:

                count = int(
                    warning.split("_")[-1]
                )

            except Exception:

                count = 0

            return (
                f'<span class="long-text">'
                f'LONG'
                f'</span> ⚡({count}) ☀️'
            )

        return (
            '<span class="long-text">'
            'LONG'
            '</span> ☀️'
        )

    # =====================================================
    # 역배열
    # =====================================================

    if alignment == "short_alignment":

        if warning.startswith(
            "short_lightning_"
        ):

            try:

                count = int(
                    warning.split("_")[-1]
                )

            except Exception:

                count = 0

            return (
                f'<span class="short-text">'
                f'SHORT'
                f'</span> 💥({count}) 🌧'
            )

        return (
            '<span class="short-text">'
            'SHORT'
            '</span> 🌧'
        )

    # =====================================================
    # 경고만 있는 경우
    # =====================================================

    if warning.startswith(
        "long_lightning_"
    ):

        try:

            count = int(
                warning.split("_")[-1]
            )

        except Exception:

            count = 0

        return (
            f'<span class="long-text">'
            f'LONG'
            f'</span> ⚡({count})'
        )

    if warning.startswith(
        "short_lightning_"
    ):

        try:

            count = int(
                warning.split("_")[-1]
            )

        except Exception:

            count = 0

        return (
            f'<span class="short-text">'
            f'SHORT'
            f'</span> 💥({count})'
        )

    # =====================================================
    # signal 값만 존재
    # =====================================================

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

    return ""


# =========================================================
# EMA HTML
# =========================================================

def ema_html(
    ema
):

    display_signal = signal_html(
        ema["signal"],
        ema["warning"],
        ema["alignment"]
    )

    return f"""

<div class="ema-display">

    <div class="signal-period">

        <span class="signal">

            {display_signal}

        </span>

    </div>

    <div class="ema-period">

        <span class="ema-time">
            1H
        </span>

        <span class="ema-status">
            {ema["1h_10_30"]}
        </span>

        <span class="ema-status">
            {ema["1h_30_60_120"]}
        </span>

    </div>

    <div class="ema-period last">

        <span class="ema-time">
            4H
        </span>

        <span class="ema-status">
            {ema["4h_10_30"]}
        </span>

        <span class="ema-status">
            {ema["4h_30_60_120"]}
        </span>

    </div>

</div>

"""


# =========================================================
# OKX TOP
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

        daily_change = None

        if (
            changes is not None
            and len(changes) > 0
        ):

            daily_change = changes[0]

        ema = get_okx_ema(
            symbol,
            daily_change
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

        rank += 1

    latest_okx_data = rows

    logging.info(
        f"OKX TOP{TOP_N} 완료"
    )


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
            "업비트 마켓을 가져오지 못했습니다."
        )

        return

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

        daily_change = None

        if (
            changes is not None
            and len(changes) > 0
        ):

            daily_change = changes[0]

        ema = get_upbit_ema(
            market,
            daily_change
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

    logging.info(
        "EMA 표시 : 1H + 4H"
    )

    logging.info(
        "⚡ LONG 번개 : "
        "1H 10-30 정배열 + "
        "1H 30-60-120 정배열 + "
        "10-30 정배열 1~10개 + "
        "4H 전체 정배열"
    )

    logging.info(
        "💥 SHORT 번개 : "
        "1H 10-30 역배열 + "
        "1H 30-60-120 역배열 + "
        "10-30 역배열 1~10개 + "
        "4H 전체 역배열"
    )

    logging.info(
        "〽️ 눌림 조건 : 삭제"
    )

    logging.info(
        "특수구간 눌림 : 삭제"
    )

    logging.info(
        "☀️ 해 : "
        "1H + 4H 모두 정배열"
    )

    logging.info(
        "🌧 구름 : "
        "1H + 4H 모두 역배열"
    )

    logging.info(
        "4H 120 EMA 부족 시 "
        "10-30-60만 사용"
    )

    logging.info(
        "모든 LONG 경고 : 당일 변동률 양수"
    )

    logging.info(
        "모든 SHORT 경고 : 당일 변동률 음수"
    )

    logging.info(
        "LONG 글자 : 초록색"
    )

    logging.info(
        "SHORT 글자 : 빨간색"
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

    width:135px;
    min-width:135px;
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
    white-space:nowrap;

}

.long-text{

    color:#00ff66;
    font-weight:bold;

}

.short-text{

    color:#ff4444;
    font-weight:bold;

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
1시간 EMA · 4시간 EMA ·
⚡ 번개 · ☀️ 해 · 🌧 구름
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

&nbsp;&nbsp;

1H 번개 카운팅:
<span class="volume-setting">
1~""" + str(MAX_WARNING_COUNT) + """
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
{ema_html(item["ema"])}
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
{ema_html(item["ema"])}
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
