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

# 거래대금 기준
VOLUME_HOURS = 4

# 표시 순위
TOP_N = 10

# 업데이트 주기
UPDATE_MINUTES = 5

# ⚡ 추세전환 최대 카운트
MAX_WARNING_COUNT = 3

# 🔥 눌림목 EMA30 근접 허용 범위
PULLBACK_DISTANCE = 0.01

# 🚀 돌파 확인용 이전 캔들 수
BREAKOUT_LOOKBACK = 5


# =========================================================
# 전역 데이터
# =========================================================

latest_upbit_data = []

latest_okx_data = []


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
# 완료된 캔들만 사용
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

        # 완료된 캔들만
        df = df[
            df["confirm"]
            .astype(str)
            == "1"
        ]

        if df.empty:
            return None

        # 오래된 → 최신
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
#
# 중요:
# 기존 업비트 데이터에는
# o / h / l / c 컬럼이 없었기 때문에
# OKX와 동일하게 변환
#
# 이것이 기존 KeyError: 'l' 오류의 원인
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

        # 오래된 → 최신
        df = (
            df
            .iloc[::-1]
            .reset_index(drop=True)
        )

        # -------------------------------------------------
        # OKX와 동일한 OHLC 컬럼으로 통일
        # -------------------------------------------------

        rename_map = {

            "opening_price": "o",

            "high_price": "h",

            "low_price": "l",

            "trade_price": "c"

        }

        df = df.rename(
            columns=rename_map
        )

        for column in [
            "o",
            "h",
            "l",
            "c",
            "candle_acc_trade_volume",
            "candle_acc_trade_price"
        ]:

            if column in df.columns:

                df[column] = pd.to_numeric(
                    df[column],
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
#
# 최신 일봉은 진행 중일 수 있으므로 제외
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

        df = df.rename(
            columns={
                "opening_price": "o",
                "high_price": "h",
                "low_price": "l",
                "trade_price": "c"
            }
        )

        for column in [
            "o",
            "h",
            "l",
            "c"
        ]:

            if column in df.columns:

                df[column] = pd.to_numeric(
                    df[column],
                    errors="coerce"
                )

        # 현재 진행 중인 일봉 제외
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
        or len(df) < period
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
#
# 30개 미만이면 판단 불가
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
#
# 120개 미만이면 전체 정배열/역배열 판단 안 함
#
# 신생 코인은 아래 표시 함수에서
# 확인 가능한 EMA까지만 표시
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
# 30-60-120 연속 카운트
#
# 중요:
# 120개 캔들이 없으면 카운트 없음
# =========================================================

def get_30_60_120_count(
    df,
    column
):

    if (
        df is None
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
# 신생 코인용 EMA 표시
#
# 캔들 개수에 따라 확인 가능한 이평만 표시
#
# 120개 이상
# → 30-60-120 + 카운트
#
# 60~119개
# → 30-60
#
# 30~59개
# → 30
#
# 30개 미만
# → —
# =========================================================

def get_ema_structure_display(
    df,
    column
):

    if (
        df is None
        or column not in df.columns
    ):

        return "—"

    length = len(df)

    if length < 30:

        return "—"

    ema30 = get_ema(
        df,
        column,
        30
    )

    if ema30 is None:

        return "—"

    # -----------------------------------------------------
    # 30개 이상 / 60개 미만
    # -----------------------------------------------------

    if length < 60:

        current = ema30.iloc[-1]

        price = pd.to_numeric(
            df[column],
            errors="coerce"
        ).iloc[-1]

        if price > current:

            return "30 🟢"

        elif price < current:

            return "30 🔴"

        return "30 ⚪"

    # -----------------------------------------------------
    # 60개 이상 / 120개 미만
    # -----------------------------------------------------

    ema60 = get_ema(
        df,
        column,
        60
    )

    if ema60 is None:

        return "30"

    e30 = ema30.iloc[-1]
    e60 = ema60.iloc[-1]

    if e30 > e60:

        return "30-60 🟢"

    elif e30 < e60:

        return "30-60 🔴"

    return "30-60 ⚪"

    # -----------------------------------------------------
    # 120개 이상
    # -----------------------------------------------------


# =========================================================
# 120개 이상일 때 EMA 표시
#
# ⚪에는 카운트를 절대 표시하지 않음
# =========================================================

def check_ema(
    df,
    column
):

    if (
        df is None
        or len(df) < 120
    ):

        return get_ema_structure_display(
            df,
            column
        )

    count, direction = (
        get_30_60_120_count(
            df,
            column
        )
    )

    if direction == "long":

        return f"30-60-120 🟢({count})"

    if direction == "short":

        return f"30-60-120 🔴({count})"

    # 흰색은 카운트 없음
    return "30-60-120 ⚪"


# =========================================================
# EMA 10-30 표시
# =========================================================

def check_ema_10_30(
    df,
    column
):

    if (
        df is None
        or len(df) < 30
    ):

        return "—"

    direction = get_ema_10_30_direction(
        df,
        column
    )

    if direction == "long":

        return "🟢"

    if direction == "short":

        return "🔴"

    return "⚪"


# =========================================================
# 일봉 + 4H 방향
#
# 실제 매매 신호는 기존대로
# 4H 30-60-120 + 일봉 10-30
# 모두 있어야 함
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
# ⚡ 추세 전환 초반
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
        pd.isna(prev["ema30"])
    ):

        return "none"

    cur_ema30 = cur["ema30"]
    prev_ema30 = prev["ema30"]

    # =====================================================
    # LONG
    # =====================================================

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
        cur_ema30 * (
            1 + PULLBACK_DISTANCE
        )
    )

    long_close = (
        cur[column]
        >
        cur_ema30
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
        prev_ema30 * (
            1 + PULLBACK_DISTANCE
        )
    )

    prev_long_close = (
        prev[column]
        >
        prev_ema30
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

    # =====================================================
    # SHORT
    # =====================================================

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
        cur_ema30 * (
            1 - PULLBACK_DISTANCE
        )
    )

    short_close = (
        cur[column]
        <
        cur_ema30
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
        prev_ema30 * (
            1 - PULLBACK_DISTANCE
        )
    )

    prev_short_close = (
        prev[column]
        <
        prev_ema30
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
        -(
            BREAKOUT_LOOKBACK + 1
        ):
        -1
    ]

    if previous.empty:

        return "none"

    previous_high = (
        pd.to_numeric(
            previous["h"],
            errors="coerce"
        )
        .max()
    )

    previous_low = (
        pd.to_numeric(
            previous["l"],
            errors="coerce"
        )
        .min()
    )

    # LONG

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

    # SHORT

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
# 최종 4H 진입 경고
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
# 최종 LONG / SHORT
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
        warning.startswith(
            "long_"
        )
    ):

        return "LONG", warning

    if (
        main_direction == "short"
        and
        warning.startswith(
            "short_"
        )
    ):

        return "SHORT", warning

    return "", "none"


# =========================================================
# OKX 4H + 1D
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

            "4h_10_30": "—",

            "4h_30_60_120": "—",

            "1d_10_30": "—",

            "signal": "",

            "warning": "none",

            "direction": "none"

        }

    signal, warning = (
        get_trade_signal(
            df4h,
            df1d,
            "c"
        )
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
# 업비트 4H + 1D
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

            "4h_10_30": "—",

            "4h_30_60_120": "—",

            "1d_10_30": "—",

            "signal": "",

            "warning": "none",

            "direction": "none"

        }

    # 진행 중인 240분봉 제외
    if len(df4h) > 1:

        df4h = (
            df4h
            .iloc[:-1]
            .reset_index(drop=True)
        )

    signal, warning = (
        get_trade_signal(
            df4h,
            df1d,
            "c"
        )
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
#
# 최종 결과 / 10
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

        if (
            "candle_acc_trade_price"
            not in df.columns
        ):

            return 0

        return float(
            pd.to_numeric(
                df[
                    "candle_acc_trade_price"
                ],
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

    if (
        df is None
        or df.empty
    ):

        return 0

    if (
        "candle_acc_trade_price"
        not in df.columns
    ):

        return 0

    return float(
        pd.to_numeric(
            df[
                "candle_acc_trade_price"
            ],
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
# 변동률 표시
# =========================================================

def format_change(
    changes
):

    if (
        changes is None
        or len(changes) == 0
    ):

        return "—"

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
        f"{color} "
        f"{sign}{x:.2f}%"
    )


# =========================================================
# 신호 HTML
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
                '</span> '
                f'⚡({count})'
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
                '</span> '
                f'💥({count})'
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

    return "—"


# =========================================================
# EMA HTML
#
# 휴대폰용 한 줄 표시
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
            '☀️LONG'
            '</span>'
        )

    elif direction == "short":

        direction_html = (
            '<span class="direction-short">'
            '🌧SHORT'
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
        <span class="ema-time">4H</span>
        <span>{ema["4h_10_30"]}</span>
        <span>{ema["4h_30_60_120"]}</span>
    </div>

    <div class="ema-period last">
        <span class="ema-time">1D</span>
        <span>10-30 {ema["1d_10_30"]}</span>
    </div>

</div>

"""


# =========================================================
# 업비트 TOP
#
# ★ 화면 표시 순서를 업비트 먼저
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

        rank += 1

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

        rank += 1

    latest_okx_data = rows

    logging.info(
        f"OKX TOP{TOP_N} 완료"
    )


# =========================================================
# 전체 업데이트
#
# ★ 업비트 먼저
# ★ OKX 나중
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
        "OKX 거래대금 : 최종 ÷10"
    )

    logging.info(
        "매매 기준 : 4H 완료 종가"
    )

    logging.info(
        "일봉 방향 : EMA 10-30"
    )

    logging.info(
        "4H 추세 : EMA 30-60-120"
    )

    logging.info(
        "⚡ 추세전환 : "
        "정배열/역배열 신규 발생 후 1~3개"
    )

    logging.info(
        "🔥 눌림목 : "
        "EMA30 부근 눌림 후 방향성 종가"
    )

    logging.info(
        "🚀 돌파 : "
        f"이전 {BREAKOUT_LOOKBACK}개 "
        "4H 고점/저점 종가 돌파"
    )

    logging.info(
        "신생 코인 : "
        "확인 가능한 EMA까지만 표시"
    )

    logging.info(
        "15분 / 1시간 : 사용하지 않음"
    )

    logging.info(
        "변동률 : 표시만 하고 "
        "LONG/SHORT 필터에서는 제외"
    )

    logging.info(
        "========================================"
    )

    # -----------------------------------------------------
    # 업비트 먼저
    # -----------------------------------------------------

    try:

        update_upbit()

    except Exception as e:

        logging.exception(
            f"업비트 업데이트 오류 : {e}"
        )

    # -----------------------------------------------------
    # OKX
    # -----------------------------------------------------

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
    content="width=device-width, initial-scale=1.0"
>

<title>
4H 종가매매
</title>

<style>

*{
    box-sizing:border-box;
}

body{

    background:#111;
    color:white;
    font-family:Arial,sans-serif;

    margin:0;
    padding:10px;

    font-size:13px;

}

.page-title{

    font-size:18px;
    margin:4px 0 5px 0;

}

.description{

    font-size:11px;
    color:#aaa;

    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;

    margin-bottom:8px;

}

/* =====================================================
   한 줄 설명
   ===================================================== */

.info-line{

    background:#181818;

    border:1px solid #333;

    border-radius:5px;

    padding:7px 8px;

    margin-bottom:10px;

    font-size:10px;

    color:#ccc;

    white-space:nowrap;

    overflow:hidden;

    text-overflow:ellipsis;

}


/* =====================================================
   섹션
   ===================================================== */

.section-title{

    margin:12px 0 5px 0;

    padding:7px 8px;

    background:#222;

    border-left:4px solid #777;

    font-size:15px;

}


/* =====================================================
   테이블
   ===================================================== */

.table-wrap{

    width:100%;

    overflow-x:auto;

    -webkit-overflow-scrolling:touch;

}

table{

    width:100%;

    border-collapse:collapse;

    border:1px solid #333;

    table-layout:auto;

}

th{

    background:#292929;

    color:#ddd;

    padding:6px 4px;

    border-right:1px solid #444;

    white-space:nowrap;

    font-size:10px;

}

td{

    padding:5px 4px;

    border-bottom:1px solid #333;

    border-right:1px solid #292929;

    text-align:center;

    white-space:nowrap;

    font-size:11px;

}

td:last-child,
th:last-child{

    border-right:none;

}

tr:hover{

    background:#1d1d1d;

}


/* =====================================================
   순위
   ===================================================== */

.rank-cell{

    width:27px;

    min-width:27px;

    max-width:27px;

    color:#aaa;

}


/* =====================================================
   코인
   ===================================================== */

.coin-cell{

    min-width:65px;

    text-align:left;

    font-weight:bold;

}


/* =====================================================
   거래대금
   ===================================================== */

.volume-cell{

    min-width:65px;

    text-align:right;

    font-family:monospace;

}


/* =====================================================
   변동률
   ===================================================== */

.change-cell{

    min-width:65px;

    font-family:monospace;

}


/* =====================================================
   EMA 전체
   ===================================================== */

.ema-display{

    display:flex;

    align-items:center;

    justify-content:flex-start;

    min-height:38px;

    height:38px;

    white-space:nowrap;

    font-family:monospace;

}


/* =====================================================
   신호 영역
   항상 같은 폭
   ===================================================== */

.signal-period{

    width:62px;

    min-width:62px;

    max-width:62px;

    text-align:center;

    font-family:Arial,sans-serif;

    font-weight:bold;

}


/* =====================================================
   방향 영역
   항상 같은 폭
   ===================================================== */

.direction-period{

    width:55px;

    min-width:55px;

    max-width:55px;

    text-align:center;

    font-size:9px;

}


/* =====================================================
   EMA 영역
   ===================================================== */

.ema-period{

    display:flex;

    align-items:center;

    gap:5px;

    height:30px;

    padding:0 5px;

    border-right:1px solid #444;

    font-size:9px;

}

.ema-period.last{

    border-right:none;

}


/* =====================================================
   시간봉
   고정 폭
   ===================================================== */

.ema-time{

    display:inline-block;

    width:19px;

    min-width:19px;

    font-weight:bold;

    color:#aaa;

}


/* =====================================================
   방향
   ===================================================== */

.direction-long{

    color:#00ff66;

    font-weight:bold;

}

.direction-short{

    color:#ff6666;

    font-weight:bold;

}

.direction-none{

    color:#777;

}


/* =====================================================
   LONG / SHORT
   ===================================================== */

.long-text{

    color:#00ff66;

    font-weight:bold;

}

.short-text{

    color:#ff4444;

    font-weight:bold;

}


/* =====================================================
   모바일
   ===================================================== */

@media(
    max-width:600px
){

    body{

        padding:7px;

    }

    .page-title{

        font-size:16px;

    }

    .description{

        font-size:10px;

    }

    .info-line{

        font-size:9px;

        padding:6px;

    }

    .section-title{

        font-size:13px;

        margin-top:9px;

        padding:6px;

    }

    th{

        font-size:9px;

        padding:5px 3px;

    }

    td{

        font-size:10px;

        padding:4px 3px;

    }

    .coin-cell{

        min-width:55px;

    }

    .volume-cell{

        min-width:55px;

    }

    .change-cell{

        min-width:55px;

    }

    .signal-period{

        width:55px;

        min-width:55px;

        max-width:55px;

        font-size:10px;

    }

    .direction-period{

        width:47px;

        min-width:47px;

        max-width:47px;

        font-size:8px;

    }

    .ema-period{

        gap:3px;

        padding:0 3px;

        font-size:8px;

    }

    .ema-time{

        width:16px;

        min-width:16px;

    }

}

</style>

</head>

<body>


<div class="page-title">

📊 4H 종가매매 TOP10

</div>


<div class="description">

일봉 방향 + 4H 추세 일치 · ⚡추세전환 · 🔥눌림목 · 🚀돌파

</div>


<div class="info-line">

☀️ 일봉10-30 + 4H30-60-120 일치 | ⚡1~3개 | 🔥 EMA30 | 🚀 이전5개 고점/저점 | 완료된4H 종가 기준

</div>


<!-- =====================================================
     업비트
     ===================================================== -->

<h2 class="section-title">

🏆 업비트 현물 TOP""" + str(TOP_N) + """

</h2>


<div class="table-wrap">

<table>

<tr>

<th class="rank-cell">
#
</th>

<th>
코인
</th>

<th>
""" + str(VOLUME_HOURS) + """H
</th>

<th>
오늘
</th>

<th>
4H 종가
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

</div>


<!-- =====================================================
     OKX
     ===================================================== -->

<h2 class="section-title">

🏆 OKX 선물 TOP""" + str(TOP_N) + """

</h2>


<div class="table-wrap">

<table>

<tr>

<th class="rank-cell">
#
</th>

<th>
코인
</th>

<th>
""" + str(VOLUME_HOURS) + """H
</th>

<th>
오늘
</th>

<th>
4H 종가
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
