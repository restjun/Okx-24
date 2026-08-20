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
# 전역 데이터
# =========================================================

latest_okx_data = []
latest_upbit_data = []


# =========================================================
# API 재시도
# =========================================================

def retry_request(func, *args, **kwargs):

    for attempt in range(10):

        try:

            result = func(
                *args,
                **kwargs
            )

            if hasattr(result, "status_code"):

                if result.status_code == 429:

                    time.sleep(1)

                    continue

            return result

        except Exception as e:

            logging.error(
                f"API 실패 {attempt + 1}/10 : {e}"
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

    url = (
        "https://www.okx.com/api/v5/market/candles"
        f"?instId={inst_id}"
        f"&bar={bar}"
        f"&limit={limit}"
    )

    response = retry_request(
        requests.get,
        url
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

        # 미완성 캔들 제외
        df = df[
            df["confirm"].astype(str) == "1"
        ]

        if df.empty:
            return None

        df = df.iloc[::-1].reset_index(
            drop=True
        )

        return df

    except Exception as e:

        logging.error(
            f"OKX 오류 {inst_id}:{e}"
        )

        return None


# =========================================================
# 업비트 분봉
# =========================================================

def get_upbit_ohlcv(
    market,
    unit=240,
    count=200
):

    url = (
        "https://api.upbit.com/v1/candles/minutes/"
        f"{unit}"
        f"?market={market}"
        f"&count={count}"
    )

    response = retry_request(
        requests.get,
        url
    )

    if response is None:
        return None

    try:

        data = response.json()

        if not data:
            return None

        df = pd.DataFrame(data)

        df = df.iloc[::-1].reset_index(
            drop=True
        )

        df["trade_price"] = (
            df["trade_price"]
            .astype(float)
        )

        return df

    except Exception as e:

        logging.error(
            f"업비트 캔들 오류 {market}:{e}"
        )

        return None


# =========================================================
# 업비트 4H 캔들
# 미완성 캔들 제외
# =========================================================

def get_upbit_4h_ohlcv(
    market,
    count=200
):

    url = (
        "https://api.upbit.com/v1/candles/minutes/240"
        f"?market={market}"
        f"&count={count}"
    )

    response = retry_request(
        requests.get,
        url
    )

    if response is None:
        return None

    try:

        data = response.json()

        if not data:
            return None

        df = pd.DataFrame(data)

        if df.empty:
            return None

        df = df.iloc[::-1].reset_index(
            drop=True
        )

        df["trade_price"] = (
            pd.to_numeric(
                df["trade_price"],
                errors="coerce"
            )
        )

        # =====================================================
        # 업비트 4H 날짜 처리
        # =====================================================

        df["candle_datetime"] = pd.to_datetime(
            df["candle_date_time_kst"],
            errors="coerce"
        )

        df = df.dropna(
            subset=["candle_datetime"]
        )

        # =====================================================
        # 현재 진행 중인 4시간봉 제외
        # =====================================================

        now = pd.Timestamp.now(
            tz="Asia/Seoul"
        )

        current_hour = now.hour

        candle_start_hour = (
            current_hour // 4
        ) * 4

        current_candle_start = (
            now.normalize()
            +
            pd.Timedelta(
                hours=candle_start_hour
            )
        )

        df = df[
            df["candle_datetime"]
            <
            current_candle_start.tz_localize(
                None
            )
        ]

        df = df.reset_index(
            drop=True
        )

        if df.empty:
            return None

        # 가격 결측 제거
        df = df.dropna(
            subset=["trade_price"]
        ).reset_index(
            drop=True
        )

        if len(df) < 20:
            return None

        return df

    except Exception as e:

        logging.error(
            f"업비트 4H 오류 {market}:{e}"
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
        url
    )

    if response is None:
        return None

    try:

        data = response.json()

        if not data:
            return None

        df = pd.DataFrame(data)

        if df.empty:
            return None

        df = df.iloc[::-1].reset_index(
            drop=True
        )

        df["trade_price"] = (
            pd.to_numeric(
                df["trade_price"],
                errors="coerce"
            )
        )

        df = df.dropna(
            subset=["trade_price"]
        ).reset_index(
            drop=True
        )

        if len(df) < 20:
            return None

        return df

    except Exception as e:

        logging.error(
            f"업비트 일봉 오류 {market}:{e}"
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
        url
    )

    if response is None:
        return []

    try:

        return [
            x["instId"]
            for x in response.json()["data"]
            if (
                x["instId"].endswith("-USDT-SWAP")
                and x.get("state") == "live"
            )
        ]

    except Exception as e:

        logging.error(
            f"OKX 목록 오류:{e}"
        )

        return []


# =========================================================
# 업비트 목록
# =========================================================

def get_upbit_markets():

    response = retry_request(
        requests.get,
        "https://api.upbit.com/v1/market/all"
    )

    if response is None:
        return []

    try:

        return [
            x["market"]
            for x in response.json()
            if x["market"].startswith("KRW-")
        ]

    except Exception as e:

        logging.error(
            f"업비트 목록 오류:{e}"
        )

        return []


# =========================================================
# USDT/KRW
# =========================================================

def get_usdt_krw():

    response = retry_request(
        requests.get,
        "https://api.upbit.com/v1/ticker?markets=KRW-USDT"
    )

    if response is None:
        return 1400

    try:

        return float(
            response.json()[0]["trade_price"]
        )

    except:

        return 1400


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
# EMA 10-20 방향
# =========================================================

def get_ema_10_20_direction(
    df,
    column
):

    if (
        df is None
        or column not in df.columns
        or len(df) < 20
    ):
        return "none"

    try:

        values = pd.to_numeric(
            df[column],
            errors="coerce"
        )

        if values.isna().all():
            return "none"

        ema10 = (
            values
            .ewm(
                span=10,
                adjust=False
            )
            .mean()
        )

        ema20 = (
            values
            .ewm(
                span=20,
                adjust=False
            )
            .mean()
        )

        if pd.isna(
            ema10.iloc[-1]
        ) or pd.isna(
            ema20.iloc[-1]
        ):
            return "none"

        if ema10.iloc[-1] > ema20.iloc[-1]:

            return "long"

        elif ema10.iloc[-1] < ema20.iloc[-1]:

            return "short"

    except Exception as e:

        logging.error(
            f"EMA 10-20 방향 오류:{e}"
        )

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
        or column not in df.columns
        or len(df) < 120
    ):
        return "none"

    try:

        values = pd.to_numeric(
            df[column],
            errors="coerce"
        )

        ema20 = (
            values
            .ewm(
                span=20,
                adjust=False
            )
            .mean()
        )

        ema60 = (
            values
            .ewm(
                span=60,
                adjust=False
            )
            .mean()
        )

        ema120 = (
            values
            .ewm(
                span=120,
                adjust=False
            )
            .mean()
        )

        if (
            pd.isna(ema20.iloc[-1])
            or
            pd.isna(ema60.iloc[-1])
            or
            pd.isna(ema120.iloc[-1])
        ):
            return "none"

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

    except Exception as e:

        logging.error(
            f"EMA 20-60-120 방향 오류:{e}"
        )

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
        or column not in df.columns
        or len(df) < 20
    ):
        return "⚪(0)"

    try:

        values = pd.to_numeric(
            df[column],
            errors="coerce"
        )

        if values.isna().all():
            return "⚪(0)"

        ema10 = (
            values
            .ewm(
                span=10,
                adjust=False
            )
            .mean()
        )

        ema20 = (
            values
            .ewm(
                span=20,
                adjust=False
            )
            .mean()
        )

        states = []

        for i in range(len(values)):

            if (
                pd.isna(ema10.iloc[i])
                or
                pd.isna(ema20.iloc[i])
            ):

                states.append("none")

            elif (
                ema10.iloc[i]
                >
                ema20.iloc[i]
            ):

                states.append("long")

            elif (
                ema10.iloc[i]
                <
                ema20.iloc[i]
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

    except Exception as e:

        logging.error(
            f"EMA 10-20 상태 오류:{e}"
        )

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
        or column not in df.columns
        or len(df) < 120
    ):
        return "⚪(0)"

    try:

        values = pd.to_numeric(
            df[column],
            errors="coerce"
        )

        ema20 = (
            values
            .ewm(
                span=20,
                adjust=False
            )
            .mean()
        )

        ema60 = (
            values
            .ewm(
                span=60,
                adjust=False
            )
            .mean()
        )

        ema120 = (
            values
            .ewm(
                span=120,
                adjust=False
            )
            .mean()
        )

        states = []

        for i in range(len(values)):

            if (
                pd.isna(ema20.iloc[i])
                or
                pd.isna(ema60.iloc[i])
                or
                pd.isna(ema120.iloc[i])
            ):

                states.append("none")

            elif (
                ema20.iloc[i]
                >
                ema60.iloc[i]
                >
                ema120.iloc[i]
            ):

                states.append("long")

            elif (
                ema20.iloc[i]
                <
                ema60.iloc[i]
                <
                ema120.iloc[i]
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

    except Exception as e:

        logging.error(
            f"EMA 20-60-120 상태 오류:{e}"
        )

    return "⚪(0)"


# =========================================================
# EMA 10-20 현재 방향 + 지속 캔들 수
# =========================================================

def get_ema_10_20_count(
    df,
    column
):

    if (
        df is None
        or column not in df.columns
        or len(df) < 20
    ):
        return 0, "none"

    try:

        values = pd.to_numeric(
            df[column],
            errors="coerce"
        )

        ema10 = (
            values
            .ewm(
                span=10,
                adjust=False
            )
            .mean()
        )

        ema20 = (
            values
            .ewm(
                span=20,
                adjust=False
            )
            .mean()
        )

        states = []

        for i in range(len(values)):

            if (
                pd.isna(ema10.iloc[i])
                or
                pd.isna(ema20.iloc[i])
            ):

                states.append("none")

            elif ema10.iloc[i] > ema20.iloc[i]:

                states.append("long")

            elif ema10.iloc[i] < ema20.iloc[i]:

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

    except Exception as e:

        logging.error(
            f"EMA 지속 캔들 오류:{e}"
        )

        return 0, "none"


# =========================================================
# 4H + 1D 눌림 경고
#
# 기존 OKX 로직 유지
#
# 롱:
# 4H 10-20 역배열
# 4H 20-60-120 정배열
#
# 추가 경고:
# 1D 10-20 정배열
# 1D 20-60-120 정배열
#
# 숏은 반대
# =========================================================

def check_4h_warning(
    df4h,
    column4h,
    df1d,
    column1d
):

    if (
        df4h is None
        or df1d is None
        or len(df4h) < 120
        or len(df1d) < 120
    ):

        return "none"

    ema4h_10_20 = get_ema_10_20_direction(
        df4h,
        column4h
    )

    ema4h_20_60_120 = get_ema_20_60_120_direction(
        df4h,
        column4h
    )

    ema1d_10_20 = get_ema_10_20_direction(
        df1d,
        column1d
    )

    ema1d_20_60_120 = get_ema_20_60_120_direction(
        df1d,
        column1d
    )

    # =====================================================
    # 롱 눌림
    # =====================================================

    if (
        ema4h_10_20 == "short"
        and
        ema4h_20_60_120 == "long"
    ):

        warning_count = 1

        if ema1d_10_20 == "long":
            warning_count += 1

        if ema1d_20_60_120 == "long":
            warning_count += 1

        return "long_warning_" + str(
            warning_count
        )

    # =====================================================
    # 숏 눌림
    # =====================================================

    if (
        ema4h_10_20 == "long"
        and
        ema4h_20_60_120 == "short"
    ):

        warning_count = 1

        if ema1d_10_20 == "short":
            warning_count += 1

        if ema1d_20_60_120 == "short":
            warning_count += 1

        return "short_warning_" + str(
            warning_count
        )

    return "none"


# =========================================================
# 4H 돌파 경고
#
# ★ 수정됨
#
# 돌파에서는 20-60-120 조건을 사용하지 않음
#
# 롱:
# 4H 10-20 정배열
# 1D 10-20 정배열
# 4H 10-20 정배열 지속 10봉 이하
#
# 숏:
# 4H 10-20 역배열
# 1D 10-20 역배열
# 4H 10-20 역배열 지속 10봉 이하
# =========================================================

def check_4h_breakout_warning(
    df4h,
    column4h,
    df1d,
    column1d
):

    if (
        df4h is None
        or df1d is None
        or len(df4h) < 20
        or len(df1d) < 20
    ):

        return "none"

    # =====================================================
    # 4H 10-20
    # =====================================================

    count4h, direction4h = get_ema_10_20_count(
        df4h,
        column4h
    )

    # =====================================================
    # 1D 10-20
    # =====================================================

    ema1d_10_20 = get_ema_10_20_direction(
        df1d,
        column1d
    )

    # =====================================================
    # ⚡ 롱 돌파
    #
    # 20-60-120 조건 없음
    # =====================================================

    if (
        direction4h == "long"
        and
        ema1d_10_20 == "long"
        and
        count4h <= 10
    ):

        return f"long_breakout_{count4h}"

    # =====================================================
    # 💥 숏 돌파
    #
    # 20-60-120 조건 없음
    # =====================================================

    if (
        direction4h == "short"
        and
        ema1d_10_20 == "short"
        and
        count4h <= 10
    ):

        return f"short_breakout_{count4h}"

    return "none"


# =========================================================
# OKX 4H + 1D EMA
# =========================================================

def get_okx_4h_ema(
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

    return {

        "4h_10_20":
            check_ema_10_20(
                df4h,
                "c"
            ),

        "4h_20_60_120":
            check_ema(
                df4h,
                "c"
            ),

        "1d_10_20":
            check_ema_10_20(
                df1d,
                "c"
            ),

        "1d_20_60_120":
            check_ema(
                df1d,
                "c"
            ),

        "warning":
            check_4h_warning(
                df4h,
                "c",
                df1d,
                "c"
            ),

        "breakout":
            check_4h_breakout_warning(
                df4h,
                "c",
                df1d,
                "c"
            )

    }


# =========================================================
# 업비트 4H + 1D EMA
# =========================================================

def get_upbit_4h_ema(
    market
):

    df4h = get_upbit_4h_ohlcv(
        market,
        200
    )

    df1d = get_upbit_day_ohlcv(
        market,
        200
    )

    # 디버깅용
    if df4h is None:

        logging.warning(
            f"업비트 4H 데이터 없음: {market}"
        )

    if df1d is None:

        logging.warning(
            f"업비트 1D 데이터 없음: {market}"
        )

    return {

        "4h_10_20":
            check_ema_10_20(
                df4h,
                "trade_price"
            ),

        "4h_20_60_120":
            check_ema(
                df4h,
                "trade_price"
            ),

        "1d_10_20":
            check_ema_10_20(
                df1d,
                "trade_price"
            ),

        "1d_20_60_120":
            check_ema(
                df1d,
                "trade_price"
            ),

        "warning":
            check_4h_warning(
                df4h,
                "trade_price",
                df1d,
                "trade_price"
            ),

        "breakout":
            check_4h_breakout_warning(
                df4h,
                "trade_price",
                df1d,
                "trade_price"
            )

    }


# =========================================================
# OKX 24시간 거래대금
# =========================================================

def get_okx_volume(
    inst_id
):

    df = get_okx_ohlcv(
        inst_id,
        "1H",
        24
    )

    if df is None:
        return 0

    return df[
        "volCcyQuote"
    ].sum()


# =========================================================
# 업비트 24시간 거래대금
# =========================================================

def get_upbit_volume_map():

    markets = get_upbit_markets()

    if not markets:
        return {}

    response = retry_request(
        requests.get,
        "https://api.upbit.com/v1/ticker?markets="
        +
        ",".join(markets)
    )

    if response is None:
        return {}

    try:

        return {

            x["market"]:
            x["acc_trade_price_24h"]

            for x in response.json()

        }

    except Exception as e:

        logging.error(
            f"업비트 거래대금 오류:{e}"
        )

        return {}


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

    if df is None or len(df) < 50:
        return None

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

    if df is None or len(df) < 50:
        return None

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
# 변동률 표시
# =========================================================

def format_change(
    changes
):

    if changes is None or len(changes) == 0:
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

    return f'''
    <span class="change-item">

        <span class="change-icon">
            {color}
        </span>

        <span class="change-value">
            {sign}{x:.2f}%
        </span>

    </span>
    '''


# =========================================================
# 눌림 경고 HTML
# =========================================================

def warning_html(
    warning
):

    if warning.startswith(
        "long_warning_"
    ):

        try:

            count = int(
                warning.split("_")[-1]
            )

        except:

            count = 0

        return "🚀" * count

    elif warning.startswith(
        "short_warning_"
    ):

        try:

            count = int(
                warning.split("_")[-1]
            )

        except:

            count = 0

        return "🚨" * count

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

        except:

            count = 0

        return f"⚡({count})"

    elif breakout.startswith(
        "short_breakout_"
    ):

        try:

            count = int(
                breakout.split("_")[-1]
            )

        except:

            count = 0

        return f"💥({count})"

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

    return f"""

<div class="ema-display">

    <div class="ema-period breakout-period">

        <span class="ema-warning breakout-warning">
            {breakout}
        </span>

    </div>


    <div class="ema-period">

        <span class="ema-warning">
            {warning}
        </span>

        <span class="ema-time">
            4H
        </span>

        <span class="ema-status">
            {ema["4h_10_20"]}
        </span>

        <span class="ema-status">
            {ema["4h_20_60_120"]}
        </span>

    </div>


    <div class="ema-period last">

        <span class="ema-time">
            1D
        </span>

        <span class="ema-status">
            {ema["1d_10_20"]}
        </span>

        <span class="ema-status">
            {ema["1d_20_60_120"]}
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
        "OKX TOP15 시작"
    )

    symbols = get_all_okx_swap_symbols()

    usdt_krw = get_usdt_krw()

    upbit_coin_set = {

        market.replace(
            "KRW-",
            ""
        )

        for market in get_upbit_markets()

    }

    volume_map = {}

    for symbol in symbols:

        volume_map[symbol] = (
            get_okx_volume(symbol)
            *
            usdt_krw
            /
            10
        )

    top15 = sorted(
        volume_map,
        key=volume_map.get,
        reverse=True
    )[:15]

    rows = []

    rank = 1

    for symbol in top15:

        coin = symbol.replace(
            "-USDT-SWAP",
            ""
        )

        if coin in upbit_coin_set:

            coin = f"{coin}(업비트)"

        changes = get_okx_change(
            symbol
        )

        ema4h = get_okx_4h_ema(
            symbol
        )

        rows.append({

            "rank":
                rank,

            "name":
                coin,

            "change":
                format_change(changes),

            "volume":
                format_volume(
                    volume_map[symbol]
                ),

            "ema4h":
                ema4h

        })

        rank += 1

    latest_okx_data = rows

    logging.info(
        "OKX 완료"
    )


# =========================================================
# 업비트 TOP15
# =========================================================

def update_upbit():

    global latest_upbit_data

    logging.info(
        "업비트 TOP15 시작"
    )

    volume_map = get_upbit_volume_map()

    if not volume_map:
        return

    top15 = sorted(
        volume_map,
        key=volume_map.get,
        reverse=True
    )[:15]

    rows = []

    rank = 1

    for market in top15:

        coin = market.replace(
            "KRW-",
            ""
        )

        changes = get_upbit_change(
            market
        )

        ema4h = get_upbit_4h_ema(
            market
        )

        rows.append({

            "rank":
                rank,

            "name":
                coin,

            "change":
                format_change(changes),

            "volume":
                format_volume(
                    volume_map[market]
                ),

            "ema4h":
                ema4h

        })

        rank += 1

    latest_upbit_data = rows

    logging.info(
        "업비트 완료"
    )


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    logging.info(
        "전체 조회 시작"
    )

    update_okx()

    update_upbit()

    logging.info(
        "전체 업데이트 완료"
    )


# =========================================================
# 스케줄러
# =========================================================

def scheduler():

    while True:

        schedule.run_pending()

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

    height:28px;

    white-space:nowrap;

    font-family:monospace;

    padding:0 5px;

}

.ema-period{

    display:flex;

    align-items:center;

    padding:0 10px;

    border-right:2px solid #555;

}

.ema-period.last{

    border-right:none;

}

.breakout-period{

    min-width:65px;

    padding-left:5px;

    padding-right:5px;

}

.breakout-warning{

    font-size:18px;

}

.ema-warning{

    display:inline-block;

    width:85px;

    min-width:85px;

    text-align:center;

}

.ema-time{

    display:inline-block;

    width:40px;

    min-width:40px;

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
4시간 눌림 · 돌파 · EMA 10-20 / 20-60-120 · 일봉 추세 확인
</p>

<h2 class="section-title">
🏆 OKX 선물 거래대금 TOP15
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
거래대금
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
{ema_html(item["ema4h"])}
</td>

</tr>

"""


    html += """

</table>

<h2 class="section-title">
🏆 업비트 현물 거래대금 TOP15
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
거래대금
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
{ema_html(item["ema4h"])}
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

    update_dashboard()

    schedule.every(5).minutes.do(
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
