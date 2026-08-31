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
# FutureWarning
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

BREAKOUT_LOOKBACK = 10

USE_UPBIT = "Y"
USE_OKX = "N"

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10

OKX_RETRY_DELAY = 2


# =========================================================
# 전역 데이터
# =========================================================

latest_okx_data = []
latest_upbit_data = []
latest_usdt_krw = 0.0


# =========================================================
# 돌파 상태 저장
#
# key = 거래소 + 종목
#
# 상태:
# pre      = 🚨 돌파 전
# breakout = 🚀(1)
# done     = 사이클 종료
# =========================================================

breakout_states = {}

breakout_state_lock = threading.Lock()


# =========================================================
# API 요청
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
                        RATE_LIMIT_WAIT * (2 ** attempt),
                        60
                    )

                    logging.warning(
                        f"API 429 "
                        f"{attempt + 1}/{MAX_RETRIES} "
                        f"- {wait_time}초 대기"
                    )

                    time.sleep(wait_time)
                    continue

                if status >= 500:

                    wait_time = min(
                        2 * (2 ** attempt),
                        30
                    )

                    logging.warning(
                        f"API 서버 오류 {status} "
                        f"- {wait_time}초 대기"
                    )

                    time.sleep(wait_time)
                    continue

                if status != 200:

                    logging.warning(
                        f"API HTTP 오류 {status}"
                    )

                    return result

            return result

        except Exception as e:

            logging.error(
                f"API 실패 "
                f"{attempt + 1}/{MAX_RETRIES} : {e}"
            )

            if attempt < MAX_RETRIES - 1:

                time.sleep(
                    min(
                        2 * (attempt + 1),
                        20
                    )
                )

    return None


# =========================================================
# USDT-KRW
# =========================================================

def get_usdt_krw():

    response = retry_request(
        requests.get,
        "https://api.upbit.com/v1/ticker?markets=KRW-USDT",
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

        return price if price > 0 else None

    except Exception as e:

        logging.error(
            f"USDT-KRW 처리 오류 : {e}"
        )

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

        for col in [
            "o",
            "h",
            "l",
            "c",
            "vol",
            "volCcyQuote"
        ]:

            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

        df = df[
            df["confirm"].astype(str) == "1"
        ]

        if df.empty:
            return None

        return (
            df.iloc[::-1]
            .reset_index(drop=True)
        )

    except Exception as e:

        logging.error(
            f"OKX 캔들 오류 {inst_id} : {e}"
        )

        return None


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
        f"{unit}?market={market}&count={count}"
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

        df = (
            pd.DataFrame(data)
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
            f"업비트 캔들 오류 {market} : {e}"
        )

        return None


# =========================================================
# 업비트 마켓
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
# 업비트 거래대금
# =========================================================

def get_upbit_ticker_volume_map(markets):

    if not markets:
        return {}

    result = {}

    for i in range(
        0,
        len(markets),
        100
    ):

        chunk = markets[i:i + 100]

        while True:

            try:

                url = (
                    "https://api.upbit.com/v1/ticker"
                    "?markets="
                    + ",".join(chunk)
                )

                response = retry_request(
                    requests.get,
                    url,
                    timeout=15
                )

                if response is None:

                    time.sleep(2)
                    continue

                data = response.json()

                for item in data:

                    market = item.get("market")

                    try:
                        volume = float(
                            item.get(
                                "acc_trade_price_24h",
                                0
                            )
                        )
                    except Exception:
                        volume = 0

                    if market:
                        result[market] = volume

                break

            except Exception as e:

                logging.error(
                    f"업비트 Ticker 오류 : {e}"
                )

                time.sleep(2)

    return result


# =========================================================
# 거래대금 표시
# =========================================================

def format_volume(volume):

    if volume >= 1_000_000_000_000:

        return (
            f"{volume / 1_000_000_000_000:.2f}조"
        )

    if volume >= 100_000_000:

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
    period
):

    if (
        df is None
        or
        "c" not in df.columns
    ):
        return None

    price = pd.to_numeric(
        df["c"],
        errors="coerce"
    )

    if price.notna().sum() < period:
        return None

    return price.ewm(
        span=period,
        adjust=False
    ).mean()


# =========================================================
# EMA 상태
# =========================================================

def get_ema_state(df):

    if df is None or df.empty:

        return {
            "direction": "none",
            "count": 0
        }

    ema10 = get_ema(df, 10)
    ema30 = get_ema(df, 30)

    if (
        ema10 is None
        or
        ema30 is None
    ):

        return {
            "direction": "none",
            "count": 0
        }

    ema60 = get_ema(df, 60)

    direction = "none"

    if ema60 is not None:

        a = ema10.iloc[-1]
        b = ema30.iloc[-1]
        c = ema60.iloc[-1]

        if (
            not pd.isna(a)
            and
            not pd.isna(b)
            and
            not pd.isna(c)
        ):

            if a > b > c:
                direction = "long"

            elif a < b < c:
                direction = "short"

    else:

        a = ema10.iloc[-1]
        b = ema30.iloc[-1]

        if (
            not pd.isna(a)
            and
            not pd.isna(b)
        ):

            if a > b:
                direction = "long"

            elif a < b:
                direction = "short"

    count = 0

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

        if ema60 is not None:

            c = ema60.iloc[i]

            if pd.isna(c):
                break

            if a > b > c:
                d = "long"

            elif a < b < c:
                d = "short"

            else:
                break

        else:

            if a > b:
                d = "long"

            elif a < b:
                d = "short"

            else:
                break

        if direction == "none":
            direction = d

        if d != direction:
            break

        count += 1

    return {
        "direction": direction,
        "count": count
    }


# =========================================================
# 1H / 4H EMA 표시
# =========================================================

def ema_display_text(state):

    direction = state.get(
        "direction",
        "none"
    )

    count = state.get(
        "count",
        0
    )

    if direction == "long":
        return f"🟢({count})"

    if direction == "short":
        return f"🔴({count})"

    return "⚪"


def combined_ema_html(
    state_1h,
    state_4h
):

    return (
        '<div class="ema-combined">'
        f'<span>{ema_display_text(state_1h)}</span>'
        '<span class="ema-separator"> | </span>'
        f'<span>{ema_display_text(state_4h)}</span>'
        '</div>'
    )


# =========================================================
# 돌파 판정
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
# 돌파 전
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
# 돌파 상태
#
# 중요:
# 🚨 = 현재 진행 중인 돌파 전 상태
# 🚀(1) = 최초 확정 돌파
#
# 추가 고점/저점 갱신은 화면 표시하지 않음
# =========================================================

def get_breakout_state(
    df,
    direction,
    state_key
):

    if (
        df is None
        or
        len(df) < BREAKOUT_LOOKBACK + 30
    ):

        return "none"

    with breakout_state_lock:

        saved = breakout_states.get(
            state_key
        )

    # 이미 종료된 사이클이면 다시 표시하지 않음
    if saved == "done":
        return "none"

    current_index = len(df) - 1

    current = df.iloc[
        current_index
    ]

    previous = df.iloc[
        current_index - BREAKOUT_LOOKBACK:
        current_index
    ]

    # =====================================================
    # 현재 캔들이 돌파 전
    # =====================================================

    if direction == "long":

        if is_long_pre_breakout(
            current,
            previous
        ):

            with breakout_state_lock:
                breakout_states[state_key] = "pre"

            return "long_breakout_0"

    if direction == "short":

        if is_short_pre_breakout(
            current,
            previous
        ):

            with breakout_state_lock:
                breakout_states[state_key] = "pre"

            return "short_breakout_0"

    # =====================================================
    # 현재 캔들이 최초 확정 돌파
    # =====================================================

    if direction == "long":

        if is_long_breakout(
            current,
            previous
        ):

            with breakout_state_lock:
                breakout_states[state_key] = "breakout"

            return "long_breakout_1"

    if direction == "short":

        if is_short_breakout(
            current,
            previous
        ):

            with breakout_state_lock:
                breakout_states[state_key] = "breakout"

            return "short_breakout_1"

    # =====================================================
    # 과거 돌파를 찾지 않음
    #
    # 이전 사이클이 아니면 경고 없음
    # =====================================================

    return "none"


# =========================================================
# 거래 신호
# =========================================================

def get_trade_signal(
    df,
    state_key
):

    if df is None or df.empty:

        return (
            "",
            "none"
        )

    ema_state = get_ema_state(df)

    direction = ema_state[
        "direction"
    ]

    if direction == "none":

        return (
            "",
            "none"
        )

    warning = get_breakout_state(
        df,
        direction,
        state_key
    )

    if warning == "none":

        return (
            "",
            "none"
        )

    if warning.startswith(
        "long_breakout_"
    ):

        return (
            "LONG",
            warning
        )

    if warning.startswith(
        "short_breakout_"
    ):

        return (
            "SHORT",
            warning
        )

    return (
        "",
        warning
    )


# =========================================================
# 경고 표시 가능 여부
#
# 오직:
# 🚨 0
# 🚀(1) 1
#
# 🚀(2) 이상 제거
# =========================================================

def is_visible_breakout_warning(
    warning
):

    if not warning:
        return False

    if warning == "none":
        return False

    if warning.endswith("_stop"):
        return False

    try:

        count = int(
            warning.split("_")[-1]
        )

    except Exception:

        return False

    return count in (0, 1)


# =========================================================
# 경고 HTML
# =========================================================

def warning_html(warning):

    if not is_visible_breakout_warning(
        warning
    ):

        return (
            '<span class="warning-empty">—</span>'
        )

    try:

        count = int(
            warning.split("_")[-1]
        )

    except Exception:

        return (
            '<span class="warning-empty">—</span>'
        )

    if count == 0:

        return (
            '<span class="warning-icon pre-breakout">'
            '🚨'
            '</span>'
        )

    if count == 1:

        return (
            '<span class="warning-icon rocket">'
            '🚀(1)'
            '</span>'
        )

    return (
        '<span class="warning-empty">—</span>'
    )


# =========================================================
# 빈 데이터
# =========================================================

def empty_ema():

    return {
        "direction": "none",
        "count": 0,
        "state_1h": {
            "direction": "none",
            "count": 0
        },
        "state_4h": {
            "direction": "none",
            "count": 0
        },
        "warning_1h": "none",
        "warning_4h": "none",
        "signal": ""
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

        return empty_ema()

    state_1h = get_ema_state(
        df1h
    )

    state_4h = get_ema_state(
        df4h
    )

    warning_1h = get_breakout_state(
        df1h,
        state_1h["direction"],
        f"OKX_1H_{inst_id}"
    )

    warning_4h = get_breakout_state(
        df4h,
        state_4h["direction"],
        f"OKX_4H_{inst_id}"
    )

    # 4H 돌파를 메인 경고로 사용
    signal = ""

    if warning_4h.startswith(
        "long_breakout_"
    ):
        signal = "LONG"

    elif warning_4h.startswith(
        "short_breakout_"
    ):
        signal = "SHORT"

    return {
        "direction": state_4h["direction"],
        "count": state_4h["count"],

        "state_1h": state_1h,
        "state_4h": state_4h,

        "warning_1h": warning_1h,
        "warning_4h": warning_4h,

        "signal": signal
    }


# =========================================================
# 업비트 EMA
#
# 1H / 4H 모두 현재 진행 중 캔들 제외
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

        return empty_ema()

    df1h = raw1h.copy()
    df4h = raw4h.copy()

    # 현재 진행 중 캔들 제외
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

    state_1h = get_ema_state(
        df1h
    )

    state_4h = get_ema_state(
        df4h
    )

    warning_1h = get_breakout_state(
        df1h,
        state_1h["direction"],
        f"UPBIT_1H_{market}"
    )

    warning_4h = get_breakout_state(
        df4h,
        state_4h["direction"],
        f"UPBIT_4H_{market}"
    )

    signal = ""

    if warning_4h.startswith(
        "long_breakout_"
    ):
        signal = "LONG"

    elif warning_4h.startswith(
        "short_breakout_"
    ):
        signal = "SHORT"

    return {
        "direction": state_4h["direction"],
        "count": state_4h["count"],

        "state_1h": state_1h,
        "state_4h": state_4h,

        "warning_1h": warning_1h,
        "warning_4h": warning_4h,

        "signal": signal
    }


# =========================================================
# 업비트 변동률
# =========================================================

def get_upbit_change(market):

    df = get_upbit_ohlcv(
        market,
        60,
        120
    )

    if df is None or len(df) < 50:
        return None

    try:

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

            result.append(
                round(
                    (
                        (
                            daily.iloc[i]
                            -
                            daily.iloc[i - 1]
                        )
                        /
                        daily.iloc[i - 1]
                        *
                        100
                    ),
                    2
                )
            )

        return result

    except Exception:

        return None


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

    try:

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

            result.append(
                round(
                    (
                        (
                            daily.iloc[i]
                            -
                            daily.iloc[i - 1]
                        )
                        /
                        daily.iloc[i - 1]
                        *
                        100
                    ),
                    2
                )
            )

        return result

    except Exception:

        return None


# =========================================================
# 변동률 HTML
# =========================================================

def format_change(changes):

    if not changes:
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
        f'<span>{icon}</span>'
        f'<span>{sign}{x:.2f}%</span>'
        '</span>'
    )


# =========================================================
# 방향 HTML
# =========================================================

def direction_html(
    direction,
    change
):

    if (
        direction == "long"
        and
        change is not None
        and
        change > 0
    ):

        return (
            '<span class="direction-long">☀️</span>'
        )

    if (
        direction == "short"
        and
        change is not None
        and
        change < 0
    ):

        return (
            '<span class="direction-short">🌧</span>'
        )

    return (
        '<span class="direction-none">—</span>'
    )


# =========================================================
# LONG / SHORT
# =========================================================

def signal_html(
    signal,
    change
):

    if (
        signal == "LONG"
        and
        change is not None
        and
        change > 0
    ):

        return (
            '<span class="signal-text long-text">'
            'LONG'
            '</span>'
        )

    if (
        signal == "SHORT"
        and
        change is not None
        and
        change < 0
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
                len(df) < hours
            ):

                time.sleep(
                    OKX_RETRY_DELAY
                )

                continue

            volume_usdt = float(
                df["volCcyQuote"]
                .tail(hours)
                .sum()
            )

            # OKX 값 보정
            volume_usdt /= 10

            volume_krw = (
                volume_usdt
                *
                usdt_krw
            )

            if volume_krw > 0:
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
# OKX 목록
# =========================================================

def get_all_okx_swap_symbols():

    response = retry_request(
        requests.get,
        "https://www.okx.com/api/v5/public/instruments?instType=SWAP",
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

    except Exception:

        return []


# =========================================================
# 업비트 업데이트
#
# 경고 리스트:
# 🚨
# 🚀(1)
# 만 저장
# =========================================================

def update_upbit():

    global latest_upbit_data

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

        try:

            changes = get_upbit_change(
                market
            )

            ema = get_upbit_ema(
                market
            )

            warning = ema.get(
                "warning_4h",
                "none"
            )

            # 🚨 / 🚀(1)만 저장
            if not is_visible_breakout_warning(
                warning
            ):
                continue

            change = (
                changes[0]
                if changes
                else None
            )

            rows.append(
                {
                    "rank": rank,
                    "name": market.replace(
                        "KRW-",
                        ""
                    ),
                    "change": format_change(
                        changes
                    ),
                    "change_percent": change,
                    "volume": format_volume(
                        volume_map[market]
                    ),
                    "ema": ema
                }
            )

        except Exception as e:

            logging.error(
                f"업비트 상세 오류 "
                f"{market} : {e}"
            )

    latest_upbit_data = rows

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

    volume_map = {}

    for symbol in symbols:

        try:

            volume = get_okx_volume(
                symbol,
                usdt_krw
            )

            if volume > 0:

                volume_map[
                    symbol
                ] = volume

        except Exception as e:

            logging.error(
                f"OKX 거래대금 오류 "
                f"{symbol} : {e}"
            )

    if not volume_map:
        return False

    top_symbols = sorted(
        volume_map,
        key=volume_map.get,
        reverse=True
    )[:TOP_N]

    upbit_markets = get_upbit_markets()

    upbit_coin_set = {
        x.replace("KRW-", "")
        for x in upbit_markets
    }

    rows = []

    for rank, symbol in enumerate(
        top_symbols,
        start=1
    ):

        try:

            coin = symbol.replace(
                "-USDT-SWAP",
                ""
            )

            if coin in upbit_coin_set:
                coin += "[UP]"

            changes = get_okx_change(
                symbol
            )

            ema = get_okx_ema(
                symbol
            )

            warning = ema.get(
                "warning_4h",
                "none"
            )

            # 🚨 / 🚀(1)만
            if not is_visible_breakout_warning(
                warning
            ):
                continue

            change = (
                changes[0]
                if changes
                else None
            )

            rows.append(
                {
                    "rank": rank,
                    "name": coin,
                    "change": format_change(
                        changes
                    ),
                    "change_percent": change,
                    "volume": format_volume(
                        volume_map[symbol]
                    ),
                    "ema": ema
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
        "========== 전체 조회 시작 =========="
    )

    if USE_UPBIT == "Y":

        try:

            update_upbit()

        except Exception as e:

            logging.exception(
                f"업비트 오류 : {e}"
            )

    else:

        latest_upbit_data = []

    if USE_OKX == "Y":

        try:

            usdt = get_usdt_krw()

            if usdt is not None:
                latest_usdt_krw = usdt

            update_okx(
                latest_usdt_krw
            )

        except Exception as e:

            logging.exception(
                f"OKX 오류 : {e}"
            )

    else:

        latest_okx_data = []

    logging.info(
        "========== 전체 조회 완료 =========="
    )


# =========================================================
# 테이블 행
# =========================================================

def make_table_rows(data):

    rows = ""

    for item in data:

        ema = item.get(
            "ema",
            empty_ema()
        )

        state1 = ema.get(
            "state_1h",
            {
                "direction": "none",
                "count": 0
            }
        )

        state4 = ema.get(
            "state_4h",
            {
                "direction": "none",
                "count": 0
            }
        )

        warning = ema.get(
            "warning_4h",
            "none"
        )

        row_class = ""

        try:

            count = int(
                warning.split("_")[-1]
            )

            if count == 0:
                row_class = "pre-breakout-row"

            elif count == 1:
                row_class = "breakout-row"

        except Exception:
            pass

        rows += f"""
<tr class="{row_class}">

<td>{item["rank"]}</td>

<td>
<div class="coin-wrap">
<span class="coin">
{item["name"]}
</span>

{direction_html(
    state4.get("direction", "none"),
    item.get("change_percent")
)}

</div>
</td>

<td>
<div class="volume-wrap">

<span class="volume-value">
{item["volume"]}
</span>

{signal_html(
    ema.get("signal", ""),
    item.get("change_percent")
)}

</div>
</td>

<td>
<div class="today-wrap">

{item["change"]}

<div class="breakout-wrap">

{warning_html(warning)}

</div>

</div>
</td>

<td>

{combined_ema_html(
    state1,
    state4
)}

</td>

</tr>
"""

    return rows


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
style="color:#555;padding:14px 4px;">
현재 🚨 / 🚀(1) 종목 없음
</td>
</tr>
"""

    return f"""
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

<div class="note">

※ TOP{TOP_N} 실제 거래대금 순위<br>
※ 경고 리스트에는 🚨 돌파 전 / 🚀(1) 최초 돌파만 표시<br>
※ 🚀(2), 🚀(3) 이후 추가 갱신은 경고 리스트에서 제거<br>
※ 1H | 4H 순서로 EMA 상태 표시<br>
※ 🟢(N) = 정배열 유지 캔들 수<br>
※ 🔴(N) = 역배열 유지 캔들 수<br>
※ ⚪ = 정렬 없음 또는 데이터 부족<br>
※ 4H 돌파는 최근 {BREAKOUT_LOOKBACK}개 확정 캔들 기준<br>
※ 업비트 1H / 4H는 진행 중 캔들을 제외<br>
※ 🚨 = 돌파 전 테스트 상태<br>
※ 🚀(1) = 최초 확정 돌파<br>
※ 추가 고점/저점 갱신은 표시하지 않음

</div>
"""


# =========================================================
# CSS
# =========================================================

DASHBOARD_CSS = """

* {
    box-sizing:border-box;
}

html,
body {
    margin:0;
    padding:0;
    width:100%;
    overflow-x:hidden;
}

body {
    background:#0f1115;
    color:#eeeeee;
    font-family:Arial,sans-serif;
    font-size:10px;
    padding:6px;
}

h1 {
    margin:3px 2px 6px;
    font-size:15px;
}

h2 {
    margin:12px 2px 6px;
    font-size:12px;
}

.info {
    margin:0 2px 7px;
    padding:6px 7px;
    color:#8b9099;
    background:#171a1f;
    border:1px solid #252a31;
    border-radius:8px;
    font-size:8px;
    line-height:1.5;
}

.exchange-status {
    display:flex;
    gap:8px;
    margin-top:5px;
    font-size:8px;
    font-weight:bold;
}

.status-y {
    color:#35e66d;
}

.status-n {
    color:#ff4d4d;
}

.table-wrap {
    width:100%;
    overflow:hidden;
    border-radius:9px;
    border:1px solid #252a31;
}

table {
    width:100%;
    table-layout:fixed;
    border-collapse:collapse;
    background:#181c21;
}

th {
    padding:6px 2px;
    background:#12151a;
    border-bottom:1px solid #2b3037;
    color:#8f949d;
    font-size:8px;
    text-align:center;
}

td {
    padding:6px 2px;
    border-bottom:1px solid #272c32;
    text-align:center;
    vertical-align:middle;
    overflow:hidden;
    word-break:break-word;
}

th:nth-child(1),
td:nth-child(1) {
    width:8%;
}

th:nth-child(2),
td:nth-child(2) {
    width:20%;
}

th:nth-child(3),
td:nth-child(3) {
    width:17%;
}

th:nth-child(4),
td:nth-child(4) {
    width:25%;
}

th:nth-child(5),
td:nth-child(5) {
    width:30%;
}


/* ================================================
   🚨
================================================ */

.pre-breakout-row {
    animation:preFlash .75s ease-in-out infinite;
}

@keyframes preFlash {

    0% {
        background:#181c21;
    }

    50% {
        background:#49341d;
    }

    100% {
        background:#181c21;
    }
}


/* ================================================
   🚀
================================================ */

.breakout-row {
    animation:breakFlash .9s ease-in-out infinite;
}

@keyframes breakFlash {

    0% {
        background:#181c21;
    }

    50% {
        background:#203b29;
    }

    100% {
        background:#181c21;
    }
}


.coin-wrap,
.volume-wrap,
.today-wrap,
.breakout-wrap {
    display:flex;
    flex-direction:column;
    align-items:center;
    justify-content:center;
    gap:3px;
    width:100%;
}

.coin {
    font-weight:bold;
    font-size:9px;
}

.direction-long,
.direction-short,
.direction-none {
    font-size:9px;
}

.volume-value {
    font-size:8px;
    font-weight:600;
}

.signal-text {
    font-weight:bold;
    font-size:8px;
}

.long-text {
    color:#35e66d;
}

.short-text {
    color:#ff4d4d;
}

.signal-none {
    color:#555;
    font-size:8px;
}

.change-item {
    display:inline-flex;
    gap:2px;
    align-items:center;
    justify-content:center;
    font-size:8px;
}

.warning-empty {
    color:#555;
}

.warning-icon {
    white-space:nowrap;
}

.pre-breakout {
    font-size:12px;
    filter:drop-shadow(
        0 0 5px rgba(255,180,0,.8)
    );
}

.rocket {
    font-size:11px;
    font-weight:bold;
    filter:drop-shadow(
        0 0 5px rgba(50,255,100,.8)
    );
}


/* ================================================
   EMA
================================================ */

.ema-combined {
    display:flex;
    align-items:center;
    justify-content:center;
    width:100%;
    white-space:nowrap;
    font-size:7px;
    font-weight:bold;
}

.ema-separator {
    color:#777;
    margin:0 2px;
}

.note {
    color:#666;
    font-size:7px;
    line-height:1.5;
    margin:5px 2px 8px;
}


@media (max-width:480px) {

    body {
        padding:4px;
        font-size:9px;
    }

    h1 {
        font-size:14px;
    }

    h2 {
        font-size:12px;
    }

    .info {
        font-size:7px;
    }

    th {
        font-size:7px;
        padding:5px 1px;
    }

    td {
        padding:5px 1px;
    }

    .coin {
        font-size:8px;
    }

    .volume-value,
    .signal-text,
    .signal-none,
    .change-item {
        font-size:7px;
    }

    .ema-combined {
        font-size:6px;
    }

    .rocket {
        font-size:9px;
    }

    .pre-breakout {
        font-size:11px;
    }

    .direction-long,
    .direction-short,
    .direction-none {
        font-size:8px;
    }

    .note {
        font-size:6px;
    }
}

"""


# =========================================================
# 대시보드
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def dashboard():

    upbit_class = (
        "status-y"
        if USE_UPBIT == "Y"
        else "status-n"
    )

    okx_class = (
        "status-y"
        if USE_OKX == "Y"
        else "status-n"
    )

    sections = ""

    if USE_UPBIT == "Y":

        sections += make_exchange_section(
            "🏆 업비트",
            latest_upbit_data
        )

    if USE_OKX == "Y":

        sections += make_exchange_section(
            "🏆 OKX",
            latest_okx_data
        )

    return f"""
<!DOCTYPE html>

<html lang="ko">

<head>

<meta charset="UTF-8">

<meta http-equiv="refresh"
      content="60">

<meta name="viewport"
      content="width=device-width,
               initial-scale=1.0,
               maximum-scale=1.0,
               user-scalable=no">

<title>Breakout Trading</title>

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
1H 상태 | 4H 상태 · 4H 돌파
</div>

<div>
업비트 24H 거래대금 · OKX 1H × {VOLUME_HOURS}
</div>

<div>
TOP{TOP_N} · 🚨 돌파 전 · 🚀(1) 최초 돌파
</div>

<div class="exchange-status">

<span class="{upbit_class}">
업비트 : {USE_UPBIT}
</span>

<span class="{okx_class}">
OKX : {USE_OKX}
</span>

</div>

</div>

{sections}

</body>

</html>
"""


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
# 시작
# =========================================================

@app.on_event("startup")
def startup():

    logging.info(
        "서버 시작"
    )

    logging.info(
        f"업비트={USE_UPBIT}, "
        f"OKX={USE_OKX}, "
        f"TOP_N={TOP_N}, "
        f"UPDATE={UPDATE_MINUTES}분"
    )

    if USE_UPBIT not in ("Y", "N"):

        raise ValueError(
            "USE_UPBIT은 Y 또는 N"
        )

    if USE_OKX not in ("Y", "N"):

        raise ValueError(
            "USE_OKX는 Y 또는 N"
        )

    # 최초 즉시 실행
    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    # 1분마다 조회
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
