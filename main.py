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

# 1분마다 현재 상태 확인
UPDATE_MINUTES = 1

# 최근 스윙 추적에 사용할 최소 캔들
MIN_CANDLES = 60

# 돌파 기준을 찾기 위한 최대 과거 캔들
SWING_LOOKBACK = 30


# =========================================================
# 거래소
# =========================================================

USE_UPBIT = "Y"

USE_OKX = "N"


# =========================================================
# API 안정화
# =========================================================

REQUEST_INTERVAL = 0.08

RATE_LIMIT_WAIT = 3

MAX_RETRIES = 10


# =========================================================
# OKX 실패 종목
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
# 돌파 상태 저장
#
# 종목별로 계속 유지
#
# state 예:
#
# direction
# 정배열 방향
#
# peak_high
# 최근 확인된 상승 고점
#
# peak_low
# 최근 확인된 하락 저점
#
# pullback
# 조정 발생 여부
#
# armed
# 돌파 감시 활성화 여부
#
# last_alert_candle
# 같은 1H 캔들에서 중복 경고 방지
# =========================================================

breakout_states = {}

breakout_lock = threading.Lock()


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
# OKX 캔들
#
# 현재 진행 캔들도 가져오는 함수
# =========================================================

def get_okx_ohlcv_raw(
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

        df["ts"] = pd.to_numeric(
            df["ts"],
            errors="coerce"
        )

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
# OKX 확정 캔들
# =========================================================

def get_okx_ohlcv(
    inst_id,
    bar="1H",
    limit=200
):

    df = get_okx_ohlcv_raw(
        inst_id,
        bar,
        limit
    )

    if df is None:
        return None

    df = df[
        df["confirm"].astype(str) == "1"
    ]

    if df.empty:
        return None

    return (
        df
        .reset_index(drop=True)
    )


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

        df["timestamp"] = pd.to_datetime(
            df["candle_date_time_kst"],
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
# 업비트 거래대금
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

                        volume = float(
                            volume
                        )

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
        or
        column not in df.columns
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
# 30-60-120 정배열
# =========================================================

def get_ema_30_60_120_direction(
    df
):

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

    ema120 = get_ema(
        df,
        "c",
        120
    )

    if (
        ema30 is None
        or
        ema60 is None
        or
        ema120 is None
    ):

        return "none"

    a = ema30.iloc[-1]
    b = ema60.iloc[-1]
    c = ema120.iloc[-1]

    if (
        pd.isna(a)
        or
        pd.isna(b)
        or
        pd.isna(c)
    ):

        return "none"

    if a > b > c:

        return "long"

    if a < b < c:

        return "short"

    return "none"


# =========================================================
# EMA 표시
# =========================================================

def check_ema(df):

    if (
        df is None
        or
        len(df) < 30
    ):

        return {
            "display": "⚪",
            "direction": "none",
            "count": 0
        }

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

    ema120 = get_ema(
        df,
        "c",
        120
    )

    if ema30 is None:

        return {
            "display": "⚪",
            "direction": "none",
            "count": 0
        }

    if (
        ema60 is not None
        and
        ema120 is not None
        and
        not pd.isna(ema30.iloc[-1])
        and
        not pd.isna(ema60.iloc[-1])
        and
        not pd.isna(ema120.iloc[-1])
    ):

        a = ema30.iloc[-1]
        b = ema60.iloc[-1]
        c = ema120.iloc[-1]

        if a > b > c:

            return {
                "display": "🟢 LONG",
                "direction": "long",
                "count": 0
            }

        if a < b < c:

            return {
                "display": "🔴 SHORT",
                "direction": "short",
                "count": 0
            }

    if (
        ema60 is not None
        and
        not pd.isna(ema30.iloc[-1])
        and
        not pd.isna(ema60.iloc[-1])
    ):

        a = ema30.iloc[-1]
        b = ema60.iloc[-1]

        if a > b:

            return {
                "display": "🟢 LONG",
                "direction": "long",
                "count": 0
            }

        if a < b:

            return {
                "display": "🔴 SHORT",
                "direction": "short",
                "count": 0
            }

    return {
        "display": "⚪",
        "direction": "none",
        "count": 0
    }


# =========================================================
# 당일 양수 / 음수
#
# KST 09:00 기준
# =========================================================

def get_daily_state_from_1h(
    df
):

    if (
        df is None
        or
        len(df) < 3
    ):

        return {
            "state": "none",
            "change": None
        }

    work = df.copy()

    if "timestamp" not in work.columns:

        if "ts" in work.columns:

            work["timestamp"] = (
                pd.to_datetime(
                    work["ts"],
                    unit="ms"
                )
                +
                pd.Timedelta(hours=9)
            )

        else:

            return {
                "state": "none",
                "change": None
            }

    work["timestamp"] = pd.to_datetime(
        work["timestamp"],
        errors="coerce"
    )

    work = work.dropna(
        subset=["timestamp"]
    )

    if work.empty:

        return {
            "state": "none",
            "change": None
        }

    work = work.sort_values(
        "timestamp"
    )

    work["date"] = (
        work["timestamp"]
        .dt.normalize()
    )

    # KST 09시 기준 일봉
    work["session"] = (
        work["timestamp"]
        -
        pd.Timedelta(hours=9)
    ).dt.date

    grouped = (
        work
        .groupby("session")
    )

    sessions = list(
        grouped.groups.keys()
    )

    if len(sessions) < 2:

        return {
            "state": "none",
            "change": None
        }

    current_session = sessions[-1]

    previous_session = sessions[-2]

    current_df = grouped.get_group(
        current_session
    )

    previous_df = grouped.get_group(
        previous_session
    )

    previous_close = float(
        previous_df["c"].iloc[-1]
    )

    current_close = float(
        current_df["c"].iloc[-1]
    )

    if previous_close <= 0:

        return {
            "state": "none",
            "change": None
        }

    change = (
        (
            current_close
            -
            previous_close
        )
        /
        previous_close
        *
        100
    )

    if change > 0:

        state = "long"

    elif change < 0:

        state = "short"

    else:

        state = "flat"

    return {
        "state": state,
        "change": round(
            change,
            2
        )
    }


# =========================================================
# 스윙 고점 / 저점
#
# 단순 최근 최고가가 아니라
# 상승 → 조정 → 재상승 구조를 추적
# =========================================================

def find_recent_swing_high(
    df
):

    if (
        df is None
        or
        len(df) < 7
    ):

        return None

    start = max(
        2,
        len(df) - SWING_LOOKBACK
    )

    end = len(df) - 2

    candidates = []

    for i in range(
        start,
        end + 1
    ):

        high = float(
            df["h"].iloc[i]
        )

        left1 = float(
            df["h"].iloc[i - 1]
        )

        left2 = float(
            df["h"].iloc[i - 2]
        )

        right1 = float(
            df["h"].iloc[i + 1]
        )

        right2 = float(
            df["h"].iloc[i + 2]
        )

        if (
            high >= left1
            and
            high >= left2
            and
            high >= right1
            and
            high >= right2
        ):

            candidates.append(
                (
                    i,
                    high
                )
            )

    if not candidates:

        return None

    return candidates[-1]


# =========================================================
# 스윙 저점
# =========================================================

def find_recent_swing_low(
    df
):

    if (
        df is None
        or
        len(df) < 7
    ):

        return None

    start = max(
        2,
        len(df) - SWING_LOOKBACK
    )

    end = len(df) - 2

    candidates = []

    for i in range(
        start,
        end + 1
    ):

        low = float(
            df["l"].iloc[i]
        )

        left1 = float(
            df["l"].iloc[i - 1]
        )

        left2 = float(
            df["l"].iloc[i - 2]
        )

        right1 = float(
            df["l"].iloc[i + 1]
        )

        right2 = float(
            df["l"].iloc[i + 2]
        )

        if (
            low <= left1
            and
            low <= left2
            and
            low <= right1
            and
            low <= right2
        ):

            candidates.append(
                (
                    i,
                    low
                )
            )

    if not candidates:

        return None

    return candidates[-1]


# =========================================================
# LONG 돌파 상태
#
# 현재 진행 캔들에서만 즉시 돌파 감지
#
# 핵심:
# 이미 과거에 돌파한 종목은
# 현재 시점에서 소급하여 경고하지 않음
# =========================================================

def track_long_breakout(
    key,
    df_confirmed,
    current
):

    now_candle_id = (
        str(
            current.get(
                "timestamp",
                ""
            )
        )
    )

    with breakout_lock:

        state = breakout_states.get(
            key
        )

        if state is None:

            state = {
                "direction": "long",
                "peak_high": None,
                "peak_index": None,
                "pullback": False,
                "armed": False,
                "last_alert_candle": None
            }

            breakout_states[key] = state

        # -------------------------------------------------
        # 과거 확정 캔들에서 최근 스윙 고점 확인
        # -------------------------------------------------

        swing = find_recent_swing_high(
            df_confirmed
        )

        if swing is not None:

            swing_index, swing_high = swing

            if (
                state["peak_index"] is None
                or
                swing_index > state["peak_index"]
            ):

                state["peak_index"] = (
                    swing_index
                )

                state["peak_high"] = (
                    swing_high
                )

                state["pullback"] = False

                state["armed"] = True

        peak_high = state.get(
            "peak_high"
        )

        if peak_high is None:

            return False

        current_low = float(
            current["l"]
        )

        current_high = float(
            current["h"]
        )

        current_open = float(
            current["o"]
        )

        current_close = float(
            current["c"]
        )

        # -------------------------------------------------
        # 고점 이후 조정 확인
        # -------------------------------------------------

        if current_low < peak_high:

            state["pullback"] = True

        # -------------------------------------------------
        # 현재 진행 캔들 양봉 + 기존 고점 돌파
        #
        # 반드시 조정 이후 다시 돌파해야 함
        # -------------------------------------------------

        if (
            state["armed"]
            and
            state["pullback"]
            and
            current_close > current_open
            and
            current_high > peak_high
        ):

            # 같은 1H 캔들에서 중복 알람 방지
            if (
                state["last_alert_candle"]
                != now_candle_id
            ):

                state["last_alert_candle"] = (
                    now_candle_id
                )

                # 이번 고점을 새로운 기준으로 변경
                state["peak_high"] = (
                    current_high
                )

                state["pullback"] = False

                return True

        return False


# =========================================================
# SHORT 돌파 상태
# =========================================================

def track_short_breakout(
    key,
    df_confirmed,
    current
):

    now_candle_id = (
        str(
            current.get(
                "timestamp",
                ""
            )
        )
    )

    with breakout_lock:

        state = breakout_states.get(
            key
        )

        if state is None:

            state = {
                "direction": "short",
                "peak_low": None,
                "peak_index": None,
                "pullback": False,
                "armed": False,
                "last_alert_candle": None
            }

            breakout_states[key] = state

        # -------------------------------------------------
        # 최근 스윙 저점
        # -------------------------------------------------

        swing = find_recent_swing_low(
            df_confirmed
        )

        if swing is not None:

            swing_index, swing_low = swing

            if (
                state["peak_index"] is None
                or
                swing_index > state["peak_index"]
            ):

                state["peak_index"] = (
                    swing_index
                )

                state["peak_low"] = (
                    swing_low
                )

                state["pullback"] = False

                state["armed"] = True

        peak_low = state.get(
            "peak_low"
        )

        if peak_low is None:

            return False

        current_low = float(
            current["l"]
        )

        current_high = float(
            current["h"]
        )

        current_open = float(
            current["o"]
        )

        current_close = float(
            current["c"]
        )

        # -------------------------------------------------
        # 저점 이후 반등
        # -------------------------------------------------

        if current_high > peak_low:

            state["pullback"] = True

        # -------------------------------------------------
        # 음봉 + 기존 저점 하향 돌파
        # -------------------------------------------------

        if (
            state["armed"]
            and
            state["pullback"]
            and
            current_close < current_open
            and
            current_low < peak_low
        ):

            if (
                state["last_alert_candle"]
                != now_candle_id
            ):

                state["last_alert_candle"] = (
                    now_candle_id
                )

                state["peak_low"] = (
                    current_low
                )

                state["pullback"] = False

                return True

        return False


# =========================================================
# 1H 현재 돌파 감시
#
# 반환:
# none
# long
# short
# =========================================================

def get_1h_breakout_signal(
    key,
    confirmed_df,
    current,
    ema_direction,
    allow_short=True
):

    if (
        confirmed_df is None
        or
        current is None
    ):

        return "none"

    if len(confirmed_df) < MIN_CANDLES:

        return "none"

    # -----------------------------------------------------
    # 정배열이 깨지면 추적 상태 완전 초기화
    # -----------------------------------------------------

    if ema_direction not in (
        "long",
        "short"
    ):

        with breakout_lock:

            breakout_states.pop(
                key,
                None
            )

        return "none"

    # -----------------------------------------------------
    # LONG
    # -----------------------------------------------------

    if ema_direction == "long":

        signal = track_long_breakout(
            key,
            confirmed_df,
            current
        )

        if signal:

            return "long"

        return "none"

    # -----------------------------------------------------
    # SHORT
    # -----------------------------------------------------

    if (
        ema_direction == "short"
        and
        allow_short
    ):

        signal = track_short_breakout(
            key,
            confirmed_df,
            current
        )

        if signal:

            return "short"

    return "none"


# =========================================================
# 업비트 1H EMA + 돌파
# =========================================================

def get_upbit_signal(
    market
):

    raw = get_upbit_ohlcv(
        market,
        60,
        200
    )

    if raw is None or len(raw) < 10:

        return {
            "ema": {
                "display": "⚪",
                "direction": "none"
            },
            "daily": {
                "state": "none",
                "change": None
            },
            "signal": "none"
        }

    # -----------------------------------------------------
    # 마지막 캔들은 현재 진행 중인 1H 캔들
    # -----------------------------------------------------

    current = raw.iloc[-1].copy()

    confirmed = (
        raw
        .iloc[:-1]
        .copy()
        .reset_index(drop=True)
    )

    if len(confirmed) < MIN_CANDLES:

        return {
            "ema": check_ema(confirmed),
            "daily": get_daily_state_from_1h(raw),
            "signal": "none"
        }

    # -----------------------------------------------------
    # EMA는 확정 캔들 기준
    # -----------------------------------------------------

    ema = check_ema(
        confirmed
    )

    ema_direction = (
        get_ema_30_60_120_direction(
            confirmed
        )
    )

    # -----------------------------------------------------
    # 현재 캔들에 timestamp 추가
    # -----------------------------------------------------

    current_dict = current.to_dict()

    if "timestamp" not in current_dict:

        current_dict["timestamp"] = (
            current_dict.get(
                "candle_date_time_kst",
                ""
            )
        )

    # -----------------------------------------------------
    # 업비트는 LONG만
    # -----------------------------------------------------

    signal = get_1h_breakout_signal(
        f"UPBIT:{market}",
        confirmed,
        current_dict,
        ema_direction,
        allow_short=False
    )

    return {
        "ema": ema,
        "daily": get_daily_state_from_1h(raw),
        "signal": signal
    }


# =========================================================
# OKX 1H EMA + 돌파
# =========================================================

def get_okx_signal(
    inst_id
):

    raw = get_okx_ohlcv_raw(
        inst_id,
        "1H",
        200
    )

    if raw is None or len(raw) < 10:

        return {
            "ema": {
                "display": "⚪",
                "direction": "none"
            },
            "daily": {
                "state": "none",
                "change": None
            },
            "signal": "none"
        }

    # -----------------------------------------------------
    # 현재 진행 캔들
    # -----------------------------------------------------

    current = raw.iloc[-1].copy()

    # -----------------------------------------------------
    # 확정 캔들
    # -----------------------------------------------------

    confirmed = raw[
        raw["confirm"].astype(str) == "1"
    ].copy()

    confirmed = (
        confirmed
        .reset_index(drop=True)
    )

    if len(confirmed) < MIN_CANDLES:

        return {
            "ema": check_ema(confirmed),
            "daily": get_daily_state_from_1h(raw),
            "signal": "none"
        }

    ema = check_ema(
        confirmed
    )

    ema_direction = (
        get_ema_30_60_120_direction(
            confirmed
        )
    )

    current_dict = current.to_dict()

    current_dict["timestamp"] = (
        pd.to_datetime(
            current_dict["ts"],
            unit="ms"
        )
        +
        pd.Timedelta(hours=9)
    )

    signal = get_1h_breakout_signal(
        f"OKX:{inst_id}",
        confirmed,
        current_dict,
        ema_direction,
        allow_short=True
    )

    return {
        "ema": ema,
        "daily": get_daily_state_from_1h(raw),
        "signal": signal
    }


# =========================================================
# 변동률 HTML
# =========================================================

def format_change(
    daily
):

    if not daily:

        return (
            '<span class="change-item">'
            '⬜ N/A'
            '</span>'
        )

    value = daily.get(
        "change"
    )

    state = daily.get(
        "state"
    )

    if value is None:

        return (
            '<span class="change-item">'
            '⬜ N/A'
            '</span>'
        )

    if state == "long":

        return (
            '<span class="change-positive">'
            f'☀️ +{value:.2f}%'
            '</span>'
        )

    if state == "short":

        return (
            '<span class="change-negative">'
            f'☁️ {value:.2f}%'
            '</span>'
        )

    return (
        '<span class="change-flat">'
        f'⬜ {value:.2f}%'
        '</span>'
    )


# =========================================================
# LONG / SHORT HTML
# =========================================================

def direction_html(
    signal,
    daily_state
):

    if signal == "long":

        return (
            '<span class="direction-long">'
            'LONG'
            '</span>'
        )

    if signal == "short":

        return (
            '<span class="direction-short">'
            'SHORT'
            '</span>'
        )

    # 현재 돌파가 없더라도
    # 당일 방향 표시
    if daily_state == "long":

        return (
            '<span class="trend-long">'
            'LONG'
            '</span>'
        )

    if daily_state == "short":

        return (
            '<span class="trend-short">'
            'SHORT'
            '</span>'
        )

    return (
        '<span class="trend-flat">'
        '-'
        '</span>'
    )


# =========================================================
# 경고 HTML
#
# 🚨만 표시
# =========================================================

def warning_html(
    signal
):

    if signal == "long":

        return (
            '<span class="alarm-long">'
            '🚨'
            '</span>'
        )

    if signal == "short":

        return (
            '<span class="alarm-short">'
            '🚨'
            '</span>'
        )

    return ""


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

            result = get_upbit_signal(
                market
            )

            daily = result[
                "daily"
            ]

            signal = result[
                "signal"
            ]

            # -------------------------------------------------
            # 업비트는 LONG 돌파만 표시
            # -------------------------------------------------

            if signal != "long":

                continue

            rows.append(
                {
                    "rank": rank,

                    "name": coin,

                    "volume":
                        format_volume(
                            volume_map[
                                market
                            ]
                        ),

                    "change":
                        format_change(
                            daily
                        ),

                    "direction":
                        direction_html(
                            signal,
                            daily.get(
                                "state",
                                "none"
                            )
                        ),

                    "warning":
                        warning_html(
                            signal
                        ),

                    "ema":
                        result[
                            "ema"
                        ],

                    "signal":
                        signal
                }
            )

        except Exception as e:

            logging.error(
                f"업비트 상세 오류 "
                f"{market} : {e}"
            )

    latest_upbit_data = rows

    logging.info(
        f"업비트 LONG 돌파 종목 "
        f"{len(rows)}개"
    )

    logging.info(
        f"========== 업비트 TOP{TOP_N} 완료 =========="
    )

    return True


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

    for symbol in symbols:

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

            result = get_okx_signal(
                symbol
            )

            daily = result[
                "daily"
            ]

            signal = result[
                "signal"
            ]

            # OKX는 LONG / SHORT 모두 표시
            if signal not in (
                "long",
                "short"
            ):

                continue

            rows.append(
                {
                    "rank": rank,

                    "name": coin,

                    "volume":
                        format_volume(
                            volume_map[
                                symbol
                            ]
                        ),

                    "change":
                        format_change(
                            daily
                        ),

                    "direction":
                        direction_html(
                            signal,
                            daily.get(
                                "state",
                                "none"
                            )
                        ),

                    "warning":
                        warning_html(
                            signal
                        ),

                    "ema":
                        result[
                            "ema"
                        ],

                    "signal":
                        signal
                }
            )

        except Exception as e:

            logging.error(
                f"OKX 상세 오류 "
                f"{symbol} : {e}"
            )

    latest_okx_data = rows

    logging.info(
        f"OKX 돌파 종목 "
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
        "1분 현재상태 조회 시작"
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

                latest_usdt_krw = (
                    usdt_krw
                )

            else:

                usdt_krw = (
                    latest_usdt_krw
                )

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
    font-size: 10px;
    padding: 5px;
}

h1 {

    margin: 3px 2px 6px 2px;
    font-size: 14px;
}

h2 {

    margin: 10px 2px 5px 2px;
    font-size: 12px;
}

.info {

    margin: 0 2px 6px 2px;
    padding: 5px 6px;
    color: #8b9099;
    background: #171a1f;
    border: 1px solid #252a31;
    border-radius: 7px;
    font-size: 8px;
    line-height: 1.5;
}

.exchange-status {

    display: flex;
    gap: 8px;
    margin-top: 4px;
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

    padding: 5px 2px;
    background: #12151a;
    border-bottom: 1px solid #2b3037;
    color: #8f949d;
    font-size: 8px;
    text-align: center;
}

td {

    padding: 5px 2px;
    border-bottom: 1px solid #272c32;
    text-align: center;
    vertical-align: middle;
}


/* =====================================================
   컬럼
   ===================================================== */

th:nth-child(1),
td:nth-child(1) {

    width: 7%;
}

th:nth-child(2),
td:nth-child(2) {

    width: 20%;
}

th:nth-child(3),
td:nth-child(3) {

    width: 20%;
}

th:nth-child(4),
td:nth-child(4) {

    width: 28%;
}

th:nth-child(5),
td:nth-child(5) {

    width: 25%;
}


/* =====================================================
   코인
   ===================================================== */

.coin {

    display: block;
    font-size: 9px;
    font-weight: 700;
    line-height: 1.2;
}


/* =====================================================
   거래대금
   ===================================================== */

.volume-value {

    display: block;
    font-size: 8px;
    font-weight: 600;
    line-height: 1.2;
}


/* =====================================================
   방향
   ===================================================== */

.direction-long,
.direction-short,
.trend-long,
.trend-short,
.trend-flat {

    display: block;
    margin-top: 3px;
    font-size: 8px;
    font-weight: 800;
    line-height: 1;
}

.direction-long,
.trend-long {

    color: #35e66d;
}

.direction-short,
.trend-short {

    color: #ff4d4d;
}

.trend-flat {

    color: #777;
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

.change-item,
.change-positive,
.change-negative,
.change-flat {

    display: block;
    width: 100%;
    font-size: 8px;
    font-weight: 700;
    text-align: center;
    white-space: nowrap;
}

.change-positive {

    color: #35e66d;
}

.change-negative {

    color: #ff5555;
}

.change-flat {

    color: #aaa;
}


/* =====================================================
   🚨 알람
   ===================================================== */

.alarm-long,
.alarm-short {

    display: inline-block;
    font-size: 10px;
    font-weight: bold;
    line-height: 1;
    animation: alarmBlink 0.8s infinite;
}

.alarm-long {

    filter: drop-shadow(
        0 0 4px rgba(50,255,100,0.9)
    );
}

.alarm-short {

    filter: drop-shadow(
        0 0 4px rgba(255,50,50,0.9)
    );
}

@keyframes alarmBlink {

    0% {

        opacity: 1;
        transform: scale(1);
    }

    50% {

        opacity: 0.35;
        transform: scale(0.88);
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
    font-size: 8px;
    font-weight: 700;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}

.ema-long {

    color: #35e66d;
}

.ema-short {

    color: #ff5555;
}


/* =====================================================
   모바일
   ===================================================== */

@media (max-width: 480px) {

    body {

        padding: 3px;
        font-size: 9px;
    }

    h1 {

        font-size: 13px;
    }

    h2 {

        font-size: 11px;
    }

    .info {

        font-size: 7px;
    }

    th {

        padding: 4px 1px;
        font-size: 7px;
    }

    td {

        padding: 4px 1px;
    }

    .coin {

        font-size: 8px;
    }

    .volume-value {

        font-size: 7px;
    }

    .change-positive,
    .change-negative,
    .change-flat {

        font-size: 7px;
    }

    .direction-long,
    .direction-short,
    .trend-long,
    .trend-short,
    .trend-flat {

        font-size: 7px;
    }

    .alarm-long,
    .alarm-short {

        font-size: 9px;
    }

    .ema-value {

        font-size: 8px;
    }

}


/* =====================================================
   더 작은 휴대폰
   ===================================================== */

@media (max-width: 360px) {

    .ema-value {

        font-size: 7px;
    }

    .coin {

        font-size: 7px;
    }

    .volume-value {

        font-size: 6px;
    }

    .change-positive,
    .change-negative,
    .change-flat {

        font-size: 6px;
    }

}

"""


# =========================================================
# EMA HTML
# =========================================================

def ema_html(
    ema
):

    display = ema.get(
        "display",
        "⚪"
    )

    direction = ema.get(
        "direction",
        "none"
    )

    if direction == "long":

        return (
            '<div class="ema-value ema-long">'
            f'1H {display}'
            '</div>'
        )

    if direction == "short":

        return (
            '<div class="ema-value ema-short">'
            f'1H {display}'
            '</div>'
        )

    return (
        '<div class="ema-value">'
        f'1H {display}'
        '</div>'
    )


# =========================================================
# 테이블 행
# =========================================================

def make_table_rows(
    data
):

    rows_html = ""

    for item in data:

        signal = item.get(
            "signal",
            "none"
        )

        alarm = item.get(
            "warning",
            ""
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

</td>

<td>

<span class="volume-value">
{item["volume"]}
</span>

{item.get("direction", "")}

</td>

<td>

<div class="today-wrap">

<div>
{item["change"]}
</div>

<div>
{alarm}
</div>

</div>

</td>

<td>

{ema_html(
    item["ema"]
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

현재 🚨 종목 없음

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
        font-size:7px;
        line-height:1.5;
        margin:5px 2px 8px 2px;
     ">

※ TOP{TOP_N} 거래대금 순위 기준<br>

※ 코인명 아래 ☀️ = 당일 양수 / ☁️ = 당일 음수<br>

※ 거래대금 아래 LONG / SHORT = 현재 방향<br>

※ 1H 30-60-120 EMA 정배열 유지 조건<br>

※ 상승 후 조정 → 재상승 → 직전 스윙고점 돌파 시 🚨<br>

※ 첫 고점보다 낮은 고점도 새로운 돌파 기준으로 추적<br>

※ 현재 진행 중인 1H 캔들을 1분마다 확인<br>

※ 이미 과거에 발생한 돌파는 현재 시점에서 소급하지 않음<br>

※ 정배열이 깨지면 돌파 추적 상태 초기화<br>

※ 🚨는 현재 발생한 돌파 신호만 표시<br>

※ 업비트 = LONG 돌파만 표시<br>

※ OKX = LONG / SHORT 돌파 표시

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
📊 1H Breakout Trading
</h1>

<div class="info">

<div>
1H 30-60-120 정배열 + 스윙 고점/저점 돌파
</div>

<div>
현재 진행 중인 1H 캔들 1분 단위 확인
</div>

<div>
🚨 현재 돌파 신호만 표시
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
