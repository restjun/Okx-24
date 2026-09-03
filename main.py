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

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


# =========================================================
# 기본 설정
# =========================================================

warnings.filterwarnings(
    "ignore",
    category=FutureWarning
)

app = FastAPI()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

KST = ZoneInfo("Asia/Seoul")


# =========================================================
# 설정값
# =========================================================

VOLUME_HOURS = 24
TOP_N = 50
UPDATE_MINUTES = 1

HISTORY_CHUNK = 200
MAX_HISTORY_CHUNKS = 10

USE_UPBIT = "Y"
USE_OKX = "N"

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10

MIN_EMA10_LONG_COUNT = 1


# =========================================================
# API
# =========================================================

UPBIT_API = "https://api.upbit.com"
OKX_API = "https://www.okx.com"


# =========================================================
# 전역 상태
# =========================================================

latest_upbit_data = []
latest_okx_data = []

latest_usdt_krw = 0

latest_upbit_update = None
latest_okx_update = None

latest_upbit_markets = set()

request_lock = threading.Lock()
update_lock = threading.Lock()

air_state_lock = threading.Lock()

last_request_time = 0


# =========================================================
# 비행기 종료 표시 관리
# =========================================================

air_ended_displayed = set()
air_ended_displayed_lock = threading.Lock()


# =========================================================
# HTTP 요청
# =========================================================

def safe_get(
    url,
    params=None,
    headers=None,
    timeout=10
):
    global last_request_time

    for attempt in range(MAX_RETRIES):

        try:

            with request_lock:

                now = time.time()
                elapsed = now - last_request_time

                if elapsed < REQUEST_INTERVAL:
                    time.sleep(
                        REQUEST_INTERVAL - elapsed
                    )

                response = requests.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=timeout
                )

                last_request_time = time.time()

            if response.status_code == 200:
                return response

            if response.status_code in (
                429,
                500,
                502,
                503,
                504
            ):

                wait = RATE_LIMIT_WAIT * (
                    attempt + 1
                )

                logging.warning(
                    "HTTP %s / %s초 후 재시도: %s",
                    response.status_code,
                    wait,
                    url
                )

                time.sleep(wait)
                continue

            logging.warning(
                "HTTP 오류 %s: %s",
                response.status_code,
                url
            )

            return None

        except Exception as e:

            logging.warning(
                "HTTP 요청 오류 (%s/%s): %s",
                attempt + 1,
                MAX_RETRIES,
                e
            )

            time.sleep(
                RATE_LIMIT_WAIT * (attempt + 1)
            )

    return None


# =========================================================
# Upbit 마켓
# =========================================================

def get_upbit_markets():

    url = (
        f"{UPBIT_API}"
        "/v1/ticker/all"
    )

    params = {
        "quote_currencies": "KRW"
    }

    response = safe_get(
        url,
        params=params
    )

    if response is None:
        return []

    try:

        data = response.json()

        result = []

        for item in data:

            market = item.get("market")

            if not market:
                continue

            if not market.startswith("KRW-"):
                continue

            result.append({
                "market": market,
                "symbol": market.replace(
                    "KRW-",
                    ""
                ),
                "acc_trade_price_24h": float(
                    item.get(
                        "acc_trade_price_24h",
                        0
                    ) or 0
                )
            })

        return result

    except Exception as e:

        logging.error(
            "Upbit 마켓 조회 오류: %s",
            e
        )

        return []


# =========================================================
# USDT/KRW
# =========================================================

def get_usdt_krw():

    url = (
        f"{UPBIT_API}"
        "/v1/ticker"
    )

    params = {
        "markets": "KRW-USDT"
    }

    response = safe_get(
        url,
        params=params
    )

    if response is None:
        return 0

    try:

        data = response.json()

        if not data:
            return 0

        return float(
            data[0].get(
                "trade_price",
                0
            ) or 0
        )

    except Exception:

        return 0


# =========================================================
# Upbit 캔들
# =========================================================

def get_upbit_candle(
    market,
    unit,
    count=200,
    to=None
):

    url = (
        f"{UPBIT_API}"
        f"/v1/candles/minutes/{unit}"
    )

    params = {
        "market": market,
        "count": min(count, 200)
    }

    if to is not None:
        params["to"] = to

    response = safe_get(
        url,
        params=params
    )

    if response is None:
        return pd.DataFrame()

    try:

        data = response.json()

        if not data:
            return pd.DataFrame()

        rows = []

        for x in data:

            dt = pd.to_datetime(
                x["candle_date_time_kst"]
            )

            rows.append({
                "datetime": dt,
                "open": float(
                    x["opening_price"]
                ),
                "high": float(
                    x["high_price"]
                ),
                "low": float(
                    x["low_price"]
                ),
                "close": float(
                    x["trade_price"]
                ),
                "volume_krw": float(
                    x.get(
                        "candle_acc_trade_price",
                        0
                    ) or 0
                )
            })

        df = pd.DataFrame(rows)

        if df.empty:
            return df

        df = df.sort_values(
            "datetime"
        ).drop_duplicates(
            "datetime"
        ).reset_index(
            drop=True
        )

        # -------------------------------------------------
        # 현재 진행 중인 캔들 제거
        # -------------------------------------------------

        now = datetime.now(KST)

        if unit == 60:

            current_start = now.replace(
                minute=0,
                second=0,
                microsecond=0
            )

        elif unit == 240:

            block_hour = (
                now.hour // 4
            ) * 4

            current_start = now.replace(
                hour=block_hour,
                minute=0,
                second=0,
                microsecond=0
            )

        else:

            current_start = None

        if current_start is not None:

            current_start_naive = (
                current_start
                .replace(tzinfo=None)
            )

            df = df[
                df["datetime"]
                < current_start_naive
            ].copy()

        return df.reset_index(
            drop=True
        )

    except Exception as e:

        logging.warning(
            "Upbit 캔들 오류 %s: %s",
            market,
            e
        )

        return pd.DataFrame()


# =========================================================
# Upbit 과거 캔들
# =========================================================

def history_upbit(
    market,
    unit,
    required=125
):

    all_df = pd.DataFrame()
    to = None

    for _ in range(MAX_HISTORY_CHUNKS):

        df = get_upbit_candle(
            market,
            unit,
            HISTORY_CHUNK,
            to
        )

        if df.empty:
            break

        all_df = pd.concat(
            [
                all_df,
                df
            ],
            ignore_index=True
        )

        all_df = (
            all_df
            .drop_duplicates("datetime")
            .sort_values("datetime")
            .reset_index(drop=True)
        )

        if len(all_df) >= required:
            break

        oldest = all_df["datetime"].min()

        if pd.isna(oldest):
            break

        to_dt = (
            oldest
            .to_pydatetime()
            .replace(
                tzinfo=KST
            )
            - timedelta(
                seconds=1
            )
        )

        to = to_dt.isoformat()

    if len(all_df) < required:
        return pd.DataFrame()

    return all_df.tail(
        required
    ).reset_index(
        drop=True
    )


# =========================================================
# OKX 캔들
# =========================================================

def get_okx_ohlcv(
    inst,
    bar="1H",
    limit=200,
    before=None
):

    url = (
        f"{OKX_API}"
        "/api/v5/market/candles"
    )

    params = {
        "instId": inst,
        "bar": bar,
        "limit": min(limit, 200)
    }

    if before is not None:
        params["before"] = before

    response = safe_get(
        url,
        params=params
    )

    if response is None:
        return pd.DataFrame()

    try:

        result = response.json()

        if result.get("code") != "0":
            return pd.DataFrame()

        data = result.get(
            "data",
            []
        )

        rows = []

        for x in data:

            if len(x) < 9:
                continue

            # OKX:
            # ts, o, h, l, c, vol, volCcy,
            # volCcyQuote, confirm

            if str(x[8]) != "1":
                continue

            dt = datetime.fromtimestamp(
                int(x[0]) / 1000,
                tz=ZoneInfo("UTC")
            ).astimezone(
                KST
            )

            rows.append({
                "datetime": dt.replace(
                    tzinfo=None
                ),
                "open": float(x[1]),
                "high": float(x[2]),
                "low": float(x[3]),
                "close": float(x[4]),
                "volume": float(x[5]),
                "volume_quote": float(x[7])
            })

        df = pd.DataFrame(rows)

        if df.empty:
            return df

        df = (
            df
            .drop_duplicates("datetime")
            .sort_values("datetime")
            .reset_index(drop=True)
        )

        return df

    except Exception as e:

        logging.warning(
            "OKX 캔들 오류 %s: %s",
            inst,
            e
        )

        return pd.DataFrame()


# =========================================================
# OKX 과거 캔들
# =========================================================

def history_okx(
    inst,
    bar="1H",
    required=125
):

    all_df = pd.DataFrame()
    before = None

    for _ in range(MAX_HISTORY_CHUNKS):

        df = get_okx_ohlcv(
            inst,
            bar,
            HISTORY_CHUNK,
            before
        )

        if df.empty:
            break

        all_df = pd.concat(
            [
                all_df,
                df
            ],
            ignore_index=True
        )

        all_df = (
            all_df
            .drop_duplicates("datetime")
            .sort_values("datetime")
            .reset_index(drop=True)
        )

        if len(all_df) >= required:
            break

        oldest = all_df["datetime"].min()

        if pd.isna(oldest):
            break

        before = str(
            int(
                pd.Timestamp(oldest)
                .timestamp()
                * 1000
            )
        )

    if len(all_df) < required:
        return pd.DataFrame()

    return all_df.tail(
        required
    ).reset_index(drop=True)


# =========================================================
# EMA
# =========================================================

def ema(
    df,
    period
):

    return df["close"].ewm(
        span=period,
        adjust=False,
        min_periods=1
    ).mean()


# =========================================================
# EMA 방향
# =========================================================

def direction(df):

    if df is None or df.empty:
        return "none"

    if len(df) < 2:
        return "none"

    e10 = ema(df, 10)
    e30 = ema(df, 30)
    e60 = ema(df, 60)
    e120 = ema(df, 120)

    a = (
        e10.iloc[-1],
        e30.iloc[-1],
        e60.iloc[-1],
        e120.iloc[-1]
    )

    if (
        a[0] > a[1]
        and a[1] > a[2]
        and a[2] > a[3]
    ):
        return "long"

    if (
        a[0] < a[1]
        and a[1] < a[2]
        and a[2] < a[3]
    ):
        return "short"

    return "none"


# =========================================================
# EMA 정배열 연속 카운트
# =========================================================

def ema_alignment_count(df):

    if df is None or df.empty:
        return 0, "none"

    e10 = ema(df, 10)
    e30 = ema(df, 30)
    e60 = ema(df, 60)
    e120 = ema(df, 120)

    count_long = 0
    count_short = 0

    for i in range(
        len(df) - 1,
        -1,
        -1
    ):

        long_ok = (
            e10.iloc[i]
            > e30.iloc[i]
            > e60.iloc[i]
            > e120.iloc[i]
        )

        short_ok = (
            e10.iloc[i]
            < e30.iloc[i]
            < e60.iloc[i]
            < e120.iloc[i]
        )

        if long_ok:
            count_long += 1
        else:
            break

    for i in range(
        len(df) - 1,
        -1,
        -1
    ):

        short_ok = (
            e10.iloc[i]
            < e30.iloc[i]
            < e60.iloc[i]
            < e120.iloc[i]
        )

        if short_ok:
            count_short += 1
        else:
            break

    if count_long > 0:
        return count_long, "long"

    if count_short > 0:
        return count_short, "short"

    return 0, "none"


# =========================================================
# EMA 표시
# =========================================================

def ema_display(df):

    count, side = ema_alignment_count(df)

    if side == "long":
        return f"🟢({count})"

    if side == "short":
        return f"🔴({count})"

    return "⚪(0)"


# =========================================================
# =========================================================
# 비행기 핵심 로직
#
# 여기서는 카운트를 딱 하나만 사용한다.
#
# air_state[market]["count"]
#
# EMA3/EMA10 별도의 visible count를 비행기
# 카운트로 사용하지 않는다.
# =========================================================
# =========================================================


def get_ema3_10_state(df):

    if df is None or df.empty:
        return {
            "state": "none",
            "count": 0,
            "candle_time": None
        }

    df = df.copy()

    e3 = ema(df, 3)
    e10 = ema(df, 10)

    candle_time = df["datetime"].iloc[-1]

    if e3.iloc[-1] > e10.iloc[-1]:

        state = "long"

    elif e3.iloc[-1] < e10.iloc[-1]:

        state = "short"

    else:

        state = "equal"

    # -----------------------------------------------------
    # EMA3 > EMA10인 현재 확정 캔들부터 역순으로
    # 몇 개의 연속 확정 캔들이 유지되는지 계산
    #
    # 이 값은 "참고용"으로만 사용.
    # 비행기 상태의 실제 count는 절대 이 값을 사용하지 않음.
    # -----------------------------------------------------

    consecutive = 0

    for i in range(
        len(df) - 1,
        -1,
        -1
    ):

        if e3.iloc[i] > e10.iloc[i]:
            consecutive += 1
        else:
            break

    return {
        "state": state,
        "count": consecutive,
        "candle_time": candle_time
    }


# =========================================================
# 비행기 신규 조건
# =========================================================

def get_air_warning(
    df1h,
    df4h
):

    if (
        df1h is None
        or df1h.empty
        or df4h is None
        or df4h.empty
    ):
        return None

    direction_1h = direction(df1h)
    direction_4h = direction(df4h)

    if direction_1h != "long":
        return None

    if direction_4h != "long":
        return None

    ema3_state = get_ema3_10_state(
        df1h
    )

    if ema3_state["state"] != "long":
        return None

    if (
        ema3_state["count"]
        < MIN_EMA10_LONG_COUNT
    ):
        return None

    return "LONG_PRE_BREAKOUT"


# =========================================================
# ★ 비행기 상태 머신
#
# 모든 카운트는 확정 1H 캔들 기준
#
# 첫 확정 조건 캔들
#       ↓
# 🛩✈️  count = 1
#
# 다음 확정 캔들
#       ↓
# ✈️    count = 2
#
# 다음 확정 캔들
#       ↓
# ✈️    count = 3
#
# EMA3 < EMA10 확정
#       ↓
# ⛔️
#
# 같은 캔들을 여러 번 조회해도
# 절대 count가 중복 증가하지 않는다.
# =========================================================

def update_air_counter(
    market,
    df1h,
    new_warning
):

    if (
        df1h is None
        or df1h.empty
    ):
        return {
            "active": False,
            "ended": False,
            "count": 0,
            "warning_candle": None,
            "counted_candle": None
        }

    current_candle = df1h[
        "datetime"
    ].iloc[-1]

    ema3_state = get_ema3_10_state(
        df1h
    )

    current_ema_state = (
        ema3_state["state"]
    )

    with air_state_lock:

        state = air_state.get(
            market
        )

        # -------------------------------------------------
        # 최초 진입
        # -------------------------------------------------

        if state is None:

            if (
                new_warning
                and current_ema_state == "long"
            ):

                state = {
                    "active": True,
                    "ended": False,
                    "count": 1,
                    "warning_candle":
                        current_candle,
                    "counted_candle":
                        current_candle,
                    "ended_candle": None
                }

                air_state[market] = state

                return state.copy()

            return {
                "active": False,
                "ended": False,
                "count": 0,
                "warning_candle": None,
                "counted_candle": None,
                "ended_candle": None
            }

        # -------------------------------------------------
        # 이미 종료된 상태
        #
        # 새로운 확정 캔들에서 조건이 다시 발생하면
        # 새로운 비행기 사이클을 시작
        # -------------------------------------------------

        if state.get("ended", False):

            if (
                new_warning
                and current_ema_state == "long"
                and current_candle
                != state.get(
                    "ended_candle"
                )
            ):

                state = {
                    "active": True,
                    "ended": False,
                    "count": 1,
                    "warning_candle":
                        current_candle,
                    "counted_candle":
                        current_candle,
                    "ended_candle": None
                }

                air_state[market] = state

                return state.copy()

            return state.copy()

        # -------------------------------------------------
        # 활성 상태
        # -------------------------------------------------

        # 같은 확정 캔들 재조회
        #
        # 스케줄러가 1분마다 실행되므로 동일한 1H 캔들을
        # 여러 번 가져오게 된다.
        #
        # 이 경우 절대 count 증가 금지.
        # -------------------------------------------------

        counted_candle = state.get(
            "counted_candle"
        )

        if (
            counted_candle is not None
            and current_candle
            <= counted_candle
        ):

            # 같은 캔들에서 EMA3 < EMA10이
            # 발생할 수 있는 상황은 없어야 하지만,
            # 안전하게 종료 조건만 확인.
            if current_ema_state == "short":

                state["active"] = False
                state["ended"] = True
                state["ended_candle"] = (
                    current_candle
                )

            air_state[market] = state

            return state.copy()

        # -------------------------------------------------
        # ★ 새로운 확정 1H 캔들
        # -------------------------------------------------

        state["counted_candle"] = (
            current_candle
        )

        # -------------------------------------------------
        # EMA3 < EMA10
        #
        # 새로운 확정 캔들에서 하향 전환되었으므로
        # 비행기 종료.
        # -------------------------------------------------

        if current_ema_state == "short":

            state["active"] = False
            state["ended"] = True
            state["ended_candle"] = (
                current_candle
            )

            air_state[market] = state

            return state.copy()

        # -------------------------------------------------
        # EMA3 == EMA10
        #
        # 카운트 증가 없음.
        # 기존 비행기 상태 유지.
        # -------------------------------------------------

        if current_ema_state == "equal":

            air_state[market] = state

            return state.copy()

        # -------------------------------------------------
        # EMA3 > EMA10
        #
        # 새로운 확정 캔들이므로 count +1
        # -------------------------------------------------

        if current_ema_state == "long":

            state["count"] = int(
                state.get("count", 0)
            ) + 1

            state["active"] = True
            state["ended"] = False

            air_state[market] = state

            return state.copy()

        return state.copy()


# =========================================================
# 일봉 등락률 - Upbit
# =========================================================

def daily_change_upbit(
    market
):

    df = get_upbit_candle(
        market,
        1440,
        2
    )

    if df.empty:
        return 0

    if len(df) < 2:
        return 0

    previous_close = float(
        df["close"].iloc[-2]
    )

    current_close = float(
        df["close"].iloc[-1]
    )

    if previous_close == 0:
        return 0

    return (
        current_close
        - previous_close
    ) / previous_close * 100


# =========================================================
# 일봉 등락률 - OKX
# =========================================================

def daily_changes(
    df
):

    if (
        df is None
        or df.empty
    ):
        return 0

    temp = df.copy()

    temp["datetime"] = pd.to_datetime(
        temp["datetime"]
    )

    temp = temp.set_index(
        "datetime"
    )

    daily = (
        temp["close"]
        .resample(
            "1D",
            offset="9h"
        )
        .last()
        .dropna()
    )

    if len(daily) < 2:
        return 0

    previous_close = float(
        daily.iloc[-2]
    )

    current_close = float(
        daily.iloc[-1]
    )

    if previous_close == 0:
        return 0

    return (
        current_close
        - previous_close
    ) / previous_close * 100


# =========================================================
# 거래대금 표시
# =========================================================

def format_volume(
    value
):

    try:

        value = float(value)

    except Exception:

        return "-"

    if value >= 1_0000_0000_0000:
        return (
            f"{value / 1_0000_0000_0000:.2f}조"
        )

    if value >= 1_0000_0000:
        return (
            f"{value / 1_0000_0000:.0f}억"
        )

    if value >= 1_0000:
        return (
            f"{value / 1_0000:.0f}만"
        )

    return f"{value:,.0f}"


# =========================================================
# 등락률 표시
# =========================================================

def format_change(
    value
):

    try:

        value = float(value)

    except Exception:

        return "-"

    if value > 0:
        return f"+{value:.2f}%"

    return f"{value:.2f}%"


# =========================================================
# 빈 분석
# =========================================================

def empty_analysis():

    return {
        "ema_1h": "⚪(0)",
        "ema_4h": "⚪(0)",

        "ema3_10_cross_1h": {
            "state": "none",
            "count": 0,
            "candle_time": None
        },

        "change": 0,

        "air_warning": False,
        "air_count": 0,
        "air_active": False,
        "air_ended": False,

        "qualified": False,

        "direction_1h": "none",
        "direction_4h": "none",

        "df1h": pd.DataFrame()
    }


# =========================================================
# 종목 분석
# =========================================================

def analyze(
    market
):

    try:

        # -------------------------------------------------
        # 1H
        # -------------------------------------------------

        df1h = history_upbit(
            market,
            60,
            125
        )

        if df1h.empty:
            return empty_analysis()

        # -------------------------------------------------
        # 4H
        # -------------------------------------------------

        df4h = history_upbit(
            market,
            240,
            125
        )

        if df4h.empty:
            return empty_analysis()

        # -------------------------------------------------
        # EMA 표시
        # -------------------------------------------------

        ema_1h = ema_display(
            df1h
        )

        ema_4h = ema_display(
            df4h
        )

        # -------------------------------------------------
        # 방향
        # -------------------------------------------------

        direction_1h = direction(
            df1h
        )

        direction_4h = direction(
            df4h
        )

        # -------------------------------------------------
        # EMA3 / EMA10 상태
        #
        # 이 값의 count는 참고용.
        #
        # ★ 비행기 표시 카운트로 사용하지 않는다.
        # -------------------------------------------------

        ema3_data = get_ema3_10_state(
            df1h
        )

        # -------------------------------------------------
        # 신규 비행기 조건
        # -------------------------------------------------

        new_warning = get_air_warning(
            df1h,
            df4h
        )

        # -------------------------------------------------
        # ★ 유일한 비행기 카운터
        # -------------------------------------------------

        air = update_air_counter(
            market,
            df1h,
            new_warning
        )

        # -------------------------------------------------
        # 일봉 등락
        # -------------------------------------------------

        change = daily_change_upbit(
            market
        )

        return {
            "ema_1h": ema_1h,
            "ema_4h": ema_4h,

            "ema3_10_cross_1h": {
                "state":
                    ema3_data["state"],

                # 참고용 EMA3/10 연속 상태
                "count":
                    ema3_data["count"],

                "candle_time":
                    ema3_data["candle_time"]
            },

            "change": change,

            # ★ 실제 비행기 상태
            "air_warning":
                air["active"],

            "air_count":
                int(
                    air.get(
                        "count",
                        0
                    )
                ),

            "air_active":
                air["active"],

            "air_ended":
                air["ended"],

            "qualified":
                air["active"],

            "direction_1h":
                direction_1h,

            "direction_4h":
                direction_4h,

            "df1h":
                df1h
        }

    except Exception as e:

        logging.error(
            "분석 오류 %s: %s",
            market,
            e
        )

        return empty_analysis()


# =========================================================
# 행 생성
# =========================================================

def make_row(
    rank,
    market,
    analysis,
    volume
):

    symbol = market.replace(
        "KRW-",
        ""
    )

    return {
        "rank": rank,
        "market": market,
        "name": symbol,

        "volume": volume,

        "change":
            analysis.get(
                "change",
                0
            ),

        "ema_1h":
            analysis.get(
                "ema_1h",
                "⚪(0)"
            ),

        "ema_4h":
            analysis.get(
                "ema_4h",
                "⚪(0)"
            ),

        "ema3_10_cross_1h":
            analysis.get(
                "ema3_10_cross_1h",
                {}
            ),

        # ★ 실제 비행기 count
        "air_count":
            int(
                analysis.get(
                    "air_count",
                    0
                )
            ),

        "air_warning":
            analysis.get(
                "air_warning",
                False
            ),

        "air_active":
            analysis.get(
                "air_active",
                False
            ),

        "air_ended":
            analysis.get(
                "air_ended",
                False
            ),

        "qualified":
            analysis.get(
                "qualified",
                False
            ),

        "direction_1h":
            analysis.get(
                "direction_1h",
                "none"
            ),

        "direction_4h":
            analysis.get(
                "direction_4h",
                "none"
            )
    }


# =========================================================
# Upbit 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update
    global latest_upbit_markets
    global latest_usdt_krw

    markets = get_upbit_markets()

    if not markets:

        logging.warning(
            "Upbit 마켓 조회 실패"
        )

        return

    markets = sorted(
        markets,
        key=lambda x:
            x["acc_trade_price_24h"],
        reverse=True
    )

    selected = markets[
        :TOP_N
    ]

    latest_upbit_markets = set(
        x["market"]
        for x in markets
    )

    rows = []

    for rank, item in enumerate(
        selected,
        start=1
    ):

        market = item["market"]

        volume = float(
            item.get(
                "acc_trade_price_24h",
                0
            )
            or 0
        )

        logging.info(
            "분석 %s/%s %s",
            rank,
            len(selected),
            market
        )

        analysis = analyze(
            market
        )

        row = make_row(
            rank,
            market,
            analysis,
            volume
        )

        rows.append(row)

    latest_upbit_data = rows

    latest_usdt_krw = (
        get_usdt_krw()
    )

    latest_upbit_update = (
        datetime.now(KST)
    )

    logging.info(
        "Upbit 업데이트 완료: %s개",
        len(rows)
    )


# =========================================================
# OKX 거래대금
# =========================================================

def get_okx_volume(
    inst
):

    df = get_okx_ohlcv(
        inst,
        "1H",
        VOLUME_HOURS
    )

    if df.empty:
        return 0

    return float(
        df["volume_quote"]
        .sum()
    )


# =========================================================
# OKX 마켓
# =========================================================

def get_okx_markets():

    url = (
        f"{OKX_API}"
        "/api/v5/market/tickers"
    )

    params = {
        "instType": "SWAP"
    }

    response = safe_get(
        url,
        params=params
    )

    if response is None:
        return []

    try:

        result = response.json()

        if result.get("code") != "0":
            return []

        data = result.get(
            "data",
            []
        )

        rows = []

        for x in data:

            inst = x.get(
                "instId",
                ""
            )

            if not inst.endswith(
                "-USDT-SWAP"
            ):
                continue

            rows.append({
                "instId": inst,
                "symbol":
                    inst.replace(
                        "-USDT-SWAP",
                        ""
                    )
            })

        return rows

    except Exception:

        return []


# =========================================================
# OKX 업데이트
# =========================================================

def update_okx():

    global latest_okx_data
    global latest_okx_update

    markets = get_okx_markets()

    if not markets:
        return

    rows = []

    for item in markets:

        inst = item["instId"]

        try:

            volume = (
                get_okx_volume(
                    inst
                )
            )

            # 기존 UI 스케일 유지
            volume = volume / 10

            df1h = history_okx(
                inst,
                "1H",
                125
            )

            df4h = history_okx(
                inst,
                "4H",
                125
            )

            if (
                df1h.empty
                or df4h.empty
            ):
                continue

            ema_1h = ema_display(
                df1h
            )

            ema_4h = ema_display(
                df4h
            )

            ema3_data = (
                get_ema3_10_state(
                    df1h
                )
            )

            direction_1h = direction(
                df1h
            )

            direction_4h = direction(
                df4h
            )

            new_warning = (
                get_air_warning(
                    df1h,
                    df4h
                )
            )

            air = update_air_counter(
                inst,
                df1h,
                new_warning
            )

            change = daily_changes(
                df1h
            )

            rows.append({
                "rank": 0,

                "market": inst,

                "name":
                    item["symbol"]
                    + " (OKX)",

                "volume": volume,

                "change": change,

                "ema_1h": ema_1h,

                "ema_4h": ema_4h,

                "ema3_10_cross_1h": {
                    "state":
                        ema3_data["state"],
                    "count":
                        ema3_data["count"],
                    "candle_time":
                        ema3_data["candle_time"]
                },

                # ★ 실제 비행기 count 하나만 사용
                "air_count":
                    int(
                        air.get(
                            "count",
                            0
                        )
                    ),

                "air_warning":
                    air["active"],

                "air_active":
                    air["active"],

                "air_ended":
                    air["ended"],

                "qualified":
                    air["active"],

                "direction_1h":
                    direction_1h,

                "direction_4h":
                    direction_4h
            })

        except Exception as e:

            logging.warning(
                "OKX 분석 오류 %s: %s",
                inst,
                e
            )

    rows.sort(
        key=lambda x:
            x["volume"],
        reverse=True
    )

    for i, row in enumerate(
        rows[:TOP_N],
        start=1
    ):
        row["rank"] = i

    latest_okx_data = rows[
        :TOP_N
    ]

    latest_okx_update = (
        datetime.now(KST)
    )


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    if not update_lock.acquire(
        blocking=False
    ):
        logging.info(
            "이전 업데이트 진행 중"
        )
        return

    try:

        if USE_UPBIT == "Y":
            update_upbit()

        if USE_OKX == "Y":
            update_okx()

    except Exception as e:

        logging.error(
            "전체 업데이트 오류: %s",
            e
        )

    finally:

        update_lock.release()


# =========================================================
# 비행기 아이콘 HTML
# =========================================================

def warning_html(
    x
):

    # -----------------------------------------------------
    # ★ 반드시 실제 air_count만 사용
    #
    # ema3_10_cross_1h["count"]를 사용하지 않는다.
    # -----------------------------------------------------

    count = int(
        x.get(
            "air_count",
            0
        )
        or 0
    )

    ended = bool(
        x.get(
            "air_ended",
            False
        )
    )

    active = bool(
        x.get(
            "air_active",
            False
        )
    )

    # -----------------------------------------------------
    # 종료
    # -----------------------------------------------------

    if ended:

        return (
            '<span class="air-end">'
            '⛔️'
            '</span>'
        )

    # -----------------------------------------------------
    # 비활성
    # -----------------------------------------------------

    if not active or count <= 0:

        return "-"

    # -----------------------------------------------------
    # 첫 확정 캔들
    # -----------------------------------------------------

    if count == 1:

        return (
            '<span class="air-icon">'
            '🛩✈️'
            '</span>'
        )

    # -----------------------------------------------------
    # 두 번째 이후 확정 캔들
    # -----------------------------------------------------

    return (
        '<span class="air-icon">'
        '✈️'
        '</span>'
    )


# =========================================================
# EMA HTML
# =========================================================

def ema_html(
    value,
    direction_value
):

    if direction_value == "long":

        return (
            '<span class="ema-long">'
            f'{value}'
            '</span>'
        )

    if direction_value == "short":

        return (
            '<span class="ema-short">'
            f'{value}'
            '</span>'
        )

    return (
        '<span class="ema-none">'
        f'{value}'
        '</span>'
    )


# =========================================================
# EMA3 / EMA10 HTML
#
# 여기 표시되는 count는 EMA3/10 자체의
# 참고용 연속 카운트.
#
# 비행기 count와 절대 연결하지 않는다.
# =========================================================

def ema3_10_cross_html(
    data
):

    if not data:
        return "-"

    state = data.get(
        "state",
        "none"
    )

    count = int(
        data.get(
            "count",
            0
        )
        or 0
    )

    if state == "long":

        return (
            '<span class="cross-long">'
            f'🟢({count})'
            '</span>'
        )

    if state == "short":

        return (
            '<span class="cross-short">'
            f'🔻({count})'
            '</span>'
        )

    return "⚪(0)"


# =========================================================
# 행 HTML
# =========================================================

def rows_html(
    rows
):

    if not rows:

        return (
            '<tr>'
            '<td colspan="9">'
            '데이터 없음'
            '</td>'
            '</tr>'
        )

    html = []

    for x in rows:

        rank = x.get(
            "rank",
            "-"
        )

        name = x.get(
            "name",
            "-"
        )

        change = x.get(
            "change",
            0
        )

        volume = x.get(
            "volume",
            0
        )

        ema1h = x.get(
            "ema_1h",
            "⚪(0)"
        )

        ema4h = x.get(
            "ema_4h",
            "⚪(0)"
        )

        direction1h = x.get(
            "direction_1h",
            "none"
        )

        direction4h = x.get(
            "direction_4h",
            "none"
        )

        ema3_data = x.get(
            "ema3_10_cross_1h",
            {}
        )

        # -------------------------------------------------
        # ★ 비행기 표시
        #
        # 기존의 잘못된:
        #
        # visual_air_count =
        #     ema3_data["count"]
        #
        # 제거.
        #
        # 이제 무조건 air_count 하나만 사용.
        # -------------------------------------------------

        warning = warning_html(
            x
        )

        change_class = (
            "positive"
            if float(change) > 0
            else (
                "negative"
                if float(change) < 0
                else ""
            )
        )

        html.append(
            f"""
            <tr>
                <td class="rank">
                    {rank}
                </td>

                <td class="coin">
                    {name}
                </td>

                <td class="{change_class}">
                    {format_change(change)}
                </td>

                <td>
                    {format_volume(volume)}
                </td>

                <td>
                    {ema_html(
                        ema1h,
                        direction1h
                    )}
                </td>

                <td>
                    {ema_html(
                        ema4h,
                        direction4h
                    )}
                </td>

                <td>
                    {ema3_10_cross_html(
                        ema3_data
                    )}
                </td>

                <td class="air-cell">
                    {warning}
                </td>

                <td>
                    {x.get(
                        "air_count",
                        0
                    )}
                </td>
            </tr>
            """
        )

    return "".join(
        html
    )


# =========================================================
# 상승 직전 집중 목록
#
# air_count == 1
# =========================================================

def rising_focus_section(
    rows
):

    focus = []

    for x in rows:

        if (
            x.get(
                "air_active",
                False
            )
            and not x.get(
                "air_ended",
                False
            )
            and int(
                x.get(
                    "air_count",
                    0
                )
                or 0
            ) == 1
        ):

            focus.append(x)

    if not focus:
        return ""

    return f"""
    <section class="focus-section rising-section">

        <h2>
            🛩✈️ 돌파 직전 포착
        </h2>

        <div class="focus-count">
            {len(focus)}개
        </div>

        <div class="table-wrap">

            <table>

                <thead>
                    <tr>
                        <th>순위</th>
                        <th>종목</th>
                        <th>등락</th>
                        <th>거래대금</th>
                        <th>1H EMA</th>
                        <th>4H EMA</th>
                        <th>3-10선</th>
                        <th>비행기</th>
                        <th>카운트</th>
                    </tr>
                </thead>

                <tbody>
                    {rows_html(focus)}
                </tbody>

            </table>

        </div>

    </section>
    """


# =========================================================
# 경고 집중 목록
#
# air_count >= 2
# + 종료 ⛔️
# =========================================================

def warning_focus_section(
    rows
):

    focus = []

    # -----------------------------------------------------
    # 진행 중
    # -----------------------------------------------------

    for x in rows:

        if (
            x.get(
                "air_active",
                False
            )
            and not x.get(
                "air_ended",
                False
            )
            and int(
                x.get(
                    "air_count",
                    0
                )
                or 0
            ) >= 2
        ):

            focus.append(x)

    # -----------------------------------------------------
    # 종료
    #
    # 해당 종료 상태는 한 번만 추가
    # -----------------------------------------------------

    ended = []

    for x in rows:

        if not x.get(
            "air_ended",
            False
        ):
            continue

        market = x.get(
            "market",
            x.get(
                "name",
                ""
            )
        )

        with air_ended_displayed_lock:

            if market in air_ended_displayed:
                continue

            air_ended_displayed.add(
                market
            )

        ended.append(x)

    focus.extend(
        ended
    )

    if not focus:
        return ""

    return f"""
    <section class="focus-section warning-section">

        <h2>
            ✈️ 비행기 경고
        </h2>

        <div class="focus-count">
            {len(focus)}개
        </div>

        <div class="table-wrap">

            <table>

                <thead>
                    <tr>
                        <th>순위</th>
                        <th>종목</th>
                        <th>등락</th>
                        <th>거래대금</th>
                        <th>1H EMA</th>
                        <th>4H EMA</th>
                        <th>3-10선</th>
                        <th>비행기</th>
                        <th>카운트</th>
                    </tr>
                </thead>

                <tbody>
                    {rows_html(focus)}
                </tbody>

            </table>

        </div>

    </section>
    """


# =========================================================
# CSS
# =========================================================

CSS = r"""
* {
    box-sizing: border-box;
}

html,
body {
    margin: 0;
    padding: 0;
    background: #111;
    color: #fff;
    font-family:
        -apple-system,
        BlinkMacSystemFont,
        "Segoe UI",
        sans-serif;
}

body {
    padding: 12px;
}

.container {
    max-width: 1600px;
    margin: 0 auto;
}

.header {
    background: #1b1b1b;
    border-radius: 12px;
    padding: 16px;
    margin-bottom: 12px;
}

.header h1 {
    margin: 0 0 8px 0;
    font-size: 21px;
}

.header-info {
    font-size: 13px;
    color: #bbb;
    line-height: 1.7;
}

.focus-section {
    background: #171717;
    border-radius: 12px;
    padding: 12px;
    margin-bottom: 14px;
    overflow: hidden;
}

.focus-section h2 {
    margin: 0 0 8px 0;
    font-size: 17px;
}

.rising-section {
    border: 2px solid #777;
}

.warning-section {
    border: 2px solid #f0f0f0;
}

.focus-count {
    color: #aaa;
    font-size: 12px;
    margin-bottom: 10px;
}

.table-wrap {
    width: 100%;
    overflow-x: auto;
}

table {
    width: 100%;
    min-width: 850px;
    border-collapse: collapse;
}

th {
    background: #222;
    color: #aaa;
    font-size: 12px;
    font-weight: 600;
    padding: 9px 6px;
    border-bottom: 1px solid #444;
    white-space: nowrap;
}

td {
    padding: 9px 6px;
    border-bottom: 1px solid #292929;
    text-align: center;
    font-size: 13px;
    white-space: nowrap;
}

.rank {
    width: 45px;
    color: #aaa;
}

.coin {
    text-align: left;
    font-weight: 700;
}

.positive {
    color: #ff5f5f;
}

.negative {
    color: #4da3ff;
}

.ema-long {
    color: #44e67b;
    font-weight: 700;
}

.ema-short {
    color: #ff5757;
    font-weight: 700;
}

.ema-none {
    color: #aaa;
}

.cross-long {
    color: #44e67b;
    font-weight: 700;
}

.cross-short {
    color: #ff5757;
    font-weight: 700;
}

.air-cell {
    font-size: 21px;
    min-width: 75px;
}

.air-icon {
    display: inline-block;
    animation: airplanePulse 1.5s infinite;
}

.air-end {
    display: inline-block;
    font-size: 20px;
}

@keyframes airplanePulse {
    0% {
        transform: scale(1);
    }

    50% {
        transform: scale(1.15);
    }

    100% {
        transform: scale(1);
    }
}

.full-section {
    background: #171717;
    border-radius: 12px;
    padding: 12px;
}

.full-section h2 {
    margin: 0 0 10px 0;
    font-size: 17px;
}

@media (
    max-width: 700px
) {

    body {
        padding: 7px;
    }

    .header {
        padding: 12px;
    }

    .header h1 {
        font-size: 18px;
    }

    th {
        font-size: 11px;
    }

    td {
        font-size: 12px;
        padding: 8px 5px;
    }

    .air-cell {
        font-size: 19px;
    }
}
"""


# =========================================================
# Dashboard
# =========================================================

def dashboard():

    all_rows = []

    if USE_UPBIT == "Y":
        all_rows.extend(
            latest_upbit_data
        )

    if USE_OKX == "Y":
        all_rows.extend(
            latest_okx_data
        )

    update_time = (
        latest_upbit_update
        or latest_okx_update
    )

    if update_time:

        update_text = (
            update_time
            .astimezone(KST)
            .strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        )

    else:

        update_text = "-"

    rising = (
        rising_focus_section(
            all_rows
        )
    )

    warning = (
        warning_focus_section(
            all_rows
        )
    )

    full = f"""
    <section class="full-section">

        <h2>
            🏆 TOP{TOP_N}
        </h2>

        <div class="table-wrap">

            <table>

                <thead>
                    <tr>
                        <th>순위</th>
                        <th>종목</th>
                        <th>등락</th>
                        <th>거래대금</th>
                        <th>1H EMA</th>
                        <th>4H EMA</th>
                        <th>3-10선</th>
                        <th>비행기</th>
                        <th>카운트</th>
                    </tr>
                </thead>

                <tbody>
                    {rows_html(all_rows)}
                </tbody>

            </table>

        </div>

    </section>
    """

    return f"""
    <!DOCTYPE html>

    <html lang="ko">

    <head>

        <meta charset="UTF-8">

        <meta
            name="viewport"
            content="width=device-width,
                     initial-scale=1.0"
        >

        <meta
            http-equiv="refresh"
            content="60"
        >

        <title>
            Crypto EMA Dashboard
        </title>

        <style>
            {CSS}
        </style>

    </head>

    <body>

        <div class="container">

            <header class="header">

                <h1>
                    🚀 Crypto EMA Dashboard
                </h1>

                <div class="header-info">

                    마지막 업데이트:
                    {update_text}

                    <br>

                    기준:
                    <b>확정 1H 캔들</b>

                    &nbsp;|&nbsp;

                    비행기 카운트:
                    <b>단일 상태 카운터</b>

                    <br>

                    상태:
                    🛩✈️ → ✈️ → ✈️ → ⛔️

                </div>

            </header>

            {rising}

            {warning}

            {full}

        </div>

    </body>

    </html>
    """


# =========================================================
# FastAPI
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def home():

    return dashboard()


# =========================================================
# 스케줄러
# =========================================================

def scheduler_loop():

    schedule.every(
        UPDATE_MINUTES
    ).minutes.do(
        update_dashboard
    )

    while True:

        try:

            schedule.run_pending()

        except Exception as e:

            logging.error(
                "스케줄러 오류: %s",
                e
            )

        time.sleep(1)


# =========================================================
# 시작
# =========================================================

if __name__ == "__main__":

    logging.info(
        "Crypto Dashboard 시작"
    )

    # -----------------------------------------------------
    # 최초 데이터 업데이트
    # -----------------------------------------------------

    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    # -----------------------------------------------------
    # 스케줄러
    # -----------------------------------------------------

    threading.Thread(
        target=scheduler_loop,
        daemon=True
    ).start()

    # -----------------------------------------------------
    # FastAPI
    # -----------------------------------------------------

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
        )
