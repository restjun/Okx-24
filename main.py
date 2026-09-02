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

from datetime import datetime
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
    format="%(asctime)s | %(levelname)s | %(message)s"
)

KST = ZoneInfo("Asia/Seoul")

UPBIT_URL = "https://api.upbit.com"

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


# =========================================================
# 전역 상태
# =========================================================

data_lock = threading.Lock()

latest_data = []

last_update_time = None


# ---------------------------------------------------------
# 비행기 상태
#
# market별:
#
# active
# count
# direction
# last_candle
# golden_candle
# ended_candle
# ---------------------------------------------------------

air_state = {}

session = requests.Session()

session.headers.update({
    "Accept": "application/json"
})


# =========================================================
# 공통 API 요청
# =========================================================

def safe_get(
    url,
    params=None,
    timeout=10
):

    for attempt in range(MAX_RETRIES):

        try:

            response = session.get(
                url,
                params=params,
                timeout=timeout
            )

            if response.status_code == 200:

                return response.json()

            if response.status_code == 429:

                logging.warning(
                    "API RATE LIMIT → %s초 대기",
                    RATE_LIMIT_WAIT
                )

                time.sleep(
                    RATE_LIMIT_WAIT
                )

                continue

            logging.warning(
                "API 오류 %s : %s",
                response.status_code,
                response.text[:200]
            )

        except Exception as e:

            logging.warning(
                "API 요청 실패 %s/%s : %s",
                attempt + 1,
                MAX_RETRIES,
                e
            )

        time.sleep(
            REQUEST_INTERVAL
        )

    return None


# =========================================================
# EMA
# =========================================================

def ema(
    series,
    period
):

    return series.ewm(
        span=period,
        adjust=False,
        min_periods=1
    ).mean()


# =========================================================
# Upbit 전체 KRW 시장 + 현재가
# =========================================================

def get_upbit_markets():

    data = safe_get(
        "{}/v1/ticker/all".format(
            UPBIT_URL
        ),
        params={
            "quote_currencies": "KRW"
        }
    )

    if not data:
        return []

    result = []

    for item in data:

        market = item.get(
            "market",
            ""
        )

        if not market.startswith("KRW-"):
            continue

        try:

            result.append({

                "market":
                    market,

                "coin":
                    market.replace(
                        "KRW-",
                        ""
                    ),

                "current_price":
                    float(
                        item.get(
                            "trade_price",
                            0
                        )
                    ),

                "change_rate":
                    float(
                        item.get(
                            "signed_change_rate",
                            0
                        )
                    ) * 100,

                "volume_24h":
                    float(
                        item.get(
                            "acc_trade_price_24h",
                            0
                        )
                    )
            })

        except Exception:
            continue

    return result


# =========================================================
# Upbit 캔들
#
# unit = 60  → 1시간
# unit = 240 → 4시간
#
# 현재 진행 중인 캔들은 제외
# =========================================================

def get_upbit_candle(
    market,
    unit=60,
    count=200
):

    data = safe_get(
        "{}/v1/candles/minutes/{}".format(
            UPBIT_URL,
            unit
        ),
        params={
            "market": market,
            "count": count
        }
    )

    if not data:
        return pd.DataFrame()

    rows = []

    now = datetime.now(
        KST
    ).replace(
        tzinfo=None
    )

    for item in data:

        try:

            dt = datetime.fromisoformat(
                item[
                    "candle_date_time_kst"
                ]
            )

            # 현재 진행 중인 캔들 제거
            if dt >= now:
                continue

            rows.append({

                "datetime":
                    dt,

                "open":
                    float(
                        item[
                            "opening_price"
                        ]
                    ),

                "high":
                    float(
                        item[
                            "high_price"
                        ]
                    ),

                "low":
                    float(
                        item[
                            "low_price"
                        ]
                    ),

                "close":
                    float(
                        item[
                            "trade_price"
                        ]
                    ),

                "volume":
                    float(
                        item[
                            "candle_acc_trade_volume"
                        ]
                    ),

                "value":
                    float(
                        item[
                            "candle_acc_trade_price"
                        ]
                    )
            })

        except Exception:
            continue

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(
        rows
    )

    df = df.sort_values(
        "datetime"
    )

    df = df.drop_duplicates(
        "datetime"
    )

    df = df.reset_index(
        drop=True
    )

    return df


# =========================================================
# 긴 역사 데이터
# =========================================================

def history_upbit(
    market,
    unit=60,
    total=1000
):

    frames = []

    to_time = None

    remain = total

    while remain > 0:

        count = min(
            HISTORY_CHUNK,
            remain
        )

        params = {

            "market":
                market,

            "count":
                count
        }

        if to_time is not None:

            params["to"] = to_time

        data = safe_get(
            "{}/v1/candles/minutes/{}".format(
                UPBIT_URL,
                unit
            ),
            params=params
        )

        if not data:
            break

        rows = []

        for item in data:

            try:

                dt = datetime.fromisoformat(
                    item[
                        "candle_date_time_kst"
                    ]
                )

                rows.append({

                    "datetime":
                        dt,

                    "open":
                        float(
                            item[
                                "opening_price"
                            ]
                        ),

                    "high":
                        float(
                            item[
                                "high_price"
                            ]
                        ),

                    "low":
                        float(
                            item[
                                "low_price"
                            ]
                        ),

                    "close":
                        float(
                            item[
                                "trade_price"
                            ]
                        ),

                    "volume":
                        float(
                            item[
                                "candle_acc_trade_volume"
                            ]
                        ),

                    "value":
                        float(
                            item[
                                "candle_acc_trade_price"
                            ]
                        )
                })

            except Exception:
                continue

        if not rows:
            break

        df = pd.DataFrame(
            rows
        )

        frames.append(
            df
        )

        oldest = df[
            "datetime"
        ].min()

        to_time = oldest.strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        remain -= len(df)

        if len(df) < count:
            break

        time.sleep(
            REQUEST_INTERVAL
        )

        if len(frames) >= MAX_HISTORY_CHUNKS:
            break

    if not frames:
        return pd.DataFrame()

    result = pd.concat(
        frames,
        ignore_index=True
    )

    result = result.drop_duplicates(
        "datetime"
    )

    result = result.sort_values(
        "datetime"
    )

    result = result.tail(
        total
    )

    result = result.reset_index(
        drop=True
    )

    return result


# =========================================================
# EMA 계산
# =========================================================

def add_ema(df):

    if df.empty:
        return df

    df = df.copy()

    df["ema10"] = ema(
        df["close"],
        10
    )

    df["ema30"] = ema(
        df["close"],
        30
    )

    df["ema60"] = ema(
        df["close"],
        60
    )

    df["ema120"] = ema(
        df["close"],
        120
    )

    return df


# =========================================================
# 방향
#
# LONG
# EMA10 > EMA30 > EMA60 > EMA120
#
# SHORT
# EMA10 < EMA30 < EMA60 < EMA120
# =========================================================

def direction(df):

    if df.empty:
        return "neutral"

    row = df.iloc[-1]

    if (
        row["ema10"] >
        row["ema30"] >
        row["ema60"] >
        row["ema120"]
    ):

        return "long"

    if (
        row["ema10"] <
        row["ema30"] <
        row["ema60"] <
        row["ema120"]
    ):

        return "short"

    return "neutral"


# =========================================================
# EMA 정배열 연속 카운트
# =========================================================

def ema_alignment_count(df):

    if df.empty:
        return 0

    current_direction = direction(
        df
    )

    if current_direction not in (
        "long",
        "short"
    ):
        return 0

    count = 0

    for i in range(
        len(df) - 1,
        -1,
        -1
    ):

        row = df.iloc[i]

        if current_direction == "long":

            ok = (
                row["ema10"] >
                row["ema30"] >
                row["ema60"] >
                row["ema120"]
            )

        else:

            ok = (
                row["ema10"] <
                row["ema30"] <
                row["ema60"] <
                row["ema120"]
            )

        if not ok:
            break

        count += 1

    return count


# =========================================================
# 1H 종가 ↔ EMA10 카운트
#
# 상승:
# 🟢(N)
#
# 하락:
# 🔻(N)
#
# 이전 카운트:
# 흰색 (N)
# =========================================================

def ema10_cross_count(df):

    if df.empty:

        return {
            "current_state": "-",
            "current_count": 0,
            "previous_count": 0
        }

    work = df.copy()

    work["ema10"] = ema(
        work["close"],
        10
    )

    states = []

    for _, row in work.iterrows():

        if row["close"] > row["ema10"]:

            states.append(
                "long"
            )

        elif row["close"] < row["ema10"]:

            states.append(
                "short"
            )

        else:

            states.append(
                "equal"
            )

    if not states:

        return {
            "current_state": "-",
            "current_count": 0,
            "previous_count": 0
        }

    current = states[-1]

    if current == "equal":

        if len(states) >= 2:
            current = states[-2]

        else:
            current = "equal"

    current_count = 0

    if current in (
        "long",
        "short"
    ):

        for state in reversed(
            states
        ):

            if state == current:

                current_count += 1

            else:

                break

    previous_count = 0

    if current_count > 0:

        previous_state = (
            "short"
            if current == "long"
            else "long"
        )

        end_index = (
            len(states)
            -
            current_count
        )

        for i in range(
            end_index - 1,
            -1,
            -1
        ):

            if states[i] == previous_state:

                previous_count += 1

            else:

                break

    return {

        "current_state":
            current,

        "current_count":
            current_count,

        "previous_count":
            previous_count
    }


# =========================================================
# ★ 비행기 기본 방향
#
# LONG:
# 1H EMA10 > EMA30 > EMA60 > EMA120
# AND
# 4H EMA10 > EMA30 > EMA60 > EMA120
#
# SHORT:
# 1H EMA10 < EMA30 < EMA60 < EMA120
# AND
# 4H EMA10 < EMA30 < EMA60 < EMA120
# =========================================================

def get_base_plane_direction(
    df1h,
    df4h
):

    direction1h = direction(
        df1h
    )

    direction4h = direction(
        df4h
    )

    if (
        direction1h == "long"
        and
        direction4h == "long"
    ):

        return "long"

    if (
        direction1h == "short"
        and
        direction4h == "short"
    ):

        return "short"

    return "neutral"


# =========================================================
# ★ 비행기 상태
#
# [LONG]
#
# 기본조건:
# 1H + 4H 정배열
#
# 현재가 > 확정 1H EMA10
# → ✈️ 사전경고
#
# 확정봉:
# 이전 종가 <= 이전 EMA10
# 현재 종가 > 현재 EMA10
# → ✈️(1)
#
# 이후:
# 종가 > EMA10
# → 카운트 +1
#
# 종가 < EMA10
# → ⛔️
#
#
# [SHORT]
#
# 기본조건:
# 1H + 4H 역배열
#
# 현재가 < 확정 1H EMA10
# → ✈️ 사전경고
#
# 확정봉:
# 이전 종가 >= 이전 EMA10
# 현재 종가 < 현재 EMA10
# → ✈️(1)
#
# 이후:
# 종가 < EMA10
# → 카운트 +1
#
# 종가 > EMA10
# → ⛔️
# =========================================================

def get_plane_state(
    market,
    df1h,
    df4h,
    current_price
):

    if (
        df1h.empty
        or
        df4h.empty
        or
        len(df1h) < 12
        or
        len(df4h) < 12
    ):

        return {

            "air_warning":
                False,

            "air_active":
                False,

            "air_count":
                0,

            "air_status":
                "-",

            "air_direction":
                "-",

            "air_candle":
                None
        }

    # -----------------------------------------------------
    # EMA 계산
    # -----------------------------------------------------

    df1 = df1h.copy()

    df1["ema10"] = ema(
        df1["close"],
        10
    )

    df4 = df4h.copy()

    df4["ema10"] = ema(
        df4["close"],
        10
    )

    # -----------------------------------------------------
    # 1H + 4H 기본 방향
    # -----------------------------------------------------

    base_direction = get_base_plane_direction(
        df1,
        df4
    )

    # -----------------------------------------------------
    # 마지막 확정 1H 봉
    # -----------------------------------------------------

    last = df1.iloc[-1]

    prev = df1.iloc[-2]

    last_time = last[
        "datetime"
    ]

    last_close = float(
        last["close"]
    )

    last_ema10 = float(
        last["ema10"]
    )

    prev_close = float(
        prev["close"]
    )

    prev_ema10 = float(
        prev["ema10"]
    )

    # -----------------------------------------------------
    # 기존 상태
    # -----------------------------------------------------

    state = air_state.get(
        market,
        {
            "active": False,
            "count": 0,
            "direction": None,
            "last_candle": None,
            "golden_candle": None,
            "ended_candle": None
        }
    )

    # -----------------------------------------------------
    # 새로운 확정 1H 봉인지 확인
    # -----------------------------------------------------

    is_new_candle = (
        state["last_candle"]
        !=
        last_time
    )

    # =====================================================
    # 새로운 확정봉 처리
    # =====================================================

    if is_new_candle:

        # =================================================
        # 이미 비행기 카운팅 중
        # =================================================

        if state["active"]:

            active_direction = state[
                "direction"
            ]

            # ---------------------------------------------
            # 기본 1H + 4H 방향이 깨진 경우
            # ---------------------------------------------

            if (
                base_direction
                !=
                active_direction
            ):

                state["active"] = False

                state["ended_candle"] = (
                    last_time
                )

                logging.info(
                    "%s | 기본 방향 종료 → ⛔️ | %s",
                    market,
                    active_direction
                )

            # ---------------------------------------------
            # LONG
            # ---------------------------------------------

            elif active_direction == "long":

                # 반대 종가
                if last_close < last_ema10:

                    state["active"] = False

                    state["ended_candle"] = (
                        last_time
                    )

                    logging.info(
                        "%s | LONG EMA10 아래 마감 → ⛔️ | 최종 %s",
                        market,
                        state["count"]
                    )

                # 같은 방향 종가
                elif last_close > last_ema10:

                    state["count"] += 1

                    logging.info(
                        "%s | LONG 지속 → ✈️(%s)",
                        market,
                        state["count"]
                    )

                # EMA10과 동일한 경우
                # 카운트 변화 없음
                else:

                    logging.info(
                        "%s | LONG EMA10 동일값 → 카운트 유지 %s",
                        market,
                        state["count"]
                    )

            # ---------------------------------------------
            # SHORT
            # ---------------------------------------------

            elif active_direction == "short":

                # 반대 종가
                if last_close > last_ema10:

                    state["active"] = False

                    state["ended_candle"] = (
                        last_time
                    )

                    logging.info(
                        "%s | SHORT EMA10 위 마감 → ⛔️ | 최종 %s",
                        market,
                        state["count"]
                    )

                # 같은 방향 종가
                elif last_close < last_ema10:

                    state["count"] += 1

                    logging.info(
                        "%s | SHORT 지속 → ✈️(%s)",
                        market,
                        state["count"]
                    )

                # EMA10과 동일한 경우
                else:

                    logging.info(
                        "%s | SHORT EMA10 동일값 → 카운트 유지 %s",
                        market,
                        state["count"]
                    )

        # =================================================
        # 아직 카운팅 전
        # =================================================

        else:

            # ---------------------------------------------
            # LONG 골든크로스
            # ---------------------------------------------

            if base_direction == "long":

                golden_cross = (
                    prev_close <= prev_ema10
                    and
                    last_close > last_ema10
                )

                if golden_cross:

                    state["active"] = True

                    state["count"] = 1

                    state["direction"] = (
                        "long"
                    )

                    state["golden_candle"] = (
                        last_time
                    )

                    state["ended_candle"] = None

                    logging.info(
                        "%s | LONG 골든크로스 확정 → ✈️(1)",
                        market
                    )

            # ---------------------------------------------
            # SHORT 데드크로스
            # ---------------------------------------------

            elif base_direction == "short":

                dead_cross = (
                    prev_close >= prev_ema10
                    and
                    last_close < last_ema10
                )

                if dead_cross:

                    state["active"] = True

                    state["count"] = 1

                    state["direction"] = (
                        "short"
                    )

                    state["golden_candle"] = (
                        last_time
                    )

                    state["ended_candle"] = None

                    logging.info(
                        "%s | SHORT 데드크로스 확정 → ✈️(1)",
                        market
                    )

        # -------------------------------------------------
        # 마지막 처리 캔들 기록
        # -------------------------------------------------

        state["last_candle"] = (
            last_time
        )

    # -----------------------------------------------------
    # 상태 저장
    # -----------------------------------------------------

    air_state[market] = state

    # =====================================================
    # 현재 카운팅 중
    # =====================================================

    if state["active"]:

        return {

            "air_warning":
                True,

            "air_active":
                True,

            "air_count":
                state["count"],

            "air_status":
                "COUNTING",

            "air_direction":
                state["direction"],

            "air_candle":
                state["golden_candle"]
        }

    # =====================================================
    # 종료 직후
    # =====================================================

    if (
        state["ended_candle"]
        is not None
        and
        state["ended_candle"]
        ==
        last_time
    ):

        return {

            "air_warning":
                True,

            "air_active":
                False,

            "air_count":
                state["count"],

            "air_status":
                "ENDED",

            "air_direction":
                "DEAD",

            "air_candle":
                last_time
        }

    # =====================================================
    # 확정 교차 전 사전경고
    #
    # LONG:
    # 현재가 > 마지막 확정 1H EMA10
    #
    # SHORT:
    # 현재가 < 마지막 확정 1H EMA10
    # =====================================================

    if base_direction == "long":

        live_warning = (
            current_price is not None
            and
            current_price > last_ema10
        )

        if live_warning:

            return {

                "air_warning":
                    True,

                "air_active":
                    False,

                "air_count":
                    0,

                "air_status":
                    "PRE",

                "air_direction":
                    "long",

                "air_candle":
                    None
            }

    elif base_direction == "short":

        live_warning = (
            current_price is not None
            and
            current_price < last_ema10
        )

        if live_warning:

            return {

                "air_warning":
                    True,

                "air_active":
                    False,

                "air_count":
                    0,

                "air_status":
                    "PRE",

                "air_direction":
                    "short",

                "air_candle":
                    None
            }

    # =====================================================
    # 아무 상태 없음
    # =====================================================

    return {

        "air_warning":
            False,

        "air_active":
            False,

        "air_count":
            0,

        "air_status":
            "-",

        "air_direction":
            "-",

        "air_candle":
            None
    }


# =========================================================
# 비행기 표시
# =========================================================

def warning_html(row):

    status = row.get(
        "air_status",
        "-"
    )

    count = row.get(
        "air_count",
        0
    )

    # -----------------------------------------------------
    # 종료
    # -----------------------------------------------------

    if status == "ENDED":

        return """
        <div class="warning ended">
            ⛔️
        </div>
        """

    # -----------------------------------------------------
    # 카운팅
    # -----------------------------------------------------

    if status == "COUNTING":

        return """
        <div class="warning counting">
            ✈️<span class="air-count">({})</span>
        </div>
        """.format(
            count
        )

    # -----------------------------------------------------
    # 사전 경고
    # -----------------------------------------------------

    if status == "PRE":

        return """
        <div class="warning pre">
            ✈️
        </div>
        """

    return """
    <div class="warning empty">
        -
    </div>
    """


# =========================================================
# 10선 표시
# =========================================================

def ema10_html(info):

    state = info[
        "ema10_state"
    ]

    count = info[
        "ema10_count"
    ]

    previous = info[
        "ema10_previous"
    ]

    # -----------------------------------------------------
    # 상승
    # -----------------------------------------------------

    if state == "long":

        return """
        <div class="ema10-wrap">

            <div class="ema10-up">
                🟢({})
            </div>

            <div class="ema10-prev">
                ({})
            </div>

        </div>
        """.format(
            count,
            previous
        )

    # -----------------------------------------------------
    # 하락
    # -----------------------------------------------------

    if state == "short":

        return """
        <div class="ema10-wrap">

            <div class="ema10-down">
                🔻({})
            </div>

            <div class="ema10-prev">
                ({})
            </div>

        </div>
        """.format(
            count,
            previous
        )

    return "-"


# =========================================================
# EMA 방향 표시
# =========================================================

def ema_direction_html(
    df1h,
    df4h
):

    d1 = direction(
        df1h
    )

    d4 = direction(
        df4h
    )

    c1 = ema_alignment_count(
        df1h
    )

    c4 = ema_alignment_count(
        df4h
    )

    def one(
        direction_value,
        count
    ):

        if direction_value == "long":

            return "🟢({})".format(
                count
            )

        if direction_value == "short":

            return "🔻({})".format(
                count
            )

        return "-"

    return (
        "<div>1H {}</div>"
        "<div>4H {}</div>"
    ).format(
        one(d1, c1),
        one(d4, c4)
    )


# =========================================================
# 분석
# =========================================================

def analyze(
    market,
    current_price
):

    try:

        # -------------------------------------------------
        # 1H
        # -------------------------------------------------

        df1h = history_upbit(
            market,
            unit=60,
            total=500
        )

        if df1h.empty:
            return None

        df1h = add_ema(
            df1h
        )

        # -------------------------------------------------
        # 4H
        # -------------------------------------------------

        df4h = history_upbit(
            market,
            unit=240,
            total=300
        )

        if df4h.empty:
            return None

        df4h = add_ema(
            df4h
        )

        # -------------------------------------------------
        # 10선 카운트
        # -------------------------------------------------

        ema10_info = (
            ema10_cross_count(
                df1h
            )
        )

        # -------------------------------------------------
        # ★ 비행기
        # -------------------------------------------------

        plane = get_plane_state(
            market,
            df1h,
            df4h,
            current_price
        )

        # -------------------------------------------------
        # 현재 EMA
        # -------------------------------------------------

        last1 = df1h.iloc[-1]

        last4 = df4h.iloc[-1]

        return {

            "market":
                market,

            "coin":
                market.replace(
                    "KRW-",
                    ""
                ),

            "current_price":
                current_price,

            "ema1h_10":
                float(
                    last1["ema10"]
                ),

            "ema4h_10":
                float(
                    last4["ema10"]
                ),

            "ema_direction":
                ema_direction_html(
                    df1h,
                    df4h
                ),

            "ema10_state":
                ema10_info[
                    "current_state"
                ],

            "ema10_count":
                ema10_info[
                    "current_count"
                ],

            "ema10_previous":
                ema10_info[
                    "previous_count"
                ],

            "air_warning":
                plane[
                    "air_warning"
                ],

            "air_active":
                plane[
                    "air_active"
                ],

            "air_count":
                plane[
                    "air_count"
                ],

            "air_status":
                plane[
                    "air_status"
                ],

            "air_direction":
                plane[
                    "air_direction"
                ],

            "air_candle":
                plane[
                    "air_candle"
                ],

            "qualified":
                plane[
                    "air_warning"
                ]
        }

    except Exception as e:

        logging.warning(
            "%s 분석 실패: %s",
            market,
            e
        )

        return None


# =========================================================
# Upbit 업데이트
# =========================================================

def update_upbit():

    global latest_data
    global last_update_time

    logging.info(
        "===== Upbit 업데이트 시작 ====="
    )

    markets = get_upbit_markets()

    if not markets:

        logging.warning(
            "Upbit 시장 데이터를 가져오지 못했습니다."
        )

        return

    # -----------------------------------------------------
    # 거래대금 순
    # -----------------------------------------------------

    markets.sort(
        key=lambda x:
            x["volume_24h"],
        reverse=True
    )

    markets = markets[
        :TOP_N
    ]

    results = []

    plane_count = 0

    for rank, item in enumerate(
        markets,
        start=1
    ):

        market = item[
            "market"
        ]

        current_price = item[
            "current_price"
        ]

        result = analyze(
            market,
            current_price
        )

        if result is None:
            continue

        result[
            "rank"
        ] = rank

        result[
            "change_rate"
        ] = item[
            "change_rate"
        ]

        result[
            "volume_24h"
        ] = item[
            "volume_24h"
        ]

        if result[
            "air_warning"
        ]:

            plane_count += 1

        results.append(
            result
        )

        time.sleep(
            REQUEST_INTERVAL
        )

    with data_lock:

        latest_data = results

        last_update_time = (
            datetime.now(KST)
        )

    logging.info(
        "===== 업데이트 완료 | "
        "%s개 | 비행기 %s개 =====",
        len(results),
        plane_count
    )


# =========================================================
# 거래대금 표시
# =========================================================

def format_volume(
    value
):

    try:

        value = float(
            value
        )

        if value >= 100_000_000_000:

            return (
                "{:.1f}천억"
            ).format(
                value / 100_000_000_000
            )

        if value >= 100_000_000:

            return (
                "{:.1f}억"
            ).format(
                value / 100_000_000
            )

        if value >= 10_000:

            return (
                "{:.1f}만"
            ).format(
                value / 10_000
            )

        return "{:,.0f}".format(
            value
        )

    except Exception:

        return "-"


# =========================================================
# 가격 표시
# =========================================================

def format_price(
    value
):

    if value is None:
        return "-"

    try:

        value = float(
            value
        )

        if value >= 1000:

            return "{:,.0f}".format(
                value
            )

        if value >= 1:

            return "{:,.2f}".format(
                value
            )

        return "{:.6f}".format(
            value
        )

    except Exception:

        return "-"


# =========================================================
# Row
# =========================================================

def make_row(
    row
):

    change = row.get(
        "change_rate",
        0
    )

    try:

        change = float(
            change
        )

    except Exception:

        change = 0

    if change > 0:

        change_class = "up"

    elif change < 0:

        change_class = "down"

    else:

        change_class = ""

    return """
<tr>

<td>
{rank}
</td>

<td class="coin">
{coin}
</td>

<td class="{change_class}">
{change:+.2f}%
</td>

<td>
{volume}
</td>

<td class="ema-direction">
{ema_direction}
</td>

<td>
{ema10}
</td>

<td>
{warning}
</td>

</tr>
""".format(

        rank=row.get(
            "rank",
            "-"
        ),

        coin=row.get(
            "coin",
            "-"
        ),

        change_class=change_class,

        change=change,

        volume=format_volume(
            row.get(
                "volume_24h",
                0
            )
        ),

        ema_direction=row.get(
            "ema_direction",
            "-"
        ),

        ema10=ema10_html(
            row
        ),

        warning=warning_html(
            row
        )
    )


# =========================================================
# HTML
#
# 중요:
# HTML 전체에 .format()을 사용하지 않습니다.
#
# CSS의 { } 때문에 KeyError가 발생했던 문제를
# 완전히 제거합니다.
#
# rows만 __ROWS__로 replace합니다.
# =========================================================

HTML = """

<!DOCTYPE html>

<html lang="ko">

<head>

<meta charset="UTF-8">

<meta name="viewport"
      content="width=device-width, initial-scale=1.0">

<title>
EMA Dashboard
</title>

<style>

* {
    box-sizing: border-box;
}

body {

    margin: 0;

    background: #111;

    color: #eee;

    font-family:
        Arial,
        "Noto Sans KR",
        sans-serif;
}

.header {

    padding: 14px 12px;

    background: #181818;

    border-bottom:
        1px solid #333;
}

.title {

    font-size: 20px;

    font-weight: bold;
}

.info {

    margin-top: 8px;

    color: #aaa;

    font-size: 12px;

    line-height: 1.8;
}

.container {

    padding: 10px;

    overflow-x: auto;
}

table {

    width: 100%;

    min-width: 900px;

    border-collapse:
        collapse;

    table-layout: fixed;
}

th {

    background: #222;

    color: #ccc;

    font-size: 12px;

    padding: 9px 4px;

    border-bottom:
        1px solid #444;
}

td {

    text-align: center;

    padding: 8px 4px;

    border-bottom:
        1px solid #292929;

    font-size: 13px;
}

.rank {

    width: 42px;
}

.coin {

    width: 100px;

    font-weight: bold;
}

.change {

    width: 70px;
}

.volume {

    width: 100px;
}

.ema {

    width: 130px;
}

.ema10 {

    width: 100px;
}

.warning-col {

    width: 90px;
}

.up {

    color: #00d084;
}

.down {

    color: #ff4d4d;
}

.ema-direction {

    line-height: 1.8;

    font-size: 12px;
}

.ema10-wrap {

    display: flex;

    flex-direction: column;

    align-items: center;

    justify-content: center;

    min-height: 42px;
}

.ema10-up {

    color: #00d084;

    font-size: 13px;

    white-space: nowrap;
}

.ema10-down {

    color: #ff4d4d;

    font-size: 13px;

    white-space: nowrap;
}

.ema10-prev {

    color: #fff;

    font-size: 11px;

    margin-top: 2px;
}

.warning {

    font-size: 22px;

    min-height: 32px;

    display: flex;

    align-items: center;

    justify-content: center;
}

.warning.pre {

    animation:
        planePulse 1.4s infinite;
}

.warning.counting {

    animation:
        planePulse 1.2s infinite;
}

.warning.ended {

    color: #fff;

    font-size: 20px;
}

.air-count {

    color: #fff;

    font-size: 13px;

    margin-left: 2px;
}

@keyframes planePulse {

    0% {
        transform: scale(1);
    }

    50% {
        transform: scale(1.12);
    }

    100% {
        transform: scale(1);
    }

}

.footer {

    padding: 15px;

    color: #777;

    font-size: 11px;

    line-height: 1.8;
}

</style>

<script>

setTimeout(
    function() {
        location.reload();
    },
    60000
);

</script>

</head>

<body>

<div class="header">

<div class="title">
🏆 Upbit EMA Dashboard
</div>

<div class="info">

① 비행기 기본조건 =
1H + 4H 같은 방향 정배열/역배열<br>

② LONG =
1H EMA10 > EMA30 > EMA60 > EMA120
+
4H EMA10 > EMA30 > EMA60 > EMA120<br>

③ SHORT =
1H EMA10 < EMA30 < EMA60 < EMA120
+
4H EMA10 < EMA30 < EMA60 < EMA120<br>

④ 기본조건 + 현재가격이 확정 1H EMA10 위/아래
→ ✈️ 사전경고<br>

⑤ 확정 1H 종가가 EMA10 교차
→ ✈️(1)부터 카운팅<br>

⑥ 이후 같은 방향 종가 유지
→ ✈️(2), ✈️(3)...<br>

⑦ 반대 종가 또는 기본방향 종료
→ ⛔️

</div>

</div>

<div class="container">

<table>

<thead>

<tr>

<th class="rank">
순위
</th>

<th class="coin">
코인
</th>

<th class="change">
등락
</th>

<th class="volume">
24H 거래대금
</th>

<th class="ema">
EMA 정배열
</th>

<th class="ema10">
10선
</th>

<th class="warning-col">
비행기
</th>

</tr>

</thead>

<tbody>

__ROWS__

</tbody>

</table>

</div>

<div class="footer">

비행기 기본 방향은
1시간 EMA10·30·60·120과
4시간 EMA10·30·60·120을 동시에 사용합니다.<br>

롱:
1H 정배열 + 4H 정배열<br>

숏:
1H 역배열 + 4H 역배열<br>

현재가격 사전경고는 마지막 확정 1시간봉 EMA10을 기준으로 합니다.<br>

종가 교차가 확정된 봉부터 ✈️(1)로 시작합니다.<br>

이후 같은 방향 종가마다 카운트가 1씩 증가합니다.<br>

종가가 반대쪽으로 마감되면 ⛔️로 종료합니다.

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

    with data_lock:

        rows = list(
            latest_data
        )

        update_time = (
            last_update_time
        )

    if rows:

        html_rows = "".join(
            make_row(row)
            for row in rows
        )

    else:

        html_rows = """

        <tr>

        <td colspan="7">
        데이터 준비 중...
        </td>

        </tr>

        """

    # =====================================================
    # ★ 핵심 수정
    #
    # 기존:
    #
    # html = HTML.format(
    #     rows=html_rows
    # )
    #
    # CSS의 { }가 format()에 의해 해석되어
    #
    # KeyError: '\\n    box-sizing'
    #
    # 발생.
    #
    # 이제 CSS 전체를 format()하지 않고
    # rows 자리만 replace.
    # =====================================================

    html = HTML.replace(
        "__ROWS__",
        html_rows
    )

    if update_time:

        update_text = (
            update_time.strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        )

        html = html.replace(
            '<div class="title">',
            """
            <div style="
                color:#777;
                font-size:11px;
                margin-bottom:5px;
            ">
            업데이트:
            {}
            </div>

            <div class="title">
            """.format(
                update_text
            ),
            1
        )

    return HTMLResponse(
        content=html
    )


# =========================================================
# 스케줄러
# =========================================================

def scheduler_loop():

    logging.info(
        "스케줄러 시작"
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
# 최초 업데이트
# =========================================================

def initial_update():

    try:

        update_upbit()

    except Exception as e:

        logging.error(
            "초기 업데이트 실패: %s",
            e
        )


# =========================================================
# 실행
# =========================================================

if __name__ == "__main__":

    logging.info(
        "=============================================="
    )

    logging.info(
        "EMA 비행기 대시보드 시작"
    )

    logging.info(
        "Python 3.9 호환 모드"
    )

    logging.info(
        "HTML.format() 사용 안 함"
    )

    logging.info(
        "LONG = 1H 정배열 + 4H 정배열"
    )

    logging.info(
        "SHORT = 1H 역배열 + 4H 역배열"
    )

    logging.info(
        "현재가격 EMA10 상회/하회 → ✈️ 사전경고"
    )

    logging.info(
        "확정 종가 교차 → ✈️(1)"
    )

    logging.info(
        "이후 같은 방향 종가 → 카운트 +1"
    )

    logging.info(
        "반대 종가 → ⛔️"
    )

    logging.info(
        "=============================================="
    )

    threading.Thread(
        target=initial_update,
        daemon=True
    ).start()

    schedule.every(
        UPDATE_MINUTES
    ).minutes.do(
        update_upbit
    )

    threading.Thread(
        target=scheduler_loop,
        daemon=True
    ).start()

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
                )
