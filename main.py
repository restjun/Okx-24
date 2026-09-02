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

VOLUME_HOURS = 24
TOP_N = 100
UPDATE_MINUTES = 1

INITIAL_CANDLE_COUNT = 200
HISTORY_CHUNK = 200
MAX_HISTORY_CHUNKS = 10

USE_UPBIT = "Y"
USE_OKX = "N"

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10

KST = ZoneInfo("Asia/Seoul")


# =========================================================
# 전역 변수
# =========================================================

latest_upbit_data = []
latest_okx_data = []

latest_usdt_krw = 0

latest_upbit_update_time = "-"
latest_okx_update_time = "-"

latest_upbit_markets = []

request_lock = threading.Lock()
update_lock = threading.Lock()

# 상승/하락 구간 상태
section_state_lock = threading.Lock()

last_request_time = 0

section_state = {}


# =========================================================
# 시간
# =========================================================

def kst():
    return datetime.now(KST)


# =========================================================
# 요청 속도 제어
# =========================================================

def wait_request():

    global last_request_time

    with request_lock:

        now = time.time()

        diff = now - last_request_time

        if diff < REQUEST_INTERVAL:
            time.sleep(REQUEST_INTERVAL - diff)

        last_request_time = time.time()


# =========================================================
# Retry
# =========================================================

def retry(func, *args, **kwargs):

    for attempt in range(MAX_RETRIES):

        try:

            wait_request()

            result = func(*args, **kwargs)

            if result is not None:
                return result

        except Exception as e:

            logging.warning(
                f"요청 실패 {attempt + 1}/{MAX_RETRIES}: {e}"
            )

            if attempt < MAX_RETRIES - 1:
                time.sleep(RATE_LIMIT_WAIT)

    return None


# =========================================================
# Upbit 마켓
# =========================================================

def get_upbit_markets():

    url = "https://api.upbit.com/v1/market/all"

    def request():

        r = requests.get(
            url,
            params={"isDetails": "false"},
            timeout=10
        )

        r.raise_for_status()

        return r.json()

    data = retry(request)

    if not data:
        return []

    return [
        x["market"]
        for x in data
        if x["market"].startswith("KRW-")
    ]


# =========================================================
# USDT / KRW
# =========================================================

def get_usdt_krw():

    url = "https://api.upbit.com/v1/ticker"

    def request():

        r = requests.get(
            url,
            params={"markets": "KRW-USDT"},
            timeout=10
        )

        r.raise_for_status()

        data = r.json()

        if data:
            return float(data[0]["trade_price"])

        return None

    value = retry(request)

    return value if value else 0


# =========================================================
# Upbit 1시간봉
# =========================================================

def get_upbit_1h_ohlcv(market, count=200):

    url = "https://api.upbit.com/v1/candles/minutes/60"

    def request():

        r = requests.get(
            url,
            params={
                "market": market,
                "count": count
            },
            timeout=10
        )

        r.raise_for_status()

        return r.json()

    data = retry(request)

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    if df.empty:
        return df

    df["candle_date_time_kst"] = pd.to_datetime(
        df["candle_date_time_kst"]
    )

    df = df.sort_values(
        "candle_date_time_kst"
    ).reset_index(drop=True)

    df.rename(
        columns={
            "opening_price": "open",
            "high_price": "high",
            "low_price": "low",
            "trade_price": "close",
            "candle_acc_trade_volume": "volume",
            "candle_acc_trade_price": "value"
        },
        inplace=True
    )

    return df


# =========================================================
# Upbit 4시간봉
# =========================================================

def get_upbit_4h_ohlcv(market, count=200):

    url = "https://api.upbit.com/v1/candles/minutes/240"

    def request():

        r = requests.get(
            url,
            params={
                "market": market,
                "count": count
            },
            timeout=10
        )

        r.raise_for_status()

        return r.json()

    data = retry(request)

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    if df.empty:
        return df

    df["candle_date_time_kst"] = pd.to_datetime(
        df["candle_date_time_kst"]
    )

    df = df.sort_values(
        "candle_date_time_kst"
    ).reset_index(drop=True)

    df.rename(
        columns={
            "opening_price": "open",
            "high_price": "high",
            "low_price": "low",
            "trade_price": "close",
            "candle_acc_trade_volume": "volume",
            "candle_acc_trade_price": "value"
        },
        inplace=True
    )

    return df


# =========================================================
# OKX 1시간봉
# =========================================================

def get_okx_ohlcv(inst_id, bar="1H", limit=200):

    url = "https://www.okx.com/api/v5/market/candles"

    def request():

        r = requests.get(
            url,
            params={
                "instId": inst_id,
                "bar": bar,
                "limit": limit
            },
            timeout=10
        )

        r.raise_for_status()

        data = r.json()

        if data.get("code") != "0":
            return None

        return data.get("data", [])

    data = retry(request)

    if not data:
        return pd.DataFrame()

    rows = []

    for x in data:

        if len(x) < 9:
            continue

        rows.append({
            "timestamp": int(x[0]),
            "open": float(x[1]),
            "high": float(x[2]),
            "low": float(x[3]),
            "close": float(x[4]),
            "volume": float(x[5]),
            "volCcyQuote": float(x[7])
        })

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df["datetime"] = pd.to_datetime(
        df["timestamp"],
        unit="ms"
    )

    df = df.sort_values(
        "datetime"
    ).reset_index(drop=True)

    return df


# =========================================================
# EMA
# =========================================================

def ema(df, period):

    if df.empty:
        return pd.Series(dtype=float)

    return df["close"].ewm(
        span=period,
        adjust=False
    ).mean()


# =========================================================
# EMA 10-30-60-120 방향
# =========================================================

def direction(df):

    if df.empty or len(df) < 120:
        return "neutral"

    e10 = ema(df, 10)
    e30 = ema(df, 30)
    e60 = ema(df, 60)
    e120 = ema(df, 120)

    if (
        e10.iloc[-1] >
        e30.iloc[-1] >
        e60.iloc[-1] >
        e120.iloc[-1]
    ):
        return "long"

    if (
        e10.iloc[-1] <
        e30.iloc[-1] <
        e60.iloc[-1] <
        e120.iloc[-1]
    ):
        return "short"

    return "neutral"


# =========================================================
# EMA 정배열 연속 카운트
# =========================================================

def ema_alignment_count(df):

    if df.empty or len(df) < 120:

        return {
            "direction": "neutral",
            "count": 0
        }

    e10 = ema(df, 10)
    e30 = ema(df, 30)
    e60 = ema(df, 60)
    e120 = ema(df, 120)

    current_direction = "neutral"

    if (
        e10.iloc[-1] >
        e30.iloc[-1] >
        e60.iloc[-1] >
        e120.iloc[-1]
    ):
        current_direction = "long"

    elif (
        e10.iloc[-1] <
        e30.iloc[-1] <
        e60.iloc[-1] <
        e120.iloc[-1]
    ):
        current_direction = "short"

    if current_direction == "neutral":

        return {
            "direction": "neutral",
            "count": 0
        }

    count = 0

    for i in range(len(df) - 1, -1, -1):

        if current_direction == "long":

            ok = (
                e10.iloc[i] >
                e30.iloc[i] >
                e60.iloc[i] >
                e120.iloc[i]
            )

        else:

            ok = (
                e10.iloc[i] <
                e30.iloc[i] <
                e60.iloc[i] <
                e120.iloc[i]
            )

        if ok:
            count += 1
        else:
            break

    return {
        "direction": current_direction,
        "count": count
    }


# =========================================================
# EMA 표시
# =========================================================

def ema_display(df):

    result = ema_alignment_count(df)

    if result["direction"] == "long":

        return f"🟢({result['count']})"

    if result["direction"] == "short":

        return f"🔴({result['count']})"

    return "⚪(0)"


# =========================================================
# 10선 하락구간
#
# EMA1 = 1시간봉 종가
# EMA1 < EMA10 이면 하락구간
#
# 현재 진행 중이면 🔻
# 종료되면 마지막 카운트를 회색으로 유지
# =========================================================

def get_down_section(df):

    empty = {
        "count": 0,
        "active": False,
        "display": "⚪(0)"
    }

    if df.empty or len(df) < 2:
        return empty

    e10 = ema(df, 10)

    close = df["close"]

    if pd.isna(e10.iloc[-1]):
        return empty

    current_below = (
        close.iloc[-1] < e10.iloc[-1]
    )

    # -----------------------------------------------------
    # 현재 하락구간 진행 중
    # -----------------------------------------------------

    if current_below:

        count = 0

        for i in range(len(df) - 1, -1, -1):

            if close.iloc[i] < e10.iloc[i]:

                count += 1

            else:

                break

        return {
            "count": count,
            "active": True,
            "display": f"🔻({count})"
        }

    # -----------------------------------------------------
    # 현재 하락구간 종료
    #
    # 과거 마지막 하락구간의 최종 카운트를 찾는다.
    # -----------------------------------------------------

    last_count = 0
    count = 0

    for i in range(len(df) - 1, -1, -1):

        if close.iloc[i] < e10.iloc[i]:

            count += 1

        else:

            if count > 0:
                last_count = count

            break

    if last_count > 0:

        return {
            "count": last_count,
            "active": False,
            "display": f"🔻({last_count})"
        }

    return empty


# =========================================================
# EMA1 ↗ EMA10 골든크로스
# EMA1 = 종가
#
# 골든크로스:
# 이전 종가 <= 이전 EMA10
# 현재 종가 > 현재 EMA10
# =========================================================

def is_golden_cross(df):

    if df.empty or len(df) < 2:
        return False

    e10 = ema(df, 10)

    prev_close = df["close"].iloc[-2]
    curr_close = df["close"].iloc[-1]

    prev_ema10 = e10.iloc[-2]
    curr_ema10 = e10.iloc[-1]

    if pd.isna(prev_ema10) or pd.isna(curr_ema10):
        return False

    return (
        prev_close <= prev_ema10
        and
        curr_close > curr_ema10
    )


# =========================================================
# 상승구간 / 로켓 카운터
#
# 골든크로스 → 🚀(1)
# 이후 양봉 마감 → +1
# 종가 < EMA10 → ⛔️ 종료
# =========================================================

def update_rocket_counter(market, df):

    result = {
        "active": False,
        "count": 0,
        "rocket": False,
        "stopped": False,
        "display": "-"
    }

    if df.empty or len(df) < 2:
        return result

    e10 = ema(df, 10)

    current_close = df["close"].iloc[-1]
    current_open = df["open"].iloc[-1]
    current_ema10 = e10.iloc[-1]

    candle_time = str(
        df["candle_date_time_kst"].iloc[-1]
        if "candle_date_time_kst" in df.columns
        else df.index[-1]
    )

    golden = is_golden_cross(df)

    with section_state_lock:

        state = section_state.get(
            market,
            {
                "active": False,
                "count": 0,
                "last_candle": None,
                "stopped": False
            }
        )

        # -------------------------------------------------
        # 같은 캔들에서 중복 카운팅 방지
        # -------------------------------------------------

        new_candle = (
            state.get("last_candle") != candle_time
        )

        # -------------------------------------------------
        # ① 골든크로스 발생
        # -------------------------------------------------

        if golden:

            state["active"] = True
            state["count"] = 1
            state["stopped"] = False
            state["last_candle"] = candle_time

            section_state[market] = state

            return {
                "active": True,
                "count": 1,
                "rocket": True,
                "stopped": False,
                "display": "🚀(1)"
            }

        # -------------------------------------------------
        # ② 상승구간 진행 중
        # -------------------------------------------------

        if state.get("active"):

            # ---------------------------------------------
            # 종가가 EMA10 아래 마감
            # ---------------------------------------------

            if current_close < current_ema10:

                state["active"] = False
                state["stopped"] = True
                state["last_candle"] = candle_time

                section_state[market] = state

                return {
                    "active": False,
                    "count": state["count"],
                    "rocket": False,
                    "stopped": True,
                    "display": "⛔️"
                }

            # ---------------------------------------------
            # 새로운 캔들에서 양봉이면 카운트 증가
            # ---------------------------------------------

            if new_candle:

                if current_close > current_open:

                    state["count"] += 1

                state["last_candle"] = candle_time

                section_state[market] = state

            return {
                "active": True,
                "count": state["count"],
                "rocket": True,
                "stopped": False,
                "display": f"🚀({state['count']})"
            }

        # -------------------------------------------------
        # ③ 이전 상승구간 종료 후 ⛔️ 유지
        # -------------------------------------------------

        if state.get("stopped"):

            return {
                "active": False,
                "count": state.get("count", 0),
                "rocket": False,
                "stopped": True,
                "display": "⛔️"
            }

        # -------------------------------------------------
        # ④ 새로운 상승구간 없음
        # -------------------------------------------------

        return result


# =========================================================
# 일봉 상승률
# =========================================================

def daily_change_upbit(market):

    url = "https://api.upbit.com/v1/candles/days"

    def request():

        r = requests.get(
            url,
            params={
                "market": market,
                "count": 2
            },
            timeout=10
        )

        r.raise_for_status()

        return r.json()

    data = retry(request)

    if not data or len(data) < 2:
        return 0

    prev_close = float(data[1]["trade_price"])
    current_close = float(data[0]["trade_price"])

    if prev_close == 0:
        return 0

    return (
        (current_close - prev_close)
        / prev_close
        * 100
    )


# =========================================================
# 변화율
# =========================================================

def daily_changes(markets):

    result = {}

    for market in markets:

        try:
            result[market] = daily_change_upbit(
                market
            )

        except Exception:
            result[market] = 0

    return result


# =========================================================
# 거래대금
# =========================================================

def get_upbit_volume():

    markets = get_upbit_markets()

    if not markets:
        return []

    result = []

    ticker_url = "https://api.upbit.com/v1/ticker"

    for start in range(0, len(markets), 100):

        batch = markets[start:start + 100]

        def request():

            r = requests.get(
                ticker_url,
                params={
                    "markets": ",".join(batch)
                },
                timeout=10
            )

            r.raise_for_status()

            return r.json()

        data = retry(request)

        if not data:
            continue

        for item in data:

            market = item["market"]

            volume = float(
                item.get(
                    "acc_trade_price_24h",
                    0
                )
            )

            result.append({
                "market": market,
                "volume": volume,
                "change": float(
                    item.get(
                        "signed_change_rate",
                        0
                    )
                ) * 100
            })

    result.sort(
        key=lambda x: x["volume"],
        reverse=True
    )

    return result[:TOP_N]


# =========================================================
# 숫자 표시
# =========================================================

def format_volume(value):

    if value is None:
        return "-"

    value = float(value)

    if value >= 1_000_000_000_000:
        return f"{value / 1_000_000_000_000:.2f}조"

    if value >= 100_000_000:
        return f"{value / 100_000_000:.0f}억"

    if value >= 10_000:
        return f"{value / 10_000:.0f}만"

    return f"{value:,.0f}"


def format_change(value):

    if value is None:
        return "-"

    value = float(value)

    if value > 0:
        return f"+{value:.2f}%"

    return f"{value:.2f}%"


# =========================================================
# 분석 결과
# =========================================================

def empty_analysis():

    return {
        "ema_1h": "⚪(0)",
        "ema_4h": "⚪(0)",

        "down_count": 0,
        "down_active": False,
        "down_display": "⚪(0)",

        "rocket_active": False,
        "rocket_count": 0,
        "rocket_stopped": False,
        "rocket_display": "-",

        "qualified": False,

        "direction_1h": "neutral",
        "direction_4h": "neutral"
    }


# =========================================================
# 코인 분석
# =========================================================

def analyze(market):

    try:

        df1h = get_upbit_1h_ohlcv(
            market,
            INITIAL_CANDLE_COUNT
        )

        df4h = get_upbit_4h_ohlcv(
            market,
            INITIAL_CANDLE_COUNT
        )

        if df1h.empty or df4h.empty:
            return empty_analysis()

        if len(df1h) < 120 or len(df4h) < 120:
            return empty_analysis()

        # -------------------------------------------------
        # EMA
        # -------------------------------------------------

        ema_1h_display = ema_display(df1h)
        ema_4h_display = ema_display(df4h)

        direction_1h = direction(df1h)
        direction_4h = direction(df4h)

        # -------------------------------------------------
        # 10선 하락구간
        # -------------------------------------------------

        down = get_down_section(df1h)

        # -------------------------------------------------
        # 상승구간 / 로켓
        # -------------------------------------------------

        rocket = update_rocket_counter(
            market,
            df1h
        )

        # -------------------------------------------------
        # 1H + 4H 정배열
        # -------------------------------------------------

        qualified = (
            direction_1h == "long"
            and
            direction_4h == "long"
        )

        return {

            "ema_1h": ema_1h_display,
            "ema_4h": ema_4h_display,

            "down_count": down["count"],
            "down_active": down["active"],
            "down_display": down["display"],

            "rocket_active": rocket["active"],
            "rocket_count": rocket["count"],
            "rocket_stopped": rocket["stopped"],
            "rocket_display": rocket["display"],

            "qualified": qualified,

            "direction_1h": direction_1h,
            "direction_4h": direction_4h
        }

    except Exception as e:

        logging.error(
            f"{market} 분석 오류: {e}"
        )

        return empty_analysis()


# =========================================================
# Upbit 데이터 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update_time
    global latest_upbit_markets

    try:

        volume_data = get_upbit_volume()

        if not volume_data:
            return

        latest_upbit_markets = [
            x["market"]
            for x in volume_data
        ]

        changes = daily_changes(
            latest_upbit_markets
        )

        rows = []

        for rank, item in enumerate(
            volume_data,
            start=1
        ):

            market = item["market"]

            name = market.replace(
                "KRW-",
                ""
            )

            analysis = analyze(
                market
            )

            rows.append({

                "rank": rank,

                "market": market,

                "name": name,

                "change": changes.get(
                    market,
                    item["change"]
                ),

                "volume": item["volume"],

                "ema_1h": analysis["ema_1h"],

                "ema_4h": analysis["ema_4h"],

                "down_display": analysis[
                    "down_display"
                ],

                "down_active": analysis[
                    "down_active"
                ],

                "rocket_display": analysis[
                    "rocket_display"
                ],

                "rocket_active": analysis[
                    "rocket_active"
                ],

                "rocket_stopped": analysis[
                    "rocket_stopped"
                ],

                "qualified": analysis[
                    "qualified"
                ]
            })

        latest_upbit_data = rows

        latest_upbit_update_time = (
            kst().strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        )

        logging.info(
            f"Upbit 업데이트 완료: {len(rows)}개"
        )

    except Exception as e:

        logging.error(
            f"Upbit 업데이트 오류: {e}"
        )


# =========================================================
# OKX 업데이트
# =========================================================

def update_okx():

    global latest_okx_data
    global latest_okx_update_time

    if USE_OKX != "Y":
        return

    try:

        # 필요 시 기존 OKX 로직을 연결
        latest_okx_data = []

        latest_okx_update_time = (
            kst().strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        )

    except Exception as e:

        logging.error(
            f"OKX 업데이트 오류: {e}"
        )


# =========================================================
# 전체 업데이트
# =========================================================

def update_all():

    global latest_usdt_krw

    if not update_lock.acquire(
        blocking=False
    ):
        return

    try:

        logging.info(
            "========== 데이터 업데이트 시작 =========="
        )

        latest_usdt_krw = get_usdt_krw()

        if USE_UPBIT == "Y":
            update_upbit()

        if USE_OKX == "Y":
            update_okx()

        logging.info(
            "========== 데이터 업데이트 완료 =========="
        )

    except Exception as e:

        logging.error(
            f"전체 업데이트 오류: {e}"
        )

    finally:

        update_lock.release()


# =========================================================
# HTML
# =========================================================

def ema_html(value):

    if not value:
        return "-"

    return f"""
    <div class="ema-cell">
        {value}
    </div>
    """


# =========================================================
# 10선 하락구간 HTML
# =========================================================

def down_section_html(row):

    value = row.get(
        "down_display",
        "⚪(0)"
    )

    active = row.get(
        "down_active",
        False
    )

    if active:

        return f"""
        <div class="down-cell active-down">
            {value}
        </div>
        """

    # 종료된 하락구간
    # 마지막 카운트를 회색으로 유지

    if value != "⚪(0)":

        return f"""
        <div class="down-cell finished-down">
            {value}
        </div>
        """

    return """
    <div class="down-cell empty-down">
        ⚪(0)
    </div>
    """


# =========================================================
# 상승구간 HTML
# =========================================================

def rocket_html(row):

    value = row.get(
        "rocket_display",
        "-"
    )

    active = row.get(
        "rocket_active",
        False
    )

    stopped = row.get(
        "rocket_stopped",
        False
    )

    # -----------------------------------------------------
    # 상승구간 종료
    # -----------------------------------------------------

    if stopped:

        return """
        <div class="rocket-cell stop">
            <span class="stop-icon">⛔️</span>
        </div>
        """

    # -----------------------------------------------------
    # 상승구간 진행
    # -----------------------------------------------------

    if active:

        return f"""
        <div class="rocket-cell active-rocket">
            <span class="rocket-icon">🚀</span>
            <span class="rocket-count">
                ({row.get("rocket_count", 1)})
            </span>
        </div>
        """

    return """
    <div class="rocket-cell empty-rocket">
        -
    </div>
    """


# =========================================================
# 행 HTML
# =========================================================

def rows_html(rows):

    if not rows:

        return """
        <div class="empty">
            데이터가 없습니다.
        </div>
        """

    html = ""

    for row in rows:

        qualified_class = (
            "qualified"
            if row.get("qualified")
            else ""
        )

        change = row.get(
            "change",
            0
        )

        change_class = (
            "up"
            if change > 0
            else
            "down"
            if change < 0
            else
            ""
        )

        html += f"""

        <div class="row {qualified_class}">

            <div class="rank">
                {row["rank"]}
            </div>

            <div class="coin">
                <div class="coin-name">
                    {row["name"]}
                </div>

                <div class="market-name">
                    {row["market"]}
                </div>
            </div>

            <div class="volume">
                {format_volume(row["volume"])}
            </div>

            <div class="change {change_class}">
                {format_change(change)}
            </div>

            <div class="ema">
                <div>
                    {ema_html(row["ema_1h"])}
                </div>
                <div>
                    {ema_html(row["ema_4h"])}
                </div>
            </div>

            <div class="down-section">
                {down_section_html(row)}
            </div>

            <div class="rising-section">
                {rocket_html(row)}
            </div>

        </div>
        """

    return html


# =========================================================
# 섹션
# =========================================================

def section(
    title,
    rows,
    update_time
):

    return f"""

    <div class="section">

        <div class="section-title">

            <div>
                {title}
            </div>

            <div class="update-time">
                {update_time}
            </div>

        </div>

        <div class="header row">

            <div class="rank">
                #
            </div>

            <div class="coin">
                코인
            </div>

            <div class="volume">
                거래대금
            </div>

            <div class="change">
                등락
            </div>

            <div class="ema">
                EMA
            </div>

            <div class="down-section">
                10선 하락구간
            </div>

            <div class="rising-section">
                상승구간
            </div>

        </div>

        {rows_html(rows)}

    </div>

    """


# =========================================================
# FastAPI
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def dashboard():

    upbit_html = section(
        f"🏆 UPBIT 거래대금 TOP{TOP_N}",
        latest_upbit_data,
        latest_upbit_update_time
    )

    okx_html = ""

    if USE_OKX == "Y":

        okx_html = section(
            f"🏆 OKX 거래대금 TOP{TOP_N}",
            latest_okx_data,
            latest_okx_update_time
        )

    html = f"""

<!DOCTYPE html>

<html lang="ko">

<head>

<meta charset="UTF-8">

<meta
    name="viewport"
    content="width=device-width, initial-scale=1.0"
>

<meta
    http-equiv="refresh"
    content="{UPDATE_MINUTES * 60}"
>

<title>
    📊 매매 전술 눌림 돌파
</title>


<style>

* {{
    box-sizing: border-box;
}}

body {{

    margin: 0;

    background: #111;

    color: #eee;

    font-family:
        Arial,
        "Noto Sans KR",
        sans-serif;

    font-size: 13px;

}}


.container {{

    width: 100%;

    max-width: 1250px;

    margin: 0 auto;

    padding: 8px;

}}


.title {{

    font-size: 20px;

    font-weight: bold;

    text-align: center;

    padding: 8px 0;

}}


.info {{

    background: #181818;

    border: 1px solid #303030;

    border-radius: 8px;

    padding: 7px 9px;

    margin-bottom: 8px;

    line-height: 1.45;

    color: #bbb;

    font-size: 12px;

}}


.section {{

    background: #151515;

    border-radius: 8px;

    overflow: hidden;

    margin-bottom: 10px;

    border: 1px solid #292929;

}}


.section-title {{

    display: flex;

    justify-content: space-between;

    align-items: center;

    padding: 7px 9px;

    background: #1c1c1c;

    font-weight: bold;

    font-size: 14px;

}}


.update-time {{

    color: #777;

    font-size: 10px;

    font-weight: normal;

}}


.row {{

    display: grid;

    grid-template-columns:
        35px
        minmax(100px, 1fr)
        85px
        65px
        125px
        105px
        105px;

    min-height: 38px;

    border-top: 1px solid #242424;

    align-items: center;

}}


.header {{

    min-height: 29px;

    background: #202020;

    border-top: none;

    color: #999;

    font-size: 11px;

    font-weight: bold;

}}


.row > div {{

    padding: 3px 5px;

    text-align: center;

    min-width: 0;

}}


.rank {{

    color: #888;

}}


.coin {{

    text-align: left !important;

    overflow: hidden;

}}


.coin-name {{

    font-weight: bold;

    color: #eee;

    white-space: nowrap;

    overflow: hidden;

    text-overflow: ellipsis;

}}


.market-name {{

    font-size: 9px;

    color: #555;

    margin-top: 1px;

}}


.volume {{

    font-size: 11px;

    font-weight: bold;

}}


.change {{

    font-size: 11px;

}}


.change.up {{

    color: #ff5555;

}}


.change.down {{

    color: #4d9cff;

}}


.ema {{

    display: flex;

    flex-direction: column;

    gap: 1px;

    font-size: 10px;

}}


.ema-cell {{

    white-space: nowrap;

}}


/* =======================================================
   하락구간
   ======================================================= */

.down-cell {{

    display: inline-flex;

    align-items: center;

    justify-content: center;

    min-width: 60px;

    font-weight: bold;

}}


.active-down {{

    color: #9b9b9b;

}}


.finished-down {{

    color: #555;

}}


.empty-down {{

    color: #444;

}}


/* =======================================================
   상승구간
   ======================================================= */

.rocket-cell {{

    display: inline-flex;

    align-items: center;

    justify-content: center;

    min-width: 70px;

    font-weight: bold;

}}


.active-rocket {{

    color: #fff;

    animation: rocketPulse 1.1s infinite;

}}


.rocket-icon {{

    font-size: 19px;

    display: inline-block;

    margin-right: 2px;

    animation:
        rocketMove
        0.8s
        ease-in-out
        infinite alternate;

}}


.rocket-count {{

    font-size: 12px;

    color: #fff;

}}


.stop-icon {{

    font-size: 17px;

}}


.empty-rocket {{

    color: #444;

}}


@keyframes rocketMove {{

    from {{
        transform: translateY(2px);
    }}

    to {{
        transform: translateY(-3px);
    }}

}}


@keyframes rocketPulse {{

    0% {{
        opacity: 0.65;
    }}

    50% {{
        opacity: 1;
    }}

    100% {{
        opacity: 0.65;
    }}

}}


/* =======================================================
   정배열 종목
   ======================================================= */

.row.qualified {{

    background: rgba(
        255,
        255,
        255,
        0.025
    );

}}


/* =======================================================
   빈 데이터
   ======================================================= */

.empty {{

    padding: 20px;

    text-align: center;

    color: #666;

}}


/* =======================================================
   모바일
   ======================================================= */

@media (
    max-width: 700px
) {{

    body {{

        font-size: 11px;

    }}

    .container {{

        padding: 4px;

    }}

    .title {{

        font-size: 17px;

        padding: 5px 0;

    }}

    .info {{

        font-size: 10px;

        padding: 5px 7px;

    }}

    .row {{

        grid-template-columns:
            25px
            minmax(72px, 1fr)
            60px
            50px
            82px
            70px
            70px;

        min-height: 34px;

    }}

    .row > div {{

        padding: 2px 2px;

    }}

    .market-name {{

        display: none;

    }}

    .volume {{

        font-size: 9px;

    }}

    .change {{

        font-size: 9px;

    }}

    .ema {{

        font-size: 8px;

    }}

    .down-cell {{

        min-width: 45px;

        font-size: 10px;

    }}

    .rocket-cell {{

        min-width: 45px;

    }}

    .rocket-icon {{

        font-size: 15px;

    }}

    .rocket-count {{

        font-size: 10px;

    }}

    .stop-icon {{

        font-size: 14px;

    }}

}}


</style>

</head>


<body>


<div class="container">


<div class="title">

    📊 매매 전술 눌림 돌파

</div>


<div class="info">

    ① 거래대금 TOP{TOP_N}<br>

    ② 1H + 4H EMA 10-30-60-120 정배열 표시<br>

    ③ EMA 정배열/역배열 연속 캔들 수 표시<br>

    ④ 1H EMA1(종가) ↗ EMA10 골든크로스 → 🚀(1)<br>

    ⑤ 골든크로스 이후 양봉 마감마다 🚀 카운트 증가<br>

    ⑥ 종가가 EMA10 아래 마감 → ⛔️ → 상승구간 종료<br>

    ⑦ EMA1(종가) < EMA10 → 🔻 하락구간 카운트<br>

    ⑧ 종료된 🔻 하락구간의 마지막 카운트는 회색으로 유지

</div>


{upbit_html}


{okx_html}


<div class="info">

    마지막 업데이트:
    {latest_upbit_update_time}

</div>


</div>


</body>

</html>

"""

    return HTMLResponse(
        content=html
    )


# =========================================================
# 스케줄러
# =========================================================

def scheduler():

    schedule.every(
        UPDATE_MINUTES
    ).minutes.do(
        update_all
    )

    while True:

        try:

            schedule.run_pending()

        except Exception as e:

            logging.error(
                f"스케줄러 오류: {e}"
            )

        time.sleep(1)


# =========================================================
# Startup
# =========================================================

@app.on_event("startup")
def startup_event():

    logging.info(
        "=========================================="
    )

    logging.info(
        "📊 EMA 상승/하락구간 대시보드 시작"
    )

    logging.info(
        "1H EMA1(종가) ↗ EMA10 = 골든크로스"
    )

    logging.info(
        "골든크로스 = 🚀(1)"
    )

    logging.info(
        "이후 양봉마다 🚀 카운트 증가"
    )

    logging.info(
        "종가 < EMA10 = ⛔️ 상승구간 종료"
    )

    logging.info(
        "종가 < EMA10 하락구간 = 🔻 카운트"
    )

    logging.info(
        "종료된 하락구간 마지막 카운트 = 회색 유지"
    )

    logging.info(
        "=========================================="
    )

    update_all()

    thread = threading.Thread(
        target=scheduler,
        daemon=True
    )

    thread.start()


# =========================================================
# 실행
# =========================================================

if __name__ == "__main__":

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
        )
