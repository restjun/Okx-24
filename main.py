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
    format="%(asctime)s | %(levelname)s | %(message)s"
)

KST = ZoneInfo("Asia/Seoul")

UPBIT_API = "https://api.upbit.com/v1"

USE_UPBIT = "Y"
USE_OKX = "N"

TOP_N = 20

UPDATE_MINUTES = 1

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10

HISTORY_CHUNK = 200
MAX_HISTORY_CHUNKS = 10

VOLUME_HOURS = 24

# =========================================================
# ROC 필터
# =========================================================

USE_1H_ROC_FILTER = "Y"
USE_4H_ROC_FILTER = "N"

USE_1H_ROC5 = "N"
USE_1H_ROC10 = "Y"
USE_1H_ROC20 = "Y"
USE_1H_ROC50 = "Y"
USE_1H_ROC100 = "N"
USE_1H_ROC200 = "N"

USE_4H_ROC5 = "N"
USE_4H_ROC10 = "N"
USE_4H_ROC20 = "N"
USE_4H_ROC50 = "N"
USE_4H_ROC100 = "N"
USE_4H_ROC200 = "N"

# =========================================================
# ROC5 신호
# =========================================================

ROC5_PERIOD = 5

# 신호 카운팅에 사용할 기준
SIGNAL_TIMEFRAME = "60"

# =========================================================
# 전역 데이터
# =========================================================

latest_upbit_data = []
latest_okx_data = []

latest_upbit_update_time = None
latest_okx_update_time = None

latest_upbit_markets = []
latest_upbit_orderbook = {}

request_lock = threading.Lock()
update_lock = threading.Lock()

last_request_time = 0

# ---------------------------------------------------------
# ROC5 상태
#
# {
#   "KRW-BTC": {
#       "active": True,
#       "mode": "up",
#       "up_count": 0,
#       "pullback_count": 1,
#       "last_candle": datetime,
#       "zero_cross": True
#   }
# }
# ---------------------------------------------------------

roc_signal_state = {}

# 화면 반짝임 상태
roc_flash_state = {}

# 이전 캔들 저장
roc_previous_values = {}

# =========================================================
# 시간
# =========================================================

def kst():

    return datetime.now(KST)


def normalize_datetime(dt):

    if dt is None:
        return None

    if dt.tzinfo is None:
        return dt.replace(tzinfo=KST)

    return dt.astimezone(KST)


def get_current_candle_start():

    now = kst()

    return now.replace(
        minute=0,
        second=0,
        microsecond=0
    )


def get_last_completed_candle_time():

    return get_current_candle_start() - timedelta(
        hours=1
    )


def candle_distance(a, b):

    if a is None or b is None:
        return 0

    a = normalize_datetime(a)
    b = normalize_datetime(b)

    return int(
        abs(
            (a - b).total_seconds()
        ) / 3600
    )


# =========================================================
# 요청 제어
# =========================================================

def wait_request():

    global last_request_time

    with request_lock:

        now = time.time()

        diff = now - last_request_time

        if diff < REQUEST_INTERVAL:

            time.sleep(
                REQUEST_INTERVAL - diff
            )

        last_request_time = time.time()


def retry(
    func,
    *args,
    **kwargs
):

    for attempt in range(
        MAX_RETRIES
    ):

        try:

            wait_request()

            result = func(
                *args,
                **kwargs
            )

            return result

        except Exception as e:

            logging.warning(
                f"API retry "
                f"{attempt + 1}/{MAX_RETRIES} "
                f"| {e}"
            )

            if attempt >= MAX_RETRIES - 1:
                break

            time.sleep(
                RATE_LIMIT_WAIT
            )

    return None


# =========================================================
# Upbit 마켓
# =========================================================

def get_upbit_markets():

    url = f"{UPBIT_API}/market/all"

    params = {
        "isDetails": "false"
    }

    data = retry(
        requests.get,
        url,
        params=params,
        timeout=10
    )

    if data is None:
        return []

    data.raise_for_status()

    markets = data.json()

    return [
        x
        for x in markets
        if x.get("market", "").startswith("KRW-")
    ]


# =========================================================
# Upbit 현재가
# =========================================================

def get_upbit_tickers(markets):

    if not markets:
        return []

    url = f"{UPBIT_API}/ticker"

    result = []

    # Upbit ticker API는 한 번에 여러 시장 조회 가능
    for i in range(
        0,
        len(markets),
        100
    ):

        batch = markets[
            i:i + 100
        ]

        params = {
            "markets": ",".join(
                x["market"]
                for x in batch
            )
        }

        data = retry(
            requests.get,
            url,
            params=params,
            timeout=10
        )

        if data is None:
            continue

        try:

            data.raise_for_status()

            result.extend(
                data.json()
            )

        except Exception as e:

            logging.warning(
                f"ticker error | {e}"
            )

    return result


# =========================================================
# Upbit 호가
# =========================================================

def get_upbit_orderbooks(markets):

    if not markets:
        return {}

    url = f"{UPBIT_API}/orderbook"

    result = {}

    for i in range(
        0,
        len(markets),
        100
    ):

        batch = markets[
            i:i + 100
        ]

        params = {
            "markets": ",".join(
                x["market"]
                for x in batch
            )
        }

        data = retry(
            requests.get,
            url,
            params=params,
            timeout=10
        )

        if data is None:
            continue

        try:

            data.raise_for_status()

            rows = data.json()

            for row in rows:

                result[
                    row["market"]
                ] = row

        except Exception as e:

            logging.warning(
                f"orderbook error | {e}"
            )

    return result


# =========================================================
# 호가 금액
# =========================================================

def calculate_orderbook_amount(
    orderbook,
    range_percent=0.01
):

    if not orderbook:
        return 0, 0

    units = orderbook.get(
        "orderbook_units",
        []
    )

    if not units:
        return 0, 0

    total_bid = 0
    total_ask = 0

    for unit in units:

        bid_price = float(
            unit.get(
                "bid_price",
                0
            )
        )

        bid_size = float(
            unit.get(
                "bid_size",
                0
            )
        )

        ask_price = float(
            unit.get(
                "ask_price",
                0
            )
        )

        ask_size = float(
            unit.get(
                "ask_size",
                0
            )
        )

        total_bid += (
            bid_price *
            bid_size
        )

        total_ask += (
            ask_price *
            ask_size
        )

    return (
        total_bid,
        total_ask
    )


# =========================================================
# Upbit 캔들
# =========================================================

def get_upbit_candle(
    market,
    unit=60,
    count=200,
    to=None
):

    url = (
        f"{UPBIT_API}/candles/"
        f"minutes/{unit}"
    )

    params = {
        "market": market,
        "count": min(
            count,
            200
        )
    }

    if to is not None:
        params["to"] = to

    data = retry(
        requests.get,
        url,
        params=params,
        timeout=10
    )

    if data is None:
        return pd.DataFrame()

    try:

        data.raise_for_status()

        rows = data.json()

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)

        df[
            "candle_date_time_kst"
        ] = pd.to_datetime(
            df[
                "candle_date_time_kst"
            ]
        )

        df = df.sort_values(
            "candle_date_time_kst"
        ).reset_index(
            drop=True
        )

        df.rename(
            columns={
                "opening_price": "open",
                "high_price": "high",
                "low_price": "low",
                "trade_price": "close",
                "candle_acc_trade_volume": "volume",
                "candle_acc_trade_price": "volume_price"
            },
            inplace=True
        )

        return df

    except Exception as e:

        logging.warning(
            f"candle error "
            f"{market} | {e}"
        )

        return pd.DataFrame()


# =========================================================
# 과거 데이터
# =========================================================

def history_upbit(
    market,
    unit=60,
    count=200
):

    all_rows = []

    to = None

    chunks = (
        count + 199
    ) // 200

    chunks = min(
        chunks,
        MAX_HISTORY_CHUNKS
    )

    for _ in range(chunks):

        df = get_upbit_candle(
            market=market,
            unit=unit,
            count=200,
            to=to
        )

        if df.empty:
            break

        all_rows.append(df)

        if len(df) < 200:
            break

        first_time = df[
            "candle_date_time_kst"
        ].iloc[0]

        to = (
            first_time
            .to_pydatetime()
            .strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        )

        time.sleep(
            REQUEST_INTERVAL
        )

    if not all_rows:
        return pd.DataFrame()

    result = pd.concat(
        all_rows,
        ignore_index=True
    )

    result = result.drop_duplicates(
        subset=[
            "candle_date_time_kst"
        ]
    )

    result = result.sort_values(
        "candle_date_time_kst"
    )

    result = result.tail(
        count
    ).reset_index(
        drop=True
    )

    return result


# =========================================================
# ROC
# =========================================================

def roc(
    series,
    period
):

    if len(series) <= period:
        return pd.Series(
            index=series.index,
            dtype=float
        )

    return (
        (
            series /
            series.shift(period)
        ) - 1
    ) * 100


# =========================================================
# ROC 설정
# =========================================================

def roc_settings():

    return {
        5: USE_1H_ROC5,
        10: USE_1H_ROC10,
        20: USE_1H_ROC20,
        50: USE_1H_ROC50,
        100: USE_1H_ROC100,
        200: USE_1H_ROC200
    }


def get_enabled_periods():

    settings = roc_settings()

    return [
        period
        for period, enabled
        in settings.items()
        if enabled == "Y"
    ]


def get_all_periods():

    return [
        5,
        10,
        20,
        50,
        100,
        200
    ]


def get_enabled_all_filters():

    filters = []

    if USE_1H_ROC_FILTER == "Y":

        filters.append(
            "1H"
        )

    if USE_4H_ROC_FILTER == "Y":

        filters.append(
            "4H"
        )

    return filters


def get_enabled_filter_text():

    result = []

    if USE_1H_ROC_FILTER == "Y":

        periods = []

        for p, enabled in roc_settings().items():

            if enabled == "Y" and p != 5:

                periods.append(
                    f"ROC{p}"
                )

        if periods:

            result.append(
                "1H "
                +
                "/".join(periods)
            )

    if USE_4H_ROC_FILTER == "Y":

        result.append(
            "4H"
        )

    return " · ".join(
        result
    )


def get_filter_setting_text():

    text = get_enabled_filter_text()

    if not text:
        return "필터 없음"

    return text


# =========================================================
# ROC 필터 분석
# =========================================================

def roc_filter_analysis(
    df,
    periods
):

    result = {}

    if df.empty:
        return result

    close = df["close"]

    for period in periods:

        series = roc(
            close,
            period
        )

        if series.empty:
            result[period] = None
            continue

        value = series.iloc[-1]

        if pd.isna(value):

            result[period] = None

        else:

            result[period] = float(
                value
            )

    return result


def roc_filter_display(
    values
):

    if not values:
        return "-"

    parts = []

    for period in sorted(
        values.keys()
    ):

        value = values[period]

        if value is None:

            parts.append(
                f"ROC{period} -"
            )

            continue

        if value > 0:

            icon = "🟢"

        elif value < 0:

            icon = "🔴"

        else:

            icon = "⚪"

        parts.append(
            f"ROC{period} "
            f"{icon}"
            f"{value:+.2f}%"
        )

    return "<br>".join(
        parts
    )


def get_all_active_roc_status(
    df,
    timeframe="1H"
):

    if df.empty:
        return {}

    periods = []

    if timeframe == "1H":

        settings = roc_settings()

    else:

        settings = {
            5: USE_4H_ROC5,
            10: USE_4H_ROC10,
            20: USE_4H_ROC20,
            50: USE_4H_ROC50,
            100: USE_4H_ROC100,
            200: USE_4H_ROC200
        }

    for period, enabled in settings.items():

        if enabled == "Y":

            # ROC5는 필터에서 제외
            # ROC5는 신호용
            if period == 5:
                continue

            periods.append(
                period
            )

    return roc_filter_analysis(
        df,
        periods
    )


def all_active_roc_filters_pass(
    df,
    timeframe="1H"
):

    values = get_all_active_roc_status(
        df,
        timeframe
    )

    if not values:
        return False

    for value in values.values():

        if value is None:
            return False

        if value < 0:
            return False

    return True


# =========================================================
# ROC5 0선 돌파
# =========================================================

def roc5_zero_cross(
    df
):

    if df.empty:
        return False

    series = roc(
        df["close"],
        ROC5_PERIOD
    )

    if len(series) < 2:
        return False

    previous = series.iloc[-2]
    current = series.iloc[-1]

    if pd.isna(previous) or pd.isna(current):
        return False

    return (
        previous < 0
        and
        current >= 0
    )


# =========================================================
# ROC5 눌림
# =========================================================

def roc5_pullback_condition(
    df
):

    if df.empty:
        return False

    series = roc(
        df["close"],
        ROC5_PERIOD
    )

    if len(series) < 2:
        return False

    previous = series.iloc[-2]
    current = series.iloc[-1]

    if pd.isna(previous) or pd.isna(current):
        return False

    # ROC5가 이전 캔들보다 낮아지면 눌림
    return current < previous


# =========================================================
# 최신 ROC5 신호 시작점
# =========================================================

def find_latest_signal_start(
    df
):

    if df.empty:
        return None

    series = roc(
        df["close"],
        ROC5_PERIOD
    )

    if series.empty:
        return None

    values = series.tolist()

    latest_cross = None

    for i in range(
        1,
        len(values)
    ):

        previous = values[i - 1]
        current = values[i]

        if pd.isna(previous):
            continue

        if pd.isna(current):
            continue

        if (
            previous < 0
            and
            current >= 0
        ):

            latest_cross = (
                df[
                    "candle_date_time_kst"
                ].iloc[i]
            )

    return latest_cross


# =========================================================
# ROC5 상승/눌림 상태 업데이트
# =========================================================

def update_signal_and_pullback(
    market,
    df,
    filter_pass
):

    now = kst()

    if market not in roc_signal_state:

        roc_signal_state[market] = {
            "active": False,
            "mode": None,
            "up_count": 0,
            "pullback_count": 0,
            "last_candle": None,
            "zero_cross": False,
            "flash": False
        }

    state = roc_signal_state[
        market
    ]

    if not filter_pass:

        state[
            "active"
        ] = False

        state[
            "mode"
        ] = None

        state[
            "up_count"
        ] = 0

        state[
            "pullback_count"
        ] = 0

        state[
            "zero_cross"
        ] = False

        return state

    if df.empty:

        return state

    series = roc(
        df["close"],
        ROC5_PERIOD
    )

    if len(series) < 3:

        return state

    candle_time = (
        df[
            "candle_date_time_kst"
        ].iloc[-1]
    )

    previous_candle = state[
        "last_candle"
    ]

    # 같은 캔들이면 카운팅하지 않음
    if (
        previous_candle is not None
        and
        normalize_datetime(
            previous_candle
        ) == normalize_datetime(
            candle_time
        )
    ):

        return state

    previous = series.iloc[-2]
    current = series.iloc[-1]

    if pd.isna(previous) or pd.isna(current):

        state[
            "last_candle"
        ] = candle_time

        return state

    state[
        "last_candle"
    ] = candle_time

    # -----------------------------------------------------
    # 1. 최초 0선 돌파
    # -----------------------------------------------------

    if (
        previous < 0
        and
        current >= 0
    ):

        state[
            "active"
        ] = True

        state[
            "mode"
        ] = "up"

        # 사용자가 원하는 표시
        # 상승 0 / 눌림 1
        state[
            "up_count"
        ] = 0

        state[
            "pullback_count"
        ] = 1

        state[
            "zero_cross"
        ] = True

        roc_flash_state[
            market
        ] = {
            "type": "zero",
            "time": now
        }

        return state

    # -----------------------------------------------------
    # 최초 상태가 없는 경우
    # -----------------------------------------------------

    if not state["active"]:

        state[
            "active"
        ] = True

        if current >= 0:

            state[
                "mode"
            ] = "up"

            state[
                "up_count"
            ] = 0

            state[
                "pullback_count"
            ] = 1

        else:

            state[
                "mode"
            ] = "pullback"

            state[
                "up_count"
            ] = 0

            state[
                "pullback_count"
            ] = 1

        state[
            "zero_cross"
        ] = False

        return state

    # -----------------------------------------------------
    # 2. 상승 모드
    # -----------------------------------------------------

    if state["mode"] == "up":

        # ROC5 하락 → 눌림 시작
        if current < previous:

            state[
                "mode"
            ] = "pullback"

            state[
                "pullback_count"
            ] = 1

            state[
                "up_count"
            ] = max(
                state["up_count"],
                0
            )

            state[
                "zero_cross"
            ] = False

            roc_flash_state[
                market
            ] = {
                "type": "normal",
                "time": now
            }

        else:

            # 상승 진행
            state[
                "up_count"
            ] += 1

            state[
                "pullback_count"
            ] = 0

            state[
                "zero_cross"
            ] = False

            roc_flash_state[
                market
            ] = {
                "type": "normal",
                "time": now
            }

    # -----------------------------------------------------
    # 3. 눌림 모드
    # -----------------------------------------------------

    elif state["mode"] == "pullback":

        # 다시 상승으로 전환
        if current >= previous:

            state[
                "mode"
            ] = "up"

            state[
                "up_count"
            ] += 1

            state[
                "pullback_count"
            ] = 0

            state[
                "zero_cross"
            ] = False

            roc_flash_state[
                market
            ] = {
                "type": "normal",
                "time": now
            }

        else:

            state[
                "pullback_count"
            ] += 1

            state[
                "zero_cross"
            ] = False

            roc_flash_state[
                market
            ] = {
                "type": "normal",
                "time": now
            }

    return state


# =========================================================
# 일일 등락률
# KST 09:00 기준
# =========================================================

def daily_change_upbit(
    market
):

    try:

        df = get_upbit_candle(
            market=market,
            unit=1440,
            count=2
        )

        if df.empty:
            return None

        if len(df) < 2:
            return None

        previous_close = float(
            df[
                "close"
            ].iloc[-2]
        )

        current_close = float(
            df[
                "close"
            ].iloc[-1]
        )

        if previous_close == 0:
            return None

        return (
            (
                current_close /
                previous_close
            ) - 1
        ) * 100

    except Exception:

        return None


# =========================================================
# 변화값
# =========================================================

def get_change_value(
    ticker
):

    if not ticker:
        return None

    value = ticker.get(
        "signed_change_rate"
    )

    if value is None:
        return None

    return float(value) * 100


def format_change(
    value
):

    if value is None:
        return "-"

    if value > 0:

        return (
            f"<span class='up'>"
            f"▲ {value:+.2f}%"
            f"</span>"
        )

    if value < 0:

        return (
            f"<span class='down'>"
            f"▼ {value:+.2f}%"
            f"</span>"
        )

    return (
        "<span class='neutral'>"
        "0.00%"
        "</span>"
    )


# =========================================================
# 거래대금
# =========================================================

def format_volume(
    value
):

    if value is None:
        return "-"

    value = float(value)

    if value >= 1_0000_0000_0000:

        return (
            f"{value / 1_0000_0000_0000:.2f}"
            "조"
        )

    if value >= 1_0000_0000:

        return (
            f"{value / 1_0000_0000:.1f}"
            "억"
        )

    if value >= 1_0000:

        return (
            f"{value / 1_0000:.0f}"
            "만"
        )

    return (
        f"{value:,.0f}"
    )


# =========================================================
# 현재가격
# =========================================================

def format_market_price(
    value
):

    if value is None:
        return "-"

    value = float(value)

    if value >= 1000:

        return f"{value:,.0f}원"

    if value >= 1:

        return f"{value:,.2f}원"

    return f"{value:.8f}원"


# =========================================================
# 분석
# =========================================================

def analyze(
    market,
    ticker
):

    try:

        df_1h = history_upbit(
            market,
            unit=60,
            count=220
        )

        if df_1h.empty:
            return None

        roc_values = get_all_active_roc_status(
            df_1h,
            "1H"
        )

        filter_pass = (
            all_active_roc_filters_pass(
                df_1h,
                "1H"
            )
            if USE_1H_ROC_FILTER == "Y"
            else True
        )

        signal_state = (
            update_signal_and_pullback(
                market,
                df_1h,
                filter_pass
            )
        )

        current_price = ticker.get(
            "trade_price"
        )

        change_rate = get_change_value(
            ticker
        )

        volume_24h = ticker.get(
            "acc_trade_price_24h"
        )

        if volume_24h is None:

            volume_24h = ticker.get(
                "acc_trade_price"
            )

        name = market.replace(
            "KRW-",
            ""
        )

        return {

            "market": market,

            "name": name,

            "current_price":
                current_price,

            "change_value":
                change_rate,

            "volume":
                volume_24h,

            "roc_values":
                roc_values,

            "roc_filter_pass":
                filter_pass,

            "signal_state":
                signal_state,

            "updated_at":
                kst()
        }

    except Exception as e:

        logging.warning(
            f"analyze error "
            f"{market} | {e}"
        )

        return None


# =========================================================
# 행 생성
# =========================================================

def make_row(
    data,
    rank
):

    if not data:
        return None

    state = data.get(
        "signal_state",
        {}
    )

    return {

        "rank":
            rank,

        "market":
            data.get(
                "market"
            ),

        "name":
            data.get(
                "name"
            ),

        "current_price":
            data.get(
                "current_price"
            ),

        "change_value":
            data.get(
                "change_value"
            ),

        "volume":
            data.get(
                "volume"
            ),

        "roc_values":
            data.get(
                "roc_values",
                {}
            ),

        "roc_filter_pass":
            data.get(
                "roc_filter_pass",
                False
            ),

        "signal_state":
            state,

        "updated_at":
            data.get(
                "updated_at"
            )
    }


# =========================================================
# Upbit 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update_time
    global latest_upbit_markets
    global latest_upbit_orderbook

    if USE_UPBIT != "Y":
        return

    try:

        markets = get_upbit_markets()

        if not markets:
            return

        latest_upbit_markets = markets

        tickers = get_upbit_tickers(
            markets
        )

        if not tickers:
            return

        ticker_map = {
            x["market"]: x
            for x in tickers
        }

        # -------------------------------------------------
        # 거래대금 TOP20
        # -------------------------------------------------

        sorted_tickers = sorted(
            tickers,
            key=lambda x:
                float(
                    x.get(
                        "acc_trade_price_24h",
                        0
                    )
                ),
            reverse=True
        )

        top_tickers = (
            sorted_tickers[
                :TOP_N
            ]
        )

        result = []

        for rank, ticker in enumerate(
            top_tickers,
            start=1
        ):

            market = ticker.get(
                "market"
            )

            row = analyze(
                market,
                ticker
            )

            if row is None:
                continue

            result.append(
                make_row(
                    row,
                    rank
                )
            )

        # 거래대금 순위 재정렬
        result = sorted(
            result,
            key=lambda x:
                float(
                    x.get(
                        "volume",
                        0
                    ) or 0
                ),
            reverse=True
        )

        for i, row in enumerate(
            result,
            start=1
        ):

            row["rank"] = i

        latest_upbit_data = result

        # 호가
        latest_upbit_orderbook = (
            get_upbit_orderbooks(
                [
                    x["market"]
                    for x in result
                ]
            )
        )

        latest_upbit_update_time = kst()

        logging.info(
            f"UPBIT UPDATE "
            f"| TOP {len(result)}"
        )

    except Exception as e:

        logging.exception(
            f"update_upbit error | {e}"
        )


# =========================================================
# OKX
# =========================================================

def update_okx():

    global latest_okx_update_time

    if USE_OKX != "Y":
        return

    # 현재 OKX 사용 안 함
    latest_okx_update_time = kst()


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    if not update_lock.acquire(
        blocking=False
    ):

        return

    try:

        if USE_UPBIT == "Y":

            update_upbit()

        if USE_OKX == "Y":

            update_okx()

    except Exception as e:

        logging.exception(
            f"dashboard error | {e}"
        )

    finally:

        update_lock.release()


# =========================================================
# 신호 HTML
# =========================================================

def signal_html(
    row
):

    state = row.get(
        "signal_state",
        {}
    )

    if not state:
        return "-"

    if not row.get(
        "roc_filter_pass",
        False
    ):
        return "-"

    up_count = state.get(
        "up_count",
        0
    )

    pullback_count = state.get(
        "pullback_count",
        0
    )

    mode = state.get(
        "mode"
    )

    zero_cross = state.get(
        "zero_cross",
        False
    )

    flash = ""

    if zero_cross:

        flash = (
            " signal-zero-flash"
        )

    elif (
        up_count > 0
        or
        pullback_count > 0
    ):

        flash = (
            " signal-normal-flash"
        )

    if mode == "up":

        icon = "🚀"

    elif mode == "pullback":

        icon = "🔻"

    else:

        icon = "•"

    return f"""
    <div class="signal-box{flash}">

        <div class="signal-main">
            {icon}
        </div>

        <div class="signal-count">

            <span class="up-count">
                상승 {up_count}
            </span>

            <span class="pull-count">
                눌림 {pullback_count}
            </span>

        </div>

    </div>
    """


# =========================================================
# ROC 필터 HTML
# =========================================================

def roc_filter_html(
    row
):

    values = row.get(
        "roc_values",
        {}
    )

    if not values:
        return "-"

    result = []

    for period in sorted(
        values.keys()
    ):

        value = values[period]

        if value is None:

            result.append(
                f"""
                <span class="roc-item">
                    ROC{period}
                    <b>-</b>
                </span>
                """
            )

            continue

        if value > 0:

            icon = "🟢"

        elif value < 0:

            icon = "🔴"

        else:

            icon = "⚪"

        result.append(
            f"""
            <span class="roc-item">
                ROC{period}
                {icon}
                <b>{value:+.2f}%</b>
            </span>
            """
        )

    return "".join(
        result
    )


# =========================================================
# 필터 상태
# =========================================================

def filter_html(
    row
):

    passed = row.get(
        "roc_filter_pass",
        False
    )

    if passed:

        return """
        <span class="filter-pass">
            PASS
        </span>
        """

    return """
    <span class="filter-fail">
        -
    </span>
    """


# =========================================================
# 상승신호 카운트
# =========================================================

def top_signal_count_html(
    row
):

    state = row.get(
        "signal_state",
        {}
    )

    up = state.get(
        "up_count",
        0
    )

    pull = state.get(
        "pullback_count",
        0
    )

    if not row.get(
        "roc_filter_pass",
        False
    ):

        return "-"

    return f"""
    <div class="count-wrap">

        <span class="count-up">
            상승 {up}
        </span>

        <span class="count-pull">
            눌림 {pull}
        </span>

    </div>
    """


# =========================================================
# 호가
# =========================================================

def orderbook_html(
    row
):

    market = row.get(
        "market"
    )

    book = latest_upbit_orderbook.get(
        market
    )

    if not book:
        return ""

    bid, ask = (
        calculate_orderbook_amount(
            book
        )
    )

    total = bid + ask

    if total <= 0:

        bid_pct = 50
        ask_pct = 50

    else:

        bid_pct = (
            bid /
            total
        ) * 100

        ask_pct = (
            ask /
            total
        ) * 100

    if bid > ask:

        status = "🟢 매수우세"

    elif ask > bid:

        status = "🔴 매도우세"

    else:

        status = "⚪ 균형"

    return f"""
    <div class="orderbook-row">

        <span>
            호가
        </span>

        <span>
            매수
            {format_volume(bid)}
        </span>

        <span>
            매도
            {format_volume(ask)}
        </span>

        <span>
            {bid_pct:.0f}%
            /
            {ask_pct:.0f}%
        </span>

        <span>
            {status}
        </span>

    </div>
    """


# =========================================================
# 거래대금 리스트 행
# =========================================================

def rows_html(
    rows,
    show_filter=True
):

    if not rows:

        return """
        <div class="empty">
            표시할 종목이 없습니다.
        </div>
        """

    html = ""

    for row in rows:

        market = row.get(
            "market",
            ""
        )

        name = row.get(
            "name",
            market
        )

        rank = row.get(
            "rank",
            "-"
        )

        price = format_market_price(
            row.get(
                "current_price"
            )
        )

        change = format_change(
            row.get(
                "change_value"
            )
        )

        volume = format_volume(
            row.get(
                "volume"
            )
        )

        roc_text = roc_filter_html(
            row
        )

        filter_text = filter_html(
            row
        )

        count_text = (
            top_signal_count_html(
                row
            )
        )

        signal = signal_html(
            row
        )

        zero_class = ""

        state = row.get(
            "signal_state",
            {}
        )

        if state.get(
            "zero_cross",
            False
        ):

            zero_class = (
                " row-zero-flash"
            )

        html += f"""

        <div class="coin-row{zero_class}">

            <div class="rank">
                {rank}
            </div>

            <div class="coin">

                <div class="coin-name">
                    {name}
                </div>

                <div class="coin-market">
                    {market}
                </div>

                <div class="coin-price">
                    {price}
                </div>

                <div class="coin-change">
                    {change}
                </div>

            </div>

            <div class="volume">
                {volume}
            </div>

            <div class="roc-filter">

                {roc_text}

                {
                    filter_text
                    if show_filter
                    else ""
                }

            </div>

            <div class="count">
                {count_text}
            </div>

            <div class="signal">
                {signal}
            </div>

        </div>

        <div class="orderbook-sub">
            {orderbook_html(row)}
        </div>

        """

    return html


# =========================================================
# 거래대금 리스트
# =========================================================

def table_html():

    return f"""

    <div class="section-card">

        <div class="section-title">

            <span>
                💰 거래대금 리스트
            </span>

            <span class="section-sub">
                UPBIT TOP {TOP_N}
            </span>

        </div>

        <div class="table-head">

            <div>순위</div>

            <div>종목</div>

            <div>거래대금</div>

            <div>ROC 필터</div>

            <div>COUNT</div>

            <div>신호</div>

        </div>

        {rows_html(
            latest_upbit_data,
            show_filter=True
        )}

    </div>

    """


# =========================================================
# ROC 필터 통과 종목
# =========================================================

def focus_section(
    rows
):

    passing = [
        row
        for row in rows
        if row.get(
            "roc_filter_pass",
            False
        )
    ]

    # 거래대금 순서 유지
    passing = sorted(
        passing,
        key=lambda x:
            float(
                x.get(
                    "volume",
                    0
                ) or 0
            ),
        reverse=True
    )

    for i, row in enumerate(
        passing,
        start=1
    ):

        row["rank"] = i

    return f"""

    <div class="section-card focus-card">

        <div class="section-title">

            <span>
                🚀 상승신호
            </span>

            <span class="section-sub">
                활성 ROC 통과종목
            </span>

        </div>

        <div class="filter-info">
            {get_filter_setting_text()}
        </div>

        <div class="table-head">

            <div>순위</div>

            <div>종목</div>

            <div>거래대금</div>

            <div>ROC</div>

            <div>COUNT</div>

            <div>신호</div>

        </div>

        {
            rows_html(
                passing,
                show_filter=False
            )
        }

    </div>

    """


# =========================================================
# BTC 비트시황
# =========================================================

def market_summary_html():

    btc = None

    for row in latest_upbit_data:

        if row.get(
            "name"
        ) == "BTC":

            btc = row
            break

    if btc:

        price = format_market_price(
            btc.get(
                "current_price"
            )
        )

        change = format_change(
            btc.get(
                "change_value"
            )
        )

        roc_text = roc_filter_html(
            btc
        )

        signal = signal_html(
            btc
        )

        if btc.get(
            "roc_filter_pass",
            False
        ):

            filter_status = (
                "<span class='btc-pass'>"
                "🟢 ROC 필터 통과"
                "</span>"
            )

        else:

            filter_status = (
                "<span class='btc-fail'>"
                "🔴 ROC 필터 미통과"
                "</span>"
            )

    else:

        price = "-"
        change = "-"
        roc_text = "-"
        signal = "-"
        filter_status = "-"

    return f"""

    <div class="market-summary">

        <div class="market-title">

            <span class="market-title-main">
                ₿ BTC 시장 시황
            </span>

            <span class="market-title-sub">
                활성 ROC
                {get_filter_setting_text()}
            </span>

        </div>

        <div class="btc-top">

            <div class="btc-name">
                ₿ BTC
            </div>

            <div class="btc-price">
                {price}
            </div>

            <div class="btc-change">
                {change}
            </div>

            <div class="btc-filter">
                {filter_status}
            </div>

            <div class="btc-signal">
                {signal}
            </div>

        </div>

        <div class="btc-roc">

            {roc_text}

        </div>

    </div>

    """


# =========================================================
# 전체 섹션
# =========================================================

def section():

    return f"""

    {market_summary_html()}

    {focus_section(
        latest_upbit_data
    )}

    {table_html()}

    """


# =========================================================
# HTML
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def home():

    update_dashboard()

    now = kst().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    return f"""

<!DOCTYPE html>

<html lang="ko">

<head>

<meta charset="UTF-8">

<meta
    name="viewport"
    content="width=device-width,
    initial-scale=1.0,
    maximum-scale=1.0,
    user-scalable=no"
>

<title>
TRADING SIGNAL CENTER
</title>

<style>

* {{
    box-sizing: border-box;
}}

html,
body {{
    margin: 0;
    padding: 0;
    background: #090d13;
    color: #e9eef5;
    font-family:
        Arial,
        "Noto Sans KR",
        sans-serif;
}}

body {{
    padding: 10px;
}}

.container {{
    width: 100%;
    max-width: 1200px;
    margin: 0 auto;
}}

.header {{
    padding: 10px 5px 14px 5px;
}}

.header-title {{
    font-size: 20px;
    font-weight: 900;
    letter-spacing: -0.5px;
}}

.header-time {{
    margin-top: 4px;
    color: #8c98a8;
    font-size: 11px;
}}

.market-summary {{
    background:
        linear-gradient(
            135deg,
            #111b29,
            #0c121b
        );

    border:
        1px solid #263447;

    border-radius: 14px;

    padding: 15px;

    margin-bottom: 12px;

    box-shadow:
        0 5px 25px
        rgba(
            0,
            0,
            0,
            .25
        );
}}

.market-title {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 10px;
    margin-bottom: 13px;
}}

.market-title-main {{
    font-size: 18px;
    font-weight: 900;
}}

.market-title-sub {{
    font-size: 10px;
    color: #7e8b9d;
    text-align: right;
}}

.btc-top {{
    display: grid;
    grid-template-columns:
        90px
        1fr
        auto
        auto
        auto;

    gap: 10px;

    align-items: center;
}}

.btc-name {{
    font-size: 17px;
    font-weight: 900;
}}

.btc-price {{
    font-size: 22px;
    font-weight: 900;
    text-align: right;
}}

.btc-change {{
    font-size: 13px;
    font-weight: 800;
}}

.btc-filter {{
    font-size: 11px;
}}

.btc-signal {{
    min-width: 100px;
}}

.btc-pass {{
    color: #55e6a5;
    font-weight: 800;
}}

.btc-fail {{
    color: #ff6878;
    font-weight: 800;
}}

.btc-roc {{
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    margin-top: 13px;
    padding-top: 11px;
    border-top: 1px solid #1e2a38;
}}

.section-card {{
    background: #0e141d;
    border:
        1px solid #202c3a;
    border-radius: 13px;
    margin-bottom: 12px;
    overflow: hidden;
}}

.focus-card {{
    border-color: #294b3d;
}}

.section-title {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 13px 14px;
    border-bottom: 1px solid #202c3a;
    font-weight: 900;
    font-size: 15px;
}}

.section-sub {{
    font-size: 10px;
    color: #8491a2;
    font-weight: 600;
}}

.filter-info {{
    padding: 7px 14px;
    color: #9ca9b8;
    font-size: 10px;
    border-bottom:
        1px solid #18222e;
}}

.table-head,
.coin-row {{
    display: grid;

    grid-template-columns:
        42px
        minmax(120px, 1.5fr)
        minmax(80px, 1fr)
        minmax(140px, 2fr)
        minmax(100px, 1.1fr)
        minmax(100px, 1.1fr);

    gap: 7px;

    align-items: center;
}}

.table-head {{
    padding: 8px 10px;

    color: #697789;

    font-size: 9px;

    font-weight: 700;

    background: #0b1017;

    border-bottom:
        1px solid #1b2632;
}}

.coin-row {{
    padding: 9px 10px;

    min-height: 67px;

    border-bottom:
        1px solid #151f2a;

    transition:
        background .2s;
}}

.coin-row:hover {{
    background: #131c27;
}}

.rank {{
    color: #758296;
    font-size: 11px;
    font-weight: 900;
    text-align: center;
}}

.coin-name {{
    font-size: 14px;
    font-weight: 900;
}}

.coin-market {{
    margin-top: 2px;
    font-size: 8px;
    color: #586678;
}}

.coin-price {{
    margin-top: 4px;
    font-size: 11px;
    color: #b7c2d0;
    font-weight: 700;
}}

.coin-change {{
    margin-top: 2px;
    font-size: 10px;
}}

.volume {{
    color: #d8e1eb;
    font-size: 11px;
    font-weight: 800;
}}

.roc-filter {{
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
}}

.roc-item {{
    display: inline-flex;
    align-items: center;
    gap: 3px;

    padding:
        3px 5px;

    border-radius: 5px;

    background: #111a24;

    color: #8997a8;

    font-size: 8px;

    white-space: nowrap;
}}

.roc-item b {{
    color: #d5dde7;
}}

.filter-pass {{
    display: inline-block;

    margin-left: 3px;

    padding: 3px 5px;

    border-radius: 5px;

    color: #55e6a5;

    background:
        rgba(
            45,
            180,
            120,
            .12
        );

    font-size: 8px;

    font-weight: 900;
}}

.filter-fail {{
    color: #536174;
}}

.count-wrap {{
    display: flex;
    flex-direction: column;
    gap: 3px;
}}

.count-up,
.count-pull {{
    font-size: 10px;
    font-weight: 900;
}}

.count-up {{
    color: #5fe0a6;
}}

.count-pull {{
    color: #ff8a98;
}}

.signal-box {{
    display: flex;
    align-items: center;
    gap: 7px;
}}

.signal-main {{
    font-size: 22px;
    line-height: 1;
}}

.signal-count {{
    display: flex;
    flex-direction: column;
    gap: 2px;
    font-size: 9px;
    font-weight: 900;
}}

.up-count {{
    color: #58e6a6;
}}

.pull-count {{
    color: #ff8493;
}}

.orderbook-sub {{
    background: #0a1017;
    border-bottom:
        1px solid #141e29;
}}

.orderbook-row {{
    display: flex;
    align-items: center;
    gap: 10px;

    padding:
        5px 10px 6px 52px;

    color: #667487;

    font-size: 8px;
}}

.orderbook-row span:last-child {{
    color: #a9b5c3;
    font-weight: 800;
}}

.up {{
    color: #55e6a5;
    font-weight: 900;
}}

.down {{
    color: #ff6878;
    font-weight: 900;
}}

.neutral {{
    color: #a0aab7;
}}

.empty {{
    padding: 30px;
    text-align: center;
    color: #687587;
    font-size: 12px;
}}

@keyframes signalFlash {{
    0% {{
        background:
            rgba(
                255,
                255,
                255,
                .32
            );
        transform:
            scale(1.008);
    }}

    35% {{
        background:
            rgba(
                65,
                230,
                160,
                .22
            );
    }}

    100% {{
        background:
            transparent;
        transform:
            scale(1);
    }}
}}

@keyframes signalZeroFlash {{
    0% {{
        background:
            rgba(
                255,
                255,
                255,
                .85
            );
        box-shadow:
            inset 0 0 0 2px
            rgba(
                255,
                255,
                255,
                .95
            );
    }}

    20% {{
        background:
            rgba(
                70,
                240,
                170,
                .55
            );
        box-shadow:
            inset 0 0 0 2px
            rgba(
                70,
                240,
                170,
                .85
            );
    }}

    50% {{
        background:
            rgba(
                70,
                240,
                170,
                .22
            );
    }}

    100% {{
        background:
            transparent;
        box-shadow:
            none;
    }}
}}

.signal-normal-flash {{
    animation:
        signalFlash
        1.2s
        ease-out;
}}

.signal-zero-flash {{
    animation:
        signalZeroFlash
        1.6s
        ease-out;
}}

.row-zero-flash {{
    animation:
        signalZeroFlash
        1.6s
        ease-out;
}}

@media (
    max-width: 700px
) {{

    body {{
        padding: 6px;
    }}

    .header-title {{
        font-size: 17px;
    }}

    .btc-top {{
        grid-template-columns:
            65px
            1fr
            auto;

        row-gap: 7px;
    }}

    .btc-price {{
        text-align: left;
        font-size: 18px;
    }}

    .btc-filter {{
        grid-column: 1 / 3;
    }}

    .btc-signal {{
        grid-column: 3;
        grid-row: 2;
    }}

    .table-head,
    .coin-row {{
        grid-template-columns:
            28px
            minmax(88px, 1.1fr)
            minmax(65px, .8fr)
            minmax(105px, 1.5fr)
            minmax(70px, .8fr)
            minmax(75px, .8fr);

        gap: 4px;
    }}

    .table-head {{
        font-size: 7px;
        padding: 7px 5px;
    }}

    .coin-row {{
        padding: 8px 5px;
    }}

    .coin-name {{
        font-size: 12px;
    }}

    .coin-market {{
        font-size: 7px;
    }}

    .coin-price {{
        font-size: 9px;
    }}

    .coin-change {{
        font-size: 8px;
    }}

    .volume {{
        font-size: 9px;
    }}

    .roc-item {{
        font-size: 7px;
        padding: 2px 3px;
    }}

    .count-up,
    .count-pull {{
        font-size: 8px;
    }}

    .signal-main {{
        font-size: 18px;
    }}

    .signal-count {{
        font-size: 7px;
    }}

    .orderbook-row {{
        padding-left: 36px;
        gap: 5px;
        font-size: 7px;
        flex-wrap: wrap;
    }}
}}

</style>

</head>

<body>

<div class="container">

    <div class="header">

        <div class="header-title">
            📊 TRADING SIGNAL CENTER
        </div>

        <div class="header-time">
            마지막 업데이트
            {now}
        </div>

    </div>

    {section()}

</div>

<script>

setTimeout(
    function() {{
        location.reload();
    }},
    {UPDATE_MINUTES * 60 * 1000}
);

</script>

</body>

</html>

    """


# =========================================================
# 백그라운드 업데이트
# =========================================================

def background_loop():

    while True:

        try:

            update_dashboard()

        except Exception as e:

            logging.exception(
                f"background error | {e}"
            )

        time.sleep(
            UPDATE_MINUTES * 60
        )


# =========================================================
# 시작
# =========================================================

@app.on_event(
    "startup"
)
def startup_event():

    logging.info(
        "=========================================="
    )

    logging.info(
        "TRADING SIGNAL CENTER START"
    )

    logging.info(
        f"UPBIT : {USE_UPBIT}"
    )

    logging.info(
        f"OKX   : {USE_OKX}"
    )

    logging.info(
        f"TOP_N : {TOP_N}"
    )

    logging.info(
        f"ROC FILTER : "
        f"{get_filter_setting_text()}"
    )

    logging.info(
        "ROC5 : SIGNAL / PULLBACK"
    )

    logging.info(
        "ZERO CROSS : 상승 0 / 눌림 1"
    )

    logging.info(
        "NORMAL COUNT : 일반 반짝임"
    )

    logging.info(
        "=========================================="
    )

    thread = threading.Thread(
        target=background_loop,
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
