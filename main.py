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


# =========================================================
# 사용자 설정
# =========================================================

VOLUME_HOURS = 24
TOP_N = 10
UPDATE_MINUTES = 1

HISTORY_CHUNK = 200
MAX_HISTORY_CHUNKS = 10

USE_UPBIT = "Y"
USE_OKX = "N"

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10


# =========================================================
# ROC 필터 설정
# =========================================================

USE_1H_ROC_FILTER = "Y"
USE_4H_ROC_FILTER = "N"

USE_1H_ROC5 = "Y"
USE_1H_ROC10 = "Y"
USE_1H_ROC20 = "Y"
USE_1H_ROC50 = "Y"
USE_1H_ROC200 = "N"

USE_4H_ROC5 = "N"
USE_4H_ROC10 = "N"
USE_4H_ROC20 = "N"
USE_4H_ROC50 = "N"
USE_4H_ROC200 = "N"

ROC_FILTER_TIMEFRAME = 60
ROC_FILTER_HIGH_TIMEFRAME = 240

ROC_TIMEFRAME = 60
ROC_PERIOD = 5

ROC_FILTER_PERIODS = [
    5,
    10,
    20,
    50,
    200
]


# =========================================================
# 호가창 설정
# =========================================================

ORDERBOOK_RANGE = 0.01
ORDERBOOK_COUNT = 30
ORDERBOOK_DOMINANCE_GAP = 5.0


# =========================================================
# API
# =========================================================

UPBIT_MARKET_URL = "https://api.upbit.com/v1/market/all"
UPBIT_TICKER_URL = "https://api.upbit.com/v1/ticker"
UPBIT_CANDLE_URL = "https://api.upbit.com/v1/candles/minutes"
UPBIT_DAY_CANDLE_URL = "https://api.upbit.com/v1/candles/days"
UPBIT_ORDERBOOK_URL = "https://api.upbit.com/v1/orderbook"


# =========================================================
# 전역 상태
# =========================================================

latest_data = []
latest_update_time = None

latest_btc_data = None

latest_usdt_dominance = None
latest_usdt_dominance_change = None

previous_upbit_total_trade_value = None

previous_top_vol_ids = []

sent_signal_coins = set()

signal_states = {}

data_lock = threading.Lock()


# =========================================================
# HTTP GET
# =========================================================

def safe_get(
    url,
    params=None,
    timeout=10,
    max_retries=MAX_RETRIES
):

    for attempt in range(max_retries):

        try:

            response = requests.get(
                url,
                params=params,
                timeout=timeout
            )

            if response.status_code == 200:

                time.sleep(REQUEST_INTERVAL)

                return response.json()

            if response.status_code == 429:

                logging.warning(
                    "429 Rate Limit: %s",
                    url
                )

                time.sleep(RATE_LIMIT_WAIT)

                continue

            logging.warning(
                "HTTP %s: %s",
                response.status_code,
                url
            )

        except Exception as e:

            logging.warning(
                "REQUEST ERROR %s : %s",
                url,
                e
            )

        time.sleep(
            min(
                RATE_LIMIT_WAIT * (attempt + 1),
                10
            )
        )

    return None


# =========================================================
# 숫자 표시
# =========================================================

def format_volume(value):

    try:
        value = float(value)
    except Exception:
        return "-"

    if value <= 0:
        return "-"

    # 1조 이상
    if value >= 1_000_000_000_000:
        return f"{value / 1_000_000_000_000:.1f}조"

    # 1,000억 이상
    if value >= 100_000_000_000:
        return f"{value / 1_000_000_000:.1f}억"

    # 1억 이상
    if value >= 100_000_000:
        return f"{value / 100_000_000:.1f}억"

    # 1만 이상
    if value >= 10_000:
        return f"{value / 10_000:.0f}만"

    return f"{value:,.0f}"


def format_total_trade_value(value):

    try:
        value = float(value)
    except Exception:
        return "-"

    if value <= 0:
        return "-"

    # 1조 이상
    if value >= 1_000_000_000_000:
        return f"{value / 1_000_000_000_000:.1f}조"

    # 1,000억 이상
    if value >= 100_000_000_000:
        return f"{value / 1_000_000_000:.1f}억"

    # 1억 이상
    if value >= 100_000_000:
        return f"{value / 100_000_000:.1f}억"

    # 1만 이상
    if value >= 10_000:
        return f"{value / 10_000:.0f}만"

    return f"{value:,.0f}원"


def format_percent(value):

    if value is None:
        return "-"

    try:
        value = float(value)
    except Exception:
        return "-"

    if value > 0:
        return f"+{value:.2f}%"

    return f"{value:.2f}%"


def percent_class(value):

    if value is None:
        return ""

    try:
        value = float(value)
    except Exception:
        return ""

    if value > 0:
        return "positive"

    if value < 0:
        return "negative"

    return "neutral"


# =========================================================
# 시간
# =========================================================

def now_kst():

    return datetime.now(KST)


def get_current_candle_start(minutes):

    now = now_kst()

    minute = (
        now.minute // minutes
    ) * minutes

    return datetime(
        now.year,
        now.month,
        now.day,
        now.hour,
        minute,
        0
    )


def normalize_datetime(value):

    if value is None:
        return None

    if isinstance(value, pd.Timestamp):

        if value.tzinfo is not None:
            value = value.tz_convert(KST).tz_localize(None)

        return value.to_pydatetime()

    if isinstance(value, datetime):

        if value.tzinfo is not None:
            return value.astimezone(KST).replace(
                tzinfo=None
            )

        return value

    return pd.to_datetime(value).to_pydatetime()


def candle_distance(
    start_time,
    current_time,
    timeframe_minutes
):

    start_time = normalize_datetime(start_time)
    current_time = normalize_datetime(current_time)

    if start_time is None or current_time is None:
        return 0

    diff = current_time - start_time

    seconds = diff.total_seconds()

    if seconds < 0:
        return 0

    return int(
        seconds // (timeframe_minutes * 60)
    )


# =========================================================
# Upbit 시장 목록
# =========================================================

def get_upbit_markets():

    data = safe_get(
        UPBIT_MARKET_URL
    )

    if not data:
        return []

    return [
        x
        for x in data
        if x.get("market", "").startswith("KRW-")
    ]


# =========================================================
# Upbit 24시간 거래대금
# =========================================================

def get_upbit_tickers(markets):

    if not markets:
        return []

    market_codes = [
        x["market"]
        for x in markets
    ]

    result = []

    for i in range(
        0,
        len(market_codes),
        100
    ):

        chunk = market_codes[i:i + 100]

        data = safe_get(
            UPBIT_TICKER_URL,
            params={
                "markets": ",".join(chunk)
            }
        )

        if data:
            result.extend(data)

    return result


# =========================================================
# Upbit 분봉
# =========================================================

def get_upbit_ohlcv(
    market,
    minutes=60,
    count=200
):

    url = (
        f"{UPBIT_CANDLE_URL}/{minutes}"
    )

    data = safe_get(
        url,
        params={
            "market": market,
            "count": count
        }
    )

    if not data:
        return pd.DataFrame()

    rows = []

    for item in reversed(data):

        rows.append({

            "time": pd.to_datetime(
                item["candle_date_time_kst"]
            ),

            "open": float(
                item["opening_price"]
            ),

            "high": float(
                item["high_price"]
            ),

            "low": float(
                item["low_price"]
            ),

            "close": float(
                item["trade_price"]
            ),

            "volume": float(
                item["candle_acc_trade_volume"]
            ),

            "trade_value": float(
                item["candle_acc_trade_price"]
            )
        })

    return pd.DataFrame(rows)


# =========================================================
# Upbit 일봉
# =========================================================

def get_upbit_day_candles(
    market,
    count=3
):

    data = safe_get(
        UPBIT_DAY_CANDLE_URL,
        params={
            "market": market,
            "count": count
        }
    )

    if not data:
        return pd.DataFrame()

    rows = []

    for item in reversed(data):

        rows.append({

            "time": pd.to_datetime(
                item["candle_date_time_kst"]
            ),

            "open": float(
                item["opening_price"]
            ),

            "high": float(
                item["high_price"]
            ),

            "low": float(
                item["low_price"]
            ),

            "close": float(
                item["trade_price"]
            ),

            "volume": float(
                item["candle_acc_trade_volume"]
            ),

            "trade_value": float(
                item["candle_acc_trade_price"]
            )
        })

    return pd.DataFrame(rows)


# =========================================================
# 일봉 등락률
# =========================================================

def daily_change_upbit(market):

    df = get_upbit_day_candles(
        market,
        count=3
    )

    if df.empty or len(df) < 2:
        return None

    previous_close = float(
        df.iloc[-2]["close"]
    )

    current_close = float(
        df.iloc[-1]["close"]
    )

    if previous_close <= 0:
        return None

    return (
        (current_close / previous_close)
        - 1
    ) * 100


# =========================================================
# ROC
# =========================================================

def roc(
    df,
    period
):

    if df is None or df.empty:
        return pd.Series(dtype=float)

    if "close" not in df.columns:
        return pd.Series(dtype=float)

    return (
        df["close"]
        .div(df["close"].shift(period))
        .sub(1)
        .mul(100)
    )


# =========================================================
# ROC 활성 설정
# =========================================================

def get_enabled_periods(
    timeframe="1H"
):

    if timeframe == "1H":

        settings = {
            5: USE_1H_ROC5,
            10: USE_1H_ROC10,
            20: USE_1H_ROC20,
            50: USE_1H_ROC50,
            200: USE_1H_ROC200
        }

    else:

        settings = {
            5: USE_4H_ROC5,
            10: USE_4H_ROC10,
            20: USE_4H_ROC20,
            50: USE_4H_ROC50,
            200: USE_4H_ROC200
        }

    return [
        period
        for period, enabled
        in settings.items()
        if enabled == "Y"
    ]


def get_all_roc_periods():

    return ROC_FILTER_PERIODS.copy()


def get_enabled_all_filters():

    result = []

    if USE_1H_ROC_FILTER == "Y":

        for period in get_enabled_periods("1H"):

            result.append(
                ("1H", period)
            )

    if USE_4H_ROC_FILTER == "Y":

        for period in get_enabled_periods("4H"):

            result.append(
                ("4H", period)
            )

    return result


def roc_filter_text():

    active = get_enabled_all_filters()

    if not active:
        return "사용 안함"

    return " / ".join(
        [
            f"{tf} ROC{period}"
            for tf, period in active
        ]
    )


# =========================================================
# 완성 캔들
# =========================================================

def get_last_completed_candle_time(
    df,
    timeframe
):

    if df is None or df.empty:
        return None

    current_start = get_current_candle_start(
        timeframe
    )

    times = []

    for value in df["time"]:

        dt = normalize_datetime(value)

        if dt < current_start:
            times.append(dt)

    if not times:
        return None

    return max(times)


# =========================================================
# ROC 분석
# =========================================================

def roc_filter_analysis(
    df,
    period
):

    result = {
        "current": None,
        "previous": None,
        "zero_cross_up": False,
        "zero_cross_down": False,
        "pass": False
    }

    if df is None or df.empty:
        return result

    series = roc(
        df,
        period
    )

    if series.empty:
        return result

    completed_time = get_last_completed_candle_time(
        df,
        ROC_FILTER_TIMEFRAME
    )

    if completed_time is None:
        return result

    completed_rows = df[
        df["time"].apply(
            lambda x:
            normalize_datetime(x) <= completed_time
        )
    ]

    if completed_rows.empty:
        return result

    completed_series = roc(
        completed_rows,
        period
    )

    valid = completed_series.dropna()

    if valid.empty:
        return result

    current_value = float(
        valid.iloc[-1]
    )

    previous_value = None

    if len(valid) >= 2:
        previous_value = float(
            valid.iloc[-2]
        )

    result["current"] = current_value
    result["previous"] = previous_value

    result["pass"] = (
        current_value >= 0
    )

    if (
        previous_value is not None
        and previous_value < 0
        and current_value >= 0
    ):
        result["zero_cross_up"] = True

    if (
        previous_value is not None
        and previous_value >= 0
        and current_value < 0
    ):
        result["zero_cross_down"] = True

    return result


# =========================================================
# 모든 활성 ROC 상태
# =========================================================

def get_all_active_roc_status(
    df1h,
    df4h
):

    statuses = []

    if USE_1H_ROC_FILTER == "Y":

        for period in get_enabled_periods("1H"):

            analysis = roc_filter_analysis(
                df1h,
                period
            )

            statuses.append({

                "timeframe": "1H",
                "period": period,
                "current": analysis["current"],
                "previous": analysis["previous"],
                "pass": analysis["pass"],
                "zero_cross_up":
                    analysis["zero_cross_up"],
                "zero_cross_down":
                    analysis["zero_cross_down"]
            })

    if USE_4H_ROC_FILTER == "Y":

        for period in get_enabled_periods("4H"):

            analysis = roc_filter_analysis(
                df4h,
                period
            )

            statuses.append({

                "timeframe": "4H",
                "period": period,
                "current": analysis["current"],
                "previous": analysis["previous"],
                "pass": analysis["pass"],
                "zero_cross_up":
                    analysis["zero_cross_up"],
                "zero_cross_down":
                    analysis["zero_cross_down"]
            })

    return statuses


# =========================================================
# 현재 활성 ROC가 모두 0 이상인지
# =========================================================

def active_roc_all_positive(
    df1h,
    df4h
):

    statuses = get_all_active_roc_status(
        df1h,
        df4h
    )

    if not statuses:
        return False

    values = [
        x["current"]
        for x in statuses
    ]

    if any(
        value is None
        for value in values
    ):
        return False

    return all(
        value >= 0
        for value in values
    )


# =========================================================
# 활성 ROC가 실제 음수가 되었는지
# =========================================================

def active_roc_broken(
    df1h,
    df4h
):

    statuses = get_all_active_roc_status(
        df1h,
        df4h
    )

    if not statuses:
        return False

    for status in statuses:

        value = status["current"]

        if (
            value is not None
            and value < 0
        ):
            return True

    return False


# =========================================================
# 활성 ROC 0선 돌파
# =========================================================

def all_active_roc_zero_cross(
    df1h,
    df4h
):

    statuses = get_all_active_roc_status(
        df1h,
        df4h
    )

    if not statuses:
        return False

    for status in statuses:

        if not status["zero_cross_up"]:
            return False

    return True


# =========================================================
# 과거 신호 시작점 복구
# =========================================================

def find_latest_signal_start(
    df1h,
    df4h
):

    active = get_enabled_all_filters()

    if not active:
        return None

    if df1h is None or df1h.empty:
        return None

    completed_time = get_last_completed_candle_time(
        df1h,
        60
    )

    if completed_time is None:
        return None

    completed_1h = df1h[
        df1h["time"].apply(
            lambda x:
            normalize_datetime(x) <= completed_time
        )
    ].copy()

    if completed_1h.empty:
        return None

    roc_columns = {}

    for period in get_enabled_periods("1H"):

        roc_columns[
            ("1H", period)
        ] = roc(
            completed_1h,
            period
        )

    if USE_4H_ROC_FILTER == "Y":

        completed_4h_time = get_last_completed_candle_time(
            df4h,
            240
        )

        if completed_4h_time is not None:

            completed_4h = df4h[
                df4h["time"].apply(
                    lambda x:
                    normalize_datetime(x)
                    <= completed_4h_time
                )
            ].copy()

            for period in get_enabled_periods("4H"):

                roc_columns[
                    ("4H", period)
                ] = roc(
                    completed_4h,
                    period
                )

    if not roc_columns:
        return None

    candidate_times = []

    for i in range(
        len(completed_1h)
    ):

        row_ok = True

        for (
            timeframe,
            period
        ) in active:

            if timeframe == "1H":

                series = roc_columns.get(
                    ("1H", period)
                )

                if series is None:
                    row_ok = False
                    break

                value = series.iloc[i]

            else:

                row_time = normalize_datetime(
                    completed_1h.iloc[i]["time"]
                )

                if df4h is None or df4h.empty:
                    row_ok = False
                    break

                temp4 = df4h[
                    df4h["time"].apply(
                        lambda x:
                        normalize_datetime(x)
                        <= row_time
                    )
                ]

                if temp4.empty:
                    row_ok = False
                    break

                temp_roc = roc(
                    temp4,
                    period
                )

                if temp_roc.empty:
                    row_ok = False
                    break

                value = temp_roc.iloc[-1]

            if pd.isna(value):
                row_ok = False
                break

            if value < 0:
                row_ok = False
                break

        if not row_ok:
            continue

        previous_ok = False

        if i > 0:

            previous_ok = True

            for (
                timeframe,
                period
            ) in active:

                if timeframe == "1H":

                    series = roc_columns.get(
                        ("1H", period)
                    )

                    if series is None:
                        previous_ok = False
                        break

                    prev_value = series.iloc[i - 1]

                else:

                    row_time_prev = normalize_datetime(
                        completed_1h.iloc[i - 1]["time"]
                    )

                    temp4_prev = df4h[
                        df4h["time"].apply(
                            lambda x:
                            normalize_datetime(x)
                            <= row_time_prev
                        )
                    ]

                    if temp4_prev.empty:
                        previous_ok = False
                        break

                    temp_roc_prev = roc(
                        temp4_prev,
                        period
                    )

                    if temp_roc_prev.empty:
                        previous_ok = False
                        break

                    prev_value = temp_roc_prev.iloc[-1]

                if pd.isna(prev_value):
                    previous_ok = False
                    break

                if prev_value >= 0:
                    previous_ok = False
                    break

        if previous_ok:

            candidate_times.append(
                normalize_datetime(
                    completed_1h.iloc[i]["time"]
                )
            )

    if candidate_times:

        return candidate_times[-1]

    latest_valid = None

    for i in range(
        len(completed_1h)
    ):

        row_ok = True

        for (
            timeframe,
            period
        ) in active:

            if timeframe == "1H":

                series = roc_columns.get(
                    ("1H", period)
                )

                if series is None:
                    row_ok = False
                    break

                value = series.iloc[i]

            else:

                row_time = normalize_datetime(
                    completed_1h.iloc[i]["time"]
                )

                temp4 = df4h[
                    df4h["time"].apply(
                        lambda x:
                        normalize_datetime(x)
                        <= row_time
                    )
                ]

                if temp4.empty:
                    row_ok = False
                    break

                temp_roc = roc(
                    temp4,
                    period
                )

                if temp_roc.empty:
                    row_ok = False
                    break

                value = temp_roc.iloc[-1]

            if pd.isna(value):
                row_ok = False
                break

            if value < 0:
                row_ok = False
                break

        if row_ok:

            if latest_valid is None:

                latest_valid = normalize_datetime(
                    completed_1h.iloc[i]["time"]
                )

    return latest_valid


# =========================================================
# 신호 상태
# =========================================================

def get_signal_qualified(
    market,
    df1h,
    df4h
):

    current_start = get_current_candle_start(
        60
    )

    completed_time = get_last_completed_candle_time(
        df1h,
        60
    )

    if completed_time is None:

        return {
            "active": False,
            "count": 0,
            "start": None
        }

    state = signal_states.get(
        market
    )

    if state is not None:

        if state.get("active"):

            if active_roc_broken(
                df1h,
                df4h
            ):

                state = {
                    "active": False,
                    "start": None,
                    "count": 0
                }

                signal_states[
                    market
                ] = state

                return state

            start = state.get(
                "start"
            )

            if start is not None:

                count = candle_distance(
                    start,
                    current_start,
                    60
                )

                state["count"] = max(
                    0,
                    count
                )

                return state

    start = find_latest_signal_start(
        df1h,
        df4h
    )

    if start is not None:

        count = candle_distance(
            start,
            current_start,
            60
        )

        state = {
            "active": True,
            "start": start,
            "count": max(
                0,
                count
            )
        }

        signal_states[
            market
        ] = state

        return state

    if active_roc_all_positive(
        df1h,
        df4h
    ):

        statuses = get_all_active_roc_status(
            df1h,
            df4h
        )

        zero_cross = False

        for status in statuses:

            if (
                status["previous"] is not None
                and status["previous"] < 0
                and status["current"] >= 0
            ):

                zero_cross = True
                break

        if zero_cross:

            state = {
                "active": True,
                "start": completed_time,
                "count": candle_distance(
                    completed_time,
                    current_start,
                    60
                )
            }

            signal_states[
                market
            ] = state

            return state

    state = {
        "active": False,
        "start": None,
        "count": 0
    }

    signal_states[
        market
    ] = state

    return state


# =========================================================
# ROC HTML
# =========================================================

def roc_value_html(
    value
):

    if value is None:
        return '<span class="roc-na">-</span>'

    try:
        value = float(value)
    except Exception:
        return '<span class="roc-na">-</span>'

    if value > 0:
        cls = "roc-positive"

    elif value < 0:
        cls = "roc-negative"

    else:
        cls = "roc-zero"

    return (
        f'<span class="{cls}">'
        f'{value:+.2f}%'
        f'</span>'
    )


def roc_html(
    df1h,
    df4h
):

    parts = []

    for period in ROC_FILTER_PERIODS:

        if period == 5:
            enabled = USE_1H_ROC5

        elif period == 10:
            enabled = USE_1H_ROC10

        elif period == 20:
            enabled = USE_1H_ROC20

        elif period == 50:
            enabled = USE_1H_ROC50

        else:
            enabled = USE_1H_ROC200

        analysis = roc_filter_analysis(
            df1h,
            period
        )

        if enabled == "Y":

            parts.append(
                f'<span class="roc-item active">'
                f'1H R{period} '
                f'{roc_value_html(analysis["current"])}'
                f'</span>'
            )

        else:

            parts.append(
                f'<span class="roc-item disabled">'
                f'1H R{period} '
                f'{roc_value_html(analysis["current"])}'
                f'</span>'
            )

    for period in ROC_FILTER_PERIODS:

        if period == 5:
            enabled = USE_4H_ROC5

        elif period == 10:
            enabled = USE_4H_ROC10

        elif period == 20:
            enabled = USE_4H_ROC20

        elif period == 50:
            enabled = USE_4H_ROC50

        else:
            enabled = USE_4H_ROC200

        analysis = roc_filter_analysis(
            df4h,
            period
        )

        if enabled == "Y":

            parts.append(
                f'<span class="roc-item active">'
                f'4H R{period} '
                f'{roc_value_html(analysis["current"])}'
                f'</span>'
            )

        else:

            parts.append(
                f'<span class="roc-item disabled">'
                f'4H R{period} '
                f'{roc_value_html(analysis["current"])}'
                f'</span>'
            )

    return "".join(parts)


# =========================================================
# 신호 HTML
# =========================================================

def signal_html(
    signal_state
):

    if not signal_state:
        return "-"

    if not signal_state.get("active"):
        return (
            '<span class="signal-none">-</span>'
        )

    count = signal_state.get(
        "count",
        0
    )

    return (
        '<span class="rocket-signal">'
        f'🚀<span class="signal-count">{count}</span>'
        '</span>'
    )


# =========================================================
# ROC 필터 상태 HTML
# =========================================================

def filter_status_html(
    df1h,
    df4h
):

    statuses = get_all_active_roc_status(
        df1h,
        df4h
    )

    if not statuses:
        return (
            '<span class="filter-off">OFF</span>'
        )

    all_pass = all(
        (
            x["current"] is not None
            and x["current"] >= 0
        )
        for x in statuses
    )

    if all_pass:

        return (
            '<span class="filter-pass">PASS</span>'
        )

    return (
        '<span class="filter-fail">FAIL</span>'
    )


# =========================================================
# 호가창
# =========================================================

def get_upbit_orderbooks(
    markets
):

    if not markets:
        return {}

    result = {}

    codes = [
        x
        for x in markets
        if x.startswith("KRW-")
    ]

    for i in range(
        0,
        len(codes),
        30
    ):

        chunk = codes[i:i + 30]

        data = safe_get(
            UPBIT_ORDERBOOK_URL,
            params={
                "markets": ",".join(chunk)
            }
        )

        if not data:
            continue

        for item in data:

            market = item.get(
                "market"
            )

            if market is None:
                continue

            result[
                market
            ] = item

    return result


def calculate_orderbook_amount(
    orderbook
):

    if not orderbook:

        return {
            "bid": 0,
            "ask": 0,
            "bid_units": 0,
            "ask_units": 0,
            "bid_ratio": 0,
            "ask_ratio": 0,
            "dominance": 0
        }

    units = orderbook.get(
        "orderbook_units",
        []
    )

    if not units:

        return {
            "bid": 0,
            "ask": 0,
            "bid_units": 0,
            "ask_units": 0,
            "bid_ratio": 0,
            "ask_ratio": 0,
            "dominance": 0
        }

    current_price = float(
        orderbook.get(
            "total_ask_size",
            0
        )
        and
        (
            units[0].get(
                "ask_price",
                0
            )
        )
        or 0
    )

    if current_price <= 0:

        current_price = float(
            units[0].get(
                "bid_price",
                0
            )
        )

    lower_price = (
        current_price
        * (1 - ORDERBOOK_RANGE)
    )

    upper_price = (
        current_price
        * (1 + ORDERBOOK_RANGE)
    )

    bid_amount = 0
    ask_amount = 0

    bid_units = 0
    ask_units = 0

    for unit in units[
        :ORDERBOOK_COUNT
    ]:

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

        if (
            bid_price >= lower_price
            and bid_price <= upper_price
        ):

            bid_amount += (
                bid_price * bid_size
            )

            bid_units += 1

        if (
            ask_price >= lower_price
            and ask_price <= upper_price
        ):

            ask_amount += (
                ask_price * ask_size
            )

            ask_units += 1

    total = (
        bid_amount
        + ask_amount
    )

    if total > 0:

        bid_ratio = (
            bid_amount / total
        ) * 100

        ask_ratio = (
            ask_amount / total
        ) * 100

    else:

        bid_ratio = 0
        ask_ratio = 0

    dominance = (
        bid_ratio
        - ask_ratio
    )

    return {
        "bid": bid_amount,
        "ask": ask_amount,
        "bid_units": bid_units,
        "ask_units": ask_units,
        "bid_ratio": bid_ratio,
        "ask_ratio": ask_ratio,
        "dominance": dominance
    }


# =========================================================
# 전일 업비트 전체 거래대금
# =========================================================

def update_previous_upbit_total_trade_value(
    markets
):

    global previous_upbit_total_trade_value

    total = 0

    for market_info in markets:

        market = market_info.get(
            "market"
        )

        if not market:
            continue

        df = get_upbit_day_candles(
            market,
            count=3
        )

        if df.empty:
            continue

        if len(df) >= 2:

            try:

                value = float(
                    df.iloc[-2][
                        "trade_value"
                    ]
                )

                total += value

            except Exception:
                pass

    previous_upbit_total_trade_value = total


# =========================================================
# BTC 분석
# =========================================================

def analyze_btc():

    global latest_btc_data

    market = "KRW-BTC"

    df1h = get_upbit_ohlcv(
        market,
        minutes=60,
        count=200
    )

    df4h = get_upbit_ohlcv(
        market,
        minutes=240,
        count=200
    )

    if df1h.empty:
        return

    daily = daily_change_upbit(
        market
    )

    signal_state = get_signal_qualified(
        market,
        df1h,
        df4h
    )

    statuses = get_all_active_roc_status(
        df1h,
        df4h
    )

    latest_btc_data = {

        "market": market,

        "name": "BTC",

        "daily_change": daily,

        "filter": filter_status_html(
            df1h,
            df4h
        ),

        "roc": roc_html(
            df1h,
            df4h
        ),

        "signal": signal_html(
            signal_state
        ),

        "signal_state": signal_state,

        "statuses": statuses
    }


# =========================================================
# 코인 분석
# =========================================================

def analyze(
    market,
    name,
    trade_value,
    daily_change,
    orderbook_data
):

    try:

        df1h = get_upbit_ohlcv(
            market,
            minutes=60,
            count=200
        )

        if df1h.empty:
            return None

        df4h = get_upbit_ohlcv(
            market,
            minutes=240,
            count=200
        )

        signal_state = get_signal_qualified(
            market,
            df1h,
            df4h
        )

        orderbook = calculate_orderbook_amount(
            orderbook_data
        )

        return {

            "market": market,

            "name": name,

            "trade_value": trade_value,

            "daily_change": daily_change,

            "df1h": df1h,

            "df4h": df4h,

            "filter": filter_status_html(
                df1h,
                df4h
            ),

            "roc": roc_html(
                df1h,
                df4h
            ),

            "signal": signal_html(
                signal_state
            ),

            "signal_state": signal_state,

            "orderbook": orderbook
        }

    except Exception as e:

        logging.exception(
            "ANALYZE ERROR %s",
            market
        )

        return None


# =========================================================
# 행 HTML
# =========================================================

def make_row(
    rank,
    data
):

    daily = data.get(
        "daily_change"
    )

    trade_value = data.get(
        "trade_value",
        0
    )

    orderbook = data.get(
        "orderbook",
        {}
    )

    bid = orderbook.get(
        "bid",
        0
    )

    ask = orderbook.get(
        "ask",
        0
    )

    dominance = orderbook.get(
        "dominance",
        0
    )

    daily_cls = percent_class(
        daily
    )

    if dominance >= ORDERBOOK_DOMINANCE_GAP:

        dom_html = (
            '<span class="bid-dominant">'
            '매수우위'
            '</span>'
        )

    elif dominance <= -ORDERBOOK_DOMINANCE_GAP:

        dom_html = (
            '<span class="ask-dominant">'
            '매도우위'
            '</span>'
        )

    else:

        dom_html = (
            '<span class="neutral-dominant">'
            '중립'
            '</span>'
        )

    return f"""

    <tr class="coin-row">

        <td class="rank">
            {rank}
        </td>

        <td class="coin-name">
            <strong>{data['name']}</strong>
        </td>

        <td class="vol">
            {format_volume(trade_value)}
        </td>

        <td class="filter">
            {data['filter']}
        </td>

        <td class="roc">
            {data['roc']}
        </td>

        <td class="signal">
            {data['signal']}
        </td>

    </tr>

    <tr class="orderbook-row">

        <td></td>

        <td colspan="2">

            <span class="order-label">
                매수
            </span>

            <span class="order-value bid">
                {format_volume(bid)}
            </span>

        </td>

        <td colspan="2">

            <span class="order-label">
                매도
            </span>

            <span class="order-value ask">
                {format_volume(ask)}
            </span>

        </td>

        <td>
            {dom_html}
        </td>

    </tr>
    """


# =========================================================
# 시장 상승/하락 통계
# =========================================================

def top_daily_breadth(
    data
):

    positive = 0
    negative = 0
    neutral = 0

    for item in data:

        change = item.get(
            "daily_change"
        )

        if change is None:
            continue

        if change > 0:
            positive += 1

        elif change < 0:
            negative += 1

        else:
            neutral += 1

    return (
        positive,
        negative,
        neutral
    )


# =========================================================
# 시장 요약
# =========================================================

def market_summary_html():

    btc = latest_btc_data

    if btc is None:

        btc_daily = "-"
        btc_roc = "-"
        btc_signal = "-"

    else:

        btc_daily = format_percent(
            btc.get("daily_change")
        )

        btc_roc = btc.get(
            "roc",
            "-"
        )

        btc_signal = btc.get(
            "signal",
            "-"
        )

    total_trade = format_total_trade_value(
        previous_upbit_total_trade_value
    )

    return f"""

    <div class="summary-grid">

        <div class="summary-box">

            <div class="summary-title">
                ROC 필터
            </div>

            <div class="summary-value small">
                {roc_filter_text()}
            </div>

        </div>


        <div class="summary-box btc-box">

            <div class="summary-title">
                BTC
            </div>

            <div class="btc-line">

                <span class="btc-change">
                    {btc_daily}
                </span>

                <span class="btc-signal">
                    {btc_signal}
                </span>

            </div>

        </div>


        <div class="summary-box">

            <div class="summary-title">
                전일 업비트 전체 거래대금
            </div>

            <div class="total-trade-value">
                {total_trade}
            </div>

        </div>

    </div>
    """


# =========================================================
# 테이블
# =========================================================

def rows_html():

    with data_lock:

        data = list(
            latest_data
        )

    if not data:

        return """
        <tr>
            <td colspan="6">
                데이터 수집 중...
            </td>
        </tr>
        """

    html = ""

    for rank, item in enumerate(
        data,
        start=1
    ):

        html += make_row(
            rank,
            item
        )

    return html


# =========================================================
# 전체 HTML
# =========================================================

def dashboard_html():

    update_text = "-"

    if latest_update_time:

        update_text = (
            latest_update_time
            .astimezone(KST)
            .strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        )

    summary_html = market_summary_html()
    table_html = rows_html()

    # -----------------------------------------------------
    # 중요:
    # CSS의 { } 때문에 f-string SyntaxError가 발생하지
    # 않도록 HTML 자체는 일반 문자열로 작성한다.
    # -----------------------------------------------------

    html = """
<!DOCTYPE html>

<html lang="ko">

<head>

<meta charset="UTF-8">

<meta
    name="viewport"
    content="width=device-width, initial-scale=1.0"
>

<title>
Upbit ROC Dashboard
</title>


<style>

/* =====================================================
   기본
   ===================================================== */

*{
    box-sizing:border-box;
}

body{

    margin:0;

    padding:8px;

    background:#101010;

    color:#f5f5f5;

    font-family:
        Arial,
        "Malgun Gothic",
        sans-serif;

    overflow-x:hidden;
}


/* =====================================================
   제목
   ===================================================== */

.header{

    display:flex;

    justify-content:
        space-between;

    align-items:center;

    margin-bottom:7px;
}

.title{

    font-size:15px;

    font-weight:900;
}

.update{

    font-size:7px;

    color:#888;

    white-space:nowrap;
}


/* =====================================================
   요약
   ===================================================== */

.summary-grid{

    display:grid;

    grid-template-columns:
        1fr 1fr 1fr;

    gap:4px;

    margin-bottom:7px;
}

.summary-box{

    background:#181818;

    border:1px solid #292929;

    border-radius:5px;

    padding:6px;

    min-height:48px;

    overflow:hidden;
}

.summary-title{

    color:#888;

    font-size:7px;

    margin-bottom:4px;
}

.summary-value{

    font-size:8px;

    font-weight:800;

    white-space:nowrap;
}

.summary-value.small{

    font-size:6.5px;

    line-height:8px;
}

.btc-line{

    display:flex;

    gap:5px;

    align-items:center;
}

.btc-change{

    font-size:10px;

    font-weight:900;
}

.btc-signal{

    font-size:8px;
}


/* =====================================================
   전일 전체 거래대금
   ===================================================== */

.total-trade-value{

    font-size:24px!important;

    line-height:27px!important;

    font-weight:900;

    letter-spacing:-1px;

    white-space:nowrap;
}


/* =====================================================
   테이블
   ===================================================== */

.table-wrap{

    width:100%;

    overflow:hidden;

    border-radius:5px;

    border:1px solid #292929;
}

table{

    width:100%;

    table-layout:fixed;

    border-collapse:collapse;

    background:#151515;
}

thead{

    background:#202020;
}

th{

    height:22px;

    font-size:7px;

    color:#999;

    font-weight:700;

    border-bottom:
        1px solid #303030;
}

td{

    padding:3px 2px;

    text-align:center;

    overflow:hidden;

    vertical-align:middle;
}


/* =====================================================
   열 너비
   ===================================================== */

th:nth-child(1),
td:nth-child(1){
    width:6%;
}

th:nth-child(2),
td:nth-child(2){
    width:15%;
}

th:nth-child(3),
td:nth-child(3){
    width:15%;
}

th:nth-child(4),
td:nth-child(4){
    width:29%;
}

th:nth-child(5),
td:nth-child(5){
    width:18%;
}

th:nth-child(6),
td:nth-child(6){
    width:17%;
}


/* =====================================================
   코인
   ===================================================== */

.coin-row{

    border-bottom:
        1px solid #252525;
}

.rank{

    color:#777;

    font-size:7px;
}

.coin-name{

    font-size:9px;

    font-weight:900;

    white-space:nowrap;
}


/* =====================================================
   거래대금
   ===================================================== */

.vol{

    font-size:4.5px;

    font-weight:800;

    white-space:nowrap;

    overflow:hidden;

    text-overflow:clip;

    letter-spacing:-0.3px;
}


/* =====================================================
   필터
   ===================================================== */

.filter{

    font-size:7px;

    white-space:nowrap;
}

.filter-pass{

    color:#48e68b;

    font-weight:900;
}

.filter-fail{

    color:#ff6666;

    font-weight:900;
}

.filter-off{

    color:#777;

    font-weight:700;
}


/* =====================================================
   ROC
   ===================================================== */

.roc{

    font-size:5.5px;

    line-height:7px;

    white-space:normal;
}

.roc-item{

    display:inline-block;

    margin-right:2px;

    white-space:nowrap;
}

.roc-item.active{

    font-weight:900;
}

.roc-item.disabled{

    opacity:0.35;
}

.roc-positive{

    color:#4ee88a;
}

.roc-negative{

    color:#ff5c5c;
}

.roc-zero{

    color:#ddd;
}

.roc-na{

    color:#555;
}


/* =====================================================
   신호
   ===================================================== */

.signal{

    white-space:nowrap;

    font-size:7px;
}

.rocket-signal{

    display:inline-flex;

    align-items:center;

    justify-content:center;

    gap:1px;

    font-size:8px;
}

.signal-count{

    font-size:7px;

    font-weight:900;
}

.signal-none{

    color:#555;
}


/* =====================================================
   호가
   ===================================================== */

.orderbook-row{

    background:#111;

    border-bottom:
        1px solid #202020;
}

.order-label{

    font-size:5px;

    color:#777;

    margin-right:2px;
}

.order-value{

    font-size:5px;

    font-weight:800;

    white-space:nowrap;
}

.bid{

    color:#4ee88a;
}

.ask{

    color:#ff6969;
}

.bid-dominant{

    color:#4ee88a;

    font-size:5px;

    font-weight:900;
}

.ask-dominant{

    color:#ff6969;

    font-size:5px;

    font-weight:900;
}

.neutral-dominant{

    color:#aaa;

    font-size:5px;

}


/* =====================================================
   모바일
   ===================================================== */

@media(max-width:380px){

    body{
        padding:5px;
    }

    .title{
        font-size:13px;
    }

    .update{
        font-size:6px;
    }

    .summary-box{
        padding:4px;
    }

    .summary-title{
        font-size:6px;
    }

    .summary-value{
        font-size:7px;
    }

    .summary-value.small{
        font-size:5.5px;
    }

    .btc-change{
        font-size:8px;
    }

    .btc-signal{
        font-size:6px;
    }

    .total-trade-value{

        font-size:21px!important;

        line-height:23px!important;

        letter-spacing:-1px;
    }

    th{
        font-size:6px;
    }

    .coin-name{
        font-size:8px;
    }

    .vol{
        font-size:4.2px;
    }

    .filter{
        font-size:6px;
    }

    .roc{
        font-size:4.8px;
        line-height:6px;
    }

    .rocket-signal{
        font-size:7px;
    }

    .signal-count{
        font-size:6px;
    }

    .order-label{
        font-size:4.5px;
    }

    .order-value{
        font-size:4.5px;
    }

    .bid-dominant,
    .ask-dominant,
    .neutral-dominant{
        font-size:4.5px;
    }
}


/* =====================================================
   PC
   ===================================================== */

@media(min-width:601px){

    body{

        max-width:1100px;

        margin:auto;

        padding:15px;
    }

    .title{
        font-size:20px;
    }

    .update{
        font-size:9px;
    }

    .summary-title{
        font-size:9px;
    }

    .summary-value{
        font-size:11px;
    }

    .summary-value.small{
        font-size:8px;
    }

    .btc-change{
        font-size:15px;
    }

    .btc-signal{
        font-size:11px;
    }

    .total-trade-value{

        font-size:28px!important;

        line-height:31px!important;

        letter-spacing:-1px;
    }

    th{
        font-size:9px;
    }

    .coin-name{
        font-size:12px;
    }

    .vol{
        font-size:6px;
    }

    .filter{
        font-size:9px;
    }

    .roc{
        font-size:7px;
        line-height:9px;
    }

    .rocket-signal{
        font-size:11px;
    }

    .signal-count{
        font-size:9px;
    }

    .order-label{
        font-size:7px;
    }

    .order-value{
        font-size:7px;
    }

    .bid-dominant,
    .ask-dominant,
    .neutral-dominant{
        font-size:7px;
    }
}

</style>

</head>


<body>


<div class="header">

    <div class="title">
        🚀 UPBIT ROC DASHBOARD
    </div>

    <div class="update">
        __UPDATE_TIME__
    </div>

</div>


__SUMMARY__


<div class="table-wrap">

<table>

<thead>

<tr>

    <th>#</th>

    <th>코인</th>

    <th>거래대금</th>

    <th>ROC 필터</th>

    <th>ROC</th>

    <th>신호</th>

</tr>

</thead>

<tbody>

__ROWS__

</tbody>

</table>

</div>


<script>

setTimeout(
    function(){
        location.reload();
    },
    60000
);

</script>


</body>

</html>
"""

    html = html.replace(
        "__UPDATE_TIME__",
        update_text
    )

    html = html.replace(
        "__SUMMARY__",
        summary_html
    )

    html = html.replace(
        "__ROWS__",
        table_html
    )

    return html


# =========================================================
# 메인 페이지
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def index():

    return HTMLResponse(
        dashboard_html()
    )


# =========================================================
# Upbit 업데이트
# =========================================================

def update_upbit():

    global latest_data
    global latest_update_time
    global previous_top_vol_ids

    logging.info(
        "========== UPBIT UPDATE START =========="
    )

    markets = get_upbit_markets()

    if not markets:

        logging.warning(
            "UPBIT MARKET EMPTY"
        )

        return

    # -----------------------------------------------------
    # 전체 거래대금
    # -----------------------------------------------------

    try:

        update_previous_upbit_total_trade_value(
            markets
        )

    except Exception:

        logging.exception(
            "PREVIOUS TOTAL TRADE VALUE ERROR"
        )

    # -----------------------------------------------------
    # 24H 거래대금
    # -----------------------------------------------------

    tickers = get_upbit_tickers(
        markets
    )

    if not tickers:

        logging.warning(
            "UPBIT TICKER EMPTY"
        )

        return

    ticker_map = {
        x["market"]: x
        for x in tickers
    }

    ranked = []

    for market_info in markets:

        market = market_info.get(
            "market"
        )

        ticker = ticker_map.get(
            market
        )

        if not ticker:
            continue

        trade_value = float(
            ticker.get(
                "acc_trade_price_24h",
                0
            )
        )

        ranked.append({

            "market": market,

            "name": market.replace(
                "KRW-",
                ""
            ),

            "trade_value":
                trade_value,

            "change_rate":
                float(
                    ticker.get(
                        "signed_change_rate",
                        0
                    )
                ) * 100
        })

    ranked.sort(
        key=lambda x:
        x["trade_value"],
        reverse=True
    )

    top = ranked[
        :TOP_N
    ]

    top_markets = [
        x["market"]
        for x in top
    ]

    previous_top_vol_ids = (
        top_markets
    )

    # -----------------------------------------------------
    # 호가
    # -----------------------------------------------------

    orderbooks = get_upbit_orderbooks(
        top_markets
    )

    # -----------------------------------------------------
    # BTC
    # -----------------------------------------------------

    try:

        analyze_btc()

    except Exception:

        logging.exception(
            "BTC ANALYZE ERROR"
        )

    # -----------------------------------------------------
    # TOP 분석
    # -----------------------------------------------------

    result = []

    for item in top:

        market = item["market"]

        logging.info(
            "ANALYZE %s",
            market
        )

        daily_change = daily_change_upbit(
            market
        )

        analyzed = analyze(
            market=market,
            name=item["name"],
            trade_value=item[
                "trade_value"
            ],
            daily_change=daily_change,
            orderbook_data=orderbooks.get(
                market
            )
        )

        if analyzed is not None:

            result.append(
                analyzed
            )

    with data_lock:

        latest_data = result

        latest_update_time = now_kst()

    logging.info(
        "========== UPBIT UPDATE END : %d ==========",
        len(result)
    )


# =========================================================
# 업데이트 전체
# =========================================================

def update_all():

    try:

        if USE_UPBIT == "Y":

            update_upbit()

    except Exception:

        logging.exception(
            "UPDATE ALL ERROR"
        )


# =========================================================
# 스케줄러
# =========================================================

def scheduler_loop():

    logging.info(
        "Scheduler started"
    )

    update_all()

    schedule.every(
        UPDATE_MINUTES
    ).minutes.do(
        update_all
    )

    while True:

        try:

            schedule.run_pending()

        except Exception:

            logging.exception(
                "SCHEDULER ERROR"
            )

        time.sleep(1)


# =========================================================
# 서버 시작
# =========================================================

if __name__ == "__main__":

    scheduler_thread = threading.Thread(
        target=scheduler_loop,
        daemon=True
    )

    scheduler_thread.start()

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )
