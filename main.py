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
# 사용 거래소
# =========================================================

USE_UPBIT = "Y"
USE_OKX = "N"


# =========================================================
# 기본 설정
# =========================================================

VOLUME_HOURS = 24
TOP_N = 10
UPDATE_MINUTES = 1

HISTORY_CHUNK = 200
MAX_HISTORY_CHUNKS = 10

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10


# =========================================================
# ROC 설정
# =========================================================

SIGNAL_TIMEFRAME = 240

ROC_FILTER_PERIODS = [5, 10, 20, 50, 200]

DISPLAY_COUNT_MIN = 0
DISPLAY_COUNT_MAX = 999999

# 0, 1까지만 반짝임
FLASH_COUNT_MIN = 0
FLASH_COUNT_MAX = 1


# =========================================================
# ROC 필터 사용 여부
# =========================================================

USE_1H_ROC_FILTER = "N"
USE_4H_ROC_FILTER = "Y"

USE_1H_ROC5 = "Y"
USE_1H_ROC10 = "Y"
USE_1H_ROC20 = "Y"
USE_1H_ROC50 = "Y"
USE_1H_ROC200 = "Y"

USE_4H_ROC5 = "N"
USE_4H_ROC10 = "Y"
USE_4H_ROC20 = "Y"
USE_4H_ROC50 = "Y"
USE_4H_ROC200 = "Y"

ROC_FILTER_TIMEFRAME = 60
ROC_FILTER_HIGH_TIMEFRAME = 240

SIGNAL_ROC_PERIOD = 5

ROC_HISTORY_REQUIRED = max(ROC_FILTER_PERIODS) + 2


# =========================================================
# 호가창
# =========================================================

ORDERBOOK_RANGE = 0.01
ORDERBOOK_COUNT = 30
ORDERBOOK_DOMINANCE_GAP = 5.0


# =========================================================
# FastAPI
# =========================================================

app = FastAPI()


# =========================================================
# 전역 데이터
# =========================================================

latest_upbit_data = {}
latest_okx_data = {}

last_upbit_update = None
last_okx_update = None

market_cache = []
market_cache_time = None

orderbook_cache = {}
orderbook_cache_time = {}

request_lock = threading.Lock()

last_request_time = 0.0


# =========================================================
# 핵심 상태
#
# 상승신호만 사용한다.
#
# roc_signal_state[market] =
# {
#     "start_candle": datetime,
#     "count": int,
#     "last_candle": datetime
# }
#
# 눌림 상태는 아예 만들지 않는다.
# =========================================================

roc_signal_state = {}

# 과거 신호를 복구했는지 관리
roc_signal_failed_candle = {}


# =========================================================
# 시간
# =========================================================

def now_kst():
    return datetime.now(KST)


def kst():
    return now_kst().strftime("%Y-%m-%d %H:%M:%S")


def format_timeframe(minutes):
    if minutes == 60:
        return "1H"

    if minutes == 240:
        return "4H"

    if minutes % 60 == 0:
        return f"{minutes // 60}H"

    return f"{minutes}M"


# =========================================================
# 시간프레임 검증
# =========================================================

def validate_timeframe():

    if SIGNAL_TIMEFRAME not in [60, 240]:
        raise ValueError(
            "SIGNAL_TIMEFRAME은 60 또는 240이어야 합니다."
        )

    logging.info(
        "SIGNAL_TIMEFRAME = %s",
        format_timeframe(SIGNAL_TIMEFRAME)
    )


# =========================================================
# 요청 제한
# =========================================================

def request_get(
    url,
    params=None,
    timeout=10
):

    global last_request_time

    for attempt in range(MAX_RETRIES):

        try:

            with request_lock:

                elapsed = time.time() - last_request_time

                if elapsed < REQUEST_INTERVAL:
                    time.sleep(
                        REQUEST_INTERVAL - elapsed
                    )

                response = requests.get(
                    url,
                    params=params,
                    timeout=timeout
                )

                last_request_time = time.time()

            if response.status_code == 429:

                logging.warning(
                    "429 발생 → %s초 대기",
                    RATE_LIMIT_WAIT
                )

                time.sleep(RATE_LIMIT_WAIT)
                continue

            response.raise_for_status()

            return response

        except Exception as e:

            logging.warning(
                "API 요청 실패 %s/%s : %s",
                attempt + 1,
                MAX_RETRIES,
                e
            )

            time.sleep(1)

    return None


# =========================================================
# Upbit 마켓
# =========================================================

def get_upbit_markets():

    global market_cache
    global market_cache_time

    if (
        market_cache
        and market_cache_time
        and (
            datetime.now()
            - market_cache_time
        ).total_seconds() < 3600
    ):
        return market_cache

    url = "https://api.upbit.com/v1/market/all"

    response = request_get(
        url,
        params={
            "isDetails": "false"
        }
    )

    if response is None:
        return []

    try:

        data = response.json()

        markets = [
            x["market"]
            for x in data
            if x["market"].startswith("KRW-")
        ]

        market_cache = markets
        market_cache_time = datetime.now()

        return markets

    except Exception as e:

        logging.error(
            "마켓 조회 실패: %s",
            e
        )

        return []


# =========================================================
# Upbit 현재가
# =========================================================

def get_upbit_tickers(markets):

    if not markets:
        return []

    result = []

    for i in range(
        0,
        len(markets),
        100
    ):

        chunk = markets[i:i + 100]

        response = request_get(
            "https://api.upbit.com/v1/ticker",
            params={
                "markets": ",".join(chunk)
            }
        )

        if response is None:
            continue

        try:
            result.extend(
                response.json()
            )

        except Exception:
            pass

    return result


# =========================================================
# Upbit OHLCV
# =========================================================

def get_upbit_ohlcv(
    market,
    minutes=240,
    count=200
):

    url = (
        f"https://api.upbit.com/v1/candles/minutes/{minutes}"
    )

    response = request_get(
        url,
        params={
            "market": market,
            "count": count
        }
    )

    if response is None:
        return pd.DataFrame()

    try:

        data = response.json()

        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)

        df["candle_date_time_kst"] = pd.to_datetime(
            df["candle_date_time_kst"]
        )

        df = df.sort_values(
            "candle_date_time_kst"
        ).reset_index(drop=True)

        return df

    except Exception as e:

        logging.warning(
            "%s OHLCV 오류: %s",
            market,
            e
        )

        return pd.DataFrame()


# =========================================================
# 긴 히스토리
# =========================================================

def get_upbit_history(
    market,
    minutes=240,
    required=202
):

    chunks = []

    fetched = 0

    for _ in range(MAX_HISTORY_CHUNKS):

        if fetched >= required:
            break

        count = min(
            HISTORY_CHUNK,
            required - fetched
        )

        df = get_upbit_ohlcv(
            market,
            minutes,
            count
        )

        if df.empty:
            break

        chunks.append(df)

        fetched += len(df)

        if len(df) < count:
            break

        oldest = df["candle_date_time_kst"].min()

        url = (
            f"https://api.upbit.com/v1/candles/minutes/{minutes}"
        )

        response = request_get(
            url,
            params={
                "market": market,
                "count": count,
                "to": (
                    oldest
                    .to_pydatetime()
                    .astimezone(ZoneInfo("UTC"))
                    .strftime("%Y-%m-%dT%H:%M:%SZ")
                )
            }
        )

        if response is None:
            break

        try:

            data = response.json()

            if not data:
                break

            next_df = pd.DataFrame(data)

            next_df["candle_date_time_kst"] = pd.to_datetime(
                next_df["candle_date_time_kst"]
            )

            chunks.append(next_df)

            fetched += len(next_df)

        except Exception:
            break

    if not chunks:
        return pd.DataFrame()

    result = pd.concat(
        chunks,
        ignore_index=True
    )

    result = result.drop_duplicates(
        subset=["candle_date_time_kst"]
    )

    result = result.sort_values(
        "candle_date_time_kst"
    ).reset_index(drop=True)

    return result.tail(required).reset_index(
        drop=True
    )


# =========================================================
# 현재 진행 중인 ROC 봉
#
# 마지막 확정봉 + 현재가로 현재 봉을 구성
# =========================================================

def get_upbit_current_roc_data(
    market,
    minutes=240,
    count=210
):

    df = get_upbit_ohlcv(
        market,
        minutes,
        count
    )

    if df.empty:
        return df

    ticker = get_upbit_tickers(
        [market]
    )

    if ticker:

        current_price = ticker[0].get(
            "trade_price"
        )

        if current_price:

            last_index = df.index[-1]

            df.loc[
                last_index,
                "trade_price"
            ] = current_price

            df.loc[
                last_index,
                "high_price"
            ] = max(
                float(df.loc[last_index, "high_price"]),
                float(current_price)
            )

            df.loc[
                last_index,
                "low_price"
            ] = min(
                float(df.loc[last_index, "low_price"]),
                float(current_price)
            )

            df.loc[
                last_index,
                "trade_price"
            ] = current_price

    return df


# =========================================================
# ROC
# =========================================================

def roc(
    series,
    period
):

    return (
        series
        .div(series.shift(period))
        .sub(1)
        .mul(100)
    )


# =========================================================
# ROC 분석
# =========================================================

def roc_filter_analysis(
    df,
    periods=ROC_FILTER_PERIODS
):

    result = {}

    if df.empty:
        return result

    close = df["trade_price"].astype(float)

    for period in periods:

        values = roc(
            close,
            period
        )

        current = values.iloc[-1]

        previous = (
            values.iloc[-2]
            if len(values) >= 2
            else None
        )

        result[period] = {
            "current": (
                float(current)
                if pd.notna(current)
                else None
            ),
            "previous": (
                float(previous)
                if previous is not None
                and pd.notna(previous)
                else None
            )
        }

    return result


# =========================================================
# ROC 필터 활성화 확인
# =========================================================

def active_roc_periods(
    timeframe
):

    if timeframe == 60:

        enabled = {
            5: USE_1H_ROC5,
            10: USE_1H_ROC10,
            20: USE_1H_ROC20,
            50: USE_1H_ROC50,
            200: USE_1H_ROC200
        }

    else:

        enabled = {
            5: USE_4H_ROC5,
            10: USE_4H_ROC10,
            20: USE_4H_ROC20,
            50: USE_4H_ROC50,
            200: USE_4H_ROC200
        }

    return [
        period
        for period, value in enabled.items()
        if value == "Y"
    ]


# =========================================================
# ROC 필터 통과 여부
# =========================================================

def check_roc_filter(
    analysis,
    timeframe
):

    periods = active_roc_periods(
        timeframe
    )

    if not periods:
        return True

    for period in periods:

        item = analysis.get(period)

        if not item:
            return False

        current = item["current"]

        if current is None:
            return False

        if current < 0:
            return False

    return True


# =========================================================
# ROC 필터 HTML
# =========================================================

def roc_filter_html(
    analysis_1h,
    analysis_4h
):

    def one_line(
        analysis,
        timeframe
    ):

        periods = active_roc_periods(
            timeframe
        )

        if not periods:
            return "사용 안 함"

        parts = []

        for period in periods:

            item = analysis.get(period)

            if not item:
                parts.append(
                    f"ROC{period} ⚪"
                )
                continue

            value = item["current"]

            if value is None:
                icon = "⚪"

            elif value > 0:
                icon = "🟢"

            elif value < 0:
                icon = "🔴"

            else:
                icon = "⚪"

            parts.append(
                f"ROC{period} {icon}"
            )

        return " ".join(parts)

    return (
        f"<div class='roc-line'>"
        f"<b>1H</b> {one_line(analysis_1h, 60)}"
        f"</div>"
        f"<div class='roc-line'>"
        f"<b>4H</b> {one_line(analysis_4h, 240)}"
        f"</div>"
    )


# =========================================================
# 0선 상승돌파
# =========================================================

def roc_signal_zero_cross(
    analysis
):

    item = analysis.get(
        SIGNAL_ROC_PERIOD
    )

    if not item:
        return False

    previous = item["previous"]
    current = item["current"]

    if previous is None or current is None:
        return False

    return (
        previous <= 0
        and current > 0
    )


# =========================================================
# 확정봉 기준 상승돌파 찾기
# =========================================================

def find_latest_signal_start(
    df
):

    if df.empty:
        return None

    close = df["trade_price"].astype(float)

    values = roc(
        close,
        SIGNAL_ROC_PERIOD
    )

    latest = None

    for i in range(1, len(df)):

        previous = values.iloc[i - 1]
        current = values.iloc[i]

        if pd.isna(previous) or pd.isna(current):
            continue

        if (
            previous <= 0
            and current > 0
        ):

            latest = (
                df.iloc[i][
                    "candle_date_time_kst"
                ]
            )

    return latest


# =========================================================
# 현재 봉 시간
# =========================================================

def current_progress_candle_time(
    df
):

    if df.empty:
        return None

    return df.iloc[-1][
        "candle_date_time_kst"
    ]


# =========================================================
# 카운트
# =========================================================

def candle_distance(
    start,
    current,
    timeframe
):

    if start is None or current is None:
        return 0

    delta = (
        current - start
    ).total_seconds()

    unit = timeframe * 60

    if delta <= 0:
        return 0

    return max(
        0,
        int(delta // unit)
    )


# =========================================================
# 상승신호 상태 업데이트
#
# 핵심:
# 🚀만 존재
# 📉 상태 없음
#
# 새로운 상승돌파가 나오면 기존 상태를 교체
# =========================================================

def update_signal_state(
    market,
    analysis,
    filter_pass,
    progress_candle,
    historical_start_candle=None
):

    market_key = market

    current_item = analysis.get(
        SIGNAL_ROC_PERIOD
    )

    if not current_item:
        return

    current_roc = current_item["current"]

    previous_roc = current_item["previous"]

    if (
        current_roc is None
        or previous_roc is None
    ):
        return

    signal_cross = (
        previous_roc <= 0
        and current_roc > 0
    )

    state = roc_signal_state.get(
        market_key
    )

    # -----------------------------------------------------
    # 새로운 상승 0선 돌파
    # -----------------------------------------------------

    if signal_cross:

        roc_signal_state[
            market_key
        ] = {
            "start_candle": progress_candle,
            "count": 0,
            "last_candle": progress_candle
        }

        return

    # -----------------------------------------------------
    # 기존 상승신호가 없는 경우
    # 과거 가장 최근 상승돌파 복구
    # -----------------------------------------------------

    if state is None:

        if (
            historical_start_candle is not None
            and current_roc > 0
        ):

            count = candle_distance(
                historical_start_candle,
                progress_candle,
                SIGNAL_TIMEFRAME
            )

            roc_signal_state[
                market_key
            ] = {
                "start_candle":
                    historical_start_candle,
                "count":
                    count,
                "last_candle":
                    progress_candle
            }

            return

    # -----------------------------------------------------
    # 기존 상승신호 관리
    # -----------------------------------------------------

    state = roc_signal_state.get(
        market_key
    )

    if state is None:
        return

    # ROC가 다시 0 이하가 되면 상승신호 종료
    if current_roc <= 0:

        roc_signal_state.pop(
            market_key,
            None
        )

        return

    # 현재 진행봉 기준 카운트 갱신
    count = candle_distance(
        state["start_candle"],
        progress_candle,
        SIGNAL_TIMEFRAME
    )

    state["count"] = count
    state["last_candle"] = progress_candle


# =========================================================
# 일일 변동률
# =========================================================

def daily_change_upbit(
    market
):

    df = get_upbit_ohlcv(
        market,
        minutes=1440,
        count=3
    )

    if df.empty or len(df) < 2:
        return None

    try:

        previous_close = float(
            df.iloc[-2]["trade_price"]
        )

        current_close = float(
            df.iloc[-1]["trade_price"]
        )

        if previous_close == 0:
            return None

        return (
            current_close
            / previous_close
            - 1
        ) * 100

    except Exception:
        return None


# =========================================================
# 거래대금
# =========================================================

def volume_24h_upbit(
    market
):

    df = get_upbit_ohlcv(
        market,
        minutes=60,
        count=24
    )

    if df.empty:
        return 0.0

    try:

        volume = (
            df["candle_acc_trade_price"]
            .astype(float)
            .sum()
        )

        return float(volume)

    except Exception:
        return 0.0


# =========================================================
# 숫자 포맷
# =========================================================

def format_price(
    value
):

    if value is None:
        return "-"

    try:

        value = float(value)

        if value >= 1000000:
            return f"{value:,.0f}"

        if value >= 1000:
            return f"{value:,.0f}"

        if value >= 1:
            return f"{value:,.2f}"

        return f"{value:,.6f}"

    except Exception:
        return "-"


def format_volume(
    value
):

    try:

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

        return f"{value:,.0f}"

    except Exception:
        return "-"


def format_change(
    value
):

    if value is None:
        return "-"

    try:

        value = float(value)

        if value > 0:
            return f"+{value:.2f}%"

        return f"{value:.2f}%"

    except Exception:
        return "-"


# =========================================================
# 호가창
# =========================================================

def get_orderbook(
    market
):

    now = time.time()

    cached_time = orderbook_cache_time.get(
        market,
        0
    )

    if (
        market in orderbook_cache
        and now - cached_time < 10
    ):
        return orderbook_cache[market]

    response = request_get(
        "https://api.upbit.com/v1/orderbook",
        params={
            "markets": market,
            "level": 0
        }
    )

    if response is None:
        return None

    try:

        data = response.json()

        if not data:
            return None

        item = data[0]

        orderbook_units = item.get(
            "orderbook_units",
            []
        )

        if not orderbook_units:
            return None

        result = {
            "units": orderbook_units,
            "timestamp": time.time()
        }

        orderbook_cache[
            market
        ] = result

        orderbook_cache_time[
            market
        ] = now

        return result

    except Exception:
        return None


# =========================================================
# 호가창 ±1% 계산
# =========================================================

def orderbook_analysis(
    market,
    current_price
):

    data = get_orderbook(
        market
    )

    if not data:
        return None

    try:

        units = data["units"]

        lower = (
            current_price
            * (1 - ORDERBOOK_RANGE)
        )

        upper = (
            current_price
            * (1 + ORDERBOOK_RANGE)
        )

        bid_amount = 0.0
        ask_amount = 0.0

        bid_count = 0
        ask_count = 0

        for unit in units:

            ask_price = float(
                unit["ask_price"]
            )

            ask_size = float(
                unit["ask_size"]
            )

            bid_price = float(
                unit["bid_price"]
            )

            bid_size = float(
                unit["bid_size"]
            )

            if (
                lower
                <= ask_price
                <= upper
            ):

                ask_amount += (
                    ask_price
                    * ask_size
                )

                ask_count += 1

            if (
                lower
                <= bid_price
                <= upper
            ):

                bid_amount += (
                    bid_price
                    * bid_size
                )

                bid_count += 1

        total = (
            bid_amount
            + ask_amount
        )

        if total <= 0:
            return None

        bid_ratio = (
            bid_amount
            / total
            * 100
        )

        ask_ratio = (
            ask_amount
            / total
            * 100
        )

        gap = (
            bid_ratio
            - ask_ratio
        )

        if gap >= ORDERBOOK_DOMINANCE_GAP:

            dominance = "🟢 매수우세"

        elif gap <= -ORDERBOOK_DOMINANCE_GAP:

            dominance = "🔴 매도우세"

        else:

            dominance = "⚪ 균형"

        return {
            "bid_amount": bid_amount,
            "ask_amount": ask_amount,
            "bid_ratio": bid_ratio,
            "ask_ratio": ask_ratio,
            "gap": gap,
            "dominance": dominance,
            "bid_count": bid_count,
            "ask_count": ask_count
        }

    except Exception as e:

        logging.warning(
            "호가창 계산 오류 %s: %s",
            market,
            e
        )

        return None


# =========================================================
# 분석
# =========================================================

def analyze(
    market
):

    # -----------------------------------------------------
    # 1H
    # -----------------------------------------------------

    df_1h = get_upbit_current_roc_data(
        market,
        60,
        ROC_HISTORY_REQUIRED + 10
    )

    # -----------------------------------------------------
    # 4H
    # -----------------------------------------------------

    df_4h = get_upbit_current_roc_data(
        market,
        240,
        ROC_HISTORY_REQUIRED + 10
    )

    if df_4h.empty:
        return None

    # -----------------------------------------------------
    # ROC 분석
    # -----------------------------------------------------

    analysis_1h = roc_filter_analysis(
        df_1h
    )

    analysis_4h = roc_filter_analysis(
        df_4h
    )

    # -----------------------------------------------------
    # 필터
    # -----------------------------------------------------

    filter_1h_pass = True
    filter_4h_pass = True

    if USE_1H_ROC_FILTER == "Y":

        filter_1h_pass = check_roc_filter(
            analysis_1h,
            60
        )

    if USE_4H_ROC_FILTER == "Y":

        filter_4h_pass = check_roc_filter(
            analysis_4h,
            240
        )

    filter_pass = (
        filter_1h_pass
        and filter_4h_pass
    )

    # -----------------------------------------------------
    # 현재 ROC
    # -----------------------------------------------------

    signal_item = analysis_4h.get(
        SIGNAL_ROC_PERIOD
    )

    if not signal_item:
        return None

    current_signal_roc = (
        signal_item["current"]
    )

    previous_signal_roc = (
        signal_item["previous"]
    )

    # -----------------------------------------------------
    # 현재 진행봉
    # -----------------------------------------------------

    progress_candle = (
        current_progress_candle_time(
            df_4h
        )
    )

    # -----------------------------------------------------
    # 확정봉 기준 가장 최근 상승돌파
    # -----------------------------------------------------

    completed_df = df_4h.iloc[:-1].copy()

    historical_start = (
        find_latest_signal_start(
            completed_df
        )
    )

    # -----------------------------------------------------
    # 상태 업데이트
    # -----------------------------------------------------

    if progress_candle is not None:

        update_signal_state(
            market,
            analysis_4h,
            filter_pass,
            progress_candle,
            historical_start
        )

    # -----------------------------------------------------
    # 현재 상태
    # -----------------------------------------------------

    state = roc_signal_state.get(
        market
    )

    if state is None:

        signal_active = False
        signal_count = None
        signal_start = None

    else:

        signal_active = True

        signal_count = int(
            state["count"]
        )

        signal_start = state[
            "start_candle"
        ]

    # -----------------------------------------------------
    # 현재가
    # -----------------------------------------------------

    try:

        current_price = float(
            df_4h.iloc[-1][
                "trade_price"
            ]
        )

    except Exception:

        current_price = None

    # -----------------------------------------------------
    # 일일 변동
    # -----------------------------------------------------

    change_value = daily_change_upbit(
        market
    )

    daily_pass = (
        change_value is not None
        and change_value >= 0
    )

    # -----------------------------------------------------
    # 거래대금
    # -----------------------------------------------------

    volume_24h = volume_24h_upbit(
        market
    )

    # -----------------------------------------------------
    # 호가
    # -----------------------------------------------------

    orderbook = None

    if current_price is not None:

        orderbook = orderbook_analysis(
            market,
            current_price
        )

    return {
        "market": market,
        "price": current_price,
        "volume_24h": volume_24h,
        "daily_change": change_value,
        "daily_pass": daily_pass,

        "analysis_1h": analysis_1h,
        "analysis_4h": analysis_4h,

        "filter_1h_pass":
            filter_1h_pass,

        "filter_4h_pass":
            filter_4h_pass,

        "filter_pass":
            filter_pass,

        "signal_active":
            signal_active,

        "signal_count":
            signal_count,

        "signal_start":
            signal_start,

        "signal_roc":
            current_signal_roc,

        "previous_signal_roc":
            previous_signal_roc,

        "orderbook":
            orderbook
    }


# =========================================================
# 행 생성
# =========================================================

def make_row(
    item,
    rank
):

    return {
        "rank": rank,
        **item
    }


# =========================================================
# Upbit 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global last_upbit_update

    markets = get_upbit_markets()

    if not markets:
        return

    tickers = get_upbit_tickers(
        markets
    )

    if not tickers:
        return

    ticker_map = {
        x["market"]: x
        for x in tickers
    }

    rows = []

    # -----------------------------------------------------
    # 거래대금 기준 TOP 선정
    # -----------------------------------------------------

    candidates = []

    for market in markets:

        ticker = ticker_map.get(
            market
        )

        if not ticker:
            continue

        acc_trade_price = float(
            ticker.get(
                "acc_trade_price_24h",
                0
            )
        )

        candidates.append(
            (
                market,
                acc_trade_price
            )
        )

    candidates.sort(
        key=lambda x: x[1],
        reverse=True
    )

    candidates = candidates[:TOP_N]

    # -----------------------------------------------------
    # 개별 분석
    # -----------------------------------------------------

    for market, _ in candidates:

        try:

            item = analyze(
                market
            )

            if item is None:
                continue

            rows.append(
                make_row(
                    item,
                    len(rows) + 1
                )
            )

        except Exception as e:

            logging.warning(
                "%s 분석 실패: %s",
                market,
                e
            )

    # -----------------------------------------------------
    # 거래대금 재정렬
    # -----------------------------------------------------

    rows.sort(
        key=lambda x:
            x.get(
                "volume_24h",
                0
            ),
        reverse=True
    )

    for i, row in enumerate(
        rows,
        start=1
    ):
        row["rank"] = i

    latest_upbit_data = {
        x["market"]: x
        for x in rows
    }

    last_upbit_update = now_kst()

    logging.info(
        "Upbit 업데이트 완료 : %s개",
        len(rows)
    )


# =========================================================
# OKX
# =========================================================

def update_okx():

    global latest_okx_data
    global last_okx_update

    if USE_OKX != "Y":
        latest_okx_data = {}
        return

    # 현재 설정은 USE_OKX = N
    # 기존 구조 유지를 위한 자리
    latest_okx_data = {}

    last_okx_update = now_kst()


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    logging.info(
        "========== 대시보드 업데이트 시작 =========="
    )

    try:

        if USE_UPBIT == "Y":
            update_upbit()

        if USE_OKX == "Y":
            update_okx()

    except Exception as e:

        logging.exception(
            "대시보드 업데이트 오류: %s",
            e
        )

    logging.info(
        "========== 대시보드 업데이트 종료 =========="
    )


# =========================================================
# ROC COUNT HTML
#
# 상승신호만 표시
#
# 0도 표시
# 1도 표시
# 2 이상도 표시
#
# 단, 반짝임은 0과 1만
# =========================================================

def roc_count_html(
    item
):

    signal_active = item.get(
        "signal_active",
        False
    )

    signal_count = item.get(
        "signal_count"
    )

    if not signal_active:
        return (
            "<span class='count-empty'>-</span>"
        )

    if signal_count is None:
        return (
            "<span class='count-empty'>-</span>"
        )

    if not (
        DISPLAY_COUNT_MIN
        <= signal_count
        <= DISPLAY_COUNT_MAX
    ):
        return (
            "<span class='count-empty'>-</span>"
        )

    # -----------------------------------------------------
    # 0 / 1만 강한 표시
    # -----------------------------------------------------

    if (
        FLASH_COUNT_MIN
        <= signal_count
        <= FLASH_COUNT_MAX
    ):

        return (
            f"<span class='roc-count signal-active "
            f"signal-flash-one'>"
            f"🚀({signal_count})"
            f"</span>"
        )

    return (
        f"<span class='roc-count signal-active'>"
        f"🚀({signal_count})"
        f"</span>"
    )


# =========================================================
# 호가창 HTML
# =========================================================

def orderbook_html(
    item
):

    ob = item.get(
        "orderbook"
    )

    if not ob:
        return ""

    return f"""
    <div class="orderbook-row">
        <span>
            ±1%
        </span>

        <span class="ask">
            매도 {format_volume(ob["ask_amount"])}
            ({ob["ask_ratio"]:.1f}%)
        </span>

        <span class="bid">
            매수 {format_volume(ob["bid_amount"])}
            ({ob["bid_ratio"]:.1f}%)
        </span>

        <span class="dominance">
            {ob["dominance"]}
        </span>
    </div>
    """


# =========================================================
# 행 HTML
# =========================================================

def rows_html(
    data
):

    if not data:

        return """
        <tr>
            <td colspan="5"
                class="empty">
                데이터 없음
            </td>
        </tr>
        """

    html = ""

    for item in data:

        market = item["market"]

        coin = market.replace(
            "KRW-",
            ""
        )

        rank = item["rank"]

        volume = format_volume(
            item["volume_24h"]
        )

        change = format_change(
            item["daily_change"]
        )

        price = format_price(
            item["price"]
        )

        filter_content = roc_filter_html(
            item["analysis_1h"],
            item["analysis_4h"]
        )

        count_content = roc_count_html(
            item
        )

        # -------------------------------------------------
        # 반짝임
        # 0,1만
        # -------------------------------------------------

        signal_count = item.get(
            "signal_count"
        )

        flash = (
            item.get("signal_active")
            and signal_count is not None
            and item.get("filter_pass")
            and item.get("daily_pass")
            and FLASH_COUNT_MIN
            <= signal_count
            <= FLASH_COUNT_MAX
        )

        row_class = (
            "signal-flash-one"
            if flash
            else ""
        )

        change_class = ""

        if (
            item["daily_change"]
            is not None
        ):

            if item["daily_change"] > 0:
                change_class = "positive"

            elif item["daily_change"] < 0:
                change_class = "negative"

        html += f"""
        <tr class="{row_class}">

            <td class="rank">
                {rank}
            </td>

            <td class="coin-cell">

                <div class="coin-name">
                    {coin}
                </div>

                <div class="price">
                    현재가격 {price}원
                </div>

                <div class="change {change_class}">
                    {change}
                </div>

            </td>

            <td class="volume-cell">
                {volume}
            </td>

            <td class="roc-filter-cell">
                {filter_content}
            </td>

            <td class="count-cell">
                {count_content}
            </td>

        </tr>

        <tr class="orderbook-subrow">

            <td colspan="5">
                {orderbook_html(item)}
            </td>

        </tr>
        """

    return html


# =========================================================
# 상승신호 집중 영역
# =========================================================

def focus_section(
    data
):

    focus = []

    for item in data:

        if not item.get(
            "filter_pass"
        ):
            continue

        if not item.get(
            "daily_pass"
        ):
            continue

        if not item.get(
            "signal_active"
        ):
            continue

        count = item.get(
            "signal_count"
        )

        if count is None:
            continue

        if not (
            DISPLAY_COUNT_MIN
            <= count
            <= DISPLAY_COUNT_MAX
        ):
            continue

        focus.append(
            item
        )

    if not focus:

        return """
        <section class="focus-section">

            <div class="section-title">
                🚀 상승 신호
            </div>

            <div class="focus-empty">
                현재 조건에 맞는 상승신호 없음
            </div>

        </section>
        """

    cards = ""

    for item in focus:

        coin = item["market"].replace(
            "KRW-",
            ""
        )

        price = format_price(
            item["price"]
        )

        count = item["signal_count"]

        cards += f"""
        <div class="focus-card">

            <div class="focus-coin">
                🚀 {coin}
            </div>

            <div class="focus-price">
                현재가격 {price}원
            </div>

            <div class="focus-count">
                COUNT {count}
            </div>

        </div>
        """

    return f"""
    <section class="focus-section">

        <div class="section-title">
            🚀 상승 신호
        </div>

        <div class="section-subtitle">
            ROC{SIGNAL_ROC_PERIOD}
            {format_timeframe(SIGNAL_TIMEFRAME)}
            0선 상승돌파
            · COUNT 0부터 표시
            · 0~1 반짝임
        </div>

        <div class="focus-grid">
            {cards}
        </div>

    </section>
    """


# =========================================================
# BTC 상태
# =========================================================

def btc_status():

    btc = latest_upbit_data.get(
        "KRW-BTC"
    )

    if not btc:

        return """
        <div class="btc-status">
            BTC 데이터 없음
        </div>
        """

    price = format_price(
        btc.get("price")
    )

    change = format_change(
        btc.get("daily_change")
    )

    count_html = roc_count_html(
        btc
    )

    return f"""
    <div class="btc-status">

        <div class="btc-title">
            ₿ BTC
        </div>

        <div class="btc-price">
            현재가격 {price}원
        </div>

        <div class="btc-change">
            일일 {change}
        </div>

        <div class="btc-signal">
            {count_html}
        </div>

    </div>
    """


# =========================================================
# 시장 요약
# =========================================================

def market_summary_html():

    data = list(
        latest_upbit_data.values()
    )

    positive = 0
    negative = 0

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

    return f"""
    <section class="market-summary">

        <div class="section-title">
            📊 시장 현황
        </div>

        <div class="summary-grid">

            <div>
                <span>TOP</span>
                <b>{len(data)}</b>
            </div>

            <div>
                <span>상승</span>
                <b>{positive}</b>
            </div>

            <div>
                <span>하락</span>
                <b>{negative}</b>
            </div>

        </div>

    </section>
    """


# =========================================================
# 전체 테이블
# =========================================================

def table_section():

    data = list(
        latest_upbit_data.values()
    )

    data.sort(
        key=lambda x:
            x.get(
                "volume_24h",
                0
            ),
        reverse=True
    )

    for i, item in enumerate(
        data,
        start=1
    ):
        item["rank"] = i

    return f"""
    <section class="table-section">

        <div class="section-title">
            🔥 거래대금 TOP {TOP_N}
        </div>

        <div class="section-subtitle">
            거래대금 순위
            · ROC 필터
            · ROC{SIGNAL_ROC_PERIOD}
            {format_timeframe(SIGNAL_TIMEFRAME)}
            상승 0선 돌파
            · COUNT 전체 표시
        </div>

        <div class="table-wrap">

            <table>

                <thead>

                    <tr>

                        <th>#</th>
                        <th>코인</th>
                        <th>거래대금</th>
                        <th>ROC 필터</th>
                        <th>ROC COUNT</th>

                    </tr>

                </thead>

                <tbody>

                    {rows_html(data)}

                </tbody>

            </table>

        </div>

    </section>
    """


# =========================================================
# CSS
# =========================================================

CSS = r"""
<style>

* {
    box-sizing: border-box;
}

body {
    margin: 0;
    background: #090d12;
    color: #e8edf3;
    font-family:
        -apple-system,
        BlinkMacSystemFont,
        "Segoe UI",
        sans-serif;
    font-size: 13px;
}

.container {
    width: 100%;
    max-width: 1100px;
    margin: 0 auto;
    padding: 10px;
}

.header {
    background: #111821;
    border: 1px solid #202a35;
    border-radius: 12px;
    padding: 13px;
    margin-bottom: 10px;
}

.title {
    font-size: 20px;
    font-weight: 800;
}

.subtitle {
    margin-top: 5px;
    color: #8d99a8;
    font-size: 11px;
}

.update-time {
    margin-top: 6px;
    color: #6f7d8c;
    font-size: 10px;
}

section {
    background: #111821;
    border: 1px solid #202a35;
    border-radius: 12px;
    margin-bottom: 10px;
    overflow: hidden;
}

.section-title {
    padding: 12px 13px 5px;
    font-size: 15px;
    font-weight: 800;
}

.section-subtitle {
    padding: 0 13px 10px;
    color: #7e8b9a;
    font-size: 10px;
}

.focus-grid {
    display: grid;
    grid-template-columns:
        repeat(auto-fit, minmax(150px, 1fr));
    gap: 7px;
    padding: 10px;
}

.focus-card {
    background: #0c1219;
    border: 1px solid #273340;
    border-radius: 9px;
    padding: 10px;
}

.focus-coin {
    font-size: 15px;
    font-weight: 800;
}

.focus-price {
    margin-top: 5px;
    color: #aeb8c4;
    font-size: 11px;
}

.focus-count {
    margin-top: 7px;
    font-size: 18px;
    font-weight: 900;
}

.focus-empty {
    padding: 15px;
    color: #657282;
    text-align: center;
}

.summary-grid {
    display: grid;
    grid-template-columns:
        repeat(3, 1fr);
    border-top: 1px solid #202a35;
}

.summary-grid div {
    padding: 11px;
    text-align: center;
    border-right: 1px solid #202a35;
}

.summary-grid div:last-child {
    border-right: 0;
}

.summary-grid span {
    display: block;
    color: #718092;
    font-size: 10px;
}

.summary-grid b {
    display: block;
    margin-top: 4px;
    font-size: 15px;
}

.table-wrap {
    overflow-x: auto;
}

table {
    width: 100%;
    border-collapse: collapse;
    table-layout: fixed;
}

th {
    background: #0c1219;
    color: #7e8b9a;
    font-size: 10px;
    font-weight: 700;
    padding: 9px 4px;
    border-top: 1px solid #202a35;
    border-bottom: 1px solid #202a35;
}

th:nth-child(1) {
    width: 6%;
}

th:nth-child(2) {
    width: 17%;
}

th:nth-child(3) {
    width: 15%;
}

th:nth-child(4) {
    width: 38%;
}

th:nth-child(5) {
    width: 24%;
}

td {
    padding: 8px 4px;
    border-bottom: 1px solid #1a232d;
    vertical-align: middle;
}

.rank {
    text-align: center;
    color: #677483;
    font-size: 11px;
}

.coin-cell {
    text-align: left;
}

.coin-name {
    font-weight: 800;
    font-size: 14px;
}

.price {
    margin-top: 3px;
    color: #aab5c1;
    font-size: 10px;
}

.change {
    margin-top: 2px;
    font-size: 10px;
    color: #7e8b9a;
}

.change.positive {
    color: #51d88a;
}

.change.negative {
    color: #ff6875;
}

.volume-cell {
    text-align: right;
    color: #c1cad4;
    font-weight: 700;
    font-size: 11px;
}

.roc-filter-cell {
    text-align: center !important;
}

.roc-line {
    line-height: 17px;
    white-space: nowrap;
    font-size: 10px;
}

.roc-line b {
    color: #aeb8c4;
    margin-right: 3px;
}

.count-cell {
    text-align: center;
}

.roc-count {
    display: inline-block;
    font-size: 16px;
    font-weight: 900;
    white-space: nowrap;
}

.signal-active {
    color: #ffb84d;
}

.count-empty {
    color: #46515e;
}

.orderbook-subrow td {
    padding: 0;
    border-bottom: 1px solid #202a35;
}

.orderbook-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 6px;
    padding: 6px 8px;
    background: #0b1117;
    color: #6f7d8c;
    font-size: 9px;
}

.orderbook-row .ask {
    color: #ff6875;
}

.orderbook-row .bid {
    color: #51d88a;
}

.orderbook-row .dominance {
    font-weight: 800;
}

.btc-status {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 11px 13px;
    border-top: 1px solid #202a35;
}

.btc-title {
    font-weight: 900;
    font-size: 15px;
}

.btc-price {
    color: #b8c2ce;
}

.btc-change {
    color: #7e8b9a;
}

.btc-signal {
    margin-left: auto;
}

/* =====================================================
   COUNT 0 / 1 반짝임
   ===================================================== */

.signal-flash-one {
    animation:
        signalFlashOne
        0.9s
        ease-in-out
        infinite;
}

@keyframes signalFlashOne {

    0% {
        opacity: 1;
        transform: scale(1);
        text-shadow:
            0 0 0 transparent;
    }

    35% {
        opacity: 0.25;
        transform: scale(1.04);
        text-shadow:
            0 0 12px rgba(255, 184, 77, 0.95);
    }

    70% {
        opacity: 1;
        transform: scale(1);
        text-shadow:
            0 0 5px rgba(255, 184, 77, 0.75);
    }

    100% {
        opacity: 1;
        transform: scale(1);
        text-shadow:
            0 0 0 transparent;
    }
}

/*
   행 전체 반짝임
   COUNT 0 / 1일 때만 적용
*/

tr.signal-flash-one {
    animation:
        rowFlashOne
        1.0s
        ease-in-out
        infinite;
}

@keyframes rowFlashOne {

    0% {
        background: rgba(255, 184, 77, 0.02);
    }

    40% {
        background: rgba(255, 184, 77, 0.16);
    }

    70% {
        background: rgba(255, 184, 77, 0.04);
    }

    100% {
        background: rgba(255, 184, 77, 0.02);
    }
}

.empty {
    text-align: center;
    padding: 25px;
    color: #657282;
}

@media (max-width: 700px) {

    body {
        font-size: 12px;
    }

    .container {
        padding: 5px;
    }

    .title {
        font-size: 17px;
    }

    th:nth-child(1) {
        width: 7%;
    }

    th:nth-child(2) {
        width: 19%;
    }

    th:nth-child(3) {
        width: 15%;
    }

    th:nth-child(4) {
        width: 35%;
    }

    th:nth-child(5) {
        width: 24%;
    }

    .roc-line {
        font-size: 8px;
    }

    .roc-count {
        font-size: 14px;
    }

    .orderbook-row {
        font-size: 8px;
    }

    .btc-status {
        flex-wrap: wrap;
        gap: 7px;
    }

    .btc-signal {
        margin-left: 0;
        width: 100%;
    }
}

</style>
"""


# =========================================================
# Dashboard
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def dashboard():

    data = list(
        latest_upbit_data.values()
    )

    data.sort(
        key=lambda x:
            x.get(
                "volume_24h",
                0
            ),
        reverse=True
    )

    for i, item in enumerate(
        data,
        start=1
    ):
        item["rank"] = i

    update_text = (
        last_upbit_update.strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        if last_upbit_update
        else "-"
    )

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
            ROC{SIGNAL_ROC_PERIOD}
            {format_timeframe(SIGNAL_TIMEFRAME)}
            SIGNAL
        </title>

        {CSS}

    </head>

    <body>

        <div class="container">

            <header class="header">

                <div class="title">
                    🚀 ROC{SIGNAL_ROC_PERIOD}
                    {format_timeframe(SIGNAL_TIMEFRAME)}
                    상승 SIGNAL
                </div>

                <div class="subtitle">
                    EMA 제거
                    · ROC 상승 0선 돌파만 표시
                    · COUNT 0부터 표시
                    · COUNT 0~1만 반짝임
                </div>

                <div class="update-time">
                    마지막 업데이트 :
                    {update_text}
                    KST
                </div>

            </header>

            {btc_status()}

            {market_summary_html()}

            {focus_section(data)}

            {table_section()}

        </div>

    </body>

    </html>
    """


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

            logging.exception(
                "스케줄러 오류: %s",
                e
            )

        time.sleep(1)


# =========================================================
# 시작
# =========================================================

def startup():

    logging.info(
        "=========================================="
    )

    logging.info(
        "ROC 대시보드 시작"
    )

    logging.info(
        "SIGNAL = ROC%s %s",
        SIGNAL_ROC_PERIOD,
        format_timeframe(
            SIGNAL_TIMEFRAME
        )
    )

    logging.info(
        "상승신호만 표시"
    )

    logging.info(
        "ROC 0선 상승돌파 COUNT = 0부터"
    )

    logging.info(
        "반짝임 COUNT = 0~1"
    )

    logging.info(
        "1H ROC 필터 = %s",
        USE_1H_ROC_FILTER
    )

    logging.info(
        "4H ROC 필터 = %s",
        USE_4H_ROC_FILTER
    )

    logging.info(
        "TOP_N = %s",
        TOP_N
    )

    logging.info(
        "EMA = 완전 제거"
    )

    logging.info(
        "=========================================="
    )

    validate_timeframe()

    update_dashboard()


# =========================================================
# 실행
# =========================================================

if __name__ == "__main__":

    startup()

    thread = threading.Thread(
        target=scheduler_loop,
        daemon=True
    )

    thread.start()

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )
