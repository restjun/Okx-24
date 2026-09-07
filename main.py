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
# FastAPI
# =========================================================

app = FastAPI()


# =========================================================
# 로그
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# =========================================================
# 기본 설정
# =========================================================

VOLUME_HOURS = 24

TOP_N = 30

UPDATE_MINUTES = 1

HISTORY_CHUNK = 200
MAX_HISTORY_CHUNKS = 10

USE_UPBIT = "Y"
USE_OKX = "N"

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10

KST = ZoneInfo("Asia/Seoul")


# =========================================================
# EMA 설정
# =========================================================

EMA_TIMEFRAME = 60

EMA1_FAST = 30
EMA1_MID = 60
EMA1_SLOW = 120

EMA1_MAX_COUNT = 100


# =========================================================
# ROC 설정
# =========================================================

ROC_PERIOD = 10

# ---------------------------------------------------------
# ROC 돌파 임박 기준
#
# 현재 ROC가
# -0.30 이상
#  0 미만
# 이면서
# 이전 ROC보다 현재 ROC가 상승하고 있으면
# 돌파 임박으로 판단
# ---------------------------------------------------------

ROC_NEAR_ZERO = -0.30


# =========================================================
# 지원 시간봉
# =========================================================

SUPPORTED_UPBIT_TIMEFRAMES = {
    5,
    15,
    30,
    60,
    240
}

SUPPORTED_OKX_TIMEFRAMES = {
    5,
    15,
    30,
    60,
    120,
    240,
    360,
    480,
    720,
    1440
}


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

last_request_time = 0


# =========================================================
# 시간
# =========================================================

def kst():
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


def format_timeframe(minutes):

    if minutes < 60:
        return f"{minutes}M"

    if minutes % 60 == 0:

        hours = minutes // 60

        if hours < 24:
            return f"{hours}H"

        days = hours // 24

        return f"{days}D"

    return f"{minutes}M"


# =========================================================
# OKX BAR
# =========================================================

def get_okx_bar(minutes):

    mapping = {
        5: "5m",
        15: "15m",
        30: "30m",
        60: "1H",
        120: "2H",
        240: "4H",
        360: "6H",
        480: "8H",
        720: "12H",
        1440: "1D"
    }

    return mapping.get(minutes)


def get_okx_bar_minutes(bar):

    mapping = {
        "5m": 5,
        "15m": 15,
        "30m": 30,
        "1H": 60,
        "2H": 120,
        "4H": 240,
        "6H": 360,
        "8H": 480,
        "12H": 720,
        "1D": 1440
    }

    return mapping.get(bar)


# =========================================================
# 현재 캔들 시작 시간
# =========================================================

def get_current_candle_start(timeframe_minutes):

    now = datetime.now(KST).replace(tzinfo=None)

    total_minutes = now.hour * 60 + now.minute

    candle_total = (
        total_minutes // timeframe_minutes
    ) * timeframe_minutes

    hour = candle_total // 60
    minute = candle_total % 60

    return now.replace(
        hour=hour,
        minute=minute,
        second=0,
        microsecond=0
    )


# =========================================================
# 시간봉 검증
# =========================================================

def validate_timeframe():

    if EMA_TIMEFRAME not in SUPPORTED_UPBIT_TIMEFRAMES:

        raise ValueError(
            f"Upbit에서 지원하지 않는 시간봉: {EMA_TIMEFRAME}"
        )

    if EMA_TIMEFRAME not in SUPPORTED_OKX_TIMEFRAMES:

        raise ValueError(
            f"OKX에서 지원하지 않는 시간봉: {EMA_TIMEFRAME}"
        )


# =========================================================
# Request
# =========================================================

def wait_request():

    global last_request_time

    with request_lock:

        now = time.time()

        elapsed = now - last_request_time

        if elapsed < REQUEST_INTERVAL:

            time.sleep(
                REQUEST_INTERVAL - elapsed
            )

        last_request_time = time.time()


# =========================================================
# Retry
# =========================================================

def retry(func, *args, **kwargs):

    for attempt in range(MAX_RETRIES):

        try:

            wait_request()

            response = func(*args, **kwargs)

            status = getattr(
                response,
                "status_code",
                200
            )

            if status == 429:

                wait_time = min(
                    RATE_LIMIT_WAIT * (2 ** attempt),
                    60
                )

                logger.warning(
                    f"429 Rate Limit | {wait_time}초 대기"
                )

                time.sleep(wait_time)

                continue

            if status >= 500:

                wait_time = min(
                    2 * (attempt + 1),
                    30
                )

                logger.warning(
                    f"서버 오류 {status} | "
                    f"{wait_time}초 대기"
                )

                time.sleep(wait_time)

                continue

            response.raise_for_status()

            return response

        except Exception as e:

            logger.warning(
                f"API 오류 "
                f"{attempt + 1}/{MAX_RETRIES}: {e}"
            )

            if attempt == MAX_RETRIES - 1:
                return None

            time.sleep(
                min(
                    2 * (attempt + 1),
                    30
                )
            )

    return None


# =========================================================
# Upbit 마켓
# =========================================================

def get_upbit_markets():

    global latest_upbit_markets

    url = "https://api.upbit.com/v1/ticker/all"

    params = {
        "markets": "KRW"
    }

    response = retry(
        requests.get,
        url,
        params=params,
        timeout=10
    )

    if response is None:
        return []

    data = response.json()

    markets = []

    for item in data:

        market = item.get("market", "")

        if not market.startswith("KRW-"):
            continue

        markets.append({
            "market": market,
            "trade_price": float(
                item.get("trade_price", 0) or 0
            ),
            "acc_trade_price_24h": float(
                item.get(
                    "acc_trade_price_24h",
                    0
                ) or 0
            )
        })

    latest_upbit_markets = markets

    return markets


# =========================================================
# USDT/KRW
# =========================================================

def get_usdt_krw():

    url = "https://api.upbit.com/v1/ticker"

    params = {
        "markets": "KRW-USDT"
    }

    response = retry(
        requests.get,
        url,
        params=params,
        timeout=10
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
            )
        )

    except Exception:

        return 0


# =========================================================
# Upbit 확정 캔들
# =========================================================

def get_upbit_candle(
    market,
    unit,
    count=200,
    to=None
):

    url = (
        f"https://api.upbit.com/v1/candles/"
        f"minutes/{unit}"
    )

    params = {
        "market": market,
        "count": count
    }

    if to is not None:
        params["to"] = to

    response = retry(
        requests.get,
        url,
        params=params,
        timeout=10
    )

    if response is None:
        return pd.DataFrame()

    try:

        data = response.json()

        rows = []

        current_start = get_current_candle_start(
            unit
        )

        for item in data:

            dt = pd.to_datetime(
                item["candle_date_time_kst"]
            )

            dt = dt.to_pydatetime()

            if dt >= current_start:
                continue

            rows.append({
                "datetime": dt,
                "o": float(
                    item["opening_price"]
                ),
                "h": float(
                    item["high_price"]
                ),
                "l": float(
                    item["low_price"]
                ),
                "c": float(
                    item["trade_price"]
                ),
                "volume": float(
                    item["candle_acc_trade_volume"]
                )
            })

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)

        df = (
            df
            .sort_values("datetime")
            .drop_duplicates(
                subset=["datetime"]
            )
            .reset_index(drop=True)
        )

        return df

    except Exception as e:

        logger.warning(
            f"Upbit candle 오류 {market}: {e}"
        )

        return pd.DataFrame()


# =========================================================
# Upbit 현재 캔들 포함
# =========================================================

def get_upbit_candle_with_current(
    market,
    unit,
    count=200
):

    url = (
        f"https://api.upbit.com/v1/candles/"
        f"minutes/{unit}"
    )

    params = {
        "market": market,
        "count": count
    }

    response = retry(
        requests.get,
        url,
        params=params,
        timeout=10
    )

    if response is None:
        return pd.DataFrame()

    try:

        data = response.json()

        rows = []

        for item in data:

            dt = pd.to_datetime(
                item["candle_date_time_kst"]
            )

            rows.append({
                "datetime": dt.to_pydatetime(),
                "o": float(
                    item["opening_price"]
                ),
                "h": float(
                    item["high_price"]
                ),
                "l": float(
                    item["low_price"]
                ),
                "c": float(
                    item["trade_price"]
                ),
                "volume": float(
                    item["candle_acc_trade_volume"]
                )
            })

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)

        df = (
            df
            .sort_values("datetime")
            .drop_duplicates(
                subset=["datetime"]
            )
            .reset_index(drop=True)
        )

        return df

    except Exception as e:

        logger.warning(
            f"Upbit current candle 오류 "
            f"{market}: {e}"
        )

        return pd.DataFrame()


# =========================================================
# Upbit 현재 ROC 데이터
# =========================================================

def get_upbit_current_roc_data(
    market,
    current_price
):

    df = get_upbit_candle_with_current(
        market,
        EMA_TIMEFRAME,
        count=200
    )

    if df.empty:
        return df

    current_start = get_current_candle_start(
        EMA_TIMEFRAME
    )

    mask = (
        df["datetime"] == current_start
    )

    if mask.any():

        df.loc[
            mask,
            "c"
        ] = current_price

    else:

        last_row = df.iloc[-1].copy()

        last_row["datetime"] = current_start
        last_row["c"] = current_price

        df = pd.concat(
            [
                df,
                pd.DataFrame(
                    [last_row]
                )
            ],
            ignore_index=True
        )

    return (
        df
        .sort_values("datetime")
        .drop_duplicates(
            subset=["datetime"],
            keep="last"
        )
        .reset_index(drop=True)
    )


# =========================================================
# Upbit 1H
# =========================================================

def get_upbit_1h(market):

    return get_upbit_candle(
        market,
        EMA_TIMEFRAME,
        count=200
    )


# =========================================================
# OKX OHLCV
# =========================================================

def get_okx_ohlcv(
    inst,
    bar="1H",
    limit=200,
    before=None
):

    url = "https://www.okx.com/api/v5/market/candles"

    params = {
        "instId": inst,
        "bar": bar,
        "limit": limit
    }

    if before is not None:
        params["before"] = before

    response = retry(
        requests.get,
        url,
        params=params,
        timeout=10
    )

    if response is None:
        return pd.DataFrame()

    try:

        result = response.json()

        data = result.get("data", [])

        rows = []

        bar_minutes = get_okx_bar_minutes(
            bar
        )

        current_start = get_current_candle_start(
            bar_minutes
        )

        for item in data:

            if len(item) < 9:
                continue

            ts = int(item[0])

            dt = (
                pd.to_datetime(
                    ts,
                    unit="ms",
                    utc=True
                )
                .tz_convert(KST)
                .tz_localize(None)
                .to_pydatetime()
            )

            # confirmed candle only
            if str(item[8]) != "1":
                continue

            if dt >= current_start:
                continue

            rows.append({
                "datetime": dt,
                "o": float(item[1]),
                "h": float(item[2]),
                "l": float(item[3]),
                "c": float(item[4]),
                "volume": float(item[5]),
                "volCcy": float(item[6]),
                "volCcyQuote": float(item[7])
            })

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)

        return (
            df
            .sort_values("datetime")
            .drop_duplicates(
                subset=["datetime"]
            )
            .reset_index(drop=True)
        )

    except Exception as e:

        logger.warning(
            f"OKX candle 오류 {inst}: {e}"
        )

        return pd.DataFrame()


# =========================================================
# OKX 현재 캔들
# =========================================================

def get_okx_ohlcv_current(
    inst,
    bar="1H",
    limit=200
):

    url = "https://www.okx.com/api/v5/market/candles"

    params = {
        "instId": inst,
        "bar": bar,
        "limit": limit
    }

    response = retry(
        requests.get,
        url,
        params=params,
        timeout=10
    )

    if response is None:
        return pd.DataFrame()

    try:

        result = response.json()

        data = result.get("data", [])

        rows = []

        for item in data:

            if len(item) < 9:
                continue

            ts = int(item[0])

            dt = (
                pd.to_datetime(
                    ts,
                    unit="ms",
                    utc=True
                )
                .tz_convert(KST)
                .tz_localize(None)
                .to_pydatetime()
            )

            rows.append({
                "datetime": dt,
                "o": float(item[1]),
                "h": float(item[2]),
                "l": float(item[3]),
                "c": float(item[4]),
                "volume": float(item[5]),
                "volCcy": float(item[6]),
                "volCcyQuote": float(item[7]),
                "confirm": str(item[8])
            })

        if not rows:
            return pd.DataFrame()

        return (
            pd.DataFrame(rows)
            .sort_values("datetime")
            .drop_duplicates(
                subset=["datetime"]
            )
            .reset_index(drop=True)
        )

    except Exception as e:

        logger.warning(
            f"OKX current candle 오류 "
            f"{inst}: {e}"
        )

        return pd.DataFrame()


# =========================================================
# OKX 현재가
# =========================================================

def get_okx_current_price(inst):

    url = (
        "https://www.okx.com/api/v5/"
        "market/ticker"
    )

    params = {
        "instId": inst
    }

    response = retry(
        requests.get,
        url,
        params=params,
        timeout=10
    )

    if response is None:
        return 0

    try:

        result = response.json()

        data = result.get("data", [])

        if not data:
            return 0

        return float(
            data[0].get(
                "last",
                0
            )
        )

    except Exception:

        return 0


# =========================================================
# History Upbit
# =========================================================

def history_upbit(
    market,
    unit,
    required=200
):

    frames = []

    to = None

    for _ in range(MAX_HISTORY_CHUNKS):

        df = get_upbit_candle(
            market,
            unit,
            count=HISTORY_CHUNK,
            to=to
        )

        if df.empty:
            break

        frames.append(df)

        if sum(
            len(x)
            for x in frames
        ) >= required:

            break

        oldest = df["datetime"].min()

        to = oldest.strftime(
            "%Y-%m-%dT%H:%M:%S"
        )

        time.sleep(0.02)

    if not frames:
        return pd.DataFrame()

    result = pd.concat(
        frames,
        ignore_index=True
    )

    result = (
        result
        .sort_values("datetime")
        .drop_duplicates(
            subset=["datetime"]
        )
        .reset_index(drop=True)
    )

    return result.tail(required).reset_index(
        drop=True
    )


# =========================================================
# History OKX
# =========================================================

def history_okx(
    inst,
    bar,
    required=200
):

    frames = []

    before = None

    for _ in range(MAX_HISTORY_CHUNKS):

        df = get_okx_ohlcv(
            inst,
            bar=bar,
            limit=HISTORY_CHUNK,
            before=before
        )

        if df.empty:
            break

        frames.append(df)

        if sum(
            len(x)
            for x in frames
        ) >= required:

            break

        oldest = df["datetime"].min()

        before = str(
            int(
                pd.Timestamp(
                    oldest,
                    tz=KST
                ).timestamp()
                * 1000
            )
        )

        time.sleep(0.02)

    if not frames:
        return pd.DataFrame()

    result = pd.concat(
        frames,
        ignore_index=True
    )

    result = (
        result
        .sort_values("datetime")
        .drop_duplicates(
            subset=["datetime"]
        )
        .reset_index(drop=True)
    )

    return result.tail(required).reset_index(
        drop=True
    )


# =========================================================
# EMA
# =========================================================

def ema(df, period):

    close = pd.to_numeric(
        df["c"],
        errors="coerce"
    )

    return close.ewm(
        span=period,
        adjust=False,
        min_periods=1
    ).mean()


# =========================================================
# 방향
# =========================================================

def direction(df):

    if df.empty:
        return "none"

    e30 = ema(
        df,
        EMA1_FAST
    ).iloc[-1]

    e60 = ema(
        df,
        EMA1_MID
    ).iloc[-1]

    e120 = ema(
        df,
        EMA1_SLOW
    ).iloc[-1]

    if (
        e30 > e60
        and e60 > e120
    ):

        return "long"

    if (
        e30 < e60
        and e60 < e120
    ):

        return "short"

    return "none"


# =========================================================
# EMA 정배열 유지 개수
# =========================================================

def ema_alignment_count(df):

    if df.empty:
        return {
            "direction": "none",
            "count": 0
        }

    temp = df.copy()

    temp["ema30"] = ema(
        temp,
        EMA1_FAST
    )

    temp["ema60"] = ema(
        temp,
        EMA1_MID
    )

    temp["ema120"] = ema(
        temp,
        EMA1_SLOW
    )

    dirs = []

    for _, row in temp.iterrows():

        if (
            row["ema30"] > row["ema60"]
            and row["ema60"] > row["ema120"]
        ):

            dirs.append("long")

        elif (
            row["ema30"] < row["ema60"]
            and row["ema60"] < row["ema120"]
        ):

            dirs.append("short")

        else:

            dirs.append("none")

    current_direction = dirs[-1]

    if current_direction == "none":

        return {
            "direction": "none",
            "count": 0
        }

    count = 0

    for d in reversed(dirs):

        if d != current_direction:
            break

        count += 1

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

        return (
            f"🟢({result['count']})"
        )

    if result["direction"] == "short":

        return (
            f"🔴({result['count']})"
        )

    return "⚪(0)"


# =========================================================
# ROC
# =========================================================

def roc(
    df,
    period=ROC_PERIOD
):

    close = pd.to_numeric(
        df["c"],
        errors="coerce"
    )

    return (
        close
        / close.shift(period)
        - 1
    ) * 100


# =========================================================
# ROC 분석
# =========================================================

def roc_analysis(
    df_confirmed,
    df_current
):

    result = {
        "roc10": 0,
        "roc10_previous": 0,
        "roc10_count": 0,
        "state": "none",
        "cross": "none",
        "near_zero": False,
        "near_zero_score": 0
    }

    if (
        df_confirmed.empty
        or df_current.empty
    ):

        return result

    confirmed_roc = roc(
        df_confirmed,
        ROC_PERIOD
    )

    current_roc = roc(
        df_current,
        ROC_PERIOD
    )

    if len(confirmed_roc) == 0:
        return result

    if len(current_roc) == 0:
        return result

    previous_10 = confirmed_roc.iloc[-1]
    current_10 = current_roc.iloc[-1]

    if pd.isna(previous_10):
        return result

    if pd.isna(current_10):
        return result

    previous_10 = float(
        previous_10
    )

    current_10 = float(
        current_10
    )

    result["roc10"] = current_10
    result["roc10_previous"] = previous_10

    # -----------------------------------------------------
    # ROC 양수 연속 개수
    # -----------------------------------------------------

    count = 0

    for value in reversed(
        current_roc.dropna().tolist()
    ):

        if value > 0:
            count += 1
        else:
            break

    result["roc10_count"] = count

    # -----------------------------------------------------
    # 기존 상향 돌파
    # -----------------------------------------------------

    if (
        previous_10 <= 0
        and current_10 > 0
    ):

        result["state"] = "long"
        result["cross"] = "up"

    # -----------------------------------------------------
    # 기존 하향 돌파
    # -----------------------------------------------------

    elif (
        previous_10 >= 0
        and current_10 < 0
    ):

        result["state"] = "short"
        result["cross"] = "down"

    # -----------------------------------------------------
    # ROC 돌파 임박
    #
    # 현재 ROC가 0보다 작고
    # 이전 ROC보다 상승하며
    # -0.30 이상
    # -----------------------------------------------------

    if (
        current_10 < 0
        and current_10 >= ROC_NEAR_ZERO
        and current_10 > previous_10
    ):

        result["near_zero"] = True

        # 0에 가까울수록 점수가 높음
        result["near_zero_score"] = (
            current_10
        )

    return result


# =========================================================
# 일봉 변화율
# =========================================================

def daily_change_upbit(market):

    url = (
        "https://api.upbit.com/v1/candles/days"
    )

    params = {
        "market": market,
        "count": 2
    }

    response = retry(
        requests.get,
        url,
        params=params,
        timeout=10
    )

    if response is None:
        return 0

    try:

        data = response.json()

        if len(data) < 2:
            return 0

        previous = float(
            data[1]["trade_price"]
        )

        current = float(
            data[0]["trade_price"]
        )

        if previous == 0:
            return 0

        return (
            (current / previous) - 1
        ) * 100

    except Exception:

        return 0


# =========================================================
# 일봉 변화율 - DataFrame
# =========================================================

def daily_changes(df):

    if df.empty:
        return 0

    temp = df.copy()

    temp["datetime"] = pd.to_datetime(
        temp["datetime"]
    )

    temp = temp.set_index(
        "datetime"
    )

    daily = temp["c"].resample(
        "1D",
        offset="9h"
    ).last().dropna()

    if len(daily) < 2:
        return 0

    previous = daily.iloc[-2]
    current = daily.iloc[-1]

    if previous == 0:
        return 0

    return (
        current / previous - 1
    ) * 100


# =========================================================
# 변화율 값
# =========================================================

def get_change_value(
    market,
    okx=False
):

    if okx:

        return 0

    return daily_change_upbit(
        market
    )


# =========================================================
# 변화율 표시
# =========================================================

def format_change(value):

    if value > 0:

        return (
            f"<span class='up'>"
            f"+{value:.2f}%"
            f"</span>"
        )

    if value < 0:

        return (
            f"<span class='down'>"
            f"{value:.2f}%"
            f"</span>"
        )

    return "0.00%"


# =========================================================
# 거래대금 표시
# =========================================================

def format_volume(value):

    value = float(value or 0)

    if value >= 100000000000:

        return (
            f"{value / 100000000000:.2f}조"
        )

    if value >= 100000000:

        return (
            f"{value / 100000000:.0f}억"
        )

    if value >= 10000:

        return (
            f"{value / 10000:.0f}만"
        )

    return f"{value:.0f}"


# =========================================================
# 빈 분석
# =========================================================

def empty_analysis():

    return {
        "ema_1h": "⚪(0)",
        "roc": {
            "roc10": 0,
            "roc10_previous": 0,
            "roc10_count": 0,
            "state": "none",
            "cross": "none",
            "near_zero": False,
            "near_zero_score": 0
        },
        "changes": 0,
        "qualified": False,
        "short_qualified": False,
        "near_zero_qualified": False,
        "direction_1h": "none",
        "df1h": pd.DataFrame()
    }


# =========================================================
# 분석
# =========================================================

def analyze(
    market,
    okx=False,
    current_price=None
):

    empty = empty_analysis()

    bar = get_okx_bar(
        EMA_TIMEFRAME
    )

    # -----------------------------------------------------
    # 확정 캔들
    # -----------------------------------------------------

    if okx:

        df_confirmed = history_okx(
            market,
            bar,
            required=200
        )

    else:

        df_confirmed = history_upbit(
            market,
            EMA_TIMEFRAME,
            required=200
        )

    if df_confirmed.empty:

        return empty

    # -----------------------------------------------------
    # EMA
    # -----------------------------------------------------

    ema_result = ema_alignment_count(
        df_confirmed
    )

    direction_1h = (
        ema_result["direction"]
    )

    ema_count = (
        ema_result["count"]
    )

    ema_text = ema_display(
        df_confirmed
    )

    # -----------------------------------------------------
    # 현재 캔들
    # -----------------------------------------------------

    if okx:

        df_current = get_okx_ohlcv_current(
            market,
            bar,
            limit=200
        )

        if (
            current_price is not None
            and not df_current.empty
        ):

            current_start = (
                get_current_candle_start(
                    EMA_TIMEFRAME
                )
            )

            mask = (
                df_current["datetime"]
                == current_start
            )

            if mask.any():

                df_current.loc[
                    mask,
                    "c"
                ] = current_price

            else:

                last_row = (
                    df_current.iloc[-1]
                    .copy()
                )

                last_row["datetime"] = (
                    current_start
                )

                last_row["c"] = (
                    current_price
                )

                df_current = pd.concat(
                    [
                        df_current,
                        pd.DataFrame(
                            [last_row]
                        )
                    ],
                    ignore_index=True
                )

    else:

        if current_price is None:

            current_price = (
                get_upbit_markets()
            )

            current_price = next(
                (
                    x["trade_price"]
                    for x in current_price
                    if x["market"] == market
                ),
                0
            )

        df_current = (
            get_upbit_current_roc_data(
                market,
                current_price
            )
        )

    if df_current.empty:

        return empty

    # -----------------------------------------------------
    # ROC
    # -----------------------------------------------------

    roc_result = roc_analysis(
        df_confirmed,
        df_current
    )

    # -----------------------------------------------------
    # 기존 매수 조건
    #
    # EMA30 > EMA60 > EMA120
    # EMA count <= 100
    # ROC10 상향 돌파
    # -----------------------------------------------------

    long_qualified = (
        direction_1h == "long"
        and ema_count <= EMA1_MAX_COUNT
        and roc_result["cross"] == "up"
    )

    # -----------------------------------------------------
    # 기존 숏 조건
    #
    # EMA30 < EMA60 < EMA120
    # EMA count <= 100
    # ROC10 하향 돌파
    # -----------------------------------------------------

    short_qualified = (
        direction_1h == "short"
        and ema_count <= EMA1_MAX_COUNT
        and roc_result["cross"] == "down"
    )

    # -----------------------------------------------------
    # 신규 ROC 돌파 임박
    #
    # 반드시 EMA 정배열
    # -----------------------------------------------------

    near_zero_qualified = (
        direction_1h == "long"
        and ema_count <= EMA1_MAX_COUNT
        and roc_result["near_zero"]
    )

    # -----------------------------------------------------
    # 변화율
    # -----------------------------------------------------

    changes = get_change_value(
        market,
        okx=okx
    )

    return {
        "ema_1h": ema_text,
        "roc": roc_result,
        "changes": changes,
        "qualified": long_qualified,
        "short_qualified": short_qualified,
        "near_zero_qualified": near_zero_qualified,
        "direction_1h": direction_1h,
        "df1h": df_confirmed
    }


# =========================================================
# Row
# =========================================================

def make_row(
    rank,
    name,
    volume,
    analysis,
    current_price=None
):

    return {
        "rank": rank,
        "name": name,
        "change": format_change(
            analysis["changes"]
        ),
        "change_value": analysis["changes"],
        "volume": volume,
        "current_price": current_price,
        "ema_1h": analysis["ema_1h"],
        "roc": analysis["roc"],
        "qualified": analysis["qualified"],
        "short_qualified": analysis["short_qualified"],
        "near_zero_qualified": (
            analysis["near_zero_qualified"]
        ),
        "direction_1h": analysis["direction_1h"]
    }


# =========================================================
# 후보
# =========================================================

def is_upbit_buy_candidate(row):

    return row.get(
        "qualified",
        False
    )


def is_okx_long_candidate(row):

    return row.get(
        "qualified",
        False
    )


def is_okx_short_candidate(row):

    return row.get(
        "short_qualified",
        False
    )


# =========================================================
# ROC 돌파 임박 후보
# =========================================================

def is_roc_near_zero_candidate(row):

    return row.get(
        "near_zero_qualified",
        False
    )


# =========================================================
# Upbit 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update_time

    markets = get_upbit_markets()

    if not markets:

        logger.warning(
            "Upbit 마켓 데이터를 가져오지 못했습니다."
        )

        return

    markets = sorted(
        markets,
        key=lambda x: x[
            "acc_trade_price_24h"
        ],
        reverse=True
    )

    top_markets = markets[:TOP_N]

    rows = []

    for rank, item in enumerate(
        top_markets,
        start=1
    ):

        market = item["market"]

        try:

            analysis = analyze(
                market,
                okx=False,
                current_price=item[
                    "trade_price"
                ]
            )

            coin_name = market.replace(
                "KRW-",
                ""
            )

            row = make_row(
                rank=rank,
                name=coin_name,
                volume=item[
                    "acc_trade_price_24h"
                ],
                analysis=analysis,
                current_price=item[
                    "trade_price"
                ]
            )

            rows.append(row)

        except Exception as e:

            logger.warning(
                f"Upbit 분석 오류 "
                f"{market}: {e}"
            )

    latest_upbit_data = rows

    latest_upbit_update_time = kst()

    buy_count = sum(
        1
        for row in rows
        if is_upbit_buy_candidate(row)
    )

    near_count = sum(
        1
        for row in rows
        if is_roc_near_zero_candidate(row)
    )

    logger.info(
        f"Upbit 업데이트 완료 | "
        f"매수 {buy_count}개 | "
        f"ROC 임박 {near_count}개"
    )


# =========================================================
# OKX 심볼
# =========================================================

def get_okx_symbols():

    url = (
        "https://www.okx.com/api/v5/"
        "public/instruments"
    )

    params = {
        "instType": "SWAP"
    }

    response = retry(
        requests.get,
        url,
        params=params,
        timeout=10
    )

    if response is None:
        return []

    try:

        result = response.json()

        data = result.get(
            "data",
            []
        )

        symbols = []

        for item in data:

            inst_id = item.get(
                "instId",
                ""
            )

            state = item.get(
                "state",
                ""
            )

            if (
                inst_id.endswith(
                    "-USDT-SWAP"
                )
                and state == "live"
            ):

                symbols.append(
                    inst_id
                )

        return symbols

    except Exception:

        return []


# =========================================================
# OKX 거래대금
# =========================================================

def get_okx_volume(
    inst,
    usdt
):

    bar = get_okx_bar(
        60
    )

    url = (
        "https://www.okx.com/api/v5/"
        "market/candles"
    )

    params = {
        "instId": inst,
        "bar": bar,
        "limit": VOLUME_HOURS
    }

    response = retry(
        requests.get,
        url,
        params=params,
        timeout=10
    )

    if response is None:
        return 0

    try:

        result = response.json()

        data = result.get(
            "data",
            []
        )

        total = 0

        for item in data:

            if len(item) < 8:
                continue

            total += float(
                item[7] or 0
            )

        # 기존 UI 기준 10분의 1
        total = total * usdt / 10

        return total

    except Exception:

        return 0


# =========================================================
# OKX 업데이트
# =========================================================

def update_okx(usdt):

    global latest_okx_data
    global latest_okx_update_time

    if usdt <= 0:

        logger.warning(
            "USDT/KRW 가격이 없습니다."
        )

        return

    symbols = get_okx_symbols()

    if not symbols:

        logger.warning(
            "OKX 심볼을 가져오지 못했습니다."
        )

        return

    upbit_coin_set = set()

    for market in latest_upbit_markets:

        market_name = market[
            "market"
        ]

        if market_name.startswith(
            "KRW-"
        ):

            upbit_coin_set.add(
                market_name.replace(
                    "KRW-",
                    ""
                )
            )

    volumes = []

    for inst in symbols:

        try:

            volume = get_okx_volume(
                inst,
                usdt
            )

            if volume <= 0:
                continue

            volumes.append({
                "inst": inst,
                "volume": volume
            })

        except Exception as e:

            logger.warning(
                f"OKX 거래대금 오류 "
                f"{inst}: {e}"
            )

    volumes.sort(
        key=lambda x: x["volume"],
        reverse=True
    )

    top_symbols = volumes[:TOP_N]

    rows = []

    for rank, item in enumerate(
        top_symbols,
        start=1
    ):

        inst = item["inst"]

        try:

            current_price = (
                get_okx_current_price(
                    inst
                )
            )

            analysis = analyze(
                inst,
                okx=True,
                current_price=current_price
            )

            coin_name = (
                inst
                .replace(
                    "-USDT-SWAP",
                    ""
                )
            )

            if coin_name in upbit_coin_set:

                display_name = (
                    f"{coin_name} (업비트)"
                )

            else:

                display_name = coin_name

            row = make_row(
                rank=rank,
                name=display_name,
                volume=item["volume"],
                analysis=analysis,
                current_price=current_price
            )

            rows.append(row)

        except Exception as e:

            logger.warning(
                f"OKX 분석 오류 "
                f"{inst}: {e}"
            )

    latest_okx_data = rows

    latest_okx_update_time = kst()

    long_count = sum(
        1
        for row in rows
        if is_okx_long_candidate(row)
    )

    short_count = sum(
        1
        for row in rows
        if is_okx_short_candidate(row)
    )

    near_count = sum(
        1
        for row in rows
        if is_roc_near_zero_candidate(row)
    )

    logger.info(
        f"OKX 업데이트 완료 | "
        f"LONG {long_count}개 | "
        f"SHORT {short_count}개 | "
        f"ROC 임박 {near_count}개"
    )


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    global latest_usdt_krw

    if not update_lock.acquire(
        blocking=False
    ):

        logger.info(
            "이전 업데이트가 아직 진행 중입니다."
        )

        return

    try:

        # -------------------------------------------------
        # Upbit
        # -------------------------------------------------

        if USE_UPBIT == "Y":

            update_upbit()

        # -------------------------------------------------
        # USDT
        # -------------------------------------------------

        usdt = get_usdt_krw()

        if usdt > 0:

            latest_usdt_krw = usdt

        else:

            usdt = latest_usdt_krw

        # -------------------------------------------------
        # OKX
        # -------------------------------------------------

        if USE_OKX == "Y":

            update_okx(usdt)

    except Exception as e:

        logger.exception(
            f"전체 업데이트 오류: {e}"
        )

    finally:

        update_lock.release()


# =========================================================
# ROC HTML
# =========================================================

def roc_html(r):

    if not r:

        return "-"

    value = float(
        r.get(
            "roc10",
            0
        )
    )

    previous = float(
        r.get(
            "roc10_previous",
            0
        )
    )

    count = int(
        r.get(
            "roc10_count",
            0
        )
    )

    cross = r.get(
        "cross",
        "none"
    )

    near_zero = r.get(
        "near_zero",
        False
    )

    if value > 0:

        value_class = "up"

    elif value < 0:

        value_class = "down"

    else:

        value_class = ""

    if cross == "up":

        cross_html = (
            "<span class='cross-up'>↑0</span>"
        )

    elif cross == "down":

        cross_html = (
            "<span class='cross-down'>↓0</span>"
        )

    else:

        cross_html = ""

    # -----------------------------------------------------
    # 돌파 임박
    # -----------------------------------------------------

    if near_zero:

        cross_html += (
            "<span class='near'>🔥</span>"
        )

    return (
        f"<div>"
        f"<span class='roc-count'>"
        f"{count}"
        f"</span>"
        f" "
        f"<span class='{value_class}'>"
        f"{value:+.2f}%"
        f"</span>"
        f" {cross_html}"
        f"</div>"
    )


# =========================================================
# Signal HTML
# =========================================================

def signal_html(row):

    if row.get(
        "qualified",
        False
    ):

        return (
            "<span class='signal-buy'>"
            "🟢 매수"
            "</span>"
        )

    if row.get(
        "short_qualified",
        False
    ):

        return (
            "<span class='signal-short'>"
            "🔴 숏"
            "</span>"
        )

    return "-"


# =========================================================
# EMA HTML
# =========================================================

def ema_html(e):

    return e or "-"


# =========================================================
# 일반 테이블
# =========================================================

def rows_html(data):

    if not data:

        return (
            "<div class='empty'>"
            "데이터 없음"
            "</div>"
        )

    html = ""

    for row in data:

        html += (
            "<tr>"
            f"<td>{row['rank']}</td>"
            f"<td class='coin'>{row['name']}</td>"
            f"<td>{format_volume(row['volume'])}</td>"
            f"<td>{ema_html(row['ema_1h'])}</td>"
            f"<td>{roc_html(row['roc'])}</td>"
            f"<td>{signal_html(row)}</td>"
            "</tr>"
        )

    return html


# =========================================================
# 테이블
# =========================================================

def table_html(
    data,
    title
):

    timeframe_label = (
        format_timeframe(
            EMA_TIMEFRAME
        )
    )

    return f"""
    <div class="table-wrap">

        <div class="table-title">
            {title}
        </div>

        <table>

            <thead>

                <tr>
                    <th>순위</th>
                    <th>코인</th>
                    <th>거래대금</th>
                    <th>EMA</th>
                    <th>ROC10</th>
                    <th>신호</th>
                </tr>

            </thead>

            <tbody>

                {rows_html(data)}

            </tbody>

        </table>

    </div>
    """


# =========================================================
# ROC 돌파 임박 TOP
# =========================================================

def roc_near_zero_section(data):

    candidates = [
        row
        for row in data
        if is_roc_near_zero_candidate(row)
    ]

    # -----------------------------------------------------
    # ROC가 0에 가장 가까운 순서
    #
    # 예:
    # -0.05
    # -0.11
    # -0.21
    # -0.29
    # -----------------------------------------------------

    candidates.sort(
        key=lambda row: row[
            "roc"
        ].get(
            "roc10",
            -999
        ),
        reverse=True
    )

    candidates = candidates[:10]

    if not candidates:

        return """
        <div class="focus-box near-box">

            <div class="focus-title">
                🔥 ROC10 돌파 임박 TOP
            </div>

            <div class="empty">
                현재 돌파 임박 코인 없음
            </div>

        </div>
        """

    html = """
    <div class="focus-box near-box">

        <div class="focus-title">
            🔥 ROC10 돌파 임박 TOP
        </div>

        <div class="focus-sub">
            EMA30 &gt; EMA60 &gt; EMA120
            · ROC10 상승
            · ROC10 -0.30 ~ 0
        </div>

        <div class="near-list">
    """

    for index, row in enumerate(
        candidates,
        start=1
    ):

        roc_value = row[
            "roc"
        ].get(
            "roc10",
            0
        )

        previous = row[
            "roc"
        ].get(
            "roc10_previous",
            0
        )

        html += f"""
        <div class="near-row">

            <div class="near-rank">
                {index}
            </div>

            <div class="near-coin">
                {row['name']}
            </div>

            <div class="near-ema">
                {row['ema_1h']}
            </div>

            <div class="near-roc">
                <span class="near-current">
                    {roc_value:+.2f}%
                </span>

                <span class="near-arrow">
                    ↑
                </span>

                <span class="near-previous">
                    {previous:+.2f}%
                </span>
            </div>

        </div>
        """

    html += """
        </div>
    </div>
    """

    return html


# =========================================================
# Buy Focus
# =========================================================

def buy_focus_section(data):

    candidates = [
        row
        for row in data
        if is_upbit_buy_candidate(row)
    ]

    if not candidates:

        return """
        <div class="focus-box">

            <div class="focus-title">
                🟢 EMA 정배열 + ROC10 상향돌파
            </div>

            <div class="empty">
                현재 조건 충족 코인 없음
            </div>

        </div>
        """

    html = """
    <div class="focus-box">

        <div class="focus-title">
            🟢 EMA 정배열 + ROC10 상향돌파
        </div>

        <div class="focus-list">
    """

    for row in candidates:

        html += f"""
        <div class="focus-row">

            <span>
                {row['name']}
            </span>

            <span>
                {row['ema_1h']}
            </span>

            <span class="up">
                {row['roc']['roc10']:+.2f}%
            </span>

        </div>
        """

    html += """
        </div>
    </div>
    """

    return html


# =========================================================
# OKX Long Focus
# =========================================================

def okx_long_section(data):

    candidates = [
        row
        for row in data
        if is_okx_long_candidate(row)
    ]

    if not candidates:

        return """
        <div class="focus-box">

            <div class="focus-title">
                🟢 OKX LONG
            </div>

            <div class="empty">
                현재 조건 충족 없음
            </div>

        </div>
        """

    html = """
    <div class="focus-box">

        <div class="focus-title">
            🟢 OKX LONG
        </div>

        <div class="focus-list">
    """

    for row in candidates:

        html += f"""
        <div class="focus-row">

            <span>
                {row['name']}
            </span>

            <span>
                {row['ema_1h']}
            </span>

            <span class="up">
                {row['roc']['roc10']:+.2f}%
            </span>

        </div>
        """

    html += """
        </div>
    </div>
    """

    return html


# =========================================================
# OKX Short Focus
# =========================================================

def okx_short_section(data):

    candidates = [
        row
        for row in data
        if is_okx_short_candidate(row)
    ]

    if not candidates:

        return """
        <div class="focus-box">

            <div class="focus-title">
                🔴 OKX SHORT
            </div>

            <div class="empty">
                현재 조건 충족 없음
            </div>

        </div>
        """

    html = """
    <div class="focus-box">

        <div class="focus-title">
            🔴 OKX SHORT
        </div>

        <div class="focus-list">
    """

    for row in candidates:

        html += f"""
        <div class="focus-row">

            <span>
                {row['name']}
            </span>

            <span>
                {row['ema_1h']}
            </span>

            <span class="down">
                {row['roc']['roc10']:+.2f}%
            </span>

        </div>
        """

    html += """
        </div>
    </div>
    """

    return html


# =========================================================
# HTML
# =========================================================

HTML_TEMPLATE = """
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
📊 EMA1 · ROC10 전략
</title>

<style>

* {
    box-sizing: border-box;
}

body {

    margin: 0;

    padding: 10px;

    background: #111;

    color: #eee;

    font-family:
        Arial,
        sans-serif;

    font-size: 13px;
}

.container {

    width: 100%;

    max-width: 900px;

    margin: auto;
}

h1 {

    font-size: 19px;

    margin:
        5px 0 10px;

    text-align: center;
}

.status {

    text-align: center;

    color: #aaa;

    font-size: 11px;

    margin-bottom: 10px;
}

.info {

    background: #1a1a1a;

    border: 1px solid #333;

    border-radius: 8px;

    padding: 9px;

    margin-bottom: 10px;

    line-height: 1.6;

    font-size: 11px;
}

.table-wrap {

    background: #171717;

    border:
        1px solid #333;

    border-radius: 8px;

    overflow: hidden;

    margin-bottom: 10px;
}

.table-title {

    padding: 9px;

    background: #202020;

    font-weight: bold;

    font-size: 13px;
}

table {

    width: 100%;

    border-collapse:
        collapse;

    table-layout: fixed;
}

th,
td {

    border-bottom:
        1px solid #292929;

    padding:
        7px 3px;

    text-align: center;

    white-space:
        nowrap;

    overflow: hidden;

    text-overflow:
        ellipsis;
}

th {

    color: #aaa;

    font-size: 10px;

    background: #181818;
}

td {

    font-size: 11px;
}

th:nth-child(1),
td:nth-child(1) {
    width: 6%;
}

th:nth-child(2),
td:nth-child(2) {
    width: 20%;
}

th:nth-child(3),
td:nth-child(3) {
    width: 15%;
}

th:nth-child(4),
td:nth-child(4) {
    width: 19%;
}

th:nth-child(5),
td:nth-child(5) {
    width: 24%;
}

th:nth-child(6),
td:nth-child(6) {
    width: 16%;
}

.coin {

    font-weight: bold;
}

.up {

    color: #00e676;

    font-weight: bold;
}

.down {

    color: #ff5252;

    font-weight: bold;
}

.cross-up {

    color: #00e676;

    font-weight: bold;

    margin-left: 2px;
}

.cross-down {

    color: #ff5252;

    font-weight: bold;

    margin-left: 2px;
}

.roc-count {

    color: #aaa;

    font-size: 10px;
}

.signal-buy {

    color: #00e676;

    font-weight: bold;
}

.signal-short {

    color: #ff5252;

    font-weight: bold;
}

.empty {

    padding: 12px;

    text-align: center;

    color: #777;
}

.focus-box {

    background: #171717;

    border:
        1px solid #333;

    border-radius: 8px;

    margin-bottom: 10px;

    overflow: hidden;
}

.near-box {

    border:
        1px solid #6d4c00;
}

.focus-title {

    padding: 9px;

    background: #202020;

    font-weight: bold;

    font-size: 13px;
}

.focus-sub {

    padding:
        5px 9px 8px;

    color: #999;

    font-size: 10px;
}

.focus-list {

    padding: 4px 9px 8px;
}

.focus-row {

    display: flex;

    justify-content:
        space-between;

    align-items: center;

    padding:
        7px 2px;

    border-bottom:
        1px solid #292929;

    font-size: 11px;
}

.focus-row:last-child {

    border-bottom: 0;
}

.near-list {

    padding:
        3px 8px 8px;
}

.near-row {

    display: grid;

    grid-template-columns:
        9%
        34%
        22%
        35%;

    align-items: center;

    padding:
        7px 2px;

    border-bottom:
        1px solid #292929;
}

.near-row:last-child {

    border-bottom: 0;
}

.near-rank {

    color: #999;

    text-align: center;
}

.near-coin {

    font-weight: bold;

    overflow: hidden;

    text-overflow: ellipsis;

    white-space: nowrap;
}

.near-ema {

    text-align: center;
}

.near-roc {

    text-align: right;

    white-space: nowrap;
}

.near-current {

    color: #ffd54f;

    font-weight: bold;
}

.near-arrow {

    color: #00e676;

    font-weight: bold;

    margin-left: 2px;
}

.near-previous {

    color: #777;

    font-size: 10px;

    margin-left: 2px;
}

.near {

    margin-left: 2px;
}

@media (max-width: 600px) {

    body {

        padding: 6px;

        font-size: 12px;
    }

    h1 {

        font-size: 17px;
    }

    th {

        font-size: 9px;
    }

    td {

        font-size: 10px;

        padding:
            6px 2px;
    }

    .focus-title {

        font-size: 12px;
    }

    .near-row {

        grid-template-columns:
            9%
            32%
            22%
            37%;
    }

}

@media (max-width: 380px) {

    body {

        padding: 4px;
    }

    th {

        font-size: 8px;
    }

    td {

        font-size: 9px;
    }

    .near-row {

        grid-template-columns:
            9%
            31%
            21%
            39%;
    }

}

</style>

</head>

<body>

<div class="container">

<h1>
📊 EMA1 · ROC10 전략
</h1>

<div class="status">

    Upbit:
    {upbit_enabled}

    ·

    OKX:
    {okx_enabled}

    <br>

    마지막 Upbit:
    {upbit_time}

    ·

    마지막 OKX:
    {okx_time}

    <br>

    USDT/KRW:
    {usdt:,.0f}

</div>

<div class="info">

    <b>{timeframe} EMA1 + ROC10</b>

    <br>

    EMA1 =
    {fast}/{mid}/{slow}

    <br>

    🟢 매수:
    EMA30 &gt; EMA60 &gt; EMA120
    + ROC10 상향돌파

    <br>

    🔴 숏:
    EMA30 &lt; EMA60 &lt; EMA120
    + ROC10 하향돌파

    <br>

    🔥 돌파 임박:
    EMA 정배열
    + ROC10 상승
    + ROC10 -0.30 ~ 0

    <br>

    ROC10 =
    (현재 종가 / 10봉 전 종가 - 1) × 100

</div>

{near_section}

{buy_section}

{okx_long_section}

{okx_short_section}

{main_sections}

</div>

</body>

</html>
"""


# =========================================================
# Section
# =========================================================

def section(
    title,
    data
):

    return table_html(
        data,
        title
    )


# =========================================================
# Dashboard HTML
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def dashboard():

    main_sections = ""

    if USE_UPBIT == "Y":

        main_sections += section(
            f"🏆 UPBIT TOP{TOP_N}",
            latest_upbit_data
        )

    if USE_OKX == "Y":

        main_sections += section(
            f"🏆 OKX TOP{TOP_N}",
            latest_okx_data
        )

    near_data = []

    if USE_UPBIT == "Y":

        near_data.extend(
            latest_upbit_data
        )

    if USE_OKX == "Y":

        near_data.extend(
            latest_okx_data
        )

    # -----------------------------------------------------
    # 거래소가 여러 개일 경우 중복 표시 방지
    # 같은 코인이 있으면 ROC가 더 가까운 쪽 유지
    # -----------------------------------------------------

    near_section = (
        roc_near_zero_section(
            near_data
        )
        if near_data
        else ""
    )

    buy_section = ""

    if USE_UPBIT == "Y":

        buy_section = buy_focus_section(
            latest_upbit_data
        )

    okx_long = ""

    if USE_OKX == "Y":

        okx_long = okx_long_section(
            latest_okx_data
        )

    okx_short = ""

    if USE_OKX == "Y":

        okx_short = okx_short_section(
            latest_okx_data
        )

    return HTML_TEMPLATE.format(

        upbit_enabled=USE_UPBIT,

        okx_enabled=USE_OKX,

        upbit_time=(
            latest_upbit_update_time
        ),

        okx_time=(
            latest_okx_update_time
        ),

        usdt=(
            latest_usdt_krw
            or 0
        ),

        timeframe=format_timeframe(
            EMA_TIMEFRAME
        ),

        fast=EMA1_FAST,

        mid=EMA1_MID,

        slow=EMA1_SLOW,

        near_section=near_section,

        buy_section=buy_section,

        okx_long_section=okx_long,

        okx_short_section=okx_short,

        main_sections=main_sections
    )


# =========================================================
# Scheduler
# =========================================================

def scheduler_loop():

    while True:

        try:

            schedule.run_pending()

        except Exception as e:

            logger.exception(
                f"스케줄러 오류: {e}"
            )

        time.sleep(1)


# =========================================================
# Startup
# =========================================================

if __name__ == "__main__":

    warnings.filterwarnings(
        "ignore",
        category=FutureWarning
    )

    validate_timeframe()

    logger.info(
        "======================================"
    )

    logger.info(
        "📊 EMA1 · ROC10 전략 시작"
    )

    logger.info(
        f"EMA TIMEFRAME: "
        f"{format_timeframe(EMA_TIMEFRAME)}"
    )

    logger.info(
        f"EMA: "
        f"{EMA1_FAST}/"
        f"{EMA1_MID}/"
        f"{EMA1_SLOW}"
    )

    logger.info(
        f"ROC PERIOD: {ROC_PERIOD}"
    )

    logger.info(
        f"ROC NEAR ZERO: "
        f"{ROC_NEAR_ZERO}"
    )

    logger.info(
        f"TOP N: {TOP_N}"
    )

    logger.info(
        f"UPDATE: {UPDATE_MINUTES}분"
    )

    logger.info(
        f"UPBIT: {USE_UPBIT}"
    )

    logger.info(
        f"OKX: {USE_OKX}"
    )

    logger.info(
        "======================================"
    )

    # -----------------------------------------------------
    # 즉시 1회 실행
    # -----------------------------------------------------

    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    # -----------------------------------------------------
    # 스케줄
    # -----------------------------------------------------

    schedule.every(
        UPDATE_MINUTES
    ).minutes.do(
        update_dashboard
    )

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
