from fastapi import FastAPI
from fastapi.responses import HTMLResponse

import requests
import threading
import time
import logging
import schedule
import uvicorn
import pandas as pd
import warnings
import html

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
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s"
)

log = logging.getLogger("trading")

KST = ZoneInfo("Asia/Seoul")


# =========================================================
# 사용자 설정
# =========================================================

TOP_N = 20

SHOW_TOP_LIST = "Y"

UPDATE_MINUTES = 1

HISTORY_CHUNK = 200
MAX_HISTORY_CHUNKS = 10

USE_UPBIT = "Y"
USE_OKX = "N"

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10


# =========================================================
# SIGNAL 설정
# =========================================================

SIGNAL_TIMEFRAME = "4h"

TIMEFRAME_LABEL = {
    "4h": "4시간봉"
}


# =========================================================
# EMA 설정
# =========================================================

EMA_FAST = 20
EMA_SLOW = 60


# =========================================================
# 전역 데이터
# =========================================================

latest_upbit_data = []

latest_upbit_daily_data = []

latest_upbit_update_time = "-"

latest_upbit_daily_update_time = "-"

latest_upbit_markets = []

latest_okx_data = []

latest_okx_update_time = "-"


# =========================================================
# SIGNAL 데이터
# =========================================================

latest_signal_data = []


# =========================================================
# BTC
# =========================================================

latest_btc_okx_price = None

latest_btc_daily_periods = []

latest_btc_daily_change = None


# =========================================================
# Lock
# =========================================================

request_lock = threading.Lock()

update_lock = threading.Lock()

last_request_time = 0


# =========================================================
# 시간
# =========================================================

def kst():

    return datetime.now(KST).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


# =========================================================
# API 요청
# =========================================================

def wait_request():

    global last_request_time

    with request_lock:

        gap = (
            time.monotonic()
            - last_request_time
        )

        if gap < REQUEST_INTERVAL:

            time.sleep(
                REQUEST_INTERVAL - gap
            )

        last_request_time = (
            time.monotonic()
        )


def retry(
    func,
    *args,
    **kwargs
):

    for n in range(
        MAX_RETRIES
    ):

        try:

            wait_request()

            r = func(
                *args,
                **kwargs
            )

            if (
                not hasattr(
                    r,
                    "status_code"
                )
                or r.status_code == 200
            ):

                return r

            if r.status_code == 429:

                time.sleep(
                    min(
                        RATE_LIMIT_WAIT
                        * (n + 1),
                        60
                    )
                )

            elif r.status_code >= 500:

                time.sleep(
                    min(
                        2 * (n + 1),
                        30
                    )
                )

            else:

                return r

        except Exception as e:

            log.warning(
                "API 오류 %s/%s: %s",
                n + 1,
                MAX_RETRIES,
                e
            )

            if n < MAX_RETRIES - 1:

                time.sleep(
                    min(
                        2 * (n + 1),
                        20
                    )
                )

    return None


# =========================================================
# 업비트 마켓
# =========================================================

def get_upbit_markets():

    global latest_upbit_markets

    r = retry(
        requests.get,
        "https://api.upbit.com/v1/market/all",
        params={
            "isDetails": "false"
        },
        timeout=15
    )

    if r is None:

        return []

    try:

        markets = [
            x["market"]
            for x in r.json()
            if x.get(
                "market",
                ""
            ).startswith("KRW-")
        ]

    except Exception:

        return []

    result = []

    for i in range(
        0,
        len(markets),
        100
    ):

        rr = retry(
            requests.get,
            "https://api.upbit.com/v1/ticker",
            params={
                "markets": ",".join(
                    markets[i:i + 100]
                )
            },
            timeout=15
        )

        if rr is None:

            continue

        try:

            data = rr.json()

        except Exception:

            continue

        if not isinstance(
            data,
            list
        ):

            continue

        for x in data:

            try:

                result.append({

                    "market":
                        x["market"],

                    "current_price":
                        float(
                            x["trade_price"]
                        ),

                    "volume_24h":
                        float(
                            x[
                                "acc_trade_price_24h"
                            ]
                        )

                })

            except Exception:

                pass

    latest_upbit_markets = [
        x["market"]
        for x in result
    ]

    return result


# =========================================================
# 업비트 일봉
#
# KST 09:00 기준
# =========================================================

def get_upbit_daily_candles(
    market,
    count=200
):

    endpoint = (
        "https://api.upbit.com/v1/candles/days"
    )

    params = {

        "market":
            market,

        "count":
            min(
                count,
                200
            )

    }

    r = retry(
        requests.get,
        endpoint,
        params=params,
        timeout=15
    )

    if r is None:

        return pd.DataFrame()

    try:

        data = r.json()

    except Exception:

        return pd.DataFrame()

    if not isinstance(
        data,
        list
    ):

        return pd.DataFrame()

    rows = []

    for x in data:

        try:

            rows.append({

                "datetime":
                    datetime.strptime(
                        x[
                            "candle_date_time_kst"
                        ],
                        "%Y-%m-%dT%H:%M:%S"
                    ).replace(
                        tzinfo=KST
                    ),

                "open":
                    float(
                        x["opening_price"]
                    ),

                "high":
                    float(
                        x["high_price"]
                    ),

                "low":
                    float(
                        x["low_price"]
                    ),

                "close":
                    float(
                        x["trade_price"]
                    )

            })

        except Exception:

            pass

    if not rows:

        return pd.DataFrame()

    return (
        pd.DataFrame(rows)
        .sort_values("datetime")
        .drop_duplicates(
            "datetime"
        )
        .reset_index(
            drop=True
        )
    )


# =========================================================
# 업비트 4시간봉
# =========================================================

def get_upbit_4h_candles(
    market,
    count=200
):

    endpoint = (
        "https://api.upbit.com/v1/candles/minutes/240"
    )

    params = {

        "market":
            market,

        "count":
            min(
                count,
                200
            )

    }

    r = retry(
        requests.get,
        endpoint,
        params=params,
        timeout=15
    )

    if r is None:

        return pd.DataFrame()

    try:

        data = r.json()

    except Exception:

        return pd.DataFrame()

    if not isinstance(
        data,
        list
    ):

        return pd.DataFrame()

    rows = []

    for x in data:

        try:

            rows.append({

                "datetime":
                    datetime.strptime(
                        x[
                            "candle_date_time_kst"
                        ],
                        "%Y-%m-%dT%H:%M:%S"
                    ).replace(
                        tzinfo=KST
                    ),

                "open":
                    float(
                        x["opening_price"]
                    ),

                "high":
                    float(
                        x["high_price"]
                    ),

                "low":
                    float(
                        x["low_price"]
                    ),

                "close":
                    float(
                        x["trade_price"]
                    )

            })

        except Exception:

            pass

    if not rows:

        return pd.DataFrame()

    return (
        pd.DataFrame(rows)
        .sort_values("datetime")
        .drop_duplicates(
            "datetime"
        )
        .reset_index(
            drop=True
        )
    )


# =========================================================
# EMA 계산
# =========================================================

def calculate_ema(
    df,
    period
):

    if df is None or df.empty:

        return None

    if "close" not in df.columns:

        return None

    closes = pd.to_numeric(
        df["close"],
        errors="coerce"
    ).dropna()

    if len(closes) < period:

        return None

    ema = (
        closes
        .ewm(
            span=period,
            adjust=False
        )
        .mean()
    )

    if ema.empty:

        return None

    return float(
        ema.iloc[-1]
    )


# =========================================================
# 4시간봉 EMA 분석
#
# EMA20 < EMA60
# = 역배열
# =========================================================

def analyze_ema_4h(
    market,
    price=None
):

    df = get_upbit_4h_candles(
        market,
        200
    )

    if df.empty:

        return {

            "ema20":
                None,

            "ema60":
                None,

            "reverse":
                False,

            "alignment":
                None

        }

    df = (
        df.sort_values(
            "datetime"
        )
        .drop_duplicates(
            "datetime"
        )
        .reset_index(
            drop=True
        )
    )

    if price is not None and len(df) > 0:

        df.loc[
            df.index[-1],
            "close"
        ] = float(
            price
        )

    ema20 = calculate_ema(
        df,
        EMA_FAST
    )

    ema60 = calculate_ema(
        df,
        EMA_SLOW
    )

    if (
        ema20 is None
        or
        ema60 is None
    ):

        return {

            "ema20":
                ema20,

            "ema60":
                ema60,

            "reverse":
                False,

            "alignment":
                None

        }

    if ema20 < ema60:

        alignment = "역배열"

        reverse = True

    elif ema20 > ema60:

        alignment = "정배열"

        reverse = False

    else:

        alignment = "동일"

        reverse = False

    return {

        "ema20":
            ema20,

        "ema60":
            ema60,

        "reverse":
            reverse,

        "alignment":
            alignment

    }


# =========================================================
# 일봉 기간 생성
#
# KST 09:00 기준
# =========================================================

def build_daily_periods(
    df,
    current_price=None
):

    if df is None or df.empty:

        return []

    df = (
        df.sort_values(
            "datetime"
        )
        .drop_duplicates(
            "datetime"
        )
        .reset_index(
            drop=True
        )
    )

    now = datetime.now(KST)

    current_start = now.replace(
        hour=9,
        minute=0,
        second=0,
        microsecond=0
    )

    if now < current_start:

        current_start -= timedelta(
            days=1
        )

    periods = []

    for i, row in df.iterrows():

        dt = row["datetime"]

        start = dt

        end = (
            start
            + timedelta(days=1)
        )

        o = float(
            row["open"]
        )

        h = float(
            row["high"]
        )

        l = float(
            row["low"]
        )

        c = float(
            row["close"]
        )

        active = (
            start == current_start
        )

        if (
            active
            and current_price is not None
        ):

            c = float(
                current_price
            )

            h = max(
                h,
                c
            )

            l = min(
                l,
                c
            )

        change = (
            (c - o) / o * 100
            if o
            else None
        )

        periods.append({

            "start":
                start,

            "end":
                end,

            "active":
                active,

            "label":
                start.strftime(
                    "%m/%d 09:00"
                ),

            "open":
                o,

            "high":
                h,

            "low":
                l,

            "close":
                c,

            "change":
                change

        })

    return periods[-6:]


# =========================================================
# 상승장악형
# =========================================================

def is_bullish_engulfing(
    previous,
    current
):

    try:

        prev_open = float(
            previous["open"]
        )

        prev_close = float(
            previous["close"]
        )

        curr_open = float(
            current["open"]
        )

        curr_close = float(
            current["close"]
        )

    except Exception:

        return False

    if prev_close >= prev_open:

        return False

    if curr_close <= curr_open:

        return False

    return bool(
        curr_open <= prev_close
        and
        curr_close >= prev_open
    )


# =========================================================
# 하락장악형
# =========================================================

def is_bearish_engulfing(
    previous,
    current
):

    try:

        prev_open = float(
            previous["open"]
        )

        prev_close = float(
            previous["close"]
        )

        curr_open = float(
            current["open"]
        )

        curr_close = float(
            current["close"]
        )

    except Exception:

        return False

    if prev_close <= prev_open:

        return False

    if curr_close >= curr_open:

        return False

    return bool(
        curr_open >= prev_close
        and
        curr_close <= prev_open
    )


# =========================================================
# 양수도지
# =========================================================

def is_positive_doji(
    current
):

    try:

        o = float(
            current["open"]
        )

        h = float(
            current["high"]
        )

        l = float(
            current["low"]
        )

        c = float(
            current["close"]
        )

    except Exception:

        return False

    total_range = h - l

    if total_range <= 0:

        return False

    body = abs(
        c - o
    )

    return bool(
        body / total_range <= 0.10
        and
        c >= o
    )


# =========================================================
# 음수도지
# =========================================================

def is_negative_doji(
    current
):

    try:

        o = float(
            current["open"]
        )

        h = float(
            current["high"]
        )

        l = float(
            current["low"]
        )

        c = float(
            current["close"]
        )

    except Exception:

        return False

    total_range = h - l

    if total_range <= 0:

        return False

    body = abs(
        c - o
    )

    return bool(
        body / total_range <= 0.10
        and
        c < o
    )


# =========================================================
# 상승관통형
# =========================================================

def is_bullish_piercing(
    previous,
    current
):

    try:

        prev_open = float(
            previous["open"]
        )

        prev_close = float(
            previous["close"]
        )

        curr_open = float(
            current["open"]
        )

        curr_close = float(
            current["close"]
        )

    except Exception:

        return False

    if prev_close >= prev_open:

        return False

    if curr_close <= curr_open:

        return False

    midpoint = (
        prev_open
        + prev_close
    ) / 2

    return bool(
        curr_close > midpoint
        and
        curr_close < prev_open
    )


# =========================================================
# 하락관통형
# =========================================================

def is_bearish_piercing(
    previous,
    current
):

    try:

        prev_open = float(
            previous["open"]
        )

        prev_close = float(
            previous["close"]
        )

        curr_open = float(
            current["open"]
        )

        curr_close = float(
            current["close"]
        )

    except Exception:

        return False

    if prev_close <= prev_open:

        return False

    if curr_close >= curr_open:

        return False

    midpoint = (
        prev_open
        + prev_close
    ) / 2

    return bool(
        curr_close < midpoint
        and
        curr_close > prev_open
    )


# =========================================================
# 현재 4시간봉 패턴
# =========================================================

def get_current_pattern(
    periods
):

    if not periods:

        return None

    current = periods[-1]

    previous = (
        periods[-2]
        if len(periods) >= 2
        else None
    )

    if previous is not None:

        if is_bullish_engulfing(
            previous,
            current
        ):

            return "상승장악형"

        if is_bearish_engulfing(
            previous,
            current
        ):

            return "하락장악형"

    if is_positive_doji(
        current
    ):

        return "양수도지"

    if is_negative_doji(
        current
    ):

        return "음수도지"

    if previous is not None:

        if is_bullish_piercing(
            previous,
            current
        ):

            return "상승관통형"

        if is_bearish_piercing(
            previous,
            current
        ):

            return "하락관통형"

    return None


# =========================================================
# 최근 2·3·4번째 캔들 상승장악형
#
# periods[-1] = 현재 캔들
# periods[-2] = 최근 2번째
# periods[-3] = 최근 3번째
# periods[-4] = 최근 4번째
#
# 현재 캔들은 제외하고
# 최근 2·3·4번째 캔들에서
# 상승장악형이 하나라도 있으면 True
# =========================================================

def bullish_engulfing_234(
    periods
):

    if not periods:

        return False

    for idx in (
        -2,
        -3,
        -4
    ):

        if abs(idx) > len(periods):

            continue

        current = periods[idx]

        previous_idx = idx - 1

        if abs(previous_idx) > len(periods):

            continue

        previous = periods[
            previous_idx
        ]

        if is_bullish_engulfing(
            previous,
            current
        ):

            return True

    return False


# =========================================================
# 최근 2·3·4번째 캔들 상승장악형 위치
#
# SIGNAL 화면에서 조건 확인용
# =========================================================

def bullish_engulfing_234_label(
    periods
):

    if not periods:

        return None

    for idx, label in (
        (-2, "2번째"),
        (-3, "3번째"),
        (-4, "4번째")
    ):

        if abs(idx) > len(periods):

            continue

        current = periods[idx]

        previous_idx = idx - 1

        if abs(previous_idx) > len(periods):

            continue

        previous = periods[
            previous_idx
        ]

        if is_bullish_engulfing(
            previous,
            current
        ):

            return label

    return None


# =========================================================
# 업비트 4시간봉 기간 생성
# =========================================================

def build_upbit_4h_periods(
    df,
    current_price=None
):

    if df is None or df.empty:

        return []

    df = (
        df.sort_values(
            "datetime"
        )
        .drop_duplicates(
            "datetime"
        )
        .reset_index(
            drop=True
        )
    )

    periods = []

    for idx, row in df.iterrows():

        dt = row["datetime"]

        o = float(
            row["open"]
        )

        h = float(
            row["high"]
        )

        l = float(
            row["low"]
        )

        c = float(
            row["close"]
        )

        active = (
            idx == len(df) - 1
        )

        if (
            active
            and current_price is not None
        ):

            c = float(
                current_price
            )

            h = max(
                h,
                c
            )

            l = min(
                l,
                c
            )

        if idx < len(df) - 1:

            next_dt = df.iloc[
                idx + 1
            ]["datetime"]

        else:

            next_dt = (
                dt
                + timedelta(hours=4)
            )

        change = (
            (c - o) / o * 100
            if o
            else None
        )

        periods.append({

            "start":
                dt,

            "end":
                next_dt,

            "active":
                active,

            "label":
                dt.strftime(
                    "%m/%d %H:%M"
                ),

            "open":
                o,

            "high":
                h,

            "low":
                l,

            "close":
                c,

            "change":
                change,

            "signal":
                False,

            "signal_reason":
                None,

            "pattern":
                None

        })

    for i in range(
        1,
        len(periods)
    ):

        previous = periods[
            i - 1
        ]

        current = periods[
            i
        ]

        bullish_engulfing = (
            is_bullish_engulfing(
                previous,
                current
            )
        )

        bullish_piercing = (
            is_bullish_piercing(
                previous,
                current
            )
        )

        pattern = None

        if bullish_engulfing:

            pattern = "상승장악형"

        elif bullish_piercing:

            pattern = "상승관통형"

        if pattern is None:

            continue

        periods[i][
            "signal"
        ] = True

        periods[i][
            "pattern"
        ] = pattern

        periods[i][
            "signal_reason"
        ] = pattern

        if i + 1 < len(periods):

            periods[i + 1][
                "signal"
            ] = True

            periods[i + 1][
                "pattern"
            ] = pattern

            periods[i + 1][
                "signal_reason"
            ] = "패턴 후 다음 캔들"

    return periods[-6:]


# =========================================================
# SIGNAL 판정
# =========================================================

def signal_pass(
    periods
):

    if not periods:

        return False

    return any(
        p.get(
            "signal",
            False
        )
        for p in periods
    )


# =========================================================
# SIGNAL 세부정보
# =========================================================

def signal_details(
    periods
):

    empty = {

        "signal":
            False,

        "current_signal":
            False,

        "previous_signal":
            False,

        "signal_change":
            None,

        "signal_period":
            None,

        "signal_reason":
            None

    }

    if not periods:

        return empty

    current = periods[-1]

    current_signal = current.get(
        "signal",
        False
    )

    previous = (
        periods[-2]
        if len(periods) >= 2
        else None
    )

    previous_signal = (
        previous.get(
            "signal",
            False
        )
        if previous is not None
        else False
    )

    if current_signal:

        return {

            "signal":
                True,

            "current_signal":
                True,

            "previous_signal":
                previous_signal,

            "signal_change":
                current.get(
                    "change"
                ),

            "signal_period":
                current.get(
                    "label"
                ),

            "signal_reason":
                current.get(
                    "signal_reason"
                )

        }

    return {

        "signal":
            False,

        "current_signal":
            False,

        "previous_signal":
            previous_signal,

        "signal_change":
            None,

        "signal_period":
            None,

        "signal_reason":
            None

    }


# =========================================================
# 일봉 변동률 분석
# =========================================================

def analyze_daily_change(
    market,
    price
):

    df = get_upbit_daily_candles(
        market,
        10
    )

    if df.empty:

        return {

            "change":
                None,

            "periods":
                []

        }

    periods = build_daily_periods(
        df,
        price
    )

    if not periods:

        return {

            "change":
                None,

            "periods":
                []

        }

    return {

        "change":
            periods[-1].get(
                "change"
            ),

        "periods":
            periods

    }


# =========================================================
# 4시간봉 분석
# =========================================================

def analyze_4h(
    market,
    price
):

    df = get_upbit_4h_candles(
        market,
        200
    )

    if df.empty:

        return {

            "periods":
                [],

            "signal_pass":
                False,

            "current_signal":
                False,

            "previous_signal":
                False,

            "signal_change":
                None,

            "signal_period":
                None,

            "signal_reason":
                None,

            "current_pattern":
                None,

            "bullish_engulfing_234":
                False,

            "bullish_engulfing_234_label":
                None

        }

    periods = build_upbit_4h_periods(
        df,
        price
    )

    details = signal_details(
        periods
    )

    current_pattern = get_current_pattern(
        periods
    )

    bullish_234 = bullish_engulfing_234(
        periods
    )

    bullish_234_label = (
        bullish_engulfing_234_label(
            periods
        )
    )

    return {

        "periods":
            periods,

        "signal_pass":
            details[
                "signal"
            ],

        "current_signal":
            details[
                "current_signal"
            ],

        "previous_signal":
            details[
                "previous_signal"
            ],

        "signal_change":
            details[
                "signal_change"
            ],

        "signal_period":
            details[
                "signal_period"
            ],

        "signal_reason":
            details[
                "signal_reason"
            ],

        "current_pattern":
            current_pattern,

        "bullish_engulfing_234":
            bullish_234,

        "bullish_engulfing_234_label":
            bullish_234_label

    }


# =========================================================
# Row
# =========================================================

def make_row(
    rank,
    market,
    item,
    analysis_daily,
    analysis_4h,
    ema_analysis
):

    coin = market.replace(
        "KRW-",
        ""
    )

    return {

        "rank":
            rank,

        "name":
            coin,

        "market":
            market,

        "volume_24h":
            item[
                "volume_24h"
            ],

        "current_price":
            item[
                "current_price"
            ],

        "volume_rank":
            rank,

        "daily_change":
            analysis_daily[
                "change"
            ],

        "periods_daily":
            analysis_daily[
                "periods"
            ],

        "periods_4h":
            analysis_4h[
                "periods"
            ],

        "signal_4h":
            analysis_4h[
                "signal_pass"
            ],

        "signal_4h_current":
            analysis_4h[
                "current_signal"
            ],

        "signal_4h_previous":
            analysis_4h[
                "previous_signal"
            ],

        "signal_4h_change":
            analysis_4h[
                "signal_change"
            ],

        "signal_4h_period":
            analysis_4h[
                "signal_period"
            ],

        "signal_4h_reason":
            analysis_4h[
                "signal_reason"
            ],

        "current_pattern":
            analysis_4h[
                "current_pattern"
            ],

        "bullish_engulfing_234":
            analysis_4h[
                "bullish_engulfing_234"
            ],

        "bullish_engulfing_234_label":
            analysis_4h[
                "bullish_engulfing_234_label"
            ],

        "ema20":
            ema_analysis[
                "ema20"
            ],

        "ema60":
            ema_analysis[
                "ema60"
            ],

        "ema_alignment":
            ema_analysis[
                "alignment"
            ],

        "ema_reverse":
            ema_analysis[
                "reverse"
            ],

        "simultaneous_signal":
            analysis_4h[
                "signal_pass"
            ]

    }


# =========================================================
# 업비트 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_daily_data
    global latest_upbit_update_time
    global latest_upbit_daily_update_time
    global latest_signal_data

    all_markets = get_upbit_markets()

    candidates = []

    for item in all_markets:

        market = item[
            "market"
        ]

        price = item[
            "current_price"
        ]

        try:

            analysis_daily = analyze_daily_change(
                market,
                price
            )

            candidates.append({

                "item":
                    item,

                "analysis_daily":
                    analysis_daily

            })

        except Exception as e:

            log.warning(
                "%s 일봉 오류: %s",
                market,
                e
            )

    candidates.sort(
        key=lambda x:
            x["item"]["volume_24h"],
        reverse=True
    )

    candidates = candidates[
        :TOP_N
    ]

    rows = []

    for rank, candidate in enumerate(
        candidates,
        1
    ):

        item = candidate[
            "item"
        ]

        analysis_daily = candidate[
            "analysis_daily"
        ]

        market = item[
            "market"
        ]

        price = item[
            "current_price"
        ]

        try:

            analysis_4h = analyze_4h(
                market,
                price
            )

        except Exception as e:

            log.warning(
                "%s 4시간봉 오류: %s",
                market,
                e
            )

            analysis_4h = {

                "periods":
                    [],

                "signal_pass":
                    False,

                "current_signal":
                    False,

                "previous_signal":
                    False,

                "signal_change":
                    None,

                "signal_period":
                    None,

                "signal_reason":
                    None,

                "current_pattern":
                    None,

                "bullish_engulfing_234":
                    False,

                "bullish_engulfing_234_label":
                    None

            }

        try:

            ema_analysis = analyze_ema_4h(
                market,
                price
            )

        except Exception as e:

            log.warning(
                "%s EMA 오류: %s",
                market,
                e
            )

            ema_analysis = {

                "ema20":
                    None,

                "ema60":
                    None,

                "reverse":
                    False,

                "alignment":
                    None

            }

        rows.append(
            make_row(
                rank,
                market,
                item,
                analysis_daily,
                analysis_4h,
                ema_analysis
            )
        )

    latest_upbit_data = rows

    latest_upbit_daily_data = rows

    latest_upbit_daily_update_time = kst()

    latest_upbit_update_time = (
        latest_upbit_daily_update_time
    )

    # =====================================================
    # SIGNAL 조건
    #
    # 1. EMA20 < EMA60 역배열
    # OR
    # 2. 최근 2번째 4시간봉 상승장악형
    # OR
    # 3. 최근 3번째 4시간봉 상승장악형
    # OR
    # 4. 최근 4번째 4시간봉 상승장악형
    # =====================================================

    latest_signal_data = [
        row
        for row in rows
        if (
            row.get(
                "ema_reverse",
                False
            )
            or
            row.get(
                "bullish_engulfing_234",
                False
            )
        )
    ]

    # SIGNAL도 거래대금순으로 정렬

    latest_signal_data.sort(
        key=lambda x:
            x.get(
                "volume_24h",
                0
            ),
        reverse=True
    )

    # SIGNAL 전용 번호

    for signal_rank, row in enumerate(
        latest_signal_data,
        1
    ):

        row["signal_rank"] = signal_rank

    log.info(
        "UPBIT | 거래대금 TOP%s | SIGNAL=%s",
        TOP_N,
        len(
            latest_signal_data
        )
    )


# =========================================================
# OKX 캔들
# =========================================================

def okx_candles(
    bar,
    limit=200
):

    r = retry(
        requests.get,
        "https://www.okx.com/api/v5/market/candles",
        params={

            "instId":
                "BTC-USDT-SWAP",

            "bar":
                bar,

            "limit":
                str(
                    min(
                        limit,
                        300
                    )
                )

        },
        timeout=15
    )

    if r is None:

        return pd.DataFrame()

    try:

        data = r.json().get(
            "data",
            []
        )

    except Exception:

        return pd.DataFrame()

    rows = []

    for x in data:

        try:

            rows.append({

                "datetime":
                    pd.to_datetime(
                        int(x[0]),
                        unit="ms",
                        utc=True
                    ).tz_convert(KST),

                "open":
                    float(x[1]),

                "high":
                    float(x[2]),

                "low":
                    float(x[3]),

                "close":
                    float(x[4])

            })

        except Exception:

            pass

    if not rows:

        return pd.DataFrame()

    return (
        pd.DataFrame(rows)
        .sort_values("datetime")
        .drop_duplicates(
            "datetime"
        )
        .reset_index(
            drop=True
        )
    )


# =========================================================
# OKX 가격
# =========================================================

def okx_price():

    r = retry(
        requests.get,
        "https://www.okx.com/api/v5/market/ticker",
        params={
            "instId":
                "BTC-USDT-SWAP"
        },
        timeout=15
    )

    if r is None:

        return None

    try:

        return float(
            r.json()[
                "data"
            ][0]["last"]
        )

    except Exception:

        return None


# =========================================================
# BTC 업데이트
# =========================================================

def update_okx_btc():

    global latest_btc_okx_price
    global latest_btc_daily_periods
    global latest_btc_daily_change

    price = okx_price()

    latest_btc_okx_price = price

    if price is None:

        return

    d1d = okx_candles(
        "1D",
        100
    )

    if d1d.empty:

        latest_btc_daily_periods = []

        latest_btc_daily_change = None

        return

    d1d = (
        d1d
        .sort_values(
            "datetime"
        )
        .drop_duplicates(
            "datetime"
        )
        .reset_index(
            drop=True
        )
    )

    now = datetime.now(KST)

    current_day_start = now.replace(
        hour=9,
        minute=0,
        second=0,
        microsecond=0
    )

    if now < current_day_start:

        current_day_start -= timedelta(
            days=1
        )

    active_rows = (
        d1d["datetime"]
        == current_day_start
    )

    if active_rows.any():

        last_idx = d1d.index[
            active_rows
        ][-1]

        d1d.loc[
            last_idx,
            "close"
        ] = float(
            price
        )

        d1d.loc[
            last_idx,
            "high"
        ] = max(
            float(
                d1d.loc[
                    last_idx,
                    "high"
                ]
            ),
            float(price)
        )

        d1d.loc[
            last_idx,
            "low"
        ] = min(
            float(
                d1d.loc[
                    last_idx,
                    "low"
                ]
            ),
            float(price)
        )

    latest_btc_daily_periods = (
        build_daily_periods(
            d1d,
            price
        )
    )

    if latest_btc_daily_periods:

        latest_btc_daily_change = (
            latest_btc_daily_periods[-1]
            .get(
                "change"
            )
        )

    else:

        latest_btc_daily_change = None


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    if not update_lock.acquire(
        False
    ):

        return

    try:

        update_okx_btc()

        if USE_UPBIT == "Y":

            update_upbit()

    except Exception as e:

        log.exception(
            "업데이트 오류: %s",
            e
        )

    finally:

        update_lock.release()


# =========================================================
# 표시
# =========================================================

def fmt_price(v):

    if v is None:

        return "-"

    try:

        v = float(v)

    except Exception:

        return "-"

    if v >= 100000000:

        return f"{v / 100000000:.2f}억"

    if v >= 10000:

        return f"{v:,.0f}"

    if v >= 1:

        return f"{v:,.2f}"

    return f"{v:.6f}"


def fmt_vol(v):

    try:

        v = float(v)

    except Exception:

        return "-"

    if v >= 1e12:

        return f"{v / 1e12:.1f}조"

    if v >= 1e8:

        return f"{v / 1e8:.0f}억"

    if v >= 1e4:

        return f"{v / 1e4:.0f}만"

    return f"{v:,.0f}"


def fmt_change(v):

    if v is None:

        return (
            '<span class="zero">-</span>'
        )

    try:

        v = float(v)

    except Exception:

        return (
            '<span class="zero">-</span>'
        )

    if v > 0:

        return (
            '<span class="up">'
            f'▲ +{v:.2f}%'
            '</span>'
        )

    if v < 0:

        return (
            '<span class="down">'
            f'▼ {v:.2f}%'
            '</span>'
        )

    return (
        '<span class="zero">'
        '0.00%'
        '</span>'
    )


# =========================================================
# EMA 표시
# =========================================================

def fmt_ema(v):

    if v is None:

        return "-"

    try:

        v = float(v)

    except Exception:

        return "-"

    return fmt_price(v)


def ema_alignment_html(
    alignment
):

    if alignment == "역배열":

        return (
            '<span class="ema-reverse">'
            '▼ 역배열'
            '</span>'
        )

    if alignment == "정배열":

        return (
            '<span class="ema-normal">'
            '▲ 정배열'
            '</span>'
        )

    return (
        '<span class="zero">'
        '-'
        '</span>'
    )


# =========================================================
# 현재 패턴 HTML
# =========================================================

def current_pattern_html(
    pattern
):

    if pattern == "상승장악형":

        return (
            '<span class="current-pattern bullish">'
            '▲ 상승장악형'
            '</span>'
        )

    if pattern == "하락장악형":

        return (
            '<span class="current-pattern bearish">'
            '▼ 하락장악형'
            '</span>'
        )

    if pattern == "양수도지":

        return (
            '<span class="current-pattern bullish">'
            '● 양수도지'
            '</span>'
        )

    if pattern == "음수도지":

        return (
            '<span class="current-pattern bearish">'
            '● 음수도지'
            '</span>'
        )

    if pattern == "상승관통형":

        return (
            '<span class="current-pattern bullish">'
            '▲ 상승관통형'
            '</span>'
        )

    if pattern == "하락관통형":

        return (
            '<span class="current-pattern bearish">'
            '▼ 하락관통형'
            '</span>'
        )

    return (
        '<span class="current-pattern none">'
        '-'
        '</span>'
    )


# =========================================================
# SIGNAL 이유
# =========================================================

def signal_reason_html(
    reason
):

    if reason == "상승장악형":

        return (
            '<span class="pattern-cross">'
            '▲ 상승장악형'
            '</span>'
        )

    if reason == "상승관통형":

        return (
            '<span class="pattern-cross">'
            '▲ 상승관통형'
            '</span>'
        )

    if reason == "패턴 후 다음 캔들":

        return (
            '<span class="pattern-next">'
            '→ 패턴 후 다음봉'
            '</span>'
        )

    return ""


# =========================================================
# 캔들 표시
# =========================================================

def cells(periods):

    result = []

    for p in periods:

        active_class = (
            "current"
            if p.get("active")
            else ""
        )

        reason_badge = signal_reason_html(
            p.get(
                "signal_reason"
            )
        )

        result.append(
            f"""
            <div class="tf-cell {active_class}">

                <div class="tf-date">
                    {html.escape(
                        p.get(
                            "label",
                            "-"
                        )
                    )}
                </div>

                <strong>
                    {fmt_change(
                        p.get(
                            "change"
                        )
                    )}
                </strong>

                {reason_badge}

            </div>
            """
        )

    return "".join(result)


# =========================================================
# BTC HTML
#
# BTC 현재가 + 당일 변동률만 표시
# BTC 일봉 영역 삭제
# =========================================================

def btc_html():

    return f"""
    <section class="btc-panel">

        <div class="section-head">

            <div>

                <span class="section-kicker">
                    MARKET
                </span>

                <b>
                    ₿ BTC 시황
                </b>

            </div>

            <span class="update-time">
                OKX · {kst()}
            </span>

        </div>


        <div class="btc-main">

            <div class="btc-name">
                BTC
            </div>

            <div class="btc-price">
                {fmt_price(
                    latest_btc_okx_price
                )}
            </div>

            <div class="btc-change">
                {fmt_change(
                    latest_btc_daily_change
                )}
            </div>

        </div>

    </section>
    """


# =========================================================
# SIGNAL 카드
#
# 표시:
# 종목 / 거래대금 / 변동률 / 캔들패턴
#
# 현재가 / EMA20 / EMA60 / 역배열 표시 삭제
# =========================================================

def signal_card(
    row
):

    return f"""

    <div class="signal-card">

        <div class="signal-coin">

            <span class="signal-rank">
                #{row.get("signal_rank", "-")}
            </span>

            <b>
                {html.escape(
                    row["name"]
                )}
            </b>

        </div>


        <div class="signal-volume">

            <span>
                거래대금
            </span>

            <strong>
                {fmt_vol(
                    row["volume_24h"]
                )}
            </strong>

        </div>


        <div class="signal-change">

            <span>
                변동률
            </span>

            <strong>
                {fmt_change(
                    row.get(
                        "daily_change"
                    )
                )}
            </strong>

        </div>


        <div class="signal-pattern">

            <span>
                캔들패턴
            </span>

            <strong>
                {current_pattern_html(
                    row.get(
                        "current_pattern"
                    )
                )}
            </strong>

        </div>

    </div>

    """


# =========================================================
# SIGNAL 영역
# =========================================================

def signal_section():

    if not latest_signal_data:

        body = """

        <div class="signal-empty">

            현재 SIGNAL 조건에
            해당하는 종목 없음

        </div>

        """

    else:

        body = "".join(
            signal_card(row)
            for row in latest_signal_data
        )

    return f"""

    <section class="signal-panel">

        <div class="section-head signal-panel-head">

            <div>

                <span class="section-kicker">
                    SIGNAL
                </span>

                <b>
                    🔴 EMA 역배열 / 상승장악 SIGNAL
                </b>

            </div>

            <span class="update-time">
                4시간봉 · {kst()}
            </span>

        </div>


        <div class="signal-condition">

            <span>
                SIGNAL 조건
            </span>

            <b>
                EMA20 &lt; EMA60
                OR
                최근 2·3·4번째 상승장악
            </b>

            <small>
                4시간봉 기준
            </small>

        </div>


        <div class="signal-header">

            <div>
                종목
            </div>

            <div>
                거래대금
            </div>

            <div>
                변동률
            </div>

            <div>
                캔들패턴
            </div>

        </div>


        <div class="signal-list">

            {body}

        </div>

    </section>

    """


# =========================================================
# TOP 카드
#
# 표시:
# 종목 / 현재가 / 거래대금 / 변동률 / 현재 캔들패턴
#
# 4시간봉 패턴 이력 표시 삭제
# =========================================================

def card(
    row,
    kind
):

    signal = (
        row.get(
            "signal_4h",
            False
        )
        or
        row.get(
            "bullish_engulfing_234",
            False
        )
        or
        row.get(
            "ema_reverse",
            False
        )
    )

    current_pattern = row.get(
        "current_pattern"
    )

    if kind == "top":

        status = ""

        if signal:

            status = (
                '<span class="signal-badge">'
                '⭐ SIGNAL'
                '</span>'
            )

        return f"""

        <article class="coin-card">

            <div class="coin-head">

                <div class="coin-title">

                    <span class="rank">
                        #{row["rank"]}
                    </span>

                    <b>
                        {html.escape(
                            row["name"]
                        )}
                    </b>

                </div>

                {status}

            </div>


            <div class="market-summary">

                <div>

                    <span>
                        현재가
                    </span>

                    <strong>
                        {fmt_price(
                            row["current_price"]
                        )}
                    </strong>

                </div>


                <div>

                    <span>
                        24H 거래대금
                    </span>

                    <strong>
                        {fmt_vol(
                            row["volume_24h"]
                        )}
                    </strong>

                </div>


                <div>

                    <span>
                        당일 변동률
                    </span>

                    <strong class="daily-change-value">

                        {fmt_change(
                            row.get(
                                "daily_change"
                            )
                        )}

                    </strong>

                </div>


                <div>

                    <span>
                        현재 캔들패턴
                    </span>

                    <strong>

                        {current_pattern_html(
                            current_pattern
                        )}

                    </strong>

                </div>

            </div>

        </article>

        """

    return ""


# =========================================================
# CSS
# =========================================================

CSS = """

* {
    box-sizing: border-box;
}


html {
    background: #080b0f;
}


body {

    margin: 0;

    padding: 8px;

    background: #080b0f;

    color: #e8edf2;

    font-family:
        Arial,
        "Noto Sans KR",
        sans-serif;

    font-size: 11px;
}


h1 {

    margin: 4px 2px 12px;

    font-size: 14px;

    font-weight: 800;

    letter-spacing: -0.4px;

    color: #f1f4f7;
}


section {

    margin-bottom: 12px;
}


.up {

    color: #38d878 !important;

    font-weight: 900;
}


.down {

    color: #ff5966 !important;

    font-weight: 900;
}


.zero {

    color: #68737e;

    font-weight: 800;
}


.daily-change-value .up {

    color: #38d878 !important;

    font-weight: 900;

    font-size: 9px;
}


.daily-change-value .down {

    color: #ff5966 !important;

    font-weight: 900;

    font-size: 9px;
}


.current-pattern {

    display: inline-block;

    font-size: 7px;

    font-weight: 900;

    white-space: nowrap;
}


.current-pattern.bullish {

    color: #38d878;

}


.current-pattern.bearish {

    color: #ff5966;

}


.current-pattern.none {

    color: #68737e;

}


/* =======================================================
   SIGNAL
   ======================================================= */

.signal-panel {

    background: #0c1116;

    border:
        1px solid #48272c;

    margin-bottom: 13px;
}


.signal-panel-head {

    border-bottom:
        1px solid #392126;
}


.signal-condition {

    min-height: 32px;

    padding: 0 9px;

    display: flex;

    align-items: center;

    gap: 8px;

    background: #120e10;

    border-bottom:
        1px solid #302024;
}


.signal-condition span {

    color: #7d6b70;

    font-size: 6px;
}


.signal-condition b {

    color: #ff5966;

    font-size: 8px;

    font-weight: 900;
}


.signal-condition small {

    color: #626e78;

    font-size: 6px;

    margin-left: auto;
}


/* SIGNAL 헤더 */

.signal-header,
.signal-card {

    display: grid;

    grid-template-columns:
        1.2fr
        1fr
        0.9fr
        1.5fr;

    align-items: center;
}


.signal-header {

    min-height: 27px;

    padding: 0 9px;

    color: #69747e;

    font-size: 6px;

    font-weight: 800;

    background: #0b1015;

    border-bottom:
        1px solid #20282f;
}


.signal-header > div {

    text-align: center;
}


.signal-card {

    min-height: 48px;

    background: #0d1217;

    border-bottom:
        1px solid #20282f;
}


.signal-card:last-child {

    border-bottom: 0;
}


.signal-card > div {

    min-height: 48px;

    padding: 5px 3px;

    display: flex;

    flex-direction: column;

    align-items: center;

    justify-content: center;

    text-align: center;
}


.signal-card > div + div {

    border-left:
        1px solid #1c252c;
}


.signal-coin {

    flex-direction: row !important;

    gap: 6px;

    justify-content: flex-start !important;

    padding-left: 9px !important;
}


.signal-rank {

    color: #b84d58;

    font-size: 7px;

    font-weight: 900;
}


.signal-coin b {

    color: #e5e9ed;

    font-size: 9px;

    font-weight: 900;
}


.signal-card span {

    color: #69747e;

    font-size: 6px;
}


.signal-card strong {

    margin-top: 3px;

    color: #dce2e7;

    font-size: 8px;
}


.signal-pattern .current-pattern {

    font-size: 7px;

}


.signal-empty {

    min-height: 46px;

    display: flex;

    align-items: center;

    justify-content: center;

    color: #626e78;

    font-size: 7px;
}


/* =======================================================
   BTC
   ======================================================= */

.btc-panel {

    background: #0c1116;

    border:
        1px solid #202a33;

    margin-bottom: 13px;
}


.btc-panel .section-head {

    height: 38px;

    padding: 0 10px;

    display: flex;

    align-items: center;

    justify-content: space-between;

    border-bottom:
        1px solid #202a33;
}


.section-head > div {

    display: flex;

    align-items: center;

    gap: 7px;
}


.section-head b {

    font-size: 10px;

    font-weight: 800;
}


.section-kicker {

    color: #7d8994;

    font-size: 6px;

    letter-spacing: 1px;

    font-weight: 700;
}


.update-time {

    color: #65717c;

    font-size: 6px;
}


.btc-main {

    min-height: 54px;

    display: grid;

    grid-template-columns:
        0.7fr 1.5fr 1fr;

    align-items: center;
}


.btc-main > div {

    text-align: center;

    padding: 5px;
}


.btc-name {

    color: #c4ccd3;

    font-size: 10px;

    font-weight: 800;
}


.btc-price {

    color: #f4f6f8;

    font-size: 16px;

    font-weight: 900;

    letter-spacing: -0.5px;
}


.btc-change {

    font-size: 11px;

    font-weight: 900;
}


/* =======================================================
   TOP10
   ======================================================= */

.coin-card {

    background: #0c1116;

    border:
        1px solid #202a33;

    margin-bottom: 7px;
}


.coin-head {

    min-height: 38px;

    padding: 0 9px;

    display: flex;

    align-items: center;

    justify-content: space-between;

    border-bottom:
        1px solid #202a33;
}


.coin-title {

    display: flex;

    align-items: center;

    gap: 8px;
}


.coin-title b {

    font-size: 10px;

    font-weight: 900;
}


.rank {

    color: #c9a83d;

    font-size: 8px;

    font-weight: 800;
}


.signal-badge {

    color: #e4c45e;

    font-size: 7px;

    font-weight: 900;
}


.market-summary {

    display: grid;

    grid-template-columns:
        repeat(4, 1fr);

    border-bottom:
        1px solid #202a33;
}


.market-summary > div {

    min-height: 48px;

    padding: 6px 3px;

    text-align: center;

    background: #0d1318;
}


.market-summary > div + div {

    border-left:
        1px solid #1d262e;
}


.market-summary span {

    display: block;

    color: #65717b;

    font-size: 6px;

    margin-bottom: 5px;
}


.market-summary strong {

    display: block;

    color: #dce2e7;

    font-size: 8px;
}


.market-summary .daily-change-value .up {

    color: #38d878 !important;
}


.market-summary .daily-change-value .down {

    color: #ff5966 !important;
}


.market-summary .daily-change-value .zero {

    color: #68737e !important;
}


@media (max-width: 600px) {

    body {

        padding: 5px;
    }


    h1 {

        margin:
            3px 2px 9px;

        font-size: 12px;
    }


    .section-head {

        padding: 0 8px;
    }


    .section-head b {

        font-size: 9px;
    }


    .update-time {

        font-size: 5px;
    }


    .btc-main {

        min-height: 48px;
    }


    .btc-price {

        font-size: 13px;
    }


    .btc-change {

        font-size: 9px;
    }


    .current-pattern {

        font-size: 6px;
    }


    .daily-change-value .up,
    .daily-change-value .down {

        font-size: 8px;
    }


    .coin-head {

        min-height: 35px;

        padding: 0 7px;
    }


    .coin-title b {

        font-size: 9px;
    }


    .market-summary > div {

        min-height: 44px;

        padding:
            5px 2px;
    }


    .market-summary span {

        font-size: 5px;

        margin-bottom: 4px;
    }


    .market-summary strong {

        font-size: 7px;
    }


    .signal-condition {

        min-height: 29px;

        padding: 0 7px;
    }


    .signal-condition b {

        font-size: 7px;
    }


    .signal-condition small {

        font-size: 5px;
    }


    .signal-header,
    .signal-card {

        grid-template-columns:
            1.2fr
            1fr
            0.9fr
            1.5fr;
    }


    .signal-card > div {

        min-height: 44px;

        padding: 4px 2px;
    }


    .signal-coin {

        gap: 4px;

        padding-left: 6px !important;
    }


    .signal-coin b {

        font-size: 7px;
    }


    .signal-rank {

        font-size: 5px;
    }


    .signal-card span {

        font-size: 5px;
    }


    .signal-card strong {

        font-size: 6px;
    }


    .signal-pattern .current-pattern {

        font-size: 6px;
    }

}
"""


# =========================================================
# DASHBOARD
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def dashboard():

    s = btc_html()

    # =====================================================
    # BTC 시황
    # ↓
    # SIGNAL
    # ↓
    # TOP10
    # =====================================================

    if USE_UPBIT == "Y":

        # -------------------------------------------------
        # SIGNAL
        # -------------------------------------------------

        s += signal_section()

        # -------------------------------------------------
        # TOP10
        # -------------------------------------------------

        if SHOW_TOP_LIST == "Y":

            if latest_upbit_data:

                top_cards = "".join(
                    card(
                        r,
                        "top"
                    )
                    for r in latest_upbit_data
                )

            else:

                top_cards = (
                    '<div class="signal-empty">'
                    '현재 거래대금 TOP10 데이터 없음'
                    '</div>'
                )

            s += f"""

            <section>

                <div class="section-head">

                    <div>

                        <span class="section-kicker">
                            RANKING
                        </span>

                        <b>
                            TOP10 · 거래대금 순
                        </b>

                    </div>

                    <span class="update-time">
                        거래대금 기준
                    </span>

                </div>

                {top_cards}

            </section>

            """

    return f"""

    <!doctype html>

    <html lang="ko">

    <head>

        <meta charset="utf-8">

        <meta
            name="viewport"
            content="width=device-width,
            initial-scale=1"
        >

        <meta
            http-equiv="refresh"
            content="60"
        >

        <title>
            TRADING SIGNAL CENTER
        </title>

        <style>
            {CSS}
        </style>

    </head>


    <body>

        <h1>
            📊 TRADING SIGNAL CENTER
        </h1>

        {s}

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

            log.exception(
                "scheduler: %s",
                e
            )

        time.sleep(1)


# =========================================================
# STARTUP
# =========================================================

@app.on_event("startup")
def startup():

    log.info(
        "START | BTC 시황 + EMA20/60 역배열 + 2·3·4번째 상승장악 SIGNAL + TOP10"
    )

    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

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
