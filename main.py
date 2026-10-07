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

TOP_N = 10

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

EMA_FAST = 5
EMA_SLOW = 15


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
# 업비트 1시간봉
#
# 4시간봉 구성용 원본 데이터
# =========================================================

def get_upbit_4h_candles(
    market,
    count=200
):

    endpoint = (
        "https://api.upbit.com/v1/candles/minutes/60"
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
# 업비트 09:00 기준 4시간봉 생성
#
# 기준:
# 09:00 ~ 13:00
# 13:00 ~ 17:00
# 17:00 ~ 21:00
# 21:00 ~ 01:00
# 01:00 ~ 05:00
# 05:00 ~ 09:00
#
# 즉 KST 09:00 기준으로 4시간씩 진행
# =========================================================

def build_upbit_4h_candles(
    df,
    current_price=None
):

    if df is None or df.empty:

        return pd.DataFrame()

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

    # -----------------------------------------------------
    # 현재 4시간봉 시작 시간 계산
    #
    # 09시를 기준으로 4시간 단위
    # -----------------------------------------------------

    day_start = now.replace(
        hour=9,
        minute=0,
        second=0,
        microsecond=0
    )

    if now < day_start:

        day_start -= timedelta(
            days=1
        )

    elapsed_hours = int(
        (
            now - day_start
        ).total_seconds()
        // 3600
    )

    current_start = (
        day_start
        + timedelta(
            hours=(
                elapsed_hours // 4
            ) * 4
        )
    )

    rows = []

    # -----------------------------------------------------
    # 각 1시간봉을 4시간봉 그룹으로 분류
    # -----------------------------------------------------

    for _, row in df.iterrows():

        dt = row["datetime"]

        # 09시 기준 상대 시간
        relative_seconds = (
            dt - day_start
        ).total_seconds()

        group_index = int(
            relative_seconds
            // (4 * 3600)
        )

        period_start = (
            day_start
            + timedelta(
                hours=group_index * 4
            )
        )

        period_end = (
            period_start
            + timedelta(
                hours=4
            )
        )

        rows.append({

            "period_start":
                period_start,

            "period_end":
                period_end,

            "open":
                float(
                    row["open"]
                ),

            "high":
                float(
                    row["high"]
                ),

            "low":
                float(
                    row["low"]
                ),

            "close":
                float(
                    row["close"]
                )

        })

    if not rows:

        return pd.DataFrame()

    raw = pd.DataFrame(rows)

    grouped = []

    for period_start, group in raw.groupby(
        "period_start",
        sort=True
    ):

        group = group.sort_index()

        o = float(
            group.iloc[0]["open"]
        )

        h = float(
            group["high"].max()
        )

        l = float(
            group["low"].min()
        )

        c = float(
            group.iloc[-1]["close"]
        )

        active = (
            period_start
            == current_start
        )

        # -------------------------------------------------
        # 현재 진행 중인 4시간봉은 현재가 반영
        # -------------------------------------------------

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

        grouped.append({

            "datetime":
                period_start,

            "start":
                period_start,

            "end":
                period_start
                + timedelta(
                    hours=4
                ),

            "active":
                active,

            "label":
                period_start.strftime(
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
                change

        })

    result = (
        pd.DataFrame(grouped)
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

    return result


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
# 업비트 KST 09:00 기준 4시간봉
#
# EMA5 < EMA15
# = 역배열
# =========================================================

def analyze_ema_4h(
    market,
    price=None
):

    df_1h = get_upbit_4h_candles(
        market,
        200
    )

    if df_1h.empty:

        return {

            "ema5":
                None,

            "ema15":
                None,

            "reverse":
                False,

            "alignment":
                None

        }

    df = build_upbit_4h_candles(
        df_1h,
        price
    )

    if df.empty:

        return {

            "ema5":
                None,

            "ema15":
                None,

            "reverse":
                False,

            "alignment":
                None

        }

    ema5 = calculate_ema(
        df,
        EMA_FAST
    )

    ema15 = calculate_ema(
        df,
        EMA_SLOW
    )

    if (
        ema5 is None
        or
        ema15 is None
    ):

        return {

            "ema5":
                ema5,

            "ema15":
                ema15,

            "reverse":
                False,

            "alignment":
                None

        }

    if ema5 < ema15:

        alignment = "역배열"

        reverse = True

    elif ema5 > ema15:

        alignment = "정배열"

        reverse = False

    else:

        alignment = "동일"

        reverse = False

    return {

        "ema5":
            ema5,

        "ema15":
            ema15,

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
#
# SIGNAL / TOP 표시에서는 사용하지 않음
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
#
# 표시에서는 사용하지 않음
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
# 패턴 이름
#
# 상승 패턴만 표시
# =========================================================

def get_candle_pattern(
    previous,
    current
):

    if (
        previous is None
        or current is None
    ):

        return None

    if is_bullish_engulfing(
        previous,
        current
    ):

        return "상승장악형"

    if is_positive_doji(
        current
    ):

        return "양수도지"

    if is_bullish_piercing(
        previous,
        current
    ):

        return "상승관통형"

    return None


# =========================================================
# 이전 / 현재 4시간봉 패턴 분석
#
# periods[-2] = 이전 완료 4시간봉
# periods[-1] = 현재 진행 4시간봉
#
# SIGNAL:
# 이전 캔들 또는 현재 캔들 중
# 하나라도 상승장악형이면 SIGNAL
# =========================================================

def analyze_previous_current_pattern(
    periods
):

    result = {

        "previous_pattern":
            None,

        "current_pattern":
            None,

        "previous_bullish_engulfing":
            False,

        "current_bullish_engulfing":
            False,

        "bullish_engulfing_signal":
            False

    }

    if not periods:

        return result

    current = periods[-1]

    previous = (
        periods[-2]
        if len(periods) >= 2
        else None
    )

    # -----------------------------------------------------
    # 현재 4시간봉 패턴
    # -----------------------------------------------------

    result[
        "current_pattern"
    ] = get_candle_pattern(
        previous,
        current
    )

    # -----------------------------------------------------
    # 현재 4시간봉 상승장악
    # -----------------------------------------------------

    if previous is not None:

        result[
            "current_bullish_engulfing"
        ] = is_bullish_engulfing(
            previous,
            current
        )

    # -----------------------------------------------------
    # 이전 4시간봉 패턴
    # -----------------------------------------------------

    if len(periods) >= 3:

        previous_previous = periods[-3]

        result[
            "previous_pattern"
        ] = get_candle_pattern(
            previous_previous,
            previous
        )

        result[
            "previous_bullish_engulfing"
        ] = is_bullish_engulfing(
            previous_previous,
            previous
        )

    # -----------------------------------------------------
    # 이전 또는 현재 상승장악
    # -----------------------------------------------------

    result[
        "bullish_engulfing_signal"
    ] = bool(
        result[
            "previous_bullish_engulfing"
        ]
        or
        result[
            "current_bullish_engulfing"
        ]
    )

    return result


# =========================================================
# 업비트 4시간봉 기간 생성
#
# KST 09:00 기준
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

    periods_df = build_upbit_4h_candles(
        df,
        current_price
    )

    if periods_df.empty:

        return []

    periods = []

    for _, row in periods_df.iterrows():

        periods.append({

            "start":
                row["start"],

            "end":
                row["end"],

            "active":
                bool(
                    row["active"]
                ),

            "label":
                row["label"],

            "open":
                float(
                    row["open"]
                ),

            "high":
                float(
                    row["high"]
                ),

            "low":
                float(
                    row["low"]
                ),

            "close":
                float(
                    row["close"]
                ),

            "change":
                row["change"],

            "signal":
                False,

            "signal_reason":
                None,

            "pattern":
                None

        })

    return periods[-6:]


# =========================================================
# SIGNAL 판정
#
# 4시간봉 EMA5 < EMA15
# OR
# 이전 4시간봉 상승장악
# OR
# 현재 4시간봉 상승장악
#
# 하락장악 / 하락관통 제외
# =========================================================

def signal_pass(
    periods,
    ema_reverse=False
):

    if ema_reverse:

        return True

    pattern_info = (
        analyze_previous_current_pattern(
            periods
        )
    )

    return bool(
        pattern_info[
            "bullish_engulfing_signal"
        ]
    )


# =========================================================
# SIGNAL 세부정보
# =========================================================

def signal_details(
    periods,
    ema_reverse=False
):

    pattern_info = (
        analyze_previous_current_pattern(
            periods
        )
    )

    current_signal = bool(
        pattern_info[
            "current_bullish_engulfing"
        ]
    )

    previous_signal = bool(
        pattern_info[
            "previous_bullish_engulfing"
        ]
    )

    if ema_reverse:

        reason = "EMA5 < EMA15"

    elif current_signal:

        reason = "현재 상승장악형"

    elif previous_signal:

        reason = "이전 상승장악형"

    else:

        reason = None

    signal = bool(
        ema_reverse
        or
        current_signal
        or
        previous_signal
    )

    current_change = None

    current_period = None

    if periods:

        current_period = periods[-1]

        current_change = current_period.get(
            "change"
        )

    return {

        "signal":
            signal,

        "current_signal":
            current_signal,

        "previous_signal":
            previous_signal,

        "signal_change":
            current_change
            if signal
            else None,

        "signal_period":
            current_period.get(
                "label"
            )
            if (
                signal
                and
                current_period is not None
            )
            else None,

        "signal_reason":
            reason

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
    price,
    ema_reverse=False
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
                bool(
                    ema_reverse
                ),

            "current_signal":
                False,

            "previous_signal":
                False,

            "signal_change":
                None,

            "signal_period":
                None,

            "signal_reason":
                (
                    "EMA5 < EMA15"
                    if ema_reverse
                    else None
                ),

            "previous_pattern":
                None,

            "current_pattern":
                None,

            "previous_bullish_engulfing":
                False,

            "current_bullish_engulfing":
                False

        }

    periods = build_upbit_4h_periods(
        df,
        price
    )

    pattern_info = (
        analyze_previous_current_pattern(
            periods
        )
    )

    details = signal_details(
        periods,
        ema_reverse
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

        "previous_pattern":
            pattern_info[
                "previous_pattern"
            ],

        "current_pattern":
            pattern_info[
                "current_pattern"
            ],

        "previous_bullish_engulfing":
            pattern_info[
                "previous_bullish_engulfing"
            ],

        "current_bullish_engulfing":
            pattern_info[
                "current_bullish_engulfing"
            ]

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

        "previous_pattern":
            analysis_4h[
                "previous_pattern"
            ],

        "current_pattern":
            analysis_4h[
                "current_pattern"
            ],

        "previous_bullish_engulfing":
            analysis_4h[
                "previous_bullish_engulfing"
            ],

        "current_bullish_engulfing":
            analysis_4h[
                "current_bullish_engulfing"
            ],

        "ema5":
            ema_analysis[
                "ema5"
            ],

        "ema15":
            ema_analysis[
                "ema15"
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

            # -------------------------------------------------
            # 업비트 4시간봉 EMA5 / EMA15
            # -------------------------------------------------

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

                "ema5":
                    None,

                "ema15":
                    None,

                "reverse":
                    False,

                "alignment":
                    None

            }

        try:

            # -------------------------------------------------
            # 업비트 4시간봉 패턴 분석
            # -------------------------------------------------

            analysis_4h = analyze_4h(
                market,
                price,
                ema_analysis.get(
                    "reverse",
                    False
                )
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
                    ema_analysis.get(
                        "reverse",
                        False
                    ),

                "current_signal":
                    False,

                "previous_signal":
                    False,

                "signal_change":
                    None,

                "signal_period":
                    None,

                "signal_reason":
                    (
                        "EMA5 < EMA15"
                        if ema_analysis.get(
                            "reverse",
                            False
                        )
                        else None
                    ),

                "previous_pattern":
                    None,

                "current_pattern":
                    None,

                "previous_bullish_engulfing":
                    False,

                "current_bullish_engulfing":
                    False

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
    # 1. 업비트 4시간봉 EMA5 < EMA15
    #
    # OR
    #
    # 2. 이전 4시간봉 상승장악형
    #
    # OR
    #
    # 3. 현재 4시간봉 상승장악형
    #
    # 하락장악 / 하락관통 제외
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
                "previous_bullish_engulfing",
                False
            )
            or
            row.get(
                "current_bullish_engulfing",
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
        "UPBIT | 거래대금 TOP%s | 4시간봉 SIGNAL=%s",
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
# 현재/이전 패턴 HTML
#
# 하락장악 / 하락관통은 표시하지 않음
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

    if pattern == "양수도지":

        return (
            '<span class="current-pattern bullish">'
            '● 양수도지'
            '</span>'
        )

    if pattern == "상승관통형":

        return (
            '<span class="current-pattern bullish">'
            '▲ 상승관통형'
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
# 종목 / 거래대금 / 변동률 / 이전패턴 / 현재패턴
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
                이전 4시간봉
            </span>

            <strong>
                {current_pattern_html(
                    row.get(
                        "previous_pattern"
                    )
                )}
            </strong>

        </div>


        <div class="signal-pattern">

            <span>
                현재 4시간봉
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
                EMA5 &lt; EMA15
                OR
                이전 / 현재 상승장악
            </b>

            <small>
                업비트 KST 09:00 기준 4시간봉
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
                이전 4시간봉
            </div>

            <div>
                현재 4시간봉
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
# 종목 / 현재가 / 거래대금 / 변동률 /
# 이전 4시간봉 패턴 / 현재 4시간봉 패턴
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
            "previous_bullish_engulfing",
            False
        )
        or
        row.get(
            "current_bullish_engulfing",
            False
        )
        or
        row.get(
            "ema_reverse",
            False
        )
    )

    previous_pattern = row.get(
        "previous_pattern"
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
                        이전 4시간봉
                    </span>

                    <strong>

                        {current_pattern_html(
                            previous_pattern
                        )}

                    </strong>

                </div>


                <div>

                    <span>
                        현재 4시간봉
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
        1.1fr
        0.9fr
        0.8fr
        1.2fr
        1.2fr;

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

    font-size: 6px;

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
        repeat(5, 1fr);

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

    font-size: 7px;
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


    .market-summary {

        grid-template-columns:
            repeat(5, 1fr);
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

        font-size: 6px;
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
            1.1fr
            0.9fr
            0.8fr
            1.2fr
            1.2fr;
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

        font-size: 5px;
    }


    .signal-pattern .current-pattern {

        font-size: 5px;
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
        "START | BTC 시황 + 업비트 09:00 기준 4시간봉 EMA5/15 역배열 + 이전/현재 상승장악 SIGNAL + TOP10"
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
