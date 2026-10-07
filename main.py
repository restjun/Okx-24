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
# 변동률 표시 전용
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
#
# 업비트 실제 240분봉
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
# 일봉 기간 생성
#
# KST 09:00 기준
#
# 최근 6개 반환
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

    # =====================================================
    # 전체 일봉 기간 생성
    # =====================================================

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

        # -------------------------------------------------
        # 현재 진행 일봉에 현재가 반영
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

    # =====================================================
    # 최근 6개만 화면 표시
    # =====================================================

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

    # 이전 캔들 음봉
    if prev_close >= prev_open:

        return False

    # 현재 캔들 양봉
    if curr_close <= curr_open:

        return False

    # 현재 양봉 실체가 이전 음봉 실체를 감싸야 함
    return bool(
        curr_open <= prev_close
        and
        curr_close >= prev_open
    )


# =========================================================
# 관통형
# =========================================================

def is_piercing_line(
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

    # 이전 캔들 음봉
    if prev_close >= prev_open:

        return False

    # 현재 캔들 양봉
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
# 업비트 실제 4시간봉 기간 생성
#
# SIGNAL 조건:
#
# 1. 상승장악형
# 또는
# 2. 관통형
#
# 패턴봉 + 다음 캔들
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

    # =====================================================
    # 전체 4시간봉 생성
    # =====================================================

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

        # -------------------------------------------------
        # 현재 진행 중인 캔들에 현재가 반영
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

    # =====================================================
    # 캔들 패턴 SIGNAL
    #
    # 상승장악형 또는 관통형
    #
    # 패턴봉 + 다음봉
    # =====================================================

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

        piercing_line = (
            is_piercing_line(
                previous,
                current
            )
        )

        pattern = None

        if bullish_engulfing:

            pattern = "상승장악형"

        elif piercing_line:

            pattern = "관통형"

        if pattern is None:

            continue

        # -------------------------------------------------
        # 패턴봉
        # -------------------------------------------------

        periods[i][
            "signal"
        ] = True

        periods[i][
            "pattern"
        ] = pattern

        periods[i][
            "signal_reason"
        ] = pattern

        # -------------------------------------------------
        # 패턴봉 다음 캔들
        # -------------------------------------------------

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

    # =====================================================
    # 최근 6개만 표시
    # =====================================================

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
                None

        }

    periods = build_upbit_4h_periods(
        df,
        price
    )

    details = signal_details(
        periods
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
    analysis_4h
):

    coin = market.replace(
        "KRW-",
        ""
    )

    signal_4h = (
        analysis_4h[
            "signal_pass"
        ]
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
            signal_4h,

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

        "simultaneous_signal":
            signal_4h

    }


# =========================================================
# 업비트 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_daily_data
    global latest_upbit_update_time
    global latest_upbit_daily_update_time

    all_markets = get_upbit_markets()

    # =====================================================
    # 1.
    # 전체 KRW 종목 대상
    #
    # 당일 양수 조건 없음
    # =====================================================

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

    # =====================================================
    # 2.
    # 전체 종목 중 거래대금 TOP10
    # =====================================================

    candidates.sort(
        key=lambda x:
            x["item"]["volume_24h"],
        reverse=True
    )

    candidates = candidates[
        :TOP_N
    ]

    rows = []

    # =====================================================
    # 3.
    # TOP10 4시간봉 분석
    # =====================================================

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
                    None

            }

        rows.append(
            make_row(
                rank,
                market,
                item,
                analysis_daily,
                analysis_4h
            )
        )

    latest_upbit_data = rows

    latest_upbit_daily_data = rows

    latest_upbit_daily_update_time = kst()

    latest_upbit_update_time = (
        latest_upbit_daily_update_time
    )

    log.info(
        "UPBIT | 거래대금 TOP%s | 4H 상승장악/관통형 SIGNAL=%s",
        TOP_N,
        sum(
            x["signal_4h"]
            for x in rows
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
#
# OKX BTC-USDT-SWAP 1D 캔들
# UTC 00:00 → KST 09:00
# =========================================================

def update_okx_btc():

    global latest_btc_okx_price
    global latest_btc_daily_periods
    global latest_btc_daily_change

    # =====================================================
    # 현재 BTC 가격
    # =====================================================

    price = okx_price()

    latest_btc_okx_price = price

    if price is None:

        return

    # =====================================================
    # OKX 1D 캔들
    # =====================================================

    d1d = okx_candles(
        "1D",
        100
    )

    if d1d.empty:

        latest_btc_daily_periods = []

        latest_btc_daily_change = None

        log.warning(
            "OKX BTC 1D 캔들 데이터 없음"
        )

        return

    # =====================================================
    # OKX 1D 캔들 사용
    #
    # UTC 00:00
    # =
    # KST 09:00
    # =====================================================

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

    # =====================================================
    # 현재 진행 중인 OKX 일봉에 현재가 반영
    # =====================================================

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

    # =====================================================
    # KST 09:00 기준 일봉
    # =====================================================

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

    log.info(
        "OKX BTC | 1D | 현재 변동률=%s",
        latest_btc_daily_change
    )


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

    if reason == "관통형":

        return (
            '<span class="pattern-cross">'
            '▲ 관통형'
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
# BTC 일봉 표시
# =========================================================

def btc_daily_cells(
    periods
):

    result = []

    for p in periods:

        active_class = (
            "current"
            if p.get("active")
            else ""
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

            </div>
            """
        )

    return "".join(result)


# =========================================================
# BTC HTML
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


        <div class="market-timeframe">

            <div class="timeframe-head">

                <b>
                    일봉
                </b>

                <span>
                    OKX 1D · KST 09:00
                </span>

            </div>

            <div class="grid">
                {btc_daily_cells(
                    latest_btc_daily_periods
                )}
            </div>

        </div>

    </section>
    """


# =========================================================
# 카드
# =========================================================

def card(
    row,
    kind
):

    signal = row.get(
        "signal_4h",
        False
    )

    if kind == "top":

        status = ""

        if signal:

            status = (
                '<span class="signal-badge">'
                '⭐ 상승장악/관통 SIGNAL'
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
                        일봉 변동률
                    </span>

                    <strong>
                        {fmt_change(
                            row.get(
                                "daily_change"
                            )
                        )}
                    </strong>

                </div>


                <div>

                    <span>
                        4H PATTERN
                    </span>

                    <strong>
                        상승장악 / 관통
                    </strong>

                </div>

            </div>


            <div class="signal-bar">

                <span>
                    4H PATTERN
                </span>

                <i></i>

                <small>
                    상승장악형 / 관통형
                </small>

            </div>


            <div class="signal-section">

                <div class="signal-head">

                    <b>
                        4시간봉 상승장악 / 관통형
                    </b>

                    <span>
                        패턴봉 + 다음봉
                    </span>

                </div>

                <div class="grid">
                    {cells(
                        row.get(
                            "periods_4h",
                            []
                        )
                    )}
                </div>

            </div>

        </article>
        """

    return f"""
    <article class="both-card">

        <div class="both-head">

            <div>

                <span class="rank">
                    #{row["rank"]}
                </span>

                <b>
                    {html.escape(
                        row["name"]
                    )}
                </b>

            </div>

            <span class="both-badge">
                ⭐ PATTERN
            </span>

        </div>


        <div class="both-summary">

            <div>

                <span>
                    일봉 변동률
                </span>

                <strong>
                    {fmt_change(
                        row.get(
                            "daily_change"
                        )
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
                    현재 4H
                </span>

                <strong>
                    {(
                        "PATTERN"
                        if row.get(
                            "signal_4h_current"
                        )
                        else "-"
                    )}
                </strong>

            </div>


            <div>

                <span>
                    이전 4H
                </span>

                <strong>
                    {(
                        "PATTERN"
                        if row.get(
                            "signal_4h_previous"
                        )
                        else "-"
                    )}
                </strong>

            </div>

        </div>


        <div class="signal-section">

            <div class="signal-head">

                <b>
                    4시간봉 상승장악 / 관통형
                </b>

                <span>
                    패턴봉 + 다음봉
                </span>

            </div>

            <div class="grid">
                {cells(
                    row.get(
                        "periods_4h",
                        []
                    )
                )}
            </div>

        </div>

    </article>
    """


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
    color: #38d878;
}


.down {
    color: #ff5966;
}


.zero {
    color: #68737e;
}


.pattern-cross {

    display: block;

    margin-top: 4px;

    color: #5ed6ff;

    font-size: 5px;

    font-weight: 900;
}


.pattern-next {

    display: block;

    margin-top: 4px;

    color: #e4c45e;

    font-size: 5px;

    font-weight: 900;
}


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

    border-bottom:
        1px solid #202a33;
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

    font-weight: 800;
}


.market-timeframe {

    border-bottom:
        1px solid #1e272f;
}


.market-timeframe:last-child {

    border-bottom: 0;
}


.timeframe-head {

    min-height: 27px;

    padding: 4px 9px;

    display: flex;

    align-items: center;

    justify-content: space-between;

    background: #0e151b;
}


.timeframe-head b {

    font-size: 8px;

    color: #dbe1e6;
}


.timeframe-head span {

    color: #626e79;

    font-size: 6px;
}


.grid {

    display: grid;

    grid-template-columns:
        repeat(6, 1fr);
}


.tf-cell {

    min-height: 59px;

    padding: 5px 2px;

    text-align: center;

    background: #0b1015;

    border-right:
        1px solid #1b242c;
}


.tf-cell:last-child {

    border-right: 0;
}


.tf-cell.current {

    background: #10241a;
}


.tf-date {

    color: #6c7782;

    font-size: 6px;

    white-space: nowrap;

    overflow: hidden;

    text-overflow: ellipsis;
}


.tf-cell strong {

    display: block;

    margin-top: 4px;

    font-size: 8px;
}


.both-card {

    background: #0c1115;

    border:
        1px solid #796329;

    margin-bottom: 7px;
}


.both-head {

    min-height: 39px;

    padding: 0 9px;

    display: flex;

    align-items: center;

    justify-content: space-between;

    border-bottom:
        1px solid #39321e;
}


.both-head > div {

    display: flex;

    align-items: center;

    gap: 7px;
}


.rank {

    color: #c9a83d;

    font-size: 8px;

    font-weight: 800;
}


.both-head b {

    font-size: 10px;

    font-weight: 900;
}


.both-badge {

    color: #e4c45e;

    font-size: 7px;

    font-weight: 800;
}


.both-summary {

    display: grid;

    grid-template-columns:
        repeat(4, 1fr);

    border-bottom:
        1px solid #39321e;
}


.both-summary > div {

    min-height: 43px;

    padding: 5px;

    text-align: center;

    background: #111611;
}


.both-summary > div + div {

    border-left:
        1px solid #292c20;
}


.both-summary span {

    display: block;

    color: #747c74;

    font-size: 6px;
}


.both-summary strong {

    display: block;

    margin-top: 4px;

    font-size: 8px;
}


.both-card .signal-head {

    background: #11150f;
}


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


.signal-bar {

    height: 29px;

    padding: 0 9px;

    display: flex;

    align-items: center;

    gap: 7px;

    border-bottom:
        1px solid #30322b;

    background: #10140f;
}


.signal-bar span {

    color: #d0ad48;

    font-size: 7px;

    font-weight: 900;

    letter-spacing: 0.7px;
}


.signal-bar i {

    flex: 1;

    height: 1px;

    background: #353a35;
}


.signal-bar small {

    color: #68736d;

    font-size: 6px;
}


.signal-section {

    background: #0b1014;
}


.signal-head {

    height: 28px;

    padding: 0 9px;

    display: flex;

    align-items: center;

    justify-content: space-between;

    background: #0f151a;

    border-bottom:
        1px solid #202931;
}


.signal-head b {

    color: #d9dfe4;

    font-size: 7px;

    font-weight: 900;
}


.signal-head span {

    color: #626e78;

    font-size: 6px;
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


    .grid {

        grid-template-columns:
            repeat(3, 1fr);
    }


    .tf-cell {

        min-height: 56px;

        padding:
            4px 2px;
    }


    .tf-cell strong {

        font-size: 7px;
    }


    .pattern-cross {

        font-size: 4px;
    }


    .pattern-next {

        font-size: 4px;
    }


    .coin-head,
    .both-head {

        min-height: 35px;

        padding: 0 7px;
    }


    .coin-title b,
    .both-head b {

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


    .signal-bar {

        height: 27px;

        padding: 0 7px;
    }


    .signal-head {

        height: 26px;

        padding: 0 7px;
    }


    .signal-head b {

        font-size: 6px;
    }


    .signal-head span {

        font-size: 5px;
    }


    .both-summary > div {

        min-height: 40px;
    }


    .both-summary span {

        font-size: 5px;
    }


    .both-summary strong {

        font-size: 6px;
    }


    .both-badge {

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

    if USE_UPBIT == "Y":

        # -------------------------------------------------
        # TOP LIST
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
                    '<div class="empty">'
                    '현재 거래대금 TOP10 데이터 없음'
                    '</div>'
                )

            s += f"""

            <section>

                <div class="section-title">

                    <div>

                        <span class="section-kicker">
                            RANKING
                        </span>

                        <b>
                            TOP10 · 거래대금 순
                        </b>

                    </div>

                    <small>
                        거래대금 기준
                    </small>

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
        "START | OKX BTC 1D + 업비트 거래대금 TOP10 + 4H 상승장악/관통형"
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
