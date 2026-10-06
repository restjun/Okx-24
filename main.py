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

SHOW_TOP_LIST = "N"

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
# 4시간봉 SIGNAL 패턴
# =========================================================

SIGNAL_CANDLE_PATTERNS = [
    "하락장악",
    "3캔들 하락장악",
    "4캔들 하락장악",
    "5캔들 하락장악",
    "역 관통형"
]


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


def timeframe_delta(tf):

    if tf == "4h":

        return timedelta(
            hours=4
        )

    return timedelta(
        days=1
    )


def current_tf_start(tf):

    now = datetime.now(KST)

    # -----------------------------------------------------
    # 4시간봉
    #
    # 00:00
    # 04:00
    # 08:00
    # 12:00
    # 16:00
    # 20:00
    # -----------------------------------------------------

    if tf == "4h":

        hour = (
            now.hour // 4
        ) * 4

        return now.replace(
            hour=hour,
            minute=0,
            second=0,
            microsecond=0
        )

    # -----------------------------------------------------
    # 일봉
    #
    # KST 09:00
    # -----------------------------------------------------

    x = now.replace(
        hour=9,
        minute=0,
        second=0,
        microsecond=0
    )

    if now < x:

        return x - timedelta(
            days=1
        )

    return x


def recent_periods(
    tf,
    count=6
):

    cur = current_tf_start(tf)

    delta = timeframe_delta(tf)

    out = []

    for i in range(
        count - 1,
        -1,
        -1
    ):

        start = cur - delta * i

        end = start + delta

        if tf == "4h":

            label = start.strftime(
                "%m/%d %H:%M"
            )

        else:

            label = start.strftime(
                "%m/%d 09:00"
            )

        out.append({

            "start":
                start,

            "end":
                end,

            "active":
                i == 0,

            "label":
                label

        })

    return out


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
# 변동률 기준
# KST 09:00
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
# 캔들 기본값
# =========================================================

def candle_parts(c):

    try:

        o, h, l, cl = map(
            float,
            (
                c["open"],
                c["high"],
                c["low"],
                c["close"]
            )
        )

    except Exception:

        return None

    total = h - l

    if total <= 0:

        return None

    body = abs(
        cl - o
    )

    return {

        "open":
            o,

        "high":
            h,

        "low":
            l,

        "close":
            cl,

        "body":
            body,

        "total":
            total,

        "upper":
            h - max(
                o,
                cl
            ),

        "lower":
            min(
                o,
                cl
            ) - l,

        "body_ratio":
            body / total,

        "bull":
            cl > o,

        "bear":
            cl < o

    }


# =========================================================
# 하락장악
# =========================================================

def bearish_engulfing(
    a,
    b
):

    p1 = candle_parts(a)

    p2 = candle_parts(b)

    if not p1 or not p2:

        return False

    return bool(

        p1["bull"]

        and p2["bear"]

        and p2["open"]
        >= max(
            p1["open"],
            p1["close"]
        )

        and p2["close"]
        <= min(
            p1["open"],
            p1["close"]
        )

        and p2["body"]
        > p1["body"]

    )


# =========================================================
# 역 관통형
# =========================================================

def bearish_piercing(
    a,
    b
):

    p1 = candle_parts(a)

    p2 = candle_parts(b)

    if not p1 or not p2:

        return False

    mid = (
        p1["open"]
        + p1["close"]
    ) / 2

    return bool(

        p1["bull"]

        and p2["bear"]

        and p2["close"] < mid

        and p2["close"] > p1["open"]

    )


# =========================================================
# 2캔들
# =========================================================

def two_patterns(
    a,
    b
):

    p1 = candle_parts(a)

    p2 = candle_parts(b)

    if not p1 or not p2:

        return []

    out = []

    # -----------------------------------------------------
    # 하락장악
    # -----------------------------------------------------

    if bearish_engulfing(
        a,
        b
    ):

        out.append(
            "하락장악"
        )

    # -----------------------------------------------------
    # 역 관통형
    # -----------------------------------------------------

    if bearish_piercing(
        a,
        b
    ):

        out.append(
            "역 관통형"
        )

    return out


# =========================================================
# 3캔들 하락장악
# =========================================================

def three_patterns(
    a,
    b,
    c
):

    p1 = candle_parts(a)

    p2 = candle_parts(b)

    p3 = candle_parts(c)

    if not p1 or not p2 or not p3:

        return []

    out = []

    # -----------------------------------------------------
    # 첫 번째 양봉
    # 두 번째 완충/중간 캔들
    # 세 번째 음봉
    # 첫 번째 몸통을 세 번째 음봉이 완전히 장악
    # -----------------------------------------------------

    if (
        p1["bull"]
        and p3["bear"]
        and p3["open"]
        >= max(
            p1["open"],
            p1["close"]
        )
        and p3["close"]
        <= min(
            p1["open"],
            p1["close"]
        )
        and p3["body"]
        > p1["body"]
    ):

        out.append(
            "3캔들 하락장악"
        )

    return out


# =========================================================
# 4캔들 하락장악
# =========================================================

def four_patterns(
    a,
    b,
    c,
    d
):

    p1 = candle_parts(a)

    p2 = candle_parts(b)

    p3 = candle_parts(c)

    p4 = candle_parts(d)

    if not all(
        (
            p1,
            p2,
            p3,
            p4
        )
    ):

        return []

    out = []

    if (
        p1["bull"]
        and p4["bear"]
        and p4["open"]
        >= max(
            p1["open"],
            p1["close"]
        )
        and p4["close"]
        <= min(
            p1["open"],
            p1["close"]
        )
        and p4["body"]
        > p1["body"]
    ):

        out.append(
            "4캔들 하락장악"
        )

    return out


# =========================================================
# 5캔들 하락장악
# =========================================================

def five_patterns(
    a,
    b,
    c,
    d,
    e
):

    p1 = candle_parts(a)

    p2 = candle_parts(b)

    p3 = candle_parts(c)

    p4 = candle_parts(d)

    p5 = candle_parts(e)

    if not all(
        (
            p1,
            p2,
            p3,
            p4,
            p5
        )
    ):

        return []

    out = []

    if (
        p1["bull"]
        and p5["bear"]
        and p5["open"]
        >= max(
            p1["open"],
            p1["close"]
        )
        and p5["close"]
        <= min(
            p1["open"],
            p1["close"]
        )
        and p5["body"]
        > p1["body"]
    ):

        out.append(
            "5캔들 하락장악"
        )

    return out


# =========================================================
# 기간 생성
# =========================================================

def build_periods(
    df,
    tf,
    current_price=None
):

    if df is None or df.empty:

        return []

    periods = recent_periods(
        tf,
        6
    )

    out = []

    for p in periods:

        part = df[
            (df.datetime >= p["start"])
            &
            (df.datetime < p["end"])
        ]

        if part.empty:

            out.append({

                **p,

                "open":
                    None,

                "high":
                    None,

                "low":
                    None,

                "close":
                    None,

                "change":
                    None,

                "patterns":
                    []

            })

            continue

        o = float(
            part.iloc[0].open
        )

        h = float(
            part.high.max()
        )

        l = float(
            part.low.min()
        )

        c = float(
            part.iloc[-1].close
        )

        # -------------------------------------------------
        # 현재 활성봉 현재가 반영
        # -------------------------------------------------

        if (
            p["active"]
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

        out.append({

            **p,

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

            "patterns":
                []

        })

    # =====================================================
    # 패턴 계산
    # =====================================================

    for i, p in enumerate(out):

        pats = []

        # -------------------------------------------------
        # 2캔들
        # -------------------------------------------------

        if (
            i >= 1
            and p["open"] is not None
            and out[i - 1]["open"] is not None
        ):

            pats += two_patterns(
                out[i - 1],
                p
            )

        # -------------------------------------------------
        # 3캔들
        # -------------------------------------------------

        if (
            i >= 2
            and all(
                out[j]["open"] is not None
                for j in (
                    i - 2,
                    i - 1,
                    i
                )
            )
        ):

            pats += three_patterns(
                out[i - 2],
                out[i - 1],
                p
            )

        # -------------------------------------------------
        # 4캔들
        # -------------------------------------------------

        if (
            i >= 3
            and all(
                out[j]["open"] is not None
                for j in (
                    i - 3,
                    i - 2,
                    i - 1,
                    i
                )
            )
        ):

            pats += four_patterns(
                out[i - 3],
                out[i - 2],
                out[i - 1],
                p
            )

        # -------------------------------------------------
        # 5캔들
        # -------------------------------------------------

        if (
            i >= 4
            and all(
                out[j]["open"] is not None
                for j in (
                    i - 4,
                    i - 3,
                    i - 2,
                    i - 1,
                    i
                )
            )
        ):

            pats += five_patterns(
                out[i - 4],
                out[i - 3],
                out[i - 2],
                out[i - 1],
                p
            )

        p["patterns"] = list(
            dict.fromkeys(
                pats
            )
        )

    return out


# =========================================================
# SIGNAL 단일 기간 판정
# =========================================================

def period_signal(period):

    if not period:

        return False

    change = period.get(
        "change"
    )

    open_price = period.get(
        "open"
    )

    close_price = period.get(
        "close"
    )

    patterns = period.get(
        "patterns",
        []
    )

    if change is None:

        return False

    if open_price is None:

        return False

    if close_price is None:

        return False

    # -----------------------------------------------------
    # 반드시 음수 봉
    # -----------------------------------------------------

    if change >= 0:

        return False

    if close_price >= open_price:

        return False

    # -----------------------------------------------------
    # 지정 SIGNAL 패턴
    # -----------------------------------------------------

    if not any(
        pattern in patterns
        for pattern in SIGNAL_CANDLE_PATTERNS
    ):

        return False

    return True


# =========================================================
# SIGNAL 판정
#
# 현재 4시간봉이 음수일 때만
# 현재봉 + 이전봉 검사
# =========================================================

def signal_pass(periods):

    if not periods:

        return False

    current = periods[-1]

    current_change = current.get(
        "change"
    )

    if current_change is None:

        return False

    # -----------------------------------------------------
    # 현재봉 + / 0이면
    # 이전봉 SIGNAL도 차단
    # -----------------------------------------------------

    if current_change >= 0:

        return False

    # -----------------------------------------------------
    # 현재봉
    # -----------------------------------------------------

    if period_signal(current):

        return True

    # -----------------------------------------------------
    # 이전봉
    # -----------------------------------------------------

    if len(periods) >= 2:

        previous = periods[-2]

        if period_signal(previous):

            return True

    return False


# =========================================================
# SIGNAL 세부정보
# =========================================================

def signal_details(periods):

    empty = {

        "signal":
            False,

        "current_signal":
            False,

        "previous_signal":
            False,

        "signal_change":
            None,

        "signal_patterns":
            [],

        "signal_period":
            None

    }

    if not periods:

        return empty

    current = periods[-1]

    current_change = current.get(
        "change"
    )

    if current_change is None:

        return empty

    # -----------------------------------------------------
    # 현재 4시간봉이 + 또는 0이면
    # 이전봉도 SIGNAL 차단
    # -----------------------------------------------------

    if current_change >= 0:

        return {

            "signal":
                False,

            "current_signal":
                False,

            "previous_signal":
                False,

            "signal_change":
                None,

            "signal_patterns":
                [],

            "signal_period":
                None

        }

    current_signal = period_signal(
        current
    )

    previous = (
        periods[-2]
        if len(periods) >= 2
        else None
    )

    previous_signal = (
        period_signal(
            previous
        )
        if previous is not None
        else False
    )

    # -----------------------------------------------------
    # 현재봉 우선
    # -----------------------------------------------------

    if current_signal:

        target = current

    elif previous_signal:

        target = previous

    else:

        target = None

    if target is None:

        return {

            "signal":
                False,

            "current_signal":
                current_signal,

            "previous_signal":
                previous_signal,

            "signal_change":
                None,

            "signal_patterns":
                [],

            "signal_period":
                None

        }

    return {

        "signal":
            True,

        "current_signal":
            current_signal,

        "previous_signal":
            previous_signal,

        "signal_change":
            target.get(
                "change"
            ),

        "signal_patterns":
            [
                x
                for x in SIGNAL_CANDLE_PATTERNS
                if x in target.get(
                    "patterns",
                    []
                )
            ],

        "signal_period":
            target.get(
                "label"
            )

    }


# =========================================================
# 일봉 변동률 분석
#
# 업비트 KST 09:00 기준
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

    periods = build_periods(
        df,
        "1d",
        price
    )

    if not periods:

        return {

            "change":
                None,

            "periods":
                []

        }

    current = periods[-1]

    return {

        "change":
            current.get(
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

            "signal_patterns":
                [],

            "signal_period":
                None

        }

    df = df.copy()

    current_start = current_tf_start(
        "4h"
    )

    active_mask = (
        df["datetime"]
        >= current_start
    )

    if active_mask.any():

        last_idx = df.index[
            active_mask
        ][-1]

        df.loc[
            last_idx,
            "close"
        ] = float(
            price
        )

    periods = build_periods(
        df,
        "4h",
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

        "signal_patterns":
            details[
                "signal_patterns"
            ],

        "signal_period":
            details[
                "signal_period"
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

        # -------------------------------------------------
        # 일봉
        # -------------------------------------------------

        "daily_change":
            analysis_daily[
                "change"
            ],

        "periods_daily":
            analysis_daily[
                "periods"
            ],

        # -------------------------------------------------
        # 4시간봉
        # -------------------------------------------------

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

        "signal_4h_patterns":
            analysis_4h[
                "signal_patterns"
            ],

        "signal_4h_period":
            analysis_4h[
                "signal_period"
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
    # 1단계
    #
    # 업비트 09:00 일봉 변동률 계산
    #
    # 마이너스 종목만 통과
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

            daily_change = analysis_daily[
                "change"
            ]

            if (
                daily_change is not None
                and daily_change < 0
            ):

                candidates.append({

                    "item":
                        item,

                    "analysis_daily":
                        analysis_daily

                })

        except Exception as e:

            log.warning(
                "%s 일봉 변동률 오류: %s",
                market,
                e
            )

    # =====================================================
    # 2단계
    #
    # 마이너스 종목 중 거래대금 TOP20
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
    # 3단계
    #
    # TOP20 종목의 4시간봉 분석
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

                "signal_patterns":
                    [],

                "signal_period":
                    None

            }

        row = make_row(
            rank,
            market,
            item,
            analysis_daily,
            analysis_4h
        )

        rows.append(row)

    latest_upbit_data = rows

    latest_upbit_daily_data = rows

    latest_upbit_daily_update_time = kst()

    latest_upbit_update_time = (
        latest_upbit_daily_update_time
    )

    log.info(
        "UPBIT | 일봉 음수 TOP%s | 4H SIGNAL=%s",

        TOP_N,

        sum(
            x["signal_4h"]
            for x in rows
        )
    )


# =========================================================
# OKX
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

    # =====================================================
    # BTC 1시간 → KST 09:00 일봉
    # 기존 유지
    # =====================================================

    d1h = okx_candles(
        "1H",
        200
    )

    if not d1h.empty:

        d1h = d1h.copy()

        d1h["datetime"] = (
            d1h["datetime"]
            .dt
            .tz_convert(KST)
        )

        shifted = d1h.copy()

        shifted["day_start"] = (
            (
                shifted["datetime"]
                - pd.Timedelta(
                    hours=9
                )
            )
            .dt
            .floor("D")
            + pd.Timedelta(
                hours=9
            )
        )

        daily = (
            shifted
            .groupby("day_start")
            .agg(

                open=(
                    "open",
                    "first"
                ),

                high=(
                    "high",
                    "max"
                ),

                low=(
                    "low",
                    "min"
                ),

                close=(
                    "close",
                    "last"
                )

            )
            .reset_index()
            .rename(
                columns={
                    "day_start":
                        "datetime"
                }
            )
        )

        current_day_start = current_tf_start(
            "1d"
        )

        active_daily = (
            daily["datetime"]
            >= current_day_start
        )

        if active_daily.any():

            last_daily_idx = daily.index[
                active_daily
            ][-1]

            daily.loc[
                last_daily_idx,
                "close"
            ] = float(
                price
            )

    else:

        daily = pd.DataFrame()

    latest_btc_daily_periods = (
        build_periods(
            daily,
            "1d",
            price
        )
    )

    if latest_btc_daily_periods:

        latest_btc_daily_change = (
            latest_btc_daily_periods[-1]
            .get("change")
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

        patterns = p.get(
            "patterns",
            []
        )

        pattern_html = ""

        if patterns:

            pattern_html = (
                "<small>"
                + " · ".join(
                    html.escape(x)
                    for x in patterns
                )
                + "</small>"
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

                {pattern_html}

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
                    당일봉 + 전일봉 · KST 09:00
                </span>

            </div>

            <div class="grid">
                {cells(
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

    # =====================================================
    # TOP
    # =====================================================

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
                        4시간봉
                    </span>

                    <strong>
                        {fmt_change(
                            row.get(
                                "signal_4h_change"
                            )
                        )}
                    </strong>

                </div>

            </div>


            <div class="signal-bar">

                <span>
                    4H SIGNAL
                </span>

                <i></i>

                <small>
                    현재봉 + 이전봉
                </small>

            </div>


            <div class="signal-section">

                <div class="signal-head">

                    <b>
                        4시간봉 SIGNAL
                    </b>

                    <span>
                        하락장악 · 3/4/5캔들 · 역 관통형
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


    # =====================================================
    # SIGNAL
    # =====================================================

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
                ⭐ 4시간봉 SIGNAL
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
                    4시간봉
                </span>

                <strong>
                    {fmt_change(
                        row.get(
                            "signal_4h_change"
                        )
                    )}
                </strong>

            </div>


            <div>

                <span>
                    기준
                </span>

                <strong>
                    현재/이전
                </strong>

            </div>

        </div>


        <div class="signal-section">

            <div class="signal-head">

                <b>
                    4시간봉
                </b>

                <span>
                    하락장악 · 3/4/5캔들 · 역 관통형
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
# SIGNAL 영역
# =========================================================

def both_section(data):

    rows = [
        r
        for r in data
        if r.get(
            "signal_4h"
        )
    ]

    if rows:

        content = "".join(
            card(
                r,
                "both"
            )
            for r in rows
        )

    else:

        content = (
            '<div class="empty">'
            '현재 4시간봉 SIGNAL 없음'
            '</div>'
        )

    return f"""

    <section>

        <div class="section-title signal-section-title">

            <div>

                <span class="section-kicker">
                    SIGNAL
                </span>

                <b>
                    ⭐ 4시간봉 SIGNAL
                </b>

            </div>

            <small>
                현재봉 + 이전봉
            </small>

        </div>

        {content}

    </section>

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


.section-title {

    min-height: 42px;

    padding: 7px 9px;

    display: flex;

    align-items: center;

    justify-content: space-between;

    border-bottom:
        1px solid #26313a;

    margin-bottom: 5px;
}


.section-title > div {

    display: flex;

    align-items: center;

    gap: 7px;
}


.section-title b {

    font-size: 10px;

    font-weight: 800;
}


.section-title small {

    color: #697580;

    font-size: 7px;
}


.section-kicker {

    color: #7d8994;

    font-size: 6px;

    letter-spacing: 1px;

    font-weight: 700;
}


.signal-section-title {

    border-bottom:
        1px solid #725f28;
}


.signal-section-title .section-kicker {

    color: #c9a83d;
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

    min-height: 54px;

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


.tf-cell small {

    display: block;

    margin-top: 3px;

    color: #c5a44b;

    font-size: 5px;

    white-space: nowrap;

    overflow: hidden;

    text-overflow: ellipsis;
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


.empty {

    padding: 25px 10px;

    text-align: center;

    color: #626e79;

    background: #0c1116;

    border:
        1px solid #202a33;

    font-size: 8px;
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

        min-height: 50px;

        padding:
            4px 2px;
    }


    .tf-cell strong {

        font-size: 7px;
    }


    .tf-cell small {

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
        # 4시간봉 SIGNAL
        # -------------------------------------------------

        s += both_section(
            latest_upbit_data
        )

        # -------------------------------------------------
        # TOP LIST
        #
        # SHOW_TOP_LIST = Y일 때만 표시
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
                    '현재 마이너스 종목 데이터 없음'
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
                            🏆 업비트 TOP{TOP_N}
                        </b>

                    </div>

                    <small>
                        일봉 음수 종목 · 거래대금 기준
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
        "START | 업비트 일봉 음수 종목 → 4시간봉 하락장악/3/4/5/역관통 → TOP20"
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
