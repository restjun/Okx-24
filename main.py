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
# 기본 설정
# =========================================================

VOLUME_HOURS = 24

TOP_N = 15

UPDATE_MINUTES = 1

HISTORY_CHUNK = 200

MAX_HISTORY_CHUNKS = 10

USE_UPBIT = "Y"

USE_OKX = "N"

REQUEST_INTERVAL = 0.08

RATE_LIMIT_WAIT = 3

MAX_RETRIES = 10


# =========================================================
# 전역
# =========================================================

latest_upbit_data = []

latest_okx_data = []

latest_upbit_update_time = "-"

latest_okx_update_time = "-"

latest_upbit_markets = []

request_lock = threading.Lock()

update_lock = threading.Lock()

last_request_time = 0


# =========================================================
# BTC 시황
#
# 당일 상승률:
# KST 09:00 기준
#
# 12시간 상승률:
# 09:00 ~ 21:00
# 21:00 ~ 09:00
#
# Signal 필터:
# 현재 시간대의 BTC 12시간 상승률 > 0
# =========================================================

OKX_BASE_URL = "https://www.okx.com"

OKX_BTC_INST_ID = "BTC-USDT"

latest_btc_okx_price = None

latest_btc_daily_change = None

latest_btc_09_21_change = None

latest_btc_21_09_change = None

latest_btc_current_12h_change = None

latest_btc_current_12h_label = "-"


# =========================================================
# 시간
# =========================================================

def kst():

    return datetime.now(
        KST
    ).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def format_timeframe(minutes):

    minutes = int(minutes)

    if minutes == 1440:
        return "1D"

    if minutes >= 60:
        return f"{minutes // 60}H"

    return f"{minutes}M"


# =========================================================
# 현재 시간대 확인
#
# 09:00 ~ 20:59
#     → 09:00 ~ 21:00
#
# 21:00 ~ 08:59
#     → 21:00 ~ 09:00
# =========================================================

def get_current_12h_period():

    now = datetime.now(KST)

    if 9 <= now.hour < 21:

        return {
            "label": "09:00 ~ 21:00",
            "start_hour": 9,
            "end_hour": 21
        }

    return {
        "label": "21:00 ~ 09:00",
        "start_hour": 21,
        "end_hour": 9
    }


# =========================================================
# 현재 12시간 구간 시작시간
# =========================================================

def get_current_12h_start():

    now = datetime.now(KST)

    if 9 <= now.hour < 21:

        start = now.replace(
            hour=9,
            minute=0,
            second=0,
            microsecond=0
        )

    else:

        if now.hour >= 21:

            start = now.replace(
                hour=21,
                minute=0,
                second=0,
                microsecond=0
            )

        else:

            start = (
                now.replace(
                    hour=21,
                    minute=0,
                    second=0,
                    microsecond=0
                )
                -
                timedelta(days=1)
            )

    return start


# =========================================================
# 이전 12시간 구간 시작시간
# =========================================================

def get_previous_12h_start():

    current_start = (
        get_current_12h_start()
    )

    return (
        current_start
        -
        timedelta(hours=12)
    )


# =========================================================
# 업비트 기준 현재 캔들 시작시간
# =========================================================

def get_current_candle_start(minutes):

    minutes = int(minutes)

    now = datetime.now(KST)

    # =====================================================
    # 업비트 240분봉
    #
    # 01 / 05 / 09 / 13 / 17 / 21
    # =====================================================

    if minutes == 240:

        if now.hour == 0:

            current = now.replace(
                hour=21,
                minute=0,
                second=0,
                microsecond=0
            ) - timedelta(days=1)

            return current.replace(
                tzinfo=None
            )

        start_hour = (
            1
            +
            (
                (now.hour - 1) // 4
            )
            * 4
        )

        current = now.replace(
            hour=start_hour,
            minute=0,
            second=0,
            microsecond=0
        )

        return current.replace(
            tzinfo=None
        )

    # =====================================================
    # 업비트 일봉
    #
    # 09:00
    # =====================================================

    if minutes == 1440:

        today_0900 = now.replace(
            hour=9,
            minute=0,
            second=0,
            microsecond=0
        )

        if now < today_0900:

            today_0900 -= timedelta(
                days=1
            )

        return today_0900.replace(
            tzinfo=None
        )

    # =====================================================
    # 기타 분봉
    # =====================================================

    total_minutes = (
        now.hour * 60
        +
        now.minute
    )

    block = (
        total_minutes // minutes
    ) * minutes

    current = now.replace(
        hour=block // 60,
        minute=block % 60,
        second=0,
        microsecond=0
    )

    return current.replace(
        tzinfo=None
    )


# =========================================================
# API 요청
# =========================================================

def wait_request():

    global last_request_time

    with request_lock:

        gap = (
            time.monotonic()
            -
            last_request_time
        )

        if gap < REQUEST_INTERVAL:

            time.sleep(
                REQUEST_INTERVAL
                -
                gap
            )

        last_request_time = (
            time.monotonic()
        )


def retry(
    func,
    *args,
    **kwargs
):

    url = (
        args[0]
        if (
            args
            and
            isinstance(
                args[0],
                str
            )
        )
        else kwargs.get(
            "url",
            ""
        )
    )

    for n in range(
        MAX_RETRIES
    ):

        try:

            wait_request()

            response = func(
                *args,
                **kwargs
            )

            if not hasattr(
                response,
                "status_code"
            ):
                return response

            if response.status_code == 200:
                return response

            if response.status_code == 429:

                wait = min(
                    RATE_LIMIT_WAIT
                    * 2 ** n,
                    60
                )

            elif response.status_code >= 500:

                wait = min(
                    2 * 2 ** n,
                    30
                )

            else:

                return response

            time.sleep(wait)

        except Exception as e:

            log.error(
                f"[API 오류] {url}: {e}"
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
# UPBIT 마켓
# =========================================================

def get_upbit_markets():

    global latest_upbit_markets

    response = retry(
        requests.get,
        "https://api.upbit.com/v1/market/all",
        params={
            "isDetails": "false"
        },
        timeout=15
    )

    if response is None:
        return []

    try:

        markets = response.json()

        krw_markets = [
            x["market"]
            for x in markets
            if x.get(
                "market",
                ""
            ).startswith("KRW-")
        ]

        ticker_result = []

        for i in range(
            0,
            len(krw_markets),
            100
        ):

            chunk = krw_markets[
                i:i + 100
            ]

            ticker_response = retry(
                requests.get,
                "https://api.upbit.com/v1/ticker",
                params={
                    "markets":
                        ",".join(chunk)
                },
                timeout=15
            )

            if ticker_response is None:
                continue

            try:

                data = (
                    ticker_response.json()
                )

            except Exception:

                continue

            if isinstance(
                data,
                list
            ):

                ticker_result.extend(
                    data
                )

        result = []

        for item in ticker_result:

            market = item.get(
                "market",
                ""
            )

            try:

                volume = float(
                    item.get(
                        "acc_trade_price_24h",
                        0
                    )
                )

                price = float(
                    item.get(
                        "trade_price",
                        0
                    )
                )

            except Exception:

                continue

            if (
                volume > 0
                and
                price > 0
            ):

                result.append({

                    "market":
                        market,

                    "volume_24h":
                        volume,

                    "current_price":
                        price

                })

        latest_upbit_markets = [
            x["market"]
            for x in result
        ]

        return result

    except Exception as e:

        log.error(
            f"업비트 마켓 오류: {e}"
        )

        return []


# =========================================================
# UPBIT 일봉 변동률
#
# 현재 진행 중인 업비트 일봉 기준
#
# 현재가 / 전일 종가
# =========================================================

def daily_change_upbit(
    market,
    current_price=None
):

    response = retry(
        requests.get,
        "https://api.upbit.com/v1/candles/days",
        params={
            "market":
                market,

            "count":
                2
        },
        timeout=15
    )

    if response is None:
        return None

    try:

        data = response.json()

        if not isinstance(
            data,
            list
        ):

            return None

        if len(data) < 2:

            return None

        current_candle = data[0]

        previous_candle = data[1]

        if current_price is None:

            current_price = float(
                current_candle[
                    "trade_price"
                ]
            )

        else:

            current_price = float(
                current_price
            )

        previous_close = float(
            previous_candle[
                "trade_price"
            ]
        )

        if previous_close == 0:

            return None

        return (
            (
                current_price
                -
                previous_close
            )
            /
            previous_close
            *
            100
        )

    except Exception as e:

        log.warning(
            f"업비트 일봉 변동률 오류 "
            f"{market}: {e}"
        )

        return None


# =========================================================
# UPBIT 60분봉 조회
#
# 09:00 / 21:00 기준 12시간 계산용
# =========================================================

def get_upbit_60m_candles(
    market,
    count=30
):

    response = retry(
        requests.get,
        "https://api.upbit.com/v1/candles/minutes/60",
        params={
            "market":
                market,

            "count":
                count
        },
        timeout=15
    )

    if response is None:

        return []

    try:

        data = response.json()

        if not isinstance(
            data,
            list
        ):

            return []

        return data

    except Exception:

        return []


# =========================================================
# UPBIT 12시간 상승률
#
# 시작가격:
# 현재 12시간 구간의
# 09:00 또는 21:00 캔들 opening_price
#
# 현재가격:
# 실시간 ticker 현재가
# =========================================================

def get_upbit_12h_change(
    market,
    current_price=None
):

    candles = (
        get_upbit_60m_candles(
            market,
            count=30
        )
    )

    if not candles:

        return None

    current_start = (
        get_current_12h_start()
    )

    current_start_naive = (
        current_start.replace(
            tzinfo=None
        )
    )

    start_price = None

    for candle in candles:

        try:

            candle_time = datetime.fromisoformat(
                candle[
                    "candle_date_time_kst"
                ]
            )

            if candle_time == current_start_naive:

                start_price = float(
                    candle[
                        "opening_price"
                    ]
                )

                break

        except Exception:

            continue

    # =====================================================
    # 시작 캔들을 찾지 못하는 경우
    #
    # 거래가 없어서 캔들이 생성되지 않는 특수한 경우를
    # 대비하여 가장 가까운 이전 캔들을 사용
    # =====================================================

    if start_price is None:

        candidates = []

        for candle in candles:

            try:

                candle_time = datetime.fromisoformat(
                    candle[
                        "candle_date_time_kst"
                    ]
                )

                if candle_time <= current_start_naive:

                    candidates.append(
                        (
                            candle_time,
                            float(
                                candle[
                                    "opening_price"
                                ]
                            )
                        )
                    )

            except Exception:

                continue

        if candidates:

            candidates.sort(
                key=lambda x:
                    x[0]
            )

            start_price = candidates[-1][1]

    if start_price is None:

        return None

    if current_price is None:

        try:

            current_price = float(
                candles[0][
                    "trade_price"
                ]
            )

        except Exception:

            return None

    else:

        try:

            current_price = float(
                current_price
            )

        except Exception:

            return None

    if start_price == 0:

        return None

    return (
        (
            current_price
            -
            start_price
        )
        /
        start_price
        *
        100
    )


# =========================================================
# UPBIT 일봉 RSI
#
# RSI(14)
#
# TradingView 기본 RSI와 최대한 동일하게 계산
#
# 기준:
# - UPBIT 일봉
# - 09:00 KST 기준
# - Wilder RMA
# - 충분한 과거 데이터 사용
# - 현재 진행 중인 일봉은 실시간 현재가 반영
#
# RSI는 표시용
# Signal 조건에는 사용하지 않음
# =========================================================

def daily_rsi_upbit(
    market,
    period=14,
    current_price=None
):

    history_count = max(
        period * 8,
        100
    )

    response = retry(
        requests.get,
        "https://api.upbit.com/v1/candles/days",
        params={
            "market":
                market,

            "count":
                history_count
        },
        timeout=15
    )

    if response is None:

        return None

    try:

        data = response.json()

        if not isinstance(
            data,
            list
        ):

            return None

        if len(data) < period + 1:

            return None

        data = list(
            reversed(data)
        )

        closes = []

        for item in data:

            closes.append(
                float(
                    item[
                        "trade_price"
                    ]
                )
            )

        if len(closes) < period + 1:

            return None

        if current_price is not None:

            closes[-1] = float(
                current_price
            )

        series = pd.Series(
            closes,
            dtype="float64"
        )

        delta = series.diff()

        gain = delta.clip(
            lower=0
        )

        loss = -delta.clip(
            upper=0
        )

        def wilder_rma(
            source,
            length
        ):

            values = source.to_numpy(
                dtype="float64"
            )

            result = [
                float("nan")
            ] * len(values)

            if len(values) <= length:

                return pd.Series(
                    result,
                    index=source.index
                )

            first_rma = (
                source.iloc[
                    1:length + 1
                ].sum()
                /
                length
            )

            result[length] = float(
                first_rma
            )

            for i in range(
                length + 1,
                len(values)
            ):

                result[i] = (
                    (
                        result[i - 1]
                        *
                        (length - 1)
                    )
                    +
                    values[i]
                ) / length

            return pd.Series(
                result,
                index=source.index
            )

        avg_gain = wilder_rma(
            gain,
            period
        )

        avg_loss = wilder_rma(
            loss,
            period
        )

        last_avg_gain = (
            avg_gain.iloc[-1]
        )

        last_avg_loss = (
            avg_loss.iloc[-1]
        )

        if pd.isna(
            last_avg_gain
        ):

            return None

        if pd.isna(
            last_avg_loss
        ):

            return None

        if last_avg_loss == 0:

            if last_avg_gain == 0:

                return 50.0

            return 100.0

        rs = (
            last_avg_gain
            /
            last_avg_loss
        )

        rsi = (
            100
            -
            (
                100
                /
                (1 + rs)
            )
        )

        if pd.isna(rsi):

            return None

        return float(rsi)

    except Exception as e:

        log.warning(
            f"업비트 RSI 오류 "
            f"{market}: {e}"
        )

        return None


# =========================================================
# BTC OKX 현재가
# =========================================================

def get_okx_btc_price():

    response = retry(
        requests.get,
        f"{OKX_BASE_URL}/api/v5/market/ticker",
        params={
            "instId":
                OKX_BTC_INST_ID
        },
        timeout=15
    )

    if response is None:
        return None

    try:

        data = response.json()

        if data.get("code") != "0":

            return None

        rows = data.get(
            "data",
            []
        )

        if not rows:

            return None

        return float(
            rows[0]["last"]
        )

    except Exception as e:

        log.warning(
            f"OKX BTC 현재가 오류: {e}"
        )

        return None


# =========================================================
# BTC OKX 1시간봉
#
# KST 09:00 / 21:00 기준 계산
# =========================================================

def get_okx_btc_1h_candles(
    limit=300,
    after=None
):

    params = {

        "instId":
            OKX_BTC_INST_ID,

        "bar":
            "1H",

        "limit":
            str(limit)

    }

    if after is not None:

        params["after"] = str(
            after
        )

    response = retry(
        requests.get,
        f"{OKX_BASE_URL}/api/v5/market/candles",
        params=params,
        timeout=15
    )

    if response is None:
        return []

    try:

        payload = response.json()

        if payload.get("code") != "0":

            return []

        return payload.get(
            "data",
            []
        )

    except Exception:

        return []


# =========================================================
# BTC 1시간봉 히스토리
# =========================================================

def get_okx_btc_1h_history():

    rows = []

    after = None

    for _ in range(
        MAX_HISTORY_CHUNKS
    ):

        data = (
            get_okx_btc_1h_candles(
                limit=HISTORY_CHUNK,
                after=after
            )
        )

        if not data:
            break

        rows.extend(data)

        try:

            timestamps = [
                int(x[0])
                for x in data
            ]

            oldest_ts = min(
                timestamps
            )

        except Exception:

            break

        if (
            after is not None
            and
            oldest_ts >= int(after)
        ):

            break

        after = oldest_ts

        if len(data) < HISTORY_CHUNK:

            break

        if len(rows) >= (
            HISTORY_CHUNK
            *
            MAX_HISTORY_CHUNKS
        ):

            break

    if not rows:

        return pd.DataFrame()

    unique = {}

    for row in rows:

        try:

            unique[
                int(row[0])
            ] = row

        except Exception:

            continue

    ordered = sorted(
        unique.values(),
        key=lambda x:
            int(x[0])
    )

    result = []

    for row in ordered:

        try:

            result.append({

                "timestamp":
                    int(row[0]),

                "open":
                    float(row[1]),

                "high":
                    float(row[2]),

                "low":
                    float(row[3]),

                "close":
                    float(row[4])

            })

        except Exception:

            continue

    if not result:

        return pd.DataFrame()

    df = pd.DataFrame(
        result
    )

    df["datetime_utc"] = (
        pd.to_datetime(
            df["timestamp"],
            unit="ms",
            utc=True
        )
    )

    df["datetime_kst"] = (
        df["datetime_utc"]
        .dt
        .tz_convert(KST)
    )

    return df


# =========================================================
# BTC KST 09:00 일봉 생성
# =========================================================

def aggregate_btc_kst_daily(
    df
):

    if (
        df is None
        or
        df.empty
    ):

        return pd.DataFrame()

    temp = df.copy()

    temp["daily_start"] = (
        temp["datetime_kst"]
        -
        pd.Timedelta(
            hours=9
        )
    ).dt.floor("D") + pd.Timedelta(
        hours=9
    )

    daily = (
        temp
        .sort_values(
            "datetime_kst"
        )
        .groupby(
            "daily_start"
        )
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last")
        )
        .reset_index()
    )

    return daily


# =========================================================
# BTC OKX 일봉 변동률
#
# KST 09:00 기준
# =========================================================

def get_okx_btc_daily_change():

    global latest_btc_okx_price

    price = get_okx_btc_price()

    if price is None:

        return None

    latest_btc_okx_price = price

    df = get_okx_btc_1h_history()

    if (
        df is None
        or
        df.empty
    ):

        return None

    daily = (
        aggregate_btc_kst_daily(
            df
        )
    )

    if daily.empty:

        return None

    now = datetime.now(
        KST
    )

    today_0900 = now.replace(
        hour=9,
        minute=0,
        second=0,
        microsecond=0
    )

    if now < today_0900:

        current_start = (
            today_0900
            -
            timedelta(days=1)
        )

    else:

        current_start = today_0900

    daily["daily_start"] = (
        daily["daily_start"]
        .dt
        .tz_localize(None)
    )

    current_start_naive = (
        current_start.replace(
            tzinfo=None
        )
    )

    previous = daily[
        daily["daily_start"]
        <
        current_start_naive
    ]

    if previous.empty:

        return None

    previous_close = float(
        previous.iloc[-1]["close"]
    )

    if previous_close == 0:

        return None

    return (
        (
            price
            -
            previous_close
        )
        /
        previous_close
        *
        100
    )


# =========================================================
# BTC 12시간 상승률
#
# 09:00 ~ 21:00
# 21:00 ~ 09:00
#
# OKX 1시간봉 사용
# =========================================================

def get_okx_btc_12h_changes():

    price = get_okx_btc_price()

    if price is None:

        return None, None

    df = get_okx_btc_1h_history()

    if (
        df is None
        or
        df.empty
    ):

        return None, None

    temp = df.copy()

    temp["datetime_kst_naive"] = (
        temp["datetime_kst"]
        .dt
        .tz_localize(None)
    )

    # =====================================================
    # 현재 시간
    # =====================================================

    now = datetime.now(KST)

    today_0900 = now.replace(
        hour=9,
        minute=0,
        second=0,
        microsecond=0
    )

    today_2100 = now.replace(
        hour=21,
        minute=0,
        second=0,
        microsecond=0
    )

    # =====================================================
    # 오늘 09:00 시작
    # =====================================================

    start_09 = today_0900

    # =====================================================
    # 오늘 21:00 시작
    # =====================================================

    start_21 = today_2100

    # =====================================================
    # 09:00 시작가격
    # =====================================================

    start_09_price = None

    start_09_naive = (
        start_09.replace(
            tzinfo=None
        )
    )

    rows_09 = temp[
        temp["datetime_kst_naive"]
        ==
        start_09_naive
    ]

    if not rows_09.empty:

        start_09_price = float(
            rows_09.iloc[-1]["open"]
        )

    # =====================================================
    # 전날 21:00 시작가격
    # =====================================================

    previous_21 = (
        start_21
        -
        timedelta(days=1)
    )

    previous_21_naive = (
        previous_21.replace(
            tzinfo=None
        )
    )

    start_previous_21_price = None

    rows_previous_21 = temp[
        temp["datetime_kst_naive"]
        ==
        previous_21_naive
    ]

    if not rows_previous_21.empty:

        start_previous_21_price = float(
            rows_previous_21.iloc[-1]["open"]
        )

    # =====================================================
    # 오늘 21:00 가격
    #
    # 21시가 아직 시작되지 않았다면
    # 현재 21시 구간의 상승률은 계산하지 않음
    # =====================================================

    start_21_price = None

    start_21_naive = (
        start_21.replace(
            tzinfo=None
        )
    )

    rows_21 = temp[
        temp["datetime_kst_naive"]
        ==
        start_21_naive
    ]

    if not rows_21.empty:

        start_21_price = float(
            rows_21.iloc[-1]["open"]
        )

    # =====================================================
    # 09~21 상승률
    #
    # 현재가가 21시 이전이면
    # 현재가 기준으로 계산
    #
    # 21시 이후에는
    # 21시 시작가격 기준으로
    # 완료된 09~21 구간의 상승률 표시
    # =====================================================

    if now.hour < 21:

        if start_09_price:

            change_09_21 = (
                (
                    price
                    -
                    start_09_price
                )
                /
                start_09_price
                *
                100
            )

        else:

            change_09_21 = None

    else:

        # 21시 이후에는 21시 직전 캔들의 종가 사용
        rows_before_21 = temp[
            temp["datetime_kst_naive"]
            <
            start_21_naive
        ]

        if (
            start_09_price
            and
            not rows_before_21.empty
        ):

            close_20 = float(
                rows_before_21.iloc[-1]["close"]
            )

            change_09_21 = (
                (
                    close_20
                    -
                    start_09_price
                )
                /
                start_09_price
                *
                100
            )

        else:

            change_09_21 = None

    # =====================================================
    # 21~09 상승률
    # =====================================================

    if now.hour >= 21:

        if start_21_price:

            change_21_09 = (
                (
                    price
                    -
                    start_21_price
                )
                /
                start_21_price
                *
                100
            )

        else:

            change_21_09 = None

    else:

        # 현재가가 09~21 시간대라면
        # 전날 21시부터 오늘 09시까지 완료된
        # 12시간 상승률
        #
        # 09시 시작 직전 가격 사용
        rows_before_09 = temp[
            temp["datetime_kst_naive"]
            <
            start_09_naive
        ]

        if (
            start_previous_21_price
            and
            not rows_before_09.empty
        ):

            close_previous_08 = float(
                rows_before_09.iloc[-1]["close"]
            )

            change_21_09 = (
                (
                    close_previous_08
                    -
                    start_previous_21_price
                )
                /
                start_previous_21_price
                *
                100
            )

        else:

            change_21_09 = None

    return (
        change_09_21,
        change_21_09
    )


# =========================================================
# BTC 시황 업데이트
# =========================================================

def update_btc_market():

    global latest_btc_okx_price

    global latest_btc_daily_change

    global latest_btc_09_21_change

    global latest_btc_21_09_change

    global latest_btc_current_12h_change

    global latest_btc_current_12h_label

    # =====================================================
    # 현재가
    # =====================================================

    price = get_okx_btc_price()

    if price is not None:

        latest_btc_okx_price = price

    # =====================================================
    # 당일 상승률
    # =====================================================

    daily_change = (
        get_okx_btc_daily_change()
    )

    if daily_change is not None:

        latest_btc_daily_change = (
            daily_change
        )

    # =====================================================
    # 12시간 상승률
    # =====================================================

    change_09_21, change_21_09 = (
        get_okx_btc_12h_changes()
    )

    latest_btc_09_21_change = (
        change_09_21
    )

    latest_btc_21_09_change = (
        change_21_09
    )

    # =====================================================
    # 현재 시간대 필터
    # =====================================================

    period = (
        get_current_12h_period()
    )

    latest_btc_current_12h_label = (
        period["label"]
    )

    if period["start_hour"] == 9:

        latest_btc_current_12h_change = (
            change_09_21
        )

    else:

        latest_btc_current_12h_change = (
            change_21_09
        )

    log.info(
        f"[BTC OKX] "
        f"가격={latest_btc_okx_price} | "
        f"당일={latest_btc_daily_change} | "
        f"09~21={latest_btc_09_21_change} | "
        f"21~09={latest_btc_21_09_change} | "
        f"현재필터={latest_btc_current_12h_label} "
        f"{latest_btc_current_12h_change}"
    )


# =========================================================
# 코인 분석
#
# 업비트 일봉 상승률
# 업비트 12시간 상승률
# 업비트 일봉 RSI(14)
#
# RSI는 표시용
# =========================================================

def analyze(
    market,
    current_price
):

    change_value = (
        daily_change_upbit(
            market,
            current_price
        )
    )

    change_12h = (
        get_upbit_12h_change(
            market,
            current_price
        )
    )

    rsi_value = (
        daily_rsi_upbit(
            market,
            period=14,
            current_price=current_price
        )
    )

    daily_pass = (
        change_value is not None
        and
        change_value > 0
    )

    # =====================================================
    # 현재 시간대 12시간 Signal
    # =====================================================

    period = (
        get_current_12h_period()
    )

    signal_12h_pass = (
        change_12h is not None
        and
        change_12h > 0
    )

    return {

        "changes":
            change_value,

        "change_12h":
            change_12h,

        "daily_pass":
            daily_pass,

        "signal_12h_pass":
            signal_12h_pass,

        "rsi":
            rsi_value

    }


# =========================================================
# ROW
# =========================================================

def make_row(
    rank,
    name,
    volume,
    analysis,
    current_price=None
):

    a = analysis or {}

    change_value = (
        get_change_value(
            a.get(
                "changes"
            )
        )
    )

    change_12h_value = (
        get_change_value(
            a.get(
                "change_12h"
            )
        )
    )

    return {

        "rank":
            rank,

        "name":
            name,

        "change":
            format_change(
                change_value
            ),

        "change_value":
            change_value,

        "change_12h":
            format_change(
                change_12h_value
            ),

        "change_12h_value":
            change_12h_value,

        "daily_pass":
            bool(
                a.get(
                    "daily_pass",
                    False
                )
            ),

        "signal_12h_pass":
            bool(
                a.get(
                    "signal_12h_pass",
                    False
                )
            ),

        "rsi":
            a.get(
                "rsi"
            ),

        "volume":
            format_volume(
                volume
            ),

        "current_price":
            current_price,

        "analysis":
            analysis

    }


# =========================================================
# UPBIT TOP 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data

    global latest_upbit_update_time

    markets = sorted(
        get_upbit_markets(),
        key=lambda x:
            x["volume_24h"],
        reverse=True
    )

    top_markets = markets[
        :TOP_N
    ]

    if not top_markets:

        latest_upbit_data = []

        latest_upbit_update_time = (
            kst()
        )

        return

    rows = []

    for rank, item in enumerate(
        top_markets,
        1
    ):

        market = item[
            "market"
        ]

        coin = market.replace(
            "KRW-",
            ""
        )

        price = item[
            "current_price"
        ]

        try:

            analysis = analyze(
                market,
                price
            )

        except Exception as e:

            log.exception(
                f"분석 오류 {market}: {e}"
            )

            analysis = None

        row = make_row(

            rank,

            coin,

            item[
                "volume_24h"
            ],

            analysis,

            price

        )

        # =================================================
        # Signal
        #
        # BTC 현재 시간대 12시간 상승률 > 0
        #
        # AND
        #
        # 코인 현재 시간대 12시간 상승률 > 0
        # =================================================

        btc_positive = (
            latest_btc_current_12h_change
            is not None
            and
            latest_btc_current_12h_change > 0
        )

        coin_positive = (
            row.get(
                "change_12h_value"
            ) is not None
            and
            row.get(
                "change_12h_value"
            ) > 0
        )

        row[
            "signal_pass"
        ] = (
            btc_positive
            and
            coin_positive
        )

        rows.append(
            row
        )

    latest_upbit_data = rows

    latest_upbit_update_time = (
        kst()
    )

    signal_rows = [

        x

        for x in rows

        if x.get(
            "signal_pass",
            False
        )

    ]

    signal_rows.sort(
        key=lambda x:
            x["change_12h_value"],
        reverse=True
    )

    signal_text = ", ".join(

        f"{x['name']} "
        f"{x['change_12h_value']:+.2f}%"

        for x in signal_rows[:10]

    )

    if latest_btc_current_12h_change is None:

        btc_status = "확인불가"

    elif latest_btc_current_12h_change > 0:

        btc_status = (
            f"양수 "
            f"{latest_btc_current_12h_change:+.2f}%"
        )

    else:

        btc_status = (
            f"음수/0 "
            f"{latest_btc_current_12h_change:+.2f}%"
        )

    log.info(
        f"TOP{TOP_N} 업데이트 완료 | "
        f"현재구간={latest_btc_current_12h_label} | "
        f"BTC={btc_status} | "
        f"Signal 12H 상승률순 = "
        f"{signal_text if signal_text else '없음'}"
    )


# =========================================================
# USDT
# =========================================================

def get_usdt_krw_internal():

    response = retry(
        requests.get,
        "https://api.upbit.com/v1/ticker",
        params={
            "markets":
                "KRW-USDT"
        },
        timeout=15
    )

    if response is None:

        return None

    try:

        data = response.json()

        if not data:

            return None

        return float(
            data[0]["trade_price"]
        )

    except Exception:

        return None


# =========================================================
# OKX 기존 영역
# =========================================================

def update_okx(
    usdt
):

    global latest_okx_data

    global latest_okx_update_time

    latest_okx_data = []

    latest_okx_update_time = (
        kst()
    )

    return True


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    if not update_lock.acquire(
        False
    ):

        return

    try:

        # =================================================
        # BTC 먼저
        # =================================================

        update_btc_market()

        # =================================================
        # UPBIT
        # =================================================

        if USE_UPBIT == "Y":

            update_upbit()

        # =================================================
        # OKX
        # =================================================

        if USE_OKX == "Y":

            usdt = (
                get_usdt_krw_internal()
            )

            if usdt:

                update_okx(
                    usdt
                )

    except Exception as e:

        log.exception(
            f"전체 업데이트 오류: {e}"
        )

    finally:

        update_lock.release()


# =========================================================
# 가격
# =========================================================

def format_market_price(price):

    if price is None:

        return "-"

    try:

        price = float(
            price
        )

    except Exception:

        return "-"

    if price >= 100000000:

        return (
            f"{price / 100000000:.2f}억"
        )

    if price >= 10000:

        return (
            f"{price:,.0f}"
        )

    if price >= 1:

        return (
            f"{price:,.2f}"
        )

    return (
        f"{price:.6f}"
    )


# =========================================================
# 변화값
# =========================================================

def get_change_value(x):

    try:

        if x is None:

            return None

        if isinstance(
            x,
            (list, tuple)
        ):

            if not x:

                return None

            return float(
                x[0]
            )

        return float(x)

    except Exception:

        return None


def format_change(x):

    x = get_change_value(x)

    if x is None:

        return "-"

    if x > 0:

        return (
            '<span class="up">'
            f'▲ +{x:.1f}%'
            '</span>'
        )

    if x < 0:

        return (
            '<span class="down">'
            f'▼ {x:.1f}%'
            '</span>'
        )

    return (
        '<span class="zero">'
        '0.0%'
        '</span>'
    )


# =========================================================
# 거래대금
# =========================================================

def format_volume(v):

    try:

        v = float(v)

    except Exception:

        return "-"

    if v >= 1e12:

        return (
            f"{v / 1e12:.1f}조"
        )

    if v >= 1e8:

        return (
            f"{v / 1e8:.0f}억"
        )

    if v >= 1e4:

        return (
            f"{v / 1e4:.0f}만"
        )

    return (
        f"{v:,.0f}"
    )


# =========================================================
# Signal HTML
# =========================================================

def signal_item_html(
    row
):

    if not row:

        return "-"

    change_value = (
        get_change_value(
            row.get(
                "change_12h_value"
            )
        )
    )

    if (
        change_value is None
        or
        change_value <= 0
    ):

        return "-"

    signal_rank = row.get(
        "signal_rank",
        "-"
    )

    top_rank = row.get(
        "rank",
        "-"
    )

    coin = html.escape(
        str(
            row.get(
                "name",
                "-"
            )
        )
    )

    volume = row.get(
        "volume",
        "-"
    )

    price = format_market_price(
        row.get(
            "current_price"
        )
    )

    return f"""

    <div class="signal-row">

        <div class="signal-rank-number">

            #{signal_rank}

        </div>

        <div class="signal-top-rank">

            TOP {top_rank}

        </div>

        <div class="signal-coin">

            {coin}

        </div>

        <div class="signal-volume">

            {volume}

        </div>

        <div class="signal-price">

            {price}

        </div>

        <div class="signal-change">

            ▲ +{change_value:.1f}%

        </div>

        <div class="signal-icon">

            🚀

        </div>

    </div>

    """


# =========================================================
# Signal 전체
# =========================================================

def focus_section(data):

    btc_positive = (
        latest_btc_current_12h_change
        is not None
        and
        latest_btc_current_12h_change > 0
    )

    current_period = (
        latest_btc_current_12h_label
    )

    if not btc_positive:

        if latest_btc_current_12h_change is None:

            btc_status = (
                f"BTC {current_period} 상승률 확인 불가"
            )

        else:

            btc_status = (
                f"BTC {current_period} "
                f"{latest_btc_current_12h_change:+.2f}%"
            )

        signal_table = f"""

        <div class="signal-empty">

            {btc_status}

            <br>

            현재 시간대 BTC 12시간 상승률이
            0% 초과일 때만 Signal 통과

        </div>

        """

        return f"""

        <div class="unified-section">

            <div class="section-title-card">

                <div class="section-number">

                    🚀

                </div>

                <div class="section-heading">

                    <div class="section-heading-main">

                        Signal

                    </div>

                    <div class="section-heading-sub">

                        BTC {current_period}
                        양수 필터 OFF

                    </div>

                </div>

                <div class="section-time">

                    {kst()} KST

                </div>

            </div>

            {signal_table}

        </div>

        """

    signal_rows = [

        x.copy()

        for x in data

        if (
            x.get(
                "change_12h_value"
            ) is not None

            and

            x.get(
                "change_12h_value"
            ) > 0
        )

    ]

    signal_rows.sort(
        key=lambda x:
            x["change_12h_value"],
        reverse=True
    )

    for signal_rank, row in enumerate(
        signal_rows,
        1
    ):

        row[
            "signal_rank"
        ] = signal_rank

    if not signal_rows:

        signal_table = f"""

        <div class="signal-empty">

            BTC는 {current_period} 양수지만

            <br>

            TOP{TOP_N} 중 현재 12시간 상승률
            0% 초과 종목 없음

        </div>

        """

    else:

        signal_items = []

        for row in signal_rows:

            signal_items.append(
                signal_item_html(
                    row
                )
            )

        signal_table = f"""

        <div class="signal-header">

            <div>Signal</div>

            <div>TOP</div>

            <div>코인</div>

            <div>거래대금</div>

            <div>현재가</div>

            <div>12H 상승률</div>

            <div></div>

        </div>

        <div class="signal-list">

            {"".join(signal_items)}

        </div>

        """

    return f"""

    <div class="unified-section">

        <div class="section-title-card">

            <div class="section-number">

                🚀

            </div>

            <div class="section-heading">

                <div class="section-heading-main">

                    Signal

                </div>

                <div class="section-heading-sub">

                    BTC {current_period} 양수

                    ·

                    업비트 거래대금 TOP{TOP_N}

                    ·

                    현재 12시간 상승률 높은 순

                </div>

            </div>

            <div class="section-time">

                BTC {latest_btc_current_12h_change:+.2f}%

            </div>

        </div>

        {signal_table}

    </div>

    """


# =========================================================
# TOP
# =========================================================

def section(
    data,
    update_time
):

    current_period = (
        latest_btc_current_12h_label
    )

    return f"""

    <div class="unified-section">

        <div class="section-title-card">

            <div class="section-number top-number">

                🏆

            </div>

            <div class="section-heading">

                <div class="section-heading-main">

                    업비트 TOP{TOP_N}

                </div>

                <div class="section-heading-sub">

                    24시간 거래대금 순위

                    ·

                    일봉 상승률 표시

                    ·

                    현재 {current_period} 12H 상승률로 Signal 필터

                    ·

                    RSI(14) 표시

                </div>

            </div>

            <div class="section-time">

                {update_time} KST

            </div>

        </div>

        {table_html(data)}

    </div>

    """


# =========================================================
# 시장 요약
# =========================================================

def market_summary_html():

    price = format_market_price(
        latest_btc_okx_price
    )

    # =====================================================
    # 당일
    # =====================================================

    if latest_btc_daily_change is not None:

        daily_change = format_change(
            latest_btc_daily_change
        )

    else:

        daily_change = "-"

    # =====================================================
    # 09~21
    # =====================================================

    if latest_btc_09_21_change is not None:

        change_09_21 = format_change(
            latest_btc_09_21_change
        )

    else:

        change_09_21 = "-"

    # =====================================================
    # 21~09
    # =====================================================

    if latest_btc_21_09_change is not None:

        change_21_09 = format_change(
            latest_btc_21_09_change
        )

    else:

        change_21_09 = "-"

    # =====================================================
    # 현재 필터
    # =====================================================

    if latest_btc_current_12h_change is None:

        signal_status = "OFF"

        signal_status_class = "btc-off"

    elif latest_btc_current_12h_change > 0:

        signal_status = "ON"

        signal_status_class = "btc-on"

    else:

        signal_status = "OFF"

        signal_status_class = "btc-off"

    return f"""

    <div class="market-card">

        <div class="market-card-header">

            <div class="market-title-block">

                <div class="market-title-main">

                    ₿ BTC 시장 시황

                </div>

                <div class="market-title-sub">

                    데이터:
                    OKX BTC-USDT

                    ·

                    당일 = KST 09:00 기준

                    ·

                    Signal = 현재 12시간 구간 양수

                </div>

            </div>

            <div class="market-time">

                {kst()} KST

            </div>

        </div>


        <!-- ============================================
             BTC 현재가 / 당일
             ============================================ -->

        <div class="btc-main-row">

            <div class="btc-name">

                ₿ BTC

            </div>

            <div class="btc-price">

                {price}

            </div>

            <div class="btc-change">

                {daily_change}

            </div>

            <div class="btc-signal-box">

                <span class="btc-info">

                    SIGNAL

                </span>

                <span class="{signal_status_class}">

                    {signal_status}

                </span>

            </div>

        </div>


        <!-- ============================================
             12시간
             ============================================ -->

        <div class="btc-12h-row">

            <div class="btc-12h-item">

                <div class="btc-12h-label">

                    09:00 ~ 21:00

                </div>

                <div class="btc-12h-value">

                    {change_09_21}

                </div>

            </div>


            <div class="btc-12h-divider"></div>


            <div class="btc-12h-item">

                <div class="btc-12h-label">

                    21:00 ~ 09:00

                </div>

                <div class="btc-12h-value">

                    {change_21_09}

                </div>

            </div>


            <div class="btc-current-filter">

                <div class="btc-filter-label">

                    현재 필터

                </div>

                <div class="btc-filter-period">

                    {latest_btc_current_12h_label}

                </div>

                <div class="btc-filter-value">

                    {
                        format_change(
                            latest_btc_current_12h_change
                        )
                    }

                </div>

            </div>

        </div>

    </div>

    """


# =========================================================
# ROW HTML
#
# 마지막 칸:
# RSI(14)
# =========================================================

def rows_html(data):

    out = []

    for x in data:

        price = format_market_price(
            x.get(
                "current_price"
            )
        )

        change = x.get(
            "change",
            "-"
        )

        coin_name = html.escape(
            str(
                x.get(
                    "name",
                    "-"
                )
            )
        )

        volume = x.get(
            "volume",
            "-"
        )

        # =================================================
        # RSI
        # =================================================

        rsi_value = x.get(
            "rsi"
        )

        try:

            rsi_value = (
                float(rsi_value)
                if rsi_value is not None
                else None
            )

        except (
            TypeError,
            ValueError
        ):

            rsi_value = None

        if rsi_value is None:

            rsi_html = "-"

        else:

            if rsi_value <= 30:

                rsi_class = (
                    "rsi-blue"
                )

            elif (
                40 <= rsi_value <= 60
            ):

                rsi_class = (
                    "rsi-green"
                )

            elif rsi_value >= 70:

                rsi_class = (
                    "rsi-red"
                )

            else:

                rsi_class = (
                    "rsi-normal"
                )

            rsi_html = (

                f'<span class="{rsi_class}">'
                f'RSI {rsi_value:.1f}'
                '</span>'

            )

        out.append(

            f"""

            <div class="coin-card">

                <div class="coin-main-row">

                    <div class="rank-cell">

                        {x.get("rank", "-")}

                    </div>

                    <div class="coin-cell">

                        <span class="coin-name">

                            {coin_name}

                        </span>

                    </div>

                    <div class="volume-cell">

                        <span class="volume-value">

                            {volume}

                        </span>

                    </div>

                    <div class="price-cell">

                        <span class="price-value">

                            {price}

                        </span>

                    </div>

                    <div class="change-cell">

                        {change}

                    </div>

                    <div class="signal-cell">

                        {rsi_html}

                    </div>

                </div>

            </div>

            """

        )

    return "".join(
        out
    )


# =========================================================
# 테이블
# =========================================================

def table_html(data):

    rows = rows_html(
        data
    )

    if not rows:

        rows = """

        <div class="empty-card">

            현재 데이터 없음

        </div>

        """

    return f"""

    <div class="card-list">

        {rows}

    </div>

    """


# =========================================================
# CSS
# =========================================================

CSS = """

*{
box-sizing:border-box;
-webkit-tap-highlight-color:transparent;
}

html,
body{
margin:0;
padding:0;
width:100%;
overflow-x:hidden;
}

body{
background:#080c11;
color:#e7ebef;

font-family:
    -apple-system,
    BlinkMacSystemFont,
    "Segoe UI",
    Arial,
    sans-serif;

font-size:9px;
padding:12px;
}

h1{
margin:3px 4px 10px;
color:#eef2f5;
font-size:15px;
line-height:18px;
font-weight:900;
}

.unified-section{
width:100%;
margin:10px 0 12px;
}

.section-title-card{
display:flex;
align-items:center;
width:100%;
min-height:48px;
padding:7px 10px;
background:#10151b;
border:2px solid #252e38;
border-radius:12px;
box-shadow:
inset 0 0 18px
rgba(255,255,255,.018);
overflow:hidden;
}

.section-number{
flex:none;
display:flex;
align-items:center;
justify-content:center;
width:34px;
height:34px;
margin-right:9px;
border-radius:8px;
background:#18251f;
border:1px solid #315a48;
color:#82d5a8;
font-size:16px;
font-weight:900;
}

.top-number{
background:#1d1a13;
border-color:#665331;
color:#e0bd6d;
font-size:14px;
}

.section-heading{
min-width:0;
flex:1;
overflow:hidden;
}

.section-heading-main{
color:#e9edf1;
font-size:12px;
line-height:15px;
font-weight:900;
white-space:nowrap;
}

.section-heading-sub{
margin-top:2px;
color:#87919b;
font-size:7px;
line-height:10px;
font-weight:700;
white-space:nowrap;
overflow:hidden;
text-overflow:ellipsis;
}

.section-time{
flex:none;
margin-left:8px;
color:#68737e;
font-size:6.5px;
font-weight:800;
white-space:nowrap;
}


/* =========================================================
   BTC
   ========================================================= */

.market-card{
width:100%;
margin:3px 0 12px;
background:#0f141a;
border:2px solid #252e38;
border-radius:13px;
overflow:hidden;
box-shadow:
inset 0 0 20px
rgba(255,255,255,.018);
}

.market-card-header{
display:flex;
align-items:center;
min-height:44px;
padding:7px 10px;
background:#121820;
border-bottom:1px solid #29323c;
}

.market-title-block{
min-width:0;
flex:1;
overflow:hidden;
}

.market-title-main{
color:#eef2f5;
font-size:11px;
line-height:14px;
font-weight:900;
white-space:nowrap;
}

.market-title-sub{
margin-top:2px;
color:#7e8994;
font-size:6.5px;
line-height:9px;
font-weight:700;
white-space:nowrap;
overflow:hidden;
text-overflow:ellipsis;
}

.market-time{
flex:none;
margin-left:8px;
color:#68737e;
font-size:6.5px;
font-weight:800;
white-space:nowrap;
}

.btc-main-row{
display:grid;

grid-template-columns:
    1.1fr
    1.3fr
    1fr
    1.4fr;

align-items:center;
min-height:58px;
background:#11161c;
}

.btc-name{
padding-left:13px;
color:#edf1f4;
font-size:11px;
font-weight:900;
white-space:nowrap;
}

.btc-price{
color:#f1f4f6;
font-size:11px;
font-weight:900;
text-align:center;
white-space:nowrap;
}

.btc-change{
font-size:11px;
font-weight:900;
text-align:center;
white-space:nowrap;
}

.btc-signal-box{
min-height:58px;
display:flex;
align-items:center;
justify-content:center;
gap:5px;
border-left:1px solid #29323c;
}

.btc-info{
color:#7f8a94;
font-size:8px;
font-weight:900;
}

.btc-on{
color:#78cfa2;
font-size:10px;
font-weight:900;
}

.btc-off{
color:#df8588;
font-size:10px;
font-weight:900;
}


/* =========================================================
   BTC 12H
   ========================================================= */

.btc-12h-row{

display:grid;

grid-template-columns:
    1fr
    1px
    1fr
    1.25fr;

align-items:center;

min-height:66px;

background:#0e141a;

border-top:1px solid #29323c;
}

.btc-12h-item{

display:flex;

flex-direction:column;

align-items:center;

justify-content:center;

gap:4px;
}

.btc-12h-label{

color:#7d8893;

font-size:7px;

font-weight:800;

white-space:nowrap;
}

.btc-12h-value{

font-size:10px;

font-weight:900;

white-space:nowrap;
}

.btc-12h-divider{

height:36px;

background:#29323c;
}

.btc-current-filter{

display:flex;

flex-direction:column;

align-items:center;

justify-content:center;

gap:2px;

height:66px;

border-left:1px solid #29323c;

background:#111820;
}

.btc-filter-label{

color:#6f7a85;

font-size:6px;

font-weight:800;
}

.btc-filter-period{

color:#e3e8ec;

font-size:7px;

font-weight:900;

white-space:nowrap;
}

.btc-filter-value{

font-size:9px;

font-weight:900;

white-space:nowrap;
}


/* =========================================================
   Signal
   ========================================================= */

.signal-header,
.signal-row{

display:grid;

grid-template-columns:
    9%
    9%
    18%
    18%
    17%
    17%
    12%;

align-items:center;
}

.signal-header{

margin-top:8px;

min-height:32px;

background:#10151b;

border:1px solid #252e38;

border-radius:8px 8px 0 0;

color:#68737e;

font-size:7px;

font-weight:800;

text-align:center;
}

.signal-row{

min-height:48px;

background:#10171d;

border-left:1px solid #26333c;

border-right:1px solid #26333c;

border-bottom:1px solid #26333c;

font-size:9px;

font-weight:800;

text-align:center;
}

.signal-row:last-child{

border-radius:0 0 8px 8px;
}

.signal-rank-number{

color:#e0bd6d;

font-size:10px;

font-weight:900;
}

.signal-top-rank{

color:#78838d;

font-size:8px;

font-weight:800;
}

.signal-coin{

color:#eef2f5;

font-size:10px;

font-weight:900;

white-space:nowrap;

overflow:hidden;

text-overflow:ellipsis;
}

.signal-volume{

color:#f1f4f6;

font-size:9px;

font-weight:900;

white-space:nowrap;
}

.signal-price{

color:#eef2f5;

font-size:9px;

font-weight:900;

white-space:nowrap;
}

.signal-change{

color:#78cfa2;

font-size:10px;

font-weight:900;

white-space:nowrap;
}

.signal-icon{

font-size:14px;
}

.signal-list{

width:100%;
}

.signal-empty{

min-height:52px;

display:flex;

align-items:center;

justify-content:center;

text-align:center;

background:#10151b;

border:2px solid #252e38;

border-radius:10px;

color:#59636e;

font-size:8px;

line-height:13px;

font-weight:800;

margin-top:8px;
}


/* =========================================================
   Coin
   ========================================================= */

.card-list{
width:100%;
display:flex;
flex-direction:column;
gap:9px;
margin-top:8px;
}

.coin-card{
width:100%;
background:#0f141a;
border:2px solid #252e38;
border-radius:12px;
overflow:hidden;
box-shadow:
inset 0 0 18px
rgba(255,255,255,.015);
}

.coin-main-row{
display:grid;

grid-template-columns:
    6%
    18%
    15%
    21%
    14%
    26%;

align-items:center;
min-height:56px;
background:#11161c;
}

.coin-main-row > div{
min-width:0;
height:56px;
display:flex;
align-items:center;
justify-content:center;
overflow:hidden;
}

.rank-cell{
justify-content:flex-start!important;
padding-left:12px;
color:#e2e7eb;
font-size:11px;
font-weight:900;
white-space:nowrap;
}

.coin-cell{
text-align:center;
}

.coin-name{
display:block;
width:100%;
padding:0 3px;
color:#eef2f5;
font-size:11px;
line-height:14px;
font-weight:900;
white-space:nowrap;
overflow:hidden;
text-overflow:ellipsis;
text-align:center;
}

.volume-cell{
text-align:center;
}

.volume-value{
display:block;
width:100%;
color:#f1f4f6;
font-size:10px;
line-height:13px;
font-weight:900;
white-space:nowrap;
overflow:hidden;
text-overflow:ellipsis;
text-align:center;
}

.price-cell{
text-align:center;
}

.price-value{
display:block;
width:100%;
color:#eef2f5;
font-size:10px;
line-height:13px;
font-weight:900;
white-space:nowrap;
overflow:hidden;
text-overflow:ellipsis;
text-align:center;
}

.change-cell{
text-align:center;
font-size:10px;
font-weight:900;
white-space:nowrap;
}

.signal-cell{
height:56px!important;
border-left:1px solid #29323c;
background:#0e141a;
text-align:center;
overflow:hidden!important;
color:#e0bd6d;
font-size:14px;
font-weight:900;
}


/* =========================================================
   RSI
   ========================================================= */

.rsi-normal,
.rsi-blue,
.rsi-green,
.rsi-red{

font-size:10px;

font-weight:900;

white-space:nowrap;
}

.rsi-blue{

color:#459dff;

text-shadow:
    0 0 6px
    rgba(69,157,255,.35);
}

.rsi-green{

color:#78cfa2;

text-shadow:
    0 0 6px
    rgba(120,207,162,.35);
}

.rsi-red{

color:#ff6b6b;

text-shadow:
    0 0 6px
    rgba(255,80,80,.35);
}

.rsi-normal{

color:#8d98a3;
}


/* =========================================================
   색상
   ========================================================= */

.up{
color:#78cfa2!important;
font-weight:900;
}

.down{
color:#df8588!important;
font-weight:900;
}

.zero{
color:#727c86!important;
}

.empty-card{
min-height:56px;
display:flex;
align-items:center;
justify-content:center;
background:#10151b;
border:2px solid #252e38;
border-radius:12px;
color:#59636e;
font-size:8px;
font-weight:800;
}


/* =========================================================
   모바일
   ========================================================= */

@media(max-width:600px){

body{
    padding:7px;
}

h1{
    margin:3px 3px 8px;
    font-size:13px;
    line-height:16px;
}

.unified-section{
    margin:8px 0 10px;
}

.section-title-card{
    min-height:40px;
    padding:5px 6px;
    border-radius:9px;
}

.section-number{
    width:27px;
    height:27px;
    margin-right:6px;
    border-radius:6px;
    font-size:12px;
}

.top-number{
    font-size:11px;
}

.section-heading-main{
    font-size:9px;
    line-height:11px;
}

.section-heading-sub{
    margin-top:1px;
    font-size:5px;
    line-height:7px;
}

.section-time{
    margin-left:4px;
    font-size:5px;
}


/* BTC */

.market-card{
    margin:3px 0 9px;
    border-radius:9px;
}

.market-card-header{
    min-height:36px;
    padding:5px 7px;
}

.market-title-main{
    font-size:8px;
    line-height:10px;
}

.market-title-sub{
    margin-top:1px;
    font-size:4.8px;
    line-height:6px;
}

.market-time{
    margin-left:4px;
    font-size:4.8px;
}

.btc-main-row{
    min-height:43px;

    grid-template-columns:
        1.1fr
        1.3fr
        1fr
        1.3fr;
}

.btc-name{
    padding-left:8px;
    font-size:8px;
}

.btc-price{
    font-size:8px;
}

.btc-change{
    font-size:8px;
}

.btc-signal-box{
    min-height:43px;
}

.btc-info{
    font-size:6px;
}

.btc-on,
.btc-off{
    font-size:7px;
}


/* BTC 12H */

.btc-12h-row{

    min-height:49px;

    grid-template-columns:
        1fr
        1px
        1fr
        1.25fr;
}

.btc-12h-item{
    gap:2px;
}

.btc-12h-label{
    font-size:5px;
}

.btc-12h-value{
    font-size:7px;
}

.btc-12h-divider{
    height:28px;
}

.btc-current-filter{
    height:49px;
    gap:1px;
}

.btc-filter-label{
    font-size:4.5px;
}

.btc-filter-period{
    font-size:5px;
}

.btc-filter-value{
    font-size:6px;
}


/* Signal */

.signal-header,
.signal-row{

    grid-template-columns:
        9%
        9%
        18%
        18%
        17%
        17%
        12%;
}

.signal-header{

    min-height:25px;

    font-size:5px;

    border-radius:6px 6px 0 0;
}

.signal-row{

    min-height:40px;

    font-size:6px;
}

.signal-rank-number{
    font-size:7px;
}

.signal-top-rank{
    font-size:5.5px;
}

.signal-coin{
    font-size:6.5px;
}

.signal-volume{
    font-size:5.5px;
}

.signal-price{
    font-size:5.5px;
}

.signal-change{
    font-size:6px;
}

.signal-icon{
    font-size:10px;
}

.signal-empty{
    min-height:43px;
    border-width:1px;
    border-radius:8px;
    font-size:6px;
    line-height:10px;
}


/* Coin */

.card-list{
    gap:6px;
    margin-top:6px;
}

.coin-card{
    border-width:1px;
    border-radius:8px;
}

.coin-main-row{

    min-height:40px;

    grid-template-columns:
        6%
        18%
        15%
        21%
        14%
        26%;
}

.coin-main-row > div{
    height:40px;
}

.rank-cell{
    padding-left:5px;
    font-size:6.8px;
}

.coin-name{
    padding:0 1px;
    font-size:6.8px;
    line-height:9px;
}

.volume-value{
    font-size:5.5px;
    line-height:8px;
}

.price-value{
    font-size:5.8px;
    line-height:8px;
}

.change-cell{
    font-size:5.8px;
}

.signal-cell{
    height:40px!important;
    border-left:1px solid #29323c;
    font-size:10px;
}


/* RSI */

.rsi-normal,
.rsi-blue,
.rsi-green,
.rsi-red{
    font-size:6px;
}

.empty-card{
    min-height:43px;
    border-width:1px;
    border-radius:8px;
    font-size:6px;
}

}


@media(max-width:380px){

body{
    padding:4px;
}

h1{
    font-size:12px;
    line-height:15px;
    margin:2px 2px 6px;
}

.section-title-card{
    min-height:35px;
    padding:4px 5px;
}

.section-number{
    width:23px;
    height:23px;
    margin-right:4px;
    font-size:10px;
}

.section-heading-main{
    font-size:8px;
    line-height:10px;
}

.section-heading-sub{
    font-size:4.2px;
    line-height:6px;
}

.section-time{
    font-size:4.2px;
}


/* BTC */

.market-card-header{
    min-height:32px;
    padding:4px 5px;
}

.market-title-main{
    font-size:7px;
}

.market-title-sub{
    font-size:4px;
}

.market-time{
    font-size:4px;
}

.btc-main-row{
    min-height:38px;
}

.btc-name{
    padding-left:6px;
    font-size:7px;
}

.btc-price{
    font-size:7px;
}

.btc-change{
    font-size:7px;
}

.btc-signal-box{
    min-height:38px;
}

.btc-info{
    font-size:5px;
}

.btc-on,
.btc-off{
    font-size:6px;
}


/* BTC 12H */

.btc-12h-row{
    min-height:43px;
}

.btc-12h-label{
    font-size:4px;
}

.btc-12h-value{
    font-size:5.8px;
}

.btc-12h-divider{
    height:25px;
}

.btc-current-filter{
    height:43px;
}

.btc-filter-label{
    font-size:3.8px;
}

.btc-filter-period{
    font-size:4.3px;
}

.btc-filter-value{
    font-size:5.2px;
}


/* Signal */

.signal-header,
.signal-row{

    grid-template-columns:
        9%
        9%
        18%
        18%
        17%
        17%
        12%;
}

.signal-header{

    min-height:22px;

    font-size:4.2px;
}

.signal-row{

    min-height:34px;
}

.signal-rank-number{
    font-size:5.5px;
}

.signal-top-rank{
    font-size:4.5px;
}

.signal-coin{
    font-size:5.5px;
}

.signal-volume{
    font-size:4.6px;
}

.signal-price{
    font-size:4.6px;
}

.signal-change{
    font-size:5px;
}

.signal-icon{
    font-size:8px;
}

.signal-empty{
    min-height:38px;
    font-size:5px;
}


/* Coin */

.card-list{
    gap:5px;
}

.coin-main-row{
    min-height:36px;

    grid-template-columns:
        6%
        18%
        15%
        21%
        14%
        26%;
}

.coin-main-row > div{
    height:36px;
}

.rank-cell{
    padding-left:3px;
    font-size:6px;
}

.coin-name{
    font-size:6px;
}

.volume-value{
    font-size:5px;
}

.price-value{
    font-size:5.2px;
}

.change-cell{
    font-size:5.1px;
}

.signal-cell{
    height:36px!important;
    font-size:8px;
}


/* RSI */

.rsi-normal,
.rsi-blue,
.rsi-green,
.rsi-red{
    font-size:5.2px;
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

    sections = ""

    if USE_UPBIT == "Y":

        sections += (
            focus_section(
                latest_upbit_data
            )
        )

        sections += (
            section(
                latest_upbit_data,
                latest_upbit_update_time
            )
        )

    return f"""

    <!DOCTYPE html>

    <html lang="ko">

    <head>

        <meta charset="UTF-8">

        <meta
            name="viewport"
            content="
                width=device-width,
                initial-scale=1,
                maximum-scale=1,
                user-scalable=no
            "
        >

        <meta
            http-equiv="refresh"
            content="60"
        >

        <meta
            name="theme-color"
            content="#080c11"
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

        {market_summary_html()}

        {sections}

    </body>

    </html>

    """


# =========================================================
# Scheduler
# =========================================================

def scheduler():

    log.info(
        "스케줄러 시작"
    )

    while True:

        try:

            schedule.run_pending()

        except Exception as e:

            log.exception(
                f"스케줄러 오류: {e}"
            )

        time.sleep(1)


# =========================================================
# 설정 검증
# =========================================================

def validate_settings():

    if TOP_N < 1:

        raise ValueError(
            "TOP_N은 1 이상이어야 합니다."
        )

    if UPDATE_MINUTES < 1:

        raise ValueError(
            "UPDATE_MINUTES는 1 이상이어야 합니다."
        )


# =========================================================
# STARTUP
# =========================================================

@app.on_event(
    "startup"
)
def startup():

    validate_settings()

    log.info(
        "========================================"
    )

    log.info(
        "TRADING SIGNAL SYSTEM START"
    )

    log.info(
        "★ 코인 데이터 = 업비트"
    )

    log.info(
        f"★ TOP 거래대금 = 업비트 TOP {TOP_N}"
    )

    log.info(
        "★ BTC 당일 상승률 = KST 09:00 기준"
    )

    log.info(
        "★ BTC 12H = 09:00~21:00 / 21:00~09:00"
    )

    log.info(
        "★ Signal = 현재 시간대 BTC 12H > 0%"
    )

    log.info(
        "★ Signal = 현재 시간대 코인 12H > 0%"
    )

    log.info(
        "★ Signal = 현재 12H 상승률 높은 순"
    )

    log.info(
        "★ Signal = TOP 순위/거래대금/현재가/12H 상승률"
    )

    log.info(
        "★ TOP 상승률 = 업비트 일봉 상승률"
    )

    log.info(
        "★ TOP 마지막 칸 = 업비트 일봉 RSI(14)"
    )

    log.info(
        "★ RSI = TradingView Wilder RMA 방식"
    )

    log.info(
        "★ RSI = 현재 진행 중인 일봉 실시간 가격 반영"
    )

    log.info(
        "★ RSI <= 30 = 파란색"
    )

    log.info(
        "★ RSI 40~60 = 녹색"
    )

    log.info(
        "★ RSI >= 70 = 빨간색"
    )

    log.info(
        "★ RSI = Signal 조건에 사용하지 않음"
    )

    log.info(
        "★ ROC = 완전 삭제"
    )

    log.info(
        "★ ROC COUNT = 완전 삭제"
    )

    log.info(
        "★ 0선 돌파 조건 = 삭제"
    )

    log.info(
        "----------------------------------------"
    )

    log.info(
        "★ BTC 시장 시황 = OKX BTC-USDT"
    )

    log.info(
        "★ BTC 당일 = KST 09:00 기준"
    )

    log.info(
        "★ BTC 12H = KST 09:00 / 21:00 경계"
    )

    log.info(
        "★ 현재 시간대에 해당하는 12H만 Signal 필터"
    )

    log.info(
        "========================================"
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
