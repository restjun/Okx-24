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
# 사용자 설정
# =========================================================

TOP_N = 20

UPDATE_MINUTES = 1

HISTORY_CHUNK = 200
MAX_HISTORY_CHUNKS = 10

USE_UPBIT = "Y"
USE_OKX = "N"

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10

OKX_BASE_URL = "https://www.okx.com"
OKX_BTC_INST_ID = "BTC-USDT"


# =========================================================
# 전역 데이터
# =========================================================

latest_upbit_data = []

latest_upbit_update_time = "-"

latest_upbit_markets = []

latest_btc_okx_price = None
latest_btc_daily_change = None

latest_btc_4h_periods = []
latest_btc_current_4h_change = None

latest_okx_update_time = "-"

request_lock = threading.Lock()
update_lock = threading.Lock()

last_request_time = 0


# =========================================================
# 업비트 4시간봉
#
# 01~05
# 05~09
# 09~13
# 13~17
# 17~21
# 21~01
# =========================================================

FOUR_HOUR_DEFINITIONS = [
    {
        "key": "01_05",
        "start_hour": 1,
        "end_hour": 5
    },
    {
        "key": "05_09",
        "start_hour": 5,
        "end_hour": 9
    },
    {
        "key": "09_13",
        "start_hour": 9,
        "end_hour": 13
    },
    {
        "key": "13_17",
        "start_hour": 13,
        "end_hour": 17
    },
    {
        "key": "17_21",
        "start_hour": 17,
        "end_hour": 21
    },
    {
        "key": "21_01",
        "start_hour": 21,
        "end_hour": 1
    }
]


# =========================================================
# HTTP
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


def request_get(
    url,
    params=None,
    headers=None,
    timeout=10
):

    for attempt in range(MAX_RETRIES):

        try:

            wait_request()

            response = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=timeout
            )

            if response.status_code == 200:

                return response

            if response.status_code == 429:

                wait_time = (
                    RATE_LIMIT_WAIT
                    * (attempt + 1)
                )

                log.warning(
                    "429 -> %.1f초 대기",
                    wait_time
                )

                time.sleep(
                    wait_time
                )

                continue

            if response.status_code >= 500:

                time.sleep(
                    min(
                        RATE_LIMIT_WAIT
                        * (attempt + 1),
                        10
                    )
                )

                continue

            log.warning(
                "HTTP %s : %s",
                response.status_code,
                url
            )

            return None

        except Exception as e:

            log.warning(
                "Request error %d/%d : %s",
                attempt + 1,
                MAX_RETRIES,
                e
            )

            time.sleep(
                min(
                    RATE_LIMIT_WAIT
                    * (attempt + 1),
                    10
                )
            )

    return None


# =========================================================
# 숫자 변환
# =========================================================

def safe_float(value):

    try:
        return float(value)

    except Exception:
        return None


# =========================================================
# 업비트 전체 KRW 마켓 + 거래대금
# =========================================================

def get_upbit_markets():

    response = request_get(
        "https://api.upbit.com/v1/market/all",
        params={
            "isDetails": "false"
        }
    )

    if response is None:
        return []

    try:

        market_info = response.json()

    except Exception:

        return []

    markets = [
        x["market"]
        for x in market_info
        if x.get("market", "").startswith("KRW-")
    ]

    if not markets:
        return []

    result = []

    for i in range(
        0,
        len(markets),
        100
    ):

        chunk = markets[
            i:i + 100
        ]

        response = request_get(
            "https://api.upbit.com/v1/ticker",
            params={
                "markets": ",".join(chunk)
            }
        )

        if response is None:
            continue

        try:

            ticker_data = response.json()

        except Exception:

            continue

        if not isinstance(
            ticker_data,
            list
        ):
            continue

        for item in ticker_data:

            market = item.get(
                "market"
            )

            price = safe_float(
                item.get("trade_price")
            )

            volume = safe_float(
                item.get(
                    "acc_trade_price_24h"
                )
            )

            if not market:
                continue

            if price is None:
                continue

            if volume is None:
                volume = 0

            result.append({
                "market": market,
                "price": price,
                "volume": volume
            })

    return result


# =========================================================
# 일간 변화율
#
# 업비트 일봉 API 기준
# =========================================================

def daily_change_upbit(
    market,
    current_price
):

    response = request_get(
        "https://api.upbit.com/v1/candles/days",
        params={
            "market": market,
            "count": 2
        }
    )

    if response is None:
        return None

    try:

        candles = response.json()

    except Exception:

        return None

    if not isinstance(
        candles,
        list
    ):
        return None

    if len(candles) < 2:
        return None

    try:

        previous_close = float(
            candles[1]["trade_price"]
        )

        if previous_close == 0:
            return None

        return (
            (
                current_price
                - previous_close
            )
            / previous_close
            * 100
        )

    except Exception:

        return None


# =========================================================
# 업비트 1시간봉
# =========================================================

def get_upbit_60m_candles(
    market,
    count=200
):

    response = request_get(
        "https://api.upbit.com/v1/candles/minutes/60",
        params={
            "market": market,
            "count": min(
                count,
                200
            )
        }
    )

    if response is None:
        return []

    try:

        data = response.json()

    except Exception:

        return []

    if not isinstance(
        data,
        list
    ):
        return []

    return data


# =========================================================
# 현재 4H 시작시간
# =========================================================

def get_current_4h_start(
    now=None
):

    if now is None:

        now = datetime.now(
            KST
        )

    now = now.astimezone(
        KST
    )

    hour = now.hour

    if 1 <= hour < 5:

        start_hour = 1

    elif 5 <= hour < 9:

        start_hour = 5

    elif 9 <= hour < 13:

        start_hour = 9

    elif 13 <= hour < 17:

        start_hour = 13

    elif 17 <= hour < 21:

        start_hour = 17

    elif hour >= 21:

        start_hour = 21

    else:

        # 00:00~00:59
        # 전일 21:00~01:00

        return (
            now.replace(
                hour=0,
                minute=0,
                second=0,
                microsecond=0
            )
            - timedelta(hours=3)
        )

    return now.replace(
        hour=start_hour,
        minute=0,
        second=0,
        microsecond=0
    )


# =========================================================
# 4H 기간 생성
# =========================================================

def make_4h_period(
    start,
    now=None,
    active=False
):

    if now is None:

        now = datetime.now(
            KST
        )

    start = start.astimezone(
        KST
    )

    end = start + timedelta(
        hours=4
    )

    if active:

        label = (
            f"현재 "
            f"{start:%H}~{end:%H}"
        )

    else:

        day_label = (
            "오늘"
            if start.date() == now.date()
            else "전일"
        )

        label = (
            f"{day_label} "
            f"{start:%H}~{end:%H}"
        )

    return {
        "key": f"{start:%Y%m%d_%H}",
        "start": start,
        "end": end,
        "label": label,
        "change": None,
        "open": None,
        "high": None,
        "low": None,
        "close": None,
        "active": active
    }


# =========================================================
# 최근 6개 4H
# =========================================================

def get_recent_4h_periods(
    count=6,
    now=None
):

    if now is None:

        now = datetime.now(
            KST
        )

    current_start = get_current_4h_start(
        now
    )

    periods = []

    for i in range(
        count - 1,
        -1,
        -1
    ):

        start = (
            current_start
            - timedelta(
                hours=4 * i
            )
        )

        active = (
            i == 0
            and start <= now
            < start + timedelta(hours=4)
        )

        periods.append(
            make_4h_period(
                start,
                now=now,
                active=active
            )
        )

    return periods


# =========================================================
# 현재 4H
# =========================================================

def get_current_4h_period(
    now=None
):

    periods = get_recent_4h_periods(
        count=1,
        now=now
    )

    return periods[0]


# =========================================================
# 이전 4H
# =========================================================

def get_previous_4h_period(
    now=None
):

    if now is None:

        now = datetime.now(
            KST
        )

    current_start = get_current_4h_start(
        now
    )

    previous_start = (
        current_start
        - timedelta(hours=4)
    )

    return make_4h_period(
        previous_start,
        now=now,
        active=False
    )


# =========================================================
# 업비트 1H → 4H 변환
# =========================================================

def build_upbit_4h_candles(
    market,
    current_price,
    candles=None,
    now=None
):

    if now is None:

        now = datetime.now(
            KST
        )

    if candles is None:

        candles = get_upbit_60m_candles(
            market,
            200
        )

    periods = get_recent_4h_periods(
        6,
        now
    )

    if not candles:

        return periods

    rows = []

    for candle in candles:

        try:

            dt = datetime.fromisoformat(
                candle[
                    "candle_date_time_kst"
                ]
            ).replace(
                tzinfo=KST
            )

            rows.append({
                "datetime": dt,
                "open": float(
                    candle["opening_price"]
                ),
                "high": float(
                    candle["high_price"]
                ),
                "low": float(
                    candle["low_price"]
                ),
                "close": float(
                    candle["trade_price"]
                )
            })

        except Exception:

            continue

    if not rows:

        return periods

    df = pd.DataFrame(
        rows
    )

    for idx, period in enumerate(
        periods
    ):

        start = period["start"]
        end = period["end"]

        subset = df[
            (df["datetime"] >= start)
            &
            (df["datetime"] < end)
        ]

        if subset.empty:

            continue

        subset = subset.sort_values(
            "datetime"
        )

        open_price = float(
            subset.iloc[0]["open"]
        )

        high_price = float(
            subset["high"].max()
        )

        low_price = float(
            subset["low"].min()
        )

        close_price = float(
            subset.iloc[-1]["close"]
        )

        # 현재 진행중인 4H
        if period["active"]:

            close_price = current_price

            high_price = max(
                high_price,
                current_price
            )

            low_price = min(
                low_price,
                current_price
            )

        if open_price != 0:

            change = (
                (
                    close_price
                    - open_price
                )
                / open_price
                * 100
            )

        else:

            change = None

        periods[idx][
            "open"
        ] = open_price

        periods[idx][
            "high"
        ] = high_price

        periods[idx][
            "low"
        ] = low_price

        periods[idx][
            "close"
        ] = close_price

        periods[idx][
            "change"
        ] = change

    return periods


# =========================================================
# 업비트 4H 분석
# =========================================================

def get_upbit_4h_changes(
    market,
    current_price,
    candles=None,
    now=None
):

    periods = build_upbit_4h_candles(
        market,
        current_price,
        candles,
        now
    )

    return {
        "periods": periods,
        "values": [
            x["change"]
            for x in periods
        ]
    }


# =========================================================
# OKX BTC 현재가
# =========================================================

def get_okx_btc_price():

    response = request_get(
        f"{OKX_BASE_URL}/api/v5/market/ticker",
        params={
            "instId": OKX_BTC_INST_ID
        }
    )

    if response is None:

        return None

    try:

        data = response.json()

        rows = data.get(
            "data",
            []
        )

        if not rows:

            return None

        return float(
            rows[0]["last"]
        )

    except Exception:

        return None


# =========================================================
# OKX BTC 1H
# =========================================================

def get_okx_btc_1h_candles():

    response = request_get(
        f"{OKX_BASE_URL}/api/v5/market/candles",
        params={
            "instId": OKX_BTC_INST_ID,
            "bar": "1H",
            "limit": 300
        }
    )

    if response is None:

        return []

    try:

        data = response.json()

        return data.get(
            "data",
            []
        )

    except Exception:

        return []


# =========================================================
# OKX BTC DataFrame
# =========================================================

def build_okx_btc_dataframe():

    candles = get_okx_btc_1h_candles()

    if not candles:

        return pd.DataFrame()

    rows = []

    UTC = ZoneInfo(
        "UTC"
    )

    for candle in candles:

        try:

            timestamp = int(
                candle[0]
            )

            dt_utc = datetime.fromtimestamp(
                timestamp / 1000,
                tz=UTC
            )

            dt_kst = dt_utc.astimezone(
                KST
            )

            rows.append({
                "datetime": dt_kst,
                "open": float(candle[1]),
                "high": float(candle[2]),
                "low": float(candle[3]),
                "close": float(candle[4])
            })

        except Exception:

            continue

    if not rows:

        return pd.DataFrame()

    df = pd.DataFrame(
        rows
    )

    df = df.drop_duplicates(
        subset=["datetime"]
    )

    df = df.sort_values(
        "datetime"
    )

    return df


# =========================================================
# BTC 일봉
# =========================================================

def aggregate_btc_kst_daily(df):

    if df is None or df.empty:

        return pd.DataFrame()

    work = df.copy()

    work["daily_start"] = (
        work["datetime"]
        - pd.Timedelta(hours=9)
    ).dt.floor("D") + pd.Timedelta(hours=9)

    result = (
        work
        .groupby("daily_start")
        .agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last"
        })
        .reset_index()
    )

    return result


# =========================================================
# BTC 일간 변화
# =========================================================

def get_okx_btc_daily_change(
    price,
    df
):

    if price is None:

        return None

    daily = aggregate_btc_kst_daily(
        df
    )

    if daily.empty:

        return None

    now = datetime.now(
        KST
    )

    current_start = now.replace(
        hour=9,
        minute=0,
        second=0,
        microsecond=0
    )

    if now.hour < 9:

        current_start -= timedelta(
            days=1
        )

    current = daily[
        daily["daily_start"]
        == current_start
    ]

    if current.empty:

        previous = daily[
            daily["daily_start"]
            < current_start
        ]

        if previous.empty:

            return None

        base = float(
            previous.iloc[-1]["open"]
        )

    else:

        base = float(
            current.iloc[-1]["open"]
        )

    if base == 0:

        return None

    return (
        (
            price - base
        )
        / base
        * 100
    )


# =========================================================
# BTC 4H
# =========================================================

def build_okx_btc_4h_candles(
    price,
    df,
    now=None
):

    if now is None:

        now = datetime.now(
            KST
        )

    periods = get_recent_4h_periods(
        6,
        now
    )

    if df is None or df.empty:

        return periods

    for idx, period in enumerate(
        periods
    ):

        subset = df[
            (df["datetime"] >= period["start"])
            &
            (df["datetime"] < period["end"])
        ]

        if subset.empty:

            continue

        subset = subset.sort_values(
            "datetime"
        )

        open_price = float(
            subset.iloc[0]["open"]
        )

        high_price = float(
            subset["high"].max()
        )

        low_price = float(
            subset["low"].min()
        )

        close_price = float(
            subset.iloc[-1]["close"]
        )

        if period["active"]:

            close_price = price

            high_price = max(
                high_price,
                price
            )

            low_price = min(
                low_price,
                price
            )

        if open_price != 0:

            change = (
                (
                    close_price
                    - open_price
                )
                / open_price
                * 100
            )

        else:

            change = None

        periods[idx]["open"] = open_price
        periods[idx]["high"] = high_price
        periods[idx]["low"] = low_price
        periods[idx]["close"] = close_price
        periods[idx]["change"] = change

    return periods


# =========================================================
# BTC 업데이트
# =========================================================

def update_btc_market():

    global latest_btc_okx_price
    global latest_btc_daily_change
    global latest_btc_4h_periods
    global latest_btc_current_4h_change
    global latest_okx_update_time

    try:

        now = datetime.now(
            KST
        )

        price = get_okx_btc_price()

        if price is None:

            log.warning(
                "BTC 가격 조회 실패"
            )

            return

        df = build_okx_btc_dataframe()

        daily_change = (
            get_okx_btc_daily_change(
                price,
                df
            )
        )

        periods = (
            build_okx_btc_4h_candles(
                price,
                df,
                now
            )
        )

        current_period = (
            get_current_4h_period(
                now
            )
        )

        current_change = None

        for period in periods:

            if (
                period["start"]
                == current_period["start"]
            ):

                current_change = (
                    period["change"]
                )

                break

        latest_btc_okx_price = price

        latest_btc_daily_change = (
            daily_change
        )

        latest_btc_4h_periods = periods

        latest_btc_current_4h_change = (
            current_change
        )

        latest_okx_update_time = (
            now.strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        )

    except Exception as e:

        log.exception(
            "BTC 업데이트 오류: %s",
            e
        )


# =========================================================
# 코인 분석
# =========================================================

def analyze(
    market,
    current_price,
    candles,
    now
):

    daily_change = (
        daily_change_upbit(
            market,
            current_price
        )
    )

    four_hour = (
        get_upbit_4h_changes(
            market,
            current_price,
            candles,
            now
        )
    )

    periods = four_hour[
        "periods"
    ]

    current_period = (
        get_current_4h_period(
            now
        )
    )

    current_4h = None
    previous_4h = None

    for i, period in enumerate(
        periods
    ):

        if (
            period["start"]
            == current_period["start"]
        ):

            current_4h = (
                period["change"]
            )

            if i > 0:

                previous_4h = (
                    periods[i - 1]["change"]
                )

            break

    return {
        "daily_change": daily_change,
        "four_hour_periods": periods,
        "current_4h_change": current_4h,
        "previous_4h_change": previous_4h
    }


# =========================================================
# 이름
# =========================================================

def coin_name(
    market
):

    if not market:

        return "-"

    return market.replace(
        "KRW-",
        ""
    )


# =========================================================
# 가격 표시
# =========================================================

def format_market_price(
    price
):

    if price is None:

        return "-"

    try:

        price = float(price)

    except Exception:

        return "-"

    if price >= 100_000_000:

        return (
            f"{price / 100_000_000:.2f}억"
        )

    if price >= 10_000:

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
# 거래대금 표시
# =========================================================

def format_volume(
    value
):

    if value is None:

        return "-"

    try:

        value = float(value)

    except Exception:

        return "-"

    if value >= 1_000_000_000_000:

        return (
            f"{value / 1_000_000_000_000:.2f}조"
        )

    if value >= 100_000_000:

        return (
            f"{value / 100_000_000:.1f}억"
        )

    if value >= 10_000:

        return (
            f"{value / 10_000:.0f}만"
        )

    return (
        f"{value:,.0f}"
    )


# =========================================================
# 상승률
# =========================================================

def format_change(
    value
):

    if value is None:

        return (
            '<span class="neutral">-</span>'
        )

    try:

        value = float(value)

    except Exception:

        return (
            '<span class="neutral">-</span>'
        )

    if value > 0:

        return (
            '<span class="positive">'
            f'▲ +{value:.2f}%'
            '</span>'
        )

    if value < 0:

        return (
            '<span class="negative">'
            f'▼ {value:.2f}%'
            '</span>'
        )

    return (
        '<span class="neutral">'
        '0.00%'
        '</span>'
    )


# =========================================================
# 데이터 Row
# =========================================================

def make_row(
    rank,
    market,
    volume,
    current_price,
    analysis
):

    return {
        "rank": rank,
        "market": market,
        "name": coin_name(
            market
        ),
        "volume": volume,
        "volume_text": format_volume(
            volume
        ),
        "current_price": current_price,
        "current_price_text": (
            format_market_price(
                current_price
            )
        ),
        "daily_change": analysis[
            "daily_change"
        ],
        "daily_html": format_change(
            analysis[
                "daily_change"
            ]
        ),
        "four_hour_periods": analysis[
            "four_hour_periods"
        ],
        "current_4h_change": analysis[
            "current_4h_change"
        ],
        "previous_4h_change": analysis[
            "previous_4h_change"
        ],
        "signal_pass": False
    }


# =========================================================
# 업비트 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update_time
    global latest_upbit_markets

    with update_lock:

        try:

            now = datetime.now(
                KST
            )

            markets = (
                get_upbit_markets()
            )

            if not markets:

                log.warning(
                    "업비트 마켓 데이터 없음"
                )

                return

            latest_upbit_markets = (
                markets
            )

            markets.sort(
                key=lambda x: x[
                    "volume"
                ],
                reverse=True
            )

            top_markets = markets[
                :TOP_N
            ]

            rows = []

            for rank, item in enumerate(
                top_markets,
                start=1
            ):

                market = item[
                    "market"
                ]

                price = item[
                    "price"
                ]

                try:

                    candles = (
                        get_upbit_60m_candles(
                            market,
                            200
                        )
                    )

                    analysis = analyze(
                        market,
                        price,
                        candles,
                        now
                    )

                    row = make_row(
                        rank,
                        market,
                        item["volume"],
                        price,
                        analysis
                    )

                    rows.append(
                        row
                    )

                except Exception as e:

                    log.warning(
                        "%s 분석 실패: %s",
                        market,
                        e
                    )

            # =================================================
            # SIGNAL 조건
            # =================================================

            btc_pass = (
                latest_btc_current_4h_change
                is not None
                and
                latest_btc_current_4h_change > 0
            )

            for row in rows:

                coin_current_pass = (
                    row[
                        "current_4h_change"
                    ]
                    is not None
                    and
                    row[
                        "current_4h_change"
                    ] > 0
                )

                coin_previous_pass = (
                    row[
                        "previous_4h_change"
                    ]
                    is not None
                    and
                    row[
                        "previous_4h_change"
                    ] > 0
                )

                row["signal_pass"] = (
                    btc_pass
                    and coin_current_pass
                    and coin_previous_pass
                )

            latest_upbit_data = rows

            latest_upbit_update_time = (
                now.strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            )

            signal_count = sum(
                1
                for x in rows
                if x["signal_pass"]
            )

            log.info(
                "UPBIT 업데이트 완료 "
                "/ TOP%d / SIGNAL %d",
                len(rows),
                signal_count
            )

        except Exception as e:

            log.exception(
                "업비트 업데이트 오류: %s",
                e
            )


# =========================================================
# 전체 업데이트
# =========================================================

def update_all():

    try:

        update_btc_market()

        update_upbit()

    except Exception as e:

        log.exception(
            "전체 업데이트 오류: %s",
            e
        )


# =========================================================
# 4H HTML
# =========================================================

def four_hour_cells_html(
    periods,
    mobile=False
):

    if not periods:

        return (
            '<div class="empty-history">'
            '데이터 없음'
            '</div>'
        )

    result = []

    for period in periods:

        classes = [
            "four-hour-cell"
        ]

        if period.get(
            "active"
        ):

            classes.append(
                "current-4h"
            )

        label = html.escape(
            str(
                period.get(
                    "label",
                    "-"
                )
            )
        )

        change = format_change(
            period.get(
                "change"
            )
        )

        result.append(
            f"""
            <div class="{' '.join(classes)}">

                <div class="four-hour-label">
                    {label}
                </div>

                <div class="four-hour-value">
                    {change}
                </div>

            </div>
            """
        )

    return "".join(
        result
    )


# =========================================================
# PC SIGNAL 카드
# =========================================================

def pc_signal_card(
    row,
    rank
):

    return f"""
    <div class="pc-coin-card pc-signal-card">

        <div class="pc-card-header">

            <div class="pc-card-title">

                <span class="pc-rank signal-rank">
                    SIGNAL {rank}
                </span>

                <strong>
                    {html.escape(row["name"])}
                </strong>

                <small>
                    {html.escape(row["market"])}
                </small>

            </div>

            <span class="pc-signal-badge">
                SIGNAL
            </span>

        </div>


        <div class="pc-info-grid">

            <div class="pc-info-box">

                <span>
                    현재가
                </span>

                <strong class="pc-price">
                    {html.escape(
                        row["current_price_text"]
                    )}
                </strong>

            </div>


            <div class="pc-info-box">

                <span>
                    24H 거래대금
                </span>

                <strong>
                    {html.escape(
                        row["volume_text"]
                    )}
                </strong>

            </div>


            <div class="pc-info-box">

                <span>
                    일간
                </span>

                <strong>
                    {row["daily_html"]}
                </strong>

            </div>

        </div>


        <div class="pc-signal-condition">

            <div>
                <span>
                    현재 4H
                </span>

                <strong>
                    {format_change(
                        row["current_4h_change"]
                    )}
                </strong>
            </div>

            <b>
                →
            </b>

            <div>
                <span>
                    이전 4H
                </span>

                <strong>
                    {format_change(
                        row["previous_4h_change"]
                    )}
                </strong>
            </div>

        </div>


        <div class="pc-history-title">
            최근 4시간 흐름
        </div>

        <div class="pc-four-grid">

            {four_hour_cells_html(
                row["four_hour_periods"]
            )}

        </div>

    </div>
    """


# =========================================================
# PC TOP 카드
# =========================================================

def pc_top_card(
    row
):

    signal = ""

    if row["signal_pass"]:

        signal = """
        <span class="pc-small-signal">
            SIGNAL
        </span>
        """

    return f"""
    <div class="pc-coin-card pc-top-card">

        <div class="pc-card-header">

            <div class="pc-card-title">

                <span class="pc-rank top-rank">
                    TOP {row["rank"]}
                </span>

                <strong>
                    {html.escape(row["name"])}
                </strong>

                <small>
                    {html.escape(row["market"])}
                </small>

            </div>

            {signal}

        </div>


        <div class="pc-info-grid">

            <div class="pc-info-box">

                <span>
                    현재가
                </span>

                <strong class="pc-price">
                    {html.escape(
                        row["current_price_text"]
                    )}
                </strong>

            </div>


            <div class="pc-info-box">

                <span>
                    24H 거래대금
                </span>

                <strong>
                    {html.escape(
                        row["volume_text"]
                    )}
                </strong>

            </div>


            <div class="pc-info-box">

                <span>
                    일간
                </span>

                <strong>
                    {row["daily_html"]}
                </strong>

            </div>

        </div>


        <div class="pc-current-4h">

            <span>
                현재 4H
            </span>

            <strong>
                {format_change(
                    row["current_4h_change"]
                )}
            </strong>

        </div>


        <div class="pc-history-title">
            최근 4시간 흐름
        </div>

        <div class="pc-four-grid">

            {four_hour_cells_html(
                row["four_hour_periods"]
            )}

        </div>

    </div>
    """


# =========================================================
# 모바일 SIGNAL 카드
# =========================================================

def mobile_signal_card(
    row,
    rank
):

    return f"""
    <div class="m-card m-signal-card">

        <div class="m-card-top">

            <div class="m-rank">
                SIGNAL {rank}
            </div>

            <div class="m-coin">
                {html.escape(row["name"])}
            </div>

            <div class="m-signal-badge">
                SIGNAL
            </div>

        </div>


        <div class="m-price-row">

            <strong class="m-price">
                {html.escape(
                    row["current_price_text"]
                )}
            </strong>

            <span>
                거래대금
                {html.escape(
                    row["volume_text"]
                )}
            </span>

            <span>
                {row["daily_html"]}
            </span>

        </div>


        <div class="m-signal-row">

            <span>
                현재 4H
                <b>
                    {format_change(
                        row["current_4h_change"]
                    )}
                </b>
            </span>

            <i>
                →
            </i>

            <span>
                이전 4H
                <b>
                    {format_change(
                        row["previous_4h_change"]
                    )}
                </b>
            </span>

        </div>


        <div class="m-four-grid">

            {four_hour_cells_html(
                row["four_hour_periods"]
            )}

        </div>

    </div>
    """


# =========================================================
# 모바일 TOP 카드
# =========================================================

def mobile_top_card(
    row
):

    signal = ""

    if row["signal_pass"]:

        signal = """
        <span class="m-small-signal">
            SIGNAL
        </span>
        """

    return f"""
    <div class="m-card m-top-card">

        <div class="m-card-top">

            <div class="m-rank">
                TOP {row["rank"]}
            </div>

            <div class="m-coin">
                {html.escape(row["name"])}
            </div>

            {signal}

        </div>


        <div class="m-price-row">

            <strong class="m-price">
                {html.escape(
                    row["current_price_text"]
                )}
            </strong>

            <span>
                거래대금
                {html.escape(
                    row["volume_text"]
                )}
            </span>

            <span>
                {row["daily_html"]}
            </span>

        </div>


        <div class="m-current-row">

            <span>
                현재 4H
            </span>

            <strong>
                {format_change(
                    row["current_4h_change"]
                )}
            </strong>

        </div>


        <div class="m-four-grid">

            {four_hour_cells_html(
                row["four_hour_periods"]
            )}

        </div>

    </div>
    """


# =========================================================
# PC BTC 카드
# =========================================================

def pc_btc_card():

    btc_on = (
        latest_btc_current_4h_change
        is not None
        and latest_btc_current_4h_change > 0
    )

    if btc_on:

        status = """
        <span class="pc-btc-on">
            ON
        </span>
        """

    else:

        status = """
        <span class="pc-btc-off">
            OFF
        </span>
        """

    return f"""
    <div class="pc-btc-card">

        <div class="pc-btc-header">

            <div>

                <div class="pc-btc-title">
                    BTC MARKET
                </div>

                <div class="pc-btc-sub">
                    OKX BTC-USDT
                </div>

            </div>

            {status}

        </div>


        <div class="pc-btc-stats">

            <div>
                <span>
                    BTC 현재가
                </span>

                <strong>
                    {format_market_price(
                        latest_btc_okx_price
                    )}
                </strong>
            </div>


            <div>
                <span>
                    일간
                </span>

                <strong>
                    {format_change(
                        latest_btc_daily_change
                    )}
                </strong>
            </div>


            <div>
                <span>
                    현재 4H
                </span>

                <strong>
                    {format_change(
                        latest_btc_current_4h_change
                    )}
                </strong>
            </div>


            <div>
                <span>
                    SIGNAL
                </span>

                <strong>
                    {status}
                </strong>
            </div>

        </div>


        <div class="pc-history-title">
            BTC 최근 4시간 흐름
        </div>

        <div class="pc-four-grid">

            {four_hour_cells_html(
                latest_btc_4h_periods
            )}

        </div>

    </div>
    """


# =========================================================
# 모바일 BTC 카드
# =========================================================

def mobile_btc_card():

    btc_on = (
        latest_btc_current_4h_change
        is not None
        and latest_btc_current_4h_change > 0
    )

    if btc_on:

        status = """
        <span class="m-btc-on">
            ON
        </span>
        """

    else:

        status = """
        <span class="m-btc-off">
            OFF
        </span>
        """

    return f"""
    <div class="m-btc-card">

        <div class="m-btc-head">

            <div>
                <strong>
                    BTC
                </strong>

                <small>
                    MARKET
                </small>
            </div>

            {status}

        </div>


        <div class="m-btc-price">

            <strong>
                {format_market_price(
                    latest_btc_okx_price
                )}
            </strong>

            <span>
                {format_change(
                    latest_btc_daily_change
                )}
            </span>

        </div>


        <div class="m-btc-bottom">

            <span>
                현재 4H
                <b>
                    {format_change(
                        latest_btc_current_4h_change
                    )}
                </b>
            </span>

            <span>
                SIGNAL
                <b>
                    {status}
                </b>
            </span>

        </div>


        <div class="m-four-grid">

            {four_hour_cells_html(
                latest_btc_4h_periods
            )}

        </div>

    </div>
    """


# =========================================================
# PC SIGNAL 영역
# =========================================================

def pc_signal_section():

    rows = [
        row
        for row in latest_upbit_data
        if row["signal_pass"]
    ]

    rows.sort(
        key=lambda x: (
            x["current_4h_change"]
            if x["current_4h_change"]
            is not None
            else -999999
        ),
        reverse=True
    )

    btc_pass = (
        latest_btc_current_4h_change
        is not None
        and latest_btc_current_4h_change > 0
    )

    if not btc_pass:

        return """
        <div class="pc-empty-signal">

            <strong>
                SIGNAL OFF
            </strong>

            <span>
                BTC 현재 4H가 양수가 아닙니다.
            </span>

        </div>
        """

    if not rows:

        return """
        <div class="pc-empty-signal">

            <strong>
                SIGNAL 대기
            </strong>

            <span>
                현재 조건을 만족하는 종목이 없습니다.
            </span>

        </div>
        """

    return "".join(
        pc_signal_card(
            row,
            idx
        )
        for idx, row in enumerate(
            rows,
            1
        )
    )


# =========================================================
# 모바일 SIGNAL 영역
# =========================================================

def mobile_signal_section():

    rows = [
        row
        for row in latest_upbit_data
        if row["signal_pass"]
    ]

    rows.sort(
        key=lambda x: (
            x["current_4h_change"]
            if x["current_4h_change"]
            is not None
            else -999999
        ),
        reverse=True
    )

    btc_pass = (
        latest_btc_current_4h_change
        is not None
        and latest_btc_current_4h_change > 0
    )

    if not btc_pass:

        return """
        <div class="m-empty-signal">

            <strong>
                SIGNAL OFF
            </strong>

            <span>
                BTC 현재 4H 음수
            </span>

        </div>
        """

    if not rows:

        return """
        <div class="m-empty-signal">

            <strong>
                SIGNAL 대기
            </strong>

            <span>
                조건 만족 종목 없음
            </span>

        </div>
        """

    return "".join(
        mobile_signal_card(
            row,
            idx
        )
        for idx, row in enumerate(
            rows,
            1
        )
    )


# =========================================================
# PC 전체
# =========================================================

def pc_dashboard():

    return f"""
    <div class="pc-dashboard">

        <div class="pc-page-header">

            <div>
                <h1>
                    CRYPTO MARKET
                </h1>

                <p>
                    BTC 시황 · 거래대금 · 상승률
                </p>
            </div>

            <span>
                {html.escape(
                    latest_upbit_update_time
                )}
            </span>

        </div>


        {pc_btc_card()}


        <section class="pc-section">

            <div class="pc-section-header">

                <h2>
                    SIGNAL
                </h2>

                <span>
                    BTC 현재4H + 코인 현재4H + 이전4H
                </span>

            </div>


            <div class="pc-signal-grid">

                {pc_signal_section()}

            </div>

        </section>


        <section class="pc-section">

            <div class="pc-section-header">

                <h2>
                    TOP {TOP_N}
                </h2>

                <span>
                    24H 거래대금 순
                </span>

            </div>


            <div class="pc-top-grid">

                {
                    "".join(
                        pc_top_card(row)
                        for row in latest_upbit_data
                    )
                }

            </div>

        </section>

    </div>
    """


# =========================================================
# 모바일 전체
# =========================================================

def mobile_dashboard():

    return f"""
    <div class="mobile-dashboard">

        <div class="m-page-header">

            <div>

                <strong>
                    CRYPTO
                </strong>

                <span>
                    MARKET
                </span>

            </div>

            <small>
                {html.escape(
                    latest_upbit_update_time
                )}
            </small>

        </div>


        {mobile_btc_card()}


        <div class="m-section">

            <div class="m-section-title">

                <strong>
                    SIGNAL
                </strong>

                <span>
                    BTC + 4H
                </span>

            </div>


            <div class="m-signal-list">

                {mobile_signal_section()}

            </div>

        </div>


        <div class="m-section">

            <div class="m-section-title">

                <strong>
                    TOP {TOP_N}
                </strong>

                <span>
                    거래대금
                </span>

            </div>


            <div class="m-top-list">

                {
                    "".join(
                        mobile_top_card(row)
                        for row in latest_upbit_data
                    )
                }

            </div>

        </div>

    </div>
    """


# =========================================================
# HTML 전체
# =========================================================

def dashboard():

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

        <meta
            http-equiv="refresh"
            content="60"
        >

        <title>
            Crypto Market
        </title>


        <style>

        /* =================================================
           공통
        ================================================= */

        * {{
            box-sizing: border-box;
        }}

        html {{
            background: #070b0f;
        }}

        body {{

            margin: 0;

            background: #070b0f;

            color: #e9eef3;

            font-family:
                Arial,
                "Noto Sans KR",
                sans-serif;

        }}

        .positive {{
            color: #50d58d;
            font-weight: 800;
        }}

        .negative {{
            color: #ff6975;
            font-weight: 800;
        }}

        .neutral {{
            color: #7c8790;
            font-weight: 700;
        }}


        /* =================================================
           PC 전용
        ================================================= */

        .pc-dashboard {{

            display: block;

            width: 100%;

            max-width: 1500px;

            margin: 0 auto;

            padding: 18px;

        }}


        .pc-page-header {{

            display: flex;

            justify-content: space-between;

            align-items: flex-end;

            margin-bottom: 14px;

        }}


        .pc-page-header h1 {{

            margin: 0;

            font-size: 24px;

            font-weight: 900;

            letter-spacing: -1px;

        }}


        .pc-page-header p {{

            margin: 5px 0 0;

            color: #66737e;

            font-size: 11px;

        }}


        .pc-page-header > span {{

            color: #64717c;

            font-size: 10px;

        }}


        /* =================================================
           PC BTC
        ================================================= */

        .pc-btc-card {{

            background: #10171e;

            border: 1px solid #27333d;

            border-radius: 12px;

            padding: 15px;

            margin-bottom: 16px;

        }}


        .pc-btc-header {{

            display: flex;

            align-items: center;

            justify-content: space-between;

            margin-bottom: 12px;

        }}


        .pc-btc-title {{

            font-size: 19px;

            font-weight: 900;

        }}


        .pc-btc-sub {{

            color: #687580;

            font-size: 9px;

            margin-top: 3px;

        }}


        .pc-btc-on,
        .pc-btc-off {{

            display: inline-block;

            padding: 5px 10px;

            border-radius: 5px;

            font-size: 10px;

            font-weight: 900;

        }}


        .pc-btc-on {{

            color: #63d99b;

            background: #143c29;

            border: 1px solid #2c6e4d;

        }}


        .pc-btc-off {{

            color: #ff7a84;

            background: #39181d;

            border: 1px solid #713139;

        }}


        .pc-btc-stats {{

            display: grid;

            grid-template-columns:
                repeat(4, 1fr);

            gap: 7px;

        }}


        .pc-btc-stats > div {{

            background: #0b1116;

            border: 1px solid #202b34;

            border-radius: 7px;

            padding: 9px;

        }}


        .pc-btc-stats span {{

            display: block;

            color: #65727d;

            font-size: 9px;

            margin-bottom: 5px;

        }}


        .pc-btc-stats strong {{

            font-size: 15px;

        }}


        /* =================================================
           PC Section
        ================================================= */

        .pc-section {{

            margin-top: 16px;

        }}


        .pc-section-header {{

            display: flex;

            align-items: center;

            justify-content: space-between;

            margin-bottom: 8px;

        }}


        .pc-section-header h2 {{

            margin: 0;

            font-size: 17px;

            font-weight: 900;

        }}


        .pc-section-header span {{

            color: #66737e;

            font-size: 10px;

        }}


        /* =================================================
           PC SIGNAL / TOP
        ================================================= */

        .pc-signal-grid,
        .pc-top-grid {{

            display: grid;

            grid-template-columns:
                repeat(2, minmax(0, 1fr));

            gap: 10px;

        }}


        /* =================================================
           PC Coin Card
        ================================================= */

        .pc-coin-card {{

            background: #10171e;

            border: 1px solid #26313a;

            border-radius: 10px;

            padding: 11px;

            min-width: 0;

        }}


        .pc-signal-card {{

            border-color: #2d684b;

            box-shadow:
                0 0 0 1px
                rgba(70,150,100,.06);

        }}


        .pc-card-header {{

            display: flex;

            align-items: center;

            justify-content: space-between;

            margin-bottom: 8px;

        }}


        .pc-card-title {{

            display: flex;

            align-items: center;

            gap: 7px;

            min-width: 0;

        }}


        .pc-card-title strong {{

            font-size: 16px;

            font-weight: 900;

        }}


        .pc-card-title small {{

            color: #64717b;

            font-size: 9px;

        }}


        .pc-rank {{

            padding: 3px 5px;

            border-radius: 4px;

            font-size: 8px;

            font-weight: 900;

            flex-shrink: 0;

        }}


        .signal-rank {{

            color: #66d99b;

            background: #143c29;

        }}


        .top-rank {{

            color: #adb8c0;

            background: #1d262e;

        }}


        .pc-signal-badge,
        .pc-small-signal {{

            padding: 4px 7px;

            border-radius: 5px;

            color: #69dca0;

            background: #153f2a;

            border: 1px solid #2c754f;

            font-size: 8px;

            font-weight: 900;

        }}


        /* =================================================
           PC 정보
        ================================================= */

        .pc-info-grid {{

            display: grid;

            grid-template-columns:
                1.1fr 1fr .8fr;

            gap: 5px;

        }}


        .pc-info-box {{

            background: #0b1116;

            border: 1px solid #202a33;

            border-radius: 6px;

            padding: 7px;

        }}


        .pc-info-box span {{

            display: block;

            color: #66737e;

            font-size: 8px;

            margin-bottom: 4px;

        }}


        .pc-info-box strong {{

            font-size: 12px;

            white-space: nowrap;

        }}


        .pc-info-box .pc-price {{

            font-size: 14px;

        }}


        /* =================================================
           PC SIGNAL 조건
        ================================================= */

        .pc-signal-condition {{

            display: grid;

            grid-template-columns:
                1fr 25px 1fr;

            gap: 5px;

            align-items: center;

            margin-top: 6px;

        }}


        .pc-signal-condition > div {{

            display: flex;

            justify-content: space-between;

            align-items: center;

            padding: 6px 8px;

            background: #0c1511;

            border: 1px solid #234434;

            border-radius: 6px;

        }}


        .pc-signal-condition span {{

            color: #6d7b74;

            font-size: 9px;

        }}


        .pc-signal-condition b {{

            text-align: center;

            color: #55a878;

        }}


        .pc-current-4h {{

            display: flex;

            justify-content: space-between;

            align-items: center;

            margin-top: 6px;

            padding: 7px 8px;

            background: #0b1116;

            border: 1px solid #202a33;

            border-radius: 6px;

            font-size: 9px;

        }}


        .pc-current-4h span {{

            color: #687580;

        }}


        /* =================================================
           PC 4H
        ================================================= */

        .pc-history-title {{

            color: #65727c;

            font-size: 8px;

            font-weight: 700;

            margin: 7px 0 4px;

        }}


        .pc-four-grid {{

            display: grid;

            grid-template-columns:
                repeat(6, minmax(0, 1fr));

            gap: 2px;

            padding: 2px;

            background: #222c34;

            border-radius: 6px;

        }}


        .four-hour-cell {{

            display: flex;

            flex-direction: column;

            align-items: center;

            justify-content: center;

            min-height: 50px;

            background: #0b1116;

            border-radius: 4px;

            gap: 3px;

        }}


        .four-hour-cell.current-4h {{

            background: #123523;

            border: 1px solid #3d875e;

        }}


        .four-hour-label {{

            color: #687580;

            font-size: 7px;

            white-space: nowrap;

        }}


        .current-4h .four-hour-label {{

            color: #72bc91;

            font-weight: 900;

        }}


        .four-hour-value {{

            font-size: 8px;

            white-space: nowrap;

        }}


        /* =================================================
           PC SIGNAL 없음
        ================================================= */

        .pc-empty-signal {{

            grid-column: 1 / -1;

            display: flex;

            flex-direction: column;

            align-items: center;

            justify-content: center;

            gap: 6px;

            min-height: 100px;

            background: #0d1319;

            border: 1px dashed #34414b;

            border-radius: 10px;

        }}


        .pc-empty-signal strong {{

            color: #aab4bc;

            font-size: 14px;

        }}


        .pc-empty-signal span {{

            color: #687580;

            font-size: 10px;

        }}


        /* =================================================
           모바일 기본은 숨김
        ================================================= */

        .mobile-dashboard {{

            display: none;

        }}


        /* =================================================
           모바일 전용
        ================================================= */

        @media (
            max-width: 700px
        ) {{

            .pc-dashboard {{

                display: none;

            }}


            .mobile-dashboard {{

                display: block;

                width: 100%;

                padding: 8px;

            }}


            body {{

                background: #070b0f;

            }}


            /* =============================================
               Mobile Header
            ============================================= */

            .m-page-header {{

                display: flex;

                align-items: center;

                justify-content: space-between;

                padding: 4px 2px 9px;

            }}


            .m-page-header div {{

                display: flex;

                align-items: baseline;

                gap: 5px;

            }}


            .m-page-header strong {{

                font-size: 18px;

                font-weight: 900;

            }}


            .m-page-header span {{

                color: #65727d;

                font-size: 11px;

                font-weight: 700;

            }}


            .m-page-header small {{

                color: #5d6973;

                font-size: 8px;

            }}


            /* =============================================
               Mobile BTC
            ============================================= */

            .m-btc-card {{

                background: #10171e;

                border: 1px solid #29343d;

                border-radius: 10px;

                padding: 10px;

            }}


            .m-btc-head {{

                display: flex;

                justify-content: space-between;

                align-items: center;

            }}


            .m-btc-head div {{

                display: flex;

                align-items: baseline;

                gap: 5px;

            }}


            .m-btc-head strong {{

                font-size: 18px;

                font-weight: 900;

            }}


            .m-btc-head small {{

                color: #697681;

                font-size: 9px;

            }}


            .m-btc-on,
            .m-btc-off {{

                padding: 4px 7px;

                border-radius: 4px;

                font-size: 8px;

                font-weight: 900;

            }}


            .m-btc-on {{

                color: #64db9a;

                background: #143d29;

                border: 1px solid #2b6c4b;

            }}


            .m-btc-off {{

                color: #ff7782;

                background: #3a171c;

                border: 1px solid #6e3037;

            }}


            .m-btc-price {{

                display: flex;

                justify-content: space-between;

                align-items: baseline;

                margin-top: 8px;

            }}


            .m-btc-price strong {{

                font-size: 21px;

                font-weight: 900;

            }}


            .m-btc-price span {{

                font-size: 11px;

            }}


            .m-btc-bottom {{

                display: grid;

                grid-template-columns: 1fr 1fr;

                gap: 5px;

                margin-top: 8px;

            }}


            .m-btc-bottom span {{

                display: flex;

                justify-content: space-between;

                align-items: center;

                padding: 6px 7px;

                background: #0b1116;

                border: 1px solid #202a33;

                border-radius: 5px;

                color: #687580;

                font-size: 8px;

            }}


            .m-btc-bottom b {{

                color: #dce3e8;

                font-size: 9px;

            }}


            /* =============================================
               Mobile Section
            ============================================= */

            .m-section {{

                margin-top: 12px;

            }}


            .m-section-title {{

                display: flex;

                justify-content: space-between;

                align-items: center;

                margin-bottom: 6px;

            }}


            .m-section-title strong {{

                font-size: 15px;

                font-weight: 900;

            }}


            .m-section-title span {{

                color: #65727d;

                font-size: 8px;

            }}


            .m-signal-list,
            .m-top-list {{

                display: flex;

                flex-direction: column;

                gap: 6px;

            }}


            /* =============================================
               Mobile Card
            ============================================= */

            .m-card {{

                background: #10171e;

                border: 1px solid #26313a;

                border-radius: 8px;

                padding: 8px;

            }}


            .m-signal-card {{

                border-color: #2d674a;

            }}


            .m-card-top {{

                display: flex;

                align-items: center;

                gap: 6px;

                min-height: 20px;

            }}


            .m-rank {{

                flex-shrink: 0;

                padding: 3px 5px;

                background: #1d262e;

                color: #aeb9c1;

                border-radius: 4px;

                font-size: 7px;

                font-weight: 900;

            }}


            .m-signal-card .m-rank {{

                color: #66d99a;

                background: #143c29;

            }}


            .m-coin {{

                font-size: 15px;

                font-weight: 900;

                flex: 1;

            }}


            .m-signal-badge,
            .m-small-signal {{

                flex-shrink: 0;

                padding: 3px 5px;

                background: #153f2a;

                color: #67da9c;

                border: 1px solid #2b704d;

                border-radius: 4px;

                font-size: 7px;

                font-weight: 900;

            }}


            /* =============================================
               Mobile Price Row
            ============================================= */

            .m-price-row {{

                display: grid;

                grid-template-columns:
                    1.2fr 1fr .8fr;

                gap: 5px;

                align-items: center;

                margin-top: 7px;

            }}


            .m-price-row span,
            .m-price-row strong {{

                min-width: 0;

                white-space: nowrap;

                overflow: hidden;

                text-overflow: ellipsis;

            }}


            .m-price {{

                font-size: 15px;

                font-weight: 900;

            }}


            .m-price-row span {{

                color: #687580;

                font-size: 8px;

                text-align: right;

            }}


            /* =============================================
               Mobile Signal
            ============================================= */

            .m-signal-row {{

                display: grid;

                grid-template-columns:
                    1fr 18px 1fr;

                align-items: center;

                gap: 3px;

                margin-top: 6px;

            }}


            .m-signal-row > span {{

                display: flex;

                justify-content: space-between;

                align-items: center;

                padding: 5px 6px;

                background: #0c1511;

                border: 1px solid #244534;

                border-radius: 5px;

                color: #68766e;

                font-size: 8px;

            }}


            .m-signal-row b {{

                font-size: 8px;

            }}


            .m-signal-row i {{

                color: #55a878;

                text-align: center;

                font-style: normal;

                font-weight: 900;

            }}


            /* =============================================
               Mobile Current 4H
            ============================================= */

            .m-current-row {{

                display: flex;

                justify-content: space-between;

                align-items: center;

                margin-top: 6px;

                padding: 6px 7px;

                background: #0b1116;

                border: 1px solid #202a33;

                border-radius: 5px;

                font-size: 8px;

            }}


            .m-current-row span {{

                color: #687580;

            }}


            .m-current-row strong {{

                font-size: 9px;

            }}


            /* =============================================
               Mobile 4H
            ============================================= */

            .m-four-grid {{

                display: grid;

                grid-template-columns:
                    repeat(3, 1fr);

                gap: 2px;

                padding: 2px;

                margin-top: 6px;

                background: #202a32;

                border-radius: 5px;

            }}


            .m-four-grid .four-hour-cell {{

                min-height: 40px;

                gap: 2px;

            }}


            .m-four-grid .four-hour-label {{

                font-size: 7px;

            }}


            .m-four-grid .four-hour-value {{

                font-size: 8px;

            }}


            /* =============================================
               Mobile Empty
            ============================================= */

            .m-empty-signal {{

                display: flex;

                flex-direction: column;

                align-items: center;

                justify-content: center;

                gap: 4px;

                padding: 18px 10px;

                background: #0d1319;

                border: 1px dashed #33404a;

                border-radius: 8px;

            }}


            .m-empty-signal strong {{

                font-size: 13px;

                color: #aab4bc;

            }}


            .m-empty-signal span {{

                font-size: 8px;

                color: #687580;

            }}

        }}

        </style>

    </head>


    <body>

        {pc_dashboard()}

        {mobile_dashboard()}

    </body>

    </html>
    """


# =========================================================
# FastAPI
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def home():

    return HTMLResponse(
        dashboard()
    )


# =========================================================
# Scheduler
# =========================================================

def scheduler_loop():

    log.info(
        "스케줄러 시작"
    )

    while True:

        try:

            schedule.run_pending()

        except Exception as e:

            log.exception(
                "스케줄러 오류: %s",
                e
            )

        time.sleep(1)


# =========================================================
# 초기 업데이트
# =========================================================

def initial_update():

    try:

        log.info(
            "초기 데이터 업데이트 시작"
        )

        update_all()

        log.info(
            "초기 데이터 업데이트 완료"
        )

    except Exception as e:

        log.exception(
            "초기 업데이트 실패: %s",
            e
        )


# =========================================================
# Startup
# =========================================================

@app.on_event(
    "startup"
)
def startup_event():

    threading.Thread(
        target=initial_update,
        daemon=True
    ).start()

    schedule.every(
        UPDATE_MINUTES
    ).minutes.do(
        update_all
    )

    threading.Thread(
        target=scheduler_loop,
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
