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

warnings.filterwarnings("ignore", category=FutureWarning)

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
# 전역 변수
# =========================================================

latest_upbit_data = []

latest_okx_data = []

latest_upbit_update_time = "-"
latest_okx_update_time = "-"

latest_upbit_markets = []

latest_btc_okx_price = None
latest_btc_daily_change = None

latest_btc_4h_periods = []
latest_btc_current_4h_change = None
latest_btc_current_4h_label = "-"

request_lock = threading.Lock()
update_lock = threading.Lock()

last_request_time = 0


# =========================================================
# 4시간봉 정의
#
# 업비트 기준 KST
#
# 01 ~ 05
# 05 ~ 09
# 09 ~ 13
# 13 ~ 17
# 17 ~ 21
# 21 ~ 01
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
            time.sleep(REQUEST_INTERVAL - diff)

        last_request_time = time.time()


def request_get(url, params=None, headers=None, timeout=10):

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

                wait_time = RATE_LIMIT_WAIT * (attempt + 1)

                log.warning(
                    "429 Too Many Requests -> %.1f초 대기",
                    wait_time
                )

                time.sleep(wait_time)

                continue

            if response.status_code >= 500:

                wait_time = min(
                    RATE_LIMIT_WAIT * (attempt + 1),
                    10
                )

                time.sleep(wait_time)

                continue

            log.warning(
                "HTTP %s : %s",
                response.status_code,
                url
            )

            return None

        except Exception as e:

            log.warning(
                "Request error %s : %s",
                attempt + 1,
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
# 숫자
# =========================================================

def safe_float(value):

    try:
        return float(value)

    except Exception:
        return None


# =========================================================
# 업비트 마켓
# =========================================================

def get_upbit_markets():

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

        markets = response.json()

    except Exception:

        return []

    krw_markets = [
        x["market"]
        for x in markets
        if x.get("market", "").startswith("KRW-")
    ]

    if not krw_markets:
        return []

    ticker_data = []

    chunk_size = 100

    for i in range(
        0,
        len(krw_markets),
        chunk_size
    ):

        chunk = krw_markets[
            i:i + chunk_size
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

            data = response.json()

            if isinstance(data, list):
                ticker_data.extend(data)

        except Exception:
            continue

    result = []

    for item in ticker_data:

        market = item.get("market")

        if not market:
            continue

        volume = safe_float(
            item.get("acc_trade_price_24h")
        )

        price = safe_float(
            item.get("trade_price")
        )

        if volume is None:
            volume = 0

        if price is None:
            continue

        result.append({
            "market": market,
            "volume": volume,
            "price": price
        })

    return result


# =========================================================
# 업비트 일봉 변화율
#
# 업비트 일봉 기준 09:00 KST
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

    if not isinstance(candles, list):
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
            (current_price - previous_close)
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
            "count": min(count, 200)
        }
    )

    if response is None:
        return []

    try:

        data = response.json()

    except Exception:

        return []

    if not isinstance(data, list):
        return []

    return data


# =========================================================
# 4H 시작시간
# =========================================================

def get_current_4h_start(now=None):

    if now is None:
        now = datetime.now(KST)

    now = now.astimezone(KST)

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

        # 00:00 ~ 00:59
        # 전일 21:00 ~ 현재 01:00

        start_hour = 21

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
        now = datetime.now(KST)

    start = start.astimezone(KST)

    end = start + timedelta(hours=4)

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
# 최근 4H 6개
# =========================================================

def get_recent_4h_periods(
    count=6,
    now=None
):

    if now is None:
        now = datetime.now(KST)

    current_start = get_current_4h_start(now)

    periods = []

    for i in range(
        count - 1,
        -1,
        -1
    ):

        start = (
            current_start
            - timedelta(hours=4 * i)
        )

        active = (
            i == 0
            and start <= now < start + timedelta(hours=4)
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
# 현재 / 이전 4H
# =========================================================

def get_current_4h_period(now=None):

    periods = get_recent_4h_periods(
        count=1,
        now=now
    )

    return periods[0]


def get_previous_4h_period(now=None):

    if now is None:
        now = datetime.now(KST)

    current_start = get_current_4h_start(now)

    start = (
        current_start
        - timedelta(hours=4)
    )

    return make_4h_period(
        start,
        now=now,
        active=False
    )


# =========================================================
# 업비트 1H → 4H
# =========================================================

def build_upbit_4h_candles(
    market,
    current_price,
    candles=None,
    now=None
):

    if now is None:
        now = datetime.now(KST)

    if candles is None:
        candles = get_upbit_60m_candles(
            market,
            count=200
        )

    periods = get_recent_4h_periods(
        count=6,
        now=now
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

    df = pd.DataFrame(rows)

    for idx, period in enumerate(periods):

        start = period["start"]
        end = period["end"]

        subset = df[
            (df["datetime"] >= start)
            & (df["datetime"] < end)
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

        # 현재 진행 중인 4H봉은
        # 실제 현재가를 종가로 사용
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
                (close_price - open_price)
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
# 업비트 4H 변화
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
        candles=candles,
        now=now
    )

    values = [
        x["change"]
        for x in periods
    ]

    return {
        "periods": periods,
        "values": values
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

        rows = data.get("data", [])

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

def get_okx_btc_1h_candles(
    limit=300
):

    response = request_get(
        f"{OKX_BASE_URL}/api/v5/market/candles",
        params={
            "instId": OKX_BTC_INST_ID,
            "bar": "1H",
            "limit": min(limit, 300)
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

    candles = get_okx_btc_1h_candles(
        limit=300
    )

    if not candles:
        return pd.DataFrame()

    rows = []

    for candle in candles:

        try:

            timestamp = int(
                candle[0]
            )

            dt_utc = datetime.fromtimestamp(
                timestamp / 1000,
                tz=ZoneInfo("UTC")
            )

            dt_kst = dt_utc.astimezone(KST)

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

    df = pd.DataFrame(rows)

    df = df.drop_duplicates(
        subset=["datetime"]
    )

    df = df.sort_values(
        "datetime"
    )

    return df


# =========================================================
# BTC 일봉 09:00 기준
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
# BTC 일봉 변화율
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

    now = datetime.now(KST)

    current_start = (
        now.replace(
            hour=9,
            minute=0,
            second=0,
            microsecond=0
        )
    )

    if now.hour < 9:

        current_start -= timedelta(
            days=1
        )

    current_rows = daily[
        daily["daily_start"] == current_start
    ]

    if current_rows.empty:

        previous_rows = daily[
            daily["daily_start"] < current_start
        ]

        if previous_rows.empty:
            return None

        base = float(
            previous_rows.iloc[-1]["open"]
        )

    else:

        base = float(
            current_rows.iloc[-1]["open"]
        )

    if base == 0:
        return None

    return (
        (price - base)
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
        now = datetime.now(KST)

    periods = get_recent_4h_periods(
        count=6,
        now=now
    )

    if df is None or df.empty:
        return periods

    for idx, period in enumerate(periods):

        start = period["start"]
        end = period["end"]

        subset = df[
            (df["datetime"] >= start)
            & (df["datetime"] < end)
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

        if period["active"] and price is not None:

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
                (close_price - open_price)
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
# BTC 시장 업데이트
# =========================================================

def update_btc_market():

    global latest_btc_okx_price
    global latest_btc_daily_change
    global latest_btc_4h_periods
    global latest_btc_current_4h_change
    global latest_btc_current_4h_label
    global latest_okx_update_time

    try:

        price = get_okx_btc_price()

        df = build_okx_btc_dataframe()

        if price is None:

            log.warning(
                "BTC 가격 조회 실패"
            )

            return

        now = datetime.now(KST)

        daily_change = get_okx_btc_daily_change(
            price,
            df
        )

        periods = build_okx_btc_4h_candles(
            price,
            df,
            now=now
        )

        current_period = get_current_4h_period(
            now
        )

        current_change = None

        for period in periods:

            if (
                period["start"]
                == current_period["start"]
            ):

                current_change = period["change"]

                break

        latest_btc_okx_price = price

        latest_btc_daily_change = daily_change

        latest_btc_4h_periods = periods

        latest_btc_current_4h_change = current_change

        latest_btc_current_4h_label = (
            current_period["label"]
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
    candles=None,
    now=None
):

    if now is None:
        now = datetime.now(KST)

    daily_change = daily_change_upbit(
        market,
        current_price
    )

    four_hour = get_upbit_4h_changes(
        market,
        current_price,
        candles=candles,
        now=now
    )

    periods = four_hour.get(
        "periods",
        []
    )

    current_period = get_current_4h_period(
        now
    )

    current_4h = None
    previous_4h = None

    for i, period in enumerate(periods):

        if (
            period["start"]
            == current_period["start"]
        ):

            current_4h = period["change"]

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
# 표시용 이름
# =========================================================

def coin_name(market):

    if not market:
        return "-"

    if market.startswith("KRW-"):
        return market.replace(
            "KRW-",
            ""
        )

    return market


# =========================================================
# 가격 포맷
# =========================================================

def format_market_price(price):

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
# 거래대금 포맷
# =========================================================

def format_volume(value):

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
# 상승률 포맷
# =========================================================

def format_change(value):

    if value is None:

        return (
            '<span class="change neutral">-</span>'
        )

    try:

        value = float(value)

    except Exception:

        return (
            '<span class="change neutral">-</span>'
        )

    if value > 0:

        return (
            '<span class="change positive">'
            f'▲ +{value:.2f}%'
            '</span>'
        )

    if value < 0:

        return (
            '<span class="change negative">'
            f'▼ {value:.2f}%'
            '</span>'
        )

    return (
        '<span class="change neutral">'
        '0.00%'
        '</span>'
    )


# =========================================================
# 4H 셀
# =========================================================

def four_hour_cells_html(
    periods
):

    if not periods:

        return (
            '<div class="empty-history">'
            '4H 데이터 없음'
            '</div>'
        )

    cells = []

    for period in periods:

        change = period.get(
            "change"
        )

        change_html = format_change(
            change
        )

        classes = [
            "four-hour-cell"
        ]

        if period.get("active"):

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

        cells.append(
            f"""
            <div class="{' '.join(classes)}">
                <div class="four-hour-label">
                    {label}
                </div>

                <div class="four-hour-change">
                    {change_html}
                </div>
            </div>
            """
        )

    return "".join(cells)


# =========================================================
# BTC 4H 카드
# =========================================================

def btc_4h_cells_html():

    return four_hour_cells_html(
        latest_btc_4h_periods
    )


# =========================================================
# Row
# =========================================================

def make_row(
    rank,
    market,
    volume,
    current_price,
    analysis
):

    daily_change = analysis.get(
        "daily_change"
    )

    current_4h = analysis.get(
        "current_4h_change"
    )

    previous_4h = analysis.get(
        "previous_4h_change"
    )

    return {
        "rank": rank,
        "market": market,
        "name": coin_name(market),
        "volume": volume,
        "volume_text": format_volume(
            volume
        ),
        "current_price": current_price,
        "current_price_text": format_market_price(
            current_price
        ),
        "daily_change": daily_change,
        "daily_html": format_change(
            daily_change
        ),
        "four_hour_periods": analysis.get(
            "four_hour_periods",
            []
        ),
        "current_4h_change": current_4h,
        "previous_4h_change": previous_4h,
        "signal_pass": False
    }


# =========================================================
# 업비트 전체 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update_time
    global latest_upbit_markets

    with update_lock:

        try:

            now = datetime.now(KST)

            markets = get_upbit_markets()

            if not markets:

                log.warning(
                    "업비트 마켓 데이터 없음"
                )

                return

            latest_upbit_markets = markets

            markets.sort(
                key=lambda x: x["volume"],
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

                market = item["market"]

                price = item["price"]

                try:

                    candles = get_upbit_60m_candles(
                        market,
                        count=200
                    )

                    analysis = analyze(
                        market,
                        price,
                        candles=candles,
                        now=now
                    )

                    row = make_row(
                        rank,
                        market,
                        item["volume"],
                        price,
                        analysis
                    )

                    rows.append(row)

                except Exception as e:

                    log.warning(
                        "%s 분석 실패: %s",
                        market,
                        e
                    )

                    continue

            # =================================================
            # SIGNAL
            #
            # 1. BTC 현재 4H 양수
            # 2. 코인 현재 4H 양수
            # 3. 코인 이전 4H 양수
            # =================================================

            btc_pass = (
                latest_btc_current_4h_change
                is not None
                and latest_btc_current_4h_change > 0
            )

            for row in rows:

                coin_current_pass = (
                    row["current_4h_change"]
                    is not None
                    and row["current_4h_change"] > 0
                )

                coin_previous_pass = (
                    row["previous_4h_change"]
                    is not None
                    and row["previous_4h_change"] > 0
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
                "UPBIT 업데이트 완료 / TOP%d / SIGNAL %d",
                len(rows),
                signal_count
            )

        except Exception as e:

            log.exception(
                "업비트 업데이트 오류: %s",
                e
            )


# =========================================================
# 전체 데이터 업데이트
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
# SIGNAL 카드
# =========================================================

def signal_card_html(
    row,
    signal_rank
):

    name = html.escape(
        str(row["name"])
    )

    market = html.escape(
        str(row["market"])
    )

    current_price = html.escape(
        str(row["current_price_text"])
    )

    volume = html.escape(
        str(row["volume_text"])
    )

    daily_html = row["daily_html"]

    current_4h = format_change(
        row["current_4h_change"]
    )

    previous_4h = format_change(
        row["previous_4h_change"]
    )

    history = four_hour_cells_html(
        row["four_hour_periods"]
    )

    return f"""
    <div class="coin-card signal-card">

        <div class="coin-card-header">

            <div class="coin-title">

                <div class="coin-rank signal-rank">
                    SIGNAL {signal_rank}
                </div>

                <div class="coin-name">
                    {name}
                </div>

                <div class="coin-market">
                    {market}
                </div>

            </div>

            <div class="signal-badge">
                SIGNAL
            </div>

        </div>


        <div class="coin-main-info">

            <div class="info-box">
                <div class="info-label">
                    현재가
                </div>

                <div class="info-value price">
                    {current_price}
                </div>
            </div>


            <div class="info-box">
                <div class="info-label">
                    24H 거래대금
                </div>

                <div class="info-value">
                    {volume}
                </div>
            </div>


            <div class="info-box">
                <div class="info-label">
                    일간
                </div>

                <div class="info-value">
                    {daily_html}
                </div>
            </div>

        </div>


        <div class="signal-condition">

            <div class="condition-item">
                <span>
                    현재 4H
                </span>

                <strong>
                    {current_4h}
                </strong>
            </div>


            <div class="condition-arrow">
                →
            </div>


            <div class="condition-item">
                <span>
                    이전 4H
                </span>

                <strong>
                    {previous_4h}
                </strong>
            </div>

        </div>


        <div class="history-title">
            최근 4시간 흐름
        </div>

        <div class="four-hour-grid signal-history">
            {history}
        </div>

    </div>
    """


# =========================================================
# SIGNAL 영역
# =========================================================

def focus_section(data):

    signal_rows = [
        row
        for row in data
        if row.get("signal_pass")
    ]

    # 현재 4H 상승률이 높은 순
    signal_rows.sort(
        key=lambda x: (
            x["current_4h_change"]
            if x["current_4h_change"] is not None
            else -999999
        ),
        reverse=True
    )

    btc_pass = (
        latest_btc_current_4h_change
        is not None
        and latest_btc_current_4h_change > 0
    )

    btc_change_html = format_change(
        latest_btc_current_4h_change
    )

    if not btc_pass:

        return f"""
        <div class="signal-off-card">

            <div class="signal-off-title">
                SIGNAL OFF
            </div>

            <div class="signal-off-text">
                BTC 현재 4H가 양수가 아니므로
                SIGNAL을 표시하지 않습니다.
            </div>

            <div class="signal-off-btc">
                BTC 현재 4H
                {btc_change_html}
            </div>

        </div>
        """

    if not signal_rows:

        return f"""
        <div class="signal-off-card">

            <div class="signal-off-title">
                SIGNAL 대기
            </div>

            <div class="signal-off-text">
                현재 조건을 동시에 만족하는 종목이 없습니다.
            </div>

            <div class="signal-off-btc">
                BTC 현재 4H
                {btc_change_html}
            </div>

        </div>
        """

    cards = []

    for idx, row in enumerate(
        signal_rows,
        start=1
    ):

        cards.append(
            signal_card_html(
                row,
                idx
            )
        )

    return "".join(cards)


# =========================================================
# TOP 카드
# =========================================================

def top_card_html(row):

    rank = row["rank"]

    name = html.escape(
        str(row["name"])
    )

    market = html.escape(
        str(row["market"])
    )

    current_price = html.escape(
        str(row["current_price_text"])
    )

    volume = html.escape(
        str(row["volume_text"])
    )

    daily_html = row["daily_html"]

    current_4h = format_change(
        row["current_4h_change"]
    )

    history = four_hour_cells_html(
        row["four_hour_periods"]
    )

    signal_badge = ""

    if row.get("signal_pass"):

        signal_badge = """
        <div class="small-signal-badge">
            SIGNAL
        </div>
        """

    return f"""
    <div class="coin-card top-card">

        <div class="coin-card-header">

            <div class="coin-title">

                <div class="coin-rank top-rank">
                    TOP {rank}
                </div>

                <div class="coin-name">
                    {name}
                </div>

                <div class="coin-market">
                    {market}
                </div>

            </div>

            {signal_badge}

        </div>


        <div class="coin-main-info">

            <div class="info-box">
                <div class="info-label">
                    현재가
                </div>

                <div class="info-value price">
                    {current_price}
                </div>
            </div>


            <div class="info-box">
                <div class="info-label">
                    24H 거래대금
                </div>

                <div class="info-value volume-value">
                    {volume}
                </div>
            </div>


            <div class="info-box">
                <div class="info-label">
                    일간
                </div>

                <div class="info-value">
                    {daily_html}
                </div>
            </div>

        </div>


        <div class="top-current-row">

            <span>
                현재 4H
            </span>

            <strong>
                {current_4h}
            </strong>

        </div>


        <div class="history-title">
            최근 4시간 흐름
        </div>

        <div class="four-hour-grid">
            {history}
        </div>

    </div>
    """


# =========================================================
# BTC 시장 카드
# =========================================================

def market_summary_html():

    price = format_market_price(
        latest_btc_okx_price
    )

    daily = format_change(
        latest_btc_daily_change
    )

    current_4h = format_change(
        latest_btc_current_4h_change
    )

    btc_signal_on = (
        latest_btc_current_4h_change
        is not None
        and latest_btc_current_4h_change > 0
    )

    if btc_signal_on:

        status_html = """
        <span class="market-status on">
            ON
        </span>
        """

    else:

        status_html = """
        <span class="market-status off">
            OFF
        </span>
        """

    return f"""
    <div class="btc-market-card">

        <div class="btc-header">

            <div>

                <div class="btc-title">
                    BTC MARKET
                </div>

                <div class="btc-subtitle">
                    OKX BTC-USDT
                </div>

            </div>

            <div>
                {status_html}
            </div>

        </div>


        <div class="btc-main-grid">

            <div class="btc-stat">

                <div class="btc-stat-label">
                    BTC 현재가
                </div>

                <div class="btc-stat-value price">
                    {price}
                </div>

            </div>


            <div class="btc-stat">

                <div class="btc-stat-label">
                    일간
                </div>

                <div class="btc-stat-value">
                    {daily}
                </div>

            </div>


            <div class="btc-stat">

                <div class="btc-stat-label">
                    현재 4H
                </div>

                <div class="btc-stat-value">
                    {current_4h}
                </div>

            </div>


            <div class="btc-stat">

                <div class="btc-stat-label">
                    SIGNAL
                </div>

                <div class="btc-stat-value">
                    {status_html}
                </div>

            </div>

        </div>


        <div class="history-title btc-history-title">
            BTC 최근 4시간 흐름
        </div>

        <div class="four-hour-grid btc-history">
            {btc_4h_cells_html()}
        </div>

    </div>
    """


# =========================================================
# Dashboard
# =========================================================

def dashboard():

    data = latest_upbit_data

    signal_html = focus_section(
        data
    )

    top_cards = []

    for row in data:

        top_cards.append(
            top_card_html(row)
        )

    top_html = "".join(
        top_cards
    )

    return f"""
    <!DOCTYPE html>

    <html lang="ko">

    <head>

        <meta charset="UTF-8">

        <meta
            name="viewport"
            content="width=device-width, initial-scale=1.0"
        >

        <meta
            http-equiv="refresh"
            content="60"
        >

        <title>
            Crypto Market Dashboard
        </title>


        <style>

            * {{
                box-sizing: border-box;
            }}


            html {{
                background: #070b0f;
            }}


            body {{

                margin: 0;

                padding: 14px;

                background:
                    linear-gradient(
                        180deg,
                        #070b0f 0%,
                        #0a0f14 100%
                    );

                color: #e9eef3;

                font-family:
                    Arial,
                    "Noto Sans KR",
                    sans-serif;

                min-height: 100vh;

            }}


            .dashboard {{
                width: 100%;
                max-width: 1500px;
                margin: 0 auto;
            }}


            /* =========================================
               상단
            ========================================= */

            .page-header {{

                display: flex;

                align-items: center;

                justify-content: space-between;

                margin-bottom: 12px;

            }}


            .page-title {{

                font-size: 22px;

                font-weight: 800;

                letter-spacing: -0.5px;

            }}


            .page-time {{

                font-size: 11px;

                color: #7f8a95;

            }}


            /* =========================================
               BTC
            ========================================= */

            .btc-market-card {{

                background: #10171e;

                border: 1px solid #26313b;

                border-radius: 12px;

                padding: 14px;

                margin-bottom: 14px;

                box-shadow:
                    0 4px 16px
                    rgba(0,0,0,.22);

            }}


            .btc-header {{

                display: flex;

                justify-content: space-between;

                align-items: center;

                margin-bottom: 12px;

            }}


            .btc-title {{

                font-size: 18px;

                font-weight: 800;

            }}


            .btc-subtitle {{

                margin-top: 3px;

                font-size: 10px;

                color: #77838f;

            }}


            .btc-main-grid {{

                display: grid;

                grid-template-columns:
                    repeat(4, 1fr);

                gap: 7px;

            }}


            .btc-stat {{

                background: #0b1116;

                border: 1px solid #202a33;

                border-radius: 8px;

                padding: 9px;

                min-width: 0;

            }}


            .btc-stat-label {{

                color: #77838f;

                font-size: 10px;

                margin-bottom: 5px;

            }}


            .btc-stat-value {{

                font-size: 15px;

                font-weight: 700;

                white-space: nowrap;

            }}


            .market-status {{

                display: inline-flex;

                align-items: center;

                justify-content: center;

                min-width: 44px;

                padding: 4px 8px;

                border-radius: 5px;

                font-size: 10px;

                font-weight: 800;

            }}


            .market-status.on {{

                background: #143b29;

                color: #61d89a;

                border: 1px solid #286a4a;

            }}


            .market-status.off {{

                background: #3a171b;

                color: #ff7c86;

                border: 1px solid #713038;

            }}


            /* =========================================
               섹션
            ========================================= */

            .section {{

                margin-top: 14px;

            }}


            .section-header {{

                display: flex;

                justify-content: space-between;

                align-items: center;

                margin-bottom: 8px;

            }}


            .section-title {{

                font-size: 16px;

                font-weight: 800;

            }}


            .section-subtitle {{

                color: #697580;

                font-size: 10px;

            }}


            .update-bar {{

                background: #0c1218;

                border: 1px solid #1d2730;

                border-radius: 7px;

                padding: 7px 9px;

                margin-bottom: 8px;

                color: #71808b;

                font-size: 10px;

            }}


            /* =========================================
               SIGNAL GRID
            ========================================= */

            .signal-card-list {{

                display: grid;

                grid-template-columns:
                    repeat(2, minmax(0, 1fr));

                gap: 10px;

            }}


            /* =========================================
               TOP GRID
            ========================================= */

            .top-card-list {{

                display: grid;

                grid-template-columns:
                    repeat(2, minmax(0, 1fr));

                gap: 10px;

            }}


            /* =========================================
               코인 카드
            ========================================= */

            .coin-card {{

                background: #10171e;

                border: 1px solid #27323b;

                border-radius: 10px;

                padding: 10px;

                min-width: 0;

            }}


            .coin-card.signal-card {{

                border-color: #2f684c;

                box-shadow:
                    0 0 0 1px
                    rgba(58,140,96,.08);

            }}


            .coin-card-header {{

                display: flex;

                align-items: center;

                justify-content: space-between;

                margin-bottom: 9px;

                min-width: 0;

            }}


            .coin-title {{

                display: flex;

                align-items: center;

                gap: 6px;

                min-width: 0;

            }}


            .coin-rank {{

                flex-shrink: 0;

                font-size: 9px;

                font-weight: 800;

                padding: 3px 5px;

                border-radius: 4px;

            }}


            .signal-rank {{

                background: #163f2a;

                color: #68d99d;

            }}


            .top-rank {{

                background: #1c252d;

                color: #aeb9c2;

            }}


            .coin-name {{

                font-size: 16px;

                font-weight: 800;

                white-space: nowrap;

            }}


            .coin-market {{

                color: #66737e;

                font-size: 9px;

                white-space: nowrap;

            }}


            .signal-badge,
            .small-signal-badge {{

                flex-shrink: 0;

                background: #16462e;

                color: #6de2a1;

                border: 1px solid #2d7950;

                border-radius: 5px;

                padding: 4px 7px;

                font-size: 9px;

                font-weight: 800;

            }}


            /* =========================================
               코인 기본 정보
            ========================================= */

            .coin-main-info {{

                display: grid;

                grid-template-columns:
                    1.15fr
                    1fr
                    .8fr;

                gap: 5px;

            }}


            .info-box {{

                background: #0b1116;

                border: 1px solid #1f2932;

                border-radius: 6px;

                padding: 7px;

                min-width: 0;

            }}


            .info-label {{

                color: #66737e;

                font-size: 9px;

                margin-bottom: 4px;

            }}


            .info-value {{

                font-size: 12px;

                font-weight: 700;

                white-space: nowrap;

                overflow: hidden;

                text-overflow: ellipsis;

            }}


            .info-value.price {{

                font-size: 14px;

            }}


            .volume-value {{

                color: #d8dee3;

            }}


            /* =========================================
               SIGNAL 조건
            ========================================= */

            .signal-condition {{

                display: grid;

                grid-template-columns:
                    1fr 22px 1fr;

                align-items: center;

                gap: 5px;

                margin-top: 7px;

            }}


            .condition-item {{

                display: flex;

                justify-content: space-between;

                align-items: center;

                background: #0c1511;

                border: 1px solid #244535;

                border-radius: 6px;

                padding: 6px 8px;

                font-size: 10px;

            }}


            .condition-item span {{

                color: #718078;

            }}


            .condition-item strong {{

                font-size: 11px;

            }}


            .condition-arrow {{

                text-align: center;

                color: #55a579;

                font-weight: 800;

            }}


            /* =========================================
               TOP 현재 4H
            ========================================= */

            .top-current-row {{

                display: flex;

                align-items: center;

                justify-content: space-between;

                background: #0c1217;

                border: 1px solid #202b34;

                border-radius: 6px;

                padding: 7px 8px;

                margin-top: 7px;

                font-size: 10px;

            }}


            .top-current-row span {{

                color: #74818c;

            }}


            .top-current-row strong {{

                font-size: 11px;

            }}


            /* =========================================
               4H
            ========================================= */

            .history-title {{

                margin-top: 8px;

                margin-bottom: 4px;

                color: #66737e;

                font-size: 9px;

                font-weight: 700;

            }}


            .four-hour-grid {{

                display: grid;

                grid-template-columns:
                    repeat(6, minmax(0, 1fr));

                gap: 3px;

                background: #202a32;

                padding: 2px;

                border-radius: 6px;

            }}


            .four-hour-cell {{

                display: flex;

                flex-direction: column;

                align-items: center;

                justify-content: center;

                min-height: 54px;

                padding: 4px 2px;

                background: #0b1116;

                border-radius: 4px;

                gap: 4px;

                min-width: 0;

            }}


            .four-hour-cell.current-4h {{

                background: #123523;

                border: 1px solid #3f8a61;

                box-shadow:
                    inset 0 0 0 1px
                    rgba(89,180,123,.1);

            }}


            .four-hour-label {{

                font-size: 8px;

                color: #697680;

                white-space: nowrap;

                transform: scale(.95);

            }}


            .current-4h .four-hour-label {{

                color: #77bd96;

                font-weight: 800;

            }}


            .four-hour-change {{

                font-size: 9px;

                white-space: nowrap;

            }}


            /* =========================================
               변화율
            ========================================= */

            .change {{

                font-weight: 800;

                white-space: nowrap;

            }}


            .positive {{

                color: #51d58e;

            }}


            .negative {{

                color: #ff6874;

            }}


            .neutral {{

                color: #7c8790;

            }}


            /* =========================================
               SIGNAL OFF
            ========================================= */

            .signal-off-card {{

                background: #0d1319;

                border: 1px dashed #33404a;

                border-radius: 10px;

                padding: 18px;

                text-align: center;

            }}


            .signal-off-title {{

                font-size: 15px;

                font-weight: 800;

                color: #a8b2ba;

            }}


            .signal-off-text {{

                margin-top: 7px;

                font-size: 11px;

                color: #687580;

            }}


            .signal-off-btc {{

                margin-top: 10px;

                font-size: 12px;

            }}


            /* =========================================
               빈 데이터
            ========================================= */

            .empty-history {{

                grid-column: 1 / -1;

                padding: 12px;

                text-align: center;

                color: #65717b;

                font-size: 10px;

                background: #0b1116;

                border-radius: 5px;

            }}


            /* =========================================
               모바일
            ========================================= */

            @media (max-width: 800px) {{

                body {{
                    padding: 8px;
                }}


                .page-title {{
                    font-size: 18px;
                }}


                .btc-main-grid {{

                    grid-template-columns:
                        repeat(2, 1fr);

                }}


                .signal-card-list,
                .top-card-list {{

                    display: flex;

                    flex-direction: column;

                    gap: 8px;

                }}


                .coin-name {{
                    font-size: 15px;
                }}

            }}


            @media (max-width: 500px) {{

                .page-header {{

                    align-items: flex-end;

                }}


                .page-time {{

                    font-size: 8px;

                }}


                .btc-market-card {{

                    padding: 10px;

                }}


                .btc-title {{

                    font-size: 16px;

                }}


                .btc-stat-value {{

                    font-size: 13px;

                }}


                .coin-main-info {{

                    grid-template-columns:
                        1fr
                        1fr
                        .85fr;

                }}


                .info-box {{

                    padding: 6px;

                }}


                .info-value.price {{

                    font-size: 13px;

                }}


                .four-hour-grid {{

                    grid-template-columns:
                        repeat(3, 1fr);

                    gap: 2px;

                }}


                .four-hour-cell {{

                    min-height: 47px;

                }}


                .four-hour-label {{

                    font-size: 8px;

                }}


                .four-hour-change {{

                    font-size: 9px;

                }}


                .signal-condition {{

                    grid-template-columns:
                        1fr
                        20px
                        1fr;

                }}

            }}

        </style>

    </head>


    <body>

        <div class="dashboard">


            <div class="page-header">

                <div class="page-title">
                    CRYPTO MARKET
                </div>

                <div class="page-time">
                    UPBIT {html.escape(latest_upbit_update_time)}
                </div>

            </div>


            <!-- =====================================
                 BTC MARKET
            ====================================== -->

            {market_summary_html()}


            <!-- =====================================
                 SIGNAL
            ====================================== -->

            <div class="section">

                <div class="section-header">

                    <div class="section-title">
                        SIGNAL
                    </div>

                    <div class="section-subtitle">
                        BTC 상승 + 현재 4H 상승 + 이전 4H 상승
                    </div>

                </div>


                <div class="update-bar">

                    BTC 현재 4H :
                    {format_change(
                        latest_btc_current_4h_change
                    )}

                    &nbsp;&nbsp;|&nbsp;&nbsp;

                    기준 :
                    비트 시황 + 거래대금 + 상승률

                </div>


                <div class="signal-card-list">

                    {signal_html}

                </div>

            </div>


            <!-- =====================================
                 TOP20
            ====================================== -->

            <div class="section">

                <div class="section-header">

                    <div class="section-title">
                        TOP {TOP_N}
                    </div>

                    <div class="section-subtitle">
                        24H 거래대금 순
                    </div>

                </div>


                <div class="update-bar">

                    업비트 거래대금 기준 TOP {TOP_N}

                    &nbsp;&nbsp;|&nbsp;&nbsp;

                    업데이트 :
                    {html.escape(
                        latest_upbit_update_time
                    )}

                </div>


                <div class="top-card-list">

                    {top_html}

                </div>

            </div>


        </div>

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
# 백그라운드 업데이트
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
# 시작
# =========================================================

@app.on_event("startup")
def startup_event():

    # 초기 데이터
    threading.Thread(
        target=initial_update,
        daemon=True
    ).start()

    # 1분마다 업데이트
    schedule.every(
        UPDATE_MINUTES
    ).minutes.do(
        update_all
    )

    # 스케줄러
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
