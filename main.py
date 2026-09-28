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
# 설정
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
# 전역 데이터
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
# OKX
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
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


def now_kst():
    return datetime.now(KST)


# =========================================================
# 현재 12시간 구간
#
# 09:00 ~ 21:00
# 21:00 ~ 다음날 09:00
# =========================================================

def get_current_12h_period():
    now = now_kst()

    if 9 <= now.hour < 21:
        return {
            "label": "09:00 ~ 21:00",
            "key": "09_21"
        }

    return {
        "label": "21:00 ~ 09:00",
        "key": "21_09"
    }


def get_12h_periods(now=None):
    """
    현재 시간을 기준으로
    현재 진행 중인 12시간봉과 직전 12시간봉의
    시작/종료 시간을 계산한다.
    """

    if now is None:
        now = now_kst()

    today = now.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0
    )

    today_09 = today + timedelta(hours=9)
    today_21 = today + timedelta(hours=21)

    yesterday_09 = today_09 - timedelta(days=1)
    yesterday_21 = today_21 - timedelta(days=1)

    tomorrow_09 = today_09 + timedelta(days=1)

    # -----------------------------------------
    # 09:00 ~ 21:00
    # -----------------------------------------

    if 9 <= now.hour < 21:

        current = {
            "key": "09_21",
            "label": "09:00 ~ 21:00",
            "start": today_09,
            "end": today_21,
            "active": True
        }

        previous = {
            "key": "21_09",
            "label": "21:00 ~ 09:00",
            "start": yesterday_21,
            "end": today_09,
            "active": False
        }

    # -----------------------------------------
    # 21:00 ~ 24:00
    # -----------------------------------------

    elif now.hour >= 21:

        current = {
            "key": "21_09",
            "label": "21:00 ~ 09:00",
            "start": today_21,
            "end": tomorrow_09,
            "active": True
        }

        previous = {
            "key": "09_21",
            "label": "09:00 ~ 21:00",
            "start": today_09,
            "end": today_21,
            "active": False
        }

    # -----------------------------------------
    # 00:00 ~ 08:59
    # -----------------------------------------

    else:

        current = {
            "key": "21_09",
            "label": "21:00 ~ 09:00",
            "start": yesterday_21,
            "end": today_09,
            "active": True
        }

        previous = {
            "key": "09_21",
            "label": "09:00 ~ 21:00",
            "start": yesterday_09,
            "end": yesterday_21,
            "active": False
        }

    return {
        "current": current,
        "previous": previous
    }


# =========================================================
# 요청 제한
# =========================================================

def wait_request():

    global last_request_time

    with request_lock:

        gap = time.monotonic() - last_request_time

        if gap < REQUEST_INTERVAL:
            time.sleep(REQUEST_INTERVAL - gap)

        last_request_time = time.monotonic()


# =========================================================
# API 재시도
# =========================================================

def retry(func, *args, **kwargs):

    url = args[0] if (
        args and isinstance(args[0], str)
    ) else kwargs.get("url", "")

    for n in range(MAX_RETRIES):

        try:

            wait_request()

            response = func(*args, **kwargs)

            if not hasattr(response, "status_code"):
                return response

            if response.status_code == 200:
                return response

            if response.status_code == 429:

                wait = min(
                    RATE_LIMIT_WAIT * 2 ** n,
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
                    min(2 * (n + 1), 20)
                )

    return None


# =========================================================
# 업비트 마켓
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
            if x.get("market", "").startswith("KRW-")
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
                    "markets": ",".join(chunk)
                },
                timeout=15
            )

            if ticker_response is None:
                continue

            try:
                data = ticker_response.json()
            except Exception:
                continue

            if isinstance(data, list):
                ticker_result.extend(data)

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

            if volume > 0 and price > 0:

                result.append({
                    "market": market,
                    "volume_24h": volume,
                    "current_price": price
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
# 업비트 1시간봉
# =========================================================

def get_upbit_60m_candles(
    market,
    count=100
):

    response = retry(
        requests.get,
        "https://api.upbit.com/v1/candles/minutes/60",
        params={
            "market": market,
            "count": count
        },
        timeout=15
    )

    if response is None:
        return []

    try:

        data = response.json()

        if not isinstance(data, list):
            return []

        return data

    except Exception:

        return []


# =========================================================
# 업비트 일봉 변동률
#
# 09:00 기준
# =========================================================

def daily_change_upbit(
    market,
    current_price=None
):

    response = retry(
        requests.get,
        "https://api.upbit.com/v1/candles/days",
        params={
            "market": market,
            "count": 2
        },
        timeout=15
    )

    if response is None:
        return None

    try:

        data = response.json()

        if (
            not isinstance(data, list)
            or len(data) < 2
        ):
            return None

        current_candle = data[0]
        previous_candle = data[1]

        if current_price is None:

            current_price = float(
                current_candle["trade_price"]
            )

        previous_close = float(
            previous_candle["trade_price"]
        )

        if previous_close == 0:
            return None

        return (
            (float(current_price) - previous_close)
            / previous_close
            * 100
        )

    except Exception as e:

        log.warning(
            f"업비트 일봉 변동률 오류 "
            f"{market}: {e}"
        )

        return None


# =========================================================
# 업비트 RSI
# =========================================================

def daily_rsi_upbit(
    market,
    current_price=None,
    period=14
):

    response = retry(
        requests.get,
        "https://api.upbit.com/v1/candles/days",
        params={
            "market": market,
            "count": period + 30
        },
        timeout=15
    )

    if response is None:
        return None

    try:

        data = response.json()

        if (
            not isinstance(data, list)
            or len(data) < period + 2
        ):
            return None

        closes = [
            float(x["trade_price"])
            for x in reversed(data)
        ]

        if current_price is not None:
            closes[-1] = float(current_price)

        series = pd.Series(closes)

        delta = series.diff()

        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)

        avg_gain = gain.ewm(
            alpha=1 / period,
            adjust=False,
            min_periods=period
        ).mean()

        avg_loss = loss.ewm(
            alpha=1 / period,
            adjust=False,
            min_periods=period
        ).mean()

        if avg_loss.iloc[-1] == 0:

            if avg_gain.iloc[-1] > 0:
                return 100.0

            return 50.0

        rs = (
            avg_gain.iloc[-1]
            / avg_loss.iloc[-1]
        )

        rsi = 100 - (
            100 / (1 + rs)
        )

        return float(rsi)

    except Exception as e:

        log.warning(
            f"RSI 오류 {market}: {e}"
        )

        return None


# =========================================================
# 업비트 12시간봉 생성
#
# 핵심 함수
#
# 1시간봉을 가지고
#
# 09:00 ~ 21:00
# 21:00 ~ 09:00
#
# 두 개의 12시간봉을 만든다.
# =========================================================

def build_upbit_12h_candles(
    market,
    current_price=None
):

    candles = get_upbit_60m_candles(
        market,
        count=72
    )

    if not candles:
        return {}

    rows = []

    for candle in candles:

        try:

            dt = datetime.strptime(
                candle["candle_date_time_kst"],
                "%Y-%m-%dT%H:%M:%S"
            ).replace(tzinfo=KST)

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
        return {}

    df = pd.DataFrame(rows)

    df = df.sort_values(
        "datetime"
    ).drop_duplicates(
        "datetime"
    )

    now = now_kst()

    periods = get_12h_periods(now)

    result = {}

    # -----------------------------------------------------
    # 두 구간 모두 계산
    # -----------------------------------------------------

    all_periods = [
        periods["current"],
        periods["previous"]
    ]

    for period in all_periods:

        start = period["start"]
        end = period["end"]

        # 12시간 구간에 포함되는 1시간봉
        # start <= candle < end
        part = df[
            (df["datetime"] >= start)
            & (df["datetime"] < end)
        ].copy()

        if part.empty:
            result[period["key"]] = None
            continue

        part = part.sort_values(
            "datetime"
        )

        # 12시간봉 OHLC
        open_price = float(
            part.iloc[0]["open"]
        )

        high_price = float(
            part["high"].max()
        )

        low_price = float(
            part["low"].min()
        )

        close_price = float(
            part.iloc[-1]["close"]
        )

        # -------------------------------------------------
        # 현재 진행 중인 12시간봉
        # 종가는 현재가
        # -------------------------------------------------

        if period["active"]:

            if current_price is not None:
                close_price = float(
                    current_price
                )

                high_price = max(
                    high_price,
                    close_price
                )

                low_price = min(
                    low_price,
                    close_price
                )

        if open_price == 0:
            change = None
        else:
            change = (
                (close_price - open_price)
                / open_price
                * 100
            )

        result[period["key"]] = {
            "start": start,
            "end": end,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "change": change,
            "active": period["active"]
        }

    return result


# =========================================================
# 업비트 12시간 변동률
# =========================================================

def get_upbit_12h_changes(
    market,
    current_price=None
):

    candles = build_upbit_12h_candles(
        market,
        current_price
    )

    return {
        "09_21": (
            candles.get("09_21", {}).get("change")
            if candles.get("09_21")
            else None
        ),
        "21_09": (
            candles.get("21_09", {}).get("change")
            if candles.get("21_09")
            else None
        )
    }


# =========================================================
# OKX BTC 현재가
# =========================================================

def get_okx_btc_price():

    response = retry(
        requests.get,
        f"{OKX_BASE_URL}/api/v5/market/ticker",
        params={
            "instId": OKX_BTC_INST_ID
        },
        timeout=15
    )

    if response is None:
        return None

    try:

        data = response.json()

        if data.get("code") != "0":
            return None

        arr = data.get("data", [])

        if not arr:
            return None

        return float(
            arr[0]["last"]
        )

    except Exception:
        return None


# =========================================================
# OKX BTC 1시간봉
# =========================================================

def get_okx_btc_1h_candles(
    limit=300,
    after=None
):

    params = {
        "instId": OKX_BTC_INST_ID,
        "bar": "1H",
        "limit": str(limit)
    }

    if after is not None:
        params["after"] = str(after)

    response = retry(
        requests.get,
        f"{OKX_BASE_URL}/api/v5/market/candles",
        params=params,
        timeout=15
    )

    if response is None:
        return []

    try:

        data = response.json()

        if data.get("code") != "0":
            return []

        return data.get(
            "data",
            []
        )

    except Exception:
        return []


# =========================================================
# BTC 1시간봉 전체
# =========================================================

def get_okx_btc_1h_history():

    all_rows = []

    after = None

    for _ in range(MAX_HISTORY_CHUNKS):

        rows = get_okx_btc_1h_candles(
            limit=HISTORY_CHUNK,
            after=after
        )

        if not rows:
            break

        all_rows.extend(rows)

        try:

            oldest_ts = min(
                int(x[0])
                for x in rows
            )

        except Exception:
            break

        after = oldest_ts - 1

        if len(rows) < HISTORY_CHUNK:
            break

    if not all_rows:
        return pd.DataFrame()

    result = []

    for row in all_rows:

        try:

            ts = int(row[0])

            dt_utc = datetime.fromtimestamp(
                ts / 1000,
                tz=ZoneInfo("UTC")
            )

            dt_kst = dt_utc.astimezone(KST)

            result.append({
                "datetime": dt_kst,
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4])
            })

        except Exception:
            continue

    if not result:
        return pd.DataFrame()

    df = pd.DataFrame(result)

    df = df.sort_values(
        "datetime"
    ).drop_duplicates(
        "datetime"
    )

    return df


# =========================================================
# BTC 12시간봉 생성
#
# OKX 1시간봉 → KST 기준
# 09~21 / 21~09
# =========================================================

def build_btc_12h_candles(
    current_price=None
):

    df = get_okx_btc_1h_history()

    if df.empty:
        return {}

    now = now_kst()

    periods = get_12h_periods(now)

    result = {}

    all_periods = [
        periods["current"],
        periods["previous"]
    ]

    for period in all_periods:

        start = period["start"]
        end = period["end"]

        part = df[
            (df["datetime"] >= start)
            & (df["datetime"] < end)
        ].copy()

        if part.empty:

            result[period["key"]] = None

            continue

        part = part.sort_values(
            "datetime"
        )

        open_price = float(
            part.iloc[0]["open"]
        )

        high_price = float(
            part["high"].max()
        )

        low_price = float(
            part["low"].min()
        )

        close_price = float(
            part.iloc[-1]["close"]
        )

        # 현재 진행 중인 12시간봉
        if period["active"]:

            if current_price is not None:

                close_price = float(
                    current_price
                )

                high_price = max(
                    high_price,
                    close_price
                )

                low_price = min(
                    low_price,
                    close_price
                )

        if open_price == 0:

            change = None

        else:

            change = (
                (close_price - open_price)
                / open_price
                * 100
            )

        result[period["key"]] = {
            "start": start,
            "end": end,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "change": change,
            "active": period["active"]
        }

    return result


# =========================================================
# BTC 12시간 변동률
# =========================================================

def get_okx_btc_12h_changes(
    current_price=None
):

    candles = build_btc_12h_candles(
        current_price
    )

    return {
        "09_21": (
            candles.get("09_21", {}).get("change")
            if candles.get("09_21")
            else None
        ),
        "21_09": (
            candles.get("21_09", {}).get("change")
            if candles.get("21_09")
            else None
        )
    }


# =========================================================
# BTC 일봉
#
# OKX 1시간봉을 KST 09:00 기준으로 묶는다.
# =========================================================

def get_okx_btc_daily_change():

    current_price = get_okx_btc_price()

    if current_price is None:
        return None

    df = get_okx_btc_1h_history()

    if df.empty:
        return None

    now = now_kst()

    today_09 = now.replace(
        hour=9,
        minute=0,
        second=0,
        microsecond=0
    )

    if now < today_09:
        today_09 -= timedelta(days=1)

    previous_09 = (
        today_09 - timedelta(days=1)
    )

    current_part = df[
        (df["datetime"] >= today_09)
    ]

    previous_part = df[
        (df["datetime"] >= previous_09)
        & (df["datetime"] < today_09)
    ]

    if (
        current_part.empty
        or previous_part.empty
    ):
        return None

    previous_close = float(
        previous_part.iloc[-1]["close"]
    )

    if previous_close == 0:
        return None

    return (
        (current_price - previous_close)
        / previous_close
        * 100
    )


# =========================================================
# BTC 시장 업데이트
# =========================================================

def update_btc_market():

    global latest_btc_okx_price
    global latest_btc_daily_change
    global latest_btc_09_21_change
    global latest_btc_21_09_change
    global latest_btc_current_12h_change
    global latest_btc_current_12h_label

    if USE_OKX != "Y":
        return

    price = get_okx_btc_price()

    if price is None:
        return

    daily = get_okx_btc_daily_change()

    changes = get_okx_btc_12h_changes(
        price
    )

    latest_btc_okx_price = price

    latest_btc_daily_change = daily

    latest_btc_09_21_change = changes.get(
        "09_21"
    )

    latest_btc_21_09_change = changes.get(
        "21_09"
    )

    current_period = get_current_12h_period()

    if current_period["key"] == "09_21":

        latest_btc_current_12h_change = (
            latest_btc_09_21_change
        )

    else:

        latest_btc_current_12h_change = (
            latest_btc_21_09_change
        )

    latest_btc_current_12h_label = (
        current_period["label"]
    )

    log.info(
        "[BTC] "
        f"현재가={price:,.2f} "
        f"일봉={daily if daily is not None else '-'} "
        f"09~21={latest_btc_09_21_change if latest_btc_09_21_change is not None else '-'} "
        f"21~09={latest_btc_21_09_change if latest_btc_21_09_change is not None else '-'}"
    )


# =========================================================
# 변동률 표시
# =========================================================

def format_change(
    value,
    show_sign=True
):

    if value is None:
        return "-"

    try:
        value = float(value)
    except Exception:
        return "-"

    if value > 0:

        if show_sign:
            return (
                '<span class="up">'
                f'▲ +{value:.2f}%'
                '</span>'
            )

        return (
            '<span class="up">'
            f'+{value:.2f}%'
            '</span>'
        )

    if value < 0:

        return (
            '<span class="down">'
            f'▼ {value:.2f}%'
            '</span>'
        )

    return (
        '<span class="flat">'
        '0.00%'
        '</span>'
    )


# =========================================================
# 거래대금 표시
# =========================================================

def format_krw(
    value
):

    if value is None:
        return "-"

    try:
        value = float(value)
    except Exception:
        return "-"

    if value >= 1_0000_0000_0000:

        return (
            f"{value / 1_0000_0000_0000:.2f}조"
        )

    if value >= 1_0000_0000:

        return (
            f"{value / 1_0000_0000:.1f}억"
        )

    if value >= 1_0000:

        return (
            f"{value / 1_0000:.0f}만원"
        )

    return f"{value:,.0f}원"


# =========================================================
# 가격 표시
# =========================================================

def format_price(
    price
):

    if price is None:
        return "-"

    try:
        price = float(price)
    except Exception:
        return "-"

    if price >= 1000:
        return f"{price:,.0f}"

    if price >= 1:
        return f"{price:,.2f}"

    if price >= 0.01:
        return f"{price:,.4f}"

    return f"{price:,.8f}"


# =========================================================
# RSI 표시
# =========================================================

def format_rsi(
    value
):

    if value is None:
        return "-"

    try:
        value = float(value)
    except Exception:
        return "-"

    if value <= 30:

        return (
            '<span class="rsi-blue">'
            f'{value:.1f}'
            '</span>'
        )

    if 40 <= value <= 60:

        return (
            '<span class="rsi-green">'
            f'{value:.1f}'
            '</span>'
        )

    if value >= 70:

        return (
            '<span class="rsi-red">'
            f'{value:.1f}'
            '</span>'
        )

    return (
        '<span class="rsi-gray">'
        f'{value:.1f}'
        '</span>'
    )


# =========================================================
# 코인 분석
# =========================================================

def analyze_coin(
    market,
    current_price
):

    daily = daily_change_upbit(
        market,
        current_price
    )

    changes = get_upbit_12h_changes(
        market,
        current_price
    )

    rsi = daily_rsi_upbit(
        market,
        current_price
    )

    current_period = get_current_12h_period()

    if current_period["key"] == "09_21":

        current_12h = changes.get(
            "09_21"
        )

    else:

        current_12h = changes.get(
            "21_09"
        )

    return {
        "daily": daily,

        "change_09_21": changes.get(
            "09_21"
        ),

        "change_21_09": changes.get(
            "21_09"
        ),

        "current_12h_change": current_12h,

        "rsi": rsi
    }


# =========================================================
# 행 생성
# =========================================================

def make_row(
    rank,
    market,
    volume,
    price,
    analysis,
    signal_pass=False
):

    coin = market.replace(
        "KRW-",
        ""
    )

    return {
        "rank": rank,
        "market": market,
        "coin": coin,

        "volume_24h": volume,
        "current_price": price,

        "daily": analysis.get(
            "daily"
        ),

        "change_09_21": analysis.get(
            "change_09_21"
        ),

        "change_21_09": analysis.get(
            "change_21_09"
        ),

        "current_12h_change": analysis.get(
            "current_12h_change"
        ),

        "rsi": analysis.get(
            "rsi"
        ),

        "signal_pass": signal_pass
    }


# =========================================================
# 업비트 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update_time

    if USE_UPBIT != "Y":
        return

    markets = get_upbit_markets()

    if not markets:
        return

    # 거래대금 순 정렬
    markets = sorted(
        markets,
        key=lambda x: x["volume_24h"],
        reverse=True
    )

    top_markets = markets[:TOP_N]

    current_period = get_current_12h_period()

    btc_12h = latest_btc_current_12h_change

    rows = []

    for rank, item in enumerate(
        top_markets,
        start=1
    ):

        market = item["market"]

        price = item["current_price"]

        try:

            analysis = analyze_coin(
                market,
                price
            )

        except Exception as e:

            log.error(
                f"코인 분석 오류 "
                f"{market}: {e}"
            )

            continue

        coin_current_12h = (
            analysis.get(
                "current_12h_change"
            )
        )

        # ---------------------------------------------
        # Signal
        #
        # 현재 12시간 구간에서
        # BTC > 0
        # 코인 > 0
        # ---------------------------------------------

        signal_pass = False

        if (
            btc_12h is not None
            and coin_current_12h is not None
        ):

            if (
                btc_12h > 0
                and coin_current_12h > 0
            ):

                signal_pass = True

        rows.append(
            make_row(
                rank,
                market,
                item["volume_24h"],
                price,
                analysis,
                signal_pass
            )
        )

    latest_upbit_data = rows

    latest_upbit_update_time = kst()


# =========================================================
# 전체 업데이트
# =========================================================

def update_all():

    if not update_lock.acquire(
        blocking=False
    ):
        return

    try:

        log.info("========== 데이터 업데이트 시작 ==========")

        # BTC
        update_btc_market()

        # Upbit
        update_upbit()

        log.info(
            "========== 데이터 업데이트 완료 =========="
        )

    except Exception as e:

        log.exception(
            f"전체 업데이트 오류: {e}"
        )

    finally:

        update_lock.release()


# =========================================================
# Signal HTML
# =========================================================

def signal_item_html(
    row
):

    return f"""
    <div class="signal-item">

        <div class="signal-title">
            <span class="signal-rank">
                #{row["rank"]}
            </span>

            <span class="signal-coin">
                {html.escape(row["coin"])}
            </span>
        </div>

        <div class="signal-data">

            <div>
                거래대금
                <b>{format_krw(row["volume_24h"])}</b>
            </div>

            <div>
                현재가
                <b>{format_price(row["current_price"])}</b>
            </div>

            <div>
                당일
                <b>{format_change(row["daily"])}</b>
            </div>

            <div>
                09~21
                <b>{format_change(row["change_09_21"])}</b>
            </div>

            <div>
                21~09
                <b>{format_change(row["change_21_09"])}</b>
            </div>

        </div>

    </div>
    """


# =========================================================
# Signal 영역
# =========================================================

def signal_section():

    current_period = get_current_12h_period()

    signal_rows = [
        row
        for row in latest_upbit_data
        if row.get("signal_pass")
    ]

    # 현재 적용되는 12시간 변동률 순
    signal_rows.sort(
        key=lambda x: (
            x.get("current_12h_change")
            if x.get("current_12h_change") is not None
            else -999999
        ),
        reverse=True
    )

    if not signal_rows:

        return f"""
        <div class="section signal-section">

            <div class="section-title">
                🔔 SIGNAL
            </div>

            <div class="signal-period">
                현재 기준:
                <b>{current_period["label"]}</b>
            </div>

            <div class="empty">
                조건을 만족하는 종목 없음
            </div>

        </div>
        """

    items = "".join(
        signal_item_html(row)
        for row in signal_rows
    )

    return f"""
    <div class="section signal-section">

        <div class="section-title">
            🔔 SIGNAL
        </div>

        <div class="signal-period">
            현재 기준:
            <b>{current_period["label"]}</b>
        </div>

        {items}

    </div>
    """


# =========================================================
# TOP 리스트
# =========================================================

def rows_html():

    if not latest_upbit_data:

        return """
        <tr>
            <td colspan="8">
                데이터 없음
            </td>
        </tr>
        """

    rows = []

    for row in latest_upbit_data:

        signal = (
            '<span class="signal-badge">SIGNAL</span>'
            if row.get("signal_pass")
            else ""
        )

        rows.append(
            f"""
            <tr>

                <td>
                    {row["rank"]}
                </td>

                <td class="coin-cell">
                    <b>{html.escape(row["coin"])}</b>
                    {signal}
                </td>

                <td>
                    {format_krw(row["volume_24h"])}
                </td>

                <td>
                    {format_price(row["current_price"])}
                </td>

                <td>
                    {format_change(row["daily"])}
                </td>

                <td>
                    {format_change(row["change_09_21"])}
                </td>

                <td>
                    {format_change(row["change_21_09"])}
                </td>

                <td>
                    {format_rsi(row["rsi"])}
                </td>

            </tr>
            """
        )

    return "".join(rows)


# =========================================================
# BTC 시황
# =========================================================

def market_summary_html():

    current_period = get_current_12h_period()

    if latest_btc_okx_price is None:

        btc_price = "-"

    else:

        btc_price = format_price(
            latest_btc_okx_price
        )

    return f"""
    <div class="section btc-section">

        <div class="section-title">
            ₿ BTC 시황
        </div>

        <div class="btc-main">

            <div class="btc-price">
                BTC
                <strong>
                    {btc_price}
                </strong>
            </div>

            <div class="btc-daily">
                당일
                <b>
                    {format_change(
                        latest_btc_daily_change
                    )}
                </b>
            </div>

        </div>

        <div class="btc-12h-row">

            <div class="btc-period-box">
                <span>
                    09:00 ~ 21:00
                </span>

                <strong>
                    {format_change(
                        latest_btc_09_21_change
                    )}
                </strong>
            </div>

            <div class="btc-period-box">
                <span>
                    21:00 ~ 09:00
                </span>

                <strong>
                    {format_change(
                        latest_btc_21_09_change
                    )}
                </strong>
            </div>

        </div>

        <div class="btc-filter">

            현재 필터:
            <b>
                {current_period["label"]}
            </b>

            <strong>
                {format_change(
                    latest_btc_current_12h_change
                )}
            </strong>

        </div>

    </div>
    """


# =========================================================
# HTML
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def home():

    current_period = get_current_12h_period()

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
            코인 시황
        </title>

        <style>

            * {{
                box-sizing: border-box;
            }}

            body {{
                margin: 0;
                padding: 14px;
                background: #101114;
                color: #eeeeee;
                font-family:
                    Arial,
                    "Noto Sans KR",
                    sans-serif;
            }}

            .container {{
                max-width: 1400px;
                margin: 0 auto;
            }}

            .header {{
                margin-bottom: 15px;
            }}

            .header h1 {{
                margin: 0 0 7px 0;
                font-size: 24px;
            }}

            .update-time {{
                color: #888;
                font-size: 12px;
            }}

            .section {{
                background: #181a1f;
                border: 1px solid #282b31;
                border-radius: 10px;
                margin-bottom: 14px;
                overflow: hidden;
            }}

            .section-title {{
                padding: 14px;
                font-size: 17px;
                font-weight: bold;
                border-bottom: 1px solid #292c33;
            }}

            .btc-main {{
                display: flex;
                align-items: center;
                justify-content: space-between;
                padding: 18px 14px;
            }}

            .btc-price {{
                font-size: 14px;
                color: #aaa;
            }}

            .btc-price strong {{
                display: block;
                margin-top: 5px;
                font-size: 25px;
                color: #fff;
            }}

            .btc-daily {{
                text-align: right;
                color: #999;
                font-size: 13px;
            }}

            .btc-daily b {{
                display: block;
                margin-top: 5px;
                font-size: 16px;
            }}

            .btc-12h-row {{
                display: grid;
                grid-template-columns: 1fr 1fr;
                border-top: 1px solid #292c33;
            }}

            .btc-period-box {{
                padding: 14px;
                text-align: center;
            }}

            .btc-period-box + .btc-period-box {{
                border-left: 1px solid #292c33;
            }}

            .btc-period-box span {{
                display: block;
                color: #888;
                font-size: 12px;
                margin-bottom: 6px;
            }}

            .btc-period-box strong {{
                font-size: 18px;
            }}

            .btc-filter {{
                padding: 12px 14px;
                background: #131519;
                color: #999;
                font-size: 12px;
            }}

            .btc-filter b {{
                color: #ddd;
                margin-left: 5px;
            }}

            .btc-filter strong {{
                margin-left: 10px;
                font-size: 14px;
            }}

            .signal-period {{
                padding: 10px 14px;
                color: #888;
                font-size: 12px;
            }}

            .signal-period b {{
                color: #ddd;
            }}

            .signal-item {{
                border-top: 1px solid #292c33;
                padding: 13px 14px;
            }}

            .signal-title {{
                display: flex;
                align-items: center;
                gap: 8px;
                margin-bottom: 10px;
            }}

            .signal-rank {{
                color: #777;
                font-size: 12px;
            }}

            .signal-coin {{
                font-size: 17px;
                font-weight: bold;
            }}

            .signal-badge {{
                padding: 2px 6px;
                border-radius: 4px;
                background: #263c2d;
                color: #66d98a;
                font-size: 10px;
            }}

            .signal-data {{
                display: grid;
                grid-template-columns:
                    repeat(5, 1fr);
                gap: 8px;
            }}

            .signal-data > div {{
                color: #777;
                font-size: 11px;
            }}

            .signal-data b {{
                display: block;
                margin-top: 4px;
                color: #ddd;
                font-size: 13px;
            }}

            .table-wrap {{
                overflow-x: auto;
            }}

            table {{
                width: 100%;
                border-collapse: collapse;
                min-width: 850px;
            }}

            th {{
                padding: 10px 7px;
                background: #131519;
                color: #888;
                font-size: 11px;
                font-weight: normal;
                white-space: nowrap;
            }}

            td {{
                padding: 11px 7px;
                border-top: 1px solid #292c33;
                text-align: center;
                font-size: 12px;
                white-space: nowrap;
            }}

            .coin-cell {{
                text-align: left;
            }}

            .empty {{
                padding: 25px;
                text-align: center;
                color: #666;
            }}

            .up {{
                color: #ff5c68;
            }}

            .down {{
                color: #4f9cff;
            }}

            .flat {{
                color: #aaa;
            }}

            .rsi-blue {{
                color: #4f9cff;
                font-weight: bold;
            }}

            .rsi-green {{
                color: #52d273;
                font-weight: bold;
            }}

            .rsi-red {{
                color: #ff5c68;
                font-weight: bold;
            }}

            .rsi-gray {{
                color: #aaa;
            }}

            @media (
                max-width: 700px
            ) {{

                body {{
                    padding: 8px;
                }}

                .header h1 {{
                    font-size: 20px;
                }}

                .btc-main {{
                    padding: 14px;
                }}

                .btc-price strong {{
                    font-size: 21px;
                }}

                .btc-12h-row {{
                    grid-template-columns: 1fr 1fr;
                }}

                .signal-data {{
                    grid-template-columns:
                        repeat(2, 1fr);
                }}

                .signal-data > div:nth-child(1) {{
                    grid-column: span 2;
                }}

            }}

        </style>

    </head>

    <body>

        <div class="container">

            <div class="header">

                <h1>
                    📊 코인 시황
                </h1>

                <div class="update-time">
                    업비트 업데이트:
                    {latest_upbit_update_time}
                    /
                    현재:
                    {kst()}
                </div>

                <div class="update-time">
                    현재 12시간:
                    <b>{current_period["label"]}</b>
                </div>

            </div>

            {market_summary_html()}

            {signal_section()}

            <div class="section">

                <div class="section-title">
                    🔥 TOP {TOP_N}
                </div>

                <div class="table-wrap">

                    <table>

                        <thead>

                            <tr>

                                <th>
                                    순위
                                </th>

                                <th>
                                    코인
                                </th>

                                <th>
                                    거래대금
                                </th>

                                <th>
                                    현재가
                                </th>

                                <th>
                                    당일
                                </th>

                                <th>
                                    09~21
                                </th>

                                <th>
                                    21~09
                                </th>

                                <th>
                                    RSI
                                </th>

                            </tr>

                        </thead>

                        <tbody>

                            {rows_html()}

                        </tbody>

                    </table>

                </div>

            </div>

        </div>

    </body>

    </html>
    """


# =========================================================
# 백그라운드 스케줄
# =========================================================

def scheduler_loop():

    schedule.every(
        UPDATE_MINUTES
    ).minutes.do(
        update_all
    )

    while True:

        try:

            schedule.run_pending()

        except Exception as e:

            log.error(
                f"스케줄 오류: {e}"
            )

        time.sleep(1)


# =========================================================
# 시작
# =========================================================

@app.on_event("startup")
def startup_event():

    log.info(
        "Trading Dashboard 시작"
    )

    thread = threading.Thread(
        target=update_all,
        daemon=True
    )

    thread.start()

    scheduler_thread = threading.Thread(
        target=scheduler_loop,
        daemon=True
    )

    scheduler_thread.start()


# =========================================================
# 실행
# =========================================================

if __name__ == "__main__":

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )
