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
import os
import re

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
    format="%(asctime)s [%(levelname)s] %(message)s"
)


# =========================================================
# 사용자 설정
# =========================================================

USE_UPBIT = "Y"
USE_OKX = "N"

TOP_N = 20

UPDATE_MINUTES = 1

HISTORY_CHUNK = 200
MAX_HISTORY_CHUNKS = 10

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10

OKX_RETRY_DELAY = 2
OKX_MAX_RETRY_ROUNDS = 3

KST = ZoneInfo("Asia/Seoul")

UPBIT_API = "https://api.upbit.com/v1"

OKX_API = "https://www.okx.com/api/v5"

# ---------------------------------------------------------
# CoinGecko 뉴스
# ---------------------------------------------------------

COINGECKO_API_KEY = os.getenv(
    "COINGECKO_API_KEY",
    ""
)

COINGECKO_BASE_URL = os.getenv(
    "COINGECKO_BASE_URL",
    "https://pro-api.coingecko.com/api/v3"
)

NEWS_CACHE_MINUTES = 15

NEWS_PER_COIN = 3

# 뉴스 언어
NEWS_LANGUAGE = "ko"

# 뉴스 종류
NEWS_TYPE = "news"


# =========================================================
# SIGNAL 설정
# =========================================================

# 관통형은 SIGNAL에서 제외
SIGNAL_CANDLE_PATTERNS = [
    "상승장악",
    "3캔들 상승장악",
    "4캔들 상승장악"
]


# =========================================================
# 전역 데이터
# =========================================================

latest_upbit_data = []

latest_upbit_update_time = None

latest_btc_data = {}

latest_btc_news = []

latest_signal_news = []

news_cache = {}

coin_id_cache = {}

previous_top_vol_ids = []

sent_signal_coins = set()


# =========================================================
# 세션
# =========================================================

session = requests.Session()

session.headers.update({
    "User-Agent": "Mozilla/5.0"
})


# =========================================================
# 공통
# =========================================================

def now_kst():
    return datetime.now(KST)


def safe_float(value, default=None):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def format_number(value):
    if value is None:
        return "-"

    try:
        return f"{value:,.0f}"
    except Exception:
        return "-"


def format_price(value):
    if value is None:
        return "-"

    try:
        value = float(value)

        if value >= 1000000:
            return f"{value:,.0f}"

        if value >= 1000:
            return f"{value:,.1f}"

        if value >= 1:
            return f"{value:,.2f}"

        if value >= 0.01:
            return f"{value:,.4f}"

        return f"{value:,.8f}"

    except Exception:
        return "-"


def format_change(value):
    if value is None:
        return "-"

    try:
        value = float(value)

        if value > 0:
            return f"+{value:.2f}%"

        return f"{value:.2f}%"

    except Exception:
        return "-"


def format_volume_krw(value):
    if value is None:
        return "-"

    try:
        value = float(value)

        if value >= 1_0000_0000_0000:
            return f"{value / 1_0000_0000_0000:.2f}조"

        if value >= 1_0000_0000:
            return f"{value / 1_0000_0000:.1f}억"

        if value >= 1_0000:
            return f"{value / 1_0000:.0f}만"

        return f"{value:,.0f}"

    except Exception:
        return "-"


# =========================================================
# HTTP
# =========================================================

def request_get(
    url,
    params=None,
    headers=None,
    retries=MAX_RETRIES,
    timeout=15
):

    for attempt in range(retries):

        try:

            response = session.get(
                url,
                params=params,
                headers=headers,
                timeout=timeout
            )

            if response.status_code == 429:

                logging.warning(
                    "429 rate limit: %s",
                    url
                )

                time.sleep(RATE_LIMIT_WAIT)

                continue

            response.raise_for_status()

            time.sleep(REQUEST_INTERVAL)

            return response

        except Exception as e:

            if attempt >= retries - 1:

                logging.error(
                    "GET 실패: %s / %s",
                    url,
                    e
                )

                return None

            time.sleep(
                min(
                    2 + attempt,
                    10
                )
            )

    return None


# =========================================================
# Upbit
# =========================================================

def get_upbit_markets():

    response = request_get(
        f"{UPBIT_API}/market/all",
        params={
            "isDetails": "false"
        }
    )

    if not response:
        return []

    try:

        data = response.json()

        return [
            x["market"]
            for x in data
            if x.get("market", "").startswith("KRW-")
        ]

    except Exception:
        return []


def get_upbit_tickers(markets):

    if not markets:
        return []

    result = []

    chunk_size = 100

    for i in range(
        0,
        len(markets),
        chunk_size
    ):

        chunk = markets[
            i:i + chunk_size
        ]

        response = request_get(
            f"{UPBIT_API}/ticker",
            params={
                "markets": ",".join(chunk)
            }
        )

        if not response:
            continue

        try:
            result.extend(
                response.json()
            )
        except Exception:
            pass

    return result


def get_upbit_candles(
    market,
    unit=60,
    count=200,
    to=None
):

    params = {
        "market": market,
        "count": min(count, 200)
    }

    if to:
        params["to"] = to

    response = request_get(
        f"{UPBIT_API}/candles/minutes/{unit}",
        params=params
    )

    if not response:
        return []

    try:

        data = response.json()

        if not isinstance(data, list):
            return []

        return data

    except Exception:
        return []


def get_upbit_daily_candles(
    market,
    count=200
):

    response = request_get(
        f"{UPBIT_API}/candles/days",
        params={
            "market": market,
            "count": min(count, 200)
        }
    )

    if not response:
        return []

    try:
        data = response.json()

        if not isinstance(data, list):
            return []

        return data

    except Exception:
        return []


# =========================================================
# Upbit 1H → 4H
# =========================================================

def make_4h_period_start(dt):

    dt = dt.astimezone(KST)

    hour = dt.hour

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

    else:

        if hour >= 21:
            start_hour = 21

        else:
            dt = dt - timedelta(days=1)
            start_hour = 21

    return dt.replace(
        hour=start_hour,
        minute=0,
        second=0,
        microsecond=0
    )


def candle_datetime(candle):

    timestamp = candle.get(
        "candle_date_time_kst"
    )

    if timestamp:

        try:
            return datetime.fromisoformat(
                timestamp
            ).replace(tzinfo=KST)

        except Exception:
            pass

    timestamp = candle.get(
        "timestamp"
    )

    if timestamp:

        try:
            return datetime.fromtimestamp(
                timestamp / 1000,
                tz=KST
            )

        except Exception:
            pass

    return None


def build_4h_candles(hourly):

    if not hourly:
        return []

    groups = {}

    for candle in hourly:

        dt = candle_datetime(candle)

        if not dt:
            continue

        start = make_4h_period_start(dt)

        key = start.strftime(
            "%Y-%m-%d %H:%M"
        )

        if key not in groups:
            groups[key] = []

        groups[key].append(candle)

    result = []

    for key in sorted(groups.keys()):

        rows = groups[key]

        rows.sort(
            key=lambda x:
            candle_datetime(x)
            or datetime.min.replace(
                tzinfo=KST
            )
        )

        if not rows:
            continue

        first = rows[0]
        last = rows[-1]

        opens = [
            safe_float(
                x.get("opening_price")
            )
            for x in rows
        ]

        highs = [
            safe_float(
                x.get("high_price")
            )
            for x in rows
        ]

        lows = [
            safe_float(
                x.get("low_price")
            )
            for x in rows
        ]

        closes = [
            safe_float(
                x.get("trade_price")
            )
            for x in rows
        ]

        opens = [
            x for x in opens
            if x is not None
        ]

        highs = [
            x for x in highs
            if x is not None
        ]

        lows = [
            x for x in lows
            if x is not None
        ]

        closes = [
            x for x in closes
            if x is not None
        ]

        if not opens or not closes:
            continue

        start_dt = candle_datetime(first)
        end_dt = candle_datetime(last)

        if not start_dt:
            continue

        open_price = opens[0]
        close_price = closes[-1]

        high_price = max(highs)
        low_price = min(lows)

        volume = sum(
            safe_float(
                x.get(
                    "candle_acc_trade_volume"
                ),
                0
            )
            or 0
            for x in rows
        )

        trade_price = close_price

        active = False

        current_start = make_4h_period_start(
            now_kst()
        )

        if start_dt == current_start:
            active = True

        result.append({
            "start": start_dt,
            "end": end_dt,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "volume": volume,
            "trade_price": trade_price,
            "active": active
        })

    return result


# =========================================================
# 일봉
# =========================================================

def daily_period_start(dt):

    dt = dt.astimezone(KST)

    boundary = dt.replace(
        hour=9,
        minute=0,
        second=0,
        microsecond=0
    )

    if dt < boundary:
        boundary -= timedelta(days=1)

    return boundary


def normalize_daily_candles(candles):

    result = []

    for candle in candles:

        dt = candle_datetime(candle)

        if not dt:
            continue

        start = daily_period_start(dt)

        result.append({
            "start": start,
            "end": start + timedelta(days=1),
            "open": safe_float(
                candle.get("opening_price")
            ),
            "high": safe_float(
                candle.get("high_price")
            ),
            "low": safe_float(
                candle.get("low_price")
            ),
            "close": safe_float(
                candle.get("trade_price")
            ),
            "volume": safe_float(
                candle.get(
                    "candle_acc_trade_volume"
                ),
                0
            ),
            "active": (
                start ==
                daily_period_start(now_kst())
            )
        })

    result.sort(
        key=lambda x: x["start"]
    )

    return result


# =========================================================
# 현재 기간 등락률
# =========================================================

def calculate_period_change(
    candle,
    current_price=None
):

    if not candle:
        return None

    open_price = candle.get("open")

    if open_price is None:
        return None

    price = (
        current_price
        if current_price is not None
        else candle.get("close")
    )

    if price is None:
        return None

    try:

        return (
            (price - open_price)
            / open_price
            * 100
        )

    except Exception:
        return None


def add_period_changes(
    periods,
    current_price
):

    for p in periods:

        if p.get("active"):

            p["change"] = calculate_period_change(
                p,
                current_price
            )

        else:

            p["change"] = calculate_period_change(
                p,
                p.get("close")
            )

    return periods


# =========================================================
# 캔들 기본 판정
# =========================================================

def is_bullish(c):
    return (
        c.get("close") is not None
        and c.get("open") is not None
        and c["close"] > c["open"]
    )


def is_bearish(c):
    return (
        c.get("close") is not None
        and c.get("open") is not None
        and c["close"] < c["open"]
    )


def body(c):

    if c.get("open") is None:
        return 0

    if c.get("close") is None:
        return 0

    return abs(
        c["close"] - c["open"]
    )


def bullish_engulfing(prev, cur):

    if not is_bearish(prev):
        return False

    if not is_bullish(cur):
        return False

    return (
        cur["open"] <= prev["close"]
        and
        cur["close"] >= prev["open"]
    )


def bearish_engulfing(prev, cur):

    if not is_bullish(prev):
        return False

    if not is_bearish(cur):
        return False

    return (
        cur["open"] >= prev["close"]
        and
        cur["close"] <= prev["open"]
    )


def piercing_pattern(prev, cur):

    if not is_bearish(prev):
        return False

    if not is_bullish(cur):
        return False

    midpoint = (
        prev["open"]
        + prev["close"]
    ) / 2

    return (
        cur["open"] < prev["close"]
        and
        cur["close"] > midpoint
        and
        cur["close"] < prev["open"]
    )


def dark_cloud(prev, cur):

    if not is_bullish(prev):
        return False

    if not is_bearish(cur):
        return False

    midpoint = (
        prev["open"]
        + prev["close"]
    ) / 2

    return (
        cur["open"] > prev["close"]
        and
        cur["close"] < midpoint
        and
        cur["close"] > prev["open"]
    )


def doji(c):

    b = body(c)

    if c.get("open") in (
        None,
        0
    ):
        return False

    return (
        b / abs(c["open"])
        <= 0.0015
    )


def hammer(c):

    if (
        c.get("open") is None
        or c.get("close") is None
        or c.get("high") is None
        or c.get("low") is None
    ):
        return False

    b = body(c)

    upper = (
        c["high"]
        - max(
            c["open"],
            c["close"]
        )
    )

    lower = (
        min(
            c["open"],
            c["close"]
        )
        - c["low"]
    )

    if b <= 0:
        return False

    return (
        lower >= b * 2
        and upper <= b
    )


def inverted_hammer(c):

    if (
        c.get("open") is None
        or c.get("close") is None
        or c.get("high") is None
        or c.get("low") is None
    ):
        return False

    b = body(c)

    upper = (
        c["high"]
        - max(
            c["open"],
            c["close"]
        )
    )

    lower = (
        min(
            c["open"],
            c["close"]
        )
        - c["low"]
    )

    if b <= 0:
        return False

    return (
        upper >= b * 2
        and lower <= b
    )


# =========================================================
# 3 / 4 캔들
# =========================================================

def three_bullish_engulfing(candles):

    if len(candles) < 3:
        return False

    a = candles[-3]
    b = candles[-2]
    c = candles[-1]

    if not is_bearish(a):
        return False

    if not is_bullish(c):
        return False

    return (
        c["open"] <= a["close"]
        and
        c["close"] >= a["open"]
    )


def four_bullish_engulfing(candles):

    if len(candles) < 4:
        return False

    a = candles[-4]
    b = candles[-3]
    c = candles[-2]
    d = candles[-1]

    if not is_bearish(a):
        return False

    if not is_bullish(d):
        return False

    return (
        d["open"] <= a["close"]
        and
        d["close"] >= a["open"]
    )


def morning_star(candles):

    if len(candles) < 3:
        return False

    a, b, c = candles[-3:]

    if not is_bearish(a):
        return False

    if body(a) <= 0:
        return False

    if body(b) > body(a) * 0.6:
        return False

    if not is_bullish(c):
        return False

    midpoint = (
        a["open"]
        + a["close"]
    ) / 2

    return c["close"] > midpoint


def three_white_soldiers(candles):

    if len(candles) < 3:
        return False

    a, b, c = candles[-3:]

    return (
        is_bullish(a)
        and is_bullish(b)
        and is_bullish(c)
        and b["close"] > a["close"]
        and c["close"] > b["close"]
        and b["open"] >= a["open"]
        and c["open"] >= b["open"]
    )


def three_bearish_engulfing(candles):

    if len(candles) < 3:
        return False

    a, b, c = candles[-3:]

    if not is_bullish(a):
        return False

    if not is_bearish(c):
        return False

    return (
        c["open"] >= a["close"]
        and
        c["close"] <= a["open"]
    )


def three_dark_cloud(candles):

    if len(candles) < 3:
        return False

    a, b, c = candles[-3:]

    return dark_cloud(a, c)


def three_piercing(candles):

    if len(candles) < 3:
        return False

    a, b, c = candles[-3:]

    return piercing_pattern(a, c)


# =========================================================
# 캔들 패턴 전체
# =========================================================

def detect_patterns(candles):

    if not candles:
        return []

    patterns = []

    last = candles[-1]

    if doji(last):
        patterns.append("도지")

    if hammer(last):
        patterns.append("망치형")

    if inverted_hammer(last):
        patterns.append("역망치형")

    if len(candles) >= 2:

        prev = candles[-2]

        if bullish_engulfing(
            prev,
            last
        ):
            patterns.append("상승장악")

        if piercing_pattern(
            prev,
            last
        ):
            patterns.append("관통형")

        if bearish_engulfing(
            prev,
            last
        ):
            patterns.append("하락장악")

        if dark_cloud(
            prev,
            last
        ):
            patterns.append("먹구름형")

    if len(candles) >= 3:

        if morning_star(candles):
            patterns.append("모닝스타")

        if three_white_soldiers(
            candles
        ):
            patterns.append("3연속양봉")

        if three_bullish_engulfing(
            candles
        ):
            patterns.append("3캔들 상승장악")

        if three_piercing(
            candles
        ):
            patterns.append("3캔들 관통형")

        if three_bearish_engulfing(
            candles
        ):
            patterns.append("3캔들 하락장악")

        if three_dark_cloud(
            candles
        ):
            patterns.append("3캔들 먹구름형")

    if len(candles) >= 4:

        if four_bullish_engulfing(
            candles
        ):
            patterns.append(
                "4캔들 상승장악"
            )

    return patterns


# =========================================================
# 기간 표시용 패턴
# =========================================================

def candle_pattern_html(patterns):

    if not patterns:
        return ""

    items = []

    for p in patterns:

        items.append(
            f"""
            <span class="candle-pattern">
                {html.escape(str(p))}
            </span>
            """
        )

    return "".join(items)


def make_period_display(
    periods,
    limit=6
):

    if not periods:
        return []

    periods = sorted(
        periods,
        key=lambda x: x["start"]
    )

    selected = periods[-limit:]

    result = []

    for p in selected:

        start = p["start"]

        label = start.strftime(
            "%H:%M"
        )

        day_label = start.strftime(
            "%m/%d"
        )

        patterns = []

        result.append({
            **p,
            "label": label,
            "day_label": day_label,
            "patterns": patterns
        })

    return result


# =========================================================
# 기간 패턴 계산
# =========================================================

def apply_patterns_to_periods(
    periods,
    all_periods=None
):

    if not periods:
        return periods

    source = (
        all_periods
        if all_periods is not None
        else periods
    )

    source = sorted(
        source,
        key=lambda x: x["start"]
    )

    index_map = {
        p["start"]: i
        for i, p in enumerate(source)
    }

    for p in periods:

        idx = index_map.get(
            p["start"]
        )

        if idx is None:
            p["patterns"] = []
            continue

        start_idx = max(
            0,
            idx - 5
        )

        sample = source[
            start_idx:idx + 1
        ]

        p["patterns"] = detect_patterns(
            sample
        )

    return periods


# =========================================================
# SIGNAL 판정
# =========================================================

def signal_patterns_from_candles(
    candles
):

    patterns = detect_patterns(
        candles
    )

    return [
        p for p in patterns
        if p in SIGNAL_CANDLE_PATTERNS
    ]


def is_signal_candle(
    candles
):

    return bool(
        signal_patterns_from_candles(
            candles
        )
    )


def current_signal(
    daily_candle,
    four_hour_candle,
    daily_candles,
    four_hour_candles,
    current_price
):

    if not daily_candle:
        return False

    if not four_hour_candle:
        return False

    daily_change = calculate_period_change(
        daily_candle,
        current_price
    )

    four_hour_change = calculate_period_change(
        four_hour_candle,
        current_price
    )

    if daily_change is None:
        return False

    if four_hour_change is None:
        return False

    if daily_change <= 0:
        return False

    if four_hour_change <= 0:
        return False

    four_hour_patterns = signal_patterns_from_candles(
        four_hour_candles
    )

    return bool(
        four_hour_patterns
    )


def daily_signal_pass(
    daily_candle,
    daily_candles,
    current_price
):

    if not daily_candle:
        return False

    change = calculate_period_change(
        daily_candle,
        current_price
    )

    if change is None:
        return False

    if change <= 0:
        return False

    return bool(
        signal_patterns_from_candles(
            daily_candles
        )
    )


# =========================================================
# CoinGecko ID
# =========================================================

def resolve_coin_id(symbol):

    symbol = symbol.lower().strip()

    if not symbol:
        return None

    if symbol in coin_id_cache:
        return coin_id_cache[symbol]

    # BTC 기본
    if symbol == "btc":
        coin_id_cache[symbol] = "bitcoin"
        return "bitcoin"

    # CoinGecko 검색
    response = request_get(
        "https://api.coingecko.com/api/v3/search",
        params={
            "query": symbol
        },
        retries=3,
        timeout=10
    )

    if not response:
        return None

    try:

        data = response.json()

        coins = data.get(
            "coins",
            []
        )

        symbol_upper = symbol.upper()

        # 정확한 심볼 우선
        exact = [
            c for c in coins
            if str(
                c.get("symbol", "")
            ).upper()
            == symbol_upper
        ]

        if exact:

            # market cap rank가 있는 경우
            exact.sort(
                key=lambda x:
                x.get(
                    "market_cap_rank"
                )
                or 999999
            )

            coin_id = exact[0].get(
                "id"
            )

        elif coins:

            coin_id = coins[0].get(
                "id"
            )

        else:

            coin_id = None

        if coin_id:

            coin_id_cache[
                symbol
            ] = coin_id

        return coin_id

    except Exception as e:

        logging.warning(
            "CoinGecko ID 검색 실패 %s: %s",
            symbol,
            e
        )

        return None


# =========================================================
# 뉴스 API
# =========================================================

def get_news_headers():

    if not COINGECKO_API_KEY:
        return {}

    return {
        "x-cg-pro-api-key":
            COINGECKO_API_KEY
    }


def fetch_coin_news(
    symbol,
    limit=3
):

    symbol = symbol.upper()

    coin_id = resolve_coin_id(
        symbol
    )

    if not coin_id:
        return []

    cache_key = (
        f"{coin_id}:"
        f"{NEWS_LANGUAGE}:"
        f"{NEWS_TYPE}"
    )

    cached = news_cache.get(
        cache_key
    )

    if cached:

        cached_time = cached.get(
            "time"
        )

        if cached_time:

            age = (
                datetime.now()
                - cached_time
            ).total_seconds()

            if age < (
                NEWS_CACHE_MINUTES
                * 60
            ):

                return cached.get(
                    "articles",
                    []
                )[:limit]

    params = {
        "coin_id": coin_id,
        "language": NEWS_LANGUAGE,
        "type": NEWS_TYPE,
        "per_page": min(
            max(limit, 1),
            20
        ),
        "page": 1
    }

    response = request_get(
        f"{COINGECKO_BASE_URL}/news",
        params=params,
        headers=get_news_headers(),
        retries=3,
        timeout=20
    )

    if not response:
        return []

    try:

        data = response.json()

        if not isinstance(
            data,
            list
        ):
            return []

        articles = []

        for article in data:

            title = str(
                article.get(
                    "title",
                    ""
                )
            ).strip()

            url = str(
                article.get(
                    "url",
                    ""
                )
            ).strip()

            source = str(
                article.get(
                    "source_name",
                    ""
                )
            ).strip()

            posted_at = str(
                article.get(
                    "posted_at",
                    ""
                )
            ).strip()

            if not title:
                continue

            articles.append({
                "title": title,
                "url": url,
                "source": source,
                "posted_at": posted_at,
                "coin_id": coin_id
            })

        news_cache[cache_key] = {
            "time": datetime.now(),
            "articles": articles
        }

        return articles[:limit]

    except Exception as e:

        logging.warning(
            "뉴스 파싱 실패 %s: %s",
            symbol,
            e
        )

        return []


def fetch_btc_news():

    return fetch_coin_news(
        "BTC",
        3
    )


# =========================================================
# 뉴스 제목 정리
# =========================================================

def clean_news_title(title):

    if not title:
        return ""

    title = re.sub(
        r"\s+",
        " ",
        title
    ).strip()

    return title


def news_line_html(
    article,
    index
):

    title = clean_news_title(
        article.get(
            "title",
            ""
        )
    )

    source = article.get(
        "source",
        ""
    )

    url = article.get(
        "url",
        ""
    )

    if len(title) > 85:
        title = title[:82] + "..."

    title_html = html.escape(
        title
    )

    source_html = html.escape(
        source
    )

    if url:

        return f"""
        <div class="news-line">
            <span class="news-index">
                {index}.
            </span>
            <a
                href="{html.escape(url)}"
                target="_blank"
                rel="noopener noreferrer"
            >
                {title_html}
            </a>
            <span class="news-source">
                {source_html}
            </span>
        </div>
        """

    return f"""
    <div class="news-line">
        <span class="news-index">
            {index}.
        </span>
        <span>
            {title_html}
        </span>
        <span class="news-source">
            {source_html}
        </span>
    </div>
    """


def news_block_html(
    title,
    articles,
    css_class=""
):

    if not articles:
        return ""

    rows = []

    for i, article in enumerate(
        articles[:3],
        1
    ):

        rows.append(
            news_line_html(
                article,
                i
            )
        )

    return f"""
    <div class="news-block {css_class}">
        <div class="news-title">
            {html.escape(title)}
        </div>
        <div class="news-list">
            {''.join(rows)}
        </div>
    </div>
    """


# =========================================================
# SIGNAL 뉴스
# =========================================================

def collect_signal_news(
    signal_items
):

    all_articles = []

    seen = set()

    for item in signal_items:

        symbol = item.get(
            "symbol"
        )

        if not symbol:
            continue

        articles = fetch_coin_news(
            symbol,
            3
        )

        for article in articles:

            key = (
                article.get("url")
                or
                article.get("title")
            )

            if key in seen:
                continue

            seen.add(key)

            article = dict(article)

            article["symbol"] = symbol

            all_articles.append(
                article
            )

    # 최신순
    all_articles.sort(
        key=lambda x:
        x.get(
            "posted_at",
            ""
        ),
        reverse=True
    )

    return all_articles[:3]


# =========================================================
# Upbit 데이터
# =========================================================

def build_coin_data(
    ticker
):

    market = ticker.get(
        "market"
    )

    if not market:
        return None

    symbol = market.replace(
        "KRW-",
        ""
    )

    current_price = safe_float(
        ticker.get(
            "trade_price"
        )
    )

    if current_price is None:
        return None

    # ---------------------------------------------
    # 1H
    # ---------------------------------------------

    hourly = get_upbit_candles(
        market,
        unit=60,
        count=200
    )

    four_hour = build_4h_candles(
        hourly
    )

    # ---------------------------------------------
    # 일봉
    # ---------------------------------------------

    daily_raw = get_upbit_daily_candles(
        market,
        count=200
    )

    daily = normalize_daily_candles(
        daily_raw
    )

    if not daily:
        return None

    if not four_hour:
        return None

    four_hour = add_period_changes(
        four_hour,
        current_price
    )

    daily = add_period_changes(
        daily,
        current_price
    )

    # ---------------------------------------------
    # 현재 기간
    # ---------------------------------------------

    current_4h = None

    for p in four_hour:

        if p.get("active"):

            current_4h = p
            break

    if current_4h is None:

        current_4h = four_hour[-1]

    current_daily = None

    for p in daily:

        if p.get("active"):

            current_daily = p
            break

    if current_daily is None:

        current_daily = daily[-1]

    # ---------------------------------------------
    # 패턴
    # ---------------------------------------------

    four_hour = apply_patterns_to_periods(
        four_hour,
        four_hour
    )

    daily = apply_patterns_to_periods(
        daily,
        daily
    )

    current_4h = next(
        (
            p for p in four_hour
            if p.get("active")
        ),
        four_hour[-1]
    )

    current_daily = next(
        (
            p for p in daily
            if p.get("active")
        ),
        daily[-1]
    )

    four_hour_patterns = current_4h.get(
        "patterns",
        []
    )

    daily_patterns = current_daily.get(
        "patterns",
        []
    )

    four_hour_signal_patterns = [
        p for p in four_hour_patterns
        if p in SIGNAL_CANDLE_PATTERNS
    ]

    daily_signal_patterns = [
        p for p in daily_patterns
        if p in SIGNAL_CANDLE_PATTERNS
    ]

    daily_change = current_daily.get(
        "change"
    )

    current_4h_change = current_4h.get(
        "change"
    )

    signal_4h = (
        daily_change is not None
        and daily_change > 0
        and current_4h_change is not None
        and current_4h_change > 0
        and bool(
            four_hour_signal_patterns
        )
    )

    signal_daily = (
        daily_change is not None
        and daily_change > 0
        and bool(
            daily_signal_patterns
        )
    )

    signal_dual = (
        signal_4h
        and signal_daily
    )

    # ---------------------------------------------
    # 거래대금
    # ---------------------------------------------

    volume_24h = safe_float(
        ticker.get(
            "acc_trade_price_24h"
        ),
        0
    )

    change_rate_24h = safe_float(
        ticker.get(
            "signed_change_rate"
        ),
        0
    )

    return {
        "market": market,
        "symbol": symbol,
        "name": symbol,
        "current_price": current_price,
        "volume_24h": volume_24h,
        "volume_rank": 999999,
        "change_24h": (
            change_rate_24h * 100
        ),
        "daily_change": daily_change,
        "current_4h_change": current_4h_change,
        "current_daily": current_daily,
        "current_4h": current_4h,
        "daily_periods": make_period_display(
            daily,
            6
        ),
        "four_hour_periods": make_period_display(
            four_hour,
            6
        ),
        "signal_4h": signal_4h,
        "signal_daily": signal_daily,
        "signal_dual": signal_dual,
        "signal_patterns": four_hour_signal_patterns,
        "daily_signal_patterns": daily_signal_patterns,
        "has_news": False
    }


# =========================================================
# 전체 Upbit 갱신
# =========================================================

def update_upbit_data():

    global latest_upbit_data
    global latest_upbit_update_time
    global latest_btc_news
    global latest_signal_news

    if USE_UPBIT != "Y":
        return

    try:

        markets = get_upbit_markets()

        if not markets:
            logging.warning(
                "Upbit 마켓 조회 실패"
            )
            return

        tickers = get_upbit_tickers(
            markets
        )

        if not tickers:
            logging.warning(
                "Upbit ticker 조회 실패"
            )
            return

        # 거래대금 순위
        tickers = sorted(
            tickers,
            key=lambda x:
            safe_float(
                x.get(
                    "acc_trade_price_24h"
                ),
                0
            ),
            reverse=True
        )

        # TOP_N + BTC
        selected = tickers[:TOP_N]

        btc_ticker = next(
            (
                x for x in tickers
                if x.get("market")
                == "KRW-BTC"
            ),
            None
        )

        if btc_ticker and btc_ticker not in selected:

            selected = (
                [btc_ticker]
                + selected
            )

        selected_markets = {
            x.get("market")
            for x in selected
        }

        data = []

        for ticker in selected:

            item = build_coin_data(
                ticker
            )

            if not item:
                continue

            data.append(item)

        # 거래대금 순위
        data.sort(
            key=lambda x:
            x.get(
                "volume_24h",
                0
            ),
            reverse=True
        )

        for rank, item in enumerate(
            data,
            1
        ):

            item[
                "volume_rank"
            ] = rank

        # BTC 별도
        btc_item = next(
            (
                x for x in data
                if x.get("symbol")
                == "BTC"
            ),
            None
        )

        if btc_item:

            latest_btc_data = btc_item

        # -----------------------------------------
        # SIGNAL 뉴스
        # -----------------------------------------

        signal_items = [
            x for x in data
            if x.get("signal_4h")
        ]

        for item in signal_items:

            articles = fetch_coin_news(
                item["symbol"],
                1
            )

            item["has_news"] = bool(
                articles
            )

        latest_signal_news = (
            collect_signal_news(
                signal_items
            )
        )

        # -----------------------------------------
        # BTC 뉴스
        # -----------------------------------------

        if btc_item:

            latest_btc_news = fetch_btc_news()

        else:

            latest_btc_news = []

        latest_upbit_data = data

        latest_upbit_update_time = now_kst()

        logging.info(
            "Upbit 갱신 완료: %d개 / SIGNAL %d개",
            len(data),
            len(signal_items)
        )

    except Exception as e:

        logging.exception(
            "Upbit 갱신 오류: %s",
            e
        )


# =========================================================
# 카드 상단 요약
# =========================================================

def current_change_class(
    value
):

    if value is None:
        return "neutral"

    if value > 0:
        return "positive"

    if value < 0:
        return "negative"

    return "neutral"


def compact_market_stats(
    item
):

    price = format_price(
        item.get(
            "current_price"
        )
    )

    volume = format_volume_krw(
        item.get(
            "volume_24h"
        )
    )

    change = item.get(
        "change_24h"
    )

    change_class = (
        "positive"
        if change is not None
        and change > 0
        else
        "negative"
        if change is not None
        and change < 0
        else
        "neutral"
    )

    return f"""
    <div class="compact-stats">

        <div class="compact-stat">
            <span class="compact-label">
                현재가
            </span>
            <span class="compact-value">
                {price}
            </span>
        </div>

        <div class="compact-stat">
            <span class="compact-label">
                거래대금
            </span>
            <span class="compact-value">
                {volume}
            </span>
        </div>

        <div class="compact-stat">
            <span class="compact-label">
                24H
            </span>
            <span class="compact-value change-{change_class}">
                {format_change(change)}
            </span>
        </div>

    </div>
    """


# =========================================================
# 일봉 / 4H 셀
# =========================================================

def period_cells_html(
    periods,
    period_type="4H"
):

    if not periods:

        return (
            '<div class="no-period-data">-</div>'
        )

    cells = []

    for period in periods:

        if period.get(
            "active",
            False
        ):

            change = period.get(
                "change"
            )

            if (
                change is not None
                and change > 0
            ):

                cell_class = (
                    "period-cell "
                    "current-period "
                    "current-positive"
                )

            elif (
                change is not None
                and change < 0
            ):

                cell_class = (
                    "period-cell "
                    "current-period "
                    "current-negative"
                )

            else:

                cell_class = (
                    "period-cell "
                    "current-period "
                    "current-zero"
                )

            day_text = "현재"

        else:

            cell_class = (
                "period-cell"
            )

            day_text = period.get(
                "day_label",
                ""
            )

        time_text = period.get(
            "label",
            "-"
        )

        cells.append(
            f"""
            <div class="{cell_class}">

                <div class="period-day">
                    {html.escape(
                        str(day_text)
                    )}
                </div>

                <div class="period-time">
                    {html.escape(
                        str(time_text)
                    )}
                </div>

                <div class="period-value">
                    {format_change(
                        period.get(
                            "change"
                        )
                    )}
                </div>

                {
                    candle_pattern_html(
                        period.get(
                            "patterns",
                            []
                        )
                    )
                }

            </div>
            """
        )

    return "".join(cells)


# =========================================================
# 뉴스 있음 배지
# =========================================================

def news_badge(item):

    if not item.get(
        "has_news",
        False
    ):
        return ""

    return """
    <span class="news-badge">
        📰 뉴스 있음
    </span>
    """


# =========================================================
# SIGNAL 카드
# =========================================================

def signal_card_html(
    item
):

    symbol = item.get(
        "symbol",
        "-"
    )

    if item.get(
        "signal_dual"
    ):

        signal_title = (
            "🔥 일봉 + 4H SIGNAL"
        )

        card_class = (
            "market-card "
            "signal-card "
            "dual-signal"
        )

    else:

        signal_title = (
            "🚀 4H SIGNAL"
        )

        card_class = (
            "market-card "
            "signal-card"
        )

    return f"""
    <div class="{card_class}">

        <div class="market-card-top">

            <div class="coin-name">
                {html.escape(symbol)}
            </div>

            <div class="card-badges">
                {news_badge(item)}
            </div>

        </div>

        <div class="signal-title">
            {signal_title}
        </div>

        {compact_market_stats(item)}

        <div class="signal-pattern">
            {
                " · ".join(
                    item.get(
                        "signal_patterns",
                        []
                    )
                )
            }
        </div>

    </div>
    """


# =========================================================
# TOP 카드
# =========================================================

def top_card_html(
    item
):

    symbol = item.get(
        "symbol",
        "-"
    )

    rank = item.get(
        "volume_rank",
        "-"
    )

    daily_periods = item.get(
        "daily_periods",
        []
    )

    four_hour_periods = item.get(
        "four_hour_periods",
        []
    )

    signal_mark = ""

    if item.get(
        "signal_dual"
    ):

        signal_mark = (
            '<span class="dual-mark">'
            '🔥 SIGNAL'
            '</span>'
        )

    elif item.get(
        "signal_4h"
    ):

        signal_mark = (
            '<span class="signal-mark">'
            '🚀 SIGNAL'
            '</span>'
        )

    return f"""
    <div class="market-card top-card">

        <div class="market-card-top">

            <div class="coin-name">
                <span class="rank">
                    #{rank}
                </span>

                {html.escape(symbol)}

                {signal_mark}
            </div>

            <div class="card-badges">
                {news_badge(item)}
            </div>

        </div>

        {compact_market_stats(item)}

        <div class="period-section">

            <div class="period-header">
                <span>일봉</span>
            </div>

            <div class="period-grid daily-grid">
                {
                    period_cells_html(
                        daily_periods,
                        "1D"
                    )
                }
            </div>

        </div>

        <div class="period-section">

            <div class="period-header">
                <span>4H</span>
            </div>

            <div class="period-grid">
                {
                    period_cells_html(
                        four_hour_periods,
                        "4H"
                    )
                }
            </div>

        </div>

    </div>
    """


# =========================================================
# SIGNAL 영역
# =========================================================

def focus_section(data):

    signals = [
        x for x in data
        if x.get(
            "signal_4h"
        )
    ]

    if not signals:
        return ""

    signals.sort(
        key=lambda x:
        x.get(
            "volume_rank",
            999999
        )
    )

    cards = []

    for item in signals:

        cards.append(
            signal_card_html(
                item
            )
        )

    return f"""
    <section class="section signal-section">

        <div class="section-title-row">

            <div class="section-title">
                🚀 SIGNAL
            </div>

            <div class="section-subtitle">
                거래대금 순
            </div>

        </div>

        <div class="market-grid signal-grid">
            {"".join(cards)}
        </div>

    </section>
    """


# =========================================================
# BTC 시황 카드
# =========================================================

def btc_summary_html():

    if not latest_btc_data:
        return ""

    item = latest_btc_data

    daily_change = item.get(
        "daily_change"
    )

    four_hour_change = item.get(
        "current_4h_change"
    )

    daily_class = current_change_class(
        daily_change
    )

    four_class = current_change_class(
        four_hour_change
    )

    return f"""
    <section class="btc-summary">

        <div class="btc-summary-header">

            <div class="btc-title">
                ₿ BTC 시황
            </div>

            <div class="btc-price">
                {format_price(
                    item.get(
                        "current_price"
                    )
                )}
            </div>

        </div>

        <div class="btc-stats">

            <div>
                <span>거래대금</span>
                <strong>
                    {format_volume_krw(
                        item.get(
                            "volume_24h"
                        )
                    )}
                </strong>
            </div>

            <div>
                <span>일봉</span>
                <strong class="{daily_class}">
                    {format_change(
                        daily_change
                    )}
                </strong>
            </div>

            <div>
                <span>4H</span>
                <strong class="{four_class}">
                    {format_change(
                        four_hour_change
                    )}
                </strong>
            </div>

        </div>

    </section>
    """


# =========================================================
# 뉴스 영역
# =========================================================

def market_news_section():

    blocks = []

    btc_block = news_block_html(
        "📰 BTC 시황",
        latest_btc_news,
        "btc-news"
    )

    if btc_block:
        blocks.append(
            btc_block
        )

    signal_block = news_block_html(
        "🔥 SIGNAL 뉴스",
        latest_signal_news,
        "signal-news"
    )

    if signal_block:
        blocks.append(
            signal_block
        )

    if not blocks:
        return ""

    return f"""
    <section class="news-section">
        {"".join(blocks)}
    </section>
    """


# =========================================================
# TOP 전체
# =========================================================

def top_section(data):

    if not data:
        return ""

    sorted_data = sorted(
        data,
        key=lambda x:
        x.get(
            "volume_rank",
            999999
        )
    )

    cards = []

    for item in sorted_data:

        cards.append(
            top_card_html(
                item
            )
        )

    return f"""
    <section class="section top-section">

        <div class="section-title-row">

            <div class="section-title">
                📊 UPBIT TOP {TOP_N}
            </div>

            <div class="section-subtitle">
                거래대금 순
            </div>

        </div>

        <div class="market-grid top-grid">
            {"".join(cards)}
        </div>

    </section>
    """


# =========================================================
# BTC 일봉 / 4H
# =========================================================

def btc_period_section():

    if not latest_btc_data:
        return ""

    item = latest_btc_data

    daily_periods = item.get(
        "daily_periods",
        []
    )

    four_hour_periods = item.get(
        "four_hour_periods",
        []
    )

    return f"""
    <section class="btc-period-section">

        <div class="btc-period-block">

            <div class="period-header-main">
                BTC 일봉
            </div>

            <div class="period-grid daily-grid">
                {
                    period_cells_html(
                        daily_periods,
                        "1D"
                    )
                }
            </div>

        </div>

        <div class="btc-period-block">

            <div class="period-header-main">
                BTC 4H
            </div>

            <div class="period-grid">
                {
                    period_cells_html(
                        four_hour_periods,
                        "4H"
                    )
                }
            </div>

        </div>

    </section>
    """


# =========================================================
# Dashboard
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def dashboard():

    update_time = (
        latest_upbit_update_time
        .strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        if latest_upbit_update_time
        else "-"
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
            Crypto Signal Dashboard
        </title>

        <style>

            * {{
                box-sizing: border-box;
            }}

            html,
            body {{
                margin: 0;
                padding: 0;
                background: #090b0e;
                color: #e7eaee;
                font-family:
                    -apple-system,
                    BlinkMacSystemFont,
                    "Segoe UI",
                    Roboto,
                    Arial,
                    sans-serif;
            }}

            body {{
                padding: 10px;
            }}

            .container {{
                width: 100%;
                max-width: 1800px;
                margin: 0 auto;
            }}

            /* =================================================
               BTC
               ================================================= */

            .btc-summary {{
                background: #101419;
                border: 1px solid #20262d;
                border-radius: 10px;
                padding: 8px 10px;
                margin-bottom: 7px;
            }}

            .btc-summary-header {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                gap: 10px;
                margin-bottom: 5px;
            }}

            .btc-title {{
                font-size: 14px;
                font-weight: 800;
            }}

            .btc-price {{
                font-size: 15px;
                font-weight: 800;
            }}

            .btc-stats {{
                display: grid;
                grid-template-columns:
                    repeat(3, minmax(0, 1fr));
                gap: 5px;
            }}

            .btc-stats > div {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                background: #0c1014;
                border-radius: 6px;
                padding: 4px 7px;
                min-height: 25px;
            }}

            .btc-stats span {{
                color: #7e8994;
                font-size: 10px;
            }}

            .btc-stats strong {{
                font-size: 11px;
            }}

            /* =================================================
               뉴스
               ================================================= */

            .news-section {{
                display: flex;
                flex-direction: column;
                gap: 6px;
                margin-bottom: 8px;
            }}

            .news-block {{
                background: #0f1419;
                border: 1px solid #20262d;
                border-radius: 8px;
                padding: 6px 9px;
            }}

            .news-title {{
                font-size: 11px;
                font-weight: 800;
                color: #cbd2d9;
                margin-bottom: 3px;
            }}

            .news-list {{
                display: flex;
                flex-direction: column;
            }}

            .news-line {{
                display: flex;
                align-items: baseline;
                gap: 4px;
                min-height: 17px;
                line-height: 1.35;
                font-size: 10px;
                color: #b9c0c7;
            }}

            .news-line a {{
                color: #cbd2d9;
                text-decoration: none;
                overflow: hidden;
                text-overflow: ellipsis;
                white-space: nowrap;
            }}

            .news-line a:hover {{
                text-decoration: underline;
                color: #ffffff;
            }}

            .news-index {{
                color: #737e89;
                flex: 0 0 auto;
            }}

            .news-source {{
                color: #69747e;
                font-size: 9px;
                flex: 0 0 auto;
            }}

            /* =================================================
               Section
               ================================================= */

            .section {{
                margin-bottom: 9px;
            }}

            .section-title-row {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin: 5px 2px;
            }}

            .section-title {{
                font-size: 13px;
                font-weight: 800;
            }}

            .section-subtitle {{
                color: #68727c;
                font-size: 9px;
            }}

            /* =================================================
               Cards
               ================================================= */

            .market-grid {{
                display: grid;
                grid-template-columns:
                    repeat(
                        auto-fill,
                        minmax(250px, 1fr)
                    );
                gap: 6px;
            }}

            .market-card {{
                background: #11161b;
                border: 1px solid #222930;
                border-radius: 8px;

                /* 카드 높이 최소화 */
                padding: 6px 7px;

                min-height: 0;
            }}

            .market-card-top {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                min-height: 17px;
                margin-bottom: 3px;
            }}

            .coin-name {{
                display: flex;
                align-items: center;
                gap: 4px;
                font-size: 12px;
                font-weight: 800;
                white-space: nowrap;
            }}

            .rank {{
                color: #68727d;
                font-size: 9px;
                font-weight: 700;
            }}

            .card-badges {{
                display: flex;
                align-items: center;
                gap: 3px;
            }}

            .news-badge {{
                display: inline-flex;
                align-items: center;
                padding: 2px 4px;
                border-radius: 4px;
                background: #182019;
                color: #86c89e;
                font-size: 8px;
                font-weight: 700;
            }}

            .signal-mark {{
                color: #efc96c;
                font-size: 8px;
            }}

            .dual-mark {{
                color: #f2ca64;
                font-size: 8px;
            }}

            .signal-title {{
                font-size: 9px;
                color: #e3bd64;
                margin-bottom: 3px;
            }}

            .signal-pattern {{
                font-size: 8px;
                color: #c8a95e;
                margin-top: 3px;
            }}

            .dual-signal {{
                border-color: #715d2c;
            }}

            /* =================================================
               현재가 / 거래대금 / 등락률
               ================================================= */

            .compact-stats {{
                display: grid;
                grid-template-columns:
                    1fr 1fr 1fr;
                gap: 3px;
                margin-bottom: 4px;
            }}

            .compact-stat {{
                display: flex;
                flex-direction: column;
                justify-content: center;

                min-width: 0;

                background: #0b0f13;
                border-radius: 4px;

                padding: 3px 5px;

                min-height: 31px;
            }}

            .compact-label {{
                color: #68727c;
                font-size: 8px;
                line-height: 1.1;
                margin-bottom: 2px;
            }}

            .compact-value {{
                color: #d6dbe0;
                font-size: 10px;
                font-weight: 800;

                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
            }}

            .change-positive {{
                color: #78cfa2 !important;
            }}

            .change-negative {{
                color: #df8588 !important;
            }}

            .change-neutral {{
                color: #8a949e !important;
            }}

            /* =================================================
               기간
               ================================================= */

            .period-section {{
                margin-top: 3px;
            }}

            .period-header {{
                color: #717c86;
                font-size: 8px;
                font-weight: 700;
                margin: 2px 1px;
            }}

            .period-grid {{
                display: grid;

                grid-template-columns:
                    repeat(6, minmax(0, 1fr));

                gap: 2px;
            }}

            .period-cell {{
                min-width: 0;

                background: #0d1115;

                border-radius: 4px;

                padding: 3px 2px;

                text-align: center;

                min-height: 40px;
            }}

            .period-day {{
                color: #68727c;
                font-size: 7px;
                line-height: 1.1;
            }}

            .period-time {{
                color: #9aa3ab;
                font-size: 8px;
                line-height: 1.15;
            }}

            .period-value {{
                color: #aeb6bd;
                font-size: 8px;
                font-weight: 800;
                line-height: 1.15;
            }}

            .candle-pattern {{
                display: block;
                margin-top: 1px;

                color: #d9b861;

                font-size: 6px;

                line-height: 1.05;

                overflow: hidden;
                text-overflow: ellipsis;
                white-space: nowrap;
            }}

            /* =================================================
               현재 기간
               ================================================= */

            .current-period {{
                box-shadow:
                    inset 0 0 0 1px
                    rgba(
                        120,
                        130,
                        140,
                        0.20
                    );
            }}

            .current-period.current-positive {{
                background: #10271c !important;

                box-shadow:
                    inset 0 0 0 1px
                    rgba(
                        120,
                        207,
                        162,
                        0.40
                    );
            }}

            .current-period.current-positive
            .period-day {{
                color: #91dcb0 !important;
            }}

            .current-period.current-positive
            .period-time {{
                color: #b9f0cf !important;
            }}

            .current-period.current-positive
            .period-value {{
                color: #78cfa2 !important;
            }}

            .current-period.current-negative {{
                background: #2a1518 !important;

                box-shadow:
                    inset 0 0 0 1px
                    rgba(
                        223,
                        133,
                        136,
                        0.40
                    );
            }}

            .current-period.current-negative
            .period-day {{
                color: #e69a9d !important;
            }}

            .current-period.current-negative
            .period-time {{
                color: #f0b4b7 !important;
            }}

            .current-period.current-negative
            .period-value {{
                color: #df8588 !important;
            }}

            .current-period.current-zero {{
                background: #15191e !important;

                box-shadow:
                    inset 0 0 0 1px
                    rgba(
                        120,
                        130,
                        140,
                        0.25
                    );
            }}

            .current-period.current-zero
            .period-day {{
                color: #89939c !important;
            }}

            .current-period.current-zero
            .period-time {{
                color: #aab3ba !important;
            }}

            .current-period.current-zero
            .period-value {{
                color: #727c86 !important;
            }}

            /* =================================================
               BTC 기간
               ================================================= */

            .btc-period-section {{
                background: #0f1419;
                border: 1px solid #20262d;
                border-radius: 8px;
                padding: 6px;
                margin-bottom: 8px;
            }}

            .btc-period-block + .btc-period-block {{
                margin-top: 6px;
            }}

            .period-header-main {{
                font-size: 9px;
                font-weight: 800;
                color: #818b95;
                margin-bottom: 3px;
            }}

            /* =================================================
               모바일
               ================================================= */

            @media (
                max-width: 700px
            ) {{

                body {{
                    padding: 5px;
                }}

                .market-grid {{
                    grid-template-columns:
                        repeat(2, minmax(0, 1fr));

                    gap: 4px;
                }}

                .market-card {{
                    padding: 5px;
                }}

                .period-grid {{
                    gap: 1px;
                }}

                .period-cell {{
                    padding: 2px 1px;
                    min-height: 36px;
                }}

                .period-day {{
                    font-size: 6px;
                }}

                .period-time {{
                    font-size: 7px;
                }}

                .period-value {{
                    font-size: 7px;
                }}

                .candle-pattern {{
                    font-size: 5px;
                }}

                .news-line {{
                    font-size: 9px;
                }}

                .news-source {{
                    display: none;
                }}

            }}

            @media (
                max-width: 430px
            ) {{

                .market-grid {{
                    grid-template-columns:
                        repeat(2, minmax(0, 1fr));
                }}

                .compact-stats {{
                    gap: 2px;
                }}

                .compact-stat {{
                    padding: 3px;
                }}

                .compact-value {{
                    font-size: 9px;
                }}

            }}

        </style>

    </head>

    <body>

        <div class="container">

            {btc_summary_html()}

            {market_news_section()}

            {focus_section(
                latest_upbit_data
            )}

            {top_section(
                latest_upbit_data
            )}

            <div
                style="
                    text-align:center;
                    color:#56616b;
                    font-size:8px;
                    margin:8px 0;
                "
            >
                마지막 업데이트 :
                {html.escape(update_time)}
            </div>

        </div>

    </body>

    </html>
    """


# =========================================================
# 백그라운드 갱신
# =========================================================

def scheduled_update():

    while True:

        try:

            update_upbit_data()

        except Exception as e:

            logging.exception(
                "스케줄 업데이트 오류: %s",
                e
            )

        time.sleep(
            UPDATE_MINUTES * 60
        )


# =========================================================
# 시작
# =========================================================

@app.on_event("startup")
def startup_event():

    logging.info(
        "======================================"
    )

    logging.info(
        "Crypto Signal Dashboard 시작"
    )

    logging.info(
        "UPBIT = %s",
        USE_UPBIT
    )

    logging.info(
        "OKX = %s",
        USE_OKX
    )

    logging.info(
        "TOP_N = %s",
        TOP_N
    )

    logging.info(
        "SIGNAL 패턴 = %s",
        SIGNAL_CANDLE_PATTERNS
    )

    logging.info(
        "관통형은 SIGNAL에서 제외"
    )

    logging.info(
        "일봉 SIGNAL 별도 목록 표시 안 함"
    )

    logging.info(
        "카드 상단 현재가/거래대금/등락률 높이 최소화"
    )

    logging.info(
        "BTC 뉴스 최대 3줄"
    )

    logging.info(
        "SIGNAL 뉴스 최대 3줄"
    )

    logging.info(
        "TOP 뉴스는 뉴스 있음 배지만 표시"
    )

    if COINGECKO_API_KEY:

        logging.info(
            "CoinGecko 뉴스 API: 활성화"
        )

    else:

        logging.warning(
            "COINGECKO_API_KEY가 없습니다. "
            "뉴스 기능은 표시되지 않습니다."
        )

    logging.info(
        "======================================"
    )

    thread = threading.Thread(
        target=scheduled_update,
        daemon=True
    )

    thread.start()


# =========================================================
# 실행
# =========================================================

if __name__ == "__main__":

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(
            os.getenv(
                "PORT",
                "8000"
            )
        )
    )
