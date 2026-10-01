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
    format="%(asctime)s | %(levelname)s | %(message)s"
)

KST = ZoneInfo("Asia/Seoul")

TOP_N = 20
UPDATE_MINUTES = 1

HISTORY_CHUNK = 200
MAX_HISTORY_CHUNKS = 10

USE_UPBIT = "Y"
USE_OKX = "N"

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10


# =========================================================
# CoinGecko 뉴스 설정
# =========================================================

COINGECKO_BASE_URL = "https://pro-api.coingecko.com/api/v3"

# 실제 API KEY는 코드에 직접 입력하지 않고
# 서버 환경변수로 설정
COINGECKO_API_KEY = os.getenv(
    "COINGECKO_API_KEY",
    ""
)

NEWS_CACHE_MINUTES = 15

news_cache = {
    "feed": {
        "time": None,
        "data": []
    },
    "coin_ids": {}
}

latest_btc_news = []
latest_signal_news = []


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
# BTC
# =========================================================

OKX_BASE_URL = "https://www.okx.com"
OKX_BTC_INST_ID = "BTC-USDT"

latest_btc_okx_price = None
latest_btc_daily_change = None

latest_btc_daily_periods = []
latest_btc_4h_periods = []

latest_btc_current_daily_change = None
latest_btc_current_daily_label = "-"

latest_btc_current_4h_change = None
latest_btc_current_4h_label = "-"


# =========================================================
# 4시간 구간
# =========================================================

FOUR_HOUR_DEFINITIONS = [
    (1, 5),
    (5, 9),
    (9, 13),
    (13, 17),
    (17, 21),
    (21, 1),
]


def kst():
    return datetime.now(KST)


def get_current_4h_start(dt=None):
    if dt is None:
        dt = kst()

    hour = dt.hour

    if 1 <= hour < 5:
        start_hour = 1
        start_date = dt.date()
    elif 5 <= hour < 9:
        start_hour = 5
        start_date = dt.date()
    elif 9 <= hour < 13:
        start_hour = 9
        start_date = dt.date()
    elif 13 <= hour < 17:
        start_hour = 13
        start_date = dt.date()
    elif 17 <= hour < 21:
        start_hour = 17
        start_date = dt.date()
    else:
        start_hour = 21

        if hour < 1:
            start_date = dt.date() - timedelta(days=1)
        else:
            start_date = dt.date()

    return datetime(
        start_date.year,
        start_date.month,
        start_date.day,
        start_hour,
        0,
        0,
        tzinfo=KST
    )


def make_4h_period(start):
    if start.hour == 21:
        end = start + timedelta(days=1)
    else:
        end = start + timedelta(hours=4)

    return {
        "start": start,
        "end": end,
        "label": f"{start:%m-%d %H:%M}~{end:%m-%d %H:%M}"
    }


def get_recent_4h_periods(count=6):
    current = get_current_4h_start()
    result = []

    for i in range(count - 1, -1, -1):
        start = current - timedelta(hours=4 * i)
        result.append(make_4h_period(start))

    return result


# =========================================================
# 일봉 구간
# =========================================================

def get_current_daily_start(dt=None):
    if dt is None:
        dt = kst()

    base = datetime(
        dt.year,
        dt.month,
        dt.day,
        9,
        0,
        0,
        tzinfo=KST
    )

    if dt < base:
        base -= timedelta(days=1)

    return base


def make_daily_period(start):
    end = start + timedelta(days=1)

    return {
        "start": start,
        "end": end,
        "label": f"{start:%m-%d 09:00}~{end:%m-%d 09:00}"
    }


def get_recent_daily_periods(count=3):
    current = get_current_daily_start()

    result = []

    for i in range(count - 1, -1, -1):
        start = current - timedelta(days=i)
        result.append(make_daily_period(start))

    return result


# =========================================================
# 현재 / 이전 구간
# =========================================================

def get_current_4h_period():
    return make_4h_period(get_current_4h_start())


def get_previous_4h_period():
    current = get_current_4h_start()
    return make_4h_period(current - timedelta(hours=4))


def get_pre_previous_4h_period():
    current = get_current_4h_start()
    return make_4h_period(current - timedelta(hours=8))


def get_current_daily_period():
    return make_daily_period(get_current_daily_start())


def get_previous_daily_period():
    current = get_current_daily_start()
    return make_daily_period(
        current - timedelta(days=1)
    )


# =========================================================
# 요청 제어
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


def retry(
    func,
    *args,
    **kwargs
):
    for attempt in range(MAX_RETRIES):
        try:
            wait_request()

            response = func(
                *args,
                **kwargs
            )

            if response.status_code == 429:
                logging.warning(
                    "429 Too Many Requests - %s초 대기",
                    RATE_LIMIT_WAIT
                )

                time.sleep(
                    RATE_LIMIT_WAIT
                )

                continue

            response.raise_for_status()

            return response

        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                logging.error(
                    "API 요청 실패: %s",
                    e
                )

                return None

            time.sleep(
                min(
                    1 + attempt,
                    RATE_LIMIT_WAIT
                )
            )

    return None


# =========================================================
# CoinGecko 뉴스
# =========================================================

def coingecko_headers():
    if not COINGECKO_API_KEY:
        return {}

    return {
        "x-cg-pro-api-key": COINGECKO_API_KEY
    }


def resolve_coingecko_coin_id(symbol):
    """
    심볼을 CoinGecko coin_id로 변환.
    결과는 캐시.
    """

    if not COINGECKO_API_KEY:
        return None

    symbol = str(symbol).upper()

    if symbol == "BTC":
        return "bitcoin"

    if symbol in news_cache["coin_ids"]:
        return news_cache["coin_ids"][symbol]

    try:
        response = retry(
            requests.get,
            f"{COINGECKO_BASE_URL}/search",
            params={
                "query": symbol
            },
            headers=coingecko_headers(),
            timeout=10
        )

        if response is None:
            return None

        data = response.json()

        coins = data.get(
            "coins",
            []
        )

        if not coins:
            return None

        selected = None

        for coin in coins:
            coin_symbol = str(
                coin.get("symbol", "")
            ).upper()

            if coin_symbol == symbol:
                selected = coin
                break

        if selected is None:
            selected = coins[0]

        coin_id = selected.get("id")

        if coin_id:
            news_cache["coin_ids"][symbol] = coin_id

        return coin_id

    except Exception as e:
        logging.warning(
            "CoinGecko coin 검색 실패 [%s]: %s",
            symbol,
            e
        )

        return None


def fetch_latest_korean_news():
    """
    CoinGecko 전체 최신 한국어 뉴스.
    15분 캐시.
    """

    if not COINGECKO_API_KEY:
        return []

    cached_time = news_cache["feed"]["time"]

    if cached_time is not None:
        elapsed = (
            datetime.now()
            - cached_time
        ).total_seconds()

        if elapsed < NEWS_CACHE_MINUTES * 60:
            return news_cache["feed"]["data"]

    try:
        response = retry(
            requests.get,
            f"{COINGECKO_BASE_URL}/news",
            params={
                "language": "ko",
                "type": "news",
                "per_page": 50,
                "page": 1
            },
            headers=coingecko_headers(),
            timeout=10
        )

        if response is None:
            return news_cache["feed"]["data"]

        data = response.json()

        if isinstance(data, dict):
            articles = data.get(
                "data",
                []
            )
        else:
            articles = data

        if not isinstance(
            articles,
            list
        ):
            articles = []

        news_cache["feed"]["time"] = datetime.now()
        news_cache["feed"]["data"] = articles

        return articles

    except Exception as e:
        logging.warning(
            "CoinGecko 뉴스 요청 실패: %s",
            e
        )

        return news_cache["feed"]["data"]


def normalize_news_article(article):
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
            article.get(
                "source",
                ""
            )
        )
    ).strip()

    posted_at = str(
        article.get(
            "posted_at",
            ""
        )
    ).strip()

    return {
        "title": title,
        "url": url,
        "source": source,
        "posted_at": posted_at,
        "related_coin_ids": article.get(
            "related_coin_ids",
            []
        )
    }


def get_news_for_coin(
    symbol,
    limit=3
):
    """
    전체 뉴스 피드에서 해당 코인 관련 뉴스만 추출.
    """

    if not COINGECKO_API_KEY:
        return []

    coin_id = resolve_coingecko_coin_id(
        symbol
    )

    if not coin_id:
        return []

    articles = fetch_latest_korean_news()

    result = []

    for article in articles:

        related = article.get(
            "related_coin_ids",
            []
        )

        if not isinstance(
            related,
            list
        ):
            continue

        if coin_id not in related:
            continue

        normalized = normalize_news_article(
            article
        )

        if not normalized["title"]:
            continue

        result.append(
            normalized
        )

        if len(result) >= limit:
            break

    return result


def collect_signal_news(rows):
    """
    SIGNAL 코인들의 뉴스 중
    중복을 제거하고 최대 3개만 반환.
    """

    result = []
    seen = set()

    signal_rows = [
        row
        for row in rows
        if row.get(
            "signal_pass",
            False
        )
    ]

    for row in signal_rows:

        symbol = row.get(
            "name",
            ""
        )

        articles = get_news_for_coin(
            symbol,
            3
        )

        for article in articles:

            key = (
                article.get("url")
                or article.get("title")
            )

            if not key:
                continue

            if key in seen:
                continue

            seen.add(key)

            result.append(
                article
            )

            if len(result) >= 3:
                return result

    return result


# =========================================================
# Upbit
# =========================================================

def get_upbit_markets():
    url = (
        "https://api.upbit.com/v1/market/all"
    )

    response = retry(
        requests.get,
        url,
        params={
            "isDetails": "false"
        },
        timeout=10
    )

    if response is None:
        return []

    data = response.json()

    return [
        item["market"]
        for item in data
        if item["market"].startswith("KRW-")
    ]


def daily_change_upbit(
    market
):
    try:
        url = (
            "https://api.upbit.com/v1/candles/days"
        )

        response = retry(
            requests.get,
            url,
            params={
                "market": market,
                "count": 2
            },
            timeout=10
        )

        if response is None:
            return None

        data = response.json()

        if len(data) < 2:
            return None

        current = float(
            data[0]["trade_price"]
        )

        previous = float(
            data[1]["trade_price"]
        )

        if previous == 0:
            return 0

        return (
            (current - previous)
            / previous
            * 100
        )

    except Exception:
        return None


def get_upbit_60m_candles(
    market,
    count=200
):
    url = (
        "https://api.upbit.com/v1/candles/minutes/60"
    )

    response = retry(
        requests.get,
        url,
        params={
            "market": market,
            "count": count
        },
        timeout=10
    )

    if response is None:
        return pd.DataFrame()

    data = response.json()

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    df["candle_date_time_kst"] = pd.to_datetime(
        df["candle_date_time_kst"]
    )

    df = df.sort_values(
        "candle_date_time_kst"
    ).reset_index(drop=True)

    return df


def get_upbit_daily_candles(
    market,
    count=200
):
    url = (
        "https://api.upbit.com/v1/candles/days"
    )

    response = retry(
        requests.get,
        url,
        params={
            "market": market,
            "count": count
        },
        timeout=10
    )

    if response is None:
        return pd.DataFrame()

    data = response.json()

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    df["candle_date_time_kst"] = pd.to_datetime(
        df["candle_date_time_kst"]
    )

    df = df.sort_values(
        "candle_date_time_kst"
    ).reset_index(drop=True)

    return df


# =========================================================
# 캔들 구성요소
# =========================================================

def candle_parts(
    row
):
    o = float(row["opening_price"])
    h = float(row["high_price"])
    l = float(row["low_price"])
    c = float(row["trade_price"])

    body = abs(c - o)

    upper = h - max(o, c)
    lower = min(o, c) - l

    return {
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "body": body,
        "upper": max(upper, 0),
        "lower": max(lower, 0),
        "bull": c > o,
        "bear": c < o
    }


# =========================================================
# 단일 캔들
# =========================================================

def detect_single_candle_pattern(
    row
):
    p = candle_parts(row)

    body = p["body"]

    if body == 0:
        return "도지"

    if (
        p["lower"] > body * 2
        and p["upper"] < body
    ):
        return "망치형"

    if (
        p["upper"] > body * 2
        and p["lower"] < body
    ):
        return "역망치형"

    return ""


# =========================================================
# 2캔들
# =========================================================

def detect_two_candle_pattern(
    prev,
    current
):
    p1 = candle_parts(prev)
    p2 = candle_parts(current)

    # 상승장악
    if (
        p1["bear"]
        and p2["bull"]
        and p2["open"] <= p1["close"]
        and p2["close"] >= p1["open"]
        and p2["body"] >= p1["body"]
    ):
        return "상승장악"

    # 관통형
    if (
        p1["bear"]
        and p2["bull"]
        and p2["open"] < p1["close"]
        and p2["close"] > (
            p1["open"]
            + p1["close"]
        ) / 2
        and p2["close"] < p1["open"]
    ):
        return "관통형"

    return ""


# =========================================================
# 3캔들
# =========================================================

def detect_three_candle_pattern(
    c1,
    c2,
    c3
):
    p1 = candle_parts(c1)
    p2 = candle_parts(c2)
    p3 = candle_parts(c3)

    # 1번째가 양봉인 경우도 허용
    # 2번째가 장대음봉
    # 3번째가 상승장악 형태
    first_condition = (
        p1["bull"]
        or p1["bear"]
    )

    second_condition = (
        p2["bear"]
        and p2["body"] >= p1["body"] * 0.8
    )

    third_engulf = (
        p3["bull"]
        and p3["open"] <= p2["close"]
        and p3["close"] >= p2["open"]
    )

    if (
        first_condition
        and second_condition
        and third_engulf
    ):
        return "3캔들 상승장악"

    return ""


# =========================================================
# 4캔들
# =========================================================

def detect_four_candle_pattern(
    c1,
    c2,
    c3,
    c4
):
    p1 = candle_parts(c1)
    p2 = candle_parts(c2)
    p3 = candle_parts(c3)
    p4 = candle_parts(c4)

    if not (
        p1["bull"]
        or p1["bear"]
    ):
        return ""

    if not p2["bear"]:
        return ""

    if not p3["bear"]:
        return ""

    if not p4["bull"]:
        return ""

    middle_body = (
        p2["body"]
        + p3["body"]
    ) / 2

    if middle_body <= 0:
        return ""

    if p4["body"] < middle_body * 0.8:
        return ""

    if (
        p4["open"] <= p3["close"]
        and p4["close"] >= p2["open"]
    ):
        return "4캔들 상승장악"

    return ""


# =========================================================
# 4H 패턴
# =========================================================

def detect_4h_patterns(
    df
):
    if df is None or df.empty:
        return []

    patterns = [
        ""
        for _ in range(len(df))
    ]

    for i in range(len(df)):

        if i >= 1:
            pattern = detect_two_candle_pattern(
                df.iloc[i - 1],
                df.iloc[i]
            )

            if pattern:
                patterns[i] = pattern

        if i >= 2:
            pattern = detect_three_candle_pattern(
                df.iloc[i - 2],
                df.iloc[i - 1],
                df.iloc[i]
            )

            if pattern:
                patterns[i] = pattern

        if i >= 3:
            pattern = detect_four_candle_pattern(
                df.iloc[i - 3],
                df.iloc[i - 2],
                df.iloc[i - 1],
                df.iloc[i]
            )

            if pattern:
                patterns[i] = pattern

    return patterns


# =========================================================
# 4H 캔들 생성
# =========================================================

def build_upbit_4h_candles(
    df
):
    if df is None or df.empty:
        return pd.DataFrame()

    work = df.copy()

    work["dt"] = pd.to_datetime(
        work["candle_date_time_kst"]
    )

    work["period_start"] = (
        work["dt"]
        .dt.floor("4h")
    )

    result = []

    for start, group in work.groupby(
        "period_start"
    ):

        if len(group) == 0:
            continue

        first = group.iloc[0]
        last = group.iloc[-1]

        result.append({
            "period_start": start,
            "opening_price": float(
                first["opening_price"]
            ),
            "high_price": float(
                group["high_price"].max()
            ),
            "low_price": float(
                group["low_price"].min()
            ),
            "trade_price": float(
                last["trade_price"]
            ),
            "volume": float(
                group["candle_acc_trade_volume"].sum()
            ),
            "value": float(
                group["candle_acc_trade_price"].sum()
            )
        })

    result_df = pd.DataFrame(result)

    if result_df.empty:
        return result_df

    result_df = result_df.sort_values(
        "period_start"
    ).reset_index(drop=True)

    return result_df


# =========================================================
# 일봉 캔들 생성
# =========================================================

def build_upbit_daily_candles(
    df
):
    if df is None or df.empty:
        return pd.DataFrame()

    work = df.copy()

    work["dt"] = pd.to_datetime(
        work["candle_date_time_kst"]
    )

    result = []

    for _, row in work.iterrows():

        result.append({
            "period_start": row["dt"],
            "opening_price": float(
                row["opening_price"]
            ),
            "high_price": float(
                row["high_price"]
            ),
            "low_price": float(
                row["low_price"]
            ),
            "trade_price": float(
                row["trade_price"]
            ),
            "volume": float(
                row["candle_acc_trade_volume"]
            ),
            "value": float(
                row["candle_acc_trade_price"]
            )
        })

    result_df = pd.DataFrame(result)

    if result_df.empty:
        return result_df

    return result_df.sort_values(
        "period_start"
    ).reset_index(drop=True)


# =========================================================
# 4H 분석
# =========================================================

def analyze_4h(
    df
):
    if df is None or df.empty:
        return []

    result = []

    patterns = detect_4h_patterns(df)

    for i, row in df.iterrows():

        start = row["period_start"]

        if start.tzinfo is None:
            start = start.replace(
                tzinfo=KST
            )

        end = start + timedelta(
            hours=4
        )

        if start.hour == 21:
            end = start + timedelta(
                days=1
            )

        change = 0

        if float(row["opening_price"]) != 0:
            change = (
                (
                    float(row["trade_price"])
                    - float(row["opening_price"])
                )
                / float(row["opening_price"])
                * 100
            )

        result.append({
            "start": start,
            "end": end,
            "label": (
                f"{start:%m-%d %H:%M}"
                f"~"
                f"{end:%m-%d %H:%M}"
            ),
            "change": change,
            "pattern": patterns[i]
        })

    return result


# =========================================================
# 일봉 분석
# =========================================================

def analyze_daily(
    df
):
    if df is None or df.empty:
        return []

    result = []

    for i, row in df.iterrows():

        start = row["period_start"]

        if start.tzinfo is None:
            start = start.replace(
                tzinfo=KST
            )

        end = start + timedelta(
            days=1
        )

        change = 0

        if float(row["opening_price"]) != 0:
            change = (
                (
                    float(row["trade_price"])
                    - float(row["opening_price"])
                )
                / float(row["opening_price"])
                * 100
            )

        pattern = ""

        if i >= 1:
            pattern = detect_two_candle_pattern(
                df.iloc[i - 1],
                row
            )

        if i >= 2:
            p = detect_three_candle_pattern(
                df.iloc[i - 2],
                df.iloc[i - 1],
                row
            )

            if p:
                pattern = p

        if i >= 3:
            p = detect_four_candle_pattern(
                df.iloc[i - 3],
                df.iloc[i - 2],
                df.iloc[i - 1],
                row
            )

            if p:
                pattern = p

        result.append({
            "start": start,
            "end": end,
            "label": (
                f"{start:%m-%d %H:%M}"
                f"~"
                f"{end:%m-%d %H:%M}"
            ),
            "change": change,
            "pattern": pattern
        })

    return result


# =========================================================
# 전체 분석
# =========================================================

def analyze(
    market,
    df_60m,
    df_daily
):
    df_4h = build_upbit_4h_candles(
        df_60m
    )

    df_day = build_upbit_daily_candles(
        df_daily
    )

    four_hour = analyze_4h(
        df_4h
    )

    daily = analyze_daily(
        df_day
    )

    return {
        "four_hour": four_hour,
        "daily": daily
    }


# =========================================================
# 값 추출
# =========================================================

def get_change_value(
    value
):
    if value is None:
        return 0

    try:
        return float(value)
    except Exception:
        return 0


def format_change(
    value
):
    if value is None:
        return "-"

    value = get_change_value(
        value
    )

    if value > 0:
        return f"+{value:.2f}%"

    if value < 0:
        return f"{value:.2f}%"

    return "0.00%"


def format_market_price(
    value
):
    if value is None:
        return "-"

    try:
        value = float(value)

        if value >= 100000000:
            return f"{value:,.0f}"

        if value >= 10000:
            return f"{value:,.0f}"

        if value >= 1:
            return f"{value:,.2f}"

        return f"{value:,.6f}"

    except Exception:
        return "-"


def format_volume(
    value
):
    if value is None:
        return "-"

    try:
        value = float(value)

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
                f"{value / 10_000:.1f}만원"
            )

        return f"{value:,.0f}"

    except Exception:
        return "-"


def make_row(
    market,
    name,
    price,
    volume,
    change,
    four_hour,
    daily,
    volume_rank=None
):
    return {
        "market": market,
        "name": name,
        "price": price,
        "volume": volume,
        "change": change,
        "four_hour": four_hour,
        "daily": daily,
        "volume_rank": volume_rank,
        "signal_pass": False,
        "daily_signal_pass": False,
        "has_news": False
    }


# =========================================================
# SIGNAL 설정
# =========================================================

SIGNAL_CANDLE_PATTERNS = [
    "상승장악",
    "3캔들 상승장악",
    "4캔들 상승장악"
]


def calculate_signal_conditions(
    row
):
    four_hour = row.get(
        "four_hour",
        []
    )

    daily = row.get(
        "daily",
        []
    )

    current_4h = (
        four_hour[-1]
        if four_hour
        else None
    )

    current_daily = (
        daily[-1]
        if daily
        else None
    )

    # -----------------------------------------------------
    # 4H SIGNAL
    # -----------------------------------------------------

    signal_pass = False

    if current_4h:

        four_change = get_change_value(
            current_4h.get(
                "change"
            )
        )

        four_pattern = current_4h.get(
            "pattern",
            ""
        )

        signal_pass = (
            four_change > 0
            and four_pattern
            in SIGNAL_CANDLE_PATTERNS
        )

    # -----------------------------------------------------
    # 일봉 SIGNAL
    # -----------------------------------------------------

    daily_signal_pass = False

    if current_daily:

        daily_change = get_change_value(
            current_daily.get(
                "change"
            )
        )

        daily_pattern = current_daily.get(
            "pattern",
            ""
        )

        daily_signal_pass = (
            daily_change > 0
            and daily_pattern
            in SIGNAL_CANDLE_PATTERNS
        )

    row["signal_pass"] = signal_pass
    row["daily_signal_pass"] = (
        daily_signal_pass
    )

    return row


# =========================================================
# Upbit 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update_time
    global latest_upbit_markets
    global latest_signal_news

    if USE_UPBIT != "Y":
        return

    try:

        markets = get_upbit_markets()

        if not markets:
            return

        latest_upbit_markets = markets

        # -------------------------------------------------
        # KRW 전체 현재가 / 거래대금
        # -------------------------------------------------

        ticker_url = (
            "https://api.upbit.com/v1/ticker"
        )

        ticker_response = retry(
            requests.get,
            ticker_url,
            params={
                "markets": ",".join(markets)
            },
            timeout=20
        )

        if ticker_response is None:
            return

        tickers = ticker_response.json()

        ticker_map = {
            item["market"]: item
            for item in tickers
        }

        # -------------------------------------------------
        # 거래대금 TOP
        # -------------------------------------------------

        ranked = sorted(
            tickers,
            key=lambda x: float(
                x.get(
                    "acc_trade_price_24h",
                    0
                )
            ),
            reverse=True
        )

        ranked = ranked[:TOP_N]

        rows = []

        for rank, ticker in enumerate(
            ranked,
            start=1
        ):

            market = ticker.get(
                "market"
            )

            if not market:
                continue

            name = market.replace(
                "KRW-",
                ""
            )

            price = ticker.get(
                "trade_price"
            )

            volume = ticker.get(
                "acc_trade_price_24h"
            )

            change = (
                ticker.get(
                    "signed_change_rate",
                    0
                )
                * 100
            )

            # ---------------------------------------------
            # 60분 / 일봉
            # ---------------------------------------------

            df_60m = get_upbit_60m_candles(
                market,
                count=HISTORY_CHUNK
            )

            df_daily = get_upbit_daily_candles(
                market,
                count=HISTORY_CHUNK
            )

            analysis = analyze(
                market,
                df_60m,
                df_daily
            )

            row = make_row(
                market=market,
                name=name,
                price=price,
                volume=volume,
                change=change,
                four_hour=analysis[
                    "four_hour"
                ],
                daily=analysis[
                    "daily"
                ],
                volume_rank=rank
            )

            row = calculate_signal_conditions(
                row
            )

            rows.append(row)

        # -------------------------------------------------
        # SIGNAL 뉴스
        # -------------------------------------------------

        for row in rows:

            # TOP 카드에는 뉴스 존재 여부만 표시
            try:
                row["has_news"] = bool(
                    get_news_for_coin(
                        row["name"],
                        1
                    )
                )
            except Exception:
                row["has_news"] = False

        latest_signal_news = (
            collect_signal_news(rows)
        )

        latest_upbit_data = rows

        latest_upbit_update_time = (
            datetime.now(KST)
            .strftime("%Y-%m-%d %H:%M:%S")
        )

        signal_count = sum(
            1
            for row in rows
            if row.get(
                "signal_pass",
                False
            )
        )

        daily_signal_count = sum(
            1
            for row in rows
            if row.get(
                "daily_signal_pass",
                False
            )
        )

        logging.info(
            "Upbit 업데이트 완료 | TOP=%s | "
            "4H SIGNAL=%s | 일봉 SIGNAL=%s",
            len(rows),
            signal_count,
            daily_signal_count
        )

    except Exception as e:

        logging.exception(
            "Upbit 업데이트 오류: %s",
            e
        )


# =========================================================
# OKX BTC 현재가
# =========================================================

def get_okx_btc_price():

    try:

        response = retry(
            requests.get,
            f"{OKX_BASE_URL}/api/v5/market/ticker",
            params={
                "instId": OKX_BTC_INST_ID
            },
            timeout=10
        )

        if response is None:
            return None

        data = response.json()

        items = data.get(
            "data",
            []
        )

        if not items:
            return None

        return float(
            items[0]["last"]
        )

    except Exception as e:

        logging.warning(
            "OKX BTC 현재가 오류: %s",
            e
        )

        return None


# =========================================================
# OKX BTC 1H
# =========================================================

def get_okx_btc_1h_candles(
    limit=200
):

    try:

        response = retry(
            requests.get,
            f"{OKX_BASE_URL}/api/v5/market/candles",
            params={
                "instId": OKX_BTC_INST_ID,
                "bar": "1H",
                "limit": limit
            },
            timeout=10
        )

        if response is None:
            return pd.DataFrame()

        data = response.json()

        items = data.get(
            "data",
            []
        )

        if not items:
            return pd.DataFrame()

        rows = []

        for item in items:

            rows.append({
                "ts": int(item[0]),
                "open": float(item[1]),
                "high": float(item[2]),
                "low": float(item[3]),
                "close": float(item[4]),
                "vol": float(item[5]),
                "volCcyQuote": float(item[7])
            })

        df = pd.DataFrame(rows)

        df["dt"] = pd.to_datetime(
            df["ts"],
            unit="ms",
            utc=True
        ).dt.tz_convert(KST)

        df = df.sort_values(
            "dt"
        ).reset_index(drop=True)

        return df

    except Exception as e:

        logging.warning(
            "OKX BTC 1H 오류: %s",
            e
        )

        return pd.DataFrame()


def get_okx_btc_1h_history():

    result = []

    for _ in range(
        MAX_HISTORY_CHUNKS
    ):

        df = get_okx_btc_1h_candles(
            HISTORY_CHUNK
        )

        if df.empty:
            break

        result.append(df)

        break

    if not result:
        return pd.DataFrame()

    df = pd.concat(
        result,
        ignore_index=True
    )

    return df.drop_duplicates(
        subset=["ts"]
    ).sort_values(
        "ts"
    ).reset_index(drop=True)


# =========================================================
# BTC 일봉 집계
# =========================================================

def aggregate_btc_daily(
    df
):

    if df is None or df.empty:
        return []

    work = df.copy()

    work["kst_date"] = (
        work["dt"]
        - pd.Timedelta(hours=9)
    ).dt.date

    grouped = []

    for date_value, group in work.groupby(
        "kst_date"
    ):

        if group.empty:
            continue

        first = group.iloc[0]
        last = group.iloc[-1]

        grouped.append({
            "date": date_value,
            "open": float(
                first["open"]
            ),
            "high": float(
                group["high"].max()
            ),
            "low": float(
                group["low"].min()
            ),
            "close": float(
                last["close"]
            ),
            "volume": float(
                group["vol"].sum()
            )
        })

    return grouped


def build_btc_daily(
    df
):

    daily = aggregate_btc_daily(
        df
    )

    result = []

    for item in daily:

        if item["open"] == 0:
            change = 0
        else:
            change = (
                (
                    item["close"]
                    - item["open"]
                )
                / item["open"]
                * 100
            )

        result.append({
            "label": str(
                item["date"]
            ),
            "change": change,
            "open": item["open"],
            "close": item["close"]
        })

    return result


def get_btc_daily_change(
    df
):

    daily = build_btc_daily(
        df
    )

    if not daily:
        return None

    return daily[-1]["change"]


# =========================================================
# BTC 4H
# =========================================================

def build_btc_4h(
    df
):

    if df is None or df.empty:
        return []

    work = df.copy()

    work["period_start"] = (
        work["dt"]
        .dt.floor("4h")
    )

    result = []

    for start, group in work.groupby(
        "period_start"
    ):

        if group.empty:
            continue

        first = group.iloc[0]
        last = group.iloc[-1]

        open_price = float(
            first["open"]
        )

        close_price = float(
            last["close"]
        )

        if open_price == 0:
            change = 0
        else:
            change = (
                (
                    close_price
                    - open_price
                )
                / open_price
                * 100
            )

        end = start + timedelta(
            hours=4
        )

        result.append({
            "start": start,
            "end": end,
            "label": (
                f"{start:%m-%d %H:%M}"
                f"~"
                f"{end:%m-%d %H:%M}"
            ),
            "change": change
        })

    return sorted(
        result,
        key=lambda x: x["start"]
    )


# =========================================================
# BTC 업데이트
# =========================================================

def update_btc_market():

    global latest_btc_okx_price
    global latest_btc_daily_change
    global latest_btc_daily_periods
    global latest_btc_4h_periods
    global latest_btc_current_daily_change
    global latest_btc_current_daily_label
    global latest_btc_current_4h_change
    global latest_btc_current_4h_label
    global latest_btc_news

    try:

        price = get_okx_btc_price()

        if price is not None:
            latest_btc_okx_price = price

        df = get_okx_btc_1h_history()

        if df.empty:
            return

        daily = build_btc_daily(
            df
        )

        four_hour = build_btc_4h(
            df
        )

        latest_btc_daily_periods = (
            daily[-3:]
        )

        latest_btc_4h_periods = (
            four_hour[-6:]
        )

        if daily:

            latest_btc_daily_change = (
                daily[-1]["change"]
            )

            latest_btc_current_daily_change = (
                daily[-1]["change"]
            )

            latest_btc_current_daily_label = (
                daily[-1]["label"]
            )

        if four_hour:

            latest_btc_current_4h_change = (
                four_hour[-1]["change"]
            )

            latest_btc_current_4h_label = (
                four_hour[-1]["label"]
            )

        # BTC 뉴스
        latest_btc_news = get_news_for_coin(
            "BTC",
            3
        )

    except Exception as e:

        logging.exception(
            "BTC 시황 업데이트 오류: %s",
            e
        )


# =========================================================
# OKX 업데이트
# =========================================================

def update_okx():

    global latest_okx_data
    global latest_okx_update_time

    if USE_OKX != "Y":
        return

    latest_okx_data = []

    latest_okx_update_time = (
        datetime.now(KST)
        .strftime("%Y-%m-%d %H:%M:%S")
    )


# =========================================================
# USDT/KRW
# =========================================================

def get_usdt_krw():

    try:

        response = retry(
            requests.get,
            "https://api.upbit.com/v1/ticker",
            params={
                "markets": "KRW-USDT"
            },
            timeout=10
        )

        if response is None:
            return None

        data = response.json()

        if not data:
            return None

        return float(
            data[0]["trade_price"]
        )

    except Exception:
        return None


# =========================================================
# 전체 대시보드 업데이트
# =========================================================

def update_dashboard():

    with update_lock:

        try:

            update_btc_market()

            if USE_UPBIT == "Y":
                update_upbit()

            if USE_OKX == "Y":
                update_okx()

        except Exception as e:

            logging.exception(
                "대시보드 업데이트 오류: %s",
                e
            )


# =========================================================
# HTML - 캔들 패턴
# =========================================================

def candle_pattern_html(
    pattern
):

    if not pattern:
        return ""

    return (
        '<span class="pattern-badge">'
        f'{html.escape(str(pattern))}'
        '</span>'
    )


# =========================================================
# HTML - 기간 셀
# =========================================================

def period_cells_html(
    periods
):

    if not periods:
        return ""

    cells = []

    for item in periods:

        change = get_change_value(
            item.get(
                "change"
            )
        )

        if change > 0:
            cls = "positive"
        elif change < 0:
            cls = "negative"
        else:
            cls = "neutral"

        pattern = candle_pattern_html(
            item.get(
                "pattern",
                ""
            )
        )

        cells.append(
            f"""
            <div class="period-cell {cls}">
                <div class="period-label">
                    {html.escape(str(item.get("label", "-")))}
                </div>
                <div class="period-change">
                    {format_change(change)}
                </div>
                {pattern}
            </div>
            """
        )

    return "".join(cells)


def four_hour_cells_html(
    periods
):

    return period_cells_html(
        periods
    )


def daily_cells_html(
    periods
):

    return period_cells_html(
        periods
    )


# =========================================================
# 뉴스 HTML
# =========================================================

def news_line_html(
    article,
    index
):

    title = html.escape(
        article.get(
            "title",
            ""
        )
    )

    source = html.escape(
        article.get(
            "source",
            ""
        )
    )

    url = html.escape(
        article.get(
            "url",
            ""
        ),
        quote=True
    )

    if len(title) > 100:
        title = title[:100] + "..."

    if url:
        title_html = (
            f'<a href="{url}" '
            f'target="_blank" '
            f'rel="noopener noreferrer">'
            f'{title}'
            f'</a>'
        )
    else:
        title_html = title

    source_html = ""

    if source:
        source_html = (
            f'<span class="news-source">'
            f'{source}'
            f'</span>'
        )

    return f"""
        <div class="news-line">
            <span class="news-number">
                {index}
            </span>
            <span class="news-headline">
                {title_html}
            </span>
            {source_html}
        </div>
    """


def news_block_html(
    title,
    articles,
    css_class=""
):

    if not articles:
        return ""

    lines = []

    for index, article in enumerate(
        articles[:3],
        start=1
    ):
        lines.append(
            news_line_html(
                article,
                index
            )
        )

    if not lines:
        return ""

    return f"""
        <div class="news-block {css_class}">
            <div class="news-block-title">
                {html.escape(title)}
            </div>
            <div class="news-list">
                {''.join(lines)}
            </div>
        </div>
    """


# =========================================================
# BTC 시황
# =========================================================

def market_summary_html():

    price_html = format_market_price(
        latest_btc_okx_price
    )

    daily_change = (
        latest_btc_current_daily_change
    )

    four_change = (
        latest_btc_current_4h_change
    )

    daily_cls = (
        "positive"
        if get_change_value(
            daily_change
        ) > 0
        else
        "negative"
        if get_change_value(
            daily_change
        ) < 0
        else
        "neutral"
    )

    four_cls = (
        "positive"
        if get_change_value(
            four_change
        ) > 0
        else
        "negative"
        if get_change_value(
            four_change
        ) < 0
        else
        "neutral"
    )

    btc_news_html = news_block_html(
        "📰 BTC 시황",
        latest_btc_news,
        "btc-news-block"
    )

    return f"""
    <section class="market-card">

        <div class="market-card-header">
            <div class="market-title">
                ₿ BTC 시황
            </div>
            <div class="market-update">
                {html.escape(
                    latest_okx_update_time
                    if USE_OKX == "Y"
                    else datetime.now(KST).strftime(
                        "%H:%M:%S"
                    )
                )}
            </div>
        </div>

        <div class="btc-main-row">

            <div class="btc-main-price">
                <span class="btc-label">
                    현재가
                </span>

                <span class="btc-price">
                    {price_html}
                </span>
            </div>

            <div class="btc-main-change {daily_cls}">
                <span class="btc-label">
                    일봉
                </span>

                <span>
                    {format_change(daily_change)}
                </span>
            </div>

            <div class="btc-main-change {four_cls}">
                <span class="btc-label">
                    4H
                </span>

                <span>
                    {format_change(four_change)}
                </span>
            </div>

        </div>

        {btc_news_html}

        <div class="timeframe-card-title">
            일봉
        </div>

        <div class="period-grid">
            {daily_cells_html(
                latest_btc_daily_periods
            )}
        </div>

        <div class="timeframe-card-title">
            4H
        </div>

        <div class="period-grid">
            {four_hour_cells_html(
                latest_btc_4h_periods
            )}
        </div>

    </section>
    """


# =========================================================
# 통합 카드
# =========================================================

def unified_card_html(
    row,
    mode="TOP"
):

    name = html.escape(
        str(
            row.get(
                "name",
                "-"
            )
        )
    )

    price = format_market_price(
        row.get(
            "price"
        )
    )

    volume = format_volume(
        row.get(
            "volume"
        )
    )

    change = get_change_value(
        row.get(
            "change"
        )
    )

    if change > 0:
        change_cls = "positive"
    elif change < 0:
        change_cls = "negative"
    else:
        change_cls = "neutral"

    daily = row.get(
        "daily",
        []
    )

    four_hour = row.get(
        "four_hour",
        []
    )

    current_daily = (
        daily[-1]
        if daily
        else None
    )

    current_4h = (
        four_hour[-1]
        if four_hour
        else None
    )

    dual_signal = (
        row.get(
            "signal_pass",
            False
        )
        and
        row.get(
            "daily_signal_pass",
            False
        )
    )

    signal_pass = row.get(
        "signal_pass",
        False
    )

    if mode == "SIGNAL_4H":

        if dual_signal:
            badge = (
                '<span class="signal-badge dual">'
                '🔥 일봉 + 4H SIGNAL'
                '</span>'
            )
        else:
            badge = (
                '<span class="signal-badge">'
                '🔥 4H SIGNAL'
                '</span>'
            )

        news_badge = ""

        header_class = (
            "unified-card-header signal-header"
        )

    else:

        rank = row.get(
            "volume_rank",
            "-"
        )

        badge = (
            f'<span class="rank-badge">'
            f'TOP {rank}'
            f'</span>'
        )

        if row.get(
            "has_news",
            False
        ):
            news_badge = (
                '<span class="news-badge">'
                '📰 뉴스 있음'
                '</span>'
            )
        else:
            news_badge = ""

        header_class = (
            "unified-card-header"
        )

    signal_text = ""

    if mode == "SIGNAL_4H":

        signal_text = """
        <div class="signal-condition-row">
            <span>4H 상승</span>
            <span>상승장악 계열</span>
            <span>일봉 조건 별도</span>
        </div>
        """

    return f"""
    <div class="unified-market-card">

        <div class="{header_class}">

            <div class="unified-rank">
                {badge}
            </div>

            <div class="unified-coin-name">
                {name}
            </div>

            <div class="unified-card-title">
                {mode}
            </div>

            {news_badge}

        </div>

        <div class="unified-main-row">

            <div class="unified-main-item">

                <div class="unified-label">
                    현재가
                </div>

                <div class="unified-price">
                    {price}
                </div>

            </div>

            <div class="unified-main-item">

                <div class="unified-label">
                    24H 거래대금
                </div>

                <div class="unified-volume">
                    {volume}
                </div>

            </div>

            <div class="unified-main-item {change_cls}">

                <div class="unified-label">
                    당일
                </div>

                <div class="unified-daily">
                    {format_change(change)}
                </div>

            </div>

        </div>

        {signal_text}

        <div class="timeframe-card-title">
            일봉
        </div>

        <div class="period-grid">
            {daily_cells_html(
                daily[-3:]
            )}
        </div>

        <div class="timeframe-card-title">
            4H
        </div>

        <div class="period-grid">
            {four_hour_cells_html(
                four_hour[-6:]
            )}
        </div>

    </div>
    """


# =========================================================
# SIGNAL 영역
# =========================================================

def focus_section(
    rows
):

    signal_rows = [
        row
        for row in rows
        if row.get(
            "signal_pass",
            False
        )
    ]

    if not signal_rows:
        return ""

    signal_rows = sorted(
        signal_rows,
        key=lambda x: (
            x.get(
                "volume_rank",
                999999
            )
        )
    )

    body = "".join(
        unified_card_html(
            row,
            "SIGNAL_4H"
        )
        for row in signal_rows
    )

    signal_news_html = news_block_html(
        "🔥 SIGNAL 뉴스",
        latest_signal_news,
        "signal-news-block"
    )

    btc_change = (
        latest_btc_current_4h_change
    )

    if btc_change is None:
        btc_change = 0

    btc_cls = (
        "positive"
        if btc_change > 0
        else
        "negative"
        if btc_change < 0
        else
        "neutral"
    )

    return f"""
    <section class="focus-section">

        <div class="section-header signal-section-header">
            <div>
                🔥 4H SIGNAL
            </div>

            <div class="section-count">
                {len(signal_rows)}개
            </div>
        </div>

        <div class="signal-btc-bar">

            <span>
                BTC
            </span>

            <span class="{btc_cls}">
                {format_change(btc_change)}
            </span>

            <span>
                SIGNAL {len(signal_rows)}
            </span>

        </div>

        {signal_news_html}

        <div class="signal-card-list">
            {body}
        </div>

    </section>
    """


# =========================================================
# TOP 영역
# =========================================================

def section(
    rows,
    update_time
):

    if not rows:
        return ""

    body = "".join(
        unified_card_html(
            row,
            "TOP"
        )
        for row in rows
    )

    return f"""
    <section class="top-section">

        <div class="section-header">
            <div>
                📊 Upbit TOP {TOP_N}
            </div>

            <div class="section-update">
                {html.escape(
                    str(update_time)
                )}
            </div>
        </div>

        <div class="top-card-list">
            {body}
        </div>

    </section>
    """


# =========================================================
# CSS
# =========================================================

CSS = """
*{
    box-sizing:border-box;
}

html,
body{
    margin:0;
    padding:0;
    background:#080b0f;
    color:#e8edf2;
    font-family:
        -apple-system,
        BlinkMacSystemFont,
        "Segoe UI",
        Roboto,
        Arial,
        sans-serif;
}

body{
    min-width:320px;
}

.container{
    width:100%;
    max-width:1200px;
    margin:0 auto;
    padding:8px;
}

.market-card,
.unified-market-card{
    background:#10151b;
    border:1px solid #202a33;
    border-radius:9px;
    overflow:hidden;
    margin-bottom:7px;
    box-shadow:
        0 2px 10px rgba(0,0,0,.22);
}

.market-card-header,
.section-header{
    display:flex;
    justify-content:space-between;
    align-items:center;
    min-height:31px;
    padding:6px 9px;
    background:#131a21;
    border-bottom:1px solid #202a33;
}

.market-title{
    font-size:12px;
    font-weight:800;
}

.market-update,
.section-update{
    font-size:8px;
    color:#77828d;
}

.btc-main-row{
    display:grid;
    grid-template-columns:
        1.4fr
        1fr
        1fr;
    min-height:48px;
    background:#11161c;
}

.btc-main-price,
.btc-main-change{
    display:flex;
    align-items:center;
    justify-content:center;
    gap:5px;
    border-right:1px solid #202a33;
}

.btc-main-change:last-child{
    border-right:0;
}

.btc-label{
    font-size:7px;
    color:#7f8a95;
}

.btc-price{
    font-size:14px;
    font-weight:800;
}

.btc-main-change{
    font-size:11px;
    font-weight:800;
}

.timeframe-card-title{
    padding:4px 8px;
    font-size:8px;
    font-weight:800;
    color:#87939f;
    background:#0e1318;
    border-top:1px solid #1d2730;
    border-bottom:1px solid #1d2730;
}

.period-grid{
    display:grid;
    grid-template-columns:
        repeat(6, minmax(0, 1fr));
    gap:1px;
    background:#202a33;
}

.period-grid .period-cell{
    min-height:38px;
}

.period-cell{
    display:flex;
    flex-direction:column;
    justify-content:center;
    align-items:center;
    padding:3px 2px;
    background:#10151b;
    text-align:center;
    overflow:hidden;
}

.period-label{
    font-size:6px;
    color:#66727e;
    white-space:nowrap;
}

.period-change{
    margin-top:2px;
    font-size:9px;
    font-weight:800;
}

.pattern-badge{
    display:block;
    max-width:100%;
    margin-top:2px;
    padding:1px 3px;
    border-radius:3px;
    background:#222d36;
    color:#aeb8c1;
    font-size:5px;
    line-height:1.2;
    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;
}

.positive{
    color:#28d17c !important;
}

.negative{
    color:#ff5c6c !important;
}

.neutral{
    color:#89939d !important;
}


/* =====================================================
   SIGNAL
   ===================================================== */

.focus-section,
.top-section{
    margin-top:7px;
}

.signal-section-header{
    border-radius:8px 8px 0 0;
}

.signal-section-header > div:first-child{
    color:#ffcc4d;
    font-size:12px;
    font-weight:900;
}

.section-count{
    font-size:9px;
    color:#89939d;
}

.signal-btc-bar{
    display:flex;
    align-items:center;
    gap:12px;
    min-height:25px;
    padding:4px 8px;
    background:#0d1318;
    border-bottom:1px solid #202a33;
    font-size:8px;
    font-weight:800;
}

.signal-card-list,
.top-card-list{
    display:flex;
    flex-direction:column;
    gap:6px;
}

.unified-card-header{
    display:flex;
    align-items:center;
    min-height:30px;
    gap:5px;
    padding:4px 7px;
    background:#131a21;
    border-bottom:1px solid #202a33;
}

.unified-rank{
    flex:0 0 auto;
}

.unified-coin-name{
    flex:0 0 auto;
    font-size:11px;
    font-weight:900;
    color:#f0f4f7;
}

.unified-card-title{
    flex:1;
    text-align:right;
    font-size:7px;
    color:#697580;
}

.rank-badge{
    display:inline-flex;
    align-items:center;
    justify-content:center;
    padding:2px 4px;
    border-radius:4px;
    background:#202b35;
    color:#aeb9c3;
    font-size:6px;
    font-weight:900;
}

.signal-badge{
    display:inline-flex;
    align-items:center;
    justify-content:center;
    padding:2px 5px;
    border-radius:4px;
    background:#332b17;
    color:#ffcc4d;
    font-size:6px;
    font-weight:900;
}

.signal-badge.dual{
    background:#38261b;
    color:#ff9d4d;
}

.news-badge{
    display:inline-flex;
    align-items:center;
    justify-content:center;
    padding:2px 4px;
    border-radius:4px;
    background:#182b35;
    color:#79c9ef;
    font-size:6px;
    font-weight:800;
    white-space:nowrap;
}


/* =====================================================
   현재가 / 거래대금 / 당일
   최소 높이
   ===================================================== */

.unified-main-row{
    display:grid;
    grid-template-columns:
        1.2fr
        1fr
        1fr;
    min-height:38px;
    background:#11161c;
}

.unified-main-item{
    display:flex;
    align-items:center;
    justify-content:center;
    gap:3px;
    padding:2px 3px;
    border-right:1px solid #202a33;
    min-width:0;
}

.unified-main-item:last-child{
    border-right:0;
}

.unified-label{
    font-size:6px;
    color:#707c87;
    white-space:nowrap;
}

.unified-price,
.unified-volume,
.unified-daily{
    font-size:8px;
    font-weight:900;
    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;
}

.signal-condition-row{
    display:flex;
    justify-content:center;
    align-items:center;
    gap:8px;
    min-height:20px;
    padding:2px 5px;
    background:#0e1419;
    border-bottom:1px solid #202a33;
    font-size:6px;
    color:#7f8a94;
}


/* =====================================================
   NEWS
   ===================================================== */

.news-block{
    margin:5px 7px;
    padding:4px 6px;
    background:#0c1217;
    border:1px solid #202b34;
    border-radius:6px;
}

.news-block-title{
    margin-bottom:3px;
    font-size:7px;
    font-weight:900;
    color:#8fd6ff;
}

.signal-news-block .news-block-title{
    color:#ffcc4d;
}

.news-line{
    display:flex;
    align-items:center;
    gap:4px;
    min-height:18px;
    padding:2px 0;
    border-top:1px solid rgba(255,255,255,.035);
}

.news-line:first-child{
    border-top:0;
}

.news-number{
    flex:0 0 12px;
    width:12px;
    height:12px;
    display:flex;
    align-items:center;
    justify-content:center;
    border-radius:50%;
    background:#1b2730;
    color:#8b98a4;
    font-size:6px;
    font-weight:900;
}

.news-headline{
    flex:1;
    min-width:0;
    overflow:hidden;
    white-space:nowrap;
    text-overflow:ellipsis;
}

.news-headline a{
    color:#cbd5dd;
    text-decoration:none;
    font-size:7px;
}

.news-headline a:hover{
    text-decoration:underline;
}

.news-source{
    flex:0 0 auto;
    max-width:60px;
    color:#596671;
    font-size:5px;
    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;
}


/* =====================================================
   반응형
   ===================================================== */

@media(max-width:700px){

    .container{
        padding:5px;
    }

    .period-grid{
        grid-template-columns:
            repeat(3, minmax(0, 1fr));
    }

    .unified-main-row{
        min-height:35px;
    }

    .unified-label{
        font-size:5px;
    }

    .unified-price,
    .unified-volume,
    .unified-daily{
        font-size:7px;
    }

    .unified-main-item{
        gap:2px;
    }

    .news-source{
        display:none;
    }

    .news-headline a{
        font-size:7px;
    }

    .news-line{
        min-height:17px;
    }
}


@media(max-width:430px){

    .container{
        padding:4px;
    }

    .market-card,
    .unified-market-card{
        border-radius:7px;
        margin-bottom:5px;
    }

    .unified-main-row{
        min-height:32px;
    }

    .unified-label{
        font-size:5px;
    }

    .unified-price,
    .unified-volume,
    .unified-daily{
        font-size:7px;
    }

    .unified-main-item{
        padding:1px 2px;
        gap:2px;
    }

    .unified-card-header{
        min-height:27px;
        padding:3px 5px;
    }

    .unified-coin-name{
        font-size:10px;
    }

    .rank-badge,
    .signal-badge,
    .news-badge{
        font-size:5px;
        padding:2px 3px;
    }

    .btc-main-row{
        min-height:42px;
    }

    .btc-price{
        font-size:12px;
    }

    .btc-main-change{
        font-size:9px;
    }

    .period-grid .period-cell{
        min-height:35px;
    }

    .period-label{
        font-size:5px;
    }

    .period-change{
        font-size:8px;
    }

    .pattern-badge{
        font-size:4px;
    }

    .news-block{
        margin:4px 5px;
        padding:3px 5px;
    }

    .news-block-title{
        font-size:6px;
    }

    .news-line{
        min-height:16px;
    }

    .news-headline a{
        font-size:6px;
    }

    .news-number{
        flex-basis:11px;
        width:11px;
        height:11px;
        font-size:5px;
    }
}
"""


# =========================================================
# Dashboard
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def dashboard():

    content = ""

    if USE_UPBIT == "Y":

        content += focus_section(
            latest_upbit_data
        )

        content += section(
            latest_upbit_data,
            latest_upbit_update_time
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
            content="{UPDATE_MINUTES * 60}"
        >

        <title>
            Crypto Dashboard
        </title>

        <style>
            {CSS}
        </style>

    </head>

    <body>

        <div class="container">

            {market_summary_html()}

            {content}

        </div>

    </body>

    </html>
    """


# =========================================================
# 스케줄러
# =========================================================

def scheduler_loop():

    schedule.every(
        UPDATE_MINUTES
    ).minutes.do(
        update_dashboard
    )

    while True:

        try:
            schedule.run_pending()

        except Exception as e:

            logging.exception(
                "스케줄러 오류: %s",
                e
            )

        time.sleep(1)


# =========================================================
# 시작
# =========================================================

@app.on_event("startup")
def startup_event():

    logging.info(
        "========================================"
    )

    logging.info(
        "Crypto Dashboard 시작"
    )

    logging.info(
        "USE_UPBIT=%s | USE_OKX=%s",
        USE_UPBIT,
        USE_OKX
    )

    logging.info(
        "TOP_N=%s | UPDATE_MINUTES=%s",
        TOP_N,
        UPDATE_MINUTES
    )

    if COINGECKO_API_KEY:
        logging.info(
            "CoinGecko News API: ENABLED"
        )
    else:
        logging.info(
            "CoinGecko News API: DISABLED "
            "(COINGECKO_API_KEY 없음)"
        )

    logging.info(
        "========================================"
    )

    update_dashboard()

    thread = threading.Thread(
        target=scheduler_loop,
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
        port=8000
    )
