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
    format="%(asctime)s | %(levelname)s | %(message)s"
)

KST = ZoneInfo("Asia/Seoul")

UPBIT_BASE_URL = "https://api.upbit.com/v1"

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
# 전역 상태
# =========================================================

latest_data = {
    "updated_at": None,
    "btc": {},
    "top_markets": [],
    "signal_rows": [],
}

data_lock = threading.Lock()


# =========================================================
# HTTP 요청
# =========================================================

def upbit_get(url, params=None):

    for attempt in range(MAX_RETRIES):

        try:
            response = requests.get(
                url,
                params=params,
                timeout=10
            )

            if response.status_code == 200:
                time.sleep(REQUEST_INTERVAL)
                return response.json()

            if response.status_code == 429:
                logging.warning(
                    "Upbit 429 rate limit - retry %s/%s",
                    attempt + 1,
                    MAX_RETRIES
                )

                time.sleep(RATE_LIMIT_WAIT)
                continue

            logging.warning(
                "Upbit HTTP %s : %s",
                response.status_code,
                response.text[:200]
            )

        except Exception as e:

            logging.warning(
                "Upbit request error: %s",
                e
            )

        time.sleep(1)

    return None


# =========================================================
# 숫자 포맷
# =========================================================

def safe_float(value):

    try:
        if value is None:
            return None

        return float(value)

    except Exception:
        return None


def format_price(value):

    value = safe_float(value)

    if value is None:
        return "-"

    if value >= 1000000:
        return f"{value:,.0f}"

    if value >= 1000:
        return f"{value:,.1f}"

    if value >= 1:
        return f"{value:,.2f}"

    return f"{value:,.6f}"


def format_percent(value):

    value = safe_float(value)

    if value is None:
        return "-"

    sign = "+" if value > 0 else ""

    return f"{sign}{value:.2f}%"


def format_volume_krw(value):

    value = safe_float(value)

    if value is None:
        return "-"

    if value >= 1_0000_0000_0000:
        return f"{value / 1_0000_0000_0000:.2f}조"

    if value >= 1_0000_0000:
        return f"{value / 1_0000_0000:.1f}억"

    if value >= 1_0000:
        return f"{value / 1_0000:.1f}만"

    return f"{value:,.0f}"


# =========================================================
# Upbit 마켓
# =========================================================

def get_upbit_markets():

    data = upbit_get(
        f"{UPBIT_BASE_URL}/market/all",
        {
            "isDetails": "false"
        }
    )

    if not data:
        return []

    return [
        item
        for item in data
        if item.get("market", "").startswith("KRW-")
    ]


# =========================================================
# 현재 시세
# =========================================================

def get_upbit_tickers(markets):

    if not markets:
        return []

    result = []

    # Upbit ticker API는 여러 마켓을 한번에 조회
    chunk_size = 100

    for i in range(0, len(markets), chunk_size):

        chunk = markets[i:i + chunk_size]

        data = upbit_get(
            f"{UPBIT_BASE_URL}/ticker",
            {
                "markets": ",".join(chunk)
            }
        )

        if data:
            result.extend(data)

    return result


# =========================================================
# 24시간 거래대금 TOP
# =========================================================

def get_top_volume_markets():

    markets_info = get_upbit_markets()

    if not markets_info:
        return []

    market_codes = [
        item["market"]
        for item in markets_info
    ]

    tickers = get_upbit_tickers(
        market_codes
    )

    markets = []

    for ticker in tickers:

        market = ticker.get("market")

        if not market:
            continue

        volume_24h = safe_float(
            ticker.get("acc_trade_price_24h")
        )

        trade_price = safe_float(
            ticker.get("trade_price")
        )

        change_rate = safe_float(
            ticker.get("signed_change_rate")
        )

        if volume_24h is None:
            continue

        markets.append({
            "market": market,
            "price": trade_price,
            "volume_24h": volume_24h,
            "daily_change": (
                change_rate * 100
                if change_rate is not None
                else None
            )
        })

    # 실제 업비트 24시간 거래대금 순위
    markets.sort(
        key=lambda x: x["volume_24h"],
        reverse=True
    )

    volume_rank_map = {
        item["market"]: rank
        for rank, item in enumerate(markets, 1)
    }

    top_markets = markets[:TOP_N]

    for item in top_markets:

        item["volume_rank"] = volume_rank_map.get(
            item["market"]
        )

    return top_markets


# =========================================================
# 일봉 변동률
# =========================================================

def get_daily_change(market):

    candles = upbit_get(
        f"{UPBIT_BASE_URL}/candles/days",
        {
            "market": market,
            "count": 3
        }
    )

    if not candles or len(candles) < 2:
        return None

    # 가장 최근 완성/진행 일봉 기준
    latest = candles[0]

    change = safe_float(
        latest.get("signed_change_rate")
    )

    if change is None:
        return None

    return change * 100


# =========================================================
# 4H 구간 생성
#
# KST 기준
# 01 / 05 / 09 / 13 / 17 / 21
# =========================================================

def get_current_4h_period():

    now = datetime.now(KST)

    base_hour = (now.hour // 4) * 4

    start = now.replace(
        hour=base_hour,
        minute=0,
        second=0,
        microsecond=0
    )

    end = start + timedelta(hours=4)

    return {
        "start": start,
        "end": end,
        "active": True
    }


def make_4h_period(start, active=False):

    return {
        "start": start,
        "end": start + timedelta(hours=4),
        "active": active
    }


# =========================================================
# 기존 4H
# =========================================================

def get_previous_4h_period():

    current = get_current_4h_period()

    if current is None:
        return None

    previous_start = (
        current["start"]
        - timedelta(hours=4)
    )

    return make_4h_period(
        previous_start,
        active=False
    )


# =========================================================
# 전전 4H
# =========================================================

def get_pre_previous_4h_period():

    current = get_current_4h_period()

    if current is None:
        return None

    pre_previous_start = (
        current["start"]
        - timedelta(hours=8)
    )

    return make_4h_period(
        pre_previous_start,
        active=False
    )


# =========================================================
# ★ 추가 ★
# 전전전 4H
# =========================================================

def get_pre_pre_previous_4h_period():

    current = get_current_4h_period()

    if current is None:
        return None

    pre_pre_previous_start = (
        current["start"]
        - timedelta(hours=12)
    )

    return make_4h_period(
        pre_pre_previous_start,
        active=False
    )


# =========================================================
# 1시간봉 가져오기
# =========================================================

def get_1h_candles(market, count=200):

    candles = upbit_get(
        f"{UPBIT_BASE_URL}/candles/minutes/60",
        {
            "market": market,
            "count": min(count, 200)
        }
    )

    if not candles:
        return pd.DataFrame()

    rows = []

    for candle in candles:

        rows.append({
            "time": pd.to_datetime(
                candle["candle_date_time_kst"]
            ),
            "open": safe_float(
                candle["opening_price"]
            ),
            "high": safe_float(
                candle["high_price"]
            ),
            "low": safe_float(
                candle["low_price"]
            ),
            "close": safe_float(
                candle["trade_price"]
            ),
            "volume": safe_float(
                candle["candle_acc_trade_volume"]
            ),
            "value": safe_float(
                candle["candle_acc_trade_price"]
            )
        })

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df = df.sort_values("time")

    df = df.reset_index(drop=True)

    return df


# =========================================================
# 4H 데이터 계산
# =========================================================

def analyze_4h(market):

    df = get_1h_candles(
        market,
        count=200
    )

    if df.empty:
        return {
            "current_4h_change": None,
            "previous_4h_change": None,
            "pre_previous_4h_change": None,
            "pre_pre_previous_4h_change": None
        }

    current_period = get_current_4h_period()
    previous_period = get_previous_4h_period()
    pre_previous_period = get_pre_previous_4h_period()
    pre_pre_previous_period = get_pre_pre_previous_4h_period()

    if not current_period:
        return {
            "current_4h_change": None,
            "previous_4h_change": None,
            "pre_previous_4h_change": None,
            "pre_pre_previous_4h_change": None
        }

    # -----------------------------------------------------
    # 현재 4H
    # -----------------------------------------------------

    current_rows = df[
        (df["time"] >= current_period["start"]) &
        (df["time"] < current_period["end"])
    ]

    # -----------------------------------------------------
    # 전 4H
    # -----------------------------------------------------

    previous_rows = df[
        (df["time"] >= previous_period["start"]) &
        (df["time"] < previous_period["end"])
    ]

    # -----------------------------------------------------
    # 전전 4H
    # -----------------------------------------------------

    pre_previous_rows = df[
        (df["time"] >= pre_previous_period["start"]) &
        (df["time"] < pre_previous_period["end"])
    ]

    # -----------------------------------------------------
    # ★ 전전전 4H
    # -----------------------------------------------------

    pre_pre_previous_rows = df[
        (df["time"] >= pre_pre_previous_period["start"]) &
        (df["time"] < pre_pre_previous_period["end"])
    ]

    def calculate_change(rows):

        if rows.empty:
            return None

        first_open = rows.iloc[0]["open"]

        last_close = rows.iloc[-1]["close"]

        if first_open is None:
            return None

        if last_close is None:
            return None

        if first_open == 0:
            return None

        return (
            (last_close - first_open)
            / first_open
            * 100
        )

    current_change = calculate_change(
        current_rows
    )

    previous_change = calculate_change(
        previous_rows
    )

    pre_previous_change = calculate_change(
        pre_previous_rows
    )

    pre_pre_previous_change = calculate_change(
        pre_pre_previous_rows
    )

    return {
        "current_4h_change": current_change,
        "previous_4h_change": previous_change,
        "pre_previous_4h_change": pre_previous_change,

        # ★ 추가
        "pre_pre_previous_4h_change":
            pre_pre_previous_change
    }


# =========================================================
# 코인 분석
# =========================================================

def analyze_market(item):

    market = item["market"]

    daily_change = item.get(
        "daily_change"
    )

    if daily_change is None:
        daily_change = get_daily_change(
            market
        )

    four_hour = analyze_4h(
        market
    )

    row = {
        "market": market,

        "price": item.get("price"),

        "volume_24h":
            item.get("volume_24h"),

        "volume_rank":
            item.get("volume_rank"),

        "daily_change":
            daily_change,

        "current_4h_change":
            four_hour.get(
                "current_4h_change"
            ),

        "previous_4h_change":
            four_hour.get(
                "previous_4h_change"
            ),

        "pre_previous_4h_change":
            four_hour.get(
                "pre_previous_4h_change"
            ),

        # ★ 추가
        "pre_pre_previous_4h_change":
            four_hour.get(
                "pre_pre_previous_4h_change"
            )
    }

    # =====================================================
    # SIGNAL 조건
    # =====================================================

    daily_condition = (
        row["daily_change"] is not None
        and row["daily_change"] > 0
    )

    # -----------------------------------------------------
    # 패턴 1
    #
    # 전전 양수
    # 전 음수
    # 현재 양수
    # -----------------------------------------------------

    pattern_1 = (
        row["pre_previous_4h_change"] is not None
        and
        row["pre_previous_4h_change"] > 0

        and

        row["previous_4h_change"] is not None
        and
        row["previous_4h_change"] < 0

        and

        row["current_4h_change"] is not None
        and
        row["current_4h_change"] > 0
    )

    # -----------------------------------------------------
    # ★ 패턴 2
    #
    # 전전전 양수
    # 전전 음수
    # 전 음수
    # 현재 양수
    # -----------------------------------------------------

    pattern_2 = (
        row["pre_pre_previous_4h_change"] is not None
        and
        row["pre_pre_previous_4h_change"] > 0

        and

        row["pre_previous_4h_change"] is not None
        and
        row["pre_previous_4h_change"] < 0

        and

        row["previous_4h_change"] is not None
        and
        row["previous_4h_change"] < 0

        and

        row["current_4h_change"] is not None
        and
        row["current_4h_change"] > 0
    )

    row["pattern_1"] = pattern_1
    row["pattern_2"] = pattern_2

    row["signal_pass"] = (
        daily_condition
        and
        (
            pattern_1
            or
            pattern_2
        )
    )

    return row


# =========================================================
# BTC 시황
# =========================================================

def get_btc_market():

    markets = get_upbit_tickers(
        ["KRW-BTC"]
    )

    if not markets:
        return {}

    ticker = markets[0]

    price = safe_float(
        ticker.get("trade_price")
    )

    change = safe_float(
        ticker.get("signed_change_rate")
    )

    if change is not None:
        change *= 100

    volume = safe_float(
        ticker.get("acc_trade_price_24h")
    )

    if change is None:
        state = "⚪"

    elif change > 0:
        state = "☀️"

    elif change < 0:
        state = "🌧️"

    else:
        state = "⚪"

    return {
        "market": "KRW-BTC",
        "price": price,
        "daily_change": change,
        "volume_24h": volume,
        "state": state
    }


# =========================================================
# 전체 데이터 업데이트
# =========================================================

def update_data():

    logging.info(
        "========== DATA UPDATE START =========="
    )

    try:

        top_markets = get_top_volume_markets()

        analyzed_rows = []

        for item in top_markets:

            try:

                row = analyze_market(
                    item
                )

                analyzed_rows.append(
                    row
                )

            except Exception as e:

                logging.exception(
                    "%s analyze error: %s",
                    item.get("market"),
                    e
                )

        # =================================================
        # SIGNAL
        # ★ 당일 변동률 높은 순
        # =================================================

        signal_rows = [
            row
            for row in analyzed_rows
            if row.get("signal_pass")
            and row.get("market") != "KRW-BTC"
        ]

        signal_rows.sort(
            key=lambda row: row.get(
                "daily_change",
                float("-inf")
            ),
            reverse=True
        )

        btc = get_btc_market()

        with data_lock:

            latest_data["updated_at"] = (
                datetime.now(KST)
            )

            latest_data["btc"] = btc

            latest_data["top_markets"] = (
                analyzed_rows
            )

            latest_data["signal_rows"] = (
                signal_rows
            )

        logging.info(
            "TOP=%s / SIGNAL=%s",
            len(analyzed_rows),
            len(signal_rows)
        )

    except Exception as e:

        logging.exception(
            "update_data error: %s",
            e
        )

    logging.info(
        "========== DATA UPDATE END =========="
    )


# =========================================================
# HTML 공통
# =========================================================

def percent_class(value):

    value = safe_float(value)

    if value is None:
        return ""

    if value > 0:
        return "positive"

    if value < 0:
        return "negative"

    return "neutral"


def percent_html(value):

    value = safe_float(value)

    if value is None:
        return "-"

    cls = percent_class(
        value
    )

    sign = "+" if value > 0 else ""

    return (
        f'<span class="{cls}">'
        f'{sign}{value:.2f}%'
        f'</span>'
    )


def coin_name(market):

    if not market:
        return "-"

    return market.replace(
        "KRW-",
        ""
    )


# =========================================================
# SIGNAL 카드
# =========================================================

def make_signal_card(row):

    market = html.escape(
        coin_name(
            row.get("market")
        )
    )

    volume_rank = row.get(
        "volume_rank"
    )

    daily_change = row.get(
        "daily_change"
    )

    pre_pre_previous_change = row.get(
        "pre_pre_previous_4h_change"
    )

    pre_previous_change = row.get(
        "pre_previous_4h_change"
    )

    previous_change = row.get(
        "previous_4h_change"
    )

    current_change = row.get(
        "current_4h_change"
    )

    pattern_1 = row.get(
        "pattern_1"
    )

    pattern_2 = row.get(
        "pattern_2"
    )

    if pattern_1:

        pattern_text = (
            "전전 + → 전 - → 현재 +"
        )

    elif pattern_2:

        pattern_text = (
            "전전전 + → 전전 - → 전 - → 현재 +"
        )

    else:

        pattern_text = "-"

    return f"""
    <div class="signal-card">

        <div class="signal-top">

            <div class="coin-title">
                <span class="signal-badge">
                    SIGNAL
                </span>

                <span class="coin-name">
                    {market}
                </span>
            </div>

            <div class="volume-rank">
                거래대금 #{volume_rank}
            </div>

        </div>


        <div class="daily-box">

            <div class="label">
                당일 변동률
            </div>

            <div class="daily-value">
                {percent_html(daily_change)}
            </div>

        </div>


        <div class="four-grid">

            <div class="four-item">
                <div class="four-label">
                    전전전 4H
                </div>

                <div class="four-value">
                    {percent_html(pre_pre_previous_change)}
                </div>
            </div>


            <div class="four-item">
                <div class="four-label">
                    전전 4H
                </div>

                <div class="four-value">
                    {percent_html(pre_previous_change)}
                </div>
            </div>


            <div class="four-item">
                <div class="four-label">
                    전 4H
                </div>

                <div class="four-value">
                    {percent_html(previous_change)}
                </div>
            </div>


            <div class="four-item current">
                <div class="four-label">
                    현재 4H
                </div>

                <div class="four-value">
                    {percent_html(current_change)}
                </div>
            </div>

        </div>


        <div class="pattern-box">

            <span class="pattern-label">
                패턴
            </span>

            <span class="pattern-value">
                {pattern_text}
            </span>

        </div>

    </div>
    """


# =========================================================
# TOP 거래대금 리스트
# =========================================================

def make_top_row(row):

    rank = row.get(
        "volume_rank"
    )

    market = html.escape(
        coin_name(
            row.get("market")
        )
    )

    price = row.get(
        "price"
    )

    volume = row.get(
        "volume_24h"
    )

    daily_change = row.get(
        "daily_change"
    )

    return f"""
    <div class="top-row">

        <div class="rank">
            {rank}
        </div>

        <div class="top-coin">
            {market}
        </div>

        <div class="top-price">
            {format_price(price)}
        </div>

        <div class="top-volume">
            {format_volume_krw(volume)}
        </div>

        <div class="top-change">
            {percent_html(daily_change)}
        </div>

    </div>
    """


# =========================================================
# 메인 HTML
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def home():

    with data_lock:

        btc = dict(
            latest_data["btc"]
        )

        top_markets = list(
            latest_data["top_markets"]
        )

        signal_rows = list(
            latest_data["signal_rows"]
        )

        updated_at = (
            latest_data["updated_at"]
        )

    if updated_at:

        updated_text = updated_at.strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    else:

        updated_text = "-"

    btc_price = format_price(
        btc.get("price")
    )

    btc_change = percent_html(
        btc.get("daily_change")
    )

    btc_state = btc.get(
        "state",
        "⚪"
    )

    signal_html = ""

    if signal_rows:

        for row in signal_rows:

            signal_html += (
                make_signal_card(
                    row
                )
            )

    else:

        signal_html = """
        <div class="empty">
            현재 조건을 만족하는 SIGNAL이 없습니다.
        </div>
        """

    top_html = ""

    for row in top_markets:

        top_html += make_top_row(
            row
        )

    if not top_html:

        top_html = """
        <div class="empty">
            거래대금 데이터를 불러오는 중입니다.
        </div>
        """

    return f"""
<!DOCTYPE html>

<html lang="ko">

<head>

<meta charset="UTF-8">

<meta
    name="viewport"
    content="width=device-width,
             initial-scale=1.0"
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

body {{

    margin: 0;

    background:
        #0b0f14;

    color:
        #e8edf3;

    font-family:
        Arial,
        sans-serif;

    padding:
        16px;

}}


.container {{

    max-width:
        1200px;

    margin:
        0 auto;

}}


.header {{

    display:
        flex;

    justify-content:
        space-between;

    align-items:
        center;

    margin-bottom:
        14px;

}}


.title {{

    font-size:
        22px;

    font-weight:
        800;

}}


.updated {{

    font-size:
        12px;

    color:
        #7f8b99;

}}


/* =====================================================
   BTC
   ===================================================== */

.btc-card {{

    background:
        #111821;

    border:
        1px solid #202b36;

    border-radius:
        14px;

    padding:
        16px;

    margin-bottom:
        18px;

}}


.btc-header {{

    display:
        flex;

    justify-content:
        space-between;

    align-items:
        center;

}}


.btc-title {{

    font-size:
        18px;

    font-weight:
        800;

}}


.btc-market {{

    color:
        #8e9aa8;

    font-size:
        12px;

}}


.btc-data {{

    display:
        flex;

    gap:
        24px;

    margin-top:
        12px;

    align-items:
        center;

}}


.btc-price {{

    font-size:
        25px;

    font-weight:
        800;

}}


.btc-change {{

    font-size:
        18px;

    font-weight:
        700;

}}


/* =====================================================
   Section
   ===================================================== */

.section-title {{

    display:
        flex;

    justify-content:
        space-between;

    align-items:
        center;

    margin:
        18px 0 10px 0;

}}


.section-title h2 {{

    margin: 0;

    font-size:
        18px;

}}


.section-sub {{

    color:
        #74808d;

    font-size:
        12px;

}}


/* =====================================================
   SIGNAL
   ===================================================== */

.signal-grid {{

    display:
        grid;

    grid-template-columns:
        repeat(auto-fit, minmax(300px, 1fr));

    gap:
        10px;

}}


.signal-card {{

    background:
        #111821;

    border:
        1px solid #26323e;

    border-radius:
        14px;

    padding:
        14px;

}}


.signal-top {{

    display:
        flex;

    justify-content:
        space-between;

    align-items:
        center;

    margin-bottom:
        13px;

}}


.coin-title {{

    display:
        flex;

    align-items:
        center;

    gap:
        8px;

}}


.signal-badge {{

    background:
        #1b7f55;

    color:
        #dfffee;

    padding:
        4px 7px;

    border-radius:
        6px;

    font-size:
        10px;

    font-weight:
        800;

}}


.coin-name {{

    font-size:
        18px;

    font-weight:
        800;

}}


.volume-rank {{

    font-size:
        11px;

    color:
        #9ba7b4;

}}


.daily-box {{

    background:
        #0c1219;

    border-radius:
        10px;

    padding:
        10px;

    display:
        flex;

    justify-content:
        space-between;

    align-items:
        center;

    margin-bottom:
        10px;

}}


.label {{

    color:
        #8995a3;

    font-size:
        12px;

}}


.daily-value {{

    font-size:
        20px;

    font-weight:
        800;

}}


.four-grid {{

    display:
        grid;

    grid-template-columns:
        repeat(4, 1fr);

    gap:
        5px;

}}


.four-item {{

    background:
        #0c1219;

    border-radius:
        8px;

    padding:
        8px 4px;

    text-align:
        center;

}}


.four-item.current {{

    border:
        1px solid #315a47;

}}


.four-label {{

    color:
        #768391;

    font-size:
        10px;

    margin-bottom:
        4px;

}}


.four-value {{

    font-size:
        12px;

    font-weight:
        700;

}}


.pattern-box {{

    margin-top:
        9px;

    padding:
        8px;

    background:
        #0c1219;

    border-radius:
        8px;

    font-size:
        11px;

}}


.pattern-label {{

    color:
        #727e8c;

    margin-right:
        6px;

}}


.pattern-value {{

    color:
        #dbe4ed;

    font-weight:
        700;

}}


/* =====================================================
   TOP LIST
   ===================================================== */

.top-list {{

    background:
        #111821;

    border:
        1px solid #202b36;

    border-radius:
        14px;

    overflow:
        hidden;

}}


.top-row {{

    display:
        grid;

    grid-template-columns:
        45px 1fr 120px 110px 85px;

    align-items:
        center;

    min-height:
        44px;

    border-bottom:
        1px solid #1b2530;

    padding:
        0 12px;

}}


.top-row:last-child {{

    border-bottom:
        none;

}}


.rank {{

    color:
        #687583;

    font-size:
        12px;

}}


.top-coin {{

    font-weight:
        700;

}}


.top-price,
.top-volume,
.top-change {{

    text-align:
        right;

    font-size:
        12px;

}}


.top-volume {{

    color:
        #b9c4cf;

}}


/* =====================================================
   Color
   ===================================================== */

.positive {{

    color:
        #42d392;

}}


.negative {{

    color:
        #ff647c;

}}


.neutral {{

    color:
        #aab4bf;

}}


.empty {{

    background:
        #111821;

    border:
        1px solid #202b36;

    border-radius:
        14px;

    padding:
        25px;

    text-align:
        center;

    color:
        #778390;

}}


/* =====================================================
   Mobile
   ===================================================== */

@media (max-width: 700px) {{

    body {{
        padding: 10px;
    }}

    .header {{
        align-items:
            flex-start;

        flex-direction:
            column;

        gap:
            5px;
    }}

    .btc-data {{
        gap:
            15px;
    }}

    .btc-price {{
        font-size:
            21px;
    }}

    .four-label {{
        font-size:
            9px;
    }}

    .four-value {{
        font-size:
            11px;
    }}

    .top-row {{
        grid-template-columns:
            35px 1fr 90px 75px 70px;

        padding:
            0 7px;
    }}

    .top-price,
    .top-volume,
    .top-change {{
        font-size:
            10px;
    }}

}}

</style>

</head>


<body>


<div class="container">


    <div class="header">

        <div class="title">
            📊 CRYPTO SIGNAL
        </div>

        <div class="updated">
            업데이트 {updated_text}
        </div>

    </div>


    <!-- BTC -->

    <div class="btc-card">

        <div class="btc-header">

            <div>

                <div class="btc-title">
                    {btc_state} BTC
                </div>

                <div class="btc-market">
                    비트코인 시황
                </div>

            </div>

        </div>


        <div class="btc-data">

            <div class="btc-price">
                {btc_price}
            </div>

            <div class="btc-change">
                {btc_change}
            </div>

        </div>

    </div>


    <!-- SIGNAL -->

    <div class="section-title">

        <h2>
            🚨 SIGNAL
        </h2>

        <div class="section-sub">
            당일 변동률 높은 순
        </div>

    </div>


    <div class="signal-grid">

        {signal_html}

    </div>


    <!-- TOP -->

    <div class="section-title">

        <h2>
            💰 거래대금 TOP {TOP_N}
        </h2>

        <div class="section-sub">
            업비트 24H 거래대금 순위
        </div>

    </div>


    <div class="top-list">

        {top_html}

    </div>


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
        update_data
    )

    while True:

        try:
            schedule.run_pending()

        except Exception as e:

            logging.exception(
                "scheduler error: %s",
                e
            )

        time.sleep(1)


# =========================================================
# 시작
# =========================================================

@app.on_event("startup")
def startup_event():

    logging.info(
        "Crypto dashboard starting..."
    )

    # 최초 데이터
    threading.Thread(
        target=update_data,
        daemon=True
    ).start()

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
