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
    format="%(asctime)s [%(levelname)s] %(message)s"
)


# =========================================================
# 설정
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


# =========================================================
# 전역 상태
# =========================================================

latest_rows = []
latest_btc = {}

last_update_time = None

upbit_markets_cache = []

lock = threading.Lock()


# =========================================================
# KST
# =========================================================

KST = ZoneInfo("Asia/Seoul")


def kst():
    return datetime.now(KST)


# =========================================================
# 4H 구간
# =========================================================

def get_current_4h_start(now=None):

    if now is None:
        now = kst()

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
    else:
        start_hour = 21

    if hour < 1:
        date_value = now.date() - timedelta(days=1)
        start_hour = 21
    else:
        date_value = now.date()

    return datetime(
        date_value.year,
        date_value.month,
        date_value.day,
        start_hour,
        0,
        0,
        tzinfo=KST
    )


def make_4h_period(start):

    end = start + timedelta(hours=4)

    return {
        "start": start,
        "end": end,
        "label": f"{start.strftime('%m/%d %H:%M')}~{end.strftime('%H:%M')}"
    }


def get_recent_4h_periods(count=6):

    current_start = get_current_4h_start()

    periods = []

    for i in range(count - 1, -1, -1):

        start = current_start - timedelta(hours=4 * i)

        periods.append(
            make_4h_period(start)
        )

    return periods


def get_current_4h_period():

    return make_4h_period(
        get_current_4h_start()
    )


def get_previous_4h_period():

    return make_4h_period(
        get_current_4h_start() - timedelta(hours=4)
    )


# =========================================================
# ★ 추가 : 전전 4H
# =========================================================

def get_pre_previous_4h_period():

    return make_4h_period(
        get_current_4h_start() - timedelta(hours=8)
    )


# =========================================================
# 요청 제어
# =========================================================

_last_request_time = 0


def wait_request():

    global _last_request_time

    now = time.time()

    elapsed = now - _last_request_time

    if elapsed < REQUEST_INTERVAL:
        time.sleep(
            REQUEST_INTERVAL - elapsed
        )

    _last_request_time = time.time()


def retry(func, *args, **kwargs):

    for attempt in range(MAX_RETRIES):

        try:

            wait_request()

            response = func(
                *args,
                **kwargs
            )

            if response.status_code == 429:

                logging.warning(
                    "Upbit 429 rate limit"
                )

                time.sleep(
                    RATE_LIMIT_WAIT
                )

                continue

            response.raise_for_status()

            return response

        except Exception as e:

            logging.warning(
                f"API 요청 실패 {attempt + 1}/{MAX_RETRIES}: {e}"
            )

            if attempt < MAX_RETRIES - 1:
                time.sleep(1)

    return None


# =========================================================
# 업비트 마켓
# =========================================================

def get_upbit_markets():

    url = "https://api.upbit.com/v1/market/all"

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

    markets = response.json()

    krw_markets = [
        x for x in markets
        if x.get("market", "").startswith("KRW-")
    ]

    if not krw_markets:
        return []

    market_codes = [
        x["market"]
        for x in krw_markets
    ]

    ticker_url = "https://api.upbit.com/v1/ticker"

    ticker_response = retry(
        requests.get,
        ticker_url,
        params={
            "markets": ",".join(market_codes)
        },
        timeout=10
    )

    if ticker_response is None:
        return []

    tickers = ticker_response.json()

    ticker_map = {
        x["market"]: x
        for x in tickers
    }

    result = []

    for market in krw_markets:

        code = market["market"]

        ticker = ticker_map.get(code)

        if not ticker:
            continue

        result.append({
            "market": code,
            "korean_name": market.get(
                "korean_name",
                code.replace("KRW-", "")
            ),
            "english_name": market.get(
                "english_name",
                code.replace("KRW-", "")
            ),
            "price": ticker.get(
                "trade_price",
                0
            ),
            "volume_24h": ticker.get(
                "acc_trade_price_24h",
                0
            )
        })

    # =====================================================
    # 24H 실제 거래대금 순
    # =====================================================

    result.sort(
        key=lambda x: x["volume_24h"],
        reverse=True
    )

    return result


# =========================================================
# 일봉 변동률
# =========================================================

def daily_change_upbit(
    market,
    current_price
):

    url = "https://api.upbit.com/v1/candles/days"

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
        return 0.0

    data = response.json()

    if len(data) < 2:
        return 0.0

    previous_close = data[1].get(
        "trade_price",
        0
    )

    if not previous_close:
        return 0.0

    return (
        (current_price - previous_close)
        / previous_close
    ) * 100


# =========================================================
# 업비트 60분봉
# =========================================================

def get_upbit_60m_candles(
    market,
    count=300
):

    url = "https://api.upbit.com/v1/candles/minutes/60"

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
        return []

    return response.json()


# =========================================================
# 4H 캔들 생성
# =========================================================

def build_upbit_4h_candles(
    market
):

    data = get_upbit_60m_candles(
        market,
        300
    )

    if not data:
        return []

    rows = []

    for item in reversed(data):

        candle_time = datetime.fromisoformat(
            item["candle_date_time_kst"]
        ).replace(
            tzinfo=KST
        )

        rows.append({
            "time": candle_time,
            "open": item["opening_price"],
            "high": item["high_price"],
            "low": item["low_price"],
            "close": item["trade_price"],
            "volume": item["candle_acc_trade_volume"],
            "value": item["candle_acc_trade_price"]
        })

    if not rows:
        return []

    df = pd.DataFrame(rows)

    def period_start(dt):

        if 1 <= dt.hour < 5:
            h = 1
            d = dt.date()

        elif 5 <= dt.hour < 9:
            h = 5
            d = dt.date()

        elif 9 <= dt.hour < 13:
            h = 9
            d = dt.date()

        elif 13 <= dt.hour < 17:
            h = 13
            d = dt.date()

        elif 17 <= dt.hour < 21:
            h = 17
            d = dt.date()

        elif dt.hour >= 21:
            h = 21
            d = dt.date()

        else:
            h = 21
            d = dt.date() - timedelta(days=1)

        return datetime(
            d.year,
            d.month,
            d.day,
            h,
            tzinfo=KST
        )

    df["period"] = df["time"].apply(
        period_start
    )

    grouped = []

    for period, group in df.groupby(
        "period",
        sort=True
    ):

        group = group.sort_values("time")

        grouped.append({
            "start": period,
            "open": group.iloc[0]["open"],
            "high": group["high"].max(),
            "low": group["low"].min(),
            "close": group.iloc[-1]["close"],
            "volume": group["volume"].sum(),
            "value": group["value"].sum()
        })

    return grouped


# =========================================================
# 4H 분석
# =========================================================

def analyze_4h(
    market,
    current_price
):

    candles = build_upbit_4h_candles(
        market
    )

    if not candles:
        return {
            "periods": [],
            "current_4h_change": 0.0,
            "previous_4h_change": 0.0,
            "pre_previous_4h_change": 0.0
        }

    current_period = get_current_4h_period()
    previous_period = get_previous_4h_period()
    pre_previous_period = get_pre_previous_4h_period()

    current_start = current_period["start"]
    previous_start = previous_period["start"]
    pre_previous_start = pre_previous_period["start"]

    current_candle = None
    previous_candle = None
    pre_previous_candle = None

    for candle in candles:

        if candle["start"] == current_start:
            current_candle = candle

        elif candle["start"] == previous_start:
            previous_candle = candle

        elif candle["start"] == pre_previous_start:
            pre_previous_candle = candle

    # =====================================================
    # 현재 4H
    # =====================================================

    current_change = 0.0

    if current_candle:

        current_open = current_candle["open"]

        if current_open:

            current_change = (
                (current_price - current_open)
                / current_open
            ) * 100

    # =====================================================
    # 이전 4H
    # =====================================================

    previous_change = 0.0

    if previous_candle:

        previous_open = previous_candle["open"]
        previous_close = previous_candle["close"]

        if previous_open:

            previous_change = (
                (previous_close - previous_open)
                / previous_open
            ) * 100

    # =====================================================
    # ★ 전전 4H
    # =====================================================

    pre_previous_change = 0.0

    if pre_previous_candle:

        pre_previous_open = pre_previous_candle["open"]
        pre_previous_close = pre_previous_candle["close"]

        if pre_previous_open:

            pre_previous_change = (
                (pre_previous_close - pre_previous_open)
                / pre_previous_open
            ) * 100

    return {
        "periods": candles,
        "current_4h_change": current_change,
        "previous_4h_change": previous_change,
        "pre_previous_4h_change": pre_previous_change
    }


# =========================================================
# 종목 분석
# =========================================================

def analyze(
    market,
    current_price
):

    daily_change = daily_change_upbit(
        market,
        current_price
    )

    four_hour = analyze_4h(
        market,
        current_price
    )

    return {
        "daily_change": daily_change,

        "current_4h_change":
            four_hour["current_4h_change"],

        "previous_4h_change":
            four_hour["previous_4h_change"],

        "pre_previous_4h_change":
            four_hour["pre_previous_4h_change"],

        "periods":
            four_hour["periods"]
    }


# =========================================================
# 변동률
# =========================================================

def get_change_value(
    value
):

    try:
        return float(value)

    except Exception:
        return 0.0


def format_change(
    value
):

    value = get_change_value(value)

    if value > 0:
        return f"+{value:.2f}%"

    return f"{value:.2f}%"


# =========================================================
# 가격
# =========================================================

def format_market_price(
    price
):

    try:
        price = float(price)

        if price >= 1_000_000:
            return f"{price:,.0f}"

        if price >= 1000:
            return f"{price:,.1f}"

        if price >= 1:
            return f"{price:,.2f}"

        if price >= 0.01:
            return f"{price:,.4f}"

        return f"{price:,.8f}"

    except Exception:

        return "-"


# =========================================================
# 거래대금
# =========================================================

def format_volume(
    value
):

    try:

        value = float(value)

        if value >= 1_000_000_000_000:
            return (
                f"{value / 1_000_000_000_000:.1f}조"
            )

        if value >= 100_000_000:
            return (
                f"{value / 100_000_000:.1f}억"
            )

        if value >= 10_000:
            return (
                f"{value / 10_000:.1f}만원"
            )

        return f"{value:,.0f}원"

    except Exception:

        return "-"


# =========================================================
# Row 생성
# =========================================================

def make_row(
    item,
    analysis,
    volume_rank
):

    market = item["market"]

    symbol = market.replace(
        "KRW-",
        ""
    )

    current_price = item["price"]

    daily_change = analysis[
        "daily_change"
    ]

    current_4h_change = analysis[
        "current_4h_change"
    ]

    previous_4h_change = analysis[
        "previous_4h_change"
    ]

    # ★ 전전 4H
    pre_previous_4h_change = analysis[
        "pre_previous_4h_change"
    ]

    # =====================================================
    # SIGNAL 조건
    #
    # 전전 4H 양수
    # 전 4H 음수
    # 현재 4H 양수
    # =====================================================

    pre_previous_4h_condition = (
        pre_previous_4h_change > 0
    )

    previous_4h_condition = (
        previous_4h_change < 0
    )

    current_4h_condition = (
        current_4h_change > 0
    )

    signal_pass = (
        pre_previous_4h_condition
        and previous_4h_condition
        and current_4h_condition
    )

    return {

        "market": market,

        "symbol": symbol,

        "korean_name":
            item.get(
                "korean_name",
                symbol
            ),

        "english_name":
            item.get(
                "english_name",
                symbol
            ),

        "price":
            current_price,

        "daily_change":
            daily_change,

        "current_4h_change":
            current_4h_change,

        "previous_4h_change":
            previous_4h_change,

        "pre_previous_4h_change":
            pre_previous_4h_change,

        "volume_24h":
            item.get(
                "volume_24h",
                0
            ),

        "volume_rank":
            volume_rank,

        "signal_pass":
            signal_pass,

        "signal_conditions": {

            "pre_previous_4h":
                pre_previous_4h_condition,

            "previous_4h":
                previous_4h_condition,

            "current_4h":
                current_4h_condition

        },

        "periods":
            analysis.get(
                "periods",
                []
            )
    }


# =========================================================
# 업비트 업데이트
# =========================================================

def update_upbit():

    global latest_rows
    global upbit_markets_cache

    if USE_UPBIT != "Y":
        return

    try:

        markets = get_upbit_markets()

        if not markets:
            logging.warning(
                "업비트 마켓 데이터를 가져오지 못했습니다."
            )
            return

        upbit_markets_cache = markets

        # =================================================
        # 실제 업비트 전체 거래대금 순위
        # =================================================

        volume_rank_map = {
            item["market"]: rank
            for rank, item in enumerate(
                markets,
                start=1
            )
        }

        # =================================================
        # TOP_N만 분석
        # =================================================

        top_markets = markets[:TOP_N]

        rows = []

        for item in top_markets:

            market = item["market"]

            current_price = item["price"]

            try:

                analysis = analyze(
                    market,
                    current_price
                )

                row = make_row(
                    item,
                    analysis,
                    volume_rank_map.get(
                        market,
                        0
                    )
                )

                rows.append(row)

            except Exception as e:

                logging.warning(
                    f"{market} 분석 실패: {e}"
                )

        # =================================================
        # SIGNAL
        #
        # 전전 4H +
        # 전 4H -
        # 현재 4H +
        #
        # SIGNAL 순위는 24H 거래대금 큰 순
        # =================================================

        signal_rows = [
            row
            for row in rows
            if row["signal_pass"]
        ]

        signal_rows.sort(
            key=lambda x: x["volume_24h"],
            reverse=True
        )

        # =================================================
        # 일반 TOP 리스트도 거래대금 순
        # =================================================

        rows.sort(
            key=lambda x: x["volume_24h"],
            reverse=True
        )

        # =================================================
        # signal rank 저장
        # =================================================

        signal_rank_map = {
            row["market"]: rank
            for rank, row in enumerate(
                signal_rows,
                start=1
            )
        }

        for row in rows:

            row["signal_rank"] = (
                signal_rank_map.get(
                    row["market"]
                )
            )

        with lock:

            latest_rows = rows

        logging.info(
            f"업비트 업데이트 완료 "
            f"TOP={len(rows)} / "
            f"SIGNAL={len(signal_rows)}"
        )

    except Exception as e:

        logging.exception(
            f"update_upbit 오류: {e}"
        )


# =========================================================
# OKX BTC
# =========================================================

def get_okx_btc():

    url = "https://www.okx.com/api/v5/market/ticker"

    response = retry(
        requests.get,
        url,
        params={
            "instId": "BTC-USDT-SWAP"
        },
        timeout=10
    )

    if response is None:
        return None

    data = response.json()

    if data.get("code") != "0":
        return None

    result = data.get(
        "data",
        []
    )

    if not result:
        return None

    return result[0]


def update_btc_market():

    global latest_btc

    try:

        ticker = get_okx_btc()

        if not ticker:
            return

        last_price = float(
            ticker.get(
                "last",
                0
            )
        )

        open_24h = float(
            ticker.get(
                "open24h",
                0
            )
        )

        if open_24h:

            change = (
                (last_price - open_24h)
                / open_24h
            ) * 100

        else:

            change = 0

        latest_btc = {

            "price":
                last_price,

            "change":
                change,

            "state":
                "☀️"
                if change > 0
                else "🌧️"
                if change < 0
                else "⚪"

        }

    except Exception as e:

        logging.warning(
            f"BTC 업데이트 실패: {e}"
        )


# =========================================================
# OKX 업데이트
# =========================================================

def update_okx():

    if USE_OKX != "Y":
        return

    # 기존 OKX 기능 자리
    pass


# =========================================================
# USDT/KRW
# =========================================================

def get_usdt_krw():

    url = (
        "https://api.upbit.com/v1/ticker"
    )

    response = retry(
        requests.get,
        url,
        params={
            "markets": "KRW-USDT"
        },
        timeout=10
    )

    if response is None:
        return 0

    data = response.json()

    if not data:
        return 0

    return data[0].get(
        "trade_price",
        0
    )


# =========================================================
# 대시보드 업데이트
# =========================================================

def update_dashboard():

    global last_update_time

    update_upbit()

    if USE_OKX == "Y":
        update_okx()

    update_btc_market()

    last_update_time = kst()


# =========================================================
# 공통 색상
# =========================================================

def change_class(value):

    value = get_change_value(value)

    if value > 0:
        return "positive"

    if value < 0:
        return "negative"

    return "neutral"


# =========================================================
# 4H 미니 표시
# =========================================================

def four_hour_item(
    title,
    period,
    value
):

    cls = change_class(value)

    return f"""
    <div class="four-hour-item">
        <div class="four-hour-title">
            {html.escape(title)}
        </div>

        <div class="four-hour-period">
            {html.escape(period)}
        </div>

        <div class="four-hour-change {cls}">
            {format_change(value)}
        </div>
    </div>
    """


# =========================================================
# SIGNAL 카드
# =========================================================

def signal_card_html(
    row,
    signal_rank
):

    symbol = html.escape(
        row["symbol"]
    )

    korean_name = html.escape(
        row["korean_name"]
    )

    volume_rank = row.get(
        "volume_rank",
        0
    )

    volume_24h = row.get(
        "volume_24h",
        0
    )

    price = row.get(
        "price",
        0
    )

    pre_previous_change = row.get(
        "pre_previous_4h_change",
        0
    )

    previous_change = row.get(
        "previous_4h_change",
        0
    )

    current_change = row.get(
        "current_4h_change",
        0
    )

    periods = row.get(
        "periods",
        []
    )

    period_map = {
        p["start"]: p
        for p in periods
    }

    current_period = get_current_4h_period()
    previous_period = get_previous_4h_period()
    pre_previous_period = get_pre_previous_4h_period()

    pre_previous_label = (
        pre_previous_period["label"]
    )

    previous_label = (
        previous_period["label"]
    )

    current_label = (
        current_period["label"]
    )

    return f"""
    <div class="signal-card">

        <div class="signal-card-top">

            <div>

                <div class="signal-rank">
                    #{signal_rank}
                </div>

                <div class="signal-symbol">
                    {symbol}
                </div>

                <div class="signal-name">
                    {korean_name}
                </div>

            </div>

            <div class="signal-volume-rank">
                거래대금 {volume_rank}위
            </div>

        </div>

        <div class="signal-price">
            {format_market_price(price)}
        </div>

        <div class="signal-volume">
            24H 거래대금
            <strong>
                {format_volume(volume_24h)}
            </strong>
        </div>

        <div class="signal-pattern">

            <div class="signal-pattern-title">
                SIGNAL
            </div>

            <div class="signal-pattern-text">
                전전 4H 양수
                →
                전 4H 음수
                →
                현재 4H 양수
            </div>

        </div>

        <div class="four-hour-grid">

            {four_hour_item(
                "전전 4H",
                pre_previous_label,
                pre_previous_change
            )}

            {four_hour_item(
                "전 4H",
                previous_label,
                previous_change
            )}

            {four_hour_item(
                "현재 4H",
                current_label,
                current_change
            )}

        </div>

    </div>
    """


# =========================================================
# TOP 카드
# =========================================================

def top_card_html(
    row,
    rank
):

    symbol = html.escape(
        row["symbol"]
    )

    korean_name = html.escape(
        row["korean_name"]
    )

    price = row.get(
        "price",
        0
    )

    daily_change = row.get(
        "daily_change",
        0
    )

    current_4h_change = row.get(
        "current_4h_change",
        0
    )

    previous_4h_change = row.get(
        "previous_4h_change",
        0
    )

    pre_previous_4h_change = row.get(
        "pre_previous_4h_change",
        0
    )

    volume = row.get(
        "volume_24h",
        0
    )

    volume_rank = row.get(
        "volume_rank",
        rank
    )

    return f"""
    <div class="top-card">

        <div class="top-header">

            <div class="top-rank">
                #{rank}
            </div>

            <div class="top-symbol">
                {symbol}
            </div>

            <div class="top-name">
                {korean_name}
            </div>

            <div class="top-volume-rank">
                거래대금 {volume_rank}위
            </div>

        </div>

        <div class="top-price">
            {format_market_price(price)}
        </div>

        <div class="top-stats">

            <div class="top-stat">

                <div class="stat-title">
                    일봉
                </div>

                <div class="
                    stat-value
                    {change_class(daily_change)}
                ">
                    {format_change(daily_change)}
                </div>

            </div>

            <div class="top-stat">

                <div class="stat-title">
                    전전 4H
                </div>

                <div class="
                    stat-value
                    {change_class(pre_previous_4h_change)}
                ">
                    {format_change(pre_previous_4h_change)}
                </div>

            </div>

            <div class="top-stat">

                <div class="stat-title">
                    전 4H
                </div>

                <div class="
                    stat-value
                    {change_class(previous_4h_change)}
                ">
                    {format_change(previous_4h_change)}
                </div>

            </div>

            <div class="top-stat">

                <div class="stat-title">
                    현재 4H
                </div>

                <div class="
                    stat-value
                    {change_class(current_4h_change)}
                ">
                    {format_change(current_4h_change)}
                </div>

            </div>

            <div class="top-stat">

                <div class="stat-title">
                    24H 거래대금
                </div>

                <div class="stat-value">
                    {format_volume(volume)}
                </div>

            </div>

        </div>

    </div>
    """


# =========================================================
# SIGNAL 섹션
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

    signal_rows.sort(
        key=lambda x: x.get(
            "volume_24h",
            0
        ),
        reverse=True
    )

    cards = []

    for rank, row in enumerate(
        signal_rows,
        start=1
    ):

        cards.append(
            signal_card_html(
                row,
                rank
            )
        )

    if not cards:

        cards_html = """
        <div class="empty-message">
            전전 4H 양수 + 전 4H 음수 + 현재 4H 양수
            조건을 모두 만족하는 종목 없음
        </div>
        """

    else:

        cards_html = "".join(
            cards
        )

    return f"""
    <section class="section signal-section">

        <div class="section-header">

            <div>

                <div class="section-title">
                    SIGNAL
                </div>

                <div class="section-subtitle">
                    전전 4H 양수 · 전 4H 음수 · 현재 4H 양수
                </div>

            </div>

            <div class="section-count">
                {len(signal_rows)}
            </div>

        </div>

        <div class="signal-grid">

            {cards_html}

        </div>

    </section>
    """


# =========================================================
# TOP 리스트
# =========================================================

def top_list_section(
    rows
):

    cards = []

    for rank, row in enumerate(
        rows,
        start=1
    ):

        cards.append(
            top_card_html(
                row,
                rank
            )
        )

    return f"""
    <section class="section">

        <div class="section-header">

            <div>

                <div class="section-title">
                    TOP {TOP_N}
                </div>

                <div class="section-subtitle">
                    24H 실제 거래대금 기준
                </div>

            </div>

        </div>

        <div class="top-grid">

            {"".join(cards)}

        </div>

    </section>
    """


# =========================================================
# BTC BAR
# =========================================================

def btc_bar_html():

    price = latest_btc.get(
        "price",
        0
    )

    change = latest_btc.get(
        "change",
        0
    )

    state = latest_btc.get(
        "state",
        "⚪"
    )

    return f"""
    <div class="btc-bar">

        <div class="btc-left">

            <div class="btc-icon">
                {state}
            </div>

            <div>

                <div class="btc-title">
                    BTC
                </div>

                <div class="btc-subtitle">
                    시장 기준
                </div>

            </div>

        </div>

        <div class="btc-price">
            {format_market_price(price)}
        </div>

        <div class="
            btc-change
            {change_class(change)}
        ">
            {format_change(change)}
        </div>

    </div>
    """


# =========================================================
# 전체 HTML
# =========================================================

def dashboard():

    with lock:

        rows = list(
            latest_rows
        )

    now_text = (
        last_update_time.strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        if last_update_time
        else "-"
    )

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
            content="{UPDATE_MINUTES * 60}"
        >

        <title>
            Crypto Dashboard
        </title>

        <style>

        * {{
            box-sizing: border-box;
        }}

        body {{
            margin: 0;
            padding: 0;
            background: #0b0f14;
            color: #e8edf3;
            font-family:
                -apple-system,
                BlinkMacSystemFont,
                "Segoe UI",
                sans-serif;
        }}

        .container {{
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
        }}

        .page-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 16px;
        }}

        .page-title {{
            font-size: 25px;
            font-weight: 800;
        }}

        .update-time {{
            color: #87919d;
            font-size: 12px;
        }}

        .btc-bar {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 15px;

            padding: 17px 20px;
            margin-bottom: 20px;

            background: #121820;
            border: 1px solid #26313d;
            border-radius: 14px;
        }}

        .btc-left {{
            display: flex;
            align-items: center;
            gap: 12px;
        }}

        .btc-icon {{
            font-size: 23px;
        }}

        .btc-title {{
            font-size: 17px;
            font-weight: 800;
        }}

        .btc-subtitle {{
            color: #7d8996;
            font-size: 11px;
        }}

        .btc-price {{
            font-weight: 800;
        }}

        .btc-change {{
            font-weight: 800;
        }}

        .section {{
            margin-bottom: 30px;
        }}

        .section-header {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 13px;
        }}

        .section-title {{
            font-size: 21px;
            font-weight: 800;
        }}

        .section-subtitle {{
            margin-top: 4px;
            color: #7d8996;
            font-size: 12px;
        }}

        .section-count {{
            min-width: 28px;
            height: 28px;

            display: flex;
            align-items: center;
            justify-content: center;

            border-radius: 50%;
            background: #1d2731;

            font-size: 12px;
            font-weight: 800;
        }}

        .signal-grid {{
            display: grid;
            grid-template-columns:
                repeat(auto-fill, minmax(290px, 1fr));
            gap: 12px;
        }}

        .signal-card {{
            padding: 16px;

            background: #121820;
            border: 1px solid #26313d;
            border-radius: 14px;
        }}

        .signal-card-top {{
            display: flex;
            align-items: flex-start;
            justify-content: space-between;
        }}

        .signal-rank {{
            color: #8f9aa7;
            font-size: 11px;
            font-weight: 700;
        }}

        .signal-symbol {{
            margin-top: 2px;
            font-size: 19px;
            font-weight: 900;
        }}

        .signal-name {{
            margin-top: 2px;
            color: #7d8996;
            font-size: 11px;
        }}

        .signal-volume-rank {{
            padding: 5px 8px;
            border-radius: 8px;

            background: #1b2832;
            color: #b9c5d1;

            font-size: 11px;
            font-weight: 700;
        }}

        .signal-price {{
            margin-top: 14px;
            font-size: 18px;
            font-weight: 800;
        }}

        .signal-volume {{
            margin-top: 4px;
            color: #7d8996;
            font-size: 11px;
        }}

        .signal-volume strong {{
            color: #dbe3ea;
            margin-left: 4px;
        }}

        .signal-pattern {{
            margin-top: 14px;
            padding: 10px;

            background: #0d1319;
            border-radius: 10px;
        }}

        .signal-pattern-title {{
            color: #8794a1;
            font-size: 10px;
            font-weight: 800;
        }}

        .signal-pattern-text {{
            margin-top: 5px;
            color: #dfe7ee;
            font-size: 12px;
            font-weight: 800;
        }}

        .four-hour-grid {{
            display: grid;
            grid-template-columns:
                repeat(3, 1fr);

            gap: 6px;
            margin-top: 9px;
        }}

        .four-hour-item {{
            padding: 9px 5px;

            background: #0d1319;
            border-radius: 9px;

            text-align: center;
        }}

        .four-hour-title {{
            color: #8b96a2;
            font-size: 10px;
            font-weight: 700;
        }}

        .four-hour-period {{
            margin-top: 3px;
            color: #66727f;
            font-size: 8px;
        }}

        .four-hour-change {{
            margin-top: 5px;
            font-size: 12px;
            font-weight: 900;
        }}

        .top-grid {{
            display: grid;
            grid-template-columns:
                repeat(auto-fill, minmax(320px, 1fr));
            gap: 10px;
        }}

        .top-card {{
            padding: 14px 15px;

            background: #121820;
            border: 1px solid #202b36;
            border-radius: 12px;
        }}

        .top-header {{
            display: flex;
            align-items: center;
            gap: 8px;
        }}

        .top-rank {{
            color: #697581;
            font-size: 11px;
            font-weight: 700;
        }}

        .top-symbol {{
            font-weight: 900;
        }}

        .top-name {{
            color: #7d8996;
            font-size: 11px;
        }}

        .top-volume-rank {{
            margin-left: auto;

            padding: 4px 7px;

            border-radius: 7px;

            background: #1a242e;
            color: #9aa6b2;

            font-size: 10px;
        }}

        .top-price {{
            margin-top: 9px;
            font-size: 16px;
            font-weight: 800;
        }}

        .top-stats {{
            display: grid;
            grid-template-columns:
                repeat(5, 1fr);

            gap: 5px;
            margin-top: 11px;
        }}

        .top-stat {{
            padding: 7px 3px;

            background: #0d1319;
            border-radius: 7px;

            text-align: center;
        }}

        .stat-title {{
            color: #697581;
            font-size: 9px;
        }}

        .stat-value {{
            margin-top: 3px;
            font-size: 10px;
            font-weight: 800;
        }}

        .positive {{
            color: #4cd97b;
        }}

        .negative {{
            color: #ff6675;
        }}

        .neutral {{
            color: #9aa6b2;
        }}

        .empty-message {{
            padding: 40px 20px;

            text-align: center;

            background: #121820;
            border: 1px solid #26313d;
            border-radius: 14px;

            color: #7d8996;
            font-size: 13px;
        }}

        @media (max-width: 700px) {{

            .container {{
                padding: 12px;
            }}

            .page-title {{
                font-size: 21px;
            }}

            .btc-bar {{
                flex-wrap: wrap;
            }}

            .top-stats {{
                grid-template-columns:
                    repeat(3, 1fr);
            }}

        }}

        </style>

    </head>

    <body>

        <div class="container">

            <div class="page-header">

                <div class="page-title">
                    Crypto Dashboard
                </div>

                <div class="update-time">
                    업데이트 {now_text}
                </div>

            </div>

            {btc_bar_html()}

            {focus_section(rows)}

            {top_list_section(rows)}

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

    return dashboard()


@app.get(
    "/health"
)
def health():

    return {
        "status": "ok",
        "updated":
            last_update_time.isoformat()
            if last_update_time
            else None
    }


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
                f"스케줄러 오류: {e}"
            )

        time.sleep(1)


# =========================================================
# 설정 검증
# =========================================================

def validate_settings():

    logging.info(
        "========================================"
    )

    logging.info(
        "Crypto Dashboard 시작"
    )

    logging.info(
        f"TOP_N = {TOP_N}"
    )

    logging.info(
        f"UPDATE_MINUTES = {UPDATE_MINUTES}"
    )

    logging.info(
        "========================================"
    )

    logging.info(
        "SIGNAL 조건"
    )

    logging.info(
        "1. 전전 4H > 0"
    )

    logging.info(
        "2. 전 4H < 0"
    )

    logging.info(
        "3. 현재 4H > 0"
    )

    logging.info(
        "=> 양 → 음 → 양"
    )

    logging.info(
        "SIGNAL 순위 = 24H 실제 거래대금 큰 순"
    )

    logging.info(
        "SIGNAL 화면 = 업비트 전체 거래대금 순위도 표시"
    )

    logging.info(
        "BTC는 SIGNAL 조건에서 제외"
    )

    logging.info(
        "========================================"
    )


# =========================================================
# Startup
# =========================================================

@app.on_event("startup")
def startup():

    validate_settings()

    update_dashboard()

    thread = threading.Thread(
        target=scheduler_loop,
        daemon=True
    )

    thread.start()

    logging.info(
        "스케줄러 시작 완료"
    )


# =========================================================
# 실행
# =========================================================

if __name__ == "__main__":

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )
