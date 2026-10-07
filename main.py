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
# 설정
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

SIGNAL_TIMEFRAME = "4h"

TIMEFRAME_LABEL = {
    "4h": "4시간봉"
}

EMA_FAST = 20
EMA_SLOW = 60


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

latest_signal_data = []

latest_btc_okx_price = None
latest_btc_daily_change = None

request_lock = threading.Lock()
update_lock = threading.Lock()

last_request_time = 0


# =========================================================
# 공통 HTTP 요청
# =========================================================

def safe_get(
    url,
    params=None,
    headers=None,
    timeout=10
):
    global last_request_time

    for attempt in range(MAX_RETRIES):

        try:

            with request_lock:

                elapsed = time.time() - last_request_time

                if elapsed < REQUEST_INTERVAL:
                    time.sleep(
                        REQUEST_INTERVAL - elapsed
                    )

                response = requests.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=timeout
                )

                last_request_time = time.time()

            if response.status_code == 200:
                return response

            if response.status_code == 429:

                log.warning(
                    "429 rate limit: %s",
                    url
                )

                time.sleep(RATE_LIMIT_WAIT)

                continue

            log.warning(
                "HTTP %s: %s",
                response.status_code,
                url
            )

        except Exception as e:

            log.warning(
                "REQUEST ERROR %s: %s",
                url,
                e
            )

            time.sleep(1)

    return None


# =========================================================
# Upbit 마켓 조회
# =========================================================

def get_upbit_markets():

    url = "https://api.upbit.com/v1/market/all"

    response = safe_get(
        url,
        params={
            "isDetails": "false"
        }
    )

    if response is None:
        return []

    try:

        markets = response.json()

        krw_markets = [
            item
            for item in markets
            if item.get("market", "").startswith("KRW-")
        ]

        return krw_markets

    except Exception as e:

        log.error(
            "get_upbit_markets error: %s",
            e
        )

        return []


# =========================================================
# Upbit 현재가 / 거래대금
# =========================================================

def get_upbit_tickers(markets):

    if not markets:
        return []

    result = []

    batch_size = 100

    for i in range(
        0,
        len(markets),
        batch_size
    ):

        batch = markets[
            i:i + batch_size
        ]

        market_codes = ",".join(
            item["market"]
            for item in batch
        )

        response = safe_get(
            "https://api.upbit.com/v1/ticker",
            params={
                "markets": market_codes
            }
        )

        if response is None:
            continue

        try:

            data = response.json()

            for item in data:

                result.append({
                    "market": item.get("market"),
                    "current_price": float(
                        item.get(
                            "trade_price",
                            0
                        )
                    ),
                    "volume_24h": float(
                        item.get(
                            "acc_trade_price_24h",
                            0
                        )
                    )
                })

        except Exception as e:

            log.error(
                "get_upbit_tickers error: %s",
                e
            )

    return result


# =========================================================
# Upbit 일봉
# =========================================================

def get_upbit_daily_candles(
    market,
    count=200
):

    response = safe_get(
        "https://api.upbit.com/v1/candles/days",
        params={
            "market": market,
            "count": count
        }
    )

    if response is None:
        return pd.DataFrame()

    try:

        data = response.json()

        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)

        df["timestamp"] = pd.to_datetime(
            df["candle_date_time_kst"]
        )

        df = df.sort_values(
            "timestamp"
        ).reset_index(drop=True)

        return df

    except Exception as e:

        log.warning(
            "daily candle error %s: %s",
            market,
            e
        )

        return pd.DataFrame()


# =========================================================
# Upbit 4시간봉
# =========================================================

def get_upbit_4h_candles(
    market,
    count=200
):

    response = safe_get(
        "https://api.upbit.com/v1/candles/minutes/240",
        params={
            "market": market,
            "count": count
        }
    )

    if response is None:
        return pd.DataFrame()

    try:

        data = response.json()

        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)

        df["timestamp"] = pd.to_datetime(
            df["candle_date_time_kst"]
        )

        df = df.sort_values(
            "timestamp"
        ).reset_index(drop=True)

        return df

    except Exception as e:

        log.warning(
            "4H candle error %s: %s",
            market,
            e
        )

        return pd.DataFrame()


# =========================================================
# EMA 계산
# =========================================================

def calculate_ema(
    df,
    period
):

    if df.empty:
        return pd.Series(dtype=float)

    return df["close"].ewm(
        span=period,
        adjust=False
    ).mean()


# =========================================================
# 4시간 EMA 분석
# =========================================================

def analyze_ema_4h(
    market,
    price=None
):

    df = get_upbit_4h_candles(
        market,
        count=200
    )

    if df.empty:
        return {
            "ema20": None,
            "ema60": None,
            "reverse": False,
            "alignment": "동일"
        }

    work = df.copy()

    if price is not None:

        work.loc[
            work.index[-1],
            "close"
        ] = price

    work["ema20"] = calculate_ema(
        work,
        EMA_FAST
    )

    work["ema60"] = calculate_ema(
        work,
        EMA_SLOW
    )

    ema20 = float(
        work["ema20"].iloc[-1]
    )

    ema60 = float(
        work["ema60"].iloc[-1]
    )

    if ema20 < ema60:

        alignment = "역배열"
        reverse = True

    elif ema20 > ema60:

        alignment = "정배열"
        reverse = False

    else:

        alignment = "동일"
        reverse = False

    return {
        "ema20": ema20,
        "ema60": ema60,
        "reverse": reverse,
        "alignment": alignment
    }


# =========================================================
# 캔들 패턴
# =========================================================

def is_bullish_engulfing(
    previous,
    current
):

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

    return (
        prev_close < prev_open
        and curr_close > curr_open
        and curr_open <= prev_close
        and curr_close >= prev_open
    )


def is_bearish_engulfing(
    previous,
    current
):

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

    return (
        prev_close > prev_open
        and curr_close < curr_open
        and curr_open >= prev_close
        and curr_close <= prev_open
    )


def is_positive_doji(
    current
):

    open_price = float(
        current["open"]
    )

    close_price = float(
        current["close"]
    )

    high_price = float(
        current["high"]
    )

    low_price = float(
        current["low"]
    )

    candle_range = (
        high_price - low_price
    )

    if candle_range <= 0:
        return False

    body = abs(
        close_price - open_price
    )

    return (
        body <= candle_range * 0.1
        and close_price >= open_price
    )


def is_negative_doji(
    current
):

    open_price = float(
        current["open"]
    )

    close_price = float(
        current["close"]
    )

    high_price = float(
        current["high"]
    )

    low_price = float(
        current["low"]
    )

    candle_range = (
        high_price - low_price
    )

    if candle_range <= 0:
        return False

    body = abs(
        close_price - open_price
    )

    return (
        body <= candle_range * 0.1
        and close_price < open_price
    )


def is_bullish_piercing(
    previous,
    current
):

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

    if not (
        prev_close < prev_open
        and curr_close > curr_open
    ):
        return False

    prev_mid = (
        prev_open + prev_close
    ) / 2

    return (
        curr_open < prev_close
        and curr_close > prev_mid
        and curr_close < prev_open
    )


def is_bearish_piercing(
    previous,
    current
):

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

    if not (
        prev_close > prev_open
        and curr_close < curr_open
    ):
        return False

    prev_mid = (
        prev_open + prev_close
    ) / 2

    return (
        curr_open > prev_close
        and curr_close < prev_mid
        and curr_close > prev_open
    )


def get_current_pattern(
    periods
):

    if not periods:
        return None

    if len(periods) >= 2:

        previous = periods[-2]
        current = periods[-1]

        if is_bullish_engulfing(
            previous,
            current
        ):
            return "상승장악형"

        if is_bearish_engulfing(
            previous,
            current
        ):
            return "하락장악형"

        if is_bullish_piercing(
            previous,
            current
        ):
            return "상승관통형"

        if is_bearish_piercing(
            previous,
            current
        ):
            return "하락관통형"

    current = periods[-1]

    if is_positive_doji(current):
        return "양수도지"

    if is_negative_doji(current):
        return "음수도지"

    return None


# =========================================================
# 4시간봉 데이터 구성
# =========================================================

def build_upbit_4h_periods(
    df,
    current_price=None
):

    if df.empty:
        return []

    periods = []

    source = df.tail(6).copy()

    for idx, row in source.iterrows():

        open_price = float(
            row["opening_price"]
        )

        high_price = float(
            row["high_price"]
        )

        low_price = float(
            row["low_price"]
        )

        close_price = float(
            row["trade_price"]
        )

        if (
            idx == source.index[-1]
            and current_price is not None
        ):
            close_price = float(
                current_price
            )

        change = 0

        if open_price != 0:

            change = (
                (
                    close_price
                    - open_price
                )
                / open_price
            ) * 100

        periods.append({
            "timestamp": row["timestamp"],
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "change": change,
            "active": idx == source.index[-1],
            "label": row["timestamp"].strftime(
                "%m/%d %H:%M"
            )
        })

    return periods


# =========================================================
# 당일 변동률
# Upbit 09:00 기준
# =========================================================

def analyze_daily_change(
    market,
    price
):

    now = datetime.now(KST)

    today_09 = now.replace(
        hour=9,
        minute=0,
        second=0,
        microsecond=0
    )

    if now < today_09:
        base_time = today_09 - timedelta(
            days=1
        )
    else:
        base_time = today_09

    df = get_upbit_daily_candles(
        market,
        count=3
    )

    if df.empty:
        return 0.0

    base_price = None

    target_date = base_time.date()

    for _, row in df.iterrows():

        candle_time = row["timestamp"]

        if candle_time.date() == target_date:

            base_price = float(
                row["opening_price"]
            )

            break

    if base_price is None:

        if len(df) >= 1:
            base_price = float(
                df.iloc[-1]["opening_price"]
            )

    if not base_price:
        return 0.0

    return (
        (
            float(price)
            - base_price
        )
        / base_price
    ) * 100


# =========================================================
# 업비트 종목 분석
# =========================================================

def make_row(
    rank,
    market,
    current_price,
    volume_24h,
    daily_change
):

    market_name = market.replace(
        "KRW-",
        ""
    )

    df_4h = get_upbit_4h_candles(
        market,
        count=200
    )

    periods_4h = build_upbit_4h_periods(
        df_4h,
        current_price
    )

    current_pattern = get_current_pattern(
        periods_4h
    )

    ema_info = analyze_ema_4h(
        market,
        current_price
    )

    return {
        "rank": rank,
        "name": market_name,
        "market": market,
        "volume_24h": volume_24h,
        "current_price": current_price,
        "daily_change": daily_change,
        "periods_4h": periods_4h,
        "current_pattern": current_pattern,
        "ema20": ema_info["ema20"],
        "ema60": ema_info["ema60"],
        "ema_alignment": ema_info["alignment"],
        "ema_reverse": ema_info["reverse"]
    }


# =========================================================
# Upbit 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update_time
    global latest_signal_data
    global latest_upbit_markets

    try:

        markets = get_upbit_markets()

        if not markets:
            return

        latest_upbit_markets = markets

        tickers = get_upbit_tickers(
            markets
        )

        if not tickers:
            return

        ticker_map = {
            item["market"]: item
            for item in tickers
        }

        rows = []

        for market_info in markets:

            market = market_info["market"]

            ticker = ticker_map.get(
                market
            )

            if not ticker:
                continue

            current_price = ticker[
                "current_price"
            ]

            volume_24h = ticker[
                "volume_24h"
            ]

            try:

                daily_change = (
                    analyze_daily_change(
                        market,
                        current_price
                    )
                )

            except Exception as e:

                log.warning(
                    "daily change error %s: %s",
                    market,
                    e
                )

                daily_change = 0.0

            rows.append({
                "market": market,
                "current_price": current_price,
                "volume_24h": volume_24h,
                "daily_change": daily_change
            })

        rows.sort(
            key=lambda x: x["volume_24h"],
            reverse=True
        )

        top_rows = rows[:TOP_N]

        final_rows = []

        for rank, item in enumerate(
            top_rows,
            start=1
        ):

            row = make_row(
                rank=rank,
                market=item["market"],
                current_price=item["current_price"],
                volume_24h=item["volume_24h"],
                daily_change=item["daily_change"]
            )

            final_rows.append(row)

        latest_upbit_data = final_rows

        # =================================================
        # SIGNAL
        # 4시간봉 EMA20 < EMA60
        # =================================================

        latest_signal_data = [
            row
            for row in final_rows
            if row.get("ema_reverse")
        ]

        latest_upbit_update_time = (
            datetime.now(KST).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        )

        log.info(
            "UPBIT TOP%d / SIGNAL=%d",
            TOP_N,
            len(latest_signal_data)
        )

    except Exception as e:

        log.exception(
            "update_upbit error: %s",
            e
        )


# =========================================================
# OKX
# BTC 시황용
# =========================================================

def okx_candles(
    bar="4H",
    limit=200
):

    response = safe_get(
        "https://www.okx.com/api/v5/market/candles",
        params={
            "instId": "BTC-USDT-SWAP",
            "bar": bar,
            "limit": limit
        }
    )

    if response is None:
        return pd.DataFrame()

    try:

        data = response.json()

        if data.get("code") != "0":
            return pd.DataFrame()

        rows = data.get(
            "data",
            []
        )

        if not rows:
            return pd.DataFrame()

        result = []

        for row in rows:

            result.append({
                "timestamp": datetime.fromtimestamp(
                    int(row[0]) / 1000,
                    tz=ZoneInfo("UTC")
                ).astimezone(KST),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4])
            })

        df = pd.DataFrame(
            result
        )

        df = df.sort_values(
            "timestamp"
        ).reset_index(drop=True)

        return df

    except Exception as e:

        log.warning(
            "OKX candles error: %s",
            e
        )

        return pd.DataFrame()


def okx_price():

    response = safe_get(
        "https://www.okx.com/api/v5/market/ticker",
        params={
            "instId": "BTC-USDT-SWAP"
        }
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
            "OKX price error: %s",
            e
        )

        return None


# =========================================================
# BTC 당일 변동률
# =========================================================

def update_okx_btc():

    global latest_btc_okx_price
    global latest_btc_daily_change
    global latest_okx_update_time

    try:

        price = okx_price()

        if price is None:
            return

        df = okx_candles(
            bar="1D",
            limit=10
        )

        daily_change = 0.0

        if not df.empty:

            now = datetime.now(KST)

            today_09 = now.replace(
                hour=9,
                minute=0,
                second=0,
                microsecond=0
            )

            if now < today_09:
                base_time = (
                    today_09
                    - timedelta(days=1)
                )
            else:
                base_time = today_09

            base_price = None

            for _, row in df.iterrows():

                candle_time = (
                    row["timestamp"]
                )

                if (
                    candle_time <= base_time
                ):
                    base_price = float(
                        row["close"]
                    )

            if base_price is None:
                base_price = float(
                    df.iloc[-1]["open"]
                )

            if base_price:

                daily_change = (
                    (
                        price
                        - base_price
                    )
                    / base_price
                ) * 100

        latest_btc_okx_price = price

        latest_btc_daily_change = (
            daily_change
        )

        latest_okx_update_time = (
            datetime.now(KST).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        )

    except Exception as e:

        log.exception(
            "update_okx_btc error: %s",
            e
        )


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    if not update_lock.acquire(
        blocking=False
    ):
        return

    try:

        update_okx_btc()

        if USE_UPBIT == "Y":
            update_upbit()

    except Exception as e:

        log.exception(
            "update_dashboard error: %s",
            e
        )

    finally:

        update_lock.release()


# =========================================================
# 포맷
# =========================================================

def fmt_price(
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

        return f"{value:.6f}"

    except Exception:
        return "-"


def fmt_vol(
    value
):

    if value is None:
        return "-"

    try:

        value = float(value)

        if value >= 1_000_000_000_000:

            return (
                f"{value / 1_000_000_000_000:.2f}"
                "조"
            )

        if value >= 100_000_000:

            return (
                f"{value / 100_000_000:.1f}"
                "억"
            )

        if value >= 10_000:

            return (
                f"{value / 10_000:.1f}"
                "만"
            )

        return f"{value:,.0f}"

    except Exception:
        return "-"


def fmt_change(
    value
):

    if value is None:
        return (
            '<span class="zero">-</span>'
        )

    try:

        value = float(value)

        if value > 0:

            return (
                '<span class="up">'
                f"▲ +{value:.2f}%"
                "</span>"
            )

        if value < 0:

            return (
                '<span class="down">'
                f"▼ {value:.2f}%"
                "</span>"
            )

        return (
            '<span class="zero">'
            "0.00%"
            "</span>"
        )

    except Exception:

        return (
            '<span class="zero">-</span>'
        )


# =========================================================
# 현재 캔들패턴 HTML
# =========================================================

def current_pattern_html(
    pattern
):

    if pattern == "상승장악형":

        return (
            '<span class="pattern bullish">'
            '▲ 상승장악형'
            '</span>'
        )

    if pattern == "하락장악형":

        return (
            '<span class="pattern bearish">'
            '▼ 하락장악형'
            '</span>'
        )

    if pattern == "양수도지":

        return (
            '<span class="pattern bullish">'
            '● 양수도지'
            '</span>'
        )

    if pattern == "음수도지":

        return (
            '<span class="pattern bearish">'
            '● 음수도지'
            '</span>'
        )

    if pattern == "상승관통형":

        return (
            '<span class="pattern bullish">'
            '▲ 상승관통형'
            '</span>'
        )

    if pattern == "하락관통형":

        return (
            '<span class="pattern bearish">'
            '▼ 하락관통형'
            '</span>'
        )

    return (
        '<span class="pattern none">'
        '-'
        '</span>'
    )


# =========================================================
# BTC 시황
# =========================================================

def btc_html():

    if latest_btc_okx_price is None:

        price = "-"
        change = "-"

    else:

        price = fmt_price(
            latest_btc_okx_price
        )

        change = fmt_change(
            latest_btc_daily_change
        )

    return f"""
    <section class="btc-section">

        <div class="section-title">
            <span>₿ BTC 시황</span>
        </div>

        <div class="btc-card">

            <div class="btc-name">
                <span class="btc-icon">₿</span>
                <b>BTC</b>
            </div>

            <div class="btc-price">
                <span>현재가</span>
                <strong>{price}</strong>
            </div>

            <div class="btc-change">
                <span>당일 변동률</span>
                <strong>{change}</strong>
            </div>

        </div>

    </section>
    """


# =========================================================
# SIGNAL 카드
# =========================================================

def signal_card(
    row
):

    name = html.escape(
        str(row.get("name", "-"))
    )

    volume = fmt_vol(
        row.get("volume_24h")
    )

    change = fmt_change(
        row.get("daily_change")
    )

    pattern = current_pattern_html(
        row.get("current_pattern")
    )

    return f"""
    <div class="signal-card">

        <div class="signal-coin">
            <span class="signal-rank">
                #{row.get("rank", "-")}
            </span>
            <b>{name}</b>
        </div>

        <div class="signal-volume">
            <span>거래대금</span>
            <strong>{volume}</strong>
        </div>

        <div class="signal-change">
            <span>변동률</span>
            <strong>{change}</strong>
        </div>

        <div class="signal-pattern">
            <span>캔들패턴</span>
            <strong>{pattern}</strong>
        </div>

    </div>
    """


# =========================================================
# SIGNAL 영역
# =========================================================

def signal_section():

    if not latest_signal_data:

        body = """
        <div class="empty-signal">
            현재 EMA20 < EMA60 조건에 해당하는 종목 없음
        </div>
        """

    else:

        body = "".join(
            signal_card(row)
            for row in latest_signal_data
        )

    return f"""
    <section class="signal-section">

        <div class="section-title">
            <span>🔴 SIGNAL</span>
        </div>

        <div class="signal-condition">
            <span>4시간봉</span>
            <b>EMA20 &lt; EMA60</b>
        </div>

        <div class="signal-header">

            <div>종목</div>
            <div>거래대금</div>
            <div>변동률</div>
            <div>캔들패턴</div>

        </div>

        <div class="signal-list">
            {body}
        </div>

    </section>
    """


# =========================================================
# TOP10 카드
# =========================================================

def card(
    row,
    kind="upbit"
):

    name = html.escape(
        str(row.get("name", "-"))
    )

    price = fmt_price(
        row.get("current_price")
    )

    volume = fmt_vol(
        row.get("volume_24h")
    )

    change = fmt_change(
        row.get("daily_change")
    )

    pattern = current_pattern_html(
        row.get("current_pattern")
    )

    return f"""
    <div class="coin-card">

        <div class="coin-header">

            <div class="coin-title">
                <span class="rank">
                    #{row.get("rank", "-")}
                </span>

                <b>{name}</b>
            </div>

        </div>

        <div class="market-summary">

            <div class="summary-item">

                <span>현재가</span>

                <strong>
                    {price}
                </strong>

            </div>

            <div class="summary-item">

                <span>24H 거래대금</span>

                <strong>
                    {volume}
                </strong>

            </div>

            <div class="summary-item">

                <span>당일 변동률</span>

                <strong>
                    {change}
                </strong>

            </div>

            <div class="summary-item">

                <span>현재 캔들패턴</span>

                <strong>
                    {pattern}
                </strong>

            </div>

        </div>

    </div>
    """


# =========================================================
# TOP10
# =========================================================

def top_list_html():

    if not latest_upbit_data:

        return """
        <div class="empty">
            데이터 없음
        </div>
        """

    cards = []

    for row in latest_upbit_data:

        cards.append(
            card(
                row,
                "upbit"
            )
        )

    return "".join(cards)


# =========================================================
# Dashboard
# =========================================================

def dashboard():

    updated = (
        latest_upbit_update_time
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
            content="{UPDATE_MINUTES * 60}"
        >

        <title>Crypto Dashboard</title>

        <style>

            * {{
                box-sizing: border-box;
            }}

            body {{

                margin: 0;

                background: #0d1117;

                color: #e6edf3;

                font-family:
                    Arial,
                    "Noto Sans KR",
                    sans-serif;

                padding: 18px;

            }}

            .container {{

                max-width: 1200px;

                margin: 0 auto;

            }}

            .top-title {{

                font-size: 24px;

                font-weight: 800;

                margin-bottom: 6px;

            }}

            .update-time {{

                color: #7d8590;

                font-size: 12px;

                margin-bottom: 20px;

            }}

            .section-title {{

                font-size: 19px;

                font-weight: 800;

                margin-bottom: 12px;

                padding-left: 3px;

            }}

            /* =========================================
               BTC
               ========================================= */

            .btc-section {{

                margin-bottom: 22px;

            }}

            .btc-card {{

                display: grid;

                grid-template-columns:
                    1fr 1fr 1fr;

                gap: 10px;

                background: #161b22;

                border:
                    1px solid #30363d;

                border-radius: 14px;

                padding: 16px;

            }}

            .btc-name,
            .btc-price,
            .btc-change {{

                display: flex;

                flex-direction: column;

                justify-content: center;

                gap: 6px;

            }}

            .btc-name b {{

                font-size: 20px;

            }}

            .btc-icon {{

                font-size: 26px;

            }}

            .btc-card span,
            .summary-item span,
            .signal-card span {{

                color: #8b949e;

                font-size: 12px;

            }}

            .btc-price strong {{

                font-size: 18px;

            }}

            .btc-change strong {{

                font-size: 18px;

            }}

            /* =========================================
               SIGNAL
               ========================================= */

            .signal-section {{

                margin-bottom: 28px;

            }}

            .signal-condition {{

                display: flex;

                align-items: center;

                gap: 10px;

                background: #161b22;

                border:
                    1px solid #30363d;

                border-radius: 10px;

                padding: 10px 13px;

                margin-bottom: 8px;

                font-size: 13px;

            }}

            .signal-condition span {{

                color: #8b949e;

            }}

            .signal-condition b {{

                color: #ff5966;

            }}

            .signal-header,
            .signal-card {{

                display: grid;

                grid-template-columns:
                    1.2fr
                    1fr
                    0.9fr
                    1.5fr;

                gap: 10px;

                align-items: center;

            }}

            .signal-header {{

                padding: 8px 14px;

                color: #8b949e;

                font-size: 11px;

                font-weight: 700;

            }}

            .signal-card {{

                background: #161b22;

                border:
                    1px solid #30363d;

                border-radius: 12px;

                padding: 13px 14px;

                margin-bottom: 7px;

            }}

            .signal-coin,
            .signal-volume,
            .signal-change,
            .signal-pattern {{

                display: flex;

                flex-direction: column;

                gap: 5px;

                min-width: 0;

            }}

            .signal-coin b {{

                font-size: 15px;

                overflow: hidden;

                text-overflow: ellipsis;

            }}

            .signal-rank {{

                color: #8b949e;

                font-size: 11px !important;

            }}

            .signal-volume strong,
            .signal-change strong,
            .signal-pattern strong {{

                font-size: 14px;

            }}

            .empty-signal {{

                background: #161b22;

                border:
                    1px solid #30363d;

                border-radius: 12px;

                padding: 18px;

                text-align: center;

                color: #8b949e;

            }}

            /* =========================================
               TOP10
               ========================================= */

            .top-section {{

                margin-top: 10px;

            }}

            .top-list {{

                display: grid;

                grid-template-columns:
                    repeat(2, minmax(0, 1fr));

                gap: 12px;

            }}

            .coin-card {{

                background: #161b22;

                border:
                    1px solid #30363d;

                border-radius: 14px;

                overflow: hidden;

            }}

            .coin-header {{

                padding: 13px 15px;

                border-bottom:
                    1px solid #30363d;

            }}

            .coin-title {{

                display: flex;

                align-items: center;

                gap: 8px;

            }}

            .coin-title b {{

                font-size: 16px;

            }}

            .rank {{

                color: #8b949e;

                font-size: 12px;

            }}

            .market-summary {{

                display: grid;

                grid-template-columns:
                    repeat(4, minmax(0, 1fr));

                gap: 0;

            }}

            .summary-item {{

                min-width: 0;

                padding: 13px 12px;

                border-right:
                    1px solid #30363d;

                display: flex;

                flex-direction: column;

                gap: 6px;

            }}

            .summary-item:last-child {{

                border-right: none;

            }}

            .summary-item strong {{

                font-size: 13px;

                word-break: break-word;

            }}

            /* =========================================
               Colors
               ========================================= */

            .up {{

                color: #38d878 !important;

            }}

            .down {{

                color: #ff5966 !important;

            }}

            .zero {{

                color: #68737e !important;

            }}

            .pattern.bullish {{

                color: #38d878 !important;

            }}

            .pattern.bearish {{

                color: #ff5966 !important;

            }}

            .pattern.none {{

                color: #68737e !important;

            }}

            .empty {{

                background: #161b22;

                border:
                    1px solid #30363d;

                border-radius: 12px;

                padding: 30px;

                text-align: center;

                color: #8b949e;

            }}

            /* =========================================
               Mobile
               ========================================= */

            @media (
                max-width: 700px
            ) {{

                body {{

                    padding: 10px;

                }}

                .btc-card {{

                    grid-template-columns:
                        1fr 1fr;

                }}

                .btc-name {{

                    grid-column:
                        1 / -1;

                }}

                .signal-header,
                .signal-card {{

                    grid-template-columns:
                        1.1fr
                        1fr
                        0.9fr
                        1.3fr;

                    gap: 6px;

                }}

                .signal-card {{

                    padding: 11px 9px;

                }}

                .signal-header {{

                    padding-left: 9px;

                    padding-right: 9px;

                }}

                .top-list {{

                    grid-template-columns:
                        1fr;

                }}

                .market-summary {{

                    grid-template-columns:
                        repeat(2, minmax(0, 1fr));

                }}

                .summary-item {{

                    border-right:
                        1px solid #30363d;

                    border-bottom:
                        1px solid #30363d;

                }}

                .summary-item:nth-child(2n) {{

                    border-right: none;

                }}

                .summary-item:nth-last-child(-n+2) {{

                    border-bottom: none;

                }}

            }}

        </style>

    </head>

    <body>

        <div class="container">

            <div class="top-title">
                📊 Crypto Dashboard
            </div>

            <div class="update-time">
                마지막 업데이트 :
                {updated}
            </div>

            {btc_html()}

            {signal_section()}

            <section class="top-section">

                <div class="section-title">
                    🔥 UPBIT TOP {TOP_N}
                </div>

                <div class="top-list">

                    {top_list_html()}

                </div>

            </section>

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


@app.get(
    "/api/data"
)
def api_data():

    return {
        "upbit": latest_upbit_data,
        "signal": latest_signal_data,
        "btc": {
            "price": latest_btc_okx_price,
            "daily_change":
                latest_btc_daily_change
        },
        "update_time":
            latest_upbit_update_time
    }


# =========================================================
# 백그라운드 업데이트
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

            log.exception(
                "scheduler error: %s",
                e
            )

        time.sleep(1)


def initial_update():

    try:

        update_dashboard()

    except Exception as e:

        log.exception(
            "initial update error: %s",
            e
        )


# =========================================================
# 실행
# =========================================================

if __name__ == "__main__":

    update_thread = threading.Thread(
        target=initial_update,
        daemon=True
    )

    update_thread.start()

    scheduler_thread = threading.Thread(
        target=scheduler_loop,
        daemon=True
    )

    scheduler_thread.start()

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )
