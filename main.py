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
    format="%(asctime)s [%(levelname)s] %(message)s"
)

KST = ZoneInfo("Asia/Seoul")


# =========================================================
# 설정
# =========================================================

TOP_N = 30
SHOW_TOP_LIST = "N"

UPDATE_MINUTES = 1

USE_UPBIT = "Y"
USE_OKX = "N"

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10

OKX_RETRY_DELAY = 2
OKX_MAX_RETRY_ROUNDS = 3

HISTORY_CHUNK = 200
MAX_HISTORY_CHUNKS = 10

# =========================================================
# SIGNAL 설정
# =========================================================

# SIGNAL은 반드시 이 두 가지 패턴만 사용
SIGNAL_CANDLE_PATTERNS = [
    "상승장악",
    "관통형"
]

SIGNAL_TIMEFRAME = "4h"


# =========================================================
# API
# =========================================================

UPBIT_MARKETS_URL = "https://api.upbit.com/v1/market/all"
UPBIT_TICKER_URL = "https://api.upbit.com/v1/ticker"
UPBIT_CANDLE_4H_URL = "https://api.upbit.com/v1/candles/minutes/240"
UPBIT_CANDLE_DAY_URL = "https://api.upbit.com/v1/candles/days"

OKX_CANDLE_URL = "https://www.okx.com/api/v5/market/candles"


# =========================================================
# 전역 상태
# =========================================================

dashboard_data = {
    "updated_at": "",
    "upbit": [],
    "btc": None,
    "error": ""
}

data_lock = threading.Lock()


# =========================================================
# HTTP GET
# =========================================================

def safe_get(
    url,
    params=None,
    headers=None,
    timeout=10,
    retries=MAX_RETRIES
):
    for attempt in range(retries):

        try:
            r = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=timeout
            )

            if r.status_code == 200:
                time.sleep(REQUEST_INTERVAL)
                return r.json()

            if r.status_code == 429:
                logging.warning(
                    "429 Rate Limit - %s초 대기 (%s/%s)",
                    RATE_LIMIT_WAIT,
                    attempt + 1,
                    retries
                )
                time.sleep(RATE_LIMIT_WAIT)
                continue

            logging.warning(
                "HTTP %s : %s",
                r.status_code,
                url
            )

        except Exception as e:
            logging.warning(
                "GET 오류 %s : %s",
                url,
                e
            )

        time.sleep(1)

    return None


# =========================================================
# 숫자 처리
# =========================================================

def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def fmt_price(v):
    if v is None:
        return "-"

    v = safe_float(v)

    if v >= 1000:
        return f"{v:,.0f}"

    if v >= 1:
        return f"{v:,.2f}"

    if v >= 0.01:
        return f"{v:,.4f}"

    return f"{v:,.8f}"


def fmt_change(v):
    if v is None:
        return "-"

    v = safe_float(v)

    if v > 0:
        return f"▲ {v:.2f}%"

    if v < 0:
        return f"▼ {abs(v):.2f}%"

    return "0.00%"


def fmt_volume(v):
    v = safe_float(v)

    if v >= 1_0000_0000_0000:
        return f"{v / 1_0000_0000_0000:.2f}조"

    if v >= 1_0000_0000:
        return f"{v / 1_0000_0000:.2f}억"

    if v >= 1_0000:
        return f"{v / 1_0000:.2f}만"

    return f"{v:,.0f}"


# =========================================================
# Upbit 마켓
# =========================================================

def get_upbit_markets():

    data = safe_get(
        UPBIT_MARKETS_URL,
        params={
            "isDetails": "false"
        }
    )

    if not data:
        return []

    markets = []

    for x in data:

        market = x.get("market", "")

        if not market.startswith("KRW-"):
            continue

        markets.append({
            "market": market,
            "symbol": market.replace("KRW-", ""),
            "korean_name": x.get("korean_name", ""),
            "english_name": x.get("english_name", "")
        })

    return markets


# =========================================================
# Upbit Ticker
# =========================================================

def get_upbit_tickers(markets):

    if not markets:
        return {}

    result = {}

    # Upbit API 요청은 여러 종목을 묶어서 처리
    chunk_size = 100

    for i in range(0, len(markets), chunk_size):

        chunk = markets[i:i + chunk_size]

        market_codes = ",".join(
            x["market"] for x in chunk
        )

        data = safe_get(
            UPBIT_TICKER_URL,
            params={
                "markets": market_codes
            }
        )

        if not data:
            continue

        for x in data:

            result[x["market"]] = x

    return result


# =========================================================
# TOP 거래대금
# =========================================================

def get_top_upbit_markets():

    markets = get_upbit_markets()

    if not markets:
        return []

    tickers = get_upbit_tickers(markets)

    rows = []

    for m in markets:

        market = m["market"]

        t = tickers.get(market)

        if not t:
            continue

        acc_trade_price_24h = safe_float(
            t.get("acc_trade_price_24h")
        )

        rows.append({
            **m,
            "trade_price": safe_float(
                t.get("trade_price")
            ),
            "acc_trade_price_24h": acc_trade_price_24h,
            "signed_change_rate_24h": safe_float(
                t.get("signed_change_rate")
            ) * 100
        })

    rows.sort(
        key=lambda x: x["acc_trade_price_24h"],
        reverse=True
    )

    return rows[:TOP_N]


# =========================================================
# 업비트 현재 09:00 기준 변동률
# =========================================================
#
# 중요:
#
# 업비트 일봉은 KST 09:00 기준으로 시작한다.
#
# 현재 진행 중인 일봉의 opening_price =
# 당일 KST 09:00 가격
#
# 따라서:
#
# (현재가 - 09:00 시가) / 09:00 시가 * 100
#
# 을 사용한다.
# =========================================================

def get_upbit_daily_09_change(market):

    data = safe_get(
        UPBIT_CANDLE_DAY_URL,
        params={
            "market": market,
            "count": 2
        }
    )

    if not data:
        return None, None, None

    # 가장 최신 일봉
    candle = data[0]

    opening_price = safe_float(
        candle.get("opening_price")
    )

    trade_price = safe_float(
        candle.get("trade_price")
    )

    candle_time = candle.get(
        "candle_date_time_kst",
        ""
    )

    if opening_price <= 0:
        return None, None, candle_time

    change = (
        (trade_price - opening_price)
        / opening_price
        * 100
    )

    return change, opening_price, candle_time


# =========================================================
# Upbit 4시간봉
# =========================================================

def get_upbit_4h_candles(
    market,
    count=100
):

    data = safe_get(
        UPBIT_CANDLE_4H_URL,
        params={
            "market": market,
            "count": count
        }
    )

    if not data:
        return pd.DataFrame()

    rows = []

    for x in data:

        rows.append({
            "time": x.get(
                "candle_date_time_kst"
            ),
            "open": safe_float(
                x.get("opening_price")
            ),
            "high": safe_float(
                x.get("high_price")
            ),
            "low": safe_float(
                x.get("low_price")
            ),
            "close": safe_float(
                x.get("trade_price")
            ),
            "volume": safe_float(
                x.get("candle_acc_trade_volume")
            ),
            "volume_price": safe_float(
                x.get("candle_acc_trade_price")
            )
        })

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df["time"] = pd.to_datetime(
        df["time"],
        errors="coerce"
    )

    df = df.dropna(
        subset=["time"]
    )

    df = df.sort_values(
        "time"
    ).reset_index(drop=True)

    return df


# =========================================================
# 현재 4시간봉 변동률
# =========================================================

def candle_change(row):

    o = safe_float(row["open"])
    c = safe_float(row["close"])

    if o <= 0:
        return 0.0

    return (
        (c - o) / o
    ) * 100


# =========================================================
# 캔들 기본 정보
# =========================================================

def candle_info(row):

    o = safe_float(row["open"])
    h = safe_float(row["high"])
    l = safe_float(row["low"])
    c = safe_float(row["close"])

    body = abs(c - o)

    bull = c > o
    bear = c < o

    upper = h - max(o, c)
    lower = min(o, c) - l

    return {
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "body": body,
        "bull": bull,
        "bear": bear,
        "upper": max(0, upper),
        "lower": max(0, lower)
    }


# =========================================================
# 상승장악
# =========================================================

def bullish_engulfing(
    previous,
    current
):

    p = candle_info(previous)
    c = candle_info(current)

    if not p["bear"]:
        return False

    if not c["bull"]:
        return False

    if c["open"] > p["close"]:
        return False

    if c["close"] < p["open"]:
        return False

    if c["body"] <= p["body"]:
        return False

    return True


# =========================================================
# 관통형
# =========================================================

def piercing_pattern(
    previous,
    current
):

    p = candle_info(previous)
    c = candle_info(current)

    # 이전 캔들은 하락봉
    if not p["bear"]:
        return False

    # 현재 캔들은 상승봉
    if not c["bull"]:
        return False

    # 현재 시가가 이전 종가보다 낮거나 같은 형태
    if c["open"] > p["close"]:
        return False

    # 현재 종가가 이전 몸통 중간값 이상
    midpoint = (
        p["open"] + p["close"]
    ) / 2

    if c["close"] < midpoint:
        return False

    # 이전 시가까지 완전히 돌파하면 장악형으로 보는 영역
    if c["close"] >= p["open"]:
        return False

    return True


# =========================================================
# 2캔들 패턴
# =========================================================

def two_patterns(
    previous,
    current
):

    result = []

    if bullish_engulfing(
        previous,
        current
    ):
        result.append("상승장악")

    if piercing_pattern(
        previous,
        current
    ):
        result.append("관통형")

    return result


# =========================================================
# 4시간봉 기간 생성
# =========================================================

def build_periods(df):

    if df is None or df.empty:
        return []

    periods = []

    for i in range(len(df)):

        row = df.iloc[i]

        change = candle_change(row)

        patterns = []

        if i >= 1:

            prev = df.iloc[i - 1]

            patterns = two_patterns(
                prev,
                row
            )

        periods.append({
            "time": row["time"],
            "open": safe_float(row["open"]),
            "high": safe_float(row["high"]),
            "low": safe_float(row["low"]),
            "close": safe_float(row["close"]),
            "change": change,
            "patterns": patterns
        })

    return periods


# =========================================================
# SIGNAL 1개 캔들 검사
# =========================================================

def period_signal(
    period,
    daily_09_change
):

    if period is None:
        return False

    # =====================================================
    # 가장 중요한 조건
    #
    # 업비트 KST 09:00 기준 변동률이
    # 0보다 작거나 같으면 SIGNAL 제외
    # =====================================================

    if daily_09_change is None:
        return False

    if daily_09_change <= 0:
        return False

    # 현재 4H 캔들이 상승봉이어야 함
    if period["close"] <= period["open"]:
        return False

    # SIGNAL 패턴
    for pattern in SIGNAL_CANDLE_PATTERNS:

        if pattern in period["patterns"]:
            return True

    return False


# =========================================================
# SIGNAL 상세
# =========================================================

def signal_details(
    periods,
    daily_09_change
):

    if not periods:
        return {
            "signal": False,
            "pattern": "",
            "time": ""
        }

    # -----------------------------------------------------
    # 09:00 기준 변동률이 마이너스 또는 0이면
    # 이전 4시간봉도 검사하지 않고 SIGNAL 제외
    # -----------------------------------------------------

    if daily_09_change is None:
        return {
            "signal": False,
            "pattern": "",
            "time": ""
        }

    if daily_09_change <= 0:
        return {
            "signal": False,
            "pattern": "",
            "time": ""
        }

    # -----------------------------------------------------
    # 최신 4H 캔들부터 검사
    # -----------------------------------------------------

    latest = periods[-1]

    if period_signal(
        latest,
        daily_09_change
    ):

        return {
            "signal": True,
            "pattern": ", ".join(
                latest["patterns"]
            ),
            "time": latest["time"]
        }

    # -----------------------------------------------------
    # 최신봉에서 없으면 바로 이전 4H 캔들 검사
    # -----------------------------------------------------

    if len(periods) >= 2:

        previous = periods[-2]

        if period_signal(
            previous,
            daily_09_change
        ):

            return {
                "signal": True,
                "pattern": ", ".join(
                    previous["patterns"]
                ),
                "time": previous["time"]
            }

    return {
        "signal": False,
        "pattern": "",
        "time": ""
    }


# =========================================================
# 4H 분석
# =========================================================

def analyze_4h(
    market,
    daily_09_change
):

    df = get_upbit_4h_candles(
        market,
        count=100
    )

    if df.empty:
        return {
            "periods": [],
            "signal": False,
            "signal_pattern": "",
            "signal_time": ""
        }

    periods = build_periods(df)

    sig = signal_details(
        periods,
        daily_09_change
    )

    return {
        "periods": periods,
        "signal": sig["signal"],
        "signal_pattern": sig["pattern"],
        "signal_time": sig["time"]
    }


# =========================================================
# 최근 6개 4H 셀
# =========================================================

def cells(
    periods,
    current_time=None
):

    if not periods:
        return "<div class='no-data'>데이터 없음</div>"

    recent = periods[-6:]

    output = []

    for p in recent:

        t = p["time"]

        if hasattr(t, "strftime"):
            time_text = t.strftime("%m/%d %H:%M")
        else:
            time_text = str(t)

        is_current = False

        if current_time is not None:

            try:
                is_current = (
                    t == current_time
                )
            except Exception:
                is_current = False

        pattern_text = ""

        if p["patterns"]:
            pattern_text = (
                "<div class='pattern'>"
                + html.escape(
                    ", ".join(p["patterns"])
                )
                + "</div>"
            )

        change = p["change"]

        if change > 0:
            change_class = "positive"
        elif change < 0:
            change_class = "negative"
        else:
            change_class = "neutral"

        current_class = (
            " current"
            if is_current
            else ""
        )

        output.append(
            f"""
            <div class="candle-cell{current_class}">
                <div class="candle-time">
                    {html.escape(time_text)}
                </div>

                <div class="candle-change {change_class}">
                    {fmt_change(change)}
                </div>

                {pattern_text}
            </div>
            """
        )

    return "".join(output)


# =========================================================
# OKX BTC 4H
# =========================================================

def get_okx_btc_4h():

    for attempt in range(
        OKX_MAX_RETRY_ROUNDS
    ):

        try:

            data = safe_get(
                OKX_CANDLE_URL,
                params={
                    "instId": "BTC-USDT-SWAP",
                    "bar": "4H",
                    "limit": "20"
                },
                retries=3
            )

            if not data:
                continue

            rows = data.get(
                "data",
                []
            )

            if not rows:
                continue

            parsed = []

            for x in rows:

                if len(x) < 5:
                    continue

                ts = safe_float(x[0])

                dt = datetime.fromtimestamp(
                    ts / 1000,
                    tz=ZoneInfo("UTC")
                ).astimezone(KST)

                parsed.append({
                    "time": dt,
                    "open": safe_float(x[1]),
                    "high": safe_float(x[2]),
                    "low": safe_float(x[3]),
                    "close": safe_float(x[4])
                })

            parsed.reverse()

            return parsed

        except Exception as e:

            logging.warning(
                "OKX BTC 오류: %s",
                e
            )

            time.sleep(
                OKX_RETRY_DELAY
            )

    return []


# =========================================================
# BTC HTML
# =========================================================

def btc_html():

    if not dashboard_data.get("btc"):
        return ""

    btc = dashboard_data["btc"]

    change = btc.get(
        "change",
        0
    )

    if change > 0:
        cls = "positive"
    elif change < 0:
        cls = "negative"
    else:
        cls = "neutral"

    return f"""
    <div class="btc-panel">

        <div class="btc-title">
            ₿ BTC / USDT-SWAP
            <span class="btc-tf">4H</span>
        </div>

        <div class="btc-main">

            <div>
                <div class="small-label">
                    현재가
                </div>

                <div class="btc-price">
                    ${fmt_price(btc.get("price"))}
                </div>
            </div>

            <div>
                <div class="small-label">
                    4H 변동률
                </div>

                <div class="btc-change {cls}">
                    {fmt_change(change)}
                </div>
            </div>

        </div>

    </div>
    """


# =========================================================
# 종목 카드
# =========================================================

def card(row):

    market = row["market"]
    symbol = row["symbol"]
    korean_name = row["korean_name"]

    daily_change = row.get(
        "daily_09_change"
    )

    signal = row.get(
        "signal",
        False
    )

    signal_pattern = row.get(
        "signal_pattern",
        ""
    )

    periods = row.get(
        "periods",
        []
    )

    # -----------------------------------------------------
    # 09:00 기준 변동률 색상
    # -----------------------------------------------------

    if daily_change is not None:

        if daily_change > 0:
            daily_cls = "positive"

        elif daily_change < 0:
            daily_cls = "negative"

        else:
            daily_cls = "neutral"

    else:
        daily_cls = "neutral"

    # -----------------------------------------------------
    # SIGNAL
    # -----------------------------------------------------

    signal_html = ""

    if signal:

        signal_html = f"""
        <div class="signal-badge">
            ⭐ SIGNAL
        </div>

        <div class="signal-pattern">
            {html.escape(signal_pattern)}
        </div>
        """

    else:

        signal_html = """
        <div class="no-signal">
            -
        </div>
        """

    # -----------------------------------------------------
    # 4H 셀
    # -----------------------------------------------------

    candle_cells = cells(
        periods
    )

    return f"""
    <div class="coin-card">

        <div class="coin-header">

            <div class="coin-name">

                <span class="symbol">
                    {html.escape(symbol)}
                </span>

                <span class="korean-name">
                    {html.escape(korean_name)}
                </span>

            </div>

            <div class="market-code">
                {html.escape(market)}
            </div>

        </div>


        <div class="summary-row">

            <div class="price-box">

                <div class="label">
                    현재가
                </div>

                <div class="price">
                    {fmt_price(row.get("trade_price"))}
                </div>

            </div>


            <div class="change-box">

                <div class="label">
                    KST 09:00 기준
                </div>

                <div class="daily-change {daily_cls}">
                    {fmt_change(daily_change)}
                </div>

            </div>


            <div class="volume-box">

                <div class="label">
                    24H 거래대금
                </div>

                <div class="volume">
                    {fmt_volume(
                        row.get(
                            "acc_trade_price_24h",
                            0
                        )
                    )}
                </div>

            </div>


            <div class="signal-box">

                {signal_html}

            </div>

        </div>


        <div class="four-hour-title">
            4시간봉
        </div>


        <div class="candle-grid">

            {candle_cells}

        </div>

    </div>
    """


# =========================================================
# Upbit 업데이트
# =========================================================

def update_upbit():

    try:

        top_markets = get_top_upbit_markets()

        if not top_markets:

            logging.warning(
                "Upbit TOP 종목을 가져오지 못했습니다."
            )

            return

        result = []

        for row in top_markets:

            market = row["market"]

            # -------------------------------------------------
            # 중요:
            #
            # 변동률은 4H 변화율이 아니라
            # KST 09:00 일봉 시가 기준
            # -------------------------------------------------

            daily_09_change, daily_open, daily_time = (
                get_upbit_daily_09_change(
                    market
                )
            )

            row["daily_09_change"] = (
                daily_09_change
            )

            row["daily_09_open"] = (
                daily_open
            )

            row["daily_09_time"] = (
                daily_time
            )

            # -------------------------------------------------
            # 4H 분석
            # -------------------------------------------------

            analysis = analyze_4h(
                market,
                daily_09_change
            )

            row["periods"] = analysis[
                "periods"
            ]

            row["signal"] = analysis[
                "signal"
            ]

            row["signal_pattern"] = analysis[
                "signal_pattern"
            ]

            row["signal_time"] = analysis[
                "signal_time"
            ]

            result.append(row)

        with data_lock:

            dashboard_data["upbit"] = result

            dashboard_data["updated_at"] = (
                datetime.now(KST).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            )

            dashboard_data["error"] = ""

        logging.info(
            "Upbit 업데이트 완료 : %s개",
            len(result)
        )

    except Exception as e:

        logging.exception(
            "Upbit 업데이트 오류"
        )

        with data_lock:
            dashboard_data["error"] = str(e)


# =========================================================
# BTC 업데이트
# =========================================================

def update_okx_btc():

    try:

        candles = get_okx_btc_4h()

        if not candles:
            return

        current = candles[-1]

        o = safe_float(
            current["open"]
        )

        c = safe_float(
            current["close"]
        )

        if o > 0:

            change = (
                (c - o)
                / o
                * 100
            )

        else:
            change = 0

        with data_lock:

            dashboard_data["btc"] = {
                "price": c,
                "change": change,
                "time": current["time"]
            }

    except Exception as e:

        logging.exception(
            "BTC 업데이트 오류"
        )


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    logging.info(
        "========== 대시보드 업데이트 시작 =========="
    )

    if USE_UPBIT == "Y":
        update_upbit()

    if USE_OKX == "Y":
        update_okx_btc()
    else:
        # BTC 패널은 기존 구조대로 OKX에서 가져옴
        update_okx_btc()

    logging.info(
        "========== 대시보드 업데이트 완료 =========="
    )


# =========================================================
# SIGNAL 섹션
# =========================================================

def signal_section():

    rows = dashboard_data.get(
        "upbit",
        []
    )

    signals = [
        x for x in rows
        if x.get("signal")
    ]

    if not signals:

        return """
        <div class="section empty-section">

            <div class="section-title">
                ⭐ 4H SIGNAL
            </div>

            <div class="empty-text">
                현재 조건을 만족하는 SIGNAL 없음
            </div>

        </div>
        """

    content = ""

    for row in signals:

        daily_change = row.get(
            "daily_09_change"
        )

        content += f"""
        <div class="signal-row">

            <div class="signal-coin">
                <b>
                    {html.escape(
                        row["symbol"]
                    )}
                </b>

                <span>
                    {html.escape(
                        row["korean_name"]
                    )}
                </span>
            </div>

            <div class="signal-change">
                KST 09:00
                <b class="positive">
                    {fmt_change(
                        daily_change
                    )}
                </b>
            </div>

            <div class="signal-pattern-name">
                ⭐
                {html.escape(
                    row.get(
                        "signal_pattern",
                        ""
                    )
                )}
            </div>

        </div>
        """

    return f"""
    <div class="section">

        <div class="section-title">
            ⭐ 4H SIGNAL
            <span class="section-sub">
                상승장악 · 관통형
            </span>
        </div>

        {content}

    </div>
    """


# =========================================================
# TOP LIST
# =========================================================

def top_list_section():

    rows = dashboard_data.get(
        "upbit",
        []
    )

    if not rows:
        return ""

    content = ""

    for idx, row in enumerate(
        rows,
        start=1
    ):

        daily_change = row.get(
            "daily_09_change"
        )

        if daily_change is not None:

            if daily_change > 0:
                cls = "positive"

            elif daily_change < 0:
                cls = "negative"

            else:
                cls = "neutral"

        else:
            cls = "neutral"

        signal = (
            "⭐"
            if row.get("signal")
            else ""
        )

        content += f"""
        <tr>

            <td>
                {idx}
            </td>

            <td>
                <b>
                    {html.escape(
                        row["symbol"]
                    )}
                </b>
            </td>

            <td>
                {html.escape(
                    row["korean_name"]
                )}
            </td>

            <td>
                {fmt_price(
                    row.get(
                        "trade_price"
                    )
                )}
            </td>

            <td class="{cls}">
                {fmt_change(
                    daily_change
                )}
            </td>

            <td>
                {fmt_volume(
                    row.get(
                        "acc_trade_price_24h",
                        0
                    )
                )}
            </td>

            <td>
                {signal}
            </td>

        </tr>
        """

    return f"""
    <div class="section">

        <div class="section-title">
            📊 UPBIT TOP {TOP_N}
            <span class="section-sub">
                거래대금 순
            </span>
        </div>

        <div class="table-wrap">

            <table>

                <thead>

                    <tr>
                        <th>순위</th>
                        <th>코인</th>
                        <th>이름</th>
                        <th>현재가</th>
                        <th>09:00 변동률</th>
                        <th>24H 거래대금</th>
                        <th>SIGNAL</th>
                    </tr>

                </thead>

                <tbody>
                    {content}
                </tbody>

            </table>

        </div>

    </div>
    """


# =========================================================
# 전체 대시보드
# =========================================================

def dashboard():

    with data_lock:

        updated_at = dashboard_data.get(
            "updated_at",
            ""
        )

        btc = dashboard_data.get(
            "btc"
        )

        rows = list(
            dashboard_data.get(
                "upbit",
                []
            )
        )

        error = dashboard_data.get(
            "error",
            ""
        )

    # -----------------------------------------------------
    # BTC
    # -----------------------------------------------------

    btc_content = ""

    if btc:
        btc_content = btc_html()

    # -----------------------------------------------------
    # SIGNAL
    # -----------------------------------------------------

    signal_content = signal_section()

    # -----------------------------------------------------
    # TOP
    # -----------------------------------------------------

    top_content = ""

    if SHOW_TOP_LIST == "Y":
        top_content = top_list_section()

    error_html = ""

    if error:

        error_html = f"""
        <div class="error-box">
            {html.escape(error)}
        </div>
        """

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
            UPBIT 4H SIGNAL DASHBOARD
        </title>


        <style>

            * {{
                box-sizing: border-box;
            }}

            body {{
                margin: 0;
                padding: 20px;

                background:
                    #0b0f14;

                color:
                    #e8edf3;

                font-family:
                    Arial,
                    "Noto Sans KR",
                    sans-serif;
            }}


            .container {{
                max-width: 1500px;
                margin: 0 auto;
            }}


            .top-header {{
                display: flex;
                justify-content: space-between;
                align-items: center;

                margin-bottom: 20px;

                padding: 18px;

                background:
                    #121821;

                border:
                    1px solid #26313d;

                border-radius:
                    12px;
            }}


            .title {{
                font-size: 24px;
                font-weight: 800;
            }}


            .subtitle {{
                margin-top: 6px;
                color: #8d9aaa;
                font-size: 13px;
            }}


            .update-time {{
                color: #9aa7b5;
                font-size: 13px;
            }}


            .section {{
                margin-bottom: 20px;

                background:
                    #121821;

                border:
                    1px solid #26313d;

                border-radius:
                    12px;

                overflow: hidden;
            }}


            .section-title {{
                padding: 15px 18px;

                font-size: 18px;
                font-weight: 800;

                border-bottom:
                    1px solid #26313d;
            }}


            .section-sub {{
                margin-left: 10px;

                color:
                    #7e8b9a;

                font-size: 12px;
                font-weight: normal;
            }}


            .empty-section {{
                padding-bottom: 20px;
            }}


            .empty-text {{
                padding: 20px;

                color:
                    #788595;

                text-align: center;
            }}


            .btc-panel {{
                margin-bottom: 20px;

                padding: 20px;

                background:
                    linear-gradient(
                        135deg,
                        #151d27,
                        #10161e
                    );

                border:
                    1px solid #34404d;

                border-radius:
                    12px;
            }}


            .btc-title {{
                font-size: 19px;
                font-weight: 800;
                margin-bottom: 18px;
            }}


            .btc-tf {{
                margin-left: 8px;

                padding: 4px 8px;

                background:
                    #202b37;

                border-radius:
                    6px;

                font-size: 11px;

                color:
                    #aeb9c5;
            }}


            .btc-main {{
                display: flex;
                gap: 50px;
                align-items: center;
            }}


            .small-label,
            .label {{
                margin-bottom: 5px;

                color:
                    #7f8c9b;

                font-size: 11px;
            }}


            .btc-price {{
                font-size: 28px;
                font-weight: 800;
            }}


            .btc-change {{
                font-size: 22px;
                font-weight: 800;
            }}


            .positive {{
                color:
                    #2ddc88 !important;
            }}


            .negative {{
                color:
                    #ff5c6c !important;
            }}


            .neutral {{
                color:
                    #9ba6b3 !important;
            }}


            .signal-row {{
                display: grid;

                grid-template-columns:
                    1.2fr
                    1fr
                    1fr;

                gap: 10px;

                padding: 13px 18px;

                border-bottom:
                    1px solid #202934;
            }}


            .signal-row:last-child {{
                border-bottom: none;
            }}


            .signal-coin b {{
                color:
                    #ffffff;
            }}


            .signal-coin span {{
                margin-left: 8px;

                color:
                    #7e8a98;

                font-size: 12px;
            }}


            .signal-pattern-name {{
                color:
                    #ffd45a;

                font-weight:
                    700;
            }}


            .coin-card {{
                margin: 15px;

                padding: 16px;

                background:
                    #0e141b;

                border:
                    1px solid #27323e;

                border-radius:
                    10px;
            }}


            .coin-header {{
                display: flex;
                justify-content: space-between;
                align-items: center;

                margin-bottom: 14px;
            }}


            .coin-name {{
                display: flex;
                align-items: baseline;
                gap: 8px;
            }}


            .symbol {{
                font-size: 19px;
                font-weight: 800;
            }}


            .korean-name {{
                color:
                    #7e8b99;

                font-size: 12px;
            }}


            .market-code {{
                color:
                    #53606e;

                font-size: 11px;
            }}


            .summary-row {{
                display: grid;

                grid-template-columns:
                    1fr
                    1fr
                    1fr
                    1fr;

                gap: 10px;

                margin-bottom: 15px;
            }}


            .price-box,
            .change-box,
            .volume-box,
            .signal-box {{
                padding: 12px;

                background:
                    #141c25;

                border:
                    1px solid #202b36;

                border-radius:
                    8px;
            }}


            .price {{
                font-size: 18px;
                font-weight: 800;
            }}


            .daily-change {{
                font-size: 17px;
                font-weight: 800;
            }}


            .volume {{
                font-size: 16px;
                font-weight: 700;
            }}


            .signal-badge {{
                color:
                    #ffd75a;

                font-size: 17px;
                font-weight: 900;
            }}


            .signal-pattern {{
                margin-top: 4px;

                color:
                    #d6b84b;

                font-size: 11px;
            }}


            .no-signal {{
                color:
                    #4e5a67;

                font-size: 18px;
            }}


            .four-hour-title {{
                margin-bottom: 8px;

                color:
                    #8794a3;

                font-size: 12px;
                font-weight: 700;
            }}


            .candle-grid {{
                display: grid;

                grid-template-columns:
                    repeat(6, 1fr);

                gap: 6px;
            }}


            .candle-cell {{
                min-height: 70px;

                padding: 8px;

                background:
                    #131a22;

                border:
                    1px solid #222d38;

                border-radius:
                    7px;
            }}


            .candle-cell.current {{
                border:
                    1px solid #63788c;

                background:
                    #18222d;
            }}


            .candle-time {{
                color:
                    #788593;

                font-size: 10px;

                margin-bottom: 6px;
            }}


            .candle-change {{
                font-size: 13px;
                font-weight: 800;
            }}


            .pattern {{
                margin-top: 5px;

                color:
                    #ffd45a;

                font-size: 10px;
                line-height: 1.3;
            }}


            .table-wrap {{
                overflow-x:
                    auto;
            }}


            table {{
                width: 100%;

                border-collapse:
                    collapse;
            }}


            th {{
                padding: 11px;

                background:
                    #0e141b;

                color:
                    #718091;

                font-size: 11px;

                white-space:
                    nowrap;
            }}


            td {{
                padding: 11px;

                border-top:
                    1px solid #202934;

                text-align: center;

                font-size: 12px;
            }}


            .error-box {{
                margin-bottom: 20px;

                padding: 15px;

                background:
                    #28171b;

                border:
                    1px solid #60313a;

                color:
                    #ff7d8a;

                border-radius:
                    8px;
            }}


            @media (
                max-width: 900px
            ) {{

                body {{
                    padding: 10px;
                }}

                .summary-row {{
                    grid-template-columns:
                        1fr 1fr;
                }}

                .candle-grid {{
                    grid-template-columns:
                        repeat(3, 1fr);
                }}

                .signal-row {{
                    grid-template-columns:
                        1fr;
                    gap: 5px;
                }}

                .btc-main {{
                    gap: 25px;
                }}
            }}


            @media (
                max-width: 550px
            ) {{

                .title {{
                    font-size: 18px;
                }}

                .top-header {{
                    display: block;
                }}

                .update-time {{
                    margin-top: 8px;
                }}

                .candle-grid {{
                    grid-template-columns:
                        repeat(2, 1fr);
                }}

            }}

        </style>

    </head>


    <body>

        <div class="container">

            <div class="top-header">

                <div>

                    <div class="title">
                        📊 UPBIT 4H SIGNAL
                    </div>

                    <div class="subtitle">
                        KST 09:00 기준 변동률
                        · 4시간봉 패턴
                        · 상승장악 / 관통형
                    </div>

                </div>

                <div class="update-time">
                    마지막 업데이트<br>
                    {html.escape(
                        updated_at
                    )}
                </div>

            </div>


            {error_html}


            {btc_content}


            {signal_content}


            {top_content}


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
    "/health"
)
def health():

    return {
        "status": "ok",
        "updated_at":
            dashboard_data.get(
                "updated_at"
            )
    }


# =========================================================
# 백그라운드 스케줄러
# =========================================================

def scheduler_loop():

    # 시작하자마자 1회 실행
    try:
        update_dashboard()
    except Exception:
        logging.exception(
            "초기 업데이트 오류"
        )

    schedule.every(
        UPDATE_MINUTES
    ).minutes.do(
        update_dashboard
    )

    while True:

        try:
            schedule.run_pending()

        except Exception:
            logging.exception(
                "스케줄러 오류"
            )

        time.sleep(1)


# =========================================================
# 실행
# =========================================================

if __name__ == "__main__":

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
