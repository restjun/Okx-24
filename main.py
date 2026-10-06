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
    format="%(asctime)s | %(levelname)s | %(message)s"
)

KST = ZoneInfo("Asia/Seoul")

UPBIT_API = "https://api.upbit.com/v1"
OKX_API = "https://www.okx.com/api/v5"

TOP_N = 20
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

ROC_PERIOD = 50
ROC_SIGNAL_LEVEL = 0.0


# =========================================================
# 전역 데이터
# =========================================================

latest_rows = []

latest_btc_okx_price = None
latest_btc_daily_periods = []
latest_btc_daily_change = None
latest_btc_daily_roc = None

last_update_time = None


# =========================================================
# 공통
# =========================================================

session = requests.Session()


def now_kst():
    return datetime.now(KST)


def safe_float(value, default=None):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def request_get(url, params=None, headers=None, timeout=10):
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(
                url,
                params=params,
                headers=headers,
                timeout=timeout
            )

            if response.status_code == 200:
                return response

            if response.status_code == 429:
                logging.warning(
                    "429 Rate Limit | %s초 대기",
                    RATE_LIMIT_WAIT
                )
                time.sleep(RATE_LIMIT_WAIT)
                continue

            logging.warning(
                "HTTP %s | %s",
                response.status_code,
                url
            )

        except Exception as e:
            logging.warning(
                "REQUEST ERROR | %s | attempt=%s",
                e,
                attempt + 1
            )

        time.sleep(REQUEST_INTERVAL)

    return None


# =========================================================
# Upbit 마켓
# =========================================================

def get_upbit_markets():

    response = request_get(
        f"{UPBIT_API}/market/all",
        params={
            "isDetails": "false"
        }
    )

    if response is None:
        return []

    try:
        data = response.json()

        return [
            x["market"]
            for x in data
            if x["market"].startswith("KRW-")
        ]

    except Exception as e:
        logging.error(
            "UPBIT MARKET ERROR | %s",
            e
        )
        return []


# =========================================================
# Upbit 현재가
# =========================================================

def get_upbit_tickers(markets):

    if not markets:
        return []

    result = []

    chunk_size = 100

    for i in range(0, len(markets), chunk_size):

        chunk = markets[i:i + chunk_size]

        response = request_get(
            f"{UPBIT_API}/ticker",
            params={
                "markets": ",".join(chunk)
            }
        )

        if response is None:
            continue

        try:
            result.extend(response.json())
        except Exception:
            pass

        time.sleep(REQUEST_INTERVAL)

    return result


# =========================================================
# Upbit 일봉
# =========================================================

def get_upbit_daily(market, count=10):

    response = request_get(
        f"{UPBIT_API}/candles/days",
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

        df["start"] = pd.to_datetime(
            df["candle_date_time_kst"]
        )

        df["open"] = df["opening_price"].astype(float)
        df["high"] = df["high_price"].astype(float)
        df["low"] = df["low_price"].astype(float)
        df["close"] = df["trade_price"].astype(float)

        df = df[
            [
                "start",
                "open",
                "high",
                "low",
                "close"
            ]
        ]

        df = df.sort_values("start")
        df = df.drop_duplicates("start")

        return df.reset_index(drop=True)

    except Exception as e:
        logging.warning(
            "UPBIT DAILY ERROR | %s | %s",
            market,
            e
        )

        return pd.DataFrame()


# =========================================================
# Upbit 09시 기준 당일 변동률
# =========================================================

def get_upbit_daily_change(
    market,
    current_price=None
):

    df = get_upbit_daily(
        market,
        count=3
    )

    if df.empty:
        return None

    now = now_kst()

    today_start = now.replace(
        hour=9,
        minute=0,
        second=0,
        microsecond=0
    )

    if now < today_start:
        today_start -= timedelta(days=1)

    current_row = df[
        df["start"] == today_start
    ]

    if current_row.empty:
        current_row = df[
            df["start"] <= today_start
        ]

        if current_row.empty:
            return None

        current_row = current_row.iloc[-1:]
    else:
        current_row = current_row.iloc[-1:]

    open_price = safe_float(
        current_row.iloc[0]["open"]
    )

    if open_price is None or open_price == 0:
        return None

    if current_price is None:
        current_price = safe_float(
            current_row.iloc[0]["close"]
        )

    if current_price is None:
        return None

    return (
        (current_price - open_price)
        / open_price
    ) * 100


# =========================================================
# Upbit 4시간봉
# =========================================================

def get_upbit_4h(
    market,
    count=200
):

    response = request_get(
        f"{UPBIT_API}/candles/minutes/240",
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

        df["start"] = pd.to_datetime(
            df["candle_date_time_kst"]
        )

        df["open"] = df["opening_price"].astype(float)
        df["high"] = df["high_price"].astype(float)
        df["low"] = df["low_price"].astype(float)
        df["close"] = df["trade_price"].astype(float)

        df = df[
            [
                "start",
                "open",
                "high",
                "low",
                "close"
            ]
        ]

        df = df.sort_values("start")
        df = df.drop_duplicates("start")

        return df.reset_index(drop=True)

    except Exception as e:

        logging.warning(
            "UPBIT 4H ERROR | %s | %s",
            market,
            e
        )

        return pd.DataFrame()


# =========================================================
# ROC
# =========================================================

def calculate_roc(
    periods,
    index,
    period=ROC_PERIOD
):

    if index < period:
        return None

    current_close = safe_float(
        periods[index]["close"]
    )

    previous_close = safe_float(
        periods[index - period]["close"]
    )

    if current_close is None:
        return None

    if previous_close is None:
        return None

    if previous_close == 0:
        return None

    return (
        (current_close - previous_close)
        / previous_close
    ) * 100


# =========================================================
# ROC 0선 상향 돌파
# =========================================================

def roc_cross_up(
    previous_roc,
    current_roc
):

    if previous_roc is None:
        return False

    if current_roc is None:
        return False

    return (
        previous_roc < ROC_SIGNAL_LEVEL
        and
        current_roc >= ROC_SIGNAL_LEVEL
    )


# =========================================================
# Upbit 4H 기간 생성
#
# SIGNAL:
# ① ROC 0선 상향 돌파봉
# ② 그 바로 다음 봉
#
# 그 이후 봉은 SIGNAL 없음
# =========================================================

def build_upbit_4h_periods(
    df,
    current_price=None
):

    if df is None or df.empty:
        return []

    df = df.copy()

    df = df.sort_values("start")
    df = df.drop_duplicates("start")
    df = df.reset_index(drop=True)

    periods = []

    for i, row in df.iterrows():

        start = row["start"].to_pydatetime()

        open_price = safe_float(row["open"])
        high_price = safe_float(row["high"])
        low_price = safe_float(row["low"])
        close_price = safe_float(row["close"])

        is_last = (
            i == len(df) - 1
        )

        # 마지막 캔들은 진행봉으로 취급
        if is_last and current_price is not None:

            close_price = current_price

            if high_price is None:
                high_price = current_price
            else:
                high_price = max(
                    high_price,
                    current_price
                )

            if low_price is None:
                low_price = current_price
            else:
                low_price = min(
                    low_price,
                    current_price
                )

        periods.append({
            "start": start,
            "end": start + timedelta(hours=4),
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,

            "change": None,
            "roc": None,

            "roc_signal": False,
            "roc_cross_up": False,
            "signal_reason": ""
        })

    # -----------------------------------------------------
    # 캔들별 변화율
    # -----------------------------------------------------

    for p in periods:

        open_price = p["open"]
        close_price = p["close"]

        if (
            open_price is not None
            and
            open_price != 0
            and
            close_price is not None
        ):
            p["change"] = (
                (close_price - open_price)
                / open_price
            ) * 100

    # -----------------------------------------------------
    # ROC(50)
    # 전체 200개 기준으로 계산
    # -----------------------------------------------------

    for i in range(len(periods)):

        periods[i]["roc"] = calculate_roc(
            periods,
            i,
            ROC_PERIOD
        )

    # -----------------------------------------------------
    # SIGNAL
    #
    # 전봉 ROC < 0
    # 현재봉 ROC >= 0
    #
    # → 현재 돌파봉 SIGNAL
    # → 바로 다음 봉 SIGNAL
    #
    # 이후 봉은 SIGNAL 없음
    # -----------------------------------------------------

    for i in range(1, len(periods)):

        previous_roc = periods[i - 1]["roc"]
        current_roc = periods[i]["roc"]

        if roc_cross_up(
            previous_roc,
            current_roc
        ):

            # 돌파봉
            periods[i]["roc_signal"] = True
            periods[i]["roc_cross_up"] = True
            periods[i]["signal_reason"] = (
                "0선 상향 돌파"
            )

            # 바로 다음 봉
            if i + 1 < len(periods):

                periods[i + 1]["roc_signal"] = True
                periods[i + 1]["signal_reason"] = (
                    "돌파 후 다음봉"
                )

    return periods[-6:]


# =========================================================
# SIGNAL 여부
# =========================================================

def signal_pass(periods):

    if not periods:
        return False

    return any(
        p.get("roc_signal", False)
        for p in periods
    )


# =========================================================
# 현재 캔들 SIGNAL 정보
# =========================================================

def signal_details(periods):

    if not periods:
        return {
            "signal": False,
            "reason": ""
        }

    current = periods[-1]

    if current.get("roc_signal", False):

        return {
            "signal": True,
            "reason": current.get(
                "signal_reason",
                ""
            )
        }

    return {
        "signal": False,
        "reason": ""
    }


# =========================================================
# 4H 분석
# =========================================================

def analyze_4h(
    market,
    current_price
):

    df = get_upbit_4h(
        market,
        count=200
    )

    if df.empty:
        return None

    periods = build_upbit_4h_periods(
        df,
        current_price
    )

    if not periods:
        return None

    details = signal_details(
        periods
    )

    return {
        "periods": periods,
        "signal_4h": signal_pass(periods),
        "current_signal": details["signal"],
        "signal_reason": details["reason"]
    }


# =========================================================
# OKX 캔들
# =========================================================

def okx_candles(
    bar,
    limit=200
):

    response = request_get(
        f"{OKX_API}/market/candles",
        params={
            "instId": "BTC-USDT-SWAP",
            "bar": bar,
            "limit": str(limit)
        }
    )

    if response is None:
        return pd.DataFrame()

    try:

        data = response.json()

        if data.get("code") != "0":
            logging.warning(
                "OKX ERROR | %s",
                data
            )
            return pd.DataFrame()

        rows = data.get(
            "data",
            []
        )

        if not rows:
            return pd.DataFrame()

        result = []

        for row in rows:

            if len(row) < 5:
                continue

            ts = int(row[0])

            dt_utc = datetime.fromtimestamp(
                ts / 1000,
                tz=ZoneInfo("UTC")
            )

            dt_kst = dt_utc.astimezone(
                KST
            )

            result.append({
                "start": dt_kst.replace(
                    tzinfo=None
                ),
                "open": safe_float(row[1]),
                "high": safe_float(row[2]),
                "low": safe_float(row[3]),
                "close": safe_float(row[4])
            })

        df = pd.DataFrame(result)

        if df.empty:
            return df

        df = df.sort_values("start")
        df = df.drop_duplicates("start")

        return df.reset_index(drop=True)

    except Exception as e:

        logging.warning(
            "OKX CANDLE ERROR | %s",
            e
        )

        return pd.DataFrame()


# =========================================================
# OKX BTC 현재가
# =========================================================

def okx_price():

    response = request_get(
        f"{OKX_API}/market/ticker",
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

        return safe_float(
            rows[0].get("last")
        )

    except Exception:
        return None


# =========================================================
# BTC 일봉
#
# OKX BTC-USDT-SWAP 1D 직접 요청
# UTC 00:00 = KST 09:00
# =========================================================

def build_daily_periods(
    df,
    current_price=None
):

    if df is None or df.empty:
        return []

    df = df.copy()

    df = df.sort_values("start")
    df = df.drop_duplicates("start")
    df = df.reset_index(drop=True)

    periods = []

    now = now_kst()

    current_start = now.replace(
        hour=9,
        minute=0,
        second=0,
        microsecond=0
    ).replace(
        tzinfo=None
    )

    if now.hour < 9:
        current_start -= timedelta(
            days=1
        )

    for i, row in df.iterrows():

        start = row["start"]

        open_price = safe_float(
            row["open"]
        )

        high_price = safe_float(
            row["high"]
        )

        low_price = safe_float(
            row["low"]
        )

        close_price = safe_float(
            row["close"]
        )

        is_active = (
            start == current_start
        )

        if (
            is_active
            and
            current_price is not None
        ):

            close_price = current_price

            if high_price is None:
                high_price = current_price
            else:
                high_price = max(
                    high_price,
                    current_price
                )

            if low_price is None:
                low_price = current_price
            else:
                low_price = min(
                    low_price,
                    current_price
                )

        periods.append({
            "start": start,
            "end": start + timedelta(days=1),

            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,

            "change": None,
            "roc": None
        })

    # 일봉 변화율
    for p in periods:

        if (
            p["open"] is not None
            and
            p["open"] != 0
            and
            p["close"] is not None
        ):

            p["change"] = (
                (p["close"] - p["open"])
                / p["open"]
            ) * 100

    # ROC(50)
    for i in range(len(periods)):

        periods[i]["roc"] = calculate_roc(
            periods,
            i,
            ROC_PERIOD
        )

    return periods[-6:]


# =========================================================
# OKX BTC 업데이트
# =========================================================

def update_okx_btc():

    global latest_btc_okx_price
    global latest_btc_daily_periods
    global latest_btc_daily_change
    global latest_btc_daily_roc

    price = okx_price()

    if price is None:
        logging.warning(
            "OKX BTC PRICE FAILED"
        )
        return

    latest_btc_okx_price = price

    # 반드시 OKX 1D 직접 요청
    d1d = okx_candles(
        "1D",
        100
    )

    if d1d.empty:
        logging.warning(
            "OKX BTC 1D FAILED"
        )
        return

    periods = build_daily_periods(
        d1d,
        price
    )

    latest_btc_daily_periods = periods

    if periods:

        latest = periods[-1]

        latest_btc_daily_change = (
            latest.get("change")
        )

        latest_btc_daily_roc = (
            latest.get("roc")
        )

    logging.info(
        "BTC | %.2f | DAILY %.2f%% | ROC50 %.2f%%",
        price,
        latest_btc_daily_change
        if latest_btc_daily_change is not None
        else 0,
        latest_btc_daily_roc
        if latest_btc_daily_roc is not None
        else 0
    )


# =========================================================
# Upbit 업데이트
#
# ① 09시 기준 당일 변동률 > 0
# ② 양수 종목만 후보
# ③ 거래대금 순 TOP20
# ④ TOP20에서 4H ROC SIGNAL 분석
# =========================================================

def update_upbit():

    global latest_rows

    markets = get_upbit_markets()

    if not markets:
        logging.warning(
            "UPBIT MARKETS EMPTY"
        )
        return

    tickers = get_upbit_tickers(
        markets
    )

    if not tickers:
        logging.warning(
            "UPBIT TICKERS EMPTY"
        )
        return

    candidates = []

    for ticker in tickers:

        market = ticker.get(
            "market"
        )

        if not market:
            continue

        current_price = safe_float(
            ticker.get("trade_price")
        )

        trade_value = safe_float(
            ticker.get(
                "acc_trade_price_24h"
            )
        )

        if current_price is None:
            continue

        if trade_value is None:
            trade_value = 0

        # ---------------------------------------------
        # Upbit 09시 기준 당일 변동률
        # ---------------------------------------------

        daily_change = (
            get_upbit_daily_change(
                market,
                current_price
            )
        )

        if daily_change is None:
            continue

        # ---------------------------------------------
        # 당일 양수 종목만
        # ---------------------------------------------

        if daily_change <= 0:
            continue

        candidates.append({
            "market": market,
            "price": current_price,
            "trade_value": trade_value,
            "daily_change": daily_change
        })

    # ---------------------------------------------
    # 거래대금 순 정렬
    # ---------------------------------------------

    candidates.sort(
        key=lambda x: x["trade_value"],
        reverse=True
    )

    candidates = candidates[:TOP_N]

    rows = []

    for rank, item in enumerate(
        candidates,
        start=1
    ):

        market = item["market"]
        price = item["price"]

        analysis = analyze_4h(
            market,
            price
        )

        if analysis is None:
            continue

        rows.append({
            "rank": rank,
            "market": market,
            "price": price,

            "trade_value": item[
                "trade_value"
            ],

            "daily_change": item[
                "daily_change"
            ],

            "periods": analysis[
                "periods"
            ],

            "signal_4h": analysis[
                "signal_4h"
            ],

            "current_signal": analysis[
                "current_signal"
            ],

            "signal_reason": analysis[
                "signal_reason"
            ]
        })

    latest_rows = rows

    logging.info(
        "UPBIT | 09시 양수 후보 %s개 | 거래대금 TOP20 | 분석 %s개",
        len(candidates),
        len(rows)
    )


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    global last_update_time

    try:

        update_upbit()

    except Exception as e:

        logging.exception(
            "UPBIT UPDATE ERROR | %s",
            e
        )

    try:

        # BTC OKX 시황은 항상 표시
        update_okx_btc()

    except Exception as e:

        logging.exception(
            "OKX BTC UPDATE ERROR | %s",
            e
        )

    last_update_time = now_kst()

    logging.info(
        "DASHBOARD UPDATE COMPLETE"
    )


# =========================================================
# 포맷
# =========================================================

def fmt_price(
    value
):

    if value is None:
        return "-"

    value = float(value)

    if value >= 1000:
        return f"{value:,.0f}"

    if value >= 1:
        return f"{value:,.2f}"

    if value >= 0.01:
        return f"{value:,.4f}"

    return f"{value:,.8f}"


def fmt_change(
    value
):

    if value is None:
        return "-"

    value = float(value)

    if value > 0:
        return f"+{value:.2f}%"

    return f"{value:.2f}%"


def fmt_roc(
    value
):

    if value is None:
        return "-"

    value = float(value)

    if value > 0:
        return f"+{value:.2f}%"

    return f"{value:.2f}%"


def fmt_vol(
    value
):

    if value is None:
        return "-"

    value = float(value)

    # 원 단위
    if value >= 1_0000_0000_0000:
        return f"{value / 1_0000_0000_0000:.2f}조"

    if value >= 1_0000_0000:
        return f"{value / 1_0000_0000:.1f}억"

    if value >= 1_0000:
        return f"{value / 1_0000:.1f}만"

    return f"{value:,.0f}"


# =========================================================
# SIGNAL HTML
# =========================================================

def signal_reason_html(
    row
):

    if not row.get(
        "current_signal",
        False
    ):
        return ""

    reason = row.get(
        "signal_reason",
        ""
    )

    if reason == "0선 상향 돌파":

        return (
            '<span class="signal-badge '
            'signal-cross">'
            '▲ 0선 돌파'
            '</span>'
        )

    if reason == "돌파 후 다음봉":

        return (
            '<span class="signal-badge '
            'signal-next">'
            '→ 돌파 후 다음봉'
            '</span>'
        )

    return (
        '<span class="signal-badge">'
        'SIGNAL'
        '</span>'
    )


# =========================================================
# 4H 캔들 HTML
# =========================================================

def cells(periods):

    if not periods:
        return (
            '<div class="empty">'
            '4시간봉 데이터 없음'
            '</div>'
        )

    parts = []

    for p in periods:

        start = p["start"]

        date_text = start.strftime(
            "%m/%d %H:%M"
        )

        change = p.get(
            "change"
        )

        roc = p.get(
            "roc"
        )

        signal = p.get(
            "roc_signal",
            False
        )

        cross = p.get(
            "roc_cross_up",
            False
        )

        reason = p.get(
            "signal_reason",
            ""
        )

        cls = "candle"

        if signal:
            cls += " roc-signal"

        if cross:
            cls += " roc-cross"
        elif reason == "돌파 후 다음봉":
            cls += " roc-next"

        if change is not None:

            if change > 0:
                change_cls = "up"
            elif change < 0:
                change_cls = "down"
            else:
                change_cls = ""

        else:
            change_cls = ""

        if roc is not None:

            if roc > 0:
                roc_cls = "roc-up"
            elif roc < 0:
                roc_cls = "roc-down"
            else:
                roc_cls = ""

        else:
            roc_cls = ""

        badge = ""

        if signal:

            if cross:

                badge = (
                    '<div class="mini-signal">'
                    '▲ 0선 돌파'
                    '</div>'
                )

            else:

                badge = (
                    '<div class="mini-signal">'
                    '→ 다음봉'
                    '</div>'
                )

        parts.append(
            f"""
            <div class="{cls}">
                <div class="candle-date">
                    {html.escape(date_text)}
                </div>

                <div class="candle-row">
                    <span>변동</span>
                    <strong class="{change_cls}">
                        {fmt_change(change)}
                    </strong>
                </div>

                <div class="candle-row">
                    <span>ROC(50)</span>
                    <strong class="{roc_cls}">
                        {fmt_roc(roc)}
                    </strong>
                </div>

                {badge}
            </div>
            """
        )

    return "".join(parts)


# =========================================================
# BTC 일봉 HTML
# =========================================================

def btc_daily_cells(
    periods
):

    if not periods:
        return (
            '<div class="empty">'
            'BTC 일봉 데이터 없음'
            '</div>'
        )

    parts = []

    for p in periods:

        start = p["start"]

        date_text = start.strftime(
            "%m/%d"
        )

        change = p.get(
            "change"
        )

        roc = p.get(
            "roc"
        )

        if change is not None:

            if change > 0:
                change_cls = "up"
            elif change < 0:
                change_cls = "down"
            else:
                change_cls = ""

        else:
            change_cls = ""

        if roc is not None:

            if roc > 0:
                roc_cls = "roc-up"
            elif roc < 0:
                roc_cls = "roc-down"
            else:
                roc_cls = ""

        else:
            roc_cls = ""

        parts.append(
            f"""
            <div class="btc-day">
                <div class="btc-date">
                    {html.escape(date_text)}
                </div>

                <div class="btc-value {change_cls}">
                    {fmt_change(change)}
                </div>

                <div class="btc-roc {roc_cls}">
                    ROC {fmt_roc(roc)}
                </div>
            </div>
            """
        )

    return "".join(parts)


# =========================================================
# BTC HTML
# =========================================================

def btc_html():

    price = latest_btc_okx_price
    change = latest_btc_daily_change
    roc = latest_btc_daily_roc

    if price is None:
        return ""

    return f"""
    <section class="btc-section">

        <div class="section-title">
            BTC Market
        </div>

        <div class="btc-card">

            <div class="btc-header">

                <div>
                    <div class="btc-name">
                        BTC-USDT-SWAP
                    </div>

                    <div class="btc-source">
                        OKX · 1D · KST 09:00
                    </div>
                </div>

                <div class="btc-price">
                    {fmt_price(price)}
                </div>

            </div>

            <div class="btc-summary">

                <div class="summary-box">
                    <span>당일</span>
                    <strong class="{
                        'up'
                        if change is not None
                        and change > 0
                        else 'down'
                        if change is not None
                        and change < 0
                        else ''
                    }">
                        {fmt_change(change)}
                    </strong>
                </div>

                <div class="summary-box">
                    <span>ROC(50)</span>
                    <strong class="{
                        'roc-up'
                        if roc is not None
                        and roc > 0
                        else 'roc-down'
                        if roc is not None
                        and roc < 0
                        else ''
                    }">
                        {fmt_roc(roc)}
                    </strong>
                </div>

            </div>

            <div class="btc-grid">
                {btc_daily_cells(
                    latest_btc_daily_periods
                )}
            </div>

        </div>

    </section>
    """


# =========================================================
# 코인 카드
# =========================================================

def card(row):

    market = row["market"]

    coin = market.replace(
        "KRW-",
        ""
    )

    price = row["price"]

    daily_change = row[
        "daily_change"
    ]

    trade_value = row[
        "trade_value"
    ]

    periods = row[
        "periods"
    ]

    signal = row.get(
        "signal_4h",
        False
    )

    current_signal = row.get(
        "current_signal",
        False
    )

    signal_reason = row.get(
        "signal_reason",
        ""
    )

    if daily_change > 0:
        daily_cls = "up"
    else:
        daily_cls = "down"

    signal_box = ""

    if current_signal:

        if signal_reason == "0선 상향 돌파":

            signal_box = """
            <div class="card-signal cross">
                ▲ 0선 상향 돌파
            </div>
            """

        elif signal_reason == "돌파 후 다음봉":

            signal_box = """
            <div class="card-signal next">
                → 돌파 후 다음봉
            </div>
            """

    return f"""
    <div class="coin-card">

        <div class="coin-header">

            <div class="rank">
                #{row["rank"]}
            </div>

            <div class="coin-name">
                {html.escape(coin)}
            </div>

            <div class="coin-price">
                {fmt_price(price)}
            </div>

        </div>

        <div class="coin-info">

            <div class="info-item">
                <span>09시 변동</span>
                <strong class="{daily_cls}">
                    {fmt_change(daily_change)}
                </strong>
            </div>

            <div class="info-item">
                <span>24H 거래대금</span>
                <strong>
                    {fmt_vol(trade_value)}
                </strong>
            </div>

        </div>

        {signal_box}

        <div class="tf-title">
            4H ROC(50)
        </div>

        <div class="candle-grid">
            {cells(periods)}
        </div>

    </div>
    """


# =========================================================
# 메인 화면
# =========================================================

def both_section():

    signal_rows = [
        row
        for row in latest_rows
        if row.get(
            "signal_4h",
            False
        )
    ]

    if not signal_rows:

        return """
        <div class="empty-signal">
            현재 SIGNAL 종목 없음
        </div>
        """

    return "".join(
        card(row)
        for row in signal_rows
    )


def top_list_section():

    if not latest_rows:

        return """
        <div class="empty">
            TOP20 데이터 없음
        </div>
        """

    return "".join(
        card(row)
        for row in latest_rows
    )


@app.get(
    "/",
    response_class=HTMLResponse
)
def index():

    update_text = "-"

    if last_update_time is not None:
        update_text = (
            last_update_time.strftime(
                "%Y-%m-%d %H:%M:%S"
            )
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
            content="60"
        >

        <title>Upbit Signal</title>

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
                    Arial,
                    "Noto Sans KR",
                    sans-serif;
            }}

            .container {{
                width: 100%;
                max-width: 1500px;

                margin: 0 auto;

                padding: 20px;
            }}

            .main-title {{
                font-size: 28px;
                font-weight: 800;

                margin-bottom: 6px;
            }}

            .update-time {{
                color: #7f8b99;
                font-size: 12px;

                margin-bottom: 25px;
            }}

            .section-title {{
                font-size: 21px;
                font-weight: 800;

                margin:
                    25px 0
                    12px;
            }}

            .btc-card {{
                background: #111720;

                border:
                    1px solid #202a35;

                border-radius: 14px;

                padding: 18px;

                margin-bottom: 28px;
            }}

            .btc-header {{
                display: flex;

                justify-content:
                    space-between;

                align-items:
                    center;

                gap: 20px;
            }}

            .btc-name {{
                font-size: 20px;
                font-weight: 800;
            }}

            .btc-source {{
                margin-top: 5px;

                font-size: 12px;
                color: #7f8b99;
            }}

            .btc-price {{
                font-size: 24px;
                font-weight: 800;
            }}

            .btc-summary {{
                display: grid;

                grid-template-columns:
                    repeat(2, 1fr);

                gap: 10px;

                margin-top: 15px;
            }}

            .summary-box {{
                background: #0c1219;

                border-radius: 10px;

                padding: 12px;
            }}

            .summary-box span {{
                display: block;

                color: #7f8b99;

                font-size: 12px;

                margin-bottom: 5px;
            }}

            .summary-box strong {{
                font-size: 17px;
            }}

            .btc-grid {{
                display: grid;

                grid-template-columns:
                    repeat(6, 1fr);

                gap: 8px;

                margin-top: 14px;
            }}

            .btc-day {{
                background: #0c1219;

                border:
                    1px solid #1c2631;

                border-radius: 9px;

                padding: 10px;

                text-align: center;
            }}

            .btc-date {{
                color: #8995a3;

                font-size: 11px;

                margin-bottom: 7px;
            }}

            .btc-value {{
                font-weight: 700;

                font-size: 13px;
            }}

            .btc-roc {{
                margin-top: 6px;

                font-size: 11px;
            }}

            .top-title {{
                font-size: 21px;
                font-weight: 800;

                margin:
                    25px 0
                    12px;
            }}

            .signal-title {{
                font-size: 21px;
                font-weight: 800;

                margin:
                    25px 0
                    12px;
            }}

            .cards {{
                display: grid;

                grid-template-columns:
                    repeat(2, 1fr);

                gap: 14px;
            }}

            .coin-card {{
                background: #111720;

                border:
                    1px solid #202a35;

                border-radius: 14px;

                padding: 16px;
            }}

            .coin-header {{
                display: flex;

                align-items: center;

                gap: 9px;
            }}

            .rank {{
                color: #778391;

                font-size: 13px;

                min-width: 30px;
            }}

            .coin-name {{
                font-size: 20px;
                font-weight: 800;

                flex: 1;
            }}

            .coin-price {{
                font-size: 17px;
                font-weight: 700;
            }}

            .coin-info {{
                display: grid;

                grid-template-columns:
                    repeat(2, 1fr);

                gap: 8px;

                margin-top: 13px;
            }}

            .info-item {{
                background: #0c1219;

                border-radius: 9px;

                padding: 10px;
            }}

            .info-item span {{
                display: block;

                color: #7f8b99;

                font-size: 11px;

                margin-bottom: 5px;
            }}

            .info-item strong {{
                font-size: 14px;
            }}

            .tf-title {{
                margin-top: 14px;

                margin-bottom: 8px;

                font-size: 13px;
                font-weight: 700;

                color: #aeb8c4;
            }}

            .candle-grid {{
                display: grid;

                grid-template-columns:
                    repeat(6, 1fr);

                gap: 6px;
            }}

            .candle {{
                position: relative;

                background: #0c1219;

                border:
                    1px solid #1c2631;

                border-radius: 8px;

                padding: 8px;

                min-height: 90px;
            }}

            .candle.roc-signal {{
                border:
                    1px solid #f0b90b;
            }}

            .candle.roc-cross {{
                box-shadow:
                    0 0 0 1px
                    rgba(240,185,11,.25)
                    inset;
            }}

            .candle.roc-next {{
                border:
                    1px solid #6f7f90;
            }}

            .candle-date {{
                font-size: 10px;

                color: #7f8b99;

                margin-bottom: 7px;
            }}

            .candle-row {{
                display: flex;

                justify-content:
                    space-between;

                align-items: center;

                gap: 4px;

                font-size: 10px;

                margin-top: 4px;
            }}

            .candle-row span {{
                color: #697583;
            }}

            .candle-row strong {{
                font-size: 10px;
            }}

            .up {{
                color: #35d07f;
            }}

            .down {{
                color: #ff6574;
            }}

            .roc-up {{
                color: #35d07f;
            }}

            .roc-down {{
                color: #ff6574;
            }}

            .mini-signal {{
                margin-top: 7px;

                padding: 4px 3px;

                border-radius: 5px;

                text-align: center;

                font-size: 9px;

                font-weight: 800;
            }}

            .roc-cross .mini-signal {{
                background: #3b300e;
                color: #f0c84b;
            }}

            .roc-next .mini-signal {{
                background: #202832;
                color: #b6c1cc;
            }}

            .card-signal {{
                margin-top: 12px;

                padding: 8px;

                border-radius: 8px;

                text-align: center;

                font-size: 12px;

                font-weight: 800;
            }}

            .card-signal.cross {{
                background: #3b300e;
                color: #f0c84b;
            }}

            .card-signal.next {{
                background: #202832;
                color: #c0cad4;
            }}

            .empty {{
                padding: 30px;

                text-align: center;

                color: #66717e;

                background: #111720;

                border-radius: 12px;
            }}

            .empty-signal {{
                padding: 30px;

                text-align: center;

                color: #66717e;

                background: #111720;

                border:
                    1px solid #202a35;

                border-radius: 12px;
            }}

            @media (
                max-width: 900px
            ) {{

                .cards {{
                    grid-template-columns:
                        1fr;
                }}

                .candle-grid {{
                    grid-template-columns:
                        repeat(3, 1fr);
                }}

                .btc-grid {{
                    grid-template-columns:
                        repeat(3, 1fr);
                }}
            }}

            @media (
                max-width: 600px
            ) {{

                .container {{
                    padding: 12px;
                }}

                .main-title {{
                    font-size: 23px;
                }}

                .coin-header {{
                    flex-wrap: wrap;
                }}

                .coin-price {{
                    width: 100%;
                    margin-top: 4px;
                }}

                .candle-grid {{
                    grid-template-columns:
                        repeat(2, 1fr);
                }}

                .btc-grid {{
                    grid-template-columns:
                        repeat(2, 1fr);
                }}

                .btc-header {{
                    align-items:
                        flex-start;

                    flex-direction:
                        column;
                }}

                .btc-price {{
                    font-size: 21px;
                }}
            }}

        </style>

    </head>

    <body>

        <div class="container">

            <div class="main-title">
                Upbit Signal
            </div>

            <div class="update-time">
                업데이트:
                {html.escape(update_text)}
            </div>

            {btc_html()}

            <div class="signal-title">
                Upbit Signal
            </div>

            <div class="cards">
                {both_section()}
            </div>

            <div class="top-title">
                TOP20 · 거래대금 순
            </div>

            <div class="cards">
                {top_list_section()}
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
        update_dashboard
    )

    while True:

        try:
            schedule.run_pending()

        except Exception as e:

            logging.exception(
                "SCHEDULER ERROR | %s",
                e
            )

        time.sleep(1)


# =========================================================
# 시작
# =========================================================

@app.on_event("startup")
def startup_event():

    logging.info(
        "START | Upbit Signal | "
        "09시 양수 TOP20 · 거래대금 순 · "
        "4H ROC(50) 0선 돌파봉 + 다음봉 | "
        "OKX BTC 1D ROC(50)"
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
