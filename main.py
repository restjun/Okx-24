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

KST = ZoneInfo("Asia/Seoul")


# =========================================================
# 사용자 설정
# =========================================================

VOLUME_HOURS = 24

# 업비트 거래대금 TOP
TOP_N = 15

# 화면 업데이트 주기
UPDATE_MINUTES = 1

# 업비트 설정
USE_UPBIT = "Y"

# OKX는 거래대금 리스트용으로 사용하지 않음
USE_OKX = "N"

# API 요청
REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10


# =========================================================
# OKX BTC 설정
# =========================================================

OKX_BASE_URL = "https://www.okx.com"

OKX_BTC_INST_ID = "BTC-USDT"

# BTC 1시간봉을 이용하여
# KST 09:00 기준 일봉을 직접 생성
OKX_CANDLE_HOURS = 1

# 충분한 1시간봉 확보
OKX_HISTORY_COUNT = 1000
OKX_HISTORY_CHUNK = 300

OKX_RETRY_DELAY = 2
OKX_MAX_RETRY_ROUNDS = 3


# =========================================================
# 전역 상태
# =========================================================

latest_btc_price = None
latest_btc_daily_change = None
latest_btc_daily_start = None

latest_upbit_data = []

last_update_time = None


# =========================================================
# 공통
# =========================================================

session = requests.Session()

last_request_time = 0


def request_get(url, params=None, timeout=10):

    global last_request_time

    for attempt in range(MAX_RETRIES):

        try:

            elapsed = time.time() - last_request_time

            if elapsed < REQUEST_INTERVAL:
                time.sleep(
                    REQUEST_INTERVAL - elapsed
                )

            response = session.get(
                url,
                params=params,
                timeout=timeout
            )

            last_request_time = time.time()

            if response.status_code == 429:

                logging.warning(
                    "429 Too Many Requests - %s초 대기",
                    RATE_LIMIT_WAIT
                )

                time.sleep(RATE_LIMIT_WAIT)
                continue

            response.raise_for_status()

            return response.json()

        except Exception as e:

            logging.warning(
                "API 요청 실패 (%s/%s): %s",
                attempt + 1,
                MAX_RETRIES,
                e
            )

            if attempt < MAX_RETRIES - 1:
                time.sleep(1)

    return None


# =========================================================
# 업비트 마켓 조회
# =========================================================

def get_upbit_markets():

    url = "https://api.upbit.com/v1/market/all"

    data = request_get(
        url,
        params={
            "isDetails": "false"
        }
    )

    if not data:
        return []

    markets = []

    for item in data:

        market = item.get("market", "")

        if market.startswith("KRW-"):

            markets.append({
                "market": market,
                "name": item.get(
                    "korean_name",
                    market
                )
            })

    return markets


# =========================================================
# 업비트 24시간 거래대금
# =========================================================

def get_upbit_tickers(markets):

    if not markets:
        return []

    url = "https://api.upbit.com/v1/ticker"

    results = []

    chunk_size = 100

    for i in range(
        0,
        len(markets),
        chunk_size
    ):

        chunk = markets[
            i:i + chunk_size
        ]

        market_codes = ",".join(
            x["market"]
            for x in chunk
        )

        data = request_get(
            url,
            params={
                "markets": market_codes
            }
        )

        if not data:
            continue

        name_map = {
            x["market"]: x["name"]
            for x in chunk
        }

        for item in data:

            market = item.get(
                "market"
            )

            results.append({
                "market": market,

                "name": name_map.get(
                    market,
                    market
                ),

                "volume_24h": float(
                    item.get(
                        "acc_trade_price_24h",
                        0
                    )
                    or 0
                ),

                "current_price": float(
                    item.get(
                        "trade_price",
                        0
                    )
                    or 0
                )
            })

    return results


# =========================================================
# 업비트 현재 일봉 상승률
#
# 업비트 일봉 기준
# KST 09:00 ~ 다음날 08:59:59
#
# 현재가 / 이전 일봉 종가
# =========================================================

def daily_change_upbit(
    market,
    current_price
):

    url = "https://api.upbit.com/v1/candles/days"

    data = request_get(
        url,
        params={
            "market": market,
            "count": 2
        }
    )

    if not data or len(data) < 2:
        return None

    try:

        current_candle = data[0]
        previous_candle = data[1]

        previous_close = float(
            previous_candle[
                "trade_price"
            ]
        )

        if previous_close <= 0:
            return None

        change = (
            (
                current_price
                - previous_close
            )
            / previous_close
            * 100
        )

        return change

    except Exception as e:

        logging.warning(
            "%s 일봉 상승률 계산 실패: %s",
            market,
            e
        )

        return None


# =========================================================
# OKX 현재 BTC 가격
# =========================================================

def get_okx_btc_price():

    url = (
        OKX_BASE_URL
        + "/api/v5/market/ticker"
    )

    data = request_get(
        url,
        params={
            "instId": OKX_BTC_INST_ID
        }
    )

    if not data:
        return None

    try:

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

        logging.warning(
            "OKX BTC 가격 처리 실패: %s",
            e
        )

        return None


# =========================================================
# OKX 1시간봉 조회
# =========================================================

def get_okx_1h_candles(
    limit=300,
    after=None
):

    url = (
        OKX_BASE_URL
        + "/api/v5/market/candles"
    )

    params = {
        "instId": OKX_BTC_INST_ID,
        "bar": "1H",
        "limit": str(limit)
    }

    if after is not None:
        params["after"] = str(after)

    data = request_get(
        url,
        params=params,
        timeout=15
    )

    if not data:
        return []

    try:

        return data.get(
            "data",
            []
        )

    except Exception:
        return []


# =========================================================
# OKX BTC 1시간봉 히스토리
# =========================================================

def get_okx_btc_1h_history(
    required=1000
):

    all_rows = []

    after = None

    rounds = 0

    while (
        len(all_rows) < required
        and rounds < OKX_MAX_RETRY_ROUNDS + 10
    ):

        rounds += 1

        rows = get_okx_1h_candles(
            limit=OKX_HISTORY_CHUNK,
            after=after
        )

        if not rows:
            break

        all_rows.extend(rows)

        try:

            timestamps = [
                int(row[0])
                for row in rows
            ]

            oldest = min(
                timestamps
            )

            if after == oldest:
                break

            after = oldest

        except Exception:
            break

        if len(rows) < OKX_HISTORY_CHUNK:
            break

    if not all_rows:
        return pd.DataFrame()

    # 중복 제거
    unique = {}

    for row in all_rows:

        try:
            ts = int(row[0])
            unique[ts] = row

        except Exception:
            continue

    rows = list(
        unique.values()
    )

    rows.sort(
        key=lambda x: int(x[0])
    )

    rows = rows[-required:]

    result = []

    for row in rows:

        try:

            result.append({
                "timestamp": int(row[0]),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5])
            })

        except Exception:
            continue

    if not result:
        return pd.DataFrame()

    df = pd.DataFrame(
        result
    )

    df["datetime_utc"] = pd.to_datetime(
        df["timestamp"],
        unit="ms",
        utc=True
    )

    df["datetime_kst"] = (
        df["datetime_utc"]
        .dt.tz_convert(KST)
    )

    return df


# =========================================================
# KST 09:00 기준 일봉 생성
# =========================================================

def aggregate_okx_to_upbit_daily(
    df
):

    if df is None or df.empty:
        return pd.DataFrame()

    temp = df.copy()

    temp["kst_datetime"] = (
        temp["datetime_kst"]
    )

    # 09:00 기준 일자 계산
    temp["daily_start"] = (
        temp["kst_datetime"]
        - pd.Timedelta(hours=9)
    ).dt.floor("D") + pd.Timedelta(
        hours=9
    )

    grouped = (
        temp
        .groupby("daily_start")
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum")
        )
        .reset_index()
    )

    grouped.sort_values(
        "daily_start",
        inplace=True
    )

    grouped.reset_index(
        drop=True,
        inplace=True
    )

    return grouped


# =========================================================
# BTC KST 일봉 상승률
# =========================================================

def calculate_okx_btc_daily_change(
    daily_df,
    current_price
):

    if (
        daily_df is None
        or daily_df.empty
        or current_price is None
    ):
        return None, None

    now_kst = datetime.now(
        KST
    )

    # 현재 KST 일봉 시작시간
    if now_kst.hour >= 9:

        current_start = now_kst.replace(
            hour=9,
            minute=0,
            second=0,
            microsecond=0
        )

    else:

        current_start = (
            now_kst
            - timedelta(days=1)
        ).replace(
            hour=9,
            minute=0,
            second=0,
            microsecond=0
        )

    # pandas timezone 제거 후 비교
    daily = daily_df.copy()

    if daily[
        "daily_start"
    ].dt.tz is None:

        daily["daily_start"] = (
            daily["daily_start"]
            .dt.tz_localize(KST)
        )

    else:

        daily["daily_start"] = (
            daily["daily_start"]
            .dt.tz_convert(KST)
        )

    previous_rows = daily[
        daily["daily_start"]
        < current_start
    ]

    if previous_rows.empty:
        return None, current_start

    previous_close = float(
        previous_rows.iloc[-1]["close"]
    )

    if previous_close <= 0:
        return None, current_start

    change = (
        (
            current_price
            - previous_close
        )
        / previous_close
        * 100
    )

    return change, current_start


# =========================================================
# BTC 시장 업데이트
# =========================================================

def update_btc_market():

    global latest_btc_price
    global latest_btc_daily_change
    global latest_btc_daily_start

    try:

        # 현재 BTC 가격
        current_price = (
            get_okx_btc_price()
        )

        if current_price is None:

            logging.warning(
                "BTC 현재 가격을 가져오지 못했습니다."
            )

            latest_btc_price = None
            latest_btc_daily_change = None

            return

        latest_btc_price = (
            current_price
        )

        # 1시간봉 확보
        df = get_okx_btc_1h_history(
            required=OKX_HISTORY_COUNT
        )

        if df.empty:

            logging.warning(
                "BTC 1시간봉 데이터를 가져오지 못했습니다."
            )

            latest_btc_daily_change = None

            return

        # KST 09:00 기준 일봉 생성
        daily_df = (
            aggregate_okx_to_upbit_daily(
                df
            )
        )

        if daily_df.empty:

            latest_btc_daily_change = None

            return

        change, daily_start = (
            calculate_okx_btc_daily_change(
                daily_df,
                current_price
            )
        )

        latest_btc_daily_change = change
        latest_btc_daily_start = (
            daily_start
        )

        if change is None:

            logging.warning(
                "BTC 당일 상승률 계산 실패"
            )

        else:

            logging.info(
                "BTC KST 일봉: %.2f%%",
                change
            )

    except Exception as e:

        logging.exception(
            "BTC 업데이트 오류: %s",
            e
        )

        latest_btc_price = None
        latest_btc_daily_change = None


# =========================================================
# 금액 포맷
# =========================================================

def format_money(
    value
):

    if value is None:
        return "-"

    value = float(value)

    # 조
    if value >= 1_000_000_000_000:

        return (
            f"{value / 1_000_000_000_000:.2f}"
            "조"
        )

    # 억
    if value >= 100_000_000:

        return (
            f"{value / 100_000_000:.1f}"
            "억"
        )

    # 만
    if value >= 10_000:

        return (
            f"{value / 10_000:.0f}"
            "만"
        )

    return f"{value:,.0f}"


# =========================================================
# 가격 포맷
# =========================================================

def format_price(
    value
):

    if value is None:
        return "-"

    value = float(value)

    if value >= 1_000_000:

        return f"{value:,.0f}"

    if value >= 1_000:

        return f"{value:,.1f}"

    if value >= 1:

        return f"{value:,.2f}"

    if value >= 0.01:

        return f"{value:,.4f}"

    return f"{value:,.8f}"


# =========================================================
# 상승률 포맷
# =========================================================

def format_change(
    value
):

    if value is None:
        return "-"

    value = float(value)

    if value > 0:

        return f"+{value:.2f}%"

    if value < 0:

        return f"{value:.2f}%"

    return "0.00%"


# =========================================================
# 업비트 데이터 분석
# =========================================================

def update_upbit():

    global latest_upbit_data

    markets = get_upbit_markets()

    if not markets:

        logging.warning(
            "업비트 마켓 정보를 가져오지 못했습니다."
        )

        latest_upbit_data = []

        return

    tickers = get_upbit_tickers(
        markets
    )

    if not tickers:

        logging.warning(
            "업비트 티커 정보를 가져오지 못했습니다."
        )

        latest_upbit_data = []

        return

    # -----------------------------------------------------
    # 거래대금 순위
    # -----------------------------------------------------

    tickers.sort(
        key=lambda x: x[
            "volume_24h"
        ],
        reverse=True
    )

    top_tickers = tickers[
        :TOP_N
    ]

    # -----------------------------------------------------
    # BTC 필터
    #
    # BTC 당일 상승률 > 0
    # -----------------------------------------------------

    btc_positive = (
        latest_btc_daily_change is not None
        and latest_btc_daily_change > 0
    )

    result = []

    for rank, item in enumerate(
        top_tickers,
        start=1
    ):

        market = item[
            "market"
        ]

        name = item[
            "name"
        ]

        current_price = item[
            "current_price"
        ]

        volume_24h = item[
            "volume_24h"
        ]

        # 업비트 당일 상승률
        change = daily_change_upbit(
            market,
            current_price
        )

        # -------------------------------------------------
        # Signal 조건
        #
        # 1. BTC 당일 양수
        # 2. 코인 당일 상승률 양수
        # -------------------------------------------------

        signal_pass = (
            btc_positive
            and change is not None
            and change > 0
        )

        result.append({
            "rank": rank,
            "market": market,
            "name": name,
            "volume": volume_24h,
            "current_price": current_price,
            "change": change,
            "signal_pass": signal_pass
        })

    latest_upbit_data = result

    logging.info(
        "업비트 TOP %s 업데이트 완료 / BTC Signal=%s",
        TOP_N,
        "ON" if btc_positive else "OFF"
    )


# =========================================================
# Signal 데이터
# =========================================================

def get_signal_rows():

    # BTC가 양수가 아니면 Signal 자체를 차단
    if not (
        latest_btc_daily_change is not None
        and latest_btc_daily_change > 0
    ):

        return []

    candidates = []

    for row in latest_upbit_data:

        change = row.get(
            "change"
        )

        if (
            change is not None
            and change > 0
        ):

            candidates.append(
                row.copy()
            )

    # 상승률 높은 순
    candidates.sort(
        key=lambda x: x[
            "change"
        ],
        reverse=True
    )

    # Signal 순위 추가
    for signal_rank, row in enumerate(
        candidates,
        start=1
    ):

        row[
            "signal_rank"
        ] = signal_rank

    return candidates


# =========================================================
# Signal HTML
# =========================================================

def render_signal_section():

    btc_change = (
        latest_btc_daily_change
    )

    if btc_change is None:

        return """
        <section class="section">
            <div class="section-title">
                🚀 SIGNAL
            </div>

            <div class="signal-status off">
                BTC 당일 상승률 확인 불가 → SIGNAL OFF
            </div>
        </section>
        """

    # BTC 음수 또는 0
    if btc_change <= 0:

        return f"""
        <section class="section">

            <div class="section-title">
                🚀 SIGNAL
            </div>

            <div class="signal-status off">
                BTC 당일
                <span class="negative">
                    {format_change(btc_change)}
                </span>
                → SIGNAL OFF
            </div>

            <div class="signal-empty">
                BTC가 당일 양수일 때만 Signal 통과
            </div>

        </section>
        """

    signal_rows = get_signal_rows()

    if not signal_rows:

        return f"""
        <section class="section">

            <div class="section-title">
                🚀 SIGNAL
            </div>

            <div class="signal-status on">
                BTC 당일
                <span class="positive">
                    {format_change(btc_change)}
                </span>
                → SIGNAL ON
            </div>

            <div class="signal-empty">
                TOP {TOP_N} 중 당일 상승 종목 없음
            </div>

        </section>
        """

    rows_html = ""

    for row in signal_rows:

        signal_rank = row[
            "signal_rank"
        ]

        top_rank = row[
            "rank"
        ]

        name = html.escape(
            row["name"]
        )

        volume = format_money(
            row["volume"]
        )

        price = format_price(
            row["current_price"]
        )

        change = format_change(
            row["change"]
        )

        change_class = (
            "positive"
            if row["change"] > 0
            else "negative"
        )

        rows_html += f"""
        <div class="signal-row">

            <div class="signal-rank">
                <span>#{signal_rank}</span>
            </div>

            <div class="top-rank">
                TOP {top_rank}
            </div>

            <div class="coin-name">
                {name}
            </div>

            <div class="volume">
                {volume}
            </div>

            <div class="price">
                {price}
            </div>

            <div class="change {change_class}">
                ▲ {change}
            </div>

            <div class="signal-icon">
                🚀
            </div>

        </div>
        """

    return f"""
    <section class="section">

        <div class="section-title">
            🚀 SIGNAL
        </div>

        <div class="signal-description">
            BTC 당일 양수 + TOP {TOP_N} 중
            상승률 양수 종목을 상승률순 정렬
        </div>

        <div class="signal-status on">
            BTC 당일
            <span class="positive">
                {format_change(btc_change)}
            </span>
            → SIGNAL ON
        </div>

        <div class="signal-header">

            <div>Signal</div>
            <div>TOP</div>
            <div>코인</div>
            <div>거래대금</div>
            <div>현재가</div>
            <div>상승률</div>
            <div></div>

        </div>

        {rows_html}

    </section>
    """


# =========================================================
# TOP 리스트 HTML
# =========================================================

def render_top_section():

    rows_html = ""

    for row in latest_upbit_data:

        rank = row[
            "rank"
        ]

        name = html.escape(
            row["name"]
        )

        volume = format_money(
            row["volume"]
        )

        price = format_price(
            row["current_price"]
        )

        change = format_change(
            row["change"]
        )

        if (
            row["change"] is not None
            and row["change"] > 0
        ):

            change_class = "positive"

        elif (
            row["change"] is not None
            and row["change"] < 0
        ):

            change_class = "negative"

        else:

            change_class = "neutral"

        signal_icon = (
            "🚀"
            if row["signal_pass"]
            else ""
        )

        rows_html += f"""
        <div class="coin-row">

            <div class="rank">
                {rank}
            </div>

            <div class="coin-name">
                {name}
            </div>

            <div class="volume">
                {volume}
            </div>

            <div class="price">
                {price}
            </div>

            <div class="change {change_class}">
                {change}
            </div>

            <div class="signal-cell">
                {signal_icon}
            </div>

        </div>
        """

    return f"""
    <section class="section">

        <div class="section-title">
            💰 UPBIT 거래대금 TOP {TOP_N}
        </div>

        <div class="top-description">
            거래대금 순위 기준
        </div>

        <div class="top-header">

            <div>순위</div>
            <div>코인</div>
            <div>24H 거래대금</div>
            <div>현재가</div>
            <div>당일</div>
            <div>Signal</div>

        </div>

        {rows_html}

    </section>
    """


# =========================================================
# BTC HTML
# =========================================================

def render_btc_section():

    if latest_btc_price is None:

        price_text = "-"
        change_text = "-"
        status_text = "BTC 데이터 확인 중"
        status_class = "off"

    else:

        price_text = format_price(
            latest_btc_price
        )

        change_text = format_change(
            latest_btc_daily_change
        )

        if (
            latest_btc_daily_change is not None
            and latest_btc_daily_change > 0
        ):

            status_text = "SIGNAL ON"
            status_class = "on"

        else:

            status_text = "SIGNAL OFF"
            status_class = "off"

    return f"""
    <section class="btc-card">

        <div class="btc-title">
            ₿ BTC MARKET
        </div>

        <div class="btc-grid">

            <div>
                <div class="btc-label">
                    OKX BTC-USDT
                </div>

                <div class="btc-price">
                    {price_text}
                </div>
            </div>

            <div>
                <div class="btc-label">
                    KST 당일
                </div>

                <div class="btc-change">
                    {change_text}
                </div>
            </div>

            <div>
                <div class="btc-label">
                    Signal Filter
                </div>

                <div class="btc-status {status_class}">
                    {status_text}
                </div>
            </div>

        </div>

        <div class="btc-note">
            BTC 일봉 기준: KST 09:00 ~ 다음날 08:59
        </div>

    </section>
    """


# =========================================================
# 전체 Dashboard HTML
# =========================================================

def build_dashboard():

    now_text = (
        datetime.now(KST)
        .strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )

    btc_html = (
        render_btc_section()
    )

    signal_html = (
        render_signal_section()
    )

    top_html = (
        render_top_section()
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

<title>Crypto Dashboard</title>

<style>

* {{
    box-sizing: border-box;
}}

body {{

    margin: 0;

    background: #080c11;

    color: #e8edf3;

    font-family:
        Arial,
        "Noto Sans KR",
        sans-serif;

    font-size: 14px;
}}

.container {{

    width: 100%;

    max-width: 1100px;

    margin: 0 auto;

    padding: 12px;
}}

.header {{

    display: flex;

    justify-content: space-between;

    align-items: center;

    margin-bottom: 12px;

    padding: 10px 4px;
}}

.title {{

    font-size: 20px;

    font-weight: 800;
}}

.update-time {{

    color: #8d98a6;

    font-size: 11px;
}}


/* =====================================================
   BTC
   ===================================================== */

.btc-card {{

    background: #10161e;

    border: 1px solid #202a35;

    border-radius: 12px;

    padding: 14px;

    margin-bottom: 12px;
}}

.btc-title {{

    font-size: 17px;

    font-weight: 800;

    margin-bottom: 12px;
}}

.btc-grid {{

    display: grid;

    grid-template-columns:
        1.4fr
        1fr
        1fr;

    gap: 10px;
}}

.btc-grid > div {{

    background: #0b1016;

    border-radius: 8px;

    padding: 10px;
}}

.btc-label {{

    color: #7f8b98;

    font-size: 11px;

    margin-bottom: 5px;
}}

.btc-price {{

    font-size: 19px;

    font-weight: 800;
}}

.btc-change {{

    font-size: 18px;

    font-weight: 800;
}}

.btc-status {{

    font-size: 16px;

    font-weight: 800;
}}

.btc-note {{

    margin-top: 10px;

    color: #697582;

    font-size: 11px;
}}


/* =====================================================
   Section
   ===================================================== */

.section {{

    background: #10161e;

    border: 1px solid #202a35;

    border-radius: 12px;

    padding: 12px;

    margin-bottom: 12px;

    overflow: hidden;
}}

.section-title {{

    font-size: 17px;

    font-weight: 800;

    margin-bottom: 6px;
}}

.top-description,
.signal-description {{

    color: #7d8996;

    font-size: 11px;

    margin-bottom: 10px;
}}


/* =====================================================
   Signal
   ===================================================== */

.signal-status {{

    padding: 9px 10px;

    border-radius: 8px;

    margin-bottom: 10px;

    font-size: 12px;

    font-weight: 700;
}}

.signal-status.on {{

    background: rgba(0, 190, 100, 0.10);

    border: 1px solid
        rgba(0, 190, 100, 0.25);
}}

.signal-status.off {{

    background: rgba(255, 80, 80, 0.10);

    border: 1px solid
        rgba(255, 80, 80, 0.25);
}}

.signal-empty {{

    padding: 16px;

    text-align: center;

    color: #697582;

    font-size: 12px;
}}

.signal-header,
.signal-row {{

    display: grid;

    grid-template-columns:
        9%
        9%
        19%
        16%
        18%
        17%
        12%;

    align-items: center;
}}

.signal-header {{

    color: #697582;

    font-size: 10px;

    padding: 7px 4px;

    border-bottom: 1px solid #202a35;

    text-align: center;
}}

.signal-row {{

    min-height: 48px;

    border-bottom: 1px solid #171f28;

    font-size: 12px;

    text-align: center;
}}

.signal-row:last-child {{

    border-bottom: none;
}}

.signal-rank {{

    font-size: 15px;

    font-weight: 900;

    color: #ffd84d;
}}

.top-rank {{

    color: #8d98a6;

    font-size: 11px;
}}

.signal-row .coin-name {{

    text-align: left;

    font-weight: 800;
}}

.signal-row .volume,
.signal-row .price {{

    font-weight: 700;
}}

.signal-icon {{

    font-size: 17px;
}}


/* =====================================================
   TOP
   ===================================================== */

.top-header,
.coin-row {{

    display: grid;

    grid-template-columns:
        7%
        20%
        19%
        20%
        19%
        15%;

    align-items: center;
}}

.top-header {{

    color: #697582;

    font-size: 10px;

    padding: 7px 4px;

    border-bottom: 1px solid #202a35;

    text-align: center;
}}

.coin-row {{

    min-height: 46px;

    border-bottom: 1px solid #171f28;

    text-align: center;

    font-size: 12px;
}}

.coin-row:last-child {{

    border-bottom: none;
}}

.coin-row .rank {{

    color: #7f8b98;

    font-weight: 700;
}}

.coin-row .coin-name {{

    text-align: left;

    font-weight: 800;
}}

.coin-row .volume,
.coin-row .price {{

    font-weight: 700;
}}

.signal-cell {{

    font-size: 16px;
}}


/* =====================================================
   Color
   ===================================================== */

.positive {{

    color: #36d68a;

    font-weight: 800;
}}

.negative {{

    color: #ff6874;

    font-weight: 800;
}}

.neutral {{

    color: #9aa4af;
}}

.on {{

    color: #36d68a;
}}

.off {{

    color: #ff6874;
}}


/* =====================================================
   Mobile
   ===================================================== */

@media (
    max-width: 700px
) {{

    .container {{
        padding: 8px;
    }}

    .title {{
        font-size: 18px;
    }}

    .btc-grid {{
        grid-template-columns:
            1fr 1fr;
    }}

    .btc-grid > div:last-child {{
        grid-column:
            1 / -1;
    }}

    .btc-price {{
        font-size: 17px;
    }}

    .btc-change {{
        font-size: 16px;
    }}

    .signal-header,
    .signal-row {{

        grid-template-columns:
            10%
            11%
            20%
            16%
            17%
            17%
            9%;

        font-size: 10px;
    }}

    .signal-row {{
        font-size: 10px;
    }}

    .signal-row .coin-name {{
        font-size: 11px;
    }}

    .top-header,
    .coin-row {{

        grid-template-columns:
            8%
            20%
            20%
            20%
            20%
            12%;
    }}

    .coin-row {{
        font-size: 10px;
    }}

    .coin-row .coin-name {{
        font-size: 11px;
    }}

    .signal-icon {{
        font-size: 14px;
    }}
}}


@media (
    max-width: 400px
) {{

    body {{
        font-size: 12px;
    }}

    .section {{
        padding: 9px;
    }}

    .btc-grid {{
        gap: 6px;
    }}

    .btc-grid > div {{
        padding: 8px;
    }}

    .signal-header,
    .signal-row {{

        grid-template-columns:
            11%
            11%
            21%
            15%
            16%
            17%
            9%;
    }}

    .top-header,
    .coin-row {{

        grid-template-columns:
            8%
            20%
            19%
            21%
            20%
            12%;
    }}
}}

</style>

</head>


<body>

<div class="container">

    <div class="header">

        <div class="title">
            📊 CRYPTO DASHBOARD
        </div>

        <div class="update-time">
            {now_text} KST
        </div>

    </div>

    {btc_html}

    {signal_html}

    {top_html}

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

    return build_dashboard()


# =========================================================
# Dashboard 업데이트
# =========================================================

def update_dashboard():

    global last_update_time

    try:

        logging.info(
            "========== Dashboard 업데이트 시작 =========="
        )

        # -------------------------------------------------
        # 중요:
        # BTC를 먼저 업데이트한다.
        #
        # 그래야 바로 이어지는 업비트 Signal 계산에서
        # 최신 BTC 당일 상승률을 사용할 수 있다.
        # -------------------------------------------------

        update_btc_market()

        # 그 다음 업비트 TOP / Signal
        if USE_UPBIT == "Y":

            update_upbit()

        last_update_time = (
            datetime.now(KST)
        )

        logging.info(
            "========== Dashboard 업데이트 완료 =========="
        )

    except Exception as e:

        logging.exception(
            "Dashboard 업데이트 오류: %s",
            e
        )


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

if __name__ == "__main__":

    logging.info(
        "Crypto Dashboard 시작"
    )

    # 최초 데이터 업데이트
    update_dashboard()

    # 스케줄러
    scheduler_thread = threading.Thread(
        target=scheduler_loop,
        daemon=True
    )

    scheduler_thread.start()

    # FastAPI
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )
