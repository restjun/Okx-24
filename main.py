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
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s"
)

log = logging.getLogger("trading")

KST = ZoneInfo("Asia/Seoul")


# =========================================================
# 기본 설정
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
# 전역
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
# BTC 시황
#
# ROC는 완전히 삭제
#
# BTC는 현재가 + 일봉 변동률만 표시
# Signal 필터에는 사용하지 않음
# =========================================================

OKX_BASE_URL = "https://www.okx.com"

OKX_BTC_INST_ID = "BTC-USDT"

latest_btc_okx_price = None

latest_btc_daily_change = None


# =========================================================
# 시간
# =========================================================

def kst():

    return datetime.now(
        KST
    ).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def format_timeframe(minutes):

    minutes = int(minutes)

    if minutes == 1440:
        return "1D"

    if minutes >= 60:
        return f"{minutes // 60}H"

    return f"{minutes}M"


# =========================================================
# 업비트 기준 현재 캔들 시작시간
# =========================================================

def get_current_candle_start(minutes):

    minutes = int(minutes)

    now = datetime.now(KST)

    # =====================================================
    # 업비트 240분봉
    #
    # 01 / 05 / 09 / 13 / 17 / 21
    # =====================================================

    if minutes == 240:

        if now.hour == 0:

            current = now.replace(
                hour=21,
                minute=0,
                second=0,
                microsecond=0
            ) - timedelta(days=1)

            return current.replace(
                tzinfo=None
            )

        start_hour = (
            1
            +
            (
                (now.hour - 1) // 4
            )
            * 4
        )

        current = now.replace(
            hour=start_hour,
            minute=0,
            second=0,
            microsecond=0
        )

        return current.replace(
            tzinfo=None
        )

    # =====================================================
    # 업비트 일봉
    #
    # 09:00
    # =====================================================

    if minutes == 1440:

        today_0900 = now.replace(
            hour=9,
            minute=0,
            second=0,
            microsecond=0
        )

        if now < today_0900:

            today_0900 -= timedelta(
                days=1
            )

        return today_0900.replace(
            tzinfo=None
        )

    # =====================================================
    # 기타 분봉
    # =====================================================

    total_minutes = (
        now.hour * 60
        +
        now.minute
    )

    block = (
        total_minutes // minutes
    ) * minutes

    current = now.replace(
        hour=block // 60,
        minute=block % 60,
        second=0,
        microsecond=0
    )

    return current.replace(
        tzinfo=None
    )


# =========================================================
# API 요청
# =========================================================

def wait_request():

    global last_request_time

    with request_lock:

        gap = (
            time.monotonic()
            -
            last_request_time
        )

        if gap < REQUEST_INTERVAL:

            time.sleep(
                REQUEST_INTERVAL
                -
                gap
            )

        last_request_time = (
            time.monotonic()
        )


def retry(
    func,
    *args,
    **kwargs
):

    url = (
        args[0]
        if (
            args
            and
            isinstance(
                args[0],
                str
            )
        )
        else kwargs.get(
            "url",
            ""
        )
    )

    for n in range(
        MAX_RETRIES
    ):

        try:

            wait_request()

            response = func(
                *args,
                **kwargs
            )

            if not hasattr(
                response,
                "status_code"
            ):
                return response

            if response.status_code == 200:
                return response

            if response.status_code == 429:

                wait = min(
                    RATE_LIMIT_WAIT
                    * 2 ** n,
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
                    min(
                        2 * (n + 1),
                        20
                    )
                )

    return None


# =========================================================
# UPBIT 마켓
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
            if x.get(
                "market",
                ""
            ).startswith("KRW-")
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
                    "markets":
                        ",".join(chunk)
                },
                timeout=15
            )

            if ticker_response is None:
                continue

            try:

                data = (
                    ticker_response.json()
                )

            except Exception:

                continue

            if isinstance(
                data,
                list
            ):

                ticker_result.extend(
                    data
                )

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

            if (
                volume > 0
                and
                price > 0
            ):

                result.append({

                    "market":
                        market,

                    "volume_24h":
                        volume,

                    "current_price":
                        price

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
# UPBIT 일봉 변동률
#
# 현재 진행 중인 업비트 일봉 기준
#
# 현재가 / 전일 종가
# =========================================================

def daily_change_upbit(
    market,
    current_price=None
):

    response = retry(
        requests.get,
        "https://api.upbit.com/v1/candles/days",
        params={
            "market":
                market,

            "count":
                2
        },
        timeout=15
    )

    if response is None:
        return None

    try:

        data = response.json()

        if not isinstance(
            data,
            list
        ):

            return None

        if len(data) < 2:

            return None

        current_candle = data[0]

        previous_candle = data[1]

        if current_price is None:

            current_price = float(
                current_candle[
                    "trade_price"
                ]
            )

        else:

            current_price = float(
                current_price
            )

        previous_close = float(
            previous_candle[
                "trade_price"
            ]
        )

        if previous_close == 0:

            return None

        return (
            (
                current_price
                -
                previous_close
            )
            /
            previous_close
            *
            100
        )

    except Exception as e:

        log.warning(
            f"업비트 일봉 변동률 오류 "
            f"{market}: {e}"
        )

        return None


# =========================================================
# BTC OKX 현재가
#
# BTC는 시황 표시용
# =========================================================

def get_okx_btc_price():

    response = retry(
        requests.get,
        f"{OKX_BASE_URL}/api/v5/market/ticker",
        params={
            "instId":
                OKX_BTC_INST_ID
        },
        timeout=15
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
            f"OKX BTC 현재가 오류: {e}"
        )

        return None


# =========================================================
# BTC OKX 일봉 변동률
#
# OKX BTC-USDT
# KST 09:00 기준
# =========================================================

def get_okx_btc_daily_change():

    global latest_btc_okx_price

    price = get_okx_btc_price()

    if price is None:

        return None

    latest_btc_okx_price = price

    response = retry(
        requests.get,
        f"{OKX_BASE_URL}/api/v5/market/candles",
        params={
            "instId":
                OKX_BTC_INST_ID,

            "bar":
                "1D",

            "limit":
                "3"
        },
        timeout=15
    )

    if response is None:

        return None

    try:

        payload = response.json()

        if payload.get("code") != "0":

            return None

        data = payload.get(
            "data",
            []
        )

        if len(data) < 2:

            return None

        rows = []

        for item in data:

            if len(item) < 5:

                continue

            rows.append({

                "ts":
                    int(item[0]),

                "o":
                    float(item[1]),

                "h":
                    float(item[2]),

                "l":
                    float(item[3]),

                "c":
                    float(item[4])

            })

        if len(rows) < 2:

            return None

        df = pd.DataFrame(
            rows
        )

        df["datetime_utc"] = (
            pd.to_datetime(
                df["ts"],
                unit="ms",
                utc=True
            )
        )

        df["datetime"] = (
            df["datetime_utc"]
            .dt
            .tz_convert(KST)
            .dt
            .tz_localize(None)
        )

        df = (
            df
            .sort_values("datetime")
            .reset_index(drop=True)
        )

        # =================================================
        # 주의
        #
        # OKX 기본 1D 캔들은 UTC 기준이므로
        # 업비트 09:00 기준 일봉과 동일하지 않음.
        #
        # 따라서 여기서는 BTC 시황 표시용으로만 사용.
        # Signal에는 사용하지 않음.
        # =================================================

        previous_close = float(
            df.iloc[-2]["c"]
        )

        if previous_close == 0:

            return None

        return (
            (
                price
                -
                previous_close
            )
            /
            previous_close
            *
            100
        )

    except Exception as e:

        log.warning(
            f"OKX BTC 일봉 변동률 오류: {e}"
        )

        return None


# =========================================================
# BTC 시황 업데이트
# =========================================================

def update_btc_market():

    global latest_btc_okx_price

    global latest_btc_daily_change

    price = get_okx_btc_price()

    if price is not None:

        latest_btc_okx_price = price

    change = (
        get_okx_btc_daily_change()
    )

    if change is not None:

        latest_btc_daily_change = change

    log.info(
        f"[BTC OKX] "
        f"가격={latest_btc_okx_price} | "
        f"일봉={latest_btc_daily_change}"
    )


# =========================================================
# 코인 분석
#
# ROC 완전 삭제
#
# 업비트 일봉 상승률만 계산
# =========================================================

def analyze(
    market,
    current_price
):

    change_value = (
        daily_change_upbit(
            market,
            current_price
        )
    )

    daily_pass = (
        change_value is not None
        and
        change_value > 0
    )

    return {

        "changes":
            change_value,

        "daily_pass":
            daily_pass

    }


# =========================================================
# ROW
# =========================================================

def make_row(
    rank,
    name,
    volume,
    analysis,
    current_price=None
):

    a = analysis or {}

    change_value = (
        get_change_value(
            a.get(
                "changes"
            )
        )
    )

    return {

        "rank":
            rank,

        "name":
            name,

        "change":
            format_change(
                change_value
            ),

        "change_value":
            change_value,

        "daily_pass":
            bool(
                a.get(
                    "daily_pass",
                    False
                )
            ),

        "volume":
            format_volume(
                volume
            ),

        "current_price":
            current_price,

        "analysis":
            analysis

    }


# =========================================================
# UPBIT TOP 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data

    global latest_upbit_update_time

    markets = sorted(
        get_upbit_markets(),
        key=lambda x:
            x["volume_24h"],
        reverse=True
    )

    top_markets = markets[
        :TOP_N
    ]

    if not top_markets:

        latest_upbit_data = []

        latest_upbit_update_time = (
            kst()
        )

        return

    rows = []

    for rank, item in enumerate(
        top_markets,
        1
    ):

        market = item[
            "market"
        ]

        coin = market.replace(
            "KRW-",
            ""
        )

        price = item[
            "current_price"
        ]

        try:

            analysis = analyze(
                market,
                price
            )

        except Exception as e:

            log.exception(
                f"분석 오류 {market}: {e}"
            )

            analysis = None

        row = make_row(

            rank,

            coin,

            item[
                "volume_24h"
            ],

            analysis,

            price

        )

        rows.append(
            row
        )

    latest_upbit_data = rows

    latest_upbit_update_time = (
        kst()
    )

    # =====================================================
    # Signal 로그
    # =====================================================

    signal_rows = [

        x

        for x in rows

        if (
            x.get(
                "change_value"
            ) is not None

            and

            x.get(
                "change_value"
            ) > 0
        )

    ]

    signal_rows.sort(
        key=lambda x:
            x["change_value"],
        reverse=True
    )

    signal_text = ", ".join(

        f"{x['name']} "
        f"{x['change_value']:+.2f}%"

        for x in signal_rows[:10]

    )

    log.info(
        f"TOP{TOP_N} 업데이트 완료 | "
        f"Signal 상승률순 = "
        f"{signal_text if signal_text else '없음'}"
    )


# =========================================================
# USDT
# =========================================================

def get_usdt_krw_internal():

    response = retry(
        requests.get,
        "https://api.upbit.com/v1/ticker",
        params={
            "markets":
                "KRW-USDT"
        },
        timeout=15
    )

    if response is None:

        return None

    try:

        data = response.json()

        if not data:

            return None

        return float(
            data[0]["trade_price"]
        )

    except Exception:

        return None


# =========================================================
# OKX 기존 영역
#
# 현재 USE_OKX = N
# =========================================================

def update_okx(
    usdt
):

    global latest_okx_data

    global latest_okx_update_time

    latest_okx_data = []

    latest_okx_update_time = (
        kst()
    )

    return True


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    if not update_lock.acquire(
        False
    ):

        return

    try:

        if USE_UPBIT == "Y":

            update_upbit()

        update_btc_market()

        if USE_OKX == "Y":

            usdt = (
                get_usdt_krw_internal()
            )

            if usdt:

                update_okx(
                    usdt
                )

    except Exception as e:

        log.exception(
            f"전체 업데이트 오류: {e}"
        )

    finally:

        update_lock.release()


# =========================================================
# 가격
# =========================================================

def format_market_price(price):

    if price is None:

        return "-"

    try:

        price = float(
            price
        )

    except Exception:

        return "-"

    if price >= 100000000:

        return (
            f"{price / 100000000:.2f}억"
        )

    if price >= 10000:

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
# 변화값
# =========================================================

def get_change_value(x):

    try:

        if x is None:

            return None

        if isinstance(
            x,
            (list, tuple)
        ):

            if not x:

                return None

            return float(
                x[0]
            )

        return float(x)

    except Exception:

        return None


def format_change(x):

    x = get_change_value(x)

    if x is None:

        return "-"

    if x > 0:

        return (
            '<span class="up">'
            f'▲ +{x:.1f}%'
            '</span>'
        )

    if x < 0:

        return (
            '<span class="down">'
            f'▼ {x:.1f}%'
            '</span>'
        )

    return (
        '<span class="zero">'
        '0.0%'
        '</span>'
    )


# =========================================================
# 거래대금
# =========================================================

def format_volume(v):

    try:

        v = float(v)

    except Exception:

        return "-"

    if v >= 1e12:

        return (
            f"{v / 1e12:.1f}조"
        )

    if v >= 1e8:

        return (
            f"{v / 1e8:.0f}억"
        )

    if v >= 1e4:

        return (
            f"{v / 1e4:.0f}만"
        )

    return (
        f"{v:,.0f}"
    )


# =========================================================
# Signal HTML
#
# 거래대금 TOP15 안에서
# 일봉 상승률 > 0%인 종목만
# 상승률 높은 순으로 표시
# =========================================================

def signal_item_html(
    row
):

    if not row:

        return "-"

    change_value = (
        get_change_value(
            row.get(
                "change_value"
            )
        )
    )

    if (
        change_value is None
        or
        change_value <= 0
    ):

        return "-"

    coin = html.escape(
        str(
            row.get(
                "name",
                "-"
            )
        )
    )

    return f"""

    <div class="signal-rank-item">

        <div class="signal-rank-number">
            {row.get("signal_rank", "-")}
        </div>

        <div class="signal-coin">
            {coin}
        </div>

        <div class="signal-change">
            ▲ +{change_value:.1f}%
        </div>

    </div>

    """


# =========================================================
# Signal 전체
# =========================================================

def signal_html(row):

    return signal_item_html(
        row
    )


# =========================================================
# Signal 집중 영역
#
# TOP15 중 상승률 > 0
# 상승률 높은 순
# =========================================================

def focus_section(data):

    signal_rows = [

        x.copy()

        for x in data

        if (
            get_change_value(
                x.get(
                    "change_value"
                )
            ) is not None

            and

            get_change_value(
                x.get(
                    "change_value"
                )
            ) > 0
        )

    ]

    signal_rows.sort(
        key=lambda x:
            x["change_value"],
        reverse=True
    )

    for signal_rank, row in enumerate(
        signal_rows,
        1
    ):

        row[
            "signal_rank"
        ] = signal_rank

    if not signal_rows:

        signal_table = """

        <div class="signal-empty">
            현재 0% 초과 상승 종목 없음
        </div>

        """

    else:

        signal_items = []

        for row in signal_rows:

            signal_items.append(
                signal_item_html(
                    row
                )
            )

        signal_table = f"""

        <div class="signal-list">

            {"".join(signal_items)}

        </div>

        """

    return f"""

    <div class="unified-section">

        <div class="section-title-card">

            <div class="section-number">

                🚀

            </div>

            <div class="section-heading">

                <div class="section-heading-main">

                    Signal

                </div>

                <div class="section-heading-sub">

                    업비트 거래대금 TOP{TOP_N}

                    ·

                    일봉 상승률 0% 초과

                    ·

                    상승률 높은 순

                </div>

            </div>

            <div class="section-time">

                {kst()} KST

            </div>

        </div>

        {signal_table}

    </div>

    """


# =========================================================
# TOP
# =========================================================

def section(
    data,
    update_time
):

    return f"""

    <div class="unified-section">

        <div class="section-title-card">

            <div class="section-number top-number">

                🏆

            </div>

            <div class="section-heading">

                <div class="section-heading-main">

                    업비트 TOP{TOP_N}

                </div>

                <div class="section-heading-sub">

                    24시간 거래대금 순위

                    ·

                    일봉 상승률 표시

                    ·

                    Signal은 상승률 순으로 별도 정렬

                </div>

            </div>

            <div class="section-time">

                {update_time} KST

            </div>

        </div>

        {table_html(data)}

    </div>

    """


# =========================================================
# 시장 요약
# =========================================================

def market_summary_html():

    price = format_market_price(
        latest_btc_okx_price
    )

    if latest_btc_daily_change is not None:

        change = format_change(
            latest_btc_daily_change
        )

    else:

        change = "-"

    return f"""

    <div class="market-card">

        <div class="market-card-header">

            <div class="market-title-block">

                <div class="market-title-main">

                    ₿ BTC 시장 시황

                </div>

                <div class="market-title-sub">

                    데이터:
                    OKX BTC-USDT

                    ·

                    현재가 및 일봉 변동률

                    ·

                    Signal 필터에는 사용하지 않음

                </div>

            </div>

            <div class="market-time">

                {kst()} KST

            </div>

        </div>

        <div class="btc-main-row">

            <div class="btc-name">

                ₿ BTC

            </div>

            <div class="btc-price">

                {price}

            </div>

            <div class="btc-change">

                {change}

            </div>

            <div class="btc-signal-box">

                <span class="btc-info">

                    SIGNAL

                </span>

            </div>

        </div>

    </div>

    """


# =========================================================
# ROW HTML
# =========================================================

def rows_html(data):

    out = []

    for x in data:

        price = format_market_price(
            x.get(
                "current_price"
            )
        )

        change = x.get(
            "change",
            "-"
        )

        coin_name = html.escape(
            str(
                x.get(
                    "name",
                    "-"
                )
            )
        )

        volume = x.get(
            "volume",
            "-"
        )

        out.append(

            f"""

            <div class="coin-card">

                <div class="coin-main-row">

                    <div class="rank-cell">

                        {x.get("rank", "-")}

                    </div>

                    <div class="coin-cell">

                        <span class="coin-name">

                            {coin_name}

                        </span>

                    </div>

                    <div class="volume-cell">

                        <span class="volume-value">

                            {volume}

                        </span>

                    </div>

                    <div class="price-cell">

                        <span class="price-value">

                            {price}

                        </span>

                    </div>

                    <div class="change-cell">

                        {change}

                    </div>

                    <div class="signal-cell">

                        -

                    </div>

                </div>

            </div>

            """

        )

    return "".join(
        out
    )


# =========================================================
# 테이블
# =========================================================

def table_html(data):

    rows = rows_html(
        data
    )

    if not rows:

        rows = """

        <div class="empty-card">

            현재 데이터 없음

        </div>

        """

    return f"""

    <div class="card-list">

        {rows}

    </div>

    """


# =========================================================
# CSS
# =========================================================

CSS = """

*{
box-sizing:border-box;
-webkit-tap-highlight-color:transparent;
}

html,
body{
margin:0;
padding:0;
width:100%;
overflow-x:hidden;
}

body{
background:#080c11;
color:#e7ebef;

font-family:
    -apple-system,
    BlinkMacSystemFont,
    "Segoe UI",
    Arial,
    sans-serif;

font-size:9px;
padding:12px;
}

h1{
margin:3px 4px 10px;
color:#eef2f5;
font-size:15px;
line-height:18px;
font-weight:900;
}

.unified-section{
width:100%;
margin:10px 0 12px;
}

.section-title-card{
display:flex;
align-items:center;
width:100%;
min-height:48px;
padding:7px 10px;
background:#10151b;
border:2px solid #252e38;
border-radius:12px;
box-shadow:
inset 0 0 18px
rgba(255,255,255,.018);
overflow:hidden;
}

.section-number{
flex:none;
display:flex;
align-items:center;
justify-content:center;
width:34px;
height:34px;
margin-right:9px;
border-radius:8px;
background:#18251f;
border:1px solid #315a48;
color:#82d5a8;
font-size:16px;
font-weight:900;
}

.top-number{
background:#1d1a13;
border-color:#665331;
color:#e0bd6d;
font-size:14px;
}

.section-heading{
min-width:0;
flex:1;
overflow:hidden;
}

.section-heading-main{
color:#e9edf1;
font-size:12px;
line-height:15px;
font-weight:900;
white-space:nowrap;
}

.section-heading-sub{
margin-top:2px;
color:#87919b;
font-size:7px;
line-height:10px;
font-weight:700;
white-space:nowrap;
overflow:hidden;
text-overflow:ellipsis;
}

.section-time{
flex:none;
margin-left:8px;
color:#68737e;
font-size:6.5px;
font-weight:800;
white-space:nowrap;
}


/* =========================================================
   BTC
   ========================================================= */

.market-card{
width:100%;
margin:3px 0 12px;
background:#0f141a;
border:2px solid #252e38;
border-radius:13px;
overflow:hidden;
box-shadow:
inset 0 0 20px
rgba(255,255,255,.018);
}

.market-card-header{
display:flex;
align-items:center;
min-height:44px;
padding:7px 10px;
background:#121820;
border-bottom:1px solid #29323c;
}

.market-title-block{
min-width:0;
flex:1;
overflow:hidden;
}

.market-title-main{
color:#eef2f5;
font-size:11px;
line-height:14px;
font-weight:900;
white-space:nowrap;
}

.market-title-sub{
margin-top:2px;
color:#7e8994;
font-size:6.5px;
line-height:9px;
font-weight:700;
white-space:nowrap;
overflow:hidden;
text-overflow:ellipsis;
}

.market-time{
flex:none;
margin-left:8px;
color:#68737e;
font-size:6.5px;
font-weight:800;
white-space:nowrap;
}

.btc-main-row{
display:grid;

grid-template-columns:
    1.1fr
    1.3fr
    1fr
    1.4fr;

align-items:center;
min-height:58px;
background:#11161c;
}

.btc-name{
padding-left:13px;
color:#edf1f4;
font-size:11px;
font-weight:900;
white-space:nowrap;
}

.btc-price{
color:#f1f4f6;
font-size:11px;
font-weight:900;
text-align:center;
white-space:nowrap;
}

.btc-change{
font-size:11px;
font-weight:900;
text-align:center;
white-space:nowrap;
}

.btc-signal-box{
min-height:58px;
display:flex;
align-items:center;
justify-content:center;
border-left:1px solid #29323c;
}

.btc-info{
color:#7f8a94;
font-size:8px;
font-weight:900;
}


/* =========================================================
   Signal
   ========================================================= */

.signal-list{
display:flex;
flex-direction:column;
gap:7px;
margin-top:8px;
}

.signal-rank-item{
display:grid;

grid-template-columns:
    12%
    44%
    44%;

align-items:center;

min-height:48px;

background:#10171d;

border:2px solid #26333c;

border-radius:10px;

box-shadow:
inset 0 0 14px
rgba(255,255,255,.015);
}

.signal-rank-number{
text-align:center;
color:#8a969f;
font-size:10px;
font-weight:900;
}

.signal-coin{
text-align:center;
color:#eef2f5;
font-size:11px;
font-weight:900;
}

.signal-change{
text-align:center;
color:#78cfa2;
font-size:11px;
font-weight:900;
}

.signal-empty{
min-height:52px;
display:flex;
align-items:center;
justify-content:center;

background:#10151b;

border:2px solid #252e38;

border-radius:10px;

color:#59636e;

font-size:8px;
font-weight:800;
}


/* =========================================================
   Coin
   ========================================================= */

.card-list{
width:100%;
display:flex;
flex-direction:column;
gap:9px;
margin-top:8px;
}

.coin-card{
width:100%;
background:#0f141a;
border:2px solid #252e38;
border-radius:12px;
overflow:hidden;
box-shadow:
inset 0 0 18px
rgba(255,255,255,.015);
}

.coin-main-row{
display:grid;

grid-template-columns:
    6%
    18%
    15%
    21%
    14%
    26%;

align-items:center;
min-height:56px;
background:#11161c;
}

.coin-main-row > div{
min-width:0;
height:56px;
display:flex;
align-items:center;
justify-content:center;
overflow:hidden;
}

.rank-cell{
justify-content:flex-start!important;
padding-left:12px;
color:#e2e7eb;
font-size:11px;
font-weight:900;
white-space:nowrap;
}

.coin-cell{
text-align:center;
}

.coin-name{
display:block;
width:100%;
padding:0 3px;
color:#eef2f5;
font-size:11px;
line-height:14px;
font-weight:900;
white-space:nowrap;
overflow:hidden;
text-overflow:ellipsis;
text-align:center;
}

.volume-cell{
text-align:center;
}

.volume-value{
display:block;
width:100%;
color:#f1f4f6;
font-size:10px;
line-height:13px;
font-weight:900;
white-space:nowrap;
overflow:hidden;
text-overflow:ellipsis;
text-align:center;
}

.price-cell{
text-align:center;
}

.price-value{
display:block;
width:100%;
color:#eef2f5;
font-size:10px;
line-height:13px;
font-weight:900;
white-space:nowrap;
overflow:hidden;
text-overflow:ellipsis;
text-align:center;
}

.change-cell{
text-align:center;
font-size:10px;
font-weight:900;
white-space:nowrap;
}

.signal-cell{
height:56px!important;
border-left:1px solid #29323c;
background:#0e141a;
text-align:center;
overflow:hidden!important;
color:#59636e;
font-size:9px;
font-weight:800;
}


/* =========================================================
   색상
   ========================================================= */

.up{
color:#78cfa2!important;
font-weight:900;
}

.down{
color:#df8588!important;
font-weight:900;
}

.zero{
color:#727c86!important;
}

.empty-card{
min-height:56px;
display:flex;
align-items:center;
justify-content:center;
background:#10151b;
border:2px solid #252e38;
border-radius:12px;
color:#59636e;
font-size:8px;
font-weight:800;
}


/* =========================================================
   모바일
   ========================================================= */

@media(max-width:600px){

body{
    padding:7px;
}

h1{
    margin:3px 3px 8px;
    font-size:13px;
    line-height:16px;
}

.unified-section{
    margin:8px 0 10px;
}

.section-title-card{
    min-height:40px;
    padding:5px 6px;
    border-radius:9px;
}

.section-number{
    width:27px;
    height:27px;
    margin-right:6px;
    border-radius:6px;
    font-size:12px;
}

.top-number{
    font-size:11px;
}

.section-heading-main{
    font-size:9px;
    line-height:11px;
}

.section-heading-sub{
    margin-top:1px;
    font-size:5px;
    line-height:7px;
}

.section-time{
    margin-left:4px;
    font-size:5px;
}


/* BTC */

.market-card{
    margin:3px 0 9px;
    border-radius:9px;
}

.market-card-header{
    min-height:36px;
    padding:5px 7px;
}

.market-title-main{
    font-size:8px;
    line-height:10px;
}

.market-title-sub{
    margin-top:1px;
    font-size:4.8px;
    line-height:6px;
}

.market-time{
    margin-left:4px;
    font-size:4.8px;
}

.btc-main-row{
    min-height:43px;

    grid-template-columns:
        1.1fr
        1.3fr
        1fr
        1.3fr;
}

.btc-name{
    padding-left:8px;
    font-size:8px;
}

.btc-price{
    font-size:8px;
}

.btc-change{
    font-size:8px;
}

.btc-signal-box{
    min-height:43px;
}

.btc-info{
    font-size:6px;
}


/* Signal */

.signal-list{
    gap:5px;
    margin-top:6px;
}

.signal-rank-item{
    min-height:38px;

    grid-template-columns:
        12%
        44%
        44%;

    border-width:1px;
    border-radius:7px;
}

.signal-rank-number{
    font-size:6.5px;
}

.signal-coin{
    font-size:7px;
}

.signal-change{
    font-size:7px;
}

.signal-empty{
    min-height:43px;
    border-width:1px;
    border-radius:8px;
    font-size:6px;
}


/* Coin */

.card-list{
    gap:6px;
    margin-top:6px;
}

.coin-card{
    border-width:1px;
    border-radius:8px;
}

.coin-main-row{

    min-height:40px;

    grid-template-columns:
        6%
        18%
        15%
        21%
        14%
        26%;
}

.coin-main-row > div{
    height:40px;
}

.rank-cell{
    padding-left:5px;
    font-size:6.8px;
}

.coin-name{
    padding:0 1px;
    font-size:6.8px;
    line-height:9px;
}

.volume-value{
    font-size:5.5px;
    line-height:8px;
}

.price-value{
    font-size:5.8px;
    line-height:8px;
}

.change-cell{
    font-size:5.8px;
}

.signal-cell{
    height:40px!important;
    border-left:1px solid #29323c;
    font-size:5.5px;
}

.empty-card{
    min-height:43px;
    border-width:1px;
    border-radius:8px;
    font-size:6px;
}

}


@media(max-width:380px){

body{
    padding:4px;
}

h1{
    font-size:12px;
    line-height:15px;
    margin:2px 2px 6px;
}

.section-title-card{
    min-height:35px;
    padding:4px 5px;
}

.section-number{
    width:23px;
    height:23px;
    margin-right:4px;
    font-size:10px;
}

.section-heading-main{
    font-size:8px;
    line-height:10px;
}

.section-heading-sub{
    font-size:4.2px;
    line-height:6px;
}

.section-time{
    font-size:4.2px;
}


/* BTC */

.market-card-header{
    min-height:32px;
    padding:4px 5px;
}

.market-title-main{
    font-size:7px;
}

.market-title-sub{
    font-size:4px;
}

.market-time{
    font-size:4px;
}

.btc-main-row{
    min-height:38px;
}

.btc-name{
    padding-left:6px;
    font-size:7px;
}

.btc-price{
    font-size:7px;
}

.btc-change{
    font-size:7px;
}

.btc-signal-box{
    min-height:38px;
}

.btc-info{
    font-size:5px;
}


/* Signal */

.signal-list{
    gap:4px;
}

.signal-rank-item{
    min-height:34px;
    border-radius:5px;
}

.signal-rank-number{
    font-size:5.5px;
}

.signal-coin{
    font-size:6px;
}

.signal-change{
    font-size:6px;
}

.signal-empty{
    min-height:38px;
    font-size:5px;
}


/* Coin */

.card-list{
    gap:5px;
}

.coin-main-row{
    min-height:36px;

    grid-template-columns:
        6%
        18%
        15%
        21%
        14%
        26%;
}

.coin-main-row > div{
    height:36px;
}

.rank-cell{
    padding-left:3px;
    font-size:6px;
}

.coin-name{
    font-size:6px;
}

.volume-value{
    font-size:5px;
}

.price-value{
    font-size:5.2px;
}

.change-cell{
    font-size:5.1px;
}

.signal-cell{
    height:36px!important;
    font-size:5px;
}

}

"""


# =========================================================
# DASHBOARD
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def dashboard():

    sections = ""

    if USE_UPBIT == "Y":

        sections += (
            focus_section(
                latest_upbit_data
            )
        )

        sections += (
            section(
                latest_upbit_data,
                latest_upbit_update_time
            )
        )

    return f"""

    <!DOCTYPE html>

    <html lang="ko">

    <head>

        <meta charset="UTF-8">

        <meta
            name="viewport"
            content="
                width=device-width,
                initial-scale=1,
                maximum-scale=1,
                user-scalable=no
            "
        >

        <meta
            http-equiv="refresh"
            content="60"
        >

        <meta
            name="theme-color"
            content="#080c11"
        >

        <title>
            TRADING SIGNAL CENTER
        </title>

        <style>

            {CSS}

        </style>

    </head>

    <body>

        <h1>
            📊 TRADING SIGNAL CENTER
        </h1>

        {market_summary_html()}

        {sections}

    </body>

    </html>

    """


# =========================================================
# Scheduler
# =========================================================

def scheduler():

    log.info(
        "스케줄러 시작"
    )

    while True:

        try:

            schedule.run_pending()

        except Exception as e:

            log.exception(
                f"스케줄러 오류: {e}"
            )

        time.sleep(1)


# =========================================================
# 설정 검증
# =========================================================

def validate_settings():

    if TOP_N < 1:

        raise ValueError(
            "TOP_N은 1 이상이어야 합니다."
        )

    if UPDATE_MINUTES < 1:

        raise ValueError(
            "UPDATE_MINUTES는 1 이상이어야 합니다."
        )


# =========================================================
# STARTUP
# =========================================================

@app.on_event(
    "startup"
)
def startup():

    validate_settings()

    log.info(
        "========================================"
    )

    log.info(
        "TRADING SIGNAL SYSTEM START"
    )

    log.info(
        "★ 코인 데이터 = 업비트"
    )

    log.info(
        f"★ TOP 거래대금 = 업비트 TOP {TOP_N}"
    )

    log.info(
        "★ Signal = TOP 거래대금 종목 중"
    )

    log.info(
        "★ Signal = 일봉 상승률 0% 초과"
    )

    log.info(
        "★ Signal = 상승률 높은 순"
    )

    log.info(
        "★ ROC = 완전 삭제"
    )

    log.info(
        "★ ROC COUNT = 완전 삭제"
    )

    log.info(
        "★ 0선 돌파 조건 = 삭제"
    )

    log.info(
        "★ BTC 양수 필터 = 사용하지 않음"
    )

    log.info(
        "----------------------------------------"
    )

    log.info(
        "★ BTC 시장 시황 = OKX BTC-USDT"
    )

    log.info(
        "★ BTC Signal 필터 = 사용하지 않음"
    )

    log.info(
        "========================================"
    )

    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    schedule.every(
        UPDATE_MINUTES
    ).minutes.do(
        update_dashboard
    )

    threading.Thread(
        target=scheduler,
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
