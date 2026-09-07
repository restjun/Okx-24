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

from datetime import datetime
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


# =========================================================
# 사용자 설정
# =========================================================

VOLUME_HOURS = 24
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
# EMA 설정
# =========================================================

EMA_TIMEFRAME = 60

EMA1_FAST = 30
EMA1_MID = 60
EMA1_SLOW = 120

EMA1_MAX_COUNT = 100


# =========================================================
# ROC 설정
# =========================================================

ROC_PERIOD = 10


# =========================================================
# API
# =========================================================

UPBIT_MARKETS_URL = "https://api.upbit.com/v1/market/all"
UPBIT_CANDLES_URL = "https://api.upbit.com/v1/candles/minutes/60"
UPBIT_TICKER_URL = "https://api.upbit.com/v1/ticker"

OKX_INSTRUMENTS_URL = "https://www.okx.com/api/v5/public/instruments"
OKX_CANDLES_URL = "https://www.okx.com/api/v5/market/candles"


# =========================================================
# 전역 데이터
# =========================================================

latest_data = {
    "upbit": [],
    "okx": []
}

latest_update_time = "-"

data_lock = threading.Lock()


# =========================================================
# 공통 요청 함수
# =========================================================

def request_get(
    url,
    params=None,
    headers=None,
    timeout=10
):
    for attempt in range(MAX_RETRIES):

        try:
            response = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=timeout
            )

            if response.status_code == 200:
                return response.json()

            if response.status_code == 429:
                logging.warning(
                    "Rate limit 발생. %s초 대기",
                    RATE_LIMIT_WAIT
                )
                time.sleep(RATE_LIMIT_WAIT)
                continue

            logging.warning(
                "HTTP 오류 %s | %s",
                response.status_code,
                url
            )

        except Exception as e:
            logging.warning(
                "요청 오류 %s/%s | %s",
                attempt + 1,
                MAX_RETRIES,
                e
            )

        time.sleep(REQUEST_INTERVAL)

    return None


# =========================================================
# Upbit 마켓
# =========================================================

def get_upbit_markets():

    data = request_get(
        UPBIT_MARKETS_URL
    )

    if not data:
        return []

    markets = []

    for item in data:

        market = item.get("market", "")

        if not market.startswith("KRW-"):
            continue

        markets.append({
            "market": market,
            "coin": market.replace("KRW-", ""),
            "korean_name": item.get(
                "korean_name",
                ""
            )
        })

    return markets


# =========================================================
# Upbit 거래대금
# =========================================================

def get_upbit_top_markets():

    markets = get_upbit_markets()

    if not markets:
        return []

    market_codes = [
        x["market"]
        for x in markets
    ]

    result = []

    for i in range(
        0,
        len(market_codes),
        100
    ):

        batch = market_codes[i:i + 100]

        data = request_get(
            UPBIT_TICKER_URL,
            params={
                "markets": ",".join(batch)
            }
        )

        if not data:
            continue

        for item in data:

            result.append({
                "market": item["market"],
                "coin": item["market"].replace(
                    "KRW-",
                    ""
                ),
                "trade_price": float(
                    item.get(
                        "trade_price",
                        0
                    )
                ),
                "acc_trade_price_24h": float(
                    item.get(
                        "acc_trade_price_24h",
                        0
                    )
                ),
                "change_rate": float(
                    item.get(
                        "signed_change_rate",
                        0
                    )
                ) * 100
            })

        time.sleep(REQUEST_INTERVAL)

    result.sort(
        key=lambda x: x["acc_trade_price_24h"],
        reverse=True
    )

    return result[:TOP_N]


# =========================================================
# Upbit 캔들
# =========================================================

def get_upbit_candles(
    market,
    count=200
):

    data = request_get(
        UPBIT_CANDLES_URL,
        params={
            "market": market,
            "count": count
        }
    )

    if not data:
        return pd.DataFrame()

    rows = []

    for item in data:

        rows.append({
            "time": pd.to_datetime(
                item["candle_date_time_kst"]
            ),
            "open": float(
                item["opening_price"]
            ),
            "high": float(
                item["high_price"]
            ),
            "low": float(
                item["low_price"]
            ),
            "close": float(
                item["trade_price"]
            ),
            "volume": float(
                item["candle_acc_trade_volume"]
            )
        })

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df = df.sort_values("time").reset_index(
        drop=True
    )

    return df


# =========================================================
# EMA 계산
# =========================================================

def calculate_ema(
    df,
    period
):

    return df["close"].ewm(
        span=period,
        adjust=False
    ).mean()


# =========================================================
# EMA 방향
# =========================================================

def get_ema_direction(
    df
):

    if df.empty:
        return "none"

    if len(df) < EMA1_SLOW:
        return "none"

    ema_fast = calculate_ema(
        df,
        EMA1_FAST
    )

    ema_mid = calculate_ema(
        df,
        EMA1_MID
    )

    ema_slow = calculate_ema(
        df,
        EMA1_SLOW
    )

    f = ema_fast.iloc[-1]
    m = ema_mid.iloc[-1]
    s = ema_slow.iloc[-1]

    if f > m > s:
        return "long"

    if f < m < s:
        return "short"

    return "none"


# =========================================================
# EMA 정배열 지속 카운트
# =========================================================

def get_ema_alignment_count(
    df,
    direction
):

    if df.empty:
        return 0

    if len(df) < EMA1_SLOW:
        return 0

    ema_fast = calculate_ema(
        df,
        EMA1_FAST
    )

    ema_mid = calculate_ema(
        df,
        EMA1_MID
    )

    ema_slow = calculate_ema(
        df,
        EMA1_SLOW
    )

    count = 0

    for i in range(
        len(df) - 1,
        -1,
        -1
    ):

        f = ema_fast.iloc[i]
        m = ema_mid.iloc[i]
        s = ema_slow.iloc[i]

        if direction == "long":

            if f > m > s:
                count += 1
            else:
                break

        elif direction == "short":

            if f < m < s:
                count += 1
            else:
                break

        else:
            break

        if count >= EMA1_MAX_COUNT:
            break

    return count


# =========================================================
# EMA 표시
# =========================================================

def ema_display(
    direction,
    count
):

    if direction == "long":
        return f"🟢({count})"

    if direction == "short":
        return f"🔴({count})"

    return "⚪(0)"


# =========================================================
# ROC 계산
# =========================================================

def calculate_roc(
    df,
    period=ROC_PERIOD
):

    if df.empty:
        return pd.Series(
            index=df.index,
            dtype=float
        )

    return (
        (
            df["close"]
            / df["close"].shift(period)
        ) - 1
    ) * 100


# =========================================================
# ROC 분석
# =========================================================

def roc_analysis(
    df_confirmed,
    df_current
):

    result = {
        "previous_10": None,
        "current_10": None,
        "roc10_count": 0,
        "roc_long": False,
        "roc_short": False,
        "roc_state": "-"
    }

    if df_confirmed.empty:
        return result

    if len(df_confirmed) <= ROC_PERIOD:
        return result

    confirmed = df_confirmed.copy()

    roc_confirmed = calculate_roc(
        confirmed,
        ROC_PERIOD
    )

    previous_10 = roc_confirmed.iloc[-1]

    current_10 = previous_10

    # -----------------------------------------------------
    # 현재 진행 중인 캔들 가격 반영
    # -----------------------------------------------------

    if (
        df_current is not None
        and not df_current.empty
    ):

        current = df_current.copy()

        if len(current) > ROC_PERIOD:

            current_close = float(
                current["close"].iloc[-1]
            )

            previous_close = float(
                current["close"].iloc[
                    -1 - ROC_PERIOD
                ]
            )

            if previous_close != 0:

                current_10 = (
                    (
                        current_close
                        / previous_close
                    ) - 1
                ) * 100

    result["previous_10"] = previous_10
    result["current_10"] = current_10

    # -----------------------------------------------------
    # 최근 ROC10 양수 지속 카운트
    # -----------------------------------------------------

    values = list(
        roc_confirmed.dropna()
    )

    if current_10 is not None:

        values.append(
            current_10
        )

    count = 0

    for value in reversed(values):

        if value > 0:
            count += 1
        else:
            break

    result["roc10_count"] = count

    # -----------------------------------------------------
    # 0선 돌파
    # -----------------------------------------------------

    if (
        previous_10 is not None
        and current_10 is not None
    ):

        # 상향 돌파
        if (
            previous_10 <= 0
            and current_10 > 0
        ):

            result["roc_long"] = True
            result["roc_state"] = "long"

        # 하향 돌파
        elif (
            previous_10 >= 0
            and current_10 < 0
        ):

            result["roc_short"] = True
            result["roc_state"] = "short"

    return result


# =========================================================
# 개별 종목 분석
# =========================================================

def analyze(
    market_info
):

    market = market_info["market"]

    df = get_upbit_candles(
        market,
        count=HISTORY_CHUNK
    )

    if df.empty:
        return None

    if len(df) < EMA1_SLOW + ROC_PERIOD:
        return None

    # -----------------------------------------------------
    # 현재 진행 중인 캔들
    # -----------------------------------------------------

    df_current = df.copy()

    # -----------------------------------------------------
    # EMA는 마지막 캔들 기준
    # -----------------------------------------------------

    direction = get_ema_direction(
        df
    )

    ema_count = get_ema_alignment_count(
        df,
        direction
    )

    # -----------------------------------------------------
    # ROC 분석
    # -----------------------------------------------------

    roc = roc_analysis(
        df,
        df_current
    )

    # -----------------------------------------------------
    # 매수 후보
    # -----------------------------------------------------

    long_qualified = (
        direction == "long"
        and ema_count <= EMA1_MAX_COUNT
        and roc["roc_long"]
    )

    # -----------------------------------------------------
    # 숏 후보
    # 현재 설정에서는 대시보드에 표시하지 않음
    # -----------------------------------------------------

    short_qualified = (
        direction == "short"
        and ema_count <= EMA1_MAX_COUNT
        and roc["roc_short"]
    )

    # -----------------------------------------------------
    # 진행 리스트
    #
    # 매수 후보가 발생한 이후
    # ROC10이 계속 양수인 종목을 추적
    # -----------------------------------------------------

    progressing = (
        direction == "long"
        and ema_count <= EMA1_MAX_COUNT
        and roc["current_10"] is not None
        and roc["current_10"] > 0
        and not long_qualified
        and roc["roc10_count"] > 0
    )

    return {
        "market": market,
        "coin": market_info["coin"],

        "trade_price": market_info.get(
            "trade_price",
            0
        ),

        "volume": market_info.get(
            "acc_trade_price_24h",
            0
        ),

        "change_rate": market_info.get(
            "change_rate",
            0
        ),

        "direction": direction,

        "ema_count": ema_count,

        "ema_display": ema_display(
            direction,
            ema_count
        ),

        "previous_10": roc[
            "previous_10"
        ],

        "current_10": roc[
            "current_10"
        ],

        "roc10_count": roc[
            "roc10_count"
        ],

        "roc_state": roc[
            "roc_state"
        ],

        "roc_long": roc[
            "roc_long"
        ],

        "roc_short": roc[
            "roc_short"
        ],

        "qualified": long_qualified,

        "short_qualified": short_qualified,

        "progressing": progressing
    }


# =========================================================
# 금액 표시
# =========================================================

def format_volume(
    value
):

    if value is None:
        return "-"

    try:
        value = float(value)
    except:
        return "-"

    if value >= 100_000_000_000:
        return (
            f"{value / 100_000_000_000:.1f}천억"
        )

    if value >= 100_000_000:
        return (
            f"{value / 100_000_000:.1f}억"
        )

    if value >= 10_000:
        return (
            f"{value / 10_000:.1f}만"
        )

    return f"{value:,.0f}"


# =========================================================
# 가격 표시
# =========================================================

def format_price(
    value
):

    if value is None:
        return "-"

    try:
        value = float(value)
    except:
        return "-"

    if value >= 1000:
        return f"{value:,.0f}"

    if value >= 1:
        return f"{value:,.2f}"

    if value >= 0.01:
        return f"{value:,.4f}"

    return f"{value:,.8f}"


# =========================================================
# ROC HTML
# =========================================================

def roc_html(
    row
):

    current = row.get(
        "current_10"
    )

    count = row.get(
        "roc10_count",
        0
    )

    if current is None:
        value_text = "-"
    else:
        value_text = f"{current:+.2f}%"

    previous = row.get(
        "previous_10"
    )

    cross = "—"

    if (
        previous is not None
        and current is not None
    ):

        if (
            previous <= 0
            and current > 0
        ):
            cross = "↑0"

        elif (
            previous >= 0
            and current < 0
        ):
            cross = "↓0"

    return (
        f"<div class='roc-count'>"
        f"ROA10({count})"
        f"</div>"
        f"<div class='roc-value'>"
        f"{value_text} "
        f"<span class='roc-cross'>{cross}</span>"
        f"</div>"
    )


# =========================================================
# Signal HTML
# =========================================================

def signal_html(
    row
):

    if row.get(
        "qualified",
        False
    ):
        return (
            "<span class='signal-buy'>"
            "🟢 매수"
            "</span>"
        )

    return "-"


# =========================================================
# 테이블 행
# =========================================================

def rows_html(
    data
):

    if not data:

        return """
        <tr>
            <td colspan="6" class="empty">
                표시할 종목이 없습니다.
            </td>
        </tr>
        """

    html = ""

    for idx, row in enumerate(
        data,
        1
    ):

        change_rate = row.get(
            "change_rate",
            0
        )

        if change_rate >= 0:
            change_class = "up"
        else:
            change_class = "down"

        html += f"""
        <tr>

            <td>
                {idx}
            </td>

            <td class="coin">
                {row.get("coin", "-")}
            </td>

            <td>
                {format_volume(
                    row.get("volume")
                )}
            </td>

            <td class="ema">
                {row.get(
                    "ema_display",
                    "⚪(0)"
                )}
            </td>

            <td class="roc">
                {roc_html(row)}
            </td>

            <td>
                {signal_html(row)}
            </td>

        </tr>
        """

    return html


# =========================================================
# 매수 후보
# =========================================================

def buy_focus_section(
    data,
    exchange="upbit"
):

    candidates = [
        x for x in data
        if x.get(
            "qualified",
            False
        )
    ]

    if exchange == "upbit":
        title = "🟢 매수 후보"
    else:
        title = "🟢 OKX 매수 후보"

    return f"""
    <div class="section">

        <div class="section-title buy-title">
            {title}
            <span class="count">
                {len(candidates)}
            </span>
        </div>

        <div class="table-wrap">

            <table>

                <thead>
                    <tr>
                        <th>#</th>
                        <th>종목</th>
                        <th>거래대금</th>
                        <th>EMA1</th>
                        <th>ROA10</th>
                        <th>신호</th>
                    </tr>
                </thead>

                <tbody>
                    {rows_html(candidates)}
                </tbody>

            </table>

        </div>

    </div>
    """


# =========================================================
# 진행 리스트
# =========================================================

def progress_section(
    data,
    exchange="upbit"
):

    progressing = [
        x for x in data
        if x.get(
            "progressing",
            False
        )
    ]

    if exchange == "upbit":
        title = "🟡 진행 리스트"
    else:
        title = "🟡 OKX 진행 리스트"

    return f"""
    <div class="section">

        <div class="section-title progress-title">
            {title}
            <span class="count">
                {len(progressing)}
            </span>
        </div>

        <div class="table-wrap">

            <table>

                <thead>
                    <tr>
                        <th>#</th>
                        <th>종목</th>
                        <th>거래대금</th>
                        <th>EMA1</th>
                        <th>ROA10</th>
                        <th>신호</th>
                    </tr>
                </thead>

                <tbody>
                    {rows_html(progressing)}
                </tbody>

            </table>

        </div>

    </div>
    """


# =========================================================
# 전체 데이터 분석
# =========================================================

def update_upbit():

    logging.info(
        "Upbit 데이터 업데이트 시작"
    )

    top_markets = get_upbit_top_markets()

    if not top_markets:

        logging.warning(
            "Upbit TOP 종목을 가져오지 못했습니다."
        )

        return []

    analyzed = []

    for market_info in top_markets:

        try:

            result = analyze(
                market_info
            )

            if result is not None:
                analyzed.append(
                    result
                )

        except Exception as e:

            logging.exception(
                "분석 오류 | %s | %s",
                market_info.get("market"),
                e
            )

        time.sleep(
            REQUEST_INTERVAL
        )

    return analyzed


# =========================================================
# OKX
# =========================================================

def get_okx_markets():

    data = request_get(
        OKX_INSTRUMENTS_URL,
        params={
            "instType": "SWAP"
        }
    )

    if not data:
        return []

    result = []

    for item in data.get(
        "data",
        []
    ):

        inst_id = item.get(
            "instId",
            ""
        )

        if not inst_id.endswith(
            "-USDT-SWAP"
        ):
            continue

        result.append(
            inst_id
        )

    return result


def get_okx_candles(
    inst_id,
    limit=200
):

    data = request_get(
        OKX_CANDLES_URL,
        params={
            "instId": inst_id,
            "bar": "1H",
            "limit": limit
        }
    )

    if not data:
        return pd.DataFrame()

    rows = []

    for item in data.get(
        "data",
        []
    ):

        if len(item) < 6:
            continue

        rows.append({
            "time": pd.to_datetime(
                int(item[0]),
                unit="ms"
            ),
            "open": float(item[1]),
            "high": float(item[2]),
            "low": float(item[3]),
            "close": float(item[4]),
            "volume": float(item[5])
        })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    df = df.sort_values(
        "time"
    ).reset_index(
        drop=True
    )

    return df


# =========================================================
# OKX 분석
# =========================================================

def analyze_okx(
    inst_id
):

    df = get_okx_candles(
        inst_id,
        HISTORY_CHUNK
    )

    if df.empty:
        return None

    if len(df) < EMA1_SLOW + ROC_PERIOD:
        return None

    direction = get_ema_direction(
        df
    )

    ema_count = get_ema_alignment_count(
        df,
        direction
    )

    roc = roc_analysis(
        df,
        df
    )

    long_qualified = (
        direction == "long"
        and ema_count <= EMA1_MAX_COUNT
        and roc["roc_long"]
    )

    progressing = (
        direction == "long"
        and ema_count <= EMA1_MAX_COUNT
        and roc["current_10"] is not None
        and roc["current_10"] > 0
        and not long_qualified
        and roc["roc10_count"] > 0
    )

    coin = inst_id.replace(
        "-USDT-SWAP",
        ""
    )

    return {
        "market": inst_id,
        "coin": coin,

        "trade_price": df["close"].iloc[-1],

        "volume": 0,

        "change_rate": (
            (
                df["close"].iloc[-1]
                / df["close"].iloc[-2]
            ) - 1
        ) * 100,

        "direction": direction,

        "ema_count": ema_count,

        "ema_display": ema_display(
            direction,
            ema_count
        ),

        "previous_10": roc[
            "previous_10"
        ],

        "current_10": roc[
            "current_10"
        ],

        "roc10_count": roc[
            "roc10_count"
        ],

        "roc_state": roc[
            "roc_state"
        ],

        "roc_long": roc[
            "roc_long"
        ],

        "roc_short": roc[
            "roc_short"
        ],

        "qualified": long_qualified,

        "short_qualified": False,

        "progressing": progressing
    }


def update_okx():

    if USE_OKX != "Y":
        return []

    logging.info(
        "OKX 데이터 업데이트 시작"
    )

    markets = get_okx_markets()

    if not markets:
        return []

    result = []

    for inst_id in markets[:TOP_N]:

        try:

            row = analyze_okx(
                inst_id
            )

            if row is not None:
                result.append(
                    row
                )

        except Exception as e:

            logging.exception(
                "OKX 분석 오류 | %s | %s",
                inst_id,
                e
            )

        time.sleep(
            REQUEST_INTERVAL
        )

    return result


# =========================================================
# 전체 업데이트
# =========================================================

def update_all():

    global latest_data
    global latest_update_time

    try:

        upbit_data = []

        if USE_UPBIT == "Y":
            upbit_data = update_upbit()

        okx_data = []

        if USE_OKX == "Y":
            okx_data = update_okx()

        with data_lock:

            latest_data = {
                "upbit": upbit_data,
                "okx": okx_data
            }

            latest_update_time = (
                datetime.now(KST)
                .strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            )

        logging.info(
            "전체 업데이트 완료 | Upbit=%s | OKX=%s",
            len(upbit_data),
            len(okx_data)
        )

    except Exception as e:

        logging.exception(
            "전체 업데이트 오류: %s",
            e
        )


# =========================================================
# 스케줄러
# =========================================================

def scheduler_loop():

    schedule.every(
        UPDATE_MINUTES
    ).minutes.do(
        update_all
    )

    update_all()

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
# CSS
# =========================================================

CSS = """

* {
    box-sizing: border-box;
}

body {
    margin: 0;
    padding: 12px;
    background: #111;
    color: #eee;
    font-family:
        Arial,
        sans-serif;
}

.container {
    max-width: 1000px;
    margin: auto;
}

.header {
    margin-bottom: 14px;
}

.title {
    font-size: 22px;
    font-weight: 700;
    margin-bottom: 5px;
}

.update {
    color: #999;
    font-size: 12px;
}

.section {
    margin-bottom: 18px;
}

.section-title {
    font-size: 18px;
    font-weight: 700;
    padding: 10px 4px;
    border-bottom: 1px solid #333;
}

.buy-title {
    color: #55ff88;
}

.progress-title {
    color: #ffd84d;
}

.count {
    font-size: 13px;
    color: #999;
    margin-left: 5px;
}

.table-wrap {
    overflow-x: auto;
}

table {
    width: 100%;
    border-collapse: collapse;
    table-layout: fixed;
}

th {
    background: #1c1c1c;
    color: #aaa;
    font-size: 12px;
    font-weight: 500;
    padding: 9px 4px;
    border-bottom: 1px solid #333;
}

td {
    padding: 9px 4px;
    text-align: center;
    border-bottom: 1px solid #222;
    font-size: 13px;
}

.coin {
    font-weight: 700;
    text-align: left;
}

.ema {
    font-weight: 700;
}

.roc {
    line-height: 1.35;
}

.roc-count {
    font-weight: 700;
}

.roc-value {
    font-size: 11px;
    color: #aaa;
}

.roc-cross {
    color: #fff;
    font-weight: 700;
}

.signal-buy {
    color: #55ff88;
    font-weight: 700;
}

.up {
    color: #55ff88;
}

.down {
    color: #ff6666;
}

.empty {
    color: #666;
    padding: 20px;
}

@media (max-width: 600px) {

    body {
        padding: 8px;
    }

    .title {
        font-size: 19px;
    }

    .section-title {
        font-size: 16px;
    }

    th {
        font-size: 11px;
    }

    td {
        font-size: 12px;
        padding: 8px 2px;
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

    with data_lock:

        upbit_data = list(
            latest_data.get(
                "upbit",
                []
            )
        )

        okx_data = list(
            latest_data.get(
                "okx",
                []
            )
        )

        update_time = latest_update_time

    # -----------------------------------------------------
    # 거래대금 순위
    # -----------------------------------------------------

    upbit_data.sort(
        key=lambda x: x.get(
            "volume",
            0
        ),
        reverse=True
    )

    okx_data.sort(
        key=lambda x: x.get(
            "volume",
            0
        ),
        reverse=True
    )

    # -----------------------------------------------------
    # HTML
    # -----------------------------------------------------

    html = f"""
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
            Crypto Dashboard
        </title>

        <style>
            {CSS}
        </style>

    </head>

    <body>

        <div class="container">

            <div class="header">

                <div class="title">
                    📊 Crypto Dashboard
                </div>

                <div class="update">
                    마지막 업데이트 :
                    {update_time}
                </div>

            </div>

    """

    # =====================================================
    # Upbit
    # =====================================================

    if USE_UPBIT == "Y":

        html += buy_focus_section(
            upbit_data,
            "upbit"
        )

        # ★ 매수 후보 아래 진행 리스트
        html += progress_section(
            upbit_data,
            "upbit"
        )

        html += f"""

            <div class="section">

                <div class="section-title">
                    🏆 업비트 실거래대금 TOP{TOP_N}
                </div>

                <div class="table-wrap">

                    <table>

                        <thead>

                            <tr>
                                <th>#</th>
                                <th>종목</th>
                                <th>거래대금</th>
                                <th>EMA1</th>
                                <th>ROA10</th>
                                <th>신호</th>
                            </tr>

                        </thead>

                        <tbody>

                            {rows_html(upbit_data)}

                        </tbody>

                    </table>

                </div>

            </div>

        """

    # =====================================================
    # OKX
    # =====================================================

    if USE_OKX == "Y":

        html += buy_focus_section(
            okx_data,
            "okx"
        )

        html += progress_section(
            okx_data,
            "okx"
        )

        html += f"""

            <div class="section">

                <div class="section-title">
                    🏆 OKX 실거래대금 TOP{TOP_N}
                </div>

                <div class="table-wrap">

                    <table>

                        <thead>

                            <tr>
                                <th>#</th>
                                <th>종목</th>
                                <th>거래대금</th>
                                <th>EMA1</th>
                                <th>ROA10</th>
                                <th>신호</th>
                            </tr>

                        </thead>

                        <tbody>

                            {rows_html(okx_data)}

                        </tbody>

                    </table>

                </div>

            </div>

        """

    html += """

        </div>

    </body>

    </html>
    """

    return HTMLResponse(
        content=html
    )


# =========================================================
# 서버 실행
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
