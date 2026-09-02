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


# =========================================================
# 설정
# =========================================================

VOLUME_HOURS = 24
TOP_N = 100

UPDATE_MINUTES = 1

INITIAL_CANDLE_COUNT = 200
HISTORY_CHUNK = 200
MAX_HISTORY_CHUNKS = 10

USE_UPBIT = "Y"
USE_OKX = "N"

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10

KST = ZoneInfo("Asia/Seoul")


# =========================================================
# 전역 변수
# =========================================================

latest_upbit_data = []
latest_okx_data = []

latest_upbit_update = None
latest_okx_update = None

usdt_krw = 0.0

market_list = []

data_lock = threading.Lock()
db_lock = threading.Lock()

last_request_time = 0

# 비행기 상태
air_state = {}


# =========================================================
# 요청 제한
# =========================================================

def request_get(url, params=None, timeout=10):

    global last_request_time

    for attempt in range(MAX_RETRIES):

        try:

            elapsed = time.time() - last_request_time

            if elapsed < REQUEST_INTERVAL:
                time.sleep(
                    REQUEST_INTERVAL - elapsed
                )

            response = requests.get(
                url,
                params=params,
                timeout=timeout
            )

            last_request_time = time.time()

            if response.status_code == 200:
                return response.json()

            if response.status_code == 429:

                logging.warning(
                    "API Rate Limit → %s초 대기",
                    RATE_LIMIT_WAIT
                )

                time.sleep(RATE_LIMIT_WAIT)
                continue

            logging.warning(
                "API 오류 %s : %s",
                response.status_code,
                url
            )

        except Exception as e:

            logging.warning(
                "요청 실패 %s/%s : %s",
                attempt + 1,
                MAX_RETRIES,
                e
            )

            time.sleep(1)

    return None


# =========================================================
# Upbit 마켓
# =========================================================

def get_upbit_markets():

    url = "https://api.upbit.com/v1/ticker/all"

    data = request_get(
        url,
        params={
            "quote_currencies": "KRW"
        }
    )

    if not data:
        return []

    markets = []

    for item in data:

        market = item.get("market", "")

        if market.startswith("KRW-"):

            markets.append(item)

    markets.sort(
        key=lambda x: x.get(
            "acc_trade_price_24h",
            0
        ),
        reverse=True
    )

    return markets


# =========================================================
# USDT/KRW
# =========================================================

def get_usdt_krw():

    url = "https://api.upbit.com/v1/ticker"

    data = request_get(
        url,
        params={
            "markets": "KRW-USDT"
        }
    )

    if not data:
        return 0.0

    try:
        return float(
            data[0]["trade_price"]
        )
    except:
        return 0.0


# =========================================================
# Upbit 1H
# =========================================================

def get_upbit_1h(market):

    url = (
        "https://api.upbit.com/v1/"
        "candles/minutes/60"
    )

    data = request_get(
        url,
        params={
            "market": market,
            "count": INITIAL_CANDLE_COUNT
        }
    )

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    if df.empty:
        return df

    df["datetime"] = pd.to_datetime(
        df["candle_date_time_kst"]
    )

    df = df.sort_values(
        "datetime"
    ).reset_index(drop=True)

    # 현재 진행 중인 1H 봉 제거
    now = datetime.now(KST)

    current_block = now.replace(
        minute=0,
        second=0,
        microsecond=0
    )

    df = df[
        df["datetime"] < current_block
    ].copy()

    df.rename(
        columns={
            "opening_price": "open",
            "high_price": "high",
            "low_price": "low",
            "trade_price": "close",
            "candle_acc_trade_volume": "volume"
        },
        inplace=True
    )

    return df[
        [
            "datetime",
            "open",
            "high",
            "low",
            "close",
            "volume"
        ]
    ]


# =========================================================
# Upbit 4H
# =========================================================

def get_upbit_4h(market):

    url = (
        "https://api.upbit.com/v1/"
        "candles/minutes/240"
    )

    data = request_get(
        url,
        params={
            "market": market,
            "count": INITIAL_CANDLE_COUNT
        }
    )

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    if df.empty:
        return df

    df["datetime"] = pd.to_datetime(
        df["candle_date_time_kst"]
    )

    df = df.sort_values(
        "datetime"
    ).reset_index(drop=True)

    # 현재 진행 중인 4H 봉 제거
    now = datetime.now(KST)

    hour_block = (
        now.hour // 4
    ) * 4

    current_block = now.replace(
        hour=hour_block,
        minute=0,
        second=0,
        microsecond=0
    )

    df = df[
        df["datetime"] < current_block
    ].copy()

    df.rename(
        columns={
            "opening_price": "open",
            "high_price": "high",
            "low_price": "low",
            "trade_price": "close",
            "candle_acc_trade_volume": "volume"
        },
        inplace=True
    )

    return df[
        [
            "datetime",
            "open",
            "high",
            "low",
            "close",
            "volume"
        ]
    ]


# =========================================================
# Upbit 과거 데이터
# =========================================================

def history_upbit(market, minute_unit):

    url = (
        f"https://api.upbit.com/v1/"
        f"candles/minutes/{minute_unit}"
    )

    all_data = []

    to = None

    for _ in range(MAX_HISTORY_CHUNKS):

        params = {
            "market": market,
            "count": HISTORY_CHUNK
        }

        if to:
            params["to"] = to

        data = request_get(
            url,
            params=params
        )

        if not data:
            break

        all_data.extend(data)

        last_time = data[-1][
            "candle_date_time_utc"
        ]

        to = last_time

        if len(data) < HISTORY_CHUNK:
            break

    if not all_data:
        return pd.DataFrame()

    df = pd.DataFrame(all_data)

    df["datetime"] = pd.to_datetime(
        df["candle_date_time_kst"]
    )

    df = df.sort_values(
        "datetime"
    ).drop_duplicates(
        "datetime"
    ).reset_index(drop=True)

    df.rename(
        columns={
            "opening_price": "open",
            "high_price": "high",
            "low_price": "low",
            "trade_price": "close",
            "candle_acc_trade_volume": "volume"
        },
        inplace=True
    )

    return df[
        [
            "datetime",
            "open",
            "high",
            "low",
            "close",
            "volume"
        ]
    ]


# =========================================================
# OKX
# =========================================================

def get_okx_ohlcv(
    inst_id,
    bar="1H",
    limit=200
):

    url = (
        "https://www.okx.com/api/v5/"
        "market/candles"
    )

    data = request_get(
        url,
        params={
            "instId": inst_id,
            "bar": bar,
            "limit": limit
        }
    )

    if not data:
        return pd.DataFrame()

    rows = data.get("data", [])

    if not rows:
        return pd.DataFrame()

    columns = [
        "ts",
        "open",
        "high",
        "low",
        "close",
        "vol",
        "volCcy",
        "volCcyQuote",
        "confirm"
    ]

    df = pd.DataFrame(
        rows,
        columns=columns
    )

    df["datetime"] = pd.to_datetime(
        pd.to_numeric(df["ts"]),
        unit="ms",
        utc=True
    ).dt.tz_convert(KST)

    df["close"] = pd.to_numeric(
        df["close"],
        errors="coerce"
    )

    df["open"] = pd.to_numeric(
        df["open"],
        errors="coerce"
    )

    df["high"] = pd.to_numeric(
        df["high"],
        errors="coerce"
    )

    df["low"] = pd.to_numeric(
        df["low"],
        errors="coerce"
    )

    df["volume"] = pd.to_numeric(
        df["volCcyQuote"],
        errors="coerce"
    )

    df = df[
        df["confirm"] == "1"
    ].copy()

    df = df.sort_values(
        "datetime"
    ).reset_index(drop=True)

    return df[
        [
            "datetime",
            "open",
            "high",
            "low",
            "close",
            "volume"
        ]
    ]


# =========================================================
# OKX 과거
# =========================================================

def history_okx(
    inst_id,
    bar="1H"
):

    return get_okx_ohlcv(
        inst_id,
        bar,
        INITIAL_CANDLE_COUNT
    )


# =========================================================
# OKX 심볼
# =========================================================

def get_okx_symbols():

    url = (
        "https://www.okx.com/api/v5/"
        "public/instruments"
    )

    data = request_get(
        url,
        params={
            "instType": "SWAP"
        }
    )

    if not data:
        return []

    result = data.get(
        "data",
        []
    )

    return [
        x["instId"]
        for x in result
        if x.get("settleCcy") == "USDT"
    ]


# =========================================================
# EMA
# =========================================================

def ema(df, period):

    return df["close"].ewm(
        span=period,
        adjust=False
    ).mean()


# =========================================================
# EMA 방향
# =========================================================

def direction(df):

    if df.empty or len(df) < 120:
        return "neutral"

    e10 = ema(df, 10).iloc[-1]
    e30 = ema(df, 30).iloc[-1]
    e60 = ema(df, 60).iloc[-1]
    e120 = ema(df, 120).iloc[-1]

    if (
        e10 > e30
        and e30 > e60
        and e60 > e120
    ):
        return "long"

    if (
        e10 < e30
        and e30 < e60
        and e60 < e120
    ):
        return "short"

    return "neutral"


# =========================================================
# EMA 정배열 연속 카운트
# =========================================================

def ema_alignment_count(df):

    if df.empty or len(df) < 120:
        return {
            "direction": "neutral",
            "count": 0
        }

    e10 = ema(df, 10)
    e30 = ema(df, 30)
    e60 = ema(df, 60)
    e120 = ema(df, 120)

    count = 0
    latest_direction = "neutral"

    for i in range(
        len(df) - 1,
        -1,
        -1
    ):

        if (
            e10.iloc[i] > e30.iloc[i]
            and
            e30.iloc[i] > e60.iloc[i]
            and
            e60.iloc[i] > e120.iloc[i]
        ):

            current = "long"

        elif (
            e10.iloc[i] < e30.iloc[i]
            and
            e30.iloc[i] < e60.iloc[i]
            and
            e60.iloc[i] < e120.iloc[i]
        ):

            current = "short"

        else:

            current = "neutral"

        if i == len(df) - 1:

            latest_direction = current

        if current == latest_direction:

            count += 1

        else:

            break

    return {
        "direction": latest_direction,
        "count": count
    }


# =========================================================
# EMA 표시
# =========================================================

def ema_display(df):

    result = ema_alignment_count(df)

    d = result["direction"]
    count = result["count"]

    if d == "long":
        return f"🟢({count})"

    if d == "short":
        return f"🔴({count})"

    return "⚪(0)"


# =========================================================
# ★ 10선 진행상태
#
# 확정된 1H 종가 기준
#
# 10선 아래:
# 🔻(1) → 🔻(2) → 🔻(3)
#
# 10선 위:
# 🟢(1) → 🟢(2) → 🟢(3)
#
# 반대 방향 종가 마감 시
# 기존 카운트 초기화 후 1부터 시작
# =========================================================

def close_vs_ema10_1h(df):

    if df.empty or len(df) < 10:

        return {
            "position": "equal",
            "count": 0,
            "display": "⚪(0)"
        }

    work = df.copy()

    work["ema10"] = ema(
        work,
        10
    )

    latest_close = float(
        work["close"].iloc[-1]
    )

    latest_ema10 = float(
        work["ema10"].iloc[-1]
    )

    # -----------------------------------------------------
    # 최근 확정봉부터 역으로 연속 카운트
    # -----------------------------------------------------

    if latest_close > latest_ema10:

        position = "above"

        count = 0

        for i in range(
            len(work) - 1,
            -1,
            -1
        ):

            close_price = float(
                work["close"].iloc[i]
            )

            ema10_price = float(
                work["ema10"].iloc[i]
            )

            if close_price > ema10_price:

                count += 1

            else:

                break

        display = f"🟢({count})"

    elif latest_close < latest_ema10:

        position = "below"

        count = 0

        for i in range(
            len(work) - 1,
            -1,
            -1
        ):

            close_price = float(
                work["close"].iloc[i]
            )

            ema10_price = float(
                work["ema10"].iloc[i]
            )

            if close_price < ema10_price:

                count += 1

            else:

                break

        display = f"🔻({count})"

    else:

        position = "equal"
        count = 0
        display = "⚪(0)"

    return {
        "position": position,
        "count": count,
        "display": display
    }


# =========================================================
# 10선 HTML
# =========================================================

def close_ema10_html(data):

    if not data:
        return "⚪(0)"

    position = data.get(
        "position",
        "equal"
    )

    display = data.get(
        "display",
        "⚪(0)"
    )

    if position == "above":

        return (
            '<span class="close-ema10 '
            'above">'
            f'{display}'
            '</span>'
        )

    if position == "below":

        return (
            '<span class="close-ema10 '
            'below">'
            f'{display}'
            '</span>'
        )

    return (
        '<span class="close-ema10 equal">'
        f'{display}'
        '</span>'
    )


# =========================================================
# 비행기 경고
# =========================================================

def get_air_warning(
    df1h,
    df4h
):

    if (
        df1h.empty
        or df4h.empty
        or len(df1h) < 120
        or len(df4h) < 120
    ):
        return None

    direction1h = direction(df1h)
    direction4h = direction(df4h)

    # 기존 LONG 조건 유지
    if (
        direction1h != "long"
        or direction4h != "long"
    ):
        return None

    e10 = ema(df1h, 10)

    if len(df1h) < 2:
        return None

    previous_close = float(
        df1h["close"].iloc[-2]
    )

    previous_ema10 = float(
        e10.iloc[-2]
    )

    current_open = float(
        df1h["open"].iloc[-1]
    )

    current_close = float(
        df1h["close"].iloc[-1]
    )

    current_ema10 = float(
        e10.iloc[-1]
    )

    # 이전봉 종가가 10선 아래
    if previous_close >= previous_ema10:
        return None

    # 현재봉 양봉
    if current_close <= current_open:
        return None

    # 현재봉 종가 10선 돌파
    if current_close <= current_ema10:
        return None

    return "LONG"


# =========================================================
# 비행기 상태 업데이트
# =========================================================

def update_air_counter(
    market,
    df1h,
    df4h
):

    if df1h.empty:
        return {
            "direction": None,
            "count": 0,
            "stopped": False
        }

    warning = get_air_warning(
        df1h,
        df4h
    )

    candle_time = df1h[
        "datetime"
    ].iloc[-1]

    candle_key = str(
        candle_time
    )

    e10 = ema(df1h, 10)

    current_close = float(
        df1h["close"].iloc[-1]
    )

    current_open = float(
        df1h["open"].iloc[-1]
    )

    current_ema10 = float(
        e10.iloc[-1]
    )

    state = air_state.get(
        market,
        {
            "active": False,
            "direction": None,
            "count": 0,
            "stopped": False,
            "last_candle": None
        }
    )

    # -----------------------------------------------------
    # 새로운 LONG 경고 발생
    # -----------------------------------------------------

    if warning == "LONG":

        if (
            state["last_candle"]
            != candle_key
        ):

            state = {
                "active": True,
                "direction": "LONG",
                "count": 1,
                "stopped": False,
                "last_candle": candle_key
            }

            air_state[market] = state

            return {
                "direction": "LONG",
                "count": 1,
                "stopped": False
            }

    # -----------------------------------------------------
    # 기존 비행기 진행
    # -----------------------------------------------------

    if state.get("active"):

        # 같은 캔들 중복 증가 방지
        if (
            state.get("last_candle")
            != candle_key
        ):

            # 10선 아래 종가 → 종료
            if current_close < current_ema10:

                state["active"] = False
                state["stopped"] = True
                state["count"] = 0

            # 양봉 유지
            elif current_close > current_open:

                state["count"] += 1
                state["last_candle"] = candle_key

            # 음봉이면 종료
            else:

                state["active"] = False
                state["stopped"] = True
                state["count"] = 0

            air_state[market] = state

    return {
        "direction": state.get(
            "direction"
        ),
        "count": state.get(
            "count",
            0
        ),
        "stopped": state.get(
            "stopped",
            False
        )
    }


# =========================================================
# 경고 HTML
# =========================================================

def warning_html(
    direction_value,
    count,
    stopped
):

    if stopped:

        return (
            '<span class="warning stopped">'
            '⛔️'
            '</span>'
        )

    if (
        direction_value == "LONG"
        and count > 0
    ):

        return (
            '<span class="warning long">'
            '<span class="airplane">🛩</span>'
            f' LONG({count})'
            '</span>'
        )

    return (
        '<span class="warning empty">'
        '—'
        '</span>'
    )


# =========================================================
# 분석
# =========================================================

def analyze(
    market,
    df1h,
    df4h,
    ticker
):

    if (
        df1h.empty
        or df4h.empty
    ):
        return None

    ema_1h = ema_display(
        df1h
    )

    ema_4h = ema_display(
        df4h
    )

    # ★ 10선 진행상태
    close_ema10 = close_vs_ema10_1h(
        df1h
    )

    # 비행기
    air = update_air_counter(
        market,
        df1h,
        df4h
    )

    warning = warning_html(
        air.get("direction"),
        air.get("count", 0),
        air.get("stopped", False)
    )

    change = ticker.get(
        "signed_change_rate",
        0
    )

    try:
        change = float(change) * 100
    except:
        change = 0

    volume = ticker.get(
        "acc_trade_price_24h",
        0
    )

    try:
        volume = float(volume)
    except:
        volume = 0

    return {
        "market": market,
        "name": market.replace(
            "KRW-",
            ""
        ),
        "change": change,
        "volume": volume,
        "ema_1h": ema_1h,
        "ema_4h": ema_4h,
        "close_ema10": close_ema10,
        "air_warning": warning,
        "air_direction": air.get(
            "direction"
        ),
        "air_count": air.get(
            "count",
            0
        ),
        "air_stopped": air.get(
            "stopped",
            False
        )
    }


# =========================================================
# Upbit 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update
    global market_list
    global usdt_krw

    logging.info(
        "===== Upbit 업데이트 시작 ====="
    )

    tickers = get_upbit_markets()

    if not tickers:
        logging.warning(
            "Upbit 마켓 조회 실패"
        )
        return

    usdt_krw = get_usdt_krw()

    market_list = [
        x["market"]
        for x in tickers
    ]

    results = []

    for rank, ticker in enumerate(
        tickers[:TOP_N],
        start=1
    ):

        market = ticker.get(
            "market"
        )

        try:

            df1h = get_upbit_1h(
                market
            )

            df4h = get_upbit_4h(
                market
            )

            if (
                df1h.empty
                or df4h.empty
            ):
                continue

            result = analyze(
                market,
                df1h,
                df4h,
                ticker
            )

            if result:

                result["rank"] = rank

                results.append(
                    result
                )

        except Exception as e:

            logging.warning(
                "%s 분석 오류: %s",
                market,
                e
            )

    with data_lock:

        latest_upbit_data = results

        latest_upbit_update = (
            datetime.now(KST)
        )

    logging.info(
        "===== Upbit 업데이트 완료 : %s개 =====",
        len(results)
    )


# =========================================================
# OKX 업데이트
# =========================================================

def update_okx():

    global latest_okx_data
    global latest_okx_update

    if USE_OKX != "Y":
        return

    symbols = get_okx_symbols()

    if not symbols:
        return

    results = []

    for symbol in symbols:

        try:

            df1h = history_okx(
                symbol,
                "1H"
            )

            df4h = history_okx(
                symbol,
                "4H"
            )

            if (
                df1h.empty
                or df4h.empty
            ):
                continue

            volume = (
                df1h["volume"]
                .tail(VOLUME_HOURS)
                .sum()
            )

            results.append(
                {
                    "symbol": symbol,
                    "volume": volume
                }
            )

        except Exception as e:

            logging.warning(
                "OKX %s 오류: %s",
                symbol,
                e
            )

    results.sort(
        key=lambda x: x["volume"],
        reverse=True
    )

    with data_lock:

        latest_okx_data = results

        latest_okx_update = (
            datetime.now(KST)
        )


# =========================================================
# 전체 업데이트
# =========================================================

def update_all():

    try:

        if USE_UPBIT == "Y":
            update_upbit()

        if USE_OKX == "Y":
            update_okx()

    except Exception as e:

        logging.exception(
            "전체 업데이트 오류: %s",
            e
        )


# =========================================================
# HTML 행
# =========================================================

def rows_html(data):

    if not data:

        return """
        <tr>
            <td colspan="6"
                class="empty-row">
                데이터를 불러오는 중...
            </td>
        </tr>
        """

    rows = []

    for item in data:

        rank = item.get(
            "rank",
            "-"
        )

        name = item.get(
            "name",
            "-"
        )

        change = item.get(
            "change",
            0
        )

        volume = item.get(
            "volume",
            0
        )

        ema1h = item.get(
            "ema_1h",
            "⚪(0)"
        )

        ema4h = item.get(
            "ema_4h",
            "⚪(0)"
        )

        close_ema10 = item.get(
            "close_ema10",
            {}
        )

        warning = item.get(
            "air_warning",
            "—"
        )

        if change > 0:

            change_html = (
                f'<span class="up">'
                f'+{change:.2f}%'
                '</span>'
            )

        elif change < 0:

            change_html = (
                f'<span class="down">'
                f'{change:.2f}%'
                '</span>'
            )

        else:

            change_html = (
                '<span>0.00%</span>'
            )

        volume_억 = volume / 100000000

        volume_html = (
            f'{volume_억:,.0f}억'
        )

        rows.append(
            f"""
            <tr>

                <td class="rank">
                    {rank}
                </td>

                <td class="coin">
                    <div class="coin-name">
                        {name}
                    </div>
                    <div class="coin-change">
                        {change_html}
                    </div>
                </td>

                <td class="volume">
                    {volume_html}
                </td>

                <td class="ema">

                    <div class="ema-line">
                        <span class="ema-label">
                            1H
                        </span>
                        <span class="ema-value">
                            {ema1h}
                        </span>
                    </div>

                    <div class="ema-line">
                        <span class="ema-label">
                            4H
                        </span>
                        <span class="ema-value">
                            {ema4h}
                        </span>
                    </div>

                </td>

                <td class="ten-line">
                    {close_ema10_html(close_ema10)}
                </td>

                <td class="warning-cell">
                    {warning}
                </td>

            </tr>
            """
        )

    return "\n".join(rows)


# =========================================================
# 메인 HTML
# =========================================================

HTML_PAGE = """
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
OKX / Upbit EMA Dashboard
</title>


<style>

* {
    box-sizing: border-box;
}

body {

    margin: 0;

    padding: 8px;

    background: #111;

    color: #eee;

    font-family:
        Arial,
        sans-serif;

}


.container {

    width: 100%;

    max-width: 1000px;

    margin: 0 auto;

}


.title {

    text-align: center;

    font-size: 15px;

    font-weight: bold;

    margin-bottom: 7px;

}


.update {

    text-align: center;

    color: #999;

    font-size: 9px;

    margin-bottom: 6px;

}


table {

    width: 100%;

    border-collapse:
        collapse;

    table-layout:
        fixed;

}


th {

    background: #202020;

    color: #aaa;

    font-size: 9px;

    font-weight: normal;

    height: 24px;

    border-bottom:
        1px solid #333;

}


td {

    height: 31px;

    padding: 2px 3px;

    border-bottom:
        1px solid #222;

    text-align: center;

    font-size: 9px;

}


tr:hover {

    background: #191919;

}


/* =====================================================
   열 폭
   ===================================================== */

th:nth-child(1),
td:nth-child(1) {
    width: 7%;
}

th:nth-child(2),
td:nth-child(2) {
    width: 23%;
}

th:nth-child(3),
td:nth-child(3) {
    width: 17%;
}

th:nth-child(4),
td:nth-child(4) {
    width: 18%;
}

th:nth-child(5),
td:nth-child(5) {
    width: 10%;
}

th:nth-child(6),
td:nth-child(6) {
    width: 25%;
}


/* =====================================================
   순위
   ===================================================== */

.rank {

    color: #999;

    font-size: 9px;

}


/* =====================================================
   코인
   ===================================================== */

.coin {

    text-align: left;

    padding-left: 5px;

}


.coin-name {

    font-size: 10px;

    font-weight: bold;

    line-height: 12px;

}


.coin-change {

    font-size: 8px;

    line-height: 9px;

}


.up {

    color: #4caf50;

}


.down {

    color: #ff5252;

}


/* =====================================================
   거래대금
   ===================================================== */

.volume {

    color: #ddd;

    font-size: 9px;

    white-space: nowrap;

}


/* =====================================================
   EMA
   ===================================================== */

.ema {

    text-align: left;

    padding-left: 4px;

}


.ema-line {

    height: 12px;

    display: flex;

    align-items: center;

}


.ema-label {

    width: 18px;

    color: #777;

    font-size: 7px;

}


.ema-value {

    width: 42px;

    text-align: left;

    font-size: 8px;

    white-space: nowrap;

}


/* =====================================================
   ★ 10선 진행상태
   ===================================================== */

.ten-line {

    text-align: center;

    white-space: nowrap;

}


.close-ema10 {

    display: inline-block;

    min-width: 30px;

    font-size: 8px;

    font-weight: bold;

    line-height: 16px;

}


.close-ema10.above {

    color: #00e676;

}


.close-ema10.below {

    color: #ff5252;

}


.close-ema10.equal {

    color: #888;

}


/* =====================================================
   경고
   ===================================================== */

.warning-cell {

    text-align: center;

    white-space: nowrap;

    overflow: hidden;

}


.warning {

    display: inline-flex;

    align-items: center;

    justify-content: center;

    gap: 2px;

    font-size: 8px;

    font-weight: bold;

}


.warning.long {

    color: #00e676;

}


.warning.stopped {

    color: #ff5252;

}


.warning.empty {

    color: #555;

}


.airplane {

    display: inline-block;

    animation:
        fly 1s
        linear
        infinite;

}


@keyframes fly {

    0% {

        transform:
            translateX(0);

    }

    50% {

        transform:
            translateX(3px);

    }

    100% {

        transform:
            translateX(0);

    }

}


/* =====================================================
   빈 행
   ===================================================== */

.empty-row {

    height: 45px;

    color: #777;

}


/* =====================================================
   모바일
   ===================================================== */

@media (
    max-width: 600px
) {

    body {

        padding: 4px;

    }


    .title {

        font-size: 13px;

        margin-bottom: 5px;

    }


    .update {

        font-size: 8px;

    }


    th {

        font-size: 8px;

        height: 21px;

    }


    td {

        height: 28px;

        padding:
            1px 2px;

        font-size: 8px;

    }


    .coin-name {

        font-size: 9px;

    }


    .coin-change {

        font-size: 7px;

    }


    .volume {

        font-size: 8px;

    }


    .ema-value {

        font-size: 7px;

    }


    .close-ema10 {

        font-size: 7px;

    }


    .warning {

        font-size: 7px;

    }

}

</style>

</head>


<body>


<div class="container">

    <div class="title">
        🏆 UPBIT 실시간 EMA 대시보드
    </div>

    <div class="update">
        마지막 업데이트:
        {{UPDATE_TIME}}
    </div>

    <table>

        <thead>

            <tr>

                <th>#</th>

                <th>코인</th>

                <th>거래대금</th>

                <th>EMA</th>

                <th>10선</th>

                <th>경고</th>

            </tr>

        </thead>

        <tbody>

            {{ROWS}}

        </tbody>

    </table>

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
def index():

    with data_lock:

        data = list(
            latest_upbit_data
        )

        update_time = (
            latest_upbit_update
        )

    if update_time:

        update_text = (
            update_time.strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        )

    else:

        update_text = "-"

    html = HTML_PAGE.replace(
        "{{UPDATE_TIME}}",
        update_text
    )

    html = html.replace(
        "{{ROWS}}",
        rows_html(data)
    )

    return HTMLResponse(
        content=html
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
def startup():

    logging.info(
        "서버 시작"
    )

    # 최초 데이터 즉시 업데이트
    threading.Thread(
        target=update_all,
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
