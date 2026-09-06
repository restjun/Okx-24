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
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s"
)

log = logging.getLogger("trading")


# =========================================================
# 설정
# =========================================================

VOLUME_HOURS = 24
TOP_N = 30
UPDATE_MINUTES = 1

HISTORY_CHUNK = 200
MAX_HISTORY_CHUNKS = 10

USE_UPBIT = "Y"
USE_OKX = "N"

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10

KST = ZoneInfo("Asia/Seoul")

# EMA 계산 시간봉
EMA_TIMEFRAME = 240


# =========================================================
# EMA 설정
# =========================================================

# EMA1 방향 기준
EMA1_FAST = 10
EMA1_MID = 60
EMA1_SLOW = 120

# EMA2 매수 기준
# 1차 = EMA30
# 2차 = EMA60
# 3차 = EMA120
EMA_BUY_FIRST = 30
EMA_BUY_SECOND = 60
EMA_BUY_THIRD = 120


SUPPORTED_UPBIT_TIMEFRAMES = {
    5, 15, 30, 60, 240
}

SUPPORTED_OKX_TIMEFRAMES = {
    5, 15, 30, 60, 120,
    240, 360, 480, 720, 1440
}


# =========================================================
# 전역 데이터
# =========================================================

latest_upbit_data = []
latest_okx_data = []

latest_usdt_krw = 0

latest_upbit_update_time = "-"
latest_okx_update_time = "-"

latest_upbit_markets = []

request_lock = threading.Lock()
update_lock = threading.Lock()

last_request_time = 0


# =========================================================
# 시간
# =========================================================

def kst():
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


def format_timeframe(minutes):

    mapping = {
        5: "5M",
        15: "15M",
        30: "30M",
        60: "1H",
        120: "2H",
        240: "4H",
        360: "6H",
        480: "8H",
        720: "12H",
        1440: "1D"
    }

    return mapping.get(minutes, f"{minutes}M")


# =========================================================
# OKX 시간봉
# =========================================================

def get_okx_bar(minutes):

    mapping = {
        5: "5m",
        15: "15m",
        30: "30m",
        60: "1H",
        120: "2H",
        240: "4H",
        360: "6H",
        480: "8H",
        720: "12H",
        1440: "1D"
    }

    return mapping.get(minutes)


def get_okx_bar_minutes(bar):

    mapping = {
        "5m": 5,
        "15m": 15,
        "30m": 30,
        "1H": 60,
        "2H": 120,
        "4H": 240,
        "6H": 360,
        "8H": 480,
        "12H": 720,
        "1D": 1440
    }

    return mapping.get(bar)


def get_current_candle_start(timeframe_minutes):

    now = datetime.now(KST)

    total_minutes = (
        now.hour * 60 +
        now.minute
    )

    candle_minutes = (
        total_minutes //
        timeframe_minutes
    ) * timeframe_minutes

    hour = candle_minutes // 60
    minute = candle_minutes % 60

    return now.replace(
        hour=hour,
        minute=minute,
        second=0,
        microsecond=0
    )


def validate_timeframe():

    if EMA_TIMEFRAME not in SUPPORTED_UPBIT_TIMEFRAMES:
        raise ValueError(
            f"지원하지 않는 업비트 시간봉: {EMA_TIMEFRAME}"
        )

    if EMA_TIMEFRAME not in SUPPORTED_OKX_TIMEFRAMES:
        raise ValueError(
            f"지원하지 않는 OKX 시간봉: {EMA_TIMEFRAME}"
        )


# =========================================================
# HTTP
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
    method,
    url,
    params=None,
    headers=None
):

    for attempt in range(MAX_RETRIES):

        try:

            wait_request()

            response = requests.request(
                method,
                url,
                params=params,
                headers=headers,
                timeout=10
            )

            if response.status_code == 200:
                return response

            if response.status_code in (
                429,
                500,
                502,
                503,
                504
            ):

                wait_time = RATE_LIMIT_WAIT * (
                    attempt + 1
                )

                log.warning(
                    "HTTP %s 재시도 %s초",
                    response.status_code,
                    wait_time
                )

                time.sleep(wait_time)
                continue

            response.raise_for_status()

        except Exception as e:

            if attempt == MAX_RETRIES - 1:

                log.error(
                    "HTTP 요청 실패: %s",
                    e
                )

                return None

            time.sleep(
                RATE_LIMIT_WAIT *
                (attempt + 1)
            )

    return None


# =========================================================
# 업비트
# =========================================================

def get_upbit_markets():

    url = (
        "https://api.upbit.com/v1/market/all"
    )

    response = retry(
        "GET",
        url,
        params={
            "isDetails": "false"
        }
    )

    if response is None:
        return []

    try:

        data = response.json()

        markets = [
            x["market"]
            for x in data
            if x["market"].startswith("KRW-")
        ]

        return markets

    except Exception:

        return []


def get_usdt_krw():

    url = (
        "https://api.upbit.com/v1/ticker"
    )

    response = retry(
        "GET",
        url,
        params={
            "markets": "KRW-USDT"
        }
    )

    if response is None:
        return 0

    try:

        data = response.json()

        if data:
            return float(
                data[0]["trade_price"]
            )

    except Exception:
        pass

    return 0


def get_upbit_candle(
    market,
    unit,
    count=200,
    to=None
):

    url = (
        f"https://api.upbit.com/v1/candles/"
        f"minutes/{unit}"
    )

    params = {
        "market": market,
        "count": min(count, 200)
    }

    if to:
        params["to"] = to

    response = retry(
        "GET",
        url,
        params=params
    )

    if response is None:
        return pd.DataFrame()

    try:

        data = response.json()

        if not data:
            return pd.DataFrame()

        rows = []

        for item in data:

            rows.append({
                "dt": pd.to_datetime(
                    item[
                        "candle_date_time_kst"
                    ]
                ),
                "o": float(
                    item["opening_price"]
                ),
                "h": float(
                    item["high_price"]
                ),
                "l": float(
                    item["low_price"]
                ),
                "c": float(
                    item["trade_price"]
                ),
                "v": float(
                    item[
                        "candle_acc_trade_volume"
                    ]
                ),
                "value": float(
                    item[
                        "candle_acc_trade_price"
                    ]
                )
            })

        df = pd.DataFrame(rows)

        if df.empty:
            return df

        df = df.sort_values("dt")
        df = df.reset_index(drop=True)

        # 현재 진행 중인 캔들 제거
        candle_start = (
            get_current_candle_start(unit)
        )

        df = df[
            df["dt"] < candle_start
        ].copy()

        return df.reset_index(drop=True)

    except Exception as e:

        log.error(
            "업비트 캔들 오류 %s: %s",
            market,
            e
        )

        return pd.DataFrame()


def get_upbit_1h(market):

    return get_upbit_candle(
        market,
        EMA_TIMEFRAME,
        count=200
    )


def history_upbit(
    market,
    unit,
    chunks=MAX_HISTORY_CHUNKS
):

    frames = []

    to = None

    for _ in range(chunks):

        df = get_upbit_candle(
            market,
            unit,
            HISTORY_CHUNK,
            to
        )

        if df.empty:
            break

        frames.append(df)

        if len(df) < HISTORY_CHUNK:
            break

        oldest = df["dt"].iloc[0]

        to = oldest.strftime(
            "%Y-%m-%dT%H:%M:%S"
        )

        time.sleep(REQUEST_INTERVAL)

    if not frames:
        return pd.DataFrame()

    result = pd.concat(
        frames,
        ignore_index=True
    )

    result = (
        result
        .drop_duplicates("dt")
        .sort_values("dt")
        .reset_index(drop=True)
    )

    return result


# =========================================================
# OKX
# =========================================================

def get_okx_ohlcv(
    inst,
    bar="1H",
    limit=200,
    before=None
):

    url = (
        "https://www.okx.com/api/v5/market/"
        "history-candles"
    )

    params = {
        "instId": inst,
        "bar": bar,
        "limit": min(limit, 100)
    }

    if before:
        params["before"] = before

    response = retry(
        "GET",
        url,
        params=params
    )

    if response is None:
        return pd.DataFrame()

    try:

        result = response.json()

        data = result.get("data", [])

        if not data:
            return pd.DataFrame()

        rows = []

        for item in data:

            # OKX:
            # [ts,o,h,l,c,vol,volCcy,volCcyQuote,...]

            rows.append({
                "dt": pd.to_datetime(
                    int(item[0]),
                    unit="ms"
                ).tz_localize(
                    "UTC"
                ).tz_convert(
                    "Asia/Seoul"
                ).tz_localize(None),

                "o": float(item[1]),
                "h": float(item[2]),
                "l": float(item[3]),
                "c": float(item[4]),
                "v": float(item[5]),
                "value": float(item[7])
            })

        df = pd.DataFrame(rows)

        if df.empty:
            return df

        df = (
            df
            .sort_values("dt")
            .reset_index(drop=True)
        )

        # 현재 진행 중인 캔들 제거
        minutes = get_okx_bar_minutes(bar)

        if minutes:

            candle_start = (
                get_current_candle_start(
                    minutes
                )
            )

            df = df[
                df["dt"] < candle_start
            ].copy()

        return df.reset_index(drop=True)

    except Exception as e:

        log.error(
            "OKX 캔들 오류 %s: %s",
            inst,
            e
        )

        return pd.DataFrame()


def get_okx_current_price(inst):

    url = (
        "https://www.okx.com/api/v5/market/"
        "ticker"
    )

    response = retry(
        "GET",
        url,
        params={
            "instId": inst
        }
    )

    if response is None:
        return None

    try:

        data = response.json().get(
            "data",
            []
        )

        if data:
            return float(
                data[0]["last"]
            )

    except Exception:
        pass

    return None


def history_okx(
    inst,
    bar,
    chunks=MAX_HISTORY_CHUNKS
):

    frames = []

    before = None

    for _ in range(chunks):

        df = get_okx_ohlcv(
            inst,
            bar,
            HISTORY_CHUNK,
            before
        )

        if df.empty:
            break

        frames.append(df)

        if len(df) < 100:
            break

        oldest = df["dt"].iloc[0]

        before = str(
            int(
                oldest.timestamp() * 1000
            )
        )

        time.sleep(
            REQUEST_INTERVAL
        )

    if not frames:
        return pd.DataFrame()

    result = pd.concat(
        frames,
        ignore_index=True
    )

    result = (
        result
        .drop_duplicates("dt")
        .sort_values("dt")
        .reset_index(drop=True)
    )

    return result


def get_okx_symbols():

    url = (
        "https://www.okx.com/api/v5/public/"
        "instruments"
    )

    response = retry(
        "GET",
        url,
        params={
            "instType": "SWAP"
        }
    )

    if response is None:
        return []

    try:

        data = response.json().get(
            "data",
            []
        )

        symbols = []

        for item in data:

            if (
                item.get("state") == "live"
                and item.get("settleCcy") == "USDT"
                and item.get("ctType") == "linear"
            ):

                symbols.append(
                    item["instId"]
                )

        return symbols

    except Exception:

        return []


def get_okx_volume(
    inst,
    usdt
):

    bar = "1H"

    df = get_okx_ohlcv(
        inst,
        bar,
        VOLUME_HOURS + 5
    )

    if df.empty:
        return 0

    df = df.tail(
        VOLUME_HOURS
    )

    try:

        volume_usdt = df["value"].sum()

        return (
            volume_usdt *
            usdt
        )

    except Exception:

        return 0


# =========================================================
# EMA
# =========================================================

def ema(df, period):

    if (
        df is None
        or df.empty
        or "c" not in df
    ):
        return None

    return pd.to_numeric(
        df["c"],
        errors="coerce"
    ).ewm(
        span=period,
        adjust=False,
        min_periods=1
    ).mean()


# =========================================================
# EMA1 방향
#
# EMA10 > EMA60 > EMA120 = LONG
# EMA10 < EMA60 < EMA120 = SHORT
# =========================================================

def direction(df):

    if df is None or df.empty:
        return "none"

    e10 = ema(
        df,
        EMA1_FAST
    )

    e60 = ema(
        df,
        EMA1_MID
    )

    e120 = ema(
        df,
        EMA1_SLOW
    )

    if (
        e10 is None
        or e60 is None
        or e120 is None
    ):
        return "none"

    try:

        e10v = float(e10.iloc[-1])
        e60v = float(e60.iloc[-1])
        e120v = float(e120.iloc[-1])

        if (
            e10v > e60v
            and e60v > e120v
        ):
            return "long"

        if (
            e10v < e60v
            and e60v < e120v
        ):
            return "short"

    except Exception:
        pass

    return "none"


# =========================================================
# EMA1 정배열 지속 캔들 수
# =========================================================

def ema_alignment_count(df):

    result = {
        "direction": "none",
        "count": 0,
        "ema10": None,
        "ema60": None,
        "ema120": None
    }

    if df is None or df.empty:
        return result

    try:

        e10 = ema(
            df,
            EMA1_FAST
        )

        e60 = ema(
            df,
            EMA1_MID
        )

        e120 = ema(
            df,
            EMA1_SLOW
        )

        if (
            e10 is None
            or e60 is None
            or e120 is None
        ):
            return result

        s10 = pd.to_numeric(
            e10,
            errors="coerce"
        )

        s60 = pd.to_numeric(
            e60,
            errors="coerce"
        )

        s120 = pd.to_numeric(
            e120,
            errors="coerce"
        )

        long_condition = (
            (s10 > s60)
            & (s60 > s120)
        )

        short_condition = (
            (s10 < s60)
            & (s60 < s120)
        )

        if long_condition.iloc[-1]:

            count = 0

            for value in reversed(
                long_condition.tolist()
            ):

                if value:
                    count += 1
                else:
                    break

            result["direction"] = "long"
            result["count"] = count

        elif short_condition.iloc[-1]:

            count = 0

            for value in reversed(
                short_condition.tolist()
            ):

                if value:
                    count += 1
                else:
                    break

            result["direction"] = "short"
            result["count"] = count

        result["ema10"] = float(
            s10.iloc[-1]
        )

        result["ema60"] = float(
            s60.iloc[-1]
        )

        result["ema120"] = float(
            s120.iloc[-1]
        )

        return result

    except Exception as e:

        log.error(
            "EMA 정배열 계산 오류: %s",
            e
        )

        return result


# =========================================================
# EMA1 표시
#
# 이격률 계산 없음
# =========================================================

def ema_display(
    df,
    current_price=None
):

    result = ema_alignment_count(
        df
    )

    direction_value = (
        result["direction"]
    )

    count = result["count"]

    if direction_value == "long":

        return (
            f'<span class="ema-long">'
            f'🟢 정배열 ({count})'
            f'</span>'
        )

    if direction_value == "short":

        return (
            f'<span class="ema-short">'
            f'🔴 역배열 ({count})'
            f'</span>'
        )

    return (
        '<span class="ema-neutral">'
        '⚪ 중립'
        '</span>'
    )


# =========================================================
# EMA2 분할매수
#
# EMA1 정배열 조건:
# EMA10 > EMA60 > EMA120
#
# 매수 기준:
# 1차 = EMA30
# 2차 = EMA60
# 3차 = EMA120
#
# 가장 깊은 단계 하나만 표시
# =========================================================

def ema2_buy_analysis(
    df,
    current_price=None
):

    result = {
        "state": "none",
        "stage": 0,
        "display": "-",

        "ema10": None,
        "ema30": None,
        "ema60": None,
        "ema120": None,

        "current_price": current_price,
        "qualified": False
    }

    if (
        df is None
        or df.empty
        or current_price is None
    ):
        return result

    try:

        current_price = float(
            current_price
        )

        e10 = ema(
            df,
            EMA1_FAST
        ).iloc[-1]

        e30 = ema(
            df,
            EMA_BUY_FIRST
        ).iloc[-1]

        e60 = ema(
            df,
            EMA_BUY_SECOND
        ).iloc[-1]

        e120 = ema(
            df,
            EMA_BUY_THIRD
        ).iloc[-1]

        e10 = float(e10)
        e30 = float(e30)
        e60 = float(e60)
        e120 = float(e120)

        result["ema10"] = e10
        result["ema30"] = e30
        result["ema60"] = e60
        result["ema120"] = e120

        result["current_price"] = (
            current_price
        )

        # =================================================
        # EMA1 정배열 확인
        # EMA10 > EMA60 > EMA120
        # =================================================

        if not (
            e10 > e60
            and e60 > e120
        ):

            return result

        result["state"] = "long"
        result["qualified"] = True

        # =================================================
        # 가장 깊은 매수 단계
        # =================================================

        if current_price <= e120:

            result["stage"] = 3
            result["display"] = (
                "③ 3차매수"
            )

        elif current_price <= e60:

            result["stage"] = 2
            result["display"] = (
                "② 2차매수"
            )

        elif current_price <= e30:

            result["stage"] = 1
            result["display"] = (
                "① 1차매수"
            )

        return result

    except Exception as e:

        log.error(
            "EMA2 분석 오류: %s",
            e
        )

        return result


# =========================================================
# 일간 변동률
# =========================================================

def daily_change_upbit(
    market
):

    url = (
        "https://api.upbit.com/v1/ticker"
    )

    response = retry(
        "GET",
        url,
        params={
            "markets": market
        }
    )

    if response is None:
        return 0

    try:

        data = response.json()

        if not data:
            return 0

        return (
            float(
                data[0]["signed_change_rate"]
            ) * 100
        )

    except Exception:

        return 0


def daily_changes(
    markets
):

    result = {}

    if not markets:
        return result

    for i in range(
        0,
        len(markets),
        100
    ):

        batch = markets[
            i:i + 100
        ]

        response = retry(
            "GET",
            "https://api.upbit.com/v1/ticker",
            params={
                "markets": ",".join(batch)
            }
        )

        if response is None:
            continue

        try:

            data = response.json()

            for item in data:

                result[
                    item["market"]
                ] = (
                    float(
                        item[
                            "signed_change_rate"
                        ]
                    ) * 100
                )

        except Exception:
            pass

    return result


# =========================================================
# 표시용
# =========================================================

def format_change(value):

    if value is None:
        return "-"

    try:

        value = float(value)

        if value > 0:
            return (
                f'<span class="up">'
                f'+{value:.2f}%'
                f'</span>'
            )

        if value < 0:
            return (
                f'<span class="down">'
                f'{value:.2f}%'
                f'</span>'
            )

        return "0.00%"

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
                f"{value / 1_000_000_000_000:.1f}조"
            )

        if value >= 100_000_000:
            return (
                f"{value / 100_000_000:.0f}억"
            )

        if value >= 10_000:
            return (
                f"{value / 10_000:.0f}만"
            )

        return f"{value:,.0f}"

    except Exception:

        return "-"


def empty_analysis():

    return {
        "ema1": "-",
        "ema2": {
            "state": "none",
            "stage": 0,
            "display": "-",
            "ema10": None,
            "ema30": None,
            "ema60": None,
            "ema120": None,
            "current_price": None,
            "qualified": False
        },
        "change": 0,
        "qualified": False,
        "direction_1h": "none",
        "df1h": pd.DataFrame()
    }


# =========================================================
# 종목 분석
# =========================================================

def analyze(
    market_or_inst,
    exchange,
    current_price=None
):

    result = empty_analysis()

    try:

        if exchange == "upbit":

            df1 = get_upbit_1h(
                market_or_inst
            )

        else:

            bar = get_okx_bar(
                EMA_TIMEFRAME
            )

            df1 = get_okx_ohlcv(
                market_or_inst,
                bar,
                200
            )

        if df1.empty:
            return result

        if current_price is None:

            if exchange == "upbit":

                url = (
                    "https://api.upbit.com/"
                    "v1/ticker"
                )

                response = retry(
                    "GET",
                    url,
                    params={
                        "markets":
                        market_or_inst
                    }
                )

                if response:

                    data = response.json()

                    if data:
                        current_price = float(
                            data[0][
                                "trade_price"
                            ]
                        )

            else:

                current_price = (
                    get_okx_current_price(
                        market_or_inst
                    )
                )

        result["ema1"] = ema_display(
            df1,
            current_price
        )

        result["ema2"] = (
            ema2_buy_analysis(
                df1,
                current_price
            )
        )

        result["direction_1h"] = direction(
            df1
        )

        result["qualified"] = (
            result["ema2"]["qualified"]
        )

        result["df1h"] = df1

        if exchange == "upbit":

            result["change"] = (
                daily_change_upbit(
                    market_or_inst
                )
            )

        return result

    except Exception as e:

        log.error(
            "분석 오류 %s: %s",
            market_or_inst,
            e
        )

        return result


# =========================================================
# 행 생성
# =========================================================

def make_row(
    rank,
    coin,
    volume,
    analysis
):

    ema1_html = analysis.get(
        "ema1",
        "-"
    )

    ema2 = analysis.get(
        "ema2",
        {}
    )

    stage = ema2.get(
        "stage",
        0
    )

    display = ema2.get(
        "display",
        "-"
    )

    change = analysis.get(
        "change",
        0
    )

    stage_class = ""

    if stage == 1:
        stage_class = "stage1"

    elif stage == 2:
        stage_class = "stage2"

    elif stage == 3:
        stage_class = "stage3"

    return f"""
    <tr class="{stage_class}">
        <td>{rank}</td>
        <td class="coin">{coin}</td>
        <td>{format_volume(volume)}</td>
        <td>{format_change(change)}</td>
        <td>{ema1_html}</td>
        <td>{display}</td>
    </tr>
    """


# =========================================================
# 업비트 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update_time
    global latest_upbit_markets

    if USE_UPBIT != "Y":
        return

    try:

        markets = get_upbit_markets()

        latest_upbit_markets = markets

        if not markets:
            return

        ticker_response = retry(
            "GET",
            "https://api.upbit.com/v1/ticker",
            params={
                "markets": ",".join(markets)
            }
        )

        if ticker_response is None:
            return

        tickers = ticker_response.json()

        volume_map = {}

        price_map = {}

        for item in tickers:

            market = item["market"]

            volume_map[market] = float(
                item["acc_trade_price_24h"]
            )

            price_map[market] = float(
                item["trade_price"]
            )

        sorted_markets = sorted(
            markets,
            key=lambda x:
            volume_map.get(x, 0),
            reverse=True
        )

        rows = []

        for rank, market in enumerate(
            sorted_markets[:TOP_N],
            1
        ):

            analysis = analyze(
                market,
                "upbit",
                price_map.get(market)
            )

            coin = market.replace(
                "KRW-",
                ""
            )

            rows.append({
                "rank": rank,
                "coin": coin,
                "market": market,
                "volume": volume_map.get(
                    market,
                    0
                ),
                "analysis": analysis
            })

        with update_lock:

            latest_upbit_data = rows
            latest_upbit_update_time = kst()

        log.info(
            "업비트 업데이트 완료: %d개",
            len(rows)
        )

    except Exception as e:

        log.error(
            "업비트 업데이트 오류: %s",
            e
        )


# =========================================================
# OKX 업데이트
# =========================================================

def update_okx(
    usdt
):

    global latest_okx_data
    global latest_okx_update_time

    if USE_OKX != "Y":
        return

    try:

        symbols = get_okx_symbols()

        if not symbols:
            return

        volume_rows = []

        for inst in symbols:

            volume = get_okx_volume(
                inst,
                usdt
            )

            volume_rows.append({
                "inst": inst,
                "volume": volume
            })

        volume_rows.sort(
            key=lambda x:
            x["volume"],
            reverse=True
        )

        rows = []

        for rank, item in enumerate(
            volume_rows[:TOP_N],
            1
        ):

            inst = item["inst"]

            current_price = (
                get_okx_current_price(
                    inst
                )
            )

            analysis = analyze(
                inst,
                "okx",
                current_price
            )

            coin = inst.split("-")[0]

            # 업비트 상장 여부
            upbit_market = (
                f"KRW-{coin}"
            )

            if (
                upbit_market
                in latest_upbit_markets
            ):
                coin += " (업비트)"

            rows.append({
                "rank": rank,
                "coin": coin,
                "market": inst,
                "volume": item["volume"],
                "analysis": analysis
            })

        with update_lock:

            latest_okx_data = rows
            latest_okx_update_time = kst()

        log.info(
            "OKX 업데이트 완료: %d개",
            len(rows)
        )

    except Exception as e:

        log.error(
            "OKX 업데이트 오류: %s",
            e
        )


# =========================================================
# 전체 업데이트
# =========================================================

def update_all():

    global latest_usdt_krw

    log.info(
        "===== 전체 업데이트 시작 ====="
    )

    usdt = get_usdt_krw()

    if usdt > 0:
        latest_usdt_krw = usdt

    if USE_UPBIT == "Y":

        update_upbit()

    if USE_OKX == "Y":

        update_okx(
            latest_usdt_krw
        )

    log.info(
        "===== 전체 업데이트 완료 ====="
    )


# =========================================================
# EMA2 HTML
# =========================================================

def ema2_buy_html(
    analysis
):

    if not analysis:
        return "-"

    if not analysis.get(
        "qualified",
        False
    ):
        return "-"

    stage = analysis.get(
        "stage",
        0
    )

    if stage == 1:

        return (
            '<span class="buy stage1">'
            '① 1차매수'
            '</span>'
        )

    if stage == 2:

        return (
            '<span class="buy stage2">'
            '② 2차매수'
            '</span>'
        )

    if stage == 3:

        return (
            '<span class="buy stage3">'
            '③ 3차매수'
            '</span>'
        )

    return (
        '<span class="waiting">'
        '정배열 대기'
        '</span>'
    )


# =========================================================
# 행 HTML
# =========================================================

def rows_html(
    data
):

    if not data:

        return """
        <tr>
            <td colspan="6">
                데이터 없음
            </td>
        </tr>
        """

    html = ""

    for item in data:

        analysis = item[
            "analysis"
        ]

        html += make_row(
            item["rank"],
            item["coin"],
            item["volume"],
            analysis
        )

    return html


# =========================================================
# 테이블
# =========================================================

def table_html(
    data
):

    return f"""
    <table>
        <thead>
            <tr>
                <th>순위</th>
                <th>코인</th>
                <th>거래대금</th>
                <th>등락률</th>
                <th>EMA1</th>
                <th>EMA2</th>
            </tr>
        </thead>

        <tbody>
            {rows_html(data)}
        </tbody>
    </table>
    """


# =========================================================
# 거래소 섹션
# =========================================================

def section(
    title,
    data,
    update_time
):

    return f"""
    <div class="section">

        <div class="section-title">
            <span>{title}</span>
            <span class="update">
                업데이트: {update_time}
            </span>
        </div>

        {table_html(data)}

    </div>
    """


# =========================================================
# 매수 집중 리스트
# =========================================================

def buy_stage_section(
    data,
    exchange_name
):

    stages = {
        1: [],
        2: [],
        3: []
    }

    for item in data:

        analysis = item[
            "analysis"
        ]

        ema2 = analysis.get(
            "ema2",
            {}
        )

        stage = ema2.get(
            "stage",
            0
        )

        if stage in stages:
            stages[stage].append(
                item
            )

    html = f"""
    <div class="buy-section">

        <div class="buy-title">
            🎯 {exchange_name} 매수 확인 리스트
        </div>

        <div class="buy-info">
            EMA10 > EMA60 > EMA120 정배열 상태에서
            현재가 기준으로 가장 깊은 매수 단계 표시
        </div>

        <div class="buy-grid">
    """

    for stage in [1, 2, 3]:

        if stage == 1:

            title = "① 1차매수"
            desc = "현재가 ≤ EMA30"

        elif stage == 2:

            title = "② 2차매수"
            desc = "현재가 ≤ EMA60"

        else:

            title = "③ 3차매수"
            desc = "현재가 ≤ EMA120"

        html += f"""
        <div class="buy-card">

            <div class="buy-card-title">
                {title}
            </div>

            <div class="buy-card-desc">
                {desc}
            </div>

            <div class="buy-card-list">
        """

        if not stages[stage]:

            html += """
            <div class="none">
                해당 종목 없음
            </div>
            """

        else:

            for item in stages[stage]:

                html += f"""
                <div class="buy-item">
                    <span>
                        {item["rank"]}.
                        {item["coin"]}
                    </span>
                    <span>
                        {item["analysis"]["ema2"]["display"]}
                    </span>
                </div>
                """

        html += """
            </div>
        </div>
        """

    html += """
        </div>
    </div>
    """

    return html


# =========================================================
# CSS / 대시보드
# =========================================================

HTML_TEMPLATE = """
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
EMA10·60·120 + 30·60·120 분할매수
</title>

<style>

* {
    box-sizing: border-box;
}

body {

    margin: 0;

    padding: 15px;

    background: #111;

    color: #eee;

    font-family:
        Arial,
        sans-serif;

}

h1 {

    margin: 5px 0 10px;

    font-size: 22px;

}

.info {

    background: #1b1b1b;

    border: 1px solid #333;

    border-radius: 10px;

    padding: 15px;

    margin-bottom: 15px;

    line-height: 1.8;

    font-size: 14px;

}

.section {

    background: #181818;

    border-radius: 10px;

    padding: 10px;

    margin-bottom: 15px;

    overflow-x: auto;

}

.section-title {

    display: flex;

    justify-content: space-between;

    align-items: center;

    font-size: 18px;

    font-weight: bold;

    padding: 8px;

}

.update {

    font-size: 11px;

    color: #888;

    font-weight: normal;

}

table {

    width: 100%;

    border-collapse: collapse;

    min-width: 650px;

}

th,
td {

    padding: 9px 7px;

    border-bottom:
        1px solid #2c2c2c;

    text-align: center;

    font-size: 13px;

}

th {

    background: #222;

    color: #aaa;

}

.coin {

    font-weight: bold;

}

.up {

    color: #4caf50;

}

.down {

    color: #ff5252;

}

.ema-long {

    color: #4caf50;

    font-weight: bold;

}

.ema-short {

    color: #ff5252;

    font-weight: bold;

}

.ema-neutral {

    color: #aaa;

}

.buy {

    font-weight: bold;

}

.stage1 {

    color: #ffd54f;

}

.stage2 {

    color: #ff9800;

}

.stage3 {

    color: #ff5252;

}

.waiting {

    color: #777;

}

.buy-section {

    background: #181818;

    border-radius: 10px;

    padding: 12px;

    margin-bottom: 15px;

}

.buy-title {

    font-size: 18px;

    font-weight: bold;

    margin-bottom: 5px;

}

.buy-info {

    font-size: 12px;

    color: #999;

    margin-bottom: 12px;

}

.buy-grid {

    display: grid;

    grid-template-columns:
        repeat(3, 1fr);

    gap: 10px;

}

.buy-card {

    background: #202020;

    border-radius: 8px;

    padding: 10px;

}

.buy-card-title {

    font-size: 16px;

    font-weight: bold;

    margin-bottom: 4px;

}

.buy-card-desc {

    color: #999;

    font-size: 12px;

    margin-bottom: 10px;

}

.buy-card-list {

    min-height: 30px;

}

.buy-item {

    display: flex;

    justify-content: space-between;

    gap: 5px;

    padding: 7px 3px;

    border-bottom:
        1px solid #333;

    font-size: 12px;

}

.none {

    color: #666;

    font-size: 12px;

}

@media (
    max-width: 700px
) {

    .buy-grid {

        grid-template-columns: 1fr;

    }

    body {

        padding: 8px;

    }

}

</style>

</head>

<body>

<h1>
📊 EMA10·60·120 + 30·60·120 분할매수 전략
</h1>

<div class="info">

    <b>📌 전략 기준</b><br>

    {timeframe_label} 확정 캔들<br>

    <b>EMA1 : EMA10 · EMA60 · EMA120 배열</b><br>

    정배열 :
    <b>EMA10 &gt; EMA60 &gt; EMA120</b><br>

    역배열 :
    <b>EMA10 &lt; EMA60 &lt; EMA120</b><br>

    <br>

    <b>EMA2 : 정배열 상태에서 현재가 기준 분할매수</b><br>

    현재가 ≤ EMA30
    → <b>① 1차매수</b><br>

    현재가 ≤ EMA60
    → <b>② 2차매수</b><br>

    현재가 ≤ EMA120
    → <b>③ 3차매수</b><br>

    ※ 정배열 상태에서만 매수 표시<br>

    ※ 가장 깊은 매수 단계 하나만 표시<br>

    ※ EMA 계산은 확정 캔들 사용<br>

    ※ EMA2 현재가는 실시간 현재가 사용

</div>

{content}

</body>

</html>
"""


# =========================================================
# 메인 페이지
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def dashboard():

    timeframe_label = (
        format_timeframe(
            EMA_TIMEFRAME
        )
    )

    content = ""

    if USE_UPBIT == "Y":

        content += section(
            "🇰🇷 업비트",
            latest_upbit_data,
            latest_upbit_update_time
        )

        content += buy_stage_section(
            latest_upbit_data,
            "업비트"
        )

    if USE_OKX == "Y":

        content += section(
            "🌎 OKX",
            latest_okx_data,
            latest_okx_update_time
        )

        content += buy_stage_section(
            latest_okx_data,
            "OKX"
        )

    return HTML_TEMPLATE.format(
        timeframe_label=timeframe_label,
        content=content
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

            log.error(
                "스케줄러 오류: %s",
                e
            )

        time.sleep(1)


# =========================================================
# 시작
# =========================================================

if __name__ == "__main__":

    validate_timeframe()

    log.info(
        "========================================"
    )

    log.info(
        "%s EMA10·60·120 + 30·60·120 "
        "분할매수 시스템 시작",
        format_timeframe(EMA_TIMEFRAME)
    )

    log.info(
        "EMA1 = EMA10-60-120"
    )

    log.info(
        "정배열 = EMA10 > EMA60 > EMA120"
    )

    log.info(
        "역배열 = EMA10 < EMA60 < EMA120"
    )

    log.info(
        "EMA2 = 정배열 상태에서 "
        "현재가 기준 분할매수"
    )

    log.info(
        "현재가 <= EMA30 → ① 1차매수"
    )

    log.info(
        "현재가 <= EMA60 → ② 2차매수"
    )

    log.info(
        "현재가 <= EMA120 → ③ 3차매수"
    )

    log.info(
        "EMA120 이격률 계산/표시: 삭제"
    )

    log.info(
        "========================================"
    )

    # 최초 업데이트
    update_all()

    # 스케줄러
    thread = threading.Thread(
        target=scheduler_loop,
        daemon=True
    )

    thread.start()

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )
