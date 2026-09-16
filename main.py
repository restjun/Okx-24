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
# 기본 설정
# =========================================================

VOLUME_HOURS = 24
TOP_N = 10
UPDATE_MINUTES = 1

HISTORY_CHUNK = 200
MAX_HISTORY_CHUNKS = 10

USE_UPBIT = "Y"
USE_OKX = "N"

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10

KST = ZoneInfo("Asia/Seoul")


# =========================================================
# 업비트 호가 설정
# =========================================================

ORDERBOOK_RANGE = 0.01
ORDERBOOK_COUNT = 30
ORDERBOOK_DOMINANCE_GAP = 5.0


# =========================================================
# ★ ROC 필터 시간봉
#
# 1H ROC5 + 4H ROC5
#
# 둘 다 양수 → ☀️
# 둘 다 음수 → 🌧️
# 그 외 → ⚪
# =========================================================

ROC_FILTER_TIMEFRAME = 60
ROC_FILTER_HIGH_TIMEFRAME = 240

USE_ROC_FILTER_TIMEFRAME = "Y"
USE_ROC_FILTER_HIGH_TIMEFRAME = "Y"

ROC_FILTER_PERIOD = 5


# =========================================================
# ★ ROC 로켓 시간봉
#
# 이 부분은 기존 ROC 로직 유지
#
# 현재:
# ROC = 1시간봉
# ROC 기간 = 5
# =========================================================

ROC_TIMEFRAME = 60
ROC_PERIOD = 5


# =========================================================
# ROC 로켓 표시
#
# 상승:
#
# 🚀0
# 🚀1
# 🚀2
# 🚀3
# 🚀4
# 🚀5
# 🚀6
# 🚀7
# 🚀8
# 🚀9
# 🚀10
# ...
#
# 음수:
#
# 🔴 ROC -(1)
# 🔴 ROC -(2)
# 🔴 ROC -(3)
# ...
#
# ROC가 다시 0선 상승 돌파하면
# 🚀0부터 다시 시작
# =========================================================

BREAKOUT_MAX_COUNT = 999999


SUPPORTED_UPBIT_TIMEFRAMES = {
    5, 15, 30, 60, 240
}

SUPPORTED_OKX_TIMEFRAMES = {
    5, 15, 30, 60, 120,
    240, 360, 480, 720, 1440
}


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
# OKX 내부 환산용 USDT/KRW
# =========================================================

latest_usdt_krw_internal = 0


# =========================================================
# 업비트 호가 캐시
# =========================================================

latest_upbit_orderbook = {}


# =========================================================
# OKX 캐시
# =========================================================

okx_ticker_cache = {}
okx_1h_cache = {}
okx_1h_cache_time = "-"


# =========================================================
# ROC 설정 텍스트
# =========================================================

def get_roc_filter_period_text():

    return f"ROC{ROC_FILTER_PERIOD}"


def get_roc_filter_text():

    return (
        f"ROC{ROC_FILTER_PERIOD}"
        f"({format_timeframe(ROC_FILTER_TIMEFRAME)})"
        f"/"
        f"ROC{ROC_FILTER_PERIOD}"
        f"({format_timeframe(ROC_FILTER_HIGH_TIMEFRAME)})"
    )


def get_roc_text():

    return (
        f"ROC{ROC_PERIOD}"
        f"({format_timeframe(ROC_TIMEFRAME)})"
    )


# =========================================================
# 공통
# =========================================================

def kst():

    return datetime.now(KST).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def format_timeframe(minutes):

    minutes = int(minutes)

    if minutes >= 1440:
        return f"{minutes // 1440}D"

    if minutes >= 60:
        return f"{minutes // 60}H"

    return f"{minutes}M"


def get_okx_bar(minutes):

    return {
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
    }.get(
        int(minutes)
    )


def get_okx_bar_minutes(bar):

    return {
        "1m": 1,
        "3m": 3,
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
    }.get(
        str(bar)
    )


# =========================================================
# 현재 캔들 시작
# =========================================================

def get_current_candle_start(minutes):

    minutes = int(minutes)

    now = datetime.now(KST)

    total = (
        now.hour * 60
        + now.minute
    )

    block = (
        total // minutes
    ) * minutes

    day_offset, block = divmod(
        block,
        1440
    )

    current = now.replace(
        hour=block // 60,
        minute=block % 60,
        second=0,
        microsecond=0
    )

    if day_offset:

        current -= pd.Timedelta(
            days=day_offset
        )

    return current.replace(
        tzinfo=None
    )


# =========================================================
# 검증
# =========================================================

def validate_timeframe():

    global ROC_FILTER_TIMEFRAME
    global ROC_FILTER_HIGH_TIMEFRAME
    global ROC_TIMEFRAME

    try:

        ROC_FILTER_TIMEFRAME = int(
            ROC_FILTER_TIMEFRAME
        )

        ROC_FILTER_HIGH_TIMEFRAME = int(
            ROC_FILTER_HIGH_TIMEFRAME
        )

        ROC_TIMEFRAME = int(
            ROC_TIMEFRAME
        )

        ROC_FILTER_PERIOD = int(
            globals()["ROC_FILTER_PERIOD"]
        )

        ROC_PERIOD_VALUE = int(
            globals()["ROC_PERIOD"]
        )

    except Exception:

        raise ValueError(
            "ROC 시간봉 및 기간 설정은 숫자여야 합니다."
        )

    if ROC_FILTER_TIMEFRAME not in SUPPORTED_UPBIT_TIMEFRAMES:

        raise ValueError(
            f"ROC_FILTER_TIMEFRAME 오류: "
            f"{ROC_FILTER_TIMEFRAME}"
        )

    if ROC_FILTER_HIGH_TIMEFRAME not in SUPPORTED_UPBIT_TIMEFRAMES:

        raise ValueError(
            f"ROC_FILTER_HIGH_TIMEFRAME 오류: "
            f"{ROC_FILTER_HIGH_TIMEFRAME}"
        )

    if ROC_TIMEFRAME not in SUPPORTED_UPBIT_TIMEFRAMES:

        raise ValueError(
            f"ROC_TIMEFRAME 오류: "
            f"{ROC_TIMEFRAME}"
        )

    if get_okx_bar(ROC_FILTER_TIMEFRAME) is None:

        raise ValueError(
            f"OKX에서 지원하지 않는 ROC 필터 시간봉: "
            f"{ROC_FILTER_TIMEFRAME}"
        )

    if get_okx_bar(ROC_FILTER_HIGH_TIMEFRAME) is None:

        raise ValueError(
            f"OKX에서 지원하지 않는 ROC 필터 HIGH 시간봉: "
            f"{ROC_FILTER_HIGH_TIMEFRAME}"
        )

    if get_okx_bar(ROC_TIMEFRAME) is None:

        raise ValueError(
            f"OKX에서 지원하지 않는 ROC 시간봉: "
            f"{ROC_TIMEFRAME}"
        )

    if USE_ROC_FILTER_TIMEFRAME not in ("Y", "N"):

        raise ValueError(
            "USE_ROC_FILTER_TIMEFRAME은 Y 또는 N만 가능합니다."
        )

    if USE_ROC_FILTER_HIGH_TIMEFRAME not in ("Y", "N"):

        raise ValueError(
            "USE_ROC_FILTER_HIGH_TIMEFRAME은 Y 또는 N만 가능합니다."
        )

    if ROC_FILTER_PERIOD_VALUE < 1:

        raise ValueError(
            "ROC_FILTER_PERIOD는 1 이상이어야 합니다."
        )

    if ROC_PERIOD_VALUE < 1:

        raise ValueError(
            "ROC_PERIOD는 1 이상이어야 합니다."
        )

    if int(TOP_N) < 1:

        raise ValueError(
            "TOP_N은 1 이상이어야 합니다."
        )

    if not 0 < float(ORDERBOOK_RANGE) <= 1:

        raise ValueError(
            "ORDERBOOK_RANGE는 0보다 크고 1 이하여야 합니다."
        )

    if int(ORDERBOOK_COUNT) < 1:

        raise ValueError(
            "ORDERBOOK_COUNT는 1 이상이어야 합니다."
        )

    if float(ORDERBOOK_DOMINANCE_GAP) < 0:

        raise ValueError(
            "ORDERBOOK_DOMINANCE_GAP은 0 이상이어야 합니다."
        )


# =========================================================
# API 요청
# =========================================================

def wait_request():

    global last_request_time

    with request_lock:

        gap = (
            time.monotonic()
            - last_request_time
        )

        if gap < REQUEST_INTERVAL:

            time.sleep(
                REQUEST_INTERVAL - gap
            )

        last_request_time = time.monotonic()


def retry(func, *args, **kwargs):

    url = (
        args[0]
        if args
        and isinstance(args[0], str)
        else kwargs.get("url", "")
    )

    for n in range(MAX_RETRIES):

        try:

            wait_request()

            r = func(
                *args,
                **kwargs
            )

            if not hasattr(
                r,
                "status_code"
            ):

                return r

            if r.status_code == 200:

                return r

            if r.status_code == 429:

                wait = min(
                    RATE_LIMIT_WAIT * 2 ** n,
                    60
                )

            elif r.status_code >= 500:

                wait = min(
                    2 * 2 ** n,
                    30
                )

            else:

                log.warning(
                    f"[HTTP {r.status_code}] {url}"
                )

                return r

            log.warning(
                f"[API 재시도] {url} {wait}초"
            )

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

    log.error(
        f"[API 최종 실패] {url}"
    )

    return None


# =========================================================
# 업비트 마켓
# =========================================================

def get_upbit_markets():

    global latest_upbit_markets

    r = retry(
        requests.get,
        "https://api.upbit.com/v1/ticker/all",
        params={
            "quote_currencies": "KRW"
        },
        timeout=15
    )

    if r is None:
        return []

    try:

        result = []

        for x in r.json():

            market = x.get(
                "market",
                ""
            )

            if not market.startswith("KRW-"):
                continue

            try:

                volume = float(
                    x["acc_trade_price_24h"]
                )

                price = float(
                    x["trade_price"]
                )

            except Exception:

                continue

            if volume > 0 and price > 0:

                result.append({
                    "market": market,
                    "volume_24h": volume,
                    "current_price": price
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
# USDT/KRW
# =========================================================

def get_usdt_krw_internal():

    r = retry(
        requests.get,
        "https://api.upbit.com/v1/ticker?markets=KRW-USDT",
        timeout=15
    )

    if r is None:
        return None

    try:

        data = r.json()

        if not data:
            return None

        price = float(
            data[0]["trade_price"]
        )

        return price if price > 0 else None

    except Exception as e:

        log.error(
            f"OKX 환산용 USDT/KRW 오류: {e}"
        )

        return None


# =========================================================
# 업비트 호가
# =========================================================

def get_upbit_orderbooks(markets):

    if not markets:
        return {}

    result = {}

    chunk_size = 30

    for i in range(
        0,
        len(markets),
        chunk_size
    ):

        chunk = markets[
            i:i + chunk_size
        ]

        try:

            r = retry(
                requests.get,
                "https://api.upbit.com/v1/orderbook",
                params={
                    "markets": ",".join(chunk),
                    "count": ORDERBOOK_COUNT
                },
                timeout=15
            )

            if r is None:
                continue

            if r.status_code != 200:

                log.warning(
                    f"업비트 호가 HTTP "
                    f"{r.status_code}"
                )

                continue

            data = r.json()

            if not isinstance(
                data,
                list
            ):
                continue

            for item in data:

                market = item.get(
                    "market"
                )

                if market:
                    result[market] = item

        except Exception as e:

            log.error(
                f"업비트 호가 조회 오류: {e}"
            )

    return result


# =========================================================
# 호가 금액
# =========================================================

def calculate_orderbook_amount(
    orderbook,
    current_price
):

    result = {

        "bid_amount": 0.0,
        "ask_amount": 0.0,

        "total_amount": 0.0,

        "bid_ratio": 0.0,
        "ask_ratio": 0.0,

        "bid_count": 0,
        "ask_count": 0,

        "lower_price": None,
        "upper_price": None,

        "dominance": "balanced",
        "dominance_text": "균형"
    }

    if not orderbook:
        return result

    try:

        current_price = float(
            current_price
        )

    except Exception:

        return result

    if current_price <= 0:
        return result

    lower_price = (
        current_price
        * (1.0 - ORDERBOOK_RANGE)
    )

    upper_price = (
        current_price
        * (1.0 + ORDERBOOK_RANGE)
    )

    result["lower_price"] = lower_price
    result["upper_price"] = upper_price

    units = orderbook.get(
        "orderbook_units",
        []
    )

    if not isinstance(
        units,
        list
    ):

        return result

    bid_amount = 0.0
    ask_amount = 0.0

    bid_count = 0
    ask_count = 0

    for unit in units:

        try:

            bid_price = float(
                unit.get(
                    "bid_price",
                    0
                )
            )

            bid_size = float(
                unit.get(
                    "bid_size",
                    0
                )
            )

            ask_price = float(
                unit.get(
                    "ask_price",
                    0
                )
            )

            ask_size = float(
                unit.get(
                    "ask_size",
                    0
                )
            )

        except Exception:

            continue

        if (
            lower_price
            <= bid_price
            <= current_price
            and bid_size > 0
        ):

            bid_amount += (
                bid_price
                * bid_size
            )

            bid_count += 1

        if (
            current_price
            <= ask_price
            <= upper_price
            and ask_size > 0
        ):

            ask_amount += (
                ask_price
                * ask_size
            )

            ask_count += 1

    total_amount = (
        bid_amount
        + ask_amount
    )

    if total_amount > 0:

        bid_ratio = (
            bid_amount
            / total_amount
            * 100
        )

        ask_ratio = (
            ask_amount
            / total_amount
            * 100
        )

    else:

        bid_ratio = 0.0
        ask_ratio = 0.0

    difference = (
        bid_ratio
        - ask_ratio
    )

    if (
        total_amount > 0
        and difference
        >= ORDERBOOK_DOMINANCE_GAP
    ):

        dominance = "bid"
        dominance_text = "매수 우세"

    elif (
        total_amount > 0
        and difference
        <= -ORDERBOOK_DOMINANCE_GAP
    ):

        dominance = "ask"
        dominance_text = "매도 우세"

    else:

        dominance = "balanced"
        dominance_text = "균형"

    result.update({

        "bid_amount":
            bid_amount,

        "ask_amount":
            ask_amount,

        "total_amount":
            total_amount,

        "bid_ratio":
            bid_ratio,

        "ask_ratio":
            ask_ratio,

        "bid_count":
            bid_count,

        "ask_count":
            ask_count,

        "dominance":
            dominance,

        "dominance_text":
            dominance_text
    })

    return result


# =========================================================
# 호가 HTML
# =========================================================

def orderbook_html(row):

    if not row:

        return (
            '<div class="orderbook-empty">'
            '호가 정보 없음'
            '</div>'
        )

    try:

        bid_amount = float(
            row.get("bid_amount", 0)
        )

        ask_amount = float(
            row.get("ask_amount", 0)
        )

        bid_ratio = float(
            row.get("bid_ratio", 0)
        )

        ask_ratio = float(
            row.get("ask_ratio", 0)
        )

    except Exception:

        return (
            '<div class="orderbook-empty">'
            '호가 정보 없음'
            '</div>'
        )

    if (
        bid_amount <= 0
        and ask_amount <= 0
    ):

        return (
            '<div class="orderbook-empty">'
            '호가 정보 없음'
            '</div>'
        )

    bid_ratio = max(
        0.0,
        min(100.0, bid_ratio)
    )

    ask_ratio = max(
        0.0,
        min(100.0, ask_ratio)
    )

    dominance = row.get(
        "orderbook_dominance",
        "balanced"
    )

    if dominance == "bid":

        dominance_html = (
            '<span class="ob-dominance bid-dominance">'
            '▲ 매수 우세'
            '</span>'
        )

    elif dominance == "ask":

        dominance_html = (
            '<span class="ob-dominance ask-dominance">'
            '▼ 매도 우세'
            '</span>'
        )

    else:

        dominance_html = (
            '<span class="ob-dominance balanced-dominance">'
            '◆ 균형'
            '</span>'
        )

    return f"""
    <div class="orderbook-wrap">

        <div class="orderbook-row">

            <span class="orderbook-label ask-label">
                매도대기
            </span>

            <span class="orderbook-amount ask-amount">
                {format_volume(ask_amount)}
            </span>

            <div class="orderbook-bar-box">

                <div
                    class="orderbook-bar ask-bar"
                    style="width:{ask_ratio:.1f}%"
                ></div>

            </div>

            <span class="orderbook-ratio ask-ratio">
                {ask_ratio:.1f}%
            </span>

        </div>

        <div class="orderbook-row">

            <span class="orderbook-label bid-label">
                매수대기
            </span>

            <span class="orderbook-amount bid-amount">
                {format_volume(bid_amount)}
            </span>

            <div class="orderbook-bar-box">

                <div
                    class="orderbook-bar bid-bar"
                    style="width:{bid_ratio:.1f}%"
                ></div>

            </div>

            <span class="orderbook-ratio bid-ratio">
                {bid_ratio:.1f}%
            </span>

        </div>

        <div class="orderbook-bottom">

            <span class="orderbook-range">
                호가 ±{ORDERBOOK_RANGE * 100:.0f}%
            </span>

            {dominance_html}

        </div>

    </div>
    """


# =========================================================
# 업비트 캔들
# =========================================================

def get_upbit_candle(
    market,
    unit,
    count=200,
    to=None,
    include_current=False
):

    unit = int(unit)

    r = retry(
        requests.get,
        f"https://api.upbit.com/v1/candles/minutes/{unit}",
        params={
            "market": market,
            "count": min(
                max(int(count), 1),
                200
            ),
            **(
                {"to": to}
                if to
                else {}
            )
        },
        timeout=15
    )

    if r is None:
        return None

    try:

        df = pd.DataFrame(
            r.json()
        )

        if df.empty:
            return None

        df["o"] = pd.to_numeric(
            df.opening_price,
            errors="coerce"
        )

        df["h"] = pd.to_numeric(
            df.high_price,
            errors="coerce"
        )

        df["l"] = pd.to_numeric(
            df.low_price,
            errors="coerce"
        )

        df["c"] = pd.to_numeric(
            df.trade_price,
            errors="coerce"
        )

        df["volume_krw"] = pd.to_numeric(
            df.candle_acc_trade_price,
            errors="coerce"
        )

        df["datetime"] = pd.to_datetime(
            df.candle_date_time_kst,
            errors="coerce"
        )

        df = df.dropna(
            subset=[
                "datetime",
                "o",
                "h",
                "l",
                "c"
            ]
        )

        if df.empty:
            return None

        if not include_current:

            current = get_current_candle_start(
                unit
            )

            df = df[
                df.datetime < current
            ]

        if df.empty:
            return None

        return (
            df
            .sort_values("datetime")
            .drop_duplicates("datetime")
            .reset_index(drop=True)
        )

    except Exception as e:

        log.error(
            f"업비트 {unit}분 오류 "
            f"{market}: {e}"
        )

        return None


def history_upbit(
    market,
    unit,
    required=200
):

    all_df = None
    to = None

    for _ in range(MAX_HISTORY_CHUNKS):

        df = get_upbit_candle(
            market,
            unit,
            HISTORY_CHUNK,
            to
        )

        if df is None or df.empty:
            break

        all_df = (
            df.copy()
            if all_df is None
            else pd.concat(
                [df, all_df],
                ignore_index=True
            )
        )

        all_df = (
            all_df
            .drop_duplicates("datetime")
            .sort_values("datetime")
            .reset_index(drop=True)
        )

        if len(all_df) >= required:
            return all_df

        to = (
            all_df.datetime.iloc[0]
            .strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        )

    return all_df


def get_upbit_current_roc_data(
    market,
    current_price
):

    df = get_upbit_candle(
        market,
        ROC_TIMEFRAME,
        include_current=True
    )

    if df is None or df.empty:
        return None

    try:

        start = get_current_candle_start(
            ROC_TIMEFRAME
        )

        price = float(
            current_price
        )

        if price <= 0:
            return df

        mask = df.datetime == start

        if mask.any():

            df.loc[
                mask,
                "c"
            ] = price

        else:

            row = df.iloc[-1].copy()

            row["datetime"] = start
            row["c"] = price

            df = pd.concat(
                [
                    df,
                    pd.DataFrame([row])
                ],
                ignore_index=True
            )

        return (
            df
            .sort_values("datetime")
            .drop_duplicates("datetime")
            .reset_index(drop=True)
        )

    except Exception as e:

        log.error(
            f"업비트 현재 ROC 오류 "
            f"{market}: {e}"
        )

        return df


# =========================================================
# OKX
# =========================================================

def get_okx_ohlcv(
    inst,
    bar="1H",
    limit=200,
    before=None,
    include_current=False
):

    params = {
        "instId": inst,
        "bar": bar,
        "limit": min(
            max(int(limit), 1),
            200
        )
    }

    if before is not None:
        params["before"] = str(before)

    r = retry(
        requests.get,
        "https://www.okx.com/api/v5/market/candles",
        params=params,
        timeout=15
    )

    if r is None:
        return None

    try:

        data = r.json().get(
            "data",
            []
        )

        if not data:
            return None

        df = pd.DataFrame(
            data,
            columns=[
                "ts",
                "o",
                "h",
                "l",
                "c",
                "vol",
                "volCcy",
                "volCcyQuote",
                "confirm"
            ]
        )

        for col in [
            "ts",
            "o",
            "h",
            "l",
            "c",
            "vol",
            "volCcy",
            "volCcyQuote"
        ]:

            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

        if not include_current:

            df = df[
                df.confirm.astype(str) == "1"
            ]

        df["datetime"] = (
            pd.to_datetime(
                df.ts,
                unit="ms",
                utc=True
            )
            .dt.tz_convert(KST)
            .dt.tz_localize(None)
        )

        if not include_current:

            minutes = get_okx_bar_minutes(
                bar
            )

            if minutes:

                current = get_current_candle_start(
                    minutes
                )

                df = df[
                    df.datetime < current
                ]

        if df.empty:
            return None

        return (
            df
            .sort_values("ts")
            .drop_duplicates("ts")
            .reset_index(drop=True)
        )

    except Exception as e:

        log.error(
            f"OKX {inst} {bar} 오류: {e}"
        )

        return None


def history_okx(
    inst,
    bar,
    required=200
):

    all_df = None
    before = None

    for _ in range(MAX_HISTORY_CHUNKS):

        df = get_okx_ohlcv(
            inst,
            bar,
            HISTORY_CHUNK,
            before
        )

        if df is None or df.empty:
            break

        all_df = (
            df.copy()
            if all_df is None
            else pd.concat(
                [df, all_df],
                ignore_index=True
            )
        )

        all_df = (
            all_df
            .drop_duplicates("ts")
            .sort_values("ts")
            .reset_index(drop=True)
        )

        if len(all_df) >= required:
            return all_df

        before = int(
            all_df.ts.iloc[0]
        )

    return all_df


def get_okx_tickers():

    global okx_ticker_cache

    r = retry(
        requests.get,
        "https://www.okx.com/api/v5/market/tickers",
        params={
            "instType": "SWAP"
        },
        timeout=15
    )

    if r is None:
        return {}

    try:

        result = {}

        for x in r.json().get(
            "data",
            []
        ):

            inst = x.get(
                "instId",
                ""
            )

            if not inst.endswith(
                "-USDT-SWAP"
            ):
                continue

            try:

                last = float(
                    x.get("last", 0)
                )

            except Exception:

                last = 0

            if last > 0:

                result[inst] = {
                    "last": last
                }

        okx_ticker_cache = result

        return result

    except Exception as e:

        log.error(
            f"OKX ticker 오류: {e}"
        )

        return {}


def get_okx_symbols():

    r = retry(
        requests.get,
        "https://www.okx.com/api/v5/public/instruments",
        params={
            "instType": "SWAP"
        },
        timeout=15
    )

    if r is None:
        return []

    try:

        return [
            x["instId"]
            for x in r.json().get(
                "data",
                []
            )
            if x.get(
                "instId",
                ""
            ).endswith("-USDT-SWAP")
            and x.get("state") == "live"
        ]

    except Exception:

        return []


def get_okx_volume_cached(
    inst,
    usdt
):

    df = get_okx_ohlcv(
        inst,
        "1H",
        VOLUME_HOURS
    )

    if df is None or df.empty:
        return None

    okx_1h_cache[inst] = df.copy()

    try:

        volume = pd.to_numeric(
            df.volCcyQuote,
            errors="coerce"
        ).sum()

        return (
            float(volume)
            * float(usdt)
        )

    except Exception:

        return None


def get_okx_cached_price(inst):

    try:

        item = okx_ticker_cache.get(
            inst
        )

        if not item:
            return None

        price = float(
            item.get("last", 0)
        )

        return price if price > 0 else None

    except Exception:

        return None


def get_okx_current_roc_data(
    inst,
    current_price
):

    bar = get_okx_bar(
        ROC_TIMEFRAME
    )

    if not bar:
        return None

    df = get_okx_ohlcv(
        inst,
        bar,
        200,
        include_current=True
    )

    if df is None or df.empty:
        return None

    try:

        start = get_current_candle_start(
            ROC_TIMEFRAME
        )

        price = float(
            current_price
        )

        if price <= 0:
            return df

        mask = df.datetime == start

        if mask.any():

            df.loc[
                mask,
                "c"
            ] = price

        else:

            row = df.iloc[-1].copy()

            row["datetime"] = start
            row["c"] = price

            df = pd.concat(
                [
                    df,
                    pd.DataFrame([row])
                ],
                ignore_index=True
            )

        return (
            df
            .sort_values("ts")
            .drop_duplicates("ts")
            .reset_index(drop=True)
        )

    except Exception as e:

        log.error(
            f"OKX 현재 ROC 오류 {inst}: {e}"
        )

        return df


# =========================================================
# ROC 계산
# =========================================================

def roc(
    df,
    period
):

    if (
        df is None
        or df.empty
        or "c" not in df
    ):

        return None

    try:

        close = pd.to_numeric(
            df["c"],
            errors="coerce"
        )

        return (
            close
            / close.shift(int(period))
            - 1
        ) * 100

    except Exception as e:

        log.error(
            f"ROC 계산 오류: {e}"
        )

        return None


# =========================================================
# ★ ROC 필터 상태
#
# ROC5 기준
#
# 양수 = 상승 방향
# 음수 = 하락 방향
# 0 = 중립
# =========================================================

def roc_filter_state(
    df,
    period=ROC_FILTER_PERIOD
):

    result = {

        "value": None,

        "direction": "none",

        "display": "⚪",

        "period": int(period)
    }

    if df is None or df.empty:
        return result

    try:

        values = roc(
            df,
            period
        )

        if values is None or values.empty:
            return result

        value = float(
            values.iloc[-1]
        )

        if pd.isna(value):
            return result

        if value > 0:

            direction = "long"
            display = "☀️"

        elif value < 0:

            direction = "short"
            display = "🌧️"

        else:

            direction = "none"
            display = "⚪"

        result.update({

            "value": value,

            "direction":
                direction,

            "display":
                display
        })

        return result

    except Exception as e:

        log.error(
            f"ROC 필터 오류: {e}"
        )

        return result


# =========================================================
# ★ ROC 필터 방향
#
# 1H ROC5
# +
# 4H ROC5
#
# 둘 다 양수 → long
# 둘 다 음수 → short
# 그 외 → none
# =========================================================

def roc_filter_direction(
    r1,
    r_high
):

    selected = []

    if USE_ROC_FILTER_TIMEFRAME == "Y":

        selected.append(
            r1.get(
                "direction",
                "none"
            )
        )

    if USE_ROC_FILTER_HIGH_TIMEFRAME == "Y":

        selected.append(
            r_high.get(
                "direction",
                "none"
            )
        )

    if not selected:

        return "none"

    if all(
        x == "long"
        for x in selected
    ):

        return "long"

    if all(
        x == "short"
        for x in selected
    ):

        return "short"

    return "none"


# =========================================================
# ★ ROC 필터 통과
# =========================================================

def roc_filter_pass(
    r1,
    r_high
):

    selected = []

    if USE_ROC_FILTER_TIMEFRAME == "Y":

        d = r1.get(
            "direction",
            "none"
        )

        if d not in (
            "long",
            "short"
        ):

            return False

        selected.append(d)

    if USE_ROC_FILTER_HIGH_TIMEFRAME == "Y":

        d = r_high.get(
            "direction",
            "none"
        )

        if d not in (
            "long",
            "short"
        ):

            return False

        selected.append(d)

    if not selected:
        return True

    return len(
        set(selected)
    ) == 1


# =========================================================
# EMA 대체용 ROC 표시
# =========================================================

def roc_filter_display(r):

    if not r:

        return {
            "display": "⚪",
            "direction": "none",
            "value": None
        }

    direction = r.get(
        "direction",
        "none"
    )

    value = r.get(
        "value"
    )

    if direction == "long":

        icon = "☀️"

    elif direction == "short":

        icon = "🌧️"

    else:

        icon = "⚪"

    return {

        "display":
            icon,

        "direction":
            direction,

        "value":
            value
    }


# =========================================================
# ROC 상태 분석
# =========================================================

def roc_analysis(
    df_confirmed,
    df_current
):

    result = {

        "roc10": None,
        "roc10_previous": None,

        "roc10_count": 0,

        "roc10_negative_count": 0,

        "roc_progress_start_time": None,
        "roc_negative_progress_start_time": None,

        "long_breakout": False,
        "long_breakout_count": 0,
        "long_breakout_state": "none",

        "state": "none",
        "display": "⚪ 0"
    }

    if (
        df_confirmed is None
        or df_confirmed.empty
        or df_current is None
        or df_current.empty
    ):

        return result

    try:

        confirmed = roc(
            df_confirmed,
            ROC_PERIOD
        )

        current = roc(
            df_current,
            ROC_PERIOD
        )

        if (
            confirmed is None
            or current is None
        ):

            return result

        previous_confirmed = float(
            confirmed.iloc[-1]
        )

        current_value = float(
            current.iloc[-1]
        )

        if (
            pd.isna(previous_confirmed)
            or pd.isna(current_value)
        ):

            return result

        values = [
            float(x)
            for x in current.tolist()
            if not pd.isna(x)
        ]

        if not values:

            return result

        # =================================================
        # 양수 연속
        # =================================================

        positive_count = 0

        for value in reversed(values):

            if value > 0:

                positive_count += 1

            else:

                break

        # =================================================
        # 음수 연속
        # =================================================

        negative_count = 0

        for value in reversed(values):

            if value < 0:

                negative_count += 1

            else:

                break

        # =================================================
        # 상승 돌파
        # =================================================

        breakout_count = 0
        breakout_state = "none"

        if current_value > 0:

            if positive_count > 0:

                if len(values) > positive_count:

                    before_value = values[
                        -positive_count - 1
                    ]

                    if before_value <= 0:

                        breakout_count = (
                            positive_count - 1
                        )

                        breakout_state = (
                            "current"
                            if breakout_count == 0
                            else "confirmed"
                        )

        result.update({

            "roc10":
                current_value,

            "roc10_previous":
                previous_confirmed,

            "roc10_count":
                positive_count,

            "roc10_negative_count":
                negative_count,

            "long_breakout":
                breakout_state != "none",

            "long_breakout_count":
                breakout_count,

            "long_breakout_state":
                breakout_state

        })

        # =================================================
        # ★ 상승 로켓
        #
        # ROC가 양수인 동안
        # 0부터 계속 증가
        # =================================================

        if (
            current_value > 0
            and positive_count > 0
        ):

            is_new_breakout = False

            if len(values) > positive_count:

                before_value = values[
                    -positive_count - 1
                ]

                if before_value <= 0:

                    is_new_breakout = True

            if is_new_breakout:

                count = (
                    positive_count - 1
                )

                result.update({

                    "state":
                        "long_breakout",

                    "long_breakout":
                        True,

                    "long_breakout_count":
                        count,

                    "long_breakout_state":
                        (
                            "current"
                            if count == 0
                            else "confirmed"
                        ),

                    "display":
                        f"🚀{count}"

                })

            else:

                count = max(
                    positive_count - 1,
                    0
                )

                result.update({

                    "state":
                        "long_breakout",

                    "long_breakout":
                        True,

                    "long_breakout":
                        True,

                    "long_breakout_count":
                        count,

                    "long_breakout_state":
                        "confirmed",

                    "display":
                        f"🚀{count}"

                })

        # =================================================
        # ★ ROC 음수
        # =================================================

        elif current_value < 0:

            result.update({

                "state":
                    "negative",

                "display":
                    f"🔴 ROC -({negative_count})",

                "long_breakout":
                    False,

                "long_breakout_count":
                    0,

                "long_breakout_state":
                    "none"

            })

        # =================================================
        # ROC 0
        # =================================================

        else:

            result.update({

                "state":
                    "none",

                "display":
                    "⚪ 0",

                "long_breakout":
                    False,

                "long_breakout_count":
                    0,

                "long_breakout_state":
                    "none"

            })

        return result

    except Exception as e:

        log.error(
            f"ROC 분석 오류: {e}"
        )

        return result


# =========================================================
# 등락률
# =========================================================

def daily_change_upbit(market):

    r = retry(
        requests.get,
        "https://api.upbit.com/v1/candles/days",
        params={
            "market": market,
            "count": 2
        },
        timeout=15
    )

    if r is None:
        return None

    try:

        data = r.json()

        if len(data) < 2:
            return None

        current = float(
            data[0]["trade_price"]
        )

        previous = float(
            data[1]["trade_price"]
        )

        if previous == 0:
            return None

        return [
            (
                current - previous
            )
            / previous
            * 100
        ]

    except Exception as e:

        log.error(
            f"업비트 일봉 등락률 오류 "
            f"{market}: {e}"
        )

        return None


def daily_changes(df):

    if df is None or df.empty:
        return None

    try:

        x = df.copy()

        x["datetime"] = pd.to_datetime(
            x.datetime,
            errors="coerce"
        )

        x["c"] = pd.to_numeric(
            x.c,
            errors="coerce"
        )

        x = (
            x
            .dropna(
                subset=[
                    "datetime",
                    "c"
                ]
            )
            .set_index("datetime")
        )

        daily = (
            x.c
            .resample(
                "1D",
                offset="9h"
            )
            .last()
            .dropna()
        )

        if len(daily) < 2:
            return None

        previous = float(
            daily.iloc[-2]
        )

        current = float(
            daily.iloc[-1]
        )

        if previous == 0:
            return None

        return [
            (
                current - previous
            )
            / previous
            * 100
        ]

    except Exception:

        return None


def get_change_value(x):

    try:

        if x is None:
            return None

        return float(
            x[0]
            if isinstance(
                x,
                (list, tuple)
            )
            else x
        )

    except Exception:

        return None


def format_change(x):

    x = get_change_value(x)

    if x is None:
        return "-"

    if x > 0:

        return (
            '<span class="up">'
            f'▲+{x:.1f}%'
            '</span>'
        )

    if x < 0:

        return (
            '<span class="down">'
            f'▼{x:.1f}%'
            '</span>'
        )

    return (
        '<span class="zero">'
        '0.0%'
        '</span>'
    )


def format_volume(v):

    try:
        v = float(v)
    except Exception:
        return "-"

    if v >= 1e12:
        return f"{v / 1e12:.1f}조"

    if v >= 1e8:
        return f"{v / 1e8:.0f}억"

    if v >= 1e4:
        return f"{v / 1e4:.0f}만"

    return f"{v:,.0f}"


# =========================================================
# 기본 분석
# =========================================================

def empty_analysis():

    r = {

        "display": "⚪",

        "direction": "none",

        "value": None
    }

    return {

        "roc_filter_1h":
            r.copy(),

        "roc_filter_high":
            r.copy(),

        "roc": {

            "roc10": None,
            "roc10_previous": None,

            "roc10_count": 0,

            "roc10_negative_count": 0,

            "long_breakout": False,
            "long_breakout_count": 0,
            "long_breakout_state": "none",

            "state": "none",
            "display": "⚪ 0"
        },

        "changes":
            None,

        "breakout_qualified":
            False,

        "progress_qualified":
            False,

        "roc3_long_progress_qualified":
            False,

        "direction_1h":
            "none",

        "df1h":
            None
    }


# =========================================================
# 신호 자격
# =========================================================

def get_signal_qualified(
    r1,
    r_high,
    r
):

    filter_direction = roc_filter_direction(
        r1,
        r_high
    )

    filter_pass = roc_filter_pass(
        r1,
        r_high
    )

    if (
        USE_ROC_FILTER_TIMEFRAME == "N"
        and USE_ROC_FILTER_HIGH_TIMEFRAME == "N"
    ):

        long_base = True

    else:

        long_base = (
            filter_pass
            and filter_direction == "long"
        )

    try:

        roc_value = float(
            r.get("roc10")
        )

    except Exception:

        roc_value = None

    long_breakout_qualified = (

        long_base

        and roc_value is not None

        and roc_value > 0

        and r.get(
            "long_breakout_state",
            "none"
        ) != "none"

        and int(
            r.get(
                "long_breakout_count",
                0
            )
        ) >= 0
    )

    return {

        "breakout_qualified":
            long_breakout_qualified,

        "progress_qualified":
            False,

        "roc3_long_progress_qualified":
            False,

        "filter_direction":
            filter_direction
    }


# =========================================================
# OKX 분석
# =========================================================

def analyze_okx(
    market,
    current_price=None
):

    filter_bar = get_okx_bar(
        ROC_FILTER_TIMEFRAME
    )

    filter_high_bar = get_okx_bar(
        ROC_FILTER_HIGH_TIMEFRAME
    )

    roc_bar = get_okx_bar(
        ROC_TIMEFRAME
    )

    if (
        not filter_bar
        or not filter_high_bar
        or not roc_bar
    ):

        return None

    df_filter = history_okx(
        market,
        filter_bar
    )

    df_filter_high = history_okx(
        market,
        filter_high_bar
    )

    df_roc_confirmed = history_okx(
        market,
        roc_bar
    )

    df_roc_current = get_okx_current_roc_data(
        market,
        current_price
    )

    if (
        df_filter is None
        or df_filter.empty
    ):

        return None

    if (
        df_filter_high is None
        or df_filter_high.empty
    ):

        return None

    if (
        df_roc_confirmed is None
        or df_roc_confirmed.empty
    ):

        return None

    r1 = roc_filter_display(
        roc_filter_state(
            df_filter,
            ROC_FILTER_PERIOD
        )
    )

    r_high = roc_filter_display(
        roc_filter_state(
            df_filter_high,
            ROC_FILTER_PERIOD
        )
    )

    r = roc_analysis(
        df_roc_confirmed,
        df_roc_current
    )

    changes = daily_changes(
        df_filter
    )

    q = get_signal_qualified(
        r1,
        r_high,
        r
    )

    return {

        "roc_filter_1h":
            r1,

        "roc_filter_high":
            r_high,

        "roc":
            r,

        "changes":
            changes,

        **q,

        "direction_1h":
            q["filter_direction"],

        "df1h":
            df_filter
    }


# =========================================================
# 통합 분석
# =========================================================

def analyze(
    market,
    okx=False,
    current_price=None
):

    if okx:

        return analyze_okx(
            market,
            current_price
        )

    df_filter = history_upbit(
        market,
        ROC_FILTER_TIMEFRAME
    )

    df_filter_high = history_upbit(
        market,
        ROC_FILTER_HIGH_TIMEFRAME
    )

    df_roc_confirmed = history_upbit(
        market,
        ROC_TIMEFRAME
    )

    df_roc_current = get_upbit_current_roc_data(
        market,
        current_price
    )

    changes = daily_change_upbit(
        market
    )

    if (
        df_filter is None
        or df_filter.empty
    ):

        return None

    if (
        df_filter_high is None
        or df_filter_high.empty
    ):

        return None

    if (
        df_roc_confirmed is None
        or df_roc_confirmed.empty
    ):

        return None

    r1 = roc_filter_display(
        roc_filter_state(
            df_filter,
            ROC_FILTER_PERIOD
        )
    )

    r_high = roc_filter_display(
        roc_filter_state(
            df_filter_high,
            ROC_FILTER_PERIOD
        )
    )

    r = roc_analysis(
        df_roc_confirmed,
        df_roc_current
    )

    q = get_signal_qualified(
        r1,
        r_high,
        r
    )

    return {

        "roc_filter_1h":
            r1,

        "roc_filter_high":
            r_high,

        "roc":
            r,

        "changes":
            changes,

        **q,

        "direction_1h":
            q["filter_direction"],

        "df1h":
            df_filter
    }


# =========================================================
# 행
# =========================================================

def make_row(
    rank,
    name,
    volume,
    analysis,
    current_price=None,
    orderbook_info=None
):

    a = (
        analysis
        or empty_analysis()
    )

    ob = (
        orderbook_info
        or {}
    )

    return {

        "rank":
            rank,

        "name":
            name,

        "change":
            format_change(
                a.get("changes")
            ),

        "change_value":
            get_change_value(
                a.get("changes")
            ),

        "volume":
            format_volume(volume),

        "current_price":
            current_price,

        "roc_filter_1h":
            a.get(
                "roc_filter_1h",
                {}
            ),

        "roc_filter_high":
            a.get(
                "roc_filter_high",
                {}
            ),

        "roc":
            a["roc"],

        "breakout_qualified":
            a["breakout_qualified"],

        "progress_qualified":
            False,

        "roc3_long_progress_qualified":
            False,

        "direction":
            a["direction_1h"],

        "bid_amount":
            float(
                ob.get(
                    "bid_amount",
                    0
                )
            ),

        "ask_amount":
            float(
                ob.get(
                    "ask_amount",
                    0
                )
            ),

        "bid_ratio":
            float(
                ob.get(
                    "bid_ratio",
                    0
                )
            ),

        "ask_ratio":
            float(
                ob.get(
                    "ask_ratio",
                    0
                )
            ),

        "bid_count":
            int(
                ob.get(
                    "bid_count",
                    0
                )
            ),

        "ask_count":
            int(
                ob.get(
                    "ask_count",
                    0
                )
            ),

        "orderbook_dominance":
            ob.get(
                "dominance",
                "balanced"
            ),

        "orderbook_dominance_text":
            ob.get(
                "dominance_text",
                "균형"
            )
    }


# =========================================================
# 상승 돌파 후보
# =========================================================

def is_breakout(row):

    if not row:
        return False

    r = row.get(
        "roc",
        {}
    )

    try:

        value = float(
            r.get("roc10")
        )

        count = int(
            r.get(
                "long_breakout_count",
                0
            )
        )

    except Exception:

        return False

    return (
        row.get(
            "breakout_qualified",
            False
        )
        and value > 0
        and count >= 0
    )


def is_progress(row):

    return False


def is_roc3_progress(row):

    return False


# =========================================================
# 상승 통합 후보
# =========================================================

def is_long_combined(row):

    if not row:
        return False

    r = row.get(
        "roc",
        {}
    )

    try:

        roc_value = float(
            r.get("roc10")
        )

        count = int(
            r.get(
                "long_breakout_count",
                0
            )
        )

    except Exception:

        return False

    if roc_value <= 0:
        return False

    if count < 0:
        return False

    daily_change = row.get(
        "change_value"
    )

    if daily_change is None:
        return False

    try:

        daily_change = float(
            daily_change
        )

    except Exception:

        return False

    if daily_change < 0:
        return False

    return bool(
        row.get(
            "breakout_qualified",
            False
        )
    )


# =========================================================
# TOP 시장폭
# =========================================================

def top_daily_breadth(data):

    result = {
        "positive": 0,
        "negative": 0,
        "zero": 0,
        "total": 0,
        "ratio": 0.0,
        "icon": "⚪",
        "state": "neutral"
    }

    if not data:
        return result

    positive = 0
    negative = 0
    zero = 0
    total = 0

    for row in data:

        if not row:
            continue

        value = row.get(
            "change_value"
        )

        try:

            if value is None:
                continue

            value = float(value)

        except Exception:

            continue

        if pd.isna(value):
            continue

        total += 1

        if value > 0:
            positive += 1

        elif value < 0:
            negative += 1

        else:
            zero += 1

    result["positive"] = positive
    result["negative"] = negative
    result["zero"] = zero
    result["total"] = total

    if total <= 0:
        return result

    result["ratio"] = (
        positive / total * 100
    )

    if positive > negative:

        result["icon"] = "☀️"
        result["state"] = "up"

    elif positive < negative:

        result["icon"] = "🌧️"
        result["state"] = "down"

    else:

        result["icon"] = "⚪"
        result["state"] = "neutral"

    return result


# =========================================================
# Upbit 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update_time
    global latest_upbit_orderbook

    log.info(
        f"========== 업비트 TOP{TOP_N} =========="
    )

    markets = sorted(
        get_upbit_markets(),
        key=lambda x: x["volume_24h"],
        reverse=True
    )

    top_markets = markets[
        :TOP_N
    ]

    market_codes = [
        x["market"]
        for x in top_markets
    ]

    orderbooks = get_upbit_orderbooks(
        market_codes
    )

    latest_upbit_orderbook = (
        orderbooks.copy()
    )

    log.info(
        f"업비트 호가 조회 "
        f"{len(orderbooks)}/{len(market_codes)}개"
    )

    rows = []

    for rank, item in enumerate(
        top_markets,
        1
    ):

        market = item["market"]

        coin = market.replace(
            "KRW-",
            ""
        )

        price = item[
            "current_price"
        ]

        try:

            a = analyze(
                market,
                current_price=price
            )

        except Exception as e:

            log.error(
                f"업비트 상세 오류 "
                f"{market}: {e}"
            )

            a = None

        ob = calculate_orderbook_amount(
            orderbooks.get(market),
            price
        )

        rows.append(
            make_row(
                rank,
                coin,
                item["volume_24h"],
                a,
                price,
                ob
            )
        )

        log.info(
            f"[호가] "
            f"{coin} | "
            f"매수 {format_volume(ob['bid_amount'])} "
            f"({ob['bid_ratio']:.1f}%) / "
            f"매도 {format_volume(ob['ask_amount'])} "
            f"({ob['ask_ratio']:.1f}%) / "
            f"{ob['dominance_text']}"
        )

    latest_upbit_data = rows

    breadth = top_daily_breadth(
        latest_upbit_data
    )

    latest_upbit_update_time = kst()

    log.info(
        f"업비트 완료 / "
        f"상승 "
        f"{sum(is_long_combined(x) for x in rows)}개"
    )

    log.info(
        f"TOP{TOP_N} 당일 변동 시장폭 / "
        f"양수 {breadth['positive']} / "
        f"음수 {breadth['negative']} / "
        f"보합 {breadth['zero']} / "
        f"판단 {breadth['icon']}"
    )


# =========================================================
# OKX 업데이트
# =========================================================

def update_okx(usdt):

    global latest_okx_data
    global latest_okx_update_time
    global okx_1h_cache
    global okx_1h_cache_time

    if not usdt or usdt <= 0:
        return False

    okx_1h_cache = {}

    tickers = get_okx_tickers()

    if not tickers:
        return False

    symbols = get_okx_symbols()

    if not symbols:
        return False

    symbols = [
        x
        for x in symbols
        if x in tickers
    ]

    upbit_set = {
        x.replace(
            "KRW-",
            ""
        )
        for x in latest_upbit_markets
    }

    volumes = {}

    for idx, symbol in enumerate(
        symbols,
        1
    ):

        v = get_okx_volume_cached(
            symbol,
            usdt
        )

        if v and v > 0:
            volumes[symbol] = v

        if idx % 50 == 0:

            log.info(
                f"OKX 거래대금 "
                f"{idx}/{len(symbols)} 완료"
            )

    top = sorted(
        volumes,
        key=volumes.get,
        reverse=True
    )[:TOP_N]

    rows = []

    for rank, symbol in enumerate(
        top,
        1
    ):

        coin = symbol.replace(
            "-USDT-SWAP",
            ""
        )

        name = (
            f"{coin} (업비트)"
            if coin in upbit_set
            else coin
        )

        price = get_okx_cached_price(
            symbol
        )

        try:

            a = analyze(
                symbol,
                True,
                price
            )

        except Exception as e:

            log.error(
                f"OKX 상세 오류 "
                f"{symbol}: {e}"
            )

            a = None

        rows.append(
            make_row(
                rank,
                name,
                volumes[symbol],
                a,
                price
            )
        )

    latest_okx_data = rows

    breadth = top_daily_breadth(
        latest_okx_data
    )

    okx_1h_cache_time = kst()
    latest_okx_update_time = kst()

    log.info(
        f"OKX TOP{TOP_N} 완료"
    )

    log.info(
        f"OKX TOP{TOP_N} 당일 변동 시장폭 / "
        f"양수 {breadth['positive']} / "
        f"음수 {breadth['negative']} / "
        f"보합 {breadth['zero']} / "
        f"판단 {breadth['icon']}"
    )

    return True


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    global latest_upbit_data
    global latest_okx_data
    global latest_usdt_krw_internal

    if not update_lock.acquire(False):

        log.warning(
            "이전 조회 진행 중 → 건너뜀"
        )

        return

    try:

        usdt = (
            latest_usdt_krw_internal
        )

        if USE_OKX == "Y":

            try:

                current_usdt = (
                    get_usdt_krw_internal()
                )

                if current_usdt is not None:

                    latest_usdt_krw_internal = (
                        current_usdt
                    )

                    usdt = current_usdt

            except Exception as e:

                log.exception(
                    f"OKX 환산용 USDT/KRW 오류: {e}"
                )

        if USE_UPBIT == "Y":

            try:

                update_upbit()

            except Exception as e:

                log.exception(
                    f"업비트 업데이트 오류: {e}"
                )

        else:

            latest_upbit_data = []

        if USE_OKX == "Y":

            try:

                if usdt and usdt > 0:

                    update_okx(
                        usdt
                    )

            except Exception as e:

                log.exception(
                    f"OKX 업데이트 오류: {e}"
                )

        else:

            latest_okx_data = []

    finally:

        update_lock.release()


# =========================================================
# 시장 등락률 HTML
# =========================================================

def market_change_html(value):

    if value is None:

        return (
            '<span class="market-zero">-</span>'
        )

    try:

        value = float(value)

    except Exception:

        return (
            '<span class="market-zero">-</span>'
        )

    if value > 0:

        return (
            '<span class="market-up">'
            f'▲+{value:.1f}%'
            '</span>'
        )

    if value < 0:

        return (
            '<span class="market-down">'
            f'▼{value:.1f}%'
            '</span>'
        )

    return (
        '<span class="market-zero">'
        '0.0%'
        '</span>'
    )


# =========================================================
# 가격
# =========================================================

def format_market_price(price):

    if price is None:
        return "-"

    try:

        price = float(price)

    except Exception:

        return "-"

    if price >= 100000000:
        return f"{price / 100000000:.2f}억"

    if price >= 10000:
        return f"{price:,.0f}"

    if price >= 1:
        return f"{price:,.2f}"

    return f"{price:.6f}"


# =========================================================
# BTC
# =========================================================

def get_market_row(coin):

    for row in latest_upbit_data:

        if row.get("name") == coin:
            return row

    return None


# =========================================================
# ★ BTC 시장 시황
#
# EMA → ROC5로 변경
# =========================================================

def market_summary_html():

    btc = get_market_row("BTC")

    if btc is None:

        btc_roc_icon = "⚪"
        btc_roc_display = "-"
        btc_roc_class = "wait"

    else:

        roc_1h = btc.get(
            "roc_filter_1h",
            {}
        )

        roc_4h = btc.get(
            "roc_filter_high",
            {}
        )

        direction_1h = roc_1h.get(
            "direction",
            "none"
        )

        direction_4h = roc_4h.get(
            "direction",
            "none"
        )

        if (
            direction_1h == "long"
            and direction_4h == "long"
        ):

            btc_roc_icon = "☀️"
            btc_roc_display = "정방향"
            btc_roc_class = "up"

        elif (
            direction_1h == "short"
            and direction_4h == "short"
        ):

            btc_roc_icon = "🌧️"
            btc_roc_display = "역방향"
            btc_roc_class = "down"

        else:

            btc_roc_icon = "⚪"
            btc_roc_display = "중립"
            btc_roc_class = "wait"

    if btc is None:

        btc_daily_icon = "⚪"
        btc_daily_display = "-"
        btc_daily_class = "wait"

    else:

        btc_daily_change = btc.get(
            "change_value"
        )

        if btc_daily_change is None:

            btc_daily_icon = "⚪"
            btc_daily_display = "-"
            btc_daily_class = "wait"

        else:

            try:

                btc_daily_change = float(
                    btc_daily_change
                )

                if btc_daily_change > 0:

                    btc_daily_icon = "☀️"

                    btc_daily_display = (
                        f"+{btc_daily_change:.2f}%"
                    )

                    btc_daily_class = "up"

                elif btc_daily_change < 0:

                    btc_daily_icon = "🌧️"

                    btc_daily_display = (
                        f"{btc_daily_change:.2f}%"
                    )

                    btc_daily_class = "down"

                else:

                    btc_daily_icon = "⚪"

                    btc_daily_display = "0.00%"

                    btc_daily_class = "wait"

            except Exception:

                btc_daily_icon = "⚪"
                btc_daily_display = "-"
                btc_daily_class = "wait"

    breadth = top_daily_breadth(
        latest_upbit_data
    )

    breadth_icon = breadth.get(
        "icon",
        "⚪"
    )

    breadth_positive = breadth.get(
        "positive",
        0
    )

    breadth_negative = breadth.get(
        "negative",
        0
    )

    breadth_zero = breadth.get(
        "zero",
        0
    )

    breadth_total = breadth.get(
        "total",
        0
    )

    if breadth_total > 0:

        breadth_display = (
            f"양수 {breadth_positive}"
            f" / "
            f"음수 {breadth_negative}"
        )

        if breadth_zero > 0:

            breadth_display += (
                f" · 0 {breadth_zero}"
            )

    else:

        breadth_display = "-"

    if breadth.get("state") == "up":

        breadth_class = "up"

    elif breadth.get("state") == "down":

        breadth_class = "down"

    else:

        breadth_class = "wait"

    if btc is None:

        btc_price_display = "-"
        btc_change_display = "-"

    else:

        btc_price_display = (
            format_market_price(
                btc.get("current_price")
            )
        )

        btc_change_display = (
            market_change_html(
                btc.get("change_value")
            )
        )

    return f"""

    <div class="market-summary">

        <div class="market-title">

            <span class="market-title-main">
                ₿ BTC 시장 시황
            </span>

            <span class="market-title-sub">
                ROC5
                {format_timeframe(ROC_FILTER_TIMEFRAME)}
                /
                ROC5
                {format_timeframe(ROC_FILTER_HIGH_TIMEFRAME)}
                필터 ·
                {get_roc_text()}
            </span>

        </div>

        <div class="btc-mobile">

            <div class="btc-top">

                <span class="btc-name">
                    ₿ BTC
                </span>

                <span class="btc-price">
                    {btc_price_display}
                </span>

                <span class="btc-change">
                    {btc_change_display}
                </span>

            </div>

            <div class="btc-bottom">

                <div class="
                    btc-info-box
                    {btc_roc_class}
                ">

                    <div class="btc-info-title">
                        ROC5 ·
                        {format_timeframe(ROC_FILTER_TIMEFRAME)}
                        /
                        {format_timeframe(ROC_FILTER_HIGH_TIMEFRAME)}
                    </div>

                    <div class="btc-info-value">
                        {btc_roc_icon}
                    </div>

                    <div class="btc-info-sub">
                        {btc_roc_display}
                    </div>

                </div>

                <div class="
                    btc-info-box
                    {btc_daily_class}
                ">

                    <div class="btc-info-title">
                        BTC · 당일
                    </div>

                    <div class="btc-info-value">
                        {btc_daily_icon}
                    </div>

                    <div class="btc-info-sub">
                        {btc_daily_display}
                    </div>

                </div>

                <div class="
                    btc-info-box
                    {breadth_class}
                ">

                    <div class="btc-info-title">
                        전체 · 당일
                    </div>

                    <div class="btc-info-value">
                        {breadth_icon}
                    </div>

                    <div class="btc-info-sub">
                        {breadth_display}
                    </div>

                </div>

            </div>

        </div>

    </div>

    """


# =========================================================
# ROC HTML
# =========================================================

def roc_html(r):

    if not r:

        return (
            '<div class="roc-cell">'
            '<span class="roc-zero">'
            '⚪ 0'
            '</span>'
            '</div>'
        )

    value = r.get("roc10")

    if value is None:

        return (
            '<div class="roc-cell">'
            '<span class="roc-zero">'
            '⚪ 0'
            '</span>'
            '</div>'
        )

    try:

        value = float(value)

    except Exception:

        return (
            '<div class="roc-cell">'
            '<span class="roc-zero">'
            '⚪ 0'
            '</span>'
            '</div>'
        )

    if (
        value > 0
        and r.get(
            "long_breakout_state",
            "none"
        ) != "none"
    ):

        breakout_count = int(
            r.get(
                "long_breakout_count",
                0
            )
        )

        return (
            '<div class="roc-cell">'
            '<span class="roc-positive">'
            f'🚀{breakout_count}'
            '</span>'
            '</div>'
        )

    if value > 0:

        positive_count = int(
            r.get(
                "roc10_count",
                0
            )
        )

        return (
            '<div class="roc-cell">'
            '<span class="roc-positive">'
            f'🟢 ROC +({positive_count})'
            '</span>'
            '</div>'
        )

    if value < 0:

        negative_count = int(
            r.get(
                "roc10_negative_count",
                0
            )
        )

        return (
            '<div class="roc-cell">'
            '<span class="roc-negative">'
            f'🔴 ROC -({negative_count})'
            '</span>'
            '</div>'
        )

    return (
        '<div class="roc-cell">'
        '<span class="roc-zero">'
        '⚪ 0'
        '</span>'
        '</div>'
    )


# =========================================================
# 신호 HTML
# =========================================================

def signal_html(
    row,
    top_list=False
):

    if not row:

        return (
            '<span class="muted">-</span>'
        )

    r = row.get(
        "roc",
        {}
    )

    if row.get(
        "breakout_qualified",
        False
    ):

        try:

            roc_value = float(
                r.get("roc10")
            )

            count = int(
                r.get(
                    "long_breakout_count",
                    0
                )
            )

        except Exception:

            roc_value = 0
            count = -1

        if (
            roc_value > 0
            and count >= 0
        ):

            return (
                '<span '
                'class="signal-icon long-breakout" '
                'title="ROC5 1H/4H 필터 일치 + ROC 0선 상승 돌파">'
                f'🚀{count}'
                '</span>'
            )

    return (
        '<span class="muted">-</span>'
    )


# =========================================================
# ROC 필터 HTML
# =========================================================

def roc_filter_html(
    r
):

    if not r:
        return "⚪"

    return r.get(
        "display",
        "⚪"
    )


# =========================================================
# 행 클래스
# =========================================================

def row_class(x):

    if is_breakout(x):
        return "breakout-qualified"

    return ""


# =========================================================
# 행 HTML
# =========================================================

def rows_html(
    data,
    focus=None,
    top_list=False
):

    out = []

    for x in data:

        if focus == "long_combined":

            cls = (
                "breakout-qualified"
                if is_long_combined(x)
                else ""
            )

        else:

            cls = row_class(x)

        out.append(
            f"""
            <tr class="{cls}">

                <td>
                    {x.get("rank", "-")}
                </td>

                <td class="coin">

                    <b>
                        {x.get("name", "-")}
                    </b>

                    <small>
                        {x.get("change", "-")}
                    </small>

                </td>

                <td class="vol">
                    {x.get("volume", "-")}
                </td>

                <td class="ema">

                    <span>

                        {format_timeframe(
                            ROC_FILTER_TIMEFRAME
                        )}

                        {roc_filter_html(
                            x.get(
                                "roc_filter_1h"
                            )
                        )}

                    </span>

                    <span class="ema-sep">
                        /
                    </span>

                    <span>

                        {format_timeframe(
                            ROC_FILTER_HIGH_TIMEFRAME
                        )}

                        {roc_filter_html(
                            x.get(
                                "roc_filter_high"
                            )
                        )}

                    </span>

                </td>

                <td>

                    {roc_html(
                        x.get(
                            "roc",
                            {}
                        )
                    )}

                </td>

                <td class="signal-cell">

                    {signal_html(
                        x,
                        top_list
                    )}

                </td>

            </tr>

            <tr class="orderbook-subrow">

                <td colspan="6">

                    {orderbook_html(x)}

                </td>

            </tr>
            """
        )

    return "".join(out)


# =========================================================
# 테이블
# =========================================================

def table_html(
    data,
    focus=None,
    top_list=False
):

    rows = rows_html(
        data,
        focus,
        top_list
    )

    if not rows:

        rows = """
        <tr>
            <td
                colspan="6"
                class="empty"
            >
                현재 후보 없음
            </td>
        </tr>
        """

    return f"""
    <div class="table-wrap">

        <table>

            <thead>

                <tr>

                    <th>#</th>
                    <th>코인</th>
                    <th>거래대금</th>
                    <th>ROC 필터</th>
                    <th>{get_roc_text()}</th>
                    <th>신호</th>

                </tr>

            </thead>

            <tbody>

                {rows}

            </tbody>

        </table>

    </div>
    """


# =========================================================
# 후보 섹션
# =========================================================

def focus_section(
    title,
    data,
    update_time,
    checker,
    focus,
    description,
    sort_key=None,
    reverse=False
):

    rows = [
        x
        for x in data
        if checker(x)
    ]

    rows.sort(
        key=lambda x: x.get(
            "rank",
            999999
        )
    )

    return f"""
    <div class="section-title {focus}-section-title">

        <span class="section-title-main">
            {title}
        </span>

        <span class="section-title-sub">

            {description}
            ·
            {update_time} KST

        </span>

    </div>

    {table_html(
        rows,
        focus,
        top_list=False
    )}

    """


# =========================================================
# 전체 TOP
# =========================================================

def section(
    title,
    data,
    update_time
):

    return f"""
    <div class="section-title">

        <span class="section-title-main">
            🏆 {title} TOP{TOP_N}
        </span>

        <span class="section-title-sub">
            {update_time} KST
        </span>

    </div>

    {table_html(
        data,
        top_list=True
    )}

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
    background:#0d1014;
    color:#eee;

    font-family:
        -apple-system,
        BlinkMacSystemFont,
        "Segoe UI",
        Arial,
        sans-serif;

    font-size:8px;

    padding:2px 2px 8px;
}

h1{
    margin:1px 2px 2px;
    font-size:12px;
    line-height:14px;
}

.market-title,
.section-title{
    display:flex;
    align-items:center;

    gap:5px;

    width:100%;
    min-height:18px;

    color:#fff;

    font-size:8px;
    line-height:10px;
    font-weight:900;

    padding:3px 5px;

    border-left:3px solid #39e875;

    background:rgba(57,232,117,.08);

    border-radius:3px;

    white-space:nowrap;
    overflow:hidden;
}

.market-title{
    margin-bottom:4px;
}

.section-title{
    margin:5px 0 4px;
}

.market-title-main,
.section-title-main{
    color:#fff;

    font-size:8px;
    line-height:10px;
    font-weight:900;

    flex:none;
}

.market-title-sub,
.section-title-sub{
    color:#7f8791;

    font-size:5.5px;
    line-height:8px;
    font-weight:700;

    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;
}

.long_combined-section-title{
    border-left-color:#39e875;
}

.market-summary{
    width:100%;

    margin:2px 0 3px;

    padding:3px 4px;

    border-top:1px solid #242a31;
    border-bottom:1px solid #242a31;

    background:#101419;

    overflow:hidden;
}

.btc-mobile{
    width:100%;
    overflow:hidden;
}

.btc-top{
    display:flex;

    align-items:center;

    width:100%;
    min-height:16px;

    gap:4px;

    white-space:nowrap;
    overflow:hidden;
}

.btc-name{
    flex:none;

    width:34px;

    font-size:6.5px;
    line-height:8px;
    font-weight:900;
}

.btc-price{
    flex:1;

    min-width:0;

    color:#e8edf2;

    font-size:6px;
    line-height:8px;
    font-weight:800;

    text-align:left;

    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;
}

.btc-change{
    flex:none;

    width:64px;

    font-size:9.5px;
    line-height:12px;

    font-weight:900;

    text-align:right;

    white-space:nowrap;
}

.btc-bottom{
    display:grid;

    grid-template-columns:
        1fr
        1fr
        1fr;

    align-items:stretch;

    width:100%;

    min-height:70px;

    gap:5px;

    overflow:hidden;
}

.btc-info-box{
    min-width:0;
    min-height:70px;

    padding:5px 6px;

    border:1px solid #292f36;

    border-radius:6px;

    background:#14181d;

    overflow:hidden;

    display:flex;

    flex-direction:column;

    align-items:center;

    justify-content:center;

    text-align:center;
}

.btc-info-title{
    width:100%;

    color:#7f8791;

    font-size:6px;
    line-height:8px;

    font-weight:800;

    margin-bottom:2px;

    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;
}

.btc-info-value{
    width:100%;

    color:#eee;

    font-size:28px;
    line-height:30px;

    font-weight:900;

    display:flex;

    align-items:center;
    justify-content:center;

    white-space:nowrap;
    overflow:hidden;
}

.btc-info-sub{
    width:100%;

    color:#737b85;

    font-size:7px;
    line-height:9px;

    margin-top:2px;

    font-weight:800;

    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;
}

.btc-info-box.up{
    color:#39e875!important;

    border-color:
        rgba(57,232,117,.28);

    background:
        rgba(57,232,117,.08);
}

.btc-info-box.down{
    color:#ff5555!important;

    border-color:
        rgba(255,85,85,.28);

    background:
        rgba(255,85,85,.08);
}

.btc-info-box.wait{
    color:#b0b7bf!important;

    border-color:#292f36;

    background:#14181d;
}

.market-up,
.roc-positive,
.up{
    color:#39e875!important;
    font-weight:900;
}

.market-down,
.roc-negative,
.down{
    color:#ff5555!important;
    font-weight:900;
}

.market-zero,
.roc-zero,
.zero,
.muted{
    color:#68717b!important;
}

.status{
    display:flex;

    justify-content:center;

    gap:9px;

    margin:2px 2px 3px;

    padding:2px 0;

    border-top:1px solid #242a31;
    border-bottom:1px solid #242a31;

    font-size:6px;
    line-height:7px;

    font-weight:800;
}

.y{
    color:#39e875!important;
}

.n{
    color:#ff5555!important;
}

.signal-cell{
    text-align:center!important;
    vertical-align:middle;
}

.signal-icon{
    display:inline-flex;

    align-items:center;
    justify-content:center;

    width:100%;

    min-height:21px;

    font-size:15px;
    line-height:17px;

    font-weight:900;

    white-space:nowrap;
}

.signal-icon.long-breakout{
    filter:
        drop-shadow(
            0 0 2px
            rgba(57,232,117,.35)
        );
}

.table-wrap{
    width:100%;

    overflow:hidden;

    border-radius:5px;

    border:1px solid #272d34;

    background:#171b20;
}

table{
    width:100%;

    table-layout:fixed;

    border-collapse:collapse;

    background:#171b20;
}

thead{
    background:#111419;
}

th{
    height:17px;

    padding:1px;

    border-bottom:1px solid #292f36;

    color:#7f8791;

    font-size:5px;
    line-height:6px;

    font-weight:700;

    text-align:center;
}

td{
    height:25px;

    padding:1px;

    border-bottom:1px solid #22282e;

    text-align:center;

    vertical-align:middle;

    overflow:hidden;
}

tr:last-child td{
    border-bottom:none;
}

th:nth-child(1),
td:nth-child(1){
    width:6%;
}

th:nth-child(2),
td:nth-child(2){
    width:15%;
}

th:nth-child(3),
td:nth-child(3){
    width:15%;
}

th:nth-child(4),
td:nth-child(4){
    width:25%;
}

th:nth-child(5),
td:nth-child(5){
    width:22%;
}

th:nth-child(6),
td:nth-child(6){
    width:17%;
}

td:nth-child(1){
    color:#8b929b;

    font-size:6px;

    font-weight:700;
}

.coin{
    text-align:left!important;

    line-height:9px;
}

.coin b{
    display:block;

    width:100%;

    font-size:6.5px;
    line-height:8px;

    font-weight:800;

    white-space:nowrap;

    overflow:hidden;

    text-overflow:ellipsis;
}

.coin small{
    display:block;

    margin:0;

    font-size:4.5px;
    line-height:6px;

    white-space:nowrap;

    overflow:hidden;
}

.vol{
    font-size:6px;
    line-height:8px;

    font-weight:800;

    white-space:nowrap;
}

.ema{
    text-align:center!important;

    font-weight:800;

    line-height:8px;

    white-space:nowrap;

    overflow:visible;
}

.ema span{
    font-size:5.8px;
    line-height:8px;

    white-space:nowrap;
}

.ema-sep{
    color:#555c65;

    margin:0 1px;
}

.roc-cell{
    display:flex;

    flex-direction:row;

    align-items:center;

    justify-content:center;

    gap:1px;

    min-height:21px;

    line-height:8px;

    white-space:nowrap;
}

.roc-cell span{
    font-size:5.8px;
    line-height:8px;

    font-weight:900;

    white-space:nowrap;
}

.breakout-qualified{
    background:
        rgba(57,232,117,.08);
}

.empty{
    height:30px;

    padding:8px;

    color:#555d67;

    font-size:6px;
}

.orderbook-subrow{
    background:#101419!important;
}

.orderbook-subrow td{
    height:auto!important;

    padding:3px 4px 4px!important;

    border-bottom:
        1px solid #252b31!important;
}

.orderbook-wrap{
    width:100%;

    padding:1px 0;

    overflow:hidden;
}

.orderbook-row{

    display:grid;

    grid-template-columns:
        43px
        43px
        minmax(55px, 1fr)
        35px;

    align-items:center;

    gap:4px;

    width:100%;

    min-height:13px;
}

.orderbook-label{

    font-size:5.5px;

    line-height:8px;

    font-weight:900;

    white-space:nowrap;
}

.ask-label{
    color:#ff5555;
}

.bid-label{
    color:#39e875;
}

.orderbook-amount{

    font-size:5.5px;

    line-height:8px;

    font-weight:900;

    text-align:right;

    white-space:nowrap;
}

.ask-amount{
    color:#ff7777;
}

.bid-amount{
    color:#39e875;
}

.orderbook-bar-box{

    position:relative;

    width:100%;

    height:7px;

    background:#252b31;

    border-radius:4px;

    overflow:hidden;
}

.orderbook-bar{

    height:100%;

    min-width:1px;

    border-radius:4px;

    transition:
        width .25s ease;
}

.ask-bar{
    background:#d94a4a;
}

.bid-bar{
    background:#39e875;
}

.orderbook-ratio{

    font-size:5.5px;

    line-height:8px;

    font-weight:900;

    text-align:right;

    white-space:nowrap;
}

.ask-ratio{
    color:#ff7777;
}

.bid-ratio{
    color:#39e875;
}

.orderbook-bottom{

    display:flex;

    align-items:center;

    justify-content:flex-end;

    gap:7px;

    min-height:11px;

    margin-top:2px;

    padding-right:1px;

    font-size:5px;

    line-height:7px;

    font-weight:800;
}

.orderbook-range{

    color:#5e6670;

    white-space:nowrap;
}

.ob-dominance{

    font-size:5.5px;

    line-height:8px;

    font-weight:900;

    white-space:nowrap;
}

.bid-dominance{
    color:#39e875;
}

.ask-dominance{
    color:#ff5555;
}

.balanced-dominance{
    color:#9aa1aa;
}

.orderbook-empty{

    width:100%;

    padding:3px 0;

    color:#555d67;

    font-size:5px;

    line-height:7px;

    font-weight:700;

    text-align:center;
}


@media(max-width:380px){

    body{
        padding:1px 1px 6px;
    }

    h1{
        font-size:11px;
        line-height:13px;
    }

    .market-summary{
        padding:3px 3px;
    }

    .market-title,
    .section-title{
        min-height:17px;

        gap:4px;

        font-size:7px;
        line-height:9px;

        padding:3px 4px;

        margin-bottom:3px;

        border-left-width:3px;
    }

    .section-title{
        margin:4px 0 3px;
    }

    .market-title-main,
    .section-title-main{
        font-size:7px;
        line-height:9px;
    }

    .market-title-sub,
    .section-title-sub{
        font-size:4.8px;
        line-height:7px;
    }

    .btc-top{
        min-height:15px;
        gap:3px;
    }

    .btc-name{
        width:30px;
        font-size:5.8px;
    }

    .btc-price{
        font-size:5.4px;
    }

    .btc-change{
        width:56px;

        font-size:8.5px;
        line-height:11px;
    }

    .btc-bottom{
        grid-template-columns:
            1fr
            1fr
            1fr;

        min-height:58px;

        gap:3px;
    }

    .btc-info-box{
        min-height:58px;

        padding:3px 4px;

        border-radius:5px;
    }

    .btc-info-title{
        font-size:4.8px;
        line-height:6px;

        margin-bottom:1px;
    }

    .btc-info-value{
        font-size:23px;
        line-height:25px;
    }

    .btc-info-sub{
        font-size:5px;
        line-height:6px;

        margin-top:1px;
    }

    .status{
        font-size:5.5px;
        line-height:6px;
    }

    th{
        height:16px;
        font-size:4.5px;
    }

    td{
        height:23px;
    }

    .coin b{
        font-size:6px;
        line-height:7px;
    }

    .coin small{
        font-size:4px;
        line-height:5px;
    }

    .vol{
        font-size:5.5px;
    }

    .ema span{
        font-size:5.3px;
    }

    .roc-cell span{
        font-size:5.2px;
    }

    .signal-icon{
        font-size:13px;
        line-height:15px;

        min-height:19px;
    }

    .orderbook-subrow td{

        height:auto!important;

        padding:2px 2px 3px!important;
    }

    .orderbook-row{

        grid-template-columns:
            37px
            38px
            minmax(42px, 1fr)
            31px;

        gap:3px;

        min-height:12px;
    }

    .orderbook-label{

        font-size:4.8px;

        line-height:7px;
    }

    .orderbook-amount{

        font-size:4.8px;

        line-height:7px;
    }

    .orderbook-bar-box{

        height:6px;
    }

    .orderbook-ratio{

        font-size:4.8px;

        line-height:7px;
    }

    .orderbook-bottom{

        min-height:10px;

        gap:5px;

        margin-top:1px;

        font-size:4.5px;

        line-height:6px;
    }

    .ob-dominance{

        font-size:5px;

        line-height:7px;
    }

    .orderbook-empty{

        font-size:4.5px;

        line-height:6px;
    }
}


@media(min-width:601px){

    body{
        max-width:900px;

        margin:auto;

        padding:8px;

        font-size:10px;
    }

    h1{
        font-size:15px;
        line-height:20px;
    }

    .market-title,
    .section-title{
        min-height:23px;

        gap:6px;

        font-size:9px;

        padding:4px 6px;

        margin-bottom:5px;

        border-left-width:3px;
    }

    .section-title{
        margin:10px 0 5px;
    }

    .market-title-main,
    .section-title-main{
        font-size:9px;
    }

    .market-title-sub,
    .section-title-sub{
        font-size:6px;
    }

    .btc-name{
        width:42px;
        font-size:8px;
    }

    .btc-price{
        font-size:8px;
    }

    .btc-change{
        width:64px;

        font-size:9.5px;
        line-height:12px;
    }

    .btc-bottom{
        grid-template-columns:
            1fr
            1fr
            1fr;

        min-height:75px;

        gap:6px;
    }

    .btc-info-box{
        min-height:75px;

        padding:6px 8px;
    }

    .btc-info-title{
        font-size:7px;
        line-height:9px;
    }

    .btc-info-value{
        font-size:32px;
        line-height:34px;
    }

    .btc-info-sub{
        font-size:7px;
        line-height:9px;
    }

    th{
        height:26px;
        font-size:7px;
    }

    td{
        height:38px;
        padding:3px;
    }

    .coin b{
        font-size:9px;
        line-height:11px;
    }

    .coin small{
        font-size:7px;
    }

    .vol{
        font-size:8px;
    }

    .ema span{
        font-size:8px;
    }

    .roc-cell span{
        font-size:7px;
    }

    .signal-icon{
        font-size:20px;
        line-height:22px;

        min-height:28px;
    }

    .orderbook-subrow td{

        height:auto!important;

        padding:4px 6px 5px!important;
    }

    .orderbook-row{

        grid-template-columns:
            55px
            60px
            minmax(80px, 1fr)
            45px;

        gap:6px;

        min-height:17px;
    }

    .orderbook-label{

        font-size:7px;

        line-height:9px;
    }

    .orderbook-amount{

        font-size:7px;

        line-height:9px;
    }

    .orderbook-bar-box{

        height:9px;
    }

    .orderbook-ratio{

        font-size:7px;

        line-height:9px;
    }

    .orderbook-bottom{

        min-height:13px;

        gap:9px;

        margin-top:2px;

        font-size:6px;

        line-height:8px;
    }

    .ob-dominance{

        font-size:6.5px;

        line-height:9px;
    }

    .orderbook-empty{

        font-size:6px;

        line-height:8px;
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

    status = f"""

    <div class="status">

        <span>
            업비트 :
            <b class="y">
                {USE_UPBIT}
            </b>
        </span>

        <span>
            OKX :
            <b class="n">
                {USE_OKX}
            </b>
        </span>

        <span>
            ROC 필터 :
            <b class="y">
                ROC{ROC_FILTER_PERIOD}
                ·
                {format_timeframe(ROC_FILTER_TIMEFRAME)}/
                {format_timeframe(ROC_FILTER_HIGH_TIMEFRAME)}
            </b>
        </span>

        <span>
            ROC :
            <b class="y">
                {get_roc_text()}
            </b>
        </span>

    </div>

    """

    sections = ""

    # =====================================================
    # 상승 신호
    # =====================================================

    if USE_UPBIT == "Y":

        sections += focus_section(

            "🚀 상승 신호",

            latest_upbit_data,

            latest_upbit_update_time,

            is_long_combined,

            "long_combined",

            (
                f"ROC{ROC_FILTER_PERIOD} "
                f"{format_timeframe(ROC_FILTER_TIMEFRAME)}/"
                f"{format_timeframe(ROC_FILTER_HIGH_TIMEFRAME)} "
                f"동시 양수 · "
                f"{get_roc_text()} "
                f"0선 상승 돌파 + "
                f"당일 변동률 ≥ 0%"
            )

        )

    if USE_OKX == "Y":

        sections += focus_section(

            "🚀 상승 신호",

            latest_okx_data,

            latest_okx_update_time,

            is_long_combined,

            "long_combined",

            (
                f"ROC{ROC_FILTER_PERIOD} "
                f"{format_timeframe(ROC_FILTER_TIMEFRAME)}/"
                f"{format_timeframe(ROC_FILTER_HIGH_TIMEFRAME)} "
                f"동시 양수 · "
                f"{get_roc_text()} "
                f"0선 상승 돌파 + "
                f"당일 변동률 ≥ 0%"
            )

        )

    # =====================================================
    # 전체 TOP
    # =====================================================

    if USE_UPBIT == "Y":

        sections += section(
            "업비트",
            latest_upbit_data,
            latest_upbit_update_time
        )

    if USE_OKX == "Y":

        sections += section(
            "OKX",
            latest_okx_data,
            latest_okx_update_time
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
            content="#0d1014"
        >

        <title>
            ROC{ROC_FILTER_PERIOD}
            /
            {format_timeframe(ROC_FILTER_TIMEFRAME)}
            /
            {format_timeframe(ROC_FILTER_HIGH_TIMEFRAME)}
            ·
            {get_roc_text()}
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

        {status}

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
# Startup
# =========================================================

@app.on_event("startup")
def startup():

    if USE_UPBIT not in ("Y", "N"):

        raise ValueError(
            "USE_UPBIT은 Y 또는 N만 가능합니다."
        )

    if USE_OKX not in ("Y", "N"):

        raise ValueError(
            "USE_OKX는 Y 또는 N만 가능합니다."
        )

    validate_timeframe()

    filter_tf = format_timeframe(
        ROC_FILTER_TIMEFRAME
    )

    filter_high_tf = format_timeframe(
        ROC_FILTER_HIGH_TIMEFRAME
    )

    roc_tf = format_timeframe(
        ROC_TIMEFRAME
    )

    log.info(
        "========================================"
    )

    log.info(
        f"ROC 필터: "
        f"ROC{ROC_FILTER_PERIOD} "
        f"{filter_tf}/{filter_high_tf}"
    )

    log.info(
        f"ROC 로켓: "
        f"ROC{ROC_PERIOD} "
        f"{roc_tf}"
    )

    log.info(
        f"업비트={USE_UPBIT} / "
        f"OKX={USE_OKX}"
    )

    log.info(
        f"TOP={TOP_N} / "
        f"UPDATE={UPDATE_MINUTES}분"
    )

    # =====================================================
    # ROC 필터
    # =====================================================

    log.info(
        "========================================"
    )

    log.info(
        "ROC 필터:"
    )

    log.info(
        f"{filter_tf} ROC{ROC_FILTER_PERIOD}"
    )

    log.info(
        f"{filter_high_tf} ROC{ROC_FILTER_PERIOD}"
    )

    log.info(
        "두 시간봉 ROC 모두 양수 → ☀️"
    )

    log.info(
        "두 시간봉 ROC 모두 음수 → 🌧️"
    )

    log.info(
        "두 시간봉 방향 불일치 → ⚪"
    )

    log.info(
        "ROC 필터는 EMA를 사용하지 않음"
    )

    # =====================================================
    # ROC 로켓
    # =====================================================

    log.info(
        "========================================"
    )

    log.info(
        f"ROC 로켓 시간봉: {roc_tf}"
    )

    log.info(
        f"ROC 로켓 기간: {ROC_PERIOD}"
    )

    log.info(
        "ROC 상승:"
    )

    log.info(
        "🚀0 = 0선 상승 돌파 현재봉"
    )

    log.info(
        "🚀1 = 돌파 후 1번째 봉"
    )

    log.info(
        "🚀2 = 돌파 후 2번째 봉"
    )

    log.info(
        "🚀3 = 돌파 후 3번째 봉"
    )

    log.info(
        "..."
    )

    log.info(
        "🚀9 이후 → 🚀10 → 🚀11 → 🚀12..."
    )

    log.info(
        "ROC 양수 유지 동안 카운트 제한 없음"
    )

    log.info(
        "ROC가 0 이하가 되면 로켓 종료"
    )

    log.info(
        "ROC 음수:"
    )

    log.info(
        "🔴 ROC -(1)"
    )

    log.info(
        "🔴 ROC -(2)"
    )

    log.info(
        "🔴 ROC -(3) ..."
    )

    log.info(
        "다시 0선 상승 돌파하면 🚀0부터 재시작"
    )

    # =====================================================
    # 호가
    # =====================================================

    log.info(
        "========================================"
    )

    log.info(
        f"업비트 호가 대기금액: "
        f"현재가 ±{ORDERBOOK_RANGE * 100:.0f}%"
    )

    log.info(
        f"업비트 호가 수: "
        f"최대 {ORDERBOOK_COUNT}호가"
    )

    log.info(
        f"호가 우세 기준: "
        f"{ORDERBOOK_DOMINANCE_GAP:.1f}%p"
    )

    log.info(
        "호가 대기금액은 참고용이며 "
        "ROC 신호에는 사용하지 않음"
    )

    log.info(
        "매도대기 = 빨강 / 매수대기 = 녹색"
    )

    # =====================================================
    # BTC 시장 시황
    # =====================================================

    log.info(
        "========================================"
    )

    log.info(
        "BTC 시장 시황:"
    )

    log.info(
        f"{filter_tf} ROC{ROC_FILTER_PERIOD} "
        f"+ {filter_high_tf} ROC{ROC_FILTER_PERIOD} "
        f"둘 다 양수 → ☀️"
    )

    log.info(
        f"{filter_tf} ROC{ROC_FILTER_PERIOD} "
        f"+ {filter_high_tf} ROC{ROC_FILTER_PERIOD} "
        f"둘 다 음수 → 🌧️"
    )

    log.info(
        "그 외 → ⚪"
    )

    # =====================================================
    # TOP 시장폭
    # =====================================================

    log.info(
        f"TOP{TOP_N} 당일 변동 시장폭 기준:"
    )

    log.info(
        "당일 변동률 양수 > 음수 → ☀️"
    )

    log.info(
        "당일 변동률 양수 < 음수 → 🌧️"
    )

    log.info(
        "당일 변동률 양수 = 음수 → ⚪"
    )

    log.info(
        f"전체 TOP{TOP_N}은 음수 종목도 표시"
    )

    # =====================================================
    # 최종 신호
    # =====================================================

    log.info(
        "========================================"
    )

    log.info(
        "상승 신호 최종 조건:"
    )

    log.info(
        f"① {filter_tf} ROC{ROC_FILTER_PERIOD} 양수"
    )

    log.info(
        f"② {filter_high_tf} ROC{ROC_FILTER_PERIOD} 양수"
    )

    log.info(
        f"③ {get_roc_text()} 0선 상승 돌파"
    )

    log.info(
        "④ ROC 양수 상태 유지 동안 "
        "카운트 제한 없음"
    )

    log.info(
        "⑤ 당일 변동률 0% 이상"
    )

    log.info(
        "⑥ 당일 변동률 음수 종목은 "
        "상승 신호에서 제외"
    )

    log.info(
        f"※ 전체 TOP{TOP_N} 표에는 "
        "음수 종목도 표시"
    )

    log.info(
        "EMA 필터는 완전히 제거됨"
    )

    # =====================================================
    # 최초 업데이트
    # =====================================================

    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    # =====================================================
    # 스케줄러
    # =====================================================

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
