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

# 4시간봉
EMA_TIMEFRAME = 240

# =========================================================
# EMA 기준
# =========================================================
#
# EMA1 정배열 기준
# EMA10 > EMA60 > EMA120
#
# 매수 기준
# 1차 = 종가 <= EMA30
# 2차 = 종가 <= EMA60
# 3차 = 종가 <= EMA120
#
# =========================================================

EMA_DIRECTION_FAST = 10
EMA_BUY_FIRST = 30
EMA_DIRECTION_MID = 60
EMA_DIRECTION_SLOW = 120


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

    if minutes >= 1440 and minutes % 1440 == 0:
        return f"{minutes // 1440}D"

    if minutes >= 60 and minutes % 60 == 0:
        return f"{minutes // 60}H"

    return f"{minutes}M"


def get_current_candle_start(timeframe_minutes):

    now = datetime.now(KST)

    total_minutes = now.hour * 60 + now.minute

    candle_total = (
        total_minutes // timeframe_minutes
    ) * timeframe_minutes

    hour = candle_total // 60
    minute = candle_total % 60

    return datetime(
        now.year,
        now.month,
        now.day,
        hour,
        minute
    )


def validate_timeframe():

    if EMA_TIMEFRAME not in SUPPORTED_UPBIT_TIMEFRAMES:
        raise ValueError(
            f"Upbit에서 지원하지 않는 timeframe: {EMA_TIMEFRAME}"
        )

    if EMA_TIMEFRAME not in SUPPORTED_OKX_TIMEFRAMES:
        raise ValueError(
            f"OKX에서 지원하지 않는 timeframe: {EMA_TIMEFRAME}"
        )


# =========================================================
# OKX timeframe
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


# =========================================================
# Request
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


def retry(func, *args, **kwargs):

    for attempt in range(MAX_RETRIES):

        try:

            wait_request()

            response = func(
                *args,
                **kwargs
            )

            if response is None:
                return None

            if response.status_code == 200:
                return response

            if response.status_code in (
                429,
                500,
                502,
                503,
                504
            ):

                wait_time = (
                    RATE_LIMIT_WAIT
                    * (attempt + 1)
                )

                log.warning(
                    "API %s 재시도 %s/%s",
                    response.status_code,
                    attempt + 1,
                    MAX_RETRIES
                )

                time.sleep(wait_time)

                continue

            log.warning(
                "API 오류: %s %s",
                response.status_code,
                response.text[:200]
            )

            return None

        except Exception as e:

            log.warning(
                "요청 오류 %s/%s: %s",
                attempt + 1,
                MAX_RETRIES,
                e
            )

            time.sleep(
                RATE_LIMIT_WAIT
                * (attempt + 1)
            )

    return None


# =========================================================
# Upbit
# =========================================================

def get_upbit_markets():

    url = (
        "https://api.upbit.com/v1/ticker/"
        "all?quote_currencies=KRW"
    )

    response = retry(
        requests.get,
        url,
        timeout=10
    )

    if not response:
        return []

    try:

        data = response.json()

        result = []

        for item in data:

            market = item.get("market", "")

            if not market.startswith("KRW-"):
                continue

            result.append({
                "market": market,
                "trade_price": float(
                    item.get("trade_price", 0)
                ),
                "acc_trade_price_24h": float(
                    item.get(
                        "acc_trade_price_24h",
                        0
                    )
                )
            })

        global latest_upbit_markets

        latest_upbit_markets = [
            x["market"] for x in result
        ]

        return result

    except Exception as e:

        log.error(
            "Upbit markets 오류: %s",
            e
        )

        return []


def get_usdt_krw():

    url = (
        "https://api.upbit.com/v1/ticker"
        "?markets=KRW-USDT"
    )

    response = retry(
        requests.get,
        url,
        timeout=10
    )

    if not response:
        return 0

    try:

        data = response.json()

        if not data:
            return 0

        return float(
            data[0]["trade_price"]
        )

    except Exception:

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
        "count": count
    }

    if to:
        params["to"] = to

    response = retry(
        requests.get,
        url,
        params=params,
        timeout=10
    )

    if not response:
        return pd.DataFrame()

    try:

        data = response.json()

        rows = []

        for item in data:

            dt = pd.to_datetime(
                item["candle_date_time_kst"]
            )

            rows.append({
                "dt": dt,
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
                "volume": float(
                    item["candle_acc_trade_volume"]
                )
            })

        df = pd.DataFrame(rows)

        if df.empty:
            return df

        df = df.sort_values("dt")

        # 현재 진행 중인 캔들 제거
        current_start = get_current_candle_start(unit)

        df = df[
            df["dt"] < current_start
        ]

        return df.reset_index(drop=True)

    except Exception as e:

        log.error(
            "Upbit candle 오류 %s: %s",
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


def history_upbit(market):

    frames = []

    to = None

    for _ in range(MAX_HISTORY_CHUNKS):

        df = get_upbit_candle(
            market,
            EMA_TIMEFRAME,
            count=HISTORY_CHUNK,
            to=to
        )

        if df.empty:
            break

        frames.append(df)

        oldest = df["dt"].min()

        to = oldest.strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        if len(df) < HISTORY_CHUNK:
            break

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
        "https://www.okx.com/api/v5/"
        "market/candles"
    )

    params = {
        "instId": inst,
        "bar": bar,
        "limit": str(limit)
    }

    if before is not None:
        params["before"] = str(before)

    response = retry(
        requests.get,
        url,
        params=params,
        timeout=10
    )

    if not response:
        return pd.DataFrame()

    try:

        data = response.json()

        if data.get("code") != "0":
            return pd.DataFrame()

        rows = []

        for item in data.get("data", []):

            if len(item) < 9:
                continue

            ts = int(item[0])

            confirm = str(item[8])

            # 확정봉만 사용
            if confirm != "1":
                continue

            dt = pd.to_datetime(
                ts,
                unit="ms",
                utc=True
            ).tz_convert(
                "Asia/Seoul"
            ).tz_localize(None)

            rows.append({
                "dt": dt,
                "o": float(item[1]),
                "h": float(item[2]),
                "l": float(item[3]),
                "c": float(item[4]),
                "volume": float(item[5]),
                "volCcy": float(item[6]),
                "volCcyQuote": float(item[7])
            })

        df = pd.DataFrame(rows)

        if df.empty:
            return df

        df = df.sort_values("dt")

        timeframe = (
            get_okx_bar_minutes(bar)
        )

        if timeframe:

            current_start = (
                get_current_candle_start(
                    timeframe
                )
            )

            df = df[
                df["dt"] < current_start
            ]

        return df.reset_index(drop=True)

    except Exception as e:

        log.error(
            "OKX candle 오류 %s: %s",
            inst,
            e
        )

        return pd.DataFrame()


def get_okx_current_price(inst):

    url = (
        "https://www.okx.com/api/v5/"
        "market/ticker"
    )

    params = {
        "instId": inst
    }

    response = retry(
        requests.get,
        url,
        params=params,
        timeout=10
    )

    if not response:
        return 0

    try:

        data = response.json()

        rows = data.get("data", [])

        if not rows:
            return 0

        return float(
            rows[0]["last"]
        )

    except Exception:

        return 0


def history_okx(inst):

    frames = []

    bar = get_okx_bar(
        EMA_TIMEFRAME
    )

    before = None

    for _ in range(MAX_HISTORY_CHUNKS):

        df = get_okx_ohlcv(
            inst,
            bar=bar,
            limit=HISTORY_CHUNK,
            before=before
        )

        if df.empty:
            break

        frames.append(df)

        oldest = df["dt"].min()

        before = int(
            oldest.timestamp() * 1000
        )

        if len(df) < HISTORY_CHUNK:
            break

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


def get_okx_volume(inst, usdt):

    df = get_okx_ohlcv(
        inst,
        bar="1H",
        limit=VOLUME_HOURS
    )

    if df.empty:
        return 0

    volume = df[
        "volCcyQuote"
    ].sum()

    return volume * usdt


def get_okx_symbols():

    url = (
        "https://www.okx.com/api/v5/"
        "public/instruments"
    )

    params = {
        "instType": "SWAP"
    }

    response = retry(
        requests.get,
        url,
        params=params,
        timeout=10
    )

    if not response:
        return []

    try:

        data = response.json()

        result = []

        for item in data.get(
            "data",
            []
        ):

            inst = item.get(
                "instId",
                ""
            )

            state = item.get(
                "state",
                ""
            )

            if (
                inst.endswith("-USDT-SWAP")
                and state == "live"
            ):
                result.append(inst)

        return result

    except Exception:

        return []


# =========================================================
# EMA
# =========================================================

def ema(df, period):

    return (
        pd.to_numeric(
            df["c"],
            errors="coerce"
        )
        .ewm(
            span=period,
            adjust=False,
            min_periods=1
        )
        .mean()
    )


# =========================================================
# EMA 10-60-120 정배열
# =========================================================

def direction(df):

    if df is None or df.empty:
        return "none"

    if len(df) < 2:
        return "none"

    e10 = ema(
        df,
        EMA_DIRECTION_FAST
    ).iloc[-1]

    e60 = ema(
        df,
        EMA_DIRECTION_MID
    ).iloc[-1]

    e120 = ema(
        df,
        EMA_DIRECTION_SLOW
    ).iloc[-1]

    if (
        e10 > e60
        and e60 > e120
    ):
        return "long"

    if (
        e10 < e60
        and e60 < e120
    ):
        return "short"

    return "none"


# =========================================================
# EMA 정배열 지속 캔들 수
# =========================================================

def ema_alignment_count(df):

    if df is None or len(df) < 2:
        return {
            "direction": "none",
            "count": 0,
            "spread_10_60": 0,
            "spread_60_120": 0,
            "average_spread": 0
        }

    e10 = ema(
        df,
        EMA_DIRECTION_FAST
    )

    e60 = ema(
        df,
        EMA_DIRECTION_MID
    )

    e120 = ema(
        df,
        EMA_DIRECTION_SLOW
    )

    latest_direction = "none"

    if (
        e10.iloc[-1] > e60.iloc[-1]
        and e60.iloc[-1] > e120.iloc[-1]
    ):
        latest_direction = "long"

    elif (
        e10.iloc[-1] < e60.iloc[-1]
        and e60.iloc[-1] < e120.iloc[-1]
    ):
        latest_direction = "short"

    count = 0

    for i in range(len(df) - 1, -1, -1):

        if latest_direction == "long":

            if (
                e10.iloc[i] > e60.iloc[i]
                and e60.iloc[i] > e120.iloc[i]
            ):
                count += 1
            else:
                break

        elif latest_direction == "short":

            if (
                e10.iloc[i] < e60.iloc[i]
                and e60.iloc[i] < e120.iloc[i]
            ):
                count += 1
            else:
                break

        else:
            break

    e10_last = e10.iloc[-1]
    e60_last = e60.iloc[-1]
    e120_last = e120.iloc[-1]

    spread_10_60 = (
        (e10_last - e60_last)
        / e60_last * 100
        if e60_last
        else 0
    )

    spread_60_120 = (
        (e60_last - e120_last)
        / e120_last * 100
        if e120_last
        else 0
    )

    average_spread = (
        spread_10_60
        + spread_60_120
    ) / 2

    return {
        "direction": latest_direction,
        "count": count,
        "spread_10_60": spread_10_60,
        "spread_60_120": spread_60_120,
        "average_spread": average_spread
    }


# =========================================================
# EMA 표시
# =========================================================

def ema_display(df, current_price):

    if df is None or df.empty:
        return {
            "main": "⚪(0)",
            "sub": "-"
        }

    info = ema_alignment_count(df)

    direction_value = info["direction"]
    count = info["count"]

    if direction_value == "long":

        main = f"🟢({count})"

    elif direction_value == "short":

        main = f"🔴({count})"

    else:

        main = "⚪(0)"

    e120 = ema(
        df,
        EMA_DIRECTION_SLOW
    ).iloc[-1]

    if current_price and e120:

        rate = (
            (current_price - e120)
            / e120
            * 100
        )

        sub = f"EMA120 대비 {rate:+.2f}%"

    else:

        sub = "-"

    return {
        "main": main,
        "sub": sub
    }


# =========================================================
# EMA2 분할매수
# =========================================================
#
# 정배열:
# EMA10 > EMA60 > EMA120
#
# 매수:
# 종가 <= EMA30 → 1차
# 종가 <= EMA60 → 2차
# 종가 <= EMA120 → 3차
#
# 반드시 확정봉 종가 사용
# =========================================================

def ema2_buy_analysis(df):

    result = {
        "state": "none",
        "stage": 0,
        "display": "",
        "qualified": False
    }

    if df is None or len(df) < 2:
        return result

    # EMA 계산
    e10 = ema(
        df,
        EMA_DIRECTION_FAST
    ).iloc[-1]

    e30 = ema(
        df,
        EMA_BUY_FIRST
    ).iloc[-1]

    e60 = ema(
        df,
        EMA_DIRECTION_MID
    ).iloc[-1]

    e120 = ema(
        df,
        EMA_DIRECTION_SLOW
    ).iloc[-1]

    # 마지막 확정봉 종가
    close = float(
        df["c"].iloc[-1]
    )

    # -----------------------------------------------------
    # EMA 10-60-120 정배열 확인
    # -----------------------------------------------------

    if not (
        e10 > e60
        and e60 > e120
    ):
        return result

    result["state"] = "long"
    result["qualified"] = True

    # -----------------------------------------------------
    # 분할매수
    # -----------------------------------------------------

    # 3차
    if close <= e120:

        result["stage"] = 3
        result["display"] = (
            "🟢 ③ 3차매수"
        )

    # 2차
    elif close <= e60:

        result["stage"] = 2
        result["display"] = (
            "🟢 ② 2차매수"
        )

    # 1차
    elif close <= e30:

        result["stage"] = 1
        result["display"] = (
            "🟢 ① 1차매수"
        )

    return result


# =========================================================
# 빈 분석
# =========================================================

def empty_analysis():

    return {
        "ema_1h": {
            "main": "⚪(0)",
            "sub": "-"
        },
        "ema2_buy": {
            "state": "none",
            "stage": 0,
            "display": "",
            "qualified": False
        },
        "qualified": False,
        "direction": "none",
        "df1h": pd.DataFrame()
    }


# =========================================================
# 일간 변동률
# =========================================================

def daily_change_upbit(market):

    df = get_upbit_candle(
        market,
        1440,
        count=3
    )

    if df.empty or len(df) < 2:
        return 0

    previous_close = float(
        df["c"].iloc[-2]
    )

    current_close = float(
        df["c"].iloc[-1]
    )

    if previous_close == 0:
        return 0

    return (
        (current_close - previous_close)
        / previous_close
        * 100
    )


def daily_changes(df):

    if df is None or df.empty:
        return 0

    temp = df.copy()

    temp["dt"] = pd.to_datetime(
        temp["dt"]
    )

    temp = temp.set_index("dt")

    daily = temp["c"].resample(
        "1D",
        offset="9h"
    ).last().dropna()

    if len(daily) < 2:
        return 0

    previous = daily.iloc[-2]
    current = daily.iloc[-1]

    if previous == 0:
        return 0

    return (
        (current - previous)
        / previous
        * 100
    )


def format_change(value):

    try:
        value = float(value)
    except Exception:
        value = 0

    if value > 0:

        return (
            f'<span class="up">'
            f'▲ {value:+.2f}%'
            f'</span>'
        )

    if value < 0:

        return (
            f'<span class="down">'
            f'▼ {value:+.2f}%'
            f'</span>'
        )

    return "0.00%"


def format_volume(value):

    try:
        value = float(value)
    except Exception:
        return "-"

    if value >= 100_000_000_000:

        return (
            f"{value / 100_000_000_000:.1f}조"
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


# =========================================================
# 종목 분석
# =========================================================

def analyze(
    market,
    okx=False,
    current_price=None
):

    try:

        if okx:

            df = history_okx(market)

        else:

            df = history_upbit(market)

        if df.empty:
            return empty_analysis()

        if len(df) < 2:
            return empty_analysis()

        # EMA1 표시
        ema1 = ema_display(
            df,
            current_price
        )

        # EMA2 분할매수
        ema2 = ema2_buy_analysis(
            df
        )

        # 변동률
        if okx:

            change = daily_changes(df)

        else:

            change = daily_change_upbit(
                market
            )

        return {
            "ema_1h": ema1,
            "ema2_buy": ema2,
            "qualified": ema2["qualified"],
            "direction": ema2["state"],
            "df1h": df,
            "change": change
        }

    except Exception as e:

        log.error(
            "분석 오류 %s: %s",
            market,
            e
        )

        return empty_analysis()


# =========================================================
# Row
# =========================================================

def make_row(
    rank,
    name,
    change,
    volume,
    analysis
):

    return {
        "rank": rank,
        "name": name,
        "change": change,
        "volume": volume,
        "ema_1h": analysis[
            "ema_1h"
        ],
        "ema2_buy": analysis[
            "ema2_buy"
        ],
        "qualified": analysis[
            "qualified"
        ]
    }


# =========================================================
# Upbit 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update_time

    markets = get_upbit_markets()

    if not markets:

        log.warning(
            "Upbit 종목을 가져오지 못했습니다."
        )

        return

    markets = sorted(
        markets,
        key=lambda x:
        x["acc_trade_price_24h"],
        reverse=True
    )

    targets = markets[:TOP_N]

    result = []

    for rank, item in enumerate(
        targets,
        start=1
    ):

        market = item["market"]

        current_price = item[
            "trade_price"
        ]

        analysis = analyze(
            market,
            okx=False,
            current_price=current_price
        )

        result.append(
            make_row(
                rank=rank,
                name=market.replace(
                    "KRW-",
                    ""
                ),
                change=analysis.get(
                    "change",
                    0
                ),
                volume=item[
                    "acc_trade_price_24h"
                ],
                analysis=analysis
            )
        )

    latest_upbit_data = result

    latest_upbit_update_time = kst()

    qualified_count = sum(
        1
        for x in result
        if x["qualified"]
    )

    stage1 = sum(
        1
        for x in result
        if x["ema2_buy"]["stage"] == 1
    )

    stage2 = sum(
        1
        for x in result
        if x["ema2_buy"]["stage"] == 2
    )

    stage3 = sum(
        1
        for x in result
        if x["ema2_buy"]["stage"] == 3
    )

    log.info(
        "Upbit 업데이트 완료 | "
        "정배열=%s | "
        "1차=%s | "
        "2차=%s | "
        "3차=%s",
        qualified_count,
        stage1,
        stage2,
        stage3
    )


# =========================================================
# OKX 업데이트
# =========================================================

def update_okx(usdt):

    global latest_okx_data
    global latest_okx_update_time

    symbols = get_okx_symbols()

    if not symbols:

        log.warning(
            "OKX 종목을 가져오지 못했습니다."
        )

        return

    volume_list = []

    for inst in symbols:

        volume = get_okx_volume(
            inst,
            usdt
        )

        volume_list.append({
            "inst": inst,
            "volume": volume
        })

    volume_list.sort(
        key=lambda x:
        x["volume"],
        reverse=True
    )

    targets = volume_list[:TOP_N]

    upbit_symbols = set(
        latest_upbit_markets
    )

    result = []

    for rank, item in enumerate(
        targets,
        start=1
    ):

        inst = item["inst"]

        base = inst.replace(
            "-USDT-SWAP",
            ""
        )

        if f"KRW-{base}" in upbit_symbols:

            name = (
                f"{base} "
                f"<span class='upbit-tag'>"
                f"(업비트)"
                f"</span>"
            )

        else:

            name = base

        current_price = (
            get_okx_current_price(inst)
        )

        analysis = analyze(
            inst,
            okx=True,
            current_price=current_price
        )

        result.append(
            make_row(
                rank=rank,
                name=name,
                change=analysis.get(
                    "change",
                    0
                ),
                # 기존 UI 표시용
                # OKX 거래대금 10분의 1
                volume=item[
                    "volume"
                ] / 10,
                analysis=analysis
            )
        )

    latest_okx_data = result

    latest_okx_update_time = kst()

    qualified_count = sum(
        1
        for x in result
        if x["qualified"]
    )

    stage1 = sum(
        1
        for x in result
        if x["ema2_buy"]["stage"] == 1
    )

    stage2 = sum(
        1
        for x in result
        if x["ema2_buy"]["stage"] == 2
    )

    stage3 = sum(
        1
        for x in result
        if x["ema2_buy"]["stage"] == 3
    )

    log.info(
        "OKX 업데이트 완료 | "
        "정배열=%s | "
        "1차=%s | "
        "2차=%s | "
        "3차=%s",
        qualified_count,
        stage1,
        stage2,
        stage3
    )


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    global latest_usdt_krw

    with update_lock:

        try:

            log.info(
                "========== 업데이트 시작 =========="
            )

            if USE_UPBIT == "Y":

                update_upbit()

            usdt = get_usdt_krw()

            if usdt > 0:

                latest_usdt_krw = usdt

            else:

                usdt = latest_usdt_krw

            if (
                USE_OKX == "Y"
                and usdt > 0
            ):

                update_okx(usdt)

            log.info(
                "========== 업데이트 완료 =========="
            )

        except Exception as e:

            log.exception(
                "Dashboard 업데이트 오류: %s",
                e
            )


# =========================================================
# HTML
# =========================================================

def ema2_buy_html(data):

    if not data:
        return "-"

    stage = data.get(
        "stage",
        0
    )

    if stage == 1:

        return (
            '<span class="buy buy1">'
            '🟢 ① 1차매수'
            '</span>'
        )

    if stage == 2:

        return (
            '<span class="buy buy2">'
            '🟢 ② 2차매수'
            '</span>'
        )

    if stage == 3:

        return (
            '<span class="buy buy3">'
            '🟢 ③ 3차매수'
            '</span>'
        )

    return "-"


def ema_html(e):

    if not e:
        return "-"

    return (
        f'<div class="ema-main">'
        f'{e.get("main", "-")}'
        f'</div>'
        f'<div class="ema-sub">'
        f'{e.get("sub", "-")}'
        f'</div>'
    )


def rows_html(data):

    if not data:

        return (
            '<tr>'
            '<td colspan="5">'
            '데이터 없음'
            '</td>'
            '</tr>'
        )

    html = ""

    for row in data:

        qualified_class = (
            " qualified"
            if row.get("qualified")
            else ""
        )

        html += (
            f'<tr class="{qualified_class}">'

            f'<td>'
            f'{row["rank"]}'
            f'</td>'

            f'<td class="coin">'
            f'{row["name"]}'
            f'</td>'

            f'<td>'
            f'{format_volume(row["volume"])}'
            f'<br>'
            f'{format_change(row["change"])}'
            f'</td>'

            f'<td>'
            f'{ema_html(row["ema_1h"])}'
            f'</td>'

            f'<td>'
            f'{ema2_buy_html(row["ema2_buy"])}'
            f'</td>'

            f'</tr>'
        )

    return html


def table_html(data):

    timeframe = format_timeframe(
        EMA_TIMEFRAME
    )

    return f"""
    <table>
        <thead>
            <tr>
                <th>순위</th>
                <th>코인</th>
                <th>거래대금</th>
                <th>EMA1<br>{timeframe}</th>
                <th>EMA2<br>매수</th>
            </tr>
        </thead>
        <tbody>
            {rows_html(data)}
        </tbody>
    </table>
    """


# =========================================================
# 매수 확인 리스트
# =========================================================

def buy_stage_section(
    title,
    data,
    stage
):

    targets = [
        x
        for x in data
        if x["ema2_buy"]["stage"] == stage
    ]

    if not targets:

        return f"""
        <div class="buy-section">
            <div class="buy-title">
                {title}
            </div>
            <div class="empty-buy">
                해당 종목 없음
            </div>
        </div>
        """

    items = ""

    for x in targets:

        items += f"""
        <div class="buy-item">
            <span class="buy-rank">
                #{x["rank"]}
            </span>

            <span class="buy-coin">
                {x["name"]}
            </span>

            <span class="buy-change">
                {format_change(x["change"])}
            </span>
        </div>
        """

    return f"""
    <div class="buy-section">
        <div class="buy-title">
            {title}
        </div>
        {items}
    </div>
    """


def buy_focus_section(
    title,
    data
):

    return f"""
    <div class="focus-box">

        <div class="focus-title">
            {title}
        </div>

        {buy_stage_section(
            "🟢 ① 1차매수",
            data,
            1
        )}

        {buy_stage_section(
            "🟢 ② 2차매수",
            data,
            2
        )}

        {buy_stage_section(
            "🟢 ③ 3차매수",
            data,
            3
        )}

    </div>
    """


def section(
    title,
    data,
    update_time
):

    return f"""
    <div class="section">

        <div class="section-title">
            <span>
                {title}
            </span>

            <span class="update">
                {update_time}
            </span>
        </div>

        {table_html(data)}

    </div>
    """


# =========================================================
# CSS
# =========================================================

CSS = """
<style>

* {
    box-sizing: border-box;
}

body {
    margin: 0;
    background: #101216;
    color: #f2f2f2;
    font-family:
        Arial,
        sans-serif;
}

.container {
    width: 100%;
    max-width: 1200px;
    margin: auto;
    padding: 10px;
}

.header {
    padding: 14px 8px;
    margin-bottom: 10px;
}

.header h1 {
    margin: 0 0 8px 0;
    font-size: 21px;
}

.info {
    color: #aaa;
    font-size: 12px;
    line-height: 1.7;
}

.info strong {
    color: #fff;
}

.status {
    margin-top: 8px;
    color: #aaa;
    font-size: 12px;
}

.section {
    margin-bottom: 16px;
    background: #17191f;
    border-radius: 10px;
    overflow: hidden;
}

.section-title {
    padding: 12px;
    font-size: 16px;
    font-weight: bold;
    display: flex;
    justify-content: space-between;
    align-items: center;
}

.update {
    color: #777;
    font-size: 10px;
    font-weight: normal;
}

table {
    width: 100%;
    border-collapse: collapse;
    table-layout: fixed;
}

th,
td {
    border-top: 1px solid #272a31;
    padding: 8px 3px;
    text-align: center;
    font-size: 11px;
}

th {
    background: #1e2128;
    color: #aaa;
    font-weight: normal;
}

th:nth-child(1),
td:nth-child(1) {
    width: 7%;
}

th:nth-child(2),
td:nth-child(2) {
    width: 22%;
}

th:nth-child(3),
td:nth-child(3) {
    width: 17%;
}

th:nth-child(4),
td:nth-child(4) {
    width: 27%;
}

th:nth-child(5),
td:nth-child(5) {
    width: 27%;
}

.coin {
    font-weight: bold;
    font-size: 12px;
}

.qualified {
    background: rgba(
        40,
        160,
        80,
        0.08
    );
}

.ema-main {
    font-size: 13px;
    font-weight: bold;
}

.ema-sub {
    margin-top: 3px;
    color: #999;
    font-size: 9px;
}

.up {
    color: #ff6575;
}

.down {
    color: #4aa3ff;
}

.buy {
    font-weight: bold;
    white-space: nowrap;
}

.buy1 {
    color: #73e6a3;
}

.buy2 {
    color: #ffd166;
}

.buy3 {
    color: #ff8b8b;
}

.upbit-tag {
    color: #8fc7ff;
    font-size: 9px;
}

.focus-box {
    background: #17191f;
    border-radius: 10px;
    margin-bottom: 16px;
    overflow: hidden;
}

.focus-title {
    padding: 12px;
    font-size: 16px;
    font-weight: bold;
    border-bottom: 1px solid #272a31;
}

.buy-section {
    padding: 10px 12px;
    border-bottom: 1px solid #272a31;
}

.buy-section:last-child {
    border-bottom: none;
}

.buy-title {
    font-weight: bold;
    margin-bottom: 8px;
    font-size: 13px;
}

.buy-item {
    display: flex;
    align-items: center;
    padding: 7px 4px;
    border-top: 1px solid #22252b;
    font-size: 12px;
}

.buy-rank {
    width: 35px;
    color: #888;
}

.buy-coin {
    flex: 1;
    font-weight: bold;
}

.buy-change {
    text-align: right;
}

.empty-buy {
    color: #666;
    font-size: 11px;
    padding: 5px 0;
}

@media (min-width: 700px) {

    .container {
        padding: 20px;
    }

    th,
    td {
        font-size: 13px;
        padding: 10px 5px;
    }

    .coin {
        font-size: 14px;
    }

    .ema-main {
        font-size: 15px;
    }

    .ema-sub {
        font-size: 10px;
    }

}

</style>
"""


# =========================================================
# Dashboard
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def dashboard():

    timeframe_label = format_timeframe(
        EMA_TIMEFRAME
    )

    upbit_status = (
        "ON"
        if USE_UPBIT == "Y"
        else "OFF"
    )

    okx_status = (
        "ON"
        if USE_OKX == "Y"
        else "OFF"
    )

    html = f"""
    <!DOCTYPE html>

    <html>
    <head>

        <meta
            charset="UTF-8"
        >

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
            EMA3 분할매수 전략
        </title>

        {CSS}

    </head>

    <body>

    <div class="container">

        <div class="header">

            <h1>
                📊 EMA3 분할매수 전략
            </h1>

            <div class="info">

                <strong>
                    {timeframe_label} 확정 캔들 기준
                </strong>
                <br>

                EMA1 정배열:
                <strong>
                    EMA10 &gt; EMA60 &gt; EMA120
                </strong>
                <br>

                EMA1:
                EMA10 / EMA60 / EMA120
                <br>

                EMA2 매수 기준:
                <strong>
                    종가 ≤ EMA30 → ①
                </strong>
                /
                <strong>
                    종가 ≤ EMA60 → ②
                </strong>
                /
                <strong>
                    종가 ≤ EMA120 → ③
                </strong>

                <br>

                ※ 정배열 상태에서만
                분할매수 신호 발생

                <br>

                ※ 매수 기준은
                현재가가 아니라
                <strong>
                    마지막 확정봉 종가
                </strong>
                사용

            </div>

            <div class="status">

                Upbit:
                <strong>
                    {upbit_status}
                </strong>
                &nbsp;&nbsp;

                OKX:
                <strong>
                    {okx_status}
                </strong>

                &nbsp;&nbsp;

                USDT/KRW:
                <strong>
                    {latest_usdt_krw:,.0f}
                </strong>

            </div>

        </div>
    """

    if USE_UPBIT == "Y":

        html += buy_focus_section(
            "🟢 업비트 매수 확인",
            latest_upbit_data
        )

        html += section(
            f"🏆 업비트 거래대금 TOP{TOP_N}",
            latest_upbit_data,
            latest_upbit_update_time
        )

    if USE_OKX == "Y":

        html += buy_focus_section(
            "🟢 OKX 매수 확인",
            latest_okx_data
        )

        html += section(
            f"🏆 OKX 거래대금 TOP{TOP_N}",
            latest_okx_data,
            latest_okx_update_time
        )

    html += """
    </div>

    </body>
    </html>
    """

    return HTMLResponse(
        content=html
    )


# =========================================================
# 시작
# =========================================================

def scheduler_loop():

    while True:

        try:

            schedule.run_pending()

        except Exception as e:

            log.exception(
                "Scheduler 오류: %s",
                e
            )

        time.sleep(1)


if __name__ == "__main__":

    validate_timeframe()

    timeframe_label = format_timeframe(
        EMA_TIMEFRAME
    )

    log.info(
        "======================================"
    )

    log.info(
        "EMA3 분할매수 시스템 시작"
    )

    log.info(
        "EMA timeframe: %s",
        timeframe_label
    )

    log.info(
        "EMA1 정배열: "
        "EMA10 > EMA60 > EMA120"
    )

    log.info(
        "1차 매수: 종가 <= EMA30"
    )

    log.info(
        "2차 매수: 종가 <= EMA60"
    )

    log.info(
        "3차 매수: 종가 <= EMA120"
    )

    log.info(
        "Upbit: %s",
        USE_UPBIT
    )

    log.info(
        "OKX: %s",
        USE_OKX
    )

    log.info(
        "======================================"
    )

    # 최초 업데이트
    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    # 스케줄
    schedule.every(
        UPDATE_MINUTES
    ).minutes.do(
        update_dashboard
    )

    threading.Thread(
        target=scheduler_loop,
        daemon=True
    ).start()

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )
