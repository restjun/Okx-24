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

warnings.filterwarnings("ignore", category=FutureWarning)

app = FastAPI()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s"
)

log = logging.getLogger("trading")


# =========================================================
# 사용자 설정
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
# EMA 설정
# =========================================================

EMA_TIMEFRAME = 60

EMA1_FAST = 30

EMA1_MID = 60

EMA1_SLOW = 120


SUPPORTED_UPBIT_TIMEFRAMES = {
    5,
    15,
    30,
    60,
    240
}


SUPPORTED_OKX_TIMEFRAMES = {
    5,
    15,
    30,
    60,
    120,
    240,
    360,
    480,
    720,
    1440
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
# 시간봉
# =========================================================

def format_timeframe(minutes):

    minutes = int(minutes)

    if minutes >= 1440:

        days = minutes // 1440

        return f"{days}D"

    if minutes >= 60:

        hours = minutes // 60

        return f"{hours}H"

    return f"{minutes}M"


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

    return mapping.get(int(minutes))


def get_okx_bar_minutes(bar):

    mapping = {
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
    }

    return mapping.get(str(bar))


def get_current_candle_start(timeframe_minutes):

    timeframe_minutes = int(timeframe_minutes)

    now = datetime.now(KST)

    total_minutes = (
        now.hour * 60
        + now.minute
    )

    block_minutes = (
        total_minutes // timeframe_minutes
    ) * timeframe_minutes

    day_offset = block_minutes // 1440

    block_minutes %= 1440

    hour = block_minutes // 60

    minute = block_minutes % 60

    current = now.replace(
        hour=hour,
        minute=minute,
        second=0,
        microsecond=0
    )

    if day_offset:

        current = current - pd.Timedelta(
            days=day_offset
        )

    return current.replace(tzinfo=None)


def validate_timeframe():

    global EMA_TIMEFRAME

    try:

        EMA_TIMEFRAME = int(
            EMA_TIMEFRAME
        )

    except Exception:

        raise ValueError(
            "EMA_TIMEFRAME은 숫자여야 합니다."
        )

    if EMA_TIMEFRAME not in SUPPORTED_UPBIT_TIMEFRAMES:

        raise ValueError(
            "EMA_TIMEFRAME 오류\n"
            f"현재값: {EMA_TIMEFRAME}\n"
            "Upbit 지원값: 5, 15, 30, 60, 240"
        )

    okx_bar = get_okx_bar(
        EMA_TIMEFRAME
    )

    if okx_bar is None:

        raise ValueError(
            f"OKX에서 지원하지 않는 시간봉: "
            f"{EMA_TIMEFRAME}"
        )

    return True


# =========================================================
# 공통 API 요청
# =========================================================

def kst():

    return datetime.now(
        KST
    ).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


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

        last_request_time = (
            time.monotonic()
        )


def retry(func, *args, **kwargs):

    name = getattr(
        func,
        "__name__",
        str(func)
    )

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
                    RATE_LIMIT_WAIT
                    * 2 ** n,
                    60
                )

            elif r.status_code >= 500:

                wait = min(
                    2 * 2 ** n,
                    30
                )

            else:

                log.warning(
                    f"[HTTP {r.status_code}] "
                    f"{url}"
                )

                return r

            log.warning(
                f"[API 재시도] "
                f"{url} {wait}초"
            )

            time.sleep(wait)

        except Exception as e:

            log.error(
                f"[API 오류] "
                f"{name} {url}: {e}"
            )

            if n < MAX_RETRIES - 1:

                time.sleep(
                    min(
                        2 * (n + 1),
                        20
                    )
                )

    log.error(
        f"[API 최종 실패] {name} {url}"
    )

    return None


# =========================================================
# UPBIT
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

            if not market.startswith(
                "KRW-"
            ):

                continue

            try:

                volume = float(
                    x["acc_trade_price_24h"]
                )

                current_price = float(
                    x["trade_price"]
                )

            except Exception:

                continue

            if (
                volume > 0
                and current_price > 0
            ):

                result.append(
                    {
                        "market": market,
                        "volume_24h": volume,
                        "current_price": current_price
                    }
                )

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


def get_usdt_krw():

    r = retry(
        requests.get,
        "https://api.upbit.com/v1/ticker?markets=KRW-USDT",
        timeout=15
    )

    if r is None:

        return None

    try:

        price = float(
            r.json()[0]["trade_price"]
        )

        return (
            price
            if price > 0
            else None
        )

    except Exception:

        return None


def get_upbit_candle(
    market,
    unit,
    count=200,
    to=None
):

    unit = int(unit)

    url = (
        f"https://api.upbit.com/v1/"
        f"candles/minutes/{unit}"
    )

    params = {
        "market": market,
        "count": min(
            max(int(count), 1),
            200
        )
    }

    if to:

        params["to"] = to

    r = retry(
        requests.get,
        url,
        params=params,
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

        current = get_current_candle_start(
            unit
        )

        # 현재 진행 중인 봉 제거
        df = df[
            df.datetime < current
        ]

        if df.empty:

            return None

        return (
            df.sort_values("datetime")
            .drop_duplicates("datetime")
            .reset_index(drop=True)
        )

    except Exception as e:

        log.error(
            f"업비트 {unit}분 오류 "
            f"{market}: {e}"
        )

        return None


def get_upbit_1h(
    market,
    count=200,
    to=None
):

    return get_upbit_candle(
        market,
        EMA_TIMEFRAME,
        count,
        to
    )


# =========================================================
# OKX
# =========================================================

def get_okx_ohlcv(
    inst,
    bar="1H",
    limit=200,
    before=None
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

        params["before"] = str(
            before
        )

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

        numeric_cols = [
            "ts",
            "o",
            "h",
            "l",
            "c",
            "vol",
            "volCcy",
            "volCcyQuote"
        ]

        for col in numeric_cols:

            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

        # 확정봉만 사용
        df = df[
            df.confirm.astype(str) == "1"
        ]

        if df.empty:

            return None

        df["datetime"] = (
            pd.to_datetime(
                df["ts"],
                unit="ms",
                utc=True
            )
            .dt.tz_convert(KST)
            .dt.tz_localize(None)
        )

        bar_minutes = get_okx_bar_minutes(
            bar
        )

        if bar_minutes is not None:

            current = (
                get_current_candle_start(
                    bar_minutes
                )
            )

            df = df[
                df.datetime < current
            ]

        if df.empty:

            return None

        return (
            df.sort_values("ts")
            .drop_duplicates("ts")
            .reset_index(drop=True)
        )

    except Exception as e:

        log.error(
            f"OKX {inst} {bar} 오류: {e}"
        )

        return None


def get_okx_current_price(inst):

    r = retry(
        requests.get,
        "https://www.okx.com/api/v5/market/ticker",
        params={
            "instId": inst
        },
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

        price = float(
            data[0]["last"]
        )

        if price <= 0:

            return None

        return price

    except Exception as e:

        log.error(
            f"OKX 현재가 오류 "
            f"{inst}: {e}"
        )

        return None


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

        data = r.json().get(
            "data",
            []
        )

        result = []

        for x in data:

            inst = x.get(
                "instId",
                ""
            )

            state = x.get(
                "state",
                ""
            )

            if (
                inst.endswith("-USDT-SWAP")
                and state == "live"
            ):

                result.append(inst)

        return result

    except Exception as e:

        log.error(
            f"OKX 종목 오류: {e}"
        )

        return []


def get_okx_volume(
    inst,
    usdt_krw
):

    bar = get_okx_bar(
        EMA_TIMEFRAME
    )

    if bar is None:

        return 0

    df = get_okx_ohlcv(
        inst,
        bar,
        VOLUME_HOURS + 1
    )

    if df is None or df.empty:

        return 0

    try:

        quote_volume = pd.to_numeric(
            df["volCcyQuote"],
            errors="coerce"
        ).fillna(0)

        volume_usdt = float(
            quote_volume.tail(
                VOLUME_HOURS
            ).sum()
        )

        return (
            volume_usdt
            * float(usdt_krw)
        )

    except Exception:

        return 0


# =========================================================
# HISTORY
# =========================================================

def history_upbit(
    market,
    unit,
    required=200
):

    all_df = None

    to = None

    for _ in range(
        MAX_HISTORY_CHUNKS
    ):

        df = get_upbit_candle(
            market,
            unit,
            HISTORY_CHUNK,
            to
        )

        if (
            df is None
            or df.empty
        ):

            break

        if all_df is None:

            all_df = df.copy()

        else:

            all_df = pd.concat(
                [
                    df,
                    all_df
                ],
                ignore_index=True
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
            .strftime("%Y-%m-%dT%H:%M:%S")
        )

    return all_df


def history_okx(
    inst,
    bar,
    required=200
):

    all_df = None

    before = None

    for _ in range(
        MAX_HISTORY_CHUNKS
    ):

        df = get_okx_ohlcv(
            inst,
            bar,
            HISTORY_CHUNK,
            before
        )

        if (
            df is None
            or df.empty
        ):

            break

        if all_df is None:

            all_df = df.copy()

        else:

            all_df = pd.concat(
                [
                    df,
                    all_df
                ],
                ignore_index=True
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


# =========================================================
# EMA
# =========================================================

def ema(
    df,
    period
):

    if (
        df is None
        or df.empty
        or "c" not in df
    ):

        return None

    return (
        pd.to_numeric(
            df.c,
            errors="coerce"
        )
        .ewm(
            span=period,
            adjust=False,
            min_periods=1
        )
        .mean()
    )


def direction(df):

    if (
        df is None
        or df.empty
    ):

        return "none"

    try:

        e30 = ema(
            df,
            EMA1_FAST
        ).iloc[-1]

        e60 = ema(
            df,
            EMA1_MID
        ).iloc[-1]

        e120 = ema(
            df,
            EMA1_SLOW
        ).iloc[-1]

        if (
            e30 > e60
            and e60 > e120
        ):

            return "long"

        if (
            e30 < e60
            and e60 < e120
        ):

            return "short"

    except Exception as e:

        log.error(
            f"EMA1 방향 오류: {e}"
        )

    return "none"


def ema_alignment_count(df):

    if (
        df is None
        or df.empty
    ):

        return {
            "direction": "none",
            "count": 0,
            "spread": 0.0,
            "spread_30_60": 0.0,
            "spread_60_120": 0.0,
            "spread_average": 0.0
        }

    try:

        e30 = ema(
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

        current_e30 = float(
            e30.iloc[-1]
        )

        current_e60 = float(
            e60.iloc[-1]
        )

        current_e120 = float(
            e120.iloc[-1]
        )

        spread_30_60 = (
            (
                current_e30
                - current_e60
            )
            / current_e60
            * 100
            if current_e60 != 0
            else 0.0
        )

        spread_60_120 = (
            (
                current_e60
                - current_e120
            )
            / current_e120
            * 100
            if current_e120 != 0
            else 0.0
        )

        spread_average = (
            spread_30_60
            + spread_60_120
        ) / 2

        if (
            current_e30 > current_e60
            and current_e60 > current_e120
        ):

            current_direction = "long"

        elif (
            current_e30 < current_e60
            and current_e60 < current_e120
        ):

            current_direction = "short"

        else:

            current_direction = "none"

        count = 0

        for i in range(
            len(df) - 1,
            -1,
            -1
        ):

            v30 = float(
                e30.iloc[i]
            )

            v60 = float(
                e60.iloc[i]
            )

            v120 = float(
                e120.iloc[i]
            )

            if (
                v30 > v60
                and v60 > v120
            ):

                candle_direction = "long"

            elif (
                v30 < v60
                and v60 < v120
            ):

                candle_direction = "short"

            else:

                candle_direction = "none"

            if (
                candle_direction
                == current_direction
            ):

                count += 1

            else:

                break

        if current_direction == "none":

            count = 0

        return {
            "direction": current_direction,
            "count": count,
            "spread": spread_average,
            "spread_30_60": spread_30_60,
            "spread_60_120": spread_60_120,
            "spread_average": spread_average
        }

    except Exception as e:

        log.error(
            f"EMA1 배열 오류: {e}"
        )

        return {
            "direction": "none",
            "count": 0,
            "spread": 0.0,
            "spread_30_60": 0.0,
            "spread_60_120": 0.0,
            "spread_average": 0.0
        }


def ema_display(
    df,
    current_price=None
):

    result = ema_alignment_count(
        df
    )

    d = result["direction"]

    count = result["count"]

    if d == "long":

        icon = "🟢"

    elif d == "short":

        icon = "🔴"

    else:

        icon = "⚪"

        count = 0

    current_rate = 0.0

    try:

        e120 = ema(
            df,
            EMA1_SLOW
        )

        if (
            e120 is not None
            and not e120.empty
            and current_price is not None
        ):

            ema120_value = float(
                e120.iloc[-1]
            )

            current_price = float(
                current_price
            )

            if ema120_value != 0:

                current_rate = (
                    (
                        current_price
                        - ema120_value
                    )
                    / ema120_value
                    * 100
                )

    except Exception as e:

        log.error(
            f"EMA120 이격도 오류: {e}"
        )

    if current_rate > 0:

        spread_display = (
            f"▲ +{current_rate:.2f}%"
        )

    elif current_rate < 0:

        spread_display = (
            f"▼ {current_rate:.2f}%"
        )

    else:

        spread_display = "0.00%"

    return {
        "display": (
            f"{icon}({count})"
        ),
        "spread_display": spread_display,
        "direction": d,
        "count": count,
        "spread": result.get(
            "spread_average",
            0.0
        ),
        "spread_30_60": result.get(
            "spread_30_60",
            0.0
        ),
        "spread_60_120": result.get(
            "spread_60_120",
            0.0
        ),
        "spread_average": result.get(
            "spread_average",
            0.0
        ),
        "ema120_rate": current_rate,
        "current_price": current_price
    }


# =========================================================
# EMA2
# 확정봉 종가 기준 1/2/3차 매수
# =========================================================

def ema2_buy_analysis(
    df,
    current_price=None
):

    result = {
        "state": "none",
        "stage": 0,
        "display": "-",

        "ema30": None,
        "ema60": None,
        "ema120": None,

        "current_price": current_price,

        "close_price": None,

        "qualified": False
    }

    if (
        df is None
        or df.empty
        or "c" not in df
    ):

        return result

    try:

        # =====================================================
        # 가장 최근 확정봉 종가
        # =====================================================

        close_price = float(
            df["c"].iloc[-1]
        )

        result["close_price"] = (
            close_price
        )

        # =====================================================
        # EMA 계산
        # =====================================================

        e30 = float(
            ema(
                df,
                EMA1_FAST
            ).iloc[-1]
        )

        e60 = float(
            ema(
                df,
                EMA1_MID
            ).iloc[-1]
        )

        e120 = float(
            ema(
                df,
                EMA1_SLOW
            ).iloc[-1]
        )

        result["ema30"] = e30

        result["ema60"] = e60

        result["ema120"] = e120

        # 실시간 현재가는 표시용
        if current_price is not None:

            result["current_price"] = float(
                current_price
            )

        # =====================================================
        # 반드시 정배열 상태에서만 매수 판정
        # EMA30 > EMA60 > EMA120
        # =====================================================

        if not (
            e30 > e60
            and e60 > e120
        ):

            return result

        result["state"] = "long"

        result["qualified"] = True

        # =====================================================
        # 확정봉 종가 기준
        #
        # 종가 <= EMA120 → 3차
        # 종가 <= EMA60  → 2차
        # 종가 <= EMA30  → 1차
        #
        # 가장 깊은 단계 하나만 표시
        # =====================================================

        if close_price <= e120:

            result["stage"] = 3

            result["display"] = (
                "🟢 ③ 3차매수"
            )

            return result

        if close_price <= e60:

            result["stage"] = 2

            result["display"] = (
                "🟢 ② 2차매수"
            )

            return result

        if close_price <= e30:

            result["stage"] = 1

            result["display"] = (
                "🟢 ① 1차매수"
            )

            return result

        return result

    except Exception as e:

        log.error(
            f"EMA2 매수 분석 오류: {e}"
        )

        return result


# =========================================================
# 등락률
# =========================================================

def daily_change_upbit(
    market
):

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

        return (
            current
            - previous
        ) / previous * 100

    except Exception:

        return None


def daily_changes(df):

    if (
        df is None
        or df.empty
    ):

        return None

    try:

        temp = df.copy()

        temp["datetime"] = pd.to_datetime(
            temp["datetime"]
        )

        temp = (
            temp
            .set_index("datetime")
            .sort_index()
        )

        daily = (
            temp["c"]
            .resample(
                "1D",
                offset="9h"
            )
            .last()
            .dropna()
        )

        if len(daily) < 2:

            return None

        current = float(
            daily.iloc[-1]
        )

        previous = float(
            daily.iloc[-2]
        )

        if previous == 0:

            return None

        return (
            current
            - previous
        ) / previous * 100

    except Exception:

        return None


# =========================================================
# 표시 함수
# =========================================================

def format_change(
    value
):

    if value is None:

        return "-"

    try:

        value = float(value)

    except Exception:

        return "-"

    if value > 0:

        return (
            f"<span class='up'>"
            f"+{value:.2f}%"
            f"</span>"
        )

    if value < 0:

        return (
            f"<span class='down'>"
            f"{value:.2f}%"
            f"</span>"
        )

    return "0.00%"


def format_volume(
    value
):

    if value is None:

        return "-"

    try:

        value = float(value)

    except Exception:

        return "-"

    if value >= 1_000_000_000_000:

        return (
            f"{value / 1_000_000_000_000:.2f}조"
        )

    if value >= 100_000_000:

        return (
            f"{value / 100_000_000:.0f}억"
        )

    if value >= 10_000_000:

        return (
            f"{value / 10_000_000:.0f}천만"
        )

    return f"{value:,.0f}"


def ema2_buy_html(
    ema2
):

    if not ema2:

        return "-"

    stage = ema2.get(
        "stage",
        0
    )

    if stage == 1:

        return (
            "<span class='buy-stage stage1'>"
            "① 1차매수"
            "</span>"
        )

    if stage == 2:

        return (
            "<span class='buy-stage stage2'>"
            "② 2차매수"
            "</span>"
        )

    if stage == 3:

        return (
            "<span class='buy-stage stage3'>"
            "③ 3차매수"
            "</span>"
        )

    return "-"


def ema_html(
    ema1
):

    if not ema1:

        return "-"

    return (
        f"<div class='ema-main'>"
        f"{ema1.get('display', '-')}"
        f"</div>"
        f"<div class='ema-sub'>"
        f"{ema1.get('spread_display', '-')}"
        f"</div>"
    )


# =========================================================
# 분석
# =========================================================

def empty_analysis():

    return {
        "ema_1h": {
            "display": "⚪(0)",
            "spread_display": "-",
            "direction": "none",
            "count": 0
        },

        "ema2_buy": {
            "state": "none",
            "stage": 0,
            "display": "-",
            "qualified": False,
            "close_price": None
        },

        "change": None,

        "qualified": False,

        "direction_1h": "none",

        "df1h": None
    }


def analyze(
    market,
    okx=False,
    current_price=None
):

    try:

        if okx:

            bar = get_okx_bar(
                EMA_TIMEFRAME
            )

            df1 = history_okx(
                market,
                bar,
                200
            )

        else:

            df1 = history_upbit(
                market,
                EMA_TIMEFRAME,
                200
            )

        if (
            df1 is None
            or df1.empty
        ):

            return empty_analysis()

        e1 = ema_display(
            df1,
            current_price
        )

        # =====================================================
        # EMA2
        # 확정봉 종가로 매수 단계 판정
        # =====================================================

        ema2 = ema2_buy_analysis(
            df1,
            current_price
        )

        if okx:

            change = daily_changes(
                df1
            )

        else:

            change = daily_change_upbit(
                market
            )

        return {
            "ema_1h": e1,

            "ema2_buy": ema2,

            "change": change,

            "qualified": ema2[
                "qualified"
            ],

            "direction_1h": e1[
                "direction"
            ],

            "df1h": df1
        }

    except Exception as e:

        log.error(
            f"분석 오류 {market}: {e}"
        )

        return empty_analysis()


# =========================================================
# ROW
# =========================================================

def make_row(
    rank,
    name,
    volume,
    analysis
):

    return {
        "rank": rank,

        "name": name,

        "volume": volume,

        "change": analysis.get(
            "change"
        ),

        "ema_1h": analysis.get(
            "ema_1h",
            {}
        ),

        "ema2_buy": analysis.get(
            "ema2_buy",
            {}
        ),

        "qualified": analysis.get(
            "qualified",
            False
        )
    }


# =========================================================
# ROW HTML
# =========================================================

def rows_html(
    data
):

    if not data:

        return """
        <tr>
            <td colspan="6" class="empty">
                현재 종목 없음
            </td>
        </tr>
        """

    html = ""

    for x in data:

        rank = x.get(
            "rank",
            "-"
        )

        name = x.get(
            "name",
            "-"
        )

        volume = format_volume(
            x.get("volume")
        )

        change = format_change(
            x.get("change")
        )

        ema1 = x.get(
            "ema_1h",
            {}
        )

        ema2 = x.get(
            "ema2_buy",
            {}
        )

        direction = ema1.get(
            "direction",
            "none"
        )

        stage = ema2.get(
            "stage",
            0
        )

        row_class = ""

        if stage == 1:

            row_class = "buy-row stage1-row"

        elif stage == 2:

            row_class = "buy-row stage2-row"

        elif stage == 3:

            row_class = "buy-row stage3-row"

        elif direction == "long":

            row_class = "long-row"

        html += f"""
        <tr class="{row_class}">

            <td>
                {rank}
            </td>

            <td class="coin-name">
                {name}
            </td>

            <td>
                {volume}
            </td>

            <td>
                {change}
            </td>

            <td>
                {ema_html(ema1)}
            </td>

            <td>
                {ema2_buy_html(ema2)}
            </td>

        </tr>
        """

    return html


# =========================================================
# 전체 TOP 리스트
# =========================================================

def main_table(
    title,
    data,
    update_time
):

    return f"""
    <h2 class="section-title">

        {title}

        <small>
            {update_time} KST
        </small>

    </h2>

    <div class="table-wrap">

        <table>

            <thead>

                <tr>

                    <th>#</th>

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

    </div>
    """


# =========================================================
# 거래소별 매수 확인 리스트
# 1차 / 2차 / 3차를 하나로 합침
# =========================================================

def buy_focus_section(
    data,
    update_time,
    exchange_name
):

    buy_list = []

    for x in data:

        ema2 = x.get(
            "ema2_buy",
            {}
        )

        stage = ema2.get(
            "stage",
            0
        )

        if stage in (
            1,
            2,
            3
        ):

            buy_list.append(x)

    # 매수 단계 순서
    # 1차 → 2차 → 3차
    buy_list.sort(
        key=lambda x:
        x.get(
            "ema2_buy",
            {}
        ).get(
            "stage",
            0
        )
    )

    return f"""
    <h2 class="focus-title buy-title">

        🟢 {exchange_name} 매수 확인 리스트

        <small>
            {update_time} KST
        </small>

    </h2>

    <div class="table-wrap buy-focus-table">

        <table>

            <thead>

                <tr>

                    <th>#</th>

                    <th>코인</th>

                    <th>거래대금</th>

                    <th>등락률</th>

                    <th>EMA1</th>

                    <th>매수상태</th>

                </tr>

            </thead>

            <tbody>

                {
                    rows_html(
                        buy_list
                    )
                }

            </tbody>

        </table>

    </div>
    """


# =========================================================
# UPBIT 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data

    global latest_upbit_update_time

    if USE_UPBIT != "Y":

        return

    try:

        markets = get_upbit_markets()

        if not markets:

            return

        markets = sorted(
            markets,
            key=lambda x:
            x["volume_24h"],
            reverse=True
        )

        selected = markets[
            :TOP_N
        ]

        result = []

        for rank, item in enumerate(
            selected,
            1
        ):

            market = item[
                "market"
            ]

            current_price = item[
                "current_price"
            ]

            analysis = analyze(
                market,
                okx=False,
                current_price=current_price
            )

            coin = market.replace(
                "KRW-",
                ""
            )

            result.append(
                make_row(
                    rank,
                    coin,
                    item["volume_24h"],
                    analysis
                )
            )

        latest_upbit_data = result

        latest_upbit_update_time = kst()

        stages = {
            1: 0,
            2: 0,
            3: 0
        }

        for x in result:

            stage = x.get(
                "ema2_buy",
                {}
            ).get(
                "stage",
                0
            )

            if stage in stages:

                stages[stage] += 1

        log.info(
            "[UPBIT] TOP%d 완료 | "
            "1차=%d 2차=%d 3차=%d",
            TOP_N,
            stages[1],
            stages[2],
            stages[3]
        )

    except Exception as e:

        log.error(
            f"업비트 업데이트 오류: {e}"
        )


# =========================================================
# OKX 업데이트
# =========================================================

def update_okx():

    global latest_okx_data

    global latest_okx_update_time

    global latest_usdt_krw

    if USE_OKX != "Y":

        return

    try:

        usdt = get_usdt_krw()

        if usdt is None:

            return

        latest_usdt_krw = usdt

        symbols = get_okx_symbols()

        if not symbols:

            return

        volume_list = []

        for inst in symbols:

            volume = get_okx_volume(
                inst,
                usdt
            )

            if volume > 0:

                volume_list.append(
                    (
                        inst,
                        volume
                    )
                )

        volume_list.sort(
            key=lambda x: x[1],
            reverse=True
        )

        selected = volume_list[
            :TOP_N
        ]

        result = []

        upbit_names = set(
            latest_upbit_markets
        )

        for rank, (
            inst,
            volume
        ) in enumerate(
            selected,
            1
        ):

            current_price = (
                get_okx_current_price(
                    inst
                )
            )

            analysis = analyze(
                inst,
                okx=True,
                current_price=current_price
            )

            coin = inst.replace(
                "-USDT-SWAP",
                ""
            )

            # 업비트 상장 여부 표시
            upbit_market = (
                f"KRW-{coin}"
            )

            if (
                upbit_market
                in upbit_names
            ):

                display_name = (
                    f"{coin} (업비트)"
                )

            else:

                display_name = coin

            # 기존 UI와 맞추기 위해 OKX 거래대금은 10분의 1 표시
            display_volume = (
                volume / 10
            )

            result.append(
                make_row(
                    rank,
                    display_name,
                    display_volume,
                    analysis
                )
            )

        latest_okx_data = result

        latest_okx_update_time = kst()

        stages = {
            1: 0,
            2: 0,
            3: 0
        }

        for x in result:

            stage = x.get(
                "ema2_buy",
                {}
            ).get(
                "stage",
                0
            )

            if stage in stages:

                stages[stage] += 1

        log.info(
            "[OKX] TOP%d 완료 | "
            "1차=%d 2차=%d 3차=%d",
            TOP_N,
            stages[1],
            stages[2],
            stages[3]
        )

    except Exception as e:

        log.error(
            f"OKX 업데이트 오류: {e}"
        )


# =========================================================
# 전체 업데이트
# =========================================================

def update_all():

    with update_lock:

        try:

            if USE_UPBIT == "Y":

                update_upbit()

            if USE_OKX == "Y":

                update_okx()

        except Exception as e:

            log.error(
                f"전체 업데이트 오류: {e}"
            )


# =========================================================
# HTML
# =========================================================

HTML_HEAD = """
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
EMA 매수 확인 대시보드
</title>

<style>

* {
    box-sizing: border-box;
}

body {

    margin: 0;

    padding: 14px;

    background: #111;

    color: #eee;

    font-family:
        Arial,
        "Noto Sans KR",
        sans-serif;

    font-size: 14px;
}

.container {

    max-width: 1400px;

    margin: 0 auto;
}

.header {

    background: #1b1b1b;

    border-radius: 12px;

    padding: 16px;

    margin-bottom: 14px;

    border: 1px solid #333;
}

.header h1 {

    margin: 0 0 10px 0;

    font-size: 21px;
}

.info {

    line-height: 1.8;

    color: #bbb;

    font-size: 13px;
}

.info strong {

    color: #fff;
}

.section-title,
.focus-title {

    margin: 18px 0 8px;

    padding: 12px 14px;

    border-radius: 10px;

    background: #1b1b1b;

    border: 1px solid #333;

    font-size: 17px;
}

.section-title small,
.focus-title small {

    float: right;

    font-size: 11px;

    color: #888;

    font-weight: normal;
}

.buy-title {

    margin-top: 24px;

    border-left: 4px solid #36d399;
}

.table-wrap {

    width: 100%;

    overflow-x: auto;

    background: #171717;

    border-radius: 10px;

    border: 1px solid #333;

    margin-bottom: 16px;
}

table {

    width: 100%;

    min-width: 650px;

    border-collapse: collapse;
}

th {

    background: #222;

    color: #aaa;

    font-weight: 600;

    padding: 10px 7px;

    border-bottom: 1px solid #333;

    white-space: nowrap;
}

td {

    padding: 10px 7px;

    text-align: center;

    border-bottom: 1px solid #292929;

    white-space: nowrap;
}

tr:last-child td {

    border-bottom: none;
}

.coin-name {

    font-weight: 700;
}

.up {

    color: #ff5b6e;
}

.down {

    color: #4ea1ff;
}

.ema-main {

    font-weight: 700;
}

.ema-sub {

    font-size: 11px;

    color: #aaa;

    margin-top: 2px;
}

.buy-stage {

    display: inline-block;

    padding: 4px 7px;

    border-radius: 6px;

    font-weight: 700;
}

.stage1 {

    background: rgba(70, 180, 100, .15);

    color: #5ee58a;
}

.stage2 {

    background: rgba(255, 190, 50, .15);

    color: #ffc94d;
}

.stage3 {

    background: rgba(255, 90, 90, .15);

    color: #ff7373;
}

.stage1-row td {

    background: rgba(70, 180, 100, .045);
}

.stage2-row td {

    background: rgba(255, 190, 50, .045);
}

.stage3-row td {

    background: rgba(255, 90, 90, .045);
}

.long-row td {

    background: rgba(50, 180, 100, .025);
}

.empty {

    padding: 22px;

    color: #777;
}

.footer {

    color: #666;

    text-align: center;

    padding: 25px 0 10px;

    font-size: 11px;
}

@media (
    max-width: 700px
) {

    body {

        padding: 7px;

        font-size: 12px;
    }

    .header {

        padding: 12px;
    }

    .header h1 {

        font-size: 17px;
    }

    .info {

        font-size: 11px;
    }

    .section-title,
    .focus-title {

        font-size: 14px;

        padding: 10px;
    }

    .section-title small,
    .focus-title small {

        display: block;

        float: none;

        margin-top: 4px;
    }

    th,
    td {

        padding: 8px 5px;
    }
}

</style>

</head>

<body>

<div class="container">
"""


HTML_TAIL = """

<div class="footer">

    EMA30 · EMA60 · EMA120 기반
    확정봉 매수 확인 시스템

</div>

</div>

</body>

</html>
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

    timeframe_text = format_timeframe(
        EMA_TIMEFRAME
    )

    # =====================================================
    # 헤더
    # =====================================================

    sections += f"""

    <div class="header">

        <h1>
            📊 EMA 매수 확인 대시보드
        </h1>

        <div class="info">

            <div>
                <strong>시간봉:</strong>
                {timeframe_text} 확정봉
            </div>

            <div>
                <strong>EMA1:</strong>
                EMA30 · EMA60 · EMA120
            </div>

            <div>
                <strong>정배열:</strong>
                EMA30 &gt; EMA60 &gt; EMA120
            </div>

            <div>
                <strong>EMA2:</strong>
                확정봉 종가 기준 분할매수
            </div>

            <div>
                <strong>① 1차:</strong>
                확정봉 종가 ≤ EMA30
            </div>

            <div>
                <strong>② 2차:</strong>
                확정봉 종가 ≤ EMA60
            </div>

            <div>
                <strong>③ 3차:</strong>
                확정봉 종가 ≤ EMA120
            </div>

            <div>
                <strong>주의:</strong>
                정배열 상태에서만 매수단계 표시
            </div>

        </div>

    </div>
    """

    # =====================================================
    # 업비트
    # =====================================================

    if USE_UPBIT == "Y":

        sections += main_table(
            "🟢 업비트 TOP "
            f"{TOP_N}",
            latest_upbit_data,
            latest_upbit_update_time
        )

        # ★ 1차/2차/3차를 하나로 통합
        sections += buy_focus_section(
            latest_upbit_data,
            latest_upbit_update_time,
            "업비트"
        )

    # =====================================================
    # OKX
    # =====================================================

    if USE_OKX == "Y":

        sections += main_table(
            "🔵 OKX 실거래대금 TOP "
            f"{TOP_N}",
            latest_okx_data,
            latest_okx_update_time
        )

        # ★ 1차/2차/3차를 하나로 통합
        sections += buy_focus_section(
            latest_okx_data,
            latest_okx_update_time,
            "OKX"
        )

    # =====================================================
    # 최종 HTML
    # =====================================================

    return HTML_HEAD + sections + HTML_TAIL


# =========================================================
# 백그라운드 스케줄러
# =========================================================

def scheduler_worker():

    schedule.clear()

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
                f"스케줄러 오류: {e}"
            )

        time.sleep(1)


# =========================================================
# 시작
# =========================================================

def startup():

    validate_timeframe()

    log.info(
        "=========================================="
    )

    log.info(
        "EMA 매수 확인 대시보드 시작"
    )

    log.info(
        f"시간봉: {format_timeframe(EMA_TIMEFRAME)}"
    )

    log.info(
        "EMA1: EMA30 > EMA60 > EMA120"
    )

    log.info(
        "EMA2: 확정봉 종가 기준"
    )

    log.info(
        "1차매수: 종가 <= EMA30"
    )

    log.info(
        "2차매수: 종가 <= EMA60"
    )

    log.info(
        "3차매수: 종가 <= EMA120"
    )

    log.info(
        "업비트: %s",
        USE_UPBIT
    )

    log.info(
        "OKX: %s",
        USE_OKX
    )

    log.info(
        "TOP_N: %d",
        TOP_N
    )

    log.info(
        "=========================================="
    )

    update_all()

    thread = threading.Thread(
        target=scheduler_worker,
        daemon=True
    )

    thread.start()


# =========================================================
# 실행
# =========================================================

if __name__ == "__main__":

    startup()

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )
