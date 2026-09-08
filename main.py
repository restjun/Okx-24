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

EMA_TIMEFRAME = 60
EMA_HIGH_TIMEFRAME = 240

EMA1_FAST = 30
EMA1_MID = 60
EMA1_SLOW = 120
EMA1_MAX_COUNT = 100

ROC_PERIOD = 10
ROC_NEAR_ZERO = -0.30
ROC_SHORT_NEAR_ZERO = 0.30
ROC_FOCUS_TOP = 10

SUPPORTED_UPBIT_TIMEFRAMES = {5, 15, 30, 60, 240}

SUPPORTED_OKX_TIMEFRAMES = {
    5, 15, 30, 60, 120, 240,
    360, 480, 720, 1440
}


# =========================================================
# 전역 상태
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
# 공통
# =========================================================

def kst():
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


def format_timeframe(minutes):
    minutes = int(minutes)

    if minutes >= 1440:
        return f"{minutes // 1440}D"

    if minutes >= 60:
        return f"{minutes // 60}H"

    return f"{minutes}M"


OKX_BARS = {
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

OKX_BAR_MINUTES = {
    v: k for k, v in OKX_BARS.items()
}


def get_okx_bar(minutes):
    return OKX_BARS.get(int(minutes))


def get_okx_bar_minutes(bar):
    return OKX_BAR_MINUTES.get(str(bar))


def get_current_candle_start(minutes):
    minutes = int(minutes)

    now = datetime.now(KST)

    total = now.hour * 60 + now.minute
    block = (total // minutes) * minutes

    return now.replace(
        hour=block // 60,
        minute=block % 60,
        second=0,
        microsecond=0
    ).replace(tzinfo=None)


def validate_timeframe():
    global EMA_TIMEFRAME

    try:
        EMA_TIMEFRAME = int(EMA_TIMEFRAME)
    except Exception:
        raise ValueError("EMA_TIMEFRAME은 숫자여야 합니다.")

    if EMA_TIMEFRAME not in SUPPORTED_UPBIT_TIMEFRAMES:
        raise ValueError(
            f"EMA_TIMEFRAME 오류: {EMA_TIMEFRAME}\n"
            "Upbit 지원값: 5, 15, 30, 60, 240"
        )

    if get_okx_bar(EMA_TIMEFRAME) is None:
        raise ValueError(
            f"OKX에서 지원하지 않는 시간봉: {EMA_TIMEFRAME}"
        )


def wait_request():
    global last_request_time

    with request_lock:
        gap = time.monotonic() - last_request_time

        if gap < REQUEST_INTERVAL:
            time.sleep(REQUEST_INTERVAL - gap)

        last_request_time = time.monotonic()


def retry(func, *args, **kwargs):
    name = getattr(func, "__name__", str(func))

    url = (
        args[0]
        if args and isinstance(args[0], str)
        else kwargs.get("url", "")
    )

    for n in range(MAX_RETRIES):
        try:
            wait_request()

            r = func(*args, **kwargs)

            if not hasattr(r, "status_code"):
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
                f"[API 오류] {name} {url}: {e}"
            )

            if n < MAX_RETRIES - 1:
                time.sleep(
                    min(2 * (n + 1), 20)
                )

    log.error(
        f"[API 최종 실패] {name} {url}"
    )

    return None


# =========================================================
# Upbit
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
            market = x.get("market", "")

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
            x["market"] for x in result
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

        return price if price > 0 else None

    except Exception:
        return None


def get_upbit_candle(
    market,
    unit,
    count=200,
    to=None,
    include_current=False
):
    unit = int(unit)

    url = (
        "https://api.upbit.com/v1/candles/minutes/"
        f"{unit}"
    )

    params = {
        "market": market,
        "count": min(max(int(count), 1), 200)
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
        df = pd.DataFrame(r.json())

        if df.empty:
            return None

        mapping = {
            "opening_price": "o",
            "high_price": "h",
            "low_price": "l",
            "trade_price": "c",
            "candle_acc_trade_price": "volume_krw",
            "candle_date_time_kst": "datetime"
        }

        for src, dst in mapping.items():

            if src == "datetime":
                df[dst] = pd.to_datetime(
                    df[src],
                    errors="coerce"
                )

            else:
                df[dst] = pd.to_numeric(
                    df[src],
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
            current = get_current_candle_start(unit)

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
            f"업비트 {unit}분 오류 {market}: {e}"
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


def get_upbit_current_roc_data(
    market,
    current_price
):
    df = get_upbit_candle(
        market,
        EMA_TIMEFRAME,
        include_current=True
    )

    if df is None or df.empty:
        return None

    try:
        start = get_current_candle_start(
            EMA_TIMEFRAME
        )

        price = float(current_price)

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
            .reset_index(drop=True)
        )

    except Exception as e:
        log.error(
            f"업비트 현재 ROC 오류 {market}: {e}"
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

        numeric = [
            "ts",
            "o",
            "h",
            "l",
            "c",
            "vol",
            "volCcy",
            "volCcyQuote"
        ]

        for col in numeric:
            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

        if not include_current:
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


def get_okx_ohlcv_current(
    inst,
    bar="1H",
    limit=200
):
    return get_okx_ohlcv(
        inst,
        bar,
        limit,
        include_current=True
    )


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

        return price if price > 0 else None

    except Exception as e:
        log.error(
            f"OKX 현재가 오류 {inst}: {e}"
        )

        return None


# =========================================================
# History
# =========================================================

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
                [
                    df,
                    all_df
                ],
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

        to = all_df.datetime.iloc[0].strftime(
            "%Y-%m-%dT%H:%M:%S"
        )

    return all_df


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
                [
                    df,
                    all_df
                ],
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


def ema_alignment_count(df):

    if df is None or df.empty:
        return {
            "direction": "none",
            "count": 0
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

        a, b, c = (
            float(e30.iloc[-1]),
            float(e60.iloc[-1]),
            float(e120.iloc[-1])
        )

        if a > b > c:
            current = "long"

        elif a < b < c:
            current = "short"

        else:
            current = "none"

        if current == "none":
            return {
                "direction": "none",
                "count": 0
            }

        count = 0

        for i in range(
            len(df) - 1,
            -1,
            -1
        ):

            x, y, z = (
                float(e30.iloc[i]),
                float(e60.iloc[i]),
                float(e120.iloc[i])
            )

            candle = (
                "long"
                if x > y > z
                else "short"
                if x < y < z
                else "none"
            )

            if candle != current:
                break

            count += 1

        return {
            "direction": current,
            "count": count
        }

    except Exception as e:

        log.error(
            f"EMA 배열 오류: {e}"
        )

        return {
            "direction": "none",
            "count": 0
        }


def ema_display(
    df,
    current_price=None
):
    r = ema_alignment_count(df)

    d = r["direction"]
    count = r["count"]

    icon = {
        "long": "🟢",
        "short": "🔴"
    }.get(
        d,
        "⚪"
    )

    if d == "none":
        count = 0

    return {
        "display": f"{icon}({count})",
        "direction": d,
        "count": count,
        "current_price": current_price
    }


# =========================================================
# ROC
# =========================================================

def roc(
    df,
    period=ROC_PERIOD
):
    if (
        df is None
        or df.empty
        or "c" not in df
    ):
        return None

    try:

        period = int(period)

        if period <= 0:
            return None

        close = pd.to_numeric(
            df["c"],
            errors="coerce"
        )

        return (
            close / close.shift(period) - 1
        ) * 100

    except Exception as e:

        log.error(
            f"ROC 계산 오류: {e}"
        )

        return None


def roc_analysis(
    df_confirmed,
    df_current
):
    result = {
        "roc10": None,
        "roc10_previous": None,
        "roc10_count": 0,
        "roc10_negative_count": 0,
        "long_candidate": False,
        "short_candidate": False,
        "near_zero_long": False,
        "near_zero_short": False,
        "breakout_long": False,
        "state": "none",
        "display": "-"
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

        if confirmed is None or current is None:
            return result

        previous_10 = float(
            confirmed.iloc[-1]
        )

        current_10 = float(
            current.iloc[-1]
        )

        if (
            pd.isna(previous_10)
            or pd.isna(current_10)
        ):
            return result

        result["roc10"] = current_10
        result["roc10_previous"] = previous_10

        positive_count = 0

        for v in reversed(
            current.tolist()
        ):

            if (
                pd.isna(v)
                or float(v) <= 0
            ):
                break

            positive_count += 1

        negative_count = 0

        for v in reversed(
            current.tolist()
        ):

            if (
                pd.isna(v)
                or float(v) >= 0
            ):
                break

            negative_count += 1

        result["roc10_count"] = positive_count
        result["roc10_negative_count"] = negative_count

        long_cross = (
            previous_10 <= 0
            and current_10 > 0
        )

        short_cross = (
            previous_10 >= 0
            and current_10 < 0
        )

        near_long = (
            current_10 < 0
            and current_10 >= ROC_NEAR_ZERO
            and current_10 > previous_10
        )

        near_short = (
            current_10 > 0
            and current_10 <= ROC_SHORT_NEAR_ZERO
            and current_10 < previous_10
        )

        result.update({
            "long_candidate": long_cross,
            "short_candidate": short_cross,
            "near_zero_long": near_long,
            "near_zero_short": near_short,
            "breakout_long": long_cross
        })

        if long_cross:

            result["state"] = "long"
            result["display"] = "🟢 매수 ①"

        elif short_cross:

            result["state"] = "short"
            result["display"] = "🔴 숏 ①"

        elif (
            current_10 > 0
            and positive_count >= 2
        ):

            result["state"] = "progress"
            result["display"] = (
                f"🚀 진행 {positive_count}"
            )

        elif (
            current_10 < 0
            and negative_count >= 2
        ):

            result["state"] = "short_progress"
            result["display"] = (
                f"📉 진행 {negative_count}"
            )

        elif near_long:

            result["state"] = "near"
            result["display"] = "🔥 돌파"

        elif near_short:

            result["state"] = "short_near"
            result["display"] = "⚠️ 하락"

        return result

    except Exception as e:

        log.error(
            f"ROC 분석 오류: {e}"
        )

        return result


# =========================================================
# 등락률 / 표시
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
            (current - previous)
            / previous * 100
        ]

    except Exception:
        return None


def daily_changes(df):

    if df is None or df.empty:
        return None

    try:

        x = df.copy()

        x["datetime"] = pd.to_datetime(
            x["datetime"],
            errors="coerce"
        )

        x["c"] = pd.to_numeric(
            x["c"],
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
            x["c"]
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

        if previous == 0:
            return None

        return [
            (
                float(daily.iloc[-1])
                - previous
            ) / previous * 100
        ]

    except Exception:
        return None


def get_change_value(change):

    if change is None:
        return None

    try:

        return float(
            change[0]
            if isinstance(
                change,
                (list, tuple)
            )
            else change
        )

    except Exception:
        return None


def format_change(x):

    value = get_change_value(x)

    if value is None:
        return "-"

    if value > 0:
        return (
            f'<span class="up">'
            f'▲ +{value:.2f}%'
            f'</span>'
        )

    if value < 0:
        return (
            f'<span class="down">'
            f'▼ {value:.2f}%'
            f'</span>'
        )

    return (
        '<span class="zero">'
        '0.00%'
        '</span>'
    )


def format_volume(v):

    if v is None:
        return "-"

    try:
        v = float(v)

    except Exception:
        return "-"

    if v >= 1e12:
        return f"{v / 1e12:.2f}조"

    if v >= 1e8:
        return f"{v / 1e8:.0f}억"

    if v >= 1e4:
        return f"{v / 1e4:.0f}만"

    return f"{v:,.0f}"


# =========================================================
# 분석 기본값
# =========================================================

def empty_ema():
    return {
        "display": "⚪(0)",
        "direction": "none",
        "count": 0,
        "current_price": None
    }


def empty_roc():
    return {
        "roc10": None,
        "roc10_previous": None,
        "roc10_count": 0,
        "roc10_negative_count": 0,
        "long_candidate": False,
        "short_candidate": False,
        "near_zero_long": False,
        "near_zero_short": False,
        "breakout_long": False,
        "state": "none",
        "display": "-"
    }


def empty_analysis():
    return {
        "ema_1h": empty_ema(),
        "ema_high": empty_ema(),
        "roc": empty_roc(),
        "changes": None,
        "qualified": False,
        "short_qualified": False,
        "near_zero_qualified": False,
        "short_near_zero_qualified": False,
        "breakout_qualified": False,
        "progress_qualified": False,
        "short_progress_qualified": False,
        "direction_1h": "none",
        "df1h": None
    }


# =========================================================
# 분석
# =========================================================

def analyze(
    market,
    okx=False,
    current_price=None
):

    # -----------------------------------------------------
    # 1H 확정봉
    # -----------------------------------------------------

    if okx:

        bar = get_okx_bar(
            EMA_TIMEFRAME
        )

        if bar is None:
            return None

        df_confirmed = history_okx(
            market,
            bar
        )

    else:

        df_confirmed = history_upbit(
            market,
            EMA_TIMEFRAME
        )

    if (
        df_confirmed is None
        or df_confirmed.empty
    ):
        return None

    e1 = ema_display(
        df_confirmed,
        current_price
    )

    # -----------------------------------------------------
    # 4H EMA 표시 전용
    # -----------------------------------------------------

    if okx:

        high_bar = get_okx_bar(
            EMA_HIGH_TIMEFRAME
        )

        df_high = (
            history_okx(
                market,
                high_bar
            )
            if high_bar
            else None
        )

    else:

        df_high = history_upbit(
            market,
            EMA_HIGH_TIMEFRAME
        )

    e_high = ema_display(
        df_high,
        current_price
    )

    # -----------------------------------------------------
    # ROC 현재 캔들
    # -----------------------------------------------------

    if okx:

        bar = get_okx_bar(
            EMA_TIMEFRAME
        )

        df_current = get_okx_ohlcv_current(
            market,
            bar,
            200
        )

        if (
            df_current is not None
            and not df_current.empty
            and current_price is not None
        ):

            try:

                start = get_current_candle_start(
                    EMA_TIMEFRAME
                )

                mask = (
                    df_current.datetime == start
                )

                if mask.any():

                    df_current.loc[
                        mask,
                        "c"
                    ] = float(
                        current_price
                    )

            except Exception as e:

                log.error(
                    f"OKX ROC 가격 반영 오류 "
                    f"{market}: {e}"
                )

    else:

        df_current = get_upbit_current_roc_data(
            market,
            current_price
        )

    roc_data = roc_analysis(
        df_confirmed,
        df_current
    )

    # -----------------------------------------------------
    # 신호 조건
    # -----------------------------------------------------

    ema_ok = (
        e1["direction"]
        in ("long", "short")
        and e1["count"]
        <= EMA1_MAX_COUNT
    )

    long_qualified = (
        e1["direction"] == "long"
        and e1["count"] <= EMA1_MAX_COUNT
        and roc_data["long_candidate"]
    )

    short_qualified = (
        e1["direction"] == "short"
        and e1["count"] <= EMA1_MAX_COUNT
        and roc_data["short_candidate"]
    )

    near_zero_qualified = (
        e1["direction"] == "long"
        and e1["count"] <= EMA1_MAX_COUNT
        and roc_data["near_zero_long"]
    )

    short_near_zero_qualified = (
        e1["direction"] == "short"
        and e1["count"] <= EMA1_MAX_COUNT
        and roc_data["near_zero_short"]
    )

    progress_qualified = (
        e1["direction"] == "long"
        and e1["count"] <= EMA1_MAX_COUNT
        and roc_data["roc10"] is not None
        and roc_data["roc10"] > 0
        and roc_data["roc10_count"] >= 2
    )

    short_progress_qualified = (
        e1["direction"] == "short"
        and e1["count"] <= EMA1_MAX_COUNT
        and roc_data["roc10"] is not None
        and roc_data["roc10"] < 0
        and roc_data["roc10_negative_count"] >= 2
    )

    changes = (
        daily_changes(df_confirmed)
        if okx
        else daily_change_upbit(market)
    )

    return {
        "ema_1h": e1,
        "ema_high": e_high,
        "roc": roc_data,
        "changes": changes,

        "qualified": long_qualified,
        "short_qualified": short_qualified,

        "near_zero_qualified":
            near_zero_qualified,

        "short_near_zero_qualified":
            short_near_zero_qualified,

        "breakout_qualified":
            long_qualified,

        "progress_qualified":
            progress_qualified,

        "short_progress_qualified":
            short_progress_qualified,

        "direction_1h":
            e1["direction"],

        "df1h":
            df_confirmed
    }


# =========================================================
# Row
# =========================================================

def make_row(
    rank,
    name,
    volume,
    analysis,
    current_price=None
):
    a = analysis or empty_analysis()

    return {
        "rank": rank,
        "name": name,

        "change": format_change(
            a["changes"]
        ),

        "change_value":
            get_change_value(
                a["changes"]
            ),

        "volume":
            format_volume(volume),

        "current_price":
            current_price,

        "ema_1h":
            a["ema_1h"],

        "ema_high":
            a["ema_high"],

        "roc":
            a["roc"],

        "qualified":
            a["qualified"],

        "short_qualified":
            a["short_qualified"],

        "near_zero_qualified":
            a["near_zero_qualified"],

        "short_near_zero_qualified":
            a["short_near_zero_qualified"],

        "breakout_qualified":
            a["breakout_qualified"],

        "progress_qualified":
            a["progress_qualified"],

        "short_progress_qualified":
            a["short_progress_qualified"],

        "direction":
            a["direction_1h"]
    }


# =========================================================
# 후보 판정
# =========================================================

def is_upbit_buy_candidate(r):
    return bool(
        r and r.get("qualified")
    )


def is_okx_long_candidate(r):
    return bool(
        r and r.get("qualified")
    )


def is_okx_short_candidate(r):
    return bool(
        r and r.get("short_qualified")
    )


def is_roc_near_zero_candidate(r):
    return bool(
        r and r.get(
            "near_zero_qualified"
        )
    )


def is_roc_progress_candidate(r):
    return bool(
        r and r.get(
            "progress_qualified"
        )
    )


def is_roc_short_near_zero_candidate(r):
    return bool(
        r and r.get(
            "short_near_zero_qualified"
        )
    )


def is_roc_short_progress_candidate(r):
    return bool(
        r and r.get(
            "short_progress_qualified"
        )
    )


# =========================================================
# Upbit 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update_time

    log.info(
        f"========== 업비트 TOP{TOP_N} =========="
    )

    markets = get_upbit_markets()

    markets.sort(
        key=lambda x: x["volume_24h"],
        reverse=True
    )

    rows = []

    for rank, item in enumerate(
        markets[:TOP_N],
        1
    ):

        market = item["market"]

        coin = market.replace(
            "KRW-",
            ""
        )

        price = item.get(
            "current_price"
        )

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

        rows.append(
            make_row(
                rank,
                coin,
                item["volume_24h"],
                a,
                price
            )
        )

    latest_upbit_data = rows
    latest_upbit_update_time = kst()

    log.info(
        f"업비트 완료 / "
        f"매수 "
        f"{sum(is_upbit_buy_candidate(x) for x in rows)}개 / "
        f"돌파 "
        f"{sum(is_roc_near_zero_candidate(x) for x in rows)}개 / "
        f"진행 "
        f"{sum(is_roc_progress_candidate(x) for x in rows)}개"
    )


# =========================================================
# OKX
# =========================================================

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
            ).endswith(
                "-USDT-SWAP"
            )
            and x.get(
                "state"
            ) == "live"
        ]

    except Exception:
        return []


def get_okx_volume(inst, usdt):

    df = get_okx_ohlcv(
        inst,
        "1H",
        VOLUME_HOURS
    )

    if df is None or df.empty:
        return None

    try:

        volume = float(
            pd.to_numeric(
                df.volCcyQuote,
                errors="coerce"
            ).sum()
        )

        return volume * float(usdt)

    except Exception:
        return None


def update_okx(usdt):

    global latest_okx_data
    global latest_okx_update_time

    if not usdt or usdt <= 0:
        return False

    symbols = get_okx_symbols()

    if not symbols:
        return False

    upbit_set = {
        x.replace(
            "KRW-",
            ""
        )
        for x in latest_upbit_markets
    }

    volumes = {}

    for symbol in symbols:

        v = get_okx_volume(
            symbol,
            usdt
        )

        if v and v > 0:
            volumes[symbol] = v

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

        try:

            price = get_okx_current_price(
                symbol
            )

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

            price = None
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
    latest_okx_update_time = kst()

    log.info(
        f"OKX 완료 / "
        f"매수 "
        f"{sum(is_okx_long_candidate(x) for x in rows)}개 / "
        f"돌파 "
        f"{sum(is_roc_near_zero_candidate(x) for x in rows)}개 / "
        f"진행 "
        f"{sum(is_roc_progress_candidate(x) for x in rows)}개 / "
        f"하락 "
        f"{sum(is_roc_short_near_zero_candidate(x) for x in rows)}개 / "
        f"숏 "
        f"{sum(is_okx_short_candidate(x) for x in rows)}개 / "
        f"숏진행 "
        f"{sum(is_roc_short_progress_candidate(x) for x in rows)}개"
    )

    return True


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    global latest_usdt_krw
    global latest_upbit_data
    global latest_okx_data

    if not update_lock.acquire(False):

        log.warning(
            "이전 조회 진행 중 → 건너뜀"
        )

        return

    try:

        log.info(
            f"========== 전체 조회 "
            f"{kst()} =========="
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

                usdt = (
                    get_usdt_krw()
                    or latest_usdt_krw
                )

                if usdt > 0:

                    latest_usdt_krw = usdt

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
# HTML 공통
# =========================================================

def roc_html(r):

    if not r or r.get("roc10") is None:

        return """
        <div class="roc-cell">
            <div class="roc-title">ROC10(0)</div>
            <div class="roc-value roc-zero">-</div>
        </div>
        """

    value = float(
        r["roc10"]
    )

    previous = float(
        r["roc10_previous"]
    )

    count = (
        r["roc10_count"]
        if value > 0
        else r["roc10_negative_count"]
        if value < 0
        else 0
    )

    cls = (
        "roc-positive"
        if value > 0
        else "roc-negative"
        if value < 0
        else "roc-zero"
    )

    if (
        previous <= 0
        and value > 0
    ):

        cross = (
            '<span class="roc-cross-up">'
            '↑0'
            '</span>'
        )

    elif (
        previous >= 0
        and value < 0
    ):

        cross = (
            '<span class="roc-cross-down">'
            '↓0'
            '</span>'
        )

    else:

        cross = (
            '<span class="roc-no-cross">'
            '—'
            '</span>'
        )

    return f"""
    <div class="roc-cell">
        <div class="roc-title">
            ROC10({count})
        </div>

        <div class="roc-value {cls}">
            {value:+.3f}% {cross}
        </div>
    </div>
    """


def signal_html(row):

    if not row:
        return '<div class="buy-none">-</div>'

    signals = [
        (
            "qualified",
            "buy-candidate",
            "🟢 매수 ①"
        ),
        (
            "progress_qualified",
            "progress-candidate",
            f'🚀 진행 '
            f'{row.get("roc", {}).get("roc10_count", 0)}'
        ),
        (
            "short_qualified",
            "short-candidate",
            "🔴 숏 ①"
        ),
        (
            "short_progress_qualified",
            "short-progress-candidate",
            f'📉 진행 '
            f'{row.get("roc", {}).get("roc10_negative_count", 0)}'
        ),
        (
            "near_zero_qualified",
            "near-candidate",
            "🔥 돌파"
        ),
        (
            "short_near_zero_qualified",
            "short-near-candidate",
            "⚠️ 하락"
        )
    ]

    for key, cls, text in signals:

        if row.get(key):

            return (
                f'<div class="buy-stage {cls}">'
                f'{text}'
                '</div>'
            )

    return '<div class="buy-none">-</div>'


def ema_html(e):

    e = e or empty_ema()

    d = e.get(
        "direction",
        "none"
    )

    count = e.get(
        "count",
        0
    )

    cls = {
        "long": "ema-long",
        "short": "ema-short"
    }.get(
        d,
        "ema-none"
    )

    icon = {
        "long": "🟢",
        "short": "🔴"
    }.get(
        d,
        "⚪"
    )

    if d == "none":
        count = 0

    return f"""
    <span class="ema1-main {cls}">
        {icon}({count})
    </span>
    """


# =========================================================
# Rows / Table
# =========================================================

def rows_html(
    data,
    focus_type=None
):
    out = []

    for x in data:

        if focus_type:

            cls = (
                f" {focus_type}-qualified"
            )

        elif x.get("qualified"):

            cls = " qualified"

        elif x.get(
            "progress_qualified"
        ):

            cls = " progress-qualified"

        elif x.get(
            "short_qualified"
        ):

            cls = " short-qualified"

        elif x.get(
            "short_progress_qualified"
        ):

            cls = (
                " short-progress-qualified"
            )

        elif x.get(
            "near_zero_qualified"
        ):

            cls = " near-qualified"

        elif x.get(
            "short_near_zero_qualified"
        ):

            cls = (
                " short-near-qualified"
            )

        else:

            cls = ""

        out.append(
            f"""
            <tr class="{cls}">

                <td class="rank">
                    {x.get("rank", "-")}
                </td>

                <td class="coin">
                    <div class="coin-name">
                        {x.get("name", "-")}
                    </div>

                    <div class="change">
                        {x.get("change", "-")}
                    </div>
                </td>

                <td class="vol">
                    {x.get("volume", "-")}
                </td>

                <td class="ema-cell">

                    <div class="ema-title">
                        EMA
                    </div>

                    <div class="ema-row">

                        <span class="tf">
                            {format_timeframe(
                                EMA_TIMEFRAME
                            )}
                        </span>

                        <span class="ema-value-wrap">
                            {ema_html(
                                x.get("ema_1h")
                            )}
                        </span>

                    </div>

                    <div class="ema-row">

                        <span class="tf">
                            {format_timeframe(
                                EMA_HIGH_TIMEFRAME
                            )}
                        </span>

                        <span class="ema-value-wrap">
                            {ema_html(
                                x.get("ema_high")
                            )}
                        </span>

                    </div>

                </td>

                <td class="roc-column">
                    {roc_html(
                        x.get("roc")
                    )}
                </td>

                <td class="close-ema10">
                    {signal_html(x)}
                </td>

            </tr>
            """
        )

    return "".join(out)


def table_html(
    data,
    focus_type=None
):

    rows = rows_html(
        data,
        focus_type
    )

    if not rows:

        rows = """
        <tr>
            <td colspan="6" class="empty">
                현재 조회 데이터 없음
            </td>
        </tr>
        """

    return f"""
    <div class="table-wrap">

        <table>

            <colgroup>
                <col class="col-rank">
                <col class="col-coin">
                <col class="col-vol">
                <col class="col-ema">
                <col class="col-roc">
                <col class="col-signal">
            </colgroup>

            <thead>

                <tr>
                    <th>#</th>
                    <th>코인</th>
                    <th>거래대금</th>
                    <th>EMA1</th>
                    <th>ROC10</th>
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
# 후보 Section 공통
# =========================================================

SECTION_INFO = {

    "buy": {
        "title": "🟢 매수",
        "class": "buy-title",
        "row": None,
        "sub": "ROC10 0선 상향돌파"
    },

    "progress": {
        "title": "🚀 진행",
        "class": "progress-title",
        "row": None,
        "sub": "ROC10 양수 유지 · ② 이상"
    },

    "near": {
        "title": "🔥 돌파",
        "class": "near-title",
        "row": None,
        "sub": "ROC10 0선 근처 상승"
    },

    "short": {
        "title": "🔴 숏",
        "class": "short-title",
        "row": None,
        "sub": "ROC10 0선 하향돌파 · ①"
    },

    "short_progress": {
        "title": "📉 진행",
        "class": "short-progress-title",
        "row": None,
        "sub": "ROC10 음수 유지 · ② 이상"
    },

    "short_near": {
        "title": "⚠️ 하락",
        "class": "short-near-title",
        "row": None,
        "sub": "ROC10 0선 근처 하락"
    }
}


def focus_section(
    data,
    update_time,
    kind,
    exchange="upbit"
):

    # -----------------------------------------------------
    # 후보 선택
    # -----------------------------------------------------

    if kind == "buy":

        candidates = [
            x for x in data
            if (
                is_upbit_buy_candidate(x)
                if exchange == "upbit"
                else is_okx_long_candidate(x)
            )
        ]

        candidates.sort(
            key=lambda x:
                float(
                    x.get(
                        "roc",
                        {}
                    ).get(
                        "roc10",
                        -999
                    )
                ),
            reverse=True
        )

    elif kind == "progress":

        candidates = [
            x for x in data
            if is_roc_progress_candidate(x)
        ]

        candidates.sort(
            key=lambda x:
                int(
                    x.get(
                        "roc",
                        {}
                    ).get(
                        "roc10_count",
                        0
                    )
                ),
            reverse=True
        )

    elif kind == "near":

        candidates = [
            x for x in data
            if is_roc_near_zero_candidate(x)
        ]

        candidates.sort(
            key=lambda x:
                float(
                    x.get(
                        "roc",
                        {}
                    ).get(
                        "roc10",
                        -999
                    )
                ),
            reverse=True
        )

        candidates = candidates[
            :ROC_FOCUS_TOP
        ]

    elif kind == "short":

        candidates = [
            x for x in data
            if is_okx_short_candidate(x)
        ]

    elif kind == "short_progress":

        candidates = [
            x for x in data
            if is_roc_short_progress_candidate(x)
        ]

        candidates.sort(
            key=lambda x:
                int(
                    x.get(
                        "roc",
                        {}
                    ).get(
                        "roc10_negative_count",
                        0
                    )
                ),
            reverse=True
        )

    elif kind == "short_near":

        candidates = [
            x for x in data
            if is_roc_short_near_zero_candidate(x)
        ]

        candidates.sort(
            key=lambda x:
                float(
                    x.get(
                        "roc",
                        {}
                    ).get(
                        "roc10",
                        999
                    )
                )
        )

        candidates = candidates[
            :ROC_FOCUS_TOP
        ]

    else:

        candidates = []

    info = SECTION_INFO[kind]

    # -----------------------------------------------------
    # Row
    # -----------------------------------------------------

    if candidates:

        rows = rows_html(
            candidates,
            kind
        )

    else:

        rows = """
        <tr>
            <td colspan="6" class="empty">
                현재 후보 없음
            </td>
        </tr>
        """

    return f"""
    <h2 class="focus-title {info['class']}">

        {info['title']}

        <small>
            {info['sub']} · {update_time} KST
        </small>

    </h2>

    <div class="table-wrap focus-{kind}-table">

        <table>

            <colgroup>
                <col class="col-rank">
                <col class="col-coin">
                <col class="col-vol">
                <col class="col-ema">
                <col class="col-roc">
                <col class="col-signal">
            </colgroup>

            <thead>

                <tr>
                    <th>#</th>
                    <th>코인</th>
                    <th>거래대금</th>
                    <th>EMA1</th>
                    <th>ROC10</th>
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
# 기존 함수명 유지
# =========================================================

def roc_near_zero_section(
    data,
    update_time,
    exchange="upbit"
):
    return focus_section(
        data,
        update_time,
        "near",
        exchange
    )


def roc_buy_section(
    data,
    update_time,
    exchange="upbit"
):
    return focus_section(
        data,
        update_time,
        "buy",
        exchange
    )


def roc_progress_section(
    data,
    update_time,
    exchange="upbit"
):
    return focus_section(
        data,
        update_time,
        "progress",
        exchange
    )


def okx_short_near_section(
    data,
    update_time
):
    return focus_section(
        data,
        update_time,
        "short_near",
        "okx"
    )


def okx_short_section(
    data,
    update_time
):
    return focus_section(
        data,
        update_time,
        "short",
        "okx"
    )


def okx_short_progress_section(
    data,
    update_time
):
    return focus_section(
        data,
        update_time,
        "short_progress",
        "okx"
    )


# =========================================================
# 전체 TOP Section
# =========================================================

def section(
    title,
    data,
    update_time
):

    return f"""
    <h2 class="top-title">
        🏆 {title} TOP{TOP_N}
        <small>{update_time} KST</small>
    </h2>

    {table_html(data)}
    """


# =========================================================
# CSS
# =========================================================

CSS = r"""
*{
    box-sizing:border-box;
    -webkit-tap-highlight-color:transparent;
}

html,
body{
    margin:0;
    padding:0;
    width:100%;
    min-width:0;
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
    font-size:10px;
    padding:6px 5px 14px;
    line-height:1.3;
}

h1{
    margin:3px 3px 7px;
    font-size:15px;
    line-height:20px;
    font-weight:800;
}

h2{
    margin:13px 3px 5px;
    font-size:12px;
    line-height:17px;
    font-weight:800;
}

h2 small{
    color:#747b85;
    font-size:7px;
    font-weight:normal;
    margin-left:4px;
    white-space:nowrap;
}

.info{
    margin:0 2px 8px;
    padding:7px 9px;
    color:#aab0b8;
    background:#15191f;
    border:1px solid #252b33;
    border-radius:9px;
    font-size:8px;
    line-height:1.45;
}

.status{
    display:flex;
    justify-content:center;
    align-items:center;
    gap:14px;
    margin-top:7px;
    padding-top:6px;
    border-top:1px solid #252a31;
    font-size:8px;
    font-weight:800;
}

.y{
    color:#42e878
}

.n{
    color:#ff5757
}

.table-wrap{
    width:100%;
    overflow:hidden;
    border-radius:9px;
    border:1px solid #282e36;
    background:#171b20;
}

table{
    width:100%;
    table-layout:fixed;
    border-collapse:collapse;
    background:#171b20;
}

/* 컬럼 고정 */
.col-rank{
    width:6%;
}

.col-coin{
    width:20%;
}

.col-vol{
    width:15%;
}

.col-ema{
    width:19%;
}

.col-roc{
    width:24%;
}

.col-signal{
    width:16%;
}

thead{
    background:#111419;
}

th{
    height:27px;
    padding:5px 2px;
    background:#111419;
    border-bottom:1px solid #2c323a;
    color:#9299a3;
    font-size:7px;
    font-weight:700;
    white-space:nowrap;
    text-align:center!important;
    vertical-align:middle;
}

td{
    height:45px;
    padding:5px 2px;
    border-bottom:1px solid #272d34;
    text-align:center!important;
    vertical-align:middle;
    overflow:hidden;
}

tbody tr:last-child td{
    border-bottom:none;
}

.rank{
    color:#858c96;
    font-size:8px;
    font-weight:600;
}

.coin{
    overflow:hidden;
    padding:3px 2px;
}

.coin-name{
    font-size:9px;
    line-height:12px;
    height:12px;
    font-weight:800;
    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;
}

.change{
    margin-top:2px;
    line-height:10px;
    height:10px;
    font-size:7px;
    font-weight:600;
    white-space:nowrap;
}

.up{
    color:#39e875;
    font-weight:800
}

.down{
    color:#ff5555;
    font-weight:800
}

.zero{
    color:#8c929a
}

.vol{
    padding:3px 1px!important;
    font-size:8px;
    font-weight:800;
    line-height:18px;
    height:45px;
    white-space:nowrap;
}


/* =====================================================
   EMA
   ===================================================== */

.ema-cell{
    overflow:hidden;
    padding:2px 1px!important;
}

/* EMA 제목 */
.ema-title{
    color:#858c96;
    font-size:6px;
    line-height:9px;
    font-weight:700;
    text-align:center;
}

/*
   핵심 수정:
   1H / 4H 라벨 영역을 고정
   신호 영역도 동일한 시작점 사용
*/
.ema-row{
    display:flex;
    align-items:center;
    width:100%;
    height:16px;
    min-height:16px;
    white-space:nowrap;
    overflow:hidden;
}

/* 1H / 4H 고정 영역 */
.tf{
    flex:0 0 24px;
    width:24px;
    color:#777f89;
    font-size:7px;
    font-weight:700;
    text-align:left;
    line-height:16px;
}

/* 신호 영역 */
.ema-value-wrap{
    flex:1 1 auto;
    min-width:0;
    height:16px;
    display:flex;
    align-items:center;
    justify-content:flex-start;
    overflow:hidden;
}

/* EMA 신호 */
.ema1-main{
    display:block;
    width:auto;
    min-width:0;
    font-size:8px;
    font-weight:800;
    line-height:16px;
    text-align:left;
    white-space:nowrap;
}

.ema-long{
    color:#3ee879
}

.ema-short{
    color:#ff5555
}

.ema-none{
    color:#eee
}


/* =====================================================
   ROC
   ===================================================== */

.roc-column{
    padding:2px 1px!important;
    overflow:hidden;
}

.roc-cell{
    display:flex;
    flex-direction:column;
    align-items:center;
    justify-content:center;
    width:100%;
    min-height:41px;
    line-height:1.15;
}

.roc-title{
    font-size:7px;
    line-height:11px;
    font-weight:700;
    color:#858c96;
}

.roc-value{
    font-size:8px;
    line-height:14px;
    font-weight:900;
    white-space:nowrap;
}

.roc-positive{
    color:#39e875
}

.roc-negative{
    color:#ff5555
}

.roc-zero{
    color:#9aa1aa
}

.roc-cross-up,
.roc-cross-down,
.roc-no-cross{
    font-size:7px;
    font-weight:900;
    margin-left:2px;
}

.roc-cross-up{
    color:#39e875
}

.roc-cross-down{
    color:#ff5555
}

.roc-no-cross{
    color:#6f7680
}


/* =====================================================
   신호
   ===================================================== */

.close-ema10{
    text-align:center!important;
    vertical-align:middle!important;
    white-space:nowrap;
    font-size:8px;
    font-weight:800;
    overflow:hidden;
}

.buy-stage{
    width:100%;
    text-align:center;
    font-size:8px;
    font-weight:900;
    line-height:18px;
    white-space:nowrap;
}

.buy-candidate{
    color:#39e875
}

.short-candidate{
    color:#ff5555
}

.near-candidate{
    color:#ff9f43
}

.progress-candidate{
    color:#4cc9ff
}

.short-near-candidate{
    color:#ffb347
}

.short-progress-candidate{
    color:#ff6666
}

.buy-none{
    color:#686f78;
    font-size:8px;
    font-weight:700;
    text-align:center;
}


/* =====================================================
   행
   ===================================================== */

.qualified{
    background:rgba(57,232,117,.055);
}

.short-qualified{
    background:rgba(255,85,85,.055);
}

.near-qualified{
    background:rgba(255,159,67,.055);
}

.progress-qualified{
    background:rgba(76,201,255,.055);
}

.short-near-qualified{
    background:rgba(255,159,67,.055);
}

.short-progress-qualified{
    background:rgba(255,85,85,.035);
}


/* =====================================================
   제목
   ===================================================== */

.focus-title{
    margin-top:12px;
    margin-bottom:5px;
    padding-left:3px;
}

.top-title{
    margin-top:12px;
    margin-bottom:5px;
    padding-left:3px;
}

.buy-title{
    color:#39e875
}

.short-title{
    color:#ff5555
}

.near-title{
    color:#ff9f43
}

.progress-title{
    color:#4cc9ff
}

.short-near-title{
    color:#ffb347
}

.short-progress-title{
    color:#ff6666
}

.buy-focus-table{
    border:1px solid #303740;
}

.focus-near-table{
    border:1px solid #4b3925;
}

.focus-progress-table{
    border:1px solid #254457;
}

.focus-short-near-table{
    border:1px solid #4b3925;
}

.focus-short-progress-table{
    border:1px solid #4a2727;
}

.empty{
    color:#555d67;
    padding:14px 5px!important;
    font-size:8px;
    height:48px;
}


/* =====================================================
   모바일
   ===================================================== */

@media(max-width:600px){

    body{
        padding:5px 4px 14px;
    }

    h1{
        font-size:15px;
    }

    h2{
        margin:12px 3px 5px;
        font-size:11px;
    }

    h2 small{
        display:block;
        margin-left:0;
        margin-top:1px;
        font-size:6px;
        line-height:9px;
    }

    .info{
        padding:7px 8px;
        font-size:7px;
    }

    .status{
        gap:12px;
        font-size:7px;
    }

    th{
        height:27px;
        padding:5px 1px;
        font-size:6px;
    }

    td{
        height:45px;
        padding:3px 1px;
    }

    .rank{
        font-size:7px
    }

    .coin{
        padding:3px 1px
    }

    .coin-name{
        font-size:8px;
        line-height:12px;
    }

    .change{
        font-size:6px;
        line-height:9px;
        height:9px;
    }

    .vol{
        font-size:7px;
    }

    .ema-cell{
        padding:2px 0!important;
    }

    .ema-title{
        font-size:5.5px;
    }

    /* 정렬 유지 */
    .tf{
        flex:0 0 21px;
        width:21px;
        font-size:6px;
        text-align:left;
    }

    .ema-value-wrap{
        justify-content:flex-start;
    }

    .ema1-main{
        font-size:7px;
        text-align:left;
    }

    .roc-title{
        font-size:5.5px;
    }

    .roc-value{
        font-size:6.5px;
    }

    .roc-cross-up,
    .roc-cross-down,
    .roc-no-cross{
        font-size:5.5px;
    }

    .close-ema10,
    .buy-stage,
    .buy-none{
        font-size:7px;
    }

    .empty{
        font-size:7px;
    }
}


/* =====================================================
   작은 화면
   ===================================================== */

@media(max-width:380px){

    body{
        padding:4px 3px 12px;
    }

    h1{
        font-size:14px
    }

    h2{
        font-size:10px
    }

    .info{
        font-size:6.5px
    }

    th{
        height:25px;
        font-size:5px;
    }

    td{
        height:45px
    }

    .coin-name{
        font-size:7px
    }

    .change{
        font-size:5.5px
    }

    .vol{
        font-size:6px
    }

    .tf{
        flex:0 0 19px;
        width:19px;
        font-size:5.5px;
    }

    .ema-title{
        font-size:5px
    }

    .ema1-main{
        font-size:6px;
    }

    .roc-title{
        font-size:5px
    }

    .roc-value{
        font-size:5.5px
    }

    .roc-cross-up,
    .roc-cross-down,
    .roc-no-cross{
        font-size:5px;
    }

    .buy-stage,
    .buy-none{
        font-size:6px;
    }
}


/* =====================================================
   PC
   ===================================================== */

@media(min-width:601px){

    body{
        max-width:900px;
        margin:0 auto;
        padding:8px;
    }

    th{
        font-size:8px
    }

    td{
        height:48px
    }

    .coin-name{
        font-size:10px
    }

    .change{
        font-size:8px
    }

    .vol{
        font-size:9px
    }

    .ema1-main{
        font-size:9px
    }

    .roc-title{
        font-size:7px
    }

    .roc-value{
        font-size:8px
    }

    .buy-stage{
        font-size:9px
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

    timeframe_label = format_timeframe(
        EMA_TIMEFRAME
    )

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

    </div>
    """

    sections = ""

    # -----------------------------------------------------
    # Upbit
    # -----------------------------------------------------

    if USE_UPBIT == "Y":

        sections += roc_buy_section(
            latest_upbit_data,
            latest_upbit_update_time,
            "upbit"
        )

        sections += roc_progress_section(
            latest_upbit_data,
            latest_upbit_update_time,
            "upbit"
        )

    # -----------------------------------------------------
    # OKX
    # -----------------------------------------------------

    if USE_OKX == "Y":

        sections += roc_buy_section(
            latest_okx_data,
            latest_okx_update_time,
            "okx"
        )

        sections += roc_progress_section(
            latest_okx_data,
            latest_okx_update_time,
            "okx"
        )

        sections += okx_short_near_section(
            latest_okx_data,
            latest_okx_update_time
        )

        sections += okx_short_section(
            latest_okx_data,
            latest_okx_update_time
        )

        sections += okx_short_progress_section(
            latest_okx_data,
            latest_okx_update_time
        )

    # -----------------------------------------------------
    # 전체 TOP
    # -----------------------------------------------------

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
                maximum-scale=1
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
            {timeframe_label} EMA1 · ROC10
        </title>

        <style>
            {CSS}
        </style>

    </head>

    <body>

        <h1>
            📊 TRADING SIGNAL CENTER
        </h1>

        <div class="info">

            {timeframe_label}
            EMA30·60·120 + ROC10

            <br>

            🟢 매수 = 0선 상향돌파 ①

            <br>

            🚀 진행 = 양수 유지 ②+

            <br>

            ⚠️ 하락 = 0선 근처 하락

            <br>

            🔴 숏 = 0선 하향돌파 ①

            <br>

            📉 진행 = 음수 유지 ②+

            <br>

            ROC10 = 현재가 기준

            {status}

        </div>

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

    timeframe_label = format_timeframe(
        EMA_TIMEFRAME
    )

    okx_bar = get_okx_bar(
        EMA_TIMEFRAME
    )

    log.info(
        "========================================"
    )

    log.info(
        f"{timeframe_label} EMA1 + ROC10 시작"
    )

    log.info(
        f"업비트={USE_UPBIT} / OKX={USE_OKX}"
    )

    log.info(
        f"TOP={TOP_N} / "
        f"UPDATE={UPDATE_MINUTES}분"
    )

    log.info(
        f"EMA={timeframe_label} / "
        f"EMA30-60-120"
    )

    log.info(
        f"EMA count <= {EMA1_MAX_COUNT}"
    )

    log.info(
        "롱: 돌파 → 매수① → 진행②+"
    )

    log.info(
        "숏: 하락 → 숏① → 진행②+"
    )

    log.info(
        f"롱 돌파 구간 = "
        f"{ROC_NEAR_ZERO:.2f}% ~ 0%"
    )

    log.info(
        f"숏 하락 구간 = "
        f"0% ~ +{ROC_SHORT_NEAR_ZERO:.2f}%"
    )

    log.info(
        f"돌파/하락 TOP = "
        f"{ROC_FOCUS_TOP}개"
    )

    log.info(
        f"OKX bar={okx_bar}"
    )

    log.info(
        f"표시용 HIGH EMA="
        f"{format_timeframe(EMA_HIGH_TIMEFRAME)}"
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
