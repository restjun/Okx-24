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


# =========================================================
# ★ EMA2 설정
# =========================================================

EMA2_LOOKBACK = 15


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
# 종료 표시 관리
# =========================================================

air_ended_displayed = set()

air_ended_displayed_lock = threading.Lock()


# =========================================================
# 공통
# =========================================================

def kst():

    return datetime.now(KST).strftime(
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

        last_request_time = time.monotonic()


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

            except Exception:

                continue

            if volume > 0:

                result.append({
                    "market": market,
                    "volume_24h": volume
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

    url = (
        "https://api.upbit.com/v1/candles/minutes/"
        f"{unit}"
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

        if unit == 60:

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

        now = datetime.now(KST)

        # =================================================
        # 현재 진행 중인 캔들 제외
        # =================================================

        if unit == 60:

            current = now.replace(
                minute=0,
                second=0,
                microsecond=0
            ).replace(
                tzinfo=None
            )

        else:

            block = (
                now.hour // 4
            ) * 4

            current = now.replace(
                hour=block,
                minute=0,
                second=0,
                microsecond=0
            ).replace(
                tzinfo=None
            )

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
            f"업비트 {unit}분 오류 {market}: {e}"
        )

        return None


def get_upbit_1h(
    market,
    count=200,
    to=None
):

    # 기존 함수명 유지
    # 실제 분석은 4H

    return get_upbit_candle(
        market,
        240,
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

        now = datetime.now(KST)

        if bar == "1H":

            current = now.replace(
                minute=0,
                second=0,
                microsecond=0
            ).replace(
                tzinfo=None
            )

        else:

            block = (
                now.hour // 4
            ) * 4

            current = now.replace(
                hour=block,
                minute=0,
                second=0,
                microsecond=0
            ).replace(
                tzinfo=None
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

    for _ in range(
        MAX_HISTORY_CHUNKS
    ):

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

    return pd.to_numeric(
        df.c,
        errors="coerce"
    ).ewm(
        span=period,
        adjust=False,
        min_periods=1
    ).mean()


# =========================================================
# EMA1
# 4H EMA 10-30-60-120
# =========================================================

def direction(df):

    if df is None or df.empty:
        return "none"

    try:

        e10 = ema(
            df,
            10
        ).iloc[-1]

        e30 = ema(
            df,
            30
        ).iloc[-1]

        e60 = ema(
            df,
            60
        ).iloc[-1]

        e120 = ema(
            df,
            120
        ).iloc[-1]

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

    except Exception:

        pass

    return "none"


def ema_alignment_count(df):

    if df is None or df.empty:

        return {
            "direction": "none",
            "count": 0
        }

    try:

        e10 = ema(df, 10)
        e30 = ema(df, 30)
        e60 = ema(df, 60)
        e120 = ema(df, 120)

        current_direction = None
        count = 0

        for i in range(
            len(df) - 1,
            -1,
            -1
        ):

            v10 = float(
                e10.iloc[i]
            )

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
                v10 > v30
                and v30 > v60
                and v60 > v120
            ):

                candle_direction = "long"

            elif (
                v10 < v30
                and v30 < v60
                and v60 < v120
            ):

                candle_direction = "short"

            else:

                candle_direction = "none"

            if i == len(df) - 1:

                current_direction = (
                    candle_direction
                )

                if current_direction == "none":

                    return {
                        "direction": "none",
                        "count": 0
                    }

            if (
                candle_direction
                == current_direction
            ):

                count += 1

            else:

                break

        return {
            "direction":
                current_direction,
            "count":
                count
        }

    except Exception as e:

        log.error(
            f"EMA 배열 카운팅 오류: {e}"
        )

        return {
            "direction": "none",
            "count": 0
        }


def ema_display(df):

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

    return {
        "display":
            f"{icon}({count})",

        "direction":
            d,

        "count":
            count
    }


# =========================================================
# EMA2
# 4H EMA3 + 이전 15개 EMA3 고점/저점 돌파
# =========================================================

def ema2_breakout_analysis(df):

    result = {
        "state": "none",
        "count": 0,
        "start_time": None,
        "end_time": None,
        "candle_time": None,
        "display": "⚪(0)",
        "breakout": False,
        "ended": False
    }

    if (
        df is None
        or df.empty
        or len(df) < EMA2_LOOKBACK + 2
    ):

        return result

    try:

        work = (
            df.copy()
            .reset_index(drop=True)
        )

        work["e3"] = ema(
            work,
            3
        )

        work["e10"] = ema(
            work,
            10
        )

        work["e30"] = ema(
            work,
            30
        )

        work["e60"] = ema(
            work,
            60
        )

        work["e120"] = ema(
            work,
            120
        )

        work["hi15"] = (
            work["e3"]
            .shift(1)
            .rolling(
                window=EMA2_LOOKBACK,
                min_periods=EMA2_LOOKBACK
            )
            .max()
        )

        work["lo15"] = (
            work["e3"]
            .shift(1)
            .rolling(
                window=EMA2_LOOKBACK,
                min_periods=EMA2_LOOKBACK
            )
            .min()
        )

        bull = (
            (work["e10"] > work["e30"])
            &
            (work["e30"] > work["e60"])
            &
            (work["e60"] > work["e120"])
        )

        bear = (
            (work["e10"] < work["e30"])
            &
            (work["e30"] < work["e60"])
            &
            (work["e60"] < work["e120"])
        )

        long_cross = (
            (work["e3"] > work["hi15"])
            &
            (
                work["e3"].shift(1)
                <=
                work["hi15"].shift(1)
            )
        )

        short_cross = (
            (work["e3"] < work["lo15"])
            &
            (
                work["e3"].shift(1)
                >=
                work["lo15"].shift(1)
            )
        )

        work["long_signal"] = (
            bull
            &
            long_cross
        ).fillna(False)

        work["short_signal"] = (
            bear
            &
            short_cross
        ).fillna(False)

        signal_indices = []

        for i in range(
            len(work)
        ):

            if bool(
                work.loc[
                    i,
                    "long_signal"
                ]
            ):

                signal_indices.append(
                    (
                        i,
                        "long"
                    )
                )

            if bool(
                work.loc[
                    i,
                    "short_signal"
                ]
            ):

                signal_indices.append(
                    (
                        i,
                        "short"
                    )
                )

        if not signal_indices:

            result["candle_time"] = (
                work["datetime"].iloc[-1]
            )

            return result

        start_index, start_state = (
            signal_indices[-1]
        )

        start_time = (
            work["datetime"].iloc[
                start_index
            ]
        )

        current_index = (
            len(work) - 1
        )

        end_index = None
        end_time = None

        if start_index < current_index:

            for i in range(
                start_index + 1,
                len(work)
            ):

                current_close = float(
                    work.loc[
                        i,
                        "c"
                    ]
                )

                previous_low = float(
                    work.loc[
                        i - 1,
                        "l"
                    ]
                )

                previous_high = float(
                    work.loc[
                        i - 1,
                        "h"
                    ]
                )

                if (
                    start_state == "long"
                    and
                    current_close
                    < previous_low
                ):

                    end_index = i

                    end_time = (
                        work["datetime"].iloc[
                            i
                        ]
                    )

                    break

                if (
                    start_state == "short"
                    and
                    current_close
                    > previous_high
                ):

                    end_index = i

                    end_time = (
                        work["datetime"].iloc[
                            i
                        ]
                    )

                    break

        if end_index is not None:

            result["state"] = (
                start_state
            )

            result["count"] = (
                end_index
                - start_index
            )

            result["start_time"] = (
                start_time
            )

            result["end_time"] = (
                end_time
            )

            result["candle_time"] = (
                work["datetime"].iloc[-1]
            )

            result["breakout"] = True
            result["ended"] = True

            if start_state == "long":

                result["display"] = (
                    f"🟢({result['count']})"
                )

            else:

                result["display"] = (
                    f"🔴({result['count']})"
                )

            return result

        current_count = (
            current_index
            - start_index
            + 1
        )

        result["state"] = (
            start_state
        )

        result["count"] = (
            current_count
        )

        result["start_time"] = (
            start_time
        )

        result["candle_time"] = (
            work["datetime"].iloc[-1]
        )

        result["breakout"] = True
        result["ended"] = False

        if start_state == "long":

            result["display"] = (
                f"🟢({current_count})"
            )

        else:

            result["display"] = (
                f"🔴({current_count})"
            )

        return result

    except Exception as e:

        log.error(
            f"EMA2 돌파 분석 오류: {e}"
        )

        return result


# =========================================================
# 비행기 경고
# =========================================================

def get_air_warning(
    ema2_result
):

    if not ema2_result:
        return False

    if ema2_result.get(
        "ended",
        False
    ):
        return False

    if not ema2_result.get(
        "breakout",
        False
    ):
        return False

    state = ema2_result.get(
        "state",
        "none"
    )

    return state in (
        "long",
        "short"
    )


# =========================================================
# 비행기 상태
# =========================================================

def update_air_counter(
    market,
    ema2_result
):

    if not ema2_result:

        return {
            "active": False,
            "ended": False,
            "direction": None,
            "count": 0
        }

    state = ema2_result.get(
        "state",
        "none"
    )

    count = int(
        ema2_result.get(
            "count",
            0
        )
    )

    ended = bool(
        ema2_result.get(
            "ended",
            False
        )
    )

    breakout = bool(
        ema2_result.get(
            "breakout",
            False
        )
    )

    if ended:

        return {
            "active": False,
            "ended": True,
            "direction": state,
            "count": count
        }

    if (
        breakout
        and
        state in (
            "long",
            "short"
        )
    ):

        return {
            "active": True,
            "ended": False,
            "direction": state,
            "count": count
        }

    return {
        "active": False,
        "ended": False,
        "direction": None,
        "count": 0
    }


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

        return [
            (
                current - previous
            )
            / previous
            * 100
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
            x.dropna(
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

        current = float(
            daily.iloc[-1]
        )

        previous = float(
            daily.iloc[-2]
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


def format_change(x):

    if x is None:
        return "-"

    try:

        value = float(
            x[0]
            if isinstance(
                x,
                (list, tuple)
            )
            else x
        )

        if value > 0:

            return (
                '<span class="up">'
                f'▲ +{value:.2f}%'
                '</span>'
            )

        if value < 0:

            return (
                '<span class="down">'
                f'▼ {value:.2f}%'
                '</span>'
            )

        return (
            '<span class="zero">'
            '0.00%'
            '</span>'
        )

    except Exception:

        return "-"


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
# 분석
# =========================================================

def empty_analysis():

    e = {
        "display": "⚪(0)",
        "direction": "none",
        "count": 0
    }

    return {

        "ema_1h":
            e.copy(),

        "ema3_10_cross_1h": {

            "state":
                "none",

            "count":
                0,

            "final_count":
                0,

            "display":
                "⚪(0)",

            "candle_time":
                None,

            "start_time":
                None,

            "end_time":
                None,

            "breakout":
                False,

            "ended":
                False
        },

        "changes":
            None,

        "air_warning":
            False,

        "air_direction":
            None,

        "air_count":
            0,

        "air_active":
            False,

        "air_ended":
            False,

        "qualified":
            False,

        "direction_1h":
            "none",

        "df1h":
            None
    }


def analyze(
    market,
    okx=False
):

    # =================================================
    # 모든 EMA 분석 = 4H
    # =================================================

    if okx:

        df1 = history_okx(
            market,
            "4H"
        )

    else:

        df1 = history_upbit(
            market,
            240
        )

    if (
        df1 is None
        or df1.empty
    ):

        return None

    e1 = ema_display(
        df1
    )

    ema2_result = (
        ema2_breakout_analysis(
            df1
        )
    )

    air = update_air_counter(
        market,
        ema2_result
    )

    changes = (
        daily_changes(df1)
        if okx
        else daily_change_upbit(market)
    )

    air_ended = False

    if ema2_result.get(
        "ended",
        False
    ):

        end_time = ema2_result.get(
            "end_time"
        )

        if end_time is not None:

            key = (
                market,
                str(end_time)
            )

            with air_ended_displayed_lock:

                if key not in air_ended_displayed:

                    air_ended_displayed.add(
                        key
                    )

                    air_ended = True

    return {

        "ema_1h":
            e1,

        "ema3_10_cross_1h":
            ema2_result,

        "changes":
            changes,

        "air_warning":
            air["active"],

        "air_direction":
            air["direction"],

        "air_count":
            air["count"],

        "air_active":
            air["active"],

        "air_ended":
            air_ended,

        "qualified":
            air["active"],

        "direction_1h":
            e1["direction"],

        "df1h":
            df1
    }


# =========================================================
# 행 생성
# =========================================================

def make_row(
    rank,
    name,
    volume,
    analysis
):

    a = (
        analysis
        or empty_analysis()
    )

    return {

        "rank":
            rank,

        "name":
            name,

        "change":
            format_change(
                a["changes"]
            ),

        "volume":
            format_volume(
                volume
            ),

        "ema_1h":
            a["ema_1h"],

        "ema3_10_cross_1h":
            a["ema3_10_cross_1h"],

        "air_warning":
            a["air_warning"],

        "air_direction":
            a["air_direction"],

        "air_count":
            a["air_count"],

        "air_active":
            a.get(
                "air_active",
                False
            ),

        "air_ended":
            a.get(
                "air_ended",
                False
            ),

        "qualified":
            a["qualified"]
    }


# =========================================================
# Upbit 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update_time

    log.info(
        f"========== 업비트 TOP{TOP_N} 시작 =========="
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

        try:

            a = analyze(
                market
            )

            rows.append(
                make_row(
                    rank,
                    coin,
                    item["volume_24h"],
                    a
                )
            )

        except Exception as e:

            log.error(
                f"업비트 상세 오류 {market}: {e}"
            )

            rows.append(
                make_row(
                    rank,
                    coin,
                    item["volume_24h"],
                    None
                )
            )

    latest_upbit_data = rows

    latest_upbit_update_time = kst()

    active_count = sum(
        x["qualified"]
        for x in rows
    )

    ended_count = sum(
        x.get(
            "air_ended",
            False
        )
        for x in rows
    )

    log.info(
        f"업비트 완료 / "
        f"EMA2 진행 {active_count}개 / "
        f"종료 ⛔️ {ended_count}개"
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


def get_okx_volume(
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

    try:

        volume = float(
            pd.to_numeric(
                df.volCcyQuote,
                errors="coerce"
            ).sum()
        )

        return (
            volume
            * float(usdt)
        )

    except Exception:

        return None


def update_okx(
    usdt
):

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

            a = analyze(
                symbol,
                True
            )

            rows.append(
                make_row(
                    rank,
                    name,
                    volumes[symbol],
                    a
                )
            )

        except Exception as e:

            log.error(
                f"OKX 상세 오류 {symbol}: {e}"
            )

            rows.append(
                make_row(
                    rank,
                    name,
                    volumes[symbol],
                    None
                )
            )

    latest_okx_data = rows

    latest_okx_update_time = kst()

    active_count = sum(
        x["qualified"]
        for x in rows
    )

    ended_count = sum(
        x.get(
            "air_ended",
            False
        )
        for x in rows
    )

    log.info(
        f"OKX 완료 / "
        f"EMA2 진행 {active_count}개 / "
        f"종료 ⛔️ {ended_count}개"
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
            f"========== 전체 조회 {kst()} =========="
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

                usdt = get_usdt_krw()

                if usdt:

                    latest_usdt_krw = usdt

                else:

                    usdt = latest_usdt_krw

                if usdt > 0:

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
# 경고 HTML
# =========================================================

def warning_html(
    air_warning,
    air_direction=None,
    air_count=0,
    air_ended=False
):

    if air_ended:

        return (
            '<div class="air-box ended">'
            '<div class="air-end">'
            '⛔️'
            '</div>'
            '</div>'
        )

    if not air_warning:

        return "-"

    try:

        count = int(
            air_count
        )

    except Exception:

        count = 1

    if count <= 1:

        return (
            '<div class="air-box">'
            '<div class="air-main">'
            '<span class="air-icon">'
            '🛩✈️'
            '</span>'
            '</div>'
            '</div>'
        )

    return (
        '<div class="air-box">'
        '<div class="air-main">'
        '<span class="air-icon">'
        '✈️'
        '</span>'
        '</div>'
        '</div>'
    )


def ema_html(e):

    if not e:

        return "⚪(0)"

    direction_value = e.get(
        "direction",
        "none"
    )

    display = e.get(
        "display",
        "⚪(0)"
    )

    cls = {
        "long": "ema-long",
        "short": "ema-short"
    }.get(
        direction_value,
        "ema-none"
    )

    return (
        f'<span class="ema-value {cls}">'
        f'{display}'
        '</span>'
    )


# =========================================================
# EMA2 표시
# =========================================================

def ema3_10_cross_html(
    data
):

    if not data:

        return "-"

    state = data.get(
        "state",
        "none"
    )

    try:

        count = int(
            data.get(
                "count",
                0
            )
        )

    except Exception:

        count = 0

    if state == "long":

        return (
            '<div class="ema10-box">'
            '<div class="ema10-long">'
            f'🟢({count})'
            '</div>'
            '</div>'
        )

    if state == "short":

        return (
            '<div class="ema10-box">'
            '<div class="ema10-short">'
            f'🔴({count})'
            '</div>'
            '</div>'
        )

    return (
        '<div class="ema10-none">'
        '⚪(0)'
        '</div>'
    )


# =========================================================
# Rows
# =========================================================

def rows_html(
    data
):

    out = []

    for x in data:

        cls = (
            " qualified"
            if x.get(
                "qualified",
                False
            )
            else ""
        )

        ema2_data = x.get(
            "ema3_10_cross_1h",
            {}
        )

        visual_air_count = int(
            ema2_data.get(
                "count",
                x.get(
                    "air_count",
                    0
                )
            )
        )

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

                    <div class="ema-row">

                        <span class="tf">
                            4H
                        </span>

                        <span class="ema-value-wrap">

                            {ema_html(
                                x.get(
                                    "ema_1h",
                                    {}
                                )
                            )}

                        </span>

                    </div>

                </td>

                <td class="close-ema10">

                    {ema3_10_cross_html(
                        ema2_data
                    )}

                </td>

                <td class="warning">

                    {warning_html(
                        x.get(
                            "air_warning",
                            False
                        ),
                        x.get(
                            "air_direction"
                        ),
                        visual_air_count,
                        x.get(
                            "air_ended",
                            False
                        )
                    )}

                </td>

            </tr>
            """
        )

    return "".join(
        out
    )


# =========================================================
# Table
# =========================================================

def table_html(
    data
):

    rows = rows_html(
        data
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

            <thead>

                <tr>
                    <th>#</th>
                    <th>코인</th>
                    <th>거래대금</th>
                    <th>EMA1</th>
                    <th>EMA2</th>
                    <th>경고</th>
                </tr>

            </thead>

            <tbody>

                {rows}

            </tbody>

        </table>

    </div>
    """


# =========================================================
# Section
# =========================================================

def section(
    title,
    data,
    update_time
):

    return f"""
    <h2>

        🏆 {title} TOP{TOP_N}

        <small>
            {update_time} KST
        </small>

    </h2>

    {table_html(data)}
    """


# =========================================================
# 실제 화면용 EMA2 카운트
# =========================================================

def get_visual_air_count(
    x
):

    ema2_data = x.get(
        "ema3_10_cross_1h",
        {}
    )

    try:

        return int(
            ema2_data.get(
                "count",
                x.get(
                    "air_count",
                    0
                )
            )
        )

    except Exception:

        return int(
            x.get(
                "air_count",
                0
            )
        )


# =========================================================
# 🚀 상승 체크
# =========================================================

def rising_focus_section(
    data,
    update_time
):

    rising_data = []

    for x in data:

        if x.get(
            "air_ended",
            False
        ):

            continue

        if not x.get(
            "air_active",
            False
        ):

            continue

        visual_air_count = (
            get_visual_air_count(x)
        )

        if visual_air_count == 1:

            rising_data.append(
                x
            )

    if not rising_data:

        rows = """
        <tr>
            <td colspan="6" class="empty">
                현재 상승 돌파 코인 없음
            </td>
        </tr>
        """

        table = f"""
        <div class="table-wrap focus-table rising-table">

            <table>

                <thead>

                    <tr>
                        <th>#</th>
                        <th>코인</th>
                        <th>거래대금</th>
                        <th>EMA1</th>
                        <th>EMA2</th>
                        <th>경고</th>
                    </tr>

                </thead>

                <tbody>

                    {rows}

                </tbody>

            </table>

        </div>
        """

    else:

        table = (
            '<div class="focus-table rising-table">'
            +
            table_html(
                rising_data
            ).replace(
                '<div class="table-wrap">',
                '',
                1
            )
            +
            '</div>'
        )

    return f"""
    <h2 class="focus-title rising-title">

        🚀 상승 체크 [EMA3 15개 돌파 = ①]

        <small>
            {update_time} KST
        </small>

    </h2>

    {table}
    """


# =========================================================
# 상승 해지 리스트
# ★ 함수는 유지
# ★ 화면에는 표시하지 않음
# =========================================================

def ended_focus_section(
    data,
    update_time
):

    ended_data = []

    for x in data:

        if x.get(
            "air_ended",
            False
        ):

            ended_data.append(
                x
            )

    if not ended_data:

        rows = """
        <tr>
            <td colspan="6" class="empty">
                현재 상승 해지 코인 없음
            </td>
        </tr>
        """

        table = f"""
        <div class="table-wrap focus-table ended-table">

            <table>

                <thead>

                    <tr>
                        <th>#</th>
                        <th>코인</th>
                        <th>거래대금</th>
                        <th>EMA1</th>
                        <th>EMA2</th>
                        <th>경고</th>
                    </tr>

                </thead>

                <tbody>

                    {rows}

                </tbody>

            </table>

        </div>
        """

    else:

        table = (
            '<div class="focus-table ended-table">'
            +
            table_html(
                ended_data
            ).replace(
                '<div class="table-wrap">',
                '',
                1
            )
            +
            '</div>'
        )

    return f"""
    <h2 class="focus-title ended-title">

        ⛔️ 상승 해지 리스트

        <small>
            {update_time} KST
        </small>

    </h2>

    {table}
    """


# =========================================================
# 🚨 상승 진행 리스트
# =========================================================

def warning_focus_section(
    data,
    update_time
):

    warning_data = []

    for x in data:

        if x.get(
            "air_ended",
            False
        ):

            continue

        if not x.get(
            "air_active",
            False
        ):

            continue

        visual_air_count = (
            get_visual_air_count(x)
        )

        if visual_air_count >= 2:

            warning_data.append(
                x
            )

    if not warning_data:

        rows = """
        <tr>
            <td colspan="6" class="empty">
                현재 경고 진행 코인 없음
            </td>
        </tr>
        """

        table = f"""
        <div class="table-wrap focus-table warning-table">

            <table>

                <thead>

                    <tr>
                        <th>#</th>
                        <th>코인</th>
                        <th>거래대금</th>
                        <th>EMA1</th>
                        <th>EMA2</th>
                        <th>경고</th>
                    </tr>

                </thead>

                <tbody>

                    {rows}

                </tbody>

            </table>

        </div>
        """

    else:

        table = (
            '<div class="focus-table warning-table">'
            +
            table_html(
                warning_data
            ).replace(
                '<div class="table-wrap">',
                '',
                1
            )
            +
            '</div>'
        )

    return f"""
    <h2 class="focus-title warning-title">

        🚨 상승 진행 리스트

        <small>
            {update_time} KST
        </small>

    </h2>

    {table}
    """


# =========================================================
# CSS
# ★ 모바일 가독성 개선
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
    min-width:0;
    overflow-x:hidden;
}

body{
    background:#0d1014;
    color:#eeeeee;

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

/* =========================================================
   제목
   ========================================================= */

h1{
    margin:3px 3px 7px;

    font-size:15px;
    line-height:20px;

    font-weight:800;

    color:#f5f5f5;
}

h2{
    margin:13px 3px 5px;

    font-size:12px;
    line-height:17px;

    font-weight:800;

    color:#eeeeee;
}

h2 small{
    color:#747b85;

    font-size:7px;
    font-weight:normal;

    margin-left:4px;

    white-space:nowrap;
}

/* =========================================================
   설명 카드
   ========================================================= */

.info{
    margin:0 2px 8px;

    padding:8px 9px;

    color:#aab0b8;

    background:#15191f;

    border:1px solid #252b33;

    border-radius:9px;

    font-size:8px;

    line-height:1.55;

    box-shadow:
        0 2px 8px rgba(0,0,0,.18);
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
    color:#42e878;
}

.n{
    color:#ff5757;
}

/* =========================================================
   테이블 외곽
   ========================================================= */

.table-wrap{
    width:100%;

    overflow:hidden;

    border-radius:9px;

    border:1px solid #282e36;

    background:#171b20;

    box-shadow:
        0 2px 8px rgba(0,0,0,.18);
}

/* =========================================================
   테이블
   ========================================================= */

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
    height:27px;

    padding:5px 2px;

    background:#111419;

    border-bottom:1px solid #2c323a;

    color:#9299a3;

    font-size:7px;

    line-height:10px;

    font-weight:700;

    white-space:nowrap;

    text-align:center !important;

    vertical-align:middle;
}

td{
    height:38px;

    padding:5px 2px;

    border-bottom:1px solid #272d34;

    text-align:center !important;

    vertical-align:middle;

    overflow:hidden;
}

tbody tr:last-child td{
    border-bottom:none;
}

/* =========================================================
   열 너비
   ========================================================= */

th:nth-child(1),
td:nth-child(1){
    width:7%;
}

th:nth-child(2),
td:nth-child(2){
    width:22%;
}

th:nth-child(3),
td:nth-child(3){
    width:17%;
}

th:nth-child(4),
td:nth-child(4){
    width:22%;
}

th:nth-child(5),
td:nth-child(5){
    width:16%;
}

th:nth-child(6),
td:nth-child(6){
    width:16%;
}

/* =========================================================
   순위
   ========================================================= */

.rank{
    color:#858c96;

    font-size:8px;

    font-weight:600;
}

/* =========================================================
   코인
   ========================================================= */

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

    font-weight:800;
}

.down{
    color:#ff5555;

    font-weight:800;
}

.zero{
    color:#8c929a;
}

/* =========================================================
   거래대금
   ========================================================= */

.vol{
    padding:3px 1px !important;

    font-size:8px;

    font-weight:800;

    line-height:18px;

    height:38px;

    white-space:nowrap;
}

/* =========================================================
   EMA1
   ========================================================= */

.ema-cell{
    overflow:hidden;

    padding:3px 1px !important;
}

.ema-row{
    display:flex;

    align-items:center;

    justify-content:center;

    width:100%;

    height:20px;

    line-height:20px;

    white-space:nowrap;

    overflow:hidden;

    font-size:8px;

    font-weight:800;
}

.tf{
    flex:0 0 21px;

    width:21px;

    color:#777f89;

    font-size:7px;

    font-weight:700;

    text-align:center;
}

.ema-value-wrap{
    flex:1;

    min-width:0;

    display:flex;

    align-items:center;

    justify-content:center;

    overflow:hidden;
}

.ema-value{
    display:inline-block;

    width:auto;

    min-width:0;

    max-width:100%;

    text-align:center;

    white-space:nowrap;

    font-size:8px;

    font-weight:800;

    line-height:20px;
}

.ema-long{
    color:#3ee879;
}

.ema-short{
    color:#ff5555;
}

.ema-none{
    color:#eeeeee;
}

/* =========================================================
   EMA2
   ========================================================= */

.close-ema10{
    text-align:center !important;

    vertical-align:middle !important;

    white-space:nowrap;

    font-size:8px;

    font-weight:800;

    overflow:hidden;
}

.ema10-box{
    width:100%;

    display:flex;

    flex-direction:column;

    align-items:center;

    justify-content:center;

    text-align:center;

    white-space:nowrap;

    line-height:14px;
}

.ema10-long{
    color:#39e875;

    font-size:8px;

    font-weight:800;

    line-height:14px;

    text-align:center;
}

.ema10-short{
    color:#ff5555;

    font-size:8px;

    font-weight:800;

    line-height:14px;

    text-align:center;
}

.ema10-none{
    color:#686f78;

    font-size:8px;

    line-height:14px;

    text-align:center;
}

/* =========================================================
   경고
   ========================================================= */

.warning{
    text-align:center !important;

    vertical-align:middle !important;

    white-space:nowrap;

    padding:2px !important;
}

.air-box{
    width:100%;

    min-height:28px;

    display:flex;

    align-items:center;

    justify-content:center;

    text-align:center;
}

.air-main{
    display:flex;

    align-items:center;

    justify-content:center;

    width:100%;
}

.air-icon{
    font-size:13px;

    font-weight:bold;

    line-height:18px;

    display:inline-block;
}

.air-end{
    font-size:12px;

    font-weight:bold;

    line-height:18px;

    color:#ff5555;
}

/* =========================================================
   조건 만족 행
   ========================================================= */

.qualified{
    background:rgba(255,255,255,.045);
}

/* =========================================================
   포커스 제목
   ========================================================= */

.focus-title{
    margin-top:12px;

    margin-bottom:5px;

    padding-left:3px;
}

.rising-title{
    color:#39e875;
}

.ended-title{
    color:#ff5555;
}

.warning-title{
    color:#39e875;
}

/* =========================================================
   포커스 테이블
   ========================================================= */

.focus-table{
    border-radius:9px;
}

.rising-table{
    border:1px solid #35c96b;

    box-shadow:
        0 0 7px rgba(53,201,107,.08);
}

.ended-table{
    border:1px solid #d84646;
}

.warning-table{
    border:1px solid #303740;
}

/* =========================================================
   빈 데이터
   ========================================================= */

.empty{
    color:#555d67;

    padding:14px 5px !important;

    font-size:8px;

    height:48px;
}

/* =========================================================
   모바일
   ========================================================= */

@media(max-width:600px){

    body{
        padding:5px 4px 14px;

        font-size:10px;
    }

    h1{
        margin:3px 3px 7px;

        font-size:15px;

        line-height:20px;
    }

    h2{
        margin:12px 3px 5px;

        font-size:11px;

        line-height:16px;
    }

    h2 small{
        display:block;

        margin-left:0;

        margin-top:1px;

        font-size:6px;

        line-height:9px;
    }

    .info{
        padding:8px;

        margin-bottom:7px;

        font-size:7px;

        line-height:1.55;
    }

    .status{
        gap:12px;

        margin-top:6px;

        padding-top:5px;

        font-size:7px;
    }

    th{
        height:27px;

        padding:5px 1px;

        font-size:6px;

        line-height:9px;
    }

    td{
        height:40px;

        padding:5px 1px;
    }

    .rank{
        font-size:8px;
    }

    .coin{
        padding:3px 1px;
    }

    .coin-name{
        font-size:8px;

        line-height:12px;

        height:12px;
    }

    .change{
        margin-top:2px;

        font-size:6px;

        line-height:9px;

        height:9px;
    }

    .vol{
        padding:3px 1px !important;

        font-size:7px;

        line-height:18px;

        height:40px;
    }

    .ema-cell{
        padding:3px 0 !important;
    }

    .ema-row{
        height:20px;

        line-height:20px;

        font-size:7px;
    }

    .tf{
        flex:0 0 18px;

        width:18px;

        font-size:6px;
    }

    .ema-value{
        font-size:7px;

        line-height:20px;
    }

    .close-ema10{
        font-size:7px;
    }

    .ema10-box{
        line-height:14px;
    }

    .ema10-long,
    .ema10-short{
        font-size:7px;

        line-height:14px;
    }

    .ema10-none{
        font-size:7px;

        line-height:14px;
    }

    .air-box{
        min-height:28px;
    }

    .air-icon{
        font-size:12px;

        line-height:18px;
    }

    .air-end{
        font-size:11px;

        line-height:18px;
    }

    .empty{
        padding:13px 4px !important;

        font-size:7px;

        height:45px;
    }
}

/* =========================================================
   아주 작은 휴대폰
   ========================================================= */

@media(max-width:380px){

    body{
        padding:4px 3px 12px;
    }

    h1{
        font-size:14px;
    }

    h2{
        font-size:10px;
    }

    .info{
        font-size:6.5px;
    }

    th{
        height:25px;

        font-size:5px;
    }

    td{
        height:37px;
    }

    .coin-name{
        font-size:7px;
    }

    .change{
        font-size:5.5px;
    }

    .vol{
        font-size:6px;
    }

    .ema-value{
        font-size:6px;
    }

    .ema10-long,
    .ema10-short,
    .ema10-none{
        font-size:6px;
    }

    .air-icon{
        font-size:11px;
    }
}

/* =========================================================
   PC / 큰 화면
   ========================================================= */

@media(min-width:601px){

    body{
        max-width:900px;

        margin:0 auto;

        padding:8px;
    }

    th{
        font-size:8px;
    }

    td{
        height:42px;
    }

    .coin-name{
        font-size:10px;
    }

    .change{
        font-size:8px;
    }

    .vol{
        font-size:9px;
    }

    .ema-value{
        font-size:9px;
    }

    .ema10-long,
    .ema10-short,
    .ema10-none{
        font-size:9px;
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

    </div>
    """

    sections = ""

    # =====================================================
    # ① 상승 체크
    # =====================================================

    if USE_UPBIT == "Y":

        sections += rising_focus_section(
            latest_upbit_data,
            latest_upbit_update_time
        )

    if USE_OKX == "Y":

        sections += rising_focus_section(
            latest_okx_data,
            latest_okx_update_time
        )

    # =====================================================
    # ② 상승 해지
    # ★ 화면 표시하지 않음
    # =====================================================

    # =====================================================
    # ③ 상승 진행
    # =====================================================

    if USE_UPBIT == "Y":

        sections += warning_focus_section(
            latest_upbit_data,
            latest_upbit_update_time
        )

    if USE_OKX == "Y":

        sections += warning_focus_section(
            latest_okx_data,
            latest_okx_update_time
        )

    # =====================================================
    # ④ 전체 TOP
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
            content="width=device-width,initial-scale=1,maximum-scale=1"
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
            4H EMA3 돌파 시스템
        </title>

        <style>

            {CSS}

        </style>

    </head>

    <body>

        <h1>
            📊 트레이딩 전략
        </h1>

        <div class="info">

            ① 4H 확정 캔들 기준<br>

            ② EMA1 = EMA 10-30-60-120 배열<br>

            ③ EMA2 = EMA3 이전 {EMA2_LOOKBACK}개 고점/저점 돌파<br>

            ④ LONG = 정배열 + EMA3 상향 돌파<br>

            ⑤ SHORT = 역배열 + EMA3 하향 돌파<br>

            ⑥ EMA2 돌파 시작 = ①부터 카운트

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

    if USE_UPBIT not in (
        "Y",
        "N"
    ):

        raise ValueError(
            "USE_UPBIT은 Y 또는 N만 가능합니다."
        )

    if USE_OKX not in (
        "Y",
        "N"
    ):

        raise ValueError(
            "USE_OKX는 Y 또는 N만 가능합니다."
        )

    log.info(
        "========================================"
    )

    log.info(
        "4H EMA3 돌파 시스템 시작"
    )

    log.info(
        f"업비트={USE_UPBIT} / OKX={USE_OKX}"
    )

    log.info(
        f"TOP={TOP_N} / UPDATE={UPDATE_MINUTES}분"
    )

    log.info(
        "EMA1 = 4H EMA 10-30-60-120"
    )

    log.info(
        f"EMA2 = 4H EMA3 이전 {EMA2_LOOKBACK}개 "
        "고점/저점 돌파"
    )

    log.info(
        "LONG = EMA10 > EMA30 > EMA60 > EMA120 "
        "+ EMA3 이전 15개 최고값 상향 돌파 마감"
    )

    log.info(
        "SHORT = EMA10 < EMA30 < EMA60 < EMA120 "
        "+ EMA3 이전 15개 최저값 하향 돌파 마감"
    )

    log.info(
        "돌파 마감 캔들 = 새로운 시작점 ①"
    )

    log.info(
        "LONG 종료 = 종가 < 이전 캔들 저가"
    )

    log.info(
        "SHORT 종료 = 종가 > 이전 캔들 고가"
    )

    log.info(
        "돌파 캔들 자체는 종료 검사에서 제외"
    )

    log.info(
        "EMA2 카운트 = 돌파 캔들부터 1"
    )

    log.info(
        "4H 확정 캔들만 사용"
    )

    log.info(
        "========================================"
    )

    # =====================================================
    # 최초 데이터 조회
    # =====================================================

    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    # =====================================================
    # 주기적 업데이트
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
