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
# 3-10선 설정
# =========================================================

MIN_EMA10_LONG_COUNT = 1


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
air_state_lock = threading.Lock()

last_request_time = 0

air_state = {}


# =========================================================
# ★ 종료 표시 관리
#
# 종료된 종목은 ⛔️를 한 번만 표시하고
# 다음 대시보드 갱신부터 제거
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

    return get_upbit_candle(
        market,
        60,
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
    required=125
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
            .strftime("%Y-%m-%dT%H:%M:%S")
        )

    return all_df


def history_okx(
    inst,
    bar,
    required=125
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
# ★ 1H EMA 10-30-60-120 정배열 / 역배열
#
# LONG
# EMA10 > EMA30 > EMA60 > EMA120
#
# SHORT
# EMA10 < EMA30 < EMA60 < EMA120
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

        # =================================================
        # 1H 완전 정배열
        # =================================================

        if (
            e10 > e30
            and e30 > e60
            and e60 > e120
        ):

            return "long"

        # =================================================
        # 1H 완전 역배열
        # =================================================

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

        e10 = ema(
            df,
            10
        )

        e30 = ema(
            df,
            30
        )

        e60 = ema(
            df,
            60
        )

        e120 = ema(
            df,
            120
        )

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
            f"EMA 10-30-60-120 카운팅 오류: {e}"
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
# 1H EMA3 ↔ EMA10
# =========================================================

def ema3_10_cross_count(df):

    result = {
        "state": "none",
        "count": 0,
        "final_count": 0,
        "display": "-",
        "candle_time": None
    }

    if (
        df is None
        or df.empty
        or len(df) < 2
    ):

        return result

    try:

        e3 = ema(
            df,
            3
        )

        e10 = ema(
            df,
            10
        )

        if (
            e3 is None
            or e10 is None
        ):

            return result

        e3_values = pd.to_numeric(
            e3,
            errors="coerce"
        ).reset_index(drop=True)

        e10_values = pd.to_numeric(
            e10,
            errors="coerce"
        ).reset_index(drop=True)

        valid_last = (
            e3_values.notna().iloc[-1]
            and
            e10_values.notna().iloc[-1]
        )

        if not valid_last:
            return result

        states = []

        for i in range(len(df)):

            ema3_value = float(
                e3_values.iloc[i]
            )

            ema10_value = float(
                e10_values.iloc[i]
            )

            if ema3_value > ema10_value:

                states.append("long")

            elif ema3_value < ema10_value:

                states.append("short")

            else:

                states.append("equal")

        current_state = states[-1]

        result["state"] = current_state

        result["candle_time"] = (
            df.datetime.iloc[-1]
        )

        if current_state == "equal":

            result["display"] = "⚪(0)"

            return result

        current_count = 0
        i = len(states) - 1

        while i >= 0:

            if states[i] == current_state:

                current_count += 1
                i -= 1

            else:

                break

        previous_state = (
            "short"
            if current_state == "long"
            else "long"
        )

        final_count = 0
        j = i

        while j >= 0:

            if states[j] == previous_state:

                final_count += 1
                j -= 1

            else:

                break

        result["count"] = current_count
        result["final_count"] = final_count

        if current_state == "long":

            if final_count > 0:

                result["display"] = (
                    f"{final_count}|"
                    f"🟢({current_count})"
                )

            else:

                result["display"] = (
                    f"🟢({current_count})"
                )

        else:

            if final_count > 0:

                result["display"] = (
                    f"{final_count}|"
                    f"🔻({current_count})"
                )

            else:

                result["display"] = (
                    f"🔻({current_count})"
                )

        return result

    except Exception as e:

        log.error(
            f"EMA3-EMA10 교차 카운팅 오류: {e}"
        )

        return result


# =========================================================
# ✈️ 비행기 경고 조건
#
# ① 1H EMA10 > EMA30 > EMA60 > EMA120
# ② 1H EMA3 > EMA10
# =========================================================

def get_air_warning(df1h):

    if (
        df1h is None
        or df1h.empty
    ):

        return None

    try:

        # =================================================
        # ① 1H EMA 10-30-60-120 정배열 필수
        # =================================================

        direction_1h = direction(
            df1h
        )

        if direction_1h != "long":

            return None

        # =================================================
        # ② 1H EMA3 > EMA10 필수
        # =================================================

        ema3_10_result = (
            ema3_10_cross_count(
                df1h
            )
        )

        if (
            ema3_10_result.get("state")
            != "long"
        ):

            return None

        current_long_count = int(
            ema3_10_result.get(
                "count",
                0
            )
        )

        if (
            current_long_count
            < MIN_EMA10_LONG_COUNT
        ):

            return None

        return "LONG_PRE_BREAKOUT"

    except Exception as e:

        log.error(
            f"비행기 경고 오류: {e}"
        )

        return None


# =========================================================
# ✈️ 비행기 상태
# =========================================================

def update_air_counter(
    market,
    df1h,
    new_warning
):

    if (
        df1h is None
        or df1h.empty
    ):

        return {
            "active": False,
            "ended": False,
            "direction": None,
            "count": 0
        }

    candle_time = (
        df1h.datetime.iloc[-1]
    )

    with air_state_lock:

        state = air_state.get(
            market
        )

        ema3_10_result = (
            ema3_10_cross_count(
                df1h
            )
        )

        current_ema_state = (
            ema3_10_result.get(
                "state",
                "none"
            )
        )

        # =================================================
        # ① 최초 비행기 포착
        # =================================================

        if state is None:

            if new_warning is None:

                return {
                    "active": False,
                    "ended": False,
                    "direction": None,
                    "count": 0
                }

            with air_ended_displayed_lock:

                air_ended_displayed.discard(
                    market
                )

            air_state[market] = {

                "active":
                    True,

                "ended":
                    False,

                "direction":
                    new_warning,

                "count":
                    1,

                "warning_candle":
                    candle_time,

                "counted_candle":
                    candle_time
            }

            return {
                "active": True,
                "ended": False,
                "direction":
                    new_warning,
                "count": 1
            }

        # =================================================
        # ② 이전 비행기가 종료된 상태
        # =================================================

        if state.get(
            "ended",
            False
        ):

            if new_warning is not None:

                if (
                    state.get(
                        "warning_candle"
                    )
                    != candle_time
                ):

                    with air_ended_displayed_lock:

                        air_ended_displayed.discard(
                            market
                        )

                    air_state[market] = {

                        "active":
                            True,

                        "ended":
                            False,

                        "direction":
                            new_warning,

                        "count":
                            1,

                        "warning_candle":
                            candle_time,

                        "counted_candle":
                            candle_time
                    }

                    return {
                        "active": True,
                        "ended": False,
                        "direction":
                            new_warning,
                        "count": 1
                    }

            return {
                "active": False,
                "ended": True,
                "direction":
                    state.get("direction"),
                "count":
                    state.get("count", 0)
            }

        # =================================================
        # ③ 비행기 진행 상태가 아닌 경우
        # =================================================

        if not state.get(
            "active",
            False
        ):

            return {
                "active": False,
                "ended":
                    state.get(
                        "ended",
                        False
                    ),
                "direction":
                    state.get("direction"),
                "count":
                    state.get("count", 0)
            }

        # =================================================
        # ④ 같은 확정 캔들이면 카운트 증가 금지
        # =================================================

        if candle_time <= state.get(
            "counted_candle"
        ):

            if current_ema_state == "short":

                state["active"] = False
                state["ended"] = True

                return {
                    "active": False,
                    "ended": True,
                    "direction":
                        state.get("direction"),
                    "count":
                        state.get("count", 1)
                }

            return {
                "active": True,
                "ended": False,
                "direction":
                    state.get("direction"),
                "count":
                    state.get("count", 1)
            }

        # =================================================
        # ⑤ 새로운 확정 1H 캔들
        # =================================================

        state["counted_candle"] = candle_time

        # =================================================
        # EMA3 < EMA10 → 종료
        # =================================================

        if current_ema_state == "short":

            state["active"] = False
            state["ended"] = True

            return {
                "active": False,
                "ended": True,
                "direction":
                    state.get("direction"),
                "count":
                    state.get("count", 1)
            }

        # =================================================
        # EMA3 = EMA10
        # =================================================

        if current_ema_state == "equal":

            return {
                "active": True,
                "ended": False,
                "direction":
                    state.get("direction"),
                "count":
                    state.get("count", 1)
            }

        # =================================================
        # EMA3 > EMA10
        #
        # 양봉/음봉 관계없이 +1
        # =================================================

        if current_ema_state == "long":

            state["count"] = (
                int(
                    state.get(
                        "count",
                        1
                    )
                ) + 1
            )

        return {
            "active": True,
            "ended": False,
            "direction":
                state.get("direction"),
            "count":
                state.get("count", 1)
        }


# =========================================================
# 등락률 / 거래대금
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
                "-",

            "candle_time":
                None
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
    # 1H 데이터만 사용
    # =================================================

    if okx:

        df1 = history_okx(
            market,
            "1H"
        )

    else:

        df1 = history_upbit(
            market,
            60
        )

    if (
        df1 is None
        or df1.empty
    ):

        return None

    # =================================================
    # 1H EMA 10-30-60-120
    # =================================================

    e1 = ema_display(
        df1
    )

    # =================================================
    # 1H EMA3-EMA10
    # =================================================

    ema3_10_cross = (
        ema3_10_cross_count(
            df1
        )
    )

    # =================================================
    # 비행기 조건
    #
    # 1H EMA10 > EMA30 > EMA60 > EMA120
    # +
    # EMA3 > EMA10
    # =================================================

    new_warning = get_air_warning(
        df1
    )

    air = update_air_counter(
        market,
        df1,
        new_warning
    )

    changes = (
        daily_changes(df1)
        if okx
        else daily_change_upbit(market)
    )

    return {

        "ema_1h":
            e1,

        "ema3_10_cross_1h":
            ema3_10_cross,

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
            air.get(
                "ended",
                False
            ),

        "qualified":
            air["active"],

        "direction_1h":
            e1["direction"],

        "df1h":
            df1
    }


# =========================================================
# 공통 행 생성
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

            a = analyze(market)

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
        f"비행기 진행 {active_count}개 / "
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
        f"비행기 진행 {active_count}개 / "
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
                    update_okx(usdt)

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
#
# 1 → 🛩✈️
# 2 이상 → ✈️
# 종료 → ⛔️
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

    direction = e.get(
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
        direction,
        "ema-none"
    )

    return (
        f'<span class="ema-value {cls}">'
        f'{display}'
        '</span>'
    )


# =========================================================
# 3-10선 표시
# =========================================================

def ema3_10_cross_html(data):

    if not data:
        return "-"

    state = data.get(
        "state",
        "none"
    )

    count = int(
        data.get(
            "count",
            0
        )
    )

    final_count = int(
        data.get(
            "final_count",
            0
        )
    )

    if state == "long":

        previous_html = ""

        if final_count > 0:

            previous_html = (
                '<div class="ema10-final">'
                f'({final_count})'
                '</div>'
            )

        return (
            '<div class="ema10-box">'

            '<div class="ema10-long">'
            f'🟢({count})'
            '</div>'

            f'{previous_html}'

            '</div>'
        )

    if state == "short":

        previous_html = ""

        if final_count > 0:

            previous_html = (
                '<div class="ema10-final">'
                f'({final_count})'
                '</div>'
            )

        return (
            '<div class="ema10-box">'

            '<div class="ema10-short">'
            f'🔻({count})'
            '</div>'

            f'{previous_html}'

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

def rows_html(data):

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

        ema3_data = x.get(
            "ema3_10_cross_1h",
            {}
        )

        visual_air_count = int(
            ema3_data.get(
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
                            1H
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
                        ema3_data
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

    return "".join(out)


# =========================================================
# Table
# =========================================================

def table_html(data):

    rows = rows_html(data)

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
                    <th>EMA 1H</th>
                    <th>3-10선</th>
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
# ★ 실제 화면용 비행기 카운트
# =========================================================

def get_visual_air_count(x):

    ema3_data = x.get(
        "ema3_10_cross_1h",
        {}
    )

    try:

        return int(
            ema3_data.get(
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
# 🚀 상승 경고리스트
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

            rising_data.append(x)

    if not rising_data:

        rows = """
        <tr>
            <td colspan="6" class="empty">
                현재 상승 경고 코인 없음
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
                        <th>EMA</th>
                        <th>3-10선</th>
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
            + table_html(
                rising_data
            ).replace(
                '<div class="table-wrap">',
                '',
                1
            )
            + '</div>'
        )

    return f"""
    <h2 class="focus-title rising-title">

        🚀 상승 체크 [EMA1(10)+EMA2(5)이상= 1️⃣+〽️]

        <small>
            {update_time} KST
        </small>

    </h2>

    {table}
    """


# =========================================================
# ⛔️ 상승 해지 리스트
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

            ended_data.append(x)

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
                        <th>EMA</th>
                        <th>3-10선</th>
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
            + table_html(
                ended_data
            ).replace(
                '<div class="table-wrap">',
                '',
                1
            )
            + '</div>'
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

        # =================================================
        # ★ 상승 해지 종목은
        # 상승 진행 리스트에서 제외
        # =================================================

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

            warning_data.append(x)

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
            + table_html(
                warning_data
            ).replace(
                '<div class="table-wrap">',
                '',
                1
            )
            + '</div>'
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
# =========================================================

CSS = """

*{
    box-sizing:border-box;
}

html,
body{
    margin:0;
    padding:0;
    width:100%;
    overflow-x:hidden;
}

body{
    background:#0f1115;
    color:#eee;
    font-family:Arial,sans-serif;
    font-size:9px;
    padding:3px;
}

h1{
    margin:2px 2px 4px;
    font-size:13px;
}

h2{
    margin:7px 2px 3px;
    font-size:10px;
}

h2 small{
    color:#777;
    font-size:6px;
    font-weight:normal;
    margin-left:3px;
}

.info{
    margin:0 2px 4px;
    padding:3px 5px;
    color:#8b9099;
    background:#171a1f;
    border:1px solid #252a31;
    border-radius:7px;
    font-size:7px;
    line-height:1.25;
}

.status{
    display:flex;
    justify-content:center;
    gap:8px;
    margin-top:2px;
    font-weight:bold;
}

.y{
    color:#35e66d;
}

.n{
    color:#ff4d4d;
}

.table-wrap{
    width:100%;
    overflow:hidden;
    border-radius:8px;
    border:1px solid #252a31;
}

table{
    width:100%;
    table-layout:fixed;
    border-collapse:collapse;
    background:#181c21;
}

th{
    padding:4px 2px;
    background:#12151a;
    border-bottom:1px solid #2b3037;
    color:#8f949d;
    font-size:6px;
    white-space:nowrap;
    text-align:center !important;
    vertical-align:middle;
}

td{
    padding:3px 2px;
    border-bottom:1px solid #272c32;
    text-align:center !important;
    vertical-align:middle;
}

th:nth-child(1),
td:nth-child(1){
    width:6%;
}

th:nth-child(2),
td:nth-child(2){
    width:21%;
}

th:nth-child(3),
td:nth-child(3){
    width:16%;
}

th:nth-child(4),
td:nth-child(4){
    width:24%;
}

th:nth-child(5),
td:nth-child(5){
    width:17%;
}

th:nth-child(6),
td:nth-child(6){
    width:16%;
}

.rank{
    color:#8f949d;
    font-size:7px;
}

.coin{
    overflow:hidden;
    padding:1px 2px;
}

.coin-name{
    font-size:8px;
    font-weight:bold;
    line-height:9px;
    height:9px;
    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;
}

.change{
    margin-top:0;
    line-height:7px;
    height:7px;
    font-size:7px;
    white-space:nowrap;
}

.up{
    color:#35e66d;
    font-weight:bold;
}

.down{
    color:#ff4d4d;
    font-weight:bold;
}

.zero{
    color:#999;
}

.vol{
    padding:1px 2px !important;
    font-size:7px;
    font-weight:bold;
    line-height:16px;
    height:16px;
    white-space:nowrap;
}

.ema-cell{
    overflow:hidden;
    padding:1px !important;
}

.ema-row{
    display:flex;
    align-items:center;
    justify-content:center;
    width:100%;
    height:13px;
    line-height:13px;
    white-space:nowrap;
    overflow:hidden;
    font-size:7px;
    font-weight:bold;
}

.tf{
    flex:0 0 20px;
    width:20px;
    color:#8f949d;
    font-size:6px;
    font-weight:bold;
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
    font-size:7px;
    font-weight:bold;
    line-height:13px;
}

.ema-long{
    color:#35e66d;
}

.ema-short{
    color:#ff4d4d;
}

.ema-none{
    color:#eee;
}

.close-ema10{
    text-align:center !important;
    vertical-align:middle !important;
    white-space:nowrap;
    font-size:7px;
    font-weight:bold;
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
    line-height:10px;
}

.ema10-final{
    width:100%;
    display:block;
    color:#fff;
    font-size:6px;
    font-weight:bold;
    line-height:9px;
    text-align:center;
}

.ema10-long{
    color:#35e66d;
    font-size:7px;
    font-weight:bold;
    line-height:10px;
    text-align:center;
}

.ema10-short{
    color:#ff4d4d;
    font-size:7px;
    font-weight:bold;
    line-height:10px;
    text-align:center;
}

.ema10-none{
    color:#777;
    font-size:7px;
    text-align:center;
}

.warning{
    text-align:center !important;
    vertical-align:middle !important;
    white-space:nowrap;
    padding:0 2px !important;
}

.air-box{
    width:100%;
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

/* =====================================================
★ 비행기 반짝임 제거
===================================================== */

.air-icon{
    font-size:11px;
    font-weight:bold;
    display:inline-block;
}

.air-end{
    font-size:12px;
    font-weight:bold;
    line-height:14px;
    color:#ff4d4d;
}

.qualified{
    background:rgba(255,255,255,.06);
}

/* =====================================================
★ 상승 경고리스트
===================================================== */

.focus-title{
    margin-top:5px;
    margin-bottom:3px;
}

.rising-title{
    color:#35e66d;
}

.rising-table{
    border:1px solid #35e66d;
}

/* =====================================================
★ 상승 해지리스트
===================================================== */

.ended-title{
    color:#ff4d4d;
}

.ended-table{
    border:1px solid #ff4d4d;
}

/* =====================================================
★ 경고리스트
===================================================== */

.warning-title{
    color:#00ff66;
}

.warning-table{
    border:1px solid #343a42;
}

.focus-table{
    border:1px solid #343a42;
}

.empty{
    color:#555;
    padding:10px 4px;
}

@media(max-width:480px){

    body{
        padding:2px;
        font-size:8px;
    }

    h1{
        font-size:12px;
        margin:2px 2px 3px;
    }

    h2{
        font-size:9px;
        margin:6px 2px 2px;
    }

    .info{
        font-size:6px;
        padding:2px 4px;
        line-height:1.2;
        margin-bottom:3px;
    }

    th{
        padding:3px 1px;
        font-size:5px;
    }

    td{
        padding:2px 1px;
    }

    .coin{
        padding:0 1px;
    }

    .coin-name{
        font-size:7px;
        line-height:8px;
        height:8px;
    }

    .change{
        font-size:6px;
        line-height:6px;
        height:6px;
    }

    .vol{
        padding:0 1px !important;
        font-size:6px;
        line-height:14px;
        height:14px;
    }

    .ema-cell{
        padding:0 !important;
    }

    .ema-row{
        height:12px;
        line-height:12px;
        font-size:6px;
    }

    .tf{
        flex:0 0 18px;
        width:18px;
        font-size:5px;
    }

    .ema-value-wrap{
        flex:1;
        min-width:0;
    }

    .ema-value{
        font-size:6px;
        line-height:12px;
    }

    .close-ema10{
        font-size:6px;
    }

    .ema10-box{
        line-height:9px;
    }

    .ema10-final{
        width:100%;
        color:#fff;
        font-size:5px;
        line-height:8px;
        text-align:center;
    }

    .ema10-long,
    .ema10-short{
        font-size:6px;
        line-height:9px;
        text-align:center;
    }

    .ema10-none{
        font-size:6px;
    }

    .air-icon{
        font-size:9px;
    }

    .air-end{
        font-size:10px;
        line-height:12px;
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

    # -----------------------------------------------------
    # ① 상승 경고리스트
    # -----------------------------------------------------

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

    # -----------------------------------------------------
    # ② 상승 해지 리스트
    # -----------------------------------------------------

    if USE_UPBIT == "Y":

        sections += ended_focus_section(
            latest_upbit_data,
            latest_upbit_update_time
        )

    if USE_OKX == "Y":

        sections += ended_focus_section(
            latest_okx_data,
            latest_okx_update_time
        )

    # -----------------------------------------------------
    # ③ 상승 진행 리스트
    # -----------------------------------------------------

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

    # -----------------------------------------------------
    # ④ 전체 TOP
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
            content="width=device-width,initial-scale=1"
        >

        <meta
            http-equiv="refresh"
            content="60"
        >

        <title>
            1H EMA10-30-60-120 비행기 경고
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

            ① 거래대금 TOP{TOP_N}<br>

            ② 1H EMA 10-30-60-120 정배열 필수<br>

            ③ 1H EMA3 &gt; EMA10 필수<br>

            ④ 1H EMA3 &gt; EMA10 연속 카운팅 → 🟢(N)<br>

            ⑤ EMA3와 EMA10은 모두 1H 캔들 종가 기준<br>

            ⑥ 모든 조건 충족 즉시 → 🛩✈️<br>

            ⑦ EMA3 &gt; EMA10 유지 → 새 확정 캔들마다 비행기 카운트 +1<br>

            ⑧ 양봉/음봉 관계없이 카운트 증가<br>

            ⑨ EMA3 &lt; EMA10 역배열 종가 확정 시 → ⛔️ 종료<br>

            ⑩ 업비트 : Y / OKX : N

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
        "1H EMA 비행기 경고 시스템 시작"
    )

    log.info(
        f"업비트={USE_UPBIT} / OKX={USE_OKX}"
    )

    log.info(
        f"TOP={TOP_N} / UPDATE={UPDATE_MINUTES}분"
    )

    log.info(
        "1H EMA = 10-30-60-120"
    )

    log.info(
        "EMA3 / EMA10 = 1H 캔들 종가 기준"
    )

    log.info(
        "비행기 시작 조건 = "
        "1H EMA10 > EMA30 > EMA60 > EMA120 + "
        "EMA3 > EMA10"
    )

    log.info(
        f"EMA3 > EMA10 최소 연속 = "
        f"{MIN_EMA10_LONG_COUNT}개"
    )

    log.info(
        "고가 >= EMA10 조건 = 사용 안 함"
    )

    log.info(
        "종가 < EMA10 조건 = 사용 안 함"
    )

    log.info(
        "양봉 필수 조건 = 사용 안 함"
    )

    log.info(
        "4H EMA 조건 = 사용 안 함"
    )

    log.info(
        "4H EMA 표시 = 삭제"
    )

    log.info(
        "조건 충족 즉시 = 🛩✈️"
    )

    log.info(
        "다음 확정 1H 캔들부터 = ✈️ 하나"
    )

    log.info(
        "음봉이어도 EMA3 > EMA10이면 카운트 증가"
    )

    log.info(
        "EMA3 < EMA10 역배열 종가 확정 = ⛔️ 종료"
    )

    log.info(
        "종료 ⛔️ = 대시보드 1회만 표시"
    )

    log.info(
        "비행기 화면 숫자 카운트 = 표시 안 함"
    )

    log.info(
        "15M EMA / N자 / 로켓 = 삭제"
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
