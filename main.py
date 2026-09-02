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
TOP_N = 20
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
        data = r.json()

        result = []

        for x in data:

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
            except:
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

    except:
        return None


# =========================================================
# OKX 캔들
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

        for c in [
            "ts",
            "o",
            "h",
            "l",
            "c",
            "vol",
            "volCcy",
            "volCcyQuote"
        ]:
            df[c] = pd.to_numeric(
                df[c],
                errors="coerce"
            )

        # 완료된 캔들만
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

        # 혹시 API가 진행 중 캔들을 포함할 경우
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


# =========================================================
# Upbit 1H
# =========================================================

def get_upbit_1h(
    market,
    count=200,
    to=None
):

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
        "https://api.upbit.com/v1/candles/minutes/60",
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

        # 현재 진행 중인 1H 봉 제외
        now = datetime.now(KST)

        current = now.replace(
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
            df
            .sort_values("datetime")
            .drop_duplicates("datetime")
            .reset_index(drop=True)
        )

    except Exception as e:

        log.error(
            f"업비트 1H 오류 {market}: {e}"
        )

        return None


# =========================================================
# Upbit 4H
# =========================================================

def get_upbit_4h(
    market,
    count=200,
    to=None
):

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
        "https://api.upbit.com/v1/candles/minutes/240",
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

        # 현재 진행 중인 4H 봉 제외
        now = datetime.now(KST)

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
            df
            .sort_values("datetime")
            .drop_duplicates("datetime")
            .reset_index(drop=True)
        )

    except Exception as e:

        log.error(
            f"업비트 4H 오류 {market}: {e}"
        )

        return None


# =========================================================
# 과거 데이터
# =========================================================

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

        # 신규 코인은 보유 캔들 수만큼 사용
        if len(all_df) >= required:
            return all_df

        before = int(
            all_df.ts.iloc[0]
        )

    return all_df


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

        if unit == 60:
            df = get_upbit_1h(
                market,
                HISTORY_CHUNK,
                to
            )
        else:
            df = get_upbit_4h(
                market,
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


def history_upbit_4h(market):

    return history_upbit(
        market,
        240,
        125
    )


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
        df.c,
        errors="coerce"
    ).ewm(
        span=period,
        adjust=False,
        min_periods=1
    ).mean()


def direction(df):

    if df is None or df.empty:
        return "none"

    es = [
        ema(df, x)
        for x in (
            10,
            30,
            60,
            120
        )
    ]

    if any(
        x is None
        for x in es
    ):
        return "none"

    try:

        a, b, c, d = [
            x.iloc[-1]
            for x in es
        ]

        if any(
            pd.isna(x)
            for x in (
                a,
                b,
                c,
                d
            )
        ):
            return "none"

        if a > b > c > d:
            return "long"

        if a < b < c < d:
            return "short"

    except:
        pass

    return "none"


def ema_display(df):

    d = direction(df)

    return {
        "display": (
            "🟢 LONG"
            if d == "long"
            else
            "🔴 SHORT"
            if d == "short"
            else
            "⚪"
        ),
        "direction": d
    }


# =========================================================
# 🛩 ✈️ 1H 비행기 경고
# =========================================================

def get_air_warning(
    df1h,
    df4h
):

    if (
        df1h is None
        or df1h.empty
        or df4h is None
        or df4h.empty
    ):
        return False

    # 최소 2개 봉 필요
    if len(df1h) < 2:
        return False

    # -----------------------------------------------------
    # ① 1H EMA 정배열
    # -----------------------------------------------------

    if direction(df1h) != "long":
        return False

    # -----------------------------------------------------
    # ② 4H EMA 정배열
    # -----------------------------------------------------

    if direction(df4h) != "long":
        return False

    try:

        e10 = ema(
            df1h,
            10
        )

        if e10 is None:
            return False

        # -------------------------------------------------
        # 이전 완료 1H 봉
        # -------------------------------------------------

        prev_close = float(
            df1h.c.iloc[-2]
        )

        prev_ema10 = float(
            e10.iloc[-2]
        )

        # -------------------------------------------------
        # 현재 완료 1H 봉
        # -------------------------------------------------

        current_open = float(
            df1h.o.iloc[-1]
        )

        current_close = float(
            df1h.c.iloc[-1]
        )

        current_ema10 = float(
            e10.iloc[-1]
        )

        # 이전 종가가 EMA10 아래
        was_below = (
            prev_close < prev_ema10
        )

        # 현재 양봉
        bullish = (
            current_close > current_open
        )

        # 현재 종가가 EMA10 위
        closed_above = (
            current_close > current_ema10
        )

        return (
            was_below
            and bullish
            and closed_above
        )

    except Exception:
        return False


# =========================================================
# 변동률 / 거래대금
# =========================================================

def daily_change_upbit(market):

    r = retry(
        requests.get,
        "https://api.upbit.com/v1/candles/days",
        params={
            "market": market,
            "count": 1
        },
        timeout=15
    )

    if r is None:
        return None

    try:

        x = r.json()[0].get(
            "change_rate"
        )

        return (
            [
                round(
                    float(x) * 100,
                    2
                )
            ]
            if x is not None
            else None
        )

    except:
        return None


def daily_changes(df):

    if df is None or df.empty:
        return None

    try:

        x = df.copy()

        if "datetime" in x.columns:

            x["datetime"] = pd.to_datetime(
                x["datetime"],
                errors="coerce"
            )

        else:

            x["datetime"] = (
                pd.to_datetime(
                    x.ts,
                    unit="ms",
                    utc=True
                )
                .dt.tz_convert(
                    "Asia/Seoul"
                )
                .dt.tz_localize(None)
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

        result = []

        for i in range(
            max(
                1,
                len(daily) - 3
            ),
            len(daily)
        ):

            if daily.iloc[i - 1] != 0:

                result.append(
                    round(
                        (
                            daily.iloc[i]
                            - daily.iloc[i - 1]
                        )
                        / daily.iloc[i - 1]
                        * 100,
                        2
                    )
                )

        return result[::-1]

    except:
        return None


def format_volume(v):

    if v is None:
        return "-"

    try:
        v = float(v)
    except:
        return "-"

    if v >= 1e12:
        return f"{v / 1e12:.2f}조"

    if v >= 1e8:
        return f"{v / 1e8:,.0f}억"

    return f"{v / 1e4:,.0f}만원"


def format_change(x):

    if not x:
        return (
            '<span class="change-item">'
            '⬜ N/A'
            '</span>'
        )

    try:
        v = float(x[0])
    except:
        return (
            '<span class="change-item">'
            '⬜ N/A'
            '</span>'
        )

    icon = (
        "☀️"
        if v > 0
        else
        "☁️"
    )

    sign = (
        "+"
        if v > 0
        else
        ""
    )

    cls = (
        "positive"
        if v > 0
        else
        "negative"
        if v < 0
        else
        "neutral"
    )

    return (
        f'<span class="change-item {cls}">'
        f'{icon} {sign}{v:.2f}%'
        f'</span>'
    )


# =========================================================
# 분석
# =========================================================

def analyze(
    market,
    okx=False
):

    if okx:

        # 1H / 4H만 조회
        df1 = history_okx(
            market,
            "1H"
        )

        df4 = history_okx(
            market,
            "4H"
        )

    else:

        # 1H / 4H만 조회
        df1 = history_upbit(
            market,
            60
        )

        df4 = history_upbit_4h(
            market
        )

    if (
        df1 is None
        or df1.empty
        or df4 is None
        or df4.empty
    ):
        return None

    e1 = ema_display(
        df1
    )

    e4 = ema_display(
        df4
    )

    air_warning = get_air_warning(
        df1,
        df4
    )

    # OKX는 기존 일간 변동률 구조 유지
    changes = (
        daily_changes(df1)
        if okx
        else
        daily_change_upbit(market)
    )

    return {
        "ema_1h": e1,
        "ema_4h": e4,

        "changes": changes,

        "air_warning": air_warning,

        "qualified": air_warning,

        "direction_1h":
            e1["direction"],

        "direction_4h":
            e4["direction"]
    }


# =========================================================
# 빈 분석
# =========================================================

def empty_analysis():

    e = {
        "display": "⚪",
        "direction": "none"
    }

    return {
        "ema_1h": e.copy(),
        "ema_4h": e.copy(),

        "changes": None,

        "air_warning": False,
        "qualified": False,

        "direction_1h": "none",
        "direction_4h": "none"
    }


# =========================================================
# 업비트 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update_time

    log.info(
        f"========== 업비트 TOP{TOP_N} 시작 =========="
    )

    markets = sorted(
        get_upbit_markets(),
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

            a = (
                analyze(market)
                or empty_analysis()
            )

            qualified = a[
                "air_warning"
            ]

            rows.append({

                "rank": rank,

                "name": coin,

                "change":
                    format_change(
                        a["changes"]
                    ),

                "volume":
                    format_volume(
                        item["volume_24h"]
                    ),

                "ema_1h":
                    a["ema_1h"],

                "ema_4h":
                    a["ema_4h"],

                "direction":
                    "long"
                    if qualified
                    else
                    "none",

                "air_warning":
                    qualified,

                "qualified":
                    qualified

            })

        except Exception as e:

            log.error(
                f"업비트 상세 오류 {market}: {e}"
            )

            a = empty_analysis()

            rows.append({

                "rank": rank,
                "name": coin,
                "change": "",

                "volume":
                    format_volume(
                        item["volume_24h"]
                    ),

                "ema_1h":
                    a["ema_1h"],

                "ema_4h":
                    a["ema_4h"],

                "direction": "none",
                "air_warning": False,
                "qualified": False
            })

    latest_upbit_data = rows

    latest_upbit_update_time = kst()

    log.info(
        f"업비트 완료 / "
        f"비행기 조건 {sum(x['qualified'] for x in rows)}개"
    )

    return True


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
            and x.get("state") == "live"
        ]

    except:
        return []


def get_okx_volume(
    inst,
    usdt
):

    # 기존 24시간 거래대금 구조 유지
    df = get_okx_ohlcv(
        inst,
        "1H",
        VOLUME_HOURS
    )

    if df is None or df.empty:
        return None

    try:

        return float(
            pd.to_numeric(
                df.volCcyQuote,
                errors="coerce"
            ).sum()
        ) * float(usdt)

    except:
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

    for s in symbols:

        v = get_okx_volume(
            s,
            usdt
        )

        if v and v > 0:
            volumes[s] = v

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

        display = (
            f"{coin}[UP]"
            if coin in upbit_set
            else coin
        )

        try:

            a = (
                analyze(
                    symbol,
                    True
                )
                or empty_analysis()
            )

            qualified = a[
                "air_warning"
            ]

            rows.append({

                "rank": rank,

                "name": display,

                "change":
                    format_change(
                        a["changes"]
                    ),

                "volume":
                    format_volume(
                        volumes[symbol]
                    ),

                "ema_1h":
                    a["ema_1h"],

                "ema_4h":
                    a["ema_4h"],

                "direction":
                    "long"
                    if qualified
                    else
                    "none",

                "air_warning":
                    qualified,

                "qualified":
                    qualified
            })

        except Exception as e:

            log.error(
                f"OKX 상세 오류 {symbol}: {e}"
            )

            a = empty_analysis()

            rows.append({

                "rank": rank,
                "name": display,
                "change": "",

                "volume":
                    format_volume(
                        volumes[symbol]
                    ),

                "ema_1h":
                    a["ema_1h"],

                "ema_4h":
                    a["ema_4h"],

                "direction": "none",
                "air_warning": False,
                "qualified": False
            })

    latest_okx_data = rows

    latest_okx_update_time = kst()

    log.info(
        f"OKX 완료 / "
        f"비행기 조건 {sum(x['qualified'] for x in rows)}개"
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

        # -------------------------------------------------
        # Upbit
        # -------------------------------------------------

        if USE_UPBIT == "Y":

            try:
                update_upbit()

            except Exception as e:

                log.exception(
                    f"업비트 업데이트 오류: {e}"
                )

        else:

            latest_upbit_data = []

        # -------------------------------------------------
        # OKX
        # -------------------------------------------------

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
# 비행기 표시
# =========================================================

def warning_html(air_warning):

    if air_warning:

        return (
            '<span class="air">'
            '🛩 ✈️'
            '</span>'
        )

    return "-"


# =========================================================
# 방향 표시
# =========================================================

def direction_html(d):

    if d == "long":

        return (
            '<span class="long">'
            'LONG'
            '</span>'
        )

    if d == "short":

        return (
            '<span class="short">'
            'SHORT'
            '</span>'
        )

    return (
        '<span class="none">'
        '-'
        '</span>'
    )


# =========================================================
# EMA HTML
# =========================================================

def ema_html(e):

    if not e:
        return "⚪"

    return e.get(
        "display",
        "⚪"
    )


# =========================================================
# 테이블
# =========================================================

def rows_html(data):

    out = ""

    for x in data:

        q = x.get(
            "qualified",
            False
        )

        dc = x.get(
            "direction",
            "none"
        )

        cls = (
            " qualified"
            if q
            else
            ""
        )

        e1 = x.get(
            "ema_1h",
            {}
        )

        e4 = x.get(
            "ema_4h",
            {}
        )

        out += f"""
        <tr class="{cls}">

            <td>
                {x.get("rank", "-")}
            </td>

            <td class="coin">
                {x.get("name", "-")}
            </td>

            <td>
                {x.get("change", "")}
            </td>

            <td>
                <div class="vol">
                    {x.get("volume", "-")}
                </div>
            </td>

            <td>

                <div class="ema">

                    <div>
                        <b>1H</b>
                        {ema_html(e1)}
                    </div>

                    <div>
                        <b>4H</b>
                        {ema_html(e4)}
                    </div>

                </div>

            </td>

            <td>
                {direction_html(dc)}
            </td>

            <td class="warning">
                {warning_html(
                    x.get(
                        "air_warning",
                        False
                    )
                )}
            </td>

        </tr>
        """

    return out


# =========================================================
# 섹션
# =========================================================

def section(
    title,
    data,
    update_time
):

    rows = rows_html(
        data
    )

    if not rows:

        rows = """
        <tr>
            <td colspan="7"
                class="empty">
                현재 조회 데이터 없음
            </td>
        </tr>
        """

    return f"""
    <h2>
        🏆 {title} TOP{TOP_N}
        <small>
            조회 {update_time} KST
        </small>
    </h2>

    <div class="table-wrap">

        <table>

            <thead>
                <tr>
                    <th>#</th>
                    <th>코인</th>
                    <th>등락</th>
                    <th>거래대금</th>
                    <th>EMA</th>
                    <th>방향</th>
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
# CSS
# =========================================================

CSS = """

*{
    box-sizing:border-box
}

html,
body{
    margin:0;
    padding:0;
    width:100%;
    overflow-x:hidden
}

body{
    background:#0f1115;
    color:#eee;
    font-family:Arial,sans-serif;
    font-size:9px;
    padding:4px
}

h1{
    margin:3px 2px 6px;
    font-size:14px
}

h2{
    margin:10px 2px 5px;
    font-size:11px
}

h2 small{
    color:#777;
    font-size:7px;
    font-weight:normal
}

.info{
    margin:0 2px 6px;
    padding:5px 6px;
    color:#8b9099;
    background:#171a1f;
    border:1px solid #252a31;
    border-radius:7px;
    font-size:7px;
    line-height:1.5
}

.status{
    display:flex;
    gap:7px;
    margin-top:4px;
    font-weight:bold
}

.y{
    color:#35e66d
}

.n{
    color:#ff4d4d
}

.table-wrap{
    width:100%;
    overflow:hidden;
    border-radius:8px;
    border:1px solid #252a31
}

table{
    width:100%;
    table-layout:fixed;
    border-collapse:collapse;
    background:#181c21
}

th{
    padding:5px 1px;
    background:#12151a;
    border-bottom:1px solid #2b3037;
    color:#8f949d;
    font-size:6px
}

td{
    padding:5px 1px;
    border-bottom:1px solid #272c32;
    text-align:center;
    vertical-align:middle
}

th:nth-child(1),
td:nth-child(1){
    width:6%
}

th:nth-child(2),
td:nth-child(2){
    width:20%
}

th:nth-child(3),
td:nth-child(3){
    width:16%
}

th:nth-child(4),
td:nth-child(4){
    width:20%
}

th:nth-child(5),
td:nth-child(5){
    width:26%
}

th:nth-child(6),
td:nth-child(6){
    width:7%
}

th:nth-child(7),
td:nth-child(7){
    width:5%
}

.coin{
    font-size:8px;
    font-weight:bold;
    white-space:nowrap
}

.change-item{
    display:block;
    font-size:7px;
    font-weight:bold
}

.positive,
.negative{
    color:#fff
}

.neutral{
    color:#aaa
}

.vol{
    font-size:7px;
    font-weight:bold;
    margin-bottom:2px
}

.long{
    display:block;
    color:#35e66d;
    font-size:7px;
    font-weight:800
}

.short{
    display:block;
    color:#ff4d4d;
    font-size:7px;
    font-weight:800
}

.none{
    color:#666;
    font-size:7px
}

.ema{
    display:flex;
    flex-direction:column;
    text-align:left
}

.ema div{
    height:14px;
    line-height:14px;
    white-space:nowrap;
    font-size:7px;
    font-weight:bold
}

.ema b{
    display:inline-block;
    width:25px;
    color:#8f949d;
    font-size:6px;
    text-align:right;
    margin-right:3px
}

.warning{
    min-height:14px;
    text-align:center
}

.air{
    font-size:11px;
    font-weight:bold;
    filter:
        drop-shadow(
            0 0 4px
            rgba(255,255,255,.9)
        )
}

.qualified{
    animation:blink 1.2s infinite
}

@keyframes blink{

    0%,100%{
        background:#181c21
    }

    50%{
        background:#26352b
    }

}

.empty{
    color:#555;
    padding:12px 4px
}

@media(max-width:480px){

    body{
        padding:3px;
        font-size:8px
    }

    h1{
        font-size:13px
    }

    h2{
        font-size:10px
    }

    .info{
        font-size:6px;
        padding:4px 5px
    }

    th{
        padding:4px 1px;
        font-size:5px
    }

    td{
        padding:4px 1px
    }

    .coin{
        font-size:7px
    }

    .change-item,
    .vol{
        font-size:6px
    }

    .long,
    .short{
        font-size:6px
    }

    .air{
        font-size:10px
    }

    .ema div{
        height:13px;
        line-height:13px;
        font-size:6px
    }

    .ema b{
        width:23px;
        font-size:5px
    }

}

"""


# =========================================================
# 대시보드
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
1H EMA Air Warning
</title>

<style>
{CSS}
</style>

</head>

<body>

<h1>
📊 1H EMA 비행기 경고
</h1>

<div class="info">

① 24시간 거래대금 TOP{TOP_N}<br>

② 1H / 4H EMA 10-30-60-120<br>

③ 1H + 4H 모두 정배열<br>

④ 이전 1H 종가가 EMA10 아래<br>

⑤ 현재 완료 1H 캔들 양봉<br>

⑥ 현재 1H 종가가 EMA10 위<br>

⑦ 모든 조건 만족 → 🛩 ✈️<br>

⑧ 진행 중인 1H / 4H 캔들은 제외

{status}

</div>

{sections}

</body>

</html>
"""


# =========================================================
# 스케줄러
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
# 시작
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

    if TOP_N <= 0:
        raise ValueError(
            "TOP_N은 1 이상이어야 합니다."
        )

    if UPDATE_MINUTES <= 0:
        raise ValueError(
            "UPDATE_MINUTES는 1 이상이어야 합니다."
        )

    log.info(
        "========================================"
    )

    log.info(
        "서버 시작"
    )

    log.info(
        f"업비트={USE_UPBIT} / OKX={USE_OKX}"
    )

    log.info(
        f"TOP={TOP_N} / "
        f"UPDATE={UPDATE_MINUTES}분"
    )

    log.info(
        "EMA = 10-30-60-120"
    )

    log.info(
        "15M 데이터/EMA/N자 로직 = 삭제"
    )

    log.info(
        "비행기 경고 = 1H 기준"
    )

    log.info(
        "1H + 4H 정배열"
    )

    log.info(
        "이전 1H 종가 < EMA10"
    )

    log.info(
        "현재 1H 양봉 + 종가 > EMA10"
    )

    log.info(
        "🛩 ✈️ 조건 만족 시 표시"
    )

    log.info(
        "현재 진행 중인 봉 제외"
    )

    log.info(
        "========================================"
    )

    # 최초 조회
    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    # 반복 조회
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
