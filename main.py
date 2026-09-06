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
# 기본 조회 설정
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


# =========================================================
# EMA 분석 시간봉
# =========================================================

EMA_TIMEFRAME = 60


# =========================================================
# EMA1
#
# 정배열
# EMA30 > EMA60 > EMA120
#
# 역배열
# EMA30 < EMA60 < EMA120
# =========================================================

EMA1_FAST = 30
EMA1_MID = 60
EMA1_SLOW = 120


# =========================================================
# EMA1 후보 최대 카운트
#
# 롱 / 숏 동일 적용
# 100 이하만 후보 표시
# =========================================================

EMA1_MAX_COUNT = 100


# =========================================================
# 지원 시간봉
# =========================================================

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
# 시간봉 표시
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


# =========================================================
# OKX 시간봉 변환
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

    return mapping.get(
        int(minutes)
    )


# =========================================================
# OKX bar → 분
# =========================================================

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

    return mapping.get(
        str(bar)
    )


# =========================================================
# 현재 진행 중 캔들 시작시간
# =========================================================

def get_current_candle_start(
    timeframe_minutes
):

    timeframe_minutes = int(
        timeframe_minutes
    )

    now = datetime.now(KST)

    total_minutes = (
        now.hour * 60
        + now.minute
    )

    block_minutes = (
        total_minutes
        // timeframe_minutes
    ) * timeframe_minutes

    day_offset = (
        block_minutes // 1440
    )

    block_minutes %= 1440

    hour = (
        block_minutes // 60
    )

    minute = (
        block_minutes % 60
    )

    current = now.replace(
        hour=hour,
        minute=minute,
        second=0,
        microsecond=0
    )

    if day_offset:

        current = (
            current
            - pd.Timedelta(
                days=day_offset
            )
        )

    return current.replace(
        tzinfo=None
    )


# =========================================================
# EMA 시간봉 검증
# =========================================================

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

    if EMA_TIMEFRAME not in (
        SUPPORTED_UPBIT_TIMEFRAMES
    ):

        raise ValueError(
            "EMA_TIMEFRAME 오류\n"
            f"현재값: {EMA_TIMEFRAME}\n"
            "Upbit 지원값: "
            "5, 15, 30, 60, 240"
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
                REQUEST_INTERVAL
                - gap
            )

        last_request_time = (
            time.monotonic()
        )


def retry(
    func,
    *args,
    **kwargs
):

    name = getattr(
        func,
        "__name__",
        str(func)
    )

    url = (
        args[0]
        if args
        and isinstance(
            args[0],
            str
        )
        else kwargs.get(
            "url",
            ""
        )
    )

    for n in range(
        MAX_RETRIES
    ):

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
                f"{url} "
                f"{wait}초"
            )

            time.sleep(
                wait
            )

        except Exception as e:

            log.error(
                f"[API 오류] "
                f"{name} "
                f"{url}: {e}"
            )

            if n < MAX_RETRIES - 1:

                time.sleep(
                    min(
                        2 * (n + 1),
                        20
                    )
                )

    log.error(
        f"[API 최종 실패] "
        f"{name} "
        f"{url}"
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

            if not market.startswith(
                "KRW-"
            ):

                continue

            try:

                volume = float(
                    x[
                        "acc_trade_price_24h"
                    ]
                )

                current_price = float(
                    x[
                        "trade_price"
                    ]
                )

            except Exception:

                continue

            if (
                volume > 0
                and current_price > 0
            ):

                result.append({

                    "market":
                        market,

                    "volume_24h":
                        volume,

                    "current_price":
                        current_price

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
            r.json()[0][
                "trade_price"
            ]
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
        "https://api.upbit.com/v1/candles/minutes/"
        f"{unit}"
    )

    params = {

        "market":
            market,

        "count":
            min(
                max(
                    int(count),
                    1
                ),
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

        current = (
            get_current_candle_start(
                unit
            )
        )

        df = df[
            df.datetime < current
        ]

        if df.empty:

            return None

        return (
            df
            .sort_values(
                "datetime"
            )
            .drop_duplicates(
                "datetime"
            )
            .reset_index(
                drop=True
            )
        )

    except Exception as e:

        log.error(
            f"업비트 "
            f"{unit}분 오류 "
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

        "instId":
            inst,

        "bar":
            bar,

        "limit":
            min(
                max(
                    int(limit),
                    1
                ),
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

        df = df[
            df.confirm.astype(
                str
            ) == "1"
        ]

        if df.empty:

            return None

        df["datetime"] = (
            pd.to_datetime(
                df["ts"],
                unit="ms",
                utc=True
            )
            .dt.tz_convert(
                KST
            )
            .dt.tz_localize(
                None
            )
        )

        bar_minutes = (
            get_okx_bar_minutes(
                bar
            )
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
            df
            .sort_values(
                "ts"
            )
            .drop_duplicates(
                "ts"
            )
            .reset_index(
                drop=True
            )
        )

    except Exception as e:

        log.error(
            f"OKX "
            f"{inst} "
            f"{bar} 오류: {e}"
        )

        return None


# =========================================================
# OKX 현재가
# =========================================================

def get_okx_current_price(inst):

    r = retry(
        requests.get,
        "https://www.okx.com/api/v5/market/ticker",
        params={
            "instId":
                inst
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

        if (
            df is None
            or df.empty
        ):

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
            .drop_duplicates(
                "datetime"
            )
            .sort_values(
                "datetime"
            )
            .reset_index(
                drop=True
            )
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

        if (
            df is None
            or df.empty
        ):

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
            .drop_duplicates(
                "ts"
            )
            .sort_values(
                "ts"
            )
            .reset_index(
                drop=True
            )
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
        df.c,
        errors="coerce"
    ).ewm(
        span=period,
        adjust=False,
        min_periods=1
    ).mean()


# =========================================================
# EMA1 방향
# =========================================================

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
            and
            e60 > e120
        ):

            return "long"

        if (
            e30 < e60
            and
            e60 < e120
        ):

            return "short"

    except Exception as e:

        log.error(
            f"EMA1 방향 오류: {e}"
        )

    return "none"


# =========================================================
# EMA1 배열 + 카운트
# =========================================================

def ema_alignment_count(df):

    if (
        df is None
        or df.empty
    ):

        return {

            "direction":
                "none",

            "count":
                0

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

        if (
            current_e30 > current_e60
            and
            current_e60 > current_e120
        ):

            current_direction = "long"

        elif (
            current_e30 < current_e60
            and
            current_e60 < current_e120
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
                and
                v60 > v120
            ):

                candle_direction = "long"

            elif (
                v30 < v60
                and
                v60 < v120
            ):

                candle_direction = "short"

            else:

                candle_direction = "none"

            if (
                candle_direction
                ==
                current_direction
            ):

                count += 1

            else:

                break

        if current_direction == "none":

            count = 0

        return {

            "direction":
                current_direction,

            "count":
                count

        }

    except Exception as e:

        log.error(
            f"EMA1 배열 오류: {e}"
        )

        return {

            "direction":
                "none",

            "count":
                0

        }


# =========================================================
# EMA1 표시
# =========================================================

def ema_display(
    df,
    current_price=None
):

    result = ema_alignment_count(
        df
    )

    d = result[
        "direction"
    ]

    count = result[
        "count"
    ]

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
            count,

        "current_price":
            current_price

    }


# =========================================================
# 매수 후보 분석
#
# 정배열 상태에서
# 현재가 <= EMA30 이면 매수 후보
#
# 1차 / 2차 / 3차 없음
# =========================================================

def buy_candidate_analysis(
    df,
    current_price=None
):

    result = {

        "state":
            "none",

        "display":
            "-",

        "ema30":
            None,

        "ema60":
            None,

        "ema120":
            None,

        "current_price":
            current_price,

        "qualified":
            False

    }

    if (
        df is None
        or df.empty
    ):

        return result

    if current_price is None:

        return result

    try:

        current_price = float(
            current_price
        )

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

        e30 = float(e30)
        e60 = float(e60)
        e120 = float(e120)

        result[
            "ema30"
        ] = e30

        result[
            "ema60"
        ] = e60

        result[
            "ema120"
        ] = e120

        result[
            "current_price"
        ] = current_price

        # 정배열이 아니면 매수 후보 아님
        if not (
            e30 > e60
            and
            e60 > e120
        ):

            return result

        result[
            "state"
        ] = "long"

        # 현재가가 EMA30 이하이면 매수 후보
        if current_price <= e30:

            result[
                "qualified"
            ] = True

            result[
                "display"
            ] = "🟢 매수 후보"

        return result

    except Exception as e:

        log.error(
            f"매수 후보 분석 오류: {e}"
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
            "market":
                market,

            "count":
                2
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
            data[0][
                "trade_price"
            ]
        )

        previous = float(
            data[1][
                "trade_price"
            ]
        )

        if previous == 0:

            return None

        return [

            (
                current
                -
                previous
            )
            /
            previous
            *
            100

        ]

    except Exception:

        return None


def daily_changes(df):

    if (
        df is None
        or df.empty
    ):

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
            .set_index(
                "datetime"
            )
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
                current
                -
                previous
            )
            /
            previous
            *
            100

        ]

    except Exception:

        return None


def get_change_value(change):

    if change is None:

        return None

    try:

        value = float(

            change[0]
            if isinstance(
                change,
                (list, tuple)
            )
            else change

        )

        return value

    except Exception:

        return None


def format_change(x):

    value = get_change_value(
        x
    )

    if value is None:

        return "-"

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


def format_volume(v):

    if v is None:

        return "-"

    try:

        v = float(v)

    except Exception:

        return "-"

    if v >= 1e12:

        return (
            f"{v / 1e12:.2f}조"
        )

    if v >= 1e8:

        return (
            f"{v / 1e8:.0f}억"
        )

    if v >= 1e4:

        return (
            f"{v / 1e4:.0f}만"
        )

    return f"{v:,.0f}"


# =========================================================
# 빈 분석
# =========================================================

def empty_analysis():

    e = {

        "display":
            "⚪(0)",

        "direction":
            "none",

        "count":
            0,

        "current_price":
            None

    }

    return {

        "ema_1h":
            e.copy(),

        "buy_candidate": {

            "state":
                "none",

            "display":
                "-",

            "ema30":
                None,

            "ema60":
                None,

            "ema120":
                None,

            "current_price":
                None,

            "qualified":
                False

        },

        "changes":
            None,

        "qualified":
            False,

        "direction_1h":
            "none",

        "df1h":
            None

    }


# =========================================================
# 핵심 분석
# =========================================================

def analyze(
    market,
    okx=False,
    current_price=None
):

    if okx:

        bar = get_okx_bar(
            EMA_TIMEFRAME
        )

        if bar is None:

            log.error(
                f"지원하지 않는 OKX "
                f"시간봉: {EMA_TIMEFRAME}"
            )

            return None

        df1 = history_okx(
            market,
            bar
        )

    else:

        df1 = history_upbit(
            market,
            EMA_TIMEFRAME
        )

    if (
        df1 is None
        or df1.empty
    ):

        return None

    e1 = ema_display(
        df1,
        current_price
    )

    buy_candidate = buy_candidate_analysis(
        df1,
        current_price
    )

    changes = (
        daily_changes(df1)
        if okx
        else daily_change_upbit(
            market
        )
    )

    return {

        "ema_1h":
            e1,

        "buy_candidate":
            buy_candidate,

        "changes":
            changes,

        "qualified":
            buy_candidate.get(
                "qualified",
                False
            ),

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
    analysis,
    current_price=None
):

    a = (
        analysis
        or empty_analysis()
    )

    # 분석 결과에 현재가가 있으면 우선 사용
    if current_price is None:

        current_price = (
            a.get(
                "buy_candidate",
                {}
            ).get(
                "current_price"
            )
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

        "change_value":
            get_change_value(
                a["changes"]
            ),

        "volume":
            format_volume(
                volume
            ),

        "current_price":
            current_price,

        "ema_1h":
            a["ema_1h"],

        "buy_candidate":
            a.get(
                "buy_candidate",
                {}
            ),

        "qualified":
            a["qualified"],

        "direction":
            a.get(
                "direction_1h",
                "none"
            )

    }


# =========================================================
# 매수 / 숏 리스트 조건
#
# 매수:
# 정배열
# + EMA1 카운트 <= 100
# + 현재가 <= EMA30
# + 당일 변동률 +
#
# 숏:
# 역배열
# + EMA1 카운트 <= 100
# + 당일 변동률 -
# =========================================================

def is_upbit_buy_candidate(row):

    if not row:

        return False

    change = row.get(
        "change_value"
    )

    direction_value = row.get(
        "direction",
        "none"
    )

    ema1_count = row.get(
        "ema_1h",
        {}
    ).get(
        "count",
        0
    )

    buy_data = row.get(
        "buy_candidate",
        {}
    )

    current_price = buy_data.get(
        "current_price"
    )

    ema30 = buy_data.get(
        "ema30"
    )

    if (
        current_price is None
        or
        ema30 is None
    ):

        return False

    return (

        direction_value == "long"

        and

        ema1_count <= EMA1_MAX_COUNT

        and

        current_price <= ema30

        and

        change is not None

        and

        change > 0

    )


def is_okx_long_candidate(row):

    if not row:

        return False

    change = row.get(
        "change_value"
    )

    direction_value = row.get(
        "direction",
        "none"
    )

    ema1_count = row.get(
        "ema_1h",
        {}
    ).get(
        "count",
        0
    )

    buy_data = row.get(
        "buy_candidate",
        {}
    )

    current_price = buy_data.get(
        "current_price"
    )

    ema30 = buy_data.get(
        "ema30"
    )

    if (
        current_price is None
        or
        ema30 is None
    ):

        return False

    return (

        direction_value == "long"

        and

        ema1_count <= EMA1_MAX_COUNT

        and

        current_price <= ema30

        and

        change is not None

        and

        change > 0

    )


def is_okx_short_candidate(row):

    if not row:

        return False

    change = row.get(
        "change_value"
    )

    direction_value = row.get(
        "direction",
        "none"
    )

    ema1_count = row.get(
        "ema_1h",
        {}
    ).get(
        "count",
        0
    )

    return (

        direction_value == "short"

        and

        ema1_count <= EMA1_MAX_COUNT

        and

        change is not None

        and

        change < 0

    )


# =========================================================
# Upbit 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update_time

    log.info(
        f"========== "
        f"업비트 TOP{TOP_N} 시작 "
        f"=========="
    )

    markets = get_upbit_markets()

    markets.sort(
        key=lambda x:
            x["volume_24h"],
        reverse=True
    )

    rows = []

    for rank, item in enumerate(
        markets[:TOP_N],
        1
    ):

        market = item[
            "market"
        ]

        coin = market.replace(
            "KRW-",
            ""
        )

        current_price = item.get(
            "current_price"
        )

        try:

            a = analyze(
                market,
                current_price=current_price
            )

            rows.append(
                make_row(
                    rank,
                    coin,
                    item[
                        "volume_24h"
                    ],
                    a,
                    current_price
                )
            )

        except Exception as e:

            log.error(
                f"업비트 상세 오류 "
                f"{market}: {e}"
            )

            rows.append(
                make_row(
                    rank,
                    coin,
                    item[
                        "volume_24h"
                    ],
                    None,
                    current_price
                )
            )

    latest_upbit_data = rows

    latest_upbit_update_time = (
        kst()
    )

    buy_rows = [

        x
        for x in rows
        if is_upbit_buy_candidate(x)

    ]

    buy_count = len(
        buy_rows
    )

    log.info(
        f"업비트 완료 / "
        f"매수후보 "
        f"(정배열 + EMA1≤{EMA1_MAX_COUNT} "
        f"+ 현재가≤EMA30 + 변동률+) "
        f"{buy_count}개"
    )


# =========================================================
# OKX
# =========================================================

def get_okx_symbols():

    r = retry(
        requests.get,
        "https://www.okx.com/api/v5/public/instruments",
        params={
            "instType":
                "SWAP"
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

            and
            x.get(
                "state"
            )
            ==
            "live"

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

    if (
        df is None
        or df.empty
    ):

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
            *
            float(usdt)
        )

    except Exception:

        return None


def update_okx(usdt):

    global latest_okx_data
    global latest_okx_update_time

    if (
        not usdt
        or
        usdt <= 0
    ):

        return False

    symbols = (
        get_okx_symbols()
    )

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

        if (
            v
            and
            v > 0
        ):

            volumes[
                symbol
            ] = v

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

            current_price = (
                get_okx_current_price(
                    symbol
                )
            )

            a = analyze(
                symbol,
                True,
                current_price=current_price
            )

            rows.append(
                make_row(
                    rank,
                    name,
                    volumes[
                        symbol
                    ],
                    a,
                    current_price
                )
            )

        except Exception as e:

            log.error(
                f"OKX 상세 오류 "
                f"{symbol}: {e}"
            )

            rows.append(
                make_row(
                    rank,
                    name,
                    volumes[
                        symbol
                    ],
                    None,
                    current_price
                )
            )

    latest_okx_data = rows

    latest_okx_update_time = (
        kst()
    )

    long_rows = [

        x
        for x in rows
        if is_okx_long_candidate(x)

    ]

    short_rows = [

        x
        for x in rows
        if is_okx_short_candidate(x)

    ]

    long_count = len(
        long_rows
    )

    short_count = len(
        short_rows
    )

    log.info(
        f"OKX 완료 / "
        f"롱 {long_count}개 / "
        f"숏 {short_count}개 "
        f"(둘 다 EMA1≤{EMA1_MAX_COUNT})"
    )

    return True


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    global latest_usdt_krw
    global latest_upbit_data
    global latest_okx_data

    if not update_lock.acquire(
        False
    ):

        log.warning(
            "이전 조회 진행 중 "
            "→ 건너뜀"
        )

        return

    try:

        log.info(
            f"========== "
            f"전체 조회 {kst()} "
            f"=========="
        )

        if USE_UPBIT == "Y":

            try:

                update_upbit()

            except Exception as e:

                log.exception(
                    f"업비트 업데이트 오류: "
                    f"{e}"
                )

        else:

            latest_upbit_data = []

        if USE_OKX == "Y":

            try:

                usdt = (
                    get_usdt_krw()
                )

                if usdt:

                    latest_usdt_krw = (
                        usdt
                    )

                else:

                    usdt = (
                        latest_usdt_krw
                    )

                if usdt > 0:

                    update_okx(
                        usdt
                    )

            except Exception as e:

                log.exception(
                    f"OKX 업데이트 오류: "
                    f"{e}"
                )

        else:

            latest_okx_data = []

    finally:

        update_lock.release()


# =========================================================
# 매수 후보 HTML
# =========================================================

def buy_candidate_html(data):

    if not data:

        return (
            '<div class="buy-none">'
            '-'
            '</div>'
        )

    if data.get(
        "qualified",
        False
    ):

        return (
            '<div class="buy-stage buy-candidate">'
            '🟢 매수 후보'
            '</div>'
        )

    return (
        '<div class="buy-none">'
        '-'
        '</div>'
    )


# =========================================================
# EMA1 표시 HTML
# =========================================================

def ema_html(e):

    if not e:

        return """
        <div class="ema1-cell">
            <div class="ema1-main ema-none">
                ⚪(0)
            </div>
        </div>
        """

    direction_value = e.get(
        "direction",
        "none"
    )

    count = e.get(
        "count",
        0
    )

    cls = {

        "long":
            "ema-long",

        "short":
            "ema-short"

    }.get(
        direction_value,
        "ema-none"
    )

    if direction_value == "none":

        count = 0

    if direction_value == "long":

        icon = "🟢"

    elif direction_value == "short":

        icon = "🔴"

    else:

        icon = "⚪"

    return f"""
    <div class="ema1-cell">
        <div class="ema1-main {cls}">
            {icon}({count})
        </div>
    </div>
    """


# =========================================================
# Rows
# =========================================================

def rows_html(data):

    out = []

    timeframe_label = (
        format_timeframe(
            EMA_TIMEFRAME
        )
    )

    for x in data:

        cls = (

            " qualified"

            if x.get(
                "qualified",
                False
            )

            else ""

        )

        buy_data = x.get(
            "buy_candidate",
            {}
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
                            {timeframe_label}
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

                    {buy_candidate_html(
                        buy_data
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

def table_html(data):

    rows = rows_html(
        data
    )

    if not rows:

        rows = """
        <tr>
            <td
                colspan="5"
                class="empty"
            >
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
                    <th>매수</th>

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
# 매수 후보 리스트
# =========================================================

def buy_focus_section(
    data,
    update_time,
    exchange="upbit"
):

    if exchange == "upbit":

        candidate_rows = [

            x
            for x in data
            if is_upbit_buy_candidate(x)

        ]

    else:

        candidate_rows = [

            x
            for x in data
            if is_okx_long_candidate(x)

        ]

    if not candidate_rows:

        rows = """
        <tr>

            <td
                colspan="5"
                class="empty"
            >
                현재 매수 후보 없음
            </td>

        </tr>
        """

    else:

        rows = rows_html(
            candidate_rows
        )

    return f"""
    <h2
        class="focus-title buy-title"
    >

        🟢 매수 후보

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
                    <th>EMA1</th>
                    <th>매수</th>

                </tr>

            </thead>

            <tbody>

                {rows}

            </tbody>

        </table>

    </div>
    """


# =========================================================
# OKX 숏
# =========================================================

def okx_short_section(
    data,
    update_time
):

    short_rows = [

        x
        for x in data
        if is_okx_short_candidate(x)

    ]

    if not short_rows:

        rows = """
        <tr>
            <td
                colspan="5"
                class="empty"
            >
                현재 숏 후보 없음
            </td>
        </tr>
        """

    else:

        rows = rows_html(
            short_rows
        )

    return f"""
    <h2
        class="focus-title short-title"
    >

        🔴 OKX 숏 후보

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
                    <th>EMA1</th>
                    <th>매수</th>

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

.table-wrap{
    width:100%;
    overflow:hidden;
    border-radius:9px;
    border:1px solid #282e36;
    background:#171b20;

    box-shadow:
        0 2px 8px rgba(0,0,0,.18);
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
    width:27%;
}

th:nth-child(5),
td:nth-child(5){
    width:27%;
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
    font-weight:800;
}

.down{
    color:#ff5555;
    font-weight:800;
}

.zero{
    color:#8c929a;
}

.vol{
    padding:3px 1px !important;
    font-size:8px;
    font-weight:800;
    line-height:18px;
    height:38px;
    white-space:nowrap;
}

.ema-cell{
    overflow:hidden;
    padding:2px 1px !important;
}

.ema-row{
    display:flex;
    align-items:center;
    justify-content:center;
    width:100%;
    min-height:34px;
    white-space:nowrap;
    overflow:hidden;
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

.ema1-cell{
    display:flex;
    align-items:center;
    justify-content:center;
    width:100%;
    min-width:0;
    height:34px;
    line-height:1.1;
    white-space:nowrap;
    overflow:hidden;
}

.ema1-main{
    display:block;
    width:100%;
    font-size:8px;
    font-weight:800;
    line-height:14px;
    text-align:center;
    white-space:nowrap;
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

.close-ema10{
    text-align:center !important;
    vertical-align:middle !important;
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
    color:#39e875;
}

.buy-none{
    color:#686f78;
    font-size:8px;
    font-weight:700;
    text-align:center;
}

.qualified{
    background:rgba(57,232,117,.055);
}

.focus-title{
    margin-top:12px;
    margin-bottom:5px;
    padding-left:3px;
}

.buy-title{
    color:#39e875;
}

.short-title{
    color:#ff5555;
}

.buy-focus-table{
    border:1px solid #303740;
}

.empty{
    color:#555d67;
    padding:14px 5px !important;
    font-size:8px;
    height:48px;
}

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
        padding:3px 1px;
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
        padding:2px 0 !important;
    }

    .ema-row{
        min-height:34px;
    }

    .tf{
        flex:0 0 18px;
        width:18px;
        font-size:6px;
    }

    .ema1-cell{
        height:34px;
    }

    .ema1-main{
        font-size:7px;
        line-height:14px;
    }

    .close-ema10{
        font-size:7px;
    }

    .buy-stage{
        font-size:7px;
        line-height:18px;
    }

    .buy-none{
        font-size:7px;
    }

    .empty{
        padding:13px 4px !important;
        font-size:7px;
        height:45px;
    }
}

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
        height:40px;
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

    .tf{
        font-size:5.5px;
    }

    .ema1-main{
        font-size:6px;
        line-height:13px;
    }

    .buy-stage{
        font-size:6px;
    }

    .buy-none{
        font-size:6px;
    }
}

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
        height:44px;
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

    .ema1-main{
        font-size:9px;
    }

    .buy-stage{
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

    timeframe_label = (
        format_timeframe(
            EMA_TIMEFRAME
        )
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

    if USE_UPBIT == "Y":

        sections += (
            buy_focus_section(
                latest_upbit_data,
                latest_upbit_update_time,
                "upbit"
            )
        )

    if USE_OKX == "Y":

        sections += (
            buy_focus_section(
                latest_okx_data,
                latest_okx_update_time,
                "okx"
            )
        )

        sections += (
            okx_short_section(
                latest_okx_data,
                latest_okx_update_time
            )
        )

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
            {timeframe_label}
            EMA30·60·120
        </title>

        <style>
            {CSS}
        </style>

    </head>

    <body>

        <h1>
            📊 EMA 전략
        </h1>

        <div class="info">

            {timeframe_label} 확정 캔들

            <br>

            EMA1 :
            EMA30 · EMA60 · EMA120

            <br>

            🟢 매수 :
            정배열 · 카운트 ≤ {EMA1_MAX_COUNT}
            · 현재가 ≤ EMA30 · 등락률 +

            <br>

            🔴 숏 :
            역배열 · 카운트 ≤ {EMA1_MAX_COUNT}
            · 등락률 -

            <br>

            ※ EMA1은 확정 캔들 기준

            <br>

            ※ 현재가는 실시간 가격

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
            "USE_UPBIT은 "
            "Y 또는 N만 가능합니다."
        )

    if USE_OKX not in (
        "Y",
        "N"
    ):

        raise ValueError(
            "USE_OKX는 "
            "Y 또는 N만 가능합니다."
        )

    validate_timeframe()

    timeframe_label = (
        format_timeframe(
            EMA_TIMEFRAME
        )
    )

    okx_bar = get_okx_bar(
        EMA_TIMEFRAME
    )

    log.info(
        "========================================"
    )

    log.info(
        f"{timeframe_label} "
        "EMA30·60·120 매수 후보 시스템 시작"
    )

    log.info(
        f"업비트={USE_UPBIT} / "
        f"OKX={USE_OKX}"
    )

    log.info(
        f"TOP={TOP_N} / "
        f"UPDATE={UPDATE_MINUTES}분"
    )

    log.info(
        f"EMA 분석 시간봉 = "
        f"{timeframe_label}"
    )

    log.info(
        f"Upbit 시간봉 = "
        f"{EMA_TIMEFRAME}분"
    )

    log.info(
        f"OKX 시간봉 = "
        f"{okx_bar}"
    )

    log.info(
        "EMA1 = "
        "EMA30-60-120"
    )

    log.info(
        "정배열 = "
        "EMA30 > EMA60 > EMA120"
    )

    log.info(
        "역배열 = "
        "EMA30 < EMA60 < EMA120"
    )

    log.info(
        f"후보 카운트 제한 = "
        f"{EMA1_MAX_COUNT} 이하"
    )

    log.info(
        "매수 = "
        "정배열 + 카운트 제한 "
        "+ 현재가 <= EMA30 "
        "+ 등락률 > 0"
    )

    log.info(
        "숏 = "
        "역배열 + 카운트 제한 "
        "+ 등락률 < 0"
    )

    log.info(
        f"{timeframe_label} "
        "확정 캔들만 EMA 계산에 사용"
    )

    log.info(
        "현재가는 실시간 현재가 사용"
    )

    log.info(
        "1차 / 2차 / 3차 분할매수 = 삭제"
    )

    log.info(
        "EMA120 이격률 계산/표시 = 삭제"
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
