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
# 설정값
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

# 1시간 EMA10
EMA10_PERIOD = 10


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
air_state_lock = threading.Lock()

last_request_time = 0

air_state = {}


# =========================================================
# 시간
# =========================================================

def kst():
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


# =========================================================
# 요청 제한
# =========================================================

def wait_request():
    global last_request_time

    with request_lock:
        now = time.time()

        elapsed = now - last_request_time

        if elapsed < REQUEST_INTERVAL:
            time.sleep(
                REQUEST_INTERVAL - elapsed
            )

        last_request_time = time.time()


# =========================================================
# requests 재시도
# =========================================================

def retry(method, url, **kwargs):

    for attempt in range(MAX_RETRIES):

        try:

            wait_request()

            r = requests.request(
                method,
                url,
                timeout=10,
                **kwargs
            )

            if r.status_code == 429:

                log.warning(
                    "429 Rate Limit: %s",
                    url
                )

                time.sleep(
                    RATE_LIMIT_WAIT
                )

                continue

            if r.status_code >= 500:

                time.sleep(
                    min(
                        RATE_LIMIT_WAIT * (attempt + 1),
                        20
                    )
                )

                continue

            r.raise_for_status()

            return r

        except Exception as e:

            log.warning(
                "Request error %s/%s: %s",
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

    global latest_upbit_markets

    url = (
        "https://api.upbit.com/v1/ticker/"
        "all?quote_currencies=KRW"
    )

    r = retry(
        "GET",
        url
    )

    if r is None:
        return []

    try:

        data = r.json()

        markets = []

        for x in data:

            market = x.get("market", "")

            if not market.startswith("KRW-"):
                continue

            volume = float(
                x.get(
                    "acc_trade_price_24h",
                    0
                ) or 0
            )

            if volume <= 0:
                continue

            markets.append(
                market
            )

        latest_upbit_markets = markets

        return markets

    except Exception as e:

        log.error(
            "get_upbit_markets: %s",
            e
        )

        return []


# =========================================================
# USDT/KRW
# =========================================================

def get_usdt_krw():

    url = (
        "https://api.upbit.com/v1/ticker"
        "?markets=KRW-USDT"
    )

    r = retry(
        "GET",
        url
    )

    if r is None:
        return 0

    try:

        data = r.json()

        if not data:
            return 0

        return float(
            data[0].get(
                "trade_price",
                0
            ) or 0
        )

    except Exception as e:

        log.error(
            "get_usdt_krw: %s",
            e
        )

        return 0


# =========================================================
# OKX 캔들
# =========================================================

def get_okx_ohlcv(
    inst,
    bar="1H",
    limit=200,
    before=None
):

    url = (
        "https://www.okx.com/api/v5/market/candles"
    )

    params = {
        "instId": inst,
        "bar": bar,
        "limit": limit
    }

    if before is not None:
        params["before"] = before

    r = retry(
        "GET",
        url,
        params=params
    )

    if r is None:
        return pd.DataFrame()

    try:

        data = r.json()

        if data.get("code") != "0":
            return pd.DataFrame()

        rows = data.get(
            "data",
            []
        )

        if not rows:
            return pd.DataFrame()

        result = []

        now = datetime.now(KST)

        for row in rows:

            if len(row) < 9:
                continue

            ts = int(row[0])

            dt = datetime.fromtimestamp(
                ts / 1000,
                tz=KST
            )

            confirm = str(
                row[8]
            )

            if confirm != "1":
                continue

            # 현재 진행 중인 1시간봉 제외
            if bar == "1H":

                if (
                    dt.year == now.year
                    and dt.month == now.month
                    and dt.day == now.day
                    and dt.hour == now.hour
                ):
                    continue

            # 현재 진행 중인 4시간봉 제외
            elif bar == "4H":

                current_block = (
                    now.hour // 4
                ) * 4

                if (
                    dt.year == now.year
                    and dt.month == now.month
                    and dt.day == now.day
                    and dt.hour == current_block
                ):
                    continue

            result.append(
                {
                    "datetime": dt,
                    "o": float(row[1]),
                    "h": float(row[2]),
                    "l": float(row[3]),
                    "c": float(row[4]),
                    "vol": float(row[5]),
                    "volCcyQuote": float(row[7])
                }
            )

        if not result:
            return pd.DataFrame()

        df = pd.DataFrame(
            result
        )

        df = (
            df.drop_duplicates(
                subset=["datetime"]
            )
            .sort_values("datetime")
            .reset_index(drop=True)
        )

        return df

    except Exception as e:

        log.error(
            "get_okx_ohlcv %s: %s",
            inst,
            e
        )

        return pd.DataFrame()


# =========================================================
# Upbit 1시간
# =========================================================

def get_upbit_1h(
    market,
    count=200,
    to=None
):

    url = (
        "https://api.upbit.com/v1/candles/"
        "minutes/60"
    )

    params = {
        "market": market,
        "count": count
    }

    if to is not None:
        params["to"] = to

    r = retry(
        "GET",
        url,
        params=params
    )

    if r is None:
        return pd.DataFrame()

    try:

        data = r.json()

        if not isinstance(data, list):
            return pd.DataFrame()

        result = []

        now = datetime.now(KST)

        for x in data:

            dt = datetime.fromisoformat(
                x["candle_date_time_kst"]
            ).replace(
                tzinfo=KST
            )

            # 현재 진행 중인 1시간봉 제외
            if (
                dt.year == now.year
                and dt.month == now.month
                and dt.day == now.day
                and dt.hour == now.hour
            ):
                continue

            result.append(
                {
                    "datetime": dt,
                    "o": float(
                        x["opening_price"]
                    ),
                    "h": float(
                        x["high_price"]
                    ),
                    "l": float(
                        x["low_price"]
                    ),
                    "c": float(
                        x["trade_price"]
                    ),
                    "volume_krw": float(
                        x["candle_acc_trade_price"]
                    )
                }
            )

        if not result:
            return pd.DataFrame()

        df = pd.DataFrame(
            result
        )

        return (
            df.drop_duplicates(
                subset=["datetime"]
            )
            .sort_values("datetime")
            .reset_index(drop=True)
        )

    except Exception as e:

        log.error(
            "get_upbit_1h %s: %s",
            market,
            e
        )

        return pd.DataFrame()


# =========================================================
# Upbit 4시간
# =========================================================

def get_upbit_4h(
    market,
    count=200,
    to=None
):

    url = (
        "https://api.upbit.com/v1/candles/"
        "minutes/240"
    )

    params = {
        "market": market,
        "count": count
    }

    if to is not None:
        params["to"] = to

    r = retry(
        "GET",
        url,
        params=params
    )

    if r is None:
        return pd.DataFrame()

    try:

        data = r.json()

        if not isinstance(data, list):
            return pd.DataFrame()

        result = []

        now = datetime.now(KST)

        current_block = (
            now.hour // 4
        ) * 4

        for x in data:

            dt = datetime.fromisoformat(
                x["candle_date_time_kst"]
            ).replace(
                tzinfo=KST
            )

            # 현재 진행 중인 4시간봉 제외
            if (
                dt.year == now.year
                and dt.month == now.month
                and dt.day == now.day
                and dt.hour == current_block
            ):
                continue

            result.append(
                {
                    "datetime": dt,
                    "o": float(
                        x["opening_price"]
                    ),
                    "h": float(
                        x["high_price"]
                    ),
                    "l": float(
                        x["low_price"]
                    ),
                    "c": float(
                        x["trade_price"]
                    )
                }
            )

        if not result:
            return pd.DataFrame()

        df = pd.DataFrame(
            result
        )

        return (
            df.drop_duplicates(
                subset=["datetime"]
            )
            .sort_values("datetime")
            .reset_index(drop=True)
        )

    except Exception as e:

        log.error(
            "get_upbit_4h %s: %s",
            market,
            e
        )

        return pd.DataFrame()


# =========================================================
# OKX 과거 데이터
# =========================================================

def history_okx(
    inst,
    bar,
    required=125
):

    frames = []

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

        if len(
            pd.concat(
                frames,
                ignore_index=True
            )
        ) >= required:
            break

        oldest = df["datetime"].min()

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

    df = pd.concat(
        frames,
        ignore_index=True
    )

    return (
        df.drop_duplicates(
            subset=["datetime"]
        )
        .sort_values("datetime")
        .tail(required)
        .reset_index(drop=True)
    )


# =========================================================
# Upbit 과거 데이터
# =========================================================

def history_upbit(
    market,
    unit,
    required=125
):

    frames = []

    to = None

    for _ in range(MAX_HISTORY_CHUNKS):

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

        if df.empty:
            break

        frames.append(df)

        combined = pd.concat(
            frames,
            ignore_index=True
        )

        if len(combined) >= required:
            break

        oldest = df["datetime"].min()

        to = oldest.strftime(
            "%Y-%m-%dT%H:%M:%S"
        )

        time.sleep(
            REQUEST_INTERVAL
        )

    if not frames:
        return pd.DataFrame()

    df = pd.concat(
        frames,
        ignore_index=True
    )

    return (
        df.drop_duplicates(
            subset=["datetime"]
        )
        .sort_values("datetime")
        .tail(required)
        .reset_index(drop=True)
    )


def history_upbit_4h(
    market
):

    return history_upbit(
        market,
        240,
        125
    )


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
        or "c" not in df.columns
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
# EMA 10 / 30 / 60 / 120 방향
# =========================================================

def direction(df):

    if (
        df is None
        or df.empty
        or len(df) < 1
    ):
        return "none"

    try:

        e10 = ema(df, 10)
        e30 = ema(df, 30)
        e60 = ema(df, 60)
        e120 = ema(df, 120)

        a = float(e10.iloc[-1])
        b = float(e30.iloc[-1])
        c = float(e60.iloc[-1])
        d = float(e120.iloc[-1])

        if (
            a > b
            and b > c
            and c > d
        ):
            return "long"

        if (
            a < b
            and b < c
            and c < d
        ):
            return "short"

        return "none"

    except Exception:

        return "none"


# =========================================================
# EMA 표시
# =========================================================

def ema_display(df):

    d = direction(df)

    if d == "long":
        return {
            "display": "🟢",
            "direction": "long"
        }

    if d == "short":
        return {
            "display": "🔴",
            "direction": "short"
        }

    return {
        "display": "⚪",
        "direction": "none"
    }


# =========================================================
# 1H 종가 vs EMA10
# =========================================================

def ema10_position(df):

    if (
        df is None
        or df.empty
        or "c" not in df.columns
    ):
        return "none"

    try:

        e10 = ema(
            df,
            EMA10_PERIOD
        )

        if e10 is None:
            return "none"

        close = float(
            df["c"].iloc[-1]
        )

        ema10_value = float(
            e10.iloc[-1]
        )

        if close > ema10_value:
            return "above"

        if close < ema10_value:
            return "below"

        return "equal"

    except Exception:

        return "none"


# =========================================================
# 1H 종가 vs EMA10 표시
# =========================================================

def ema10_position_text(df):

    position = ema10_position(
        df
    )

    if position == "above":

        return {
            "display": "▲ 위",
            "position": "above"
        }

    if position == "below":

        return {
            "display": "▼ 아래",
            "position": "below"
        }

    if position == "equal":

        return {
            "display": "= 동일",
            "position": "equal"
        }

    return {
        "display": "-",
        "position": "none"
    }


# =========================================================
# LONG 경고
# =========================================================

def get_air_warning(
    df1h,
    df4h
):

    if (
        df1h is None
        or df4h is None
        or df1h.empty
        or df4h.empty
    ):
        return None

    if len(df1h) < 2:
        return None

    try:

        # ---------------------------------------------
        # 현재 1H 방향
        # ---------------------------------------------

        direction_1h = direction(
            df1h
        )

        # ---------------------------------------------
        # 현재 4H 방향
        # ---------------------------------------------

        direction_4h = direction(
            df4h
        )

        if direction_1h != "long":
            return None

        if direction_4h != "long":
            return None

        # ---------------------------------------------
        # 1H EMA10
        # ---------------------------------------------

        e10 = ema(
            df1h,
            10
        )

        if e10 is None:
            return None

        previous_close = float(
            df1h["c"].iloc[-2]
        )

        previous_ema10 = float(
            e10.iloc[-2]
        )

        current_open = float(
            df1h["o"].iloc[-1]
        )

        current_close = float(
            df1h["c"].iloc[-1]
        )

        current_ema10 = float(
            e10.iloc[-1]
        )

        # ---------------------------------------------
        # 이전 1H 종가가 EMA10 아래
        # ---------------------------------------------

        if not (
            previous_close
            < previous_ema10
        ):
            return None

        # ---------------------------------------------
        # 현재 1H 완성봉 양봉
        # ---------------------------------------------

        if not (
            current_close
            > current_open
        ):
            return None

        # ---------------------------------------------
        # 현재 종가가 EMA10 위
        # ---------------------------------------------

        if not (
            current_close
            > current_ema10
        ):
            return None

        return "LONG"

    except Exception as e:

        log.error(
            "get_air_warning: %s",
            e
        )

        return None


# =========================================================
# 비행기 카운터
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
            "direction": None,
            "count": 0
        }

    current_candle = (
        df1h["datetime"].iloc[-1]
    )

    with air_state_lock:

        state = air_state.get(
            market
        )

        # ---------------------------------------------
        # 새 경고 발생
        # ---------------------------------------------

        if new_warning:

            if (
                state is None
                or not state.get(
                    "active",
                    False
                )
                or state.get(
                    "warning_candle"
                ) != current_candle
            ):

                air_state[market] = {
                    "active": True,
                    "direction": new_warning,
                    "count": 0,
                    "warning_candle": current_candle,
                    "counted_candle": current_candle
                }

                state = air_state[
                    market
                ]

        # ---------------------------------------------
        # 활성 상태가 없으면 종료
        # ---------------------------------------------

        if (
            state is None
            or not state.get(
                "active",
                False
            )
        ):

            return {
                "active": False,
                "direction": None,
                "count": 0
            }

        # ---------------------------------------------
        # 같은 캔들이면 중복 카운트 금지
        # ---------------------------------------------

        if (
            state.get(
                "counted_candle"
            ) == current_candle
        ):

            return {
                "active": True,
                "direction": state.get(
                    "direction"
                ),
                "count": state.get(
                    "count",
                    0
                )
            }

        # ---------------------------------------------
        # 다음 완성봉
        # ---------------------------------------------

        current_open = float(
            df1h["o"].iloc[-1]
        )

        current_close = float(
            df1h["c"].iloc[-1]
        )

        # LONG 카운터
        if state.get(
            "direction"
        ) == "LONG":

            # 양봉이면 카운트
            if current_close > current_open:

                state["count"] += 1

                state[
                    "counted_candle"
                ] = current_candle

            else:

                state["active"] = False

        # 현재 코드에서는 SHORT 경고 발생 로직 없음
        else:

            if current_close < current_open:

                state["count"] += 1

                state[
                    "counted_candle"
                ] = current_candle

            else:

                state["active"] = False

        return {
            "active": state.get(
                "active",
                False
            ),
            "direction": state.get(
                "direction"
            ),
            "count": state.get(
                "count",
                0
            )
        }


# =========================================================
# 일간 등락률
# =========================================================

def daily_change_upbit(
    market
):

    url = (
        "https://api.upbit.com/v1/candles/days"
    )

    params = {
        "market": market,
        "count": 2
    }

    r = retry(
        "GET",
        url,
        params=params
    )

    if r is None:
        return None

    try:

        data = r.json()

        if len(data) < 2:
            return None

        # 가장 최근 완성 일봉
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

        return (
            (current - previous)
            / previous
            * 100
        )

    except Exception:

        return None


# =========================================================
# 1H 데이터 일간 등락률
# =========================================================

def daily_changes(df):

    if (
        df is None
        or df.empty
        or "c" not in df.columns
    ):
        return None

    try:

        x = df.copy()

        x["datetime"] = pd.to_datetime(
            x["datetime"]
        )

        x = x.set_index(
            "datetime"
        )

        daily = (
            x["c"]
            .resample(
                "1D",
                offset="9h"
            )
            .agg(
                ["first", "last"]
            )
            .dropna()
        )

        if len(daily) < 2:
            return None

        previous = float(
            daily["last"].iloc[-2]
        )

        current = float(
            daily["last"].iloc[-1]
        )

        if previous == 0:
            return None

        return (
            (current - previous)
            / previous
            * 100
        )

    except Exception:

        return None


# =========================================================
# 등락률 HTML
# =========================================================

def format_change(
    x
):

    if x is None:
        return "-"

    try:

        x = float(x)

        if x > 0:

            return (
                '<span class="change-up">'
                f'▲ +{x:.2f}%'
                '</span>'
            )

        if x < 0:

            return (
                '<span class="change-down">'
                f'▼ {x:.2f}%'
                '</span>'
            )

        return (
            '<span class="change-zero">'
            '0.00%'
            '</span>'
        )

    except Exception:

        return "-"


# =========================================================
# 거래대금
# =========================================================

def format_volume(
    v
):

    if v is None:
        return "-"

    try:

        v = float(v)

        if v >= 1_000_000_000_000:

            return (
                f"{v / 1_000_000_000_000:.2f}조"
            )

        if v >= 100_000_000:

            return (
                f"{v / 100_000_000:.0f}억"
            )

        if v >= 10_000:

            return (
                f"{v / 10_000:.0f}만"
            )

        return f"{v:,.0f}"

    except Exception:

        return "-"


# =========================================================
# 빈 분석 결과
# =========================================================

def empty_analysis():

    return {
        "ema_1h": {
            "display": "⚪",
            "direction": "none"
        },

        "ema_4h": {
            "display": "⚪",
            "direction": "none"
        },

        "changes": None,

        "air_warning": False,
        "air_direction": None,
        "air_count": 0,
        "air_active": False,

        "qualified": False,

        "direction_1h": "none",
        "direction_4h": "none",

        "ema10_position": {
            "display": "-",
            "position": "none"
        },

        "df1h": None
    }


# =========================================================
# 종합 분석
# =========================================================

def analyze(
    market,
    okx=False
):

    try:

        # ---------------------------------------------
        # 캔들
        # ---------------------------------------------

        if okx:

            df1h = history_okx(
                market,
                "1H",
                125
            )

            df4h = history_okx(
                market,
                "4H",
                125
            )

        else:

            df1h = history_upbit(
                market,
                60,
                125
            )

            df4h = history_upbit_4h(
                market
            )

        if (
            df1h.empty
            or df4h.empty
        ):

            return empty_analysis()

        # ---------------------------------------------
        # EMA
        # ---------------------------------------------

        ema_1h = ema_display(
            df1h
        )

        ema_4h = ema_display(
            df4h
        )

        # ---------------------------------------------
        # 방향
        # ---------------------------------------------

        direction_1h = direction(
            df1h
        )

        direction_4h = direction(
            df4h
        )

        # ---------------------------------------------
        # 1H 종가 vs EMA10
        # ---------------------------------------------

        ema10_pos = ema10_position_text(
            df1h
        )

        # ---------------------------------------------
        # LONG 경고
        # ---------------------------------------------

        warning = get_air_warning(
            df1h,
            df4h
        )

        # ---------------------------------------------
        # 카운터
        # ---------------------------------------------

        counter = update_air_counter(
            market,
            df1h,
            warning
        )

        # ---------------------------------------------
        # 등락률
        # ---------------------------------------------

        if okx:

            changes = daily_changes(
                df1h
            )

        else:

            changes = daily_change_upbit(
                market
            )

        # ---------------------------------------------
        # 조건
        # ---------------------------------------------

        qualified = (
            direction_1h == "long"
            and
            direction_4h == "long"
        )

        return {

            "ema_1h": ema_1h,

            "ema_4h": ema_4h,

            "changes": changes,

            "air_warning": (
                warning is not None
            ),

            "air_direction": counter.get(
                "direction"
            ),

            "air_count": counter.get(
                "count",
                0
            ),

            "air_active": counter.get(
                "active",
                False
            ),

            "qualified": qualified,

            "direction_1h": direction_1h,

            "direction_4h": direction_4h,

            "ema10_position": ema10_pos,

            "df1h": df1h
        }

    except Exception as e:

        log.error(
            "analyze %s: %s",
            market,
            e
        )

        return empty_analysis()


# =========================================================
# Upbit 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update_time

    markets = get_upbit_markets()

    if not markets:
        return

    # 24시간 거래대금
    ticker_url = (
        "https://api.upbit.com/v1/ticker"
    )

    try:

        r = retry(
            "GET",
            ticker_url,
            params={
                "markets": ",".join(
                    markets
                )
            }
        )

        if r is None:
            return

        ticker_data = r.json()

    except Exception as e:

        log.error(
            "Upbit ticker: %s",
            e
        )

        return

    volume_map = {}

    for x in ticker_data:

        market = x.get(
            "market"
        )

        volume = float(
            x.get(
                "acc_trade_price_24h",
                0
            ) or 0
        )

        volume_map[
            market
        ] = volume

    sorted_markets = sorted(
        markets,
        key=lambda m:
        volume_map.get(
            m,
            0
        ),
        reverse=True
    )

    rows = []

    for rank, market in enumerate(
        sorted_markets[:TOP_N],
        1
    ):

        a = analyze(
            market,
            okx=False
        )

        if not a:
            a = empty_analysis()

        coin = market.replace(
            "KRW-",
            ""
        )

        rows.append(
            {
                "rank": rank,

                "coin": coin,

                "market": market,

                "change": a.get(
                    "changes"
                ),

                "volume": volume_map.get(
                    market,
                    0
                ),

                "ema_1h": a.get(
                    "ema_1h",
                    {
                        "display": "⚪",
                        "direction": "none"
                    }
                ),

                "ema_4h": a.get(
                    "ema_4h",
                    {
                        "display": "⚪",
                        "direction": "none"
                    }
                ),

                "ema10_position": a.get(
                    "ema10_position",
                    {
                        "display": "-",
                        "position": "none"
                    }
                ),

                "air_warning": a.get(
                    "air_warning",
                    False
                ),

                "air_direction": a.get(
                    "air_direction"
                ),

                "air_count": a.get(
                    "air_count",
                    0
                ),

                "air_active": a.get(
                    "air_active",
                    False
                ),

                "qualified": a.get(
                    "qualified",
                    False
                )
            }
        )

    latest_upbit_data = rows

    latest_upbit_update_time = kst()

    log.info(
        "Upbit 업데이트 완료: %s",
        latest_upbit_update_time
    )


# =========================================================
# OKX 심볼
# =========================================================

def get_okx_symbols():

    url = (
        "https://www.okx.com/api/v5/public/"
        "instruments"
    )

    r = retry(
        "GET",
        url,
        params={
            "instType": "SWAP"
        }
    )

    if r is None:
        return []

    try:

        data = r.json()

        if data.get("code") != "0":
            return []

        symbols = []

        for x in data.get(
            "data",
            []
        ):

            inst = x.get(
                "instId",
                ""
            )

            state = x.get(
                "state",
                ""
            )

            if (
                state == "live"
                and inst.endswith(
                    "-USDT-SWAP"
                )
            ):

                symbols.append(
                    inst
                )

        return symbols

    except Exception as e:

        log.error(
            "get_okx_symbols: %s",
            e
        )

        return []


# =========================================================
# OKX 거래대금
# =========================================================

def get_okx_volume(
    inst,
    usdt
):

    if usdt <= 0:
        return 0

    df = get_okx_ohlcv(
        inst,
        "1H",
        VOLUME_HOURS
    )

    if df.empty:
        return 0

    try:

        volume_usdt = float(
            df[
                "volCcyQuote"
            ].sum()
        )

        # 기존 UI 기준 1/10
        volume_krw = (
            volume_usdt
            * usdt
            / 10
        )

        return volume_krw

    except Exception:

        return 0


# =========================================================
# OKX 업데이트
# =========================================================

def update_okx(
    usdt
):

    global latest_okx_data
    global latest_okx_update_time

    symbols = get_okx_symbols()

    if not symbols:
        return

    # 업비트 상장 코인 목록
    upbit_coins = set()

    for market in latest_upbit_markets:

        if market.startswith(
            "KRW-"
        ):

            upbit_coins.add(
                market.replace(
                    "KRW-",
                    ""
                )
            )

    rows_temp = []

    for inst in symbols:

        try:

            volume = get_okx_volume(
                inst,
                usdt
            )

            if volume <= 0:
                continue

            rows_temp.append(
                (
                    inst,
                    volume
                )
            )

        except Exception:

            continue

    rows_temp.sort(
        key=lambda x: x[1],
        reverse=True
    )

    rows_temp = rows_temp[
        :TOP_N
    ]

    rows = []

    for rank, item in enumerate(
        rows_temp,
        1
    ):

        inst = item[0]
        volume = item[1]

        coin = inst.replace(
            "-USDT-SWAP",
            ""
        )

        a = analyze(
            inst,
            okx=True
        )

        if not a:
            a = empty_analysis()

        rows.append(
            {
                "rank": rank,

                "coin": coin,

                "market": inst,

                "upbit": (
                    coin in upbit_coins
                ),

                "change": a.get(
                    "changes"
                ),

                "volume": volume,

                "ema_1h": a.get(
                    "ema_1h",
                    {
                        "display": "⚪",
                        "direction": "none"
                    }
                ),

                "ema_4h": a.get(
                    "ema_4h",
                    {
                        "display": "⚪",
                        "direction": "none"
                    }
                ),

                "ema10_position": a.get(
                    "ema10_position",
                    {
                        "display": "-",
                        "position": "none"
                    }
                ),

                "air_warning": a.get(
                    "air_warning",
                    False
                ),

                "air_direction": a.get(
                    "air_direction"
                ),

                "air_count": a.get(
                    "air_count",
                    0
                ),

                "air_active": a.get(
                    "air_active",
                    False
                ),

                "qualified": a.get(
                    "qualified",
                    False
                )
            }
        )

    latest_okx_data = rows

    latest_okx_update_time = kst()

    log.info(
        "OKX 업데이트 완료: %s",
        latest_okx_update_time
    )


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    global latest_usdt_krw

    with update_lock:

        try:

            if USE_UPBIT == "Y":

                update_upbit()

            if USE_OKX == "Y":

                latest_usdt_krw = (
                    get_usdt_krw()
                )

                update_okx(
                    latest_usdt_krw
                )

        except Exception as e:

            log.error(
                "update_dashboard: %s",
                e
            )


# =========================================================
# EMA HTML
# =========================================================

def ema_html(
    e
):

    if not e:
        return "-"

    direction_value = e.get(
        "direction",
        "none"
    )

    display = e.get(
        "display",
        "⚪"
    )

    if direction_value == "long":

        return (
            '<span class="ema-long">'
            f'{display}'
            '</span>'
        )

    if direction_value == "short":

        return (
            '<span class="ema-short">'
            f'{display}'
            '</span>'
        )

    return display


# =========================================================
# EMA10 위치 HTML
# =========================================================

def ema10_position_html(
    e
):

    if not e:
        return ""

    position = e.get(
        "position",
        "none"
    )

    display = e.get(
        "display",
        "-"
    )

    if position == "above":

        return (
            '<span class="ema10-above">'
            f'{display}'
            '</span>'
        )

    if position == "below":

        return (
            '<span class="ema10-below">'
            f'{display}'
            '</span>'
        )

    if position == "equal":

        return (
            '<span class="ema10-equal">'
            f'{display}'
            '</span>'
        )

    return display


# =========================================================
# 경고 HTML
# =========================================================

def warning_html(
    air_warning,
    air_direction=None,
    air_count=0,
    ema10_position=None
):

    parts = []

    # ---------------------------------------------
    # EMA10 위치
    # ---------------------------------------------

    if ema10_position:

        position = ema10_position.get(
            "position",
            "none"
        )

        display = ema10_position.get(
            "display",
            ""
        )

        if position == "above":

            parts.append(
                '<span class="ema10-above">'
                f'{display}'
                '</span>'
            )

        elif position == "below":

            parts.append(
                '<span class="ema10-below">'
                f'{display}'
                '</span>'
            )

        elif position == "equal":

            parts.append(
                '<span class="ema10-equal">'
                f'{display}'
                '</span>'
            )

    # ---------------------------------------------
    # LONG / SHORT 경고
    # ---------------------------------------------

    if air_warning or (
        air_direction
        and air_count is not None
    ):

        direction_value = (
            air_direction
        )

        if direction_value == "LONG":

            parts.append(
                '<span class="warning-long">'
                '🚀 LONG'
                '</span>'
            )

        elif direction_value == "SHORT":

            parts.append(
                '<span class="warning-short">'
                '🚀 SHORT'
                '</span>'
            )

        if (
            air_count is not None
            and air_count > 0
        ):

            parts.append(
                '<span class="air-count">'
                f'① {air_count}'
                '</span>'
            )

    if not parts:
        return "-"

    return " ".join(
        parts
    )


# =========================================================
# 테이블 행
# =========================================================

def rows_html(
    data
):

    if not data:

        return (
            '<tr>'
            '<td colspan="5" '
            'class="empty">'
            '데이터 없음'
            '</td>'
            '</tr>'
        )

    rows = []

    for x in data:

        coin = x.get(
            "coin",
            "-"
        )

        # OKX 업비트 상장 표시
        if x.get(
            "upbit",
            False
        ):

            coin_html = (
                f'{coin}'
                '<span class="upbit-tag">'
                '(업비트)'
                '</span>'
            )

        else:

            coin_html = coin

        warning = warning_html(
            x.get(
                "air_warning",
                False
            ),
            x.get(
                "air_direction"
            ),
            x.get(
                "air_count",
                0
            ),
            x.get(
                "ema10_position"
            )
        )

        rows.append(
            f"""
            <tr>

                <td class="rank">
                    {x.get("rank", "-")}
                </td>

                <td class="coin">
                    {coin_html}
                </td>

                <td class="volume">
                    {format_volume(
                        x.get("volume")
                    )}
                </td>

                <td class="ema">
                    <span class="ema-label">1H</span>
                    {ema_html(
                        x.get("ema_1h")
                    )}

                    <span class="ema-label">4H</span>
                    {ema_html(
                        x.get("ema_4h")
                    )}
                </td>

                <td class="warning">
                    {warning}
                </td>

            </tr>
            """
        )

    return "".join(
        rows
    )


# =========================================================
# 섹션
# =========================================================

def section(
    title,
    data,
    update_time
):

    return f"""
    <section>

        <div class="section-title">
            <div>
                {title}
            </div>

            <div class="update-time">
                {update_time}
            </div>
        </div>

        <div class="table-wrap">

            <table>

                <thead>

                    <tr>

                        <th>#</th>

                        <th>코인</th>

                        <th>거래대금</th>

                        <th>EMA</th>

                        <th>경고</th>

                    </tr>

                </thead>

                <tbody>

                    {rows_html(data)}

                </tbody>

            </table>

        </div>

    </section>
    """


# =========================================================
# 메인 HTML
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def dashboard():

    usdt_text = "-"

    if latest_usdt_krw:

        usdt_text = (
            f"{latest_usdt_krw:,.0f}원"
        )

    return f"""
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
    OKX / Upbit Dashboard
</title>

<style>

* {{
    box-sizing: border-box;
}}

html,
body {{
    margin: 0;
    padding: 0;
    background: #111;
    color: #eee;
    font-family:
        Arial,
        "Noto Sans KR",
        sans-serif;
}}

body {{
    padding: 8px;
}}

.container {{
    width: 100%;
    max-width: 1400px;
    margin: 0 auto;
}}

header {{
    margin-bottom: 8px;
}}

h1 {{
    margin: 0;
    font-size: 18px;
    font-weight: 800;
}}

.sub-info {{
    margin-top: 5px;
    font-size: 11px;
    color: #999;
}}

section {{
    margin-top: 10px;
    background: #181818;
    border: 1px solid #292929;
    border-radius: 6px;
    overflow: hidden;
}}

.section-title {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 8px 10px;
    background: #202020;
    font-size: 13px;
    font-weight: 800;
}}

.update-time {{
    color: #888;
    font-size: 9px;
    font-weight: normal;
}}

.table-wrap {{
    width: 100%;
    overflow-x: hidden;
}}

table {{
    width: 100%;
    border-collapse: collapse;
    table-layout: fixed;
}}

th,
td {{
    border-bottom: 1px solid #252525;
    padding: 7px 3px;
    text-align: center;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}}

th {{
    background: #161616;
    color: #aaa;
    font-size: 9px;
    font-weight: 700;
}}

td {{
    font-size: 10px;
}}

tr:last-child td {{
    border-bottom: 0;
}}

/* =====================================================
   최종 칸 너비
   ===================================================== */

th:nth-child(1),
td:nth-child(1) {{
    width: 6%;
}}

th:nth-child(2),
td:nth-child(2) {{
    width: 21%;
}}

th:nth-child(3),
td:nth-child(3) {{
    width: 17%;
}}

th:nth-child(4),
td:nth-child(4) {{
    width: 21%;
}}

th:nth-child(5),
td:nth-child(5) {{
    width: 35%;
}}

/* =====================================================
   기본
   ===================================================== */

.rank {{
    color: #888;
    font-weight: 700;
}}

.coin {{
    text-align: left;
    padding-left: 7px;
    font-weight: 800;
}}

.volume {{
    color: #ddd;
    font-weight: 700;
}}

.ema {{
    font-size: 10px;
    font-weight: 800;
}}

.ema-label {{
    color: #777;
    font-size: 8px;
    margin-right: 2px;
}}

.ema-label + .ema-label {{
    margin-left: 5px;
}}

.warning {{
    font-size: 9px;
    font-weight: 900;
    text-align: center;
    white-space: nowrap;
}}

/* =====================================================
   EMA
   ===================================================== */

.ema-long {{
    color: #35e66d;
}}

.ema-short {{
    color: #ff4d4d;
}}

/* =====================================================
   1H 종가 vs EMA10
   글자 자체에 색상 적용
   ===================================================== */

.ema10-above {{
    color: #35e66d;
    font-weight: 900;
}}

.ema10-below {{
    color: #ff4d4d;
    font-weight: 900;
}}

.ema10-equal {{
    color: #aaa;
    font-weight: 900;
}}

/* =====================================================
   LONG / SHORT
   ===================================================== */

.warning-long {{
    color: #35e66d;
    font-weight: 900;
}}

.warning-short {{
    color: #ff4d4d;
    font-weight: 900;
}}

.air-count {{
    color: #fff;
    font-weight: 900;
    margin-left: 2px;
}}

/* =====================================================
   등락률
   ===================================================== */

.change-up {{
    color: #35e66d;
}}

.change-down {{
    color: #ff4d4d;
}}

.change-zero {{
    color: #999;
}}

.upbit-tag {{
    color: #777;
    font-size: 8px;
    margin-left: 2px;
}}

.empty {{
    padding: 20px;
    color: #777;
}}

/* =====================================================
   설명
   ===================================================== */

.info {{
    margin-top: 10px;
    padding: 9px;
    background: #181818;
    border: 1px solid #292929;
    border-radius: 6px;
    color: #888;
    font-size: 9px;
    line-height: 1.7;
}}

.info strong {{
    color: #aaa;
}}

@media (
    max-width: 600px
) {{

    body {{
        padding: 4px;
    }}

    h1 {{
        font-size: 15px;
    }}

    .section-title {{
        font-size: 11px;
        padding: 7px;
    }}

    th,
    td {{
        padding: 6px 2px;
    }}

    th {{
        font-size: 8px;
    }}

    td {{
        font-size: 9px;
    }}

    .ema {{
        font-size: 8px;
    }}

    .ema-label {{
        font-size: 7px;
    }}

    .warning {{
        font-size: 8px;
    }}

    .coin {{
        padding-left: 4px;
    }}

    .upbit-tag {{
        font-size: 7px;
    }}

}}

</style>

</head>

<body>

<div class="container">

<header>

    <h1>
        📊 매매 전술 눌림 돌파
    </h1>

    <div class="sub-info">

        USDT/KRW:
        <strong>
            {usdt_text}
        </strong>

        &nbsp;|&nbsp;

        현재:
        {kst()}

    </div>

</header>


{section(
    "🏆 UPBIT 실시간 거래대금 TOP100",
    latest_upbit_data,
    latest_upbit_update_time
)}


{section(
    "🏆 OKX 실거래대금 TOP100",
    latest_okx_data,
    latest_okx_update_time
)}


<div class="info">

    <strong>표시 기준</strong><br>

    ① 거래대금 기준 실시간 순위<br>

    ② EMA = 1H / 4H EMA 10-30-60-120 정배열 방향<br>

    ③ 🟢 = EMA 정배열 / 🔴 = EMA 역배열<br>

    ④ <span class="ema10-above">
        ▲ 위
    </span>
    = 1H 완성봉 종가가 EMA10 위<br>

    ⑤ <span class="ema10-below">
        ▼ 아래
    </span>
    = 1H 완성봉 종가가 EMA10 아래<br>

    ⑥ LONG = 1H + 4H EMA 정배열 조건 충족<br>

    ⑦ LONG 경고 발생 후 다음 1H 양봉마다 카운터 증가<br>

    ⑧ 경고 칸 안에서
    <span class="warning-long">LONG</span>은 녹색,
    <span class="warning-short">SHORT</span>는 적색<br>

    ⑨ 현재 진행 중인 1H / 4H 캔들은 계산에서 제외<br>

</div>

</div>

</body>

</html>
"""


# =========================================================
# 백그라운드 스케줄러
# =========================================================

def scheduler_loop():

    schedule.every(
        UPDATE_MINUTES
    ).minutes.do(
        update_dashboard
    )

    # 시작 즉시 1회 실행
    update_dashboard()

    while True:

        try:

            schedule.run_pending()

        except Exception as e:

            log.error(
                "scheduler: %s",
                e
            )

        time.sleep(1)


# =========================================================
# 시작
# =========================================================

if __name__ == "__main__":

    log.info(
        "트레이딩 대시보드 시작"
    )

    log.info(
        "1H 종가 vs EMA10: "
        "위 = 녹색 / 아래 = 적색"
    )

    log.info(
        "테이블 비율: "
        "6 / 21 / 17 / 21 / 35"
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
