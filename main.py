from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import schedule, time, requests, threading, uvicorn, logging, pandas as pd, warnings

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

latest_usdt_krw = None

latest_upbit_update_time = None
latest_okx_update_time = None

latest_upbit_markets = set()

request_lock = threading.Lock()
update_lock = threading.Lock()

last_request_time = 0.0


# =========================================================
# 공통
# =========================================================

def kst():
    return datetime.now(KST)


def wait_request():
    global last_request_time

    with request_lock:
        now = time.time()
        diff = now - last_request_time

        if diff < REQUEST_INTERVAL:
            time.sleep(REQUEST_INTERVAL - diff)

        last_request_time = time.time()


def retry(func, *args, **kwargs):
    for i in range(MAX_RETRIES):
        try:
            wait_request()
            r = func(*args, **kwargs)

            if hasattr(r, "raise_for_status"):
                r.raise_for_status()

            return r

        except Exception as e:
            if i == MAX_RETRIES - 1:
                logging.warning("API 실패: %s", e)
                return None

            time.sleep(RATE_LIMIT_WAIT)

    return None


# =========================================================
# Upbit
# =========================================================

def get_upbit_markets():
    r = retry(
        requests.get,
        "https://api.upbit.com/v1/market/all",
        params={"isDetails": "false"},
        timeout=10
    )

    if not r:
        return []

    try:
        return [
            x["market"]
            for x in r.json()
            if x["market"].startswith("KRW-")
        ]
    except Exception:
        return []


def get_usdt_krw():
    r = retry(
        requests.get,
        "https://api.upbit.com/v1/ticker",
        params={"markets": "KRW-USDT"},
        timeout=10
    )

    if not r:
        return None

    try:
        return float(r.json()[0]["trade_price"])
    except Exception:
        return None


def history_upbit(market, unit=60):
    """
    Upbit 분봉 API.
    unit=60 → 1시간봉.
    진행 중인 1시간봉은 제외.
    """

    rows = []
    to = None

    for _ in range(MAX_HISTORY_CHUNKS):
        params = {
            "market": market,
            "count": HISTORY_CHUNK
        }

        if to:
            params["to"] = to

        r = retry(
            requests.get,
            f"https://api.upbit.com/v1/candles/minutes/{unit}",
            params=params,
            timeout=10
        )

        if not r:
            break

        try:
            data = r.json()
        except Exception:
            break

        if not data:
            break

        rows.extend(data)

        if len(data) < HISTORY_CHUNK:
            break

        to = data[-1]["candle_date_time_utc"]

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    df["datetime"] = pd.to_datetime(
        df["candle_date_time_kst"]
    )

    df = df.rename(columns={
        "opening_price": "o",
        "high_price": "h",
        "low_price": "l",
        "trade_price": "c",
        "candle_acc_trade_volume": "v",
        "candle_acc_trade_price": "value"
    })

    df = df.sort_values("datetime").drop_duplicates(
        "datetime"
    ).reset_index(drop=True)

    # 현재 진행 중인 1시간봉 제거
    now = kst()

    current = now.replace(
        minute=0,
        second=0,
        microsecond=0
    ).replace(tzinfo=None)

    df = df[df["datetime"] < current]

    return df.reset_index(drop=True)


def history_upbit_4h(market):
    """
    Upbit 4시간봉.
    진행 중인 4시간봉 제외.
    """

    df = history_upbit_4h_raw(market)

    if df.empty:
        return df

    now = kst()

    block = (now.hour // 4) * 4

    current = now.replace(
        hour=block,
        minute=0,
        second=0,
        microsecond=0
    ).replace(tzinfo=None)

    df = df[df["datetime"] < current]

    return df.reset_index(drop=True)


def history_upbit_4h_raw(market):
    rows = []
    to = None

    for _ in range(MAX_HISTORY_CHUNKS):
        params = {
            "market": market,
            "count": HISTORY_CHUNK
        }

        if to:
            params["to"] = to

        r = retry(
            requests.get,
            "https://api.upbit.com/v1/candles/minutes/240",
            params=params,
            timeout=10
        )

        if not r:
            break

        try:
            data = r.json()
        except Exception:
            break

        if not data:
            break

        rows.extend(data)

        if len(data) < HISTORY_CHUNK:
            break

        to = data[-1]["candle_date_time_utc"]

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    df["datetime"] = pd.to_datetime(
        df["candle_date_time_kst"]
    )

    df = df.rename(columns={
        "opening_price": "o",
        "high_price": "h",
        "low_price": "l",
        "trade_price": "c",
        "candle_acc_trade_volume": "v",
        "candle_acc_trade_price": "value"
    })

    return (
        df.sort_values("datetime")
        .drop_duplicates("datetime")
        .reset_index(drop=True)
    )


# =========================================================
# OKX
# =========================================================

def get_okx_symbols():
    r = retry(
        requests.get,
        "https://www.okx.com/api/v5/market/tickers",
        params={"instType": "SWAP"},
        timeout=10
    )

    if not r:
        return []

    try:
        data = r.json().get("data", [])

        return [
            x["instId"]
            for x in data
            if x["instId"].endswith("-USDT-SWAP")
        ]

    except Exception:
        return []


def get_okx_ohlcv(symbol, bar="1H", limit=200):
    r = retry(
        requests.get,
        "https://www.okx.com/api/v5/market/candles",
        params={
            "instId": symbol,
            "bar": bar,
            "limit": limit
        },
        timeout=10
    )

    if not r:
        return pd.DataFrame()

    try:
        data = r.json().get("data", [])
    except Exception:
        return pd.DataFrame()

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    df = df.iloc[:, :9]

    df.columns = [
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

    for col in ["o", "h", "l", "c", "vol", "volCcyQuote"]:
        df[col] = pd.to_numeric(
            df[col],
            errors="coerce"
        )

    df["datetime"] = pd.to_datetime(
        pd.to_numeric(df["ts"]),
        unit="ms"
    ).dt.tz_localize("UTC").dt.tz_convert(KST).dt.tz_localize(None)

    df = (
        df.sort_values("datetime")
        .drop_duplicates("datetime")
        .reset_index(drop=True)
    )

    # OKX 현재 진행 중 캔들 제거
    if bar == "1H":
        now = kst()

        current = now.replace(
            minute=0,
            second=0,
            microsecond=0
        ).replace(tzinfo=None)

        df = df[df["datetime"] < current]

    elif bar == "4H":
        now = kst()

        block = (now.hour // 4) * 4

        current = now.replace(
            hour=block,
            minute=0,
            second=0,
            microsecond=0
        ).replace(tzinfo=None)

        df = df[df["datetime"] < current]

    return df.reset_index(drop=True)


def history_okx(symbol, bar="1H"):
    return get_okx_ohlcv(
        symbol,
        bar=bar,
        limit=INITIAL_CANDLE_COUNT
    )


# =========================================================
# EMA
# =========================================================

def ema(df, period):
    if df is None or df.empty:
        return pd.Series(dtype=float)

    return df["c"].ewm(
        span=period,
        adjust=False
    ).mean()


def direction(df):
    if df is None or df.empty:
        return "none"

    try:
        e10 = ema(df, 10)
        e30 = ema(df, 30)
        e60 = ema(df, 60)
        e120 = ema(df, 120)

        if (
            e10.iloc[-1] >
            e30.iloc[-1] >
            e60.iloc[-1] >
            e120.iloc[-1]
        ):
            return "long"

        if (
            e10.iloc[-1] <
            e30.iloc[-1] <
            e60.iloc[-1] <
            e120.iloc[-1]
        ):
            return "short"

    except Exception:
        pass

    return "none"


def direction_series(df):
    if df is None or df.empty:
        return "none"

    return direction(df)


def ema_display(df):
    if df is None or df.empty:
        return "-"

    try:
        e10 = ema(df, 10).iloc[-1]
        e30 = ema(df, 30).iloc[-1]
        e60 = ema(df, 60).iloc[-1]
        e120 = ema(df, 120).iloc[-1]

        return (
            f"10:{e10:,.2f} / "
            f"30:{e30:,.2f} / "
            f"60:{e60:,.2f} / "
            f"120:{e120:,.2f}"
        )

    except Exception:
        return "-"


# =========================================================
# 🛩 ✈️ 1시간 비행기 경고
# =========================================================

def get_air_warning(df1h, df4h):

    if (
        df1h is None or df1h.empty or
        df4h is None or df4h.empty
    ):
        return False

    if len(df1h) < 2:
        return False

    # 1H 정배열
    if direction(df1h) != "long":
        return False

    # 4H 정배열
    if direction(df4h) != "long":
        return False

    try:
        e10 = ema(df1h, 10)

        # 이전 완료 1H 캔들
        prev_close = float(
            df1h["c"].iloc[-2]
        )

        prev_ema10 = float(
            e10.iloc[-2]
        )

        # 현재 완료 1H 캔들
        current_open = float(
            df1h["o"].iloc[-1]
        )

        current_close = float(
            df1h["c"].iloc[-1]
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
            was_below and
            bullish and
            closed_above
        )

    except Exception:
        return False


# =========================================================
# 분석
# =========================================================

def analyze(symbol, is_okx=False):

    if is_okx:
        df1h = history_okx(symbol, "1H")
        df4h = history_okx(symbol, "4H")

    else:
        df1h = history_upbit(symbol, 60)
        df4h = history_upbit_4h(symbol)

    if (
        df1h.empty or
        df4h.empty
    ):
        return {
            "ema_1h": "-",
            "ema_4h": "-",
            "direction_1h": "none",
            "direction_4h": "none",
            "air_warning": False
        }

    direction_1h = direction(df1h)
    direction_4h = direction(df4h)

    air_warning = get_air_warning(
        df1h,
        df4h
    )

    return {
        "ema_1h": ema_display(df1h),
        "ema_4h": ema_display(df4h),
        "direction_1h": direction_1h,
        "direction_4h": direction_4h,
        "air_warning": air_warning
    }


# =========================================================
# 거래대금 / 등락률
# =========================================================

def format_volume(value):
    try:
        value = float(value)

        if value >= 1_000_000_000_000:
            return f"{value / 1_000_000_000_000:.2f}조"

        if value >= 100_000_000:
            return f"{value / 100_000_000:.0f}억"

        if value >= 10_000:
            return f"{value / 10_000:.1f}만"

        return f"{value:,.0f}"

    except Exception:
        return "-"


def format_change(value):
    try:
        value = float(value)

        return f"{value:+.2f}%"

    except Exception:
        return "-"


def daily_change_upbit(market):

    r = retry(
        requests.get,
        "https://api.upbit.com/v1/ticker",
        params={"markets": market},
        timeout=10
    )

    if not r:
        return 0

    try:
        return float(
            r.json()[0]["signed_change_rate"]
        ) * 100

    except Exception:
        return 0


# =========================================================
# OKX 거래대금
# =========================================================

def get_okx_volume(symbol):

    df = history_okx(
        symbol,
        "1H"
    )

    if df.empty:
        return 0

    # 최근 24개 완료 1H 캔들
    df = df.tail(VOLUME_HOURS)

    try:
        volume = df["volCcyQuote"].sum()

        # 기존 UI 표시 기준 유지
        return float(volume) / 10

    except Exception:
        return 0


# =========================================================
# Upbit 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update_time
    global latest_upbit_markets

    markets = get_upbit_markets()

    if not markets:
        return

    latest_upbit_markets = set(markets)

    ticker = retry(
        requests.get,
        "https://api.upbit.com/v1/ticker",
        params={
            "markets": ",".join(markets)
        },
        timeout=10
    )

    if not ticker:
        return

    try:
        tickers = ticker.json()
    except Exception:
        return

    rows = []

    for item in tickers:

        market = item.get("market", "")

        if not market.startswith("KRW-"):
            continue

        try:
            volume = float(
                item.get(
                    "acc_trade_price_24h",
                    0
                )
            )

            change = float(
                item.get(
                    "signed_change_rate",
                    0
                )
            ) * 100

            name = market.replace(
                "KRW-",
                ""
            )

            a = analyze(
                market,
                False
            )

            rows.append({
                "rank": 0,
                "name": name,
                "change": change,
                "volume": volume,
                "ema_1h": a["ema_1h"],
                "ema_4h": a["ema_4h"],
                "direction_1h": a["direction_1h"],
                "direction_4h": a["direction_4h"],
                "air_warning": a["air_warning"],
                "qualified": a["air_warning"]
            })

        except Exception as e:
            logging.debug(
                "Upbit 분석 실패 %s: %s",
                market,
                e
            )

    rows.sort(
        key=lambda x: x["volume"],
        reverse=True
    )

    rows = rows[:TOP_N]

    for i, row in enumerate(rows, 1):
        row["rank"] = i

    latest_upbit_data = rows
    latest_upbit_update_time = kst()


# =========================================================
# OKX 업데이트
# =========================================================

def update_okx():

    global latest_okx_data
    global latest_okx_update_time

    symbols = get_okx_symbols()

    if not symbols:
        return

    rows = []

    for symbol in symbols:

        try:
            volume = get_okx_volume(
                symbol
            )

            if volume <= 0:
                continue

            change_df = history_okx(
                symbol,
                "1H"
            )

            change = 0

            if (
                not change_df.empty and
                len(change_df) >= 2
            ):
                old = float(
                    change_df["c"].iloc[-2]
                )

                new = float(
                    change_df["c"].iloc[-1]
                )

                if old != 0:
                    change = (
                        new - old
                    ) / old * 100

            a = analyze(
                symbol,
                True
            )

            coin = symbol.replace(
                "-USDT-SWAP",
                ""
            )

            if (
                f"KRW-{coin}"
                in latest_upbit_markets
            ):
                name = f"{coin} (업비트)"
            else:
                name = coin

            rows.append({
                "rank": 0,
                "name": name,
                "change": change,
                "volume": volume,
                "ema_1h": a["ema_1h"],
                "ema_4h": a["ema_4h"],
                "direction_1h": a["direction_1h"],
                "direction_4h": a["direction_4h"],
                "air_warning": a["air_warning"],
                "qualified": a["air_warning"]
            })

        except Exception as e:
            logging.debug(
                "OKX 분석 실패 %s: %s",
                symbol,
                e
            )

    rows.sort(
        key=lambda x: x["volume"],
        reverse=True
    )

    rows = rows[:TOP_N]

    for i, row in enumerate(rows, 1):
        row["rank"] = i

    latest_okx_data = rows
    latest_okx_update_time = kst()


# =========================================================
# 전체 업데이트
# =========================================================

def update_all():

    global latest_usdt_krw

    if not update_lock.acquire(
        blocking=False
    ):
        return

    try:

        latest_usdt_krw = get_usdt_krw()

        if USE_UPBIT == "Y":
            update_upbit()

        if USE_OKX == "Y":
            update_okx()

        logging.info(
            "업데이트 완료"
        )

    except Exception as e:

        logging.exception(
            "전체 업데이트 오류: %s",
            e
        )

    finally:
        update_lock.release()


# =========================================================
# 비행기 표시
# =========================================================

def warning_html(air_warning):

    if air_warning:
        return (
            '<span class="air">🛩 ✈️</span>'
        )

    return "-"


# =========================================================
# HTML
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def home():

    upbit_rows = ""

    for x in latest_upbit_data:

        cls = (
            "qualified"
            if x.get("air_warning")
            else ""
        )

        upbit_rows += f"""
        <tr class="{cls}">
            <td>{x["rank"]}</td>
            <td class="coin">
                {x["name"]}
            </td>
            <td>
                {format_change(x["change"])}
            </td>
            <td>
                {format_volume(x["volume"])}
            </td>
            <td class="ema">
                {x["ema_1h"]}
            </td>
            <td class="ema">
                {x["ema_4h"]}
            </td>
            <td>
                {warning_html(
                    x.get("air_warning", False)
                )}
            </td>
        </tr>
        """

    okx_rows = ""

    for x in latest_okx_data:

        cls = (
            "qualified"
            if x.get("air_warning")
            else ""
        )

        okx_rows += f"""
        <tr class="{cls}">
            <td>{x["rank"]}</td>
            <td class="coin">
                {x["name"]}
            </td>
            <td>
                {format_change(x["change"])}
            </td>
            <td>
                {format_volume(x["volume"])}
            </td>
            <td class="ema">
                {x["ema_1h"]}
            </td>
            <td class="ema">
                {x["ema_4h"]}
            </td>
            <td>
                {warning_html(
                    x.get("air_warning", False)
                )}
            </td>
        </tr>
        """

    upbit_time = (
        latest_upbit_update_time.strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        if latest_upbit_update_time
        else "-"
    )

    okx_time = (
        latest_okx_update_time.strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        if latest_okx_update_time
        else "-"
    )

    return f"""
<!DOCTYPE html>
<html lang="ko">
<head>

<meta charset="UTF-8">

<meta
    http-equiv="refresh"
    content="60"
>

<meta
    name="viewport"
    content="width=device-width, initial-scale=1"
>

<title>1H EMA 비행기 경고</title>

<style>

body {{
    background:#111;
    color:#eee;
    font-family:
        Arial,
        sans-serif;

    margin:0;
    padding:15px;
}}

h1 {{
    margin:5px 0 15px;
    font-size:22px;
}}

h2 {{
    margin-top:25px;
    font-size:18px;
}}

.info {{
    background:#1b1b1b;
    padding:12px;
    border-radius:8px;
    line-height:1.7;
    font-size:13px;
}}

table {{
    width:100%;
    border-collapse:collapse;
    margin-top:10px;
    background:#181818;
}}

th,
td {{
    padding:9px 5px;
    border-bottom:1px solid #333;
    text-align:center;
    font-size:12px;
}}

th {{
    background:#222;
}}

.coin {{
    font-weight:bold;
    text-align:left;
}}

.ema {{
    font-size:10px;
    white-space:nowrap;
}}

.air {{
    font-size:20px;
    display:inline-block;
}}

.qualified {{
    animation:blink 1.2s infinite;
}}

@keyframes blink {{
    0%,100% {{
        background:#181818;
    }}

    50% {{
        background:#263b20;
    }}
}}

@media(max-width:800px) {{

    .ema {{
        font-size:8px;
    }}

    th,
    td {{
        padding:7px 2px;
        font-size:10px;
    }}

    .air {{
        font-size:17px;
    }}
}}

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
⑧ 진행 중인 1H / 4H 캔들은 판정에서 제외

</div>


{"<h2>🇰🇷 UPBIT</h2>" if USE_UPBIT == "Y" else ""}

{"<div>업데이트: " + upbit_time + "</div>" if USE_UPBIT == "Y" else ""}

{
f'''
<table>
<thead>
<tr>
    <th>순위</th>
    <th>코인</th>
    <th>등락</th>
    <th>거래대금</th>
    <th>1H EMA</th>
    <th>4H EMA</th>
    <th>경고</th>
</tr>
</thead>

<tbody>
{upbit_rows}
</tbody>
</table>
'''
if USE_UPBIT == "Y"
else ""
}


{"<h2>🔥 OKX FUTURES</h2>" if USE_OKX == "Y" else ""}

{"<div>업데이트: " + okx_time + "</div>" if USE_OKX == "Y" else ""}

{
f'''
<table>
<thead>
<tr>
    <th>순위</th>
    <th>코인</th>
    <th>등락</th>
    <th>거래대금</th>
    <th>1H EMA</th>
    <th>4H EMA</th>
    <th>경고</th>
</tr>
</thead>

<tbody>
{okx_rows}
</tbody>
</table>
'''
if USE_OKX == "Y"
else ""
}

</body>
</html>
"""


# =========================================================
# 스케줄러
# =========================================================

def scheduler():

    schedule.every(
        UPDATE_MINUTES
    ).minutes.do(
        update_all
    )

    while True:

        try:
            schedule.run_pending()

        except Exception as e:
            logging.exception(
                "스케줄러 오류: %s",
                e
            )

        time.sleep(1)


# =========================================================
# 시작
# =========================================================

if __name__ == "__main__":

    update_all()

    threading.Thread(
        target=scheduler,
        daemon=True
    ).start()

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
        )
