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

log = logging.getLogger("crypto")

KST = ZoneInfo("Asia/Seoul")

USE_UPBIT = "Y"
USE_OKX = "N"

TOP_N = 20
VOLUME_HOURS = 24
UPDATE_MINUTES = 5

MIN_ORDER_KRW = 5000

latest_upbit_markets = []


# =========================================================
# 공통 요청
# =========================================================

def retry(func, *args, retries=3, delay=1, **kwargs):
    for i in range(retries):
        try:
            r = func(*args, **kwargs)

            if hasattr(r, "raise_for_status"):
                r.raise_for_status()

            return r

        except Exception as e:
            if i == retries - 1:
                raise

            time.sleep(delay)

    return None


# =========================================================
# Upbit 마켓
# =========================================================

def get_upbit_markets():
    try:
        r = retry(
            requests.get,
            "https://api.upbit.com/v1/market/all",
            params={"isDetails": "false"},
            timeout=15
        )

        data = r.json()

        return [
            x["market"]
            for x in data
            if x["market"].startswith("KRW-")
        ]

    except Exception as e:
        log.error(f"Upbit 마켓 조회 오류: {e}")
        return []


# =========================================================
# Upbit 캔들
# 1H / 4H 공통
# =========================================================

def get_upbit_candles(market, unit, count=200, to=None):

    try:
        params = {
            "market": market,
            "count": min(count, 200)
        }

        if to:
            params["to"] = to

        url = f"https://api.upbit.com/v1/candles/minutes/{unit}"

        r = retry(
            requests.get,
            url,
            params=params,
            timeout=15
        )

        data = r.json()

        if not data:
            return None

        rows = []

        for x in reversed(data):

            rows.append({
                "datetime": pd.to_datetime(
                    x["candle_date_time_kst"]
                ),
                "o": float(x["opening_price"]),
                "h": float(x["high_price"]),
                "l": float(x["low_price"]),
                "c": float(x["trade_price"]),
                "v": float(x["candle_acc_trade_volume"]),
                "value": float(x["candle_acc_trade_price"])
            })

        df = pd.DataFrame(rows)

        if df.empty:
            return None

        # -------------------------------------------------
        # 현재 진행 중인 봉 제외
        # -------------------------------------------------

        now = datetime.now(KST)

        if unit == 60:

            current = now.replace(
                minute=0,
                second=0,
                microsecond=0
            ).replace(tzinfo=None)

        else:

            block = (now.hour // 4) * 4

            current = now.replace(
                hour=block,
                minute=0,
                second=0,
                microsecond=0
            ).replace(tzinfo=None)

        df = df[df["datetime"] < current].copy()

        return df.reset_index(drop=True)

    except Exception as e:

        log.error(
            f"Upbit {market} {unit}분봉 오류: {e}"
        )

        return None


def get_upbit_1h(market, count=200, to=None):
    return get_upbit_candles(
        market,
        60,
        count,
        to
    )


def get_upbit_4h(market, count=200, to=None):
    return get_upbit_candles(
        market,
        240,
        count,
        to
    )


# =========================================================
# Upbit 히스토리
# =========================================================

def history_upbit(market, unit, required=125):

    frames = []

    to = None

    try:

        while sum(len(x) for x in frames) < required:

            df = get_upbit_candles(
                market,
                unit,
                200,
                to
            )

            if df is None or df.empty:
                break

            frames.append(df)

            oldest = df["datetime"].iloc[0]

            to = (
                oldest
                .strftime("%Y-%m-%dT%H:%M:%S")
            )

            if len(df) < 200:
                break

        if not frames:
            return None

        result = (
            pd.concat(frames)
            .drop_duplicates("datetime")
            .sort_values("datetime")
            .reset_index(drop=True)
        )

        return result.tail(required).reset_index(drop=True)

    except Exception as e:

        log.error(
            f"Upbit history 오류 {market} {unit}: {e}"
        )

        return None


def history_upbit_1h(market, required=125):
    return history_upbit(
        market,
        60,
        required
    )


def history_upbit_4h(market, required=125):
    return history_upbit(
        market,
        240,
        required
    )


# =========================================================
# OKX
# =========================================================

def get_okx_ohlcv(inst_id, bar="1H", limit=200):

    try:

        r = retry(
            requests.get,
            "https://www.okx.com/api/v5/market/candles",
            params={
                "instId": inst_id,
                "bar": bar,
                "limit": limit
            },
            timeout=15
        )

        data = r.json()

        if data.get("code") != "0":
            return None

        rows = []

        for x in reversed(data["data"]):

            rows.append({
                "ts": int(x[0]),
                "o": float(x[1]),
                "h": float(x[2]),
                "l": float(x[3]),
                "c": float(x[4]),
                "v": float(x[5]),
                "confirm": str(x[8])
            })

        df = pd.DataFrame(rows)

        if df.empty:
            return None

        df = df[
            df["confirm"] == "1"
        ].copy()

        return df.reset_index(drop=True)

    except Exception as e:

        log.error(
            f"OKX {inst_id} {bar} 오류: {e}"
        )

        return None


def history_okx(inst_id, bar="1H", required=125):

    frames = []

    try:

        for _ in range(3):

            df = get_okx_ohlcv(
                inst_id,
                bar,
                200
            )

            if df is None or df.empty:
                break

            frames.append(df)

            if len(df) < 200:
                break

            break

        if not frames:
            return None

        result = (
            pd.concat(frames)
            .drop_duplicates("ts")
            .sort_values("ts")
            .reset_index(drop=True)
        )

        return result.tail(required).reset_index(drop=True)

    except Exception as e:

        log.error(
            f"OKX history 오류 {inst_id}: {e}"
        )

        return None


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

    close = pd.to_numeric(
        df["c"],
        errors="coerce"
    )

    return close.ewm(
        span=period,
        adjust=False,
        min_periods=1
    ).mean()


def direction(df):

    if df is None or df.empty:
        return "none"

    try:

        e10 = ema(df, 10).iloc[-1]
        e30 = ema(df, 30).iloc[-1]
        e60 = ema(df, 60).iloc[-1]
        e120 = ema(df, 120).iloc[-1]

        if e10 > e30 > e60 > e120:
            return "long"

        if e10 < e30 < e60 < e120:
            return "short"

        return "none"

    except Exception:
        return "none"


def ema_display(df):

    if df is None or df.empty:
        return {
            "display": "⚪",
            "direction": "none"
        }

    try:

        e10 = ema(df, 10).iloc[-1]
        e30 = ema(df, 30).iloc[-1]
        e60 = ema(df, 60).iloc[-1]
        e120 = ema(df, 120).iloc[-1]

        d = direction(df)

        if d == "long":
            icon = "🟢"

        elif d == "short":
            icon = "🔴"

        else:
            icon = "⚪"

        display = (
            f"{icon} "
            f"10:{e10:,.4f} "
            f"30:{e30:,.4f} "
            f"60:{e60:,.4f} "
            f"120:{e120:,.4f}"
        )

        return {
            "display": display,
            "direction": d
        }

    except Exception:

        return {
            "display": "⚪",
            "direction": "none"
        }


# =========================================================
# 비행기 경고
#
# 조건
# 1. 1H 정배열
# 2. 4H 정배열
# 3. 이전 1H 종가 < 이전 EMA10
# 4. 현재 완료 1H 봉 양봉
# 5. 현재 완료 1H 종가 > 현재 EMA10
# =========================================================

def get_air_warning(df1h, df4h):

    if (
        df1h is None
        or df1h.empty
        or df4h is None
        or df4h.empty
        or len(df1h) < 2
    ):
        return False

    if direction(df1h) != "long":
        return False

    if direction(df4h) != "long":
        return False

    try:

        e10 = ema(df1h, 10)

        if e10 is None:
            return False

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

        return (
            previous_close < previous_ema10
            and current_close > current_open
            and current_close > current_ema10
        )

    except Exception:
        return False


# =========================================================
# 등락률
# =========================================================

def daily_change_upbit(market):

    try:

        r = retry(
            requests.get,
            "https://api.upbit.com/v1/candles/days",
            params={
                "market": market,
                "count": 2
            },
            timeout=15
        )

        data = r.json()

        if len(data) < 2:
            return None

        yesterday = float(
            data[1]["trade_price"]
        )

        current = float(
            data[0]["trade_price"]
        )

        if yesterday == 0:
            return None

        return (
            (current - yesterday)
            / yesterday
            * 100
        )

    except Exception:
        return None


def daily_changes(df):

    try:

        if (
            df is None
            or len(df) < 25
        ):
            return None

        close = float(df["c"].iloc[-1])

        prev = float(df["c"].iloc[-25])

        if prev == 0:
            return None

        return (
            (close - prev)
            / prev
            * 100
        )

    except Exception:
        return None


def format_change(value):

    if value is None:
        return "-"

    try:

        value = float(value)

        if value > 0:
            return f"<span class='up'>▲ {value:.2f}%</span>"

        if value < 0:
            return f"<span class='down'>▼ {abs(value):.2f}%</span>"

        return "0.00%"

    except Exception:
        return "-"


# =========================================================
# 거래대금
# =========================================================

def get_usdt_krw():

    try:

        r = retry(
            requests.get,
            "https://api.upbit.com/v1/ticker",
            params={
                "markets": "KRW-USDT"
            },
            timeout=15
        )

        data = r.json()

        if data:
            return float(
                data[0]["trade_price"]
            )

    except Exception:
        pass

    return 1400.0


def get_okx_volume(inst_id):

    try:

        df = get_okx_ohlcv(
            inst_id,
            "1H",
            VOLUME_HOURS + 1
        )

        if df is None or df.empty:
            return 0

        df = df.tail(
            VOLUME_HOURS
        )

        volume_usdt = (
            pd.to_numeric(
                df["v"],
                errors="coerce"
            )
            .fillna(0)
            .sum()
        )

        return (
            float(volume_usdt)
            * get_usdt_krw()
        )

    except Exception:

        return 0


def format_volume(value):

    try:

        value = float(value)

        if value >= 1_0000_0000:

            return (
                f"{value / 1_0000_0000:.1f}억"
            )

        if value >= 1_0000:

            return (
                f"{value / 1_0000:.1f}만"
            )

        return f"{value:,.0f}"

    except Exception:
        return "-"


# =========================================================
# 분석
# =========================================================

def empty_analysis():

    e = {
        "display": "⚪",
        "direction": "none"
    }

    return {
        "ema_1h": e.copy(),
        "ema_4h": e.copy(),
        "direction_1h": "none",
        "direction_4h": "none",
        "air_warning": False,
        "changes": None,
        "qualified": False
    }


def analyze(market, okx=False):

    if okx:

        df1 = history_okx(
            market,
            "1H"
        )

        df4 = history_okx(
            market,
            "4H"
        )

    else:

        df1 = history_upbit_1h(
            market
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

    d1 = direction(df1)
    d4 = direction(df4)

    air_warning = get_air_warning(
        df1,
        df4
    )

    if okx:

        changes = daily_changes(df1)

    else:

        changes = daily_change_upbit(
            market
        )

    return {
        "ema_1h": ema_display(df1),
        "ema_4h": ema_display(df4),
        "direction_1h": d1,
        "direction_4h": d4,
        "air_warning": air_warning,
        "changes": changes,
        "qualified": air_warning
    }


# =========================================================
# Row 공통
# =========================================================

def make_row(rank, name, volume, analysis):

    if analysis is None:
        analysis = empty_analysis()

    air = bool(
        analysis.get(
            "air_warning",
            False
        )
    )

    return {
        "rank": rank,
        "name": name,
        "change": format_change(
            analysis.get("changes")
        ),
        "volume": format_volume(
            volume
        ),
        "ema_1h": analysis.get(
            "ema_1h",
            {"display": "⚪"}
        ),
        "ema_4h": analysis.get(
            "ema_4h",
            {"display": "⚪"}
        ),
        "direction": analysis.get(
            "direction_1h",
            "none"
        ),
        "air_warning": air,
        "qualified": air
    }


# =========================================================
# Upbit 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_markets

    markets = get_upbit_markets()

    if not markets:
        return

    rows = []

    for rank, market in enumerate(
        markets,
        start=1
    ):

        try:

            ticker = retry(
                requests.get,
                "https://api.upbit.com/v1/ticker",
                params={
                    "markets": market
                },
                timeout=15
            ).json()

            if not ticker:
                continue

            volume = float(
                ticker[0].get(
                    "acc_trade_price_24h",
                    0
                )
            )

            if volume < MIN_ORDER_KRW:
                continue

            analysis = analyze(
                market,
                okx=False
            )

            if analysis is None:
                continue

            rows.append(
                make_row(
                    rank,
                    market.replace(
                        "KRW-",
                        ""
                    ),
                    volume,
                    analysis
                )
            )

        except Exception as e:

            log.error(
                f"Upbit 분석 오류 {market}: {e}"
            )

    rows.sort(
        key=lambda x: float(
            x["volume"]
            .replace(",", "")
            .replace("억", "")
            if x["volume"] != "-"
            else 0
        ),
        reverse=True
    )

    latest_upbit_markets = rows[:TOP_N]


# =========================================================
# OKX 업데이트
# =========================================================

def get_okx_instruments():

    try:

        r = retry(
            requests.get,
            "https://www.okx.com/api/v5/market/tickers",
            params={
                "instType": "SWAP"
            },
            timeout=15
        )

        data = r.json()

        if data.get("code") != "0":
            return []

        result = []

        for x in data["data"]:

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

                vol = float(
                    x.get("volCcy24h", 0)
                )

                if last <= 0:
                    continue

                result.append(
                    (
                        inst,
                        vol * last
                    )
                )

            except Exception:
                continue

        result.sort(
            key=lambda x: x[1],
            reverse=True
        )

        return result[:TOP_N]

    except Exception as e:

        log.error(
            f"OKX 목록 오류: {e}"
        )

        return []


def update_okx():

    instruments = get_okx_instruments()

    rows = []

    for rank, (
        inst,
        _raw_volume
    ) in enumerate(
        instruments,
        start=1
    ):

        try:

            volume = get_okx_volume(
                inst
            )

            analysis = analyze(
                inst,
                okx=True
            )

            if analysis is None:
                continue

            name = inst.replace(
                "-USDT-SWAP",
                ""
            )

            rows.append(
                make_row(
                    rank,
                    name,
                    volume,
                    analysis
                )
            )

        except Exception as e:

            log.error(
                f"OKX 분석 오류 {inst}: {e}"
            )

    rows.sort(
        key=lambda x: float(
            x["volume"]
            .replace(",", "")
            .replace("억", "")
            if x["volume"] != "-"
            else 0
        ),
        reverse=True
    )

    global latest_okx_markets

    latest_okx_markets = rows[:TOP_N]


latest_okx_markets = []


# =========================================================
# 경고 HTML
# =========================================================

def warning_html(air_warning):

    if air_warning:

        return (
            "<span class='air'>"
            "🛩 ✈️"
            "</span>"
        )

    return "-"


# =========================================================
# 방향 HTML
# =========================================================

def direction_html(direction):

    if direction == "long":
        return "<span class='long'>LONG</span>"

    if direction == "short":
        return "<span class='short'>SHORT</span>"

    return "-"


# =========================================================
# Row HTML
# =========================================================

def rows_html(data):

    out = ""

    for x in data:

        qualified = x.get(
            "qualified",
            False
        )

        cls = (
            " qualified"
            if qualified
            else ""
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

                <div class="coin-name">
                    {x.get("name", "-")}
                </div>

                <div class="change">
                    {x.get("change", "")}
                </div>

            </td>

            <td class="vol">
                {x.get("volume", "-")}
            </td>

            <td class="ema-cell">

                <div class="ema">

                    <div>
                        <b>1H</b>
                        {e1.get(
                            "display",
                            "⚪"
                        )}
                    </div>

                    <div>
                        <b>4H</b>
                        {e4.get(
                            "display",
                            "⚪"
                        )}
                    </div>

                </div>

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
# Section
# =========================================================

def section(title, data):

    return f"""
    <div class="section">

        <div class="section-title">
            🏆 {title} TOP{TOP_N}
        </div>

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
    """


# =========================================================
# Dashboard
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def dashboard():

    upbit_html = ""

    if USE_UPBIT == "Y":

        upbit_html = section(
            "업비트",
            latest_upbit_markets
        )

    okx_html = ""

    if USE_OKX == "Y":

        okx_html = section(
            "OKX",
            latest_okx_markets
        )

    now = datetime.now(
        KST
    ).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    html = f"""
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
    content="{UPDATE_MINUTES * 60}"
>

<title>
    1H EMA 비행기 경고
</title>

<style>

* {{
    box-sizing: border-box;
}}

body {{

    margin: 0;

    padding: 10px;

    background: #111;

    color: #eee;

    font-family:
        Arial,
        sans-serif;

    font-size: 13px;

}}

h1 {{

    margin:
        5px 0 8px 0;

    font-size: 20px;

}}

.info {{

    background: #1b1b1b;

    border-radius: 8px;

    padding: 10px;

    margin-bottom: 10px;

    line-height: 1.6;

    color: #ccc;

}}

.section {{

    background: #181818;

    border-radius: 8px;

    margin-bottom: 12px;

    overflow: hidden;

}}

.section-title {{

    padding: 10px;

    font-size: 16px;

    font-weight: bold;

    background: #222;

}}

table {{

    width: 100%;

    table-layout: fixed;

    border-collapse:
        collapse;

}}

th,
td {{

    padding:
        7px 4px;

    border-bottom:
        1px solid #292929;

    text-align:
        center;

    vertical-align:
        middle;

}}

th {{

    background: #202020;

    color: #aaa;

    font-weight:
        normal;

    white-space:
        nowrap;

}}

/* -------------------------
   컬럼 너비
   ------------------------- */

th:nth-child(1),
td:nth-child(1) {{

    width: 6%;

}}

th:nth-child(2),
td:nth-child(2) {{

    width: 23%;

    text-align:
        left;

}}

th:nth-child(3),
td:nth-child(3) {{

    width: 18%;

}}

th:nth-child(4),
td:nth-child(4) {{

    width: 38%;

}}

th:nth-child(5),
td:nth-child(5) {{

    width: 15%;

}}

/* -------------------------
   코인
   ------------------------- */

.coin {{

    overflow:
        hidden;

}}

.coin-name {{

    font-weight:
        bold;

    font-size:
        14px;

    white-space:
        nowrap;

    overflow:
        hidden;

    text-overflow:
        ellipsis;

}}

.change {{

    margin-top:
        3px;

    font-size:
        11px;

    white-space:
        nowrap;

}}

.up {{

    color:
        #ff5252;

}}

.down {{

    color:
        #4da6ff;

}}

/* -------------------------
   거래대금
   ------------------------- */

.vol {{

    white-space:
        nowrap;

    font-size:
        12px;

}}

/* -------------------------
   EMA
   ------------------------- */

.ema-cell {{

    text-align:
        left;

    overflow:
        hidden;

}}

.ema {{

    font-size:
        10px;

    line-height:
        1.65;

    white-space:
        nowrap;

    overflow:
        hidden;

}}

.ema b {{

    display:
        inline-block;

    width:
        22px;

    color:
        #aaa;

}}

/* -------------------------
   경고
   ------------------------- */

.warning {{

    font-size:
        20px;

    white-space:
        nowrap;

}}

.air {{

    display:
        inline-block;

    animation:
        pulse 1.2s infinite;

}}

@keyframes pulse {{

    0% {{
        transform:
            scale(1);
    }}

    50% {{
        transform:
            scale(1.15);
    }}

    100% {{
        transform:
            scale(1);
    }}

}}

/* -------------------------
   조건 충족 행
   ------------------------- */

tr.qualified {{

    background:
        rgba(
            255,
            255,
            255,
            0.06
        );

}}

/* -------------------------
   방향
   ------------------------- */

.long {{

    color:
        #00e676;

    font-weight:
        bold;

}}

.short {{

    color:
        #ff5252;

    font-weight:
        bold;

}}

.footer {{

    color:
        #777;

    font-size:
        11px;

    text-align:
        center;

    padding:
        10px;

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

    ④ 이전 1H 완성봉 종가 &lt; EMA10<br>

    ⑤ 현재 1H 완성봉 양봉<br>

    ⑥ 현재 1H 완성봉 종가 &gt; EMA10<br>

    ⑦ 모든 조건 만족 → 🛩 ✈️

</div>

{upbit_html}

{okx_html}

<div class="footer">

    마지막 업데이트:
    {now}

    <br>

    현재 진행 중인 1H / 4H 봉은 제외

</div>

</body>

</html>
    """

    return HTMLResponse(
        content=html
    )


# =========================================================
# 업데이트
# =========================================================

def update_all():

    log.info(
        "========================================"
    )

    log.info(
        "시장 데이터 업데이트 시작"
    )

    if USE_UPBIT == "Y":

        update_upbit()

        log.info(
            f"Upbit TOP{TOP_N} 업데이트 완료"
        )

    if USE_OKX == "Y":

        update_okx()

        log.info(
            f"OKX TOP{TOP_N} 업데이트 완료"
        )

    log.info(
        "EMA = 10-30-60-120"
    )

    log.info(
        "🛩 ✈️ = 1H/4H 정배열 + "
        "이전 1H 종가 EMA10 아래 + "
        "현재 1H 양봉 + "
        "현재 1H 종가 EMA10 위"
    )

    log.info(
        "현재 진행 중인 봉 제외"
    )

    log.info(
        "========================================"
    )


# =========================================================
# 스케줄러
# =========================================================

def scheduler_loop():

    update_all()

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

if __name__ == "__main__":

    log.info(
        "🚀 1H EMA 비행기 경고 시스템 시작"
    )

    log.info(
        "15M EMA 및 N자 검색 로직 제거"
    )

    log.info(
        "비행기 기준 = 1H"
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
