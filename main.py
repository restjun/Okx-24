from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import schedule
import time
import requests
import threading
import uvicorn
import logging
import pandas as pd


app = FastAPI()


logging.basicConfig(
    level=logging.INFO
)


# =========================================================
# 전역 데이터
# =========================================================

latest_okx_data = []
latest_upbit_data = []


# =========================================================
# API 재시도
# =========================================================

def retry_request(func, *args, **kwargs):

    for attempt in range(10):

        try:

            result = func(
                *args,
                **kwargs
            )

            if hasattr(result, "status_code"):

                if result.status_code == 429:

                    time.sleep(1)

                    continue

            return result

        except Exception as e:

            logging.error(
                f"API 실패 {attempt + 1}/10 : {e}"
            )

            time.sleep(3)

    return None


# =========================================================
# OKX 캔들
# =========================================================

def get_okx_ohlcv(
    inst_id,
    bar="1H",
    limit=200
):

    url = (
        "https://www.okx.com/api/v5/market/candles"
        f"?instId={inst_id}"
        f"&bar={bar}"
        f"&limit={limit}"
    )

    response = retry_request(
        requests.get,
        url
    )

    if response is None:
        return None

    try:

        data = response.json()["data"]

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

        df["c"] = (
            df["c"]
            .astype(float)
        )

        df["volCcyQuote"] = (
            df["volCcyQuote"]
            .astype(float)
        )

        df = df.iloc[::-1].reset_index(
            drop=True
        )

        return df

    except Exception as e:

        logging.error(
            f"OKX 오류 {inst_id}:{e}"
        )

        return None


# =========================================================
# 업비트 분봉
# =========================================================

def get_upbit_ohlcv(
    market,
    unit=60,
    count=200
):

    url = (
        "https://api.upbit.com/v1/candles/minutes/"
        f"{unit}"
        f"?market={market}"
        f"&count={count}"
    )

    response = retry_request(
        requests.get,
        url
    )

    if response is None:
        return None

    try:

        data = response.json()

        if not data:
            return None

        df = pd.DataFrame(data)

        df = df.iloc[::-1].reset_index(
            drop=True
        )

        df["trade_price"] = (
            df["trade_price"]
            .astype(float)
        )

        return df

    except Exception as e:

        logging.error(
            f"업비트 캔들 오류 {market}:{e}"
        )

        return None


# =========================================================
# OKX 목록
# =========================================================

def get_all_okx_swap_symbols():

    url = (
        "https://www.okx.com/api/v5/"
        "public/instruments?instType=SWAP"
    )

    response = retry_request(
        requests.get,
        url
    )

    if response is None:
        return []

    try:

        return [
            x["instId"]
            for x in response.json()["data"]
            if (
                x["instId"].endswith("-USDT-SWAP")
                and x.get("state") == "live"
            )
        ]

    except Exception as e:

        logging.error(
            f"OKX 목록 오류:{e}"
        )

        return []


# =========================================================
# 업비트 목록
# =========================================================

def get_upbit_markets():

    response = retry_request(
        requests.get,
        "https://api.upbit.com/v1/market/all"
    )

    if response is None:
        return []

    try:

        return [
            x["market"]
            for x in response.json()
            if x["market"].startswith("KRW-")
        ]

    except Exception as e:

        logging.error(
            f"업비트 목록 오류:{e}"
        )

        return []


# =========================================================
# USDT/KRW
# =========================================================

def get_usdt_krw():

    response = retry_request(
        requests.get,
        "https://api.upbit.com/v1/ticker?markets=KRW-USDT"
    )

    if response is None:
        return 1400

    try:

        return float(
            response.json()[0]["trade_price"]
        )

    except:

        return 1400


# =========================================================
# 거래대금 표시
# =========================================================

def format_volume(volume):

    if volume >= 1_000_000_000_000:

        return (
            f"{volume / 1_000_000_000_000:.2f}조"
        )

    elif volume >= 100_000_000:

        return (
            f"{volume / 100_000_000:,.0f}억"
        )

    else:

        return (
            f"{volume / 10_000:,.0f}만원"
        )


# =========================================================
# EMA 10-20 방향
# =========================================================

def get_ema_10_20_direction(
    df,
    column
):

    if df is None or len(df) < 20:
        return "none"

    ema10 = (
        df[column]
        .ewm(
            span=10,
            adjust=False
        )
        .mean()
    )

    ema20 = (
        df[column]
        .ewm(
            span=20,
            adjust=False
        )
        .mean()
    )

    if ema10.iloc[-1] > ema20.iloc[-1]:

        return "long"

    elif ema10.iloc[-1] < ema20.iloc[-1]:

        return "short"

    return "none"


# =========================================================
# EMA 20-60-120 방향
# =========================================================

def get_ema_20_60_120_direction(
    df,
    column
):

    if df is None or len(df) < 120:
        return "none"

    ema20 = (
        df[column]
        .ewm(
            span=20,
            adjust=False
        )
        .mean()
    )

    ema60 = (
        df[column]
        .ewm(
            span=60,
            adjust=False
        )
        .mean()
    )

    ema120 = (
        df[column]
        .ewm(
            span=120,
            adjust=False
        )
        .mean()
    )

    if (
        ema20.iloc[-1]
        >
        ema60.iloc[-1]
        >
        ema120.iloc[-1]
    ):

        return "long"

    elif (
        ema20.iloc[-1]
        <
        ema60.iloc[-1]
        <
        ema120.iloc[-1]
    ):

        return "short"

    return "none"


# =========================================================
# EMA 10-20 상태
# =========================================================

def check_ema_10_20(
    df,
    column
):

    if df is None or len(df) < 20:

        return "⚪(0)"

    df = df.copy()

    df["ema10"] = (
        df[column]
        .ewm(
            span=10,
            adjust=False
        )
        .mean()
    )

    df["ema20"] = (
        df[column]
        .ewm(
            span=20,
            adjust=False
        )
        .mean()
    )

    states = []

    for _, row in df.iterrows():

        if row["ema10"] > row["ema20"]:

            states.append("long")

        elif row["ema10"] < row["ema20"]:

            states.append("short")

        else:

            states.append("none")

    current_state = states[-1]

    if current_state == "none":

        return "⚪(0)"

    count = 0

    for state in reversed(states):

        if state == current_state:

            count += 1

        else:

            break

    if current_state == "long":

        return f"🟢({count})"

    elif current_state == "short":

        return f"🔴({count})"

    return "⚪(0)"


# =========================================================
# EMA 20-60-120 상태
# =========================================================

def check_ema(
    df,
    column
):

    if df is None or len(df) < 120:

        return "⚪(0)"

    df = df.copy()

    df["ema20"] = (
        df[column]
        .ewm(
            span=20,
            adjust=False
        )
        .mean()
    )

    df["ema60"] = (
        df[column]
        .ewm(
            span=60,
            adjust=False
        )
        .mean()
    )

    df["ema120"] = (
        df[column]
        .ewm(
            span=120,
            adjust=False
        )
        .mean()
    )

    states = []

    for _, row in df.iterrows():

        if (
            row["ema20"]
            >
            row["ema60"]
            >
            row["ema120"]
        ):

            states.append("long")

        elif (
            row["ema20"]
            <
            row["ema60"]
            <
            row["ema120"]
        ):

            states.append("short")

        else:

            states.append("none")

    current_state = states[-1]

    if current_state == "none":

        return "⚪(0)"

    count = 0

    for state in reversed(states):

        if state == current_state:

            count += 1

        else:

            break

    if current_state == "long":

        return f"🟢({count})"

    elif current_state == "short":

        return f"🔴({count})"

    return "⚪(0)"


# =========================================================
# 15분 경고
#
# 🚨 숏
# 15M 10-20 정배열
# 15M 20-60-120 역배열
# 4H 10-20 역배열
#
# 🚀 롱
# 15M 10-20 역배열
# 15M 20-60-120 정배열
# 4H 10-20 정배열
# =========================================================

def check_15m_warning(
    df15m,
    column15m,
    df4h,
    column4h
):

    if (
        df15m is None
        or df4h is None
        or len(df15m) < 120
        or len(df4h) < 20
    ):

        return "none"

    ema15m_10_20 = get_ema_10_20_direction(
        df15m,
        column15m
    )

    ema15m_20_60_120 = get_ema_20_60_120_direction(
        df15m,
        column15m
    )

    ema4h_10_20 = get_ema_10_20_direction(
        df4h,
        column4h
    )


    # 🚨 숏

    if (
        ema15m_10_20 == "long"
        and
        ema15m_20_60_120 == "short"
        and
        ema4h_10_20 == "short"
    ):

        return "short_warning"


    # 🚀 롱

    if (
        ema15m_10_20 == "short"
        and
        ema15m_20_60_120 == "long"
        and
        ema4h_10_20 == "long"
    ):

        return "long_warning"


    return "none"


# =========================================================
# 4H 경고
#
# 일봉을 삭제했기 때문에
# 4H 자체의 10-20 / 20-60-120 구조만 표시
#
# 4H 경고는 15M 경고에서 사용
# =========================================================

def check_4h_structure(
    df4h,
    column4h
):

    if df4h is None or len(df4h) < 120:

        return "none"

    ema4h_10_20 = get_ema_10_20_direction(
        df4h,
        column4h
    )

    ema4h_20_60_120 = get_ema_20_60_120_direction(
        df4h,
        column4h
    )

    if (
        ema4h_10_20 == "long"
        and
        ema4h_20_60_120 == "short"
    ):

        return "short_warning"

    if (
        ema4h_10_20 == "short"
        and
        ema4h_20_60_120 == "long"
    ):

        return "long_warning"

    return "none"


# =========================================================
# OKX 15M + 4H
# =========================================================

def get_okx_ema(
    inst_id
):

    df15m = get_okx_ohlcv(
        inst_id,
        "15m",
        200
    )

    df4h = get_okx_ohlcv(
        inst_id,
        "4H",
        200
    )

    return {

        "ema15m_10_20":
            check_ema_10_20(
                df15m,
                "c"
            ),

        "ema15m_20_60_120":
            check_ema(
                df15m,
                "c"
            ),

        "ema4h_10_20":
            check_ema_10_20(
                df4h,
                "c"
            ),

        "ema4h_20_60_120":
            check_ema(
                df4h,
                "c"
            ),

        "warning15m":
            check_15m_warning(
                df15m,
                "c",
                df4h,
                "c"
            ),

        "warning4h":
            check_4h_structure(
                df4h,
                "c"
            )

    }


# =========================================================
# 업비트 15M + 4H
# =========================================================

def get_upbit_ema(
    market
):

    df15m = get_upbit_ohlcv(
        market,
        15,
        200
    )

    df4h = get_upbit_ohlcv(
        market,
        240,
        200
    )

    return {

        "ema15m_10_20":
            check_ema_10_20(
                df15m,
                "trade_price"
            ),

        "ema15m_20_60_120":
            check_ema(
                df15m,
                "trade_price"
            ),

        "ema4h_10_20":
            check_ema_10_20(
                df4h,
                "trade_price"
            ),

        "ema4h_20_60_120":
            check_ema(
                df4h,
                "trade_price"
            ),

        "warning15m":
            check_15m_warning(
                df15m,
                "trade_price",
                df4h,
                "trade_price"
            ),

        "warning4h":
            check_4h_structure(
                df4h,
                "trade_price"
            )

    }


# =========================================================
# OKX 24시간 거래대금
# =========================================================

def get_okx_volume(
    inst_id
):

    df = get_okx_ohlcv(
        inst_id,
        "1H",
        24
    )

    if df is None:

        return 0

    return df[
        "volCcyQuote"
    ].sum()


# =========================================================
# 업비트 24시간 거래대금
# =========================================================

def get_upbit_volume_map():

    markets = get_upbit_markets()

    if not markets:

        return {}

    response = retry_request(
        requests.get,
        "https://api.upbit.com/v1/ticker?markets="
        +
        ",".join(markets)
    )

    if response is None:

        return {}

    try:

        return {

            x["market"]:
            x["acc_trade_price_24h"]

            for x in response.json()

        }

    except Exception as e:

        logging.error(
            f"업비트 거래대금 오류:{e}"
        )

        return {}


# =========================================================
# OKX 변동률
# 당일
# =========================================================

def get_okx_change(
    inst_id
):

    df = get_okx_ohlcv(
        inst_id,
        "1H",
        120
    )

    if df is None or len(df) < 50:

        return None

    df["datetime"] = (
        pd.to_datetime(
            df["ts"],
            unit="ms"
        )
        +
        pd.Timedelta(hours=9)
    )

    df.set_index(
        "datetime",
        inplace=True
    )

    daily = (
        df["c"]
        .resample(
            "1D",
            offset="9h"
        )
        .last()
    )

    if len(daily) < 2:

        return None

    current = daily.iloc[-1]
    previous = daily.iloc[-2]

    if previous == 0:

        return [0]

    change = (
        (current - previous)
        /
        previous
        *
        100
    )

    return [
        round(change, 2)
    ]


# =========================================================
# 업비트 변동률
# 당일
# =========================================================

def get_upbit_change(
    market
):

    df = get_upbit_ohlcv(
        market,
        60,
        120
    )

    if df is None or len(df) < 50:

        return None

    df["datetime"] = pd.to_datetime(
        df["candle_date_time_kst"]
    )

    df.set_index(
        "datetime",
        inplace=True
    )

    daily = (
        df["trade_price"]
        .resample(
            "1D",
            offset="9h"
        )
        .last()
    )

    if len(daily) < 2:

        return None

    current = daily.iloc[-1]
    previous = daily.iloc[-2]

    if previous == 0:

        return [0]

    change = (
        (current - previous)
        /
        previous
        *
        100
    )

    return [
        round(change, 2)
    ]


# =========================================================
# 변동률 표시
# =========================================================

def format_change(
    changes
):

    if changes is None or len(changes) == 0:

        return "N/A"

    x = changes[0]

    if x > 0:

        color = "🟩"
        sign = "+"

    elif x < 0:

        color = "🟥"
        sign = ""

    else:

        color = "⬜️"
        sign = ""

    return f"""
    <span class="change-item">

        <span class="change-icon">
            {color}
        </span>

        <span class="change-value">
            {sign}{x:.2f}%
        </span>

    </span>
    """


# =========================================================
# OKX TOP30
# =========================================================

def update_okx():

    global latest_okx_data

    logging.info(
        "OKX TOP30 시작"
    )

    symbols = get_all_okx_swap_symbols()

    if not symbols:

        return

    usdt_krw = get_usdt_krw()

    upbit_coin_set = {

        market.replace(
            "KRW-",
            ""
        )

        for market in get_upbit_markets()

    }

    volume_map = {}

    for symbol in symbols:

        try:

            volume_map[symbol] = (
                get_okx_volume(symbol)
                *
                usdt_krw
                /
                10
            )

        except Exception as e:

            logging.error(
                f"OKX 거래대금 오류 {symbol}: {e}"
            )

            volume_map[symbol] = 0


    top30 = sorted(
        volume_map,
        key=volume_map.get,
        reverse=True
    )[:30]


    rows = []

    rank = 1

    for symbol in top30:

        coin = symbol.replace(
            "-USDT-SWAP",
            ""
        )

        if coin in upbit_coin_set:

            coin = f"{coin}(업비트)"

        changes = get_okx_change(
            symbol
        )

        ema = get_okx_ema(
            symbol
        )

        rows.append({

            "rank":
                rank,

            "name":
                coin,

            "change":
                format_change(changes),

            "volume":
                format_volume(
                    volume_map[symbol]
                ),

            "ema":
                ema

        })

        rank += 1


    latest_okx_data = rows

    logging.info(
        "OKX 완료"
    )


# =========================================================
# 업비트 TOP30
# =========================================================

def update_upbit():

    global latest_upbit_data

    logging.info(
        "업비트 TOP30 시작"
    )

    volume_map = get_upbit_volume_map()

    if not volume_map:

        return

    top30 = sorted(
        volume_map,
        key=volume_map.get,
        reverse=True
    )[:30]


    rows = []

    rank = 1

    for market in top30:

        coin = market.replace(
            "KRW-",
            ""
        )

        changes = get_upbit_change(
            market
        )

        ema = get_upbit_ema(
            market
        )

        rows.append({

            "rank":
                rank,

            "name":
                coin,

            "change":
                format_change(changes),

            "volume":
                format_volume(
                    volume_map[market]
                ),

            "ema":
                ema

        })

        rank += 1


    latest_upbit_data = rows

    logging.info(
        "업비트 완료"
    )


# =========================================================
# 전체 업데이트
# =========================================================

def update_dashboard():

    logging.info(
        "전체 조회 시작"
    )

    try:

        update_okx()

    except Exception as e:

        logging.error(
            f"OKX 업데이트 오류: {e}"
        )

    try:

        update_upbit()

    except Exception as e:

        logging.error(
            f"업비트 업데이트 오류: {e}"
        )

    logging.info(
        "전체 업데이트 완료"
    )


# =========================================================
# 업데이트를 백그라운드에서 실행
# =========================================================

def run_update():

    try:

        update_dashboard()

    except Exception as e:

        logging.error(
            f"백그라운드 업데이트 오류: {e}"
        )


# =========================================================
# 스케줄러
# =========================================================

def scheduler():

    while True:

        try:

            schedule.run_pending()

        except Exception as e:

            logging.error(
                f"스케줄러 오류: {e}"
            )

        time.sleep(1)


# =========================================================
# EMA HTML
#
# 15M
# 🚨/🚀 + 10-20 + 20-60-120
#
# 4H
# 10-20 + 20-60-120
# =========================================================

def ema_html(
    ema
):

    # -----------------------------------------------------
    # 15M 경고
    # -----------------------------------------------------

    if ema["warning15m"] == "long_warning":

        warning15m = "🚀🚀"

    elif ema["warning15m"] == "short_warning":

        warning15m = "🚨🚨"

    else:

        warning15m = ""


    # -----------------------------------------------------
    # 4H 구조 경고
    # -----------------------------------------------------

    if ema["warning4h"] == "long_warning":

        warning4h = "🚀"

    elif ema["warning4h"] == "short_warning":

        warning4h = "🚨"

    else:

        warning4h = ""


    return f"""

<div class="ema-display">


    <!-- 15분 -->

    <div class="ema-period">

        <span class="ema-warning">
            {warning15m}
        </span>

        <span class="ema-time">
            15M
        </span>

        <span class="ema-status">
            {ema["ema15m_10_20"]}
        </span>

        <span class="ema-status">
            {ema["ema15m_20_60_120"]}
        </span>

    </div>


    <!-- 4시간 -->

    <div class="ema-period last">

        <span class="ema-warning">
            {warning4h}
        </span>

        <span class="ema-time">
            4H
        </span>

        <span class="ema-status">
            {ema["ema4h_10_20"]}
        </span>

        <span class="ema-status">
            {ema["ema4h_20_60_120"]}
        </span>

    </div>


</div>

"""


# =========================================================
# 웹 대시보드
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def dashboard():

    html = """

<html>

<head>

<meta
    http-equiv="refresh"
    content="300"
>

<title>
OKX + UPBIT
</title>


<style>


/* =====================================================
   전체
   ===================================================== */

body{

    background:#111;

    color:white;

    font-family:Arial;

    padding:20px;

}


/* =====================================================
   테이블
   ===================================================== */

table{

    width:auto;

    border-collapse:collapse;

    border:1px solid #444;

}


/* =====================================================
   헤더
   ===================================================== */

th{

    background:#333;

    padding:10px 12px;

    border-right:2px solid #555;

    white-space:nowrap;

}


th:last-child{

    border-right:none;

}


/* =====================================================
   일반 셀
   ===================================================== */

td{

    padding:8px 12px;

    border-bottom:1px solid #444;

    border-right:2px solid #333;

    text-align:center;

    white-space:nowrap;

}


td:last-child{

    border-right:none;

}


/* =====================================================
   순위
   ===================================================== */

.rank-cell{

    width:45px;

    min-width:45px;

}


/* =====================================================
   코인
   ===================================================== */

.coin-cell{

    min-width:90px;

    text-align:left;

    font-weight:bold;

}


/* =====================================================
   거래대금
   ===================================================== */

.volume-cell{

    min-width:100px;

    padding-left:15px;

    padding-right:15px;

    text-align:right;

    white-space:nowrap;

}


/* =====================================================
   변동률
   ===================================================== */

.change-cell{

    min-width:105px;

    padding-left:12px;

    padding-right:12px;

    white-space:nowrap;

}


.change-item{

    display:inline-flex;

    align-items:center;

    width:95px;

    min-width:95px;

    box-sizing:border-box;

}


.change-icon{

    display:inline-block;

    width:28px;

    min-width:28px;

    text-align:center;

}


.change-value{

    display:inline-block;

    width:67px;

    min-width:67px;

    text-align:right;

    font-family:monospace;

}


/* =====================================================
   EMA 전체
   ===================================================== */

.ema-display{

    display:flex;

    align-items:center;

    height:28px;

    white-space:nowrap;

    font-family:monospace;

    padding:0 5px;

}


/* =====================================================
   EMA 시간봉
   ===================================================== */

.ema-period{

    display:flex;

    align-items:center;

    padding:0 10px;

    border-right:2px solid #555;

}


.ema-period.last{

    border-right:none;

}


/* =====================================================
   경고
   ===================================================== */

.ema-warning{

    display:inline-block;

    width:45px;

    min-width:45px;

    text-align:center;

}


/* =====================================================
   시간봉
   ===================================================== */

.ema-time{

    display:inline-block;

    width:40px;

    min-width:40px;

    text-align:left;

    font-weight:bold;

}


/* =====================================================
   EMA 상태
   ===================================================== */

.ema-status{

    display:inline-block;

    width:75px;

    min-width:75px;

    text-align:left;

}


/* =====================================================
   섹션
   ===================================================== */

.section-title{

    margin-top:25px;

    padding:10px 12px;

    background:#222;

    border-left:5px solid #666;

}


/* =====================================================
   hover
   ===================================================== */

tr:hover{

    background:#1d1d1d;

}


</style>

</head>


<body>


<h2>
📊 암호화폐 실시간 분석
</h2>


<p>
변동률 : 오늘
</p>


<h2 class="section-title">
🏆 OKX 선물 거래대금 TOP30
</h2>


<table>

<tr>

<th class="rank-cell">
순위
</th>

<th>
코인
</th>

<th>
거래대금
</th>

<th>
오늘
</th>

<th>
EMA 상태
</th>

</tr>

"""


    # =====================================================
    # OKX
    # =====================================================

    for item in latest_okx_data:

        html += f"""

<tr>

<td class="rank-cell">
{item['rank']}
</td>

<td class="coin-cell">
{item['name']}
</td>

<td class="volume-cell">
{item['volume']}
</td>

<td class="change-cell">
{item['change']}
</td>

<td>

{ema_html(
    item["ema"]
)}

</td>

</tr>

"""


    html += """

</table>


<h2 class="section-title">
🏆 업비트 현물 거래대금 TOP30
</h2>


<table>

<tr>

<th class="rank-cell">
순위
</th>

<th>
코인
</th>

<th>
거래대금
</th>

<th>
오늘
</th>

<th>
EMA 상태
</th>

</tr>

"""


    # =====================================================
    # 업비트
    # =====================================================

    for item in latest_upbit_data:

        html += f"""

<tr>

<td class="rank-cell">
{item['rank']}
</td>

<td class="coin-cell">
{item['name']}
</td>

<td class="volume-cell">
{item['volume']}
</td>

<td class="change-cell">
{item['change']}
</td>

<td>

{ema_html(
    item["ema"]
)}

</td>

</tr>

"""


    html += """

</table>


</body>

</html>

"""


    return html


# =========================================================
# 시작
# =========================================================

@app.on_event("startup")
def startup():

    # 서버 시작과 데이터 수집을 분리
    # Uvicorn lifespan CancelledError 방지

    threading.Thread(
        target=run_update,
        daemon=True
    ).start()


    schedule.every(5).minutes.do(
        run_update
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
