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

logging.basicConfig(level=logging.INFO)


# =========================
# 전역 데이터
# =========================

# OKX
latest_okx_data = []

okx_long_candidates = []

okx_short_candidates = []


# 업비트
latest_upbit_data = []

upbit_long_candidates = []

upbit_short_candidates = []



# =========================
# API 재시도
# =========================

def retry_request(func, *args, **kwargs):

    for attempt in range(10):

        try:

            result = func(*args, **kwargs)


            if hasattr(result, "status_code"):

                if result.status_code == 429:

                    time.sleep(1)

                    continue


            return result


        except Exception as e:

            logging.error(
                f"API 실패 {attempt+1}/10 : {e}"
            )

            time.sleep(3)


    return None




# =========================
# OKX 캔들 조회
# =========================

def get_okx_ohlcv(
        inst_id,
        bar="1H",
        limit=48
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

        df = pd.DataFrame(
            response.json()["data"],
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


        df = (
            df.iloc[::-1]
            .reset_index(drop=True)
        )


        return df


    except Exception as e:

        logging.error(
            f"OKX 캔들 오류 {inst_id} : {e}"
        )

        return None




# =========================
# 업비트 캔들 조회
# =========================

def get_upbit_ohlcv(
        market,
        unit=60,
        count=48
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


        df = pd.DataFrame(data)


        df = df.iloc[::-1]


        df["trade_price"] = (
            df["trade_price"]
            .astype(float)
        )


        df["candle_acc_trade_price"] = (
            df["candle_acc_trade_price"]
            .astype(float)
        )


        return df.reset_index(drop=True)



    except Exception as e:


        logging.error(
            f"업비트 캔들 오류 {market} : {e}"
        )

        return None




# =========================
# OKX USDT-SWAP 목록
# =========================

def get_all_okx_swap_symbols():


    url = (
        "https://www.okx.com/api/v5/public/"
        "instruments?instType=SWAP"
    )


    response = retry_request(
        requests.get,
        url
    )


    if response is None:

        return []


    data = response.json().get(
        "data",
        []
    )


    return [

        x["instId"]

        for x in data

        if "USDT" in x["instId"]

    ]




# =========================
# 업비트 KRW 목록
# =========================

def get_upbit_markets():


    url = (
        "https://api.upbit.com/v1/market/all"
    )


    response = retry_request(
        requests.get,
        url
    )


    if response is None:

        return []



    return [

        x["market"]

        for x in response.json()

        if x["market"].startswith("KRW-")

    ]




# =========================
# USDT → KRW
# =========================

def get_usdt_krw():


    url = (
        "https://api.upbit.com/v1/ticker"
        "?markets=KRW-USDT"
    )


    response = retry_request(
        requests.get,
        url
    )


    if response is None:

        return 1400



    try:

        return response.json()[0]["trade_price"]


    except:

        return 1400

# =========================
# 거래대금 표시
# =========================

def format_volume(volume):

    if volume >= 1_000_000_000_000:

        return f"{volume / 1_000_000_000_000:.2f}조"


    elif volume >= 100_000_000:

        return f"{volume / 100_000_000:,.0f}억"


    else:

        return f"{volume / 10_000:,.0f}만원"





# =========================
# EMA 상태 계산
# =========================

def check_ema(
        df,
        price_column="c"
):

    if df is None or len(df) < 200:

        return "N/A"


    df["ema50"] = (
        df[price_column]
        .ewm(
            span=50,
            adjust=False
        )
        .mean()
    )


    df["ema200"] = (
        df[price_column]
        .ewm(
            span=200,
            adjust=False
        )
        .mean()
    )


    if (
        df["ema50"].iloc[-1]
        >
        df["ema200"].iloc[-1]
    ):

        return "🟢정배열"


    else:

        return "🔴역배열"





# =========================
# OKX EMA
# =========================

def get_okx_ema_status(
        inst_id,
        bar
):

    df = get_okx_ohlcv(
        inst_id,
        bar,
        220
    )


    return check_ema(
        df,
        "c"
    )





# =========================
# 업비트 EMA
# =========================

def get_upbit_ema_status(
        market,
        unit
):

    df = get_upbit_ohlcv(
        market,
        unit,
        220
    )


    return check_ema(
        df,
        "trade_price"
    )





# =========================
# OKX 24시간 거래대금
# =========================

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


    return (
        df["volCcyQuote"]
        .sum()
    )





# =========================
# 업비트 24시간 거래대금
# =========================

def get_upbit_volume(
        market
):

    df = get_upbit_ohlcv(
        market,
        60,
        24
    )


    if df is None:

        return 0


    return (
        df["candle_acc_trade_price"]
        .sum()
    )





# =========================
# 변동률
# =========================

def calculate_change(
        close_list
):

    if len(close_list) < 2:

        return None


    return round(

        (
            close_list[-1]
            -
            close_list[-2]
        )
        /
        close_list[-2]
        *
        100,

        2
    )





# =========================
# OKX 변동률
# =========================

def get_okx_change(
        inst_id
):

    df = get_okx_ohlcv(
        inst_id,
        "1H",
        48
    )


    if df is None:

        return None


    return calculate_change(
        df["c"].tolist()
    )





# =========================
# 업비트 변동률
# =========================

def get_upbit_change(
        market
):

    df = get_upbit_ohlcv(
        market,
        60,
        48
    )


    if df is None:

        return None


    return calculate_change(
        df["trade_price"].tolist()
    )





# =========================
# OKX TOP10 업데이트
# =========================

def update_okx():


    global latest_okx_data
    global okx_long_candidates
    global okx_short_candidates


    logging.info(
        "OKX TOP10 시작"
    )


    symbols = get_all_okx_swap_symbols()


    usdt_krw = get_usdt_krw()


    volume_map = {}


    for symbol in symbols:


        volume = get_okx_volume(
            symbol
        )


        volume_map[symbol] = (
            volume
            *
            usdt_krw
        )



    top10 = sorted(

        volume_map,

        key=volume_map.get,

        reverse=True

    )[:10]



    rows = []

    okx_long_candidates = []

    okx_short_candidates = []



    for rank, symbol in enumerate(
            top10,
            1
    ):


        coin = symbol.replace(
            "-USDT-SWAP",
            ""
        )


        change = get_okx_change(
            symbol
        )


        change_text = (
            f"🟢+{change}%"
            if change and change > 0
            else
            f"🔴{change}%"
        )


        ema1d = get_okx_ema_status(
            symbol,
            "1D"
        )


        ema4h = get_okx_ema_status(
            symbol,
            "4H"
        )


        ema15m = get_okx_ema_status(
            symbol,
            "15m"
        )



        if (
            ema4h == "🟢정배열"
            and
            ema15m == "🔴역배열"
        ):

            okx_long_candidates.append(
                {
                    "name":coin,
                    "volume":format_volume(
                        volume_map[symbol]
                    ),
                    "ema1d":ema1d,
                    "ema4h":ema4h,
                    "ema15m":ema15m
                }
            )


        elif (
            ema4h == "🔴역배열"
            and
            ema15m == "🟢정배열"
        ):

            okx_short_candidates.append(
                {
                    "name":coin,
                    "volume":format_volume(
                        volume_map[symbol]
                    ),
                    "ema1d":ema1d,
                    "ema4h":ema4h,
                    "ema15m":ema15m
                }
            )


        rows.append(
            {
                "rank":rank,
                "name":coin,
                "change":change_text,
                "volume":format_volume(
                    volume_map[symbol]
                ),
                "ema1d":ema1d,
                "ema4h":ema4h,
                "ema15m":ema15m
            }
        )


    latest_okx_data = rows



    logging.info(
        "OKX 완료"
        )

# =========================
# 업비트 TOP10 업데이트
# =========================

def update_upbit():

    global latest_upbit_data
    global upbit_long_candidates
    global upbit_short_candidates


    logging.info(
        "업비트 TOP10 시작"
    )


    markets = get_upbit_markets()


    volume_map = {}


    for market in markets:

        volume_map[market] = get_upbit_volume(
            market
        )



    top10 = sorted(
        volume_map,
        key=volume_map.get,
        reverse=True
    )[:10]



    rows = []

    upbit_long_candidates = []

    upbit_short_candidates = []



    for rank, market in enumerate(
        top10,
        1
    ):


        coin = market.replace(
            "KRW-",
            ""
        )


        change = get_upbit_change(
            market
        )


        if change is None:

            change_text = "N/A"


        elif change > 0:

            change_text = (
                f"🟢+{change}%"
            )

        else:

            change_text = (
                f"🔴{change}%"
            )



        ema1d = get_upbit_ema_status(
            market,
            1440
        )


        ema4h = get_upbit_ema_status(
            market,
            240
        )


        ema15m = get_upbit_ema_status(
            market,
            15
        )



        # 업비트 롱 후보

        if (
            ema4h == "🟢정배열"
            and
            ema15m == "🔴역배열"
        ):


            upbit_long_candidates.append(
                {
                    "name":coin,
                    "volume":format_volume(
                        volume_map[market]
                    ),
                    "ema1d":ema1d,
                    "ema4h":ema4h,
                    "ema15m":ema15m
                }
            )



        # 업비트 숏 후보

        elif (
            ema4h == "🔴역배열"
            and
            ema15m == "🟢정배열"
        ):


            upbit_short_candidates.append(
                {
                    "name":coin,
                    "volume":format_volume(
                        volume_map[market]
                    ),
                    "ema1d":ema1d,
                    "ema4h":ema4h,
                    "ema15m":ema15m
                }
            )



        rows.append(
            {
                "rank":rank,
                "name":coin,
                "change":change_text,
                "volume":format_volume(
                    volume_map[market]
                ),
                "ema1d":ema1d,
                "ema4h":ema4h,
                "ema15m":ema15m
            }
        )


    latest_upbit_data = rows


    logging.info(
        "업비트 완료"
    )





# =========================
# 전체 업데이트
# =========================

def update_dashboard():


    logging.info(
        "전체 조회 시작"
    )


    update_okx()


    update_upbit()


    logging.info(
        "전체 업데이트 완료"
    )





# =========================
# 웹 대시보드
# =========================

@app.get(
    "/",
    response_class=HTMLResponse
)

def dashboard():


    html = """

<html>

<head>

<meta http-equiv="refresh" content="300">


<title>
OKX + UPBIT DASHBOARD
</title>


<style>

body{

background:#111;
color:white;
font-family:Arial;
padding:20px;

}


table{

width:100%;
border-collapse:collapse;

}


th{

background:#333;
padding:10px;

}


td{

padding:8px;
border-bottom:1px solid #444;
text-align:center;

}


.box{

background:#222;
padding:15px;
margin-top:20px;

}

</style>


</head>


<body>


<h2>
📊 암호화폐 실시간 분석
</h2>


<p>
조회 : 5분 자동 업데이트
</p>


<h2>
🏆 OKX 선물 실거래대금 TOP10
</h2>


<table>

<tr>

<th>순위</th>
<th>코인</th>
<th>변동</th>
<th>거래대금</th>
<th>1D</th>
<th>4H</th>
<th>15M</th>

</tr>

"""


    for item in latest_okx_data:


        html += f"""

<tr>

<td>{item['rank']}</td>

<td>{item['name']}</td>

<td>{item['change']}</td>

<td>{item['volume']}</td>

<td>{item['ema1d']}</td>

<td>{item['ema4h']}</td>

<td>{item['ema15m']}</td>

</tr>

"""



    html += """

</table>


<div class="box">

📌 OKX 롱 추천

<br><br>

"""


    if okx_long_candidates:

        for i,item in enumerate(
            okx_long_candidates,
            1
        ):

            html += (

                f"{i}. {item['name']} "
                f"{item['volume']}<br>"
                f"1D {item['ema1d']} "
                f"4H {item['ema4h']} "
                f"15M {item['ema15m']}<br><br>"

            )

    else:

        html += "없음"



    html += """

<br>

📌 OKX 숏 추천

<br><br>

"""


    if okx_short_candidates:

        for i,item in enumerate(
            okx_short_candidates,
            1
        ):

            html += (

                f"{i}. {item['name']} "
                f"{item['volume']}<br>"
                f"1D {item['ema1d']} "
                f"4H {item['ema4h']} "
                f"15M {item['ema15m']}<br><br>"

            )

    else:

        html += "없음"



    html += """

</div>


<hr>


<h2>
🏆 업비트 현물 실거래대금 TOP10
</h2>


<table>


<tr>

<th>순위</th>
<th>코인</th>
<th>변동</th>
<th>거래대금</th>
<th>1D</th>
<th>4H</th>
<th>15M</th>

</tr>

"""



    for item in latest_upbit_data:


        html += f"""

<tr>

<td>{item['rank']}</td>

<td>{item['name']}</td>

<td>{item['change']}</td>

<td>{item['volume']}</td>

<td>{item['ema1d']}</td>

<td>{item['ema4h']}</td>

<td>{item['ema15m']}</td>

</tr>

"""


    html += """

</table>


<div class="box">

📌 업비트 롱 추천

<br><br>

"""



    if upbit_long_candidates:

        for i,item in enumerate(
            upbit_long_candidates,
            1
        ):

            html += (

                f"{i}. {item['name']} "
                f"{item['volume']}<br>"
                f"1D {item['ema1d']} "
                f"4H {item['ema4h']} "
                f"15M {item['ema15m']}<br><br>"

            )

    else:

        html += "없음"



    html += """

<br>

📌 업비트 숏 추천

<br><br>

"""


    if upbit_short_candidates:

        for i,item in enumerate(
            upbit_short_candidates,
            1
        ):

            html += (

                f"{i}. {item['name']} "
                f"{item['volume']}<br>"
                f"1D {item['ema1d']} "
                f"4H {item['ema4h']} "
                f"15M {item['ema15m']}<br><br>"

            )

    else:

        html += "없음"



    html += """

</div>


</body>

</html>

"""


    return html

# =========================
# 스케줄러
# =========================

def scheduler():

    while True:

        schedule.run_pending()

        time.sleep(1)





# =========================
# 서버 시작
# =========================

@app.on_event("startup")
def startup():


    # 최초 실행

    update_dashboard()



    # 5분마다 갱신

    schedule.every(
        5
    ).minutes.do(
        update_dashboard
    )



    threading.Thread(

        target=scheduler,

        daemon=True

    ).start()





# =========================
# 실행
# =========================

if __name__ == "__main__":


    uvicorn.run(

        app,

        host="0.0.0.0",

        port=8000

    )


