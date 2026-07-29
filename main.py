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


# =========================
# 전역 데이터
# =========================

latest_data = []

long_candidates = []

short_candidates = []



# =========================
# API 재시도
# =========================

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
                f"API 실패 {attempt+1}/10 : {e}"
            )

            time.sleep(3)


    return None




# =========================
# OKX 캔들 데이터
# =========================

def get_ohlcv_okx(
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
            f"{inst_id} 데이터 오류 : {e}"
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

        item["instId"]

        for item in data

        if "USDT" in item["instId"]

    ]





# =========================
# 업비트 KRW 상장 목록
# =========================

def get_upbit_symbols():


    url = (
        "https://api.upbit.com/v1/market/all"
    )



    response = retry_request(
        requests.get,
        url
    )



    if response is None:

        return set()



    data = response.json()



    return {

        item["market"]
        .replace(
            "KRW-",
            ""
        )

        for item in data

        if item["market"].startswith(
            "KRW-"
        )

    }






# =========================
# 24시간 거래대금
# =========================

def get_24h_volume(inst_id):


    df = get_ohlcv_okx(
        inst_id,
        bar="1H",
        limit=24
    )


    if df is None:

        return 0



    return (
        df["volCcyQuote"]
        .sum()
    )





# =========================
# 거래대금 표시
# =========================

def format_volume(volume):


    if volume >= 100_000_000_000:


        return (
            f"{volume / 100_000_000_000:.2f}조"
        )



    elif volume >= 100_000_000:


        return (
            f"{volume / 100_000_000:,.0f}억"
        )



    else:


        return (
            f"{volume / 10_000:,.0f}만"
        )





# =========================
# EMA50 / EMA200 체크
# =========================

def check_ema_status(
    inst_id,
    bar
):


    df = get_ohlcv_okx(
        inst_id,
        bar=bar,
        limit=220
    )



    if df is None or len(df) < 200:

        return "N/A"




    df["ema50"] = (
        df["c"]
        .ewm(
            span=50,
            adjust=False
        )
        .mean()
    )



    df["ema200"] = (
        df["c"]
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
# 일봉 변동률
# =========================

def calculate_daily_change(inst_id):

    df = get_ohlcv_okx(
        inst_id,
        bar="1H",
        limit=48
    )


    if df is None or len(df) < 24:

        return None


    try:

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


        return round(
            (
                daily.iloc[-1]
                -
                daily.iloc[-2]
            )
            /
            daily.iloc[-2]
            *
            100,
            2
        )


    except:

        return None





# =========================
# TOP20 업데이트
# =========================

def update_dashboard():

    global latest_data
    global long_candidates
    global short_candidates


    logging.info(
        "OKX TOP20 검색 시작"
    )



    symbols = get_all_okx_swap_symbols()


    if not symbols:

        return



    upbit_list = get_upbit_symbols()



    volume_map = {}



    # 거래대금 계산

    for inst_id in symbols:


        volume_map[inst_id] = (
            get_24h_volume(inst_id)
        )



    # 거래대금 TOP20

    top20 = sorted(
        volume_map,
        key=volume_map.get,
        reverse=True
    )[:20]



    rows = []

    long_candidates = []

    short_candidates = []



    for rank, inst_id in enumerate(
        top20,
        start=1
    ):


        coin = (
            inst_id
            .replace(
                "-USDT-SWAP",
                ""
            )
        )


        display_name = coin



        if coin in upbit_list:

            display_name += "(업비트)"



        volume = format_volume(
            volume_map[inst_id]
        )



        change = calculate_daily_change(
            inst_id
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



        ema4h = check_ema_status(
            inst_id,
            "4H"
        )


        ema15m = check_ema_status(
            inst_id,
            "15m"
        )



        # =========================
        # 매매 후보
        # 거래대금 순서 유지
        # =========================


        if (
            ema4h == "🟢정배열"
            and
            ema15m == "🔴역배열"
        ):


            long_candidates.append(
                {
                    "name": display_name,
                    "volume": volume,
                    "ema4h": ema4h,
                    "ema15m": ema15m
                }
            )



        elif (
            ema4h == "🔴역배열"
            and
            ema15m == "🟢역배열"
        ):


            short_candidates.append(
                {
                    "name": display_name,
                    "volume": volume,
                    "ema4h": ema4h,
                    "ema15m": ema15m
                }
            )



        rows.append(
            {
                "rank": rank,
                "name": display_name,
                "change": change_text,
                "volume": volume,
                "ema4h": ema4h,
                "ema15m": ema15m
            }
        )



    latest_data = rows



    logging.info(
        "TOP20 업데이트 완료"
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
OKX TOP20
</title>


<style>

body{

background:#111;

color:white;

font-family:Arial;

padding:20px;

}


h2{

color:#00ff99;

}


table{

width:100%;

border-collapse:collapse;

}


th{

background:#333;

padding:12px;

}


td{

padding:10px;

border-bottom:1px solid #444;

text-align:center;

font-size:17px;

}


.box{

margin-top:25px;

padding:15px;

background:#222;

font-size:17px;

}

</style>


</head>


<body>


<h2>
🏆 OKX 실거래대금 TOP20
</h2>



<table>

<tr>

<th>순위</th>
<th>코인</th>
<th>변동</th>
<th>거래대금</th>
<th>4H EMA</th>
<th>15M EMA</th>

</tr>

"""



    for item in latest_data:


        html += f"""

<tr>

<td>{item['rank']}</td>

<td>{item['name']}</td>

<td>{item['change']}</td>

<td>{item['volume']}</td>

<td>{item['ema4h']}</td>

<td>{item['ema15m']}</td>

</tr>

"""



    html += """

</table>


<div class="box">


📌 롱 추천 (거래대금 순)

<br><br>

"""



    if long_candidates:


        for i, item in enumerate(
            long_candidates,
            start=1
        ):


            html += (
                f"{i} "
                f"{item['name']} "
                f"{item['volume']} "
                f"4H{item['ema4h']} "
                f"15M{item['ema15m']}<br>"
            )


    else:

        html += "없음"



    html += """

<br><br>


📌 숏 추천 (거래대금 순)

<br><br>

"""



    if short_candidates:


        for i, item in enumerate(
            short_candidates,
            start=1
        ):


            html += (
                f"{i} "
                f"{item['name']} "
                f"{item['volume']} "
                f"4H{item['ema4h']} "
                f"15M{item['ema15m']}<br>"
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





@app.on_event("startup")
def startup():


    update_dashboard()



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
