from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import schedule
import time
import requests
import threading
import uvicorn
import logging
import pandas as pd


app=FastAPI()


logging.basicConfig(
    level=logging.INFO
)


# =========================
# 전역 데이터
# =========================

latest_okx_data=[]

latest_upbit_data=[]



# =========================
# API 재시도
# =========================

def retry_request(func,*args,**kwargs):

    for attempt in range(10):

        try:

            result=func(
                *args,
                **kwargs
            )


            if hasattr(result,"status_code"):

                if result.status_code==429:

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
# OKX 캔들
# =========================

def get_okx_ohlcv(
    inst_id,
    bar="1H",
    limit=200
):

    url=(

        "https://www.okx.com/api/v5/market/candles"
        f"?instId={inst_id}"
        f"&bar={bar}"
        f"&limit={limit}"

    )


    response=retry_request(
        requests.get,
        url
    )


    if response is None:

        return None



    try:

        df=pd.DataFrame(

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


        df["c"]=(
            df["c"]
            .astype(float)
        )


        df["volCcyQuote"]=(
            df["volCcyQuote"]
            .astype(float)
        )


        df=df.iloc[::-1].reset_index(
            drop=True
        )


        return df



    except Exception as e:


        logging.error(
            f"OKX 오류 {inst_id}:{e}"
        )


        return None



# =========================
# 업비트 분봉
# =========================

def get_upbit_ohlcv(
    market,
    unit=60,
    count=200
):

    url=(

        "https://api.upbit.com/v1/candles/minutes/"
        f"{unit}"
        f"?market={market}"
        f"&count={count}"

    )


    response=retry_request(
        requests.get,
        url
    )


    if response is None:

        return None



    try:

        df=pd.DataFrame(
            response.json()
        )


        df=df.iloc[::-1].reset_index(
            drop=True
        )


        df["trade_price"]=(
            df["trade_price"]
            .astype(float)
        )


        return df



    except Exception as e:


        logging.error(
            f"업비트 캔들 오류 {market}:{e}"
        )


        return None

# =========================
# 업비트 일봉
# =========================

def get_upbit_day_ohlcv(
    market,
    count=200
):

    url=(

        "https://api.upbit.com/v1/candles/days"
        f"?market={market}"
        f"&count={count}"

    )


    response=retry_request(
        requests.get,
        url
    )


    if response is None:

        return None


    try:

        df=pd.DataFrame(
            response.json()
        )


        df=df.iloc[::-1].reset_index(
            drop=True
        )


        df["trade_price"]=(
            df["trade_price"]
            .astype(float)
        )


        return df


    except Exception as e:

        logging.error(
            f"업비트 일봉 오류 {market}:{e}"
        )

        return None



# =========================
# OKX 목록
# =========================

def get_all_okx_swap_symbols():

    url=(
        "https://www.okx.com/api/v5/"
        "public/instruments?instType=SWAP"
    )


    response=retry_request(
        requests.get,
        url
    )


    if response is None:

        return []


    try:

        return [

            x["instId"]

            for x in response.json()["data"]

            if "USDT" in x["instId"]

        ]


    except Exception as e:

        logging.error(
            f"OKX 목록 오류:{e}"
        )

        return []



# =========================
# 업비트 목록
# =========================

def get_upbit_markets():

    response=retry_request(
        requests.get,
        "https://api.upbit.com/v1/market/all"
    )


    if response is None:

        return []


    try:

        return [

            x["market"]

            for x in response.json()

            if x["market"].startswith(
                "KRW-"
            )

        ]


    except Exception as e:

        logging.error(
            f"업비트 목록 오류:{e}"
        )

        return []



# =========================
# USDT/KRW
# =========================

def get_usdt_krw():

    response=retry_request(
        requests.get,
        "https://api.upbit.com/v1/ticker?markets=KRW-USDT"
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

    if volume>=1_000_000_000_000:

        return (
            f"{volume/1_000_000_000_000:.2f}조"
        )


    elif volume>=100_000_000:

        return (
            f"{volume/100_000_000:,.0f}억"
        )


    else:

        return (
            f"{volume/10_000:,.0f}만원"
        )



# =========================
# EMA 계산
# =========================

def check_ema(df,column):

    if df is None or len(df)<200:

        return "N/A"


    df["ema50"]=(
        df[column]
        .ewm(
            span=50,
            adjust=False
        )
        .mean()
    )


    df["ema200"]=(
        df[column]
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

        return "🟢"


    return "🔴"

# =========================
# OKX EMA 상태
# =========================

def get_okx_ema_status(
    inst_id,
    bar
):

    df=get_okx_ohlcv(
        inst_id,
        bar,
        200
    )


    return check_ema(
        df,
        "c"
    )



# =========================
# 업비트 EMA 상태
# =========================

def get_upbit_ema_status(
    market,
    unit
):

    if unit=="DAY":

        df=get_upbit_day_ohlcv(
            market,
            200
        )

    else:

        df=get_upbit_ohlcv(
            market,
            unit,
            200
        )


    return check_ema(
        df,
        "trade_price"
    )



# =========================
# OKX 24시간 거래대금
# =========================

def get_okx_volume(inst_id):

    df=get_okx_ohlcv(
        inst_id,
        "1H",
        24
    )


    if df is None:

        return 0


    return df["volCcyQuote"].sum()



# =========================
# 업비트 24시간 거래대금
# =========================

def get_upbit_volume_map():

    markets=get_upbit_markets()


    response=retry_request(
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



# =========================
# OKX 변동률
# -3일 / -2일 / -1일 / 오늘
# =========================

def get_okx_change(inst_id):

    df=get_okx_ohlcv(
        inst_id,
        "1H",
        120
    )


    if df is None or len(df)<50:

        return None



    df["datetime"]=(
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


    daily=(

        df["c"]
        .resample(
            "1D",
            offset="9h"
        )
        .last()

    )


    if len(daily)<5:

        return None



    result=[]


    for i in [-3,-2,-1]:


        if daily.iloc[i-1]==0:

            result.append(0)

            continue



        change=(

            (
                daily.iloc[i]
                -
                daily.iloc[i-1]
            )
            /
            daily.iloc[i-1]
            *
            100

        )


        result.append(
            round(change,2)
        )


    return result

# =========================
# 업비트 변동률
# -3일 / -2일 / -1일 / 오늘
# =========================

def get_upbit_change(market):

    df=get_upbit_ohlcv(
        market,
        60,
        120
    )


    if df is None or len(df)<50:

        return None



    df["datetime"]=pd.to_datetime(
        df["candle_date_time_kst"]
    )


    df.set_index(
        "datetime",
        inplace=True
    )


    daily=(

        df["trade_price"]
        .resample(
            "1D",
            offset="9h"
        )
        .last()

    )


    if len(daily)<5:

        return None



    result=[]


    for i in [-3,-2,-1]:


        if daily.iloc[i-1]==0:

            result.append(0)

            continue



        change=(

            (
                daily.iloc[i]
                -
                daily.iloc[i-1]
            )
            /
            daily.iloc[i-1]
            *
            100

        )


        result.append(
            round(change,2)
        )


    return result



# =========================
# 변동률 표시
# 자리 고정 + 색상볼
# =========================

def format_change(changes):

    if changes is None:

        return "N/A"


    result=[]


    for x in changes:


        if x>0:

            text=f"🟢+{x:.2f}%"

        elif x<0:

            text=f"🔴{x:.2f}%"

        else:

            text=f"⚪0.00%"



        # 자리 고정
        result.append(
            text.rjust(11)
        )


    return " / ".join(result)

# =========================
# OKX TOP20 업데이트
# =========================

def update_okx():

    global latest_okx_data


    logging.info(
        "OKX TOP20 시작"
    )


    symbols=get_all_okx_swap_symbols()


    usdt_krw=get_usdt_krw()



    upbit_coin_set={

        market.replace(
            "KRW-",
            ""
        )

        for market in get_upbit_markets()

    }



    volume_map={}



    for symbol in symbols:


        volume_map[symbol]=(

            get_okx_volume(symbol)
            *
            usdt_krw

        )



    top20=sorted(

        volume_map,

        key=volume_map.get,

        reverse=True

    )[:10]



    rows=[]


    rank=1



    for symbol in top20:


        coin=symbol.replace(
            "-USDT-SWAP",
            ""
        )



        if coin in upbit_coin_set:

            coin=f"{coin}(업비트)"



        changes=get_okx_change(
            symbol
        )


        change_text=format_change(
            changes
        )



        ema1d=get_okx_ema_status(
            symbol,
            "1D"
        )


        ema4h=get_okx_ema_status(
            symbol,
            "4H"
        )


        ema15m=get_okx_ema_status(
            symbol,
            "15m"
        )



        rows.append({

            "rank":rank,

            "name":coin,

            "change":change_text,

            "volume":format_volume(
                volume_map[symbol]
            ),

            "ema1d":ema1d,

            "ema4h":ema4h,

            "ema15m":ema15m

        })


        rank+=1



    latest_okx_data=rows


    logging.info(
        "OKX 완료"
    )



# =========================
# 업비트 TOP20 업데이트
# =========================

def update_upbit():

    global latest_upbit_data


    logging.info(
        "업비트 TOP20 시작"
    )



    volume_map=get_upbit_volume_map()



    if not volume_map:

        return



    top20=sorted(

        volume_map,

        key=volume_map.get,

        reverse=True

    )[:10]



    rows=[]


    rank=1



    for market in top20:


        coin=market.replace(
            "KRW-",
            ""
        )



        changes=get_upbit_change(
            market
        )


        change_text=format_change(
            changes
        )



        ema1d=get_upbit_ema_status(
            market,
            "DAY"
        )


        ema4h=get_upbit_ema_status(
            market,
            240
        )


        ema15m=get_upbit_ema_status(
            market,
            15
        )



        rows.append({

            "rank":rank,

            "name":coin,

            "change":change_text,

            "volume":format_volume(
                volume_map[market]
            ),

            "ema1d":ema1d,

            "ema4h":ema4h,

            "ema15m":ema15m

        })


        rank+=1



    latest_upbit_data=rows


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
# 스케줄러
# =========================

def scheduler():

    while True:

        schedule.run_pending()

        time.sleep(1)



# =========================
# 웹 대시보드
# =========================

@app.get("/",response_class=HTMLResponse)
def dashboard():

    html="""

<html>

<head>

<meta http-equiv="refresh" content="300">


<title>
OKX+UPBIT
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

white-space:pre;

font-family:monospace;

}



</style>


</head>


<body>


<h2>
📊 암호화폐 실시간 분석
</h2>


<p>
변동률 : -2일 / -1일 / 오늘
</p>



<h2>
🏆 OKX 선물 거래대금 TOP20
</h2>


<table>

<tr>

<th>순위</th>

<th>코인</th>

<th>-3일/-2일/-1일/오늘</th>

<th>거래대금</th>

<th>1D</th>

<th>4H</th>

<th>15M</th>

</tr>

"""



    for item in latest_okx_data:


        html+=f"""

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



    html+="""

</table>


<hr>



<h2>
🏆 업비트 현물 거래대금 TOP20
</h2>



<table>


<tr>


<th>순위</th>

<th>코인</th>

<th>-2일/-1일/오늘</th>

<th>거래대금</th>

<th>1D</th>

<th>4H</th>

<th>15M</th>


</tr>


"""



    for item in latest_upbit_data:


        html+=f"""

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



    html+="""

</table>


</body>


</html>

"""


    return html




# =========================
# 시작
# =========================

@app.on_event("startup")
def startup():


    update_dashboard()


    schedule.every(5).minutes.do(
        update_dashboard
    )


    threading.Thread(

        target=scheduler,

        daemon=True

    ).start()



# =========================
# 실행
# =========================

if __name__=="__main__":


    uvicorn.run(

        app,

        host="0.0.0.0",

        port=8000

    )
    
