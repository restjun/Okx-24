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
# 전역 저장 데이터
# =========================

previous_top10 = set()

latest_data = []


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
                f"API 실패 ({attempt+1}/10): {e}"
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
        f"?instId={inst_id}&bar={bar}&limit={limit}"
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
                "confirm",
            ],
        )


        for col in [
            "c",
            "volCcyQuote"
        ]:

            df[col] = df[col].astype(float)



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
            + pd.Timedelta(hours=9)
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
# 거래대금 표시
# =========================

def format_volume_in_eok(volume):

    try:

        m = int(
            volume
            /
            1_000_000
        )

        return f"{m}M"


    except:

        return "0"
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
# 24시간 거래대금
# =========================

def get_24h_volume(inst_id):

    df = get_ohlcv_okx(
        inst_id,
        bar="1H",
        limit=24
    )


    if df is None or len(df) < 24:

        return 0


    return df["volCcyQuote"].sum()



# =========================
# TOP10 데이터 저장
# =========================

def save_dashboard_data(all_ids):

    global latest_data
    global previous_top10


    volume_map = {}


    for inst_id in all_ids:

        volume_map[inst_id] = (
            get_24h_volume(inst_id)
        )



    top_ids = sorted(
        volume_map,
        key=volume_map.get,
        reverse=True
    )[:10]



    current_top10 = set(top_ids)



    rows = []



    for rank, inst_id in enumerate(
        top_ids,
        start=1
    ):


        name = (
            inst_id
            .replace(
                "-USDT-SWAP",
                ""
            )
        )


        volume = format_volume_in_eok(
            volume_map[inst_id]
        )


        change = calculate_daily_change(
            inst_id
        )



        if change is None:

            change_text = "N/A"


        elif change > 0:

            change_text = (
                f"🟢 +{change}%"
            )


        else:

            change_text = (
                f"🔴 {change}%"
            )



        rows.append(
            {
                "rank": rank,
                "name": name,
                "change": change_text,
                "volume": volume
            }
        )



    latest_data = rows


    previous_top10 = current_top10


    logging.info(
        "대시보드 데이터 업데이트 완료"
    )



# =========================
# 메인 실행
# =========================

def main():

    logging.info(
        "OKX TOP10 검색 시작"
    )


    all_ids = (
        get_all_okx_swap_symbols()
    )


    if not all_ids:

        logging.error(
            "심볼 조회 실패"
        )

        return



    save_dashboard_data(
        all_ids
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

<meta http-equiv="refresh" content="10">


<title>
OKX Dashboard
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

padding:12px;

border-bottom:1px solid #444;

text-align:center;

font-size:18px;

}


</style>


</head>


<body>


<h2>
🏆 OKX 실거래대금 TOP10
</h2>


<p>
자동 업데이트 : 1분
</p>


<table>


<tr>

<th>
순위
</th>

<th>
코인
</th>

<th>
24시간 변동
</th>

<th>
거래대금
</th>


</tr>

"""


    for item in latest_data:


        html += f"""

<tr>

<td>
{item['rank']}
</td>


<td>
{item['name']}
</td>


<td>
{item['change']}
</td>


<td>
{item['volume']}
</td>


</tr>

"""


    html += """

</table>


</body>


</html>

"""


    return html



# =========================
# 스케줄러
# =========================

def run_scheduler():

    while True:

        schedule.run_pending()

        time.sleep(1)



@app.on_event("startup")
def start_scheduler():


    main()


    schedule.every(
        1
    ).minutes.do(
        main
    )


    threading.Thread(
        target=run_scheduler,
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
