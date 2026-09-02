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
# 설정
# =========================================================

VOLUME_HOURS = 24
TOP_N = 30
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
air_state_lock = threading.Lock()

last_request_time = 0

air_state = {}


# =========================================================
# 시간
# =========================================================

def kst():
    return datetime.now(KST)


# =========================================================
# API 요청
# =========================================================

def wait_request():

    global last_request_time

    with request_lock:

        now = time.time()

        diff = now - last_request_time

        if diff < REQUEST_INTERVAL:
            time.sleep(
                REQUEST_INTERVAL - diff
            )

        last_request_time = time.time()


def retry(func, *args, **kwargs):

    for attempt in range(MAX_RETRIES):

        try:

            wait_request()

            return func(*args, **kwargs)

        except Exception as e:

            log.warning(
                f"API retry {attempt + 1}/{MAX_RETRIES}: {e}"
            )

            if attempt < MAX_RETRIES - 1:

                time.sleep(RATE_LIMIT_WAIT)

    return None


# =========================================================
# EMA
# =========================================================

def ema(df, period):

    if df is None or df.empty:
        return pd.Series(dtype=float)

    close = pd.to_numeric(
        df["c"],
        errors="coerce"
    )

    return close.ewm(
        span=period,
        adjust=False
    ).mean()


# =========================================================
# EMA 방향
# =========================================================

def direction(df):

    if df is None or len(df) < 120:
        return "none"

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

    return "none"


# =========================================================
# EMA 정배열 연속 카운트
# =========================================================

def ema_alignment_count(df):

    if df is None or df.empty:
        return 0

    e10 = ema(df, 10)
    e30 = ema(df, 30)
    e60 = ema(df, 60)
    e120 = ema(df, 120)

    count = 0

    for i in range(len(df) - 1, -1, -1):

        if (
            e10.iloc[i] >
            e30.iloc[i] >
            e60.iloc[i] >
            e120.iloc[i]
        ):
            count += 1

        else:
            break

    return count


# =========================================================
# EMA 표시
# =========================================================

def ema_display(df):

    if df is None or df.empty:
        return "-"

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

        count = ema_alignment_count(df)

        return f"🟢({count})"

    if (
        e10.iloc[-1] <
        e30.iloc[-1] <
        e60.iloc[-1] <
        e120.iloc[-1]
    ):

        count = ema_alignment_count(df)

        return f"🔴({count})"

    return "⚪"


# =========================================================
# ★ 핵심
# EMA1(종가) ↔ EMA10 교차 카운팅
#
# 위줄  : 상승
# 아래줄: 하락
#
# 종료된 이전 카운트는 회색
# =========================================================

def ema10_cross_count(df):

    result = {
        "up": 0,
        "down": 0,
        "final_up": 0,
        "final_down": 0
    }

    if df is None or len(df) < 2:
        return result

    close = pd.to_numeric(
        df["c"],
        errors="coerce"
    )

    e10 = ema(df, 10)

    relation = []

    for i in range(len(df)):

        c = close.iloc[i]
        e = e10.iloc[i]

        if pd.isna(c) or pd.isna(e):

            relation.append("none")

        elif c > e:

            relation.append("up")

        elif c < e:

            relation.append("down")

        else:

            relation.append("equal")


    # -----------------------------------------------------
    # 마지막 종가 상태
    # -----------------------------------------------------

    current = relation[-1]

    if current == "equal":

        return result


    # -----------------------------------------------------
    # 가장 최근 EMA1 ↔ EMA10 교차 찾기
    # -----------------------------------------------------

    last_cross = None

    for i in range(len(df) - 1, 0, -1):

        prev = relation[i - 1]
        curr = relation[i]

        # 상승 교차
        if (
            prev in ("down", "equal")
            and curr == "up"
        ):

            last_cross = i
            break

        # 하락 교차
        if (
            prev in ("up", "equal")
            and curr == "down"
        ):

            last_cross = i
            break


    # -----------------------------------------------------
    # 교차가 없으면 현재 연속 종가만 계산
    # -----------------------------------------------------

    if last_cross is None:

        count = 0

        for i in range(len(relation) - 1, -1, -1):

            if relation[i] == current:
                count += 1

            else:
                break

        if current == "up":
            result["up"] = count

        elif current == "down":
            result["down"] = count

        return result


    # -----------------------------------------------------
    # 현재 방향 카운트
    # 교차 발생 캔들 = 1
    # -----------------------------------------------------

    current_count = 0

    for i in range(
        len(relation) - 1,
        last_cross - 1,
        -1
    ):

        if relation[i] == current:

            current_count += 1

        else:

            break


    # -----------------------------------------------------
    # 이전 방향 최종 카운트
    # 교차 직전까지
    # -----------------------------------------------------

    previous = (
        "down"
        if current == "up"
        else "up"
    )

    final_count = 0

    i = last_cross - 1

    while i >= 0:

        if relation[i] == previous:

            final_count += 1
            i -= 1

        else:

            break


    # -----------------------------------------------------
    # 결과
    # -----------------------------------------------------

    if current == "up":

        result["up"] = current_count
        result["final_down"] = final_count

    elif current == "down":

        result["down"] = current_count
        result["final_up"] = final_count


    return result


# =========================================================
# ★ 10선 HTML
#
# 항상 2줄 고정
#
# 위 : 상승
# 아래 : 하락
# =========================================================

def ema10_cross_html(data):

    if not data:
        return """
        <div class="ema10-box">
            <div class="ema10-row">-</div>
            <div class="ema10-row">-</div>
        </div>
        """


    up = int(
        data.get("up", 0) or 0
    )

    down = int(
        data.get("down", 0) or 0
    )

    final_up = int(
        data.get("final_up", 0) or 0
    )

    final_down = int(
        data.get("final_down", 0) or 0
    )


    # =====================================================
    # 상승 줄
    # =====================================================

    if up > 0:

        up_html = (
            f'<span class="ema10-active up">'
            f'🟢({up})'
            f'</span>'
        )

    elif final_up > 0:

        up_html = (
            f'<span class="ema10-final">'
            f'⚪({final_up})'
            f'</span>'
        )

    else:

        up_html = '<span class="ema10-empty">-</span>'


    # =====================================================
    # 하락 줄
    # =====================================================

    if down > 0:

        down_html = (
            f'<span class="ema10-active down">'
            f'🔻({down})'
            f'</span>'
        )

    elif final_down > 0:

        down_html = (
            f'<span class="ema10-final">'
            f'⚪({final_down})'
            f'</span>'
        )

    else:

        down_html = '<span class="ema10-empty">-</span>'


    return f"""
    <div class="ema10-box">
        <div class="ema10-row">
            {up_html}
        </div>

        <div class="ema10-row">
            {down_html}
        </div>
    </div>
    """


# =========================================================
# 비행기 경고
# =========================================================

def get_air_warning(df1h, df4h):

    if (
        df1h is None
        or df4h is None
        or len(df1h) < 2
        or len(df4h) < 120
    ):
        return None


    if direction(df1h) != "long":
        return None


    if direction(df4h) != "long":
        return None


    e10 = ema(df1h, 10)


    prev_close = float(
        df1h["c"].iloc[-2]
    )

    prev_ema10 = float(
        e10.iloc[-2]
    )

    curr_open = float(
        df1h["o"].iloc[-1]
    )

    curr_close = float(
        df1h["c"].iloc[-1]
    )

    curr_ema10 = float(
        e10.iloc[-1]
    )


    # 이전 종가가 EMA10 아래
    if prev_close >= prev_ema10:
        return None


    # 현재 양봉
    if curr_close <= curr_open:
        return None


    # 현재 종가 EMA10 위
    if curr_close <= curr_ema10:
        return None


    return "LONG"


# =========================================================
# 비행기 카운터
# =========================================================

def update_air_counter(symbol, warning):

    global air_state

    with air_state_lock:

        if warning == "LONG":

            if symbol not in air_state:

                air_state[symbol] = {
                    "direction": "LONG",
                    "count": 1
                }

            else:

                if (
                    air_state[symbol]["direction"]
                    == "LONG"
                ):

                    air_state[symbol]["count"] += 1

                else:

                    air_state[symbol] = {
                        "direction": "LONG",
                        "count": 1
                    }

            return air_state[symbol]["count"]


        if symbol in air_state:

            return air_state[symbol]["count"]


        return 0


# =========================================================
# 빈 분석
# =========================================================

def empty_analysis():

    return {

        "direction_1h": "none",
        "direction_4h": "none",

        "ema_1h": "-",
        "ema_4h": "-",

        "ema10_cross_1h": {
            "up": 0,
            "down": 0,
            "final_up": 0,
            "final_down": 0
        },

        "air_warning": None,
        "air_count": 0
    }


# =========================================================
# 분석
# =========================================================

def analyze(symbol, df1h, df4h):

    result = empty_analysis()


    try:

        result["direction_1h"] = direction(df1h)

        result["direction_4h"] = direction(df4h)


        result["ema_1h"] = ema_display(df1h)

        result["ema_4h"] = ema_display(df4h)


        # ★ 10선
        result["ema10_cross_1h"] = (
            ema10_cross_count(df1h)
        )


        # ★ 비행기
        warning = get_air_warning(
            df1h,
            df4h
        )

        result["air_warning"] = warning


        if warning:

            result["air_count"] = (
                update_air_counter(
                    symbol,
                    warning
                )
            )


        return result


    except Exception as e:

        log.warning(
            f"analyze error {symbol}: {e}"
        )

        return result


# =========================================================
# HTML - EMA
# =========================================================

def ema_html(value):

    if value is None:
        return "-"

    return value


# =========================================================
# HTML - 경고
# =========================================================

def warning_html(data):

    if not data:
        return "-"


    warning = data.get(
        "air_warning"
    )

    count = data.get(
        "air_count",
        0
    )


    if warning == "LONG":

        return (
            '<div class="warning-box">'
            '<span class="long-text">LONG</span>'
            f'<span class="plane">🛩({count})</span>'
            '</div>'
        )


    return "-"


# =========================================================
# 행 HTML
# =========================================================

def rows_html(data):

    rows = []


    for rank, item in enumerate(
        data,
        1
    ):

        symbol = item.get(
            "symbol",
            "-"
        )

        coin = item.get(
            "coin",
            symbol
        )

        volume = item.get(
            "volume",
            "-"
        )

        ema_value = item.get(
            "ema_1h",
            "-"
        )

        cross_data = item.get(
            "ema10_cross_1h",
            {}
        )

        warning_data = item


        rows.append(
            f"""
            <tr>

                <td>
                    {rank}
                </td>

                <td class="coin">
                    {coin}
                </td>

                <td class="volume">
                    {volume}
                </td>

                <td class="ema">
                    {ema_html(ema_value)}
                </td>

                <td class="ema10">
                    {ema10_cross_html(
                        cross_data
                    )}
                </td>

                <td class="warning">
                    {warning_html(
                        warning_data
                    )}
                </td>

            </tr>
            """
        )


    return "".join(rows)


# =========================================================
# 섹션
# =========================================================

def section(title, data):

    return f"""
    <div class="section">

        <div class="section-title">
            {title}
        </div>

        <table>

            <thead>

                <tr>

                    <th>순위</th>
                    <th>코인</th>
                    <th>거래대금</th>
                    <th>EMA</th>
                    <th>10선</th>
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
# 대시보드
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def dashboard():

    upbit_html = section(
        "🏆 UPBIT",
        latest_upbit_data
    )

    okx_html = section(
        "🏆 OKX",
        latest_okx_data
    )


    return f"""
    <!DOCTYPE html>

    <html>

    <head>

        <meta
            name="viewport"
            content="width=device-width,
                     initial-scale=1.0,
                     maximum-scale=1.0"
        >

        <meta
            http-equiv="refresh"
            content="60"
        >

        <style>

        * {{
            box-sizing: border-box;
        }}


        body {{

            margin: 0;

            padding: 4px;

            background: #111;

            color: #eee;

            font-family:
                Arial,
                sans-serif;

            font-size: 9px;

        }}


        .section {{

            width: 100%;

            margin-bottom: 6px;

            background: #181818;

            border-radius: 5px;

            overflow: hidden;

        }}


        .section-title {{

            height: 22px;

            display: flex;

            align-items: center;

            padding-left: 6px;

            font-size: 10px;

            font-weight: bold;

            background: #202020;

        }}


        table {{

            width: 100%;

            table-layout: fixed;

            border-collapse:
                collapse;

        }}


        th {{

            height: 20px;

            background: #252525;

            color: #aaa;

            font-size: 8px;

            font-weight: bold;

            white-space: nowrap;

        }}


        td {{

            height: 28px;

            padding: 1px 2px;

            text-align: center;

            border-bottom:
                1px solid #292929;

            font-size: 8px;

            white-space: nowrap;

            overflow: hidden;

        }}


        /* =================================================
           ★ 컬럼 폭
           ================================================= */

        th:nth-child(1),
        td:nth-child(1) {{
            width: 6%;
        }}


        th:nth-child(2),
        td:nth-child(2) {{
            width: 22%;
        }}


        th:nth-child(3),
        td:nth-child(3) {{
            width: 16%;
        }}


        th:nth-child(4),
        td:nth-child(4) {{
            width: 17%;
        }}


        /* ★ 10선 확대 */

        th:nth-child(5),
        td:nth-child(5) {{
            width: 17%;
        }}


        th:nth-child(6),
        td:nth-child(6) {{
            width: 22%;
        }}


        .coin {{

            text-align: left;

            padding-left: 4px;

            font-weight: bold;

        }}


        .volume {{

            font-size: 7px;

        }}


        .ema {{

            font-size: 7px;

        }}


        /* =================================================
           ★ 10선 2줄 영역
           ================================================= */

        .ema10-box {{

            width: 100%;

            height: 28px;

            display: flex;

            flex-direction: column;

            justify-content:
                center;

            align-items: center;

            line-height: 12px;

            overflow: hidden;

            white-space: nowrap;

        }}


        .ema10-row {{

            width: 100%;

            height: 12px;

            display: flex;

            justify-content: center;

            align-items: center;

            overflow: hidden;

            white-space: nowrap;

        }}


        .ema10-active {{

            font-size: 7px;

            font-weight: bold;

            white-space: nowrap;

        }}


        .ema10-active.up {{

            color: #35e66d;

        }}


        .ema10-active.down {{

            color: #ff4d4d;

        }}


        /* ★ 이전 최종 카운트 */

        .ema10-final {{

            color: #777;

            font-size: 7px;

            font-weight: bold;

            white-space: nowrap;

        }}


        .ema10-empty {{

            color: #555;

            font-size: 7px;

        }}


        /* =================================================
           경고
           ================================================= */

        .warning-box {{

            width: 100%;

            display: flex;

            justify-content:
                center;

            align-items: center;

            gap: 3px;

            white-space: nowrap;

            overflow: hidden;

        }}


        .long-text {{

            color: #35e66d;

            font-size: 7px;

            font-weight: bold;

        }}


        .plane {{

            font-size: 8px;

            white-space: nowrap;

        }}


        /* =================================================
           모바일
           ================================================= */

        @media (
            max-width: 480px
        ) {{

            body {{

                padding: 2px;

                font-size: 8px;

            }}


            .section-title {{

                height: 20px;

                font-size: 9px;

            }}


            th {{

                height: 18px;

                font-size: 7px;

            }}


            td {{

                height: 27px;

                font-size: 7px;

            }}


            .ema10-box {{

                height: 27px;

                line-height: 11px;

            }}


            .ema10-row {{

                height: 11px;

            }}


            .ema10-active,
            .ema10-final {{

                font-size: 6px;

            }}


            .long-text {{

                font-size: 6px;

            }}


            .plane {{

                font-size: 7px;

            }}

        }}

        </style>

    </head>


    <body>

        {upbit_html}

        {okx_html}

    </body>

    </html>
    """


# =========================================================
# 여기부터 기존 데이터 수집 함수
# =========================================================

# ---------------------------------------------------------
# 아래 부분은 기존에 사용하시던
#
# get_upbit_markets()
# get_usdt_krw()
# get_okx_ohlcv()
# get_upbit_1h()
# get_upbit_4h()
# history_okx()
# history_upbit()
# history_upbit_4h()
# update_upbit()
# get_okx_symbols()
# get_okx_volume()
# update_okx()
# update_dashboard()
#
# 를 그대로 사용하면 됩니다.
#
# 단, 각 coin item에 반드시 아래 필드를 넣어주세요.
#
# "ema10_cross_1h":
#     ema10_cross_count(df1h)
#
# 그리고 기존의
#
# "close_ema10_1h"
#
# 는 삭제합니다.
# ---------------------------------------------------------


# =========================================================
# update_upbit() 안
# =========================================================

"""
기존 분석 부분을 다음처럼 변경:

analysis = analyze(
    symbol,
    df1h,
    df4h
)

item = {
    "symbol": symbol,
    "coin": coin_name,
    "volume": volume_display,

    "ema_1h":
        analysis["ema_1h"],

    "ema10_cross_1h":
        analysis["ema10_cross_1h"],

    "air_warning":
        analysis["air_warning"],

    "air_count":
        analysis["air_count"],
}
"""


# =========================================================
# update_okx() 안
# =========================================================

"""
OKX도 동일하게:

analysis = analyze(
    symbol,
    df1h,
    df4h
)

item = {
    "symbol": symbol,
    "coin": coin_name,
    "volume": volume_display,

    "ema_1h":
        analysis["ema_1h"],

    "ema10_cross_1h":
        analysis["ema10_cross_1h"],

    "air_warning":
        analysis["air_warning"],

    "air_count":
        analysis["air_count"],
}
"""


# =========================================================
# 스케줄러
# =========================================================

def scheduler():

    while True:

        try:

            schedule.run_pending()

        except Exception as e:

            log.error(
                f"scheduler error: {e}"
            )

        time.sleep(1)


# =========================================================
# 메인
# =========================================================

if __name__ == "__main__":

    # 기존 데이터 업데이트 함수가 있다면
    # 여기에서 그대로 등록

    # schedule.every(
    #     UPDATE_MINUTES
    # ).minutes.do(
    #     update_dashboard
    # )

    threading.Thread(
        target=scheduler,
        daemon=True
    ).start()

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )
