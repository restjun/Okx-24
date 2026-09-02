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

VOLUME_HOURS = 24
TOP_N = 100
UPDATE_MINUTES = 1

INITIAL_CANDLE_COUNT = 300
HISTORY_CHUNK = 200
MAX_HISTORY_CHUNKS = 10

USE_UPBIT = "Y"
USE_OKX = "N"

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10

KST = ZoneInfo("Asia/Seoul")


# =========================================================
# ★ 1시간봉 기준선
#
# 1시간 × 24 = 1일
# 1시간 × 240 = 10일
# =========================================================

EMA_1DAY = 24
EMA_10DAY = 240


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
    return datetime.now(KST).strftime(
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
                REQUEST_INTERVAL - gap
            )

        last_request_time = (
            time.monotonic()
        )


def retry(func, *args, **kwargs):

    name = getattr(
        func,
        "__name__",
        str(func)
    )

    url = (
        args[0]
        if args
        and isinstance(args[0], str)
        else kwargs.get("url", "")
    )

    for n in range(MAX_RETRIES):

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
                f"{url} {wait}초"
            )

            time.sleep(wait)

        except Exception as e:

            log.error(
                f"[API 오류] "
                f"{name} {url}: {e}"
            )

            if n < MAX_RETRIES - 1:

                time.sleep(
                    min(
                        2 * (n + 1),
                        20
                    )
                )

    log.error(
        f"[API 최종 실패] {name} {url}"
    )

    return None


# =========================================================
# Upbit 마켓
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
                    x["acc_trade_price_24h"]
                )

            except:

                continue

            if volume > 0:

                result.append({
                    "market": market,
                    "volume_24h": volume
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


# =========================================================
# USDT/KRW
# =========================================================

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
            r.json()[0]["trade_price"]
        )

        return (
            price
            if price > 0
            else None
        )

    except:

        return None


# =========================================================
# Upbit 1시간봉
# =========================================================

def get_upbit_1h(
    market,
    count=200,
    to=None
):

    params = {
        "market": market,
        "count": min(
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
        "https://api.upbit.com/v1/candles/minutes/60",
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
            df["opening_price"],
            errors="coerce"
        )

        df["h"] = pd.to_numeric(
            df["high_price"],
            errors="coerce"
        )

        df["l"] = pd.to_numeric(
            df["low_price"],
            errors="coerce"
        )

        df["c"] = pd.to_numeric(
            df["trade_price"],
            errors="coerce"
        )

        df["volume_krw"] = pd.to_numeric(
            df["candle_acc_trade_price"],
            errors="coerce"
        )

        df["datetime"] = pd.to_datetime(
            df["candle_date_time_kst"],
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

        # =============================================
        # 현재 진행 중인 1H 봉 제거
        # =============================================

        now = datetime.now(KST)

        current = now.replace(
            minute=0,
            second=0,
            microsecond=0
        ).replace(
            tzinfo=None
        )

        df = df[
            df["datetime"] < current
        ]

        if df.empty:
            return None

        return (
            df
            .sort_values("datetime")
            .drop_duplicates(
                "datetime"
            )
            .reset_index(drop=True)
        )

    except Exception as e:

        log.error(
            f"업비트 1H 오류 "
            f"{market}: {e}"
        )

        return None


# =========================================================
# 1H History
# =========================================================

def history_upbit_1h(
    market,
    required=250
):

    all_df = None
    to = None

    for _ in range(
        MAX_HISTORY_CHUNKS
    ):

        df = get_upbit_1h(
            market,
            HISTORY_CHUNK,
            to
        )

        if (
            df is None
            or df.empty
        ):
            break

        if all_df is None:

            all_df = df.copy()

        else:

            all_df = pd.concat(
                [
                    df,
                    all_df
                ],
                ignore_index=True
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
            all_df
            .datetime
            .iloc[0]
            .strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        )

    return all_df


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
        or "c" not in df
    ):
        return None

    return (
        pd.to_numeric(
            df["c"],
            errors="coerce"
        )
        .ewm(
            span=period,
            adjust=False,
            min_periods=1
        )
        .mean()
    )


# =========================================================
# 일반 EMA 표시
#
# 기존 화면은 4H / 1H 구조 유지
# =========================================================

def ema_alignment_count(df):

    if (
        df is None
        or df.empty
    ):
        return {
            "direction": "none",
            "count": 0
        }

    try:

        e10 = ema(
            df,
            10
        )

        e30 = ema(
            df,
            30
        )

        e60 = ema(
            df,
            60
        )

        e120 = ema(
            df,
            120
        )

        if any(
            x is None
            for x in [
                e10,
                e30,
                e60,
                e120
            ]
        ):

            return {
                "direction": "none",
                "count": 0
            }

        current_direction = None
        count = 0

        for i in range(
            len(df) - 1,
            -1,
            -1
        ):

            a = float(
                e10.iloc[i]
            )

            b = float(
                e30.iloc[i]
            )

            c = float(
                e60.iloc[i]
            )

            d = float(
                e120.iloc[i]
            )

            if a > b > c > d:

                candle_direction = "long"

            elif a < b < c < d:

                candle_direction = "short"

            else:

                candle_direction = "none"

            if i == len(df) - 1:

                current_direction = (
                    candle_direction
                )

                if (
                    current_direction
                    == "none"
                ):

                    return {
                        "direction": "none",
                        "count": 0
                    }

            if (
                candle_direction
                == current_direction
            ):

                count += 1

            else:

                break

        return {
            "direction":
                current_direction,
            "count":
                count
        }

    except Exception as e:

        log.error(
            f"EMA 정배열 오류: {e}"
        )

        return {
            "direction": "none",
            "count": 0
        }


def ema_display(df):

    result = (
        ema_alignment_count(df)
    )

    d = result["direction"]
    count = result["count"]

    icon = {
        "long": "🟢",
        "short": "🔴"
    }.get(
        d,
        "⚪"
    )

    return {
        "display":
            f"{icon}({count})",
        "direction":
            d,
        "count":
            count
    }


# =========================================================
# ★ 핵심 카운트
#
# 모든 기준은 1H
#
# 1일선 = EMA24
# 10일선 = EMA240
#
# 🔻 10선:
# 1H 종가 < EMA240
#
# ✈️ 비행기:
# 1H 종가 > EMA24 > EMA240
# =========================================================

def calculate_10line_and_airplane(
    df
):

    empty = {
        "down_count": 0,
        "up_count": 0,

        "ten_line": "-",
        "warning": "-",

        "position": "none",

        "ema24": None,
        "ema240": None,

        "close": None,

        "candle_time": None
    }

    if (
        df is None
        or df.empty
        or len(df) < 2
    ):
        return empty

    try:

        # =============================================
        # EMA24 = 1시간봉 기준 1일선
        # EMA240 = 1시간봉 기준 10일선
        # =============================================

        ema24 = ema(
            df,
            EMA_1DAY
        )

        ema240 = ema(
            df,
            EMA_10DAY
        )

        if (
            ema24 is None
            or ema240 is None
        ):
            return empty

        work = df[
            [
                "datetime",
                "c"
            ]
        ].copy()

        work["ema24"] = (
            ema24.values
        )

        work["ema240"] = (
            ema240.values
        )

        # =============================================
        # 각 1H 확정봉 상태 계산
        # =============================================

        work["state"] = "neutral"

        # ---------------------------------------------
        # 🔻 하락
        #
        # 1H 종가 < 1H EMA240
        # ---------------------------------------------

        work.loc[
            work["c"]
            <
            work["ema240"],
            "state"
        ] = "down"

        # ---------------------------------------------
        # ✈️ 상승
        #
        # 1H 종가 > EMA24
        # 그리고
        # EMA24 > EMA240
        # ---------------------------------------------

        work.loc[
            (
                work["c"]
                >
                work["ema24"]
            )
            &
            (
                work["ema24"]
                >
                work["ema240"]
            ),
            "state"
        ] = "up"

        current = work.iloc[-1]

        current_state = (
            current["state"]
        )

        close = float(
            current["c"]
        )

        current_ema24 = float(
            current["ema24"]
        )

        current_ema240 = float(
            current["ema240"]
        )

        candle_time = str(
            current["datetime"]
        )

        # =============================================
        # 🔻 연속 하락 카운트
        # =============================================

        down_count = 0

        if current_state == "down":

            for i in range(
                len(work) - 1,
                -1,
                -1
            ):

                if (
                    work.iloc[i]["state"]
                    == "down"
                ):

                    down_count += 1

                else:

                    break

        # =============================================
        # ✈️ 연속 상승 카운트
        # =============================================

        up_count = 0

        if current_state == "up":

            for i in range(
                len(work) - 1,
                -1,
                -1
            ):

                if (
                    work.iloc[i]["state"]
                    == "up"
                ):

                    up_count += 1

                else:

                    break

        # =============================================
        # 화면 표시
        # =============================================

        if current_state == "down":

            ten_line = (
                f"🔻({down_count})"
            )

            warning = "-"

        elif current_state == "up":

            ten_line = "-"

            warning = (
                f"✈️({up_count})"
            )

        else:

            ten_line = "-"
            warning = "-"

        # =============================================
        # 결과
        # =============================================

        return {

            "down_count":
                down_count,

            "up_count":
                up_count,

            "ten_line":
                ten_line,

            "warning":
                warning,

            "position":
                current_state,

            "ema24":
                current_ema24,

            "ema240":
                current_ema240,

            "close":
                close,

            "candle_time":
                candle_time
        }

    except Exception as e:

        log.error(
            f"10선/비행기 계산 오류: {e}"
        )

        return empty


# =========================================================
# 거래대금
# =========================================================

def get_upbit_volume(
    market,
    hours=24
):

    df = get_upbit_1h(
        market,
        min(
            hours + 5,
            200
        )
    )

    if (
        df is None
        or df.empty
    ):
        return 0

    try:

        return float(
            df[
                "volume_krw"
            ]
            .tail(hours)
            .sum()
        )

    except:

        return 0


# =========================================================
# 거래대금 포맷
# =========================================================

def format_volume(
    value
):

    if value is None:
        return "-"

    try:

        value = float(value)

        if value >= 1_000_000_000_000:

            return (
                f"{value / 1_000_000_000_000:.1f}조"
            )

        if value >= 100_000_000:

            return (
                f"{value / 100_000_000:.0f}억"
            )

        if value >= 10_000:

            return (
                f"{value / 10_000:.0f}만"
            )

        return f"{value:,.0f}"

    except:

        return "-"


# =========================================================
# 분석
# =========================================================

def analyze(
    market,
    volume
):

    try:

        # =============================================
        # ★ 1H 데이터 하나만 사용
        # =============================================

        df1h = history_upbit_1h(
            market,
            250
        )

        if (
            df1h is None
            or df1h.empty
        ):
            return None

        # =============================================
        # EMA 표시
        #
        # 기존 화면 유지
        # =============================================

        ema1h = ema_display(
            df1h
        )

        # =============================================
        # ★ 1H 기준
        #
        # EMA24 = 1일선
        # EMA240 = 10일선
        # =============================================

        counter = (
            calculate_10line_and_airplane(
                df1h
            )
        )

        # =============================================
        # 현재가
        # =============================================

        last_close = float(
            df1h["c"].iloc[-1]
        )

        return {

            "market":
                market,

            "volume":
                volume,

            "close":
                last_close,

            "ema1h":
                ema1h,

            "counter":
                counter
        }

    except Exception as e:

        log.error(
            f"분석 오류 "
            f"{market}: {e}"
        )

        return None


# =========================================================
# EMA HTML
#
# ★ 일봉 표시 없음
# ★ 1H만 표시
# =========================================================

def ema_cell(
    data
):

    if not data:
        return "-"

    e1h = data.get(
        "ema1h"
    )

    if not e1h:
        return "-"

    return (
        '<div class="ema-box">'
        f'1H {e1h.get("display", "-")}'
        '</div>'
    )


# =========================================================
# 10선 HTML
#
# ★ 🔻만 표시
# =========================================================

def ema10_html(
    data
):

    if not data:
        return "-"

    value = data.get(
        "ten_line",
        "-"
    )

    if value == "-":
        return "-"

    return (
        '<span class="down-count">'
        f'{value}'
        '</span>'
    )


# =========================================================
# 경고 HTML
#
# ★ ✈️만 표시
# =========================================================

def warning_html(
    data
):

    if not data:
        return "-"

    value = data.get(
        "warning",
        "-"
    )

    if value == "-":
        return "-"

    return (
        '<span class="plane">'
        f'{value}'
        '</span>'
    )


# =========================================================
# 행
# =========================================================

def make_row(
    data,
    rank
):

    if not data:
        return ""

    market = data[
        "market"
    ]

    coin = market.replace(
        "KRW-",
        ""
    )

    volume = format_volume(
        data.get(
            "volume",
            0
        )
    )

    counter = data.get(
        "counter",
        {}
    )

    ten_line = (
        ema10_html(
            counter
        )
    )

    warning = (
        warning_html(
            counter
        )
    )

    return f"""
    <tr>

        <td class="rank">
            {rank}
        </td>

        <td class="coin">
            <b>{coin}</b>
        </td>

        <td class="volume">
            {volume}
        </td>

        <td class="ema">
            {ema_cell(data)}
        </td>

        <td class="ten">
            {ten_line}
        </td>

        <td class="warning">
            {warning}
        </td>

    </tr>
    """


# =========================================================
# 전체 행
# =========================================================

def rows_html():

    rows = []

    for rank, data in enumerate(
        latest_upbit_data,
        1
    ):

        row = make_row(
            data,
            rank
        )

        if row:
            rows.append(row)

    return "".join(rows)


# =========================================================
# HTML
# =========================================================

HTML_TEMPLATE = """
<!DOCTYPE html>

<html lang="ko">

<head>

<meta charset="UTF-8">

<meta
    name="viewport"
    content="width=device-width, initial-scale=1.0"
>

<title>Crypto Dashboard</title>

<style>

* {
    box-sizing: border-box;
}

body {

    margin: 0;

    background: #111;

    color: #eee;

    font-family:
        Arial,
        sans-serif;

    font-size: 12px;
}

.header {

    height: 36px;

    padding:
        6px 10px;

    background: #181818;

    border-bottom:
        1px solid #333;

    display: flex;

    justify-content:
        space-between;

    align-items:
        center;
}

.title {

    font-size: 14px;

    font-weight: bold;
}

.update {

    color: #888;

    font-size: 9px;
}

.table-wrap {

    width: 100%;

    overflow-x: auto;
}

table {

    width: 100%;

    border-collapse:
        collapse;

    table-layout:
        fixed;
}

thead {

    background: #202020;

    position: sticky;

    top: 0;

    z-index: 5;
}

th {

    height: 24px;

    padding: 2px;

    border-bottom:
        1px solid #333;

    color: #888;

    font-size: 9px;

    font-weight: normal;
}

td {

    height: 29px;

    padding:
        2px 3px;

    border-bottom:
        1px solid #252525;

    text-align: center;

    white-space: nowrap;

    overflow: hidden;
}


/* =========================================
   열 위치
   ========================================= */

th:nth-child(1),
td:nth-child(1) {

    width: 28px;
}

th:nth-child(2),
td:nth-child(2) {

    width: 65px;
}

th:nth-child(3),
td:nth-child(3) {

    width: 65px;
}

th:nth-child(4),
td:nth-child(4) {

    width: 85px;
}

th:nth-child(5),
td:nth-child(5) {

    width: 65px;
}

th:nth-child(6),
td:nth-child(6) {

    width: 65px;
}


/* =========================================
   순위
   ========================================= */

.rank {

    color: #777;

    font-size: 9px;
}


/* =========================================
   코인
   ========================================= */

.coin {

    text-align: left;

    font-size: 11px;
}


/* =========================================
   거래대금
   ========================================= */

.volume {

    color: #ccc;

    font-size: 10px;
}


/* =========================================
   EMA
   ========================================= */

.ema-box {

    font-size: 9px;

    line-height: 11px;

    text-align: center;
}


/* =========================================
   ★ 10선
   ========================================= */

.ten {

    font-size: 12px;

    font-weight: bold;
}

.down-count {

    color: #ff4d4d;

    font-size: 12px;

    font-weight: bold;
}


/* =========================================
   ★ 비행기
   ========================================= */

.warning {

    font-size: 12px;

    font-weight: bold;
}

.plane {

    font-size: 12px;

    font-weight: bold;
}

</style>

</head>

<body>

<div class="header">

    <div class="title">
        🏆 실거래대금 TOP
    </div>

    <div class="update">
        {{UPDATE}}
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

<th>10선</th>

<th>경고</th>

</tr>

</thead>


<tbody>

{{ROWS}}

</tbody>

</table>

</div>


<script>

setTimeout(
    function()
    {
        location.reload();
    },
    60000
);

</script>

</body>

</html>
"""


# =========================================================
# FastAPI
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def home():

    html = (
        HTML_TEMPLATE
        .replace(
            "{{ROWS}}",
            rows_html()
        )
        .replace(
            "{{UPDATE}}",
            latest_upbit_update_time
        )
    )

    return HTMLResponse(
        content=html
    )


# =========================================================
# Upbit 업데이트
# =========================================================

def update_upbit():

    global latest_upbit_data
    global latest_upbit_update_time

    markets = get_upbit_markets()

    if not markets:
        return

    # 거래대금 순
    markets = sorted(
        markets,
        key=lambda x:
            x["volume_24h"],
        reverse=True
    )

    markets = markets[
        :TOP_N
    ]

    result = []

    for item in markets:

        market = item[
            "market"
        ]

        try:

            data = analyze(
                market,
                item["volume_24h"]
            )

            if data:

                result.append(
                    data
                )

        except Exception as e:

            log.error(
                f"{market} 처리 오류: "
                f"{e}"
            )

    with update_lock:

        latest_upbit_data = result

        latest_upbit_update_time = (
            kst()
        )


# =========================================================
# 전체 업데이트
# =========================================================

def update_all():

    try:

        if USE_UPBIT == "Y":

            update_upbit()

        log.info(
            f"업데이트 완료 "
            f"{kst()} / "
            f"UPBIT={len(latest_upbit_data)}"
        )

    except Exception as e:

        log.error(
            f"전체 업데이트 오류: "
            f"{e}"
        )


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

            log.error(
                f"스케줄러 오류: {e}"
            )

        time.sleep(1)


# =========================================================
# 실행
# =========================================================

if __name__ == "__main__":

    log.info(
        "초기 데이터 업데이트 시작"
    )

    update_all()

    t = threading.Thread(
        target=scheduler,
        daemon=True
    )

    t.start()

    log.info(
        "FastAPI 서버 시작"
    )

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )
