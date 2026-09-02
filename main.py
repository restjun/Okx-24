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
    format="%(asctime)s | %(levelname)s | %(message)s"
)

KST = ZoneInfo("Asia/Seoul")

UPBIT_URL = "https://api.upbit.com"

VOLUME_HOURS = 24
TOP_N = 50
UPDATE_MINUTES = 1

HISTORY_CHUNK = 200
MAX_HISTORY_CHUNKS = 10

USE_UPBIT = "Y"
USE_OKX = "N"

REQUEST_INTERVAL = 0.08
RATE_LIMIT_WAIT = 3
MAX_RETRIES = 10


# =========================================================
# 전역 상태
# =========================================================

data_lock = threading.Lock()

latest_data = []

# 비행기 상태
air_state = {}

# 마지막 업데이트 시간
last_update_time = None

# API 세션
session = requests.Session()
session.headers.update({
    "Accept": "application/json"
})


# =========================================================
# 공통 요청
# =========================================================

def safe_get(url, params=None, timeout=10):

    for attempt in range(MAX_RETRIES):

        try:
            r = session.get(
                url,
                params=params,
                timeout=timeout
            )

            if r.status_code == 200:
                return r.json()

            if r.status_code == 429:
                logging.warning("API RATE LIMIT - %s초 대기", RATE_LIMIT_WAIT)
                time.sleep(RATE_LIMIT_WAIT)
                continue

            logging.warning(
                "API 오류 %s : %s",
                r.status_code,
                r.text[:200]
            )

        except Exception as e:

            logging.warning(
                "API 요청 실패 %s/%s : %s",
                attempt + 1,
                MAX_RETRIES,
                e
            )

        time.sleep(REQUEST_INTERVAL)

    return None


# =========================================================
# EMA
# =========================================================

def ema(series, period):

    return series.ewm(
        span=period,
        adjust=False,
        min_periods=1
    ).mean()


# =========================================================
# Upbit 전체 KRW 종목 + 현재가
# =========================================================

def get_upbit_markets():

    data = safe_get(
        f"{UPBIT_URL}/v1/ticker/all",
        params={
            "quote_currencies": "KRW"
        }
    )

    if not data:
        return []

    result = []

    for x in data:

        market = x.get("market", "")

        if not market.startswith("KRW-"):
            continue

        try:

            result.append({
                "market": market,
                "coin": market.replace("KRW-", ""),
                "current_price": float(x.get("trade_price", 0)),
                "change_rate": float(x.get("signed_change_rate", 0)) * 100,
                "volume_24h": float(x.get("acc_trade_price_24h", 0))
            })

        except Exception:
            continue

    return result


# =========================================================
# Upbit 1시간 / 4시간 캔들
# =========================================================

def get_upbit_candle(
    market,
    unit=60,
    count=200
):

    data = safe_get(
        f"{UPBIT_URL}/v1/candles/minutes/{unit}",
        params={
            "market": market,
            "count": count
        }
    )

    if not data:
        return pd.DataFrame()

    rows = []

    now = datetime.now(KST).replace(tzinfo=None)

    for x in data:

        try:

            dt = datetime.fromisoformat(
                x["candle_date_time_kst"]
            )

            # 현재 진행 중인 캔들은 제외
            if dt >= now:
                continue

            rows.append({
                "datetime": dt,
                "open": float(x["opening_price"]),
                "high": float(x["high_price"]),
                "low": float(x["low_price"]),
                "close": float(x["trade_price"]),
                "volume": float(x["candle_acc_trade_volume"]),
                "value": float(x["candle_acc_trade_price"])
            })

        except Exception:
            continue

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    df = df.sort_values("datetime")
    df = df.drop_duplicates("datetime")
    df = df.reset_index(drop=True)

    return df


# =========================================================
# 긴 1시간 역사 데이터
# =========================================================

def history_upbit(
    market,
    unit=60,
    total=1000
):

    frames = []

    to_time = None
    remain = total

    while remain > 0:

        count = min(
            HISTORY_CHUNK,
            remain
        )

        params = {
            "market": market,
            "count": count
        }

        if to_time is not None:
            params["to"] = to_time

        data = safe_get(
            f"{UPBIT_URL}/v1/candles/minutes/{unit}",
            params=params
        )

        if not data:
            break

        rows = []

        for x in data:

            try:

                dt = datetime.fromisoformat(
                    x["candle_date_time_kst"]
                )

                rows.append({
                    "datetime": dt,
                    "open": float(x["opening_price"]),
                    "high": float(x["high_price"]),
                    "low": float(x["low_price"]),
                    "close": float(x["trade_price"]),
                    "volume": float(x["candle_acc_trade_volume"]),
                    "value": float(x["candle_acc_trade_price"])
                })

            except Exception:
                continue

        if not rows:
            break

        df = pd.DataFrame(rows)

        frames.append(df)

        oldest = df["datetime"].min()

        to_time = oldest.strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        remain -= len(df)

        if len(df) < count:
            break

        time.sleep(REQUEST_INTERVAL)

        if len(frames) >= MAX_HISTORY_CHUNKS:
            break

    if not frames:
        return pd.DataFrame()

    result = pd.concat(
        frames,
        ignore_index=True
    )

    result = result.drop_duplicates(
        "datetime"
    )

    result = result.sort_values(
        "datetime"
    )

    result = result.tail(total)

    result = result.reset_index(
        drop=True
    )

    return result


# =========================================================
# EMA 10 / 30 / 60 / 120
# =========================================================

def add_ema(df):

    if df.empty:
        return df

    df = df.copy()

    df["ema10"] = ema(
        df["close"],
        10
    )

    df["ema30"] = ema(
        df["close"],
        30
    )

    df["ema60"] = ema(
        df["close"],
        60
    )

    df["ema120"] = ema(
        df["close"],
        120
    )

    return df


# =========================================================
# EMA 정배열
# =========================================================

def direction(df):

    if df.empty or len(df) < 2:
        return "-"

    row = df.iloc[-1]

    if (
        row["ema10"] >
        row["ema30"] >
        row["ema60"] >
        row["ema120"]
    ):
        return "long"

    if (
        row["ema10"] <
        row["ema30"] <
        row["ema60"] <
        row["ema120"]
    ):
        return "short"

    return "neutral"


# =========================================================
# EMA 정배열 연속 카운트
# =========================================================

def ema_alignment_count(df):

    if df.empty:
        return 0

    state = direction(df)

    if state not in ("long", "short"):
        return 0

    count = 0

    for i in range(len(df) - 1, -1, -1):

        row = df.iloc[i]

        if state == "long":

            ok = (
                row["ema10"] >
                row["ema30"] >
                row["ema60"] >
                row["ema120"]
            )

        else:

            ok = (
                row["ema10"] <
                row["ema30"] <
                row["ema60"] <
                row["ema120"]
            )

        if not ok:
            break

        count += 1

    return count


# =========================================================
# 1시간 종가 EMA10 위/아래 카운트
# =========================================================

def ema10_cross_count(df):

    if df.empty:
        return {
            "current_state": "-",
            "current_count": 0,
            "previous_count": 0
        }

    work = df.copy()

    work["ema10"] = ema(
        work["close"],
        10
    )

    states = []

    for _, row in work.iterrows():

        if row["close"] > row["ema10"]:
            states.append("long")

        elif row["close"] < row["ema10"]:
            states.append("short")

        else:
            states.append("equal")

    if not states:
        return {
            "current_state": "-",
            "current_count": 0,
            "previous_count": 0
        }

    current = states[-1]

    if current == "equal":

        if len(states) >= 2:
            current = states[-2]
        else:
            current = "equal"

    current_count = 0

    if current in ("long", "short"):

        for state in reversed(states):

            if state == current:
                current_count += 1
            else:
                break

    previous_count = 0

    if current_count > 0:

        previous_state = (
            "short"
            if current == "long"
            else "long"
        )

        end_index = len(states) - current_count

        for i in range(end_index - 1, -1, -1):

            if states[i] == previous_state:
                previous_count += 1
            else:
                break

    return {
        "current_state": current,
        "current_count": current_count,
        "previous_count": previous_count
    }


# =========================================================
# ★ 비행기 로직
#
# 1. 현재가격 > 확정 EMA10
#       → ✈️ 사전 경고
#
# 2. 확정 종가가 EMA10 골든크로스
#       → ✈️(1)
#
# 3. 이후 종가 EMA10 위
#       → ✈️(2), ✈️(3) ...
#
# 4. 종가 EMA10 아래 마감
#       → ⛔️ 종료
# =========================================================

def get_plane_state(
    market,
    df1h,
    current_price
):

    if df1h.empty or len(df1h) < 12:

        return {
            "air_warning": False,
            "air_active": False,
            "air_count": 0,
            "air_status": "-",
            "air_direction": "-",
            "air_candle": None
        }

    df = df1h.copy()

    df["ema10"] = ema(
        df["close"],
        10
    )

    # -----------------------------------------------------
    # 확정 1시간봉
    # -----------------------------------------------------

    last = df.iloc[-1]

    last_time = last["datetime"]

    last_close = float(last["close"])
    last_ema10 = float(last["ema10"])

    # -----------------------------------------------------
    # 이전 확정봉
    # -----------------------------------------------------

    prev = df.iloc[-2]

    prev_close = float(prev["close"])
    prev_ema10 = float(prev["ema10"])

    # -----------------------------------------------------
    # 실제 골든크로스
    #
    # 이전:
    # 종가 <= EMA10
    #
    # 현재:
    # 종가 > EMA10
    # -----------------------------------------------------

    golden_cross = (
        prev_close <= prev_ema10
        and
        last_close > last_ema10
    )

    # -----------------------------------------------------
    # 데드크로스
    # -----------------------------------------------------

    dead_cross = (
        prev_close >= prev_ema10
        and
        last_close < last_ema10
    )

    # -----------------------------------------------------
    # 현재 저장 상태
    # -----------------------------------------------------

    state = air_state.get(
        market,
        {
            "active": False,
            "count": 0,
            "last_candle": None,
            "golden_candle": None,
            "ended_candle": None
        }
    )

    # -----------------------------------------------------
    # 새로운 확정 1시간봉이 생겼을 때만
    # 카운터를 변경
    # -----------------------------------------------------

    is_new_candle = (
        state["last_candle"] != last_time
    )

    if is_new_candle:

        # =================================================
        # 이미 비행기 카운팅 중
        # =================================================

        if state["active"]:

            # 데드크로스
            if last_close < last_ema10:

                state["active"] = False

                # 마지막 카운트 유지
                # 종료 표시를 위해 저장
                state["ended_candle"] = last_time

                logging.info(
                    "%s | ✈️ 종료 → ⛔️ | 최종 %s",
                    market,
                    state["count"]
                )

            else:

                # 종가 EMA10 위 유지
                state["count"] += 1

        # =================================================
        # 아직 비행기 카운팅 전
        # =================================================

        else:

            # 골든크로스 확정
            if golden_cross:

                state["active"] = True
                state["count"] = 1
                state["golden_candle"] = last_time
                state["ended_candle"] = None

                logging.info(
                    "%s | 골든크로스 확정 → ✈️(1)",
                    market
                )

        state["last_candle"] = last_time

    # -----------------------------------------------------
    # 현재 진행 중인 가격
    #
    # 아직 골든크로스 마감 전이라도
    # 현재가격 > 확정 EMA10이면 ✈️
    # -----------------------------------------------------

    live_warning = (
        current_price is not None
        and
        current_price > last_ema10
    )

    # -----------------------------------------------------
    # 활성 카운팅 중
    # -----------------------------------------------------

    if state["active"]:

        return {
            "air_warning": True,
            "air_active": True,
            "air_count": state["count"],
            "air_status": "COUNTING",
            "air_direction": "LONG",
            "air_candle": state["golden_candle"]
        }

    # -----------------------------------------------------
    # 종료 직후
    #
    # 해당 확정봉에서는 ⛔️ 표시
    # -----------------------------------------------------

    if (
        state["ended_candle"] is not None
        and
        state["ended_candle"] == last_time
    ):

        return {
            "air_warning": True,
            "air_active": False,
            "air_count": state["count"],
            "air_status": "ENDED",
            "air_direction": "DEAD",
            "air_candle": last_time
        }

    # -----------------------------------------------------
    # 골든크로스 전 현재가격이 EMA10 위
    # -----------------------------------------------------

    if live_warning:

        return {
            "air_warning": True,
            "air_active": False,
            "air_count": 0,
            "air_status": "PRE",
            "air_direction": "WAIT",
            "air_candle": None
        }

    # -----------------------------------------------------
    # 아무 상태 없음
    # -----------------------------------------------------

    return {
        "air_warning": False,
        "air_active": False,
        "air_count": 0,
        "air_status": "-",
        "air_direction": "-",
        "air_candle": None
    }


# =========================================================
# 비행기 HTML
# =========================================================

def warning_html(row):

    status = row.get(
        "air_status",
        "-"
    )

    count = row.get(
        "air_count",
        0
    )

    # -----------------------------------------------------
    # 종료
    # -----------------------------------------------------

    if status == "ENDED":

        return """
        <div class="warning ended">
            ⛔️
        </div>
        """

    # -----------------------------------------------------
    # 카운팅
    # -----------------------------------------------------

    if status == "COUNTING":

        return f"""
        <div class="warning counting">
            ✈️<span class="air-count">({count})</span>
        </div>
        """

    # -----------------------------------------------------
    # 현재가격 EMA10 위
    # -----------------------------------------------------

    if status == "PRE":

        return """
        <div class="warning pre">
            ✈️
        </div>
        """

    return """
    <div class="warning empty">
        -
    </div>
    """


# =========================================================
# 1시간 EMA10 표시
# =========================================================

def ema10_html(info):

    state = info["current_state"]
    count = info["current_count"]
    previous = info["previous_count"]

    if state == "long":

        return f"""
        <div class="ema10-wrap">
            <div class="ema10-up">
                🟢({count})
            </div>
            <div class="ema10-prev">
                ({previous})
            </div>
        </div>
        """

    if state == "short":

        return f"""
        <div class="ema10-wrap">
            <div class="ema10-down">
                🔻({count})
            </div>
            <div class="ema10-prev">
                ({previous})
            </div>
        </div>
        """

    return "-"


# =========================================================
# EMA 방향 HTML
# =========================================================

def ema_direction_html(
    df1h,
    df4h
):

    d1 = direction(df1h)
    d4 = direction(df4h)

    c1 = ema_alignment_count(df1h)
    c4 = ema_alignment_count(df4h)

    def one(d, c):

        if d == "long":
            return f"🟢({c})"

        if d == "short":
            return f"🔻({c})"

        return "-"

    return (
        f"<div>1H {one(d1, c1)}</div>"
        f"<div>4H {one(d4, c4)}</div>"
    )


# =========================================================
# 분석
# =========================================================

def analyze(
    market,
    current_price
):

    try:

        # -------------------------------------------------
        # 1시간
        # -------------------------------------------------

        df1h = history_upbit(
            market,
            unit=60,
            total=500
        )

        if df1h.empty:
            return None

        df1h = add_ema(df1h)

        # -------------------------------------------------
        # 4시간
        # -------------------------------------------------

        df4h = history_upbit(
            market,
            unit=240,
            total=300
        )

        if df4h.empty:
            return None

        df4h = add_ema(df4h)

        # -------------------------------------------------
        # 1시간 EMA10
        # -------------------------------------------------

        ema10_info = ema10_cross_count(
            df1h
        )

        # -------------------------------------------------
        # 비행기
        # -------------------------------------------------

        plane = get_plane_state(
            market,
            df1h,
            current_price
        )

        # -------------------------------------------------
        # 현재 EMA
        # -------------------------------------------------

        last1 = df1h.iloc[-1]
        last4 = df4h.iloc[-1]

        result = {

            "market": market,

            "coin": market.replace(
                "KRW-",
                ""
            ),

            "current_price": current_price,

            "ema1h_10": float(
                last1["ema10"]
            ),

            "ema4h_10": float(
                last4["ema10"]
            ),

            "ema_direction": ema_direction_html(
                df1h,
                df4h
            ),

            "ema10_state":
                ema10_info["current_state"],

            "ema10_count":
                ema10_info["current_count"],

            "ema10_previous":
                ema10_info["previous_count"],

            "air_warning":
                plane["air_warning"],

            "air_active":
                plane["air_active"],

            "air_count":
                plane["air_count"],

            "air_status":
                plane["air_status"],

            "air_direction":
                plane["air_direction"],

            "air_candle":
                plane["air_candle"],

            "qualified":
                plane["air_warning"]
        }

        return result

    except Exception as e:

        logging.warning(
            "%s 분석 실패: %s",
            market,
            e
        )

        return None


# =========================================================
# Upbit 업데이트
# =========================================================

def update_upbit():

    global latest_data
    global last_update_time

    logging.info(
        "===== Upbit 업데이트 시작 ====="
    )

    markets = get_upbit_markets()

    if not markets:

        logging.warning(
            "Upbit 시장 데이터를 가져오지 못했습니다."
        )

        return

    # 거래대금 순 정렬
    markets.sort(
        key=lambda x: x["volume_24h"],
        reverse=True
    )

    markets = markets[:TOP_N]

    results = []

    plane_count = 0

    for idx, item in enumerate(markets, start=1):

        market = item["market"]

        current_price = item[
            "current_price"
        ]

        result = analyze(
            market,
            current_price
        )

        if result is None:
            continue

        result["rank"] = idx

        result["change_rate"] = item[
            "change_rate"
        ]

        result["volume_24h"] = item[
            "volume_24h"
        ]

        if result["air_warning"]:
            plane_count += 1

        results.append(result)

        time.sleep(
            REQUEST_INTERVAL
        )

    with data_lock:

        latest_data = results
        last_update_time = datetime.now(
            KST
        )

    logging.info(
        "===== 업데이트 완료 | %s개 | 비행기 %s개 =====",
        len(results),
        plane_count
    )


# =========================================================
# 금액 표시
# =========================================================

def format_volume(value):

    try:

        value = float(value)

        if value >= 100_000_000_000:
            return f"{value / 100_000_000_000:.1f}천억"

        if value >= 100_000_000:
            return f"{value / 100_000_000:.1f}억"

        if value >= 10_000:
            return f"{value / 10_000:.1f}만"

        return f"{value:,.0f}"

    except Exception:
        return "-"


# =========================================================
# 가격 표시
# =========================================================

def format_price(value):

    if value is None:
        return "-"

    try:

        value = float(value)

        if value >= 1000:
            return f"{value:,.0f}"

        if value >= 1:
            return f"{value:,.2f}"

        return f"{value:.6f}"

    except Exception:
        return "-"


# =========================================================
# HTML
# =========================================================

HTML = """
<!DOCTYPE html>

<html lang="ko">

<head>

<meta charset="UTF-8">

<meta name="viewport"
      content="width=device-width, initial-scale=1.0">

<title>OKX / Upbit EMA Dashboard</title>

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
        "Noto Sans KR",
        sans-serif;

}

.header {

    padding: 14px 12px;

    background: #181818;

    border-bottom: 1px solid #333;

}

.title {

    font-size: 20px;

    font-weight: bold;

}

.info {

    margin-top: 8px;

    color: #aaa;

    font-size: 12px;

    line-height: 1.7;

}

.container {

    padding: 10px;

    overflow-x: auto;

}

table {

    width: 100%;

    min-width: 900px;

    border-collapse: collapse;

    table-layout: fixed;

}

th {

    background: #222;

    color: #ccc;

    font-size: 12px;

    padding: 9px 4px;

    border-bottom: 1px solid #444;

}

td {

    text-align: center;

    padding: 8px 4px;

    border-bottom: 1px solid #292929;

    font-size: 13px;

}

.rank {

    width: 42px;

}

.coin {

    width: 100px;

    font-weight: bold;

}

.change {

    width: 70px;

}

.volume {

    width: 100px;

}

.ema {

    width: 130px;

}

.ema10 {

    width: 100px;

}

.warning-col {

    width: 90px;

}

.up {

    color: #00d084;

}

.down {

    color: #ff4d4d;

}

.ema-direction {

    line-height: 1.8;

    font-size: 12px;

}

.ema10-wrap {

    display: flex;

    flex-direction: column;

    align-items: center;

    justify-content: center;

    min-height: 42px;

}

.ema10-up {

    color: #00d084;

    font-size: 13px;

    white-space: nowrap;

}

.ema10-down {

    color: #ff4d4d;

    font-size: 13px;

    white-space: nowrap;

}

.ema10-prev {

    color: #fff;

    font-size: 11px;

    margin-top: 2px;

}

.warning {

    font-size: 22px;

    min-height: 32px;

    display: flex;

    align-items: center;

    justify-content: center;

}

.warning.counting {

    animation: planePulse 1.2s infinite;

}

.warning.pre {

    animation: planePulse 1.4s infinite;

}

.warning.ended {

    font-size: 20px;

    color: #fff;

}

.air-count {

    font-size: 13px;

    margin-left: 2px;

    color: #fff;

}

@keyframes planePulse {

    0% {
        transform: scale(1);
    }

    50% {
        transform: scale(1.12);
    }

    100% {
        transform: scale(1);
    }
}

.footer {

    padding: 15px;

    color: #777;

    font-size: 11px;

    line-height: 1.7;

}

</style>

<script>

setTimeout(function() {

    location.reload();

}, 60000);

</script>

</head>


<body>

<div class="header">

    <div class="title">
        🏆 Upbit 실시간 EMA 대시보드
    </div>

    <div class="info">

        ① 1H EMA1(종가) ↔ EMA10<br>

        ② 현재가격이 확정 EMA10 위 → ✈️ 사전경고<br>

        ③ 1H 종가 EMA10 골든크로스 확정 → ✈️(1)<br>

        ④ 이후 종가 EMA10 위 유지 → ✈️(2), ✈️(3) ...<br>

        ⑤ 종가 EMA10 아래 마감 → ⛔️ 종료

    </div>

</div>

<div class="container">

<table>

<thead>

<tr>

<th class="rank">
순위
</th>

<th class="coin">
코인
</th>

<th class="change">
등락
</th>

<th class="volume">
24H 거래대금
</th>

<th class="ema">
EMA 정배열
</th>

<th class="ema10">
10선
</th>

<th class="warning-col">
비행기
</th>

</tr>

</thead>

<tbody>

{rows}

</tbody>

</table>

</div>

<div class="footer">

비행기 기준은 1시간 EMA1(종가) / EMA10만 사용합니다.<br>

현재가격 경고는 마지막 확정 1시간봉의 EMA10을 기준으로 판단합니다.<br>

⛔️는 종가 기준 EMA10 아래 마감으로 비행기 카운팅이 종료된 직후 표시됩니다.

</div>

</body>

</html>
"""


# =========================================================
# Row HTML
# =========================================================

def make_row(row):

    change = row.get(
        "change_rate",
        0
    )

    try:
        change = float(change)
    except Exception:
        change = 0

    change_class = (
        "up"
        if change > 0
        else
        "down"
        if change < 0
        else ""
    )

    return f"""

<tr>

<td>
{row.get("rank", "-")}
</td>

<td class="coin">
{row.get("coin", "-")}
</td>

<td class="{change_class}">
{change:+.2f}%
</td>

<td>
{format_volume(
    row.get("volume_24h", 0)
)}
</td>

<td class="ema-direction">
{row.get("ema_direction", "-")}
</td>

<td>
{ema10_html(row)}
</td>

<td>
{warning_html(row)}
</td>

</tr>

"""


# =========================================================
# FastAPI
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
def home():

    with data_lock:

        rows = list(latest_data)

        update_time = last_update_time

    if rows:

        html_rows = "".join(
            make_row(x)
            for x in rows
        )

    else:

        html_rows = """

        <tr>

            <td colspan="7">
                데이터 준비 중...
            </td>

        </tr>

        """

    html = HTML.format(
        rows=html_rows
    )

    if update_time:

        html = html.replace(
            "</div>\n\n<div class=\"container\">",
            f"""
            <div style="
                color:#777;
                font-size:11px;
                margin-top:6px;
            ">
                업데이트:
                {update_time.strftime("%Y-%m-%d %H:%M:%S")}
            </div>
            </div>

            <div class="container">
            """
        )

    return HTMLResponse(
        content=html
    )


# =========================================================
# 스케줄러
# =========================================================

def scheduler_loop():

    logging.info(
        "스케줄러 시작"
    )

    while True:

        try:

            schedule.run_pending()

        except Exception as e:

            logging.error(
                "스케줄러 오류: %s",
                e
            )

        time.sleep(1)


# =========================================================
# 초기 업데이트
# =========================================================

def initial_update():

    try:

        update_upbit()

    except Exception as e:

        logging.error(
            "초기 업데이트 실패: %s",
            e
        )


# =========================================================
# 실행
# =========================================================

if __name__ == "__main__":

    logging.info(
        "=============================================="
    )

    logging.info(
        "EMA 대시보드 시작"
    )

    logging.info(
        "비행기 로직:"
    )

    logging.info(
        "현재가격 > 확정 EMA10 → ✈️"
    )

    logging.info(
        "종가 골든크로스 확정 → ✈️(1)"
    )

    logging.info(
        "이후 종가 EMA10 위 → 카운트 +1"
    )

    logging.info(
        "종가 EMA10 아래 → ⛔️ 종료"
    )

    logging.info(
        "=============================================="
    )

    # 최초 데이터
    threading.Thread(
        target=initial_update,
        daemon=True
    ).start()

    # 1분마다 업데이트
    schedule.every(
        UPDATE_MINUTES
    ).minutes.do(
        update_upbit
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
