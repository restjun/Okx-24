from fastapi import FastAPI
import telepot
import schedule
import time
import requests
import threading
import uvicorn
import logging
import pandas as pd

app = FastAPI()

# =========================
# Telegram 설정
# =========================
telegram_bot_token = "8451481398:AAHHg2wVDKphMruKsjN2b6NFKJ50jhxEe-g"
telegram_user_id = 6596886700
bot = telepot.Bot(telegram_bot_token)

logging.basicConfig(level=logging.INFO)

# =========================
# 거래대금 TOP10 상태 저장
# =========================
previous_top10 = set()

# =========================
# Telegram 메시지 전송
# =========================
def send_telegram_message(message):
    for retry_count in range(1, 11):
        try:
            bot.sendMessage(chat_id=telegram_user_id, text=message)
            logging.info("텔레그램 메시지 전송 성공")
            return
        except Exception as e:
            logging.error(f"텔레그램 전송 실패 ({retry_count}/10): {e}")
            time.sleep(5)

# =========================
# API 재시도
# =========================
def retry_request(func, *args, **kwargs):
    for attempt in range(10):
        try:
            result = func(*args, **kwargs)
            if hasattr(result, "status_code") and result.status_code == 429:
                time.sleep(1)
                continue
            return result
        except Exception as e:
            logging.error(f"API 실패 ({attempt+1}/10): {e}")
            time.sleep(3)
    return None

# =========================
# OKX OHLCV
# =========================
def get_ohlcv_okx(inst_id, bar="1H", limit=48):
    url = f"https://www.okx.com/api/v5/market/candles?instId={inst_id}&bar={bar}&limit={limit}"
    response = retry_request(requests.get, url)
    if response is None:
        return None

    try:
        df = pd.DataFrame(
            response.json()["data"],
            columns=["ts", "o", "h", "l", "c", "vol", "volCcy", "volCcyQuote", "confirm"]
        )
        for col in ["c", "volCcyQuote"]:
            df[col] = df[col].astype(float)
        df = df.iloc[::-1].reset_index(drop=True)
        return df
    except Exception as e:
        logging.error(f"{inst_id} OHLCV 파싱 실패: {e}")
        return None

# =========================
# 일간 상승률 계산
# =========================
def calculate_daily_change(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=48)
    if df is None or len(df) < 24:
        return None

    try:
        df["datetime"] = pd.to_datetime(df["ts"], unit="ms") + pd.Timedelta(hours=9)
        df.set_index("datetime", inplace=True)
        daily = df["c"].resample("1D", offset="9h").last()

        if len(daily) < 2:
            return None

        return round((daily.iloc[-1] - daily.iloc[-2]) / daily.iloc[-2] * 100, 2)
    except:
        return None

# =========================
# 거래대금 포맷
# =========================
def format_volume_in_eok(volume):
    try:
        eok = int(volume // 1_000_000)
        return f"{eok}M" if eok >= 1 else "🚫"
    except:
        return "🚫"

# =========================
# OKX USDT-SWAP 전체 심볼
# =========================
def get_all_okx_swap_symbols():
    url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
    response = retry_request(requests.get, url)
    if response is None:
        return []

    data = response.json().get("data", [])
    return [item["instId"] for item in data if "USDT" in item["instId"]]

# =========================
# 24시간 거래대금
# =========================
def get_24h_volume(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=24)
    if df is None or len(df) < 24:
        return 0
    return df["volCcyQuote"].sum()

# =========================
# 거래대금 TOP 신규진입 감지
# =========================
def send_volume_rank_message(all_ids):
    global previous_top10

    volume_map = {inst_id: get_24h_volume(inst_id) for inst_id in all_ids}
    top_ids = sorted(volume_map, key=volume_map.get, reverse=True)[:10]
    current_top10 = set(top_ids)

    new_entries = current_top10 - previous_top10

    if not new_entries:
        logging.info("🔕 TOP10 신규 진입 없음 → 알림 미전송")
        previous_top10 = current_top10
        return

    message_lines = [
        "🚨 실거래대금 TOP10 신규 진입 감지",
        "━━━━━━━━━━━━━━━━━━━"
    ]

    for inst_id in sorted(new_entries, key=lambda x: volume_map[x], reverse=True):
        name = inst_id.replace("-USDT-SWAP", "")
        volume_str = format_volume_in_eok(volume_map[inst_id])

        daily_change = calculate_daily_change(inst_id)
        if daily_change is None:
            daily_str = "N/A"
        elif daily_change >= 5:
            daily_str = f"🟢🚨 {daily_change:.2f}%"
        elif daily_change > 0:
            daily_str = f"🟢 {daily_change:.2f}%"
        else:
            daily_str = f"🔴 {daily_change:.2f}%"

        message_lines.append(
            f"{name}\n"
            f"{daily_str} | 💰 거래대금: {volume_str}"
        )

    message_lines.append("━━━━━━━━━━━━━━━━━━━")
    send_telegram_message("\n".join(message_lines))

    previous_top10 = current_top10

# =========================
# 메인
# =========================
def main():
    logging.info("📥 실거래대금 TOP 신규진입 분석")
    all_ids = get_all_okx_swap_symbols()
    send_volume_rank_message(all_ids)

# =========================
# 스케줄러
# =========================
def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

@app.on_event("startup")
def start_scheduler():
    schedule.every(1).minutes.do(main)
    threading.Thread(target=run_scheduler, daemon=True).start()

# =========================
# FastAPI 실행
# =========================
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
