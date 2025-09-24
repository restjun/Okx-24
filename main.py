from fastapi import FastAPI
import telepot
import schedule
import time
import requests
import threading
import uvicorn
import logging
import pandas as pd
import numpy as np

app = FastAPI()

# =========================
# Telegram 설정
# =========================
telegram_bot_token = "8451481398:AAHHg2wVDKphMruKsjN2b6NFKJ50jhxEe-g"
telegram_user_id = 6596886700
bot = telepot.Bot(telegram_bot_token)

logging.basicConfig(level=logging.INFO)

sent_signal_coins = {}
last_top10 = []  # ✅ 최근 발송한 TOP10 저장


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
            logging.error(f"텔레그램 메시지 전송 실패 (재시도 {retry_count}/10): {e}")
            time.sleep(5)
    logging.error("텔레그램 메시지 전송 실패: 최대 재시도 초과")


# =========================
# API 호출 재시도
# =========================
def retry_request(func, *args, **kwargs):
    for attempt in range(10):
        try:
            result = func(*args, **kwargs)
            if hasattr(result, 'status_code') and result.status_code == 429:
                time.sleep(1)
                continue
            return result
        except Exception as e:
            logging.error(f"API 호출 실패 (재시도 {attempt+1}/10): {e}")
            time.sleep(5)
    return None


# =========================
# OKX OHLCV 가져오기
# =========================
def get_ohlcv_okx(inst_id, bar='1H', limit=300):
    url = f"https://www.okx.com/api/v5/market/candles?instId={inst_id}&bar={bar}&limit={limit}"
    response = retry_request(requests.get, url)
    if response is None:
        return None
    try:
        df = pd.DataFrame(response.json()['data'], columns=[
            'ts', 'o', 'h', 'l', 'c', 'vol', 'volCcy', 'volCcyQuote', 'confirm'
        ])
        for col in ['o', 'h', 'l', 'c', 'vol', 'volCcyQuote']:
            df[col] = df[col].astype(float)
        df = df.iloc[::-1].reset_index(drop=True)
        return df
    except Exception as e:
        logging.error(f"{inst_id} OHLCV 파싱 실패: {e}")
        return None


# =========================
# RMA 계산
# =========================
def rma(series, period):
    series = series.copy()
    alpha = 1 / period
    r = series.ewm(alpha=alpha, adjust=False).mean()
    r.iloc[:period] = series.iloc[:period].expanding().mean()[:period]
    return r


# =========================
# RSI 계산 (5기간)
# =========================
def calc_rsi(df, period=5):
    delta = df['c'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = rma(gain, period)
    avg_loss = rma(loss, period)
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


# =========================
# EMA 계산
# =========================
def calc_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


# =========================
# RSI 포맷팅
# =========================
def format_rsi(value, threshold=70):
    if pd.isna(value):
        return "(N/A)"
    return f"🔴 {value:.1f}" if value <= threshold else f"🟢 {value:.1f}"


# =========================
# 일간 상승률 계산 (1H 데이터 기반)
# =========================
def calculate_daily_change(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=48)
    if df is None or len(df) < 6:
        return None
    try:
        df['datetime'] = pd.to_datetime(df['ts'], unit='ms') + pd.Timedelta(hours=9)
        df.set_index('datetime', inplace=True)
        daily = df['c'].resample('1D', offset='9h').last()
        if len(daily) < 2:
            return None
        today_close = daily.iloc[-1]
        yesterday_close = daily.iloc[-2]
        return round((today_close - yesterday_close) / yesterday_close * 100, 2)
    except Exception as e:
        logging.error(f"{inst_id} 상승률 계산 오류: {e}")
        return None


# =========================
# 거래대금 포맷
# =========================
def format_volume_in_eok(volume):
    try:
        eok = int(volume // 1_000_000)
        return str(eok) if eok >= 1 else "🚫"
    except:
        return "🚫"


# =========================
# 모든 USDT-SWAP 심볼
# =========================
def get_all_okx_swap_symbols():
    url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
    response = retry_request(requests.get, url)
    if response is None:
        return []
    data = response.json().get("data", [])
    return [item["instId"] for item in data if "USDT" in item["instId"]]


# =========================
# 24시간 거래대금 계산 (1H 데이터 기반)
# =========================
def get_24h_volume(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=24)
    if df is None or len(df) < 24:
        return 0
    return df['volCcyQuote'].sum()


# =========================
# 거래대금 TOP10 알림 (TOP10 변화 or RSI>60 존재 시 발송)
# =========================
def send_new_entry_message(all_ids):
    global last_top10

    volume_map = {inst_id: get_24h_volume(inst_id) for inst_id in all_ids}
    top_ids = sorted(volume_map, key=volume_map.get, reverse=True)[:10]

    # ✅ RSI>60 존재 여부 체크
    rsi_over_60 = False
    for inst_id in top_ids:
        df_1h = get_ohlcv_okx(inst_id, bar='1H', limit=100)
        if df_1h is not None and len(df_1h) >= 5:
            rsi_val = calc_rsi(df_1h, 5).iloc[-1]
            if rsi_val is not None and rsi_val > 60:
                rsi_over_60 = True
                break

    # ✅ TOP10 변화 없고 RSI>60도 없으면 메시지 발송 안 함
    if last_top10 == top_ids and not rsi_over_60:
        logging.info("TOP10 변화 없음 & RSI>60 없음 → 메시지 전송 안 함")
        return

    # ✅ TOP10 갱신
    last_top10 = top_ids  

    message_lines = [
        "💰 실시간 24H 거래대금 TOP 10",
        "━━━━━━━━━━━━━━━━━━━\n"
    ]

    for rank, inst_id in enumerate(top_ids, start=1):
        name = inst_id.replace("-USDT-SWAP", "")
        volume_str = format_volume_in_eok(volume_map.get(inst_id, 0))

        daily_change = calculate_daily_change(inst_id)
        if daily_change is None:
            daily_str = "N/A"
        elif daily_change >= 5:
            daily_str = f"🟢🚨🚨🚨 {daily_change:.2f}%"
        elif daily_change > 0:
            daily_str = f"🟢 {daily_change:.2f}%"
        else:
            daily_str = f"🔴 {daily_change:.2f}%"

        df_1h = get_ohlcv_okx(inst_id, bar='1H', limit=100)
        rsi_1h = calc_rsi(df_1h, 5).iloc[-1] if df_1h is not None and len(df_1h) >= 5 else None

        message_lines.append(
            f"{rank}위 {name}\n"
            f"{daily_str} | 💰 거래대금: {volume_str}M\n"
            f"📊 1H → RSI: {format_rsi(rsi_1h, 70)}"
        )

    message_lines.append("\n━━━━━━━━━━━━━━━━━━━")

    if rsi_over_60:
        message_lines.append("⚡ RSI 60 이상 종목 존재 → 리스트 갱신")

    send_telegram_message("\n".join(message_lines))


# =========================
# 메인 실행
# =========================
def main():
    logging.info("📥 거래대금 분석 시작")
    all_ids = get_all_okx_swap_symbols()
    send_new_entry_message(all_ids)


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
