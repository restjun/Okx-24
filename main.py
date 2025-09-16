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
from datetime import datetime

app = FastAPI()

# =========================
# Telegram 설정
# =========================
telegram_bot_token = "8451481398:AAHHg2wVDKphMruKsjN2b6NFKJ50jhxEe-g"
telegram_user_id = 6596886700
bot = telepot.Bot(telegram_bot_token)

logging.basicConfig(level=logging.INFO)
last_sent_top10 = []

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
def get_ohlcv_okx(inst_id, bar='4h', limit=300):  # 기본 bar 4h로 변경
    url = f"https://www.okx.com/api/v5/market/candles?instId={inst_id}&bar={bar}&limit={limit}"
    response = retry_request(requests.get, url)
    if response is None:
        return None
    try:
        df = pd.DataFrame(
            response.json()['data'],
            columns=['ts', 'o', 'h', 'l', 'c', 'vol', 'volCcy', 'volCcyQuote', 'confirm']
        )
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
# RSI 계산
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
# MFI 계산
# =========================
def calc_mfi(df, period=5):
    tp = (df['h'] + df['l'] + df['c']) / 3
    mf = tp * df['volCcyQuote']
    delta_tp = tp.diff()
    positive_mf = mf.where(delta_tp > 0, 0.0)
    negative_mf = mf.where(delta_tp < 0, 0.0)
    pos_sum = positive_mf.rolling(period).sum()
    neg_sum = negative_mf.rolling(period).sum()
    with np.errstate(divide='ignore', invalid='ignore'):
        mfi = 100 * pos_sum / (pos_sum + neg_sum)
    return mfi

# =========================
# 일간 상승률 계산 (1시간봉 기준)
# =========================
def calculate_daily_change(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=48)
    if df is None or len(df) < 24:
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
# 24시간 거래대금
# =========================
def get_24h_volume(inst_id):
    df = get_ohlcv_okx(inst_id, bar="4h", limit=24)  # 4시간봉 기준
    if df is None or len(df) < 24:
        return 0
    return df['volCcyQuote'].sum()

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
# 메시지 발송 (조건: RSI와 MFI 둘 다 70 이상일 때)
# =========================
def send_new_entry_message(all_ids):
    global last_sent_top10

    volume_map = {inst_id: get_24h_volume(inst_id) for inst_id in all_ids}  
    sorted_by_volume = sorted(volume_map, key=volume_map.get, reverse=True)[:30]  

    alert_coins = []  

    for inst_id in sorted_by_volume:  
        df_4h = get_ohlcv_okx(inst_id, bar='4h', limit=10)
        if df_4h is None or len(df_4h) < 6:  
            continue  

        rsi_series = calc_rsi(df_4h, period=5)  
        mfi_series = calc_mfi(df_4h, period=5)  

        rsi_now = rsi_series.iloc[-1]  
        mfi_now = mfi_series.iloc[-1]  

        daily_change = calculate_daily_change(inst_id)  

        both_above_70 = (rsi_now >= 70 and mfi_now >= 70)  

        if both_above_70 and daily_change is not None and daily_change > 0:  
            if inst_id not in [coin[0] for coin in last_sent_top10]:  
                rank = sorted_by_volume.index(inst_id) + 1  
                alert_coins.append(  
                    (inst_id, mfi_now, rsi_now, daily_change, volume_map[inst_id], rank)  
                )  

    if not alert_coins:  
        return  

    last_sent_top10.extend(alert_coins)  

    message_lines = ["⚠️ 4h RSI/MFI 둘 다 70 이상 신호 👀"]  

    for idx, (inst_id, mfi_4h, rsi_4h, daily_change, vol, rank) in enumerate(alert_coins, start=1):  
        name = inst_id.replace("-USDT-SWAP", "")  

        def fmt_val(val):  
            if val is None:  
                return "N/A"  
            if val >= 70:  
                return f"🔴{val:.2f}"  
            elif val <= 30:  
                return f"🟢{val:.2f}"  
            return f"{val:.2f}"  

        message_lines.append(  
            f"{idx}. {name}\n"  
            f"🕒 4h MFI: {fmt_val(mfi_4h)} | RSI: {fmt_val(rsi_4h)}\n"  
            f"📈 {daily_change:.2f}% | 💰 {format_volume_in_eok(vol)}M (#{rank})"  
        )  

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
