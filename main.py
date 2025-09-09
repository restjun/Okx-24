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

telegram_bot_token = "8451481398:AAHHg2wVDKphMruKsjN2b6NFKJ50jhxEe-g"
telegram_user_id = 6596886700
bot = telepot.Bot(telegram_bot_token)

logging.basicConfig(level=logging.INFO)
sent_signal_coins = {}

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
# OKX OHLCV 가져오기 (일봉)
# =========================
def get_ohlcv_okx(inst_id, bar='1D', limit=200):
    url = f"https://www.okx.com/api/v5/market/candles?instId={inst_id}&bar={bar}&limit={limit}"
    response = retry_request(requests.get, url)
    if response is None:
        return None
    try:
        df = pd.DataFrame(response.json()['data'], columns=[
            'ts','o','h','l','c','vol','volCcy','volCcyQuote','confirm'
        ])
        for col in ['o','h','l','c','vol','volCcyQuote']:
            df[col] = df[col].astype(float)
        df = df.iloc[::-1].reset_index(drop=True)
        return df
    except Exception as e:
        logging.error(f"{inst_id} OHLCV 파싱 실패: {e}")
        return None

# =========================
# RMA / RSI / MFI 계산 (5일)
# =========================
def rma(series, period):
    series = series.copy()
    alpha = 1 / period
    r = series.ewm(alpha=alpha, adjust=False).mean()
    r.iloc[:period] = series.iloc[:period].expanding().mean()[:period]
    return r

def calc_rsi(df, period=5):
    delta = df['c'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = rma(gain, period)
    avg_loss = rma(loss, period)
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

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
# 일봉 RSI/MFI 돌파 확인
# =========================
def check_daily_mfi_rsi_cross(inst_id, period=5, threshold=70):
    df = get_ohlcv_okx(inst_id, bar='1D', limit=200)
    if df is None or len(df) < period + 1:
        return False, None

    mfi = calc_mfi(df, period)
    rsi = calc_rsi(df, period)

    prev_mfi, curr_mfi = mfi.iloc[-2], mfi.iloc[-1]
    prev_rsi, curr_rsi = rsi.iloc[-2], rsi.iloc[-1]
    cross_time = pd.to_datetime(df['ts'].iloc[-1], unit='ms') + pd.Timedelta(hours=9)

    if pd.isna(curr_mfi) or pd.isna(curr_rsi):
        return False, None

    crossed = (
        (curr_mfi >= threshold and curr_rsi >= threshold) and
        (prev_mfi < threshold or prev_rsi < threshold)
    )
    return crossed, cross_time if crossed else None

# =========================
# 24시간 거래대금
# =========================
def get_24h_volume(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1D", limit=1)
    if df is None or len(df) < 1:
        return 0
    return df['volCcyQuote'].iloc[-1]

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
# 메시지 생성 및 전송
# =========================
def send_new_entry_message(all_ids):
    today_str = datetime.now().strftime("%Y-%m-%d")
    volume_map = {inst_id: get_24h_volume(inst_id) for inst_id in all_ids}
    sorted_by_volume = sorted(volume_map, key=volume_map.get, reverse=True)[:50]

    change_map = {}
    for inst_id in sorted_by_volume:
        df = get_ohlcv_okx(inst_id, bar='1D', limit=2)
        if df is not None and len(df) >= 2:
            today_close, yesterday_close = df['c'].iloc[-1], df['c'].iloc[-2]
            change_map[inst_id] = round((today_close - yesterday_close) / yesterday_close * 100, 2)

    top_ids = sorted(change_map, key=change_map.get, reverse=True)[:10]

    message_lines = ["📊 일봉 TOP10 코인 (RSI/MFI 70 돌파 여부)"]
    for rank, inst_id in enumerate(top_ids, start=1):
        crossed, cross_time = check_daily_mfi_rsi_cross(inst_id)
        name = inst_id.replace("-USDT-SWAP", "")
        volume_str = f"{volume_map.get(inst_id,0)/1_000_000:.1f}M"
        change = change_map.get(inst_id, 0)
        cross_str = cross_time.strftime("%Y-%m-%d") if cross_time else "-"
        status = "✅ 돌파" if crossed else "❌"
        message_lines.append(f"{rank}위 {name} | {change:.2f}% | 💰 {volume_str} | {status} ({cross_str})")

    send_telegram_message("\n".join(message_lines))

# =========================
# 메인 실행
# =========================
def main():
    logging.info("📥 일봉 거래대금 분석 시작")
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

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
