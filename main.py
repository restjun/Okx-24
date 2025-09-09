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
def get_ohlcv_okx(inst_id, bar='4H', limit=300):
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
# MFI 계산 (5기간)
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
# 일간 상승률 계산
# =========================
def calculate_daily_change(inst_id):
    df = get_ohlcv_okx(inst_id, bar="4H", limit=48)
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

def format_volume_in_eok(volume):
    try:
        eok = int(volume // 1_000_000)
        return str(eok) if eok >= 1 else "🚫"
    except:
        return "🚫"

# =========================
# 색상 포맷 (RSI/MFI 시각화)
# =========================
def format_indicator_color(value):
    if value <= 30:
        return f"🟢{value:.1f}"
    elif value >= 70:
        return f"🔴{value:.1f}"
    else:
        return f"{value:.1f}"

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
# 24시간 거래대금
# =========================
def get_24h_volume(inst_id):
    df = get_ohlcv_okx(inst_id, bar="4H", limit=24)
    if df is None or len(df) < 24:
        return 0
    return df['volCcyQuote'].sum()

# =========================
# 메시지 발송
# =========================
def send_new_entry_message(all_ids):
    global last_sent_top10

    volume_map = {inst_id: get_24h_volume(inst_id) for inst_id in all_ids}
    sorted_by_volume = sorted(volume_map, key=volume_map.get, reverse=True)[:20]
    volume_rank_map = {inst_id: rank+1 for rank, inst_id in enumerate(sorted_by_volume)}

    top_positive_coins = []
    force_send = False  # 10분봉 조건에 의해 메시지 강제 전송 여부

    for inst_id in sorted_by_volume:
        if len(top_positive_coins) >= 10:
            break
        df = get_ohlcv_okx(inst_id, bar='4H', limit=10)
        if df is None or len(df) < 5:
            continue

        mfi = calc_mfi(df, period=5).iloc[-1]
        rsi = calc_rsi(df, period=5).iloc[-1]
        daily_change = calculate_daily_change(inst_id)

        if mfi >= 70 and rsi >= 70 and daily_change is not None and daily_change > 0:
            top_positive_coins.append(inst_id)

    if not top_positive_coins:
        return

    # =============================
    # 10분봉 MFI/RSI 체크 (30 이하 발견 시 강제 전송)
    # =============================
    for inst_id in top_positive_coins:
        df_10m = get_ohlcv_okx(inst_id, bar='10m', limit=50)
        if df_10m is not None and len(df_10m) >= 5:
            mfi_10m_val = calc_mfi(df_10m, period=5).iloc[-1]
            rsi_10m_val = calc_rsi(df_10m, period=5).iloc[-1]
            if mfi_10m_val <= 30 or rsi_10m_val <= 30:
                force_send = True
                break

    # =============================
    # 기존 동일 로직 + 강제 전송 조건 추가
    # =============================
    if top_positive_coins == last_sent_top10 and not force_send:
        return

    last_sent_top10 = top_positive_coins.copy()

    message_lines = ["🆕 거래대금 TOP10 RSI/MFI 70 이상 코인 👀 \n(4시간봉 기준, 5기간)"]

    for inst_id in top_positive_coins:
        daily_change = calculate_daily_change(inst_id)
        volume_24h = volume_map.get(inst_id, 0)
        volume_rank = volume_rank_map.get(inst_id, 0)

        # 10분봉 지표 계산
        df_10m = get_ohlcv_okx(inst_id, bar='10m', limit=50)
        if df_10m is not None and len(df_10m) >= 5:
            mfi_10m = format_indicator_color(calc_mfi(df_10m, period=5).iloc[-1])
            rsi_10m = format_indicator_color(calc_rsi(df_10m, period=5).iloc[-1])
        else:
            mfi_10m = rsi_10m = "N/A"

        name = inst_id.replace("-USDT-SWAP", "")
        volume_str = format_volume_in_eok(volume_24h)

        message_lines.append(
            f"{volume_rank}위 {name}\n"
            f"🟢🔥 {daily_change:.2f}% | 💰 {volume_str}M\n"
            f"10분봉 MFI: {mfi_10m} | RSI: {rsi_10m}"
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
