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
sent_rsi70_coins = {}  # RSI 70 이상 코인과 이전 랭크 저장

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
def get_ohlcv_okx(inst_id, bar='1D', limit=300):
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
# Wilder RSI (TradingView 동일)
# =========================
def wilder_rsi(series, period=3):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(period).mean().iloc[period-1]
    avg_loss = loss.rolling(period).mean().iloc[period-1]

    rsi_values = [np.nan]*(period-1)
    rsi_values.append(100 - 100 / (1 + avg_gain/avg_loss))

    for i in range(period, len(series)):
        avg_gain = (avg_gain*(period-1) + gain.iloc[i])/period
        avg_loss = (avg_loss*(period-1) + loss.iloc[i])/period
        rs = avg_gain / avg_loss if avg_loss != 0 else 0
        rsi = 100 - 100 / (1 + rs)
        rsi_values.append(rsi)

    return pd.Series(rsi_values, index=series.index)

# =========================
# RSI 포맷팅
# =========================
def format_rsi(value, threshold=70):
    if pd.isna(value):
        return "(N/A)"
    if value >= threshold:
        return f"🟢 {value:.1f}"
    else:
        return f"🔴 {value:.1f}"

# =========================
# RSI 70 이상 체크
# =========================
def check_rsi70(inst_id, period=3, threshold=70):
    df = get_ohlcv_okx(inst_id, bar='1D', limit=200)
    if df is None or len(df) < period:
        return None
    rsi = wilder_rsi(df['c'], period).iloc[-1]
    if rsi >= threshold:
        return rsi
    return None

# =========================
# 일간 상승률 계산
# =========================
def calculate_daily_change(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1D", limit=60)
    if df is None or len(df) < 2:
        return None
    try:
        df['datetime'] = pd.to_datetime(df['ts'], unit='ms') + pd.Timedelta(hours=9)
        df.set_index('datetime', inplace=True)
        daily = df['c']
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
    df = get_ohlcv_okx(inst_id, bar="1H", limit=24)
    if df is None or len(df) < 24:
        return 0
    return df['volCcyQuote'].sum()

# =========================
# RSI70 종목 상위 10 표시, 랭킹 변경 시만 메시지 전송
# =========================
def send_rsi70_top10_message(all_ids):
    global sent_rsi70_coins
    rsi70_map = {}
    for inst_id in all_ids:
        rsi_val = check_rsi70(inst_id)
        if rsi_val is not None:
            rsi70_map[inst_id] = rsi_val

    if not rsi70_map:
        logging.info("RSI70 이상 코인 없음")
        return

    # 거래대금 계산 후 상위 10 선정
    volume_map = {inst_id: get_24h_volume(inst_id) for inst_id in rsi70_map.keys()}
    top_ids = sorted(volume_map, key=volume_map.get, reverse=True)[:10]

    # 랭킹 변경 체크
    prev_rank = {k: sent_rsi70_coins.get(k, -1) for k in top_ids}
    rank_changed = False
    for i, inst_id in enumerate(top_ids):
        if prev_rank.get(inst_id, -1) != i:
            rank_changed = True
        sent_rsi70_coins[inst_id] = i

    if not rank_changed:
        logging.info("랭킹 변동 없음 → 메시지 전송 안 함")
        return

    # 메시지 생성
    message_lines = ["⚡ RSI ≥70 거래대금 TOP 10", "━━━━━━━━━━━━━━━━━━━\n"]
    for rank, inst_id in enumerate(top_ids, start=1):
        rsi_val = rsi70_map[inst_id]
        volume_str = format_volume_in_eok(volume_map[inst_id])
        name = inst_id.replace("-USDT-SWAP", "")
        daily_change = calculate_daily_change(inst_id)
        daily_str = f"{daily_change:.2f}%" if daily_change is not None else "(N/A)"
        if daily_change is not None:
            if daily_change >= 5:
                daily_str = f"🟢🔥 {daily_str}"
            elif daily_change > 0:
                daily_str = f"🟢 {daily_str}"
            else:
                daily_str = f"🔴 {daily_str}"

        message_lines.append(
            f"{rank}위 {name} | 💰 거래대금: {volume_str}M | 📊 RSI: {format_rsi(rsi_val)} | 일간: {daily_str}"
        )
    message_lines.append("\n━━━━━━━━━━━━━━━━━━━")
    send_telegram_message("\n".join(message_lines))

# =========================
# 메인 실행
# =========================
def main():
    logging.info("📥 RSI70 거래대금 분석 시작")
    all_ids = get_all_okx_swap_symbols()
    send_rsi70_top10_message(all_ids)

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
