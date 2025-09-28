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
# EMA 계산
# =========================
def calc_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

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
# 신규 진입 알림 (5-20 EMA 돌파 + 일봉 정배열 조건)
# =========================
def send_new_entry_message(all_ids):
    global sent_signal_coins
    volume_map = {inst_id: get_24h_volume(inst_id) for inst_id in all_ids}
    top_ids = sorted(volume_map, key=volume_map.get, reverse=True)[:10]
    rank_map = {inst_id: rank + 1 for rank, inst_id in enumerate(top_ids)}

    new_entry_coins = []

    for inst_id in ["BTC-USDT-SWAP"] + top_ids:
        if inst_id not in sent_signal_coins:
            sent_signal_coins[inst_id] = {"crossed": False, "time": None}

    for inst_id in top_ids:
        df_1d = get_ohlcv_okx(inst_id, bar='1D', limit=300)
        if df_1d is None or len(df_1d) < 200:
            continue

        ema5 = calc_ema(df_1d['c'], 5)
        ema20 = calc_ema(df_1d['c'], 20)
        ema50 = calc_ema(df_1d['c'], 50)
        ema200 = calc_ema(df_1d['c'], 200)

        prev_ema5 = ema5.iloc[-2]
        prev_ema20 = ema20.iloc[-2]
        curr_ema5 = ema5.iloc[-1]
        curr_ema20 = ema20.iloc[-1]

        # 정배열 조건: 20 > 50 > 200
        is_bullish = ema20.iloc[-1] > ema50.iloc[-1] > ema200.iloc[-1]
        if not is_bullish:
            continue

        daily_change = calculate_daily_change(inst_id)
        if daily_change is None:
            continue

        # 5 → 20 신규 돌파 조건
        if prev_ema5 <= prev_ema20 and curr_ema5 > curr_ema20:
            new_entry_coins.append(
                (inst_id, daily_change, volume_map.get(inst_id, 0), rank_map.get(inst_id))
            )
            sent_signal_coins[inst_id]["crossed"] = True
        else:
            sent_signal_coins[inst_id]["crossed"] = False

    if new_entry_coins:
        new_entry_coins.sort(key=lambda x: x[2], reverse=True)

        message_lines = [
            "⚡ 일봉 5-20 EMA 돌파 신호 (20>50>200 정배열 조건)",
            "━━━━━━━━━━━━━━━━━━━\n",
            "🏆 실거래대금 TOP 10\n"
        ]

        for rank, (inst_id, daily_change, volume_24h, coin_rank) in enumerate(new_entry_coins[:10], start=1):
            name = inst_id.replace("-USDT-SWAP", "")
            volume_str = format_volume_in_eok(volume_24h)
            daily_str = f"{daily_change:.2f}%"
            if daily_change >= 5:
                daily_str = f"🟢🚨🚨🚨 {daily_str}"
            elif daily_change > 0:
                daily_str = f"🟢 {daily_str}"

            message_lines.append(
                f"{rank}위 {name} | 실거래대금 순위: {coin_rank}\n"
                f"{daily_str} | 💰 거래대금: {volume_str}M"
            )

        message_lines.append("\n━━━━━━━━━━━━━━━━━━━")
        send_telegram_message("\n".join(message_lines))
    else:
        logging.info("⚡ 조건 만족 코인 없음 → 메시지 전송 안 함")

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
