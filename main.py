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
# RSI 포맷팅
# =========================
def format_rsi(value, threshold=70):
    if pd.isna(value):
        return "(N/A)"
    return f"🔴 {value:.1f}" if value <= threshold else f"🟢 {value:.1f}"

# =========================
# 1H RSI 상향 돌파 확인
# =========================
def check_1h_rsi_cross_up(inst_id, period=5, threshold=70):
    df = get_ohlcv_okx(inst_id, bar='1H', limit=200)
    if df is None or len(df) < period + 1:
        return False, None
    rsi = calc_rsi(df, period)
    prev_rsi, curr_rsi = rsi.iloc[-2], rsi.iloc[-1]
    cross_time = pd.to_datetime(df['ts'].iloc[-1], unit='ms') + pd.Timedelta(hours=9)
    if pd.isna(curr_rsi):
        return False, None
    crossed = (curr_rsi >= threshold) and (prev_rsi < threshold)
    return crossed, cross_time if crossed else None

# =========================
# 1H RSI 하향 돌파 확인 (10개 캔들 전 기준 + 10캔들 유지)
# =========================
def check_1h_rsi_cross_down(inst_id, period=5, threshold=70, maintain_candles=10):
    df = get_ohlcv_okx(inst_id, bar='1H', limit=200)
    if df is None or len(df) < period + maintain_candles + 1:
        return False, None

    rsi = calc_rsi(df, period)
    prev_rsi = rsi.iloc[-(maintain_candles+1)]
    curr_rsi = rsi.iloc[-maintain_candles]

    crossed = (curr_rsi <= threshold) and (prev_rsi > threshold)
    if not crossed:
        return False, None

    # 돌파 후 유지 확인
    maintain_rsi = rsi.iloc[-maintain_candles:]
    if (maintain_rsi <= threshold).all():
        cross_time = pd.to_datetime(df['ts'].iloc[-maintain_candles], unit='ms') + pd.Timedelta(hours=9)
        return True, cross_time
    else:
        return False, None

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
# 신규 진입 알림
# =========================
def send_new_entry_message(all_ids):
    global sent_signal_coins
    volume_map = {inst_id: get_24h_volume(inst_id) for inst_id in all_ids}
    top_ids = sorted(volume_map, key=volume_map.get, reverse=True)[:20]
    rank_map = {inst_id: rank + 1 for rank, inst_id in enumerate(top_ids)}

    new_entry_up = []
    new_entry_down = []

    for inst_id in ["BTC-USDT-SWAP"] + top_ids:
        if inst_id not in sent_signal_coins:
            sent_signal_coins[inst_id] = {"crossed_up": False, "crossed_down": False}

    for inst_id in top_ids:
        df = get_ohlcv_okx(inst_id, bar="1H", limit=200)
        if df is None:
            continue
        rsi = calc_rsi(df, period=5)
        curr_rsi = rsi.iloc[-1]

        # 상향 돌파
        is_cross_up, cross_time_up = check_1h_rsi_cross_up(inst_id, period=5, threshold=70)
        if is_cross_up and not sent_signal_coins[inst_id]["crossed_up"]:
            daily_change = calculate_daily_change(inst_id)
            if daily_change is not None and daily_change > 0:
                new_entry_up.append((inst_id, daily_change, volume_map[inst_id], rank_map[inst_id], cross_time_up, curr_rsi))
            sent_signal_coins[inst_id]["crossed_up"] = True
        elif not is_cross_up:
            sent_signal_coins[inst_id]["crossed_up"] = False

        # 하향 돌파 (10캔들 유지)
        is_cross_down, cross_time_down = check_1h_rsi_cross_down(inst_id, period=5, threshold=70, maintain_candles=10)
        if is_cross_down and not sent_signal_coins[inst_id]["crossed_down"]:
            daily_change = calculate_daily_change(inst_id)
            if daily_change is not None and daily_change > 0:
                new_entry_down.append((inst_id, daily_change, volume_map[inst_id], rank_map[inst_id], cross_time_down, curr_rsi))
            sent_signal_coins[inst_id]["crossed_down"] = True
        elif not is_cross_down:
            sent_signal_coins[inst_id]["crossed_down"] = False

    # 메시지 전송
    if new_entry_up or new_entry_down:
        message_lines = ["⚡ 1H RSI 필터 (5기간)", "━━━━━━━━━━━━━━━━━━━\n"]

        if new_entry_up:
            message_lines.append("🆕 신규 상향 돌파 코인 👆")
            for inst_id, daily_change, volume_24h, coin_rank, cross_time, curr_rsi in new_entry_up:
                name = inst_id.replace("-USDT-SWAP", "")
                vol_str = format_volume_in_eok(volume_24h)
                rsi_str = format_rsi(curr_rsi, threshold=70)
                message_lines.append(f"{coin_rank}위 {name} | +{daily_change:.2f}% | 💰 {vol_str}M | RSI {rsi_str}")

        if new_entry_down:
            message_lines.append("\n🆕 신규 하향 돌파 코인 👇")
            for inst_id, daily_change, volume_24h, coin_rank, cross_time, curr_rsi in new_entry_down:
                name = inst_id.replace("-USDT-SWAP", "")
                vol_str = format_volume_in_eok(volume_24h)
                rsi_str = format_rsi(curr_rsi, threshold=70)
                message_lines.append(f"{coin_rank}위 {name} | +{daily_change:.2f}% | 💰 {vol_str}M | RSI {rsi_str}")

        message_lines.append("\n━━━━━━━━━━━━━━━━━━━")
        send_telegram_message("\n".join(message_lines))
    else:
        logging.info("⚡ 신규 진입 없음 → 메시지 전송 안 함")

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
