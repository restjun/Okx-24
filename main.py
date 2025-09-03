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
sent_signal_coins = {}  # 알림 발송 기록

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
# RMA 계산
# =========================
def rma(series, period):
    series = series.copy()
    alpha = 1 / period
    r = series.ewm(alpha=alpha, adjust=False).mean()
    r.iloc[:period] = series.iloc[:period].expanding().mean()[:period]
    return r

# =========================
# RSI 계산 (3일선)
# =========================
def calc_rsi(df, period=3):
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
def format_rsi(value):
    if pd.isna(value):
        return "(N/A)"
    return f"🔵 {value:.1f}"

# =========================
# 일간 상승률 계산
# =========================
def calculate_daily_change(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1D", limit=10)
    if df is None or len(df) < 2:
        return None
    try:
        df['datetime'] = pd.to_datetime(df['ts'], unit='ms') + pd.Timedelta(hours=9)
        df.set_index('datetime', inplace=True)
        daily_close = df['c']
        today_close = daily_close.iloc[-1]
        yesterday_close = daily_close.iloc[-2]
        return round((today_close - yesterday_close) / yesterday_close * 100, 2)
    except Exception as e:
        logging.error(f"{inst_id} 상승률 계산 오류: {e}")
        return None

# =========================
# 거래대금 포맷팅
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
# 24시간 거래대금
# =========================
def get_24h_volume(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=24)
    if df is None or len(df) < 24:
        return 0
    return df['volCcyQuote'].sum()

# =========================
# 신규 돌파 종목 알림 (RSI 3일선 ≥70, 상승률 ≥0, 거래대금 순위 표시, RSI 지수)
# =========================
def send_new_entry_message(all_ids, top_n=10):
    global sent_signal_coins
    new_entry_coins = []
    rsi_70_up_count = 0
    rsi_70_down_count = 0

    for inst_id in all_ids:
        df = get_ohlcv_okx(inst_id, bar='1D', limit=10)
        if df is None or len(df) < 3:
            continue

        rsi_val = calc_rsi(df, period=3).iloc[-1]
        if pd.isna(rsi_val):
            continue

        # RSI 70 이상/미만 카운트
        if rsi_val >= 70:
            rsi_70_up_count += 1
        else:
            rsi_70_down_count += 1

        # 신규 돌파 조건
        if rsi_val < 70:
            continue
        if inst_id in sent_signal_coins and sent_signal_coins[inst_id]:
            continue
        daily_change = calculate_daily_change(inst_id)
        if daily_change is None or daily_change < 0:
            continue

        volume = get_24h_volume(inst_id)
        new_entry_coins.append((inst_id, daily_change, volume, rsi_val))

    total_checked = rsi_70_up_count + rsi_70_down_count
    ratio_up = f"{rsi_70_up_count}/{total_checked}" if total_checked > 0 else "N/A"
    ratio_down = f"{rsi_70_down_count}/{total_checked}" if total_checked > 0 else "N/A"

    if not new_entry_coins:
        logging.info("⚡ 신규 돌파 종목 없음 → 메시지 전송 안 함")
        return

    # 거래대금 상위 정렬
    new_entry_coins.sort(key=lambda x: x[2], reverse=True)
    new_entry_coins = new_entry_coins[:top_n]

    # 메시지 생성
    message_lines = [
        "⚡ 신규 돌파 종목 (일봉 RSI 3일선 ≥70, 상승 종목)",
        "━━━━━━━━━━━━━━━━━━━\n",
        f"📊 거래대금 기준 상위 {top_n}종목",
        f"RSI 70 이상/미만 지수: {ratio_up} / {ratio_down}\n"
    ]

    for rank, (inst_id, daily_change, volume, rsi_val) in enumerate(new_entry_coins, start=1):
        volume_str = format_volume_in_eok(volume)
        name = inst_id.replace("-USDT-SWAP", "")
        daily_str = f"{daily_change:.2f}%" if daily_change is not None else "(N/A)"
        message_lines.append(
            f"{rank}위 {name}\n"
            f"{daily_str} | 💰 거래대금: {volume_str}M\n"
            f"📊 일봉 → RSI: 🔵 {rsi_val:.1f}"
        )
        # 중복 방지 기록
        sent_signal_coins[inst_id] = True

    message_lines.append("\n━━━━━━━━━━━━━━━━━━━")
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
