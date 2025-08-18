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

telegram_bot_token = "8451481398:AAHHg2wVDKphMruKsjN2b6NFKJ50jhxEe-g"
telegram_user_id = 6596886700
bot = telepot.Bot(telegram_bot_token)

logging.basicConfig(level=logging.INFO)

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

def calculate_ema(close, period):
    if len(close) < period:
        return None
    return pd.Series(close).ewm(span=period, adjust=False).mean().iloc[-1]

def get_ema_with_retry(close, period):
    for _ in range(5):
        result = calculate_ema(close, period)
        if result is not None:
            return result
        time.sleep(0.5)
    return None

def get_ohlcv_okx(instId, bar='1H', limit=200):
    url = f"https://www.okx.com/api/v5/market/candles?instId={instId}&bar={bar}&limit={limit}"
    response = retry_request(requests.get, url)
    if response is None:
        return None
    try:
        df = pd.DataFrame(response.json()['data'], columns=[
            'ts', 'o', 'h', 'l', 'c', 'vol', 'volCcy', 'volCcyQuote', 'confirm'
        ])
        df['c'] = df['c'].astype(float)
        df['o'] = df['o'].astype(float)
        df['vol'] = df['vol'].astype(float)
        df['volCcyQuote'] = df['volCcyQuote'].astype(float)
        return df.iloc[::-1]
    except Exception as e:
        logging.error(f"{instId} OHLCV 파싱 실패: {e}")
        return None

# === EMA 상태 계산 ===
def get_ema_status_line(inst_id):
    try:
        # --- 1D EMA (5-10) ---
        df_1d = get_ohlcv_okx(inst_id, bar='1D', limit=300)
        if df_1d is None:
            daily_status = "[1D] ❌"
        else:
            ema_5_1d = get_ema_with_retry(df_1d['c'].values, 5)
            ema_10_1d = get_ema_with_retry(df_1d['c'].values, 10)
            if None in [ema_5_1d, ema_10_1d]:
                daily_status = "[1D] ❌"
            else:
                status_5_10_1d = "🟩" if ema_5_1d > ema_10_1d else "🟥"
                daily_status = f"[1D] 📊: {status_5_10_1d}"

        # --- 4H EMA (5-10) ---
        df_4h = get_ohlcv_okx(inst_id, bar='4H', limit=300)
        if df_4h is None:
            fourh_status = "[4H] ❌"
            fourh_ok_long = False
            fourh_ok_short = False
        else:
            ema_5_4h = get_ema_with_retry(df_4h['c'].values, 5)
            ema_10_4h = get_ema_with_retry(df_4h['c'].values, 10)
            if None in [ema_5_4h, ema_10_4h]:
                fourh_status = "[4H] ❌"
                fourh_ok_long = False
                fourh_ok_short = False
            else:
                status_5_10_4h = "🟩" if ema_5_4h > ema_10_4h else "🟥"
                fourh_status = f"[4H] 📊: {status_5_10_4h}"
                fourh_ok_long = ema_5_4h > ema_10_4h
                fourh_ok_short = ema_5_4h < ema_10_4h

        # --- 1H EMA (3-5, 5-10) ---
        df_1h = get_ohlcv_okx(inst_id, bar='1H', limit=300)
        if df_1h is None or len(df_1h) < 5:
            return f"{daily_status} | {fourh_status} | [1H] ❌", None

        closes = df_1h['c'].values
        ema_3_now = get_ema_with_retry(closes, 3)
        ema_5_now = get_ema_with_retry(closes, 5)
        ema_10_now = get_ema_with_retry(closes, 10)
        ema_3_prev = get_ema_with_retry(closes[:-1], 3)
        ema_5_prev = get_ema_with_retry(closes[:-1], 5)

        if None in [ema_3_now, ema_5_now, ema_10_now, ema_3_prev, ema_5_prev]:
            return f"{daily_status} | {fourh_status} | [1H] ❌", None
        else:
            status_5_10_1h = "🟩" if ema_5_now > ema_10_now else "🟥"
            status_3_5_1h = "🟩" if ema_3_now > ema_5_now else "🟥"
            oneh_status = f"[1H] 📊: {status_5_10_1h} {status_3_5_1h}"

            # 🚀 롱 조건 (3-5 역배열)
            rocket_condition = (
                ema_3_prev >= ema_5_prev and ema_3_now < ema_5_now
                and fourh_ok_long and (ema_5_now > ema_10_now)
            )
            # ⚡ 숏 조건 (3-5 정배열)
            short_condition = (
                ema_3_prev <= ema_5_prev and ema_3_now > ema_5_now
                and fourh_ok_short and (ema_5_now < ema_10_now)
            )

            if rocket_condition:
                signal = " 🚀🚀🚀(롱)"
                signal_type = "long"
            elif short_condition:
                signal = " ⚡⚡⚡(숏)"
                signal_type = "short"
            else:
                signal = ""
                signal_type = None

        return f"{daily_status} | {fourh_status} | {oneh_status}{signal}", signal_type

    except Exception as e:
        logging.error(f"{inst_id} EMA 상태 계산 실패: {e}")
        return "[1D/4H/1H] ❌", None

# === 당일 변동률 계산 ===
def calculate_daily_change(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1D", limit=2)
    if df is None or len(df) < 2:
        return None
    try:
        close_prev = df['c'].values[-2]
        close_now = df['c'].values[-1]
        change_rate = (close_now - close_prev) / close_prev * 100
        return change_rate
    except Exception as e:
        logging.error(f"{inst_id} 일일 변동률 계산 실패: {e}")
        return None

# === 메시지 발송 스케줄 ===
def check_and_send():
    coin_list = ["BTC-USDT", "ETH-USDT", "XRP-USDT"]  # 예시 코인
    for coin in coin_list:
        status_line, signal_type = get_ema_status_line(coin)
        daily_change = calculate_daily_change(coin)
        if daily_change is None:
            continue
        if daily_change > 0 and signal_type is not None:
            message = f"{coin} | 변동률: {daily_change:.2f}%\n{status_line}"
            send_telegram_message(message)

def run_schedule():
    schedule.every(1).hours.do(check_and_send)
    while True:
        schedule.run_pending()
        time.sleep(1)

# 스레드로 스케줄 실행
threading.Thread(target=run_schedule, daemon=True).start()

# FastAPI 실행용
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
