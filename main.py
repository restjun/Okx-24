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
    for retry_count in range(10):
        try:
            bot.sendMessage(chat_id=telegram_user_id, text=message, parse_mode="Markdown")
            logging.info("텔레그램 메시지 전송 성공")
            return
        except Exception as e:
            logging.error(f"텔레그램 메시지 전송 실패 (재시도 {retry_count + 1}/10): {e}")
            time.sleep(5)
    logging.error("텔레그램 메시지 전송 실패: 최대 재시도 횟수 초과")

def retry_request(func, *args, **kwargs):
    for attempt in range(10):
        try:
            result = func(*args, **kwargs)
            if hasattr(result, 'status_code') and result.status_code == 429:
                time.sleep(1)
                continue
            return result
        except Exception as e:
            logging.error(f"API 호출 실패 (재시도 {attempt + 1}/10): {e}")
            time.sleep(5)
    return None

def calculate_ema(close, period):
    if len(close) < period:
        return None
    return pd.Series(close).ewm(span=period, adjust=False).mean().iloc[-1]

def calculate_rsi(series, period=14):
    if len(series) < period + 1:
        return None
    delta = series.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1]

def get_all_okx_swap_symbols():
    url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
    response = retry_request(requests.get, url)
    if response is None:
        return []
    data = response.json().get("data", [])
    return [item["instId"] for item in data if "USDT" in item["instId"]]

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
        df['volCcyQuote'] = df['volCcyQuote'].astype(float)
        return df.iloc[::-1]  # 시간순 정렬
    except Exception as e:
        logging.error(f"{instId} OHLCV 파싱 실패: {e}")
        return None

def format_volume_in_eok(volume):
    try:
        eok = int(volume // 1_000_000)
        return str(eok) if eok >= 300 else None
    except:
        return None

def get_ema_status_and_rsi(inst_id):
    df = get_ohlcv_okx(inst_id, bar='1H', limit=200)
    if df is None or len(df) < 50:
        return None

    close = df['c']
    ema_10 = calculate_ema(close, 10)
    ema_20 = calculate_ema(close, 20)
    ema_50 = calculate_ema(close, 50)
    ema_200 = calculate_ema(close, 200)
    rsi_14 = calculate_rsi(close, 14)

    if None in [ema_10, ema_20, ema_50, ema_200, rsi_14]:
        return None

    status_10_20 = "🟩" if ema_10 > ema_20 else "🟥"
    status_20_50 = "🟩" if ema_20 > ema_50 else "🟥"
    status_50_200 = "🟩" if ema_50 > ema_200 else "🟥"

    return f"{status_10_20} {status_20_50} {status_50_200} | RSI14: {rsi_14:.1f}"

def calculate_1h_volume(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=24)
    if df is None or len(df) == 0:
        return 0
    return df["volCcyQuote"].sum()

def calculate_daily_change(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=48)
    if df is None or len(df) < 24:
        return None
    try:
        df['datetime'] = pd.to_datetime(df['ts'], unit='ms')
        df['datetime_kst'] = df['datetime'] + pd.Timedelta(hours=9)
        df.set_index('datetime_kst', inplace=True)
        daily = df.resample('1D', offset='9h').agg({'o':'first','h':'max','l':'min','c':'last','vol':'sum'}).dropna()
        if len(daily) < 2:
            return None
        today_close = daily['c'][-1]
        yesterday_close = daily['c'][-2]
        return round(((today_close - yesterday_close) / yesterday_close) * 100, 2)
    except Exception as e:
        logging.error(f"{inst_id} 상승률 계산 오류: {e}")
        return None

def create_message():
    symbols = get_all_okx_swap_symbols()
    if not symbols:
        return "코인 정보를 불러오지 못했습니다."

    bullish_coins = []
    total = len(symbols)

    for sym in symbols:
        volume = calculate_1h_volume(sym)
        if volume < 300_000_000_000:  # 300억 이상 필터링 (OKX는 USDT 마켓 볼륨, 단위 주의)
            continue

        ema_rsi = get_ema_status_and_rsi(sym)
        if ema_rsi is None:
            continue

        daily_change = calculate_daily_change(sym)
        if daily_change is None:
            daily_change_str = "(N/A)"
        elif daily_change >= 5:
            daily_change_str = f"🚨🚨🚨 (+{daily_change:.2f}%)"
        elif daily_change > 0:
            daily_change_str = f"🟢 (+{daily_change:.2f}%)"
        else:
            daily_change_str = f"🔴 ({daily_change:.2f}%)"

        bullish_coins.append((sym, ema_rsi, daily_change_str, volume))

    bullish_coins.sort(key=lambda x: x[3], reverse=True)  # 거래대금 내림차순 정렬

    if not bullish_coins:
        return "조건에 맞는 코인이 없습니다."

    message = f"📊 *거래대금 300억 이상 코인 중 EMA(10>20>50>200) 및 RSI14 현황*\n\n"
    for sym, ema_rsi, change, vol in bullish_coins[:20]:
        vol_eok = int(vol // 1_000_000)
        message += f"{sym} | {ema_rsi} | {change} | 거래대금: {vol_eok}억\n"

    return message

def job():
    logging.info("알림 시작")
    message = create_message()
    send_telegram_message(message)
    logging.info("알림 완료")

def run_schedule():
    schedule.every(1).hours.do(job)
    while True:
        schedule.run_pending()
        time.sleep(1)

@app.get("/")
async def root():
    return {"message": "OKX EMA RSI 알림 봇 실행 중"}

if __name__ == "__main__":
    thread = threading.Thread(target=run_schedule)
    thread.start()
    uvicorn.run(app, host="0.0.0.0", port=8000)
