from fastapi import FastAPI
import telepot
import schedule
import time
import requests
import threading
import uvicorn
import logging
import pandas as pd
import random

app = FastAPI()

telegram_bot_token = "8170040373:AAFaEM789kB8aemN69BWwSjZ74HEVOQXP5s"
telegram_user_id = 6596886700
bot = telepot.Bot(telegram_bot_token)

logging.basicConfig(level=logging.INFO)



# ==== 텔레그램 메시지 전송 함수 (재시도 최대 10회) ====
def send_telegram_message(message):
    for retry_count in range(10):
        try:
            bot.sendMessage(chat_id=telegram_user_id, text=message, parse_mode="Markdown")
            logging.info("텔레그램 메시지 전송 성공")
            return
        except Exception as e:
            logging.error(f"텔레그램 전송 실패 {retry_count+1}/10: {e}")
            time.sleep(5)
    logging.error("텔레그램 전송 실패: 최대 재시도 횟수 초과")

# ==== API 호출 재시도 함수 (최대 10회) ====
def retry_request(func, *args, **kwargs):
    for attempt in range(10):
        try:
            res = func(*args, **kwargs)
            if hasattr(res, 'status_code') and res.status_code == 429:
                logging.warning("429 Too Many Requests, 1초 대기 후 재시도")
                time.sleep(1)
                continue
            return res
        except Exception as e:
            logging.error(f"API 호출 실패 {attempt+1}/10: {e}")
            time.sleep(5)
    return None

# ==== EMA 계산 함수 ====
def calculate_ema(close_list, period):
    if len(close_list) < period:
        return None
    s = pd.Series(close_list)
    return s.ewm(span=period, adjust=False).mean().iloc[-1]

def get_ema_with_retry(close_list, period):
    for _ in range(5):
        ema = calculate_ema(close_list, period)
        if ema is not None:
            return ema
        time.sleep(0.5)
    return None

# ==== OKX에서 SWAP 심볼 리스트 가져오기 ====
def get_all_okx_swap_symbols():
    url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
    res = retry_request(requests.get, url)
    if res is None:
        return []
    try:
        data = res.json().get("data", [])
        return [item["instId"] for item in data if "USDT" in item["instId"]]
    except Exception as e:
        logging.error(f"심볼 리스트 파싱 실패: {e}")
        return []

# ==== OKX OHLCV 데이터 요청 ====
def get_ohlcv_okx(instId, bar='1H', limit=200):
    url = f"https://www.okx.com/api/v5/market/candles?instId={instId}&bar={bar}&limit={limit}"
    res = retry_request(requests.get, url)
    if res is None:
        return None
    try:
        df = pd.DataFrame(res.json()['data'], columns=['ts','o','h','l','c','vol','volCcy','volCcyQuote','confirm'])
        df = df.astype({'o':float, 'h':float, 'l':float, 'c':float, 'vol':float, 'volCcyQuote':float})
        df = df.iloc[::-1].reset_index(drop=True)  # 시간순 정렬 (오래된 순)
        return df
    except Exception as e:
        logging.error(f"OHLCV 파싱 실패 {instId}: {e}")
        return None

# ==== EMA 강세 조건 ====
def is_ema_bullish(df):
    close = df['c'].values
    ema20 = get_ema_with_retry(close, 20)
    ema50 = get_ema_with_retry(close, 50)
    ema200 = get_ema_with_retry(close, 200)
    if None in [ema20, ema50, ema200]:
        return False
    return (ema20 > ema50 > ema200)

# ==== 1H, 4H 모두 EMA 강세인 심볼 필터링 ====
def filter_bullish_symbols(symbols):
    bullish = []
    for s in symbols:
        df1h = get_ohlcv_okx(s, '1H')
        df4h = get_ohlcv_okx(s, '4H')
        if df1h is None or df4h is None:
            continue
        if is_ema_bullish(df1h) and is_ema_bullish(df4h):
            bullish.append(s)
        time.sleep(random.uniform(0.2, 0.5))
    return bullish

# ==== 1시간 거래대금 합산 ====
def get_1h_volume(instId):
    df = get_ohlcv_okx(instId, bar='1H', limit=24)
    if df is None:
        return 0
    return df['volCcyQuote'].sum()

# ==== 일일 변동률 계산 ====
def get_daily_change(instId):
    df = get_ohlcv_okx(instId, bar='1D', limit=2)
    if df is None or len(df) < 2:
        return None
    try:
        open_price = df.iloc[-1]['o']
        close_price = df.iloc[-1]['c']
        change = (close_price - open_price) / open_price * 100
        return round(change, 2)
    except Exception as e:
        logging.error(f"변동률 계산 실패 {instId}: {e}")
        return None

# ==== 거래대금 '억' 단위 문자열 변환 ====
def format_volume(volume):
    try:
        eok = int(volume // 100_000)
        return f"{eok}억"
    except:
        return "N/A"

# ==== BTC EMA 상태 텍스트 생성 ====
def get_btc_ema_status():
    btc = "BTC-USDT-SWAP"
    timeframes = ['1D','4H','1H','15m']
    texts = []
    for tf in timeframes:
        df = get_ohlcv_okx(btc, bar=tf)
        if df is None:
            texts.append(f"[{tf}] EMA: 불러오기 실패 ❌")
            continue
        close = df['c'].values
        ema10 = get_ema_with_retry(close, 10)
        ema20 = get_ema_with_retry(close, 20)
        ema50 = get_ema_with_retry(close, 50)
        ema200 = get_ema_with_retry(close, 200)
        if None in [ema10, ema20, ema50, ema200]:
            texts.append(f"[{tf}] EMA: 데이터 부족 ❌")
            continue
        status = (
            ("✅" if ema10 > ema20 else "❌") +
            ("✅" if ema20 > ema50 else "❌") +
            ("✅" if ema50 > ema200 else "❌")
        )
        texts.append(f"[{tf}] EMA: {status}")
        time.sleep(random.uniform(0.2, 0.4))
    return "\n".join(texts)

# ==== 변동률에 따른 이모지 부착 ====
def format_change_emoji(change):
    if change is None:
        return "(N/A)"
    if change >= 5:
        return f"🚀🚀🚀 (+{change}%)"
    elif change > 0:
        return f"🟢 (+{change}%)"
    else:
        return f"🔴 ({change}%)"

# ==== 메인 작업 함수 ====
def job():
    logging.info("작업 시작: OKX 심볼 EMA 필터링 및 텔레그램 전송")
    all_symbols = get_all_okx_swap_symbols()
    if not all_symbols:
        send_telegram_message("❌ OKX 심볼 리스트 불러오기 실패")
        return

    bullish_symbols = filter_bullish_symbols(all_symbols)
    if not bullish_symbols:
        send_telegram_message("⚠️ EMA 강세 조건을 만족하는 심볼이 없습니다.")
        return

    volume_dict = {}
    for sym in bullish_symbols:
        vol = get_1h_volume(sym)
        volume_dict[sym] = vol
        time.sleep(random.uniform(0.2, 0.5))

    # 거래대금 내림차순 정렬 상위 10개
    top10 = sorted(volume_dict.items(), key=lambda x: x[1], reverse=True)[:10]

    btc_ema_status = get_btc_ema_status()
    btc_change = get_daily_change("BTC-USDT-SWAP")
    btc_change_str = format_change_emoji(btc_change)
    btc_volume = get_1h_volume("BTC-USDT-SWAP")
    btc_volume_str = format_volume(btc_volume)

    msg = f"📊 OKX 스왑 강세 코인 (EMA 20>50>200, 1H/4H)\n\n"
    msg += f"BTC 상태:\n{btc_ema_status}\n변동률: {btc_change_str}\n거래대금: {btc_volume_str}\n\n"
    msg += "상위 10종목 (1시간 거래대금 기준):\n"
    for sym, vol in top10:
        change = get_daily_change(sym)
        change_str = format_change_emoji(change)
        vol_str = format_volume(vol)
        msg += f"{sym}: 거래대금 {vol_str}, 변동률 {change_str}\n"
        time.sleep(random.uniform(0.1, 0.3))

    send_telegram_message(msg)
    logging.info("작업 완료: 메시지 전송됨")

# ==== 스케줄러 스레드 시작 ====
def run_schedule():
    schedule.every().hour.at(":00").do(job)
    while True:
        schedule.run_pending()
        time.sleep(1)

@app.on_event("startup")
def startup_event():
    threading.Thread(target=run_schedule, daemon=True).start()
    logging.info("스케줄러 시작")

# ==== FastAPI 기본 루트 ====
@app.get("/")
def read_root():
    return {"message": "OKX EMA Bot is running"}

# ==== uvicorn 실행 함수 ====
if __name__ == "__main__":
    uvicorn.run("okx_ema_bot:app", host="0.0.0.0", port=8000, reload=False)
