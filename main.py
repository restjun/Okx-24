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
            bot.sendMessage(chat_id=telegram_user_id, text=message, parse_mode="Markdown")
            logging.info("텔레그램 메시지 전송 성공: %s", message)
            return
        except Exception as e:
            logging.error("텔레그램 메시지 전송 실패 (재시도 %d/10): %s", retry_count, str(e))
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
            logging.error(f"API 호출 실패 (재시도 {attempt+1}/10): {str(e)}")
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

def calculate_rsi(series, period=14):
    if len(series) < period + 1:
        return None
    delta = pd.Series(series).diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(window=period).mean().iloc[-1]
    avg_loss = loss.rolling(window=period).mean().iloc[-1]
    if avg_loss == 0:
        return 100
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return round(rsi, 2)

def get_rsi_with_retry(close, period=14):
    for _ in range(5):
        result = calculate_rsi(close, period)
        if result is not None:
            return result
        time.sleep(0.5)
    return None

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
        df['vol'] = df['vol'].astype(float)
        df['volCcyQuote'] = df['volCcyQuote'].astype(float)
        return df.iloc[::-1]
    except Exception as e:
        logging.error(f"{instId} OHLCV 파싱 실패: {e}")
        return None

def get_ema_bullish_status(inst_id):
    try:
        df_1h = get_ohlcv_okx(inst_id, bar='1H', limit=300)
        df_4h = get_ohlcv_okx(inst_id, bar='4H', limit=300)
        if df_1h is None or df_4h is None:
            return None

        close_1h = df_1h['c'].values
        close_4h = df_4h['c'].values

        ema_1h_10 = get_ema_with_retry(close_1h, 10)
        ema_1h_20 = get_ema_with_retry(close_1h, 20)
        ema_1h_50 = get_ema_with_retry(close_1h, 50)

        ema_4h_10 = get_ema_with_retry(close_4h, 10)
        ema_4h_20 = get_ema_with_retry(close_4h, 20)
        ema_4h_50 = get_ema_with_retry(close_4h, 50)

        if None in [ema_1h_10, ema_1h_20, ema_1h_50, ema_4h_10, ema_4h_20, ema_4h_50]:
            return None

        return (ema_1h_10 > ema_1h_20 > ema_1h_50) and \
               (ema_4h_10 > ema_4h_20 > ema_4h_50)
    except Exception as e:
        logging.error(f"{inst_id} EMA 상태 계산 실패: {e}")
        return None

def calculate_daily_change(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=48)
    if df is None or len(df) < 24:
        return None
    try:
        df['datetime'] = pd.to_datetime(df['ts'], unit='ms')
        df['datetime_kst'] = df['datetime'] + pd.Timedelta(hours=9)
        df.set_index('datetime_kst', inplace=True)
        daily = df.resample('1D', offset='9h').agg({
            'o': 'first', 'h': 'max', 'l': 'min', 'c': 'last', 'vol': 'sum'
        }).dropna().sort_index(ascending=False).reset_index()
        if len(daily) < 2:
            return None
        today_close = daily.loc[0, 'c']
        yesterday_close = daily.loc[1, 'c']
        return round(((today_close - yesterday_close) / yesterday_close) * 100, 2)
    except Exception as e:
        logging.error(f"{inst_id} 상승률 계산 오류: {e}")
        return None

def format_volume_in_eok(volume):
    try:
        eok = int(volume // 1_000_000)
        return str(eok) if eok >= 300 else None
    except:
        return None

def format_change_with_emoji(change):
    if change is None:
        return "(N/A)"
    if change >= 5:
        return f"🚨🚨🚨 (+{change:.2f}%)"
    elif change > 0:
        return f"🟢 (+{change:.2f}%)"
    else:
        return f"🔴 ({change:.2f}%)"

def get_ema_status_text(df, timeframe="1H"):
    close = df['c'].values
    ema_10 = get_ema_with_retry(close, 10)
    ema_20 = get_ema_with_retry(close, 20)
    ema_50 = get_ema_with_retry(close, 50)
    ema_200 = get_ema_with_retry(close, 200)

    rsi_14 = get_rsi_with_retry(close, 14)

    def check(cond):
        if cond is None:
            return "[❌]"
        return "[🟩]" if cond else "[🟥]"

    def safe_compare(a, b):
        if a is None or b is None:
            return None
        return a > b

    status_parts = [
        check(safe_compare(ema_10, ema_20)),
        check(safe_compare(ema_20, ema_50)),
        check(safe_compare(ema_50, ema_200))
    ]
    rsi_text = f" RSI(14): {rsi_14}" if rsi_14 is not None else " RSI(14): N/A"
    return f"[{timeframe}] EMA 📊: {' '.join(status_parts)}{rsi_text}"

def get_all_timeframe_ema_status(inst_id):
    timeframes = {'1D': 250, '4H': 300, '1H': 300, '15m': 300}
    status_lines = []
    for tf, limit in timeframes.items():
        df = get_ohlcv_okx(inst_id, bar=tf, limit=limit)
        if df is not None:
            status = get_ema_status_text(df, timeframe=tf)
        else:
            status = f"[{tf}] 📊: ❌ 불러오기 실패"
        status_lines.append(status)
        time.sleep(0.2)
    return "\n".join(status_lines)

def calculate_1h_volume(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=24)
    if df is None or len(df) < 1:
        return 0
    return df["volCcyQuote"].sum()

def send_ranked_volume_message(top_bullish, total_count, bullish_count):
    bearish_count = total_count - bullish_count

    message_lines = [
        f"📊 *전체 조회 코인 수:* {total_count}개",
        f"🟢 *EMA 정배열:* {bullish_count}개",
        f"🔴 *EMA 역배열:* {bearish_count}개",
        "",
        "*🟢 정배열 코인 리스트 🟢*",
        "인증: EMA10>20>50 1H,4H 정배열 + RSI(14) 확인\n"
    ]

    for rank, (inst_id, change, vol, ema_status) in enumerate(top_bullish, start=1):
        vol_eok = format_volume_in_eok(vol)
        if vol_eok is None:
            continue
        change_str = format_change_with_emoji(change)
        message_lines.append(f"{rank}. {inst_id} 거래대금:{vol_eok}억 {change_str}\n{ema_status}\n")

    message = "\n".join(message_lines)
    send_telegram_message(message)

def analyze_okx_ema_and_rsi():
    symbols = get_all_okx_swap_symbols()
    total_count = len(symbols)
    if total_count == 0:
        logging.error("심볼을 불러오지 못했습니다.")
        return

    bullish_coins = []

    for inst_id in symbols:
        try:
            vol_1h = calculate_1h_volume(inst_id)
            if vol_1h < 300_000_000_000:  # 300억 미만 필터링 (볼륨 단위 확인 필요)
                continue

            change = calculate_daily_change(inst_id)
            if change is None:
                continue

            # EMA 정배열 + RSI 체크 (1H, 4H)
            bullish = get_ema_bullish_status(inst_id)
            if bullish:
                # 여러 시간대 EMA+RSI 상태도 포함해 메시지 생성
                ema_rsi_status = get_all_timeframe_ema_status(inst_id)
                bullish_coins.append((inst_id, change, vol_1h, ema_rsi_status))

        except Exception as e:
            logging.error(f"{inst_id} 분석 중 오류 발생: {e}")

    bullish_count = len(bullish_coins)
    # 거래대금으로 내림차순 정렬 후 상위 20개만 선택
    top_bullish = sorted(bullish_coins, key=lambda x: x[2], reverse=True)[:20]

    send_ranked_volume_message(top_bullish, total_count, bullish_count)

def job():
    logging.info("OKX EMA & RSI 분석 시작")
    analyze_okx_ema_and_rsi()
    logging.info("OKX EMA & RSI 분석 완료")

@app.get("/")
def root():
    return {"message": "OKX EMA & RSI 분석 API"}

def run_schedule():
    schedule.every(20).minutes.do(job)
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    threading.Thread(target=run_schedule, daemon=True).start()
    uvicorn.run(app, host="0.0.0.0", port=8000)

