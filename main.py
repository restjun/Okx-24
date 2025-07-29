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
                logging.warning("⚠️ 429 Too Many Requests - 대기 후 재시도")
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
    close_series = pd.Series(close)
    return close_series.ewm(span=period, adjust=False).mean().iloc[-1]

def get_ema_with_retry(close, period):
    for _ in range(5):
        result = calculate_ema(close, period)
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

def is_ema_bullish_5_20_50_200(df):
    close = df['c'].values
    ema_5 = get_ema_with_retry(close, 5)
    ema_20 = get_ema_with_retry(close, 20)
    ema_50 = get_ema_with_retry(close, 50)
    ema_200 = get_ema_with_retry(close, 200)
    if None in [ema_5, ema_20, ema_50, ema_200]:
        return False
    return ema_5 > ema_20 > ema_50 > ema_200

def filter_by_all_ema_alignment(inst_ids):
    bullish_ids = []
    for inst_id in inst_ids:
        df_1h = get_ohlcv_okx(inst_id, bar='1H', limit=200)
        df_4h = get_ohlcv_okx(inst_id, bar='4H', limit=200)
        df_1d = get_ohlcv_okx(inst_id, bar='1D', limit=200)
        if None in [df_1h, df_4h, df_1d]:
            continue
        if all([
            is_ema_bullish_5_20_50_200(df_1h),
            is_ema_bullish_5_20_50_200(df_4h),
            is_ema_bullish_5_20_50_200(df_1d)
        ]):
            bullish_ids.append(inst_id)
        time.sleep(random.uniform(0.2, 0.4))
    return bullish_ids

def calculate_1h_volume(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=24)
    if df is None or len(df) < 1:
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
        daily = df.resample('1D', offset='9h').agg({
            'o': 'first',
            'h': 'max',
            'l': 'min',
            'c': 'last',
            'vol': 'sum'
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
        return f"{int(volume // 100_000)}"
    except:
        return "N/A"

def format_change_with_emoji(change):
    if change is None:
        return "(N/A)"
    if change >= 5:
        return f"🎯🎯🎯 (+{change:.2f}%)"
    elif change > 0:
        return f"🟢 (+{change:.2f}%)"
    else:
        return f"🔴 ({change:.2f}%)"

def get_ema_status_text(df, timeframe="1H"):
    close = df['c'].values
    ema_5 = get_ema_with_retry(close, 5)
    ema_10 = get_ema_with_retry(close, 10)
    ema_20 = get_ema_with_retry(close, 20)
    ema_50 = get_ema_with_retry(close, 50)
    if None in [ema_5, ema_10, ema_20, ema_50]:
        return f"[{timeframe}] EMA 📊: ❌ 데이터 부족"
    check = lambda cond: "[✅] " if cond else "[❌] "
    return (
        f"[{timeframe}] EMA 📊:  "
        f"{check(ema_5 > ema_20)}"
        f"{check(ema_20 > ema_50)} "
        f"[[5-10]: {check(ema_5 > ema_10)}]"
    )

def get_ema_status_all_timeframes(inst_id):
    status_list = []
    for tf in ["1H", "4H", "1D"]:
        df = get_ohlcv_okx(inst_id, bar=tf, limit=200)
        status = get_ema_status_text(df, tf) if df is not None else f"[{tf}] EMA 📊: ❌"
        status_list.append(status)
        time.sleep(0.2)
    return "\n".join(status_list)

def get_btc_ema_status_all():
    return get_ema_status_all_timeframes("BTC-USDT-SWAP")

def send_ranked_volume_message(bullish_ids):
    volume_data = {}
    btc_id = "BTC-USDT-SWAP"
    btc_change = calculate_daily_change(btc_id)
    btc_volume = calculate_1h_volume(btc_id)
    btc_status = get_btc_ema_status_all()

    for inst_id in bullish_ids:
        df = get_ohlcv_okx(inst_id, bar="1D", limit=2)
        if df is not None:
            volume_data[inst_id] = df['volCcyQuote'].sum()
        time.sleep(0.2)

    top_3 = sorted(volume_data.items(), key=lambda x: x[1], reverse=True)[:3]
    top_3_ids = [x[0] for x in top_3]

    final_list = []
    for inst_id in top_3_ids:
        vol = calculate_1h_volume(inst_id)
        if vol >= 100_000:
            final_list.append((inst_id, vol))
        time.sleep(0.2)

    message_lines = [
        "📅 *[정배열 ] | Top 거래대금 3종목*",
        "━━━━━━━━━━━━━━━━━━━",
        f"💰 *BTC* {format_change_with_emoji(btc_change)} / 거래대금: {format_volume_in_eok(btc_volume)}",
        f"{btc_status}",
        "━━━━━━━━━━━━━━━━━━━"
    ]

    if not final_list:
        message_lines.append("⚠️ 조건을 만족하는 종목이 없습니다.")
    else:
        for i, (inst_id, vol) in enumerate(final_list, 1):
            name = inst_id.replace("-USDT-SWAP", "")
            change = calculate_daily_change(inst_id)
            ema_status = get_ema_status_all_timeframes(inst_id)
            message_lines.append(
                f"*{i}. {name}* {format_change_with_emoji(change)} | 💰 {format_volume_in_eok(vol)}\n{ema_status}"
            )
            message_lines.append("─────")

    message_lines.append("━━━━━━━━━━━━━━━━━━━")
    message_lines.append("📡 *작은 파동보다 큰 파동을 보자*")
    send_telegram_message("\n".join(message_lines))

def main():
    logging.info("📥 전체 종목 기준 1D + 4H + 1H 정배열 필터 중...")
    all_ids = get_all_okx_swap_symbols()
    bullish_ids = filter_by_all_ema_alignment(all_ids)
    if not bullish_ids:
        send_telegram_message("🔴 정배열 만족 종목 없음.")
    else:
        send_ranked_volume_message(bullish_ids)

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
