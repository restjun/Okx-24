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

def get_ohlcv_okx(instId, bar='1D', limit=200):
    logging.info(f"📊 {instId} - {bar} 캔들 데이터 요청 중...")
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

def is_ema_bullish_5_20_50(df):
    close = df['c'].values
    ema_5 = get_ema_with_retry(close, 5)
    ema_20 = get_ema_with_retry(close, 20)
    ema_50 = get_ema_with_retry(close, 50)
    if None in [ema_5, ema_20, ema_50]:
        return False
    return ema_5 > ema_20 > ema_50

def filter_by_daily_ema_alignment(inst_ids):
    bullish_ids = []
    for inst_id in inst_ids:
        df_1d = get_ohlcv_okx(inst_id, bar='1D', limit=200)
        if df_1d is None:
            continue
        if is_ema_bullish_5_20_50(df_1d):
            bullish_ids.append(inst_id)
        time.sleep(random.uniform(0.2, 0.4))
    return bullish_ids

def calculate_1d_volume(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1D", limit=1)
    if df is None or len(df) < 1:
        return 0
    return df["volCcyQuote"].sum()

def calculate_daily_change(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1D", limit=2)
    if df is None or len(df) < 2:
        return None
    try:
        today_close = df.iloc[-1]['c']
        yesterday_close = df.iloc[-2]['c']
        change = ((today_close - yesterday_close) / yesterday_close) * 100
        return round(change, 2)
    except Exception as e:
        logging.error(f"{inst_id} 상승률 계산 오류: {e}")
        return None

def format_volume_in_eok(volume):
    try:
        return f"{int(volume // 100_000_000)}"
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

def get_ema_status_text(df, timeframe="1D"):
    close = df['c'].values
    ema_5 = get_ema_with_retry(close, 5)
    ema_10 = get_ema_with_retry(close, 10)
    ema_20 = get_ema_with_retry(close, 20)
    ema_50 = get_ema_with_retry(close, 50)

    if None in [ema_5, ema_10, ema_20, ema_50]:
        return f"[{timeframe}] EMA 📊: ❌ 데이터 부족"

    def check(cond): return "[✅] " if cond else "[❌] "

    return (
        f"[{timeframe}] EMA 📊:  "
        f"{check(ema_5 > ema_20)}"
        f"{check(ema_20 > ema_50)} "
        f" [[5-10]: {check(ema_5 > ema_10)}]"
    )

def get_btc_ema_status_1d_only():
    btc_id = "BTC-USDT-SWAP"
    df = get_ohlcv_okx(btc_id, bar='1D', limit=200)
    if df is not None:
        return get_ema_status_text(df, timeframe="1D")
    return "[1D] EMA 📊:  ❌ 불러오기 실패"

def send_ranked_volume_message(bullish_ids):
    volume_data = {}

    btc_id = "BTC-USDT-SWAP"
    btc_ema_status = get_btc_ema_status_1d_only()
    btc_change = calculate_daily_change(btc_id)
    btc_change_str = format_change_with_emoji(btc_change)
    btc_volume = calculate_1d_volume(btc_id)
    btc_volume_str = format_volume_in_eok(btc_volume)

    for inst_id in bullish_ids:
        vol_1d = calculate_1d_volume(inst_id)
        volume_data[inst_id] = vol_1d
        time.sleep(random.uniform(0.2, 0.4))

    top_3_ids = sorted(volume_data.items(), key=lambda x: x[1], reverse=True)[:3]

    message_lines = [
        "📅 *[정배열 5/20/50] + [일봉 거래대금 Top3]*",
        "━━━━━━━━━━━━━━━━━━━",
        f"💰 *BTC* {btc_change_str} / 거래대금: {btc_volume_str}",
        f"    {btc_ema_status}",
        "━━━━━━━━━━━━━━━━━━━"
    ]

    rank = 1
    for inst_id, vol in top_3_ids:
        try:
            change = calculate_daily_change(inst_id)
            df_1d = get_ohlcv_okx(inst_id, bar="1D", limit=200)
            if change is None or df_1d is None:
                continue

            ema_status = get_ema_status_text(df_1d, timeframe="1D")
            name = inst_id.replace("-USDT-SWAP", "")
            vol_text = format_volume_in_eok(vol)
            change_str = format_change_with_emoji(change)

            message_lines.append(
                f"*{rank}. {name}* {change_str} | 💰 {vol_text}\n   {ema_status}"
            )
            message_lines.append("─────")
            rank += 1

        except Exception as e:
            logging.error(f"{inst_id} 메시지 생성 오류: {e}")
            continue

    if rank == 1:
        message_lines.append("⚠️ 조건을 만족하는 종목이 없습니다.")
    else:
        message_lines.append("━━━━━━━━━━━━━━━━━━━")
        message_lines.append("📡 *큰 파동을 보자. 일봉이 진리.*")

    send_telegram_message("\n".join(message_lines))

def main():
    logging.info("📥 전체 종목 기준 *일봉 정배열 + 거래대금* 분석 시작")
    all_ids = get_all_okx_swap_symbols()
    bullish_ids = filter_by_daily_ema_alignment(all_ids)
    if not bullish_ids:
        send_telegram_message("🔴 일봉 기준 정배열 종목 없음.")
        return
    send_ranked_volume_message(bullish_ids)

@app.on_event("startup")
def start_scheduler():
    schedule.every(3).minutes.do(main)
    threading.Thread(target=run_scheduler, daemon=True).start()

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
