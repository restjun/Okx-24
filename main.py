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

# 이전 상위 10 거래대금 코인 저장
previous_top_vol_ids = []

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
        return str(eok) if eok >= 0 else None
    except:
        return None

def format_change_with_emoji(change):
    if change is None:
        return "(N/A)"
    if change >= 5:
        return f"🚨 (+{change:.2f}%)"
    elif change > 0:
        return f"🟢 (+{change:.2f}%)"
    else:
        return f"🔴 ({change:.2f}%)"

# 트레이딩뷰 RSI(Wilder 방식)
def calculate_rsi(close, period=5):
    close = pd.Series(close)
    delta = close.diff().dropna()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(window=period, min_periods=period).mean().iloc[period-1]
    avg_loss = loss.rolling(window=period, min_periods=period).mean().iloc[period-1]
    for i in range(period, len(gain)):
        avg_gain = (avg_gain * (period - 1) + gain.iloc[i]) / period
        avg_loss = (avg_loss * (period - 1) + loss.iloc[i]) / period
    if avg_loss == 0:
        return 100
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def get_ema_icon(close):
    ema_3 = get_ema_with_retry(close, 3)
    ema_5 = get_ema_with_retry(close, 5)
    if ema_3 is None or ema_5 is None:
        return "[❌]"
    return "[🟩]" if ema_3 > ema_5 else "[🟥]"

def get_all_timeframe_ema_status(inst_id):
    try:
        df_1d = get_ohlcv_okx(inst_id, bar="1D", limit=250)
        df_4h = get_ohlcv_okx(inst_id, bar="4H", limit=300)

        status_1d = get_ema_icon(df_1d['c'].astype(float).values) if df_1d is not None else "[❌]"

        if df_4h is not None:
            close_4h = df_4h['c'].astype(float).values
            status_4h = get_ema_icon(close_4h)
            rsi_5 = calculate_rsi(close_4h, period=5)
            rsi_text = f"RSI(5): {rsi_5:.2f}" if rsi_5 is not None else "RSI(5): N/A"
        else:
            status_4h = "[❌]"
            rsi_text = "RSI(5): N/A"

        return f"1D: {status_1d} | 4H: {status_4h} | {rsi_text}"

    except Exception as e:
        logging.error(f"{inst_id} 상태 표시 오류: {e}")
        return "❌ 상태 계산 실패"

def calculate_1h_volume(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=24)
    if df is None or len(df) < 1:
        return 0
    return df["volCcyQuote"].sum()

def main():
    global previous_top_vol_ids
    logging.info("📥 EMA 분석 시작")
    all_ids = get_all_okx_swap_symbols()
    total_count = len(all_ids)

    vol_list = []
    for inst_id in all_ids:
        df_24h = get_ohlcv_okx(inst_id, bar="1H", limit=24)
        if df_24h is None:
            continue
        vol_24h = df_24h['volCcyQuote'].sum()
        vol_list.append((inst_id, vol_24h))
        time.sleep(0.1)

    top_vol = sorted(vol_list, key=lambda x: x[1], reverse=True)[:10]
    current_top_vol_ids = [inst_id for inst_id, _ in top_vol]

    # 이전 랭킹과 비교해서 변동 없으면 메시지 전송 생략
    if previous_top_vol_ids == current_top_vol_ids:
        logging.info("🔄 상위 10 거래대금 변동 없음, 메시지 전송 생략")
        return
    previous_top_vol_ids = current_top_vol_ids

    # 메시지 작성
    message_lines = [
        f"📊 전체 조회 코인 수: {total_count}개",
        "━━━━━━━━━━━━━━━━━━━"
    ]

    btc_id = "BTC-USDT-SWAP"
    btc_ema_status = get_all_timeframe_ema_status(btc_id)
    btc_change = calculate_daily_change(btc_id)
    btc_volume = calculate_1h_volume(btc_id)
    btc_volume_str = format_volume_in_eok(btc_volume) or "🚫"

    message_lines += [
        "🎯 코인지수 비트코인",
        "━━━━━━━━━━━━━━━━━━━",
        f"💰 BTC {format_change_with_emoji(btc_change)} / 거래대금: ({btc_volume_str})",
        f"{btc_ema_status}",
        "━━━━━━━━━━━━━━━━━━━"
    ]

    if top_vol:
        message_lines.append("📈 거래대금 상위 10 코인")
        for i, (inst_id, vol) in enumerate(top_vol, 1):
            name = inst_id.replace("-USDT-SWAP", "")
            ema_status = get_all_timeframe_ema_status(inst_id)
            volume_str = format_volume_in_eok(vol) or "🚫"
            daily_change = calculate_daily_change(inst_id)
            message_lines += [
                f"*{i}. {name}* {format_change_with_emoji(daily_change)} / 거래대금: ({volume_str})\n{ema_status}",
                "━━━━━━━━━━━━━━━━━━━"
            ]
    else:
        message_lines.append("📉 거래대금 상위 코인 없음")

    send_telegram_message("\n".join(message_lines))

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








