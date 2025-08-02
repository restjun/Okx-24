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

def preload_ohlcv_data(inst_id):
    bars = {'15m': 300, '1H': 300, '4H': 300, '1D': 250}
    ohlcv_data = {}
    for bar, limit in bars.items():
        df = get_ohlcv_okx(inst_id, bar=bar, limit=limit)
        if df is not None:
            ohlcv_data[bar] = df
        time.sleep(0.1)
    return ohlcv_data

def get_combined_ema_status_from_df(df_dict):
    df_1h = df_dict.get('1H')
    if df_1h is None:
        return None
    close_1h = df_1h['c'].values
    ema_5 = get_ema_with_retry(close_1h, 5)
    ema_20 = get_ema_with_retry(close_1h, 20)
    ema_50 = get_ema_with_retry(close_1h, 50)
    if None in [ema_5, ema_20, ema_50]:
        return None
    return {
        "bullish": ema_5 > ema_20 > ema_50,
        "bearish": ema_5 < ema_20 < ema_50
    }

def calculate_daily_change_from_df(df_dict):
    df = df_dict.get('1H')
    if df is None or len(df) < 24:
        return None
    try:
        df['datetime'] = pd.to_datetime(df['ts'], unit='ms')
        df['datetime_kst'] = df['datetime'] + pd.Timedelta(hours=9)
        df.set_index('datetime_kst', inplace=True)
        daily = df.resample('1D', offset='9h').agg({
            'o': 'first', 'h': 'max', 'l': 'min', 'c': 'last', 'vol': 'sum'
        }).dropna()
        daily = daily.sort_index(ascending=False).reset_index()
        if len(daily) < 2:
            return None
        today_close = daily.loc[0, 'c']
        yesterday_close = daily.loc[1, 'c']
        return round(((today_close - yesterday_close) / yesterday_close) * 100, 2)
    except Exception as e:
        logging.error(f"상승률 계산 오류: {e}")
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
        return f"🚨🚨🚨 (+{change:.2f}%)"
    elif change > 0:
        return f"🟢 (+{change:.2f}%)"
    else:
        return f"🔴 ({change:.2f}%)"

def get_ema_status_text(df, timeframe="1H"):
    close = df['c'].values
    ema_1 = get_ema_with_retry(close, 1)
    ema_2 = get_ema_with_retry(close, 2)
    ema_5 = get_ema_with_retry(close, 5)
    ema_20 = get_ema_with_retry(close, 20)
    ema_50 = get_ema_with_retry(close, 50)
    ema_200 = get_ema_with_retry(close, 200)

    def check(cond):
        if cond is None:
            return "[❌]"
        return "[🟩]" if cond else "[🟥]"

    def safe_compare(a, b):
        if a is None or b is None:
            return None
        return a > b

    status_parts = [
        check(safe_compare(ema_5, ema_20)),
        check(safe_compare(ema_20, ema_50)),
        check(safe_compare(ema_50, ema_200))
    ]
    short_term_status = check(safe_compare(ema_1, ema_2))
    return f"[{timeframe}] EMA 📊: {' '.join(status_parts)}   [(🟩) : {short_term_status}]"

def get_all_timeframe_ema_status_from_df(df_dict):
    status_lines = []
    for tf, df in df_dict.items():
        status_lines.append(get_ema_status_text(df, timeframe=tf))
    return "\n".join(status_lines)

def main():
    logging.info("📥 전체 종목 기준 초기 OHLCV + EMA + 거래대금 분석 시작")
    all_ids = get_all_okx_swap_symbols()

    candidates = []
    for inst_id in all_ids:
        df_dict = preload_ohlcv_data(inst_id)
        if not df_dict:
            continue
        status = get_combined_ema_status_from_df(df_dict)
        if status is None:
            continue
        df_1d = df_dict.get('1D')
        if df_1d is None:
            continue
        vol_24h = df_1d['volCcyQuote'].sum()
        candidates.append((inst_id, vol_24h, status, df_dict))
        time.sleep(random.uniform(0.1, 0.3))

    sorted_by_volume = sorted(candidates, key=lambda x: x[1], reverse=True)
    top_bullish = [(id, vol, df) for id, vol, s, df in sorted_by_volume if s['bullish']][:1]
    top_bearish = next(((id, vol, df) for id, vol, s, df in sorted_by_volume if s['bearish']), None)

    send_ranked_volume_message_preloaded(top_bullish, top_bearish)

def send_ranked_volume_message_preloaded(top_bullish, top_bearish):
    btc_id = "BTC-USDT-SWAP"
    btc_data = preload_ohlcv_data(btc_id)

    btc_ema_status = get_all_timeframe_ema_status_from_df(btc_data)
    btc_change = calculate_daily_change_from_df(btc_data)
    btc_volume = btc_data.get('1H')['volCcyQuote'].sum() if '1H' in btc_data else 0

    message_lines = [
        "🎯 *코인지수 비트코인*",
        "━━━━━━━━━━━━━━━━━━━",
        f"💰 *BTC* {format_change_with_emoji(btc_change)} / 거래대금: ({format_volume_in_eok(btc_volume)})",
        f"{btc_ema_status}",
        "━━━━━━━━━━━━━━━━━━━"
    ]

    if top_bullish:
        message_lines += ["📈 *[정배열] + [거래대금 24시간 Top1]*", "━━━━━━━━━━━━━━━━━━━"]
        for i, (inst_id, _, df_dict) in enumerate(top_bullish, 1):
            name = inst_id.replace("-USDT-SWAP", "")
            change = calculate_daily_change_from_df(df_dict)
            ema_status = get_all_timeframe_ema_status_from_df(df_dict)
            vol_1h = df_dict.get('1H')['volCcyQuote'].sum()
            message_lines += [
                f"*{i}. {name}* {format_change_with_emoji(change)} | 💵 {format_volume_in_eok(vol_1h)}\n{ema_status}",
                "━━━━━━━━━━━━━━━━━━━"
            ]
    else:
        message_lines.append("⚠️ 정배열 조건을 만족하는 종목이 없습니다.")

    if top_bearish:
        inst_id, _, df_dict = top_bearish
        name = inst_id.replace("-USDT-SWAP", "")
        change = calculate_daily_change_from_df(df_dict)
        ema_status = get_all_timeframe_ema_status_from_df(df_dict)
        vol_1h = df_dict.get('1H')['volCcyQuote'].sum()
        message_lines += [
            "📉 *[역배열] + [거래대금 24시간 Top1]*",
            "━━━━━━━━━━━━━━━━━━━",
            f"*1. {name}* {format_change_with_emoji(change)} | 💵 {format_volume_in_eok(vol_1h)}\n{ema_status}",
            "━━━━━━━━━━━━━━━━━━━"
        ]
    else:
        message_lines.append("⚠️ 역배열 조건을 만족하는 종목이 없습니다.")

    message_lines += [
        "✅️ *1.10시간 이상 추세유지.*",
        "✅️ *2.직전고점을 돌파하거나 돌파전.*",
        "✅️ *3.거래대금 우선 / 패턴 / 추격금지*",
        "✅️ *4.기준봉손절/ 5-20-50*"
    ]

    send_telegram_message("\n".join(message_lines))

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

@app.on_event("startup")
def start_scheduler():
    schedule.every(3).minutes.do(main)
    threading.Thread(target=run_scheduler, daemon=True).start()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
