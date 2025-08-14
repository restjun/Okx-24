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

# ===== 거래대금 계산 (24시간 단일 기준) =====
def calculate_24h_volume(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=24)
    if df is None or len(df) < 1:
        return 0
    return df["volCcyQuote"].sum()

# ===== EMA 정배열 여부 =====
def get_ema_bullish_status(inst_id):
    try:
        df_1d = get_ohlcv_okx(inst_id, bar='1D', limit=300)
        if df_1d is None:
            return False

        close_1d = df_1d['c'].values

        ema_5 = get_ema_with_retry(close_1d, 5)
        ema_10 = get_ema_with_retry(close_1d, 10)
        ema_15 = get_ema_with_retry(close_1d, 15)
        ema_20 = get_ema_with_retry(close_1d, 20)

        if None in [ema_5, ema_10, ema_15, ema_20]:
            return False

        return ema_5 > ema_10 > ema_15 > ema_20

    except Exception as e:
        logging.error(f"{inst_id} EMA 상태 계산 실패: {e}")
        return False

# ===== 코인 목록 조회 =====
def get_all_okx_swap_symbols():
    url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
    response = retry_request(requests.get, url)
    if response is None:
        return []
    data = response.json().get("data", [])
    return [item["instId"] for item in data if "USDT" in item["instId"]]

# ===== 일간 상승률 계산 =====
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

# ===== 메세지 전송 =====
def send_ranked_volume_message(top_bullish, total_count, bullish_count, volume_rank_map, all_volume_data):
    bearish_count = total_count - bullish_count
    bullish_ratio = bullish_count / total_count if total_count > 0 else 0

    if bullish_ratio >= 0.7:
        market_status = "📈 장이 좋음 (강세장)"
    elif bullish_ratio >= 0.4:
        market_status = "🔶 장 보통 (횡보장)"
    else:
        market_status = "📉 장이 안좋음 (약세장)"

    message_lines = [
        f"🟢 EMA 정배열: {bullish_count}개",
        f"🔴 EMA 역배열: {bearish_count}개",
        f"💡 시장 상태: {market_status}",
        "━━━━━━━━━━━━━━━━━━━",
    ]

    if top_bullish:
        message_lines.append("📈 1H 24시간 거래대금 순위 10위 내 정배열")
        for i, (inst_id, vol, change) in enumerate(top_bullish, 1):
            name = inst_id.replace("-USDT-SWAP", "")
            message_lines.append(f"{i}. {name} / 상승률: {change:.2f}% / 거래대금: {int(vol//1_000_000)}M")
        message_lines.append("━━━━━━━━━━━━━━━━━━━")
    else:
        message_lines.append("📉 조건 만족 종목이 없습니다.")

    send_telegram_message("\n".join(message_lines))

# ===== 메인 분석 함수 =====
def main():
    logging.info("📥 EMA 분석 시작")
    all_ids = get_all_okx_swap_symbols()
    total_count = len(all_ids)
    bullish_count_only = 0
    bullish_list = []

    # ===== 거래대금 계산 (24시간 기준) =====
    volume_map_24h = {}
    for inst_id in all_ids:
        volume_map_24h[inst_id] = calculate_24h_volume(inst_id)
        time.sleep(0.05)

    # ===== EMA 정배열 체크 =====
    for inst_id in all_ids:
        if get_ema_bullish_status(inst_id):
            bullish_count_only += 1
        time.sleep(0.05)

    # ===== 정배열 종목 필터링 =====
    bullish_candidates = []
    for inst_id in all_ids:
        if not get_ema_bullish_status(inst_id):
            continue
        vol_24h = volume_map_24h.get(inst_id, 0)
        if vol_24h == 0:
            continue
        bullish_candidates.append((inst_id, vol_24h))

    # ===== 거래대금 랭킹 계산 =====
    rank_24h = sorted(bullish_candidates, key=lambda x: x[1], reverse=True)
    volume_rank_map_24h = {inst_id: idx+1 for idx, (inst_id, _) in enumerate(rank_24h)}

    # ===== 랭킹 10위 내 조건 적용 =====
    top_bullish = []
    for inst_id, vol_24h in bullish_candidates:
        if volume_rank_map_24h[inst_id] <= 10:
            change = calculate_daily_change(inst_id) or 0
            top_bullish.append((inst_id, vol_24h, change))

    send_ranked_volume_message(top_bullish, total_count, bullish_count_only, volume_rank_map_24h, rank_24h)

# ===== 스케줄러 =====
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
