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


# 📩 텔레그램 메시지 전송 함수
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


# 📡 API 재시도 유틸
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


# 📈 EMA 계산
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


# 📊 종목 목록 조회
def get_all_okx_swap_symbols():
    url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
    response = retry_request(requests.get, url)
    if response is None:
        return []
    data = response.json().get("data", [])
    return [item["instId"] for item in data if "USDT" in item["instId"]]


# 📊 OHLCV 조회
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


# 🔎 EMA 정배열 여부
def is_ema_bullish(df):
    close = df['c'].values
    ema_20 = get_ema_with_retry(close, 20)
    ema_50 = get_ema_with_retry(close, 50)
    ema_200 = get_ema_with_retry(close, 200)
    if None in [ema_20, ema_50, ema_200]:
        return False
    return ema_20 > ema_50 > ema_200


# 📋 EMA 정배열 필터링
def filter_by_1h_and_4h_ema_alignment(inst_ids):
    bullish_ids = []
    for inst_id in inst_ids:
        df_1h = get_ohlcv_okx(inst_id, bar='1H', limit=200)
        df_4h = get_ohlcv_okx(inst_id, bar='4H', limit=200)
        if df_1h is None or df_4h is None:
            continue
        if is_ema_bullish(df_1h) and is_ema_bullish(df_4h):
            bullish_ids.append(inst_id)
        time.sleep(random.uniform(0.2, 0.4))
    return bullish_ids


# 📊 거래대금 / 변동률 계산
def calculate_1h_volume(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=24)
    if df is None or len(df) < 1:
        return 0
    return df["volCcyQuote"].sum()


def calculate_daily_change(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1D", limit=2)
    if df is None or len(df) < 2:
        return None
    try:
        open_price = df.iloc[-1]['o']
        close_price = df.iloc[-1]['c']
        return round(((close_price - open_price) / open_price) * 100, 2)
    except Exception as e:
        logging.error(f"{inst_id} 상승률 계산 오류: {e}")
        return None


# 🧮 EMA 상태 텍스트 출력
def format_volume_in_eok(volume):
    try:
        return f"{int(volume // 100_000)}억"
    except:
        return "N/A"


def format_change_with_emoji(change):
    if change is None:
        return "(N/A)"
    if change >= 5:
        return f"🚀🚀🚀 (+{change:.2f}%)"
    elif change > 0:
        return f"🟢 (+{change:.2f}%)"
    else:
        return f"🔴 ({change:.2f}%)"


def get_ema_status_text(df, timeframe="15m"):
    close = df['c'].values
    ema_10 = get_ema_with_retry(close, 10)
    ema_20 = get_ema_with_retry(close, 20)
    ema_50 = get_ema_with_retry(close, 50)
    ema_200 = get_ema_with_retry(close, 200)
    if None in [ema_10, ema_20, ema_50, ema_200]:
        return f"[{timeframe}] EMA 📊: ❌ 데이터 부족"
    def check(cond): return "✅" if cond else "❌"
    return (
        f"[{timeframe}] EMA 📊: "
        f"{check(ema_10 > ema_20)}"
        f"{check(ema_20 > ema_50)}"
        f"{check(ema_50 > ema_200)}"
    )


def is_15m_check_condition(df):
    close = df['c'].values
    ema_10 = get_ema_with_retry(close, 10)
    ema_20 = get_ema_with_retry(close, 20)
    ema_50 = get_ema_with_retry(close, 50)
    ema_200 = get_ema_with_retry(close, 200)
    if None in [ema_10, ema_20, ema_50, ema_200]:
        return False
    return (ema_10 < ema_20) and (ema_20 > ema_50) and (ema_50 > ema_200)


def get_btc_ema_status_all_timeframes():
    timeframes = ['1D', '4H', '1H', '15m']
    btc_id = "BTC-USDT-SWAP"
    status_texts = []
    for tf in timeframes:
        df = get_ohlcv_okx(btc_id, bar=tf, limit=200)
        if df is not None:
            status = get_ema_status_text(df, timeframe=tf)
        else:
            status = f"[{tf}] EMA 📊: ❌ 불러오기 실패"
        status_texts.append(f"    {status}")
        time.sleep(random.uniform(0.2, 0.4))
    return "\n".join(status_texts)


# 📬 최종 메시지 생성 및 전송
def send_ranked_volume_message(bullish_ids):
    volume_24h_data = {}
    volume_1h_data = {}

    btc_id = "BTC-USDT-SWAP"
    btc_ema_status_all = get_btc_ema_status_all_timeframes()
    btc_change = calculate_daily_change(btc_id)
    btc_change_str = format_change_with_emoji(btc_change)
    btc_volume = calculate_1h_volume(btc_id)
    btc_volume_str = format_volume_in_eok(btc_volume)

    # Step 1: 24H 거래대금 수집
    for inst_id in bullish_ids:
        df_24h = get_ohlcv_okx(inst_id, bar="1D", limit=2)
        vol_24h = df_24h['volCcyQuote'].sum() if df_24h is not None else 0
        volume_24h_data[inst_id] = vol_24h
        time.sleep(random.uniform(0.2, 0.4))

    # Step 2: 24H 거래대금 상위 10개
    top_10_24h = sorted(volume_24h_data.items(), key=lambda x: x[1], reverse=True)[:10]
    top_10_24h_ids = [item[0] for item in top_10_24h]

    # Step 3: 1H 거래대금 수집
    for inst_id in top_10_24h_ids:
        vol_1h = calculate_1h_volume(inst_id)
        volume_1h_data[inst_id] = vol_1h
        time.sleep(random.uniform(0.2, 0.4))

    # Step 4: 1H 거래대금 필터링 + 정렬
    MIN_1H_VOLUME = 100_000_000
    filtered_and_sorted = [
        (inst_id, vol) for inst_id, vol in sorted(volume_1h_data.items(), key=lambda x: x[1], reverse=True)
        if vol >= MIN_1H_VOLUME
    ]

    # 메시지 작성
    message_lines = [
        "📊 *OKX 정배열 매물대 분석*",
        "📅 *[1H + 4H EMA 정배열] + [24H 거래대금 Top10 중 1H 기준 필터]*",
        "━━━━━━━━━━━━━━━━━━━",
        f"💰 *BTC* {btc_change_str} / 거래대금: {btc_volume_str}",
        btc_ema_status_all,
        "━━━━━━━━━━━━━━━━━━━"
    ]

    rank = 1
    for inst_id, vol_1h in filtered_and_sorted:
        try:
            change = calculate_daily_change(inst_id)
            df_15m = get_ohlcv_okx(inst_id, bar="15m", limit=200)

            if change is None or df_15m is None:
                continue

            ema_status = get_ema_status_text(df_15m, timeframe="15m")
            name = inst_id.replace("-USDT-SWAP", "")
            vol_1h_text = format_volume_in_eok(vol_1h)
            change_str = format_change_with_emoji(change)
            star = "  🎯🎯🎯 차트확인" if change > 0 and is_15m_check_condition(df_15m) else ""

            message_lines.append(
                f"*{rank}. {name}* {change_str} | 💰 {vol_1h_text}\n   {ema_status}{star}"
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
        message_lines.append("📡 *조건: 24H 거래대금 Top10 → 1H 거래대금 1억 이상 종목만 정렬*")

    send_telegram_message("\n".join(message_lines))


# 📌 메인 루프
def main():
    logging.info("📥 전체 종목 기준 정배열 + 거래대금 분석 시작")
    all_ids = get_all_okx_swap_symbols()
    bullish_ids = filter_by_1h_and_4h_ema_alignment(all_ids)
    if not bullish_ids:
        send_telegram_message("🔴 1H + 4H 정배열 종목 없음.")
        return
    send_ranked_volume_message(bullish_ids)


# ⏱️ 스케줄러 시작
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
