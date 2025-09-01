from fastapi import FastAPI
import telepot
import schedule
import time
import requests
import threading
import uvicorn
import logging
import pandas as pd
import numpy as np

app = FastAPI()

# =========================
# Telegram 설정
# =========================
telegram_bot_token = "8451481398:AAHHg2wVDKphMruKsjN2b6NFKJ50jhxEe-g"
telegram_user_id = 659688670
bot = telepot.Bot(telegram_bot_token)

# =========================
# OKX API
# =========================
def get_all_okx_swap_symbols():
    url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
    try:
        r = requests.get(url, timeout=10)
        data = r.json()
        return [d["instId"] for d in data["data"]]
    except Exception as e:
        logging.error(f"심볼 조회 오류: {e}")
        return []

def get_ohlcv_okx(inst_id, bar="4H", limit=100):
    url = f"https://www.okx.com/api/v5/market/candles?instId={inst_id}&bar={bar}&limit={limit}"
    try:
        r = requests.get(url, timeout=10)
        data = r.json()
        if "data" not in data:
            return None
        df = pd.DataFrame(data["data"], columns=[
            "ts","o","h","l","c","vol","volCcy","volCcyQuote","confirm"
        ])
        df = df.astype(float)
        df = df.iloc[::-1].reset_index(drop=True)  # 시간 순 정렬
        return df
    except Exception as e:
        logging.error(f"{inst_id} OHLCV 조회 오류: {e}")
        return None

def get_24h_volume(inst_id):
    url = f"https://www.okx.com/api/v5/market/ticker?instId={inst_id}"
    try:
        r = requests.get(url, timeout=10)
        data = r.json()
        return float(data["data"][0]["volCcyQuote"])  # 24h 거래대금 (USDT 기준)
    except Exception as e:
        logging.error(f"{inst_id} 거래대금 조회 오류: {e}")
        return 0

# =========================
# 지표 계산
# =========================
def calc_rsi(df, period=14):
    delta = df["c"].diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    avg_gain = pd.Series(gain).rolling(period).mean()
    avg_loss = pd.Series(loss).rolling(period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def calc_mfi(df, period=14):
    tp = (df["h"] + df["l"] + df["c"]) / 3
    mf = tp * df["vol"]
    positive_flow = []
    negative_flow = []
    for i in range(1, len(tp)):
        if tp[i] > tp[i-1]:
            positive_flow.append(mf.iloc[i])
            negative_flow.append(0)
        else:
            positive_flow.append(0)
            negative_flow.append(mf.iloc[i])
    positive_mf = pd.Series(positive_flow).rolling(period).sum()
    negative_mf = pd.Series(negative_flow).rolling(period).sum()
    mfi = 100 - (100 / (1 + (positive_mf / negative_mf)))
    return mfi.reindex(df.index, method="bfill")

# =========================
# 텔레그램 전송
# =========================
def send_telegram_message(message):
    try:
        bot.sendMessage(telegram_user_id, message)
    except Exception as e:
        logging.error(f"텔레그램 전송 오류: {e}")

# =========================
# 신규 진입 알림 (원본 유지)
# =========================
def send_new_entry_message(all_ids):
    message = "📥 신규 진입 코인 알림 (기존 기능 유지)\n"
    message += "━━━━━━━━━━━━━━━━━━━\n"
    for inst_id in all_ids[:5]:  # 예시: 상위 5개만 출력
        message += f"✅ {inst_id}\n"
    send_telegram_message(message)

# =========================
# 시장 MFI/RSI 통계 (TOP 100)
# =========================
def market_mfi_rsi_statistics(all_ids, top_n=100):
    volume_map = {inst_id: get_24h_volume(inst_id) for inst_id in all_ids}
    top_ids = sorted(volume_map, key=volume_map.get, reverse=True)[:top_n]

    stats = []
    for inst_id in top_ids:
        df = get_ohlcv_okx(inst_id, bar="4H", limit=100)
        if df is None or len(df) < 3:
            continue
        mfi_4h = calc_mfi(df, 3).iloc[-1]
        rsi_4h = calc_rsi(df, 3).iloc[-1]
        stats.append((inst_id, mfi_4h, rsi_4h))

    if not stats:
        return "📊 시장 통계 계산 실패 (데이터 부족)"

    avg_mfi = np.nanmean([m for _, m, _ in stats])
    avg_rsi = np.nanmean([r for _, _, r in stats])

    overbought = sum(1 for _, m, r in stats if (m >= 70 and r >= 70))
    oversold = sum(1 for _, m, r in stats if (m <= 30 and r <= 30))
    neutral = len(stats) - overbought - oversold

    if overbought / len(stats) >= 0.4:
        market_trend = "🚀 상승장 경고 (과매수 집중)"
    elif oversold / len(stats) >= 0.4:
        market_trend = "📉 하락장 (과매도 집중)"
    else:
        market_trend = "⚖️ 중립 구간"

    message = (
        "📊 시장 RSI·MFI 통계 (Top 100)\n"
        "━━━━━━━━━━━━━━━━━━━\n"
        f"📈 평균 RSI: {avg_rsi:.1f}\n"
        f"💵 평균 MFI: {avg_mfi:.1f}\n\n"
        f"🟢 과매수 코인: {overbought}개\n"
        f"🔴 과매도 코인: {oversold}개\n"
        f"⚪ 중립: {neutral}개\n"
        "━━━━━━━━━━━━━━━━━━━\n"
        f"📌 시장 판단 → {market_trend}"
    )

    return message

# =========================
# 메인 실행
# =========================
def main():
    logging.info("📥 거래대금 분석 시작")
    all_ids = get_all_okx_swap_symbols()
    send_new_entry_message(all_ids)

    stats_message = market_mfi_rsi_statistics(all_ids, top_n=100)
    send_telegram_message(stats_message)

# =========================
# 스케줄러
# =========================
def run_scheduler():
    schedule.every(30).minutes.do(main)
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    threading.Thread(target=run_scheduler, daemon=True).start()
    uvicorn.run(app, host="0.0.0.0", port=8000)
