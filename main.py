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
telegram_user_id = 6596886700
bot = telepot.Bot(telegram_bot_token)

logging.basicConfig(level=logging.INFO)
sent_signal_coins = {}

# =========================
# Telegram 메시지 전송
# =========================
def send_telegram_message(message):
    for retry_count in range(1, 10 + 1):
        try:
            bot.sendMessage(chat_id=telegram_user_id, text=message)
            logging.info("텔레그램 메시지 전송 성공")
            return
        except Exception as e:
            logging.error(f"텔레그램 메시지 전송 실패 (재시도 {retry_count}/10): {e}")
            time.sleep(5)
    logging.error("텔레그램 메시지 전송 실패: 최대 재시도 초과")

# =========================
# API 호출 재시도
# =========================
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

# =========================
# OKX OHLCV 가져오기
# =========================
def get_ohlcv_okx(inst_id, bar='1H', limit=200):
    url = f"https://www.okx.com/api/v5/market/candles?instId={inst_id}&bar={bar}&limit={limit}"
    response = retry_request(requests.get, url)
    if response is None:
        return None
    try:
        df = pd.DataFrame(response.json()['data'], columns=[
            'ts','o','h','l','c','vol','volCcy','volCcyQuote','confirm'
        ])
        for col in ['o','h','l','c','vol','volCcyQuote']:
            df[col] = df[col].astype(float)
        df = df.iloc[::-1].reset_index(drop=True)
        return df
    except Exception as e:
        logging.error(f"{inst_id} OHLCV 파싱 실패: {e}")
        return None

# =========================
# RMA 계산
# =========================
def rma(series, period):
    series = series.copy()
    alpha = 1 / period
    r = series.ewm(alpha=alpha, adjust=False).mean()
    r.iloc[:period] = series.iloc[:period].expanding().mean()[:period]
    return r

# =========================
# RSI 계산 (5일선)
# =========================
def calc_rsi(df, period=5):
    delta = df['c'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = rma(gain, period)
    avg_loss = rma(loss, period)
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# =========================
# MFI 계산 (5일선)
# =========================
def calc_mfi(df, period=5):
    tp = (df['h'] + df['l'] + df['c']) / 3
    mf = tp * df['volCcyQuote']
    delta_tp = tp.diff()
    positive_mf = mf.where(delta_tp > 0, 0.0)
    negative_mf = mf.where(delta_tp < 0, 0.0)
    pos_rma = rma(positive_mf, period)
    neg_rma = rma(negative_mf, period)
    mfi = 100 * pos_rma / (pos_rma + neg_rma)
    return mfi

def format_rsi_mfi(value):
    if pd.isna(value):
        return "(N/A)"
    return f"🟢 {value:.1f}" if value >= 70 else f"🔴 {value:.1f}"

# =========================
# 4H RSI/MFI 크로스 확인 (5일선)
# =========================
def check_4h_mfi_rsi_cross(inst_id, period=5, threshold=70):  # ✅ 80 → 70으로 수정
    df = get_ohlcv_okx(inst_id, bar='4H', limit=100)
    if df is None or len(df) < period + 1:
        return False, None
    mfi = calc_mfi(df, period)
    rsi = calc_rsi(df, period)
    prev_mfi, curr_mfi = mfi.iloc[-2], mfi.iloc[-1]
    prev_rsi, curr_rsi = rsi.iloc[-2], rsi.iloc[-1]
    cross_time = pd.to_datetime(df['ts'].iloc[-1], unit='ms') + pd.Timedelta(hours=9)
    if pd.isna(curr_mfi) or pd.isna(curr_rsi):
        return False, None
    crossed = curr_mfi >= threshold and curr_rsi >= threshold and (prev_mfi < threshold or prev_rsi < threshold)
    return crossed, cross_time if crossed else None

# =========================
# 일간 상승률 계산
# =========================
def calculate_daily_change(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=48)
    if df is None or len(df) < 24:
        return None
    try:
        df['datetime'] = pd.to_datetime(df['ts'], unit='ms') + pd.Timedelta(hours=9)
        df.set_index('datetime', inplace=True)
        daily = df['c'].resample('1D', offset='9h').last()
        if len(daily) < 2:
            return None
        today_close = daily.iloc[-1]
        yesterday_close = daily.iloc[-2]
        return round((today_close - yesterday_close) / yesterday_close * 100, 2)
    except Exception as e:
        logging.error(f"{inst_id} 상승률 계산 오류: {e}")
        return None

def format_volume_in_eok(volume):
    try:
        eok = int(volume // 1_000_000)
        return str(eok) if eok >= 1 else "🚫"
    except:
        return "🚫"

# =========================
# 모든 USDT-SWAP 심볼
# =========================
def get_all_okx_swap_symbols():
    url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
    response = retry_request(requests.get, url)
    if response is None:
        return []
    data = response.json().get("data", [])
    return [item["instId"] for item in data if "USDT" in item["instId"]]

# =========================
# 24시간 거래대금
# =========================
def get_24h_volume(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=24)
    if df is None or len(df) < 24:
        return 0
    return df['volCcyQuote'].sum()

# =========================
# 신규 진입 알림
# =========================
def send_new_entry_message(all_ids):
    global sent_signal_coins
    volume_map = {inst_id: get_24h_volume(inst_id) for inst_id in all_ids}
    top_ids = sorted(volume_map, key=volume_map.get, reverse=True)[:100]
    rank_map = {inst_id: rank+1 for rank, inst_id in enumerate(top_ids)}
    new_entry_coins = []

    for inst_id in ["BTC-USDT-SWAP"] + top_ids:
        if inst_id not in sent_signal_coins:
            sent_signal_coins[inst_id] = {"crossed": False, "time": None}

    for inst_id in top_ids:
        # 4H 조건 체크 (80 기준)
        is_cross_4h, cross_time = check_4h_mfi_rsi_cross(inst_id, period=5, threshold=70)
        if not is_cross_4h:
            sent_signal_coins[inst_id]["crossed"] = False
            sent_signal_coins[inst_id]["time"] = None
            continue

        # ❌ 일봉 조건 제거됨 (원래 1D ≥ 70 체크 부분)

        # 일간 상승률 확인
        daily_change = calculate_daily_change(inst_id)
        if daily_change is None or daily_change <= 0:
            continue

        if not sent_signal_coins[inst_id]["crossed"]:
            new_entry_coins.append(
                (inst_id, daily_change, volume_map.get(inst_id, 0),
                 rank_map.get(inst_id), cross_time)
            )

        sent_signal_coins[inst_id]["crossed"] = True
        sent_signal_coins[inst_id]["time"] = cross_time

    # 메시지 전송
    if new_entry_coins:
        new_entry_coins.sort(key=lambda x: x[2], reverse=True)
        new_entry_coins = new_entry_coins[:3]

        message_lines = ["⚡ 4H RSI·MFI 필터 (≥80)", "━━━━━━━━━━━━━━━━━━━\n"]

        # BTC 현황
        btc_id = "BTC-USDT-SWAP"
        btc_change = calculate_daily_change(btc_id)
        btc_volume = volume_map.get(btc_id, 0)
        btc_volume_str = format_volume_in_eok(btc_volume)
        btc_status = "(N/A)"
        if btc_change is not None:
            if btc_change >= 5:
                btc_status = f"🟢🔥 +{btc_change:.2f}%"
            elif btc_change > 0:
                btc_status = f"🟢 +{btc_change:.2f}%"
            else:
                btc_status = f"🔴 {btc_change:.2f}%"

        df_btc_4h = get_ohlcv_okx(btc_id, bar='4H', limit=100)
        if df_btc_4h is not None and len(df_btc_4h) >= 5:
            mfi_btc_4h = calc_mfi(df_btc_4h, 5).iloc[-1]
            rsi_btc_4h = calc_rsi(df_btc_4h, 5).iloc[-1]
        else:
            mfi_btc_4h, rsi_btc_4h = None, None

        df_btc_1d = get_ohlcv_okx(btc_id, bar='1D', limit=30)
        if df_btc_1d is not None and len(df_btc_1d) >= 5:
            mfi_btc_1d = calc_mfi(df_btc_1d, 5).iloc[-1]
            rsi_btc_1d = calc_rsi(df_btc_1d, 5).iloc[-1]
        else:
            mfi_btc_1d, rsi_btc_1d = None, None

        message_lines.append(
            f"💎 BTC 현황 (실시간)\n"
            f"{btc_status} | 💰 거래대금: {btc_volume_str}M\n"
            f"📊 4H → RSI: {format_rsi_mfi(rsi_btc_4h)} | MFI: {format_rsi_mfi(mfi_btc_4h)}\n"
            f"📊 1D → RSI: {format_rsi_mfi(rsi_btc_1d)} | MFI: {format_rsi_mfi(mfi_btc_1d)}\n"
        )

        # 거래대금 TOP 10
        message_lines.append("━━━━━━━━━━━━━━━━━━━\n")
        message_lines.append("🏆 실시간 거래대금 TOP 10\n")

        for rank, inst_id in enumerate(top_ids[:10], start=1):
            change = calculate_daily_change(inst_id)
            volume = volume_map.get(inst_id, 0)
            volume_str = format_volume_in_eok(volume)
            name = inst_id.replace("-USDT-SWAP", "")

            if change is not None:
                if change >= 5:
                    status = f"🟢🔥 +{change:.2f}%"
                elif change > 0:
                    status = f"🟢 +{change:.2f}%"
                else:
                    status = f"🔴 {change:.2f}%"
            else:
                status = "(N/A)"

            df_4h = get_ohlcv_okx(inst_id, bar='4H', limit=100)
            if df_4h is not None and len(df_4h) >= 5:
                mfi_4h = calc_mfi(df_4h, 5).iloc[-1]
                rsi_4h = calc_rsi(df_4h, 5).iloc[-1]
            else:
                mfi_4h, rsi_4h = None, None

            df_1d = get_ohlcv_okx(inst_id, bar='1D', limit=30)
            if df_1d is not None and len(df_1d) >= 5:
                mfi_1d = calc_mfi(df_1d, 5).iloc[-1]
                rsi_1d = calc_rsi(df_1d, 5).iloc[-1]
            else:
                mfi_1d, rsi_1d = None, None

            message_lines.append(
                f"{rank}위 {name}\n"
                f"{status} | 💰 거래대금: {volume_str}M\n"
                f"📊 4H → RSI: {format_rsi_mfi(rsi_4h)} | MFI: {format_rsi_mfi(mfi_4h)}\n"
                f"📊 1D → RSI: {format_rsi_mfi(rsi_1d)} | MFI: {format_rsi_mfi(mfi_1d)}"
            )

        # 신규 진입 코인
        message_lines.append("\n━━━━━━━━━━━━━━━━━━━")
        message_lines.append("🆕 신규 진입 코인 (상위 3개) 👀")
        for inst_id, daily_change, volume_24h, coin_rank, cross_time in new_entry_coins:
            name = inst_id.replace("-USDT-SWAP", "")
            volume_str = format_volume_in_eok(volume_24h)

            df_4h = get_ohlcv_okx(inst_id, bar='4H', limit=100)
            if df_4h is not None and len(df_4h) >= 5:
                mfi_4h = calc_mfi(df_4h, 5).iloc[-1]
                rsi_4h = calc_rsi(df_4h, 5).iloc[-1]
            else:
                mfi_4h, rsi_4h = None, None

            df_1d = get_ohlcv_okx(inst_id, bar='1D', limit=30)
            if df_1d is not None and len(df_1d) >= 5:
                mfi_1d = calc_mfi(df_1d, 5).iloc[-1]
                rsi_1d = calc_rsi(df_1d, 5).iloc[-1]
            else:
                mfi_1d, rsi_1d = None, None

            daily_str = f"+{daily_change:.2f}%"
            if daily_change >= 5:
                daily_str = f"🔥 {daily_str}"

            message_lines.append(
                f"\n{coin_rank}위 {name}\n"
                f"{daily_str} | 💰 거래대금: {volume_str}M\n"
                f"📊 4H → RSI: {format_rsi_mfi(rsi_4h)} | MFI: {format_rsi_mfi(mfi_4h)}\n"
                f"📊 1D → RSI: {format_rsi_mfi(rsi_1d)} | MFI: {format_rsi_mfi(mfi_1d)}"
            )

        message_lines.append("\n━━━━━━━━━━━━━━━━━━━")
        send_telegram_message("\n".join(message_lines))
    else:
        logging.info("⚡ 신규 진입 없음 → 메시지 전송 안 함")

# =========================
# 메인 실행
# =========================
def main():
    logging.info("📥 거래대금 분석 시작")
    all_ids = get_all_okx_swap_symbols()
    send_new_entry_message(all_ids)

# =========================
# 스케줄러
# =========================
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
