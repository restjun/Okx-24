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

telegram_bot_token = "8451481398:AAHHg2wVDKphMruKsjN2b6NFKJ50jhxEe-g"
telegram_user_id = 6596886700
bot = telepot.Bot(telegram_bot_token)

logging.basicConfig(level=logging.INFO)

# 🔹 4H 돌파 상태 및 일시 저장
sent_signal_coins = {}  # {symbol: {"crossed": bool, "time": timestamp}}

# 🔹 텔레그램 메시지
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

# 🔹 API 재시도
def retry_request(func, *args, **kwargs):
    for attempt in range(10):
        try:
            result = func(*args, **kwargs)
            if hasattr(result, 'status_code') and result.status_code == 429:
                time.sleep(1)
                continue
            return result
        except Exception as e:
            logging.error(f"API 호출 실패 (재시도 {attempt + 1}/10): {e}")
            time.sleep(5)
    return None

# 🔹 OHLCV 가져오기
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

# 🔹 Wilder's RMA (트레이딩뷰 방식)
def rma(series, period):
    series = series.copy()
    alpha = 1 / period
    r = series.ewm(alpha=alpha, adjust=False).mean()
    r.iloc[:period] = series.iloc[:period].expanding().mean()[:period]
    return r

# 🔹 RSI
def calc_rsi(df, period=3):
    delta = df['c'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = rma(gain, period)
    avg_loss = rma(loss, period)
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# 🔹 MFI
def calc_mfi(df, period=3):
    tp = (df['h'] + df['l'] + df['c']) / 3
    mf = tp * df['volCcyQuote']
    delta_tp = tp.diff()
    positive_mf = mf.where(delta_tp > 0, 0.0)
    negative_mf = mf.where(delta_tp < 0, 0.0)
    pos_rma = rma(positive_mf, period)
    neg_rma = rma(negative_mf, period)
    mfi = 100 * pos_rma / (pos_rma + neg_rma)
    return mfi

# 🔹 RSI/MFI 포맷
def format_rsi_mfi(value):
    if pd.isna(value):
        return "(N/A)"
    return f"🟢 {value:.1f}" if value >= 60 else f"🔴 {value:.1f}"

# 🔹 4H MFI·RSI 돌파 체크
def check_4h_mfi_rsi_cross(inst_id, period=3, threshold=70):
    df = get_ohlcv_okx(inst_id, bar='4H', limit=100)
    if df is None or len(df) < period+1:
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

# 🔹 일일 상승률
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

# 🔹 거래대금 단위 변환
def format_volume_in_eok(volume):
    try:
        eok = int(volume // 1_000_000)
        return str(eok) if eok >= 1 else "🚫"
    except:
        return "🚫"

# 🔹 상승률 이모지
def format_change_with_emoji(change):
    if change is None:
        return "(N/A)"
    if change >= 5:
        return f"🚨🚨🚨 (+{change:.2f}%)"
    elif change > 0:
        return f"🟢 (+{change:.2f}%)"
    else:
        return f"🔴 ({change:.2f}%)"

# 🔹 OKX USDT-SWAP 심볼
def get_all_okx_swap_symbols():
    url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
    response = retry_request(requests.get, url)
    if response is None:
        return []
    data = response.json().get("data", [])
    return [item["instId"] for item in data if "USDT" in item["instId"]]

# 🔹 24시간 거래대금
def get_24h_volume(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=24)
    if df is None or len(df) < 24:
        return 0
    return df['volCcyQuote'].sum()

# 🔹 신규 돌파 메시지
def send_new_entry_message(all_ids):
    global sent_signal_coins
    volume_map = {inst_id: get_24h_volume(inst_id) for inst_id in all_ids}
    top_ids = sorted(volume_map, key=volume_map.get, reverse=True)[:50]
    rank_map = {inst_id: rank+1 for rank, inst_id in enumerate(top_ids)}
    new_entry_coins = []

    # BTC 포함 상태 초기화
    for inst_id in ["BTC-USDT-SWAP"] + top_ids:
        if inst_id not in sent_signal_coins:
            sent_signal_coins[inst_id] = {"crossed": False, "time": None}

    for inst_id in top_ids:
        is_cross_4h, cross_time = check_4h_mfi_rsi_cross(inst_id, period=3, threshold=70)
        if not is_cross_4h:
            sent_signal_coins[inst_id]["crossed"] = False
            sent_signal_coins[inst_id]["time"] = None
            continue

        df_1d = get_ohlcv_okx(inst_id, bar='1D', limit=100)
        if df_1d is None or len(df_1d) < 3:
            continue
        d1_mfi = calc_mfi(df_1d, 3).iloc[-1]
        d1_rsi = calc_rsi(df_1d, 3).iloc[-1]
        if d1_mfi < 70 or d1_rsi < 70:
            continue

        daily_change = calculate_daily_change(inst_id)
        if daily_change is None or daily_change <= 0:
            continue

        # 🔹 신규 돌파 코인만
        if not sent_signal_coins[inst_id]["crossed"]:
            new_entry_coins.append(
                (inst_id, daily_change, volume_map.get(inst_id, 0),
                 d1_mfi, d1_rsi, cross_time, rank_map.get(inst_id))
            )

        # 상태 업데이트
        sent_signal_coins[inst_id]["crossed"] = True
        sent_signal_coins[inst_id]["time"] = cross_time

    if new_entry_coins:
        new_entry_coins.sort(key=lambda x: x[2], reverse=True)
        new_entry_coins = new_entry_coins[:3]

        message_lines = ["⚡ 4H MFI·RSI 3일선 돌파 + 1D MFI·RSI ≥ 70 필터", "━━━━━━━━━━━━━━━━━━━"]

        # BTC 상태 포함
        btc_id = "BTC-USDT-SWAP"
        btc_change = calculate_daily_change(btc_id)
        btc_volume = volume_map.get(btc_id, 0)
        btc_volume_str = format_volume_in_eok(btc_volume)
        btc_state = sent_signal_coins[btc_id]["crossed"]
        btc_time = sent_signal_coins[btc_id]["time"]
        btc_time_str = btc_time.strftime("%Y-%m-%d %H:%M") if btc_time else "(N/A)"

        message_lines += [
            "📌 BTC 현황",
            f"BTC\n거래대금: {btc_volume_str}\n상승률: {format_change_with_emoji(btc_change)}\n"
            f"4H 돌파 상태: {'✅' if btc_state else '❌'}\n4H 돌파 시간: {btc_time_str}",
            "━━━━━━━━━━━━━━━━━━━",
            "🆕 신규 진입 코인 (상위 3개)"
        ]

        for inst_id, daily_change, volume_24h, d1_mfi, d1_rsi, cross_time, coin_rank in new_entry_coins:
            name = inst_id.replace("-USDT-SWAP","")
            volume_str = format_volume_in_eok(volume_24h)
            cross_time_str = cross_time.strftime("%Y-%m-%d %H:%M") if cross_time else "(N/A)"
            message_lines.append(
                f"{name}\n거래대금: {volume_str}\n순위: {coin_rank}위\n상승률: {format_change_with_emoji(daily_change)}\n"
                f"4H 돌파 상태: ✅\n4H 돌파 시간: {cross_time_str}\n"
                f"📊 1D RSI: {format_rsi_mfi(d1_rsi)} / MFI: {format_rsi_mfi(d1_mfi)}"
            )

        message_lines.append("━━━━━━━━━━━━━━━━━━━")
        send_telegram_message("\n".join(message_lines))
    else:
        logging.info("⚡ 신규 진입 없음 → 메시지 전송 안 함")

# 🔹 메인 실행
def main():
    logging.info("📥 거래대금 분석 시작")
    all_ids = get_all_okx_swap_symbols()
    send_new_entry_message(all_ids)

# 🔹 스케줄러
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
