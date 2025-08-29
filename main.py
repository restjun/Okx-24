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

sent_signal_coins = {}

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
            logging.error(f"API 호출 실패 (재시도 {attempt + 1}/10): {e}")
            time.sleep(5)
    return None

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

def rma(series, period):
    series = series.copy()
    alpha = 1 / period
    r = series.ewm(alpha=alpha, adjust=False).mean()
    r.iloc[:period] = series.iloc[:period].expanding().mean()[:period]
    return r

def calc_rsi(df, period=3):
    delta = df['c'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = rma(gain, period)
    avg_loss = rma(loss, period)
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

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

def format_rsi_mfi(value):
    if pd.isna(value):
        return "(N/A)"
    return f"🟢 {value:.1f}" if value >= 60 else f"🔴 {value:.1f}"

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

    crossed = curr_mfi >= threshold and curr_rsi >= threshold and \
              (prev_mfi < threshold or prev_rsi < threshold)
    return crossed, cross_time if crossed else None

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

def get_all_okx_swap_symbols():
    url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
    response = retry_request(requests.get, url)
    if response is None:
        return []
    data = response.json().get("data", [])
    return [item["instId"] for item in data if "USDT" in item["instId"]]

def get_24h_volume(inst_id):
    df = get_ohlcv_okx(inst_id, bar="1H", limit=24)
    if df is None or len(df) < 24:
        return 0
    return df['volCcyQuote'].sum()

def send_new_entry_message(all_ids):
    global sent_signal_coins
    volume_map = {inst_id: get_24h_volume(inst_id) for inst_id in all_ids}
    top_ids = sorted(volume_map, key=volume_map.get, reverse=True)[:10]
    rank_map = {inst_id: rank+1 for rank, inst_id in enumerate(top_ids)}
    new_entry_coins = []

    for inst_id in ["BTC-USDT-SWAP"] + top_ids:
        if inst_id not in sent_signal_coins:
            sent_signal_coins[inst_id] = {"crossed": False, "time": None}

    for inst_id in top_ids:
        is_cross_4h, cross_time = check_4h_mfi_rsi_cross(inst_id, period=3, threshold=70)
        if not is_cross_4h:
            sent_signal_coins[inst_id]["crossed"] = False
            sent_signal_coins[inst_id]["time"] = None
            continue

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

    if new_entry_coins:
        new_entry_coins.sort(key=lambda x: x[2], reverse=True)
        new_entry_coins = new_entry_coins[:3]

        message_lines = ["⚡ 4H·RSI·MFI 필터", "━━━━━━━━━━━━━━━━━━━"]

        # BTC 현황
        btc_id = "BTC-USDT-SWAP"
        btc_change = calculate_daily_change(btc_id)
        btc_volume = volume_map.get(btc_id, 0)
        btc_volume_str = format_volume_in_eok(btc_volume)

        if btc_change is None:
            btc_status = "(N/A)"
        elif btc_change > 0:
            btc_status = f"🟢 +{btc_change:.2f}%"
        elif btc_change < 0:
            btc_status = f"🔴 {btc_change:.2f}%"
        else:
            btc_status = f"{btc_change:.2f}%"

        message_lines.append(
            f"📌 BTC 현황: BTC {btc_status}\n거래대금: {btc_volume_str}"
        )

        # 거래대금 1위
        if top_ids:
            top1_id = top_ids[0]
            if top1_id != btc_id:
                top1_change = calculate_daily_change(top1_id)
                top1_volume = volume_map.get(top1_id, 0)
                top1_volume_str = format_volume_in_eok(top1_volume)
                top1_name = top1_id.replace("-USDT-SWAP", "")

                if top1_change is None:
                    top1_status = "(N/A)"
                elif top1_change > 0:
                    top1_status = f"🟢 +{top1_change:.2f}%"
                elif top1_change < 0:
                    top1_status = f"🔴 {top1_change:.2f}%"
                else:
                    top1_status = f"{top1_change:.2f}%"

                message_lines.append(
                    f"\n📌 거래대금 1위: {top1_name} {top1_status}\n거래대금: {top1_volume_str}"
                )

        message_lines.append("━━━━━━━━━━━━━━━━━━━")
        message_lines.append("🆕 신규 진입 코인 (상위 3개)")

        for inst_id, daily_change, volume_24h, coin_rank, cross_time in new_entry_coins:
            name = inst_id.replace("-USDT-SWAP", "")
            volume_str = format_volume_in_eok(volume_24h)
            df_4h = get_ohlcv_okx(inst_id, bar='4H', limit=100)
            if df_4h is not None and len(df_4h) >= 3:
                mfi_4h = calc_mfi(df_4h, 3).iloc[-1]
                rsi_4h = calc_rsi(df_4h, 3).iloc[-1]
            else:
                mfi_4h, rsi_4h = None, None

            message_lines.append(
                f"{name} (+{daily_change:.2f}%)\n거래대금: {volume_str} (순위: {coin_rank}위)\n"
                f"📊 4H RSI: {format_rsi_mfi(rsi_4h)} / MFI: {format_rsi_mfi(mfi_4h)}"
            )

        message_lines.append("━━━━━━━━━━━━━━━━━━━")
        send_telegram_message("\n".join(message_lines))
    else:
        logging.info("⚡ 신규 진입 없음 → 메시지 전송 안 함")

def main():
    logging.info("📥 거래대금 분석 시작")
    all_ids = get_all_okx_swap_symbols()
    send_new_entry_message(all_ids)

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
