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

=========================
Telegram 설정
=========================
telegram_bot_token = "6389499820:AAFjTTrRgNjhoKPSZ-bWB5RMhPLlWQ0lnGU"
telegram_user_id = 6596886700
bot = telepot.Bot(telegram_bot_token)

logging.basicConfig(level=logging.INFO)

=========================
거래대금 TOP10 상태 저장
=========================
previous_top10 = set()

=========================
Telegram 메시지 전송
=========================
def send_telegram_message(message):
for retry_count in range(1, 11):
try:
bot.sendMessage(chat_id=telegram_user_id, text=message)
logging.info("텔레그램 메시지 전송 성공")
return
except Exception as e:
logging.error(f"텔레그램 전송 실패 ({retry_count}/10): {e}")
time.sleep(5)

=========================
API 재시도
=========================
def retry_request(func, *args, **kwargs):
for attempt in range(10):
try:
result = func(*args, **kwargs)
if hasattr(result, "status_code") and result.status_code == 429:
time.sleep(1)
continue
return result
except Exception as e:
logging.error(f"API 실패 ({attempt+1}/10): {e}")
time.sleep(3)
return None

=========================
OKX OHLCV
=========================
def get_ohlcv_okx(inst_id, bar="1H", limit=48):
url = f"https://www.okx.com/api/v5/market/candles?instId={inst_id}&bar={bar}&limit={limit}"
response = retry_request(requests.get, url)
if response is None:
return None

try:
df = pd.DataFrame(
response.json()["data"],
columns=["ts", "o", "h", "l", "c", "vol", "volCcy", "volCcyQuote", "confirm"]
)
for col in ["c", "volCcyQuote"]:
df[col] = df[col].astype(float)

df = df.iloc[::-1].reset_index(drop=True)
return df
except Exception as e:
logging.error(f"{inst_id} OHLCV 파싱 실패: {e}")
return None
=========================
4시간봉 EMA50 > EMA200 확인
=========================
def check_4h_ema_alignment(inst_id):
df = get_ohlcv_okx(inst_id, bar="4H", limit=250)

if df is None or len(df) < 200:
return None

try:
close = df["c"]

ema50 = close.ewm(span=50, adjust=False).mean().iloc[-1]
ema200 = close.ewm(span=200, adjust=False).mean().iloc[-1]

return ema50 > ema200

except Exception as e:
logging.error(f"{inst_id} EMA 계산 실패: {e}")
return None
=========================
일간 상승률 계산
=========================
def calculate_daily_change(inst_id):
df = get_ohlcv_okx(inst_id, bar="1H", limit=48)
if df is None or len(df) < 24:
return None

try:
df["datetime"] = pd.to_datetime(df["ts"], unit="ms") + pd.Timedelta(hours=9)
df.set_index("datetime", inplace=True)
daily = df["c"].resample("1D", offset="9h").last()

if len(daily) < 2:
return None

return round((daily.iloc[-1] - daily.iloc[-2]) / daily.iloc[-2] * 100, 2)
except:
return None

=========================
거래대금 포맷
=========================
def format_volume_in_eok(volume):
try:
m = int(volume // 1_000_000)
return f"{m}M" if m >= 1 else "🚫"
except:
return "🚫"

=========================
OKX USDT-SWAP 전체 심볼
=========================
def get_all_okx_swap_symbols():
url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
response = retry_request(requests.get, url)
if response is None:
return []

data = response.json().get("data", [])
return [item["instId"] for item in data if "USDT" in item["instId"]]

=========================
24시간 거래대금
=========================
def get_24h_volume(inst_id):
df = get_ohlcv_okx(inst_id, bar="1H", limit=24)
if df is None or len(df) < 24:
return 0
return df["volCcyQuote"].sum()

=========================
거래대금 TOP10 알림
=========================
def send_volume_rank_message(all_ids):
global previous_top10

volume_map = {inst_id: get_24h_volume(inst_id) for inst_id in all_ids}
top_ids = sorted(volume_map, key=volume_map.get, reverse=True)[:30]

current_top10 = set(top_ids)
new_entries = current_top10 - previous_top10

message_lines = [
"🏆 OKX 실거래대금 TOP30",
"━━━━━━━━━━━━━━━━━━━"
 ]

for rank, inst_id in enumerate(top_ids, start=1):
name = inst_id.replace("-USDT-SWAP", "")
volume_str = format_volume_in_eok(volume_map[inst_id])

daily_change = calculate_daily_change(inst_id)
ema_alignment = check_4h_ema_alignment(inst_id)

if ema_alignment is None:
ema_str = "EMA N/A"
elif ema_alignment:
ema_str = " 📈 정배열"
else:
ema_str = " "
if daily_change is None:
daily_str = "N/A"
elif daily_change >= 5:
daily_str = f"🟢🚨 {daily_change:.2f}%"
elif daily_change > 0:
daily_str = f"🟢 {daily_change:.2f}%"
else:
daily_str = f"🔴 {daily_change:.2f}%"

new_mark = " 🚨NEW" if inst_id in new_entries else ""

message_lines.append(
f"🏅 {rank}위 | {name}{new_mark}\n"
f"{daily_str} "
f"{ema_str} "
f"💰 {volume_str}\n"
)
message_lines.append("━━━━━━━━━━━━━━━━━━━")
send_telegram_message("\n".join(message_lines))

previous_top10 = current_top10

=========================
메인
=========================
def main():
logging.info("📥 OKX 실거래대금 TOP10 분석")
all_ids = get_all_okx_swap_symbols()
send_volume_rank_message(all_ids)

=========================
스케줄러
=========================
def run_scheduler():
while True:
schedule.run_pending()
time.sleep(1)

@app.on_event("startup")
def start_scheduler():
schedule.every(1).minutes.do(main)
threading.Thread(target=run_scheduler, daemon=True).start()

=========================
FastAPI 실행
=========================
if name == "main":
uvicorn.run(app, host="0.0.0.0", port=8000)

