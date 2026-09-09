from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import schedule,time,requests,threading,uvicorn,logging,pandas as pd,warnings
from datetime import datetime
from zoneinfo import ZoneInfo

warnings.filterwarnings("ignore",category=FutureWarning)
app=FastAPI()

logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s:%(name)s:%(message)s")
log=logging.getLogger("trading")

# =========================================================
# 기본 설정
# =========================================================

VOLUME_HOURS=24
TOP_N=50
UPDATE_MINUTES=1
HISTORY_CHUNK=200
MAX_HISTORY_CHUNKS=10

USE_UPBIT="Y"
USE_OKX="N"

REQUEST_INTERVAL=.08
RATE_LIMIT_WAIT=3
MAX_RETRIES=10
KST=ZoneInfo("Asia/Seoul")

# =========================================================
# 전략 설정
# =========================================================

EMA_TIMEFRAME=240
EMA_HIGH_TIMEFRAME=60

EMA1_FAST=30
EMA1_MID=60
EMA1_SLOW=120
EMA1_MAX_COUNT=100

ROC_PERIOD=10

SUPPORTED_UPBIT_TIMEFRAMES={5,15,30,60,240}
SUPPORTED_OKX_TIMEFRAMES={5,15,30,60,120,240,360,480,720,1440}

# =========================================================
# 전역 변수
# =========================================================

latest_upbit_data=[]
latest_okx_data=[]
latest_usdt_krw=0
latest_upbit_update_time="-"
latest_okx_update_time="-"
latest_upbit_markets=[]

request_lock=threading.Lock()
update_lock=threading.Lock()
last_request_time=0

# =========================================================
# 기본 함수
# =========================================================

def kst():
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")

def format_timeframe(minutes):
    minutes=int(minutes)
    if minutes>=1440:return f"{minutes//1440}D"
    if minutes>=60:return f"{minutes//60}H"
    return f"{minutes}M"

def get_okx_bar(minutes):
    return {
        5:"5m",15:"15m",30:"30m",60:"1H",
        120:"2H",240:"4H",360:"6H",480:"8H",
        720:"12H",1440:"1D"
    }.get(int(minutes))

def get_okx_bar_minutes(bar):
    return {
        "1m":1,"3m":3,"5m":5,"15m":15,
        "30m":30,"1H":60,"2H":120,"4H":240,
        "6H":360,"8H":480,"12H":720,"1D":1440
    }.get(str(bar))

# =========================================================
# 정확한 KST 캔들 경계
# 240분 = 00 / 04 / 08 / 12 / 16 / 20
# =========================================================

def get_current_candle_start(minutes):
    minutes=int(minutes)
    now=datetime.now(KST)

    total=now.hour*60+now.minute
    block=(total//minutes)*minutes

    day_offset,block=divmod(block,1440)

    current=now.replace(
        hour=block//60,
        minute=block%60,
        second=0,
        microsecond=0
    )

    if day_offset:
        current-=pd.Timedelta(days=day_offset)

    return current.replace(tzinfo=None)

def validate_timeframe():
    global EMA_TIMEFRAME

    EMA_TIMEFRAME=int(EMA_TIMEFRAME)

    if EMA_TIMEFRAME not in SUPPORTED_UPBIT_TIMEFRAMES:
        raise ValueError(
            f"EMA_TIMEFRAME 오류: {EMA_TIMEFRAME}\n"
            "Upbit 지원값: 5,15,30,60,240"
        )

    if get_okx_bar(EMA_TIMEFRAME) is None:
        raise ValueError(
            f"OKX에서 지원하지 않는 시간봉: {EMA_TIMEFRAME}"
        )

# =========================================================
# API 요청
# =========================================================

def wait_request():
    global last_request_time

    with request_lock:
        gap=time.monotonic()-last_request_time

        if gap<REQUEST_INTERVAL:
            time.sleep(REQUEST_INTERVAL-gap)

        last_request_time=time.monotonic()

def retry(func,*args,**kwargs):
    url=args[0] if args and isinstance(args[0],str) else kwargs.get("url","")

    for n in range(MAX_RETRIES):
        try:
            wait_request()
            r=func(*args,**kwargs)

            if not hasattr(r,"status_code"):
                return r

            if r.status_code==200:
                return r

            if r.status_code==429:
                wait=min(RATE_LIMIT_WAIT*2**n,60)
            elif r.status_code>=500:
                wait=min(2*2**n,30)
            else:
                log.warning(f"[HTTP {r.status_code}] {url}")
                return r

            log.warning(f"[API 재시도] {url} {wait}초")
            time.sleep(wait)

        except Exception as e:
            log.error(f"[API 오류] {url}: {e}")

            if n<MAX_RETRIES-1:
                time.sleep(min(2*(n+1),20))

    log.error(f"[API 최종 실패] {url}")
    return None

# =========================================================
# UPBIT
# =========================================================

def get_upbit_markets():
    global latest_upbit_markets

    r=retry(
        requests.get,
        "https://api.upbit.com/v1/ticker/all",
        params={"quote_currencies":"KRW"},
        timeout=15
    )

    if r is None:
        return []

    try:
        result=[]

        for x in r.json():
            market=x.get("market","")

            if not market.startswith("KRW-"):
                continue

            try:
                volume=float(x["acc_trade_price_24h"])
                price=float(x["trade_price"])
            except Exception:
                continue

            if volume>0 and price>0:
                result.append({
                    "market":market,
                    "volume_24h":volume,
                    "current_price":price
                })

        latest_upbit_markets=[x["market"] for x in result]
        return result

    except Exception as e:
        log.error(f"업비트 마켓 오류: {e}")
        return []

def get_usdt_krw():
    r=retry(
        requests.get,
        "https://api.upbit.com/v1/ticker?markets=KRW-USDT",
        timeout=15
    )

    if r is None:
        return None

    try:
        price=float(r.json()[0]["trade_price"])
        return price if price>0 else None
    except Exception:
        return None

def get_upbit_candle(
    market,
    unit,
    count=200,
    to=None,
    include_current=False
):
    unit=int(unit)

    r=retry(
        requests.get,
        f"https://api.upbit.com/v1/candles/minutes/{unit}",
        params={
            "market":market,
            "count":min(max(int(count),1),200),
            **({"to":to} if to else {})
        },
        timeout=15
    )

    if r is None:
        return None

    try:
        df=pd.DataFrame(r.json())

        if df.empty:
            return None

        df["o"]=pd.to_numeric(df.opening_price,errors="coerce")
        df["h"]=pd.to_numeric(df.high_price,errors="coerce")
        df["l"]=pd.to_numeric(df.low_price,errors="coerce")
        df["c"]=pd.to_numeric(df.trade_price,errors="coerce")
        df["volume_krw"]=pd.to_numeric(
            df.candle_acc_trade_price,
            errors="coerce"
        )

        df["datetime"]=pd.to_datetime(
            df.candle_date_time_kst,
            errors="coerce"
        )

        df=df.dropna(
            subset=["datetime","o","h","l","c"]
        )

        if df.empty:
            return None

        if not include_current:
            current=get_current_candle_start(unit)
            df=df[df.datetime<current]

        if df.empty:
            return None

        return (
            df.sort_values("datetime")
            .drop_duplicates("datetime")
            .reset_index(drop=True)
        )

    except Exception as e:
        log.error(f"업비트 {unit}분 오류 {market}: {e}")
        return None

def history_upbit(market,unit,required=200):
    all_df=None
    to=None

    for _ in range(MAX_HISTORY_CHUNKS):
        df=get_upbit_candle(
            market,
            unit,
            HISTORY_CHUNK,
            to
        )

        if df is None or df.empty:
            break

        all_df=(
            df.copy()
            if all_df is None
            else pd.concat([df,all_df],ignore_index=True)
        )

        all_df=(
            all_df
            .drop_duplicates("datetime")
            .sort_values("datetime")
            .reset_index(drop=True)
        )

        if len(all_df)>=required:
            return all_df

        to=all_df.datetime.iloc[0].strftime(
            "%Y-%m-%dT%H:%M:%S"
        )

    return all_df

def get_upbit_current_roc_data(market,current_price):
    df=get_upbit_candle(
        market,
        EMA_TIMEFRAME,
        include_current=True
    )

    if df is None or df.empty:
        return None

    try:
        start=get_current_candle_start(EMA_TIMEFRAME)
        price=float(current_price)

        if price<=0:
            return df

        mask=df.datetime==start

        if mask.any():
            df.loc[mask,"c"]=price
        else:
            row=df.iloc[-1].copy()
            row["datetime"]=start
            row["c"]=price

            df=pd.concat(
                [df,pd.DataFrame([row])],
                ignore_index=True
            )

        return (
            df.sort_values("datetime")
            .drop_duplicates("datetime")
            .reset_index(drop=True)
        )

    except Exception as e:
        log.error(f"업비트 현재 ROC 오류 {market}: {e}")
        return df

# =========================================================
# OKX
# =========================================================

def get_okx_ohlcv(
    inst,
    bar="1H",
    limit=200,
    before=None,
    include_current=False
):
    params={
        "instId":inst,
        "bar":bar,
        "limit":min(max(int(limit),1),200)
    }

    if before is not None:
        params["before"]=str(before)

    r=retry(
        requests.get,
        "https://www.okx.com/api/v5/market/candles",
        params=params,
        timeout=15
    )

    if r is None:
        return None

    try:
        data=r.json().get("data",[])

        if not data:
            return None

        df=pd.DataFrame(
            data,
            columns=[
                "ts","o","h","l","c",
                "vol","volCcy","volCcyQuote","confirm"
            ]
        )

        for col in [
            "ts","o","h","l","c",
            "vol","volCcy","volCcyQuote"
        ]:
            df[col]=pd.to_numeric(
                df[col],
                errors="coerce"
            )

        if not include_current:
            df=df[df.confirm.astype(str)=="1"]

        df["datetime"]=(
            pd.to_datetime(
                df.ts,
                unit="ms",
                utc=True
            )
            .dt.tz_convert(KST)
            .dt.tz_localize(None)
        )

        if not include_current:
            minutes=get_okx_bar_minutes(bar)

            if minutes:
                current=get_current_candle_start(minutes)
                df=df[df.datetime<current]

        if df.empty:
            return None

        return (
            df.sort_values("ts")
            .drop_duplicates("ts")
            .reset_index(drop=True)
        )

    except Exception as e:
        log.error(f"OKX {inst} {bar} 오류: {e}")
        return None

def history_okx(inst,bar,required=200):
    all_df=None
    before=None

    for _ in range(MAX_HISTORY_CHUNKS):
        df=get_okx_ohlcv(
            inst,
            bar,
            HISTORY_CHUNK,
            before
        )

        if df is None or df.empty:
            break

        all_df=(
            df.copy()
            if all_df is None
            else pd.concat([df,all_df],ignore_index=True)
        )

        all_df=(
            all_df
            .drop_duplicates("ts")
            .sort_values("ts")
            .reset_index(drop=True)
        )

        if len(all_df)>=required:
            return all_df

        before=int(all_df.ts.iloc[0])

    return all_df

def get_okx_current_price(inst):
    r=retry(
        requests.get,
        "https://www.okx.com/api/v5/market/ticker",
        params={"instId":inst},
        timeout=15
    )

    if r is None:
        return None

    try:
        price=float(r.json()["data"][0]["last"])
        return price if price>0 else None
    except Exception:
        return None

def get_okx_symbols():
    r=retry(
        requests.get,
        "https://www.okx.com/api/v5/public/instruments",
        params={"instType":"SWAP"},
        timeout=15
    )

    if r is None:
        return []

    try:
        return [
            x["instId"]
            for x in r.json().get("data",[])
            if x.get("instId","").endswith("-USDT-SWAP")
            and x.get("state")=="live"
        ]
    except Exception:
        return []

def get_okx_volume(inst,usdt):
    df=get_okx_ohlcv(
        inst,
        "1H",
        VOLUME_HOURS
    )

    if df is None or df.empty:
        return None

    try:
        volume=pd.to_numeric(
            df.volCcyQuote,
            errors="coerce"
        ).sum()

        return float(volume)*float(usdt)

    except Exception:
        return None

# =========================================================
# EMA
# =========================================================

def ema(df,period):
    if df is None or df.empty or "c" not in df:
        return None

    return pd.to_numeric(
        df["c"],
        errors="coerce"
    ).ewm(
        span=period,
        adjust=False,
        min_periods=1
    ).mean()

def ema_alignment_count(df):
    if df is None or df.empty:
        return {
            "direction":"none",
            "count":0
        }

    try:
        e30=ema(df,EMA1_FAST)
        e60=ema(df,EMA1_MID)
        e120=ema(df,EMA1_SLOW)

        def get_dir(i):
            a=float(e30.iloc[i])
            b=float(e60.iloc[i])
            c=float(e120.iloc[i])

            if a>b>c:
                return "long"

            if a<b<c:
                return "short"

            return "none"

        current=get_dir(-1)

        if current=="none":
            return {
                "direction":"none",
                "count":0
            }

        count=0

        for i in range(len(df)-1,-1,-1):
            if get_dir(i)==current:
                count+=1
            else:
                break

        return {
            "direction":current,
            "count":count
        }

    except Exception as e:
        log.error(f"EMA 배열 오류: {e}")
        return {
            "direction":"none",
            "count":0
        }

def ema_display(df,current_price=None):
    x=ema_alignment_count(df)
    d=x["direction"]

    icon={
        "long":"🟢",
        "short":"🔴"
    }.get(d,"⚪")

    return {
        "display":f"{icon}({x['count']})",
        "direction":d,
        "count":x["count"],
        "current_price":current_price
    }

# =========================================================
# ROC
# =========================================================

def roc(df,period=ROC_PERIOD):
    if df is None or df.empty or "c" not in df:
        return None

    try:
        close=pd.to_numeric(
            df["c"],
            errors="coerce"
        )

        return (
            close/close.shift(int(period))-1
        )*100

    except Exception as e:
        log.error(f"ROC 계산 오류: {e}")
        return None

def roc_count(series,positive=True):
    count=0

    for value in reversed(series.tolist()):
        if pd.isna(value):
            break

        if (float(value)>0)==positive:
            count+=1
        else:
            break

    return count

def roc_analysis(df_confirmed,df_current):
    result={
        "roc10":None,
        "roc10_previous":None,
        "roc10_count":0,
        "roc10_negative_count":0,
        "long_candidate":False,
        "short_candidate":False,
        "state":"none",
        "display":"-"
    }

    if (
        df_confirmed is None
        or df_confirmed.empty
        or df_current is None
        or df_current.empty
    ):
        return result

    try:
        confirmed=roc(df_confirmed)
        current=roc(df_current)

        if confirmed is None or current is None:
            return result

        previous=float(confirmed.iloc[-1])
        current_value=float(current.iloc[-1])

        if pd.isna(previous) or pd.isna(current_value):
            return result

        positive_count=roc_count(current,True)
        negative_count=roc_count(current,False)

        long_cross=previous<=0 and current_value>0
        short_cross=previous>=0 and current_value<0

        result.update({
            "roc10":current_value,
            "roc10_previous":previous,
            "roc10_count":positive_count,
            "roc10_negative_count":negative_count,
            "long_candidate":long_cross,
            "short_candidate":short_cross
        })

        if long_cross:
            result.update({
                "state":"long",
                "display":"🟢 매수 ①"
            })

        elif short_cross:
            result.update({
                "state":"short",
                "display":"🔴 숏 ①"
            })

        elif current_value>0 and positive_count>=2:
            result.update({
                "state":"progress",
                "display":f"🚀 진행 {positive_count}"
            })

        elif current_value<0 and negative_count>=2:
            result.update({
                "state":"short_progress",
                "display":f"📉 진행 {negative_count}"
            })

        return result

    except Exception as e:
        log.error(f"ROC 분석 오류: {e}")
        return result

# =========================================================
# 등락률
# =========================================================

def daily_change_upbit(market):
    r=retry(
        requests.get,
        "https://api.upbit.com/v1/candles/days",
        params={
            "market":market,
            "count":2
        },
        timeout=15
    )

    if r is None:
        return None

    try:
        data=r.json()

        if len(data)<2:
            return None

        current=float(data[0]["trade_price"])
        previous=float(data[1]["trade_price"])

        if previous==0:
            return None

        return [
            (current-previous)/previous*100
        ]

    except Exception:
        return None

def daily_changes(df):
    if df is None or df.empty:
        return None

    try:
        x=df.copy()

        x["datetime"]=pd.to_datetime(
            x.datetime,
            errors="coerce"
        )

        x["c"]=pd.to_numeric(
            x.c,
            errors="coerce"
        )

        x=x.dropna(
            subset=["datetime","c"]
        ).set_index("datetime")

        daily=x.c.resample(
            "1D",
            offset="9h"
        ).last().dropna()

        if len(daily)<2:
            return None

        previous=float(daily.iloc[-2])
        current=float(daily.iloc[-1])

        if previous==0:
            return None

        return [
            (current-previous)/previous*100
        ]

    except Exception:
        return None

def get_change_value(x):
    try:
        if x is None:
            return None

        return float(
            x[0]
            if isinstance(x,(list,tuple))
            else x
        )

    except Exception:
        return None

def format_change(x):
    x=get_change_value(x)

    if x is None:
        return "-"

    if x>0:
        return f'<span class="up">▲ +{x:.2f}%</span>'

    if x<0:
        return f'<span class="down">▼ {x:.2f}%</span>'

    return '<span class="zero">0.00%</span>'

def format_volume(v):
    try:
        v=float(v)
    except Exception:
        return "-"

    if v>=1e12:
        return f"{v/1e12:.2f}조"

    if v>=1e8:
        return f"{v/1e8:.0f}억"

    if v>=1e4:
        return f"{v/1e4:.0f}만"

    return f"{v:,.0f}"

# =========================================================
# 분석
# =========================================================

def empty_analysis():
    e={
        "display":"⚪(0)",
        "direction":"none",
        "count":0,
        "current_price":None
    }

    return {
        "ema_1h":e.copy(),
        "ema_high":e.copy(),
        "roc":{
            "roc10":None,
            "roc10_previous":None,
            "roc10_count":0,
            "roc10_negative_count":0,
            "long_candidate":False,
            "short_candidate":False,
            "state":"none",
            "display":"-"
        },
        "changes":None,
        "qualified":False,
        "short_qualified":False,
        "progress_qualified":False,
        "short_progress_qualified":False,
        "direction_1h":"none",
        "df1h":None
    }

def analyze(
    market,
    okx=False,
    current_price=None
):
    if okx:
        bar=get_okx_bar(EMA_TIMEFRAME)

        if not bar:
            return None

        df_confirmed=history_okx(
            market,
            bar
        )

        df_high=history_okx(
            market,
            get_okx_bar(EMA_HIGH_TIMEFRAME)
        )

        df_current=get_okx_ohlcv(
            market,
            bar,
            200,
            include_current=True
        )

        if (
            df_current is not None
            and not df_current.empty
            and current_price is not None
        ):
            start=get_current_candle_start(
                EMA_TIMEFRAME
            )

            mask=df_current.datetime==start

            if mask.any():
                df_current.loc[
                    mask,
                    "c"
                ]=float(current_price)

        changes=daily_changes(df_confirmed)

    else:
        df_confirmed=history_upbit(
            market,
            EMA_TIMEFRAME
        )

        df_high=history_upbit(
            market,
            EMA_HIGH_TIMEFRAME
        )

        df_current=get_upbit_current_roc_data(
            market,
            current_price
        )

        changes=daily_change_upbit(market)

    if df_confirmed is None or df_confirmed.empty:
        return None

    e1=ema_display(
        df_confirmed,
        current_price
    )

    e_high=ema_display(
        df_high,
        current_price
    )

    r=roc_analysis(
        df_confirmed,
        df_current
    )

    base=(
        e1["direction"] in ("long","short")
        and e1["count"]<=EMA1_MAX_COUNT
    )

    long_qualified=(
        base
        and e1["direction"]=="long"
        and r["long_candidate"]
    )

    short_qualified=(
        base
        and e1["direction"]=="short"
        and r["short_candidate"]
    )

    progress_qualified=(
        base
        and e1["direction"]=="long"
        and r["roc10"] is not None
        and r["roc10"]>0
        and r["roc10_count"]>=2
    )

    short_progress_qualified=(
        base
        and e1["direction"]=="short"
        and r["roc10"] is not None
        and r["roc10"]<0
        and r["roc10_negative_count"]>=2
    )

    return {
        "ema_1h":e1,
        "ema_high":e_high,
        "roc":r,
        "changes":changes,
        "qualified":long_qualified,
        "short_qualified":short_qualified,
        "progress_qualified":progress_qualified,
        "short_progress_qualified":short_progress_qualified,
        "direction_1h":e1["direction"],
        "df1h":df_confirmed
    }

def make_row(
    rank,
    name,
    volume,
    analysis,
    current_price=None
):
    a=analysis or empty_analysis()

    return {
        "rank":rank,
        "name":name,
        "change":format_change(a["changes"]),
        "change_value":get_change_value(a["changes"]),
        "volume":format_volume(volume),
        "current_price":current_price,
        "ema_1h":a["ema_1h"],
        "ema_high":a["ema_high"],
        "roc":a["roc"],
        "qualified":a["qualified"],
        "short_qualified":a["short_qualified"],
        "progress_qualified":a["progress_qualified"],
        "short_progress_qualified":a["short_progress_qualified"],
        "direction":a["direction_1h"]
    }

def is_buy(row):
    return bool(row and row.get("qualified"))

def is_short(row):
    return bool(row and row.get("short_qualified"))

def is_progress(row):
    return bool(row and row.get("progress_qualified"))

def is_short_progress(row):
    return bool(row and row.get("short_progress_qualified"))

# =========================================================
# 업데이트
# =========================================================

def update_upbit():
    global latest_upbit_data,latest_upbit_update_time

    log.info(f"========== 업비트 TOP{TOP_N} ==========")

    markets=sorted(
        get_upbit_markets(),
        key=lambda x:x["volume_24h"],
        reverse=True
    )

    rows=[]

    for rank,item in enumerate(
        markets[:TOP_N],
        1
    ):
        market=item["market"]
        coin=market.replace("KRW-","")
        price=item["current_price"]

        try:
            a=analyze(
                market,
                current_price=price
            )
        except Exception as e:
            log.error(
                f"업비트 상세 오류 {market}: {e}"
            )
            a=None

        rows.append(
            make_row(
                rank,
                coin,
                item["volume_24h"],
                a,
                price
            )
        )

    latest_upbit_data=rows
    latest_upbit_update_time=kst()

    log.info(
        f"업비트 완료 / "
        f"매수 {sum(is_buy(x) for x in rows)}개 / "
        f"진행 {sum(is_progress(x) for x in rows)}개"
    )

def update_okx(usdt):
    global latest_okx_data,latest_okx_update_time

    if not usdt or usdt<=0:
        return False

    symbols=get_okx_symbols()

    if not symbols:
        return False

    upbit_set={
        x.replace("KRW-","")
        for x in latest_upbit_markets
    }

    volumes={}

    for symbol in symbols:
        v=get_okx_volume(
            symbol,
            usdt
        )

        if v and v>0:
            volumes[symbol]=v

    top=sorted(
        volumes,
        key=volumes.get,
        reverse=True
    )[:TOP_N]

    rows=[]

    for rank,symbol in enumerate(top,1):
        coin=symbol.replace(
            "-USDT-SWAP",
            ""
        )

        name=(
            f"{coin} (업비트)"
            if coin in upbit_set
            else coin
        )

        try:
            price=get_okx_current_price(symbol)

            a=analyze(
                symbol,
                True,
                price
            )

        except Exception as e:
            log.error(
                f"OKX 상세 오류 {symbol}: {e}"
            )
            price=None
            a=None

        rows.append(
            make_row(
                rank,
                name,
                volumes[symbol],
                a,
                price
            )
        )

    latest_okx_data=rows
    latest_okx_update_time=kst()

    log.info(
        f"OKX 완료 / "
        f"매수 {sum(is_buy(x) for x in rows)}개 / "
        f"진행 {sum(is_progress(x) for x in rows)}개 / "
        f"숏 {sum(is_short(x) for x in rows)}개 / "
        f"숏진행 {sum(is_short_progress(x) for x in rows)}개"
    )

    return True

def update_dashboard():
    global latest_usdt_krw
    global latest_upbit_data
    global latest_okx_data

    if not update_lock.acquire(False):
        log.warning("이전 조회 진행 중 → 건너뜀")
        return

    try:
        log.info(
            f"========== 전체 조회 {kst()} =========="
        )

        if USE_UPBIT=="Y":
            try:
                update_upbit()
            except Exception as e:
                log.exception(
                    f"업비트 업데이트 오류: {e}"
                )
        else:
            latest_upbit_data=[]

        if USE_OKX=="Y":
            try:
                usdt=get_usdt_krw()

                if usdt:
                    latest_usdt_krw=usdt
                else:
                    usdt=latest_usdt_krw

                if usdt>0:
                    update_okx(usdt)

            except Exception as e:
                log.exception(
                    f"OKX 업데이트 오류: {e}"
                )
        else:
            latest_okx_data=[]

    finally:
        update_lock.release()

# =========================================================
# HTML
# =========================================================

def roc_html(r):
    if not r:
        return (
            '<div class="roc-cell">'
            '<b>ROC10(0)</b><span>-</span>'
            '</div>'
        )

    value=r.get("roc10")
    previous=r.get("roc10_previous")

    if value is None or previous is None:
        return (
            '<div class="roc-cell">'
            '<b>ROC10(0)</b><span>-</span>'
            '</div>'
        )

    count=(
        r.get("roc10_count",0)
        if value>0
        else r.get("roc10_negative_count",0)
        if value<0
        else 0
    )

    cls=(
        "roc-positive"
        if value>0
        else "roc-negative"
        if value<0
        else "roc-zero"
    )

    cross=(
        '<i class="up">↑0</i>'
        if previous<=0<value
        else '<i class="down">↓0</i>'
        if previous>=0>value
        else '<i class="muted">—</i>'
    )

    return f"""
    <div class="roc-cell">
        <b>ROC10({count})</b>
        <span class="{cls}">
            {value:+.3f}% {cross}
        </span>
    </div>
    """

def signal_html(row):
    if row.get("qualified"):
        return '<b class="buy">🟢 매수 ①</b>'

    if row.get("progress_qualified"):
        return (
            f'<b class="progress">'
            f'🚀 진행 {row["roc"].get("roc10_count",0)}'
            f'</b>'
        )

    if row.get("short_qualified"):
        return '<b class="short">🔴 숏 ①</b>'

    if row.get("short_progress_qualified"):
        return (
            f'<b class="short-progress">'
            f'📉 진행 {row["roc"].get("roc10_negative_count",0)}'
            f'</b>'
        )

    return '<span class="muted">-</span>'

def ema_html(e):
    if not e:
        return "⚪(0)"

    d=e.get("direction","none")
    count=e.get("count",0)

    icon={
        "long":"🟢",
        "short":"🔴"
    }.get(d,"⚪")

    return f"{icon}({count})"

def row_class(x):
    if x.get("qualified"):
        return "qualified"

    if x.get("progress_qualified"):
        return "progress-qualified"

    if x.get("short_qualified"):
        return "short-qualified"

    if x.get("short_progress_qualified"):
        return "short-progress-qualified"

    return ""

def rows_html(data,focus=None):
    out=[]

    for x in data:
        if focus=="buy":
            cls="qualified"
        elif focus=="progress":
            cls="progress-qualified"
        elif focus=="short":
            cls="short-qualified"
        elif focus=="short_progress":
            cls="short-progress-qualified"
        else:
            cls=row_class(x)

        out.append(f"""
        <tr class="{cls}">
            <td>{x.get("rank","-")}</td>

            <td class="coin">
                <b>{x.get("name","-")}</b>
                <small>{x.get("change","-")}</small>
            </td>

            <td class="vol">
                {x.get("volume","-")}
            </td>

            <td class="ema">
                <small>EMA</small>
                <div>
                    {format_timeframe(EMA_TIMEFRAME)}
                    {ema_html(x.get("ema_1h"))}
                </div>
                <div>
                    {format_timeframe(EMA_HIGH_TIMEFRAME)}
                    {ema_html(x.get("ema_high"))}
                </div>
            </td>

            <td>
                {roc_html(x.get("roc",{}))}
            </td>

            <td class="signal">
                {signal_html(x)}
            </td>
        </tr>
        """)

    return "".join(out)

def table_html(data,focus=None):
    rows=rows_html(data,focus)

    if not rows:
        rows=(
            '<tr><td colspan="6" class="empty">'
            '현재 후보 없음'
            '</td></tr>'
        )

    return f"""
    <div class="table-wrap">
        <table>
            <thead>
                <tr>
                    <th>#</th>
                    <th>코인</th>
                    <th>거래대금</th>
                    <th>EMA</th>
                    <th>ROC10</th>
                    <th>신호</th>
                </tr>
            </thead>
            <tbody>{rows}</tbody>
        </table>
    </div>
    """

def focus_section(
    title,
    data,
    update_time,
    checker,
    focus,
    description,
    sort_key=None,
    reverse=False
):
    rows=[
        x for x in data
        if checker(x)
    ]

    if sort_key:
        rows.sort(
            key=lambda x:
                x.get("roc",{}).get(
                    sort_key,
                    0
                ) or 0,
            reverse=reverse
        )

    return f"""
    <h2 class="{focus}-title">
        {title}
        <small>
            {description} · {update_time} KST
        </small>
    </h2>

    {table_html(rows,focus)}
    """

def section(title,data,update_time):
    return f"""
    <h2>
        🏆 {title} TOP{TOP_N}
        <small>{update_time} KST</small>
    </h2>

    {table_html(data)}
    """

# =========================================================
# CSS
#
# ★ 글자 크기 통일
# ★ 모바일/PC에서 글자 크기를 별도로 변경하지 않음
# ★ @media에서 폰트 크기 덮어쓰기 제거
# =========================================================

CSS="""
*{
    box-sizing:border-box;
    -webkit-tap-highlight-color:transparent;
}

html,body{
    margin:0;
    padding:0;
    width:100%;
    overflow-x:hidden;
}

body{
    background:#0d1014;
    color:#eee;
    font-family:
        -apple-system,
        BlinkMacSystemFont,
        "Segoe UI",
        Arial,
        sans-serif;
    font-size:10px;
    padding:7px 5px 15px;
}

h1{
    margin:3px;
    font-size:16px;
    line-height:21px;
    font-weight:800;
}

h2{
    margin:14px 3px 6px;
    font-size:13px;
    line-height:18px;
    font-weight:800;
}

h2 small{
    color:#747b85;
    font-size:8px;
    font-weight:400;
    margin-left:5px;
}

.info{
    margin:0 2px 9px;
    padding:8px 9px;
    color:#aab0b8;
    background:#15191f;
    border:1px solid #252b33;
    border-radius:9px;
    font-size:8px;
    line-height:1.55;
}

.status{
    display:flex;
    justify-content:center;
    gap:14px;
    margin-top:7px;
    padding-top:6px;
    border-top:1px solid #252a31;
    font-weight:800;
    font-size:8px;
}

.y,
.buy,
.roc-positive{
    color:#39e875;
}

.n,
.short,
.roc-negative{
    color:#ff5555;
}

.progress{
    color:#4cc9ff;
}

.short-progress{
    color:#ff6666;
}

.muted,
.roc-zero{
    color:#747b85;
}

/* =========================
   TABLE
   ========================= */

.table-wrap{
    width:100%;
    overflow-x:auto;
    overflow-y:hidden;
    border-radius:9px;
    border:1px solid #282e36;
    background:#171b20;
}

table{
    width:100%;
    min-width:520px;
    table-layout:fixed;
    border-collapse:collapse;
    background:#171b20;
}

thead{
    background:#111419;
}

th{
    height:29px;
    padding:5px 2px;
    border-bottom:1px solid #2c323a;
    color:#9299a3;
    font-size:8px;
    font-weight:700;
    text-align:center;
    white-space:nowrap;
}

td{
    height:48px;
    padding:5px 2px;
    border-bottom:1px solid #272d34;
    text-align:center;
    vertical-align:middle;
    overflow:hidden;
    font-size:9px;
    white-space:nowrap;
}

tr:last-child td{
    border-bottom:none;
}

/* =========================
   COLUMN WIDTH
   ========================= */

th:nth-child(1),
td:nth-child(1){
    width:6%;
}

th:nth-child(2),
td:nth-child(2){
    width:20%;
}

th:nth-child(3),
td:nth-child(3){
    width:14%;
}

th:nth-child(4),
td:nth-child(4){
    width:18%;
}

th:nth-child(5),
td:nth-child(5){
    width:24%;
}

th:nth-child(6),
td:nth-child(6){
    width:18%;
}

/* =========================
   COIN
   ========================= */

.coin{
    overflow:hidden;
}

.coin b{
    display:block;
    font-size:10px;
    line-height:13px;
    font-weight:800;
    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;
}

.coin small{
    display:block;
    margin-top:2px;
    font-size:7px;
    line-height:10px;
    white-space:nowrap;
}

/* =========================
   VOLUME
   ========================= */

.vol{
    font-size:9px;
    font-weight:700;
}

/* =========================
   EMA
   ========================= */

.ema{
    text-align:left!important;
    font-weight:800;
}

.ema small{
    display:block;
    color:#858c96;
    font-size:7px;
    line-height:9px;
}

.ema div{
    font-size:9px;
    line-height:17px;
    white-space:nowrap;
}

/* =========================
   ROC
   ========================= */

.roc-cell{
    display:flex;
    flex-direction:column;
    align-items:center;
    justify-content:center;
    min-height:41px;
}

.roc-cell b{
    color:#858c96;
    font-size:7px;
    line-height:10px;
}

.roc-cell span{
    font-size:9px;
    line-height:13px;
    font-weight:900;
    white-space:nowrap;
}

.roc-cell i{
    font-style:normal;
    font-size:7px;
    margin-left:2px;
}

/* =========================
   SIGNAL
   ========================= */

.signal{
    font-size:9px;
    font-weight:800;
}

.signal b{
    font-size:9px;
}

/* =========================
   ROW HIGHLIGHT
   ========================= */

.qualified{
    background:rgba(57,232,117,.055);
}

.progress-qualified{
    background:rgba(76,201,255,.055);
}

.short-qualified{
    background:rgba(255,85,85,.055);
}

.short-progress-qualified{
    background:rgba(255,85,85,.035);
}

.empty{
    padding:14px;
    color:#555d67;
    font-size:9px;
}

/* =========================
   SECTION TITLE
   ========================= */

.buy-title{
    color:#39e875;
}

.progress-title{
    color:#4cc9ff;
}

.short-title{
    color:#ff5555;
}

.short_progress-title{
    color:#ff6666;
}

/* =========================
   MOBILE
   글자 크기는 여기서 변경하지 않음
   ========================= */

@media(max-width:600px){
    body{
        padding:7px 4px 15px;
    }

    .table-wrap{
        border-radius:8px;
    }

    table{
        min-width:520px;
    }
}

/* =========================
   DESKTOP
   글자 크기는 동일
   ========================= */

@media(min-width:601px){
    body{
        max-width:900px;
        margin:auto;
        padding:9px;
    }
}
"""

# =========================================================
# DASHBOARD
# =========================================================

@app.get("/",response_class=HTMLResponse)
def dashboard():
    tf=format_timeframe(EMA_TIMEFRAME)

    status=f"""
    <div class="status">
        <span>
            업비트 :
            <b class="y">{USE_UPBIT}</b>
        </span>

        <span>
            OKX :
            <b class="n">{USE_OKX}</b>
        </span>
    </div>
    """

    sections=""

    if USE_UPBIT=="Y":
        sections+=focus_section(
            "🟢 매수",
            latest_upbit_data,
            latest_upbit_update_time,
            is_buy,
            "buy",
            "ROC10 0선 상향돌파"
        )

        sections+=focus_section(
            "🚀 진행",
            latest_upbit_data,
            latest_upbit_update_time,
            is_progress,
            "progress",
            "ROC10 양수 유지 · ②+",
            "roc10_count"
        )

    if USE_OKX=="Y":
        sections+=focus_section(
            "🟢 매수",
            latest_okx_data,
            latest_okx_update_time,
            is_buy,
            "buy",
            "ROC10 0선 상향돌파"
        )

        sections+=focus_section(
            "🚀 진행",
            latest_okx_data,
            latest_okx_update_time,
            is_progress,
            "progress",
            "ROC10 양수 유지 · ②+",
            "roc10_count"
        )

        sections+=focus_section(
            "🔴 숏",
            latest_okx_data,
            latest_okx_update_time,
            is_short,
            "short",
            "ROC10 0선 하향돌파 · ①"
        )

        sections+=focus_section(
            "📉 진행",
            latest_okx_data,
            latest_okx_update_time,
            is_short_progress,
            "short_progress",
            "ROC10 음수 유지 · ②+",
            "roc10_negative_count"
        )

    if USE_UPBIT=="Y":
        sections+=section(
            "업비트",
            latest_upbit_data,
            latest_upbit_update_time
        )

    if USE_OKX=="Y":
        sections+=section(
            "OKX",
            latest_okx_data,
            latest_okx_update_time
        )

    return f"""
    <!DOCTYPE html>
    <html lang="ko">

    <head>
        <meta charset="UTF-8">

        <meta
            name="viewport"
            content="width=device-width,
                     initial-scale=1,
                     viewport-fit=cover"
        >

        <meta
            http-equiv="refresh"
            content="60"
        >

        <meta
            name="theme-color"
            content="#0d1014"
        >

        <title>
            {tf} EMA30·60·120 · ROC10
        </title>

        <style>
            {CSS}
        </style>
    </head>

    <body>

        <h1>
            📊 TRADING SIGNAL CENTER
        </h1>

        <div class="info">
            {tf} EMA30·60·120 + ROC10<br>
            🟢 매수 = 0선 상향돌파 ①<br>
            🚀 진행 = 양수 유지 ②+<br>
            🔴 숏 = 0선 하향돌파 ①<br>
            📉 진행 = 음수 유지 ②+<br>
            ROC10 = 현재가 기준
            {status}
        </div>

        {sections}

    </body>
    </html>
    """

# =========================================================
# SCHEDULER
# =========================================================

def scheduler():
    log.info("스케줄러 시작")

    while True:
        try:
            schedule.run_pending()

        except Exception as e:
            log.exception(
                f"스케줄러 오류: {e}"
            )

        time.sleep(1)

# =========================================================
# STARTUP
# =========================================================

@app.on_event("startup")
def startup():

    if USE_UPBIT not in ("Y","N"):
        raise ValueError(
            "USE_UPBIT은 Y 또는 N만 가능합니다."
        )

    if USE_OKX not in ("Y","N"):
        raise ValueError(
            "USE_OKX는 Y 또는 N만 가능합니다."
        )

    validate_timeframe()

    tf=format_timeframe(
        EMA_TIMEFRAME
    )

    log.info("========================================")
    log.info(
        f"{tf} EMA30·60·120 + ROC10 시작"
    )
    log.info(
        f"업비트={USE_UPBIT} / OKX={USE_OKX}"
    )
    log.info(
        f"TOP={TOP_N} / UPDATE={UPDATE_MINUTES}분"
    )
    log.info(
        f"EMA={tf} / EMA30-60-120"
    )
    log.info(
        f"EMA count <= {EMA1_MAX_COUNT}"
    )
    log.info(
        "롱: ROC 0선 상향 → 매수① → 진행②+"
    )
    log.info(
        "숏: ROC 0선 하향 → 숏① → 진행②+"
    )
    log.info(
        "돌파 기능 = OFF"
    )
    log.info(
        f"OKX bar={get_okx_bar(EMA_TIMEFRAME)}"
    )
    log.info(
        f"표시용 HIGH EMA={format_timeframe(EMA_HIGH_TIMEFRAME)}"
    )
    log.info("========================================")

    threading.Thread(
        target=update_dashboard,
        daemon=True
    ).start()

    schedule.every(
        UPDATE_MINUTES
    ).minutes.do(
        update_dashboard
    )

    threading.Thread(
        target=scheduler,
        daemon=True
    ).start()

# =========================================================
# RUN
# =========================================================

if __name__=="__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )
