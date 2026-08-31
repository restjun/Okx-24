from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse

import requests
import uuid
import jwt
import hashlib
import logging
import sqlite3
import threading
import time
import json

from datetime import datetime
from urllib.parse import urlencode
from zoneinfo import ZoneInfo


# =========================================================
# FastAPI
# =========================================================

app = FastAPI()


# =========================================================
# 로그
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# =========================================================
# 업비트
# =========================================================

SERVER_URL = "https://api.upbit.com"

REQUEST_TIMEOUT = 10

ORDER_WAIT_TIMEOUT = 15

ORDER_WAIT_INTERVAL = 0.5

MIN_ORDER_KRW = 5000


# =========================================================
# 월 시작금액
# =========================================================

MONTH_START_AMOUNT = 3_000_000.0


# =========================================================
# 전체 시드 손실한도
# 1%
# =========================================================

MAX_LOSS_RATE = 0.01


# =========================================================
# 한국시간
# =========================================================

KST = ZoneInfo("Asia/Seoul")


# =========================================================
# SQLite
# =========================================================

DB_FILE = "trading.db"

db_lock = threading.Lock()


# =========================================================
# 최신 업비트 자산
# =========================================================

latest_upbit_assets = []

latest_upbit_total_krw = 0.0

latest_upbit_available_krw = 0.0

latest_upbit_update = "TradingView 신호 대기"

latest_order_info = "주문 없음"


# =========================================================
# 마지막 매수 수수료율
# =========================================================

latest_bid_fee_rate = 0.0


# =========================================================
# 숫자 변환
# =========================================================

def safe_float(value, default=0.0):

    if value is None:
        return default

    if isinstance(value, str):

        value = value.strip()

        if value == "":
            return default

    try:
        return float(value)

    except Exception:
        return default


# =========================================================
# 문자열 변환
# =========================================================

def safe_string(value, default=""):

    if value is None:
        return default

    try:
        return str(value).strip()

    except Exception:
        return default


# =========================================================
# TradingView 데이터 추출
# =========================================================

def get_payload_value(
    data: dict,
    names,
    default=None
):

    for name in names:

        if name in data:
            return data[name]

    return default


# =========================================================
# DB 연결
# =========================================================

def get_db():

    conn = sqlite3.connect(
        DB_FILE,
        timeout=30
    )

    conn.row_factory = sqlite3.Row

    return conn


# =========================================================
# DB 초기화
# =========================================================

def init_db():

    with db_lock:

        conn = get_db()

        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS trades (

            id INTEGER PRIMARY KEY AUTOINCREMENT,

            uuid TEXT UNIQUE,

            coin TEXT NOT NULL,

            side TEXT NOT NULL,

            order_amount REAL DEFAULT 0,

            executed_funds REAL DEFAULT 0,

            executed_volume REAL DEFAULT 0,

            avg_price REAL DEFAULT 0,

            fee REAL DEFAULT 0,

            requested_ratio REAL DEFAULT 0,

            created_at TEXT,

            completed_at TEXT,

            state TEXT,

            realized_cost REAL DEFAULT 0,

            realized_profit REAL DEFAULT 0,

            realized_return REAL DEFAULT 0

        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS buy_lots (

            id INTEGER PRIMARY KEY AUTOINCREMENT,

            trade_id INTEGER NOT NULL,

            coin TEXT NOT NULL,

            original_volume REAL NOT NULL,

            remaining_volume REAL NOT NULL,

            cost_per_unit REAL NOT NULL,

            total_cost REAL NOT NULL,

            fee REAL DEFAULT 0,

            created_at TEXT,

            FOREIGN KEY(trade_id)
                REFERENCES trades(id)

        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS sell_allocations (

            id INTEGER PRIMARY KEY AUTOINCREMENT,

            sell_trade_id INTEGER NOT NULL,

            buy_lot_id INTEGER NOT NULL,

            volume REAL NOT NULL,

            cost REAL NOT NULL,

            FOREIGN KEY(sell_trade_id)
                REFERENCES trades(id),

            FOREIGN KEY(buy_lot_id)
                REFERENCES buy_lots(id)

        )
        """)

        conn.commit()

        conn.close()


# =========================================================
# 코인명 정리
# =========================================================

def clean_coin_name(coin):

    coin = safe_string(coin).upper()

    coin = (
        coin
        .replace("USDT.P", "")
        .replace("USDT", "")
        .replace("KRW-", "")
        .replace("KRW", "")
        .strip()
    )

    return coin


# =========================================================
# 현재시간
# 한국시간 기준
# =========================================================

def now_string():

    return datetime.now(
        KST
    ).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


# =========================================================
# Query String
# =========================================================

def build_query_string(data: dict):

    return urlencode(
        data,
        doseq=True
    )


# =========================================================
# JWT
# =========================================================

def create_jwt(
    api_key,
    secret_key,
    query_string=""
):

    payload = {
        "access_key": api_key,
        "nonce": str(uuid.uuid4())
    }

    if query_string:

        query_hash = hashlib.sha512(
            query_string.encode("utf-8")
        ).hexdigest()

        payload["query_hash"] = query_hash

        payload["query_hash_alg"] = "SHA512"

    token = jwt.encode(
        payload,
        secret_key,
        algorithm="HS512"
    )

    if isinstance(token, bytes):

        token = token.decode("utf-8")

    return token


# =========================================================
# 인증 Header
# =========================================================

def create_auth_headers(
    api_key,
    secret_key,
    query_string=""
):

    token = create_jwt(
        api_key,
        secret_key,
        query_string
    )

    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }


# =========================================================
# 오류 응답
# =========================================================

def get_error_detail(response):

    try:
        return response.json()

    except Exception:

        return {
            "status_code": response.status_code,
            "text": response.text
        }


# =========================================================
# 수량 절삭
# =========================================================

def truncate_volume(
    volume,
    decimals=8
):

    factor = 10 ** decimals

    return int(
        volume * factor
    ) / factor


# =========================================================
# KRW 절삭
# =========================================================

def truncate_krw(amount):

    return float(
        int(amount)
    )


# =========================================================
# 현재가 조회
# =========================================================

def get_ticker_price(market):

    try:

        response = requests.get(
            f"{SERVER_URL}/v1/ticker",
            params={
                "markets": market
            },
            timeout=REQUEST_TIMEOUT
        )

        if response.status_code != 200:

            logger.error(
                f"Ticker error | "
                f"{market} | "
                f"{response.status_code}"
            )

            return 0.0

        data = response.json()

        if not data:
            return 0.0

        return float(
            data[0]["trade_price"]
        )

    except Exception as e:

        logger.error(
            f"Ticker exception | "
            f"{market} | {e}"
        )

        return 0.0


# =========================================================
# 업비트 주문 가능 정보 + 매수 수수료 조회
# =========================================================

def get_order_chance(
    market,
    api_key,
    secret_key
):

    global latest_bid_fee_rate

    query = {
        "market": market
    }

    query_string = build_query_string(
        query
    )

    headers = create_auth_headers(
        api_key,
        secret_key,
        query_string
    )

    try:

        response = requests.get(
            SERVER_URL + "/v1/orders/chance",
            params=query,
            headers=headers,
            timeout=REQUEST_TIMEOUT
        )

    except requests.RequestException as e:

        logger.error(
            f"ORDER CHANCE REQUEST FAILED | "
            f"{market} | {e}"
        )

        return {
            "bid_fee": 0.0,
            "min_total": MIN_ORDER_KRW
        }

    if response.status_code != 200:

        logger.error(
            f"ORDER CHANCE ERROR | "
            f"{market} | "
            f"{response.status_code} | "
            f"{response.text}"
        )

        return {
            "bid_fee": 0.0,
            "min_total": MIN_ORDER_KRW
        }

    try:

        data = response.json()

    except Exception as e:

        logger.error(
            f"ORDER CHANCE JSON ERROR | "
            f"{market} | {e}"
        )

        return {
            "bid_fee": 0.0,
            "min_total": MIN_ORDER_KRW
        }

    bid_fee = safe_float(
        data.get(
            "bid_fee",
            0
        )
    )

    min_total = safe_float(
        data
        .get("market", {})
        .get("bid", {})
        .get(
            "min_total",
            MIN_ORDER_KRW
        )
    )

    if min_total <= 0:

        min_total = MIN_ORDER_KRW

    latest_bid_fee_rate = bid_fee

    logger.info(
        f"ORDER CHANCE | "
        f"{market} | "
        f"bid_fee={bid_fee * 100:.4f}% | "
        f"min_total={min_total:,.0f}"
    )

    return {

        "bid_fee":
            bid_fee,

        "min_total":
            min_total,

        "data":
            data

    }


# =========================================================
# 수수료 포함 실제 최대 매수 가능금액
# =========================================================

def calculate_fee_safe_buy_amount(
    available_krw,
    bid_fee_rate
):

    if available_krw <= 0:
        return 0.0

    if bid_fee_rate < 0:
        bid_fee_rate = 0.0

    safe_amount = (
        available_krw /
        (1.0 + bid_fee_rate)
    )

    safe_amount = truncate_krw(
        safe_amount
    )

    if safe_amount > 1:

        safe_amount -= 1

    return max(
        safe_amount,
        0
    )


# =========================================================
# 업비트 전체 자산조회
# =========================================================

def fetch_upbit_assets(
    api_key,
    secret_key
):

    global latest_upbit_assets
    global latest_upbit_total_krw
    global latest_upbit_available_krw
    global latest_upbit_update

    headers = create_auth_headers(
        api_key,
        secret_key
    )

    try:

        response = requests.get(
            SERVER_URL + "/v1/accounts",
            headers=headers,
            timeout=REQUEST_TIMEOUT
        )

    except requests.RequestException as e:

        logger.error(
            f"Account request failed: {e}"
        )

        return -1

    if response.status_code != 200:

        logger.error(
            f"Account fetch failed | "
            f"{response.status_code} | "
            f"{response.text}"
        )

        return -1

    try:

        accounts = response.json()

    except Exception as e:

        logger.error(
            f"Account JSON error | {e}"
        )

        return -1

    assets = []

    total_krw = 0.0

    available_krw = 0.0


    # =====================================================
    # KRW
    # =====================================================

    for account in accounts:

        currency = account.get(
            "currency",
            ""
        )

        if currency != "KRW":
            continue

        balance = safe_float(
            account.get(
                "balance",
                0
            )
        )

        locked = safe_float(
            account.get(
                "locked",
                0
            )
        )

        available_krw = max(
            balance,
            0
        )

        total_krw += (
            balance +
            locked
        )

        break


    # =====================================================
    # 코인
    # =====================================================

    for account in accounts:

        currency = account.get(
            "currency",
            ""
        )

        if currency == "KRW":
            continue

        balance = safe_float(
            account.get(
                "balance",
                0
            )
        )

        locked = safe_float(
            account.get(
                "locked",
                0
            )
        )

        avg_buy_price = safe_float(
            account.get(
                "avg_buy_price",
                0
            )
        )

        if balance <= 0:
            continue

        market = f"KRW-{currency}"

        current_price = get_ticker_price(
            market
        )

        if current_price <= 0:

            logger.info(
                f"Hide unsupported asset | "
                f"{currency}"
            )

            continue

        evaluation = (
            balance *
            current_price
        )

        total_krw += evaluation

        if evaluation < MIN_ORDER_KRW:

            logger.info(
                f"Hide small asset | "
                f"{currency} | "
                f"{evaluation:,.0f} KRW"
            )

            continue

        profit_rate = 0.0

        profit_amount = 0.0

        if avg_buy_price > 0:

            profit_rate = (
                (
                    current_price -
                    avg_buy_price
                )
                /
                avg_buy_price
            ) * 100

            profit_amount = (
                current_price -
                avg_buy_price
            ) * balance

        buy_amount_krw = (
            avg_buy_price *
            balance
        )

        assets.append({

            "currency":
                currency,

            "balance":
                balance,

            "locked":
                locked,

            "avg_buy_price":
                avg_buy_price,

            "buy_amount_krw":
                buy_amount_krw,

            "current_price":
                current_price,

            "evaluation_krw":
                evaluation,

            "profit_rate":
                profit_rate,

            "profit_amount":
                profit_amount,

            "market":
                market

        })


    assets.sort(
        key=lambda x:
            -x["evaluation_krw"]
    )

    latest_upbit_assets = assets

    latest_upbit_total_krw = total_krw

    latest_upbit_available_krw = available_krw

    latest_upbit_update = now_string()

    logger.info(
        f"Assets updated | "
        f"Total={total_krw:,.0f} KRW | "
        f"Available={available_krw:,.0f} KRW | "
        f"Visible coins={len(assets)}"
    )

    return total_krw


# =========================================================
# 현재 저장값
# =========================================================

def log_current_dashboard_asset():

    logger.info(
        f"CURRENT DASHBOARD ASSET | "
        f"Total={latest_upbit_total_krw:,.0f} | "
        f"Available={latest_upbit_available_krw:,.0f} | "
        f"Updated={latest_upbit_update}"
    )


# =========================================================
# 특정 코인 실제 잔고
# =========================================================

def get_coin_balance(
    coin,
    api_key,
    secret_key
):

    coin = clean_coin_name(
        coin
    )

    headers = create_auth_headers(
        api_key,
        secret_key
    )

    try:

        response = requests.get(
            SERVER_URL + "/v1/accounts",
            headers=headers,
            timeout=REQUEST_TIMEOUT
        )

    except requests.RequestException as e:

        logger.error(
            f"Coin balance request failed | "
            f"{coin} | {e}"
        )

        raise HTTPException(
            status_code=502,
            detail=str(e)
        )

    if response.status_code != 200:

        raise HTTPException(
            status_code=response.status_code,
            detail=get_error_detail(response)
        )

    for asset in response.json():

        if asset.get("currency") == coin:

            balance = safe_float(
                asset.get(
                    "balance",
                    0
                )
            )

            locked = safe_float(
                asset.get(
                    "locked",
                    0
                )
            )

            available = max(
                balance,
                0
            )

            logger.info(
                f"REAL BALANCE | "
                f"{coin} | "
                f"balance={balance:.18f} | "
                f"locked={locked:.18f} | "
                f"available={available:.18f}"
            )

            return available

    logger.info(
        f"REAL BALANCE | "
        f"{coin} | "
        f"balance=0"
    )

    return 0.0


# =========================================================
# 매수금액 계산
# =========================================================

def calculate_buy_amount(
    stop_loss
):

    if stop_loss <= 0:
        return 0.0

    loss_amount = (
        MONTH_START_AMOUNT *
        MAX_LOSS_RATE
    )

    stop_rate = (
        stop_loss /
        100
    )

    buy_amount = (
        loss_amount /
        stop_rate
    )

    return truncate_krw(
        buy_amount
    )


# =========================================================
# 매수 주문
# =========================================================

def place_bid_order(
    coin,
    krw_amount,
    api_key,
    secret_key
):

    coin = clean_coin_name(
        coin
    )

    market = f"KRW-{coin}"

    krw_amount = truncate_krw(
        krw_amount
    )

    if krw_amount < MIN_ORDER_KRW:

        raise HTTPException(
            status_code=400,
            detail=(
                f"매수금액 {krw_amount:,.0f}원이 "
                f"최소 주문금액 "
                f"{MIN_ORDER_KRW:,}원보다 작습니다."
            )
        )

    query = {

        "market":
            market,

        "side":
            "bid",

        "price":
            str(int(krw_amount)),

        "ord_type":
            "price"

    }

    query_string = build_query_string(
        query
    )

    headers = create_auth_headers(
        api_key,
        secret_key,
        query_string
    )

    logger.info(
        f"BUY ORDER | "
        f"market={market} | "
        f"price={krw_amount:,.0f}"
    )

    try:

        response = requests.post(
            SERVER_URL + "/v1/orders",
            json=query,
            headers=headers,
            timeout=REQUEST_TIMEOUT
        )

    except requests.RequestException as e:

        logger.error(
            f"BUY ORDER REQUEST FAILED | {e}"
        )

        raise HTTPException(
            status_code=502,
            detail=str(e)
        )

    if response.status_code not in (200, 201):

        logger.error(
            f"BUY ORDER ERROR | "
            f"{response.status_code} | "
            f"{response.text}"
        )

        raise HTTPException(
            status_code=response.status_code,
            detail=get_error_detail(response)
        )

    return response.json()


# =========================================================
# 매도 주문
# =========================================================

def place_ask_order(
    coin,
    sell_volume,
    api_key,
    secret_key
):

    coin = clean_coin_name(
        coin
    )

    market = f"KRW-{coin}"

    sell_volume = truncate_volume(
        sell_volume,
        8
    )

    if sell_volume <= 0:

        raise HTTPException(
            status_code=400,
            detail=(
                "volume=0 테스트입니다. "
                "잔고 조회는 완료되었으며 "
                "실제 SELL 주문은 실행하지 않았습니다."
            )
        )

    logger.info(
        f"SELL ORDER | "
        f"market={market} | "
        f"volume={sell_volume:.8f}"
    )

    query = {

        "market":
            market,

        "side":
            "ask",

        "volume":
            f"{sell_volume:.8f}",

        "ord_type":
            "market"

    }

    query_string = build_query_string(
        query
    )

    headers = create_auth_headers(
        api_key,
        secret_key,
        query_string
    )

    try:

        response = requests.post(
            SERVER_URL + "/v1/orders",
            json=query,
            headers=headers,
            timeout=REQUEST_TIMEOUT
        )

    except requests.RequestException as e:

        logger.error(
            f"SELL ORDER REQUEST FAILED | "
            f"{e}"
        )

        raise HTTPException(
            status_code=502,
            detail=str(e)
        )

    if response.status_code not in (200, 201):

        logger.error(
            f"SELL ORDER ERROR | "
            f"{response.status_code} | "
            f"{response.text}"
        )

        raise HTTPException(
            status_code=response.status_code,
            detail=get_error_detail(response)
        )

    return response.json()


# =========================================================
# 주문 체결 대기
# =========================================================

def wait_for_order_complete(
    order_uuid,
    api_key,
    secret_key
):

    start_time = time.time()

    while True:

        query = {
            "uuid": order_uuid
        }

        query_string = build_query_string(
            query
        )

        headers = create_auth_headers(
            api_key,
            secret_key,
            query_string
        )

        try:

            response = requests.get(
                SERVER_URL + "/v1/order",
                params=query,
                headers=headers,
                timeout=REQUEST_TIMEOUT
            )

        except requests.RequestException as e:

            logger.error(
                f"Order query failed: {e}"
            )

            time.sleep(
                ORDER_WAIT_INTERVAL
            )

            if (
                time.time() -
                start_time
                >= ORDER_WAIT_TIMEOUT
            ):

                return {
                    "uuid": order_uuid,
                    "state": "timeout"
                }

            continue

        if response.status_code != 200:

            logger.error(
                f"Order query error | "
                f"{response.status_code} | "
                f"{response.text}"
            )

            time.sleep(
                ORDER_WAIT_INTERVAL
            )

            if (
                time.time() -
                start_time
                >= ORDER_WAIT_TIMEOUT
            ):

                return {
                    "uuid": order_uuid,
                    "state": "timeout"
                }

            continue

        order = response.json()

        state = order.get(
            "state",
            ""
        )

        logger.info(
            f"Order {order_uuid} | "
            f"state={state}"
        )

        if state in (
            "done",
            "cancel"
        ):

            return order

        if (
            time.time() -
            start_time
            >= ORDER_WAIT_TIMEOUT
        ):

            return order

        time.sleep(
            ORDER_WAIT_INTERVAL
        )


# =========================================================
# 주문 결과 파싱
# =========================================================

def parse_order_result(order):

    executed_volume = safe_float(
        order.get(
            "executed_volume",
            0
        )
    )

    executed_funds = safe_float(
        order.get(
            "executed_funds",
            0
        )
    )

    paid_fee = safe_float(
        order.get(
            "paid_fee",
            0
        )
    )

    if executed_volume > 0:

        avg_price = (
            executed_funds /
            executed_volume
        )

    else:

        avg_price = 0.0

    return {

        "uuid":
            order.get("uuid"),

        "market":
            order.get("market"),

        "side":
            order.get("side"),

        "state":
            order.get("state"),

        "executed_volume":
            executed_volume,

        "executed_funds":
            executed_funds,

        "paid_fee":
            paid_fee,

        "avg_price":
            avg_price,

        "created_at":
            order.get("created_at"),

        "trades":
            order.get("trades", [])

    }


# =========================================================
# 매수 기록
# =========================================================

def save_buy_trade(
    result,
    requested_amount,
    stop_loss
):

    coin = result["market"].replace(
        "KRW-",
        ""
    )

    created_at = (
        result["created_at"]
        or now_string()
    )

    completed_at = now_string()

    with db_lock:

        conn = get_db()

        cursor = conn.cursor()

        cursor.execute("""
        INSERT INTO trades (
            uuid,
            coin,
            side,
            order_amount,
            executed_funds,
            executed_volume,
            avg_price,
            fee,
            requested_ratio,
            created_at,
            completed_at,
            state
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (

            result["uuid"],
            coin,
            "buy",
            requested_amount,
            result["executed_funds"],
            result["executed_volume"],
            result["avg_price"],
            result["paid_fee"],
            stop_loss,
            created_at,
            completed_at,
            result["state"]

        ))

        trade_id = cursor.lastrowid

        total_cost = (
            result["executed_funds"]
            +
            result["paid_fee"]
        )

        if result["executed_volume"] > 0:

            cost_per_unit = (
                total_cost /
                result["executed_volume"]
            )

        else:

            cost_per_unit = 0

        cursor.execute("""
        INSERT INTO buy_lots (
            trade_id,
            coin,
            original_volume,
            remaining_volume,
            cost_per_unit,
            total_cost,
            fee,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (

            trade_id,
            coin,
            result["executed_volume"],
            result["executed_volume"],
            cost_per_unit,
            total_cost,
            result["paid_fee"],
            completed_at

        ))

        conn.commit()

        conn.close()

    logger.info(
        f"BUY saved | "
        f"{coin} | "
        f"{result['executed_funds']:,.0f} KRW | "
        f"SL={stop_loss:.2f}%"
    )


# =========================================================
# FIFO 매도 원가
# =========================================================

def calculate_fifo_cost(
    coin,
    sell_volume,
    sell_trade_id
):

    remaining_to_sell = sell_volume

    total_cost = 0.0

    allocations = []

    with db_lock:

        conn = get_db()

        cursor = conn.cursor()

        cursor.execute("""
        SELECT *
        FROM buy_lots
        WHERE coin = ?
          AND remaining_volume > 0
        ORDER BY id ASC
        """, (coin,))

        lots = cursor.fetchall()

        for lot in lots:

            if remaining_to_sell <= 0:
                break

            available = safe_float(
                lot["remaining_volume"]
            )

            use_volume = min(
                remaining_to_sell,
                available
            )

            cost_per_unit = safe_float(
                lot["cost_per_unit"]
            )

            cost = (
                use_volume *
                cost_per_unit
            )

            total_cost += cost

            allocations.append({

                "lot_id":
                    lot["id"],

                "volume":
                    use_volume,

                "cost":
                    cost

            })

            new_remaining = (
                available -
                use_volume
            )

            cursor.execute("""
            UPDATE buy_lots
            SET remaining_volume = ?
            WHERE id = ?
            """, (

                new_remaining,

                lot["id"]

            ))

            remaining_to_sell -= use_volume

        if remaining_to_sell > 0:

            logger.warning(
                f"FIFO cost insufficient | "
                f"coin={coin} | "
                f"remaining={remaining_to_sell}"
            )

        for allocation in allocations:

            cursor.execute("""
            INSERT INTO sell_allocations (
                sell_trade_id,
                buy_lot_id,
                volume,
                cost
            )
            VALUES (?, ?, ?, ?)
            """, (

                sell_trade_id,
                allocation["lot_id"],
                allocation["volume"],
                allocation["cost"]

            ))

        conn.commit()

        conn.close()

    return total_cost


# =========================================================
# 매도 기록
# =========================================================

def save_sell_trade(
    result,
    ratio
):

    coin = result["market"].replace(
        "KRW-",
        ""
    )

    created_at = (
        result["created_at"]
        or now_string()
    )

    completed_at = now_string()

    with db_lock:

        conn = get_db()

        cursor = conn.cursor()

        cursor.execute("""
        INSERT INTO trades (
            uuid,
            coin,
            side,
            order_amount,
            executed_funds,
            executed_volume,
            avg_price,
            fee,
            requested_ratio,
            created_at,
            completed_at,
            state
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (

            result["uuid"],
            coin,
            "sell",
            result["executed_funds"],
            result["executed_funds"],
            result["executed_volume"],
            result["avg_price"],
            result["paid_fee"],
            ratio,
            created_at,
            completed_at,
            result["state"]

        ))

        trade_id = cursor.lastrowid

        conn.commit()

        conn.close()

    realized_cost = calculate_fifo_cost(
        coin,
        result["executed_volume"],
        trade_id
    )

    net_sell_amount = (
        result["executed_funds"]
        -
        result["paid_fee"]
    )

    realized_profit = (
        net_sell_amount
        -
        realized_cost
    )

    if realized_cost > 0:

        realized_return = (
            realized_profit
            /
            realized_cost
            *
            100
        )

    else:

        realized_return = 0.0

    with db_lock:

        conn = get_db()

        cursor = conn.cursor()

        cursor.execute("""
        UPDATE trades
        SET
            realized_cost = ?,
            realized_profit = ?,
            realized_return = ?
        WHERE id = ?
        """, (

            realized_cost,
            realized_profit,
            realized_return,
            trade_id

        ))

        conn.commit()

        conn.close()

    return {

        "trade_id":
            trade_id,

        "coin":
            coin,

        "sell_volume":
            result["executed_volume"],

        "gross_sell":
            result["executed_funds"],

        "fee":
            result["paid_fee"],

        "net_sell":
            net_sell_amount,

        "cost":
            realized_cost,

        "profit":
            realized_profit,

        "return":
            realized_return,

        "avg_price":
            result["avg_price"],

        "completed_at":
            completed_at

    }


# =========================================================
# 주문 전 자산 조회
# =========================================================

def refresh_before_order(
    api_key,
    secret_key,
    action
):

    logger.info(
        f"========== BEFORE {action.upper()} ASSET =========="
    )

    result = fetch_upbit_assets(
        api_key,
        secret_key
    )

    if result < 0:

        logger.error(
            f"BEFORE {action.upper()} ASSET FAILED"
        )

        return False

    logger.info(
        f"BEFORE {action.upper()} ASSET | "
        f"Total={latest_upbit_total_krw:,.0f} | "
        f"Available={latest_upbit_available_krw:,.0f}"
    )

    return True


# =========================================================
# 주문 후 자산 조회
# =========================================================

def refresh_after_order(
    api_key,
    secret_key,
    action
):

    logger.info(
        f"========== AFTER {action.upper()} ASSET =========="
    )

    result = fetch_upbit_assets(
        api_key,
        secret_key
    )

    if result < 0:

        logger.error(
            f"AFTER {action.upper()} ASSET FAILED"
        )

        log_current_dashboard_asset()

        return False

    logger.info(
        f"AFTER {action.upper()} ASSET | "
        f"Total={latest_upbit_total_krw:,.0f} | "
        f"Available={latest_upbit_available_krw:,.0f}"
    )

    return True


# =========================================================
# TradingView Webhook
# =========================================================

@app.post("/tradingview_webhook")
async def tradingview_webhook(
    request: Request
):

    global latest_order_info

    try:

        raw_body = await request.body()

    except Exception as e:

        logger.error(
            f"Webhook body read error | {e}"
        )

        raise HTTPException(
            status_code=400,
            detail="Webhook body를 읽을 수 없습니다."
        )

    if not raw_body:

        raise HTTPException(
            status_code=400,
            detail="Webhook body가 비어 있습니다."
        )

    try:

        payload = json.loads(
            raw_body.decode("utf-8")
        )

    except Exception as e:

        logger.error(
            f"Webhook JSON error | "
            f"{e} | "
            f"body={raw_body[:500]!r}"
        )

        raise HTTPException(
            status_code=400,
            detail="올바른 JSON 형식이 아닙니다."
        )

    if not isinstance(payload, dict):

        raise HTTPException(
            status_code=400,
            detail="Webhook JSON은 객체 형식이어야 합니다."
        )

    action = safe_string(
        get_payload_value(
            payload,
            [
                "Action",
                "action",
                "ACTION"
            ],
            ""
        )
    ).lower()

    coin = clean_coin_name(
        get_payload_value(
            payload,
            [
                "coin",
                "Coin",
                "COIN",
                "ticker",
                "Ticker",
                "symbol",
                "Symbol"
            ],
            ""
        )
    )

    volume = safe_float(
        get_payload_value(
            payload,
            [
                "volume",
                "Volume",
                "VOLUME"
            ],
            0
        )
    )

    stop_loss = safe_float(
        get_payload_value(
            payload,
            [
                "stop_loss",
                "StopLoss",
                "stopLoss",
                "STOP_LOSS"
            ],
            0
        )
    )

    api_key = safe_string(
        get_payload_value(
            payload,
            [
                "api_key",
                "API_KEY",
                "ApiKey"
            ],
            ""
        )
    )

    secret_key = safe_string(
        get_payload_value(
            payload,
            [
                "secret_key",
                "SECRET_KEY",
                "SecretKey"
            ],
            ""
        )
    )

    logger.info(
        "========== TRADINGVIEW WEBHOOK =========="
    )

    logger.info(
        f"Action={action} | "
        f"Coin={coin} | "
        f"Volume={volume} | "
        f"StopLoss={stop_loss:.2f}%"
    )

    if not action:

        raise HTTPException(
            status_code=400,
            detail="Action 값이 없습니다."
        )

    if action not in (
        "buy",
        "sell"
    ):

        raise HTTPException(
            status_code=400,
            detail=(
                "Action은 Buy 또는 Sell만 "
                "사용할 수 있습니다."
            )
        )

    if not api_key or not secret_key:

        raise HTTPException(
            status_code=400,
            detail="API key가 없습니다."
        )


    # =====================================================
    # BUY
    # =====================================================

    if action == "buy":

        before_ok = refresh_before_order(
            api_key,
            secret_key,
            "BUY"
        )

        if not before_ok:

            raise HTTPException(
                status_code=502,
                detail="BUY 전 업비트 잔고 조회 실패"
            )

        if not coin:

            raise HTTPException(
                status_code=400,
                detail="BUY 코인이 없습니다."
            )


        if stop_loss <= 0:

            latest_order_info = (
                f"🟡 BUY 조회 테스트 | "
                f"{coin} | "
                f"StopLoss=0 | "
                f"주문하지 않음"
            )

            return {

                "status": "test",

                "message":
                    "BUY 조회 테스트입니다. "
                    "실제 주문은 실행하지 않았습니다.",

                "Action": "Buy",

                "Coin": coin,

                "StopLoss": stop_loss,

                "Total KRW":
                    latest_upbit_total_krw,

                "Available KRW":
                    latest_upbit_available_krw,

                "Updated At":
                    latest_upbit_update,

                "Order Executed": False

            }


        target_buy_amount = calculate_buy_amount(
            stop_loss
        )

        available_krw = (
            latest_upbit_available_krw
        )

        if available_krw <= 0:

            raise HTTPException(
                status_code=400,
                detail="현재 매수 가능한 KRW가 없습니다."
            )


        market = f"KRW-{coin}"

        order_chance = get_order_chance(
            market,
            api_key,
            secret_key
        )

        bid_fee_rate = safe_float(
            order_chance.get(
                "bid_fee",
                0
            )
        )

        min_order_total = safe_float(
            order_chance.get(
                "min_total",
                MIN_ORDER_KRW
            )
        )

        if min_order_total <= 0:

            min_order_total = MIN_ORDER_KRW


        fee_safe_available = (
            calculate_fee_safe_buy_amount(
                available_krw,
                bid_fee_rate
            )
        )


        amount_to_invest = min(
            target_buy_amount,
            fee_safe_available
        )


        amount_to_invest = truncate_krw(
            amount_to_invest
        )


        if amount_to_invest < min_order_total:

            raise HTTPException(
                status_code=400,
                detail=(
                    f"실제 주문가능금액 "
                    f"{amount_to_invest:,.0f}원이 "
                    f"최소 주문금액 "
                    f"{min_order_total:,.0f}원보다 작습니다."
                )
            )


        loss_limit_amount = (
            MONTH_START_AMOUNT *
            MAX_LOSS_RATE
        )


        estimated_fee = (
            amount_to_invest *
            bid_fee_rate
        )


        estimated_total_required = (
            amount_to_invest +
            estimated_fee
        )


        logger.info(
            f"BUY CALC | "
            f"MonthStart={MONTH_START_AMOUNT:,.0f} | "
            f"LossLimit={loss_limit_amount:,.0f} | "
            f"StopLoss={stop_loss:.2f}% | "
            f"Target={target_buy_amount:,.0f} | "
            f"Available={available_krw:,.0f} | "
            f"BidFee={bid_fee_rate * 100:.4f}% | "
            f"FeeSafeAvailable={fee_safe_available:,.0f} | "
            f"Actual={amount_to_invest:,.0f} | "
            f"EstimatedFee={estimated_fee:,.0f} | "
            f"EstimatedTotal={estimated_total_required:,.0f}"
        )


        try:

            order = place_bid_order(
                coin,
                amount_to_invest,
                api_key,
                secret_key
            )

            completed_order = wait_for_order_complete(
                order["uuid"],
                api_key,
                secret_key
            )

            result = parse_order_result(
                completed_order
            )

            if result["state"] != "done":

                raise HTTPException(
                    status_code=400,
                    detail={
                        "message":
                            "BUY 주문이 완료되지 않았습니다.",
                        "order":
                            completed_order
                    }
                )

            save_buy_trade(
                result,
                amount_to_invest,
                stop_loss
            )


            latest_order_info = (
                f"🟢 BUY {coin} | "
                f"손절 {stop_loss:.2f}% | "
                f"목표 {target_buy_amount:,.0f}원 | "
                f"주문 {amount_to_invest:,.0f}원 | "
                f"체결 {result['executed_funds']:,.0f}원 | "
                f"수수료 {result['paid_fee']:,.0f}원"
            )


            return {

                "status": "success",

                "Action": "Buy",

                "Coin": coin,

                "Stop Loss Percent":
                    stop_loss,

                "Month Start Amount":
                    MONTH_START_AMOUNT,

                "Maximum Loss Amount":
                    loss_limit_amount,

                "Target Buy KRW":
                    target_buy_amount,

                "Available KRW Before Buy":
                    available_krw,

                "Bid Fee Rate":
                    bid_fee_rate,

                "Fee Safe Available KRW":
                    fee_safe_available,

                "Actual Order KRW":
                    amount_to_invest,

                "Estimated Fee":
                    estimated_fee,

                "Estimated Total Required":
                    estimated_total_required,

                "Actual Executed KRW":
                    result["executed_funds"],

                "Executed Volume":
                    result["executed_volume"],

                "Average Price":
                    result["avg_price"],

                "Fee":
                    result["paid_fee"],

                "Order UUID":
                    result["uuid"],

                "Order Executed":
                    True

            }

        finally:

            refresh_after_order(
                api_key,
                secret_key,
                "BUY"
            )


    # =====================================================
    # SELL
    # =====================================================

    if action == "sell":

        before_ok = refresh_before_order(
            api_key,
            secret_key,
            "SELL"
        )

        if not before_ok:

            raise HTTPException(
                status_code=502,
                detail="SELL 전 업비트 잔고 조회 실패"
            )

        if not coin:

            raise HTTPException(
                status_code=400,
                detail="SELL 코인이 없습니다."
            )

        if volume <= 0:

            latest_order_info = (
                f"🟡 SELL 조회 테스트 | "
                f"{coin} | "
                f"Volume=0 | "
                f"주문하지 않음"
            )

            return {

                "status": "test",

                "message":
                    "SELL 조회 테스트입니다. "
                    "실제 주문은 실행하지 않았습니다.",

                "Action": "Sell",

                "Coin": coin,

                "Volume": volume,

                "Total KRW":
                    latest_upbit_total_krw,

                "Available KRW":
                    latest_upbit_available_krw,

                "Updated At":
                    latest_upbit_update,

                "Order Executed": False

            }

        if volume > 1:

            raise HTTPException(
                status_code=400,
                detail=(
                    "SELL volume은 "
                    "0보다 크고 1 이하여야 합니다."
                )
            )

        current_coin_balance = get_coin_balance(
            coin,
            api_key,
            secret_key
        )

        if current_coin_balance <= 0:

            raise HTTPException(
                status_code=400,
                detail=(
                    f"{coin} 실제 업비트 "
                    f"보유수량이 없습니다."
                )
            )

        if volume >= 1.0:

            sell_volume = current_coin_balance

            sell_label = "100% 전체매도"

        else:

            sell_volume = (
                current_coin_balance *
                volume
            )

            sell_label = (
                f"{volume * 100:.0f}%"
            )

        sell_volume = truncate_volume(
            sell_volume,
            8
        )

        if sell_volume <= 0:

            raise HTTPException(
                status_code=400,
                detail="매도수량이 0입니다."
            )

        logger.info(
            f"REAL SELL BALANCE | "
            f"{coin} | "
            f"balance={current_coin_balance:.18f} | "
            f"ratio={volume:.4f} | "
            f"sell={sell_volume:.8f}"
        )

        try:

            order = place_ask_order(
                coin,
                sell_volume,
                api_key,
                secret_key
            )

            completed_order = wait_for_order_complete(
                order["uuid"],
                api_key,
                secret_key
            )

            result = parse_order_result(
                completed_order
            )

            if result["state"] != "done":

                raise HTTPException(
                    status_code=400,
                    detail={
                        "message":
                            "SELL 주문이 완료되지 않았습니다.",
                        "order":
                            completed_order
                    }
                )

            sell_result = save_sell_trade(
                result,
                volume
            )

            latest_order_info = (
                f"🔴 SELL {coin} | "
                f"{sell_label} | "
                f"수량 "
                f"{sell_result['sell_volume']:,.6f} | "
                f"체결금액 "
                f"{sell_result['gross_sell']:,.0f}원 | "
                f"수익 "
                f"{sell_result['profit']:+,.0f}원 "
                f"({sell_result['return']:+.2f}%)"
            )

            return {

                "status": "success",

                "Action": "Sell",

                "Coin": coin,

                "Real Balance Before Sell":
                    current_coin_balance,

                "Sell Ratio":
                    volume,

                "Requested Sell Volume":
                    sell_volume,

                "Executed Volume":
                    sell_result["sell_volume"],

                "Gross Sell Amount":
                    sell_result["gross_sell"],

                "Fee":
                    sell_result["fee"],

                "Net Sell Amount":
                    sell_result["net_sell"],

                "Cost":
                    sell_result["cost"],

                "Realized Profit":
                    sell_result["profit"],

                "Return Percent":
                    sell_result["return"],

                "Average Sell Price":
                    sell_result["avg_price"],

                "Order UUID":
                    result["uuid"],

                "Order Executed":
                    True

            }

        finally:

            refresh_after_order(
                api_key,
                secret_key,
                "SELL"
            )


# =========================================================
# 자산 API
# =========================================================

@app.get("/api/upbit-assets")
async def api_upbit_assets():

    return {

        "month_start_amount":
            MONTH_START_AMOUNT,

        "max_loss_rate":
            MAX_LOSS_RATE,

        "max_loss_amount":
            MONTH_START_AMOUNT *
            MAX_LOSS_RATE,

        "total_krw":
            latest_upbit_total_krw,

        "available_krw":
            latest_upbit_available_krw,

        "updated_at":
            latest_upbit_update,

        "bid_fee_rate":
            latest_bid_fee_rate,

        "assets":
            latest_upbit_assets,

        "latest_order":
            latest_order_info

    }


# =========================================================
# 대시보드
# =========================================================

@app.get(
    "/",
    response_class=HTMLResponse
)
async def dashboard():

    return HTMLResponse("""
<!DOCTYPE html>

<html lang="ko">

<head>

<meta charset="UTF-8">

<meta
name="viewport"
content="width=device-width, initial-scale=1.0"
>

<title>업비트 잔고 확인</title>

<style>

* {
    box-sizing: border-box;
}

body {
    margin: 0;
    background: #0e1116;
    color: #f1f3f5;
    font-family: Arial, "Noto Sans KR", sans-serif;
}

.container {
    width: 100%;
    max-width: 900px;
    margin: auto;
    padding: 10px;
}

h1 {
    margin: 3px 0 4px 0;
    font-size: 21px;
}

.subtitle {
    color: #8b949e;
    font-size: 11px;
    margin-bottom: 10px;
}

.grid {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 7px;
    margin-bottom: 12px;
}

.card {
    background: #171b22;
    border: 1px solid #292f38;
    border-radius: 9px;
    padding: 11px;
    min-width: 0;
}

.title {
    color: #8b949e;
    font-size: 11px;
    margin-bottom: 5px;
}

.value {
    font-size: 17px;
    font-weight: 700;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}

.section {
    margin-top: 13px;
    margin-bottom: 7px;
    font-size: 15px;
    font-weight: 700;
    display: flex;
    justify-content: space-between;
    align-items: center;
}

.section-coin {
    color: #ffffff;
    font-size: 13px;
    font-weight: 700;
}


/* =====================================================
   보유자산 2열
   ===================================================== */

.asset-card {
    width: 100%;
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 7px;
    background: transparent;
    border: none;
    border-radius: 0;
    overflow: visible;
}

.asset-item {
    width: 100%;
    background: #171b22;
    border: 1px solid #292f38;
    border-radius: 9px;
    overflow: hidden;
}

.asset-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 9px 12px;
    border-bottom: 1px solid #292f38;
}

.asset-row:last-child {
    border-bottom: none;
}

.asset-row-label {
    color: #8b949e;
    font-size: 12px;
}

.asset-row-value {
    color: #ffffff;
    font-size: 12px;
    font-weight: 700;
    text-align: right;
}

.target-title {
    padding: 9px 12px;
    background: #12151b;
    color: #8b949e;
    font-size: 11px;
    font-weight: 700;
}

.target-title-inner {
    display: grid;
    grid-template-columns: 1fr 1fr 1fr;
    gap: 4px;
    align-items: center;
}

.target-title-inner span:nth-child(2),
.target-title-inner span:nth-child(3) {
    text-align: right;
}

.target-row {
    display: grid;
    grid-template-columns: 1fr 1fr 1fr;
    gap: 4px;
    align-items: center;
    padding: 9px 12px;
    border-top: 1px solid #292f38;
}

.target-money {
    font-size: 12px;
    font-weight: 700;
}

.target-rate {
    font-size: 12px;
    font-weight: 700;
    color: #d29922;
    text-align: right;
}

.target-price {
    font-size: 12px;
    font-weight: 700;
    color: #58a6ff;
    text-align: right;
}


/* =====================================================
   손절폭 / 진입금액 / 1:2 / 1:3 수익금
   ===================================================== */

.risk-table {
    width: 100%;
    border-collapse: collapse;
    background: #171b22;
    border: 1px solid #292f38;
    border-radius: 9px;
    overflow: hidden;
    margin-top: 7px;
}

.risk-table th {
    padding: 8px 4px;
    background: #12151b;
    color: #8b949e;
    font-size: 10px;
    text-align: center;
}

.risk-table td {
    padding: 8px 4px;
    border-top: 1px solid #252a32;
    text-align: center;
    font-size: 11px;
    font-weight: 700;
}

.risk-entry {
    color: #ffffff;
}

.risk-2x {
    color: #58a6ff;
}

.risk-3x {
    color: #3fb950;
}


.green {
    color: #3fb950;
}

.red {
    color: #f85149;
}

.yellow {
    color: #d29922;
}

.blue {
    color: #58a6ff;
}

.small {
    font-size: 10px;
    color: #8b949e;
    line-height: 1.7;
    margin-top: 10px;
}


/* =====================================================
   모바일
   ===================================================== */

@media(max-width:500px) {

    .container {
        padding: 7px;
    }

    h1 {
        font-size: 19px;
    }

    .grid {
        gap: 6px;
    }

    .card {
        padding: 9px;
    }

    .title {
        font-size: 10px;
    }

    .value {
        font-size: 15px;
    }

    .section {
        font-size: 14px;
    }

    .section-coin {
        font-size: 12px;
    }


    /* 보유자산 2열 유지 */

    .asset-card {
        grid-template-columns:
            repeat(2, minmax(0, 1fr));

        gap: 6px;
    }

    .asset-item {
        min-width: 0;
    }

    .asset-row {
        padding: 8px 8px;
    }

    .asset-row-label,
    .asset-row-value {
        font-size: 10px;
    }

    .target-title {
        padding: 8px 8px;
        font-size: 9px;
    }

    .target-title-inner,
    .target-row {
        grid-template-columns:
            1fr 1fr 1fr;

        gap: 3px;
    }

    .target-row {
        padding: 8px 8px;
    }

    .target-money,
    .target-rate,
    .target-price {
        font-size: 9px;
    }

    .risk-table th {
        padding: 7px 2px;
        font-size: 9px;
    }

    .risk-table td {
        padding: 7px 2px;
        font-size: 9px;
    }

}

</style>

</head>

<body>

<div class="container">

<h1>
    업비트 잔고 확인
</h1>

<div class="subtitle">
    TradingView BUY / SELL 신호 기준 실시간 조회
</div>


<!-- ===================================================
     상단 요약
     =================================================== -->

<div class="grid">

    <div class="card">

        <div class="title">
            월 시작금액
        </div>

        <div
            class="value"
            id="month-start"
        >
            -
        </div>

    </div>


    <div class="card">

        <div class="title">
            현재 총자산
        </div>

        <div
            class="value"
            id="total"
        >
            -
        </div>

    </div>


    <div class="card">

        <div class="title">
            월 수익금
        </div>

        <div
            class="value"
            id="month-profit"
        >
            -
        </div>

    </div>


    <div class="card">

        <div class="title">
            월 수익률
        </div>

        <div
            class="value"
            id="month-return"
        >
            -
        </div>

    </div>


    <div class="card">

        <div class="title">
            현재 매수 가능금액
        </div>

        <div
            class="value"
            id="available"
        >
            -
        </div>

    </div>


    <div class="card">

        <div class="title">
            손절한도
        </div>

        <div
            class="value red"
            id="loss-limit-top"
        >
            -
        </div>

    </div>

</div>


<!-- ===================================================
     보유자산
     =================================================== -->

<div class="section">

    <span>
        보유자산
    </span>

    <span
        class="section-coin"
        id="asset-coin"
    >
        보유 없음
    </span>

</div>


<div
    class="asset-card"
    id="asset-card"
>
</div>


<!-- ===================================================
     손절폭 / 진입금액 / 목표수익금
     =================================================== -->

<div class="section">

    <span>
        손절폭별 진입금액 / 목표수익금
    </span>

</div>


<table class="risk-table">

<thead>

<tr>

<th>
    손절폭
</th>

<th>
    진입금액
</th>

<th>
    1:2
</th>

<th>
    1:3
</th>

</tr>

</thead>

<tbody id="risk-body">
</tbody>

</table>


<div class="small">

월 시작금액:
<span id="loss-base">
-
</span>

<br>

전체 시드 손실기준:
<span id="loss-rate">
1%
</span>

<br>

마지막 조회:
<span id="updated">
TradingView 신호 대기
</span>

<br>

마지막 주문:
<span id="latest-order">
주문 없음
</span>

<br>

※ 현재 보유 중인 코인의 평가손익은 월 수익금에 별도로 반영되는 평가손익입니다.

<br>

※ BUY/SELL 신호가 들어오면 주문 전 업비트 자산을 먼저 조회합니다.

<br>

※ 실제 주문 후 업비트 자산을 다시 조회합니다.

<br>

※ BUY stop_loss=0은 조회 테스트이며 실제 주문하지 않습니다.

<br>

※ SELL volume=0은 조회 테스트이며 실제 주문하지 않습니다.

<br>

※ BUY 주문은 업비트 현재 매수 수수료를 조회한 뒤 수수료를 제외하고 실제 주문 가능한 금액을 계산합니다.

<br>

※ 목표 수익률은 현재 보유 코인의 매수금액 기준으로 계산합니다.

<br>

※ 목표 단가는 매수단가에 목표 수익률을 적용한 가격입니다.

<br>

※ 대시보드 자체에서는 업비트 API를 직접 호출하지 않습니다.

</div>


</div>


<script>


// ======================================================
// 숫자
// ======================================================

function money(value) {

    if (
        value === null ||
        value === undefined ||
        isNaN(value)
    ) {
        return "-";
    }

    return Number(value).toLocaleString(
        "ko-KR",
        {
            maximumFractionDigits: 0,
            minimumFractionDigits: 0
        }
    ) + "원";

}


function number(value) {

    if (
        value === null ||
        value === undefined ||
        isNaN(value)
    ) {
        return "-";
    }

    return Number(value).toLocaleString(
        "ko-KR",
        {
            maximumFractionDigits: 8
        }
    );

}


// ======================================================
// 가격
// 매수단가 / 현재가 / 목표단가
// 소수점 둘째 자리까지 표시
// ======================================================

function price(value) {

    if (
        value === null ||
        value === undefined ||
        isNaN(value)
    ) {
        return "-";
    }

    return Number(value).toLocaleString(
        "ko-KR",
        {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        }
    ) + "원";

}


// ======================================================
// 수익률
// ======================================================

function profitRate(value) {

    if (
        value === null ||
        value === undefined ||
        isNaN(value)
    ) {
        return "-";
    }

    const n = Number(value);

    const cls =
        n >= 0
        ? "green"
        : "red";

    const sign =
        n >= 0
        ? "+"
        : "";

    return `
        <span class="${cls}">
            ${sign}${n.toFixed(2)}%
        </span>
    `;

}


// ======================================================
// 수익금
// ======================================================

function profitMoney(value) {

    if (
        value === null ||
        value === undefined ||
        isNaN(value)
    ) {
        return "-";
    }

    const n = Number(value);

    const cls =
        n >= 0
        ? "green"
        : "red";

    const sign =
        n >= 0
        ? "+"
        : "";

    return `
        <span class="${cls}">
            ${sign}${n.toLocaleString(
                "ko-KR",
                {
                    maximumFractionDigits: 0
                }
            )}원
        </span>
    `;

}


// ======================================================
// 보유자산 전체 표시
// 2열
// ======================================================

function renderAssets(assets) {

    const coinTitle =
        document.getElementById(
            "asset-coin"
        );

    const card =
        document.getElementById(
            "asset-card"
        );


    if (
        !assets ||
        assets.length === 0
    ) {

        coinTitle.textContent =
            "보유 없음";

        card.innerHTML = `

            <div class="asset-item">

                <div class="asset-row">

                    <span class="asset-row-label">
                        보유자산
                    </span>

                    <span class="asset-row-value">
                        보유자산이 없습니다.
                    </span>

                </div>

            </div>

        `;

        return;

    }


    // ==================================================
    // 보유 코인 개수
    // ==================================================

    coinTitle.textContent =
        assets.length + "개";


    let html = "";


    // ==================================================
    // 모든 보유 코인 표시
    // ==================================================

    assets.forEach(function(a) {

        const buyAmount =
            Number(
                a.buy_amount_krw || 0
            );


        const avgBuyPrice =
            Number(
                a.avg_buy_price || 0
            );


        // ==================================================
        // 목표 수익률
        // ==================================================

        const target100000 =
            buyAmount > 0
            ? (
                100000 /
                buyAmount
            ) * 100
            : 0;


        const target200000 =
            buyAmount > 0
            ? (
                200000 /
                buyAmount
            ) * 100
            : 0;


        const target300000 =
            buyAmount > 0
            ? (
                300000 /
                buyAmount
            ) * 100
            : 0;


        // ==================================================
        // 목표 단가
        // ==================================================

        const targetPrice100000 =
            avgBuyPrice > 0
            ? avgBuyPrice *
              (
                  1 +
                  target100000 / 100
              )
            : 0;


        const targetPrice200000 =
            avgBuyPrice > 0
            ? avgBuyPrice *
              (
                  1 +
                  target200000 / 100
              )
            : 0;


        const targetPrice300000 =
            avgBuyPrice > 0
            ? avgBuyPrice *
              (
                  1 +
                  target300000 / 100
              )
            : 0;


        html += `

            <div class="asset-item">


                <!-- =====================================
                     코인명
                     ===================================== -->

                <div class="target-title">

                    <div
                        style="
                            font-size:14px;
                            color:#ffffff;
                            font-weight:700;
                        "
                    >
                        ${a.currency}
                    </div>

                </div>


                <!-- =====================================
                     수량
                     ===================================== -->

                <div class="asset-row">

                    <span class="asset-row-label">
                        수량
                    </span>

                    <span class="asset-row-value">
                        ${number(a.balance)}
                    </span>

                </div>


                <!-- =====================================
                     매수단가
                     소수점 2자리
                     ===================================== -->

                <div class="asset-row">

                    <span class="asset-row-label">
                        매수단가
                    </span>

                    <span class="asset-row-value">
                        ${price(a.avg_buy_price)}
                    </span>

                </div>


                <!-- =====================================
                     현재가
                     소수점 2자리
                     ===================================== -->

                <div class="asset-row">

                    <span class="asset-row-label">
                        현재가
                    </span>

                    <span class="asset-row-value">
                        ${price(a.current_price)}
                    </span>

                </div>


                <!-- =====================================
                     매수금액
                     ===================================== -->

                <div class="asset-row">

                    <span class="asset-row-label">
                        매수금액
                    </span>

                    <span class="asset-row-value">
                        ${money(a.buy_amount_krw)}
                    </span>

                </div>


                <!-- =====================================
                     평가금액
                     ===================================== -->

                <div class="asset-row">

                    <span class="asset-row-label">
                        평가금액
                    </span>

                    <span class="asset-row-value">
                        ${money(a.evaluation_krw)}
                    </span>

                </div>


                <!-- =====================================
                     수익률
                     ===================================== -->

                <div class="asset-row">

                    <span class="asset-row-label">
                        수익률
                    </span>

                    <span class="asset-row-value">
                        ${profitRate(a.profit_rate)}
                    </span>

                </div>


                <!-- =====================================
                     수익금
                     ===================================== -->

                <div class="asset-row">

                    <span class="asset-row-label">
                        수익금
                    </span>

                    <span class="asset-row-value">
                        ${profitMoney(a.profit_amount)}
                    </span>

                </div>


                <!-- =====================================
                     목표 수익금
                     ===================================== -->

                <div class="target-title">

                    <div class="target-title-inner">

                        <span>
                            목표
                        </span>

                        <span>
                            수익률
                        </span>

                        <span>
                            목표단가
                        </span>

                    </div>

                </div>


                <!-- =====================================
                     10만원
                     ===================================== -->

                <div class="target-row">

                    <span class="target-money">
                        10만원
                    </span>

                    <span class="target-rate">
                        ${target100000.toFixed(2)}%
                    </span>

                    <span class="target-price">
                        ${price(targetPrice100000)}
                    </span>

                </div>


                <!-- =====================================
                     20만원
                     ===================================== -->

                <div class="target-row">

                    <span class="target-money">
                        20만원
                    </span>

                    <span class="target-rate">
                        ${target200000.toFixed(2)}%
                    </span>

                    <span class="target-price">
                        ${price(targetPrice200000)}
                    </span>

                </div>


                <!-- =====================================
                     30만원
                     ===================================== -->

                <div class="target-row">

                    <span class="target-money">
                        30만원
                    </span>

                    <span class="target-rate">
                        ${target300000.toFixed(2)}%
                    </span>

                    <span class="target-price">
                        ${price(targetPrice300000)}
                    </span>

                </div>


            </div>

        `;

    });


    card.innerHTML = html;

}


// ======================================================
// 손절폭별 진입금액 / 1:2 / 1:3 수익금
// ======================================================

function renderRiskTable(
    monthStart,
    maxLossRate
) {

    const body =
        document.getElementById(
            "risk-body"
        );


    const lossLimit =
        monthStart *
        maxLossRate;


    let html = "";


    for (
        let stop = 1;
        stop <= 10;
        stop++
    ) {

        // ==============================================
        // 손절폭에 따른 진입금액
        // ==============================================

        const entryAmount =
            lossLimit /
            (stop / 100);


        // ==============================================
        // 1:2 / 1:3 목표수익금
        // ==============================================

        const profit2x =
            lossLimit * 2;

        const profit3x =
            lossLimit * 3;


        html += `

            <tr>

                <td>
                    ${stop}%
                </td>

                <td class="risk-entry">
                    ${money(entryAmount)}
                </td>

                <td class="risk-2x">
                    ${money(profit2x)}
                </td>

                <td class="risk-3x">
                    ${money(profit3x)}
                </td>

            </tr>

        `;

    }


    body.innerHTML = html;

}


// ======================================================
// 데이터 조회
// ======================================================

async function loadData() {

    try {

        const response =
            await fetch(
                "/api/upbit-assets",
                {
                    cache: "no-store"
                }
            );


        const data =
            await response.json();


        const monthStart =
            Number(
                data.month_start_amount || 0
            );


        const total =
            Number(
                data.total_krw || 0
            );


        const maxLossRate =
            Number(
                data.max_loss_rate || 0.01
            );


        const monthProfit =
            total -
            monthStart;


        let monthReturn = 0;


        if (monthStart > 0) {

            monthReturn =
                (
                    monthProfit /
                    monthStart
                ) * 100;

        }


        const lossLimit =
            monthStart *
            maxLossRate;


        document.getElementById(
            "month-start"
        ).textContent =
            money(monthStart);


        document.getElementById(
            "total"
        ).textContent =
            money(total);


        document.getElementById(
            "month-profit"
        ).innerHTML =
            profitMoney(monthProfit);


        document.getElementById(
            "month-return"
        ).innerHTML =
            profitRate(monthReturn);


        document.getElementById(
            "available"
        ).textContent =
            money(data.available_krw);


        document.getElementById(
            "loss-limit-top"
        ).textContent =
            money(lossLimit);


        document.getElementById(
            "loss-base"
        ).textContent =
            money(monthStart);


        document.getElementById(
            "loss-rate"
        ).textContent =
            (maxLossRate * 100)
            .toFixed(0) + "%";


        document.getElementById(
            "updated"
        ).textContent =
            data.updated_at
            || "TradingView 신호 대기";


        document.getElementById(
            "latest-order"
        ).textContent =
            data.latest_order
            || "주문 없음";


        // ==================================================
        // 손절폭별 진입금액 / 1:2 / 1:3
        // ==================================================

        renderRiskTable(
            monthStart,
            maxLossRate
        );


        // ==================================================
        // 모든 보유자산 전달
        // ==================================================

        renderAssets(
            data.assets
        );

    }

    catch(error) {

        console.error(
            "Dashboard error:",
            error
        );

    }

}


// ======================================================
// 페이지 열 때 1회
// ======================================================

loadData();


</script>

</body>

</html>
""")


# =========================================================
# DB 생성
# =========================================================

init_db()


# =========================================================
# 실행
# =========================================================

if __name__ == "__main__":

    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )0h8
