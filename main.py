from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

import requests
import uuid
import jwt
import hashlib
import logging
import sqlite3
import threading
import time

from datetime import datetime
from urllib.parse import urlencode


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


# =========================================================
# SQLite
# =========================================================

DB_FILE = "trading.db"

db_lock = threading.Lock()


# =========================================================
# 전역 데이터
# =========================================================

latest_upbit_assets = []

latest_upbit_total_krw = 0.0

latest_upbit_krw = 0.0

latest_upbit_available_krw = 0.0

latest_upbit_update = "조회 전"

latest_order_info = "주문 없음"

latest_sell_result = None

# 마지막 웹훅 발생 시간
latest_webhook_time = "웹훅 없음"


# =========================================================
# TradingView Payload
# =========================================================

class TradingViewPayload(BaseModel):

    Action: str

    coin: str

    volume: float

    api_key: str

    secret_key: str


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

        # -------------------------------------------------
        # 거래
        # -------------------------------------------------

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

        # -------------------------------------------------
        # 매수 Lot
        # -------------------------------------------------

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

        # -------------------------------------------------
        # 매도와 매수 연결
        # -------------------------------------------------

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

def clean_coin_name(coin: str):

    coin = str(coin).upper().strip()

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
# =========================================================

def now_string():

    return datetime.now().strftime(
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
    api_key: str,
    secret_key: str,
    query_string: str = ""
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
    api_key: str,
    secret_key: str,
    query_string: str = ""
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
# 현재가
# =========================================================

def get_ticker_price(market: str):

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
# 업비트 자산조회
#
# 중요:
# balance = 총 잔고
# locked = 주문 등에 잠긴 잔고
# available = 실제 주문 가능한 잔고
# =========================================================

def fetch_upbit_assets(
    api_key: str,
    secret_key: str
):

    global latest_upbit_assets
    global latest_upbit_total_krw
    global latest_upbit_krw
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

    accounts = response.json()

    assets = []

    total_krw = 0.0

    krw_balance = 0.0

    krw_locked = 0.0

    for account in accounts:

        currency = account.get(
            "currency",
            ""
        )

        balance = float(
            account.get(
                "balance",
                0
            )
        )

        locked = float(
            account.get(
                "locked",
                0
            )
        )

        avg_buy_price = float(
            account.get(
                "avg_buy_price",
                0
            )
        )

        # -------------------------------------------------
        # KRW
        # -------------------------------------------------

        if currency == "KRW":

            krw_balance = balance

            krw_locked = locked

            available_krw = max(
                balance - locked,
                0
            )

            total_krw += balance

            assets.append({

                "currency": "KRW",

                "balance": balance,

                "locked": locked,

                "available": available_krw,

                "avg_buy_price": 0,

                "current_price": 1,

                "evaluation_krw": balance,

                "market": "KRW"

            })

            continue

        # -------------------------------------------------
        # 잔고 없는 코인은 제외
        # -------------------------------------------------

        if balance <= 0 and locked <= 0:

            continue

        market = f"KRW-{currency}"

        current_price = get_ticker_price(
            market
        )

        evaluation = 0.0

        if current_price > 0:

            evaluation = (
                balance *
                current_price
            )

            total_krw += evaluation

        available = max(
            balance - locked,
            0
        )

        assets.append({

            "currency": currency,

            "balance": balance,

            "locked": locked,

            "available": available,

            "avg_buy_price": avg_buy_price,

            "current_price": current_price,

            "evaluation_krw": evaluation,

            "market": market

        })

    # -----------------------------------------------------
    # 평가금액 순
    # -----------------------------------------------------

    assets.sort(
        key=lambda x: (
            0
            if x["currency"] == "KRW"
            else 1,
            -x["evaluation_krw"]
        )
    )

    latest_upbit_assets = assets

    latest_upbit_total_krw = total_krw

    latest_upbit_krw = krw_balance

    latest_upbit_available_krw = max(
        krw_balance - krw_locked,
        0
    )

    latest_upbit_update = now_string()

    logger.info(
        f"Assets updated | "
        f"Total={total_krw:,.0f} KRW | "
        f"Available KRW="
        f"{latest_upbit_available_krw:,.0f}"
    )

    return total_krw


# =========================================================
# 주문 가능한 KRW
# =========================================================

def get_available_krw(
    api_key: str,
    secret_key: str
):

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

        raise HTTPException(
            status_code=502,
            detail=str(e)
        )

    if response.status_code != 200:

        raise HTTPException(
            status_code=response.status_code,
            detail=get_error_detail(response)
        )

    for account in response.json():

        if account.get("currency") == "KRW":

            balance = float(
                account.get(
                    "balance",
                    0
                )
            )

            locked = float(
                account.get(
                    "locked",
                    0
                )
            )

            return max(
                balance - locked,
                0
            )

    return 0.0


# =========================================================
# 주문 가능한 코인 잔고
# =========================================================

def get_available_coin_balance(
    coin: str,
    api_key: str,
    secret_key: str
):

    coin = clean_coin_name(coin)

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

        raise HTTPException(
            status_code=502,
            detail=str(e)
        )

    if response.status_code != 200:

        raise HTTPException(
            status_code=response.status_code,
            detail=get_error_detail(response)
        )

    for account in response.json():

        if account.get("currency") == coin:

            balance = float(
                account.get(
                    "balance",
                    0
                )
            )

            locked = float(
                account.get(
                    "locked",
                    0
                )
            )

            return max(
                balance - locked,
                0
            )

    return 0.0


# =========================================================
# 주문 생성 - 매수
# =========================================================

def place_bid_order(
    coin: str,
    krw_amount: float,
    api_key: str,
    secret_key: str
):

    coin = clean_coin_name(coin)

    query = {

        "market": f"KRW-{coin}",

        "side": "bid",

        "price": str(krw_amount),

        "ord_type": "price"

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

        raise HTTPException(
            status_code=502,
            detail=str(e)
        )

    if response.status_code not in (200, 201):

        raise HTTPException(

            status_code=response.status_code,

            detail=get_error_detail(response)

        )

    return response.json()


# =========================================================
# 주문 생성 - 매도
# =========================================================

def place_ask_order(
    coin: str,
    ratio: float,
    api_key: str,
    secret_key: str
):

    coin = clean_coin_name(coin)

    # -----------------------------------------------------
    # 실제 현재 주문 가능한 코인 잔고 조회
    # -----------------------------------------------------

    coin_amount = get_available_coin_balance(

        coin,

        api_key,

        secret_key

    )

    if coin_amount <= 0:

        raise HTTPException(

            status_code=400,

            detail=f"No available balance for {coin}"

        )

    # -----------------------------------------------------
    # 가용잔고 × 매도비율
    # -----------------------------------------------------

    sell_volume = (

        coin_amount *
        ratio

    )

    if sell_volume <= 0:

        raise HTTPException(

            status_code=400,

            detail="Sell volume is zero"

        )

    logger.info(

        f"SELL calculation | "
        f"{coin} | "
        f"available={coin_amount:.12f} | "
        f"ratio={ratio * 100:.2f}% | "
        f"sell={sell_volume:.12f}"

    )

    query = {

        "market": f"KRW-{coin}",

        "side": "ask",

        "volume": str(sell_volume),

        "ord_type": "market"

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

        raise HTTPException(

            status_code=502,

            detail=str(e)

        )

    if response.status_code not in (200, 201):

        raise HTTPException(

            status_code=response.status_code,

            detail=get_error_detail(response)

        )

    return response.json()


# =========================================================
# 주문 체결 결과 조회
# =========================================================

def wait_for_order_complete(
    order_uuid: str,
    api_key: str,
    secret_key: str
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
            time.time() - start_time
            >= ORDER_WAIT_TIMEOUT
        ):

            return order

        time.sleep(
            ORDER_WAIT_INTERVAL
        )


# =========================================================
# 주문 체결 데이터 계산
# =========================================================

def parse_order_result(order):

    executed_volume = float(
        order.get(
            "executed_volume",
            0
        )
    )

    executed_funds = float(
        order.get(
            "executed_funds",
            0
        )
    )

    paid_fee = float(
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
# 매수 기록 DB 저장
# =========================================================

def save_buy_trade(
    result,
    requested_amount,
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

            "buy",

            requested_amount,

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

        # -------------------------------------------------
        # 실제 매수원가
        #
        # 매수금액 + 매수수수료
        # -------------------------------------------------

        total_cost = (
            result["executed_funds"]
            + result["paid_fee"]
        )

        cost_per_unit = (

            total_cost /
            result["executed_volume"]

            if result["executed_volume"] > 0

            else 0

        )

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
        f"executed="
        f"{result['executed_funds']:,.0f} KRW | "
        f"volume="
        f"{result['executed_volume']:.12f} | "
        f"avg="
        f"{result['avg_price']:,.0f}"

    )


# =========================================================
# FIFO 매도 원가 계산
# =========================================================

def calculate_fifo_cost(
    coin: str,
    sell_volume: float,
    sell_trade_id: int
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

            available = float(
                lot["remaining_volume"]
            )

            use_volume = min(
                remaining_to_sell,
                available
            )

            cost_per_unit = float(
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

            # 부동소수점 잔여량 보정
            if abs(new_remaining) < 1e-15:

                new_remaining = 0.0

            cursor.execute("""
            UPDATE buy_lots
            SET remaining_volume = ?
            WHERE id = ?
            """, (

                new_remaining,

                lot["id"]

            ))

            remaining_to_sell -= use_volume

        if remaining_to_sell > 1e-12:

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
# 매도 기록 저장
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

    # -----------------------------------------------------
    # FIFO 매수원가
    # -----------------------------------------------------

    realized_cost = calculate_fifo_cost(

        coin,

        result["executed_volume"],

        trade_id

    )

    # -----------------------------------------------------
    # 실제 매도 수령액
    # -----------------------------------------------------

    net_sell_amount = (

        result["executed_funds"]
        - result["paid_fee"]

    )

    # -----------------------------------------------------
    # 실현손익
    # -----------------------------------------------------

    realized_profit = (

        net_sell_amount
        - realized_cost

    )

    if realized_cost > 0:

        realized_return = (

            realized_profit
            / realized_cost
            * 100

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

    logger.info(

        f"SELL saved | "
        f"{coin} | "
        f"sell={net_sell_amount:,.0f} | "
        f"cost={realized_cost:,.0f} | "
        f"profit={realized_profit:,.0f} | "
        f"return={realized_return:.2f}%"

    )

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
# 현재 보유 코인의 DB 원가
#
# 남아 있는 BUY LOT 기준
# =========================================================

def get_open_lot_summary():

    with db_lock:

        conn = get_db()

        rows = conn.execute("""
        SELECT
            coin,
            SUM(remaining_volume) AS volume,
            SUM(
                remaining_volume *
                cost_per_unit
            ) AS cost
        FROM buy_lots
        WHERE remaining_volume > 0
        GROUP BY coin
        """).fetchall()

        conn.close()

    result = {}

    for row in rows:

        volume = float(
            row["volume"] or 0
        )

        cost = float(
            row["cost"] or 0
        )

        avg_cost = (

            cost / volume

            if volume > 0

            else 0

        )

        result[row["coin"]] = {

            "volume":
                volume,

            "cost":
                cost,

            "avg_cost":
                avg_cost

        }

    return result


# =========================================================
# 최근 매수
# =========================================================

def get_recent_buys(limit=10):

    with db_lock:

        conn = get_db()

        rows = conn.execute("""
        SELECT *
        FROM trades
        WHERE side = 'buy'
        ORDER BY id DESC
        LIMIT ?
        """, (limit,)).fetchall()

        conn.close()

    return [
        dict(row)
        for row in rows
    ]


# =========================================================
# 최근 매도
# =========================================================

def get_recent_sells(limit=10):

    with db_lock:

        conn = get_db()

        rows = conn.execute("""
        SELECT *
        FROM trades
        WHERE side = 'sell'
        ORDER BY id DESC
        LIMIT ?
        """, (limit,)).fetchall()

        conn.close()

    return [
        dict(row)
        for row in rows
    ]


# =========================================================
# 전체 거래
# =========================================================

def get_recent_trades(limit=30):

    with db_lock:

        conn = get_db()

        rows = conn.execute("""
        SELECT *
        FROM trades
        ORDER BY id DESC
        LIMIT ?
        """, (limit,)).fetchall()

        conn.close()

    return [
        dict(row)
        for row in rows
    ]


# =========================================================
# TradingView Webhook
# =========================================================

@app.post("/tradingview_webhook")
async def tradingview_webhook(
    payload: TradingViewPayload
):

    global latest_order_info
    global latest_sell_result
    global latest_webhook_time

    action = str(
        payload.Action
    ).strip()

    coin = clean_coin_name(
        payload.coin
    )

    ratio = float(
        payload.volume
    )

    api_key = payload.api_key

    secret_key = payload.secret_key

    if not api_key or not secret_key:

        raise HTTPException(

            status_code=400,

            detail="API key is required"

        )

    if not coin:

        raise HTTPException(

            status_code=400,

            detail="Invalid coin"

        )

    if not (0 < ratio <= 1):

        raise HTTPException(

            status_code=400,

            detail=(
                "volume must be "
                "between 0 and 1"
            )

        )

    # =====================================================
    # BUY
    # =====================================================

    if action.lower() == "buy":

        # -------------------------------------------------
        # 중요:
        # 총자산이 아니라 현재 주문 가능한 KRW만 조회
        # -------------------------------------------------

        available_krw = get_available_krw(

            api_key,

            secret_key

        )

        if available_krw <= 0:

            raise HTTPException(

                status_code=400,

                detail="No available KRW balance"

            )

        # -------------------------------------------------
        # 주문금액 =
        # 현재 주문가능 KRW × volume
        # -------------------------------------------------

        amount_to_invest = (

            available_krw *
            ratio

        )

        if amount_to_invest < 5000:

            raise HTTPException(

                status_code=400,

                detail={

                    "message":
                        "Amount too small. "
                        "Minimum 5,000 KRW.",

                    "available_krw":
                        available_krw,

                    "ratio":
                        ratio,

                    "calculated_amount":
                        amount_to_invest

                }

            )

        logger.info(

            f"BUY request | "
            f"{coin} | "
            f"available KRW="
            f"{available_krw:,.0f} | "
            f"ratio="
            f"{ratio * 100:.2f}% | "
            f"order="
            f"{amount_to_invest:,.0f} KRW"

        )

        # -------------------------------------------------
        # 주문
        # -------------------------------------------------

        order = place_bid_order(

            coin,

            amount_to_invest,

            api_key,

            secret_key

        )

        order_uuid = order["uuid"]

        # -------------------------------------------------
        # 실제 체결 결과
        # -------------------------------------------------

        completed_order = wait_for_order_complete(

            order_uuid,

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
                        "Order was not completed",

                    "order":
                        completed_order

                }

            )

        # -------------------------------------------------
        # DB 저장
        # -------------------------------------------------

        save_buy_trade(

            result,

            amount_to_invest,

            ratio

        )

        latest_order_info = (

            f"BUY {coin} | "

            f"가용KRW "
            f"{available_krw:,.0f}원 | "

            f"{ratio * 100:.2f}% | "

            f"실제체결 "
            f"{result['executed_funds']:,.0f}원 | "

            f"{result['executed_volume']:.12f} "
            f"{coin}"

        )

        # -------------------------------------------------
        # 웹훅 발생시간
        # -------------------------------------------------

        latest_webhook_time = now_string()

        # -------------------------------------------------
        # 웹훅이 발생했으므로 자산 즉시 갱신
        # -------------------------------------------------

        fetch_upbit_assets(

            api_key,

            secret_key

        )

        return {

            "Action": "Buy",

            "Coin": coin,

            "Available KRW":
                available_krw,

            "Buy Ratio":
                ratio,

            "Requested KRW":
                amount_to_invest,

            "Actual Executed KRW":
                result["executed_funds"],

            "Executed Volume":
                result["executed_volume"],

            "Average Price":
                result["avg_price"],

            "Fee":
                result["paid_fee"],

            "Order UUID":
                result["uuid"]

        }

    # =====================================================
    # SELL
    # =====================================================

    elif action.lower() == "sell":

        # -------------------------------------------------
        # 실제 매도 가능한 코인 잔고 조회
        # -------------------------------------------------

        available_coin = get_available_coin_balance(

            coin,

            api_key,

            secret_key

        )

        if available_coin <= 0:

            raise HTTPException(

                status_code=400,

                detail=f"No available balance for {coin}"

            )

        # -------------------------------------------------
        # 매도 예정 수량
        #
        # 코인 가용잔고 × volume
        # -------------------------------------------------

        expected_sell_volume = (

            available_coin *
            ratio

        )

        logger.info(

            f"SELL request | "
            f"{coin} | "
            f"available="
            f"{available_coin:.12f} | "
            f"ratio="
            f"{ratio * 100:.2f}% | "
            f"expected="
            f"{expected_sell_volume:.12f}"

        )

        # -------------------------------------------------
        # 주문
        # -------------------------------------------------

        order = place_ask_order(

            coin,

            ratio,

            api_key,

            secret_key

        )

        order_uuid = order["uuid"]

        # -------------------------------------------------
        # 실제 체결
        # -------------------------------------------------

        completed_order = wait_for_order_complete(

            order_uuid,

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
                        "Sell order was not completed",

                    "order":
                        completed_order

                }

            )

        # -------------------------------------------------
        # DB 저장 + 실제 실현손익 계산
        # -------------------------------------------------

        sell_result = save_sell_trade(

            result,

            ratio

        )

        latest_sell_result = sell_result

        latest_order_info = (

            f"SELL {coin} | "

            f"보유가용 "
            f"{available_coin:.12f} {coin} | "

            f"{ratio * 100:.2f}% | "

            f"실제매도 "
            f"{result['executed_volume']:.12f} {coin} | "

            f"{sell_result['net_sell']:,.0f}원 | "

            f"손익 "
            f"{sell_result['profit']:+,.0f}원"

        )

        # -------------------------------------------------
        # 웹훅 발생시간
        # -------------------------------------------------

        latest_webhook_time = now_string()

        # -------------------------------------------------
        # 웹훅마다 잔고/자산 즉시 갱신
        # -------------------------------------------------

        fetch_upbit_assets(

            api_key,

            secret_key

        )

        return {

            "Action": "Sell",

            "Coin": coin,

            "Available Coin":
                available_coin,

            "Sell Ratio":
                ratio,

            "Expected Sell Volume":
                expected_sell_volume,

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
                result["uuid"]

        }

    else:

        raise HTTPException(

            status_code=400,

            detail="Action must be Buy or Sell"

        )


# =========================================================
# 자산 API
#
# 웹훅에서 마지막으로 조회한 잔고를 반환
# =========================================================

@app.get("/api/upbit-assets")
async def api_upbit_assets():

    return {

        "total_krw":
            latest_upbit_total_krw,

        "krw_balance":
            latest_upbit_krw,

        "available_krw":
            latest_upbit_available_krw,

        "updated_at":
            latest_upbit_update,

        "webhook_time":
            latest_webhook_time,

        "last_order":
            latest_order_info,

        "assets":
            latest_upbit_assets

    }


# =========================================================
# 실시간 현재가 API
#
# 잔고는 다시 조회하지 않음
# 현재가만 5초마다 조회
# =========================================================

@app.get("/api/live-prices")
async def api_live_prices():

    open_lots = get_open_lot_summary()

    result = []

    for asset in latest_upbit_assets:

        currency = asset["currency"]

        if currency == "KRW":

            result.append({

                "currency":
                    "KRW",

                "current_price":
                    1,

                "db_volume":
                    0,

                "db_cost":
                    0,

                "db_avg_cost":
                    0,

                "evaluation":
                    asset["evaluation_krw"],

                "profit":
                    0,

                "return":
                    0

            })

            continue

        market = asset["market"]

        current_price = get_ticker_price(
            market
        )

        # -------------------------------------------------
        # DB에 저장된 실제 매수 Lot
        # -------------------------------------------------

        lot = open_lots.get(

            currency,

            {

                "volume": 0,

                "cost": 0,

                "avg_cost": 0

            }

        )

        db_volume = float(
            lot["volume"]
        )

        db_cost = float(
            lot["cost"]
        )

        db_avg_cost = float(
            lot["avg_cost"]
        )

        # -------------------------------------------------
        # 현재 평가금액
        #
        # 실제 거래소 잔고 기준
        # -------------------------------------------------

        actual_balance = float(
            asset["balance"]
        )

        evaluation = (

            actual_balance *
            current_price

            if current_price > 0

            else 0

        )

        # -------------------------------------------------
        # 현재 수익률
        #
        # DB에 남아있는 매수원가 기준
        # -------------------------------------------------

        if (
            db_volume > 0
            and db_avg_cost > 0
            and current_price > 0
        ):

            return_percent = (

                (
                    current_price
                    - db_avg_cost
                )
                / db_avg_cost
                * 100

            )

            # 현재 DB 보유수량 기준 손익
            profit = (

                (
                    current_price
                    * db_volume
                )
                - db_cost

            )

        else:

            return_percent = 0.0

            profit = 0.0

        result.append({

            "currency":
                currency,

            "current_price":
                current_price,

            "db_volume":
                db_volume,

            "db_cost":
                db_cost,

            "db_avg_cost":
                db_avg_cost,

            "actual_balance":
                actual_balance,

            "evaluation":
                evaluation,

            "profit":
                profit,

            "return":
                return_percent

        })

    return {

        "updated_at":
            now_string(),

        "assets":
            result

    }


# =========================================================
# 거래 기록 API
# =========================================================

@app.get("/api/trades")
async def api_trades():

    return {

        "recent_buys":
            get_recent_buys(10),

        "recent_sells":
            get_recent_sells(10),

        "recent_trades":
            get_recent_trades(30)

    }


# =========================================================
# 대시보드
# =========================================================

@app.get("/", response_class=HTMLResponse)
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

<title>업비트 자동매매 대시보드</title>

<style>

* {
    box-sizing: border-box;
}

body {

    margin: 0;

    background: #0e1116;

    color: #f1f3f5;

    font-family:
        Arial,
        "Noto Sans KR",
        sans-serif;

}

.container {

    max-width: 1400px;

    margin: auto;

    padding: 20px;

}

h1 {

    margin: 0 0 5px 0;

    font-size: 24px;

}

.subtitle {

    color: #8b949e;

    font-size: 13px;

    margin-bottom: 20px;

}

.grid {

    display: grid;

    grid-template-columns:
        repeat(
            4,
            minmax(0, 1fr)
        );

    gap: 12px;

    margin-bottom: 15px;

}

.card {

    background: #171b22;

    border: 1px solid #292f38;

    border-radius: 12px;

    padding: 17px;

}

.title {

    color: #8b949e;

    font-size: 13px;

    margin-bottom: 9px;

}

.value {

    font-size: 22px;

    font-weight: 700;

}

.section {

    margin-top: 18px;

    margin-bottom: 10px;

    font-size: 17px;

    font-weight: 700;

}

.table-wrap {

    overflow-x: auto;

    background: #171b22;

    border:
        1px solid #292f38;

    border-radius: 12px;

}

table {

    width: 100%;

    border-collapse: collapse;

    min-width: 1100px;

}

th {

    padding: 12px;

    color: #8b949e;

    background: #12151b;

    font-size: 12px;

    text-align: right;

}

td {

    padding: 12px;

    border-top:
        1px solid #252a32;

    text-align: right;

    font-size: 13px;

}

th:first-child,
td:first-child {

    text-align: left;

}

.green {

    color: #3fb950;
