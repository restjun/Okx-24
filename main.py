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
import os

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
# 기본 설정
# =========================================================

SERVER_URL = "https://api.upbit.com"

REQUEST_TIMEOUT = 10
ORDER_WAIT_TIMEOUT = 15
ORDER_WAIT_INTERVAL = 0.5

MIN_ORDER_KRW = 5000


# =========================================================
# API KEY
# =========================================================
# 반드시 환경변수로 설정
#
# Windows:
# set UPBIT_ACCESS_KEY=YOUR_ACCESS_KEY
# set UPBIT_SECRET_KEY=YOUR_SECRET_KEY
#
# Linux:
# export UPBIT_ACCESS_KEY=YOUR_ACCESS_KEY
# export UPBIT_SECRET_KEY=YOUR_SECRET_KEY
# =========================================================

UPBIT_ACCESS_KEY = os.getenv(
    "UPBIT_ACCESS_KEY",
    ""
)

UPBIT_SECRET_KEY = os.getenv(
    "UPBIT_SECRET_KEY",
    ""
)


# =========================================================
# 기본 자동 손절 / 익절
# =========================================================

DEFAULT_AUTO_STOP_LOSS_KRW = 25_000.0

DEFAULT_AUTO_TAKE_PROFIT_KRW = 0.0


# =========================================================
# 모니터 설정
# =========================================================

AUTO_CHECK_INTERVAL = 2

UPBIT_ASSET_REFRESH_INTERVAL = 5


# =========================================================
# 리스크 설정
# =========================================================

DEFAULT_MONTH_START_AMOUNT = 2_500_000.0

DEFAULT_MAX_LOSS_RATE = 0.01


# =========================================================
# 기타
# =========================================================

KST = ZoneInfo("Asia/Seoul")

DB_FILE = "trading.db"


# =========================================================
# Lock
# =========================================================

db_lock = threading.Lock()

order_execution_lock = threading.Lock()

auto_exit_lock = threading.Lock()

auto_exit_in_progress = set()


# =========================================================
# ★ 수동 전체매도 상태
# =========================================================

manual_sell_all_lock = threading.Lock()

manual_sell_all_in_progress = False


# =========================================================
# 업비트 실시간 상태
# =========================================================

latest_upbit_assets = []

latest_upbit_total_krw = 0.0

latest_upbit_available_krw = 0.0

latest_upbit_update = "업비트 조회 대기"

latest_order_info = "주문 없음"

latest_bid_fee_rate = 0.0


# =========================================================
# 공통
# =========================================================

def safe_float(value, default=0.0):

    if value is None or value == "":
        return default

    try:
        return float(value)

    except Exception:
        return default


def safe_string(value, default=""):

    if value is None:
        return default

    try:
        return str(value).strip()

    except Exception:
        return default


def get_payload_value(
    data,
    names,
    default=None
):

    for name in names:

        if name in data:
            return data[name]

    return default


def now_string():

    return datetime.now(KST).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def clean_coin_name(coin):

    return (
        safe_string(coin)
        .upper()
        .replace("USDT.P", "")
        .replace("USDT", "")
        .replace("KRW-", "")
        .replace("KRW", "")
        .strip()
    )


def build_query_string(data):

    return urlencode(
        data,
        doseq=True
    )


def truncate_krw(amount):

    return float(
        int(
            max(amount, 0)
        )
    )


def truncate_volume(
    volume,
    decimals=8
):

    factor = 10 ** decimals

    return (
        int(
            max(volume, 0) * factor
        ) / factor
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

        payload["query_hash"] = hashlib.sha512(
            query_string.encode()
        ).hexdigest()

        payload["query_hash_alg"] = "SHA512"

    token = jwt.encode(
        payload,
        secret_key,
        algorithm="HS512"
    )

    return (
        token.decode()
        if isinstance(token, bytes)
        else token
    )


def create_auth_headers(
    api_key,
    secret_key,
    query_string=""
):

    return {
        "Authorization":
            f"Bearer {create_jwt(api_key, secret_key, query_string)}",

        "Accept":
            "application/json",

        "Content-Type":
            "application/json"
    }


def get_error_detail(response):

    try:
        return response.json()

    except Exception:

        return {
            "status_code":
                response.status_code,

            "text":
                response.text
        }


# =========================================================
# DB
# =========================================================

def get_db():

    conn = sqlite3.connect(
        DB_FILE,
        timeout=30
    )

    conn.row_factory = sqlite3.Row

    return conn


def init_db():

    with db_lock:

        conn = get_db()

        c = conn.cursor()


        c.execute("""
        CREATE TABLE IF NOT EXISTS trades(
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


        c.execute("""
        CREATE TABLE IF NOT EXISTS buy_lots(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id INTEGER NOT NULL,
            coin TEXT NOT NULL,
            original_volume REAL NOT NULL,
            remaining_volume REAL NOT NULL,
            cost_per_unit REAL NOT NULL,
            total_cost REAL NOT NULL,
            fee REAL DEFAULT 0,
            created_at TEXT,
            FOREIGN KEY(trade_id) REFERENCES trades(id)
        )
        """)


        c.execute("""
        CREATE TABLE IF NOT EXISTS sell_allocations(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sell_trade_id INTEGER NOT NULL,
            buy_lot_id INTEGER NOT NULL,
            volume REAL NOT NULL,
            cost REAL NOT NULL,
            FOREIGN KEY(sell_trade_id) REFERENCES trades(id),
            FOREIGN KEY(buy_lot_id) REFERENCES buy_lots(id)
        )
        """)


        c.execute("""
        CREATE TABLE IF NOT EXISTS settings(
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """)


        c.execute("""
        CREATE TABLE IF NOT EXISTS trailing_exit_state(
            coin TEXT PRIMARY KEY,
            highest_stage INTEGER DEFAULT 0,
            highest_profit REAL DEFAULT 0,
            updated_at TEXT
        )
        """)


        c.execute("""
        CREATE TABLE IF NOT EXISTS coin_target_exit(
            coin TEXT PRIMARY KEY,
            enabled INTEGER DEFAULT 0,
            target_stage INTEGER DEFAULT 0,
            target_profit REAL DEFAULT 0,
            updated_at TEXT
        )
        """)


        defaults = {

            "month_start_amount":
                DEFAULT_MONTH_START_AMOUNT,

            "max_loss_rate":
                DEFAULT_MAX_LOSS_RATE,

            "auto_stop_loss_krw":
                DEFAULT_AUTO_STOP_LOSS_KRW,

            "auto_take_profit_krw":
                DEFAULT_AUTO_TAKE_PROFIT_KRW
        }


        for key, value in defaults.items():

            c.execute(
                """
                INSERT OR IGNORE INTO settings(
                    key,
                    value
                )
                VALUES(?,?)
                """,
                (
                    key,
                    str(value)
                )
            )


        conn.commit()

        conn.close()


# =========================================================
# 설정
# =========================================================

def get_settings(keys):

    if not keys:
        return {}

    with db_lock:

        conn = get_db()

        placeholders = ",".join(
            "?" * len(keys)
        )

        rows = conn.execute(
            f"""
            SELECT key,value
            FROM settings
            WHERE key IN ({placeholders})
            """,
            keys
        ).fetchall()

        conn.close()


    return {
        r["key"]: r["value"]
        for r in rows
    }


def get_risk_settings():

    v = get_settings([
        "month_start_amount",
        "max_loss_rate"
    ])


    month = safe_float(
        v.get("month_start_amount"),
        DEFAULT_MONTH_START_AMOUNT
    )


    rate = safe_float(
        v.get("max_loss_rate"),
        DEFAULT_MAX_LOSS_RATE
    )


    if month <= 0:
        month = DEFAULT_MONTH_START_AMOUNT


    if rate <= 0:
        rate = DEFAULT_MAX_LOSS_RATE


    return month, rate


def get_auto_exit_settings():

    v = get_settings([
        "auto_stop_loss_krw",
        "auto_take_profit_krw"
    ])


    stop = max(
        safe_float(
            v.get("auto_stop_loss_krw"),
            DEFAULT_AUTO_STOP_LOSS_KRW
        ),
        0
    )


    take = max(
        safe_float(
            v.get("auto_take_profit_krw"),
            DEFAULT_AUTO_TAKE_PROFIT_KRW
        ),
        0
    )


    return stop, take


def get_month_start_amount():

    return get_risk_settings()[0]


def get_max_loss_rate():

    return get_risk_settings()[1]


def get_risk_unit():

    month, rate = get_risk_settings()

    return max(
        month * rate,
        0
    )


def save_risk_settings(
    month,
    rate
):

    month = truncate_krw(month)

    with db_lock:

        conn = get_db()

        conn.execute(
            """
            INSERT OR REPLACE INTO settings(
                key,value
            )
            VALUES(?,?)
            """,
            (
                "month_start_amount",
                str(month)
            )
        )

        conn.execute(
            """
            INSERT OR REPLACE INTO settings(
                key,value
            )
            VALUES(?,?)
            """,
            (
                "max_loss_rate",
                str(rate)
            )
        )

        conn.commit()

        conn.close()


def save_auto_exit_settings(
    stop,
    take
):

    stop = truncate_krw(stop)

    take = truncate_krw(take)

    with db_lock:

        conn = get_db()

        conn.execute(
            """
            INSERT OR REPLACE INTO settings(
                key,value
            )
            VALUES(?,?)
            """,
            (
                "auto_stop_loss_krw",
                str(stop)
            )
        )

        conn.execute(
            """
            INSERT OR REPLACE INTO settings(
                key,value
            )
            VALUES(?,?)
            """,
            (
                "auto_take_profit_krw",
                str(take)
            )
        )

        conn.commit()

        conn.close()


# =========================================================
# 리스크 단계
# =========================================================

def calculate_risk_stage(
    profit_amount
):

    risk = get_risk_unit()

    if (
        risk <= 0
        or
        profit_amount <= 0
    ):
        return 0

    return max(
        int(profit_amount / risk),
        0
    )


# =========================================================
# 트레일링 상태
# =========================================================

def get_trailing_state(coin):

    coin = clean_coin_name(coin)

    with db_lock:

        conn = get_db()

        row = conn.execute(
            """
            SELECT
                highest_stage,
                highest_profit,
                updated_at
            FROM trailing_exit_state
            WHERE coin=?
            """,
            (coin,)
        ).fetchone()

        conn.close()


    if row is None:

        return {
            "highest_stage": 0,
            "highest_profit": 0.0,
            "updated_at": ""
        }


    return {

        "highest_stage":
            int(
                safe_float(
                    row["highest_stage"]
                )
            ),

        "highest_profit":
            safe_float(
                row["highest_profit"]
            ),

        "updated_at":
            safe_string(
                row["updated_at"]
            )
    }


def save_trailing_state(
    coin,
    stage,
    profit
):

    coin = clean_coin_name(coin)

    with db_lock:

        conn = get_db()

        conn.execute(
            """
            INSERT OR REPLACE INTO trailing_exit_state(
                coin,
                highest_stage,
                highest_profit,
                updated_at
            )
            VALUES(?,?,?,?)
            """,
            (
                coin,
                int(stage),
                float(profit),
                now_string()
            )
        )

        conn.commit()

        conn.close()


def delete_trailing_state(coin):

    with db_lock:

        conn = get_db()

        conn.execute(
            """
            DELETE FROM trailing_exit_state
            WHERE coin=?
            """,
            (
                clean_coin_name(coin),
            )
        )

        conn.commit()

        conn.close()


def update_trailing_stage(
    coin,
    current_profit
):

    coin = clean_coin_name(coin)

    current_profit = safe_float(
        current_profit
    )

    risk = get_risk_unit()

    current_stage = calculate_risk_stage(
        current_profit
    )

    state = get_trailing_state(
        coin
    )

    highest_stage = state[
        "highest_stage"
    ]

    highest_profit = state[
        "highest_profit"
    ]

    if current_stage > highest_stage:

        highest_stage = current_stage

        highest_profit = current_profit

        save_trailing_state(
            coin,
            highest_stage,
            highest_profit
        )

        logger.info(
            f"TRAILING NEW HIGH | "
            f"{coin} | "
            f"최고 1:{highest_stage} | "
            f"{highest_profit:+,.0f}원"
        )

    elif current_profit > highest_profit:

        highest_profit = current_profit

        save_trailing_state(
            coin,
            highest_stage,
            highest_profit
        )

    trigger_profit = (

        (highest_stage - 1) * risk

        if (
            highest_stage > 0
            and
            risk > 0
        )

        else 0
    )

    return {

        "trigger":
            (
                highest_stage > 0
                and
                current_profit <= trigger_profit
            ),

        "current_stage":
            current_stage,

        "highest_stage":
            highest_stage,

        "highest_profit":
            highest_profit,

        "current_profit":
            current_profit,

        "risk_unit":
            risk,

        "sell_trigger_profit":
            trigger_profit
    }


# =========================================================
# 코인별 목표
# =========================================================

def get_coin_target(coin):

    coin = clean_coin_name(coin)

    with db_lock:

        conn = get_db()

        row = conn.execute(
            """
            SELECT
                coin,
                enabled,
                target_stage,
                target_profit,
                updated_at
            FROM coin_target_exit
            WHERE coin=?
            """,
            (coin,)
        ).fetchone()

        conn.close()


    if row is None:

        return {

            "coin": coin,
            "enabled": False,
            "target_stage": 0,
            "target_profit": 0.0,
            "updated_at": ""
        }


    return {

        "coin": coin,

        "enabled":
            bool(row["enabled"]),

        "target_stage":
            int(
                safe_float(
                    row["target_stage"]
                )
            ),

        "target_profit":
            safe_float(
                row["target_profit"]
            ),

        "updated_at":
            safe_string(
                row["updated_at"]
            )
    }


def save_coin_target(
    coin,
    enabled,
    target_stage
):

    coin = clean_coin_name(
        coin
    )

    target_stage = int(
        max(
            min(
                target_stage,
                10
            ),
            0
        )
    )

    risk = get_risk_unit()

    target_profit = (

        risk * target_stage

        if (
            enabled
            and
            target_stage > 0
        )

        else 0
    )

    with db_lock:

        conn = get_db()

        conn.execute(
            """
            INSERT OR REPLACE INTO coin_target_exit(
                coin,
                enabled,
                target_stage,
                target_profit,
                updated_at
            )
            VALUES(?,?,?,?,?)
            """,
            (
                coin,
                1 if enabled else 0,
                target_stage,
                target_profit,
                now_string()
            )
        )

        conn.commit()

        conn.close()

    return get_coin_target(
        coin
    )


def delete_coin_target(coin):

    coin = clean_coin_name(
        coin
    )

    with db_lock:

        conn = get_db()

        conn.execute(
            """
            DELETE FROM coin_target_exit
            WHERE coin=?
            """,
            (coin,)
        )

        conn.commit()

        conn.close()


# =========================================================
# 최우선 고정금액 손절 판정
# =========================================================

def is_fixed_stop_loss_hit(
    profit
):

    stop, _ = get_auto_exit_settings()

    if stop <= 0:
        return False

    return safe_float(profit) <= -stop


# =========================================================
# 업비트 조회
# =========================================================

def get_ticker_price(market):

    try:

        r = requests.get(
            f"{SERVER_URL}/v1/ticker",
            params={
                "markets": market
            },
            timeout=REQUEST_TIMEOUT
        )

        if r.status_code != 200:

            logger.error(
                f"TICKER ERROR | "
                f"{market} | "
                f"{r.status_code}"
            )

            return 0.0

        data = r.json()

        return (

            safe_float(
                data[0].get(
                    "trade_price",
                    0
                )
            )

            if data

            else 0.0
        )

    except Exception as e:

        logger.error(
            f"TICKER EXCEPTION | "
            f"{market} | {e}"
        )

        return 0.0


def get_order_chance(
    market,
    api_key,
    secret_key
):

    global latest_bid_fee_rate

    query = {
        "market": market
    }

    qs = build_query_string(
        query
    )

    try:

        r = requests.get(
            SERVER_URL + "/v1/orders/chance",
            params=query,
            headers=create_auth_headers(
                api_key,
                secret_key,
                qs
            ),
            timeout=REQUEST_TIMEOUT
        )

    except requests.RequestException as e:

        logger.error(
            f"ORDER CHANCE FAILED | "
            f"{market} | {e}"
        )

        return {
            "bid_fee": 0.0,
            "min_total": MIN_ORDER_KRW
        }

    if r.status_code != 200:

        logger.error(
            f"ORDER CHANCE ERROR | "
            f"{market} | "
            f"{r.status_code}"
        )

        return {
            "bid_fee": 0.0,
            "min_total": MIN_ORDER_KRW
        }

    try:

        data = r.json()

    except Exception:

        return {
            "bid_fee": 0.0,
            "min_total": MIN_ORDER_KRW
        }

    fee = safe_float(
        data.get("bid_fee")
    )

    minimum = safe_float(
        data.get("market", {})
        .get("bid", {})
        .get("min_total"),
        MIN_ORDER_KRW
    )

    if minimum <= 0:
        minimum = MIN_ORDER_KRW

    latest_bid_fee_rate = fee

    return {
        "bid_fee": fee,
        "min_total": minimum,
        "data": data
    }


def calculate_fee_safe_buy_amount(
    available,
    fee
):

    if available <= 0:
        return 0.0

    fee = max(
        fee,
        0
    )

    amount = truncate_krw(
        available /
        (1 + fee)
    )

    if amount > 1:
        amount -= 1

    return max(
        amount,
        0
    )


# =========================================================
# 업비트 자산
# =========================================================

def fetch_upbit_assets(
    api_key,
    secret_key
):

    global latest_upbit_assets
    global latest_upbit_total_krw
    global latest_upbit_available_krw
    global latest_upbit_update

    try:

        r = requests.get(
            SERVER_URL + "/v1/accounts",
            headers=create_auth_headers(
                api_key,
                secret_key
            ),
            timeout=REQUEST_TIMEOUT
        )

    except requests.RequestException as e:

        logger.error(
            f"ACCOUNT REQUEST FAILED | {e}"
        )

        return -1

    if r.status_code != 200:

        logger.error(
            f"ACCOUNT ERROR | "
            f"{r.status_code}"
        )

        return -1

    try:

        accounts = r.json()

    except Exception:

        return -1

    assets = []

    total = 0.0

    available = 0.0

    for a in accounts:

        if a.get("currency") == "KRW":

            balance = safe_float(
                a.get("balance")
            )

            locked = safe_float(
                a.get("locked")
            )

            available = max(
                balance,
                0
            )

            total += (
                balance +
                locked
            )

            break

    for a in accounts:

        currency = a.get(
            "currency",
            ""
        )

        if currency == "KRW":
            continue

        balance = safe_float(
            a.get("balance")
        )

        locked = safe_float(
            a.get("locked")
        )

        avg = safe_float(
            a.get("avg_buy_price")
        )

        if balance <= 0:
            continue

        market = f"KRW-{currency}"

        current = get_ticker_price(
            market
        )

        if current <= 0:
            continue

        evaluation = (
            balance *
            current
        )

        total += evaluation

        if evaluation < MIN_ORDER_KRW:
            continue

        buy_amount = (
            balance *
            avg
        )

        profit = (
            evaluation -
            buy_amount
        )

        rate = (

            profit /
            buy_amount *
            100

            if buy_amount > 0

            else 0
        )

        assets.append({

            "currency": currency,

            "balance": balance,

            "locked": locked,

            "avg_buy_price": avg,

            "buy_amount_krw":
                buy_amount,

            "current_price":
                current,

            "evaluation_krw":
                evaluation,

            "profit_amount":
                profit,

            "profit_rate":
                rate,

            "market":
                market
        })

    assets.sort(
        key=lambda x:
            -x["evaluation_krw"]
    )

    latest_upbit_assets = assets

    latest_upbit_total_krw = total

    latest_upbit_available_krw = available

    latest_upbit_update = now_string()

    return total


def get_real_upbit_positions():

    if (
        not UPBIT_ACCESS_KEY
        or
        not UPBIT_SECRET_KEY
    ):
        return []

    try:

        r = requests.get(
            SERVER_URL + "/v1/accounts",
            headers=create_auth_headers(
                UPBIT_ACCESS_KEY,
                UPBIT_SECRET_KEY
            ),
            timeout=REQUEST_TIMEOUT
        )

    except requests.RequestException as e:

        logger.error(
            f"REAL POSITION REQUEST FAILED | {e}"
        )

        return []

    if r.status_code != 200:
        return []

    try:

        accounts = r.json()

    except Exception:

        return []

    positions = []

    for a in accounts:

        coin = clean_coin_name(
            a.get("currency")
        )

        if (
            not coin
            or
            coin == "KRW"
        ):
            continue

        balance = safe_float(
            a.get("balance")
        )

        locked = safe_float(
            a.get("locked")
        )

        avg = safe_float(
            a.get("avg_buy_price")
        )

        if (
            balance <= 0
            or
            avg <= 0
        ):
            continue

        market = f"KRW-{coin}"

        current = get_ticker_price(
            market
        )

        if current <= 0:
            continue

        buy_amount = (
            balance *
            avg
        )

        value = (
            balance *
            current
        )

        profit = (
            value -
            buy_amount
        )

        positions.append({

            "currency": coin,

            "market": market,

            "balance": balance,

            "locked": locked,

            "avg_buy_price": avg,

            "buy_amount_krw":
                buy_amount,

            "current_price":
                current,

            "evaluation_krw":
                value,

            "profit_amount":
                profit,

            "profit_rate": (

                profit /
                buy_amount *
                100

                if buy_amount > 0

                else 0
            )
        })

    return positions


def get_coin_balance(
    coin,
    api_key,
    secret_key
):

    coin = clean_coin_name(
        coin
    )

    r = requests.get(
        SERVER_URL + "/v1/accounts",
        headers=create_auth_headers(
            api_key,
            secret_key
        ),
        timeout=REQUEST_TIMEOUT
    )

    if r.status_code != 200:

        raise HTTPException(
            status_code=r.status_code,
            detail=get_error_detail(r)
        )

    for a in r.json():

        if a.get("currency") == coin:

            return max(
                safe_float(
                    a.get("balance")
                ),
                0
            )

    return 0.0


# =========================================================
# 주문
# =========================================================

def place_bid_order(
    coin,
    amount,
    api_key,
    secret_key
):

    coin = clean_coin_name(
        coin
    )

    amount = truncate_krw(
        amount
    )

    if amount < MIN_ORDER_KRW:

        raise HTTPException(
            status_code=400,
            detail=(
                f"매수금액 "
                f"{amount:,.0f}원 < "
                f"최소 {MIN_ORDER_KRW:,}원"
            )
        )

    query = {

        "market":
            f"KRW-{coin}",

        "side":
            "bid",

        "price":
            str(int(amount)),

        "ord_type":
            "price"
    }

    qs = build_query_string(
        query
    )

    r = requests.post(
        SERVER_URL + "/v1/orders",
        json=query,
        headers=create_auth_headers(
            api_key,
            secret_key,
            qs
        ),
        timeout=REQUEST_TIMEOUT
    )

    if r.status_code not in (
        200,
        201
    ):

        raise HTTPException(
            status_code=r.status_code,
            detail=get_error_detail(r)
        )

    return r.json()


def place_ask_order(
    coin,
    volume,
    api_key,
    secret_key
):

    coin = clean_coin_name(
        coin
    )

    volume = truncate_volume(
        volume,
        8
    )

    if volume <= 0:

        raise HTTPException(
            status_code=400,
            detail="매도수량이 0입니다."
        )

    query = {

        "market":
            f"KRW-{coin}",

        "side":
            "ask",

        "volume":
            f"{volume:.8f}",

        "ord_type":
            "market"
    }

    qs = build_query_string(
        query
    )

    r = requests.post(
        SERVER_URL + "/v1/orders",
        json=query,
        headers=create_auth_headers(
            api_key,
            secret_key,
            qs
        ),
        timeout=REQUEST_TIMEOUT
    )

    if r.status_code not in (
        200,
        201
    ):

        raise HTTPException(
            status_code=r.status_code,
            detail=get_error_detail(r)
        )

    return r.json()


def wait_for_order_complete(
    order_uuid,
    api_key,
    secret_key
):

    start = time.time()

    while True:

        query = {
            "uuid":
                order_uuid
        }

        qs = build_query_string(
            query
        )

        try:

            r = requests.get(
                SERVER_URL + "/v1/order",
                params=query,
                headers=create_auth_headers(
                    api_key,
                    secret_key,
                    qs
                ),
                timeout=REQUEST_TIMEOUT
            )

        except requests.RequestException:

            r = None

        if (
            r is not None
            and
            r.status_code == 200
        ):

            order = r.json()

            state = order.get(
                "state",
                ""
            )

            if state in (
                "done",
                "cancel"
            ):

                return order

            if (
                time.time() -
                start >=
                ORDER_WAIT_TIMEOUT
            ):

                return order

        elif (
            time.time() -
            start >=
            ORDER_WAIT_TIMEOUT
        ):

            return {

                "uuid":
                    order_uuid,

                "state":
                    "timeout"
            }

        time.sleep(
            ORDER_WAIT_INTERVAL
        )


def parse_order_result(order):

    volume = safe_float(
        order.get(
            "executed_volume"
        )
    )

    funds = safe_float(
        order.get(
            "executed_funds"
        )
    )

    fee = safe_float(
        order.get(
            "paid_fee"
        )
    )

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
            volume,

        "executed_funds":
            funds,

        "paid_fee":
            fee,

        "avg_price":
            (
                funds / volume
                if volume > 0
                else 0
            ),

        "created_at":
            order.get(
                "created_at"
            ),

        "trades":
            order.get(
                "trades",
                []
            )
    }


# =========================================================
# 거래 DB
# =========================================================

def save_buy_trade(
    result,
    requested_amount,
    stop_loss
):

    coin = result[
        "market"
    ].replace(
        "KRW-",
        ""
    )

    created = (
        result["created_at"]
        or
        now_string()
    )

    completed = now_string()

    with db_lock:

        conn = get_db()

        c = conn.cursor()

        c.execute(
            """
            INSERT INTO trades(
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
            VALUES(
                ?,?,?,?,?,?,?,?,?,?,?,?
            )
            """,
            (
                result["uuid"],
                coin,
                "buy",
                requested_amount,
                result["executed_funds"],
                result["executed_volume"],
                result["avg_price"],
                result["paid_fee"],
                stop_loss,
                created,
                completed,
                result["state"]
            )
        )

        trade_id = c.lastrowid

        total_cost = (
            result["executed_funds"]
            +
            result["paid_fee"]
        )

        cpu = (

            total_cost /
            result["executed_volume"]

            if result["executed_volume"] > 0

            else 0
        )

        c.execute(
            """
            INSERT INTO buy_lots(
                trade_id,
                coin,
                original_volume,
                remaining_volume,
                cost_per_unit,
                total_cost,
                fee,
                created_at
            )
            VALUES(
                ?,?,?,?,?,?,?,?
            )
            """,
            (
                trade_id,
                coin,
                result["executed_volume"],
                result["executed_volume"],
                cpu,
                total_cost,
                result["paid_fee"],
                completed
            )
        )

        conn.commit()

        conn.close()


def calculate_fifo_cost(
    coin,
    sell_volume,
    sell_trade_id
):

    remain = sell_volume

    total_cost = 0

    allocations = []

    with db_lock:

        conn = get_db()

        c = conn.cursor()

        lots = c.execute(
            """
            SELECT *
            FROM buy_lots
            WHERE coin=?
              AND remaining_volume>0
            ORDER BY id ASC
            """,
            (coin,)
        ).fetchall()

        for lot in lots:

            if remain <= 0:
                break

            available = safe_float(
                lot["remaining_volume"]
            )

            use = min(
                remain,
                available
            )

            cost = (
                use *
                safe_float(
                    lot["cost_per_unit"]
                )
            )

            total_cost += cost

            allocations.append(
                (
                    lot["id"],
                    use,
                    cost
                )
            )

            c.execute(
                """
                UPDATE buy_lots
                SET remaining_volume=?
                WHERE id=?
                """,
                (
                    available - use,
                    lot["id"]
                )
            )

            remain -= use

        for (
            lot_id,
            volume,
            cost
        ) in allocations:

            c.execute(
                """
                INSERT INTO sell_allocations(
                    sell_trade_id,
                    buy_lot_id,
                    volume,
                    cost
                )
                VALUES(?,?,?,?)
                """,
                (
                    sell_trade_id,
                    lot_id,
                    volume,
                    cost
                )
            )

        conn.commit()

        conn.close()

    return total_cost


def save_sell_trade(
    result,
    ratio
):

    coin = result[
        "market"
    ].replace(
        "KRW-",
        ""
    )

    created = (
        result["created_at"]
        or
        now_string()
    )

    completed = now_string()

    with db_lock:

        conn = get_db()

        c = conn.cursor()

        c.execute(
            """
            INSERT INTO trades(
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
            VALUES(
                ?,?,?,?,?,?,?,?,?,?,?,?
            )
            """,
            (
                result["uuid"],
                coin,
                "sell",
                result["executed_funds"],
                result["executed_funds"],
                result["executed_volume"],
                result["avg_price"],
                result["paid_fee"],
                ratio,
                created,
                completed,
                result["state"]
            )
        )

        trade_id = c.lastrowid

        conn.commit()

        conn.close()

    cost = calculate_fifo_cost(
        coin,
        result["executed_volume"],
        trade_id
    )

    net_sell = (
        result["executed_funds"]
        -
        result["paid_fee"]
    )

    profit = (
        net_sell -
        cost
    )

    ret = (

        profit /
        cost *
        100

        if cost > 0

        else 0
    )

    with db_lock:

        conn = get_db()

        conn.execute(
            """
            UPDATE trades
            SET
                realized_cost=?,
                realized_profit=?,
                realized_return=?
            WHERE id=?
            """,
            (
                cost,
                profit,
                ret,
                trade_id
            )
        )

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
            net_sell,

        "cost":
            cost,

        "profit":
            profit,

        "return":
            ret,

        "avg_price":
            result["avg_price"]
    }


# =========================================================
# 자산 갱신
# =========================================================

def refresh_before_order(
    api_key,
    secret_key,
    action
):

    return (
        fetch_upbit_assets(
            api_key,
            secret_key
        ) >= 0
    )


def refresh_after_order(
    api_key,
    secret_key,
    action
):

    return (
        fetch_upbit_assets(
            api_key,
            secret_key
        ) >= 0
    )


# =========================================================
# ★ 수동 매도 공통 함수
# =========================================================

def execute_manual_market_sell(
    position,
    reason="MANUAL SELL"
):

    global latest_order_info

    coin = clean_coin_name(
        position["currency"]
    )

    balance = safe_float(
        position.get("balance")
    )

    if balance <= 0:

        return {
            "success": False,
            "coin": coin,
            "message": "보유수량 없음"
        }

    # -----------------------------------------------------
    # 실제 잔고 재확인
    # -----------------------------------------------------

    real_balance = get_coin_balance(
        coin,
        UPBIT_ACCESS_KEY,
        UPBIT_SECRET_KEY
    )

    if real_balance <= 0:

        delete_trailing_state(
            coin
        )

        delete_coin_target(
            coin
        )

        return {
            "success": False,
            "coin": coin,
            "message": "실제 보유수량 없음"
        }

    volume = truncate_volume(
        real_balance,
        8
    )

    if volume <= 0:

        return {
            "success": False,
            "coin": coin,
            "message": "매도수량 0"
        }

    # -----------------------------------------------------
    # 시장가 전량 매도
    # -----------------------------------------------------

    logger.warning(
        f"🚨 {reason} START | "
        f"{coin} | "
        f"수량={volume:.8f}"
    )

    order = place_ask_order(
        coin,
        volume,
        UPBIT_ACCESS_KEY,
        UPBIT_SECRET_KEY
    )

    completed = wait_for_order_complete(
        order["uuid"],
        UPBIT_ACCESS_KEY,
        UPBIT_SECRET_KEY
    )

    result = parse_order_result(
        completed
    )

    if (
        result["state"] != "done"
        or
        result["executed_volume"] <= 0
    ):

        logger.error(
            f"🚨 {reason} FAILED | "
            f"{coin} | "
            f"{completed}"
        )

        return {
            "success": False,
            "coin": coin,
            "message": "주문 체결 실패",
            "order": completed
        }

    # -----------------------------------------------------
    # DB 기록
    # -----------------------------------------------------

    try:

        sell_result = save_sell_trade(
            result,
            1.0
        )

        realized_profit = safe_float(
            sell_result["profit"]
        )

    except Exception as e:

        logger.exception(
            f"MANUAL SELL DB ERROR | "
            f"{coin} | {e}"
        )

        realized_profit = 0.0

    delete_trailing_state(
        coin
    )

    delete_coin_target(
        coin
    )

    logger.warning(
        f"🚨 {reason} COMPLETE | "
        f"{coin} | "
        f"실현={realized_profit:+,.0f}원"
    )

    return {

        "success":
            True,

        "coin":
            coin,

        "executed_volume":
            result["executed_volume"],

        "executed_funds":
            result["executed_funds"],

        "fee":
            result["paid_fee"],

        "profit":
            realized_profit,

        "uuid":
            result["uuid"]
    }


# =========================================================
# ★ 개별 코인 수동 전량매도
# =========================================================

def execute_manual_sell_coin(
    coin
):

    coin = clean_coin_name(
        coin
    )

    if not coin:

        return {
            "success": False,
            "message": "코인이 없습니다."
        }

    # 자동매도 중인 코인과 충돌 방지
    with auto_exit_lock:

        if coin in auto_exit_in_progress:

            return {
                "success": False,
                "coin": coin,
                "message":
                    "현재 자동매도가 진행 중입니다."
            }

        auto_exit_in_progress.add(
            coin
        )

    try:

        with order_execution_lock:

            positions = (
                get_real_upbit_positions()
            )

            position = next(
                (
                    p for p in positions
                    if clean_coin_name(
                        p["currency"]
                    ) == coin
                ),
                None
            )

            if position is None:

                delete_trailing_state(
                    coin
                )

                delete_coin_target(
                    coin
                )

                return {
                    "success": False,
                    "coin": coin,
                    "message":
                        "현재 보유 중인 코인이 없습니다."
                }

            result = execute_manual_market_sell(
                position,
                "MANUAL COIN SELL"
            )

            if result["success"]:

                latest_order_info = (
                    f"🔴 수동 전량매도 {coin} | "
                    f"{result['executed_funds']:,.0f}원 | "
                    f"{result['profit']:+,.0f}원"
                )

            refresh_after_order(
                UPBIT_ACCESS_KEY,
                UPBIT_SECRET_KEY,
                "MANUAL COIN SELL"
            )

            return result

    finally:

        with auto_exit_lock:

            auto_exit_in_progress.discard(
                coin
            )


# =========================================================
# ★ 모든 보유코인 시장가 전량매도
# =========================================================

def execute_manual_sell_all():

    global latest_order_info
    global manual_sell_all_in_progress

    with manual_sell_all_lock:

        if manual_sell_all_in_progress:

            return {
                "success": False,
                "message":
                    "이미 전체매도가 진행 중입니다."
            }

        manual_sell_all_in_progress = True

    try:

        if (
            not UPBIT_ACCESS_KEY
            or
            not UPBIT_SECRET_KEY
        ):

            return {
                "success": False,
                "message":
                    "UPBIT API KEY가 설정되지 않았습니다."
            }

        # -------------------------------------------------
        # 전체매도 중에는 자동매도 실행을 새로 잡지 못하게 함
        # -------------------------------------------------

        with order_execution_lock:

            positions = (
                get_real_upbit_positions()
            )

            if not positions:

                latest_order_info = (
                    "🟡 전체매도 요청 | "
                    "보유 코인 없음"
                )

                return {
                    "success": True,
                    "message":
                        "현재 보유 코인이 없습니다.",
                    "sold": [],
                    "failed": []
                }

            sold = []

            failed = []

            logger.warning(
                f"🚨🚨🚨 MANUAL SELL ALL START | "
                f"{len(positions)}개"
            )

            # -------------------------------------------------
            # 현재 보유 중인 모든 코인을 순서대로 시장가 매도
            # -------------------------------------------------

            for position in positions:

                coin = clean_coin_name(
                    position["currency"]
                )

                # 이미 다른 자동매도가 잡혀 있으면 건너뜀
                with auto_exit_lock:

                    if coin in auto_exit_in_progress:

                        failed.append({
                            "coin": coin,
                            "message":
                                "자동매도 진행 중"
                        })

                        continue

                    auto_exit_in_progress.add(
                        coin
                    )

                try:

                    result = (
                        execute_manual_market_sell(
                            position,
                            "MANUAL SELL ALL"
                        )
                    )

                    if result["success"]:

                        sold.append(
                            result
                        )

                    else:

                        failed.append({
                            "coin": coin,
                            "message":
                                result.get(
                                    "message",
                                    "매도 실패"
                                )
                        })

                except Exception as e:

                    logger.exception(
                        f"MANUAL SELL ALL ERROR | "
                        f"{coin} | {e}"
                    )

                    failed.append({
                        "coin": coin,
                        "message":
                            str(e)
                    })

                finally:

                    with auto_exit_lock:

                        auto_exit_in_progress.discard(
                            coin
                        )

            # -------------------------------------------------
            # 자산 새로고침
            # -------------------------------------------------

            refresh_after_order(
                UPBIT_ACCESS_KEY,
                UPBIT_SECRET_KEY,
                "MANUAL SELL ALL"
            )

            latest_order_info = (
                f"🚨 전체매도 완료 | "
                f"성공 {len(sold)}개 | "
                f"실패 {len(failed)}개"
            )

            logger.warning(
                f"🚨🚨🚨 MANUAL SELL ALL COMPLETE | "
                f"성공={len(sold)} | "
                f"실패={len(failed)}"
            )

            return {

                "success":
                    len(failed) == 0,

                "message":
                    (
                        "전체매도 완료"
                        if not failed
                        else
                        "전체매도 중 일부 실패"
                    ),

                "sold":
                    sold,

                "failed":
                    failed
            }

    finally:

        with manual_sell_all_lock:

            manual_sell_all_in_progress = False


# =========================================================
# ★★★ 최우선 고정금액 손절 ★★★
# =========================================================

def execute_real_upbit_fixed_stop_loss(
    position,
    detected_profit
):

    global latest_order_info

    coin = clean_coin_name(
        position["currency"]
    )

    with auto_exit_lock:

        if coin in auto_exit_in_progress:
            return False

        auto_exit_in_progress.add(
            coin
        )

    try:

        with order_execution_lock:

            stop, _ = get_auto_exit_settings()

            if stop <= 0:
                return False

            logger.warning(
                f"🚨 STOP LOSS HIT | "
                f"{coin} | "
                f"감지수익={detected_profit:+,.0f}원 | "
                f"손절기준=-{stop:,.0f}원"
            )

            r = requests.get(
                SERVER_URL + "/v1/accounts",
                headers=create_auth_headers(
                    UPBIT_ACCESS_KEY,
                    UPBIT_SECRET_KEY
                ),
                timeout=REQUEST_TIMEOUT
            )

            if r.status_code != 200:

                logger.error(
                    f"STOP LOSS ACCOUNT ERROR | "
                    f"{coin} | "
                    f"{r.status_code}"
                )

                return False

            balance = 0.0

            avg = 0.0

            for a in r.json():

                if clean_coin_name(
                    a.get("currency")
                ) == coin:

                    balance = safe_float(
                        a.get("balance")
                    )

                    avg = safe_float(
                        a.get("avg_buy_price")
                    )

                    break

            if balance <= 0:

                delete_trailing_state(
                    coin
                )

                delete_coin_target(
                    coin
                )

                return False

            current = get_ticker_price(
                f"KRW-{coin}"
            )

            current_profit = (
                balance * current -
                balance * avg
                if current > 0
                else detected_profit
            )

            evaluation = (
                balance * current
                if current > 0
                else 0
            )

            if (
                current <= 0
                or
                evaluation < MIN_ORDER_KRW
            ):

                logger.error(
                    f"STOP LOSS ORDER BLOCKED | "
                    f"{coin} | "
                    f"현재가={current} | "
                    f"평가={evaluation:,.0f}"
                )

                return False

            volume = truncate_volume(
                balance,
                8
            )

            if volume <= 0:
                return False

            order = place_ask_order(
                coin,
                volume,
                UPBIT_ACCESS_KEY,
                UPBIT_SECRET_KEY
            )

            completed = wait_for_order_complete(
                order["uuid"],
                UPBIT_ACCESS_KEY,
                UPBIT_SECRET_KEY
            )

            result = parse_order_result(
                completed
            )

            if (
                result["state"] != "done"
                or
                result["executed_volume"] <= 0
            ):

                logger.error(
                    f"STOP LOSS ORDER FAILED | "
                    f"{coin} | "
                    f"{completed}"
                )

                return False

            try:

                sell_result = save_sell_trade(
                    result,
                    1.0
                )

                realized_profit = sell_result[
                    "profit"
                ]

            except Exception as e:

                logger.exception(
                    f"STOP LOSS DB ERROR | "
                    f"{coin} | {e}"
                )

                realized_profit = current_profit

            latest_order_info = (

                f"🛑 AUTO 손절 {coin} | "
                f"기준 -{stop:,.0f}원 | "
                f"감지 {detected_profit:+,.0f}원 | "
                f"실제 {realized_profit:+,.0f}원"
            )

            logger.warning(
                f"🛑 STOP LOSS EXECUTED | "
                f"{coin} | "
                f"기준=-{stop:,.0f}원 | "
                f"감지={detected_profit:+,.0f}원 | "
                f"실현={realized_profit:+,.0f}원"
            )

            delete_trailing_state(
                coin
            )

            delete_coin_target(
                coin
            )

            refresh_after_order(
                UPBIT_ACCESS_KEY,
                UPBIT_SECRET_KEY,
                "STOP LOSS"
            )

            return True

    except Exception as e:

        logger.exception(
            f"STOP LOSS EXIT ERROR | "
            f"{coin} | {e}"
        )

        return False

    finally:

        with auto_exit_lock:

            auto_exit_in_progress.discard(
                coin
            )


# =========================================================
# ★ 기존 고정금액 익절
# =========================================================

def execute_real_upbit_fixed_take_profit(
    position
):

    global latest_order_info

    coin = clean_coin_name(
        position["currency"]
    )

    with auto_exit_lock:

        if coin in auto_exit_in_progress:
            return False

        auto_exit_in_progress.add(
            coin
        )

    try:

        with order_execution_lock:

            stop, take = get_auto_exit_settings()

            if take <= 0:
                return False

            r = requests.get(
                SERVER_URL + "/v1/accounts",
                headers=create_auth_headers(
                    UPBIT_ACCESS_KEY,
                    UPBIT_SECRET_KEY
                ),
                timeout=REQUEST_TIMEOUT
            )

            if r.status_code != 200:
                return False

            balance = 0.0

            avg = 0.0

            for a in r.json():

                if clean_coin_name(
                    a.get("currency")
                ) == coin:

                    balance = safe_float(
                        a.get("balance")
                    )

                    avg = safe_float(
                        a.get("avg_buy_price")
                    )

                    break

            if (
                balance <= 0
                or
                avg <= 0
            ):
                return False

            current = get_ticker_price(
                f"KRW-{coin}"
            )

            if current <= 0:
                return False

            buy_amount = (
                balance *
                avg
            )

            current_value = (
                balance *
                current
            )

            profit = (
                current_value -
                buy_amount
            )

            if profit < take:
                return False

            if current_value < MIN_ORDER_KRW:
                return False

            volume = truncate_volume(
                balance,
                8
            )

            if volume <= 0:
                return False

            order = place_ask_order(
                coin,
                volume,
                UPBIT_ACCESS_KEY,
                UPBIT_SECRET_KEY
            )

            completed = wait_for_order_complete(
                order["uuid"],
                UPBIT_ACCESS_KEY,
                UPBIT_SECRET_KEY
            )

            result = parse_order_result(
                completed
            )

            if (
                result["state"] != "done"
                or
                result["executed_volume"] <= 0
            ):
                return False

            try:

                sell_result = save_sell_trade(
                    result,
                    1.0
                )

                realized_profit = sell_result[
                    "profit"
                ]

            except Exception:

                realized_profit = profit

            latest_order_info = (

                f"🎯 AUTO 익절 {coin} | "
                f"+{take:,.0f}원 | "
                f"실제 {realized_profit:+,.0f}원"
            )

            delete_trailing_state(
                coin
            )

            delete_coin_target(
                coin
            )

            refresh_after_order(
                UPBIT_ACCESS_KEY,
                UPBIT_SECRET_KEY,
                "AUTO TAKE"
            )

            return True

    except Exception as e:

        logger.exception(
            f"FIXED TAKE EXIT ERROR | "
            f"{coin} | {e}"
        )

        return False

    finally:

        with auto_exit_lock:

            auto_exit_in_progress.discard(
                coin
            )


# =========================================================
# ★★★ 자동매도 통합 모니터 ★★★
# =========================================================

def real_upbit_auto_exit_monitor():

    logger.info(
        "REAL UPBIT AUTO EXIT MONITOR START"
    )

    while True:

        try:

            if (
                not UPBIT_ACCESS_KEY
                or
                not UPBIT_SECRET_KEY
            ):

                time.sleep(
                    AUTO_CHECK_INTERVAL
                )

                continue

            # 수동 전체매도 중에는 자동매도 신규 실행 금지
            with manual_sell_all_lock:

                if manual_sell_all_in_progress:

                    time.sleep(
                        AUTO_CHECK_INTERVAL
                    )

                    continue

            stop, take = get_auto_exit_settings()

            if (
                stop <= 0
                and
                take <= 0
            ):

                time.sleep(
                    AUTO_CHECK_INTERVAL
                )

                continue

            positions = (
                get_real_upbit_positions()
            )

            for position in positions:

                coin = clean_coin_name(
                    position["currency"]
                )

                profit = safe_float(
                    position["profit_amount"]
                )

                if (
                    stop > 0
                    and
                    profit <= -stop
                ):

                    logger.warning(
                        f"🚨 STOP PRIORITY | "
                        f"{coin} | "
                        f"현재 {profit:+,.0f}원 | "
                        f"기준 -{stop:,.0f}원"
                    )

                    execute_real_upbit_fixed_stop_loss(
                        position,
                        profit
                    )

                    continue

                if (
                    take > 0
                    and
                    profit >= take
                ):

                    execute_real_upbit_fixed_take_profit(
                        position
                    )

        except Exception as e:

            logger.exception(
                f"FIXED AUTO MONITOR ERROR | {e}"
            )

        time.sleep(
            AUTO_CHECK_INTERVAL
        )


# =========================================================
# 리스크 트레일링 자동매도
# =========================================================

def execute_real_upbit_trailing_exit(
    position
):

    global latest_order_info

    coin = clean_coin_name(
        position["currency"]
    )

    with auto_exit_lock:

        if coin in auto_exit_in_progress:
            return False

        auto_exit_in_progress.add(
            coin
        )

    try:

        with order_execution_lock:

            headers = create_auth_headers(
                UPBIT_ACCESS_KEY,
                UPBIT_SECRET_KEY
            )

            r = requests.get(
                SERVER_URL + "/v1/accounts",
                headers=headers,
                timeout=REQUEST_TIMEOUT
            )

            if r.status_code != 200:
                return False

            balance = 0.0
            avg = 0.0

            for a in r.json():

                if clean_coin_name(
                    a.get("currency")
                ) == coin:

                    balance = safe_float(
                        a.get("balance")
                    )

                    avg = safe_float(
                        a.get(
                            "avg_buy_price"
                        )
                    )

                    break

            if balance <= 0:

                delete_trailing_state(
                    coin
                )

                return False

            if avg <= 0:
                return False

            current = get_ticker_price(
                f"KRW-{coin}"
            )

            if current <= 0:
                return False

            buy_amount = (
                balance *
                avg
            )

            current_value = (
                balance *
                current
            )

            profit = (
                current_value -
                buy_amount
            )

            if is_fixed_stop_loss_hit(
                profit
            ):

                logger.warning(
                    f"TRAILING OVERRIDE BY STOP | "
                    f"{coin} | "
                    f"{profit:+,.0f}원"
                )

                return False

            trailing = update_trailing_stage(
                coin,
                profit
            )

            if not trailing["trigger"]:
                return False

            evaluation = (
                balance *
                current
            )

            if evaluation < MIN_ORDER_KRW:
                return False

            volume = truncate_volume(
                balance,
                8
            )

            if volume <= 0:
                return False

            order = place_ask_order(
                coin,
                volume,
                UPBIT_ACCESS_KEY,
                UPBIT_SECRET_KEY
            )

            completed = wait_for_order_complete(
                order["uuid"],
                UPBIT_ACCESS_KEY,
                UPBIT_SECRET_KEY
            )

            result = parse_order_result(
                completed
            )

            if (
                result["state"] != "done"
                or
                result["executed_volume"] <= 0
            ):

                return False

            try:

                sell_result = save_sell_trade(
                    result,
                    1.0
                )

                realized_profit = sell_result[
                    "profit"
                ]

            except Exception:

                realized_profit = profit

            latest_order_info = (

                f"🔥 TRAILING {coin} | "
                f"최고 1:{trailing['highest_stage']} | "
                f"현재 1:{trailing['current_stage']} | "
                f"실제 {realized_profit:+,.0f}원"
            )

            delete_trailing_state(
                coin
            )

            refresh_after_order(
                UPBIT_ACCESS_KEY,
                UPBIT_SECRET_KEY,
                "RISK TRAILING"
            )

            return True

    except Exception as e:

        logger.exception(
            f"TRAILING EXIT ERROR | "
            f"{coin} | {e}"
        )

        return False

    finally:

        with auto_exit_lock:

            auto_exit_in_progress.discard(
                coin
            )


def real_upbit_trailing_monitor():

    logger.info(
        "REAL UPBIT RISK TRAILING MONITOR START"
    )

    while True:

        try:

            if (
                not UPBIT_ACCESS_KEY
                or
                not UPBIT_SECRET_KEY
            ):

                time.sleep(
                    AUTO_CHECK_INTERVAL
                )

                continue

            # 수동 전체매도 중에는 트레일링 실행 금지
            with manual_sell_all_lock:

                if manual_sell_all_in_progress:

                    time.sleep(
                        AUTO_CHECK_INTERVAL
                    )

                    continue

            for position in get_real_upbit_positions():

                coin = position[
                    "currency"
                ]

                profit = safe_float(
                    position[
                        "profit_amount"
                    ]
                )

                if is_fixed_stop_loss_hit(
                    profit
                ):

                    logger.warning(
                        f"TRAILING MONITOR STOP PRIORITY | "
                        f"{coin} | "
                        f"{profit:+,.0f}원"
                    )

                    execute_real_upbit_fixed_stop_loss(
                        position,
                        profit
                    )

                    continue

                trailing = update_trailing_stage(
                    coin,
                    profit
                )

                logger.info(

                    f"TRAILING STATUS | "
                    f"{coin} | "
                    f"현재=1:{trailing['current_stage']} | "
                    f"최고=1:{trailing['highest_stage']} | "
                    f"수익={profit:+,.0f}원 | "
                    f"기준={trailing['sell_trigger_profit']:+,.0f}원"
                )

                if trailing["trigger"]:

                    execute_real_upbit_trailing_exit(
                        position
                    )

        except Exception as e:

            logger.exception(
                f"TRAILING MONITOR ERROR | {e}"
            )

        time.sleep(
            AUTO_CHECK_INTERVAL
        )


# =========================================================
# ★ 코인별 목표 자동매도
# =========================================================

def execute_real_upbit_coin_target_exit(
    position
):

    global latest_order_info

    coin = clean_coin_name(
        position["currency"]
    )

    with auto_exit_lock:

        if coin in auto_exit_in_progress:
            return False

        auto_exit_in_progress.add(
            coin
        )

    try:

        with order_execution_lock:

            target = get_coin_target(
                coin
            )

            if not target["enabled"]:
                return False

            target_stage = int(
                target["target_stage"]
            )

            if target_stage <= 0:
                return False

            risk = get_risk_unit()

            if risk <= 0:
                return False

            target_profit = (
                risk *
                target_stage
            )

            r = requests.get(
                SERVER_URL + "/v1/accounts",
                headers=create_auth_headers(
                    UPBIT_ACCESS_KEY,
                    UPBIT_SECRET_KEY
                ),
                timeout=REQUEST_TIMEOUT
            )

            if r.status_code != 200:
                return False

            balance = 0.0

            avg = 0.0

            for a in r.json():

                if clean_coin_name(
                    a.get("currency")
                ) == coin:

                    balance = safe_float(
                        a.get("balance")
                    )

                    avg = safe_float(
                        a.get("avg_buy_price")
                    )

                    break

            if (
                balance <= 0
                or
                avg <= 0
            ):
                return False

            current = get_ticker_price(
                f"KRW-{coin}"
            )

            if current <= 0:
                return False

            buy_amount = (
                balance *
                avg
            )

            current_value = (
                balance *
                current
            )

            profit = (
                current_value -
                buy_amount
            )

            if is_fixed_stop_loss_hit(
                profit
            ):

                logger.warning(
                    f"COIN TARGET OVERRIDE BY STOP | "
                    f"{coin} | "
                    f"{profit:+,.0f}원"
                )

                return False

            if profit < target_profit:
                return False

            if current_value < MIN_ORDER_KRW:
                return False

            volume = truncate_volume(
                balance,
                8
            )

            if volume <= 0:
                return False

            logger.info(

                f"COIN TARGET HIT | "
                f"{coin} | "
                f"1:{target_stage} | "
                f"목표={target_profit:+,.0f}원 | "
                f"현재={profit:+,.0f}원"
            )

            order = place_ask_order(
                coin,
                volume,
                UPBIT_ACCESS_KEY,
                UPBIT_SECRET_KEY
            )

            completed = wait_for_order_complete(
                order["uuid"],
                UPBIT_ACCESS_KEY,
                UPBIT_SECRET_KEY
            )

            result = parse_order_result(
                completed
            )

            if (
                result["state"] != "done"
                or
                result["executed_volume"] <= 0
            ):

                return False

            try:

                sell_result = save_sell_trade(
                    result,
                    1.0
                )

                realized_profit = sell_result[
                    "profit"
                ]

            except Exception:

                realized_profit = profit

            latest_order_info = (

                f"🎯 목표 익절 {coin} | "
                f"1:{target_stage} | "
                f"목표 +{target_profit:,.0f}원 | "
                f"실제 {realized_profit:+,.0f}원"
            )

            delete_trailing_state(
                coin
            )

            delete_coin_target(
                coin
            )

            refresh_after_order(
                UPBIT_ACCESS_KEY,
                UPBIT_SECRET_KEY,
                "COIN TARGET"
            )

            return True

    except Exception as e:

        logger.exception(
            f"COIN TARGET EXIT ERROR | "
            f"{coin} | {e}"
        )

        return False

    finally:

        with auto_exit_lock:

            auto_exit_in_progress.discard(
                coin
            )


def real_upbit_coin_target_monitor():

    logger.info(
        "REAL UPBIT COIN TARGET MONITOR START"
    )

    while True:

        try:

            if (
                not UPBIT_ACCESS_KEY
                or
                not UPBIT_SECRET_KEY
            ):

                time.sleep(
                    AUTO_CHECK_INTERVAL
                )

                continue

            # 수동 전체매도 중에는 목표매도 실행 금지
            with manual_sell_all_lock:

                if manual_sell_all_in_progress:

                    time.sleep(
                        AUTO_CHECK_INTERVAL
                    )

                    continue

            positions = (
                get_real_upbit_positions()
            )

            for position in positions:

                coin = clean_coin_name(
                    position["currency"]
                )

                profit = safe_float(
                    position[
                        "profit_amount"
                    ]
                )

                if is_fixed_stop_loss_hit(
                    profit
                ):

                    logger.warning(
                        f"COIN TARGET MONITOR "
                        f"STOP PRIORITY | "
                        f"{coin} | "
                        f"{profit:+,.0f}원"
                    )

                    execute_real_upbit_fixed_stop_loss(
                        position,
                        profit
                    )

                    continue

                target = get_coin_target(
                    coin
                )

                if not target["enabled"]:
                    continue

                if (
                    target["target_stage"]
                    <= 0
                ):
                    continue

                risk = get_risk_unit()

                if risk <= 0:
                    continue

                target_profit = (
                    risk *
                    target["target_stage"]
                )

                logger.info(

                    f"COIN TARGET STATUS | "
                    f"{coin} | "
                    f"목표 1:{target['target_stage']} | "
                    f"현재={profit:+,.0f}원 | "
                    f"목표={target_profit:+,.0f}원"
                )

                if profit >= target_profit:

                    execute_real_upbit_coin_target_exit(
                        position
                    )

        except Exception as e:

            logger.exception(
                f"COIN TARGET MONITOR ERROR | {e}"
            )

        time.sleep(
            AUTO_CHECK_INTERVAL
        )


# =========================================================
# 업비트 자산 갱신
# =========================================================

def upbit_asset_refresh_monitor():

    logger.info(
        "UPBIT ASSET REFRESH MONITOR START"
    )

    while True:

        try:

            if (
                UPBIT_ACCESS_KEY
                and
                UPBIT_SECRET_KEY
            ):

                fetch_upbit_assets(
                    UPBIT_ACCESS_KEY,
                    UPBIT_SECRET_KEY
                )

        except Exception as e:

            logger.exception(
                f"ASSET REFRESH ERROR | {e}"
            )

        time.sleep(
            UPBIT_ASSET_REFRESH_INTERVAL
        )


# =========================================================
# TradingView Webhook
# =========================================================

@app.post(
    "/tradingview_webhook"
)
async def tradingview_webhook(
    request: Request
):

    global latest_order_info

    try:

        payload = json.loads(
            (
                await request.body()
            ).decode(
                "utf-8"
            )
        )

    except Exception:

        raise HTTPException(
            status_code=400,
            detail="올바른 JSON 형식이 아닙니다."
        )

    if not isinstance(
        payload,
        dict
    ):

        raise HTTPException(
            status_code=400,
            detail="JSON 객체가 필요합니다."
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

    api_key = UPBIT_ACCESS_KEY

    secret_key = UPBIT_SECRET_KEY

    if action not in (
        "buy",
        "sell"
    ):

        raise HTTPException(
            status_code=400,
            detail="Action은 Buy 또는 Sell입니다."
        )

    if (
        not api_key
        or
        not secret_key
    ):

        raise HTTPException(
            status_code=500,
            detail=(
                "UPBIT_ACCESS_KEY / "
                "UPBIT_SECRET_KEY "
                "환경변수가 설정되지 않았습니다."
            )
        )

    # =====================================================
    # BUY
    # =====================================================

    if action == "buy":

        if not coin:

            raise HTTPException(
                status_code=400,
                detail="BUY 코인이 없습니다."
            )

        refresh_before_order(
            api_key,
            secret_key,
            "BUY"
        )

        if stop_loss <= 0:

            latest_order_info = (

                f"🟡 BUY 조회 테스트 | "
                f"{coin} | "
                f"주문하지 않음"
            )

            return {

                "status":
                    "test",

                "Action":
                    "Buy",

                "Coin":
                    coin,

                "StopLoss":
                    stop_loss,

                "Total KRW":
                    latest_upbit_total_krw,

                "Available KRW":
                    latest_upbit_available_krw,

                "Order Executed":
                    False
            }

        month, max_loss_rate = (
            get_risk_settings()
        )

        loss_limit = (
            month *
            max_loss_rate
        )

        target_buy = truncate_krw(
            loss_limit /
            (stop_loss / 100)
        )

        available = (
            latest_upbit_available_krw
        )

        if available <= 0:

            raise HTTPException(
                status_code=400,
                detail="매수 가능한 KRW가 없습니다."
            )

        chance = get_order_chance(
            f"KRW-{coin}",
            api_key,
            secret_key
        )

        fee = safe_float(
            chance.get("bid_fee")
        )

        minimum = safe_float(
            chance.get(
                "min_total"
            ),
            MIN_ORDER_KRW
        )

        safe_available = (
            calculate_fee_safe_buy_amount(
                available,
                fee
            )
        )

        amount = truncate_krw(
            min(
                target_buy,
                safe_available
            )
        )

        if amount < minimum:

            raise HTTPException(
                status_code=400,
                detail=(
                    f"주문가능금액 "
                    f"{amount:,.0f}원 < "
                    f"최소주문 "
                    f"{minimum:,.0f}원"
                )
            )

        try:

            with order_execution_lock:

                order = place_bid_order(
                    coin,
                    amount,
                    api_key,
                    secret_key
                )

                completed = wait_for_order_complete(
                    order["uuid"],
                    api_key,
                    secret_key
                )

                result = parse_order_result(
                    completed
                )

                if result["state"] != "done":

                    raise HTTPException(
                        status_code=400,
                        detail=completed
                    )

                save_buy_trade(
                    result,
                    amount,
                    stop_loss
                )

                latest_order_info = (

                    f"🟢 BUY {coin} | "
                    f"손절 {stop_loss:.2f}% | "
                    f"{amount:,.0f}원"
                )

            return {

                "status":
                    "success",

                "Action":
                    "Buy",

                "Coin":
                    coin,

                "Stop Loss Percent":
                    stop_loss,

                "Target Buy KRW":
                    target_buy,

                "Actual Order KRW":
                    amount,

                "Executed Volume":
                    result[
                        "executed_volume"
                    ],

                "Average Price":
                    result[
                        "avg_price"
                    ],

                "Fee":
                    result[
                        "paid_fee"
                    ],

                "Order UUID":
                    result[
                        "uuid"
                    ],

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

        if not coin:

            raise HTTPException(
                status_code=400,
                detail="SELL 코인이 없습니다."
            )

        refresh_before_order(
            api_key,
            secret_key,
            "SELL"
        )

        if volume <= 0:

            latest_order_info = (

                f"🟡 SELL 조회 테스트 | "
                f"{coin} | "
                f"주문하지 않음"
            )

            return {

                "status":
                    "test",

                "Action":
                    "Sell",

                "Coin":
                    coin,

                "Volume":
                    volume,

                "Order Executed":
                    False
            }

        if volume > 1:

            raise HTTPException(
                status_code=400,
                detail="SELL volume은 0~1입니다."
            )

        balance = get_coin_balance(
            coin,
            api_key,
            secret_key
        )

        if balance <= 0:

            raise HTTPException(
                status_code=400,
                detail=(
                    f"{coin} "
                    f"보유수량이 없습니다."
                )
            )

        sell_volume = (

            balance

            if volume >= 1

            else balance * volume
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

        try:

            with order_execution_lock:

                order = place_ask_order(
                    coin,
                    sell_volume,
                    api_key,
                    secret_key
                )

                completed = wait_for_order_complete(
                    order["uuid"],
                    api_key,
                    secret_key
                )

                result = parse_order_result(
                    completed
                )

                if result["state"] != "done":

                    raise HTTPException(
                        status_code=400,
                        detail=completed
                    )

                sell_result = save_sell_trade(
                    result,
                    volume
                )

                latest_order_info = (

                    f"🔴 SELL {coin} | "
                    f"{volume * 100:.0f}% | "
                    f"{sell_result['profit']:+,.0f}원"
                )

            if volume >= 1:

                delete_trailing_state(
                    coin
                )

                delete_coin_target(
                    coin
                )

            return {

                "status":
                    "success",

                "Action":
                    "Sell",

                "Coin":
                    coin,

                "Sell Ratio":
                    volume,

                "Executed Volume":
                    sell_result[
                        "sell_volume"
                    ],

                "Gross Sell Amount":
                    sell_result[
                        "gross_sell"
                    ],

                "Fee":
                    sell_result[
                        "fee"
                    ],

                "Net Sell Amount":
                    sell_result[
                        "net_sell"
                    ],

                "Realized Profit":
                    sell_result[
                        "profit"
                    ],

                "Return Percent":
                    sell_result[
                        "return"
                    ],

                "Order UUID":
                    result[
                        "uuid"
                    ],

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
# ★ 수동 개별 전량매도 API
# =========================================================

@app.post(
    "/api/manual-sell-coin"
)
async def api_manual_sell_coin(
    request: Request
):

    if (
        not UPBIT_ACCESS_KEY
        or
        not UPBIT_SECRET_KEY
    ):

        raise HTTPException(
            status_code=500,
            detail=(
                "UPBIT_ACCESS_KEY / "
                "UPBIT_SECRET_KEY "
                "환경변수가 설정되지 않았습니다."
            )
        )

    try:

        data = await request.json()

    except Exception:

        raise HTTPException(
            status_code=400,
            detail="JSON 오류"
        )

    coin = clean_coin_name(
        data.get(
            "coin",
            ""
        )
    )

    if not coin:

        raise HTTPException(
            status_code=400,
            detail="코인이 없습니다."
        )

    try:

        result = execute_manual_sell_coin(
            coin
        )

    except HTTPException:
        raise

    except Exception as e:

        logger.exception(
            f"MANUAL COIN SELL API ERROR | "
            f"{coin} | {e}"
        )

        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

    if not result.get("success"):

        raise HTTPException(
            status_code=400,
            detail=result.get(
                "message",
                "개별 매도 실패"
            )
        )

    return {

        "status":
            "success",

        **result
    }


# =========================================================
# ★ 수동 전체매도 API
# =========================================================

@app.post(
    "/api/manual-sell-all"
)
async def api_manual_sell_all():

    if (
        not UPBIT_ACCESS_KEY
        or
        not UPBIT_SECRET_KEY
    ):

        raise HTTPException(
            status_code=500,
            detail=(
                "UPBIT_ACCESS_KEY / "
                "UPBIT_SECRET_KEY "
                "환경변수가 설정되지 않았습니다."
            )
        )

    try:

        result = execute_manual_sell_all()

    except Exception as e:

        logger.exception(
            f"MANUAL SELL ALL API ERROR | {e}"
        )

        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

    return {

        "status":
            "success"
            if result.get("success")
            else "partial",

        **result
    }


# =========================================================
# 자산 API
# =========================================================

@app.get(
    "/api/upbit-assets"
)
async def api_upbit_assets():

    month, rate = (
        get_risk_settings()
    )

    stop, take = (
        get_auto_exit_settings()
    )

    risk_unit = (
        month *
        rate
    )

    trailing = {

        clean_coin_name(
            a["currency"]
        ):

        get_trailing_state(
            a["currency"]
        )

        for a in latest_upbit_assets
    }

    coin_targets = {

        clean_coin_name(
            a["currency"]
        ):

        get_coin_target(
            a["currency"]
        )

        for a in latest_upbit_assets
    }

    return {

        "month_start_amount":
            month,

        "max_loss_rate":
            rate,

        "max_loss_rate_percent":
            rate * 100,

        "max_loss_amount":
            month * rate,

        "auto_stop_loss_krw":
            stop,

        "auto_take_profit_krw":
            take,

        "auto_take_profit_enabled":
            take > 0,

        "risk_unit":
            risk_unit,

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

        "trailing_states":
            trailing,

        "coin_targets":
            coin_targets,

        "latest_order":
            latest_order_info,

        "manual_sell_all_in_progress":
            manual_sell_all_in_progress
    }


# =========================================================
# 리스크 설정 API
# =========================================================

@app.post(
    "/api/risk-settings"
)
async def update_risk_settings(
    request: Request
):

    try:

        data = await request.json()

    except Exception:

        raise HTTPException(
            status_code=400,
            detail="JSON 오류"
        )

    month = safe_float(
        data.get(
            "month_start_amount"
        )
    )

    rate_percent = safe_float(
        data.get(
            "max_loss_rate"
        )
    )

    if month < MIN_ORDER_KRW:

        raise HTTPException(
            status_code=400,
            detail=(
                f"월 시작금액은 "
                f"{MIN_ORDER_KRW:,}원 이상"
            )
        )

    if (
        rate_percent <= 0
        or
        rate_percent > 100
    ):

        raise HTTPException(
            status_code=400,
            detail="손실기준은 0~100%"
        )

    rate = (
        rate_percent /
        100
    )

    save_risk_settings(
        month,
        rate
    )

    return {

        "status":
            "success",

        "month_start_amount":
            truncate_krw(month),

        "max_loss_rate":
            rate,

        "max_loss_rate_percent":
            rate_percent,

        "max_loss_amount":
            month * rate
    }


# =========================================================
# 자동 손절 / 익절 API
# =========================================================

@app.post(
    "/api/auto-exit-settings"
)
async def update_auto_exit_settings(
    request: Request
):

    try:

        data = await request.json()

    except Exception:

        raise HTTPException(
            status_code=400,
            detail="JSON 오류"
        )

    stop = safe_float(
        data.get(
            "auto_stop_loss_krw"
        )
    )

    take = safe_float(
        data.get(
            "auto_take_profit_krw"
        )
    )

    if (
        stop < 0
        or
        take < 0
    ):

        raise HTTPException(
            status_code=400,
            detail="금액은 0 이상이어야 합니다."
        )

    save_auto_exit_settings(
        stop,
        take
    )

    return {

        "status":
            "success",

        "auto_stop_loss_krw":
            truncate_krw(stop),

        "auto_take_profit_krw":
            truncate_krw(take),

        "auto_take_profit_enabled":
            take > 0
    }


# =========================================================
# 코인별 목표 API
# =========================================================

@app.post(
    "/api/coin-target"
)
async def update_coin_target(
    request: Request
):

    try:

        data = await request.json()

    except Exception:

        raise HTTPException(
            status_code=400,
            detail="JSON 오류"
        )

    coin = clean_coin_name(
        data.get(
            "coin",
            ""
        )
    )

    enabled = bool(
        data.get(
            "enabled",
            False
        )
    )

    target_stage = int(
        safe_float(
            data.get(
                "target_stage",
                0
            )
        )
    )

    if not coin:

        raise HTTPException(
            status_code=400,
            detail="코인이 없습니다."
        )

    if (
        target_stage < 0
        or
        target_stage > 10
    ):

        raise HTTPException(
            status_code=400,
            detail="목표 단계는 0~10입니다."
        )

    if (
        enabled
        and
        target_stage <= 0
    ):

        raise HTTPException(
            status_code=400,
            detail=(
                "목표 사용 시 "
                "1:1~1:10 중 하나를 선택하세요."
            )
        )

    result = save_coin_target(
        coin,
        enabled,
        target_stage
    )

    return {

        "status":
            "success",

        **result
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
 content="width=device-width,initial-scale=1"
>

<title>TRADING CONTROL CENTER</title>

<style>

*{
 box-sizing:border-box
}

body{
 margin:0;
 padding:10px;
 background:#111827;
 color:#f3f4f6;
 font-family:Arial,"Noto Sans KR",sans-serif;
 font-size:13px
}

.container{
 max-width:900px;
 margin:auto
}

h1{
 margin:0 0 3px;
 font-size:20px
}

.subtitle{
 color:#9ca3af;
 font-size:10px;
 margin-bottom:10px
}

.grid{
 display:grid;
 grid-template-columns:repeat(2,1fr);
 gap:6px
}

.card,
.section,
.today-box{
 background:#1f2937;
 border:1px solid #374151;
 border-radius:8px;
 padding:9px
}

.title,
.today-title{
 color:#9ca3af;
 font-size:10px;
 margin-bottom:4px
}

.value{
 font-size:16px;
 font-weight:bold
}

.green{
 color:#22c55e
}

.red{
 color:#ef4444
}

.today-card{
 grid-column:span 2;
 display:grid;
 grid-template-columns:repeat(2,1fr);
 gap:6px
}

.today-input{
 width:100%;
 padding:7px;
 border-radius:6px;
 border:1px solid #4b5563;
 background:#111827;
 color:#fff;
 font-size:13px;
 font-weight:bold
}

.input-row{
 display:flex;
 gap:4px
}

.input-row input{
 flex:1;
 min-width:0
}

.off-button{
 padding:6px 8px;
 border:1px solid #4b5563;
 border-radius:6px;
 background:#374151;
 color:#d1d5db;
 font-size:10px;
 font-weight:bold;
 cursor:pointer
}

.off-button.active{
 background:#ef4444;
 border-color:#ef4444;
 color:#fff
}

.off-button:disabled{
 opacity:.5;
 cursor:not-allowed
}


/* =====================================================
   전체매도 버튼
   ===================================================== */

.manual-sell-all-button{
 padding:6px 9px;
 border:1px solid #ef4444;
 border-radius:6px;
 background:#374151;
 color:#ef4444;
 font-size:10px;
 font-weight:900;
 cursor:pointer;
 white-space:nowrap
}

.manual-sell-all-button:hover{
 background:#ef4444;
 color:#fff
}

.manual-sell-all-button.active{
 background:#ef4444;
 color:#fff;
 animation:pulse 1s infinite
}

.manual-sell-all-button:disabled{
 opacity:.5;
 cursor:not-allowed;
 animation:none
}

@keyframes pulse{
 0%{
  opacity:1
 }
 50%{
  opacity:.55
 }
 100%{
  opacity:1
 }
}


/* =====================================================
   개별매도 버튼
   ===================================================== */

.asset-header-row{
 display:flex;
 justify-content:space-between;
 align-items:center;
 gap:6px;
 margin-bottom:5px
}

.manual-sell-button{
 padding:5px 8px;
 border:1px solid #ef4444;
 border-radius:6px;
 background:#374151;
 color:#ef4444;
 font-size:9px;
 font-weight:900;
 cursor:pointer;
 white-space:nowrap
}

.manual-sell-button:hover{
 background:#ef4444;
 color:#fff
}

.manual-sell-button:disabled{
 opacity:.5;
 cursor:not-allowed
}


.section{
 margin-top:7px
}

.section-header{
 display:flex;
 justify-content:space-between;
 align-items:center;
 margin-bottom:6px;
 font-weight:bold
}

.section-coin{
 color:#60a5fa;
 font-size:11px
}

.asset{
 border-bottom:1px solid #374151;
 padding:8px 0
}

.asset:last-child{
 border-bottom:0
}

.asset-name{
 font-size:15px;
 font-weight:bold
}

.asset-grid{
 display:grid;
 grid-template-columns:repeat(2,1fr);
 gap:4px;
 font-size:10px
}

.asset-row{
 color:#d1d5db
}

.asset-row span{
 color:#9ca3af
}

.asset-profit{
 font-size:14px;
 font-weight:900
}

.asset-risk{
 font-size:12px;
 font-weight:900
}

.asset-risk-targets{
 margin-top:7px;
 padding-top:7px;
 border-top:1px solid #374151
}

.asset-risk-title{
 color:#9ca3af;
 font-size:9px;
 margin-bottom:4px
}

.asset-target-grid{
 display:grid;
 grid-template-columns:repeat(5,1fr);
 gap:3px
}

.asset-target-item{
 background:#111827;
 border-radius:5px;
 padding:5px 1px;
 text-align:center;
 border:1px solid #374151;
 min-height:51px
}

.asset-target-item.reached{
 border-color:#22c55e
}

.asset-target-item.current{
 border-color:#facc15
}

.asset-target-item.selected{
 border-color:#22c55e;
 box-shadow:0 0 0 1px #22c55e inset
}

.asset-target-label{
 color:#9ca3af;
 font-size:8px
}

.asset-target-value{
 font-size:9px;
 font-weight:bold
}

.asset-target-status{
 margin-top:2px;
 font-size:7px;
 font-weight:bold
}

.target-checkbox{
 width:12px;
 height:12px;
 margin:0;
 vertical-align:middle;
 accent-color:#22c55e
}

.compact-risk{
 display:flex;
 align-items:center;
 gap:6px;
 overflow-x:auto;
 white-space:nowrap
}

.risk-item{
 background:#111827;
 border:1px solid #374151;
 border-radius:5px;
 padding:5px 7px;
 text-align:center
}

.risk-stop{
 color:#9ca3af;
 font-size:8px
}

.risk-entry{
 font-size:9px;
 font-weight:bold
}

.notes{
 margin-top:7px;
 color:#6b7280;
 font-size:9px;
 line-height:1.45
}

.empty{
 color:#9ca3af;
 text-align:center;
 padding:12px
}

@media(max-width:600px){

 body{
  padding:6px
 }

 h1{
  font-size:17px
 }

 .card,
 .section,
 .today-box{
  padding:7px
 }

 .value{
  font-size:14px
 }

 .asset-grid{
  font-size:9px
 }

 .asset-target-label{
  font-size:7px
 }

 .asset-target-value{
  font-size:8px
 }

 .asset-target-status{
  font-size:6px
 }

 .risk-item{
  padding:4px 6px
 }

 .notes{
  font-size:8px
 }

 .manual-sell-all-button{
  font-size:9px;
  padding:6px 7px
 }

 .manual-sell-button{
  font-size:8px;
  padding:5px 6px
 }

}

</style>

</head>

<body>

<div class="container">

<h1>TRADING CONTROL CENTER</h1>

<div class="subtitle">
TradingView BUY / SELL · 업비트 실시간 보유자산
</div>

<div class="grid">

<div class="card">

<div class="title">
월 시작금액
</div>

<input
 id="month-start"
 class="today-input"
 type="number"
 min="5000"
 step="1000"
>

</div>

<div class="card">

<div class="title">
현재 총자산
</div>

<div id="total" class="value">-</div>

</div>

<div class="card">

<div class="title">
월 수익금
</div>

<div id="month-profit" class="value">-</div>

</div>

<div class="card">

<div class="title">
월 수익률
</div>

<div id="month-return" class="value">-</div>

</div>

<div class="card">

<div class="title">
매수 가능금액
</div>

<div id="available" class="value">-</div>

</div>

<div class="card">

<div class="title">
손절한도
</div>

<div id="loss-limit-top" class="value red">-</div>

</div>


<!-- =====================================================
     오늘 카드
     ===================================================== -->

<div class="today-card">

<div class="today-box">

<div class="today-title">
오늘 시작금액
</div>

<input
 id="today-start"
 class="today-input"
 type="number"
 step="1000"
>

</div>


<div class="today-box">

<div class="today-title">
당일 수익금
</div>

<div id="today-profit" class="value">-</div>

</div>


<div class="today-box">

<div class="today-title">
전체 시드 손실기준 %
</div>

<input
 id="max-loss-rate"
 class="today-input"
 type="number"
 min="0.01"
 max="100"
 step="0.01"
>

</div>


<div class="today-box">

<div class="today-title">
당일 목표수익
</div>

<input
 id="today-target"
 class="today-input"
 type="number"
 step="1000"
>

</div>


<!-- =====================================================
     ★ 자동손절 + 전체매도
     ===================================================== -->

<div class="today-box">

<div class="today-title">
자동 손절 · 전체매도
</div>

<div class="input-row">

<input
 id="auto-stop-loss-krw"
 class="today-input"
 type="number"
 min="0"
 step="1000"
>

<button
 id="manual-sell-all"
 class="manual-sell-all-button"
 type="button"
>
전체매도 OFF
</button>

</div>

</div>


<div class="today-box">

<div class="today-title">
고정금액 자동 익절
</div>

<div class="input-row">

<input
 id="auto-take-profit-krw"
 class="today-input"
 type="number"
 min="0"
 step="1000"
>

<button
 id="auto-take-profit-off"
 class="off-button"
>
OFF
</button>

</div>

</div>

</div>

</div>


<!-- =====================================================
     보유자산
     ===================================================== -->

<div class="section">

<div class="section-header">

<span>보유자산</span>

<span
 id="asset-coin"
 class="section-coin"
>
보유 없음
</span>

</div>

<div id="assets">

<div class="empty">
보유자산 없음
</div>

</div>

</div>


<!-- =====================================================
     리스크
     ===================================================== -->

<div class="section">

<div class="section-header">

<span>손절폭 / 진입금액</span>

<span
 id="risk-unit-mini"
 class="section-coin"
>
1R -
</span>

</div>

<div id="risk-table" class="compact-risk"></div>

</div>


<div class="notes">

월 시작금액
<strong id="month-start-note">-</strong>

 · 손실기준
<strong id="max-loss-rate-note">-</strong>

 · 1R
<strong id="risk-unit-note">-</strong>

 · 손절한도
<strong id="max-loss-amount-note">-</strong>

 · 자동손절
<strong id="auto-stop-note">-</strong>

 · 자동익절
<strong id="auto-take-note">OFF</strong>

 · 조회
<strong id="updated">-</strong>

<br>

<span id="latest-order">
주문 없음
</span>

<br>

※ 보유자산별 1:1~1:10 표시
· 체크한 목표는 해당 코인에서 독립적으로 자동매도
· 손절금액 도달 시 손절이 최우선
· 전체매도는 현재 보유 중인 모든 코인을 시장가 전량매도
· 각 코인의 전량매도 버튼은 해당 코인만 시장가 전량매도

</div>

</div>


<script>


// =========================================================
// LocalStorage
// =========================================================

const TODAY_START_KEY =
 "upbit_today_start_amount";

const TODAY_TARGET_KEY =
 "upbit_today_target_amount";


const $ =
 id =>
 document.getElementById(id);


// =========================================================
// 표시 함수
// =========================================================

function money(v){

 return (
  Math.round(
   Number(v||0)
  ).toLocaleString("ko-KR")
  +
  "원"
 );

}


function profitMoney(v){

 const n =
  Number(v||0);

 return (
  n>=0
   ?"+"
   :""
 )
 +
 Math.round(n)
  .toLocaleString("ko-KR")
 +
 "원";

}


function profitRate(v){

 const n =
  Number(v||0);

 return (
  n>=0
   ?"+"
   :""
 )
 +
 n.toFixed(2)
 +
 "%";

}


function num(v){

 return Number(v||0)
  .toLocaleString(
   "ko-KR",
   {
    maximumFractionDigits:8
   }
  );

}


function price(v){

 return Number(v||0)
  .toLocaleString(
   "ko-KR",
   {
    maximumFractionDigits:8
   }
  )
 +
 "원";

}


// =========================================================
// Local 설정
// =========================================================

function loadLocalSettings(){

 const a =
  localStorage.getItem(
   TODAY_START_KEY
  );

 const b =
  localStorage.getItem(
   TODAY_TARGET_KEY
  );

 if(a!==null){

  $("today-start").value =
   a;

 }

 if(b!==null){

  $("today-target").value =
   b;

 }

}


$("today-start")
.addEventListener(
 "input",
 function(){

  localStorage.setItem(
   TODAY_START_KEY,
   this.value
  );

  updateTodayProfit(
   window.currentTotal||0
  );

 }
);


$("today-target")
.addEventListener(
 "input",
 function(){

  localStorage.setItem(
   TODAY_TARGET_KEY,
   this.value
  );

 }
);


// =========================================================
// 오늘 수익
// =========================================================

function updateTodayProfit(
 total
){

 const start =
  Number(
   $("today-start").value||0
  );

 if(start<=0){

  $("today-profit")
   .textContent="-";

  $("today-profit")
   .className="value";

  return;

 }

 const p =
  Number(total||0) -
  start;

 $("today-profit")
  .textContent =
  profitMoney(p);

 $("today-profit")
  .className =
  "value "
  +
  (
   p>0
    ?"green"
    :p<0
    ?"red"
    :""
  );

}


// =========================================================
// 자동 익절 OFF
// =========================================================

$("auto-take-profit-off")
.addEventListener(
 "click",
 async function(){

  $("auto-take-profit-krw")
   .value=0;

  await saveAutoExitSettings();

 }
);


// =========================================================
// 자동 손절 / 익절 저장
// =========================================================

async function saveAutoExitSettings(){

 const stop =
  Number(
   $("auto-stop-loss-krw")
    .value||0
  );

 const take =
  Number(
   $("auto-take-profit-krw")
    .value||0
  );

 if(
  stop<0
  ||
  take<0
 ){

  alert(
   "금액은 0원 이상이어야 합니다."
  );

  return loadData();

 }

 try{

  const r =
   await fetch(
    "/api/auto-exit-settings",
    {
     method:"POST",

     headers:{
      "Content-Type":
       "application/json"
     },

     body:
      JSON.stringify({

       auto_stop_loss_krw:
        stop,

       auto_take_profit_krw:
        take

      })
    }
   );

  const d =
   await r.json();

  if(!r.ok){

   throw new Error(
    d.detail||
    "저장 실패"
   );

  }

  await loadData();

 }catch(e){

  alert(e.message);

  await loadData();

 }

}


$("auto-stop-loss-krw")
 .addEventListener(
  "change",
  saveAutoExitSettings
 );


$("auto-take-profit-krw")
 .addEventListener(
  "change",
  saveAutoExitSettings
 );


// =========================================================
// 리스크 저장
// =========================================================

async function saveRiskSettings(){

 const month =
  Number(
   $("month-start")
    .value||0
  );

 const rate =
  Number(
   $("max-loss-rate")
    .value||0
  );

 if(
  month<5000
  ||
  rate<=0
  ||
  rate>100
 ){

  alert(
   "월 시작금액 5,000원 이상 / 손실기준 0~100%"
  );

  return loadData();

 }

 try{

  const r =
   await fetch(
    "/api/risk-settings",
    {
     method:"POST",

     headers:{
      "Content-Type":
       "application/json"
     },

     body:
      JSON.stringify({

       month_start_amount:
        month,

       max_loss_rate:
        rate

      })
    }
   );

  const d =
   await r.json();

  if(!r.ok){

   throw new Error(
    d.detail||
    "저장 실패"
   );

  }

  await loadData();

 }catch(e){

  alert(e.message);

  await loadData();

 }

}


$("month-start")
 .addEventListener(
  "change",
  saveRiskSettings
 );


$("max-loss-rate")
 .addEventListener(
  "change",
  saveRiskSettings
 );


// =========================================================
// 코인별 목표
// =========================================================

async function setCoinTarget(
 coin,
 stage,
 enabled
){

 try{

  const r =
   await fetch(
    "/api/coin-target",
    {
     method:"POST",

     headers:{
      "Content-Type":
       "application/json"
     },

     body:
      JSON.stringify({

       coin:
        coin,

       enabled:
        enabled,

       target_stage:
        enabled
         ?stage
         :0

      })
    }
   );

  const d =
   await r.json();

  if(!r.ok){

   throw new Error(
    d.detail||
    "목표 설정 실패"
   );

  }

  await loadData();

 }catch(e){

  alert(
   e.message
  );

  await loadData();

 }

}


// =========================================================
// ★ 개별 코인 전량매도
// =========================================================

async function manualSellCoin(
 coin
){

 const ok =
  confirm(
   `⚠️ ${coin} 전체 보유수량을 시장가로 매도합니다.\\n\\n계속하시겠습니까?`
  );

 if(!ok){
  return;
 }

 const button =
  document.querySelector(
   `[data-sell-coin="${coin}"]`
  );

 if(button){

  button.disabled =
   true;

  button.textContent =
   "매도중...";
 }

 try{

  const r =
   await fetch(
    "/api/manual-sell-coin",
    {
     method:"POST",

     headers:{
      "Content-Type":
       "application/json"
     },

     body:
      JSON.stringify({
       coin:coin
      })
    }
   );

  const d =
   await r.json();

  if(!r.ok){

   throw new Error(
    d.detail||
    "개별 전량매도 실패"
   );

  }

  alert(
   `🔴 ${coin} 전량매도 완료\\n\\n` +
   `매도금액: ${money(d.executed_funds)}\\n` +
   `실현손익: ${profitMoney(d.profit)}`
  );

  await loadData();

 }catch(e){

  alert(
   "개별 전량매도 실패\\n\\n" +
   e.message
  );

  await loadData();

 }

}


// =========================================================
// ★ 전체 보유코인 전량매도
// =========================================================

$("manual-sell-all")
.addEventListener(
 "click",
 async function(){

  const button =
   this;

  const first =
   confirm(
    "🚨 전체매도\\n\\n" +
    "현재 보유 중인 모든 코인을 시장가로 전량 매도합니다.\\n\\n" +
    "자동 손절/익절보다 수동 전체매도가 먼저 실행될 수 있습니다.\\n\\n" +
    "정말 전체매도 하시겠습니까?"
   );

  if(!first){
   return;
  }

  const second =
   confirm(
    "⚠️ 최종 확인\\n\\n" +
    "보유 중인 모든 코인을 시장가로 매도합니다.\\n" +
    "이 작업은 되돌릴 수 없습니다.\\n\\n" +
    "확인을 누르면 즉시 주문합니다."
   );

  if(!second){
   return;
  }

  button.disabled =
   true;

  button.classList.add(
   "active"
  );

  button.textContent =
   "전체매도중...";

  try{

   const r =
    await fetch(
     "/api/manual-sell-all",
     {
      method:"POST"
     }
    );

   const d =
    await r.json();

   if(!r.ok){

    throw new Error(
     d.detail||
     "전체매도 실패"
    );

   }

   let message =
    "🚨 전체매도 결과\\n\\n";

   message +=
    "성공: "
    +
    (d.sold||[]).length
    +
    "개\\n";

   message +=
    "실패: "
    +
    (d.failed||[]).length
    +
    "개\\n";

   if(
    d.sold
    &&
    d.sold.length
   ){

    message +=
     "\\n[매도 완료]\\n";

    d.sold.forEach(
     x=>{

      message +=
       `${x.coin} `
       +
       `${money(x.executed_funds)} `
       +
       `${profitMoney(x.profit)}\\n`;

     }
    );

   }

   if(
    d.failed
    &&
    d.failed.length
   ){

    message +=
     "\\n[매도 실패]\\n";

    d.failed.forEach(
     x=>{

      message +=
       `${x.coin}: `
       +
       `${x.message}\\n`;

     }
    );

   }

   alert(
    message
   );

   await loadData();

  }catch(e){

   alert(
    "전체매도 실패\\n\\n"
    +
    e.message
   );

   await loadData();

  }finally{

   button.disabled =
    false;

   button.classList.remove(
    "active"
   );

   button.textContent =
    "전체매도 OFF";

  }

 }
);


// =========================================================
// 코인별 목표 렌더링
// =========================================================

function renderCoinRiskTargets(
 coin,
 risk,
 current,
 highest,
 targetInfo
){

 if(risk<=0)
  return "";

 let html="";

 const targetEnabled =
  targetInfo &&
  targetInfo.enabled;

 const targetStage =
  Number(
   targetInfo &&
   targetInfo.target_stage
   ||
   0
  );

 for(
  let i=1;
  i<=10;
  i++
 ){

  const target =
   risk*i;

  const reached =
   current>=target;

  const isHighest =
   highest===i;

  const isSelected =
   targetEnabled &&
   targetStage===i;

  let cls =
   "asset-target-item";

  if(isSelected){

   cls +=
    " selected";

  }else if(isHighest){

   cls +=
    " current";

  }else if(reached){

   cls +=
    " reached";

  }

  const status =
   isSelected
    ? "🎯 매도"
    : isHighest
    ? "★ 최고"
    : reached
    ? "✓ 도달"
    : "";

  html += `

   <div class="${cls}">

    <div class="asset-target-label">
     1:${i}
    </div>

    <div class="asset-target-value">
     ${money(target)}
    </div>

    <div class="asset-target-status">

     <label
      style="
       display:flex;
       align-items:center;
       justify-content:center;
       gap:2px;
       cursor:pointer;
      "
     >

      <input
       class="target-checkbox"
       type="checkbox"

       ${isSelected
        ?"checked"
        :""}

       onchange="
        setCoinTarget(
         '${coin}',
         ${i},
         this.checked
        )
       "
      >

      <span>
       ${status}
      </span>

     </label>

    </div>

   </div>

  `;

 }

 return html;

}


// =========================================================
// 보유자산 렌더링
// =========================================================

function renderAssets(
 assets,
 risk,
 states,
 targets
){

 const box =
  $("assets");

 if(
  !assets
  ||
  !assets.length
 ){

  box.innerHTML =
   '<div class="empty">보유자산 없음</div>';

  $("asset-coin")
   .textContent =
   "보유 없음";

  return;

 }

 $("asset-coin")
  .textContent =
  assets
   .map(
    a=>a.currency
   )
   .join(
    " / "
   );

 let html="";

 assets.forEach(
  a=>{

   const coin =
    a.currency;

   const current =
    Number(
     a.profit_amount||0
    );

   const currentStage =
    risk>0
     ?Math.max(
       0,
       Math.floor(
        current/risk
       )
      )
     :0;

   const state =
    states[coin]||
    {
     highest_stage:0,
     highest_profit:0
    };

   const highest =
    Number(
     state.highest_stage||0
    );

   const highestProfit =
    Number(
     state.highest_profit||0
    );

   const trigger =
    highest>0
     ?(
       highest-1
      )*risk
     :0;

   const targetInfo =
    targets[coin]||
    {
     enabled:false,
     target_stage:0,
     target_profit:0
    };

   const targetStage =
    Number(
     targetInfo.target_stage||0
    );

   const targetProfit =
    targetStage>0
     ?risk*targetStage
     :0;

   const cls =
    current>=0
     ?"green"
     :"red";


   html += `

   <div class="asset">

    <!-- =========================================
         코인명 + 개별 전량매도
         ========================================= -->

    <div class="asset-header-row">

     <div class="asset-name">
      ${coin}
     </div>

     <button
      class="manual-sell-button"
      data-sell-coin="${coin}"
      onclick="
       manualSellCoin('${coin}')
      "
     >
      전량매도
     </button>

    </div>


    <div class="asset-grid">

     <div class="asset-row">
      <span>수량</span>
      ${num(a.balance)}
     </div>

     <div class="asset-row">
      <span>매수단가</span>
      ${price(a.avg_buy_price)}
     </div>

     <div class="asset-row">
      <span>현재가</span>
      ${price(a.current_price)}
     </div>

     <div class="asset-row">
      <span>매수금액</span>
      ${money(a.buy_amount_krw)}
     </div>

     <div class="asset-row">
      <span>평가금액</span>
      ${money(a.evaluation_krw)}
     </div>

     <div class="asset-row">
      <span>수익률</span>

      <strong class="${cls}">
       ${profitRate(a.profit_rate)}
      </strong>

     </div>

     <div
      class="asset-row"
      style="grid-column:span 2"
     >

      <span>
       현재 수익
      </span>

      <strong
       class="${cls} asset-profit"
      >
       ${profitMoney(current)}
      </strong>

     </div>

     <div
      class="asset-row"
      style="grid-column:span 2"
     >

      <span>
       리스크
      </span>

      <strong class="asset-risk">
       현재 1:${currentStage}
       ·
       최고 1:${highest}
      </strong>

     </div>

    </div>


    <div class="asset-risk-targets">

     <div class="asset-risk-title">

      리스크 목표
      ·
      1R ${money(risk)}

      ${
       targetInfo.enabled
        ?`
          ·
          <strong style="color:#22c55e">
           목표 1:${targetStage}
           (${money(targetProfit)})
          </strong>
         `
        :""
      }

      ${
       highest>10
        ?` · 내부 최고 1:${highest}`
        :""
      }

     </div>


     <div class="asset-target-grid">

      ${
       renderCoinRiskTargets(
        coin,
        risk,
        current,
        highest,
        targetInfo
       )
      }

     </div>


     ${
      highest>0
       ?`

        <div
         style="
          margin-top:5px;
          color:#9ca3af;
          font-size:9px
         "
        >

         최고
         ${profitMoney(highestProfit)}

         →

         기존 트레일링 매도
         ${profitMoney(trigger)}

        </div>

       `
       :""
     }

    </div>

   </div>

   `;

  }
 );

 box.innerHTML =
  html;

}


// =========================================================
// 리스크 테이블
// =========================================================

function renderRiskTable(
 month,
 rate
){

 const risk =
  month*rate;

 $("risk-unit-mini")
  .textContent =
  "1R "
  +
  money(risk);

 let html="";

 for(
  let stop=1;
  stop<=10;
  stop++
 ){

  const entry =
   risk/
   (stop/100);

  html += `

   <div class="risk-item">

    <div class="risk-stop">
     ${stop}%
    </div>

    <div class="risk-entry">
     ${money(entry)}
    </div>

   </div>

  `;

 }

 $("risk-table")
  .innerHTML =
  html;

}


// =========================================================
// 데이터 로드
// =========================================================

async function loadData(){

 try{

  const r =
   await fetch(
    "/api/upbit-assets"
   );

  if(!r.ok)
   throw new Error(
    "API error"
   );

  const d =
   await r.json();

  const total =
   Number(
    d.total_krw||0
   );

  const month =
   Number(
    d.month_start_amount||0
   );

  const rate =
   Number(
    d.max_loss_rate||0
   );

  const loss =
   Number(
    d.max_loss_amount||0
   );

  const risk =
   Number(
    d.risk_unit||0
   );

  const stop =
   Number(
    d.auto_stop_loss_krw||0
   );

  const take =
   Number(
    d.auto_take_profit_krw||0
   );

  window.currentTotal =
   total;


  $("month-start")
   .value =
   month;

  $("max-loss-rate")
   .value =
   (
    rate*100
   ).toFixed(2);

  $("auto-stop-loss-krw")
   .value =
   stop;

  $("auto-take-profit-krw")
   .value =
   take;


  $("auto-take-profit-off")
   .classList.toggle(
    "active",
    take<=0
   );


  // 전체매도 상태
  const allSelling =
   Boolean(
    d.manual_sell_all_in_progress
   );

  $("manual-sell-all")
   .disabled =
   allSelling;

  $("manual-sell-all")
   .classList.toggle(
    "active",
    allSelling
   );

  $("manual-sell-all")
   .textContent =
   allSelling
    ?"전체매도중..."
    :"전체매도 OFF";


  const monthProfit =
   total-month;

  const monthReturn =
   month>0
    ?monthProfit/
     month*
     100
    :0;


  $("total")
   .textContent =
   money(total);

  $("month-profit")
   .textContent =
   profitMoney(
    monthProfit
   );

  $("month-return")
   .textContent =
   profitRate(
    monthReturn
   );

  $("available")
   .textContent =
   money(
    d.available_krw
   );

  $("loss-limit-top")
   .textContent =
   money(loss);

  $("month-start-note")
   .textContent =
   money(month);

  $("max-loss-rate-note")
   .textContent =
   (
    rate*100
   ).toFixed(2)
   +
   "%";

  $("risk-unit-note")
   .textContent =
   money(risk);

  $("max-loss-amount-note")
   .textContent =
   money(loss);

  $("auto-stop-note")
   .textContent =
   money(stop);

  $("auto-take-note")
   .textContent =
   take>0
    ?money(take)
    :"OFF";

  $("updated")
   .textContent =
   d.updated_at||
   "-";

  $("latest-order")
   .textContent =
   d.latest_order||
   "주문 없음";


  updateTodayProfit(
   total
  );


  renderAssets(
   d.assets||[],
   risk,
   d.trailing_states||{},
   d.coin_targets||{}
  );


  renderRiskTable(
   month,
   rate
  );

 }catch(e){

  console.error(e);

 }

}


// =========================================================
// 시작
// =========================================================

loadLocalSettings();

loadData();

setInterval(
 loadData,
 5000
);

</script>

</body>

</html>
""")


# =========================================================
# 시작
# =========================================================

init_db()


# =========================================================
# 자동 손절 / 익절
# =========================================================

threading.Thread(
    target=real_upbit_auto_exit_monitor,
    daemon=True
).start()


# =========================================================
# 기존 리스크 트레일링
# =========================================================

threading.Thread(
    target=real_upbit_trailing_monitor,
    daemon=True
).start()


# =========================================================
# 코인별 목표 자동매도
# =========================================================

threading.Thread(
    target=real_upbit_coin_target_monitor,
    daemon=True
).start()


# =========================================================
# 업비트 자산 갱신
# =========================================================

threading.Thread(
    target=upbit_asset_refresh_monitor,
    daemon=True
).start()


# =========================================================
# 실행
# =========================================================

if __name__ == "__main__":

    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )
