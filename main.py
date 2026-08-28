import os
import time
import uuid
import threading
from datetime import datetime
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
)

import ccxt
import requests
from flask import Flask, jsonify, render_template_string


# ============================================================
# НАСТРОЙКИ
# ============================================================

TELEGRAM_BOT_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN",
    ""
).strip()

TELEGRAM_CHAT_ID = str(
    os.getenv(
        "TELEGRAM_CHAT_ID",
        ""
    )
).strip()


# ============================================================
# РАЗМЕР СДЕЛКИ
# ============================================================

# Максимальный фактический бюджет на одну сделку.
# В него входят:
# - стоимость покупки монеты
# - комиссия покупки
# - дополнительный защитный буфер
TRADE_AMOUNT_USD = 1000.0


# ============================================================
# ПРИБЫЛЬ И КОМИССИИ
# ============================================================

# Минимальная чистая прибыль после полного расчёта.
MIN_NET_PROFIT_PERCENT = 0.20

# Дополнительный резерв на небольшое ухудшение исполнения.
EXTRA_COST_BUFFER_PERCENT = 0.05


# Комиссии taker в процентах.
# При реальной торговле желательно заменить на комиссии
# именно вашего аккаунта.
EXCHANGE_FEES = {
    "binance": 0.10,
    "okx": 0.10,
    "bybit": 0.10,
    "bitget": 0.10,
    "kucoin": 0.10,
    "kraken": 0.26,
    "mexc": 0.10,
    "gate": 0.20,
    "htx": 0.20,
}


# ============================================================
# СКАНИРОВАНИЕ
# ============================================================

SCAN_INTERVAL = 10

MAX_NOTIFICATIONS_PER_SCAN = 3

NOTIFICATION_COOLDOWN = 300

OPPORTUNITY_TTL = 300

CLEANUP_INTERVAL = 60


# ============================================================
# ДИАГНОСТИКА
# ============================================================

# Включить подробную статистику в Railway logs.
DEBUG_DIAGNOSTICS = True

# Отправлять краткую диагностику в Telegram после каждого скана.
TELEGRAM_DIAGNOSTICS = False

# Если True — отправляем сообщение в Telegram даже если
# не найдено ни одной итоговой возможности.
TELEGRAM_ZERO_OPPORTUNITIES_ALERT = False


# ============================================================
# СВЕЖЕСТЬ СТАКАНА
# ============================================================

MAX_ORDER_BOOK_AGE_SECONDS = 15

# Некоторые биржи не передают timestamp.
ALLOW_ORDER_BOOK_WITHOUT_TIMESTAMP = True

# Старый timestamp не отбрасывает стакан автоматически.
# Вместо этого стакан получает штраф качества.
REJECT_STALE_ORDER_BOOKS = False


# ============================================================
# ЛИКВИДНОСТЬ И ИСПОЛНЕНИЕ
# ============================================================

ORDER_BOOK_LIMIT = 50

# Для тестирования снижаем запас ликвидности.
LIQUIDITY_SAFETY_MULTIPLIER = 1.05

MAX_BUY_SLIPPAGE_PERCENT = 0.50

MAX_SELL_SLIPPAGE_PERCENT = 0.50

MAX_EXECUTION_PRICE_DEVIATION_PERCENT = 0.75


# ============================================================
# ПАРАЛЛЕЛЬНОСТЬ
# ============================================================

# Пул только для запуска задач сканирования монет.
# В нём НЕ выполняются сетевые запросы стаканов.
SCAN_WORKERS = 4

# Отдельный постоянный пул только для запросов к биржам.
NETWORK_WORKERS = 36

# Максимальное время ожидания одного сетевого запроса.
NETWORK_TASK_TIMEOUT = 20


# ============================================================
# TELEGRAM
# ============================================================

TELEGRAM_POLL_INTERVAL = 2

TELEGRAM_LONG_POLL_TIMEOUT = 20


# ============================================================
# 4 ЛИКВИДНЫЕ МОНЕТЫ
# ============================================================

SYMBOLS = [
    "BTC/USDT",
    "ETH/USDT",
    "SOL/USDT",
    "XRP/USDT",
]


# ============================================================
# БИРЖИ
# ============================================================

EXCHANGE_NAMES = {
    "binance": "BINANCE",
    "okx": "OKX",
    "bybit": "BYBIT",
    "bitget": "BITGET",
    "kucoin": "KUCOIN",
    "kraken": "KRAKEN",
    "mexc": "MEXC",
    "gate": "GATE.IO",
    "htx": "HTX",
}


EXCHANGE_CLASSES = {
    "binance": ccxt.binance,
    "okx": ccxt.okx,
    "bybit": ccxt.bybit,
    "bitget": ccxt.bitget,
    "kucoin": ccxt.kucoin,
    "kraken": ccxt.kraken,
    "mexc": ccxt.mexc,
    "gate": ccxt.gate,
    "htx": ccxt.htx,
}


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# ДВА ПОСТОЯННЫХ ПУЛА ПОТОКОВ
# ============================================================

SCAN_EXECUTOR = ThreadPoolExecutor(
    max_workers=SCAN_WORKERS,
    thread_name_prefix="scan-worker",
)

NETWORK_EXECUTOR = ThreadPoolExecutor(
    max_workers=NETWORK_WORKERS,
    thread_name_prefix="network-worker",
)


# ============================================================
# СОЗДАНИЕ БИРЖ
# ============================================================

exchanges = {}
available_symbols_by_exchange = {}

for exchange_id, exchange_class in EXCHANGE_CLASSES.items():

    try:

        exchange = exchange_class({
            "enableRateLimit": True,
            "timeout": 15000,
        })

        exchanges[exchange_id] = exchange

        print(
            f"✅ Создано подключение: {exchange_id}"
        )

    except Exception as e:

        print(
            f"❌ Ошибка создания "
            f"{exchange_id}: {e}"
        )


# ============================================================
# СОСТОЯНИЕ
# ============================================================

last_opportunities = []
last_scan_time = None
last_scan_diagnostics = {}

last_sent_notifications = {}
pending_opportunities = {}

scanner_started = False
telegram_started = False
services_started = False

current_symbols = []

total_scans = 0
total_opportunities_found = 0

total_order_book_requests = 0
total_successful_order_books = 0
total_failed_order_books = 0

lock = threading.RLock()
notification_lock = threading.RLock()
pending_lock = threading.RLock()
startup_lock = threading.Lock()
stats_lock = threading.RLock()


# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================

def safe_float(
    value,
    default=0.0
):

    try:

        return float(value)

    except (
        TypeError,
        ValueError,
    ):

        return default


def create_diagnostics(
    symbol=None
):

    return {
        "symbol": symbol,

        "order_book_requests": 0,
        "order_books_received": 0,
        "order_books_failed": 0,

        "exchange_pairs_total": 0,

        # Направления, где Sell Bid > Buy Ask.
        # Это ещё НЕ означает наличие прибыли.
        "exchange_pairs_price_possible": 0,

        # Направления, где Sell Bid <= Buy Ask.
        "rejected_fast_precheck": 0,

        "rejected_buy_liquidity": 0,
        "rejected_sell_liquidity": 0,
        "rejected_buy_execution": 0,
        "rejected_buy_budget": 0,
        "rejected_sell_execution": 0,
        "rejected_buy_slippage": 0,
        "rejected_sell_slippage": 0,
        "rejected_profit": 0,

        "full_calculations": 0,
        "final_opportunities": 0,
    }


def add_diagnostics(
    target,
    source
):

    for key, value in source.items():

        if key == "symbol":

            continue

        if isinstance(
            value,
            (int, float),
        ):

            target[key] = (
                target.get(
                    key,
                    0,
                )
                + value
            )


# ============================================================
# ЗАГРУЗКА MARKETS
# ============================================================

def load_all_markets():

    global available_symbols_by_exchange

    print("")
    print("=" * 55)
    print("📚 ЗАГРУЗКА MARKETS")
    print("=" * 55)

    futures = {}

    for exchange_id, exchange in exchanges.items():

        future = NETWORK_EXECUTOR.submit(
            exchange.load_markets
        )

        futures[future] = exchange_id

    for future in as_completed(futures):

        exchange_id = futures[future]

        try:

            markets = future.result(
                timeout=NETWORK_TASK_TIMEOUT
            )

            supported = set()

            for symbol in SYMBOLS:

                if symbol in markets:

                    supported.add(
                        symbol
                    )

            available_symbols_by_exchange[
                exchange_id
            ] = supported

            print(
                f"✅ {exchange_id}: "
                f"{len(supported)}/{len(SYMBOLS)} "
                f"доступно"
            )

        except Exception as e:

            available_symbols_by_exchange[
                exchange_id
            ] = set()

            print(
                f"❌ load_markets "
                f"{exchange_id}: {e}"
            )

    print("=" * 55)
    print("")


# ============================================================
# TELEGRAM API
# ============================================================

def telegram_api(
    method,
    data=None,
    request_method="POST"
):

    if not TELEGRAM_BOT_TOKEN:

        return None

    url = (
        f"https://api.telegram.org/bot"
        f"{TELEGRAM_BOT_TOKEN}/{method}"
    )

    try:

        if request_method == "GET":

            response = requests.get(
                url,
                params=data or {},
                timeout=35,
            )

        else:

            response = requests.post(
                url,
                json=data or {},
                timeout=25,
            )

        result = response.json()

        if not result.get("ok"):

            print(
                f"❌ Telegram "
                f"{method}: {result}"
            )

        return result

    except Exception as e:

        print(
            f"❌ Telegram API "
            f"{method}: {e}"
        )

        return None


def send_telegram_message(
    text,
    reply_markup=None
):

    if not TELEGRAM_BOT_TOKEN:

        print(
            "❌ TELEGRAM_BOT_TOKEN отсутствует"
        )

        return None

    if not TELEGRAM_CHAT_ID:

        print(
            "❌ TELEGRAM_CHAT_ID отсутствует"
        )

        return None

    data = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
    }

    if reply_markup:

        data[
            "reply_markup"
        ] = reply_markup

    return telegram_api(
        "sendMessage",
        data,
        request_method="POST",
    )


def answer_callback_query(
    callback_query_id,
    text=""
):

    return telegram_api(
        "answerCallbackQuery",
        {
            "callback_query_id":
                callback_query_id,

            "text":
                text,
        },
    )


# ============================================================
# TELEGRAM ПРОВЕРКА
# ============================================================

def check_telegram_connection():

    print("")
    print("=" * 55)
    print("🤖 ПРОВЕРКА TELEGRAM")
    print("=" * 55)

    if not TELEGRAM_BOT_TOKEN:

        print(
            "❌ TELEGRAM_BOT_TOKEN не найден"
        )

        return False

    if not TELEGRAM_CHAT_ID:

        print(
            "❌ TELEGRAM_CHAT_ID не найден"
        )

        return False

    result = telegram_api(
        "getMe",
        request_method="GET"
    )

    if not result or not result.get("ok"):

        print(
            "❌ TELEGRAM TOKEN НЕ РАБОТАЕТ"
        )

        return False

    bot = result.get(
        "result",
        {}
    )

    bot_name = bot.get(
        "username",
        "unknown"
    )

    startup_text = f"""
🟢 <b>АРБИТРАЖНЫЙ БОТ ЗАПУЩЕН</b>

🤖 Бот:
<b>@{bot_name}</b>

💰 Максимальный бюджет:
<b>${TRADE_AMOUNT_USD:,.2f}</b>

📈 Минимальная чистая прибыль:
<b>{MIN_NET_PROFIT_PERCENT}%</b>

🪙 Монет:
<b>{len(SYMBOLS)}</b>

🏦 Бирж:
<b>{len(exchanges)}</b>

⚡ Пул сканирования:
<b>{SCAN_WORKERS} потоков</b>

🌐 Сетевой пул:
<b>{NETWORK_WORKERS} потоков</b>

📊 Диагностика причин отбрасывания:
<b>ВКЛЮЧЕНА</b>

🧪 <b>ТЕСТОВЫЙ РЕЖИМ</b>
Реальные ордера не выставляются.
"""

    result = send_telegram_message(
        startup_text
    )

    if result and result.get("ok"):

        print(
            f"✅ Telegram подключён: "
            f"@{bot_name}"
        )

        print("=" * 55)

        return True

    print(
        "❌ Не удалось отправить "
        "стартовое сообщение"
    )

    return False


# ============================================================
# ОЧИСТКА УРОВНЕЙ СТАКАНА
# ============================================================

def clean_order_levels(
    levels
):

    clean_levels = []

    if not levels:

        return clean_levels

    for level in levels:

        if not isinstance(
            level,
            (list, tuple),
        ):

            continue

        if len(level) < 2:

            continue

        price = safe_float(
            level[0]
        )

        amount = safe_float(
            level[1]
        )

        if (
            price > 0
            and amount > 0
        ):

            clean_levels.append(
                [price, amount]
            )

    return clean_levels


# ============================================================
# ПОЛУЧЕНИЕ СТАКАНА
# ============================================================

def get_order_book(
    exchange_id,
    symbol
):

    global total_order_book_requests
    global total_successful_order_books
    global total_failed_order_books

    exchange = exchanges.get(
        exchange_id
    )

    if not exchange:

        return None

    supported_symbols = (
        available_symbols_by_exchange.get(
            exchange_id,
            set(),
        )
    )

    if (
        supported_symbols
        and symbol not in supported_symbols
    ):

        return None

    started_at = time.time()

    with stats_lock:

        total_order_book_requests += 1

    try:

        order_book = exchange.fetch_order_book(
            symbol,
            limit=ORDER_BOOK_LIMIT,
        )

        received_at = time.time()

        asks = clean_order_levels(
            order_book.get(
                "asks",
                [],
            )
        )

        bids = clean_order_levels(
            order_book.get(
                "bids",
                [],
            )
        )

        if not asks or not bids:

            with stats_lock:

                total_failed_order_books += 1

            return None

        asks.sort(
            key=lambda x: x[0]
        )

        bids.sort(
            key=lambda x: x[0],
            reverse=True,
        )

        exchange_timestamp = (
            order_book.get(
                "timestamp"
            )
        )

        timestamp_age = None

        if exchange_timestamp:

            try:

                timestamp_seconds = (
                    float(exchange_timestamp)
                    / 1000
                )

                timestamp_age = (
                    received_at
                    - timestamp_seconds
                )

            except Exception:

                timestamp_age = None

        is_stale = False

        if timestamp_age is not None:

            if (
                timestamp_age
                > MAX_ORDER_BOOK_AGE_SECONDS
            ):

                is_stale = True

        elif not ALLOW_ORDER_BOOK_WITHOUT_TIMESTAMP:

            is_stale = True

        if (
            is_stale
            and REJECT_STALE_ORDER_BOOKS
        ):

            with stats_lock:

                total_failed_order_books += 1

            return None

        with stats_lock:

            total_successful_order_books += 1

        return {

            "asks": asks,
            "bids": bids,

            "best_ask": asks[0][0],
            "best_bid": bids[0][0],

            "exchange_timestamp":
                exchange_timestamp,

            "timestamp_age":
                timestamp_age,

            "is_stale":
                is_stale,

            "received_at":
                received_at,

            "request_duration":
                received_at
                - started_at,
        }

    except Exception as e:

        with stats_lock:

            total_failed_order_books += 1

        print(
            f"⚠️ Стакан "
            f"{exchange_id} "
            f"{symbol}: {e}"
        )

        return None


# ============================================================
# ПАРАЛЛЕЛЬНОЕ ПОЛУЧЕНИЕ ВСЕХ СТАКАНОВ
# ============================================================

def get_all_order_books_parallel(
    symbol,
    diagnostics
):

    results = {}

    futures = {}

    for exchange_id in exchanges:

        supported = (
            available_symbols_by_exchange.get(
                exchange_id,
                set(),
            )
        )

        if (
            supported
            and symbol not in supported
        ):

            continue

        diagnostics[
            "order_book_requests"
        ] += 1

        future = NETWORK_EXECUTOR.submit(
            get_order_book,
            exchange_id,
            symbol,
        )

        futures[
            future
        ] = exchange_id

    for future in as_completed(
        futures
    ):

        exchange_id = futures[
            future
        ]

        try:

            order_book = future.result()

            if order_book:

                results[
                    exchange_id
                ] = order_book

                diagnostics[
                    "order_books_received"
                ] += 1

            else:

                diagnostics[
                    "order_books_failed"
                ] += 1

        except Exception as e:

            diagnostics[
                "order_books_failed"
            ] += 1

            print(
                f"⚠️ Ошибка "
                f"{exchange_id} "
                f"{symbol}: {e}"
            )

    return results


# ============================================================
# РАСЧЁТ ДОСТУПНОЙ СУММЫ НА ПОКУПКУ
# ============================================================

def calculate_asset_budget(
    buy_fee_percent
):

    reserve_percent = (
        buy_fee_percent
        + EXTRA_COST_BUFFER_PERCENT
    )

    return (
        TRADE_AMOUNT_USD
        / (
            1
            + reserve_percent / 100
        )
    )


# ============================================================
# СИМУЛЯЦИЯ ПОКУПКИ
# ============================================================

def simulate_buy(
    asks,
    quote_amount
):

    if not asks:

        return None

    remaining_usd = safe_float(
        quote_amount
    )

    total_spent = 0.0
    total_quantity = 0.0

    best_ask = asks[0][0]

    max_price = (
        best_ask
        * (
            1
            + MAX_EXECUTION_PRICE_DEVIATION_PERCENT
            / 100
        )
    )

    for price, available_quantity in asks:

        if price > max_price:

            break

        if remaining_usd <= 0:

            break

        level_value = (
            price
            * available_quantity
        )

        spend = min(
            remaining_usd,
            level_value,
        )

        quantity = (
            spend / price
        )

        total_spent += spend
        total_quantity += quantity
        remaining_usd -= spend

    if remaining_usd > 0.000001:

        return None

    if total_quantity <= 0:

        return None

    average_price = (
        total_spent
        / total_quantity
    )

    return {

        "spent": total_spent,

        "quantity": total_quantity,

        "average_price":
            average_price,

        "max_price":
            max_price,
    }


# ============================================================
# СИМУЛЯЦИЯ ПРОДАЖИ
# ============================================================

def simulate_sell(
    bids,
    quantity
):

    if not bids:

        return None

    remaining_quantity = safe_float(
        quantity
    )

    total_revenue = 0.0
    total_sold = 0.0

    best_bid = bids[0][0]

    min_price = (
        best_bid
        * (
            1
            - MAX_EXECUTION_PRICE_DEVIATION_PERCENT
            / 100
        )
    )

    for price, available_quantity in bids:

        if price < min_price:

            break

        if remaining_quantity <= 0:

            break

        sell_quantity = min(
            remaining_quantity,
            available_quantity,
        )

        total_revenue += (
            sell_quantity
            * price
        )

        total_sold += sell_quantity
        remaining_quantity -= sell_quantity

    if remaining_quantity > 0.00000001:

        return None

    if total_sold <= 0:

        return None

    average_price = (
        total_revenue
        / total_sold
    )

    return {

        "revenue": total_revenue,

        "quantity": total_sold,

        "average_price":
            average_price,

        "min_price":
            min_price,
    }


# ============================================================
# ЛИКВИДНОСТЬ ПОКУПКИ
# ============================================================

def calculate_buy_execution_liquidity(
    asks
):

    if not asks:

        return 0.0

    best_ask = asks[0][0]

    max_price = (
        best_ask
        * (
            1
            + MAX_EXECUTION_PRICE_DEVIATION_PERCENT
            / 100
        )
    )

    liquidity = 0.0

    for price, amount in asks:

        if price > max_price:

            break

        liquidity += (
            price * amount
        )

    return liquidity


# ============================================================
# ЛИКВИДНОСТЬ ПРОДАЖИ
# ============================================================

def calculate_sell_execution_liquidity(
    bids
):

    if not bids:

        return 0.0

    best_bid = bids[0][0]

    min_price = (
        best_bid
        * (
            1
            - MAX_EXECUTION_PRICE_DEVIATION_PERCENT
            / 100
        )
    )

    liquidity = 0.0

    for price, amount in bids:

        if price < min_price:

            break

        liquidity += (
            price * amount
        )

    return liquidity


# ============================================================
# БЫСТРЫЙ ФИЛЬТР
# ============================================================

def passes_fast_precheck(
    buy_exchange,
    buy_order_book,
    sell_exchange,
    sell_order_book
):

    # Получаем лучшие цены безопасно.
    buy_ask = safe_float(
        buy_order_book.get(
            "best_ask",
            0.0,
        )
    )

    sell_bid = safe_float(
        sell_order_book.get(
            "best_bid",
            0.0,
        )
    )

    # --------------------------------------------------------
    # ЗАЩИТА ОТ НЕКОРРЕКТНЫХ ЦЕН
    # --------------------------------------------------------

    if buy_ask <= 0:

        return False

    if sell_bid <= 0:

        return False


    # --------------------------------------------------------
    # ВАЖНО
    #
    # Быстрый фильтр НЕ учитывает заранее:
    #
    # - комиссию покупки
    # - комиссию продажи
    # - защитный буфер
    # - минимальную чистую прибыль
    #
    # Эти параметры проверяются только после реальной
    # симуляции покупки и продажи по уровням стакана.
    #
    # Здесь проверяется только наличие потенциального
    # положительного ценового направления.
    # --------------------------------------------------------

    if sell_bid <= buy_ask:

        return False


    return True


# ============================================================
# РЕЙТИНГ КАЧЕСТВА
# ============================================================

def calculate_opportunity_score(
    net_profit_percent,
    gross_spread_percent,
    buy_slippage_percent,
    sell_slippage_percent,
    buy_liquidity,
    sell_liquidity,
    buy_is_stale=False,
    sell_is_stale=False,
):

    profit_score = min(
        40.0,
        max(
            0.0,
            net_profit_percent
            / 2.0
            * 40.0,
        ),
    )

    spread_score = min(
        15.0,
        max(
            0.0,
            gross_spread_percent
            / 3.0
            * 15.0,
        ),
    )

    required_liquidity = (
        TRADE_AMOUNT_USD
        * LIQUIDITY_SAFETY_MULTIPLIER
    )

    min_liquidity = min(
        buy_liquidity,
        sell_liquidity,
    )

    liquidity_ratio = (
        min_liquidity
        / required_liquidity
        if required_liquidity > 0
        else 0
    )

    liquidity_score = min(
        20.0,
        liquidity_ratio * 10.0,
    )

    total_slippage = (
        buy_slippage_percent
        + sell_slippage_percent
    )

    slippage_score = max(
        0.0,
        25.0
        - total_slippage * 25.0,
    )

    freshness_penalty = 0.0

    if buy_is_stale:

        freshness_penalty += 5.0

    if sell_is_stale:

        freshness_penalty += 5.0

    score = (
        profit_score
        + spread_score
        + liquidity_score
        + slippage_score
        - freshness_penalty
    )

    return round(
        max(
            0.0,
            min(
                100.0,
                score,
            ),
        ),
        1,
    )


def get_quality_label(
    score
):

    if score >= 80:

        return "ОТЛИЧНАЯ ⭐⭐⭐"

    if score >= 60:

        return "ХОРОШАЯ ⭐⭐"

    if score >= 40:

        return "СРЕДНЯЯ ⭐"

    return "НИЗКАЯ"


# ============================================================
# ПОЛНЫЙ РАСЧЁТ ВОЗМОЖНОСТИ
# ============================================================

def calculate_order_book_opportunity(
    symbol,
    buy_exchange,
    buy_order_book,
    sell_exchange,
    sell_order_book,
    diagnostics=None,
):

    if diagnostics is not None:

        diagnostics[
            "full_calculations"
        ] += 1


    # --------------------------------------------------------
    # КОМИССИЯ И БЮДЖЕТ
    # --------------------------------------------------------

    buy_fee_percent = (
        EXCHANGE_FEES.get(
            buy_exchange,
            0.20,
        )
    )

    sell_fee_percent = (
        EXCHANGE_FEES.get(
            sell_exchange,
            0.20,
        )
    )

    asset_budget = (
        calculate_asset_budget(
            buy_fee_percent
        )
    )


    # --------------------------------------------------------
    # ЛИКВИДНОСТЬ ПОКУПКИ
    # --------------------------------------------------------

    buy_liquidity = (
        calculate_buy_execution_liquidity(
            buy_order_book["asks"]
        )
    )

    required_buy_liquidity = (
        asset_budget
        * LIQUIDITY_SAFETY_MULTIPLIER
    )

    if (
        buy_liquidity
        < required_buy_liquidity
    ):

        if diagnostics is not None:

            diagnostics[
                "rejected_buy_liquidity"
            ] += 1

        return None


    # --------------------------------------------------------
    # РЕАЛЬНАЯ ПОКУПКА
    # --------------------------------------------------------

    buy_result = simulate_buy(
        buy_order_book["asks"],
        asset_budget,
    )

    if not buy_result:

        if diagnostics is not None:

            diagnostics[
                "rejected_buy_execution"
            ] += 1

        return None


    buy_fee_usd = (
        buy_result["spent"]
        * buy_fee_percent
        / 100
    )

    buy_extra_buffer_usd = (
        buy_result["spent"]
        * EXTRA_COST_BUFFER_PERCENT
        / 100
    )

    buy_cost = (
        buy_result["spent"]
        + buy_fee_usd
        + buy_extra_buffer_usd
    )

    if buy_cost > TRADE_AMOUNT_USD:

        if diagnostics is not None:

            diagnostics[
                "rejected_buy_budget"
            ] += 1

        return None


    # --------------------------------------------------------
    # ЛИКВИДНОСТЬ ПРОДАЖИ
    # --------------------------------------------------------

    sell_liquidity = (
        calculate_sell_execution_liquidity(
            sell_order_book["bids"]
        )
    )

    required_sell_liquidity = (
        buy_result["quantity"]
        * sell_order_book["best_bid"]
        * LIQUIDITY_SAFETY_MULTIPLIER
    )

    if (
        sell_liquidity
        < required_sell_liquidity
    ):

        if diagnostics is not None:

            diagnostics[
                "rejected_sell_liquidity"
            ] += 1

        return None


    # --------------------------------------------------------
    # РЕАЛЬНАЯ ПРОДАЖА
    # --------------------------------------------------------

    sell_result = simulate_sell(
        sell_order_book["bids"],
        buy_result["quantity"],
    )

    if not sell_result:

        if diagnostics is not None:

            diagnostics[
                "rejected_sell_execution"
            ] += 1

        return None


    sell_fee_usd = (
        sell_result["revenue"]
        * sell_fee_percent
        / 100
    )

    sell_revenue = (
        sell_result["revenue"]
        - sell_fee_usd
    )


    # --------------------------------------------------------
    # ПРИБЫЛЬ
    # --------------------------------------------------------

    net_profit_usd = (
        sell_revenue
        - buy_cost
    )

    if buy_cost <= 0:

        return None

    net_profit_percent = (
        net_profit_usd
        / buy_cost
    ) * 100

    if (
        net_profit_percent
        < MIN_NET_PROFIT_PERCENT
    ):

        if diagnostics is not None:

            diagnostics[
                "rejected_profit"
            ] += 1

        return None


    # --------------------------------------------------------
    # ВАЛОВЫЙ СПРЕД
    # --------------------------------------------------------

    gross_spread_percent = (
        (
            sell_result["average_price"]
            - buy_result["average_price"]
        )
        / buy_result["average_price"]
    ) * 100


    # --------------------------------------------------------
    # ПРОСКАЛЬЗЫВАНИЕ
    # --------------------------------------------------------

    buy_slippage_percent = (
        (
            buy_result["average_price"]
            - buy_order_book["best_ask"]
        )
        / buy_order_book["best_ask"]
    ) * 100

    sell_slippage_percent = (
        (
            sell_order_book["best_bid"]
            - sell_result["average_price"]
        )
        / sell_order_book["best_bid"]
    ) * 100


    if (
        buy_slippage_percent
        > MAX_BUY_SLIPPAGE_PERCENT
    ):

        if diagnostics is not None:

            diagnostics[
                "rejected_buy_slippage"
            ] += 1

        return None


    if (
        sell_slippage_percent
        > MAX_SELL_SLIPPAGE_PERCENT
    ):

        if diagnostics is not None:

            diagnostics[
                "rejected_sell_slippage"
            ] += 1

        return None


    # --------------------------------------------------------
    # РЕЙТИНГ
    # --------------------------------------------------------

    quality_score = (
        calculate_opportunity_score(
            net_profit_percent,
            gross_spread_percent,
            buy_slippage_percent,
            sell_slippage_percent,
            buy_liquidity,
            sell_liquidity,
            buy_order_book["is_stale"],
            sell_order_book["is_stale"],
        )
    )

    quality_label = (
        get_quality_label(
            quality_score
        )
    )


    # --------------------------------------------------------
    # УСПЕШНАЯ ВОЗМОЖНОСТЬ
    # --------------------------------------------------------

    if diagnostics is not None:

        diagnostics[
            "final_opportunities"
        ] += 1


    return {

        "symbol":
            symbol,

        "buy_exchange":
            buy_exchange,

        "buy_exchange_name":
            EXCHANGE_NAMES.get(
                buy_exchange,
                buy_exchange.upper(),
            ),

        "buy_price":
            round(
                buy_result["average_price"],
                8,
            ),

        "buy_best_ask":
            round(
                buy_order_book["best_ask"],
                8,
            ),

        "buy_exchange_liquidity":
            round(
                buy_liquidity,
                2,
            ),

        "sell_exchange":
            sell_exchange,

        "sell_exchange_name":
            EXCHANGE_NAMES.get(
                sell_exchange,
                sell_exchange.upper(),
            ),

        "sell_price":
            round(
                sell_result["average_price"],
                8,
            ),

        "sell_best_bid":
            round(
                sell_order_book["best_bid"],
                8,
            ),

        "sell_exchange_liquidity":
            round(
                sell_liquidity,
                2,
            ),

        "quantity":
            buy_result["quantity"],

        "asset_budget":
            round(
                asset_budget,
                2,
            ),

        "actual_buy_cost":
            round(
                buy_cost,
                2,
            ),

        "buy_fee_usd":
            round(
                buy_fee_usd,
                2,
            ),

        "buy_extra_buffer_usd":
            round(
                buy_extra_buffer_usd,
                2,
            ),

        "sell_fee_usd":
            round(
                sell_fee_usd,
                2,
            ),

        "gross_spread_percent":
            round(
                gross_spread_percent,
                4,
            ),

        "net_profit_percent":
            round(
                net_profit_percent,
                4,
            ),

        "net_profit_usd":
            round(
                net_profit_usd,
                2,
            ),

        "buy_fee_percent":
            buy_fee_percent,

        "sell_fee_percent":
            sell_fee_percent,

        "buy_slippage_percent":
            round(
                buy_slippage_percent,
                4,
            ),

        "sell_slippage_percent":
            round(
                sell_slippage_percent,
                4,
            ),

        "quality_score":
            quality_score,

        "quality_label":
            quality_label,

        "buy_order_book_age":
            buy_order_book[
                "timestamp_age"
            ],

        "sell_order_book_age":
            sell_order_book[
                "timestamp_age"
            ],

        "buy_order_book_stale":
            buy_order_book[
                "is_stale"
            ],

        "sell_order_book_stale":
            sell_order_book[
                "is_stale"
            ],
    }


# ============================================================
# СКАНИРОВАНИЕ ОДНОЙ МОНЕТЫ
# ============================================================

def scan_symbol(
    symbol
):

    diagnostics = create_diagnostics(
        symbol
    )

    order_books = (
        get_all_order_books_parallel(
            symbol,
            diagnostics,
        )
    )

    opportunities = []

    exchange_ids = list(
        order_books.keys()
    )

    if len(exchange_ids) < 2:

        return (
            opportunities,
            diagnostics,
        )


    # --------------------------------------------------------
    # ПРОВЕРКА ВСЕХ НАПРАВЛЕНИЙ
    # --------------------------------------------------------

    for buy_exchange in exchange_ids:

        buy_book = order_books[
            buy_exchange
        ]

        for sell_exchange in exchange_ids:

            if buy_exchange == sell_exchange:

                continue

            diagnostics[
                "exchange_pairs_total"
            ] += 1

            sell_book = order_books[
                sell_exchange
            ]

            # ------------------------------------------------
            # ПРЕДВАРИТЕЛЬНАЯ ПРОВЕРКА
            #
            # Здесь отбрасываются только направления, где
            # цена продажи не выше цены покупки.
            #
            # Комиссии и минимальная прибыль проверяются
            # только в полном расчёте.
            # ------------------------------------------------

            if not passes_fast_precheck(
                buy_exchange,
                buy_book,
                sell_exchange,
                sell_book,
            ):

                diagnostics[
                    "rejected_fast_precheck"
                ] += 1

                continue

            diagnostics[
                "exchange_pairs_price_possible"
            ] += 1

            opportunity = (
                calculate_order_book_opportunity(
                    symbol=symbol,
                    buy_exchange=buy_exchange,
                    buy_order_book=buy_book,
                    sell_exchange=sell_exchange,
                    sell_order_book=sell_book,
                    diagnostics=diagnostics,
                )
            )

            if opportunity:

                opportunities.append(
                    opportunity
                )


    return (
        opportunities,
        diagnostics,
    )


# ============================================================
# WORKER СКАНИРОВАНИЯ
# ============================================================

def scan_symbol_worker(
    symbol
):

    with lock:

        if symbol not in current_symbols:

            current_symbols.append(
                symbol
            )

    try:

        return scan_symbol(
            symbol
        )

    finally:

        with lock:

            if symbol in current_symbols:

                current_symbols.remove(
                    symbol
                )


# ============================================================
# ПОЛНОЕ СКАНИРОВАНИЕ
# ============================================================

def scan_all():

    all_opportunities = []

    total_diagnostics = create_diagnostics(
        "ALL"
    )

    symbol_diagnostics = {}

    futures = {}

    for symbol in SYMBOLS:

        future = SCAN_EXECUTOR.submit(
            scan_symbol_worker,
            symbol,
        )

        futures[
            future
        ] = symbol

    for future in as_completed(
        futures
    ):

        symbol = futures[
            future
        ]

        try:

            (
                opportunities,
                diagnostics,
            ) = future.result()

            symbol_diagnostics[
                symbol
            ] = diagnostics

            add_diagnostics(
                total_diagnostics,
                diagnostics,
            )

            if opportunities:

                all_opportunities.extend(
                    opportunities
                )

        except Exception as e:

            print(
                f"⚠️ Ошибка "
                f"сканирования "
                f"{symbol}: {e}"
            )

    all_opportunities.sort(
        key=lambda x: (
            x["quality_score"],
            x["net_profit_percent"],
        ),
        reverse=True,
    )

    total_diagnostics[
        "symbols"
    ] = symbol_diagnostics

    total_diagnostics[
        "final_opportunities"
    ] = len(
        all_opportunities
    )

    return (
        all_opportunities,
        total_diagnostics,
    )


# ============================================================
# ВЫВОД ДИАГНОСТИКИ
# ============================================================

def print_diagnostics(
    diagnostics
):

    if not DEBUG_DIAGNOSTICS:

        return

    print("")
    print("📊 ДИАГНОСТИКА СКАНА")
    print("-" * 55)

    print(
        f"📚 Запросов стаканов: "
        f"{diagnostics['order_book_requests']}"
    )

    print(
        f"✅ Стаканов получено: "
        f"{diagnostics['order_books_received']}"
    )

    print(
        f"❌ Стаканов не получено: "
        f"{diagnostics['order_books_failed']}"
    )

    print(
        f"🔗 Всего направлений: "
        f"{diagnostics['exchange_pairs_total']}"
    )

    print(
        f"🔎 Направлений с положительным спредом: "
        f"{diagnostics['exchange_pairs_price_possible']}"
    )

    print(
        f"🚫 Нет положительного ценового направления: "
        f"{diagnostics['rejected_fast_precheck']}"
    )

    print(
        f"🧮 Полных расчётов выполнено: "
        f"{diagnostics['full_calculations']}"
    )

    print(
        f"💧 Покупка — ликвидность: "
        f"{diagnostics['rejected_buy_liquidity']}"
    )

    print(
        f"💧 Продажа — ликвидность: "
        f"{diagnostics['rejected_sell_liquidity']}"
    )

    print(
        f"📉 Покупка — исполнение: "
        f"{diagnostics['rejected_buy_execution']}"
    )

    print(
        f"📉 Продажа — исполнение: "
        f"{diagnostics['rejected_sell_execution']}"
    )

    print(
        f"💰 Превышен бюджет: "
        f"{diagnostics['rejected_buy_budget']}"
    )

    print(
        f"📉 Проскальзывание покупки: "
        f"{diagnostics['rejected_buy_slippage']}"
    )

    print(
        f"📉 Проскальзывание продажи: "
        f"{diagnostics['rejected_sell_slippage']}"
    )

    print(
        f"📈 Недостаточная прибыль: "
        f"{diagnostics['rejected_profit']}"
    )

    print(
        f"🎯 ИТОГОВЫХ ВОЗМОЖНОСТЕЙ: "
        f"{diagnostics['final_opportunities']}"
    )

    print("-" * 55)

    for symbol, symbol_data in diagnostics.get(
        "symbols",
        {},
    ).items():

        print(
            f"🪙 {symbol}: "
            f"стаканов="
            f"{symbol_data['order_books_received']}, "
            f"пар="
            f"{symbol_data['exchange_pairs_total']}, "
            f"положительный_спред="
            f"{symbol_data['exchange_pairs_price_possible']}, "
            f"полных_расчётов="
            f"{symbol_data['full_calculations']}, "
            f"итог="
            f"{symbol_data['final_opportunities']}"
        )

    print("-" * 55)
    print("")


# ============================================================
# АВТОМАТИЧЕСКАЯ ОЧИСТКА
# ============================================================

def cleanup_old_data():

    current_time = time.time()

    with pending_lock:

        expired_ids = [

            opportunity_id

            for (
                opportunity_id,
                opportunity,
            ) in pending_opportunities.items()

            if (
                current_time
                - opportunity[
                    "created_at"
                ]
                > OPPORTUNITY_TTL
            )
        ]

        for opportunity_id in expired_ids:

            pending_opportunities.pop(
                opportunity_id,
                None,
            )

    with notification_lock:

        old_keys = [

            key

            for (
                key,
                last_sent,
            ) in last_sent_notifications.items()

            if (
                current_time
                - last_sent
                > NOTIFICATION_COOLDOWN * 2
            )
        ]

        for key in old_keys:

            last_sent_notifications.pop(
                key,
                None,
            )


# ============================================================
# ОТПРАВКА ВОЗМОЖНОСТИ
# ============================================================

def send_opportunity_to_telegram(
    opportunity
):

    notification_key = (
        f"{opportunity['symbol']}_"
        f"{opportunity['buy_exchange']}_"
        f"{opportunity['sell_exchange']}"
    )

    current_time = time.time()

    with notification_lock:

        last_sent = (
            last_sent_notifications.get(
                notification_key,
                0,
            )
        )

        if (
            current_time
            - last_sent
            < NOTIFICATION_COOLDOWN
        ):

            return False

    opportunity_id = str(
        uuid.uuid4()
    )[:8]

    with pending_lock:

        pending_opportunities[
            opportunity_id
        ] = {

            "id":
                opportunity_id,

            "symbol":
                opportunity["symbol"],

            "buy_exchange":
                opportunity["buy_exchange"],

            "sell_exchange":
                opportunity["sell_exchange"],

            "created_at":
                current_time,
        }

    text = f"""
🚨 <b>АРБИТРАЖНАЯ ВОЗМОЖНОСТЬ</b>

🪙 <b>{opportunity['symbol']}</b>

⭐ Качество:
<b>{opportunity['quality_score']}/100</b>

{opportunity['quality_label']}

━━━━━━━━━━━━━━━━━━

🟢 <b>КУПИТЬ</b>

🏦 <b>{opportunity['buy_exchange_name']}</b>

Средняя цена:
<b>${opportunity['buy_price']}</b>

Ликвидность:
<b>${opportunity['buy_exchange_liquidity']:,.2f}</b>

Проскальзывание:
<b>{opportunity['buy_slippage_percent']}%</b>

━━━━━━━━━━━━━━━━━━

🔴 <b>ПРОДАТЬ</b>

🏦 <b>{opportunity['sell_exchange_name']}</b>

Средняя цена:
<b>${opportunity['sell_price']}</b>

Ликвидность:
<b>${opportunity['sell_exchange_liquidity']:,.2f}</b>

Проскальзывание:
<b>{opportunity['sell_slippage_percent']}%</b>

━━━━━━━━━━━━━━━━━━

📊 Валовый спред:
<b>+{opportunity['gross_spread_percent']}%</b>

💰 Фактическая стоимость покупки:
<b>${opportunity['actual_buy_cost']:,.2f}</b>

💸 Комиссия покупки:
<b>${opportunity['buy_fee_usd']:,.2f}</b>

💸 Комиссия продажи:
<b>${opportunity['sell_fee_usd']:,.2f}</b>

📈 <b>ЧИСТАЯ ПРИБЫЛЬ:</b>
<b>+{opportunity['net_profit_percent']}%</b>

💵 Ожидаемая прибыль:
<b>+${opportunity['net_profit_usd']:,.2f}</b>

━━━━━━━━━━━━━━━━━━

🔄 После нажатия «ДА» бот
получит новые стаканы только
с этих двух бирж и пересчитает
всю сделку заново.

🧪 <b>ТЕСТОВЫЙ РЕЖИМ</b>
Реальные ордера не выставляются.
"""

    reply_markup = {

        "inline_keyboard": [
            [

                {
                    "text":
                        "🔄 ДА — ПРОВЕРИТЬ",

                    "callback_data":
                        f"yes:{opportunity_id}",
                },

                {
                    "text":
                        "❌ НЕТ",

                    "callback_data":
                        f"no:{opportunity_id}",
                },
            ]
        ]
    }

    result = send_telegram_message(
        text,
        reply_markup,
    )

    if result and result.get("ok"):

        with notification_lock:

            last_sent_notifications[
                notification_key
            ] = current_time

        print(
            f"📨 Telegram: "
            f"{opportunity['symbol']} "
            f"{opportunity['buy_exchange']} → "
            f"{opportunity['sell_exchange']} "
            f"+{opportunity['net_profit_percent']}%"
        )

        return True

    with pending_lock:

        pending_opportunities.pop(
            opportunity_id,
            None,
        )

    return False


# ============================================================
# ПОВТОРНАЯ ПРОВЕРКА
# ============================================================

def recheck_opportunity(
    opportunity_id
):

    with pending_lock:

        opportunity = (
            pending_opportunities.get(
                opportunity_id
            )
        )

        if not opportunity:

            return (
                None,
                "Возможность уже устарела.",
            )

        if (
            time.time()
            - opportunity["created_at"]
            > OPPORTUNITY_TTL
        ):

            pending_opportunities.pop(
                opportunity_id,
                None,
            )

            return (
                None,
                "Возможность устарела.",
            )

        symbol = opportunity["symbol"]
        buy_exchange = opportunity["buy_exchange"]
        sell_exchange = opportunity["sell_exchange"]

    buy_future = NETWORK_EXECUTOR.submit(
        get_order_book,
        buy_exchange,
        symbol,
    )

    sell_future = NETWORK_EXECUTOR.submit(
        get_order_book,
        sell_exchange,
        symbol,
    )

    try:

        buy_order_book = (
            buy_future.result(
                timeout=NETWORK_TASK_TIMEOUT
            )
        )

        sell_order_book = (
            sell_future.result(
                timeout=NETWORK_TASK_TIMEOUT
            )
        )

    except Exception as e:

        return (
            None,
            f"Ошибка повторной проверки: {e}",
        )

    if not buy_order_book:

        return (
            None,
            f"Не удалось получить "
            f"стакан {buy_exchange}.",
        )

    if not sell_order_book:

        return (
            None,
            f"Не удалось получить "
            f"стакан {sell_exchange}.",
        )

    diagnostics = create_diagnostics(
        symbol
    )

    current_opportunity = (
        calculate_order_book_opportunity(
            symbol=symbol,
            buy_exchange=buy_exchange,
            buy_order_book=buy_order_book,
            sell_exchange=sell_exchange,
            sell_order_book=sell_order_book,
            diagnostics=diagnostics,
        )
    )

    if not current_opportunity:

        return (
            None,
            "После повторной проверки "
            "возможность больше не соответствует "
            "требованиям по ликвидности, "
            "исполнению, проскальзыванию "
            "или прибыли.",
        )

    return (
        current_opportunity,
        None,
    )


# ============================================================
# ОБРАБОТКА TELEGRAM КНОПОК
# ============================================================

def handle_callback(
    callback_query
):

    callback_id = callback_query.get(
        "id"
    )

    callback_data = callback_query.get(
        "data",
        ""
    )

    message = callback_query.get(
        "message",
        {}
    )

    chat_id = str(
        message.get(
            "chat",
            {}
        ).get(
            "id",
            ""
        )
    )

    if chat_id != TELEGRAM_CHAT_ID:

        answer_callback_query(
            callback_id,
            "Нет доступа.",
        )

        return

    if ":" not in callback_data:

        answer_callback_query(
            callback_id,
            "Неизвестная команда.",
        )

        return

    action, opportunity_id = (
        callback_data.split(
            ":",
            1,
        )
    )

    if action == "no":

        with pending_lock:

            pending_opportunities.pop(
                opportunity_id,
                None,
            )

        answer_callback_query(
            callback_id,
            "Сделка отклонена.",
        )

        return

    if action == "yes":

        answer_callback_query(
            callback_id,
            "Повторно проверяю...",
        )

        current, error = (
            recheck_opportunity(
                opportunity_id
            )
        )

        with pending_lock:

            pending_opportunities.pop(
                opportunity_id,
                None,
            )

        if error:

            send_telegram_message(
                f"""
⚠️ <b>ВОЗМОЖНОСТЬ НЕ ПОДТВЕРЖДЕНА</b>

{error}

🧪 Реальные ордера
<b>НЕ выставлялись.</b>
"""
            )

            return

        send_telegram_message(
            f"""
✅ <b>ВОЗМОЖНОСТЬ ПОДТВЕРЖДЕНА</b>

🪙 <b>{current['symbol']}</b>

⭐ Качество:
<b>{current['quality_score']}/100</b>

{current['quality_label']}

━━━━━━━━━━━━━━━━━━

🟢 Купить:
<b>{current['buy_exchange_name']}</b>

Средняя цена:
<b>${current['buy_price']}</b>

🔴 Продать:
<b>{current['sell_exchange_name']}</b>

Средняя цена:
<b>${current['sell_price']}</b>

━━━━━━━━━━━━━━━━━━

📈 <b>АКТУАЛЬНАЯ ЧИСТАЯ ПРИБЫЛЬ:</b>
<b>+{current['net_profit_percent']}%</b>

💵 Ожидаемая прибыль:
<b>+${current['net_profit_usd']:,.2f}</b>

━━━━━━━━━━━━━━━━━━

🧪 <b>ТЕСТОВЫЙ РЕЖИМ</b>

Реальные ордера
<b>НЕ выставляются.</b>
"""
        )


# ============================================================
# TELEGRAM LONG POLLING
# ============================================================

def telegram_polling():

    print(
        "🤖 Telegram polling запущен."
    )

    offset = None

    while True:

        try:

            request_data = {
                "timeout":
                    TELEGRAM_LONG_POLL_TIMEOUT,
            }

            if offset is not None:

                request_data[
                    "offset"
                ] = offset

            result = telegram_api(
                "getUpdates",
                request_data,
                request_method="GET",
            )

            if not result:

                time.sleep(
                    TELEGRAM_POLL_INTERVAL
                )

                continue

            if not result.get("ok"):

                time.sleep(5)

                continue

            for update in result.get(
                "result",
                []
            ):

                offset = (
                    update["update_id"]
                    + 1
                )

                callback_query = (
                    update.get(
                        "callback_query"
                    )
                )

                if callback_query:

                    handle_callback(
                        callback_query
                    )

        except Exception as e:

            print(
                f"⚠️ Telegram polling: {e}"
            )

            time.sleep(5)


# ============================================================
# ФОНОВАЯ ОЧИСТКА
# ============================================================

def cleanup_loop():

    while True:

        try:

            cleanup_old_data()

        except Exception as e:

            print(
                f"⚠️ Ошибка очистки: {e}"
            )

        time.sleep(
            CLEANUP_INTERVAL
        )


# ============================================================
# ДИАГНОСТИКА В TELEGRAM
# ============================================================

def send_scan_diagnostics_to_telegram(
    diagnostics
):

    text = f"""
📊 <b>ДИАГНОСТИКА СКАНА</b>

📚 Запросов стаканов:
<b>{diagnostics['order_book_requests']}</b>

✅ Получено стаканов:
<b>{diagnostics['order_books_received']}</b>

🔗 Проверено направлений:
<b>{diagnostics['exchange_pairs_total']}</b>

🔎 Направлений с положительным спредом:
<b>{diagnostics['exchange_pairs_price_possible']}</b>

🚫 Без положительного ценового направления:
<b>{diagnostics['rejected_fast_precheck']}</b>

🧮 Полных расчётов:
<b>{diagnostics['full_calculations']}</b>

💧 Ликвидность покупки:
<b>{diagnostics['rejected_buy_liquidity']}</b>

💧 Ликвидность продажи:
<b>{diagnostics['rejected_sell_liquidity']}</b>

📉 Проскальзывание:
<b>
{diagnostics['rejected_buy_slippage']
+ diagnostics['rejected_sell_slippage']}
</b>

📈 Недостаточная прибыль:
<b>{diagnostics['rejected_profit']}</b>

🎯 Итоговых возможностей:
<b>{diagnostics['final_opportunities']}</b>
"""

    send_telegram_message(
        text
    )


# ============================================================
# ГЛАВНЫЙ ЦИКЛ СКАНЕРА
# ============================================================

def scanner_loop():

    global last_opportunities
    global last_scan_time
    global last_scan_diagnostics
    global total_scans
    global total_opportunities_found

    print(
        "🔎 Арбитражный сканер запущен."
    )

    while True:

        scan_started = time.time()

        try:

            print("")
            print("=" * 55)

            print(
                f"🔄 СКАНИРОВАНИЕ "
                f"{datetime.now().strftime('%H:%M:%S')}"
            )

            print(
                f"🪙 Монет: {len(SYMBOLS)}"
            )

            print(
                f"🏦 Бирж: {len(exchanges)}"
            )

            print(
                "⚡ Используются два независимых "
                "пула потоков"
            )

            (
                opportunities,
                diagnostics,
            ) = scan_all()

            with lock:

                last_opportunities = opportunities

                last_scan_time = (
                    datetime.now()
                )

                last_scan_diagnostics = (
                    diagnostics
                )

                total_scans += 1

                total_opportunities_found += (
                    len(opportunities)
                )

            scan_duration = (
                time.time()
                - scan_started
            )

            print(
                f"📊 Возможностей: "
                f"{len(opportunities)}"
            )

            print(
                f"⏱ Длительность: "
                f"{scan_duration:.2f} сек."
            )

            print_diagnostics(
                diagnostics
            )

            sent_count = 0

            for opportunity in opportunities:

                if (
                    sent_count
                    >= MAX_NOTIFICATIONS_PER_SCAN
                ):

                    break

                if send_opportunity_to_telegram(
                    opportunity
                ):

                    sent_count += 1

            print(
                f"📨 Отправлено: "
                f"{sent_count}"
            )

            if TELEGRAM_DIAGNOSTICS:

                send_scan_diagnostics_to_telegram(
                    diagnostics
                )

            elif (
                TELEGRAM_ZERO_OPPORTUNITIES_ALERT
                and not opportunities
            ):

                send_scan_diagnostics_to_telegram(
                    diagnostics
                )

            print("=" * 55)

        except Exception as e:

            print(
                f"❌ Критическая ошибка "
                f"сканера: {e}"
            )

        elapsed = (
            time.time()
            - scan_started
        )

        sleep_time = max(
            1,
            SCAN_INTERVAL
            - elapsed,
        )

        time.sleep(
            sleep_time
        )


# ============================================================
# ЗАПУСК ФОНОВЫХ СЕРВИСОВ
# ============================================================

def start_background_services():

    global scanner_started
    global telegram_started
    global services_started

    with startup_lock:

        if services_started:

            return

        services_started = True

        print("")
        print("=" * 55)
        print("🚀 ЗАПУСК АРБИТРАЖНОЙ СИСТЕМЫ")
        print("=" * 55)

        print(
            f"💰 Бюджет: "
            f"${TRADE_AMOUNT_USD:,.2f}"
        )

        print(
            f"📈 Мин. прибыль: "
            f"{MIN_NET_PROFIT_PERCENT}%"
        )

        print(
            f"🪙 Монет: "
            f"{len(SYMBOLS)}"
        )

        print(
            f"🏦 Бирж: "
            f"{len(exchanges)}"
        )

        print(
            f"⚡ Scan workers: "
            f"{SCAN_WORKERS}"
        )

        print(
            f"🌐 Network workers: "
            f"{NETWORK_WORKERS}"
        )

        load_all_markets()

        telegram_ok = (
            check_telegram_connection()
        )

        if telegram_ok:

            telegram_thread = (
                threading.Thread(
                    target=telegram_polling,
                    daemon=True,
                    name="telegram-polling",
                )
            )

            telegram_thread.start()

            telegram_started = True

        else:

            print(
                "⚠️ Telegram polling "
                "не запущен."
            )

        cleanup_thread = (
            threading.Thread(
                target=cleanup_loop,
                daemon=True,
                name="cleanup-service",
            )
        )

        cleanup_thread.start()

        scanner_thread = (
            threading.Thread(
                target=scanner_loop,
                daemon=True,
                name="arbitrage-scanner",
            )
        )

        scanner_thread.start()

        scanner_started = True

        print(
            "✅ Все сервисы запущены."
        )

        print("=" * 55)


# ============================================================
# WEB UI
# ============================================================

HTML = """
<!DOCTYPE html>
<html lang="ru">

<head>

<meta charset="UTF-8">

<meta
name="viewport"
content="width=device-width, initial-scale=1.0"
>

<title>Arbitrage Scanner</title>

<style>

body {
    margin: 0;
    padding: 20px;
    background: #0f1623;
    color: #e8edf5;
    font-family: Arial, sans-serif;
}

.container {
    max-width: 1100px;
    margin: auto;
}

.status {
    display: inline-block;
    background: #124d3d;
    padding: 18px 28px;
    border-radius: 40px;
    font-size: 20px;
    margin-bottom: 20px;
}

.card,
.opportunity {
    background: #1d2939;
    border: 1px solid #31445f;
    border-radius: 25px;
    padding: 25px;
    margin-bottom: 20px;
}

.stats-grid {
    display: grid;
    grid-template-columns:
        repeat(
            auto-fit,
            minmax(170px, 1fr)
        );
    gap: 15px;
    margin-bottom: 20px;
}

.stat-box {
    background: #1d2939;
    border: 1px solid #31445f;
    border-radius: 20px;
    padding: 20px;
}

.stat-number {
    font-size: 30px;
    font-weight: bold;
}

.stat-label {
    margin-top: 8px;
    color: #b8c2d2;
}

.symbol {
    font-size: 30px;
    font-weight: bold;
    margin-bottom: 15px;
}

.profit {
    padding: 20px;
    border-radius: 20px;
    background: #27395b;
    margin-bottom: 15px;
}

.buy,
.sell {
    padding: 20px;
    border-radius: 20px;
    margin-top: 12px;
    background: #223047;
}

.exchange {
    font-size: 24px;
    font-weight: bold;
    margin: 10px 0;
}

.small-info {
    margin-top: 8px;
    color: #b8c2d2;
}

.empty {
    text-align: center;
    padding: 40px;
}

table {
    width: 100%;
    border-collapse: collapse;
}

td,
th {
    padding: 12px;
    border-bottom: 1px solid #31445f;
    text-align: left;
}

</style>

</head>

<body>

<div class="container">

<div class="status">
🟢 Арбитражный сканер активен
</div>


<div class="stats-grid">

<div class="stat-box">
<div class="stat-number">
{{ total_coins }}
</div>
<div class="stat-label">
🪙 Монеты
</div>
</div>

<div class="stat-box">
<div class="stat-number">
{{ exchanges_count }}
</div>
<div class="stat-label">
🏦 Биржи
</div>
</div>

<div class="stat-box">
<div class="stat-number">
{{ opportunities|length }}
</div>
<div class="stat-label">
📈 Возможности
</div>
</div>

<div class="stat-box">
<div class="stat-number">
{{ total_scans }}
</div>
<div class="stat-label">
🔄 Всего сканов
</div>
</div>

</div>


<div class="card">

💰 Максимальный бюджет:
<b>${{ "%.2f"|format(trade_amount) }}</b>

<br><br>

📈 Минимальная чистая прибыль:
<b>{{ min_profit }}%</b>

<br><br>

⚡ Отдельный пул сканирования:
<b>ВКЛЮЧЕН</b>

<br><br>

🌐 Отдельный сетевой пул:
<b>ВКЛЮЧЕН</b>

<br><br>

📊 Диагностика отбрасывания:
<b>ВКЛЮЧЕНА</b>

</div>


{% if diagnostics %}

<div class="card">

<h2>📊 Последняя диагностика</h2>

<table>

<tr>
<th>Показатель</th>
<th>Количество</th>
</tr>

<tr>
<td>Получено стаканов</td>
<td>{{ diagnostics.order_books_received }}</td>
</tr>

<tr>
<td>Всего направлений</td>
<td>{{ diagnostics.exchange_pairs_total }}</td>
</tr>

<tr>
<td>Направлений с положительным спредом</td>
<td>{{ diagnostics.exchange_pairs_price_possible }}</td>
</tr>

<tr>
<td>Нет положительного ценового направления</td>
<td>{{ diagnostics.rejected_fast_precheck }}</td>
</tr>

<tr>
<td>Полных расчётов выполнено</td>
<td>{{ diagnostics.full_calculations }}</td>
</tr>

<tr>
<td>Недостаточная ликвидность покупки</td>
<td>{{ diagnostics.rejected_buy_liquidity }}</td>
</tr>

<tr>
<td>Недостаточная ликвидность продажи</td>
<td>{{ diagnostics.rejected_sell_liquidity }}</td>
</tr>

<tr>
<td>Недостаточная прибыль</td>
<td>{{ diagnostics.rejected_profit }}</td>
</tr>

<tr>
<td><b>Итоговых возможностей</b></td>
<td><b>{{ diagnostics.final_opportunities }}</b></td>
</tr>

</table>

</div>

{% endif %}


{% if current_symbols %}

<div class="card">

🔍 Сейчас сканируются:

<br><br>

<b>
{{ current_symbols|join(", ") }}
</b>

</div>

{% endif %}


{% if opportunities %}

{% for op in opportunities %}

<div class="opportunity">

<div class="symbol">
🪙 {{ op.symbol }}
</div>

<div class="profit">

⭐ Качество:
<b>{{ op.quality_score }}/100</b>

<br><br>

<b>{{ op.quality_label }}</b>

<br><br>

📈 Чистая прибыль:
<b>+{{ op.net_profit_percent }}%</b>

<br><br>

💵 Прибыль:
<b>+${{ op.net_profit_usd }}</b>

</div>


<div class="buy">

🟢 КУПИТЬ

<div class="exchange">
{{ op.buy_exchange_name }}
</div>

Средняя цена:
<b>${{ op.buy_price }}</b>

<div class="small-info">
Ликвидность:
${{ op.buy_exchange_liquidity }}
</div>

<div class="small-info">
Проскальзывание:
{{ op.buy_slippage_percent }}%
</div>

</div>


<div class="sell">

🔴 ПРОДАТЬ

<div class="exchange">
{{ op.sell_exchange_name }}
</div>

Средняя цена:
<b>${{ op.sell_price }}</b>

<div class="small-info">
Ликвидность:
${{ op.sell_exchange_liquidity }}
</div>

<div class="small-info">
Проскальзывание:
{{ op.sell_slippage_percent }}%
</div>

</div>

</div>

{% endfor %}

{% else %}

<div class="card empty">

🔎 Подходящих возможностей пока нет.

<br><br>

Смотри блок «Последняя диагностика» —
теперь там видно, на каком именно этапе
отбрасываются направления.

</div>

{% endif %}

</div>


<script>

setTimeout(
    () => {
        location.reload();
    },
    5000
);

</script>

</body>
</html>
"""


# ============================================================
# ГЛАВНАЯ СТРАНИЦА
# ============================================================

@app.route("/")
def index():

    start_background_services()

    with lock:

        opportunities = list(
            last_opportunities
        )

        scans = total_scans

        current = list(
            current_symbols
        )

        diagnostics = dict(
            last_scan_diagnostics
        )

    return render_template_string(

        HTML,

        opportunities=opportunities,

        trade_amount=
            TRADE_AMOUNT_USD,

        min_profit=
            MIN_NET_PROFIT_PERCENT,

        exchanges_count=
            len(exchanges),

        total_coins=
            len(SYMBOLS),

        total_scans=
            scans,

        current_symbols=
            current,

        diagnostics=
            diagnostics,
    )


# ============================================================
# API /scan
# ============================================================

@app.route("/scan")
def scan_api():

    start_background_services()

    with lock:

        opportunities = list(
            last_opportunities
        )

        current = list(
            current_symbols
        )

        scan_time = last_scan_time

        scans = total_scans

        found = (
            total_opportunities_found
        )

        diagnostics = dict(
            last_scan_diagnostics
        )

    with stats_lock:

        order_book_requests = (
            total_order_book_requests
        )

        successful_order_books = (
            total_successful_order_books
        )

        failed_order_books = (
            total_failed_order_books
        )

    return jsonify({

        "status": "success",

        "test_mode": True,

        "scanner_active":
            scanner_started,

        "telegram_active":
            telegram_started,

        "trade_amount_usd":
            TRADE_AMOUNT_USD,

        "min_net_profit_percent":
            MIN_NET_PROFIT_PERCENT,

        "total_coins":
            len(SYMBOLS),

        "symbols":
            SYMBOLS,

        "exchanges_count":
            len(exchanges),

        "exchanges":
            list(exchanges.keys()),

        "currently_scanning":
            current,

        "total_scans":
            scans,

        "total_opportunities_found":
            found,

        "order_book_requests":
            order_book_requests,

        "successful_order_books":
            successful_order_books,

        "failed_order_books":
            failed_order_books,

        "last_scan": (
            scan_time.isoformat()
            if scan_time
            else None
        ),

        "diagnostics":
            diagnostics,

        "opportunities":
            opportunities,
    })


# ============================================================
# HEALTH
# ============================================================

@app.route("/health")
def health():

    start_background_services()

    return jsonify({

        "status": "ok",

        "test_mode": True,

        "scanner":
            scanner_started,

        "telegram":
            telegram_started,

        "trade_amount":
            TRADE_AMOUNT_USD,

        "min_profit":
            MIN_NET_PROFIT_PERCENT,

        "symbols":
            SYMBOLS,

        "exchanges":
            list(exchanges.keys()),

        "scan_workers":
            SCAN_WORKERS,

        "network_workers":
            NETWORK_WORKERS,

        "separate_thread_pools":
            True,

        "diagnostics":
            True,
    })


# ============================================================
# ЗАПУСК
# ============================================================

if __name__ == "__main__":

    start_background_services()

    port = int(
        os.getenv(
            "PORT",
            5000,
        )
    )

    try:

        app.run(
            host="0.0.0.0",
            port=port,
            debug=False,
            use_reloader=False,
            threaded=True,
        )

    finally:

        SCAN_EXECUTOR.shutdown(
            wait=False,
            cancel_futures=True,
        )

        NETWORK_EXECUTOR.shutdown(
            wait=False,
            cancel_futures=True,
        )