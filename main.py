import os
import time
import uuid
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

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

# Фактический бюджет на одну сделку.
# Это сумма, которую бот готов использовать на ПОКУПКУ
# вместе с запасом на комиссию.
TRADE_AMOUNT_USD = 1000.0


# ============================================================
# ПРИБЫЛЬ И КОМИССИИ
# ============================================================

# Минимальная чистая прибыль после комиссий и исполнения
MIN_NET_PROFIT_PERCENT = 0.50

# Дополнительный запас на непредвиденные расходы:
# изменение комиссии, небольшое ухудшение цены и т.д.
EXTRA_COST_BUFFER_PERCENT = 0.10

# Комиссии считаются как taker-комиссии.
# При необходимости их можно изменить под свой аккаунт.
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

# Интервал между полными сканированиями
SCAN_INTERVAL = 10

# Сколько возможностей максимум отправлять за один скан
MAX_NOTIFICATIONS_PER_SCAN = 3

# Не отправлять одну и ту же связку слишком часто
NOTIFICATION_COOLDOWN = 300

# Возможность живёт 5 минут
OPPORTUNITY_TTL = 300

# Как часто чистить устаревшие данные
CLEANUP_INTERVAL = 60


# ============================================================
# СВЕЖЕСТЬ СТАКАНА
# ============================================================

# Максимальный возраст стакана в секундах.
# Если биржа передала timestamp и стакан старше этого
# значения, возможность игнорируется.
MAX_ORDER_BOOK_AGE_SECONDS = 5

# Если timestamp отсутствует, данные всё равно могут быть
# использованы, но помечаются как менее надёжные.
ALLOW_ORDER_BOOK_WITHOUT_TIMESTAMP = True


# ============================================================
# ЛИКВИДНОСТЬ И ИСПОЛНЕНИЕ
# ============================================================

# Сколько уровней стакана запрашивать
ORDER_BOOK_LIMIT = 50

# Минимальный запас ликвидности относительно размера сделки
LIQUIDITY_SAFETY_MULTIPLIER = 1.20

# Максимально допустимое проскальзывание на покупке
MAX_BUY_SLIPPAGE_PERCENT = 0.30

# Максимально допустимое проскальзывание на продаже
MAX_SELL_SLIPPAGE_PERCENT = 0.30

# Максимальный процент отклонения цены от лучшей цены,
# в пределах которого считаем ликвидность.
MAX_EXECUTION_PRICE_DEVIATION_PERCENT = 0.50


# ============================================================
# ПАРАЛЛЕЛЬНОСТЬ
# ============================================================

# Один постоянный общий пул потоков.
# Он создаётся только один раз.
MAX_WORKERS = 36

# Одновременно сканируем несколько монет
MAX_PARALLEL_SYMBOLS = 4


# ============================================================
# TELEGRAM
# ============================================================

TELEGRAM_POLL_INTERVAL = 2
TELEGRAM_LONG_POLL_TIMEOUT = 20


# ============================================================
# 4 САМЫЕ ЛИКВИДНЫЕ МОНЕТЫ
# ============================================================

SYMBOLS = [
    "BTC/USDT",
    "ETH/USDT",
    "SOL/USDT",
    "XRP/USDT",
]


# ============================================================
# БИРЖИ — ВСЕГО 9
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
# ПОСТОЯННЫЙ ПУЛ ПОТОКОВ
# ============================================================

executor = ThreadPoolExecutor(
    max_workers=MAX_WORKERS,
    thread_name_prefix="arbitrage-worker",
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
            f"❌ Ошибка создания {exchange_id}: {e}"
        )


# ============================================================
# СОСТОЯНИЕ СИСТЕМЫ
# ============================================================

last_opportunities = []
last_scan_time = None

last_sent_notifications = {}
pending_opportunities = {}

scanner_started = False
telegram_started = False
services_started = False

current_symbols = []
current_scan_total = len(SYMBOLS)

total_scans = 0
total_opportunities_found = 0

total_order_book_requests = 0
total_successful_order_books = 0

lock = threading.RLock()
notification_lock = threading.RLock()
pending_lock = threading.RLock()
startup_lock = threading.Lock()
stats_lock = threading.Lock()


# ============================================================
# ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ БЕЗОПАСНОГО ЧИСЛА
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


# ============================================================
# ЗАГРУЗКА MARKETS ОДИН РАЗ
# ============================================================

def load_all_markets():

    global available_symbols_by_exchange

    print("")
    print(
        "=========================================="
    )
    print(
        "📚 ЗАГРУЗКА MARKETS"
    )
    print(
        "=========================================="
    )

    futures = {}

    for exchange_id, exchange in exchanges.items():

        futures[
            executor.submit(
                exchange.load_markets
            )
        ] = exchange_id

    for future in as_completed(futures):

        exchange_id = futures[future]

        try:

            markets = future.result()

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
                f"монет доступно"
            )

        except Exception as e:

            available_symbols_by_exchange[
                exchange_id
            ] = set()

            print(
                f"❌ load_markets {exchange_id}: {e}"
            )

    print(
        "=========================================="
    )
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
                f"❌ Telegram {method}: {result}"
            )

        return result

    except Exception as e:

        print(
            f"❌ Telegram API {method}: {e}"
        )

        return None


# ============================================================
# ОТПРАВКА TELEGRAM
# ============================================================

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
# ПРОВЕРКА TELEGRAM
# ============================================================

def check_telegram_connection():

    print("")
    print(
        "=========================================="
    )
    print(
        "🤖 ПРОВЕРКА TELEGRAM"
    )
    print(
        "=========================================="
    )

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

💰 Фактический бюджет сделки:
<b>${TRADE_AMOUNT_USD:,.2f}</b>

📈 Минимальная чистая прибыль:
<b>{MIN_NET_PROFIT_PERCENT}%</b>

🪙 Монет:
<b>{len(SYMBOLS)}</b>

🏦 Бирж:
<b>{len(exchanges)}</b>

⚡ Постоянный пул потоков:
<b>ВКЛЮЧЕН</b>

📚 Markets загружаются:
<b>ОДИН РАЗ ПРИ СТАРТЕ</b>

📖 Проверка стакана:
<b>ВКЛЮЧЕНА</b>

💧 Проверка ликвидности
в зоне исполнения:
<b>ВКЛЮЧЕНА</b>

🕒 Проверка свежести стакана:
<b>ВКЛЮЧЕНА</b>

🔁 Двойная проверка Telegram:
<b>ВКЛЮЧЕНА</b>

⭐ Рейтинг качества возможностей:
<b>ВКЛЮЧЕН</b>

🧪 <b>ТЕСТОВЫЙ РЕЖИМ</b>
Реальные ордера не выставляются.
"""

    result = send_telegram_message(
        startup_text
    )

    if result and result.get("ok"):

        print(
            "✅ Telegram подключён"
        )

        print(
            f"🤖 @{bot_name}"
        )

        print(
            "=========================================="
        )

        return True

    print(
        "❌ Не удалось отправить стартовое сообщение"
    )

    return False


# ============================================================
# ОЧИСТКА СТАКАНА
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

    request_started_at = time.time()

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

        is_fresh = True

        if timestamp_age is not None:

            if (
                timestamp_age
                > MAX_ORDER_BOOK_AGE_SECONDS
            ):

                is_fresh = False

        elif not ALLOW_ORDER_BOOK_WITHOUT_TIMESTAMP:

            is_fresh = False

        if not is_fresh:

            return None

        with stats_lock:

            total_successful_order_books += 1

        return {

            "asks": asks,

            "bids": bids,

            "best_ask":
                asks[0][0],

            "best_bid":
                bids[0][0],

            "exchange_timestamp":
                exchange_timestamp,

            "timestamp_age":
                timestamp_age,

            "received_at":
                received_at,

            "request_duration":
                received_at
                - request_started_at,
        }

    except Exception as e:

        print(
            f"⚠️ Стакан {exchange_id} "
            f"{symbol}: {e}"
        )

        return None


# ============================================================
# ПАРАЛЛЕЛЬНОЕ ПОЛУЧЕНИЕ СТАКАНОВ
# ============================================================

def get_all_order_books_parallel(
    symbol
):

    results = {}

    futures = {}

    for exchange_id in exchanges:

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

            continue

        future = executor.submit(
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

        except Exception as e:

            print(
                f"⚠️ Параллельная ошибка "
                f"{exchange_id} {symbol}: {e}"
            )

    return results


# ============================================================
# РАСЧЁТ МАКСИМАЛЬНОЙ СУММЫ ПОКУПКИ
# ============================================================

def calculate_buy_budget():

    # Бюджет включает возможную комиссию.
    # Например, если бюджет $1000, нельзя купить
    # ровно на $1000 и потом сверху добавить комиссию.
    # Поэтому заранее оставляем место для комиссии
    # и дополнительного буфера.

    highest_fee = max(
        EXCHANGE_FEES.values()
    )

    reserve_percent = (
        highest_fee
        + EXTRA_COST_BUFFER_PERCENT
    )

    available_for_asset = (
        TRADE_AMOUNT_USD
        / (
            1
            + reserve_percent / 100
        )
    )

    return available_for_asset


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

        if (
            price <= 0
            or available_quantity <= 0
        ):

            continue

        # Не используем слишком далёкие уровни
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
            spend
            / price
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

        "spent":
            total_spent,

        "quantity":
            total_quantity,

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

        if (
            price <= 0
            or available_quantity <= 0
        ):

            continue

        # Не продаём по слишком плохой цене
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

        total_sold += (
            sell_quantity
        )

        remaining_quantity -= (
            sell_quantity
        )

    if remaining_quantity > 0.00000001:

        return None

    if total_sold <= 0:

        return None

    average_price = (
        total_revenue
        / total_sold
    )

    return {

        "revenue":
            total_revenue,

        "quantity":
            total_sold,

        "average_price":
            average_price,

        "min_price":
            min_price,
    }


# ============================================================
# ЛИКВИДНОСТЬ В ЗОНЕ ИСПОЛНЕНИЯ
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
            price
            * amount
        )

    return liquidity


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
            price
            * amount
        )

    return liquidity


# ============================================================
# БЫСТРЫЙ ПРЕДВАРИТЕЛЬНЫЙ ФИЛЬТР
# ============================================================

def passes_fast_precheck(
    buy_exchange,
    buy_order_book,
    sell_exchange,
    sell_order_book
):

    buy_ask = buy_order_book[
        "best_ask"
    ]

    sell_bid = sell_order_book[
        "best_bid"
    ]

    if sell_bid <= buy_ask:

        return False

    buy_fee = EXCHANGE_FEES.get(
        buy_exchange,
        0.20,
    )

    sell_fee = EXCHANGE_FEES.get(
        sell_exchange,
        0.20,
    )

    minimum_required_spread = (
        buy_fee
        + sell_fee
        + EXTRA_COST_BUFFER_PERCENT
        + MIN_NET_PROFIT_PERCENT
    )

    gross_spread = (
        (
            sell_bid
            - buy_ask
        )
        / buy_ask
    ) * 100

    return (
        gross_spread
        >= minimum_required_spread
    )


# ============================================================
# РЕЙТИНГ КАЧЕСТВА ВОЗМОЖНОСТИ
# ============================================================

def calculate_opportunity_score(
    net_profit_percent,
    gross_spread_percent,
    buy_slippage_percent,
    sell_slippage_percent,
    buy_liquidity,
    sell_liquidity,
):

    # 0-40 баллов за чистую прибыль
    profit_score = min(
        40.0,
        max(
            0.0,
            (
                net_profit_percent
                / 2.0
            )
            * 40.0,
        ),
    )

    # 0-15 баллов за валовый спред
    spread_score = min(
        15.0,
        max(
            0.0,
            (
                gross_spread_percent
                / 3.0
            )
            * 15.0,
        ),
    )

    # 0-20 баллов за ликвидность
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

    # 0-25 баллов за низкое проскальзывание
    total_slippage = (
        buy_slippage_percent
        + sell_slippage_percent
    )

    slippage_score = max(
        0.0,
        25.0
        - total_slippage * 25.0,
    )

    score = (
        profit_score
        + spread_score
        + liquidity_score
        + slippage_score
    )

    return round(
        min(
            100.0,
            score,
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
# РАСЧЁТ АРБИТРАЖА
# ============================================================

def calculate_order_book_opportunity(
    symbol,
    buy_exchange,
    buy_order_book,
    sell_exchange,
    sell_order_book,
):

    # --------------------------------------------------------
    # БЫСТРАЯ ПРОВЕРКА
    # --------------------------------------------------------

    if not passes_fast_precheck(
        buy_exchange,
        buy_order_book,
        sell_exchange,
        sell_order_book,
    ):

        return None


    # --------------------------------------------------------
    # РЕАЛЬНЫЙ БЮДЖЕТ НА ПОКУПКУ
    # --------------------------------------------------------

    asset_budget = (
        calculate_buy_budget()
    )


    # --------------------------------------------------------
    # ЛИКВИДНОСТЬ ПОКУПКИ В ЗОНЕ ИСПОЛНЕНИЯ
    # --------------------------------------------------------

    required_liquidity = (
        asset_budget
        * LIQUIDITY_SAFETY_MULTIPLIER
    )

    buy_liquidity = (
        calculate_buy_execution_liquidity(
            buy_order_book["asks"]
        )
    )

    sell_liquidity = (
        calculate_sell_execution_liquidity(
            sell_order_book["bids"]
        )
    )

    if buy_liquidity < required_liquidity:

        return None

    if sell_liquidity < required_liquidity:

        return None


    # --------------------------------------------------------
    # ПОКУПКА ПО РЕАЛЬНОМУ СТАКАНУ
    # --------------------------------------------------------

    buy_result = simulate_buy(
        buy_order_book["asks"],
        asset_budget,
    )

    if not buy_result:

        return None


    # --------------------------------------------------------
    # КОМИССИЯ ПОКУПКИ
    # --------------------------------------------------------

    buy_fee_percent = (
        EXCHANGE_FEES.get(
            buy_exchange,
            0.20,
        )
    )

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

    # Фактическая стоимость покупки
    buy_cost = (
        buy_result["spent"]
        + buy_fee_usd
        + buy_extra_buffer_usd
    )

    # Защита: не превышаем заданный бюджет
    if buy_cost > TRADE_AMOUNT_USD:

        return None


    # --------------------------------------------------------
    # ПРОДАЖА ТОГО ЖЕ КОЛИЧЕСТВА
    # --------------------------------------------------------

    sell_result = simulate_sell(
        sell_order_book["bids"],
        buy_result["quantity"],
    )

    if not sell_result:

        return None


    # --------------------------------------------------------
    # КОМИССИЯ ПРОДАЖИ
    # --------------------------------------------------------

    sell_fee_percent = (
        EXCHANGE_FEES.get(
            sell_exchange,
            0.20,
        )
    )

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
    # ФАКТИЧЕСКАЯ ЧИСТАЯ ПРИБЫЛЬ
    # --------------------------------------------------------

    net_profit_usd = (
        sell_revenue
        - buy_cost
    )

    if buy_cost <= 0:

        return None

    # ВАЖНО:
    # Процент считается от фактической стоимости покупки,
    # а не от номинальных $1000.
    net_profit_percent = (
        net_profit_usd
        / buy_cost
    ) * 100


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


    # --------------------------------------------------------
    # ФИЛЬТР ПРОСКАЛЬЗЫВАНИЯ
    # --------------------------------------------------------

    if (
        buy_slippage_percent
        > MAX_BUY_SLIPPAGE_PERCENT
    ):

        return None

    if (
        sell_slippage_percent
        > MAX_SELL_SLIPPAGE_PERCENT
    ):

        return None


    # --------------------------------------------------------
    # РЕЙТИНГ КАЧЕСТВА
    # --------------------------------------------------------

    quality_score = (
        calculate_opportunity_score(
            net_profit_percent,
            gross_spread_percent,
            buy_slippage_percent,
            sell_slippage_percent,
            buy_liquidity,
            sell_liquidity,
        )
    )

    quality_label = (
        get_quality_label(
            quality_score
        )
    )


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
    }


# ============================================================
# СКАНИРОВАНИЕ ОДНОЙ МОНЕТЫ
# ============================================================

def scan_symbol(
    symbol
):

    order_books = (
        get_all_order_books_parallel(
            symbol
        )
    )

    opportunities = []

    exchange_ids = list(
        order_books.keys()
    )

    if len(exchange_ids) < 2:

        return opportunities


    # --------------------------------------------------------
    # СОЗДАЁМ ТОЛЬКО ПЕРСПЕКТИВНЫЕ ПАРЫ
    # --------------------------------------------------------

    candidate_pairs = []

    for buy_exchange in exchange_ids:

        buy_book = order_books[
            buy_exchange
        ]

        for sell_exchange in exchange_ids:

            if buy_exchange == sell_exchange:

                continue

            sell_book = order_books[
                sell_exchange
            ]

            if not passes_fast_precheck(
                buy_exchange,
                buy_book,
                sell_exchange,
                sell_book,
            ):

                continue

            candidate_pairs.append(
                (
                    buy_exchange,
                    sell_exchange,
                )
            )


    # --------------------------------------------------------
    # ПОЛНЫЙ РАСЧЁТ ТОЛЬКО КАНДИДАТОВ
    # --------------------------------------------------------

    for (
        buy_exchange,
        sell_exchange,
    ) in candidate_pairs:

        opportunity = (
            calculate_order_book_opportunity(
                symbol=symbol,
                buy_exchange=buy_exchange,
                buy_order_book=order_books[
                    buy_exchange
                ],
                sell_exchange=sell_exchange,
                sell_order_book=order_books[
                    sell_exchange
                ],
            )
        )

        if not opportunity:

            continue

        if (
            opportunity[
                "net_profit_percent"
            ]
            >= MIN_NET_PROFIT_PERCENT
        ):

            opportunities.append(
                opportunity
            )

    return opportunities


# ============================================================
# СКАНИРОВАНИЕ ОДНОЙ МОНЕТЫ В ПОТОКЕ
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
# ПОЛНОЕ ПАРАЛЛЕЛЬНОЕ СКАНИРОВАНИЕ
# ============================================================

def scan_all():

    global current_scan_total

    all_opportunities = []

    current_scan_total = len(
        SYMBOLS
    )

    # Используем постоянный общий пул.
    # Монеты запускаются параллельно.
    futures = {}

    for symbol in SYMBOLS:

        future = executor.submit(
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

            opportunities = future.result()

            if opportunities:

                all_opportunities.extend(
                    opportunities
                )

        except Exception as e:

            print(
                f"⚠️ Ошибка сканирования "
                f"{symbol}: {e}"
            )

    # Сначала лучшие по рейтингу,
    # затем по чистой прибыли.
    all_opportunities.sort(
        key=lambda x: (
            x["quality_score"],
            x["net_profit_percent"],
        ),
        reverse=True,
    )

    return all_opportunities


# ============================================================
# АВТОМАТИЧЕСКАЯ ОЧИСТКА СТАРЫХ ДАННЫХ
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
# ОТПРАВКА ВОЗМОЖНОСТИ В TELEGRAM
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


    # Сохраняем возможность потокобезопасно
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

⭐ <b>Качество: {opportunity['quality_score']}/100</b>
{opportunity['quality_label']}

━━━━━━━━━━━━━━━━━━

🟢 <b>КУПИТЬ</b>

🏦 <b>{opportunity['buy_exchange_name']}</b>

💵 Средняя цена:
<b>${opportunity['buy_price']}</b>

📚 Ликвидность в зоне исполнения:
<b>${opportunity['buy_exchange_liquidity']:,.2f}</b>

📉 Проскальзывание:
<b>{opportunity['buy_slippage_percent']}%</b>

━━━━━━━━━━━━━━━━━━

🔴 <b>ПРОДАТЬ</b>

🏦 <b>{opportunity['sell_exchange_name']}</b>

💵 Средняя цена:
<b>${opportunity['sell_price']}</b>

📚 Ликвидность в зоне исполнения:
<b>${opportunity['sell_exchange_liquidity']:,.2f}</b>

📉 Проскальзывание:
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

🔁 <b>ДВОЙНАЯ ПРОВЕРКА ВКЛЮЧЕНА</b>

После нажатия «ДА» бот повторно
получит свежие стаканы с обеих бирж,
заново рассчитает ликвидность,
исполнение, комиссии и прибыль.

🧪 <b>ТЕСТОВЫЙ РЕЖИМ</b>
Реальные ордера не выставляются.
"""

    reply_markup = {

        "inline_keyboard": [
            [

                {
                    "text":
                        "🔄 ДА — ПОВТОРНО ПРОВЕРИТЬ",

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
            f"+{opportunity['net_profit_percent']}% "
            f"Score {opportunity['quality_score']}"
        )

        return True


    with pending_lock:

        pending_opportunities.pop(
            opportunity_id,
            None,
        )

    return False


# ============================================================
# ПОВТОРНАЯ ПРОВЕРКА В TELEGRAM
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

        buy_exchange = (
            opportunity["buy_exchange"]
        )

        sell_exchange = (
            opportunity["sell_exchange"]
        )


    # ВАЖНО:
    # Не создаём новый ThreadPoolExecutor.
    # Используем постоянный общий пул.
    buy_future = executor.submit(
        get_order_book,
        buy_exchange,
        symbol,
    )

    sell_future = executor.submit(
        get_order_book,
        sell_exchange,
        symbol,
    )

    try:

        buy_order_book = (
            buy_future.result()
        )

        sell_order_book = (
            sell_future.result()
        )

    except Exception as e:

        return (
            None,
            f"Ошибка повторной проверки: {e}",
        )

    if not buy_order_book:

        return (
            None,
            f"Не удалось получить свежий "
            f"стакан {buy_exchange}.",
        )

    if not sell_order_book:

        return (
            None,
            f"Не удалось получить свежий "
            f"стакан {sell_exchange}.",
        )


    current_opportunity = (
        calculate_order_book_opportunity(
            symbol=symbol,
            buy_exchange=buy_exchange,
            buy_order_book=buy_order_book,
            sell_exchange=sell_exchange,
            sell_order_book=sell_order_book,
        )
    )

    if not current_opportunity:

        return (
            None,
            "После повторной проверки "
            "возможность больше не соответствует "
            "требованиям по прибыли, ликвидности "
            "или проскальзыванию.",
        )

    if (
        current_opportunity[
            "net_profit_percent"
        ]
        < MIN_NET_PROFIT_PERCENT
    ):

        return (
            None,
            f"После повторной проверки "
            f"чистая прибыль "
            f"{current_opportunity['net_profit_percent']}%, "
            f"что ниже требуемых "
            f"{MIN_NET_PROFIT_PERCENT}%.",
        )

    return (
        current_opportunity,
        None,
    )


# ============================================================
# ОБРАБОТКА КНОПОК TELEGRAM
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


    # --------------------------------------------------------
    # НЕТ
    # --------------------------------------------------------

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

        send_telegram_message(
            """
❌ <b>ВОЗМОЖНОСТЬ ОТКЛОНЕНА</b>

Никаких реальных ордеров
не выставлялось.
"""
        )

        return


    # --------------------------------------------------------
    # ДА
    # --------------------------------------------------------

    if action == "yes":

        answer_callback_query(
            callback_id,
            "Повторно проверяю свежие стаканы...",
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

Ликвидность:
<b>${current['buy_exchange_liquidity']:,.2f}</b>

Проскальзывание:
<b>{current['buy_slippage_percent']}%</b>

━━━━━━━━━━━━━━━━━━

🔴 Продать:
<b>{current['sell_exchange_name']}</b>

Средняя цена:
<b>${current['sell_price']}</b>

Ликвидность:
<b>${current['sell_exchange_liquidity']:,.2f}</b>

Проскальзывание:
<b>{current['sell_slippage_percent']}%</b>

━━━━━━━━━━━━━━━━━━

💰 Фактическая стоимость покупки:
<b>${current['actual_buy_cost']:,.2f}</b>

📊 Валовый спред:
<b>+{current['gross_spread_percent']}%</b>

📈 <b>АКТУАЛЬНАЯ ЧИСТАЯ ПРИБЫЛЬ:</b>
<b>+{current['net_profit_percent']}%</b>

💵 Ожидаемая прибыль:
<b>+${current['net_profit_usd']:,.2f}</b>

━━━━━━━━━━━━━━━━━━

🧪 <b>ТЕСТОВЫЙ РЕЖИМ</b>

Повторная проверка успешно пройдена.

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
# ФОНОВОЕ СКАНИРОВАНИЕ
# ============================================================

def scanner_loop():

    global last_opportunities
    global last_scan_time
    global total_scans
    global total_opportunities_found

    print(
        "🔎 Арбитражный сканер запущен."
    )

    while True:

        scan_started = time.time()

        try:

            print("")
            print(
                "=========================================="
            )

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
                "⚡ Монеты и стаканы "
                "получаются параллельно"
            )

            opportunities = scan_all()

            with lock:

                last_opportunities = (
                    opportunities
                )

                last_scan_time = (
                    datetime.now()
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

            print(
                "=========================================="
            )

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
        print(
            "=========================================="
        )
        print(
            "🚀 ЗАПУСК АРБИТРАЖНОЙ СИСТЕМЫ"
        )
        print(
            "=========================================="
        )

        print(
            f"💰 Размер сделки: "
            f"${TRADE_AMOUNT_USD:,.2f}"
        )

        print(
            f"📈 Минимальная прибыль: "
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
            f"⚡ Постоянный пул: "
            f"{MAX_WORKERS} потоков"
        )

        # Markets загружаются только один раз
        load_all_markets()

        # Telegram
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

        # Очистка старых данных
        cleanup_thread = (
            threading.Thread(
                target=cleanup_loop,
                daemon=True,
                name="cleanup-service",
            )
        )

        cleanup_thread.start()

        # Сканер
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
            "✅ Все фоновые сервисы запущены."
        )

        print(
            "=========================================="
        )


# ============================================================
# ВЕБ-ИНТЕРФЕЙС
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
    max-width: 1000px;
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

💰 Размер сделки:
<b>${{ "%.2f"|format(trade_amount) }}</b>

<br><br>

📈 Минимальная чистая прибыль:
<b>{{ min_profit }}%</b>

<br><br>

⚡ Постоянный пул потоков:
<b>ВКЛЮЧЕН</b>

<br><br>

📚 Проверка стакана:
<b>ВКЛЮЧЕНА</b>

<br><br>

💧 Ликвидность в зоне исполнения:
<b>ВКЛЮЧЕНА</b>

<br><br>

🕒 Свежесть стакана:
<b>ВКЛЮЧЕНА</b>

<br><br>

🔁 Двойная проверка Telegram:
<b>ВКЛЮЧЕНА</b>

<br><br>

⭐ Рейтинг качества:
<b>ВКЛЮЧЕН</b>

<br><br>

🧪 Режим:
<b>ТЕСТОВЫЙ</b>

</div>


{% if current_symbols %}

<div class="card">

🔍 Сейчас параллельно сканируются:

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

Бот проверяет реальные стаканы,
ликвидность, комиссии,
проскальзывание и свежесть данных.

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
    )


# ============================================================
# API СКАНИРОВАНИЯ
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

    with stats_lock:

        order_book_requests = (
            total_order_book_requests
        )

        successful_order_books = (
            total_successful_order_books
        )

    return jsonify({

        "status":
            "success",

        "test_mode":
            True,

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

        "last_scan": (
            scan_time.isoformat()
            if scan_time
            else None
        ),

        "opportunities":
            opportunities,
    })


# ============================================================
# HEALTH
# ============================================================

@app.route("/health")
def health():

    start_background_services()

    with stats_lock:

        order_book_requests = (
            total_order_book_requests
        )

        successful_order_books = (
            total_successful_order_books
        )

    return jsonify({

        "status":
            "ok",

        "test_mode":
            True,

        "scanner":
            scanner_started,

        "telegram":
            telegram_started,

        "trade_amount":
            TRADE_AMOUNT_USD,

        "min_profit":
            MIN_NET_PROFIT_PERCENT,

        "total_coins":
            len(SYMBOLS),

        "symbols":
            SYMBOLS,

        "exchanges_count":
            len(exchanges),

        "exchanges":
            list(exchanges.keys()),

        "order_book_requests":
            order_book_requests,

        "successful_order_books":
            successful_order_books,

        "constant_thread_pool":
            True,

        "parallel_symbols":
            True,

        "order_book_check":
            True,

        "execution_zone_liquidity":
            True,

        "freshness_check":
            True,

        "double_telegram_check":
            True,

        "quality_score":
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

        executor.shutdown(
            wait=False,
            cancel_futures=True,
        )