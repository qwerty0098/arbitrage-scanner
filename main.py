import os
import time
import uuid
import threading
from datetime import datetime

import ccxt
import requests
from flask import Flask, jsonify, render_template_string


# ============================================================
# НАСТРОЙКИ
# ============================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = str(os.getenv("TELEGRAM_CHAT_ID", "")).strip()

TRADE_AMOUNT_USD = 1000

# Минимальная чистая прибыль
MIN_NET_PROFIT_PERCENT = 0.01

# Интервал сканирования
SCAN_INTERVAL = 15

# Интервал проверки Telegram
TELEGRAM_POLL_INTERVAL = 2

# Не отправлять одну и ту же связку слишком часто
NOTIFICATION_COOLDOWN = 300

# Сколько возможностей максимум отправлять за один скан
MAX_NOTIFICATIONS_PER_SCAN = 3


# ============================================================
# БИРЖИ И КОМИССИИ
# ============================================================

EXCHANGE_FEES = {
    "kraken": 0.26,
    "kucoin": 0.10,
    "bitget": 0.10,
    "bybit": 0.10,
}

EXCHANGE_NAMES = {
    "kraken": "KRAKEN",
    "kucoin": "KUCOIN",
    "bitget": "BITGET",
    "bybit": "BYBIT",
}

SYMBOLS = [
    "BTC/USDT",
    "ETH/USDT",
    "SOL/USDT",
    "XRP/USDT",
    "ADA/USDT",
    "DOGE/USDT",
    "AVAX/USDT",
    "LINK/USDT",
]

EXCHANGE_CLASSES = {
    "kraken": ccxt.kraken,
    "kucoin": ccxt.kucoin,
    "bitget": ccxt.bitget,
    "bybit": ccxt.bybit,
}


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# СОЗДАНИЕ БИРЖ
# ============================================================

exchanges = {}

for exchange_id, exchange_class in EXCHANGE_CLASSES.items():
    try:
        exchanges[exchange_id] = exchange_class({
            "enableRateLimit": True,
            "timeout": 10000,
        })

        print(f"✅ Подключена биржа: {exchange_id}")

    except Exception as e:
        print(
            f"❌ Ошибка подключения {exchange_id}: {e}"
        )


# ============================================================
# СОСТОЯНИЕ
# ============================================================

last_opportunities = []
last_scan_time = None

last_sent_notifications = {}
pending_opportunities = {}

scanner_started = False
telegram_started = False

lock = threading.Lock()
startup_lock = threading.Lock()


# ============================================================
# TELEGRAM API
# ============================================================

def telegram_api(method, data=None, request_method="POST"):
    """
    Запрос к Telegram Bot API.
    """

    if not TELEGRAM_BOT_TOKEN:
        print("❌ TELEGRAM_BOT_TOKEN отсутствует")
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
                timeout=30,
            )
        else:
            response = requests.post(
                url,
                json=data or {},
                timeout=20,
            )

        result = response.json()

        if not result.get("ok"):
            print(
                f"❌ Telegram API {method}: "
                f"{result}"
            )

        return result

    except Exception as e:
        print(
            f"❌ Telegram API ошибка "
            f"({method}): {e}"
        )

        return None


# ============================================================
# ПРОВЕРКА TELEGRAM ПРИ ЗАПУСКЕ
# ============================================================

def check_telegram_connection():
    """
    Проверяет токен и отправляет стартовое сообщение.
    """

    print("")
    print("==========================================")
    print("🤖 ПРОВЕРКА TELEGRAM")
    print("==========================================")

    if not TELEGRAM_BOT_TOKEN:
        print(
            "❌ TELEGRAM_BOT_TOKEN не найден "
            "в Variables Railway"
        )
        return False

    if not TELEGRAM_CHAT_ID:
        print(
            "❌ TELEGRAM_CHAT_ID не найден "
            "в Variables Railway"
        )
        return False

    print("🔑 Проверяю Telegram токен...")

    result = telegram_api(
        "getMe",
        request_method="GET"
    )

    if not result or not result.get("ok"):
        print("")
        print("❌ TELEGRAM ТОКЕН НЕ РАБОТАЕТ")
        print(
            "Проверь новый TELEGRAM_BOT_TOKEN "
            "в Railway."
        )
        print("")

        return False

    bot = result.get("result", {})
    bot_name = bot.get("username", "неизвестно")

    print(
        f"✅ Telegram токен работает"
    )

    print(
        f"🤖 Бот: @{bot_name}"
    )

    print(
        f"💬 Chat ID: {TELEGRAM_CHAT_ID}"
    )

    startup_text = f"""
🟢 <b>АРБИТРАЖНЫЙ БОТ ЗАПУЩЕН</b>

🤖 Бот: <b>@{bot_name}</b>

💰 Размер сделки:
<b>${TRADE_AMOUNT_USD}</b>

📈 Минимальная чистая прибыль:
<b>{MIN_NET_PROFIT_PERCENT}%</b>

🔄 Сканирование каждые:
<b>{SCAN_INTERVAL} секунд</b>

🪙 Монет отслеживается:
<b>{len(SYMBOLS)}</b>

📊 Бирж подключено:
<b>{len(exchanges)}</b>

🚀 Система готова к поиску возможностей.
"""

    send_result = send_telegram_message(
        startup_text
    )

    if send_result and send_result.get("ok"):
        print(
            "✅ Стартовое сообщение "
            "успешно отправлено в Telegram"
        )
    else:
        print("")
        print(
            "❌ ТОКЕН РАБОТАЕТ, НО СООБЩЕНИЕ "
            "НЕ ОТПРАВЛЕНО"
        )
        print(
            "👉 Открой Telegram, зайди в своего "
            "бота и нажми START."
        )
        print(
            "👉 Также проверь TELEGRAM_CHAT_ID."
        )
        print("")

        return False

    print("==========================================")
    print("")

    return True


# ============================================================
# ОТПРАВКА TELEGRAM СООБЩЕНИЯ
# ============================================================

def send_telegram_message(text, reply_markup=None):
    """
    Отправляет сообщение пользователю.
    """

    if not TELEGRAM_BOT_TOKEN:
        print(
            "❌ Невозможно отправить Telegram: "
            "нет TOKEN"
        )
        return None

    if not TELEGRAM_CHAT_ID:
        print(
            "❌ Невозможно отправить Telegram: "
            "нет CHAT_ID"
        )
        return None

    data = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
    }

    if reply_markup:
        data["reply_markup"] = reply_markup

    return telegram_api(
        "sendMessage",
        data,
        request_method="POST",
    )


def answer_callback_query(
    callback_query_id,
    text=""
):
    """
    Убирает загрузку после нажатия кнопки.
    """

    return telegram_api(
        "answerCallbackQuery",
        {
            "callback_query_id": callback_query_id,
            "text": text,
        },
    )


# ============================================================
# ПОЛУЧЕНИЕ ЦЕНЫ
# ============================================================

def get_price(exchange_id, symbol):

    exchange = exchanges.get(exchange_id)

    if not exchange:
        return None

    try:
        ticker = exchange.fetch_ticker(symbol)

        ask = ticker.get("ask")
        bid = ticker.get("bid")

        if not ask or not bid:
            return None

        ask = float(ask)
        bid = float(bid)

        if ask <= 0 or bid <= 0:
            return None

        return {
            "ask": ask,
            "bid": bid,
        }

    except Exception as e:
        print(
            f"⚠️ Ошибка цены "
            f"{exchange_id} {symbol}: {e}"
        )

        return None


# ============================================================
# РАСЧЁТ АРБИТРАЖА
# ============================================================

def calculate_opportunity(
    symbol,
    buy_exchange,
    buy_price,
    sell_exchange,
    sell_price,
):

    buy_fee_percent = EXCHANGE_FEES.get(
        buy_exchange,
        0.10
    )

    sell_fee_percent = EXCHANGE_FEES.get(
        sell_exchange,
        0.10
    )

    # Реальная стоимость покупки
    buy_cost = TRADE_AMOUNT_USD * (
        1 + buy_fee_percent / 100
    )

    # Количество монет, которое покупаем
    quantity = (
        TRADE_AMOUNT_USD / buy_price
    )

    # Выручка от продажи
    gross_revenue = quantity * sell_price

    # После комиссии продажи
    sell_revenue = gross_revenue * (
        1 - sell_fee_percent / 100
    )

    # Чистая прибыль
    net_profit_usd = (
        sell_revenue - buy_cost
    )

    net_profit_percent = (
        net_profit_usd
        / TRADE_AMOUNT_USD
    ) * 100

    gross_spread_percent = (
        (sell_price - buy_price)
        / buy_price
    ) * 100

    return {
        "symbol": symbol,

        "buy_exchange": buy_exchange,

        "buy_exchange_name":
            EXCHANGE_NAMES.get(
                buy_exchange,
                buy_exchange.upper()
            ),

        "buy_price": round(
            buy_price,
            8
        ),

        "sell_exchange": sell_exchange,

        "sell_exchange_name":
            EXCHANGE_NAMES.get(
                sell_exchange,
                sell_exchange.upper()
            ),

        "sell_price": round(
            sell_price,
            8
        ),

        "gross_spread_percent": round(
            gross_spread_percent,
            4
        ),

        "net_profit_percent": round(
            net_profit_percent,
            4
        ),

        "net_profit_usd": round(
            net_profit_usd,
            2
        ),

        "buy_fee_percent":
            buy_fee_percent,

        "sell_fee_percent":
            sell_fee_percent,
    }


# ============================================================
# СКАНИРОВАНИЕ ОДНОЙ МОНЕТЫ
# ============================================================

def scan_symbol(symbol):

    prices = {}

    for exchange_id in exchanges.keys():

        price_data = get_price(
            exchange_id,
            symbol
        )

        if price_data:
            prices[exchange_id] = price_data

    opportunities = []

    for buy_exchange, buy_data in prices.items():

        for sell_exchange, sell_data in prices.items():

            if buy_exchange == sell_exchange:
                continue

            buy_price = buy_data["ask"]
            sell_price = sell_data["bid"]

            if sell_price <= buy_price:
                continue

            opportunity = calculate_opportunity(
                symbol=symbol,
                buy_exchange=buy_exchange,
                buy_price=buy_price,
                sell_exchange=sell_exchange,
                sell_price=sell_price,
            )

            if (
                opportunity["net_profit_percent"]
                >= MIN_NET_PROFIT_PERCENT
            ):
                opportunities.append(
                    opportunity
                )

    return opportunities


# ============================================================
# ПОЛНОЕ СКАНИРОВАНИЕ
# ============================================================

def scan_all():

    all_opportunities = []

    for symbol in SYMBOLS:

        try:
            opportunities = scan_symbol(
                symbol
            )

            all_opportunities.extend(
                opportunities
            )

        except Exception as e:
            print(
                f"⚠️ Ошибка сканирования "
                f"{symbol}: {e}"
            )

    all_opportunities.sort(
        key=lambda x:
            x["net_profit_percent"],
        reverse=True
    )

    return all_opportunities


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

    last_sent = (
        last_sent_notifications.get(
            notification_key,
            0
        )
    )

    # Защита от повторного спама
    if (
        current_time - last_sent
        < NOTIFICATION_COOLDOWN
    ):
        return False

    opportunity_id = str(
        uuid.uuid4()
    )[:8]

    pending_opportunities[
        opportunity_id
    ] = {
        "id": opportunity_id,

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

🟢 <b>КУПИТЬ</b>
<b>{opportunity['buy_exchange_name']}</b>
💵 Цена: <b>${opportunity['buy_price']}</b>

🔴 <b>ПРОДАТЬ</b>
<b>{opportunity['sell_exchange_name']}</b>
💵 Цена: <b>${opportunity['sell_price']}</b>

📊 Валовый спред:
<b>+{opportunity['gross_spread_percent']}%</b>

📈 Чистая прибыль:
<b>+{opportunity['net_profit_percent']}%</b>

💰 Размер сделки:
<b>${TRADE_AMOUNT_USD}</b>

💵 Ожидаемая прибыль:
<b>+${opportunity['net_profit_usd']}</b>

⚠️ После нажатия «ДА» цены будут
проверены ещё раз.
"""

    reply_markup = {
        "inline_keyboard": [
            [
                {
                    "text":
                        "✅ ДА — ПРОВЕРИТЬ",
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
        reply_markup
    )

    if result and result.get("ok"):

        last_sent_notifications[
            notification_key
        ] = current_time

        print(
            f"📨 Telegram отправлен: "
            f"{opportunity['symbol']} "
            f"{opportunity['buy_exchange']} → "
            f"{opportunity['sell_exchange']} "
            f"+{opportunity['net_profit_percent']}%"
        )

        return True

    # Если сообщение не ушло —
    # удаляем из ожидающих
    pending_opportunities.pop(
        opportunity_id,
        None
    )

    return False


# ============================================================
# ПОВТОРНАЯ ПРОВЕРКА ПОСЛЕ «ДА»
# ============================================================

def recheck_opportunity(
    opportunity_id
):

    opportunity = pending_opportunities.get(
        opportunity_id
    )

    if not opportunity:
        return (
            None,
            "Возможность уже устарела."
        )

    # Возможность действует 5 минут
    if (
        time.time()
        - opportunity["created_at"]
        > 300
    ):
        pending_opportunities.pop(
            opportunity_id,
            None
        )

        return (
            None,
            "Возможность устарела."
        )

    symbol = opportunity["symbol"]

    buy_exchange = (
        opportunity["buy_exchange"]
    )

    sell_exchange = (
        opportunity["sell_exchange"]
    )

    buy_data = get_price(
        buy_exchange,
        symbol
    )

    sell_data = get_price(
        sell_exchange,
        symbol
    )

    if not buy_data or not sell_data:

        return (
            None,
            "Не удалось получить "
            "актуальные цены."
        )

    current_opportunity = (
        calculate_opportunity(
            symbol=symbol,
            buy_exchange=buy_exchange,
            buy_price=buy_data["ask"],
            sell_exchange=sell_exchange,
            sell_price=sell_data["bid"],
        )
    )

    return current_opportunity, None


# ============================================================
# ОБРАБОТКА КНОПОК
# ============================================================

def handle_callback(
    callback_query
):

    callback_id = callback_query.get("id")

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

    # Только владелец бота
    if chat_id != TELEGRAM_CHAT_ID:

        answer_callback_query(
            callback_id,
            "Нет доступа."
        )

        return

    if ":" not in callback_data:

        answer_callback_query(
            callback_id,
            "Неизвестная команда."
        )

        return

    action, opportunity_id = (
        callback_data.split(
            ":",
            1
        )
    )

    # ========================================================
    # НЕТ
    # ========================================================

    if action == "no":

        pending_opportunities.pop(
            opportunity_id,
            None
        )

        answer_callback_query(
            callback_id,
            "Сделка отменена."
        )

        send_telegram_message(
            """
❌ <b>СДЕЛКА ОТКЛОНЕНА</b>

Никаких действий выполнено не было.
"""
        )

        return


    # ========================================================
    # ДА
    # ========================================================

    if action == "yes":

        answer_callback_query(
            callback_id,
            "Проверяю актуальные цены..."
        )

        current, error = (
            recheck_opportunity(
                opportunity_id
            )
        )

        pending_opportunities.pop(
            opportunity_id,
            None
        )

        if error:

            send_telegram_message(
                f"""
⚠️ <b>СДЕЛКА НЕ ВЫПОЛНЕНА</b>

{error}
"""
            )

            return

        if (
            current["net_profit_percent"]
            < MIN_NET_PROFIT_PERCENT
        ):

            send_telegram_message(
                f"""
⚠️ <b>СДЕЛКА ОТМЕНЕНА</b>

Цены изменились.

🪙 <b>{current['symbol']}</b>

📈 Новая чистая прибыль:
<b>{current['net_profit_percent']}%</b>

❌ Минимум:
<b>{MIN_NET_PROFIT_PERCENT}%</b>

Никаких реальных ордеров
выставлено не было.
"""
            )

            return

        send_telegram_message(
            f"""
✅ <b>ВОЗМОЖНОСТЬ ПОДТВЕРЖДЕНА</b>

🪙 <b>{current['symbol']}</b>

🟢 Купить:
<b>{current['buy_exchange_name']}</b>
${current['buy_price']}

🔴 Продать:
<b>{current['sell_exchange_name']}</b>
${current['sell_price']}

📈 Актуальная чистая прибыль:
<b>+{current['net_profit_percent']}%</b>

💵 Ожидаемый результат:
<b>+${current['net_profit_usd']}</b>

🧪 <b>ТЕСТОВЫЙ РЕЖИМ</b>

Реальные ордера пока
НЕ выставляются.
"""
        )

        return


# ============================================================
# TELEGRAM LONG POLLING
# ============================================================

def telegram_polling():

    global telegram_started

    print(
        "🤖 Telegram polling запущен."
    )

    offset = None

    while True:

        try:

            result = telegram_api(
                "getUpdates",
                {
                    "timeout": 20,

                    **(
                        {"offset": offset}
                        if offset is not None
                        else {}
                    ),
                },
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
                    update["update_id"] + 1
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
                f"⚠️ Ошибка Telegram "
                f"polling: {e}"
            )

            time.sleep(5)


# ============================================================
# ФОНОВОЕ СКАНИРОВАНИЕ
# ============================================================

def scanner_loop():

    global last_opportunities
    global last_scan_time

    print(
        "🔎 Арбитражный сканер запущен."
    )

    while True:

        try:

            print(
                f"🔄 Сканирование: "
                f"{datetime.now().strftime('%H:%M:%S')}"
            )

            opportunities = scan_all()

            with lock:

                last_opportunities = (
                    opportunities
                )

                last_scan_time = (
                    datetime.now()
                )

            print(
                f"📊 Найдено возможностей: "
                f"{len(opportunities)}"
            )

            sent_count = 0

            for opportunity in opportunities:

                if (
                    sent_count
                    >= MAX_NOTIFICATIONS_PER_SCAN
                ):
                    break

                sent = (
                    send_opportunity_to_telegram(
                        opportunity
                    )
                )

                if sent:
                    sent_count += 1

        except Exception as e:

            print(
                f"❌ Критическая ошибка "
                f"сканера: {e}"
            )

        time.sleep(
            SCAN_INTERVAL
        )


# ============================================================
# ЗАПУСК ФОНОВЫХ СЕРВИСОВ
# ============================================================

def start_background_services():

    global scanner_started
    global telegram_started

    with startup_lock:

        # Защита от двойного запуска
        if scanner_started:
            return

        print("")
        print("==========================================")
        print("🚀 ЗАПУСК АРБИТРАЖНОЙ СИСТЕМЫ")
        print("==========================================")

        telegram_ok = (
            check_telegram_connection()
        )

        # Запускаем Telegram polling
        # только если токен и отправка работают
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
                "⚠️ Telegram polling не запущен."
            )

        # Сканер запускаем всегда
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
            "✅ Фоновые сервисы запущены."
        )

        print("==========================================")
        print("")


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
    padding: 25px;
    background: #0f1623;
    color: #e8edf5;
    font-family: Arial, sans-serif;
}

.container {
    max-width: 900px;
    margin: auto;
}

.status {
    display: inline-block;
    background: #124d3d;
    color: #7de0bb;
    padding: 18px 35px;
    border-radius: 40px;
    font-size: 22px;
    margin-bottom: 25px;
}

.card {
    background: #1d2939;
    border: 1px solid #31445f;
    border-radius: 28px;
    padding: 28px;
    margin-bottom: 20px;
}

.number {
    font-size: 58px;
    font-weight: bold;
    color: #66aaff;
}

.label {
    font-size: 24px;
    color: #b8c2d2;
}

.opportunity {
    background: #1d2939;
    border: 1px solid #31445f;
    border-radius: 28px;
    padding: 28px;
    margin-bottom: 20px;
}

.symbol {
    font-size: 38px;
    font-weight: bold;
    margin-bottom: 20px;
}

.profit {
    background: #922321;
    padding: 22px;
    border-radius: 28px;
    font-size: 25px;
    margin-bottom: 20px;
}

.buy {
    background: #34446b;
    padding: 22px;
    border-radius: 25px;
    margin-bottom: 15px;
}

.sell {
    background: #542c3a;
    padding: 22px;
    border-radius: 25px;
}

.exchange {
    font-size: 30px;
    font-weight: bold;
}

.price {
    font-size: 27px;
    margin-top: 10px;
}

</style>

</head>

<body>

<div class="container">

<div class="status">
🟢 Сканер активен
</div>

<div class="card">

🎯 Минимальная чистая прибыль:
<b>{{ min_profit }}%</b>

<br><br>

💸 Сделка:
<b>${{ trade_amount }}</b>

<br><br>

🔄 Обновление каждые
<b>{{ interval }} секунд</b>

<br><br>

🤖 Telegram:
<b>{{ telegram_status }}</b>

<br><br>

🕒 Последний скан:
{{ last_scan }}

</div>

<div class="card">

<div class="number">
{{ opportunities|length }}
</div>

<div class="label">
Возможностей найдено
</div>

</div>

{% for op in opportunities %}

<div class="opportunity">

<div class="symbol">
{{ op.symbol }}
</div>

<div class="profit">

<b>
Чистая:
+{{ op.net_profit_percent }}%
</b>

<br><br>

Валовый спред:
+{{ op.gross_spread_percent }}%

<br><br>

💰 Результат на ${{ trade_amount }}:
${{ op.net_profit_usd }}

</div>

<div class="buy">

🟢 КУПИТЬ

<div class="exchange">
{{ op.buy_exchange_name }}
</div>

<div class="price">
${{ op.buy_price }}
</div>

</div>

<div class="sell">

🔴 ПРОДАТЬ

<div class="exchange">
{{ op.sell_exchange_name }}
</div>

<div class="price">
${{ op.sell_price }}
</div>

</div>

</div>

{% endfor %}

</div>

<script>

setTimeout(() => {
    location.reload();
}, {{ interval * 1000 }});

</script>

</body>
</html>
"""


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def index():

    start_background_services()

    with lock:

        opportunities = list(
            last_opportunities
        )

        scan_time = last_scan_time

    return render_template_string(
        HTML,

        opportunities=opportunities,

        min_profit=MIN_NET_PROFIT_PERCENT,

        trade_amount=TRADE_AMOUNT_USD,

        interval=SCAN_INTERVAL,

        telegram_status=(
            "🟢 подключён"
            if telegram_started
            else "🔴 не подключён"
        ),

        last_scan=(
            scan_time.strftime(
                "%d.%m.%Y %H:%M:%S"
            )
            if scan_time
            else "Ожидание первого скана"
        ),
    )


@app.route("/scan")
def scan_api():

    start_background_services()

    with lock:

        opportunities = list(
            last_opportunities
        )

        scan_time = last_scan_time

    return jsonify({

        "status": "success",

        "scan_active": scanner_started,

        "telegram_active":
            telegram_started,

        "symbols": SYMBOLS,

        "trade_amount":
            TRADE_AMOUNT_USD,

        "min_net_profit_percent":
            MIN_NET_PROFIT_PERCENT,

        "last_scan": (
            scan_time.isoformat()
            if scan_time
            else None
        ),

        "opportunities":
            opportunities,
    })


@app.route("/health")
def health():

    start_background_services()

    return jsonify({

        "status": "ok",

        "scanner":
            scanner_started,

        "telegram_configured": bool(
            TELEGRAM_BOT_TOKEN
            and TELEGRAM_CHAT_ID
        ),

        "telegram_polling":
            telegram_started,

        "min_profit":
            MIN_NET_PROFIT_PERCENT,

        "symbols_count":
            len(SYMBOLS),

        "exchanges_count":
            len(exchanges),
    })


# ============================================================
# ЗАПУСК
# ============================================================

if __name__ == "__main__":

    # Важно:
    # Railway Start Command лучше поставить:
    #
    # python app.py
    #
    # Тогда всё запустится сразу.

    start_background_services()

    port = int(
        os.getenv(
            "PORT",
            5000
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        use_reloader=False,
    )