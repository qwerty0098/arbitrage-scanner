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

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = str(os.getenv("TELEGRAM_CHAT_ID", ""))

# Размер одной тестовой сделки
TRADE_AMOUNT_USD = 1000

# Минимальная чистая прибыль после комиссий
MIN_NET_PROFIT_PERCENT = 0.10

# Интервал нового сканирования
SCAN_INTERVAL = 15

# Интервал проверки Telegram
TELEGRAM_POLL_INTERVAL = 2

# Не отправлять одинаковую возможность повторно 5 минут
NOTIFICATION_COOLDOWN = 300


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
# СОСТОЯНИЕ ПРИЛОЖЕНИЯ
# ============================================================

last_opportunities = []
last_scan_time = None
last_sent_notifications = {}
pending_opportunities = {}

lock = threading.Lock()


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

        print(
            f"Биржа подключена: {exchange_id}"
        )

    except Exception as error:

        print(
            f"Ошибка подключения {exchange_id}: {error}"
        )


# ============================================================
# TELEGRAM API
# ============================================================

def telegram_api(method, data=None):

    if not TELEGRAM_BOT_TOKEN:

        print(
            "Telegram API не настроен: "
            "отсутствует TELEGRAM_BOT_TOKEN"
        )

        return None

    url = (
        f"https://api.telegram.org/bot"
        f"{TELEGRAM_BOT_TOKEN}/{method}"
    )

    try:

        response = requests.post(
            url,
            json=data or {},
            timeout=15
        )

        result = response.json()

        if not result.get("ok"):

            print(
                f"Ошибка Telegram {method}: "
                f"{result}"
            )

        return result

    except Exception as error:

        print(
            f"Telegram API ошибка: {error}"
        )

        return None


# ============================================================
# ОТПРАВКА TELEGRAM СООБЩЕНИЯ
# ============================================================

def send_telegram_message(
    text,
    reply_markup=None
):

    if (
        not TELEGRAM_BOT_TOKEN
        or not TELEGRAM_CHAT_ID
    ):

        print(
            "Telegram не настроен: "
            "отсутствует TOKEN или CHAT_ID"
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

    result = telegram_api(
        "sendMessage",
        data
    )

    if result and result.get("ok"):

        print(
            "Telegram сообщение успешно отправлено."
        )

    return result


# ============================================================
# ОТВЕТ НА НАЖАТИЕ КНОПКИ
# ============================================================

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
        }
    )


# ============================================================
# СООБЩЕНИЕ ПРИ ЗАПУСКЕ
# ============================================================

def send_startup_message():

    if (
        not TELEGRAM_BOT_TOKEN
        or not TELEGRAM_CHAT_ID
    ):

        print(
            "Стартовое Telegram сообщение "
            "не отправлено: нет TOKEN или CHAT_ID"
        )

        return

    text = f"""
🤖 <b>БОТ ЗАПУЩЕН!</b>

📡 Arbitrage Scanner работает.

💰 Размер сделки:
<b>${TRADE_AMOUNT_USD}</b>

🎯 Минимальная чистая прибыль:
<b>{MIN_NET_PROFIT_PERCENT}%</b>

🪙 Монет отслеживается:
<b>{len(SYMBOLS)}</b>

🏦 Бирж подключено:
<b>{len(exchanges)}</b>

🔍 Ожидаю арбитражные возможности...

Отправь <b>/start</b> для проверки связи.
"""

    send_telegram_message(text)


# ============================================================
# ПОЛУЧЕНИЕ ЦЕНЫ
# ============================================================

def get_price(
    exchange_id,
    symbol
):

    exchange = exchanges.get(
        exchange_id
    )

    if not exchange:

        return None

    try:

        ticker = exchange.fetch_ticker(
            symbol
        )

        ask = ticker.get("ask")
        bid = ticker.get("bid")

        if ask is None or bid is None:

            return None

        ask = float(ask)
        bid = float(bid)

        if ask <= 0 or bid <= 0:

            return None

        return {
            "ask": ask,
            "bid": bid,
        }

    except Exception as error:

        print(
            f"Ошибка {exchange_id} "
            f"{symbol}: {str(error)[:150]}"
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

    # Сколько реально тратим на покупку
    buy_cost = (
        TRADE_AMOUNT_USD
        * (1 + buy_fee_percent / 100)
    )

    # Количество монет, которое покупаем
    coin_amount = (
        TRADE_AMOUNT_USD
        / buy_price
    )

    # Доход от продажи
    gross_revenue = (
        coin_amount
        * sell_price
    )

    # Доход после комиссии биржи
    sell_revenue = (
        gross_revenue
        * (1 - sell_fee_percent / 100)
    )

    # Чистая прибыль в долларах
    net_profit_usd = (
        sell_revenue
        - buy_cost
    )

    # Чистая прибыль в процентах
    net_profit_percent = (
        net_profit_usd
        / TRADE_AMOUNT_USD
    ) * 100

    # Валовый спред
    gross_spread_percent = (
        (
            sell_price
            - buy_price
        )
        / buy_price
    ) * 100

    return {
        "symbol":
            symbol,

        "buy_exchange":
            buy_exchange,

        "buy_exchange_name":
            EXCHANGE_NAMES.get(
                buy_exchange,
                buy_exchange.upper()
            ),

        "buy_price":
            round(
                buy_price,
                8
            ),

        "sell_exchange":
            sell_exchange,

        "sell_exchange_name":
            EXCHANGE_NAMES.get(
                sell_exchange,
                sell_exchange.upper()
            ),

        "sell_price":
            round(
                sell_price,
                8
            ),

        "gross_spread_percent":
            round(
                gross_spread_percent,
                4
            ),

        "net_profit_percent":
            round(
                net_profit_percent,
                4
            ),

        "net_profit_usd":
            round(
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

    # Получаем цены со всех бирж
    for exchange_id in exchanges.keys():

        price_data = get_price(
            exchange_id,
            symbol
        )

        if price_data:

            prices[
                exchange_id
            ] = price_data

    opportunities = []

    # Сравниваем все биржи друг с другом
    for buy_exchange, buy_data in prices.items():

        for sell_exchange, sell_data in prices.items():

            if (
                buy_exchange
                == sell_exchange
            ):

                continue

            buy_price = buy_data["ask"]
            sell_price = sell_data["bid"]

            # Если продавать дешевле,
            # чем покупать — сразу пропускаем
            if sell_price <= buy_price:

                continue

            opportunity = (
                calculate_opportunity(
                    symbol=symbol,
                    buy_exchange=buy_exchange,
                    buy_price=buy_price,
                    sell_exchange=sell_exchange,
                    sell_price=sell_price,
                )
            )

            # Оставляем только прибыльные
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

        except Exception as error:

            print(
                f"Ошибка сканирования "
                f"{symbol}: {error}"
            )

    all_opportunities.sort(
        key=lambda item:
            item["net_profit_percent"],
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

    # Не спамим одинаковой связкой
    if (
        current_time - last_sent
        < NOTIFICATION_COOLDOWN
    ):

        return

    opportunity_id = (
        str(uuid.uuid4())[:8]
    )

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

🟢 <b>КУПИТЬ</b>
🏦 {opportunity['buy_exchange_name']}
💵 Цена: <b>${opportunity['buy_price']}</b>

🔴 <b>ПРОДАТЬ</b>
🏦 {opportunity['sell_exchange_name']}
💵 Цена: <b>${opportunity['sell_price']}</b>

📊 Валовый спред:
<b>+{opportunity['gross_spread_percent']}%</b>

📈 Чистая прибыль:
<b>+{opportunity['net_profit_percent']}%</b>

💰 Сделка:
<b>${TRADE_AMOUNT_USD}</b>

💵 Ожидаемая прибыль:
<b>+${opportunity['net_profit_usd']}</b>

⚠️ После нажатия «ДА» цены будут
проверены ещё раз.

🧪 Сейчас работает тестовый режим:
реальные ордера пока не выставляются.
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

    if (
        result
        and result.get("ok")
    ):

        last_sent_notifications[
            notification_key
        ] = current_time

        print(
            "Telegram: отправлена "
            f"возможность "
            f"{opportunity['symbol']}"
        )


# ============================================================
# ПОВТОРНАЯ ПРОВЕРКА ПОСЛЕ НАЖАТИЯ «ДА»
# ============================================================

def recheck_opportunity(
    opportunity_id
):

    opportunity = (
        pending_opportunities.get(
            opportunity_id
        )
    )

    if not opportunity:

        return (
            None,
            "Возможность уже устарела."
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

    if (
        not buy_data
        or not sell_data
    ):

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

    return (
        current_opportunity,
        None
    )


# ============================================================
# ОБРАБОТКА КНОПОК TELEGRAM
# ============================================================

def handle_callback(
    callback_query
):

    callback_id = (
        callback_query.get("id")
    )

    callback_data = (
        callback_query.get(
            "data",
            ""
        )
    )

    message = (
        callback_query.get(
            "message",
            {}
        )
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

    # Только твой Chat ID
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

    # --------------------------------------------------------
    # НЕТ
    # --------------------------------------------------------

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
            "❌ <b>СДЕЛКА ОТКЛОНЕНА</b>\n\n"
            "Никаких действий выполнено не было."
        )

        return

    # --------------------------------------------------------
    # ДА
    # --------------------------------------------------------

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
                "⚠️ <b>СДЕЛКА НЕ ВЫПОЛНЕНА</b>\n\n"
                f"{error}"
            )

            return

        # Если прибыль уже упала
        if (
            current[
                "net_profit_percent"
            ]
            < MIN_NET_PROFIT_PERCENT
        ):

            send_telegram_message(
                f"""
⚠️ <b>СДЕЛКА ОТМЕНЕНА</b>

Цены изменились.

🪙 <b>{current['symbol']}</b>

📊 Новая чистая прибыль:
<b>{current['net_profit_percent']}%</b>

❌ Это ниже установленного минимума:
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
🏦 <b>{current['buy_exchange_name']}</b>
💵 ${current['buy_price']}

🔴 Продать:
🏦 <b>{current['sell_exchange_name']}</b>
💵 ${current['sell_price']}

📈 Актуальная чистая прибыль:
<b>+{current['net_profit_percent']}%</b>

💰 Ожидаемый результат:
<b>+${current['net_profit_usd']}</b>

🧪 <b>ТЕСТОВЫЙ РЕЖИМ</b>

Реальные ордера пока НЕ выставлены.

Система успешно получила твоё
подтверждение и повторно проверила
сделку.
"""
        )

        return


# ============================================================
# TELEGRAM LONG POLLING
# ============================================================

def telegram_polling():

    if not TELEGRAM_BOT_TOKEN:

        print(
            "Telegram polling не запущен: "
            "нет TOKEN"
        )

        return

    print(
        "Telegram бот запущен."
    )

    offset = None

    while True:

        try:

            url = (
                f"https://api.telegram.org/bot"
                f"{TELEGRAM_BOT_TOKEN}/getUpdates"
            )

            params = {
                "timeout": 20,
            }

            if offset is not None:

                params["offset"] = offset

            response = requests.get(
                url,
                params=params,
                timeout=30
            )

            data = response.json()

            if not data.get("ok"):

                print(
                    "Telegram getUpdates ошибка: "
                    f"{data}"
                )

                time.sleep(5)

                continue

            for update in data.get(
                "result",
                []
            ):

                offset = (
                    update["update_id"]
                    + 1
                )

                # =================================================
                # ОБЫЧНЫЕ СООБЩЕНИЯ
                # =================================================

                message = update.get(
                    "message"
                )

                if message:

                    chat_id = str(
                        message.get(
                            "chat",
                            {}
                        ).get(
                            "id",
                            ""
                        )
                    )

                    text = message.get(
                        "text",
                        ""
                    )

                    if (
                        chat_id
                        == TELEGRAM_CHAT_ID
                    ):

                        if text == "/start":

                            send_telegram_message(
                                f"""
🤖 <b>БОТ АКТИВЕН!</b>

📡 Сканер работает.

🪙 Отслеживается монет:
<b>{len(SYMBOLS)}</b>

🏦 Подключено бирж:
<b>{len(exchanges)}</b>

🎯 Минимальная прибыль:
<b>{MIN_NET_PROFIT_PERCENT}%</b>

🚨 Когда найдётся подходящая
арбитражная возможность,
я отправлю её сюда.

После этого ты сможешь нажать:

✅ <b>ДА — ПРОВЕРИТЬ</b>

или

❌ <b>НЕТ</b>
"""
                            )

                        elif text == "/status":

                            with lock:

                                count = len(
                                    last_opportunities
                                )

                                scan_time = (
                                    last_scan_time
                                )

                            send_telegram_message(
                                f"""
📊 <b>СТАТУС СКАНЕРА</b>

🟢 Сканер активен

🔍 Возможностей сейчас:
<b>{count}</b>

🪙 Монет:
<b>{len(SYMBOLS)}</b>

🏦 Бирж:
<b>{len(exchanges)}</b>

🕒 Последний скан:
<b>{
    scan_time.strftime(
        '%d.%m.%Y %H:%M:%S'
    )
    if scan_time
    else 'ещё не было'
}</b>
"""
                            )

                # =================================================
                # НАЖАТИЯ КНОПОК
                # =================================================

                callback_query = update.get(
                    "callback_query"
                )

                if callback_query:

                    handle_callback(
                        callback_query
                    )

        except Exception as error:

            print(
                "Ошибка Telegram polling: "
                f"{error}"
            )

            time.sleep(5)

        time.sleep(
            TELEGRAM_POLL_INTERVAL
        )


# ============================================================
# ФОНОВОЕ СКАНИРОВАНИЕ
# ============================================================

def scanner_loop():

    global last_opportunities
    global last_scan_time

    print(
        "Арбитражный сканер запущен."
    )

    while True:

        try:

            print(
                "Сканирование: "
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
                "Найдено возможностей: "
                f"{len(opportunities)}"
            )

            # Максимум 3 уведомления за скан
            for opportunity in opportunities[:3]:

                send_opportunity_to_telegram(
                    opportunity
                )

        except Exception as error:

            print(
                "Критическая ошибка сканера: "
                f"{error}"
            )

        time.sleep(
            SCAN_INTERVAL
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
    padding: 25px;

    background: #0f1623;
    color: #e8edf5;

    font-family:
        Arial,
        sans-serif;
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

    border:
        1px solid #31445f;

    border-radius: 28px;

    padding: 28px;

    margin-bottom: 20px;

    font-size: 20px;

    line-height: 1.6;
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

    border:
        1px solid #31445f;

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
    background: #184f3e;

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

.empty {
    text-align: center;

    background: #1d2939;

    border:
        1px solid #31445f;

    border-radius: 28px;

    padding: 35px;

    font-size: 22px;

    color: #b8c2d2;
}

@media (max-width: 600px) {

    body {
        padding: 15px;
    }

    .symbol {
        font-size: 30px;
    }

    .number {
        font-size: 48px;
    }
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

🪙 Отслеживается монет:
<b>{{ symbols_count }}</b>

<br><br>

🏦 Подключено бирж:
<b>{{ exchanges_count }}</b>

<br><br>

🔄 Обновление каждые
<b>{{ interval }} секунд</b>

<br><br>

🕒 Последний скан:
<b>{{ last_scan }}</b>

</div>


<div class="card">

<div class="number">
{{ opportunities|length }}
</div>

<div class="label">
Возможностей найдено
</div>

</div>


{% if opportunities %}

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

<b>
+${{ op.net_profit_usd }}
</b>

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

{% else %}

<div class="empty">

🔍 Сейчас подходящих
арбитражных возможностей
не найдено.

<br><br>

Сканер продолжает работать
в фоновом режиме.

</div>

{% endif %}

</div>


<script>

setTimeout(
    () => {
        location.reload();
    },
    {{ interval * 1000 }}
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

    with lock:

        opportunities = list(
            last_opportunities
        )

        scan_time = (
            last_scan_time
        )

    return render_template_string(
        HTML,

        opportunities=
            opportunities,

        min_profit=
            MIN_NET_PROFIT_PERCENT,

        trade_amount=
            TRADE_AMOUNT_USD,

        interval=
            SCAN_INTERVAL,

        symbols_count=
            len(SYMBOLS),

        exchanges_count=
            len(exchanges),

        last_scan=(
            scan_time.strftime(
                "%d.%m.%Y %H:%M:%S"
            )
            if scan_time
            else "Ожидание первого скана"
        ),
    )


# ============================================================
# API СКАНЕРА
# ============================================================

@app.route("/scan")
def scan_api():

    with lock:

        opportunities = list(
            last_opportunities
        )

        scan_time = (
            last_scan_time
        )

    return jsonify({

        "status":
            "success",

        "scan_active":
            True,

        "symbols":
            SYMBOLS,

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


# ============================================================
# HEALTH CHECK
# ============================================================

@app.route("/health")
def health():

    return jsonify({

        "status":
            "ok",

        "scanner":
            True,

        "telegram_configured":
            bool(
                TELEGRAM_BOT_TOKEN
                and TELEGRAM_CHAT_ID
            ),

        "telegram_chat_id_configured":
            bool(
                TELEGRAM_CHAT_ID
            ),

        "symbols_count":
            len(SYMBOLS),

        "exchanges_count":
            len(exchanges),
    })


# ============================================================
# ЗАПУСК
# ============================================================

if __name__ == "__main__":

    print("=" * 60)

    print(
        "ARBITRAGE SCANNER ЗАПУСКАЕТСЯ"
    )

    print(
        f"Монет: {len(SYMBOLS)}"
    )

    print(
        f"Бирж: {len(exchanges)}"
    )

    print(
        f"Минимальная прибыль: "
        f"{MIN_NET_PROFIT_PERCENT}%"
    )

    print("=" * 60)

    # Сразу отправляем тестовое сообщение
    send_startup_message()

    # Запускаем сканер
    scanner_thread = threading.Thread(
        target=scanner_loop,
        daemon=True
    )

    scanner_thread.start()

    # Запускаем Telegram polling
    telegram_thread = threading.Thread(
        target=telegram_polling,
        daemon=True
    )

    telegram_thread.start()

    # Railway автоматически передаёт PORT
    port = int(
        os.getenv(
            "PORT",
            5000
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False
    )