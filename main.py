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

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = str(os.getenv("TELEGRAM_CHAT_ID", ""))

# Размер одной сделки
TRADE_AMOUNT_USD = 1000

# Минимальная ЧИСТАЯ прибыль после торговых комиссий
MIN_NET_PROFIT_PERCENT = 0.01

# Интервал сканирования
SCAN_INTERVAL = 15

# Интервал проверки Telegram-кнопок
TELEGRAM_POLL_INTERVAL = 2

# Не отправлять одну и ту же возможность слишком часто
NOTIFICATION_COOLDOWN = 300

# Таймаут запросов
REQUEST_TIMEOUT = 10000

# Количество одновременных потоков
MAX_WORKERS = 4


# ============================================================
# БИРЖИ И КОМИССИИ
# ============================================================

# Комиссии указаны в процентах.
# Используются для предварительного расчёта.
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

EXCHANGE_CLASSES = {
    "kraken": ccxt.kraken,
    "kucoin": ccxt.kucoin,
    "bitget": ccxt.bitget,
    "bybit": ccxt.bybit,
}


# ============================================================
# МОНЕТЫ
# ============================================================

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


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# ОБЩЕЕ СОСТОЯНИЕ
# ============================================================

last_opportunities = []
last_scan_time = None
last_scan_errors = []

last_sent_notifications = {}
pending_opportunities = {}

lock = threading.Lock()


# ============================================================
# СОЗДАНИЕ БИРЖ
# ============================================================

def get_exchange(exchange_id):
    exchange_class = EXCHANGE_CLASSES.get(exchange_id)

    if not exchange_class:
        return None

    try:
        return exchange_class({
            "enableRateLimit": True,
            "timeout": REQUEST_TIMEOUT,
        })

    except Exception as error:
        print(
            f"❌ Ошибка создания {exchange_id}: "
            f"{error}"
        )
        return None


# ============================================================
# TELEGRAM API
# ============================================================

def telegram_api(method, data=None):

    if not TELEGRAM_BOT_TOKEN:
        print("❌ TELEGRAM_BOT_TOKEN отсутствует")
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
                f"❌ Telegram API {method}: "
                f"{result}"
            )

        return result

    except Exception as error:
        print(
            f"❌ Telegram API ошибка "
            f"{method}: {error}"
        )
        return None


# ============================================================
# ОТПРАВКА СООБЩЕНИЯ
# ============================================================

def send_telegram_message(text, reply_markup=None):

    if not TELEGRAM_BOT_TOKEN:
        print(
            "❌ Telegram не настроен: "
            "нет TELEGRAM_BOT_TOKEN"
        )
        return None

    if not TELEGRAM_CHAT_ID:
        print(
            "❌ Telegram не настроен: "
            "нет TELEGRAM_CHAT_ID"
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
        data
    )


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
# ПОЛУЧЕНИЕ ЦЕН С ОДНОЙ БИРЖИ
# ============================================================

def scan_exchange(exchange_id):

    results = []
    errors = []

    exchange = get_exchange(exchange_id)

    if not exchange:
        return results, errors

    try:

        try:
            markets = exchange.load_markets()

        except Exception as error:

            errors.append(
                f"load_markets: {str(error)[:150]}"
            )

            markets = None

        for symbol in SYMBOLS:

            try:

                if (
                    markets is not None
                    and symbol not in markets
                ):
                    errors.append(
                        f"{symbol}: недоступна"
                    )
                    continue

                ticker = exchange.fetch_ticker(
                    symbol
                )

                ask = ticker.get("ask")
                bid = ticker.get("bid")

                if ask is None or bid is None:
                    errors.append(
                        f"{symbol}: нет bid/ask"
                    )
                    continue

                ask = float(ask)
                bid = float(bid)

                if ask <= 0 or bid <= 0:
                    errors.append(
                        f"{symbol}: некорректная цена"
                    )
                    continue

                results.append({
                    "exchange": exchange_id,
                    "symbol": symbol,
                    "ask": ask,
                    "bid": bid,
                })

            except Exception as error:

                errors.append(
                    f"{symbol}: "
                    f"{str(error)[:120]}"
                )

    except Exception as error:

        errors.append(
            f"exchange error: "
            f"{str(error)[:150]}"
        )

    finally:

        try:
            exchange.close()
        except Exception:
            pass

    return results, errors


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

    # Реальная стоимость покупки с комиссией
    buy_cost = (
        TRADE_AMOUNT_USD
        * (1 + buy_fee_percent / 100)
    )

    # Количество купленной монеты
    amount = (
        TRADE_AMOUNT_USD
        / buy_price
    )

    # Доход от продажи
    gross_revenue = (
        amount
        * sell_price
    )

    # Доход после комиссии продажи
    sell_revenue = (
        gross_revenue
        * (1 - sell_fee_percent / 100)
    )

    # Чистая прибыль
    net_profit_usd = (
        sell_revenue
        - buy_cost
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

        "buy_exchange":
            buy_exchange,

        "buy_exchange_name":
            EXCHANGE_NAMES.get(
                buy_exchange,
                buy_exchange.upper()
            ),

        "buy_price":
            round(buy_price, 8),

        "sell_exchange":
            sell_exchange,

        "sell_exchange_name":
            EXCHANGE_NAMES.get(
                sell_exchange,
                sell_exchange.upper()
            ),

        "sell_price":
            round(sell_price, 8),

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
# ПОЛНОЕ СКАНИРОВАНИЕ
# ============================================================

def scan_all():

    all_prices = []
    all_errors = []

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        futures = {
            executor.submit(
                scan_exchange,
                exchange_id
            ): exchange_id

            for exchange_id
            in EXCHANGE_CLASSES.keys()
        }

        for future in as_completed(futures):

            exchange_id = futures[future]

            try:

                prices, errors = future.result()

                all_prices.extend(prices)

                for error in errors[:5]:

                    all_errors.append(
                        f"{exchange_id}: {error}"
                    )

            except Exception as error:

                all_errors.append(
                    f"{exchange_id}: "
                    f"future error {str(error)[:150]}"
                )

    opportunities = []

    # --------------------------------------------------------
    # ИЩЕМ ЛУЧШУЮ ПОКУПКУ И ЛУЧШУЮ ПРОДАЖУ
    # --------------------------------------------------------

    for symbol in SYMBOLS:

        symbol_prices = [
            price
            for price in all_prices
            if price["symbol"] == symbol
        ]

        if len(symbol_prices) < 2:
            continue

        buy_exchange_data = min(
            symbol_prices,
            key=lambda item: item["ask"]
        )

        sell_exchange_data = max(
            symbol_prices,
            key=lambda item: item["bid"]
        )

        if (
            buy_exchange_data["exchange"]
            == sell_exchange_data["exchange"]
        ):
            continue

        buy_price = buy_exchange_data["ask"]
        sell_price = sell_exchange_data["bid"]

        if sell_price <= buy_price:
            continue

        opportunity = calculate_opportunity(
            symbol=symbol,

            buy_exchange=
                buy_exchange_data["exchange"],

            buy_price=
                buy_price,

            sell_exchange=
                sell_exchange_data["exchange"],

            sell_price=
                sell_price,
        )

        # ФИЛЬТР МИНИМАЛЬНОЙ ПРИБЫЛИ 0.01%
        if (
            opportunity[
                "net_profit_percent"
            ]
            >= MIN_NET_PROFIT_PERCENT
        ):

            opportunities.append(
                opportunity
            )

    opportunities.sort(
        key=lambda item:
            item["net_profit_percent"],
        reverse=True
    )

    return (
        opportunities,
        all_prices,
        all_errors
    )


# ============================================================
# ОТПРАВКА АРБИТРАЖНОЙ ВОЗМОЖНОСТИ
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

    # Защита от повторных сообщений
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
{opportunity['buy_exchange_name']}
💵 Цена: <b>${opportunity['buy_price']}</b>

🔴 <b>ПРОДАТЬ</b>
{opportunity['sell_exchange_name']}
💵 Цена: <b>${opportunity['sell_price']}</b>

📊 Валовый спред:
<b>+{opportunity['gross_spread_percent']}%</b>

📈 Чистая прибыль:
<b>+{opportunity['net_profit_percent']}%</b>

💰 Сделка:
<b>${TRADE_AMOUNT_USD}</b>

💵 Ожидаемая прибыль:
<b>+${opportunity['net_profit_usd']}</b>

⚠️ После нажатия «ДА»
цены будут проверены ещё раз.
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
            f"📨 Telegram: "
            f"отправлена возможность "
            f"{opportunity['symbol']} "
            f"{opportunity['net_profit_percent']}%"
        )

    else:

        print(
            "❌ Возможность не отправлена "
            "в Telegram"
        )


# ============================================================
# ПОЛУЧЕНИЕ АКТУАЛЬНОЙ ЦЕНЫ
# ============================================================

def get_price(
    exchange_id,
    symbol
):

    exchange = get_exchange(
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
            f"❌ Ошибка повторной проверки "
            f"{exchange_id} {symbol}: "
            f"{error}"
        )

        return None

    finally:

        try:
            exchange.close()
        except Exception:
            pass


# ============================================================
# ПОВТОРНАЯ ПРОВЕРКА СДЕЛКИ
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

    if not buy_data or not sell_data:

        return (
            None,
            "Не удалось получить "
            "актуальные цены."
        )

    current_opportunity = (
        calculate_opportunity(
            symbol=symbol,

            buy_exchange=
                buy_exchange,

            buy_price=
                buy_data["ask"],

            sell_exchange=
                sell_exchange,

            sell_price=
                sell_data["bid"],
        )
    )

    return (
        current_opportunity,
        None
    )


# ============================================================
# ОБРАБОТКА TELEGRAM КНОПОК
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

    # Только разрешённый CHAT ID
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
            """
❌ <b>СДЕЛКА ОТКЛОНЕНА</b>

Никаких действий выполнено не было.
"""
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
                f"""
⚠️ <b>СДЕЛКА НЕ ВЫПОЛНЕНА</b>

{error}
"""
            )

            return

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

        # Пока тестовый режим
        send_telegram_message(
            f"""
✅ <b>ВОЗМОЖНОСТЬ ПОДТВЕРЖДЕНА</b>

🪙 <b>{current['symbol']}</b>

🟢 Купить:
<b>{current['buy_exchange_name']}</b>

💵 ${current['buy_price']}

🔴 Продать:
<b>{current['sell_exchange_name']}</b>

💵 ${current['sell_price']}

📈 Актуальная чистая прибыль:
<b>+{current['net_profit_percent']}%</b>

💰 Ожидаемый результат:
<b>+${current['net_profit_usd']}</b>

🧪 <b>ТЕСТОВЫЙ РЕЖИМ</b>

Реальные ордера пока
НЕ выставляются.

Система успешно получила
твоё подтверждение и повторно
проверила актуальные цены.
"""
        )

        return


# ============================================================
# TELEGRAM LONG POLLING
# ============================================================

def telegram_polling():

    if not TELEGRAM_BOT_TOKEN:

        print(
            "❌ Telegram polling не запущен: "
            "нет TOKEN"
        )

        return

    print(
        "🤖 Telegram polling запущен."
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
                    f"❌ getUpdates ошибка: "
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

                callback_query = (
                    update.get(
                        "callback_query"
                    )
                )

                if callback_query:

                    handle_callback(
                        callback_query
                    )

        except Exception as error:

            print(
                f"❌ Ошибка Telegram polling: "
                f"{error}"
            )

            time.sleep(
                TELEGRAM_POLL_INTERVAL
            )


# ============================================================
# ФОНОВЫЙ СКАНЕР
# ============================================================

def scanner_loop():

    global last_opportunities
    global last_scan_time
    global last_scan_errors

    print(
        "🔍 Арбитражный сканер запущен."
    )

    while True:

        try:

            print(
                f"🔄 Сканирование: "
                f"{datetime.now().strftime('%H:%M:%S')}"
            )

            (
                opportunities,
                prices,
                errors
            ) = scan_all()

            with lock:

                last_opportunities = (
                    opportunities
                )

                last_scan_time = (
                    datetime.now()
                )

                last_scan_errors = (
                    errors
                )

            print(
                f"💰 Получено цен: "
                f"{len(prices)}"
            )

            print(
                f"🚨 Найдено возможностей: "
                f"{len(opportunities)}"
            )

            # Отправляем максимум 3 лучшие
            # возможности за один скан
            for opportunity in opportunities[:3]:

                send_opportunity_to_telegram(
                    opportunity
                )

        except Exception as error:

            print(
                f"❌ Критическая ошибка "
                f"сканера: {error}"
            )

        time.sleep(
            SCAN_INTERVAL
        )


# ============================================================
# HTML
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

* {
    box-sizing: border-box;
}

body {
    margin: 0;
    padding: 20px;

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

h1 {
    font-size: 34px;
}

.status {
    display: inline-block;

    background: #124d3d;

    color: #7de0bb;

    padding: 15px 28px;

    border-radius: 40px;

    font-size: 20px;

    margin-bottom: 20px;
}

.card {
    background: #1d2939;

    border:
        1px solid #31445f;

    border-radius: 22px;

    padding: 24px;

    margin-bottom: 20px;

    line-height: 1.8;

    font-size: 18px;
}

.stats {
    display: grid;

    grid-template-columns:
        1fr 1fr;

    gap: 15px;

    margin-bottom: 20px;
}

.stat {
    background: #1d2939;

    border:
        1px solid #31445f;

    border-radius: 22px;

    padding: 24px;
}

.number {
    font-size: 48px;

    font-weight: bold;

    color: #66aaff;
}

.label {
    font-size: 18px;

    color: #b8c2d2;
}

.opportunity {
    background: #1d2939;

    border:
        1px solid #31445f;

    border-radius: 22px;

    padding: 24px;

    margin-bottom: 20px;
}

.symbol {
    font-size: 32px;

    font-weight: bold;

    margin-bottom: 18px;
}

.profit {
    background: #124d3d;

    padding: 20px;

    border-radius: 20px;

    font-size: 20px;

    margin-bottom: 15px;
}

.buy {
    background: #34446b;

    padding: 20px;

    border-radius: 18px;

    margin-bottom: 12px;
}

.sell {
    background: #542c3a;

    padding: 20px;

    border-radius: 18px;
}

.exchange {
    font-size: 25px;

    font-weight: bold;
}

.price {
    font-size: 22px;

    margin-top: 8px;
}

.empty {
    text-align: center;

    background: #1d2939;

    border:
        1px solid #31445f;

    border-radius: 22px;

    padding: 35px;

    color: #b8c2d2;

    font-size: 18px;
}

.footer {
    text-align: center;

    color: #8290a5;

    margin-top: 30px;
}

@media (max-width: 600px) {

    body {
        padding: 15px;
    }

    h1 {
        font-size: 28px;
    }

    .stats {
        grid-template-columns: 1fr;
    }

    .number {
        font-size: 42px;
    }

}

</style>

</head>

<body>

<div class="container">

<h1>
🚀 Arbitrage Scanner
</h1>

<div class="status">
🟢 Сканер активен
</div>


<div class="card">

🎯 Минимальная чистая прибыль:
<b>{{ min_profit }}%</b>

<br>

💸 Сделка:
<b>${{ trade_amount }}</b>

<br>

🔄 Обновление каждые:
<b>{{ interval }} секунд</b>

<br>

🤖 Telegram:
{% if telegram_configured %}
<b>подключён</b>
{% else %}
<b>не настроен</b>
{% endif %}

<br>

🕒 Последний скан:
<b>{{ last_scan }}</b>

</div>


<div class="stats">

<div class="stat">

<div class="number">
{{ opportunities|length }}
</div>

<div class="label">
Возможностей найдено
</div>

</div>


<div class="stat">

<div class="number">
{{ prices_count }}
</div>

<div class="label">
Цен получено
</div>

</div>

</div>


{% if opportunities %}

{% for op in opportunities %}

<div class="opportunity">

<div class="symbol">
{{ op.symbol }}
</div>

<div class="profit">

📈 <b>
Чистая:
+{{ op.net_profit_percent }}%
</b>

<br>

📊 Валовый спред:
+{{ op.gross_spread_percent }}%

<br>

💰 Результат на
${{ trade_amount }}:

<b>
${{ op.net_profit_usd }}
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

🔍 Сейчас подходящих возможностей
с чистой прибылью от
<b>{{ min_profit }}%</b>
не найдено.

<br><br>

Сканер продолжает работать.

</div>

{% endif %}


<div class="footer">

Страница автоматически обновляется
каждые {{ interval }} секунд

</div>

</div>


<script>

setTimeout(
    function () {
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

        opportunities = (
            list(last_opportunities)
        )

        scan_time = (
            last_scan_time
        )

        errors = (
            list(last_scan_errors)
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

        prices_count=(
            len(SYMBOLS)
            * len(EXCHANGE_CLASSES)
        ),

        telegram_configured=bool(
            TELEGRAM_BOT_TOKEN
            and TELEGRAM_CHAT_ID
        ),

        last_scan=(
            scan_time.strftime(
                "%d.%m.%Y %H:%M:%S"
            )
            if scan_time
            else
            "Ожидание первого скана"
        ),
    )


# ============================================================
# JSON API
# ============================================================

@app.route("/scan")
def scan_api():

    with lock:

        opportunities = (
            list(last_opportunities)
        )

        scan_time = (
            last_scan_time
        )

        errors = (
            list(last_scan_errors)
        )

    return jsonify({
        "status":
            "success",

        "scan_active":
            True,

        "telegram_configured":
            bool(
                TELEGRAM_BOT_TOKEN
                and TELEGRAM_CHAT_ID
            ),

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

        "errors":
            errors,
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

        "min_net_profit_percent":
            MIN_NET_PROFIT_PERCENT,
    })


# ============================================================
# ЗАПУСК
# ============================================================

if __name__ == "__main__":

    print("========================================")
    print("🚀 ЗАПУСК ARBITRAGE SCANNER")
    print("========================================")

    print(
        f"🎯 Минимальная прибыль: "
        f"{MIN_NET_PROFIT_PERCENT}%"
    )

    # --------------------------------------------------------
    # ПРОВЕРКА TELEGRAM
    # --------------------------------------------------------

    if (
        TELEGRAM_BOT_TOKEN
        and TELEGRAM_CHAT_ID
    ):

        print("🤖 Telegram настроен")

        print(
            f"💬 CHAT ID: "
            f"{TELEGRAM_CHAT_ID}"
        )

        test_result = (
            send_telegram_message(
                f"""
🤖 <b>БОТ ЗАПУЩЕН</b>

✅ Telegram успешно подключён.

🔍 Арбитражный сканер начал работу.

🎯 Минимальная чистая прибыль:
<b>{MIN_NET_PROFIT_PERCENT}%</b>

💰 Размер сделки:
<b>${TRADE_AMOUNT_USD}</b>

⏱ Сканирование каждые:
<b>{SCAN_INTERVAL} секунд</b>

Как только будет найдена
подходящая возможность,
я отправлю её сюда с кнопками:

✅ ДА — ПРОВЕРИТЬ
❌ НЕТ
"""
            )
        )

        if (
            test_result
            and test_result.get("ok")
        ):

            print(
                "✅ Тестовое сообщение "
                "Telegram успешно отправлено"
            )

        else:

            print(
                "❌ Не удалось отправить "
                "тестовое сообщение"
            )

            print(test_result)

    else:

        print(
            "❌ Telegram НЕ настроен"
        )

        print(
            "Проверь Variables:"
        )

        print(
            "TELEGRAM_BOT_TOKEN"
        )

        print(
            "TELEGRAM_CHAT_ID"
        )

    # --------------------------------------------------------
    # ЗАПУСК СКАНЕРА
    # --------------------------------------------------------

    scanner_thread = threading.Thread(
        target=scanner_loop,
        daemon=True
    )

    scanner_thread.start()

    print(
        "🔍 Фоновый сканер запущен"
    )

    # --------------------------------------------------------
    # ЗАПУСК TELEGRAM POLLING
    # --------------------------------------------------------

    telegram_thread = threading.Thread(
        target=telegram_polling,
        daemon=True
    )

    telegram_thread.start()

    print(
        "🤖 Telegram polling запущен"
    )

    # --------------------------------------------------------
    # ЗАПУСК FLASK
    # --------------------------------------------------------

    port = int(
        os.getenv("PORT", 5000)
    )

    print(
        f"🌐 Web server запускается "
        f"на порту {port}"
    )

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False
    )