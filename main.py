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

TRADE_AMOUNT_USD = 1000
MIN_NET_PROFIT_PERCENT = 0.10
SCAN_INTERVAL = 15
TELEGRAM_POLL_INTERVAL = 2
NOTIFICATION_COOLDOWN = 300  # 5 минут


# ============================================================
# БИРЖИ
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
# СОЗДАНИЕ БИРЖ
# ============================================================

exchanges = {}

for exchange_id, exchange_class in EXCHANGE_CLASSES.items():
    try:
        exchanges[exchange_id] = exchange_class({
            "enableRateLimit": True,
            "timeout": 10000,
        })
    except Exception as e:
        print(f"Ошибка подключения {exchange_id}: {e}")


# ============================================================
# СОСТОЯНИЕ
# ============================================================

app = Flask(__name__)

last_opportunities = []
last_scan_time = None
last_sent_notifications = {}
pending_opportunities = {}

lock = threading.Lock()


# ============================================================
# TELEGRAM
# ============================================================

def telegram_api(method, data=None):
    """Отправка запросов к Telegram Bot API."""

    if not TELEGRAM_BOT_TOKEN:
        return None

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"

    try:
        response = requests.post(url, json=data or {}, timeout=15)
        return response.json()
    except Exception as e:
        print(f"Telegram API ошибка: {e}")
        return None


def send_telegram_message(text, reply_markup=None):
    """Отправляет сообщение пользователю."""

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram не настроен: отсутствует TOKEN или CHAT_ID")
        return None

    data = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
    }

    if reply_markup:
        data["reply_markup"] = reply_markup

    return telegram_api("sendMessage", data)


def answer_callback_query(callback_query_id, text=""):
    """Убирает загрузку с кнопки Telegram."""

    return telegram_api(
        "answerCallbackQuery",
        {
            "callback_query_id": callback_query_id,
            "text": text,
        },
    )


# ============================================================
# ПОЛУЧЕНИЕ ЦЕН
# ============================================================

def get_price(exchange_id, symbol):
    """Получает лучшую цену покупки и продажи."""

    exchange = exchanges.get(exchange_id)

    if not exchange:
        return None

    try:
        ticker = exchange.fetch_ticker(symbol)

        ask = ticker.get("ask")
        bid = ticker.get("bid")

        if not ask or not bid:
            return None

        return {
            "ask": float(ask),
            "bid": float(bid),
        }

    except Exception as e:
        print(f"Ошибка {exchange_id} {symbol}: {e}")
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
    """Рассчитывает чистую прибыль с учётом комиссий."""

    buy_fee_percent = EXCHANGE_FEES.get(buy_exchange, 0.10)
    sell_fee_percent = EXCHANGE_FEES.get(sell_exchange, 0.10)

    buy_cost = TRADE_AMOUNT_USD * (1 + buy_fee_percent / 100)

    gross_revenue = TRADE_AMOUNT_USD * (
        sell_price / buy_price
    )

    sell_revenue = gross_revenue * (
        1 - sell_fee_percent / 100
    )

    net_profit_usd = sell_revenue - buy_cost
    net_profit_percent = (
        net_profit_usd / TRADE_AMOUNT_USD
    ) * 100

    gross_spread_percent = (
        (sell_price - buy_price) / buy_price
    ) * 100

    return {
        "symbol": symbol,
        "buy_exchange": buy_exchange,
        "buy_exchange_name": EXCHANGE_NAMES.get(
            buy_exchange,
            buy_exchange.upper()
        ),
        "buy_price": round(buy_price, 8),
        "sell_exchange": sell_exchange,
        "sell_exchange_name": EXCHANGE_NAMES.get(
            sell_exchange,
            sell_exchange.upper()
        ),
        "sell_price": round(sell_price, 8),
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
        "buy_fee_percent": buy_fee_percent,
        "sell_fee_percent": sell_fee_percent,
    }


# ============================================================
# СКАНИРОВАНИЕ ОДНОЙ МОНЕТЫ
# ============================================================

def scan_symbol(symbol):
    prices = {}

    for exchange_id in exchanges.keys():
        price_data = get_price(exchange_id, symbol)

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
                opportunities.append(opportunity)

    return opportunities


# ============================================================
# ПОЛНОЕ СКАНИРОВАНИЕ
# ============================================================

def scan_all():
    all_opportunities = []

    for symbol in SYMBOLS:
        try:
            opportunities = scan_symbol(symbol)
            all_opportunities.extend(opportunities)
        except Exception as e:
            print(f"Ошибка сканирования {symbol}: {e}")

    all_opportunities.sort(
        key=lambda x: x["net_profit_percent"],
        reverse=True
    )

    return all_opportunities


# ============================================================
# TELEGRAM УВЕДОМЛЕНИЕ
# ============================================================

def send_opportunity_to_telegram(opportunity):
    """Отправляет новую возможность с кнопками."""

    notification_key = (
        f"{opportunity['symbol']}_"
        f"{opportunity['buy_exchange']}_"
        f"{opportunity['sell_exchange']}"
    )

    current_time = time.time()
    last_sent = last_sent_notifications.get(
        notification_key,
        0
    )

    # Защита от постоянного спама одной и той же сделкой
    if current_time - last_sent < NOTIFICATION_COOLDOWN:
        return

    opportunity_id = str(uuid.uuid4())[:8]

    pending_opportunities[opportunity_id] = {
        "id": opportunity_id,
        "symbol": opportunity["symbol"],
        "buy_exchange": opportunity["buy_exchange"],
        "sell_exchange": opportunity["sell_exchange"],
        "created_at": current_time,
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

📊 Валовый спред: <b>+{opportunity['gross_spread_percent']}%</b>
📈 Чистая прибыль: <b>+{opportunity['net_profit_percent']}%</b>

💰 Сделка: <b>${TRADE_AMOUNT_USD}</b>
💵 Ожидаемая прибыль: <b>+${opportunity['net_profit_usd']}</b>

⚠️ После нажатия «ДА» цены будут проверены ещё раз.
"""

    reply_markup = {
        "inline_keyboard": [
            [
                {
                    "text": "✅ ДА — ПРОВЕРИТЬ",
                    "callback_data": f"yes:{opportunity_id}",
                },
                {
                    "text": "❌ НЕТ",
                    "callback_data": f"no:{opportunity_id}",
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
            f"Telegram: отправлена возможность "
            f"{opportunity['symbol']}"
        )


# ============================================================
# ПОВТОРНАЯ ПРОВЕРКА ПОСЛЕ «ДА»
# ============================================================

def recheck_opportunity(opportunity_id):
    opportunity = pending_opportunities.get(
        opportunity_id
    )

    if not opportunity:
        return None, "Возможность уже устарела."

    symbol = opportunity["symbol"]
    buy_exchange = opportunity["buy_exchange"]
    sell_exchange = opportunity["sell_exchange"]

    buy_data = get_price(
        buy_exchange,
        symbol
    )

    sell_data = get_price(
        sell_exchange,
        symbol
    )

    if not buy_data or not sell_data:
        return None, "Не удалось получить актуальные цены."

    current_opportunity = calculate_opportunity(
        symbol=symbol,
        buy_exchange=buy_exchange,
        buy_price=buy_data["ask"],
        sell_exchange=sell_exchange,
        sell_price=sell_data["bid"],
    )

    return current_opportunity, None


# ============================================================
# ОБРАБОТКА КНОПОК TELEGRAM
# ============================================================

def handle_callback(callback_query):
    callback_id = callback_query.get("id")
    callback_data = callback_query.get("data", "")
    message = callback_query.get("message", {})

    chat_id = str(
        message.get("chat", {}).get("id", "")
    )

    # Только твой Telegram может управлять ботом
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

    action, opportunity_id = callback_data.split(
        ":",
        1
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
            "❌ <b>Сделка отклонена.</b>\n\n"
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

        current, error = recheck_opportunity(
            opportunity_id
        )

        pending_opportunities.pop(
            opportunity_id,
            None
        )

        if error:
            send_telegram_message(
                f"⚠️ <b>Сделка не выполнена.</b>\n\n"
                f"{error}"
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

🪙 {current['symbol']}

📊 Новая чистая прибыль:
<b>{current['net_profit_percent']}%</b>

❌ Это ниже установленного минимума
<b>{MIN_NET_PROFIT_PERCENT}%</b>

Никаких реальных ордеров не было выставлено.
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

💰 Ожидаемый результат:
<b>+${current['net_profit_usd']}</b>

🧪 <b>ТЕСТОВЫЙ РЕЖИМ</b>

Реальные ордера пока НЕ выставлены.
Система успешно получила твоё подтверждение и повторно проверила сделку.
"""
        )

        return


# ============================================================
# TELEGRAM LONG POLLING
# ============================================================

def telegram_polling():
    """Постоянно ждёт нажатия кнопок."""

    if not TELEGRAM_BOT_TOKEN:
        print("Telegram polling не запущен: нет TOKEN")
        return

    print("Telegram бот запущен.")

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
                time.sleep(5)
                continue

            for update in data.get("result", []):
                offset = update["update_id"] + 1

                callback_query = update.get(
                    "callback_query"
                )

                if callback_query:
                    handle_callback(
                        callback_query
                    )

        except Exception as e:
            print(
                f"Ошибка Telegram polling: {e}"
            )
            time.sleep(5)


# ============================================================
# ФОНОВОЕ СКАНИРОВАНИЕ
# ============================================================

def scanner_loop():
    global last_opportunities
    global last_scan_time

    print("Арбитражный сканер запущен.")

    while True:
        try:
            print(
                f"Сканирование: "
                f"{datetime.now().strftime('%H:%M:%S')}"
            )

            opportunities = scan_all()

            with lock:
                last_opportunities = opportunities
                last_scan_time = datetime.now()

            print(
                f"Найдено возможностей: "
                f"{len(opportunities)}"
            )

            # Отправляем только лучшие возможности
            # Максимум 3 за один скан
            for opportunity in opportunities[:3]:
                send_opportunity_to_telegram(
                    opportunity
                )

        except Exception as e:
            print(
                f"Критическая ошибка сканера: {e}"
            )

        time.sleep(SCAN_INTERVAL)


# ============================================================
# ВЕБ-ИНТЕРФЕЙС
# ============================================================

HTML = """
<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport"
content="width=device-width, initial-scale=1.0">

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
{{ min_profit }}%

<br><br>

💸 Сделка: ${{ trade_amount }}

<br><br>

🔄 Обновление каждые
{{ interval }} секунд

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
<b>Чистая: +{{ op.net_profit_percent }}%</b>
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
# МАРШРУТЫ
# ============================================================

@app.route("/")
def index():

    with lock:
        opportunities = list(last_opportunities)
        scan_time = last_scan_time

    return render_template_string(
        HTML,
        opportunities=opportunities,
        min_profit=MIN_NET_PROFIT_PERCENT,
        trade_amount=TRADE_AMOUNT_USD,
        interval=SCAN_INTERVAL,
        last_scan=(
            scan_time.strftime("%d.%m.%Y %H:%M:%S")
            if scan_time
            else "Ожидание первого скана"
        ),
    )


@app.route("/scan")
def scan_api():

    with lock:
        opportunities = list(last_opportunities)
        scan_time = last_scan_time

    return jsonify({
        "status": "success",
        "scan_active": True,
        "symbols": SYMBOLS,
        "trade_amount": TRADE_AMOUNT_USD,
        "min_net_profit_percent":
            MIN_NET_PROFIT_PERCENT,
        "last_scan": (
            scan_time.isoformat()
            if scan_time
            else None
        ),
        "opportunities": opportunities,
    })


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "scanner": True,
        "telegram_configured": bool(
            TELEGRAM_BOT_TOKEN
            and TELEGRAM_CHAT_ID
        ),
    })


# ============================================================
# ЗАПУСК
# ============================================================

if __name__ == "__main__":

    scanner_thread = threading.Thread(
        target=scanner_loop,
        daemon=True
    )
    scanner_thread.start()

    telegram_thread = threading.Thread(
        target=telegram_polling,
        daemon=True
    )
    telegram_thread.start()

    port = int(
        os.getenv("PORT", 5000)
    )

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False
    )