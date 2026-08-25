from flask import Flask, jsonify, render_template_string
import ccxt
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__)

# =========================
# НАСТРОЙКИ
# =========================

EXCHANGE_NAMES = [
    "kraken",
    "kucoin",
    "bitget",
    "bybit",
]

SYMBOLS = [
    "BTC/USDT",
    "ETH/USDT",
    "SOL/USDT",
    "XRP/USDT",
]

# Обновление каждые 15 секунд
SCAN_INTERVAL = 15

# Таймаут одного запроса к бирже
REQUEST_TIMEOUT = 5000

# Комиссия покупки 0.1%
BUY_FEE = 0.001

# Комиссия продажи 0.1%
SELL_FEE = 0.001

# Минимальная чистая прибыль после комиссий
MIN_NET_PROFIT = 0.07


# =========================
# ОБЩЕЕ СОСТОЯНИЕ СКАНЕРА
# =========================

scanner_state = {
    "status": "starting",
    "opportunities": [],
    "prices": [],
    "opportunities_found": 0,
    "prices_received": 0,
    "last_scan": None,
    "error": None,
}

state_lock = threading.Lock()
scan_lock = threading.Lock()


# =========================
# СОЗДАНИЕ БИРЖИ
# =========================

def get_exchange(name):
    exchange_class = getattr(ccxt, name)

    return exchange_class({
        "enableRateLimit": True,
        "timeout": REQUEST_TIMEOUT,
    })


# =========================
# СКАНИРОВАНИЕ ОДНОЙ БИРЖИ
# =========================

def scan_exchange(exchange_name):
    results = []
    errors = []

    try:
        exchange = get_exchange(exchange_name)

        print(f"[{exchange_name}] Загружаю рынки...")

        exchange.load_markets()

        print(
            f"[{exchange_name}] Рынки загружены. "
            f"Проверяю {len(SYMBOLS)} пары."
        )

        for symbol in SYMBOLS:

            try:
                # Проверяем, существует ли такая пара
                if symbol not in exchange.markets:
                    error_message = (
                        f"{exchange_name}: "
                        f"пара {symbol} недоступна"
                    )

                    errors.append(error_message)
                    print(error_message)

                    continue

                ticker = exchange.fetch_ticker(symbol)

                bid = ticker.get("bid")
                ask = ticker.get("ask")

                if (
                    bid is not None
                    and ask is not None
                    and bid > 0
                    and ask > 0
                ):

                    results.append({
                        "exchange": exchange_name,
                        "symbol": symbol,
                        "bid": float(bid),
                        "ask": float(ask),
                    })

                    print(
                        f"[{exchange_name}] "
                        f"{symbol} OK | "
                        f"bid={bid} ask={ask}"
                    )

                else:
                    error_message = (
                        f"{exchange_name} {symbol}: "
                        f"нет корректных bid/ask. "
                        f"bid={bid}, ask={ask}"
                    )

                    errors.append(error_message)
                    print(error_message)

            except Exception as error:

                error_message = (
                    f"{exchange_name} {symbol}: "
                    f"{type(error).__name__}: {str(error)}"
                )

                errors.append(error_message)
                print(error_message)

    except Exception as error:

        error_message = (
            f"{exchange_name}: "
            f"ошибка подключения: "
            f"{type(error).__name__}: {str(error)}"
        )

        errors.append(error_message)
        print(error_message)

    return results, errors


# =========================
# ОДИН ПОЛНЫЙ СКАН
# =========================

def get_opportunities():

    all_prices = []
    all_errors = []

    print("\n========== НАЧАЛО СКАНИРОВАНИЯ ==========")

    # Сканируем биржи одновременно
    with ThreadPoolExecutor(max_workers=4) as executor:

        futures = {
            executor.submit(
                scan_exchange,
                exchange_name
            ): exchange_name

            for exchange_name in EXCHANGE_NAMES
        }

        for future in as_completed(futures):

            exchange_name = futures[future]

            try:
                prices, errors = future.result()

                all_prices.extend(prices)
                all_errors.extend(errors)

            except Exception as error:

                error_message = (
                    f"{exchange_name}: "
                    f"критическая ошибка: "
                    f"{type(error).__name__}: {str(error)}"
                )

                all_errors.append(error_message)
                print(error_message)

    print(
        f"ВСЕГО ЦЕН ПОЛУЧЕНО: "
        f"{len(all_prices)}"
    )

    print(
        f"ВСЕГО ОШИБОК: "
        f"{len(all_errors)}"
    )

    if all_errors:

        print("\n========== СПИСОК ОШИБОК ==========")

        for error in all_errors:
            print(error)

        print("===================================")

    opportunities = []

    # =========================
    # ИЩЕМ АРБИТРАЖ
    # =========================

    for symbol in SYMBOLS:

        prices = [
            item
            for item in all_prices
            if item["symbol"] == symbol
        ]

        if len(prices) < 2:

            print(
                f"{symbol}: "
                f"недостаточно цен для сравнения "
                f"({len(prices)})"
            )

            continue

        # Лучшая цена покупки
        buy_exchange = min(
            prices,
            key=lambda x: x["ask"]
        )

        # Лучшая цена продажи
        sell_exchange = max(
            prices,
            key=lambda x: x["bid"]
        )

        # Не покупаем и продаём на одной бирже
        if (
            buy_exchange["exchange"]
            == sell_exchange["exchange"]
        ):

            print(
                f"{symbol}: "
                f"лучшая покупка и продажа "
                f"на одной бирже "
                f"({buy_exchange['exchange']})"
            )

            continue

        buy_price = buy_exchange["ask"]
        sell_price = sell_exchange["bid"]

        # Валовой спред
        gross_spread = (
            (sell_price - buy_price)
            / buy_price
        ) * 100

        # Цена покупки с комиссией
        buy_cost = buy_price * (
            1 + BUY_FEE
        )

        # Выручка от продажи с комиссией
        sell_revenue = sell_price * (
            1 - SELL_FEE
        )

        # Чистая прибыль
        net_profit_percent = (
            (sell_revenue - buy_cost)
            / buy_cost
        ) * 100

        print(
            f"{symbol}: "
            f"купить {buy_exchange['exchange']} "
            f"${buy_price} | "
            f"продать {sell_exchange['exchange']} "
            f"${sell_price} | "
            f"валовая {gross_spread:.3f}% | "
            f"чистая {net_profit_percent:.3f}%"
        )

        # Добавляем только выгодные варианты
        if net_profit_percent >= MIN_NET_PROFIT:

            opportunities.append({

                "symbol": symbol,

                "buy_exchange":
                    buy_exchange["exchange"].upper(),

                "buy_price":
                    round(buy_price, 4),

                "sell_exchange":
                    sell_exchange["exchange"].upper(),

                "sell_price":
                    round(sell_price, 4),

                "gross_spread_percent":
                    round(gross_spread, 3),

                "net_profit_percent":
                    round(net_profit_percent, 3),
            })

    opportunities.sort(
        key=lambda x: x["net_profit_percent"],
        reverse=True
    )

    print(
        f"ВОЗМОЖНОСТЕЙ НАЙДЕНО: "
        f"{len(opportunities)}"
    )

    print(
        "========== КОНЕЦ СКАНИРОВАНИЯ ==========\n"
    )

    return opportunities, all_prices, all_errors


# =========================
# ЗАПУСК ОДНОГО СКАНА
# =========================

def run_scan():

    # Не запускаем два скана одновременно
    if not scan_lock.acquire(blocking=False):

        print(
            "Предыдущий скан ещё работает. "
            "Новый скан пропущен."
        )

        return

    try:

        with state_lock:
            scanner_state["status"] = "scanning"
            scanner_state["error"] = None

        print("Запускаю новый фоновый скан...")

        (
            opportunities,
            prices,
            errors
        ) = get_opportunities()

        with state_lock:

            scanner_state["opportunities"] = opportunities

            scanner_state["prices"] = prices

            scanner_state[
                "opportunities_found"
            ] = len(opportunities)

            scanner_state[
                "prices_received"
            ] = len(prices)

            scanner_state[
                "last_scan"
            ] = time.strftime(
                "%d.%m.%Y %H:%M:%S"
            )

            # Сохраняем ошибки для API
            if errors:
                scanner_state["error"] = "\n".join(
                    errors
                )
            else:
                scanner_state["error"] = None

            # Если хотя бы часть цен получена —
            # сканер считаем рабочим
            if len(prices) > 0:
                scanner_state["status"] = "active"
            else:
                scanner_state["status"] = "error"

    except Exception as error:

        error_message = (
            f"{type(error).__name__}: "
            f"{str(error)}"
        )

        print(
            "КРИТИЧЕСКАЯ ОШИБКА СКАНЕРА: "
            + error_message
        )

        with state_lock:

            scanner_state["status"] = "error"

            scanner_state["error"] = (
                error_message
            )

    finally:

        scan_lock.release()


# =========================
# ПОСТОЯННЫЙ ФОНОВЫЙ СКАНЕР
# =========================

def scanner_loop():

    print(
        "Фоновый сканер запущен."
    )

    # Первый скан сразу после запуска
    run_scan()

    while True:

        time.sleep(
            SCAN_INTERVAL
        )

        run_scan()


# =========================
# ЗАПУСК ФОНОВОГО ПОТОКА
# =========================

scanner_thread = threading.Thread(
    target=scanner_loop,
    daemon=True
)

scanner_thread.start()


# =========================
# ГЛАВНАЯ СТРАНИЦА
# =========================

@app.route("/")
def home():

    with state_lock:

        data = {
            "status":
                scanner_state["status"],

            "opportunities":
                list(
                    scanner_state["opportunities"]
                ),

            "opportunities_found":
                scanner_state[
                    "opportunities_found"
                ],

            "prices_received":
                scanner_state[
                    "prices_received"
                ],

            "last_scan":
                scanner_state[
                    "last_scan"
                ],

            "error":
                scanner_state[
                    "error"
                ],
        }

    return render_template_string("""
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

    font-family:
        Arial,
        sans-serif;

    background: #0b0f19;
    color: #f8fafc;
}

.container {
    max-width: 900px;
    margin: auto;
}

h1 {
    font-size: 38px;
    margin-bottom: 10px;
}

.subtitle {
    color: #aab4c4;
    font-size: 20px;
    margin-bottom: 25px;
}

.status {
    display: inline-block;
    padding: 12px 20px;
    border-radius: 30px;
    font-size: 18px;
    margin-bottom: 25px;
}

.status.active {
    background: #064e3b;
    color: #6ee7b7;
}

.status.scanning,
.status.starting {
    background: #78350f;
    color: #fbbf24;
}

.status.error {
    background: #7f1d1d;
    color: #fca5a5;
}

.info-box {
    background: #151e2e;
    border: 1px solid #26364f;
    border-radius: 20px;
    padding: 22px;
    margin-bottom: 25px;
    color: #aeb8c8;
    font-size: 18px;
    line-height: 1.8;
}

.stats {
    display: flex;
    gap: 20px;
    margin-bottom: 25px;
}

.stat {
    flex: 1;
    background: #151e2e;
    border: 1px solid #26364f;
    border-radius: 20px;
    padding: 25px;
}

.stat-value {
    font-size: 44px;
    font-weight: bold;
    color: #60a5fa;
}

.stat-label {
    color: #aeb8c8;
    font-size: 18px;
    margin-top: 10px;
}

.card {
    background: #151e2e;
    border: 1px solid #26364f;
    border-radius: 20px;
    padding: 25px;
    margin-bottom: 20px;
}

.card-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 15px;
    margin-bottom: 20px;
}

.symbol {
    font-size: 30px;
    font-weight: bold;
}

.spread {
    background: #065f46;
    color: #6ee7b7;
    padding: 10px 18px;
    border-radius: 30px;
    font-size: 20px;
    font-weight: bold;
    white-space: nowrap;
}

.trade-row {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 18px;
}

.buy,
.sell {
    padding: 20px;
    border-radius: 16px;
}

.buy {
    background: #23315b;
}

.sell {
    background: #4a2435;
}

.label {
    color: #b7c0d0;
    font-size: 16px;
    margin-bottom: 10px;
}

.exchange {
    font-size: 25px;
    font-weight: bold;
}

.price {
    font-size: 22px;
    margin-top: 10px;
}

.empty {
    text-align: center;
    background: #151e2e;
    border: 1px solid #26364f;
    padding: 45px 25px;
    border-radius: 20px;
    color: #aeb8c8;
    font-size: 20px;
    line-height: 1.6;
}

.error-box {
    background: #3b1620;
    border: 1px solid #7f1d1d;
    padding: 20px;
    border-radius: 16px;
    color: #fca5a5;
    margin-bottom: 25px;
    white-space: pre-wrap;
    overflow-wrap: anywhere;
}

.footer {
    text-align: center;
    color: #6b7280;
    margin-top: 30px;
    font-size: 14px;
}

@media (max-width: 600px) {

    body {
        padding: 16px;
    }

    h1 {
        font-size: 32px;
    }

    .subtitle {
        font-size: 18px;
    }

    .stats {
        flex-direction: column;
    }

    .trade-row {
        grid-template-columns: 1fr;
    }

    .card-header {
        align-items: flex-start;
        flex-direction: column;
    }
}

</style>

</head>

<body>

<div class="container">

<h1>🚀 Arbitrage Scanner</h1>

<div class="subtitle">
Мониторинг цен криптовалют между биржами
</div>


{% if status == "active" %}

<div class="status active">
● Сканер активен
</div>

{% elif status == "scanning" %}

<div class="status scanning">
● Сканирование...
</div>

{% elif status == "starting" %}

<div class="status starting">
● Сканер запускается...
</div>

{% else %}

<div class="status error">
● Ошибка получения цен
</div>

{% endif %}


<div class="info-box">

🎯 Минимальная чистая прибыль: 0.1%

<br>

💸 Торговые комиссии учитываются

<br>

🔄 Фоновое обновление каждые 15 секунд

<br>

💵 Расчёт сделки: $1000

{% if last_scan %}

<br>

🕒 Последний скан:
{{ last_scan }}

{% endif %}

</div>


{% if error %}

<div class="error-box">

⚠️ Диагностика сканера:

<br><br>

{{ error }}

</div>

{% endif %}


<div class="stats">

<div class="stat">

<div class="stat-value">
{{ opportunities_found }}
</div>

<div class="stat-label">
Возможностей найдено
</div>

</div>


<div class="stat">

<div class="stat-value">
{{ prices_received }}
</div>

<div class="stat-label">
Цен получено
</div>

</div>

</div>


{% if opportunities %}

{% for item in opportunities %}

<div class="card">

<div class="card-header">

<div class="symbol">
{{ item.symbol }}
</div>

<div class="spread">
Чистая: +{{ item.net_profit_percent }}%
</div>

</div>


<div class="trade-row">

<div class="buy">

<div class="label">
🟢 КУПИТЬ
</div>

<div class="exchange">
{{ item.buy_exchange }}
</div>

<div class="price">
${{ item.buy_price }}
</div>

</div>


<div class="sell">

<div class="label">
🔴 ПРОДАТЬ
</div>

<div class="exchange">
{{ item.sell_exchange }}
</div>

<div class="price">
${{ item.sell_price }}
</div>

</div>

</div>

</div>

{% endfor %}


{% else %}

<div class="empty">

🔍 Сейчас нет возможностей
с чистой прибылью от 0.1%.

<br><br>

Сканер продолжает работать в фоне.

</div>

{% endif %}


<div class="footer">

Страница обновляется автоматически •
Следующее сканирование через 15 секунд

</div>

</div>


<script>

setTimeout(function () {
    window.location.reload();
}, 15000);

</script>

</body>
</html>
    """, **data)


# =========================
# JSON API
# =========================

@app.route("/scan")
def scan():

    with state_lock:

        return jsonify({

            "status":
                scanner_state["status"],

            "opportunities_found":
                scanner_state[
                    "opportunities_found"
                ],

            "opportunities":
                scanner_state[
                    "opportunities"
                ],

            "prices_received":
                scanner_state[
                    "prices_received"
                ],

            "prices":
                scanner_state[
                    "prices"
                ],

            "last_scan":
                scanner_state[
                    "last_scan"
                ],

            "error":
                scanner_state[
                    "error"
                ],
        })


# =========================
# ЗАПУСК СЕРВЕРА
# =========================

if __name__ == "__main__":

    import os

    port = int(
        os.environ.get(
            "PORT",
            10000
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )