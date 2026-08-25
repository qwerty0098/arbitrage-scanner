from flask import Flask, jsonify, render_template_string
import ccxt
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__)

# ==========================================
# НАСТРОЙКИ
# ==========================================

exchange_names = [
    "kraken",
    "kucoin",
    "bitget",
    "bybit",
]

symbols = [
    "BTC/USDT",
    "ETH/USDT",
    "SOL/USDT",
    "XRP/USDT",
]

TRADE_AMOUNT_USD = 1000

MIN_NET_PROFIT_PERCENT = 0.1

SCAN_INTERVAL = 15

EXCHANGE_FEES = {
    "kraken": 0.0026,
    "kucoin": 0.0010,
    "bitget": 0.0010,
    "bybit": 0.0010,
}


# ==========================================
# КЭШ РЕЗУЛЬТАТОВ
# ==========================================

cache_lock = threading.Lock()

scan_cache = {
    "opportunities": [],
    "prices": [],
    "last_scan": None,
    "scan_time": 0,
    "status": "starting",
    "error": None,
}


# ==========================================
# ПОДКЛЮЧЕНИЕ К БИРЖЕ
# ==========================================

def get_exchange(name):

    exchange_class = getattr(ccxt, name)

    return exchange_class({
        "enableRateLimit": True,
        "timeout": 5000,
    })


# ==========================================
# ПОЛУЧЕНИЕ ОДНОЙ ЦЕНЫ
# ==========================================

def fetch_one_price(exchange_name, symbol):

    try:

        exchange = get_exchange(exchange_name)

        ticker = exchange.fetch_ticker(symbol)

        bid = ticker.get("bid")
        ask = ticker.get("ask")

        if bid and ask and bid > 0 and ask > 0:

            return {
                "exchange": exchange_name,
                "symbol": symbol,
                "bid": float(bid),
                "ask": float(ask),
            }

    except Exception as e:
        print(
            f"Ошибка {exchange_name} {symbol}: {str(e)[:100]}"
        )

    return None


# ==========================================
# СКАНИРОВАНИЕ
# ==========================================

def scan_market():

    start_time = time.time()

    all_prices = []

    tasks = []

    # Создаём все задачи
    for exchange_name in exchange_names:
        for symbol in symbols:
            tasks.append(
                (exchange_name, symbol)
            )

    # Выполняем запросы параллельно
    with ThreadPoolExecutor(
        max_workers=8
    ) as executor:

        futures = {
            executor.submit(
                fetch_one_price,
                exchange_name,
                symbol
            ): (
                exchange_name,
                symbol
            )

            for exchange_name, symbol in tasks
        }

        for future in as_completed(futures):

            try:

                result = future.result(
                    timeout=7
                )

                if result:
                    all_prices.append(result)

            except Exception:
                continue

    results = []

    # ==========================================
    # ИЩЕМ АРБИТРАЖ
    # ==========================================

    for symbol in symbols:

        prices = [
            item
            for item in all_prices
            if item["symbol"] == symbol
        ]

        if len(prices) < 2:
            continue

        buy_exchange = min(
            prices,
            key=lambda x: x["ask"]
        )

        sell_exchange = max(
            prices,
            key=lambda x: x["bid"]
        )

        # Не покупаем и не продаём на одной бирже
        if (
            buy_exchange["exchange"]
            == sell_exchange["exchange"]
        ):
            continue

        buy_price = buy_exchange["ask"]

        sell_price = sell_exchange["bid"]

        # Валовый спред
        gross_spread_percent = (
            (
                sell_price - buy_price
            )
            / buy_price
        ) * 100

        buy_fee = EXCHANGE_FEES.get(
            buy_exchange["exchange"],
            0.001
        )

        sell_fee = EXCHANGE_FEES.get(
            sell_exchange["exchange"],
            0.001
        )

        # Более точный расчёт чистой прибыли
        quantity = (
            TRADE_AMOUNT_USD
            / buy_price
        )

        buy_fee_usd = (
            TRADE_AMOUNT_USD
            * buy_fee
        )

        gross_sell_value = (
            quantity
            * sell_price
        )

        sell_fee_usd = (
            gross_sell_value
            * sell_fee
        )

        net_profit_usd = (
            gross_sell_value
            - TRADE_AMOUNT_USD
            - buy_fee_usd
            - sell_fee_usd
        )

        net_profit_percent = (
            net_profit_usd
            / TRADE_AMOUNT_USD
        ) * 100

        if (
            net_profit_percent
            < MIN_NET_PROFIT_PERCENT
        ):
            continue

        results.append({

            "symbol": symbol,

            "buy_exchange":
                buy_exchange["exchange"].upper(),

            "buy_price":
                round(buy_price, 6),

            "sell_exchange":
                sell_exchange["exchange"].upper(),

            "sell_price":
                round(sell_price, 6),

            "gross_spread_percent":
                round(
                    gross_spread_percent,
                    3
                ),

            "net_profit_percent":
                round(
                    net_profit_percent,
                    3
                ),

            "net_profit_usd":
                round(
                    net_profit_usd,
                    2
                ),

            "trade_amount":
                TRADE_AMOUNT_USD,

            "buy_fee_percent":
                round(
                    buy_fee * 100,
                    3
                ),

            "sell_fee_percent":
                round(
                    sell_fee * 100,
                    3
                ),
        })

    # Самые прибыльные сверху
    results.sort(
        key=lambda x: x[
            "net_profit_percent"
        ],
        reverse=True
    )

    scan_time = round(
        time.time() - start_time,
        2
    )

    return (
        results,
        all_prices,
        scan_time
    )


# ==========================================
# ФОНОВЫЙ СКАНЕР
# ==========================================

def background_scanner():

    print("Фоновый сканер запущен")

    while True:

        try:

            with cache_lock:

                scan_cache["status"] = (
                    "scanning"
                )

            opportunities, prices, scan_time = (
                scan_market()
            )

            with cache_lock:

                scan_cache[
                    "opportunities"
                ] = opportunities

                scan_cache[
                    "prices"
                ] = prices

                scan_cache[
                    "last_scan"
                ] = time.strftime(
                    "%H:%M:%S"
                )

                scan_cache[
                    "scan_time"
                ] = scan_time

                scan_cache[
                    "status"
                ] = "active"

                scan_cache[
                    "error"
                ] = None

            print(
                f"Сканирование завершено: "
                f"{len(opportunities)} возможностей, "
                f"{len(prices)} цен, "
                f"{scan_time} сек"
            )

        except Exception as e:

            print(
                f"Ошибка сканера: {str(e)}"
            )

            with cache_lock:

                scan_cache[
                    "status"
                ] = "error"

                scan_cache[
                    "error"
                ] = str(e)

        # Ждём перед следующим сканированием
        time.sleep(
            SCAN_INTERVAL
        )


# ==========================================
# ЗАПУСК ФОНОВОГО СКАНЕРА
# ==========================================

scanner_thread = threading.Thread(
    target=background_scanner,
    daemon=True
)

scanner_thread.start()


# ==========================================
# ГЛАВНАЯ СТРАНИЦА
# ==========================================

@app.route("/")
def home():

    with cache_lock:

        opportunities = (
            scan_cache[
                "opportunities"
            ].copy()
        )

        prices = (
            scan_cache[
                "prices"
            ].copy()
        )

        status = (
            scan_cache["status"]
        )

        last_scan = (
            scan_cache["last_scan"]
        )

        scan_time = (
            scan_cache["scan_time"]
        )

    return render_template_string("""

<!DOCTYPE html>
<html lang="ru">

<head>

<meta charset="UTF-8">

<meta
name="viewport"
content="width=device-width, initial-scale=1.0"
>

<meta http-equiv="refresh" content="15">

<title>Arbitrage Scanner</title>

<style>

* {
    box-sizing: border-box;
}

body {
    margin: 0;
    padding: 20px;
    font-family: Arial, sans-serif;
    background: #0b0f19;
    color: #ffffff;
}

.container {
    max-width: 900px;
    margin: auto;
}

h1 {
    font-size: 34px;
    margin-bottom: 8px;
}

.subtitle {
    color: #9ca3af;
    font-size: 18px;
    margin-bottom: 25px;
}

.status {
    display: inline-block;
    background: #064e3b;
    color: #6ee7b7;
    padding: 12px 18px;
    border-radius: 20px;
    margin-bottom: 25px;
    font-size: 17px;
}

.status-starting {
    background: #78350f;
    color: #fbbf24;
}

.info-box {
    background: #111827;
    border: 1px solid #1f2937;
    border-radius: 16px;
    padding: 20px;
    margin-bottom: 25px;
    color: #9ca3af;
    line-height: 1.8;
}

.stats {
    display: flex;
    gap: 15px;
    margin-bottom: 25px;
}

.stat {
    flex: 1;
    background: #111827;
    padding: 20px;
    border-radius: 16px;
    border: 1px solid #1f2937;
}

.stat-value {
    font-size: 32px;
    font-weight: bold;
    color: #60a5fa;
}

.stat-label {
    color: #9ca3af;
    margin-top: 8px;
}

.card {
    background: #111827;
    border: 1px solid #1f2937;
    border-radius: 16px;
    padding: 20px;
    margin-bottom: 18px;
}

.card-header {
    display: flex;
    justify-content: space-between;
    gap: 15px;
    margin-bottom: 20px;
}

.symbol {
    font-size: 25px;
    font-weight: bold;
}

.spread {
    background: #065f46;
    color: #6ee7b7;
    padding: 9px 15px;
    border-radius: 20px;
    font-weight: bold;
    white-space: nowrap;
}

.trade-row {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 15px;
}

.buy,
.sell {
    padding: 18px;
    border-radius: 12px;
}

.buy {
    background: #172554;
}

.sell {
    background: #3f1d2e;
}

.label {
    color: #9ca3af;
    font-size: 14px;
    margin-bottom: 10px;
}

.exchange {
    font-size: 22px;
    font-weight: bold;
}

.price {
    font-size: 20px;
    margin-top: 8px;
}

.profit-box {
    margin-top: 18px;
    background: #064e3b;
    border-radius: 12px;
    padding: 16px;
}

.profit-title {
    color: #9ca3af;
}

.profit-value {
    font-size: 28px;
    color: #6ee7b7;
    font-weight: bold;
    margin-top: 5px;
}

.details {
    margin-top: 10px;
    color: #9ca3af;
    font-size: 13px;
}

.empty {
    text-align: center;
    background: #111827;
    padding: 45px 25px;
    border-radius: 16px;
    color: #9ca3af;
    border: 1px solid #1f2937;
    font-size: 18px;
    line-height: 1.7;
}

.footer {
    text-align: center;
    color: #6b7280;
    margin-top: 30px;
}

.footer a {
    color: #60a5fa;
}

@media (max-width: 600px) {

    body {
        padding: 15px;
    }

    h1 {
        font-size: 28px;
    }

    .stats {
        flex-direction: column;
    }

    .trade-row {
        grid-template-columns: 1fr;
    }

    .card-header {
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

<div class="status">
● Сканер активен
</div>

{% else %}

<div class="status status-starting">
● Сканер запускается...
</div>

{% endif %}


<div class="info-box">

🎯 Минимальная чистая прибыль:
{{ min_profit }}%

<br>

💸 Торговые комиссии учитываются

<br>

🔄 Фоновое обновление каждые
15 секунд

<br>

💵 Расчёт сделки:
${{ trade_amount }}

</div>


<div class="stats">

<div class="stat">

<div class="stat-value">
{{ opportunities|length }}
</div>

<div class="stat-label">
Возможностей найдено
</div>

</div>


<div class="stat">

<div class="stat-value">
{{ prices|length }}
</div>

<div class="stat-label">
Цен получено
</div>

</div>


<div class="stat">

<div class="stat-value">
{{ scan_time }}
</div>

<div class="stat-label">
Секунд на сканирование
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


<div class="profit-box">

<div class="profit-title">
💵 Примерная чистая прибыль
с ${{ item.trade_amount }}
</div>

<div class="profit-value">
+${{ item.net_profit_usd }}
</div>

<div class="details">

Валовый спред:
{{ item.gross_spread_percent }}%

•

Комиссии уже учтены

</div>

</div>

</div>

{% endfor %}


{% else %}

<div class="empty">

🔍 Пока подходящих возможностей нет.

<br><br>

{% if status != "active" %}

Сканер получает первые данные с бирж.

{% else %}

Все найденные варианты после комиссий
дают чистую прибыль менее
{{ min_profit }}%.

{% endif %}

</div>

{% endif %}


<div class="footer">

Последнее сканирование:
{{ last_scan or "ожидание..." }}

•

<a href="/scan">
JSON API
</a>

</div>

</div>

</body>
</html>

    """,

    opportunities=opportunities,
    prices=prices,
    status=status,
    last_scan=last_scan,
    scan_time=scan_time,
    min_profit=MIN_NET_PROFIT_PERCENT,
    trade_amount=TRADE_AMOUNT_USD
    )


# ==========================================
# JSON API
# ==========================================

@app.route("/scan")
def scan():

    with cache_lock:

        return jsonify({
            "status": scan_cache["status"],
            "last_scan": scan_cache["last_scan"],
            "scan_time": scan_cache["scan_time"],
            "trade_amount_usd":
                TRADE_AMOUNT_USD,
            "min_net_profit_percent":
                MIN_NET_PROFIT_PERCENT,
            "opportunities_found":
                len(
                    scan_cache[
                        "opportunities"
                    ]
                ),
            "opportunities":
                scan_cache[
                    "opportunities"
                ],
            "prices_checked":
                len(
                    scan_cache[
                        "prices"
                    ]
                ),
        })


# ==========================================
# ЗАПУСК
# ==========================================

if __name__ == "__main__":

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