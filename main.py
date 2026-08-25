from flask import Flask, jsonify, render_template_string
import ccxt
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import permutations

app = Flask(__name__)


# =========================================================
# НАСТРОЙКИ
# =========================================================

EXCHANGE_NAMES = [
    "kraken",
    "kucoin",
    "bitget",
    "bybit",
]

# 8 монет
SYMBOLS = [
    "BTC/USDT",
    "ETH/USDT",
    "SOL/USDT",
    "XRP/USDT",
    "BNB/USDT",
    "DOGE/USDT",
    "ADA/USDT",
    "AVAX/USDT",
]

# Как часто запускать новый скан
SCAN_INTERVAL = 15

# Таймаут запросов к биржам
REQUEST_TIMEOUT = 8000

# Комиссия на покупку
BUY_FEE = 0.001

# Комиссия на продажу
SELL_FEE = 0.001

# Минимальная ЧИСТАЯ прибыль после комиссий
# 0.01 = минимум +0.01%
MIN_NET_PROFIT = 0.01

# Сумма расчёта сделки
TRADE_AMOUNT = 1000

# Максимальное количество потоков
MAX_WORKERS = 4


# =========================================================
# ОБЩЕЕ СОСТОЯНИЕ СКАНЕРА
# =========================================================

scanner_state = {
    "status": "starting",
    "opportunities": [],
    "prices": [],
    "opportunities_found": 0,
    "prices_received": 0,
    "last_scan": None,
    "error": None,
    "diagnostics": {},
}

state_lock = threading.Lock()
scan_lock = threading.Lock()


# =========================================================
# СОЗДАНИЕ БИРЖИ
# =========================================================

def get_exchange(name):

    exchange_class = getattr(ccxt, name)

    return exchange_class({
        "enableRateLimit": True,
        "timeout": REQUEST_TIMEOUT,
    })


# =========================================================
# СКАНИРОВАНИЕ ОДНОЙ БИРЖИ
# =========================================================

def scan_exchange(exchange_name):

    results = []

    diagnostic = {
        "status": "starting",
        "prices_received": 0,
        "errors": [],
    }

    exchange = None

    try:

        exchange = get_exchange(exchange_name)

        diagnostic["status"] = "loading_markets"

        markets = exchange.load_markets()

        diagnostic["status"] = "scanning"

        for symbol in SYMBOLS:

            try:

                if symbol not in markets:

                    diagnostic["errors"].append(
                        f"{symbol}: пара недоступна"
                    )

                    continue

                ticker = exchange.fetch_ticker(symbol)

                bid = ticker.get("bid")
                ask = ticker.get("ask")

                if bid is None or ask is None:

                    diagnostic["errors"].append(
                        f"{symbol}: нет bid/ask"
                    )

                    continue

                bid = float(bid)
                ask = float(ask)

                if bid <= 0 or ask <= 0:

                    diagnostic["errors"].append(
                        f"{symbol}: некорректная цена"
                    )

                    continue

                results.append({
                    "exchange": exchange_name,
                    "symbol": symbol,
                    "bid": bid,
                    "ask": ask,
                })

            except Exception as error:

                diagnostic["errors"].append(
                    f"{symbol}: {str(error)[:120]}"
                )

        diagnostic["prices_received"] = len(results)

        if results:
            diagnostic["status"] = "success"
        else:
            diagnostic["status"] = "no_prices"

    except Exception as error:

        diagnostic["status"] = "exchange_error"

        diagnostic["errors"].append(
            str(error)[:200]
        )

    finally:

        try:

            if exchange is not None:
                exchange.close()

        except Exception:
            pass

    return results, diagnostic


# =========================================================
# РАСЧЁТ АРБИТРАЖНОЙ СДЕЛКИ
# =========================================================

def calculate_trade(buy_price, sell_price):

    # Сколько USDT реально уходит на покупку
    total_buy_cost = TRADE_AMOUNT

    # Сумма самой покупки без комиссии
    amount_before_fee = (
        total_buy_cost
        / (1 + BUY_FEE)
    )

    # Количество монет, которое покупаем
    coins_bought = (
        amount_before_fee
        / buy_price
    )

    # Сколько получаем после продажи
    gross_revenue = (
        coins_bought
        * sell_price
    )

    # Выручка после комиссии продажи
    net_revenue = (
        gross_revenue
        * (1 - SELL_FEE)
    )

    # Чистая прибыль в USDT
    profit_usdt = (
        net_revenue
        - TRADE_AMOUNT
    )

    # Чистая прибыль в процентах
    net_profit_percent = (
        profit_usdt
        / TRADE_AMOUNT
    ) * 100

    # Валовый спред без комиссий
    gross_spread_percent = (
        (sell_price - buy_price)
        / buy_price
    ) * 100

    return {
        "gross_spread_percent":
            gross_spread_percent,

        "net_profit_percent":
            net_profit_percent,

        "profit_usdt":
            profit_usdt,

        "coins_bought":
            coins_bought,
    }


# =========================================================
# ПОИСК ВСЕХ АРБИТРАЖНЫХ ВОЗМОЖНОСТЕЙ
# =========================================================

def get_opportunities():

    all_prices = []
    diagnostics = {}

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        futures = {
            executor.submit(
                scan_exchange,
                exchange_name
            ): exchange_name

            for exchange_name
            in EXCHANGE_NAMES
        }

        for future in as_completed(futures):

            exchange_name = futures[future]

            try:

                prices, diagnostic = future.result()

                all_prices.extend(prices)

                diagnostics[
                    exchange_name
                ] = diagnostic

            except Exception as error:

                diagnostics[
                    exchange_name
                ] = {
                    "status": "future_error",
                    "prices_received": 0,
                    "errors": [
                        str(error)[:200]
                    ],
                }

    opportunities = []

    # =====================================================
    # ПРОВЕРЯЕМ ВСЕ КОМБИНАЦИИ БИРЖ
    # =====================================================

    for symbol in SYMBOLS:

        symbol_prices = [
            item
            for item in all_prices
            if item["symbol"] == symbol
        ]

        if len(symbol_prices) < 2:
            continue

        best_opportunity = None

        # Проверяем каждую комбинацию:
        # купить на одной бирже -> продать на другой
        for buy_exchange, sell_exchange in permutations(
            symbol_prices,
            2
        ):

            if (
                buy_exchange["exchange"]
                == sell_exchange["exchange"]
            ):
                continue

            buy_price = buy_exchange["ask"]
            sell_price = sell_exchange["bid"]

            trade = calculate_trade(
                buy_price,
                sell_price
            )

            # ВАЖНО:
            # Пропускаем всё, что не приносит
            # минимальную чистую прибыль
            if (
                trade["net_profit_percent"]
                < MIN_NET_PROFIT
            ):
                continue

            opportunity = {
                "symbol": symbol,

                "buy_exchange":
                    buy_exchange[
                        "exchange"
                    ].upper(),

                "buy_price":
                    round(buy_price, 8),

                "sell_exchange":
                    sell_exchange[
                        "exchange"
                    ].upper(),

                "sell_price":
                    round(sell_price, 8),

                "gross_spread_percent":
                    round(
                        trade[
                            "gross_spread_percent"
                        ],
                        4
                    ),

                "net_profit_percent":
                    round(
                        trade[
                            "net_profit_percent"
                        ],
                        4
                    ),

                "profit_usdt":
                    round(
                        trade[
                            "profit_usdt"
                        ],
                        2
                    ),

                "coins_bought":
                    round(
                        trade[
                            "coins_bought"
                        ],
                        8
                    ),
            }

            # Для каждой монеты оставляем
            # только самую прибыльную комбинацию
            if (
                best_opportunity is None
                or opportunity[
                    "net_profit_percent"
                ] > best_opportunity[
                    "net_profit_percent"
                ]
            ):

                best_opportunity = opportunity

        # Добавляем только реально прибыльную сделку
        if best_opportunity is not None:

            opportunities.append(
                best_opportunity
            )

    # Самые прибыльные сверху
    opportunities.sort(
        key=lambda item:
            item["net_profit_percent"],
        reverse=True
    )

    return (
        opportunities,
        all_prices,
        diagnostics
    )


# =========================================================
# ОДИН ФОНОВЫЙ СКАН
# =========================================================

def run_scan():

    if not scan_lock.acquire(
        blocking=False
    ):
        return

    try:

        with state_lock:

            scanner_state["status"] = "scanning"
            scanner_state["error"] = None

        (
            opportunities,
            prices,
            diagnostics
        ) = get_opportunities()

        with state_lock:

            scanner_state[
                "opportunities"
            ] = opportunities

            scanner_state[
                "prices"
            ] = prices

            scanner_state[
                "opportunities_found"
            ] = len(opportunities)

            scanner_state[
                "prices_received"
            ] = len(prices)

            scanner_state[
                "diagnostics"
            ] = diagnostics

            scanner_state[
                "last_scan"
            ] = time.strftime(
                "%d.%m.%Y %H:%M:%S"
            )

            scanner_state["status"] = "active"

            scanner_state["error"] = None

    except Exception as error:

        with state_lock:

            scanner_state["status"] = "error"

            scanner_state["error"] = str(
                error
            )

    finally:

        scan_lock.release()


# =========================================================
# ПОСТОЯННЫЙ ФОНОВЫЙ СКАНЕР
# =========================================================

def scanner_loop():

    run_scan()

    while True:

        time.sleep(SCAN_INTERVAL)

        run_scan()


# =========================================================
# ЗАПУСК ФОНОВОГО ПОТОКА
# =========================================================

scanner_thread = threading.Thread(
    target=scanner_loop,
    daemon=True
)

scanner_thread.start()


# =========================================================
# ГЛАВНАЯ СТРАНИЦА
# =========================================================

@app.route("/")
def home():

    with state_lock:

        data = {
            "status":
                scanner_state["status"],

            "opportunities":
                list(
                    scanner_state[
                        "opportunities"
                    ]
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

            "diagnostics":
                dict(
                    scanner_state[
                        "diagnostics"
                    ]
                ),

            "min_net_profit":
                MIN_NET_PROFIT,

            "trade_amount":
                TRADE_AMOUNT,

            "scan_interval":
                SCAN_INTERVAL,
        }

    return render_template_string(
        """
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

.symbol {
    font-size: 30px;
    font-weight: bold;
    margin-bottom: 20px;
}

.profit-box {
    background: #064e3b;
    color: #6ee7b7;
    border-radius: 18px;
    padding: 18px;
    margin-bottom: 20px;
}

.net-profit {
    font-size: 26px;
    font-weight: bold;
}

.gross-profit {
    margin-top: 8px;
    font-size: 18px;
    color: #a7f3d0;
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

.result {
    margin-top: 20px;
    color: #aab4c4;
    font-size: 20px;
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

.diagnostics {
    margin-top: 25px;
    background: #151e2e;
    border: 1px solid #26364f;
    border-radius: 20px;
    padding: 20px;
}

.diagnostics h2 {
    margin-top: 0;
    font-size: 22px;
}

.diagnostic-item {
    padding: 15px;
    border-top: 1px solid #26364f;
}

.diagnostic-item:first-of-type {
    border-top: none;
}

.diag-name {
    font-size: 18px;
    font-weight: bold;
}

.diag-status {
    color: #60a5fa;
    margin-top: 5px;
}

.diag-prices {
    color: #6ee7b7;
    margin-top: 5px;
}

.diag-errors {
    color: #fca5a5;
    margin-top: 8px;
    font-size: 14px;
    word-break: break-word;
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
}

</style>

</head>

<body>

<div class="container">

<h1>🚀 Arbitrage Scanner</h1>

<div class="subtitle">
Мониторинг реальных арбитражных возможностей между биржами
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
● Ошибка сканирования
</div>

{% endif %}


<div class="info-box">

🎯 Минимальная чистая прибыль:
{{ min_net_profit }}%

<br>

💸 Комиссия покупки:
{{ BUY_FEE if BUY_FEE else "учитывается" }}

<br>

💸 Торговые комиссии учитываются с обеих сторон

<br>

🔄 Новое обновление каждые
{{ scan_interval }} секунд

<br>

💵 Расчёт сделки:
${{ trade_amount }}

{% if last_scan %}

<br>

🕒 Последний скан:
{{ last_scan }}

{% endif %}

</div>


<div class="stats">

<div class="stat">

<div class="stat-value">
{{ opportunities_found }}
</div>

<div class="stat-label">
Прибыльных возможностей
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

<div class="symbol">
{{ item.symbol }}
</div>


<div class="profit-box">

<div class="net-profit">
🟢 Чистая: +{{ item.net_profit_percent }}%
</div>

<div class="gross-profit">
Валовый спред:
+{{ item.gross_spread_percent }}%
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


<div class="result">
💰 Результат на ${{ trade_amount }}:
<strong>+${{ item.profit_usdt }}</strong>
</div>

</div>

{% endfor %}


{% else %}

<div class="empty">

🔍 Сейчас нет реально прибыльных
арбитражных возможностей.

<br><br>

Все убыточные сделки автоматически
отфильтрованы.

<br><br>

Сканер продолжает проверять
8 монет на 4 биржах.

</div>

{% endif %}


<div class="diagnostics">

<h2>🔧 Диагностика бирж</h2>

{% for name, diag in diagnostics.items() %}

<div class="diagnostic-item">

<div class="diag-name">
{{ name.upper() }}
</div>

<div class="diag-status">
Статус: {{ diag.status }}
</div>

<div class="diag-prices">
Цен получено:
{{ diag.prices_received }}
</div>

{% if diag.errors %}

<div class="diag-errors">

{% for error in diag.errors[:3] %}

• {{ error }}<br>

{% endfor %}

</div>

{% endif %}

</div>

{% endfor %}

</div>


{% if error %}

<div class="diagnostics">

<h2>❌ Общая ошибка</h2>

<div class="diag-errors">
{{ error }}
</div>

</div>

{% endif %}


<div class="footer">

Автоматическое обновление страницы каждые
{{ scan_interval }} секунд

</div>

</div>


<script>

setTimeout(
    function () {
        window.location.reload();
    },
    {{ scan_interval * 1000 }}
);

</script>

</body>
</html>
        """,
        BUY_FEE=BUY_FEE * 100,
        **data
    )


# =========================================================
# JSON API
# =========================================================

@app.route("/scan")
def scan():

    with state_lock:

        return jsonify({

            "status":
                scanner_state["status"],

            "min_net_profit":
                MIN_NET_PROFIT,

            "trade_amount":
                TRADE_AMOUNT,

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

            "diagnostics":
                scanner_state[
                    "diagnostics"
                ],

            "error":
                scanner_state[
                    "error"
                ],
        })


# =========================================================
# ЗАПУСК FLASK
# =========================================================

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
        port=port,
        debug=False
    )