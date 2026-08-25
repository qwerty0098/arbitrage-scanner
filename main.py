from flask import Flask, jsonify, render_template_string
import ccxt

app = Flask(__name__)

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


def get_exchange(name):
    exchange_class = getattr(ccxt, name)

    return exchange_class({
        "enableRateLimit": True,
        "timeout": 10000,
    })


def get_opportunities():
    results = []
    all_prices = []

    for exchange_name in exchange_names:
        try:
            exchange = get_exchange(exchange_name)

            for symbol in symbols:
                try:
                    ticker = exchange.fetch_ticker(symbol)

                    bid = ticker.get("bid")
                    ask = ticker.get("ask")

                    if bid and ask:
                        all_prices.append({
                            "exchange": exchange_name,
                            "symbol": symbol,
                            "bid": bid,
                            "ask": ask
                        })

                except Exception:
                    continue

        except Exception:
            continue

    for symbol in symbols:
        prices = [
            item for item in all_prices
            if item["symbol"] == symbol
        ]

        if len(prices) < 2:
            continue

        buy_exchange = min(prices, key=lambda x: x["ask"])
        sell_exchange = max(prices, key=lambda x: x["bid"])

        buy_price = buy_exchange["ask"]
        sell_price = sell_exchange["bid"]

        gross_spread = (
            (sell_price - buy_price) / buy_price
        ) * 100

        if gross_spread > 0:
            results.append({
                "symbol": symbol,
                "buy_exchange": buy_exchange["exchange"].upper(),
                "buy_price": round(buy_price, 4),
                "sell_exchange": sell_exchange["exchange"].upper(),
                "sell_price": round(sell_price, 4),
                "gross_spread_percent": round(gross_spread, 3)
            })

    results.sort(
        key=lambda x: x["gross_spread_percent"],
        reverse=True
    )

    return results, all_prices


@app.route("/")
def home():
    opportunities, prices = get_opportunities()

    return render_template_string("""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="refresh" content="30">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">

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
            font-size: 32px;
            margin-bottom: 8px;
        }

        .subtitle {
            color: #9ca3af;
            margin-bottom: 30px;
        }

        .status {
            display: inline-block;
            background: #064e3b;
            color: #6ee7b7;
            padding: 8px 14px;
            border-radius: 20px;
            margin-bottom: 25px;
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
            border-radius: 14px;
            border: 1px solid #1f2937;
        }

        .stat-value {
            font-size: 28px;
            font-weight: bold;
            color: #60a5fa;
        }

        .stat-label {
            color: #9ca3af;
            margin-top: 5px;
        }

        .card {
            background: #111827;
            border: 1px solid #1f2937;
            border-radius: 16px;
            padding: 20px;
            margin-bottom: 15px;
        }

        .card-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
        }

        .symbol {
            font-size: 24px;
            font-weight: bold;
        }

        .spread {
            background: #065f46;
            color: #6ee7b7;
            padding: 8px 14px;
            border-radius: 20px;
            font-weight: bold;
        }

        .trade-row {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 15px;
        }

        .buy, .sell {
            padding: 15px;
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
            font-size: 13px;
            margin-bottom: 8px;
        }

        .exchange {
            font-size: 20px;
            font-weight: bold;
        }

        .price {
            font-size: 18px;
            margin-top: 5px;
        }

        .empty {
            text-align: center;
            background: #111827;
            padding: 40px;
            border-radius: 16px;
            color: #9ca3af;
        }

        .footer {
            text-align: center;
            color: #6b7280;
            margin-top: 30px;
            font-size: 14px;
        }

        @media (max-width: 600px) {
            body {
                padding: 15px;
            }

            .stats {
                flex-direction: column;
            }

            .trade-row {
                grid-template-columns: 1fr;
            }

            h1 {
                font-size: 26px;
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

    <div class="status">
        ● Сканер активен
    </div>

    <div class="stats">
        <div class="stat">
            <div class="stat-value">{{ opportunities|length }}</div>
            <div class="stat-label">Возможностей найдено</div>
        </div>

        <div class="stat">
            <div class="stat-value">{{ prices|length }}</div>
            <div class="stat-label">Цен проверено</div>
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
                    +{{ item.gross_spread_percent }}%
                </div>
            </div>

            <div class="trade-row">

                <div class="buy">
                    <div class="label">🟢 КУПИТЬ</div>
                    <div class="exchange">
                        {{ item.buy_exchange }}
                    </div>
                    <div class="price">
                        ${{ item.buy_price }}
                    </div>
                </div>

                <div class="sell">
                    <div class="label">🔴 ПРОДАТЬ</div>
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
            🔍 Сейчас подходящих ценовых расхождений не найдено.
            <br><br>
            Страница обновится автоматически через 30 секунд.
        </div>

    {% endif %}

    <div class="footer">
        Данные обновляются автоматически каждые 30 секунд •
        <a href="/scan" style="color:#60a5fa;">JSON API</a>
    </div>

</div>
</body>
</html>
    """, opportunities=opportunities, prices=prices)


@app.route("/scan")
def scan():
    opportunities, prices = get_opportunities()

    return jsonify({
        "opportunities_found": len(opportunities),
        "opportunities": opportunities,
        "prices_checked": prices
    })


if __name__ == "__main__":
    import os

    port = int(os.environ.get("PORT", 10000))

    app.run(
        host="0.0.0.0",
        port=port
    )