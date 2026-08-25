from flask import Flask, jsonify
import ccxt

app = Flask(__name__)

# Биржи, которые будем сравнивать
exchange_names = [
    "kraken",
    "kucoin",
    "bitget",
    "bybit",
]

# Криптовалютные пары для проверки
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


@app.route("/")
def home():
    return """
    <h1>Arbitrage Scanner 🚀</h1>
    <p>Сервис работает.</p>
    <p>Откройте <a href="/scan">/scan</a> для поиска арбитражных возможностей.</p>
    """


@app.route("/scan")
def scan():
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

    # Группируем цены по торговым парам
    for symbol in symbols:
        prices = [
            item for item in all_prices
            if item["symbol"] == symbol
        ]

        if len(prices) < 2:
            continue

        # Где дешевле купить
        buy_exchange = min(prices, key=lambda x: x["ask"])

        # Где дороже продать
        sell_exchange = max(prices, key=lambda x: x["bid"])

        buy_price = buy_exchange["ask"]
        sell_price = sell_exchange["bid"]

        profit_percent = (
            (sell_price - buy_price) / buy_price
        ) * 100

        if profit_percent > 0:
            results.append({
                "symbol": symbol,
                "buy_exchange": buy_exchange["exchange"],
                "buy_price": round(buy_price, 6),
                "sell_exchange": sell_exchange["exchange"],
                "sell_price": round(sell_price, 6),
                "gross_spread_percent": round(profit_percent, 3)
            })

    # Сначала самые большие расхождения
    results.sort(
        key=lambda x: x["gross_spread_percent"],
        reverse=True
    )

    return jsonify({
        "opportunities_found": len(results),
        "opportunities": results,
        "prices_checked": all_prices
    })


if __name__ == "__main__":
    import os

    port = int(os.environ.get("PORT", 10000))

    app.run(
        host="0.0.0.0",
        port=port
    )