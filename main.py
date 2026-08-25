from flask import Flask, jsonify
import ccxt

app = Flask(__name__)

EXCHANGES = {
    "binance": ccxt.binance(),
    "bybit": ccxt.bybit(),
    "kucoin": ccxt.kucoin(),
}

SYMBOLS = [
    "BTC/USDT",
    "ETH/USDT",
    "SOL/USDT",
]


@app.route("/")
def home():
    return """
    <h1>Arbitrage Scanner 🚀</h1>
    <p>Сервис работает.</p>
    <p>Откройте <a href="/scan">/scan</a> для поиска цен.</p>
    """


@app.route("/scan")
def scan():
    results = []

    for symbol in SYMBOLS:
        prices = {}

        for name, exchange in EXCHANGES.items():
            try:
                ticker = exchange.fetch_ticker(symbol)
                price = ticker.get("last")

                if price:
                    prices[name] = price

            except Exception:
                pass

        if len(prices) >= 2:
            lowest_exchange = min(prices, key=prices.get)
            highest_exchange = max(prices, key=prices.get)

            lowest_price = prices[lowest_exchange]
            highest_price = prices[highest_exchange]

            spread = (
                (highest_price - lowest_price)
                / lowest_price
                * 100
            )

            results.append({
                "symbol": symbol,
                "buy_exchange": lowest_exchange,
                "buy_price": lowest_price,
                "sell_exchange": highest_exchange,
                "sell_price": highest_price,
                "spread_percent": round(spread, 2),
            })

    results.sort(
        key=lambda x: x["spread_percent"],
        reverse=True
    )

    return jsonify(results)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)