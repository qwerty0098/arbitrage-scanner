from flask import Flask

app = Flask(__name__)

@app.route("/")
def home():
    return """
    <h1>Arbitrage Scanner</h1>
    <p>Сервис успешно запущен на Render 🚀</p>
    """