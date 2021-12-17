import json
import requests
import os

symbols = ("MSFT", "AAPL", "AMZN", "INTC", "GOOG", "FB", "NFLX", "TSLA")
from_date = 1631022248
to_date = 1631627048
topic = "topic1"

if __name__ == "__main__":
    headers = {
        'X-Finnhub-Token': os.getenv("FINNHUB_API_KEY")
    }
    for symbol in symbols:

        url = f"https://finnhub.io/api/v1/stock/candle?symbol={symbol}&resolution=1&from={from_date}&to={to_date}"
        response = requests.request("GET", url, headers=headers)
        print(response.text)
