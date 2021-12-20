import json
import requests
from kafka import KafkaProducer, errors
import os
from time import sleep

symbols = ("MSFT", "AAPL", "AMZN", "INTC", "GOOG", "FB", "NFLX", "TSLA")

from_date = 1631022248
to_date = 1631627048
topic = "topic1"

def connect(servers: str, retries: int = 10) -> KafkaProducer:
    while(True):
        if retries == 0:
            print("Terminated")
            exit()
        try:
            producer = KafkaProducer(bootstrap_servers=servers)
            break
        except errors.NoBrokersAvailable:
            print(f"No brokers avaialable at {servers}. Try {10 - retries}")
            sleep(5)
            retries -= 1
    print("Kafka servers connected succesfully")
    return producer

if __name__ == "__main__":
    producer = connect("kafka:9092")

    headers = {
        'X-Finnhub-Token': os.getenv("FINNHUB_API_KEY")
    }    
    for symbol in symbols:

        url = f"https://finnhub.io/api/v1/stock/candle?symbol={symbol}&resolution=1&from={from_date}&to={to_date}"
        while(True):
            try:
                response = requests.request("GET", url, headers=headers)
                break
            except requests.exceptions.Timeout:
                continue
            except requests.exceptions.RequestException as e:
                raise exit(e)

        r = producer.send(topic, json.dumps({"company_name": symbol, "data": response.text}).encode())
        producer.flush()
        print(f"{symbol} r.succeeded()")
