import os
import time
import json
import requests
from datetime import datetime
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData

# Load environment variables

load_dotenv()

API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
CONNECTION_STRING = os.getenv("EVENT_HUBS_CONNECTION_STRING")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")

# Stocks to track

SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]

def fetch_stock_price(symbol: str) -> dict:
    """Fetch latest stock price from Alpha Vantage API"""
    url = f"https://www.alphavantage.co/query"
    params = {
        "function": "GLOBAL_QUOTE",
        "symbol": symbol,
        "apikey": API_KEY
    }
    response = requests.get(url, params=params)
    data = response.json()

    quote = data.get("Global Quote", {})

    return {
        "symbol": symbol,
        "price": float(quote.get("05. price", 0)),
        "volume": int(quote.get("06. volume", 0)),
        "change_percent": quote.get("10. change percent", "0%"),
        "timestamp": datetime.utcnow().isoformat()
    }

def send_to_event_hub(producer: EventHubProducerClient, events: list):
    """Send a batch of stock events to Azure Event Hubs"""
    event_batch = producer.create_batch()
    for event in events:
        event_batch.add(EventData(json.dumps(event)))
    producer.send_batch(event_batch)
    print(f"Sent {len(events)} events to Event Hubs")


def main():
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        eventhub_name=EVENT_HUB_NAME
    )

    print("Starting stock price producer...")

    with producer:
        while True:
            events = []
            for symbol in SYMBOLS:
                try:
                    stock_data = fetch_stock_price(symbol)
                    events.append(stock_data)
                    print(f"{symbol}: ${stock_data['price']} | {stock_data['timestamp']}")
                except Exception as e:
                    print(f"Error fetching {symbol}: {e}")

            if events:
                send_to_event_hub(producer, events)

            # Alpha Vantage free tier allows 5 API calls/min
            # We have 5 symbols so we wait 60 seconds
            print("Waiting 60 seconds...\n")
            time.sleep(60)

if __name__ == "__main__":
    main()


