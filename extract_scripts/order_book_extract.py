import requests
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime

# Load PostgreSQL credentials from .env
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "dbname": os.getenv("PG_DATABASE")
}

# Connect to PostgreSQL
def connect_pg():
    return psycopg2.connect(**DB_CONFIG)

# Fetch order book data from Binance
def fetch_order_book(symbol, limit=10):
    url = "https://api.binance.com/api/v3/depth"
    params = {"symbol": symbol, "limit": limit}
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

# Insert or update bids/asks into PostgreSQL
def insert_order_book(data, symbol, conn):
    cursor = conn.cursor()
    last_update_id = data['lastUpdateId']
    timestamp = datetime.now()

    def upsert_orders(side, orders):
        for price_str, qty_str in orders:
            price = float(price_str)
            qty = float(qty_str)
            try:
                cursor.execute("""
                    INSERT INTO binance.binance_order_book (
                        symbol, side, price, quantity, last_update_id, fetched_at
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, side, price) DO UPDATE SET
                        quantity = EXCLUDED.quantity,
                        last_update_id = EXCLUDED.last_update_id,
                        fetched_at = EXCLUDED.fetched_at;
                """, (symbol, side, price, qty, last_update_id, timestamp))
            except Exception as e:
                conn.rollback()
                print(f"Error inserting {symbol} {side} @ {price}: {e}")

    # Upsert bids and asks
    upsert_orders("bid", data.get("bids", []))
    upsert_orders("ask", data.get("asks", []))
    conn.commit()
    cursor.close()

if __name__ == "__main__":
    print("Starting Binance Order Book Extractor")

    # List of trading pairs to extract
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "ADAUSDT"]

    try:
        conn = connect_pg()

        for symbol in symbols:
            print(f"Fetching order book for {symbol}...")
            try:
                data = fetch_order_book(symbol, limit=10)
                insert_order_book(data, symbol, conn)
                print(f" Inserted order book for {symbol}")
            except Exception as e:
                print(f"Failed to fetch/insert {symbol}: {e}")

        conn.close()
        print("All order books processed successfully.")

    except Exception as e:
        print(f"Top-level DB error: {e}")
