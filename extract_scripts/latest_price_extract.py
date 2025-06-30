import requests
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime

# Load Aiven PostgreSQL credentials from .env
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

# Fetch current prices for all symbols
def fetch_latest_prices():
    url = "https://api.binance.com/api/v3/ticker/price"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

# Append latest prices to the table with a timestamp
def insert_latest_prices(data, conn):
    cursor = conn.cursor()
    for item in data:
        try:
            cursor.execute("""
                INSERT INTO binance.binance_latest_prices (symbol, price, collected_at)
                VALUES (%s, %s, CURRENT_TIMESTAMP);
            """, (item['symbol'], float(item['price'])))
        except Exception as e:
            conn.rollback()
            print(f"Error inserting {item['symbol']}: {e}")
    conn.commit()
    cursor.close()

# Run everything
if __name__ == "__main__":
    print("Fetching latest prices...")
    try:
        conn = connect_pg()
        data = fetch_latest_prices()
        insert_latest_prices(data, conn)
        conn.close()
        print(f"Inserted {len(data)} price snapshots into binance_latest_prices.")
    except Exception as e:
        print(f"Top-level error: {e}")
