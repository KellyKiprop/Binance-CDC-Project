import requests
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime

# Load DB credentials from .env
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "dbname": os.getenv("PG_DATABASE")
}

# Connect to Aiven PostgreSQL
def connect_pg():
    return psycopg2.connect(**DB_CONFIG)

# Fetch 24h ticker data
def fetch_24h_stats():
    url = "https://api.binance.com/api/v3/ticker/24hr"
    res = requests.get(url)
    return res.json()

# Insert or update data
def upsert_24h_stats(data, conn):
    cursor = conn.cursor()
    for item in data:
        try:
            cursor.execute("""
                INSERT INTO binance.binance_24h_stats (
                    symbol, price_change, price_change_percent, weighted_avg_price,
                    prev_close_price, last_price, open_price, high_price, low_price,
                    volume, quote_volume, open_time, close_time
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, to_timestamp(%s/1000), to_timestamp(%s/1000)
                )
                ON CONFLICT (symbol) DO UPDATE SET
                    price_change = EXCLUDED.price_change,
                    price_change_percent = EXCLUDED.price_change_percent,
                    weighted_avg_price = EXCLUDED.weighted_avg_price,
                    prev_close_price = EXCLUDED.prev_close_price,
                    last_price = EXCLUDED.last_price,
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    volume = EXCLUDED.volume,
                    quote_volume = EXCLUDED.quote_volume,
                    open_time = EXCLUDED.open_time,
                    close_time = EXCLUDED.close_time,
                    updated_at = CURRENT_TIMESTAMP;
            """, (
                item['symbol'],
                float(item['priceChange']),
                float(item['priceChangePercent']),
                float(item['weightedAvgPrice']),
                float(item['prevClosePrice']),
                float(item['lastPrice']),
                float(item['openPrice']),
                float(item['highPrice']),
                float(item['lowPrice']),
                float(item['volume']),
                float(item['quoteVolume']),
                int(item['openTime']),
                int(item['closeTime'])
            ))
        except Exception as e:
            print(f"Error inserting {item['symbol']}: {e}")
    conn.commit()
    cursor.close()

if __name__ == "__main__":
    print("Fetching 24h stats and inserting into PostgreSQL...")
    conn = connect_pg()
    data = fetch_24h_stats()
    upsert_24h_stats(data, conn)
    conn.close()
    print("Done.")
