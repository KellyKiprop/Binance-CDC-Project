import requests
import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv
from datetime import datetime

# Load credentials from .env
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

# Fetch Kline/Candlestick data from Binance
def get_klines(symbol="BTCUSDT", interval="1h", limit=100):
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

# Insert kline data into PostgreSQL
def insert_klines(data, symbol, interval, conn):
    cursor = conn.cursor()
    for row in data:
        try:
            cursor.execute("""
                INSERT INTO binance.binance_klines (
                    symbol, interval, open_time, open_price, high_price,
                    low_price, close_price, volume, close_time,
                    quote_asset_volume, number_of_trades,
                    taker_buy_base_volume, taker_buy_quote_volume
                ) VALUES (
                    %s, %s, to_timestamp(%s/1000), %s, %s,
                    %s, %s, %s, to_timestamp(%s/1000),
                    %s, %s, %s, %s
                )
                ON CONFLICT (symbol, interval, open_time) DO UPDATE SET
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    volume = EXCLUDED.volume,
                    close_time = EXCLUDED.close_time,
                    quote_asset_volume = EXCLUDED.quote_asset_volume,
                    number_of_trades = EXCLUDED.number_of_trades,
                    taker_buy_base_volume = EXCLUDED.taker_buy_base_volume,
                    taker_buy_quote_volume = EXCLUDED.taker_buy_quote_volume,
                    updated_at = CURRENT_TIMESTAMP;
            """, (
                symbol, interval,
                row[0], float(row[1]), float(row[2]),
                float(row[3]), float(row[4]), float(row[5]),
                row[6], float(row[7]), int(row[8]),
                float(row[9]), float(row[10])
            ))
        except Exception as e:
            conn.rollback()
            print(f"Failed to insert for {symbol} at {row[0]}: {e}")
    conn.commit()
    cursor.close()

# Main loop to fetch data for multiple symbols
if __name__ == "__main__":
    print("Starting Kline Data Extraction...")

    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
    interval = "1m"
    limit = 100  # up to 1000 supported by Binance

    try:
        conn = connect_pg()
        for symbol in symbols:
            print(f"Fetching {limit} {interval} candles for {symbol}...")
            klines = get_klines(symbol, interval, limit)
            insert_klines(klines, symbol, interval, conn)
        conn.close()
        print("All candlestick data loaded successfully.")
    except Exception as e:
        print(f"Top-level error: {e}")
