import requests
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime

# Load .env PostgreSQL config
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

# Get top 5 symbols by 24h % gain
def get_top_5_symbols(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT symbol
        FROM binance.binance_24h_stats
        ORDER BY price_change_percent DESC
        LIMIT 5;
    """)
    symbols = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return symbols

# Fetch recent trades from Binance API
def fetch_recent_trades(symbol, limit=10):
    url = f"https://api.binance.com/api/v3/trades"
    params = {"symbol": symbol, "limit": limit}
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

# Insert recent trades into PostgreSQL
def insert_recent_trades(data, symbol, conn):
    cursor = conn.cursor()
    timestamp = datetime.now()

    for trade in data:
        try:
            cursor.execute("""
                INSERT INTO binance.binance_recent_trades (
                    trade_id, symbol, price, qty, quote_qty,
                    time, is_buyer_maker, fetched_at
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    to_timestamp(%s/1000), %s, %s
                )
                ON CONFLICT (trade_id, symbol) DO NOTHING;
            """, (
                trade['id'],
                symbol,
                float(trade['price']),
                float(trade['qty']),
                float(trade['price']) * float(trade['qty']),
                trade['time'],
                trade['isBuyerMaker'],
                timestamp
            ))
        except Exception as e:
            conn.rollback()
            print(f"Insert failed: {symbol} trade {trade['id']} → {e}")

    conn.commit()
    cursor.close()

if __name__ == "__main__":
    print("Extracting recent trades for top 5 performers...")

    try:
        conn = connect_pg()
        top_symbols = get_top_5_symbols(conn)

        print(f"Top 5 Symbols: {', '.join(top_symbols)}")

        for symbol in top_symbols:
            try:
                trades = fetch_recent_trades(symbol, limit=10)
                insert_recent_trades(trades, symbol, conn)
                print(f"Inserted {len(trades)} trades for {symbol}")
            except Exception as e:
                print(f"Error with {symbol}: {e}")

        conn.close()
        print("All top 5 symbols processed.")

    except Exception as e:
        print(f" Fatal error: {e}")
