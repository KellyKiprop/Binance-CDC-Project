import json
from datetime import datetime
from kafka import KafkaConsumer
from cassandra.cluster import Cluster

# Cassandra config
CASSANDRA_CONTACT_POINTS = ['127.0.0.1']
CASSANDRA_KEYSPACE = 'crypto_data'

# Connect to Cassandra
cluster = Cluster(CASSANDRA_CONTACT_POINTS)
session = cluster.connect(CASSANDRA_KEYSPACE)

# Kafka config
consumer = KafkaConsumer(
    'crypto_pg.binance.binance_24h_stats',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='cdc-cassandra-24h-stats',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Listening to binance_24h_stats CDC topic...")

for msg in consumer:
    payload = msg.value

    try:
        after = payload.get("after")
        if after is None:
            continue

        # Extract values from payload
        symbol = after["symbol"]
        price_change = float(after.get("price_change", 0))
        price_change_percent = float(after.get("price_change_percent", 0))
        weighted_avg_price = float(after.get("weighted_avg_price", 0))
        prev_close_price = float(after.get("prev_close_price", 0))
        last_price = float(after.get("last_price", 0))
        open_price = float(after.get("open_price", 0))
        high_price = float(after.get("high_price", 0))
        low_price = float(after.get("low_price", 0))
        volume = float(after.get("volume", 0))
        quote_volume = float(after.get("quote_volume", 0))
        open_time = datetime.fromtimestamp(after["open_time"] / 1_000_000)
        close_time = datetime.fromtimestamp(after["close_time"] / 1_000_000)
        updated_at = datetime.fromtimestamp(after["updated_at"] / 1_000_000)

        stmt = session.prepare("""
            INSERT INTO binance_24h_stats (
                symbol, price_change, price_change_percent, weighted_avg_price,
                prev_close_price, last_price, open_price, high_price, low_price,
                volume, quote_volume, open_time, close_time, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        session.execute(stmt, (
            symbol, price_change, price_change_percent, weighted_avg_price,
            prev_close_price, last_price, open_price, high_price, low_price,
            volume, quote_volume, open_time, close_time, updated_at
        ))

        print(f"Inserted: {symbol} stats for {open_time.date()}")

    except Exception as e:
        print(" Error processing message:", e)
        print("Raw message:", json.dumps(payload, indent=2))
