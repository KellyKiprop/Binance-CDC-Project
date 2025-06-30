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
    'crypto_pg.binance.binance_latest_prices',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='cdc-cassandra-latest-prices',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Listening to binance_latest_prices CDC topic...")

for msg in consumer:
    payload = msg.value

    try:
        after = payload.get("after")
        if after is None:
            continue

        symbol = after['symbol']
        price = float(after['price'])
        updated_at_raw = after['collected_at']
        updated_at = datetime.fromtimestamp(updated_at_raw / 1_000_000)

        stmt = session.prepare("""
            INSERT INTO binance_latest_prices (symbol, price, collected_at)
            VALUES (?, ?, ?)
        """)
        session.execute(stmt, (symbol, price, updated_at))
        print(f"Inserted: {symbol} @ {price} on {collected_at}")

    except Exception as e:
        print("Error processing message:", e)
        print("Raw message:", json.dumps(payload, indent=2))
