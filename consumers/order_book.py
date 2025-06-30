import json
from datetime import datetime
from kafka import KafkaConsumer
from cassandra.cluster import Cluster

# Cassandra configuration
CASSANDRA_CONTACT_POINTS = ['127.0.0.1']
CASSANDRA_KEYSPACE = 'crypto_data'

# Connect to Cassandra
cluster = Cluster(CASSANDRA_CONTACT_POINTS)
session = cluster.connect(CASSANDRA_KEYSPACE)

# Kafka configuration
consumer = KafkaConsumer(
    'crypto_pg.binance.binance_order_book',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='cdc-cassandra-orderbook',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Listening to binance_order_book CDC topic...")

for msg in consumer:
    payload = msg.value

    try:
        after = payload.get("after")
        if after is None:
            continue

        symbol = after["symbol"]
        side = after["side"]
        price = float(after["price"])
        quantity = float(after["quantity"])
        last_update_id = int(after["last_update_id"])
        fetched_at = datetime.fromtimestamp(after["fetched_at"] / 1_000_000)

        stmt = session.prepare("""
            INSERT INTO binance_order_book (
                symbol, side, price, quantity, last_update_id, fetched_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """)
        session.execute(stmt, (
            symbol, side, price, quantity, last_update_id, fetched_at
        ))

        print(f" Inserted order book: {symbol} {side} @ {price} qty {quantity}")

    except Exception as e:
        print(" Error processing message:", e)
        print("Raw message:", json.dumps(payload, indent=2))
