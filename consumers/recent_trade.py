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

# Kafka Consumer configuration
consumer = KafkaConsumer(
    'crypto_pg.binance.binance_recent_trades',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='cdc-cassandra-recenttrades',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Listening to binance_recent_trades CDC topic...")

for msg in consumer:
    payload = msg.value

    try:
        after = payload.get("after")
        if after is None:
            continue

        trade_id = int(after["trade_id"])
        symbol = after["symbol"]
        price = float(after["price"])
        qty = float(after["qty"])
        quote_qty = float(after["quote_qty"])
        time = datetime.fromtimestamp(after["time"] / 1_000_000)
        is_buyer_maker = after["is_buyer_maker"]
        fetched_at = datetime.fromtimestamp(after["fetched_at"] / 1_000_000)

        stmt = session.prepare("""
            INSERT INTO binance_recent_trades (
                trade_id, symbol, price, qty, quote_qty, time, is_buyer_maker, fetched_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """)
        session.execute(stmt, (
            trade_id, symbol, price, qty, quote_qty, time, is_buyer_maker, fetched_at
        ))

        print(f"Inserted trade: {symbol} trade_id {trade_id} @ {price}")

    except Exception as e:
        print("Error processing message:", e)
        print("Raw message:", json.dumps(payload, indent=2))
