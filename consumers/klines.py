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
    'crypto_pg.binance.binance_klines',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='cdc-cassandra-klines',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Listening to binance_klines CDC topic...")

for msg in consumer:
    payload = msg.value

    try:
        after = payload.get("after")
        if after is None:
            continue

        # Extract fields
        symbol = after["symbol"]
        interval = after["interval"]
        open_time = datetime.fromtimestamp(after["open_time"] / 1_000_000)
        open_price = float(after["open_price"])
        high_price = float(after["high_price"])
        low_price = float(after["low_price"])
        close_price = float(after["close_price"])
        volume = float(after["volume"])
        close_time = datetime.fromtimestamp(after["close_time"] / 1_000_000)
        quote_asset_volume = float(after["quote_asset_volume"])
        number_of_trades = int(after["number_of_trades"])
        taker_buy_base_volume = float(after["taker_buy_base_volume"])
        taker_buy_quote_volume = float(after["taker_buy_quote_volume"])
        updated_at = datetime.fromtimestamp(after["updated_at"] / 1_000_000)

        stmt = session.prepare("""
            INSERT INTO binance_klines (
                symbol, interval, open_time, open_price, high_price, low_price,
                close_price, volume, close_time, quote_asset_volume, number_of_trades,
                taker_buy_base_volume, taker_buy_quote_volume, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        session.execute(stmt, (
            symbol, interval, open_time, open_price, high_price, low_price,
            close_price, volume, close_time, quote_asset_volume, number_of_trades,
            taker_buy_base_volume, taker_buy_quote_volume, updated_at
        ))

        print(f"✅ Inserted kline: {symbol} @ {interval} starting {open_time}")

    except Exception as e:
        print("Error processing message:", e)
        print("Raw message:", json.dumps(payload, indent=2))
