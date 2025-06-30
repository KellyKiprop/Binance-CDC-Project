# 📦 Binance Real-Time CDC Pipeline

This project implements a real-time Change Data Capture (CDC) pipeline for streaming cryptocurrency data from **PostgreSQL** to **Apache Cassandra** using **Debezium**, **Apache Kafka**, and **Python**.

It captures updates from Binance API data stored in PostgreSQL and replicates them to Cassandra for fast, scalable access and time-series analysis.

---

## Tech Stack

| Layer           | Technology         |
|----------------|--------------------|
| CDC Engine      | Debezium           |
| Message Broker  | Apache Kafka       |
| Source DB       | PostgreSQL (Aiven) |
| Target DB       | Apache Cassandra   |
| Language        | Python (Kafka Consumers) |
| Environment     | Ubuntu VM    |

---

##  Data Flow

1. Binance data is stored in PostgreSQL.
2. Debezium detects changes and publishes them to Kafka topics.
3. Python Kafka Consumers read messages and insert into corresponding Cassandra tables.
4. Cassandra stores time-series optimized tables for analytics.

---

##  Tables Covered

| Table | Description |
|-------|-------------|
| `binance_latest_prices` | Real-time market prices |
| `binance_24h_stats` | 24-hour trading statistics |
| `binance_klines` | Historical candlestick data |
| `binance_order_book` | Bid/ask order book snapshots |
| `binance_recent_trades` | Recent trade data |

---



---

## How to Run

> Ensure Kafka, Zookeeper, and Cassandra are all running on your machine.

### 1. Start Cassandra

```bash
sudo systemctl start cassandra
```
2. Create Keyspace & Tables
Use cqlsh to run the schema under cassandra/schema.cql.

3. Start Kafka + Debezium
From kafka directory/:

```
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
bin/connect-distributed.sh config/connect-distributed.properties
```
Then post the connector config:

```
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  --data @postgres-source.json
```
4.create topics
```
bin/kafka-topics.sh --create --topic connect-configs --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
bin/kafka-topics.sh --create --topic connect-offsets --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
bin/kafka-topics.sh --create --topic connect-status --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
```

5. Run Python Consumer
Each table has its own consumer:
```
python consumers/latest_prices.py
```

## Features

Real-time CDC from PostgreSQL to Cassandra
Handles updates/inserts via Kafka events
Python deserialization and transformation
Time-series schema optimized for reads

## Lessons Learned

How to use Debezium and Kafka for real-time CDC
PostgreSQL logical replication setup
Handling microsecond timestamps
Cassandra schema design for time-series

## Todo (Next Steps)

Combine all consumers into one scalable handler
Add Grafana for real-time metrics visualization
Enable authentication & SSL for production use
Deploy to cloud (e.g., AWS, GCP, or DigitalOcean)

## Inspiration

Inspired by the need for high-performance crypto analytics and low-latency event propagation using open-source tools.

## Author
Kiprop Kelly

## License
MIT License

