CREATE TABLE IF NOT EXISTS binance.binance_24h_stats (
    symbol VARCHAR(20) PRIMARY KEY,            -- e.g. BTCUSDT
    price_change FLOAT,                        -- Price change in 24h
    price_change_percent FLOAT,                -- Percent change
    weighted_avg_price FLOAT,                  -- Weighted average price in 24h
    prev_close_price FLOAT,                    -- Previous close price
    last_price FLOAT,                          -- Last traded price
    open_price FLOAT,                          -- Price at the start of the 24h window
    high_price FLOAT,                          -- Highest price
    low_price FLOAT,                           -- Lowest price
    volume FLOAT,                              -- Traded volume in base asset
    quote_volume FLOAT,                        -- Traded volume in quote asset
    open_time TIMESTAMP,                       -- Start of the 24h window
    close_time TIMESTAMP,                      -- End of the 24h window
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Timestamp of last update
);

CREATE TABLE IF NOT EXISTS binance.binance_klines (
    symbol VARCHAR(20),
    interval VARCHAR(5),
    open_time TIMESTAMP,
    open_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    close_price FLOAT,
    volume FLOAT,
    close_time TIMESTAMP,
    quote_asset_volume FLOAT,
    number_of_trades INTEGER,
    taker_buy_base_volume FLOAT,
    taker_buy_quote_volume FLOAT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, interval, open_time)
);

CREATE TABLE IF NOT EXISTS binance.binance_latest_prices (
    symbol VARCHAR(20) PRIMARY KEY,        -- e.g., BTCUSDT
    price FLOAT NOT NULL,                  -- Latest market price
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Time of last update
);

INSERT INTO binance.binance_latest_prices (symbol, price)
VALUES ('BTCUSDT', 202020.75)
ON CONFLICT (symbol)
DO UPDATE SET
    price = EXCLUDED.price,
    updated_at = CURRENT_TIMESTAMP;



CREATE TABLE IF NOT EXISTS binance.binance_order_book (
    symbol VARCHAR(20),
    side VARCHAR(4) CHECK (side IN ('bid', 'ask')),  -- 'bid' or 'ask'
    price FLOAT,
    quantity FLOAT,
    last_update_id BIGINT,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (symbol, side, price)
);

CREATE TABLE IF NOT EXISTS binance.binance_recent_trades (
    trade_id BIGINT,
    symbol VARCHAR(20),
    price FLOAT,
    qty FLOAT,
    quote_qty FLOAT,
    time TIMESTAMP,
    is_buyer_maker BOOLEAN,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_id, symbol)
);


select * from binance.binance_24h_stats;
select * from binance.binance_klines bk;
select * from binance.binance_latest_prices;
select * from binance.binance_order_book bob;

select count(symbol)
from binance.binance_24h_stats;

-- 1. Check if logical replication is enabled
SHOW wal_level;

-- 2. Attempt to create a publication for your Binance tables
CREATE PUBLICATION binance_pub FOR TABLE 
    binance.binance_24h_stats,
    binance.binance_klines,
    binance.binance_order_book,
    binance.binance_latest_prices,
    binance.binance_recent_trades;

-- View publication tables
SELECT * FROM pg_publication_tables;

SELECT * FROM pg_replication_slots;

SELECT * FROM pg_publication_tables;

DROP PUBLICATION IF EXISTS debezium_pub;

CREATE PUBLICATION debezium_pub
FOR TABLE
  binance.binance_latest_prices,
  binance.binance_24h_stats,
  binance.binance_klines,
  binance.binance_order_book,
  binance.binance_recent_trades;


























