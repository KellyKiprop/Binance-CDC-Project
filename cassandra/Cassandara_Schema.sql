CREATE TABLE crypto_data.binance_24h_stats (
    symbol text,
    updated_at timestamp,
    close_time timestamp,
    high_price float,
    last_price float,
    low_price float,
    open_price float,
    open_time timestamp,
    prev_close_price float,
    price_change float,
    price_change_percent float,
    quote_volume float,
    volume float,
    weighted_avg_price float,
    PRIMARY KEY (symbol, updated_at)
) WITH CLUSTERING ORDER BY (updated_at DESC);

CREATE TABLE crypto_data.binance_klines (
    symbol text,
    interval text,
    open_time timestamp,
    close_price float,
    close_time timestamp,
    high_price float,
    low_price float,
    number_of_trades int,
    open_price float,
    quote_asset_volume float,
    taker_buy_base_volume float,
    taker_buy_quote_volume float,
    updated_at timestamp,
    volume float,
    PRIMARY KEY ((symbol, interval), open_time)
) WITH CLUSTERING ORDER BY (open_time ASC);

CREATE TABLE crypto_data.binance_latest_prices (
    symbol text,
    updated_at timestamp,
    collected_at timestamp,
    price float,
    PRIMARY KEY (symbol, updated_at)
) WITH CLUSTERING ORDER BY (updated_at DESC);

CREATE TABLE crypto_data.binance_order_book (
    symbol text,
    side text,
    price float,
    fetched_at timestamp,
    last_update_id bigint,
    quantity float,
    PRIMARY KEY ((symbol, side), price)
) WITH CLUSTERING ORDER BY (price DESC);

CREATE TABLE crypto_data.binance_recent_trades (
    symbol text,
    trade_id bigint,
    fetched_at timestamp,
    is_buyer_maker boolean,
    price float,
    qty float,
    quote_qty float,
    time timestamp,
    PRIMARY KEY (symbol, trade_id)
) WITH CLUSTERING ORDER BY (trade_id DESC);