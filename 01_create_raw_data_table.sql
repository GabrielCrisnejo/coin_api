CREATE TABLE IF NOT EXISTS raw_crypto_data (
    id SERIAL PRIMARY KEY,
    coin_id TEXT NOT NULL,
    date DATE NOT NULL,
    price_usd NUMERIC NOT NULL,
    raw_json JSONB NOT NULL,
    UNIQUE (coin_id, date)
);
