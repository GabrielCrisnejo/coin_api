CREATE TABLE IF NOT EXISTS aggregated_crypto_data (
    id SERIAL PRIMARY KEY,
    coin_id TEXT NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    max_price NUMERIC NOT NULL,
    min_price NUMERIC NOT NULL,
    UNIQUE (coin_id, year, month)
);
