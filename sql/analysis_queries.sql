-- 1. Obtener el promedio de precio por cada moneda y mes
SELECT 
    coin_id, 
    EXTRACT(YEAR FROM date) AS year, 
    EXTRACT(MONTH FROM date) AS month, 
    AVG(price_usd) AS avg_price
FROM raw_crypto_data
GROUP BY coin_id, year, month
ORDER BY coin_id, year, month;

-- 2. Calcular el aumento promedio de precio después de caídas consecutivas de más de 3 días
WITH price_changes AS (
    SELECT 
        coin_id,
        date,
        price_usd,
        LAG(price_usd, 1) OVER (PARTITION BY coin_id ORDER BY date) AS prev_price,
        CASE 
            WHEN price_usd < LAG(price_usd, 1) OVER (PARTITION BY coin_id ORDER BY date) THEN 1
            ELSE 0
        END AS is_drop
    FROM raw_crypto_data
),
drop_streaks AS (
    SELECT 
        coin_id,
        date,
        price_usd,
        SUM(is_drop) OVER (PARTITION BY coin_id ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS drop_streak
    FROM price_changes
),
price_recovery AS (
    SELECT 
        coin_id,
        date,
        price_usd,
        LAG(price_usd) OVER (PARTITION BY coin_id ORDER BY date) AS prev_price,
        (price_usd - LAG(price_usd) OVER (PARTITION BY coin_id ORDER BY date)) AS price_increase
    FROM raw_crypto_data
    WHERE coin_id IN (SELECT DISTINCT coin_id FROM drop_streaks WHERE drop_streak >= 3)
),
market_cap_data AS (
    SELECT
        coin_id,
        MAX((raw_json->'market_data'->'market_cap'->>'usd')::numeric) AS market_cap_usd -- Usamos MAX para evitar duplicados
    FROM raw_crypto_data
    WHERE raw_json->'market_data'->>'market_cap' IS NOT NULL
    GROUP BY coin_id -- Agrupamos por coin_id para evitar duplicación
)
SELECT 
    pr.coin_id,
    AVG(pr.price_increase) AS avg_price_increase,
    mc.market_cap_usd
FROM price_recovery pr
JOIN market_cap_data mc ON pr.coin_id = mc.coin_id
GROUP BY pr.coin_id, mc.market_cap_usd
ORDER BY pr.coin_id;
