-- 1. Get the average price for each coin and month
SELECT 
    coin_id, 
    EXTRACT(YEAR FROM date) AS year, 
    EXTRACT(MONTH FROM date) AS month, 
    AVG(price_usd) AS avg_price
FROM raw_crypto_data
GROUP BY coin_id, year, month
ORDER BY coin_id, year, month;

-- 2. Calculate the average price increase after consecutive drops of more than 3 days
WITH price_changes AS (
    SELECT
        coin_id,
        date,
        price_usd,
        LAG(price_usd) OVER (PARTITION BY coin_id ORDER BY date) AS prev_price,
        CASE 
            WHEN price_usd < LAG(price_usd) OVER (PARTITION BY coin_id ORDER BY date) THEN 1
            ELSE 0
        END AS is_drop
    FROM raw_crypto_data
),

streak_groups AS (
    SELECT
        coin_id,
        date,
        price_usd,
        is_drop,
        SUM(CASE WHEN is_drop = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY coin_id 
            ORDER BY date 
            ROWS UNBOUNDED PRECEDING
        ) AS group_id
    FROM price_changes
),

streaks_summary AS (
    SELECT
        coin_id,
        group_id,
        COUNT(*) FILTER (WHERE is_drop = 1) AS drop_days,
        MIN(date) FILTER (WHERE is_drop = 1) AS drop_start_date,
        MAX(date) FILTER (WHERE is_drop = 1) AS drop_end_date
    FROM streak_groups
    GROUP BY coin_id, group_id
),

qualified_drops AS (
    SELECT
        coin_id,
        drop_start_date,
        drop_end_date,
        drop_days
    FROM streaks_summary
    WHERE drop_days >= 3
),

recovery_prices AS (
    SELECT
        q.coin_id,
        q.drop_end_date,
        
        -- Price at the end of the drop (exact date of the drop_end_date)
        ed.price_usd AS end_of_drop_price,

        -- First recovery price (first price 3 days after the drop, adjust the interval if necessary)
        rp.price_usd AS recovery_price

    FROM qualified_drops q

    -- Price at the end of the drop (exact date of the drop_end_date)
    LEFT JOIN LATERAL (
        SELECT price_usd
        FROM raw_crypto_data r
        WHERE r.coin_id = q.coin_id
          AND r.date = q.drop_end_date
        ORDER BY r.date ASC
        LIMIT 1
    ) ed ON TRUE

    -- Recovery price: first price >= drop_end_date + 3 days
    LEFT JOIN LATERAL (
        SELECT price_usd
        FROM raw_crypto_data r
        WHERE r.coin_id = q.coin_id
          AND r.date >= q.drop_end_date + INTERVAL '{DAYS_AFTER_DROP} DAY'
        ORDER BY r.date ASC
        LIMIT 1
    ) rp ON TRUE
),

price_increase_calc AS (
    SELECT
        coin_id,
        recovery_price,
        end_of_drop_price,
        (recovery_price - end_of_drop_price) AS price_increase
    FROM recovery_prices
    WHERE recovery_price IS NOT NULL
      AND end_of_drop_price IS NOT NULL
),

final_recovery AS (
    SELECT
        coin_id,
        AVG(price_increase) AS avg_price_increase
    FROM price_increase_calc
    GROUP BY coin_id
),

market_cap_data AS (
    SELECT
        coin_id,
        MAX((raw_json->'market_data'->'market_cap'->>'usd')::numeric) AS market_cap_usd
    FROM raw_crypto_data
    WHERE raw_json->'market_data'->'market_cap'->>'usd' IS NOT NULL
    GROUP BY coin_id
)

SELECT
    fr.coin_id,
    fr.avg_price_increase,
    mc.market_cap_usd
FROM final_recovery fr
JOIN market_cap_data mc 
    ON fr.coin_id = mc.coin_id
ORDER BY fr.coin_id;
