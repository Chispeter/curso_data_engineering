-- 1. ¿Cuántos usuarios tenemos?
-- Solucion: 130 usuarios

WITH dim_customers AS (
    SELECT customer_id
    FROM {{ ref('dim_customers') }}
    WHERE customer_id <> 'f14cc5cdce0420f4a5a6b6d9d7b85f39' AND valid_to_utc IS NOT NULL
),

total_number_of_total_customers AS (
    SELECT
        count(customer_id) AS total_number_of_total_customers
    FROM dim_customers
)

SELECT * FROM total_number_of_total_customers
