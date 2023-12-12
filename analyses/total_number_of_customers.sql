-- 1. ¿Cuántos usuarios tenemos?
-- Solucion: 130 usuarios + 1 (null customer)

WITH dim_customers AS (
    SELECT customer_id
    FROM {{ ref('dim_customers') }}
),

total_number_of_total_customers AS (
    SELECT
        count(customer_id) AS total_number_of_total_customers
    FROM dim_customers
)

SELECT * FROM total_number_of_total_customers
