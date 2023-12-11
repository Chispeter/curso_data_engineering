WITH int_order_status_orders AS (
    SELECT *
    FROM {{ ref('int_order_status_orders') }}
),

dim_order_status AS (
    SELECT
        order_status_id,
        order_status
    FROM int_order_status_orders
)

SELECT * FROM dim_order_status