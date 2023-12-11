WITH int_orders_dates__joined AS (
    SELECT
        *
    FROM {{ ref('int_orders_dates__joined') }}
),

fct_orders AS (
    SELECT
        order_id,
        promotion_id,
        creation_date_id,
        shipping_service_name,
        shipping_service_cost_in_usd,
        order_cost_in_usd,
        order_total_cost_in_usd,
    FROM fct_orders
)

SELECT * FROM dim_orders