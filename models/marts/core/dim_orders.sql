WITH int_orders_dates__joined AS (
    SELECT
        *
    FROM {{ ref('int_orders_dates__joined') }}
),

dim_orders AS (
    SELECT
        order_id,
        customer_id,
        address_id,
        tracking_id,
        creation_date_id,
        estimated_delivery_date_id,
        delivery_date_id,
        creation_time,
        estimated_delivery_time,
        delivery_time,
        shipping_service_name,
        shipping_service_cost_in_usd,
        order_cost_in_usd,
        order_total_cost_in_usd,
        order_status
    FROM int_orders_dates__joined
)

SELECT * FROM dim_orders