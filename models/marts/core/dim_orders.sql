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
        shipping_service_name,
        order_status
    FROM int_orders_dates__joined
)

SELECT * FROM dim_orders