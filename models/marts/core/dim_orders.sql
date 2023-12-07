WITH stg_orders AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__orders') }}
),

dim_orders AS (
    SELECT
        -- INT_ORDERS_GROUPED (order data of each order_id)
        -- order data
        order_id,
        order_customer_id,
        order_address_id,
        order_promotion_id,
        order_tracking_id,
        order_shipping_service_name,
        order_shipping_service_cost_in_usd,
        order_cost_in_usd,
        order_total_cost_in_usd,
        order_status,
        order_created_at_utc,
        order_estimated_delivery_at_utc,
        order_delivered_at_utc,
    FROM stg_orders
)

SELECT * FROM dim_orders