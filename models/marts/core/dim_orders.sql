WITH stg_orders AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__orders') }}
),

dim_orders AS (
    SELECT
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
        -- 
        timestampdiff(minute, min(cast(order_created_at_utc as timestamp_ntz(9))), max(cast(order_delivered_at_utc as timestamp_ntz(9)))) AS session_length_minutes
    FROM stg_orders
    )

SELECT * FROM dim_orders