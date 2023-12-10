WITH stg_orders AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__orders') }}
),

stg_dates AS (
    SELECT
        date_id,
        date_day
    FROM {{ ref('stg_sql_server_dbo__orders') }}
),

dim_orders AS (
    SELECT
        order_id,
        customer_id,
        address_id,
        promotion_id,
        tracking_id,
        shipping_service_name,
        shipping_service_cost_in_usd,
        order_cost_in_usd,
        order_total_cost_in_usd,
        order_status,
        order_created_at_utc,
        order_estimated_delivery_at_utc,
        order_delivered_at_utc
    FROM stg_orders AS o
    LEFT JOIN stg_dates AS d ON
)

SELECT * FROM dim_orders