WITH dim_orders AS (
    SELECT * 
    FROM {{ ref('dim_orders') }}
),

dim_dates AS (
    SELECT * 
    FROM {{ ref('dim_dates') }}
),

int_orders_date__joined AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.address_id,
        o.tracking_id,
        o.promotion_id,
        d1.date_day                         AS creation_date,
        d2.date_day                         AS estimated_delivery_date,
        d3.date_day                         AS delivery_date,
        o.creation_time,
        o.estimated_delivery_time,
        o.delivery_time,
        o.shipping_service_name,
        o.shipping_service_cost_in_usd,
        o.order_cost_in_usd,
        o.order_total_cost_in_usd,
        o.order_status

    FROM dim_orders AS o
    LEFT JOIN dim_dates AS d1 ON o.creation_date_id = d1.date_id
    LEFT JOIN dim_dates AS d2 ON o.estimated_delivery_date_id = d2.date_id
    LEFT JOIN dim_dates AS d3 ON o.delivery_date_id = d3.date_id
)

SELECT * FROM int_orders_date__joined