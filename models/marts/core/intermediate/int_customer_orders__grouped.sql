WITH orders AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__orders') }}
    ),

int_customer_orders__grouped AS (
    SELECT orders.order_customer_id AS customer_id,
            min(orders.order_created_at_utc) AS first_order_date,
            max(orders.order_created_at_utc) AS last_order_date,
            count(orders.order_customer_id) AS number_of_orders
    FROM orders
    GROUP BY orders.order_customer_id
    )

SELECT * FROM int_customer_orders__grouped