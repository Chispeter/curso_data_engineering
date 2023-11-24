WITH stg_orders AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__orders') }}
),

stg_events AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__events') }}
),

number_of_total_orders_by_customer_id AS (
     SELECT order_customer_id,
            COUNT(DISTINCT order_id) AS order_number
     FROM stg_orders
     GROUP BY order_customer_id
     
),

number_of_total_orders_customers AS (
    SELECT CASE
                WHEN order_number >= 3 THEN '3+'
                ELSE cast(order_number as varchar(5))
                END AS total_order_number,
            COUNT(DISTINCT order_customer_id) AS total_customer_number
    FROM number_of_total_orders_by_customer_id
    GROUP BY total_order_number
),

example AS (
    SELECT DISTINCT(event_session_id) AS event_counter,
            event_created_at_utc
    FROM stg_events
)

SELECT * FROM example