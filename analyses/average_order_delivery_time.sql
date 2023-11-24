WITH stg_orders AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__orders') }}
),

average_order_delivery_time AS (
    SELECT cast(avg(timediff(day, order_created_at_utc, order_delivered_at_utc)) as number(38,2)) AS average_order_delivery_time_in_days
    FROM stg_orders
    -- la siguiente linea no hace falta porque avg() ignora nulls
    -- WHERE order_created_at_utc is not null AND order_delivered_at_utc is not null
)

SELECT * FROM average_order_delivery_time