-- 2. En promedio, ¿cuánto tiempo tarda un pedido desde que se realiza hasta que se entrega?
-- Solucion: 3.89 dias

WITH fct_orders AS (
    SELECT
        creation_date_id,
        creation_time,
        delivery_date_id,
        delivery_time
    FROM {{ ref('fct_orders') }}
),

dim_dates AS (
    SELECT
        date_id,
        date_day
    FROM {{ ref('dim_dates') }}
),

int_order_dates__joined AS (
    SELECT
        d1.date_day                                         AS creation_date,
        creation_time                                       AS creation_time,
        timestamp_from_parts(d1.date_day, creation_time)    AS creation_timestamp,
        d2.date_day                                         AS delivery_date,
        delivery_time                                       AS delivery_time,
        timestamp_from_parts(d2.date_day, delivery_time)    AS delivery_timestamp
    FROM fct_orders AS o
    LEFT JOIN dim_dates AS d1 ON o.creation_date_id = d1.date_id
    LEFT JOIN dim_dates AS d2 ON o.delivery_date_id = d2.date_id
),

average_order_delivery_time AS (
    SELECT
        cast(avg(timediff(hour, creation_timestamp, delivery_timestamp)) as number(38,2)) AS average_order_delivery_time_in_hours,
        cast(avg(timediff(day, creation_timestamp, delivery_timestamp)) as number(38,2)) AS average_order_delivery_time_in_days
    FROM int_order_dates__joined
    -- la siguiente linea no hace falta porque avg() ignora nulls
    -- WHERE creation_timestamp is not null AND delivery_timestamp is not null

)

SELECT * FROM average_order_delivery_time
