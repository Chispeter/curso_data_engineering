-- 1. ¿Cuántos usuarios tenemos?
-- Solucion: 130 usuarios
SELECT count(*) AS number_of_total_customers
FROM {{ ref('stg_sql_server_dbo__customers') }}

-- 2. En promedio, ¿cuánto tiempo tarda un pedido desde que se realiza hasta que se entrega?
-- Solucion: 3.89 dias
SELECT cast(avg(timediff(day, order_created_at_utc, order_delivered_at_utc)) as number(38,2)) AS average_order_delivery_time_in_days
FROM {{ ref('stg_sql_server_dbo__orders') }}
-- la siguiente linea no hace falta porque avg() ignora nulls
-- WHERE order_created_at_utc is not null AND order_delivered_at_utc is not null

-- 3. ¿Cuántos usuarios han realizado una sola compra? ¿Dos compras? ¿Tres o más compras? Nota: debe considerar una compra
-- como un solo pedido. En otras palabras, si un usuario realiza un pedido de 3 productos, se considera que ha realizado 1 compra.
-- Solucion: 1 compra -> 26 usuarios, 2 compras -> 28 usuarios, +3 compras -> 71 usuarios.
WITH number_of_total_orders_by_customer_id AS (
     SELECT order_customer_id,
            COUNT(DISTINCT order_id) AS order_number
     FROM {{ ref('stg_sql_server_dbo__orders') }}
     GROUP BY order_customer_id
)

SELECT CASE
        WHEN order_number >= 3 THEN '3+'
        ELSE cast(order_number as varchar(5))
        END AS total_order_number,
        COUNT(DISTINCT order_customer_id) AS total_customer_number
FROM number_of_total_orders_by_customer_id
GROUP BY total_order_number

-- 4. 1. En promedio, ¿Cuántas sesiones únicas tenemos por hora?
SELECT COUNT(DISTINCT(event_session_id)) AS number_of_unique_sessions,
        --year(date(event_created_at_utc)) AS event_year,
        --month(date(event_created_at_utc)) AS event_month,
        --day(date(event_created_at_utc)) AS event_day,
        date_trunc('hour', time(event_created_at_utc)) AS event_hour
    FROM {{ ref('stg_sql_server_dbo__events') }}
    GROUP BY 2
    ORDER BY 2 ASC