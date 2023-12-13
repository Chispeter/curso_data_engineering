-- 3. ¿Cuántos usuarios han realizado una sola compra? ¿Dos compras? ¿Tres o más compras? Nota: debe considerar una compra
-- como un solo pedido. En otras palabras, si un usuario realiza un pedido de 3 productos, se considera que ha realizado 1 compra.
-- Solucion: 1 compra -> 26 usuarios, 2 compras -> 28 usuarios, +3 compras -> 71 usuarios.

WITH total_number_of_orders_by_customer AS (
     SELECT customer_id,
            count(distinct order_id) AS total_number_of_orders_by_customer
     FROM {{ ref('fct_orders') }}
     GROUP BY customer_id
),

total_number_of_customers_by_orders AS (
    SELECT
        case
            when total_number_of_orders_by_customer >= 3 THEN '3+'
            else cast(total_number_of_orders_by_customer as varchar(5))
            end AS number_of_orders,
        count(distinct customer_id) AS total_number_of_customers
    FROM total_number_of_orders_by_customer
    GROUP BY number_of_orders
)

SELECT * FROM total_number_of_customers_by_orders
