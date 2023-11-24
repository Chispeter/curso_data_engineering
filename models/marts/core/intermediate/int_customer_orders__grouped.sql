WITH orders AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__orders') }}
    ),

int_customer_orders__grouped AS (
    SELECT orders.order_customer_id AS customer_id,
            -- order dates
            cast(min(orders.order_created_at_utc) as date) AS oldest_order_date,
            cast(max(orders.order_created_at_utc) as date) AS most_recent_order_date,
            -- order cost aggregations
            cast(min(orders.order_cost_in_usd) as number(38,2)) AS cheapest_order_cost_in_usd,
            cast(max(orders.order_cost_in_usd) as number(38,2)) AS most_expensive_order_cost_in_usd,
            cast(avg(orders.order_cost_in_usd) as number(38,2)) AS average_order_cost_in_usd,
            cast(sum(orders.order_cost_in_usd) as number(38,2)) AS total_amount_spent_in_usd,
            -- number of orders
            cast(count(orders.order_customer_id) as number(38,0)) AS number_of_total_orders,
            -- customer_value = average_order_cost_in_usd * number_of_orders
            cast((avg(orders.order_cost_in_usd) *  count(orders.order_customer_id)) as number(38,2)) AS customer_value_in_usd
    FROM orders
    GROUP BY orders.order_customer_id
    )

SELECT * FROM int_customer_orders__grouped