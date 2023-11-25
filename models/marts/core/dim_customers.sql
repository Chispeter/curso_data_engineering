WITH int_customer_orders__joined AS (
    SELECT *
    FROM {{ ref('int_customer_orders__joined') }}
    ),

dim_customers AS (
    SELECT
        -- customer data
        customer_orders.customer_id AS customer_id,
        customer_orders.customer_first_name AS customer_first_name,
        customer_orders.customer_last_name AS customer_last_name,
        customer_orders.customer_phone_number AS customer_phone_number,
        customer_orders.customer_email AS customer_email,
        -- order dates
        customer_orders.oldest_order_date AS oldest_order_date,
        customer_orders.most_recent_order_date AS most_recent_order_date,
        -- order cost aggregations
        customer_orders.cheapest_order_cost_in_usd AS cheapest_order_cost_in_usd,
        customer_orders.most_expensive_order_cost_in_usd AS most_expensive_order_cost_in_usd,
        customer_orders.average_order_cost_in_usd AS average_order_cost_in_usd,
        customer_orders.total_amount_spent_in_usd AS total_amount_spent_in_usd,
        -- number of total orders
        customer_orders.number_of_total_orders AS number_of_total_orders,
        -- customer_value = average_order_cost_in_usd * number_of_orders
        customer_orders.customer_value_in_usd AS customer_value_in_usd
    FROM int_customer_orders__joined AS customer_orders
    )

SELECT * FROM dim_customers