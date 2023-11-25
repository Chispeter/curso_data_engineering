WITH stg_customers AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__customers') }}
    ),
    
int_customer_orders__grouped AS (
    SELECT * 
    FROM {{ ref('int_customer_orders__grouped') }}
),

int_customer_orders__joined AS (
    SELECT customers.customer_id AS customer_id,
            -- order dates
            customer_orders.oldest_order_date AS oldest_order_date,
            customer_orders.most_recent_order_date AS most_recent_order_date,
            -- order cost aggregations
            coalesce(customer_orders.cheapest_order_cost_in_usd, 0) AS cheapest_order_cost_in_usd,
            coalesce(customer_orders.most_expensive_order_cost_in_usd, 0) AS most_expensive_order_cost_in_usd,
            coalesce(customer_orders.average_order_cost_in_usd, 0) AS average_order_cost_in_usd,
            coalesce(customer_orders.total_amount_spent_in_usd, 0) AS total_amount_spent_in_usd,
            -- number of total orders
            coalesce (customer_orders.number_of_total_orders, 0) AS number_of_total_orders,
            -- customer_value = average_order_cost_in_usd * number_of_orders
            coalesce(customer_orders.customer_value_in_usd, 0) AS customer_value_in_usd
    FROM stg_customers AS customers
    LEFT JOIN int_customer_orders__grouped customer_orders ON customers.customer_id = customer_orders.order_customer_id
    
    )

SELECT * FROM int_customer_orders__joined