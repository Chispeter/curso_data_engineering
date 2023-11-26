WITH snapshot_customers AS (
    SELECT * 
    FROM {{ ref('snapshot_customers') }}
    ),
    
int_customer_orders__grouped AS (
    SELECT * 
    FROM {{ ref('int_customer_orders__grouped') }}
),

int_customer_orders__joined AS (
    SELECT -- SNAPSHOT_CUSTOMERS
            -- customer data
            customers.customer_id AS customer_id,
            customers.customer_first_name AS customer_first_name,
            customers.customer_last_name AS customer_last_name,
            customers.customer_phone_number AS customer_phone_number,
            customers.customer_email AS customer_email,
            customers.customer_created_at_utc AS customer_created_at_utc,
            customers.customer_updated_at_utc AS customer_updated_at_utc,
            -- INT_CUSTOMER_ORDERS__GROUPED (order data of each customer_id)
            -- order dates
            customer_orders.oldest_order_date AS oldest_order_date,
            customer_orders.most_recent_order_date AS most_recent_order_date,
            -- order cost aggregations
            coalesce(customer_orders.cheapest_order_cost_in_usd, 0) AS cheapest_order_cost_in_usd,
            coalesce(customer_orders.most_expensive_order_cost_in_usd, 0) AS most_expensive_order_cost_in_usd,
            coalesce(customer_orders.average_order_cost_in_usd, 0) AS average_order_cost_in_usd,
            coalesce(customer_orders.total_amount_spent_in_usd, 0) AS total_amount_spent_in_usd,
            -- number of orders
            coalesce (customer_orders.number_of_pending_orders, 0) AS number_of_pending_orders,
            coalesce (customer_orders.number_of_preparing_orders, 0) AS number_of_preparing_orders,
            coalesce (customer_orders.number_of_shipped_orders, 0) AS number_of_shipped_orders,
            coalesce (customer_orders.number_of_delivered_orders, 0) AS number_of_delivered_orders,
            -- total number of orders should be equal to the sum of all the above
            coalesce (customer_orders.total_number_of_orders, 0) AS total_number_of_orders,
            -- customer_value = average_order_cost_in_usd * total_number_of_orders
            coalesce(customer_orders.customer_value_in_usd, 0) AS customer_value_in_usd
    FROM snapshot_customers AS customers
    LEFT JOIN int_customer_orders__grouped AS customer_orders ON customers.customer_id = customer_orders.order_customer_id
    
    )

SELECT * FROM int_customer_orders__joined