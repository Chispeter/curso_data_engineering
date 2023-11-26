WITH snapshot_customers AS (
    SELECT *
    FROM {{ ref('snapshot_customers') }}
),

int_customer_orders__joined AS (
    SELECT *
    FROM {{ ref('int_customer_orders__joined') }}
),

dim_customers AS (
    SELECT
        -- SNAPSHOT_CUSTOMERS
        -- customer data of customer_id
        customers.customer_id AS customer_id,
        customers.customer_first_name AS customer_first_name,
        customers.customer_last_name AS customer_last_name,
        customers.customer_phone_number AS customer_phone_number,
        customers.customer_email AS customer_email,
        customers.customer_created_at_utc AS customer_created_at_utc,
        customers.customer_updated_at_utc AS customer_updated_at_utc,
        -- address_id of customer_id
        customers.customer_address_id AS customer_address_id,
        -- INT_CUSTOMER_ORDERS__JOINED
        -- order dates
        customer_orders.oldest_order_date AS oldest_order_date,
        customer_orders.most_recent_order_date AS most_recent_order_date,
        -- order cost aggregations
        customer_orders.cheapest_order_cost_in_usd AS cheapest_order_cost_in_usd,
        customer_orders.most_expensive_order_cost_in_usd AS most_expensive_order_cost_in_usd,
        customer_orders.average_order_cost_in_usd AS average_order_cost_in_usd,
        customer_orders.total_amount_spent_in_usd AS total_amount_spent_in_usd,
        -- total number of orders
        customer_orders.number_of_total_orders AS total_number_of_orders,
        -- customer_value = average_order_cost_in_usd * total_number_of_orders
        customer_orders.customer_value_in_usd AS customer_value_in_usd
    FROM snapshot_customers AS customers
    LEFT JOIN int_customer_orders__joined AS customer_orders ON customers.customer_id = customer_orders.customer_id
    )

SELECT * FROM dim_customers