WITH int_address_customers__grouped AS (
    SELECT * 
    FROM {{ ref('int_address_customers__grouped') }}
),

int_address_orders__joined AS (
    SELECT * 
    FROM {{ ref('int_address_orders__joined') }}
),

dim_addresses AS (
    SELECT
        -- INT_ADDRESS_CUSTOMERS__GROUPED (customer data of each address_id)
        -- address data
        address_customers.address_id AS address_id,
        address_customers.address_street_number AS address_street_number,
        address_customers.address_street_name AS address_street_name,
        address_customers.address_state_name AS address_state_name,
        address_customers.address_zipcode AS address_zipcode,
        address_customers.address_country_name AS address_country_name,
        -- total number of customers living in each address_id
        address_customers.total_number_of_customers AS total_number_of_customers,
        -- INT_ADDRESS_ORDERS_JOINED (order data of each address_id)
        -- order dates
        address_orders.oldest_order_date AS oldest_order_date,
        address_orders.most_recent_order_date AS most_recent_order_date,
        -- order cost aggregations
        address_orders.cheapest_order_cost_in_usd AS cheapest_order_cost_in_usd,
        address_orders.most_expensive_order_cost_in_usd AS most_expensive_order_cost_in_usd,
        address_orders.average_order_cost_in_usd AS average_order_cost_in_usd,
        address_orders.total_amount_spent_in_usd AS total_amount_spent_in_usd,
        -- number of orders
        address_orders.number_of_pending_orders AS number_of_pending_orders,
        address_orders.number_of_preparing_orders AS number_of_preparing_orders,
        address_orders.number_of_shipped_orders AS number_of_shipped_orders,
        address_orders.number_of_delivered_orders AS number_of_delivered_orders,
        -- total number of orders should be equal to the sum of all the above
        address_orders.total_number_of_orders AS total_number_of_orders,
        -- address_value = average_order_cost_in_usd * total_number_of_orders
        address_orders.address_value_in_usd AS address_value_in_usd
    FROM int_address_customers__grouped AS address_customers
    JOIN int_address_orders__joined AS address_orders ON address_customers.address_id = address_orders.address_id
)

SELECT * FROM dim_addresses