WITH stg_addresses AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__addresses') }}
    ),
    
int_address_orders__grouped AS (
    SELECT * 
    FROM {{ ref('int_address_orders__grouped') }}
),

int_address_orders__joined AS (
    SELECT -- STG_ADDRESSES
            -- address data
            addresses.address_id AS address_id,
            addresses.address_street_number AS address_street_number,
            addresses.address_street_name AS address_street_name,
            addresses.address_state_name AS address_state_name,
            addresses.address_zipcode AS address_zipcode,
            addresses.address_country_name AS address_country_name,
            -- INT_ADDRESS_ORDERS__GROUPED (order data of each address_id)
            -- order dates
            address_orders.oldest_order_date AS oldest_order_date,
            address_orders.most_recent_order_date AS most_recent_order_date,
            -- order cost aggregations
            coalesce(address_orders.cheapest_order_cost_in_usd, 0) AS cheapest_order_cost_in_usd,
            coalesce(address_orders.most_expensive_order_cost_in_usd, 0) AS most_expensive_order_cost_in_usd,
            coalesce(address_orders.average_order_cost_in_usd, 0) AS average_order_cost_in_usd,
            coalesce(address_orders.total_amount_spent_in_usd, 0) AS total_amount_spent_in_usd,
            -- number of orders
            coalesce (address_orders.number_of_pending_orders, 0) AS number_of_pending_orders,
            coalesce (address_orders.number_of_preparing_orders, 0) AS number_of_preparing_orders,
            coalesce (address_orders.number_of_shipped_orders, 0) AS number_of_shipped_orders,
            coalesce (address_orders.number_of_delivered_orders, 0) AS number_of_delivered_orders,
            -- total number of orders should be equal to the sum of all the above
            coalesce (address_orders.total_number_of_orders, 0) AS total_number_of_orders,
            -- address_value = average_order_cost_in_usd * total_number_of_orders
            coalesce(address_orders.address_value_in_usd, 0) AS address_value_in_usd
    FROM stg_addresses AS addresses
    LEFT JOIN int_address_orders__grouped AS address_orders ON addresses.address_id = address_orders.order_address_id
    
    )

SELECT * FROM int_address_orders__joined