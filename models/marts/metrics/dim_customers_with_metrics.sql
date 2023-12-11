/*WITH int_customer_addresses__joined AS (
    SELECT *
    FROM {{ ref('int_customer_addresses__joined') }}
),

agg_customer_orders AS (
    SELECT *
    FROM {{ ref('agg_customer_orders') }}
),

int_customer_events__joined AS (
    SELECT *
    FROM {{ ref('int_customer_events__joined') }}
),

dim_customers AS (
    SELECT
        -- SNAPSHOT_CUSTOMERS
        -- customer data of each customer_id
        customer_addresses.customer_id AS customer_id,
        customer_addresses.customer_first_name AS customer_first_name,
        customer_addresses.customer_last_name AS customer_last_name,
        customer_addresses.customer_phone_number AS customer_phone_number,
        customer_addresses.customer_email AS customer_email,
        customer_addresses.customer_created_at_utc AS customer_created_at_utc,
        customer_addresses.customer_updated_at_utc AS customer_updated_at_utc,
        -- address data of each customer_id
        customer_addresses.address_id AS address_id,
        customer_addresses.address_street_number AS address_street_number,
        customer_addresses.address_street_name AS address_street_name,
        customer_addresses.address_state_name AS address_state_name,
        customer_addresses.address_zipcode AS address_zipcode,
        customer_addresses.address_country_name AS address_country_name,
        -- INT_CUSTOMER_ORDERS__JOINED (order data of each customer_id)
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
        customer_orders.customer_value_in_usd AS customer_value_in_usd,
        -- INT_CUSTOMER_EVENTS__JOINED (event data of each customer_id)
        -- event dates
        customer_events.oldest_event_date AS oldest_event_date,
        customer_events.most_recent_event_date AS most_recent_event_date,
        -- number of events
        customer_events.number_of_page_view_events AS number_of_page_view_events,
        customer_events.number_of_add_to_cart_events AS number_of_add_to_cart_events,
        customer_events.number_of_checkout_events AS number_of_checkout_events,
        customer_events.number_of_package_shipped_events AS number_of_package_shipped_events,
        -- total number of events should be equal to the sum of all the above
        customer_events.total_number_of_events AS total_number_of_events,
        -- conditional probabilities of events
        -- % probability of adding a product to the cart after viewing and analyzing it
        customer_events.probability_of_add_to_cart_in_percentage AS probability_of_add_to_cart_in_percentage,
        -- % probability of checkout a order after adding products to the cart
        customer_events.probability_of_checkout_in_percentage AS probability_of_checkout_in_percentage,
        -- % probability of ending in a purchase
        customer_events.probability_of_package_shipped_in_percentage AS probability_of_package_shipped_in_percentage
    FROM int_customer_addresses__joined AS customer_addresses
    INNER JOIN int_customer_orders__joined AS customer_orders ON customer_addresses.customer_id = customer_orders.customer_id
    INNER JOIN int_customer_events__joined AS customer_events ON customer_addresses.customer_id = customer_events.customer_id
    )

SELECT * FROM dim_customers*/