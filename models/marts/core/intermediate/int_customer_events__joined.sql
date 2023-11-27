WITH snapshot_customers AS (
    SELECT * 
    FROM {{ ref('snapshot_customers') }}
    ),
    
int_customer_events__grouped AS (
    SELECT * 
    FROM {{ ref('int_customer_events__grouped') }}
),

int_customer_events__joined AS (
    SELECT -- SNAPSHOT_CUSTOMERS
            -- customer data
            customers.customer_id AS customer_id,
            customers.customer_first_name AS customer_first_name,
            customers.customer_last_name AS customer_last_name,
            customers.customer_phone_number AS customer_phone_number,
            customers.customer_email AS customer_email,
            customers.customer_created_at_utc AS customer_created_at_utc,
            customers.customer_updated_at_utc AS customer_updated_at_utc,
            -- INT_CUSTOMER_EVENTS__GROUPED (event data of each customer_id)
            -- event dates
            customer_events.oldest_event_date AS oldest_event_date,
            customer_events.most_recent_event_date AS most_recent_event_date,
            -- number of events
            coalesce(customer_events.number_of_page_view_events, 0) AS number_of_page_view_events,
            coalesce(customer_events.number_of_add_to_cart_events, 0) AS number_of_add_to_cart_events,
            coalesce(customer_events.number_of_checkout_events, 0) AS number_of_checkout_events,
            coalesce(customer_events.number_of_package_shipped_events, 0) AS number_of_package_shipped_events,
            -- total number of events should be equal to the sum of all the above
            coalesce(customer_events.total_number_of_events, 0) AS total_number_of_events,
            -- conditional probabilities of events
            -- % probability of adding a product to the cart after viewing and analyzing it
            coalesce(customer_events.probability_of_add_to_cart_in_percentage, 0) AS probability_of_add_to_cart_in_percentage,
            -- % probability of checkout a order after adding products to the cart
            coalesce(customer_events.probability_of_checkout_in_percentage, 0) AS probability_of_checkout_in_percentage,
            -- % probability of ending in a purchase
            coalesce(customer_events.probability_of_package_shipped_in_percentage, 0) AS probability_of_package_shipped_in_percentage
    FROM snapshot_customers AS customers
    LEFT JOIN int_customer_events__grouped AS customer_events ON customers.customer_id = customer_events.event_customer_id
)

SELECT * FROM int_customer_events__joined