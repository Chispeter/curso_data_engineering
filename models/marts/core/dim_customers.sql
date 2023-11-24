WITH customers AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__customers') }}
    ),

addresses AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__addresses') }}
    ),

orders AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__orders') }}
    ),

customer_orders AS (
    SELECT orders.order_customer_id AS customer_id,
            min(orders.order_created_at_utc) AS first_order_date,
            max(orders.order_created_at_utc) AS last_order_date,
            count(orders.order_customer_id) AS number_of_orders
    FROM orders
    GROUP BY orders.order_customer_id
    ),

dim_customers AS (
    SELECT
        customers.customer_id AS customer_id,
        customers.customer_first_name AS customer_first_name,
        customers.customer_last_name AS customer_last_name,
        customers.customer_phone_number AS customer_phone_number,
        customers.customer_email AS customer_email,
        addresses.address_street_number AS customer_street_number,
        addresses.address_street_name AS customer_street_name,
        addresses.address_state_name AS customer_state_name,
        addresses.address_zipcode AS customer_zipcode,
        addresses.address_country_name AS customer_country_name,
        customer_orders.first_order_date AS customer_first_order_date,
        customer_orders.last_order_date AS customer_last_order_date,
        coalesce(customer_orders.number_of_orders, 0) AS customer_number_of_orders,
        customers.customer_created_at_utc AS customer_created_at_utc,
        customers.customer_updated_at_utc AS customer_updated_at_utc
    FROM customers
    JOIN addresses ON customers.customer_address_id = addresses.address_id
    JOIN customer_orders ON customers.customer_id = customer_orders.customer_id
    )

SELECT * FROM dim_customers