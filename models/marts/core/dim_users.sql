WITH stg_sql_server_dbo__customers AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__customers') }}
    ),

stg_sql_server_dbo__addresses AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__addresses') }}
    ),

stg_sql_server_dbo__orders AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__orders') }}
    ),

customer_orders AS (
    SELECT orders.customer_id AS customer_id,
            min(orders.created_at) AS first_order_date,
            max(orders.created_at) AS last_order_date,
            count(orders.customer_id) AS number_of_orders
    FROM stg_sql_server_dbo__orders orders
    GROUP BY orders.customer_id
    ),

dim_customers AS (
    SELECT
        users.customer_id,
        users.first_name,
        users.last_name,
        users.phone_number,
        users.email,
        addresses.address AS street_number_and_name,
        addresses.state,
        addresses.zipcode,
        addresses.country,
        orders.first_order_date,
        orders.last_order_date,
        coalesce(orders.number_of_orders, 0) AS number_of_orders,
        users.created_at AS created_at_utc,
        users.updated_at AS updated_at_utc
    FROM stg_sql_server_dbo__customers customers
    JOIN stg_sql_server_dbo__addresses addresses ON customers.customer_address_id = addresses.address_id
    JOIN customer_orders ON customers.customer_id = customer_orders.customer_id
    )

SELECT * FROM dim_customers