WITH dim_orders AS (
    SELECT *
    FROM {{ ref('dim_orders') }}
),

dim_customers AS (
    SELECT *
    FROM {{ ref('dim_customers') }}
),

dim_addresses AS (
    SELECT *
    FROM {{ ref('dim_addresses') }}
),

int_orders__joined AS (
    SELECT 
        -- DIM_ORDERS (order data)
        o.order_id                  AS order_id,
        o.customer_id               AS customer_id,
        -- DIM_ORDERS (address data)
        a.address_id                AS order_address_id,
        a.street_number             AS order_street_number,
        a.street_name               AS order_street_name,
        a.state_name                AS order_state_name,
        a.zipcode                   AS order_zipcode,
        a.country_name              AS order_country_name
    FROM dim_orders AS o

    LEFT JOIN dim_addresses AS a ON o.address_id = a.address_id
)

SELECT * FROM int_orders__joined