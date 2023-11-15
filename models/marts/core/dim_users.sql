WITH users AS (
    SELECT * 
    FROM {{ ref('stg_users') }}
    ),

addresses AS (
    SELECT * 
    FROM {{ ref('stg_addresses') }}
    ),

orders AS (
    SELECT orders.user_id,
            min(orders.created_at) AS first_order_date,
            max(orders.created_at) AS last_order_date,
            count(orders.order_id) AS number_of_orders
    FROM {{ ref('stg_orders') }} AS orders
    GROUP BY orders.user_id
    ),

dim_users AS (
    SELECT
        users.user_id,
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
    FROM users
    JOIN addresses USING (address_id)
    JOIN orders USING (user_id)
    )

SELECT * FROM dim_users