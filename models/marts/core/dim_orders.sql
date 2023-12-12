WITH int_orders_dates__joined AS (
    SELECT
        order_id,
        customer_id,
        address_id
    FROM {{ ref('int_orders_dates__joined') }}
),

dim_orders AS (
    SELECT
        order_id,
        customer_id,
        address_id
    FROM int_orders_dates__joined
    
)

SELECT * FROM dim_orders