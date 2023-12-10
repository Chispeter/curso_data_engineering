WITH stg_orders AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__orders') }}
),

int_order_order_products__grouped AS (
    SELECT * 
    FROM {{ ref('int_order_order_products__grouped') }}
),

int_order_order_products__joined AS (
    SELECT
        -- STG_ORDERS
        -- order data
        orders.order_id,
        orders.order_customer_id AS order_customer_id,
        orders.order_address_id AS order_address_id,
        orders.order_promotion_id AS order_promotion_id,
        orders.order_tracking_id AS order_tracking_id,
        orders.order_shipping_service_name AS order_shipping_service_name,
        orders.order_shipping_service_cost_in_usd AS order_shipping_service_cost_in_usd,
        orders.order_cost_in_usd AS order_cost_in_usd,
        orders.order_total_cost_in_usd AS order_total_cost_in_usd,
        orders.order_status AS order_status,
        orders.order_created_at_utc AS order_created_at_utc,
        orders.order_estimated_delivery_at_utc AS order_estimated_delivery_at_utc,
        orders.order_delivered_at_utc AS order_delivered_at_utc,
        orders.was_this_order_row_deleted AS was_this_order_row_deleted,
        orders.order_batched_at_utc AS order_batched_at_utc,
        -- INT_ORDER_ORDER_PRODUCTS__GROUPED (order_product data of each order_id)
        -- total number of distinct products in each order_id
        order_order_products.total_number_of_distinct_products AS total_number_of_distinct_products,
        -- total number of units of product_id sold in each order_id
        order_order_products.total_number_of_units_of_product_sold AS total_number_of_units_of_product_sold
    FROM stg_orders AS orders
    JOIN int_order_order_products__grouped AS order_order_products ON orders.order_id = order_order_products.order_id
    )

SELECT * FROM int_order_order_products__joined