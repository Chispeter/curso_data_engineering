WITH int_orders__grouped AS (
    SELECT * 
    FROM {{ ref('int_orders__grouped') }}
),

int_order_order_products__grouped AS (
    SELECT * 
    FROM {{ ref('int_order_order_products__grouped') }}
),

dim_orders AS (
    SELECT
        -- INT_ORDERS_GROUPED (order data of each order_id)
        -- order data of each order_id
        orders.order_id,
        orders.order_customer_id,
        orders.order_address_id,
        orders.order_promotion_id,
        orders.order_tracking_id,
        orders.order_shipping_service_name,
        orders.order_shipping_service_cost_in_usd,
        orders.order_cost_in_usd,
        orders.order_total_cost_in_usd,
        orders.order_status,
        orders.order_created_at_utc,
        orders.order_estimated_delivery_at_utc,
        orders.order_delivered_at_utc,
        -- order_estimated_delivery_time_in_hours is the time in hours that elapses from the creation of the order to the date on which the product is estimated to be delivered to the customer.
        orders.order_estimated_delivery_time_in_hours,
        -- order_delivery_time_in_hours is the time in hours that elapses from the creation of the order to the date on which the product is delivered to the customer.
        orders.order_delivery_time_in_hours,
        -- order_delivery_time_deviation_in_hours is the time in hours between estimated delivery time and delivery time
        orders.order_delivery_time_deviation_in_hours,
        -- order_delivery_status is a more detailed order_status that is related to delivery time deviation
        orders.order_delivery_status,
        -- deadline_compliance_rating
        orders.were_deadlines_being_met
        -- INT_ORDER_ORDER_PRODUCTS__GROUPED (order_product data of each order_id)
        -- total number of distinct products in each order_id
        total_number_of_distinct_products,
        -- total number of units of product_id sold in each order_id
        total_number_of_units_of_product_sold
    FROM int_orders__grouped AS orders
    JOIN int_order_order_products__grouped AS order_order_products ON orders.order_id = order_order_products.order_id
)

SELECT * FROM dim_orders