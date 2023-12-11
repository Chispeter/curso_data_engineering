WITH int_orders_dates__joined AS (
    SELECT
        order_id,
        customer_id,
        address_id,
        tracking_id,
        shipping_service_name,
        order_status
    FROM {{ ref('int_orders_dates__joined') }}
),

int_shipping_service_orders AS (
    SELECT *
    FROM {{ ref('int_shipping_service_orders') }}
),

int_order_status_orders AS (
    SELECT *
    FROM {{ ref('int_order_status_orders') }}
),

dim_orders AS (
    SELECT
        o_d.order_id,
        o_d.customer_id,
        o_d.address_id,
        o_d.tracking_id,
        ss_o.shipping_service_id,
        os_o.order_status_id
    FROM int_orders_dates__joined AS o_d
    LEFT JOIN int_shipping_service_orders AS ss_o ON o_d.shipping_service_name = ss_o.shipping_service_name
    LEFT JOIN int_order_status_orders AS os_o ON o_d.order_status = os_o.order_status
)

SELECT * FROM dim_orders