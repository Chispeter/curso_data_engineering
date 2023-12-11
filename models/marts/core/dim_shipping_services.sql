WITH int_shipping_service_orders AS (
    SELECT
        shipping_service_id,
        shipping_service_name
    FROM {{ ref('int_shipping_service_orders') }}
),

dim_shipping_services AS (
    SELECT
        shipping_service_id,
        shipping_service_name
    FROM int_shipping_service_orders
)

SELECT * FROM dim_shipping_services