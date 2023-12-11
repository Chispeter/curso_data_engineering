WITH int_orders_dates__joined AS (
    SELECT
        shipping_service_name
    FROM {{ ref('int_orders_dates__joined') }}
),

int_shipping_services_orders AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['shipping_service_name']) }}   AS shipping_service_id,
        shipping_service_name                                               AS shipping_service_name
    FROM int_orders_dates__joined
    GROUP BY shipping_service_id, shipping_service_name
)

SELECT * FROM int_shipping_services_orders
