WITH distinct_shipping_services AS (
    SELECT
        distinct(shipping_service_name) AS shipping_service_name
    FROM {{ ref('stg_sql_server_dbo__orders') }}
),

int_shipping_service_orders AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['shipping_service_name']) }}   AS shipping_service_id,
        shipping_service_name                                               AS shipping_service_name
    FROM distinct_shipping_services
)

SELECT * FROM int_shipping_service_orders