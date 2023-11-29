WITH stg_orders AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__orders') }}
),

int_orders__grouped AS (
    SELECT *
    FROM {{ ref('int_orders__grouped') }}
),

dim_shipping_services AS (
    SELECT -- STG_ORDERS
            {{ dbt_utils.generate_surrogate_key(['orders.order_shipping_service_name']) }} AS shipping_service_id,
            orders.order_shipping_service_name AS shipping_service_name,
            count(orders.order_id) AS total_number_of_orders
            -- INT_ORDERS_GROUPED
    FROM stg_orders AS orders
    LEFT JOIN int_orders__grouped AS int_orders ON orders.order_shipping_service_name = int_orders.order_shipping_service_name
    GROUP BY 1, 2
/*
    UNION ALL
    
    SELECT
        
    FROM stg_orders AS orders
*/
)

SELECT * FROM dim_shipping_services