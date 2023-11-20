WITH src_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
    ),

base_orders AS (
    SELECT order_id,
            shipping_service,
            shipping_cost,
            address_id,
            created_at,
            lower(promo_id) AS promo_name,
            estimated_delivery_at,
            order_cost,
            user_id,
            order_total,
            delivered_at,
            tracking_id,
            status,
            _fivetran_deleted,
            _fivetran_synced
    FROM src_orders
    )

SELECT * FROM base_orders

