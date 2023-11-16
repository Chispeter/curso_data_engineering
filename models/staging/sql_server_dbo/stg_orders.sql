WITH src_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
    ),

stg_orders AS (
    SELECT order_id,
            user_id,
            address_id,
            promo_id,
            tracking_id,
            shipping_service,
            shipping_cost,
            order_cost,
            order_total,
            created_at,
            status AS order_status,
            estimated_delivery_at,
            delivered_at,
            _fivetran_deleted,
            _fivetran_synced AS batched_at
    FROM src_orders
    )

SELECT * FROM stg_orders