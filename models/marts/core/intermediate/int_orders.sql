WITH stg_orders AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__orders') }}
    ),

stg_promos AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__promos') }}
    ),

int_orders AS (
    SELECT order_id,
            user_id,
            address_id,
            stg_promos.promo_id,
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
    FROM stg_orders
    JOIN stg_promos USING (promo_id)
    )

SELECT * FROM int_orders