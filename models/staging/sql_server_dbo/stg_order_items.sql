WITH src_order_items AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'order_items') }}
    ),

stg_order_items AS (
    SELECT order_id,
            product_id,
            quantity,
            _fivetran_deleted,
            _fivetran_synced AS batched_at
    FROM src_order_items
    )

SELECT * FROM stg_order_items