WITH src_products AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'products') }}
    ),

stg_products AS (
    SELECT product_id,
            price,
            name,
            inventory,
            _fivetran_deleted,
            _fivetran_synced AS date_load
    FROM src_products
    )

SELECT * FROM stg_products