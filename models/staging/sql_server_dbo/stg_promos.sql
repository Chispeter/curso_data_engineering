WITH src_promos AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'promos') }}
    ),

stg_promos AS (
    SELECT promo_id,
            discount,
            status,
            _fivetran_deleted,
            _fivetran_synced AS date_load
    FROM src_promos
    )

SELECT * FROM stg_promos