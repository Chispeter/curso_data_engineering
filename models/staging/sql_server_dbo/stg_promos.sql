WITH src_promos AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'promos') }}
    ),

stg_promos AS (
    SELECT {{ dbt_utils.generate_surrogate_key(['promo_id']) }} AS promo_id
            promo_id AS promo_name,
            discount AS promo_discount,
            status AS promo_status,
            _fivetran_synced AS promo_batched_at
    FROM src_promos
    )

SELECT * FROM stg_promos