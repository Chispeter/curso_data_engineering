WITH base_promos AS (
    SELECT * 
    FROM {{ ref('base_promos') }}
    ),

stg_promos AS (
    SELECT promo_id,
            promo_name,
            discount,
            status,
            _fivetran_deleted,
            _fivetran_synced AS batched_at
    FROM base_promos
    )

SELECT * FROM stg_promos