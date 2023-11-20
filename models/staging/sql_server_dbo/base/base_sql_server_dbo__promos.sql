WITH src_promos AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'promos') }}
    ),

base_promos AS (
    SELECT {{ dbt_utils.generate_surrogate_key(['promo_id', 'discount']) }} AS promo_id,
            lower(promo_id) AS name,
            discount,
            status,
            _fivetran_deleted,
            _fivetran_synced
    FROM src_promos
    )

SELECT * FROM base_promos