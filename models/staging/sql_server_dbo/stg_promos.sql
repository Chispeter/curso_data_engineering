WITH src_promos AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'promos') }}
    ),

stg_promos AS (
    SELECT {{ dbt_utils.generate_surrogate_key(['promo_id']) }}::varchar(50) AS promo_id,
            promo_id AS promo_name,
            discount AS promo_discount,
            status AS promo_status,
            coalesce(_fivetran_deleted, false) AS was_this_promo_row_deleted,
            _fivetran_synced::date AS promo_load_date
    FROM src_promos
    )

SELECT * FROM stg_promos