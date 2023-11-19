WITH src_promos AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'promos') }}
    ),

stg_promos AS (
    SELECT {{ dbt_utils.generate_surrogate_key(['promo_id', '_fivetran_synced']) }}::varchar(50) AS promo_id,
            {{ replace_empty_and_null_values_with_tag(get_lowercased_column('promo_id'), 'not defined') }}::varchar(50) AS promo_name,
            {{ replace_empty_and_null_values_with_tag('discount', 'not defined') }}::number(38,2) AS promo_discount,
            {{ replace_empty_and_null_values_with_tag(get_lowercased_column('status'), 'not defined') }}::varchar(50) AS promo_status,
            coalesce(_fivetran_deleted, false) AS was_this_promo_row_deleted,
            _fivetran_synced::date AS promo_load_date
    FROM src_promos
    )

SELECT * FROM stg_promos