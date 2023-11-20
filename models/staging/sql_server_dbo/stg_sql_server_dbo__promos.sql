WITH base_promos AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo__promos') }}
    ),

stg_promos AS (
    SELECT promo_id::varchar(50) AS promo_id,
            {{ replace_empty_and_null_values_with_tag(get_lowercased_column('name'), 'not defined') }}::varchar(50) AS promo_name,
            {{ get_trimmed_column('discount') }}::number(38,2) AS promo_discount,
            {{ replace_empty_and_null_values_with_tag(get_lowercased_column('status'), 'not defined') }}::varchar(50) AS promo_status,
            coalesce(_fivetran_deleted, false) AS was_this_promo_row_deleted,
            _fivetran_synced::date AS promo_load_date
    FROM base_promos
    )

SELECT * FROM stg_promos