WITH src_sql_server_dbo_promos AS (
    SELECT *
    FROM {{ source('sql_server_dbo', 'promos') }}
),

base_sql_server_dbo_promos AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['promo_id', 'discount']) }} AS promo_id,
        {{ get_lowercased_column('promo_id') }} AS name,
        discount,
        status,
        _fivetran_deleted,
        _fivetran_synced
    FROM src_sql_server_dbo_promos
)

SELECT * FROM base_sql_server_dbo_promos
UNION ALL
SELECT
    {{ dbt_utils.generate_surrogate_key(['null']) }},
    'no promo',
    0,
    'inactive',
    null,
    min(_fivetran_synced)
FROM base_sql_server_dbo_promos