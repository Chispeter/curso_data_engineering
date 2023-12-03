WITH src_sql_server_dbo__products AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'products') }}
    UNION ALL
    SELECT
        {{ dbt_utils.generate_surrogate_key(['null']) }},
        0,
        'No Product',
        0,
        null,
        min(_fivetran_synced)
    FROM {{ source('sql_server_dbo', 'products') }}
),

base_sql_server_dbo__products AS (
    SELECT
        product_id,
        price,
        name,
        inventory,
        _fivetran_deleted,
        _fivetran_synced
    FROM src_sql_server_dbo__products
)

SELECT * FROM base_sql_server_dbo__products