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

stg_sql_server_dbo__products AS (
    SELECT
        cast(product_id as varchar(50))             AS product_id,
        cast(name as varchar(50))                   AS name,
        cast(price as number(38,2))                 AS price_in_usd,
        cast(inventory as number(38,0))             AS number_of_units_in_inventory,
        coalesce(_fivetran_deleted, false)          AS was_this_row_deleted,
        cast(_fivetran_synced as timestamp_tz(9))   AS batched_at_utc
    FROM src_sql_server_dbo__products
    )

SELECT * FROM stg_sql_server_dbo__products