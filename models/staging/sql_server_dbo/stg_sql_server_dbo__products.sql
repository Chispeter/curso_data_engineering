WITH base_sql_server_dbo__products AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo__products') }}
    ),

stg_sql_server_dbo__products AS (
    SELECT cast(product_id as varchar(50)) AS product_id,
            cast(name as varchar(100)) AS product_name,
            cast(price as number(38, 2)) AS product_price_in_usd,
            cast(inventory as number(38,0)) AS number_of_units_of_product_in_inventory,
            coalesce(_fivetran_deleted, false) AS was_this_product_row_deleted,
            cast(_fivetran_synced as timestamp_tz(9)) AS product_batched_at_utc
    FROM base_sql_server_dbo__products
    )

SELECT * FROM stg_sql_server_dbo__products