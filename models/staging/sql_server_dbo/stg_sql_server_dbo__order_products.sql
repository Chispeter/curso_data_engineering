WITH src_sql_server_dbo__order_items AS (
    SELECT *
    FROM {{ source('sql_server_dbo', 'order_items') }}
    {% if is_incremental() %}
        WHERE _fivetran_synced > (SELECT max(_fivetran_synced) FROM {{ this }})
    {% endif %}
),

stg_sql_server_dbo__order_products AS (
    SELECT
        cast({{ dbt_utils.generate_surrogate_key(['order_id', 'product_id']) }} as varchar(50)) AS order_product_id,
        cast(order_id as varchar(50)) AS order_id,
        cast(product_id as varchar(50)) AS product_id,
        cast(quantity as number(38,0)) AS number_of_units_of_product_sold,
        cast(coalesce(_fivetran_deleted, false) as boolean) AS was_this_order_product_row_deleted,
        cast(_fivetran_synced as timestamp_tz(9)) AS order_product_batched_at_utc
    FROM src_sql_server_dbo__order_items
)

SELECT * FROM stg_sql_server_dbo__order_products
