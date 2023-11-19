WITH src_order_items AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'order_items') }}
    ),

stg_order_items AS (
    SELECT {{ dbt_utils.generate_surrogate_key(['order_id', 'product_id']) }}::varchar(50) AS order_item_id,
            {{ replace_empty_and_null_values_with_tag('order_id', 'not registered') }}::varchar(50) AS order_id,
            {{ replace_empty_and_null_values_with_tag('product_id', 'not registered') }}::varchar(50) AS product_id,
            {{ replace_empty_and_null_values_with_tag('quantity', 'not defined') }}::number(38,0) AS units_of_product_sold,
            coalesce(_fivetran_deleted, false) AS was_this_order_item_row_deleted,
            _fivetran_synced::date AS order_item_load_date
    FROM src_order_items
    )

SELECT * FROM stg_order_items