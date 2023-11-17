WITH src_order_items AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'order_items') }}
    ),

stg_order_items AS (
    SELECT {{ dbt_utils.generate_surrogate_key(['order_id', 'product_id']) }}::varchar(50) AS order_item_id,
            (CASE WHEN trim(order_id, ' ') = '' THEN 'not_registered'
                    ELSE trim(order_id, ' ')
                    END)::varchar(50) AS order_id,
            (CASE WHEN trim(product_id, ' ') = '' THEN 'not_registered'
                    ELSE trim(product_id, ' ')
                    END)::varchar(50) AS product_id,
            (CASE WHEN trim(quantity, ' ') = '' THEN 'not_defined'
                    ELSE trim(quantity, ' ') END)::number(38,0) AS units_of_product_sold,
            coalesce(_fivetran_deleted, false) AS was_this_order_item_row_deleted,
            _fivetran_synced::date AS order_item_load_date
    FROM src_order_items
    )

SELECT * FROM stg_order_items