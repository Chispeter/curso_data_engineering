WITH src_products AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'products') }}
    ),

stg_products AS (
    SELECT product_id::varchar(50) AS product_id,
            {{ replace_empty_and_null_values_with_tag(get_uppercased_column_each_word('name'), 'not registered') }}::varchar(100) AS product_name,
            {{ get_trimmed_column('price') }}::number(38, 2) AS product_price_in_usd,
            {{ get_trimmed_column('inventory') }}::number(38,0) AS number_of_units_of_product_in_inventory,
            coalesce(_fivetran_deleted, false) AS was_this_product_row_deleted,
            _fivetran_synced::date AS product_load_date
    FROM src_products
    )

SELECT * FROM stg_products