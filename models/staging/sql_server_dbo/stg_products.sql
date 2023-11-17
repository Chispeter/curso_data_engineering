WITH src_products AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'products') }}
    ),

stg_products AS (
    SELECT (CASE WHEN trim(product_id, ' ') = '' THEN 'not_registered'
                    ELSE trim(product_id, ' ')
                    END)::varchar(50) AS product_id,
            initcap(trim(name, ' '))::varchar(100) AS product_name,
            trim(price, ' ')::float AS product_price,
            trim(inventory, ' ')::number(38,0) AS product_number_in_inventory,
            coalesce(_fivetran_deleted, false) AS was_this_product_row_deleted,
            _fivetran_synced::date AS product_load_date
    FROM src_products
    )

SELECT * FROM stg_products