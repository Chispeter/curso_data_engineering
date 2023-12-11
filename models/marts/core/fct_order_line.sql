WITH stg_order_products AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__order_products') }}
),

fct_order_line AS (
    SELECT
        order_product_id,
        order_id,
        product_id,
        number_of_units_of_product_sold
    FROM stg_order_products
)

SELECT * FROM fct_order_line