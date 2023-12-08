WITH stg_order_products AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__order_products') }}
),

stg_orders AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__orders') }}
),

stg_products AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__products') }}
),

fct_order_products AS (
    SELECT
        order_products.order_product_id                 AS order_product_id,
        order_products.order_id                         AS order_id,
        order_products.product_id                       AS product_id,
        order_products.number_of_units_of_product_sold  AS number_of_units_of_product_sold
    FROM stg_order_products AS order_products
    LEFT JOIN stg_orders AS orders ON order_products.order_id = orders.order_id
    LEFT JOIN stg_products AS products ON order_products.product_id = products.product_id
    ORDER BY 2, 3
)

SELECT * FROM fct_order_products