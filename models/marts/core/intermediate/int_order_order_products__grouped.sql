WITH stg_order_products AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__order_products') }}
),

int_order_order_products__grouped AS (
    SELECT
        order_id,
        -- total number of distinct products in each order_id
        cast(count(distinct product_id) as number(38,0)) AS total_number_of_distinct_products,
        -- total number of units of product_id sold in each order_id
        cast(sum(number_of_units_of_product_sold) as number(38,0)) AS total_number_of_units_of_product_sold
    FROM stg_order_products
    GROUP BY order_id
)

SELECT * FROM int_order_order_products__grouped