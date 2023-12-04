WITH stg_order_products AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__order_products') }}
),

int_product_order_products__grouped AS (
    SELECT product_id,
            -- total number of distinct orders in which product_id is sold
            cast(count(distinct order_id) as number(38,0)) AS total_number_of_distinct_orders,
            -- total number of units of product sold
            cast(sum(number_of_units_of_product_sold) as number(38,0)) AS total_number_of_units_of_product_sold
    FROM stg_order_products
    GROUP BY product_id
    )

SELECT * FROM int_product_order_products__grouped