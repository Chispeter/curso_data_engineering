WITH stg_budgets AS (
    SELECT * 
    FROM {{ ref('stg_google_sheets__budgets') }}
),

stg_products AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__products') }}
),

stg_product_orders AS (
    SELECT
        o_p.product_id AS product_id,
        cast(year(o.order_created_at_utc) as number(4,0)) AS product_purchase_year,
        cast(month(o.order_created_at_utc) as number(2,0)) AS product_purchase_month,
        sum(o_p.number_of_units_of_product_sold) AS total_number_of_units_of_product_sold
    FROM {{ ref('stg_sql_server_dbo__order_products') }} AS o_p
    LEFT JOIN {{ ref('stg_sql_server_dbo__orders') }} AS o ON o_p.order_id = o.order_id
    GROUP BY product_id, product_purchase_year, product_purchase_month
),

fct_budgets AS (
    SELECT
        b.budget_id                                                         AS budget_id,
        b.budget_year                                                       AS budget_year,
        b.budget_month                                                      AS budget_month,
        b.product_id                                                        AS product_id,
        p.price_in_usd                                                      AS product_price_in_usd,
        b.number_of_units_of_product_expected_to_be_sold                    AS total_number_of_units_of_product_expected_to_be_sold,
        p_o.total_number_of_units_of_product_sold                           AS total_number_of_units_of_product_sold,
        b.number_of_units_of_product_expected_to_be_sold * p.price_in_usd   AS expected_sales_in_usd,
        p_o.total_number_of_units_of_product_sold * p.price_in_usd          AS real_sales_in_usd
    FROM stg_budgets AS b
    LEFT JOIN stg_products AS p ON b.product_id = p.product_id
    LEFT JOIN stg_product_orders AS p_o ON p_o.product_purchase_year = b.budget_year
            AND p_o.product_purchase_month = b.budget_month
)

SELECT * FROM fct_budgets