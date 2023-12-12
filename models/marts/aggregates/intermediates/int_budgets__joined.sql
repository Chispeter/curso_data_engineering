WITH fct_budgets AS (
    SELECT *
    FROM {{ ref('fct_budgets') }}
),

dim_years AS (
    SELECT *
    FROM {{ ref('dim_years') }}
),

dim_months AS (
    SELECT
        month_id,
        month_of_year
    FROM {{ ref('dim_months') }}
),

dim_products AS (
    SELECT *
    FROM {{ ref('dim_products') }}
),

int_budgets__joined AS (
    SELECT
        -- fact_budgets (budget data)
        b.budget_id                                         AS budget_id,
        y.year_id                                           AS budget_year_id,
        y.year_number                                       AS budget_year_number,
        m.month_id                                          AS budget_month_id,
        m.month_of_year                                     AS budget_month_of_year,
        p.product_id                                        AS product_id,
        p.name                                              AS product_name,
        p.price_in_usd                                      AS product_price_in_usd,
        p.number_of_units_in_inventory                      AS number_of_units_of_product_in_inventory,
        b.number_of_units_of_product_expected_to_be_sold    AS number_of_units_of_product_expected_to_be_sold
    FROM fct_budgets AS b
    LEFT JOIN dim_years AS y ON b.year_id = y.year_id
    LEFT JOIN dim_months AS m ON b.month_id = m.month_id
    LEFT JOIN dim_products AS p ON b.estimated_delivery_date_id = p.date_id
)

SELECT * FROM int_budgets__joined
