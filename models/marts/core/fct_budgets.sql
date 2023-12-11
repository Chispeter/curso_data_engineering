WITH int_budgets_dates__joined AS (
    SELECT * 
    FROM {{ ref('int_budgets_dates__joined') }}
),

fct_budgets AS (
    SELECT
        budget_id,
        budget_year_id,
        budget_month_id,
        product_id,
        total_number_of_units_of_product_expected_to_be_sold
    FROM int_budgets_dates__joined
)

SELECT * FROM fct_budgets