WITH snapshot_budgets AS (
    SELECT *
    FROM {{ ref('snapshot_budgets') }}
),

distinct_months AS (
    SELECT
        month_id,
        month_of_year
    FROM {{ ref('stg_staging__dates') }}
    GROUP BY month_id, month_of_year
),

distinct_years AS (
    SELECT
        year_id,
        year_number
    FROM {{ ref('stg_staging__dates') }}
    GROUP BY year_id, year_number
),

int_budgets_dates__joined AS (
    SELECT
        b.budget_id,
        y.year_id,
        m.month_id,
        b.product_id,
        b.number_of_units_of_product_expected_to_be_sold,
        b.dbt_valid_to AS valid_to_utc
    FROM snapshot_budgets AS b
    LEFT JOIN distinct_months AS m ON b.budget_month = m.month_of_year
    LEFT JOIN distinct_years AS y ON b.budget_year = y.year_number
),

fct_budgets AS (
    SELECT
        budget_id,
        year_id,
        month_id,
        product_id,
        number_of_units_of_product_expected_to_be_sold,
        valid_to_utc
    FROM int_budgets_dates__joined
)

SELECT * FROM fct_budgets