WITH stg_budgets AS (
    SELECT * 
    FROM {{ ref('stg_google_sheets__budgets') }}
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
        b.budget_id                                                         AS budget_id,
        y.year_id                                                           AS year_id,
        m.month_id                                                          AS month_id,
        b.product_id                                                        AS product_id,
        b.number_of_units_of_product_expected_to_be_sold                    AS number_of_units_of_product_expected_to_be_sold
    FROM stg_budgets AS b
    LEFT JOIN distinct_months AS m ON b.budget_month = m.month_of_year
    LEFT JOIN distinct_years AS y ON b.budget_year = y.year_number
)

SELECT * FROM int_budgets_dates__joined