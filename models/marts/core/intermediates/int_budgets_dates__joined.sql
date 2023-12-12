WITH stg_budgets AS (
    SELECT * 
    FROM {{ ref('stg_google_sheets__budgets') }}
),

stg_dates AS (
    SELECT
        month_of_year_id,
        month_of_year,
        year_number_id,
        year_number
    FROM {{ ref('stg_staging__dates') }}
),

int_budgets_dates__joined AS (
    SELECT
        b.budget_id                                                         AS budget_id,
        d.year_number_id                                                    AS budget_year_id,
        d.month_of_year_id                                                  AS budget_month_id,
        b.product_id                                                        AS product_id,
        b.number_of_units_of_product_expected_to_be_sold                    AS number_of_units_of_product_expected_to_be_sold
    FROM stg_budgets AS b
    LEFT JOIN stg_dates AS d ON b.budget_year = d.year_number AND b.budget_month = d.month_of_year
)

SELECT * FROM int_budgets_dates__joined