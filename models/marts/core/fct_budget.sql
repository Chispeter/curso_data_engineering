WITH stg_budget AS (
    SELECT * 
    FROM {{ ref('stg_google_sheets__budget') }}
),

fct_budget AS (
    SELECT
        budget_id,
        product_id,
        budget_date,
        number_of_units_of_product_expected_to_be_sold
    FROM stg_budget
    )

SELECT * FROM fct_budget