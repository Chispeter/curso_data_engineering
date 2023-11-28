WITH snapshot_budget AS (
    SELECT * 
    FROM {{ ref('snapshot_budget') }}
),

fct_budget AS (
    SELECT
        budget_id,
        product_id,
        budget_date,
        number_of_units_of_product_expected_to_be_sold
    FROM snapshot_budget
    )

SELECT * FROM fct_budget