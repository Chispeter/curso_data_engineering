WITH src_budget AS (
    SELECT * 
    FROM {{ source('google_sheets', 'budget') }}
    ),

stg_budget AS (
    SELECT {{ dbt_utils.generate_surrogate_key(['_row']) }}::varchar(50) AS budget_id,
            {{ replace_empty_and_null_values_with_tag('product_id', 'not registered') }}::varchar(50) AS product_id,
            month::date AS budget_date,
            quantity::number(38, 0) AS number_of_units_of_product_sold,
            _fivetran_synced::date AS budget_load_date
    FROM src_budget
    )

SELECT * FROM stg_budget