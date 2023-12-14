WITH src_google_sheets__budgets AS (
    SELECT * 
    FROM {{ source('google_sheets', 'budget') }}
),

stg_google_sheets__budgets AS (
    SELECT
        cast({{ dbt_utils.generate_surrogate_key(['_row']) }} as varchar(50))   AS budget_id,
        cast(year(month) as number(4,0))                                        AS budget_year,
        cast(month(month) as number(2,0))                                       AS budget_month,
        cast(product_id as varchar(50))                                         AS product_id,
        cast(quantity as number(38,0))                                          AS number_of_units_of_product_expected_to_be_sold,
        cast(_fivetran_synced as timestamp_tz(9))                               AS batched_at_utc
    FROM src_google_sheets__budgets
)

SELECT * FROM stg_google_sheets__budgets