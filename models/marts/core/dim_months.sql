WITH distinct_months AS (
    SELECT
        month_of_year_id,
        month_of_year,
        month_name,
        month_name_short
    FROM {{ ref('stg_staging__dates') }}
    GROUP BY month_of_year_id, month_of_year, month_name, month_name_short
),

dim_months AS (
    SELECT
        month_of_year_id,
        month_of_year,
        month_name,
        month_name_short
    FROM distinct_months
)

SELECT * FROM dim_months
