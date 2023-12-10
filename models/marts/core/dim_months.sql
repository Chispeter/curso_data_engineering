WITH stg_dates AS (
    SELECT
        month_of_year_id,
        month_of_year,
        month_name,
        month_name_short,
        month_start_date,
        month_end_date
    FROM {{ ref('stg_staging__dates') }}
    GROUP BY month_of_year_id, month_of_year, month_name, month_name_short, month_start_date, month_end_date
),

dim_months AS (
    SELECT
        month_of_year_id,
        month_of_year,
        month_name,
        month_name_short,
        month_start_date,
        month_end_date
    FROM stg_dates
)

SELECT * FROM dim_months
