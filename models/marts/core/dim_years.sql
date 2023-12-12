WITH stg_dates AS (
    SELECT
        year_id,
        year_number
    FROM {{ ref('stg_staging__dates') }}
    GROUP BY year_id, year_number
),

dim_years AS (
    SELECT
        year_id,
        year_number
    FROM stg_dates
)

SELECT * FROM dim_years
