WITH distinct_years AS (
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
    FROM distinct_years
)

SELECT * FROM dim_years
