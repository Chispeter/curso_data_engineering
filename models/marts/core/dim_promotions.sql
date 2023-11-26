WITH stg_promotions AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__promotions') }}
),

dim_promotions AS (
    SELECT
        1
    FROM stg_promotions
    )

SELECT * FROM dim_promotions