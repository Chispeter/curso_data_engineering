WITH stg_products AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__products') }}
),

dim_products AS (
    SELECT
        1
    FROM stg_products
    )

SELECT * FROM dim_products