WITH stg_promotions AS (
    SELECT
        promotion_id,
        name,
        discount_in_usd,
        status
    FROM {{ ref('stg_sql_server_dbo__promotions') }}
),

dim_promotions AS (
    SELECT
        promotion_id,
        name,
        discount_in_usd,
        status
    FROM stg_promotions
)

SELECT * FROM dim_promotions