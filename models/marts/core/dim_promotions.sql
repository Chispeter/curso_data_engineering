WITH snapshot_promotions AS (
    SELECT *
    FROM {{ ref('snapshot_promotions') }}
),

dim_promotions AS (
    SELECT
        promotion_id,
        name,
        discount_in_usd,
        status,
        dbt_valid_to AS valid_to_utc
    FROM snapshot_promotions
)

SELECT * FROM dim_promotions