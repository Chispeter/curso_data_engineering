WITH snapshot_promotions AS (
    SELECT * 
    FROM {{ ref('snapshot_promotions') }}
),

dim_promotions AS (
    SELECT
        promotion_id,
        promotion_name,
        promotion_discount_in_percentage,
        promotion_status,
    FROM snapshot_promotions
    )

SELECT * FROM dim_promotions