WITH src_budget_products AS (
    SELECT * 
    FROM {{ source('google_sheets', 'budget') }}
    ),

stg_budget AS (
    SELECT _row AS budget_id,
            product_id,
            month AS sold_at,
            quantity,
            _fivetran_synced AS batched_at
    FROM src_budget_products
    )

SELECT * FROM stg_budget