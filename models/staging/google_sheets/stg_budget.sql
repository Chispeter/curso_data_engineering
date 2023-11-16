WITH src_budget AS (
    SELECT * 
    FROM {{ source('google_sheets', 'budget') }}
    ),

stg_budget AS (
    SELECT {{ dbt_utils.generate_surrogate_key(['_row']) }} AS budget_id,
            product_id,
            month AS budgeted_at,
            quantity,
            _fivetran_synced AS batched_at
    FROM src_budget
    )

SELECT * FROM stg_budget