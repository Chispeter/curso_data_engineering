WITH snapshot_products AS (
    SELECT * 
    FROM {{ ref('snapshot_products') }}
),

dim_products AS (
    SELECT
        product_id,
        product_name,
        product_price_in_usd,
        product_number_of_units_in_inventory
    FROM snapshot_products
    )

SELECT * FROM dim_products