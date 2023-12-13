WITH snapshot_products AS (
    SELECT *
    FROM {{ ref('snapshot_products') }}
),

dim_products AS (
    SELECT
        product_id,
        name,
        price_in_usd,
        number_of_units_in_inventory,
        dbt_valid_to AS valid_to_utc
    FROM stg_products
)

SELECT * FROM dim_products