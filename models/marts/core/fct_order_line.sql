{{
    config(
        materialized='incremental'
    )
}}

WITH stg_order_products AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__order_products') }}
    {% if is_incremental() %}
        WHERE batched_at_utc > (SELECT max(batched_at_utc) FROM {{ this }})
    {% endif %}
),

fct_order_line AS (
    SELECT
        order_product_id,
        order_id,
        product_id,
        number_of_units_of_product_sold,
        batched_at_utc
    FROM stg_order_products
)

SELECT * FROM fct_order_line