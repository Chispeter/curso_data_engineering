WITH stg_order_products AS (
    SELECT
        order_product_id,
        order_id,
        product_id,
        number_of_units_of_product_sold
    FROM {{ ref('stg_sql_server_dbo__order_products') }}
),

stg_orders AS (
    SELECT
        order_created_at_utc,
        shipping_service_cost_in_usd,
        order_cost_in_usd,
        order_total_cost_in_usd
    FROM {{ ref('stg_sql_server_dbo__orders') }}
),

stg_products AS (
    SELECT
        price_in_usd
    FROM {{ ref('stg_sql_server_dbo__products') }}
),

fct_order_products AS (
    SELECT
        o_p.order_product_id                  AS order_product_id,
        o_p.order_id                          AS order_id,
        o_p.product_id                        AS product_id,
        o_p.number_of_units_of_product_sold   AS number_of_units_of_product_sold,
        o.order_created_at_utc                AS order_created_at_utc
        o.shipping_service_cost_in_usd        AS shipping_service_cost_in_usd,
        o.order_cost_in_usd                   AS order_cost_in_usd,
        o.order_total_cost_in_usd             AS order_total_cost_in_usd
    FROM stg_order_products AS o_p
    LEFT JOIN stg_orders AS o ON o_p.order_id = o.order_id
    LEFT JOIN stg_products AS p ON o_p.product_id = p.product_id

)

SELECT * FROM fct_order_products