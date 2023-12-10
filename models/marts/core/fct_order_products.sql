WITH stg_order_products AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__order_products') }}
),

stg_orders AS (
    SELECT
        order_id,
        promotion_id,
        shipping_service_cost_in_usd
    FROM {{ ref('stg_sql_server_dbo__orders') }}
),

stg_promotions AS (
    SELECT
        promotion_id,
        discount_in_usd
    FROM {{ ref('stg_sql_server_dbo__promotions') }}
),

stg_products AS (
    SELECT
        product_id,
        price_in_usd
    FROM {{ ref('stg_sql_server_dbo__products') }}
),

int_order_line_normalization AS (
    SELECT
        o_p.order_id,
        p.promotion_id,
        count(o_p.product_id) AS number_of_products,
        cast(p.discount_in_usd/count(o_p.product_id) as number(38,2)) AS promotion_discount_in_usd,
        cast(o.shipping_service_cost_in_usd/count(o_p.product_id) as number(38,2)) AS shipping_service_cost_in_usd
    FROM stg_order_products AS o_p
    LEFT JOIN stg_orders AS o ON o_p.order_id = o.order_id
    LEFT JOIN stg_promotions AS p ON o.promotion_id = p.promotion_id
    GROUP BY o_p.order_id, p.promotion_id, p.discount_in_usd, o.shipping_service_cost_in_usd
),

fct_order_products AS (
    SELECT
        o_p.order_product_id                                                                AS order_product_id,
        o_p.order_id                                                                        AS order_id,
        o_p.product_id                                                                      AS product_id,
        o_p.number_of_units_of_product_sold                                                 AS number_of_units_of_product_sold,
        o_p.number_of_units_of_product_sold * p.price_in_usd                                AS product_revenue_in_usd,
        int_oln.promotion_discount_in_usd                                                   AS promotion_discount_in_usd,
        int_oln.shipping_service_cost_in_usd                                                AS shipping_service_cost_in_usd,
        product_revenue_in_usd - promotion_discount_in_usd - shipping_service_cost_in_usd   AS total_product_revenue_in_usd
    FROM stg_order_products AS o_p
    LEFT JOIN stg_products AS p ON o_p.product_id = p.product_id
    LEFT JOIN int_order_line_normalization AS int_oln ON o_p.order_id = int_oln.order_id
)

SELECT * FROM fct_order_products