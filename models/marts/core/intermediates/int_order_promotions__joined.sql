WITH stg_orders AS (
    SELECT
        order_id,
        promotion_id
    FROM {{ ref('stg_sql_server_dbo__orders') }}
),

stg_promotions AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__promotions') }}
),

int_order_promotions__joined AS (
    SELECT
        o.order_id          AS order_id,
        p.promotion_id      AS promotion_id,
        p.name              AS promotion_name,
        p.discount_in_usd   AS promotion_discount_in_usd,
        p.status            AS promotion_status
    FROM stg_orders AS o
    LEFT JOIN stg_promotions AS p ON o.promotion_id = p.promotion_id
)

SELECT * FROM int_order_promotions__joined