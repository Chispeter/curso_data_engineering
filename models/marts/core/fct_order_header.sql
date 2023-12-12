WITH int_orders_dates__joined AS (
    SELECT
        order_id,
        promotion_id,
        creation_date_id,
        estimated_delivery_date_id,
        delivery_date_id,
        creation_time,
        estimated_delivery_time,
        delivery_time,
        shipping_service_cost_in_usd,
        order_cost_in_usd,
        order_total_cost_in_usd
    FROM {{ ref('int_orders_dates__joined') }}
),

stg_promotions AS (
    SELECT
        promotion_id,
        discount_in_usd
    FROM {{ ref('stg_sql_server_dbo__promotions') }}
),

fct_order_header AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['o_d.order_id']) }}    AS order_header_id,
        o_d.order_id                                                AS order_id,
        o_d.promotion_id                                            AS promotion_id,
        o_d.creation_date_id                                        AS creation_date_id,
        o_d.estimated_delivery_date_id                              AS estimated_delivery_date_id,
        o_d.delivery_date_id                                        AS delivery_date_id,
        o_d.creation_time                                           AS creation_time,
        o_d.estimated_delivery_time                                 AS estimated_delivery_time,
        o_d.delivery_time                                           AS delivery_time,
        o_d.shipping_service_cost_in_usd                            AS shipping_service_cost_in_usd,
        p.discount_in_usd                                           AS promotion_discount_in_usd,
        o_d.order_cost_in_usd                                       AS order_cost_in_usd,
        -- order_total_cost_in_usd = order_cost_in_usd + shipping_service_cost_in_usd - promotion_discount_in_usd
        o_d.order_total_cost_in_usd                                 AS order_total_cost_in_usd
    FROM int_orders_dates__joined AS o_d
    LEFT JOIN stg_promotions AS p ON o_d.promotion_id = p.promotion_id
)

SELECT * FROM fct_order_header