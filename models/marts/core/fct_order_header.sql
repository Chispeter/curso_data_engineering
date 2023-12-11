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

int_order_promotions__joined AS (
    SELECT
        promotion_id,
        promotion_discount_in_usd
    FROM {{ ref('int_order_promotions__joined') }}
),

fct_order_header AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['o_d.order_id']) }} AS order_header_id,
        o_d.order_id,
        o_d.promotion_id,
        o_d.creation_date_id,
        o_d.estimated_delivery_date_id,
        o_d.delivery_date_id,
        o_d.creation_time,
        o_d.estimated_delivery_time,
        o_d.delivery_time,
        o_d.shipping_service_cost_in_usd,
        o_p.promotion_discount_in_usd,
        o_d.order_cost_in_usd,
        -- order_total_cost_in_usd = order_cost_in_usd + shipping_service_cost_in_usd - promotion_discount_in_usd
        o_d.order_total_cost_in_usd
    FROM int_orders_dates__joined AS o_d
    LEFT JOIN int_order_promotions__joined AS o_p ON o_d.promotion_id = o_p.promotion_id
)

SELECT count(*) FROM fct_order_header