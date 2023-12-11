WITH int_orders_dates__joined AS (
    SELECT *
    FROM {{ ref('int_orders_dates__joined') }}
),

int_order_promotions__joined AS (
    SELECT *
    FROM {{ ref('int_order_promotions__joined') }}
),

fct_order_header AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['order_id']) }} AS order_header_id,
        order_id,
        creation_date_id,
        promotion_id,
        estimated_delivery_date_id,
        delivery_date_id,
        creation_time,
        estimated_delivery_time,
        delivery_time,
        shipping_service_cost_in_usd,
        order_cost_in_usd,
        order_total_cost_in_usd
    FROM int_orders_dates__joined AS o_d
    LEFT JOIN int_order_promotions__joined AS o_p
)

SELECT * FROM fct_order_header