WITH fct_order_header AS (
    SELECT *
    FROM {{ ref('fct_order_header') }}
),

dim_orders AS (
    SELECT *
    FROM {{ ref('dim_orders') }}
),

dim_customers AS (
    SELECT *
    FROM {{ ref('dim_customers') }}
),

dim_addresses AS (
    SELECT *
    FROM {{ ref('dim_addresses') }}
),

dim_promotions AS (
    SELECT *
    FROM {{ ref('dim_promotions') }}
),

dim_dates AS (
    SELECT
        date_id,
        date_day
    FROM {{ ref('dim_dates') }}
),

int_order_header__joined AS (
    SELECT
        o_h.order_header_id,
        o_d.order_id,
        o_d.customer_id,
        o_d.address_id,
        o_d.tracking_id,
        ss_o.shipping_service_id,
        os_o.order_status_id

        o_h.promotion_id,
        d1.date_day                         AS creation_date,
        d2.date_day                         AS estimated_delivery_date,
        d3.date_day                         AS delivery_date,
        o_h.creation_time,
        o_h.estimated_delivery_time,
        o_h.delivery_time,
        o_h.shipping_service_cost_in_usd,
        o_h.discount_in_usd                 AS promotion_discount_in_usd,
        o_h.order_cost_in_usd,
        o_h.order_total_cost_in_usd
    FROM fct_order_header AS o_h
    LEFT JOIN dim_orders AS o ON o_h.order_id = o.order_id
    LEFT JOIN dim_customers AS c ON o.customer_id = c.customer_id
    LEFT JOIN dim_addresses AS a ON o.address_id = a.address_id
    LEFT JOIN dim_promotions AS p ON o_h.promotion_id = p.promotion_id
    LEFT JOIN dim_dates AS d1 ON o_h.creation_date_id = d1.date_id
    LEFT JOIN dim_dates AS d2 ON o_h.estimated_delivery_date_id = d2.date_id
    LEFT JOIN dim_dates AS d3 ON o_h.delivery_date_id = d3.date_id
)

SELECT * FROM int_order_header__joined
