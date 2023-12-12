WITH fct_order_header AS (
    SELECT *
    FROM {{ ref('fct_order_header') }}
),

dim_orders AS (
    SELECT *
    FROM {{ ref('dim_orders') }}
),

dim_shipping_services AS (
    SELECT *
    FROM {{ ref('dim_shipping_services') }}
),

dim_order_status AS (
    SELECT *
    FROM {{ ref('dim_order_status') }}
),

dim_dates AS (
    SELECT
        date_id,
        date_day
    FROM {{ ref('dim_dates') }}
),

dim_promotions AS (
    SELECT *
    FROM {{ ref('dim_promotions') }}
),


int_order_header__joined AS (
    SELECT 
        -- order header data with dates
        o_h.order_header_id                      AS order_header_id,
        o_h.order_id                             AS order_id,
        o_h.tracking_id                          AS tracking_id,
        o_h.creation_time                        AS creation_time,
        o_h.estimated_delivery_time              AS estimated_delivery_time,
        o_h.delivery_time                        AS delivery_time,
        o_h.shipping_service_cost_in_usd         AS shipping_service_cost_in_usd,
        o_h.promotion_discount_in_usd            AS promotion_discount_in_usd,
        o_h.order_cost_in_usd                    AS order_cost_in_usd,
        o_h.order_total_cost_in_usd              AS order_total_cost_in_usd,
        -- DIM_ORDERS (order data)
        o.customer_id                            AS customer_id,
        o.address_id                             AS address_id,
        -- DIM_PROMOTIONS (promotion data)
        p.promotion_id                           AS promotion_id,
        p.name                                   AS promotion_name,
        p.status                                 AS promotion_status,
        -- DIM_SHIPPING_SERVICES (shipping service data)
        ss.shipping_service_id                   AS shipping_service_id,
        ss.shipping_service_name                 AS shipping_service_name,
        -- DIM_ORDER_STATUS (order status data)
        os.order_status_id                       AS order_status_id,
        os.order_status                          AS order_status,
        -- DIM_DATES (dates data)
        d1.date_id                               AS creation_date_id,
        d1.date_day                              AS creation_date,
        d2.date_id                               AS estimated_delivery_date_id,
        d2.date_day                              AS estimated_delivery_date,
        d3.date_id                               AS delivery_date_id,
        d3.date_day                              AS delivery_date
    FROM fct_order_header AS o_h
    LEFT JOIN dim_orders AS o ON o_h.order_id = o.order_id
    LEFT JOIN dim_promotions AS p ON o_h.promotion_id = p.promotion_id
    LEFT JOIN dim_shipping_services AS ss ON o_h.shipping_service_id = ss.shipping_service_id
    LEFT JOIN dim_order_status AS os ON o_h.order_status_id = os.order_status_id
    LEFT JOIN dim_dates AS d1 ON o_h.creation_date_id = d1.date_id
    LEFT JOIN dim_dates AS d2 ON o_h.estimated_delivery_date_id = d2.date_id
    LEFT JOIN dim_dates AS d3 ON o_h.delivery_date_id = d3.date_id
    
)

SELECT * FROM int_order_header__joined