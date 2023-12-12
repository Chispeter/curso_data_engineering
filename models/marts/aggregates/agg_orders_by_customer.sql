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

customer_orders__grouped AS (
    SELECT
        customer_id,
        -- order dates
        cast(min(timestamp_from_parts(creation_date, creation_time)) as date) AS oldest_order_date,
        cast(max(timestamp_from_parts(creation_date, creation_time)) as date) AS most_recent_order_date,
        -- order cost aggregations
        cast(min(order_cost_in_usd) as number(38,2)) AS cheapest_order_cost_in_usd,
        cast(max(order_cost_in_usd) as number(38,2)) AS most_expensive_order_cost_in_usd,
        cast(avg(order_cost_in_usd) as number(38,2)) AS average_order_cost_in_usd,
        cast(sum(order_cost_in_usd) as number(38,2)) AS total_amount_spent_in_usd,
        -- number of orders
        cast(count(case when order_status = 'no status' then 1 else 0 end) as number(38,2)) AS number_of_pending_orders,
        cast(count(case when order_status = 'preparing' then 1 else 0 end) as number(38,2)) AS number_of_preparing_orders,
        cast(count(case when order_status = 'shipped' then 1 else 0 end) as number(38,2)) AS number_of_shipped_orders,
        cast(count(case when order_status = 'delivered' then 1 else 0 end) as number(38,2)) AS number_of_delivered_orders,
        -- total number of orders should be equal to the sum of all the above
        cast(count(customer_id) as number(38,2)) AS total_number_of_orders,
        -- customer_value = average_order_cost_in_usd * total_number_of_orders
        cast((avg(order_cost_in_usd) * count(customer_id)) as number(38,2)) AS customer_value_in_usd
    FROM int_order_header__joined
    GROUP BY customer_id
),

dim_adresses AS (
    SELECT *
    FROM {{ ref('dim_adresses') }}
),

agg_orders_by_customer AS (
    SELECT 
        -- INT_CUSTOMERS__JOINED
        -- customer data
        c.customer_id,
        c.first_name,
        c.last_name,
        c.phone_number,
        c.email,
        -- DIM_ADDRESSES (address data)
        a.street_number,
        a.street_name,
        a.state_name,
        a.zipcode,
        a.country_name,
        -- CUSTOMER_ORDERS__GROUPED (order data of each customer_id)
        -- order dates
        c_o.oldest_order_date                               AS oldest_order_date,
        c_o.most_recent_order_date                          AS most_recent_order_date,
        -- order cost aggregations
        coalesce(c_o.cheapest_order_cost_in_usd, 0)         AS cheapest_order_cost_in_usd,
        coalesce(c_o.most_expensive_order_cost_in_usd, 0)   AS most_expensive_order_cost_in_usd,
        coalesce(c_o.average_order_cost_in_usd, 0)          AS average_order_cost_in_usd,
        coalesce(c_o.total_amount_spent_in_usd, 0)          AS total_amount_spent_in_usd,
        -- number of orders
        coalesce(c_o.number_of_pending_orders, 0)           AS number_of_pending_orders,
        coalesce(c_o.number_of_preparing_orders, 0)         AS number_of_preparing_orders,
        coalesce(c_o.number_of_shipped_orders, 0)           AS number_of_shipped_orders,
        coalesce(c_o.number_of_delivered_orders, 0)         AS number_of_delivered_orders,
        -- total number of orders should be equal to the sum of all the above
        coalesce (c_o.total_number_of_orders, 0)            AS total_number_of_orders,
        -- customer_value = average_order_cost_in_usd * total_number_of_orders
        coalesce(c_o.customer_value_in_usd, 0)              AS customer_value_in_usd
    FROM dim_customers AS c
    LEFT JOIN dim_addresses AS a ON c.address_id = a.address_id
    LEFT JOIN customer_orders__grouped AS c_o ON c.customer_id = c_o.customer_id
)

SELECT * FROM agg_orders_by_customer