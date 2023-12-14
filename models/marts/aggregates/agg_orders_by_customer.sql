{{
    config(
        materialized='incremental'
    )
}}

WITH fct_order_products AS (
    SELECT
        order_id,
        product_id,
        customer_id,
        promotion_id,
        creation_date_id,
        number_of_units_of_product_sold,
        order_shipping_service_cost_in_usd,
        batched_at_utc
    FROM {{ ref('fct_order_products') }}
    {% if is_incremental() %}
        WHERE batched_at_utc > (SELECT max(batched_at_utc) FROM {{ this }})
    {% endif %}
),

dim_products AS (
    SELECT
        product_id,
        price_in_usd
    FROM {{ ref('dim_products') }}
),

dim_promotions AS (
    SELECT
        promotion_id,
        discount_in_usd
    FROM {{ ref('dim_promotions') }}
),

dim_dates AS (
    SELECT
        date_id,
        date_day
    FROM {{ ref('dim_dates') }}
),

metrics_by_order AS (
    SELECT
        o_p.order_id,
        o_p.customer_id,
        o_p.promotion_id,
        sum(o_p.number_of_units_of_product_sold * prod.price_in_usd)     AS order_cost_in_usd,
        o_p.order_shipping_service_cost_in_usd,
        prom.discount_in_usd                                             AS order_promotion_discount_in_usd,
        d.date_day                                                       AS creation_date,
        o_p.batched_at_utc
    FROM fct_order_products AS o_p
    LEFT JOIN dim_promotions AS prom ON o_p.promotion_id = prom.promotion_id
    LEFT JOIN dim_products AS prod ON o_p.product_id = prod.product_id
    LEFT JOIN dim_dates AS d ON o_p.creation_date_id = d.date_id
    GROUP BY 1, 2, 3, 5, 6, 7, 8
),

customer_orders__grouped AS (
    SELECT
        customer_id,
        -- order dates
        cast(min(creation_date) as date)                                            AS oldest_order_date,
        cast(max(creation_date) as date)                                            AS most_recent_order_date,
        -- order cost aggregations
        cast(min(order_cost_in_usd) as number(38,2))                            AS cheapest_order_cost_in_usd,
        cast(max(order_cost_in_usd) as number(38,2))                            AS most_expensive_order_cost_in_usd,
        cast(avg(order_cost_in_usd) as number(38,2))                            AS average_order_cost_in_usd,
        cast(sum(order_cost_in_usd) as number(38,2))                            AS order_total_amount_spent_in_usd,
        cast(sum(order_shipping_service_cost_in_usd) as number(38,2))           AS shipping_service_total_amount_spent_in_usd,
        cast(sum(order_promotion_discount_in_usd) as number(38,2))              AS promotion_total_amount_saving_in_usd,
        -- total number of orders
        cast(count(order_id) as number(38,2))                                   AS total_number_of_orders,
        -- customer_value = average_order_cost_in_usd * total_number_of_orders
        cast((avg(order_cost_in_usd) * count(order_id)) as number(38,2))     AS customer_value_in_usd,
        batched_at_utc                                                          AS batched_at_utc
    FROM metrics_by_order
    GROUP BY customer_id, batched_at_utc
),

dim_customers AS (
    SELECT *
    FROM {{ ref('dim_customers') }}
    WHERE valid_to_utc IS NULL
),

dim_addresses AS (
    SELECT *
    FROM {{ ref('dim_addresses') }}
),

customer_orders__joined AS (
    SELECT
        -- DIM_CUSTOMERS (customer data)
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
        coalesce(c_o.order_total_amount_spent_in_usd, 0)    AS order_total_amount_spent_in_usd,
        coalesce(c_o.shipping_service_total_amount_spent_in_usd, 0)          AS shipping_service_total_amount_spent_in_usd,
        coalesce(c_o.promotion_total_amount_saving_in_usd, 0)          AS promotion_total_amount_saving_in_usd,
        -- total number of orders should be equal to the sum of all the above
        coalesce (c_o.total_number_of_orders, 0)            AS total_number_of_orders,
        -- customer_value = average_order_cost_in_usd * total_number_of_orders
        coalesce(c_o.customer_value_in_usd, 0)              AS customer_value_in_usd,
        c_o.batched_at_utc
    FROM dim_customers AS c
    LEFT JOIN dim_addresses AS a ON c.address_id = a.address_id
    LEFT JOIN customer_orders__grouped AS c_o ON c.customer_id = c_o.customer_id
)

SELECT * FROM customer_orders__joined