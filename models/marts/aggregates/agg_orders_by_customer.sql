{{
    config(
        materialized='incremental'
    )
}}

WITH fct_orders AS (
    SELECT
        order_id,
        customer_id,
        creation_date_id,
        creation_time,
        order_cost_in_usd,
        batched_at_utc
    FROM {{ ref('fct_orders') }}
    {% if is_incremental() %}
        WHERE batched_at_utc > (SELECT max(batched_at_utc) FROM {{ this }})
    {% endif %}
),

dim_dates AS (
    SELECT
        date_id,
        date_day
    FROM {{ ref('dim_dates') }}
),

customer_orders__grouped AS (
    SELECT
        o.customer_id,
        -- order dates
        cast(min(timestamp_from_parts(d.date_day, o.creation_time)) as date)      AS oldest_order_date,
        cast(max(timestamp_from_parts(d.date_day, o.creation_time)) as date)      AS most_recent_order_date,
        -- order cost aggregations
        cast(min(o.order_cost_in_usd) as number(38,2))                            AS cheapest_order_cost_in_usd,
        cast(max(o.order_cost_in_usd) as number(38,2))                            AS most_expensive_order_cost_in_usd,
        cast(avg(o.order_cost_in_usd) as number(38,2))                            AS average_order_cost_in_usd,
        cast(sum(o.order_cost_in_usd) as number(38,2))                            AS total_amount_spent_in_usd,
        -- total number of orders
        cast(count(o.customer_id) as number(38,2))                                  AS total_number_of_orders,
        -- customer_value = average_order_cost_in_usd * total_number_of_orders
        cast((avg(o.order_cost_in_usd) * count(o.customer_id)) as number(38,2))   AS customer_value_in_usd,
        o.batched_at_utc                                                          AS batched_at_utc
    FROM fct_orders AS o
    LEFT JOIN dim_dates AS d ON o.creation_date_id = d.date_id
    GROUP BY o.customer_id, o.batched_at_utc
),

dim_customers AS (
    SELECT *
    FROM {{ ref('dim_customers') }}
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
        coalesce(c_o.total_amount_spent_in_usd, 0)          AS total_amount_spent_in_usd,
        -- total number of orders should be equal to the sum of all the above
        coalesce (c_o.total_number_of_orders, 0)            AS total_number_of_orders,
        -- customer_value = average_order_cost_in_usd * total_number_of_orders
        coalesce(c_o.customer_value_in_usd, 0)              AS customer_value_in_usd,
        c_o.batched_at_utc                                  AS batched_at_utc
    FROM dim_customers AS c
    LEFT JOIN dim_addresses AS a ON c.address_id = a.address_id
    LEFT JOIN customer_orders__grouped AS c_o ON c.customer_id = c_o.customer_id
)

SELECT * FROM customer_orders__joined