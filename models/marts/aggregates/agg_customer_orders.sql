WITH int_customers_dates_id__joined AS (
    SELECT *
    FROM {{ ref('int_customers_dates_id__joined') }}
),

int_order_header_dates_id__joined AS (
    SELECT *
    FROM {{ ref('int_order_header_dates_id__joined') }}
),

int_orders_dates_id__joined AS (

)

agg_customer_orders AS (
    SELECT 
        -- DIM_CUSTOMERS
        -- customer data
        c_d.customer_id                                     AS customer_id,
        c_d.first_name                                      AS first_name,
        c_d.last_name                                       AS last_name,
        c_d.phone_number                                    AS phone_number,
        c_d.email                                           AS email,
        c_d.creation_date                                   AS creation_date,
        c_d.update_date                                     AS update_date,
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
    FROM int_customers_date__joined AS c_d
    LEFT JOIN int_customer_orders__grouped AS c_o ON c_d.customer_id = c_o.customer_id
)

SELECT * FROM agg_customer_orders