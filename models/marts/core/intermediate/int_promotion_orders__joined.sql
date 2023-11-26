WITH snapshot_promotions AS (
    SELECT * 
    FROM {{ ref('snapshot_promotions') }}
    ),
    
int_customer_orders__grouped AS (
    SELECT * 
    FROM {{ ref('int_customer_orders__grouped') }}
),

int_promotion_orders__joined AS (
    SELECT -- SNAPSHOT_PROMOTIONS
            -- promotion data
            customers.customer_id AS customer_id,
            customers.customer_first_name AS customer_first_name,
            customers.customer_last_name AS customer_last_name,
            customers.customer_phone_number AS customer_phone_number,
            customers.customer_email AS customer_email,
            -- INT_PROMOTION_ORDERS__GROUPED (order data of each promotion_id)
            -- order dates
            promotion_orders.oldest_order_date AS oldest_order_date,
            promotion_orders.most_recent_order_date AS most_recent_order_date,
            -- order cost aggregations
            coalesce(promotion_orders.cheapest_order_cost_in_usd, 0) AS cheapest_order_cost_in_usd,
            coalesce(promotion_orders.most_expensive_order_cost_in_usd, 0) AS most_expensive_order_cost_in_usd,
            coalesce(promotion_orders.average_order_cost_in_usd, 0) AS average_order_cost_in_usd,
            coalesce(promotion_orders.total_amount_spent_in_usd, 0) AS total_amount_spent_in_usd,
            -- number of orders
            coalesce (promotion_orders.number_of_pending_orders, 0) AS number_of_pending_orders,
            coalesce (promotion_orders.number_of_preparing_orders, 0) AS number_of_preparing_orders,
            coalesce (promotion_orders.number_of_shipped_orders, 0) AS number_of_shipped_orders,
            coalesce (promotion_orders.number_of_delivered_orders, 0) AS number_of_delivered_orders,
            -- number of total orders should be equal to the sum of all the above
            coalesce (promotion_orders.number_of_total_orders, 0) AS number_of_total_orders,
            -- promotion_value = average_order_cost_in_usd * number_of_total_orders
            coalesce(promotion_orders.customer_value_in_usd, 0) AS promotion_value_in_usd
    FROM snapshot_promotions AS promotions
    LEFT JOIN int_promotion_orders__grouped AS promotion_orders ON promotions.promotion_id = promotion_orders.order_promotion_id
    
    )

SELECT * FROM int_promotion_orders__joined