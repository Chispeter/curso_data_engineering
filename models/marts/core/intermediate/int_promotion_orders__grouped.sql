WITH stg_orders AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__orders') }}
),

int_promotion_orders__grouped AS (
    SELECT order_promotion_id,
            -- order dates
            cast(min(order_created_at_utc) as date) AS oldest_order_date,
            cast(max(order_created_at_utc) as date) AS most_recent_order_date,
            -- order cost aggregations
            min(order_cost_in_usd) AS cheapest_order_cost_in_usd,
            max(order_cost_in_usd) AS most_expensive_order_cost_in_usd,
            cast(avg(order_cost_in_usd) as number(38,2)) AS average_order_cost_in_usd,
            sum(order_cost_in_usd) AS total_amount_spent_in_usd,
            -- number of orders
            count(case when order_status = 'no status' then 1 end) AS number_of_pending_orders,
            count(case when order_status = 'preparing' then 1 end) AS number_of_preparing_orders,
            count(case when order_status = 'shipped' then 1 end) AS number_of_shipped_orders,
            count(case when order_status = 'delivered' then 1 end) AS number_of_delivered_orders,
            -- total number of orders should be equal to the sum of all the above
            count(order_promotion_id) AS total_number_of_orders
    FROM stg_orders
    GROUP BY order_promotion_id
    )

SELECT * FROM int_promotion_orders__grouped