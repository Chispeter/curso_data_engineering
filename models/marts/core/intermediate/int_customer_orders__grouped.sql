WITH stg_orders AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__orders') }}
),

int_customer_orders__grouped AS (
    SELECT order_customer_id,
            -- order dates
            cast(min(order_created_at_utc) as date) AS oldest_order_date,
            cast(max(order_created_at_utc) as date) AS most_recent_order_date,
            -- order cost aggregations
            cast(min(order_cost_in_usd) as number(38,2)) AS cheapest_order_cost_in_usd,
            cast(max(order_cost_in_usd) as number(38,2)) AS most_expensive_order_cost_in_usd,
            cast(avg(order_cost_in_usd) as number(38,2)) AS average_order_cost_in_usd,
            cast(sum(order_cost_in_usd) as number(38,2)) AS total_amount_spent_in_usd,
            -- number of orders
            cast(count(case when order_status = 'no status' then 1 end) as number(38,2))) AS number_of_pending_orders,
            cast(count(case when order_status = 'preparing' then 1 end) as number(38,2))) AS number_of_preparing_orders,
            cast(count(case when order_status = 'shipped' then 1 end) as number(38,2))) AS number_of_shipped_orders,
            cast(count(case when order_status = 'delivered' then 1 end as number(38,2))) AS number_of_delivered_orders,
            -- total number of orders should be equal to the sum of all the above
            cast(count(order_customer_id) as number(38,2)) AS total_number_of_orders,
            -- customer_value = average_order_cost_in_usd * total_number_of_orders
            cast((avg(order_cost_in_usd) *  count(order_customer_id)) as number(38,2)) AS customer_value_in_usd
    FROM stg_orders
    GROUP BY order_customer_id
    )

SELECT * FROM int_customer_orders__grouped