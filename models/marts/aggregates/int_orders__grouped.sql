WITH stg_orders AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__orders') }}
),

int_orders__grouped AS (
    SELECT
        order_id,
        order_customer_id,
        order_address_id,
        order_promotion_id,
        order_tracking_id,
        order_shipping_service_name,
        order_shipping_service_cost_in_usd,
        order_cost_in_usd,
        order_total_cost_in_usd,
        order_status,
        order_created_at_utc,
        order_estimated_delivery_at_utc,
        order_delivered_at_utc,
        -- order_estimated_delivery_time_in_hours is the time in hours that elapses from the creation of the order to the date on which the product is estimated to be delivered to the customer.
        cast(coalesce(timestampdiff(hour, min(cast(order_created_at_utc as timestamp_ntz(9))), max(cast(order_estimated_delivery_at_utc as timestamp_ntz(9)))), 0) as number(38,0)) AS order_estimated_delivery_time_in_hours,
        -- order_delivery_time_in_hours is the time in hours that elapses from the creation of the order to the date on which the product is delivered to the customer.
        cast(coalesce(timestampdiff(hour, min(cast(order_created_at_utc as timestamp_ntz(9))), max(cast(order_delivered_at_utc as timestamp_ntz(9)))), 0) as number(38,0)) AS order_delivery_time_in_hours,
        -- order_delivery_time_deviation_in_hours is the time in hours between estimated delivery time and delivery time
        order_estimated_delivery_time_in_hours - order_delivery_time_in_hours AS order_delivery_time_deviation_in_hours,
        -- order_delivery_status is a more detailed order_status that is related to delivery time deviation
        cast(case when order_delivery_time_deviation_in_hours > 0
                    AND order_delivery_time_in_hours = 0 then 'shipped'
                when order_delivery_time_deviation_in_hours > 0 then 'delivered early'
                when order_delivery_time_deviation_in_hours < 0 then 'delivered late'
                when order_delivery_time_deviation_in_hours = 0
                        AND order_estimated_delivery_time_in_hours != 0
                        AND order_delivery_time_in_hours != 0 then 'delivered on time'
                else 'preparing' end as varchar(17)) AS order_delivery_status,
        -- deadline_compliance_rating
        (case when (order_delivery_status = 'delivered early'
                    OR order_delivery_status = 'delivered on time') then true
                when order_delivery_status = 'delivered late' then false
                end) AS were_deadlines_being_met
    FROM stg_orders
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
    )

SELECT * FROM int_orders__grouped