WITH stg_events AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__events') }}
),

int_order_events__grouped AS (
    SELECT event_order_id,
            -- event dates
            cast(min(event_created_at_utc) as date) AS oldest_event_date,
            cast(max(event_created_at_utc) as date) AS most_recent_event_date,
            -- number of events
            cast(count(case when event_type = 'checkout' then 1 end) as number(38,0)) AS number_of_checkout_events,
            cast(count(case when event_type = 'package_shipped' then 1 end) as number(38,0)) AS number_of_package_shipped_events,
            -- total number of events should be equal to the sum of all the above, except in one case
            -- that counts null order_ids in the events table (in other words, that row counts
            -- the total number of events related to the product_ids)
            cast(count(event_product_id) as number(38,0)) AS total_number_of_events,
            -- conditional probability of package shipped after checking it out
            case when number_of_checkout_events = 0 then 0
                    else cast(100*number_of_package_shipped_events/number_of_checkout_events as number(38,2))
                    end AS probability_of_package_shipped_in_percentage
    FROM stg_events
    GROUP BY event_order_id
    )

SELECT * FROM int_order_events__grouped