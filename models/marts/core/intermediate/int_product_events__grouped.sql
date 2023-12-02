WITH stg_events AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__events') }}
),

int_product_events__grouped AS (
    SELECT event_product_id,
            -- event dates
            cast(min(event_created_at_utc) as date) AS oldest_event_date,
            cast(max(event_created_at_utc) as date) AS most_recent_event_date,
            -- number of events
            cast(count(case when event_type = 'page_view' then 1 end) as number(38,0)) AS number_of_page_view_events,
            cast(count(case when event_type = 'add_to_cart' then 1 end) as number(38,0)) AS number_of_add_to_cart_events,
            -- total number of events should be equal to the sum of all the above, except in the first row,
            -- which counts null product_ids in the events table (in other words, the first row counts
            -- the total number of events related to the order_ids)
            cast(count(event_product_id) as number(38,0)) AS total_number_of_events,
            -- probability of adding a product to the cart after viewing and analyzing it
            case when number_of_page_view_events = 0 then 0
                    else cast(100*number_of_add_to_cart_events/number_of_page_view_events as number(38,2))
                    end AS probability_of_add_to_cart_in_percentage
    FROM stg_events
    GROUP BY event_product_id
    )

SELECT * FROM int_product_events__grouped