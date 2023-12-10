{% set event_types = get_unique_values_from_column(ref('stg_sql_server_dbo__events'), 'event_type') %}

WITH stg_events AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__events') }}
),

int_customer_events__grouped AS (
    SELECT event_customer_id,
            -- event dates
            cast(min(event_created_at_utc) as date) AS oldest_event_date,
            cast(max(event_created_at_utc) as date) AS most_recent_event_date,
            -- number of events
            {%- for event_type in event_types %}
                cast(sum(case when event_type = '{{event_type}}' then 1 else 0 end) as number(38,0)) AS number_of_{{event_type}}_events,
            --{%- if not loop.last %},{% endif -%}
            {% endfor %}
            -- total number of events should be equal to the sum of all the above
            cast(count(event_customer_id) as number(38,0)) AS total_number_of_events,
            -- conditional probabilities of events
            -- % probability of adding a product to the cart after viewing and analyzing it
            cast(100*number_of_add_to_cart_events/number_of_page_view_events as number(38,2)) AS probability_of_add_to_cart_in_percentage,
            -- % probability of checkout a order after adding products to the cart
            cast(100*number_of_checkout_events/number_of_add_to_cart_events as number(38,2)) AS probability_of_checkout_in_percentage,
            -- % probability of ending in a purchase
            cast(100*number_of_package_shipped_events/number_of_checkout_events as number(38,2)) AS probability_of_package_shipped_in_percentage
    FROM stg_events
    GROUP BY event_customer_id
    )

SELECT * FROM int_customer_events__grouped