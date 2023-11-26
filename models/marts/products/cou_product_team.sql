WITH events AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__events') }}
),

customers AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__customers') }}
),

cou_product_team AS (
    SELECT events.event_session_id AS session_id,
            events.event_customer_id AS customer_id,
            customers.customer_first_name AS customer_first_name,
            customers.customer_email AS customer_email,
            min(events.event_created_at_utc) AS first_event_time_utc,
            max(events.event_created_at_utc) AS last_event_time_utc,
            timestampdiff(minute, min(cast(events.event_created_at_utc as timestamp_ntz(9))), max(cast(events.event_created_at_utc as timestamp_ntz(9)))) AS session_length_minutes,
            count(case when events.event_type = 'page_view' then 1 end) AS page_view,
            count(case when events.event_type = 'add_to_cart' then 1 end) AS add_to_cart,
            count(case when events.event_type = 'checkout' then 1 end) AS checkout,
            count(case when events.event_type = 'package_shipped' then 1 end) AS package_shipped
    FROM events
    LEFT JOIN customers ON events.event_customer_id = customers.customer_id
    GROUP BY 1, 2, 3, 4
)

SELECT * FROM cou_product_team