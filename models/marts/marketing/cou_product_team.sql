WITH fct_events AS (
    SELECT
        session_id,
        customer_id,
        creation_date_id,
        creation_time,
        event_type
    FROM {{ ref('fct_events') }}
),

dim_dates AS (
    SELECT
        date_id,
        date_day
    FROM {{ ref('dim_dates') }}
),

dim_customers AS (
    SELECT *
    FROM {{ ref('dim_customers') }}
),

cou_product_team AS (
    SELECT e.session_id AS session_id,
            e.customer_id AS customer_id,
            c.first_name AS customer_first_name,
            c.email AS customer_email,
            min((timestamp_from_parts(d.date_day, e.creation_time))) AS first_event_created_at_utc,
            max((timestamp_from_parts(d.date_day, e.creation_time))) AS last_event_created_at_utc,
            timestampdiff(minute, min((timestamp_from_parts(d.date_day, e.creation_time))), max((timestamp_from_parts(d.date_day, e.creation_time)))) AS session_length_in_minutes,
            count(case when e.event_type = 'page_view' then 1 end) AS page_view,
            count(case when e.event_type = 'add_to_cart' then 1 end) AS add_to_cart,
            count(case when e.event_type = 'checkout' then 1 end) AS checkout,
            count(case when e.event_type = 'package_shipped' then 1 end) AS package_shipped
    FROM fct_events AS e
    LEFT JOIN dim_customers AS c ON e.customer_id = c.customer_id
    LEFT JOIN dim_dates AS d ON e.creation_date_id = d.date_id
    GROUP BY 1, 2, 3, 4
)

SELECT * FROM cou_product_team