WITH int_events_dates__joined AS (
    SELECT *
    FROM {{ ref('int_events_dates__joined') }}
),

fct_events AS (
    SELECT
        event_id,
        customer_id,
        session_id,
        product_id,
        order_id,
        creation_date_id,
        creation_time,
        page_url,
        event_type
    FROM int_events_dates__joined
)

SELECT * FROM fct_events