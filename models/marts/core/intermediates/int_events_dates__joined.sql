WITH stg_events AS (
    SELECT
        event_id,
        customer_id,
        session_id,
        product_id,
        order_id,
        cast(created_at_utc as date) AS creation_date,
        cast(created_at_utc as time) AS creation_time,
        page_url,
        event_type
    FROM {{ ref('stg_sql_server_dbo__events') }}
),

stg_dates AS (
    SELECT
        date_id,
        date_day
    FROM {{ ref('stg_staging__dates') }}
),

int_events_dates__joined AS (
    SELECT
        e.event_id              AS event_id,
        e.customer_id           AS customer_id,
        e.session_id            AS session_id,
        e.product_id            AS product_id,
        e.order_id              AS order_id,
        d.date_id               AS creation_date_id,
        e.creation_time         AS creation_time,
        e.page_url              AS page_url,
        e.event_type            AS event_type
    FROM stg_events AS e
    LEFT JOIN stg_dates AS d ON e.creation_date = d.date_day
)

SELECT * FROM int_events_dates__joined