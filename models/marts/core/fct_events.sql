WITH stg_events AS (
    SELECT
        event_id,
        cast(event_created_at_utc as date) AS event_creation_date,
        cast(event_created_at_utc as time) AS event_creation_time,
        customer_id,
        session_id,
        product_id,
        order_id,
        page_url,
        event_type
    FROM {{ ref('stg_sql_server_dbo__events') }}
),

stg_dates AS (
    SELECT * 
    FROM {{ ref('stg_staging__dates') }}
),

fct_events AS (
    SELECT
        e.event_id              AS event_id,
        e.customer_id           AS customer_id,
        e.session_id            AS session_id,
        e.product_id            AS product_id,
        e.order_id              AS order_id,
        d.date_id               AS event_creation_date_id,
        e.event_creation_time   AS event_creation_time,
        e.page_url              AS page_url,
        e.event_type            AS event_type
    FROM stg_events AS e
    LEFT JOIN stg_dates AS d ON e.event_creation_date = d.date_day
)

SELECT * FROM fct_events