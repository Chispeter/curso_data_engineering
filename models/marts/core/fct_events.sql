WITH stg_events AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__events') }}
),

fct_events AS (
    SELECT
        event_id,
        customer_id,
        session_id,
        product_id,
        order_id,
        event_created_at_utc,
        page_url,
        event_type
    FROM stg_events
    )

SELECT * FROM fct_events