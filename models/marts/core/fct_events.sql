WITH stg_events AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__events') }}
),

fct_events AS (
    SELECT
        event_id,
        event_customer_id,
        event_session_id,
        event_product_id,
        event_order_id,
        event_created_at_utc,
        event_page_url,
        event_type
    FROM stg_events
    )

SELECT * FROM fct_events