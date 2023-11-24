WITH base_sql_server_dbo__events AS (
    SELECT *
    FROM {{ ref('base_sql_server_dbo__events') }}
),

stg_sql_server_dbo__events AS (
    SELECT
        cast(event_id as varchar(50)) AS event_id,
        cast(user_id as varchar(50)) AS event_customer_id,
        cast(session_id as varchar(50)) AS event_session_id,
        cast(product_id as varchar(50)) AS event_product_id,
        cast(order_id as varchar(50)) AS event_order_id,
        cast(created_at as timestamp_tz(9)) AS event_created_at_utc,
        cast(page_url as varchar(200)) AS event_page_url,
        cast(event_type as varchar(50)) AS event_type,
        cast(coalesce(_fivetran_deleted, false) as boolean) AS was_this_event_row_deleted,
        cast(_fivetran_synced as timestamp_tz(9)) AS event_batched_at_utc
    FROM base_sql_server_dbo__events
)

SELECT * FROM stg_sql_server_dbo__events
