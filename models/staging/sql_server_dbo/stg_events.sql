WITH src_events AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'events') }}
    ),

stg_events AS (
    SELECT event_id,
            user_id,
            product_id,
            order_id,
            session_id,
            page_url,
            event_type,
            created_at,
            _fivetran_deleted,
            _fivetran_synced AS batched_at
    FROM src_events
    )

SELECT * FROM stg_events