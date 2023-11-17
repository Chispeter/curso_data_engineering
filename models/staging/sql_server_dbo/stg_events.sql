WITH src_events AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'events') }}
    ),

stg_events AS (
    SELECT trim(event_id, ' ')::varchar(50) AS event_id,
            trim(user_id, ' ')::varchar(50) AS user_id,
            trim(product_id, ' ')::varchar(50) AS product_id,
            trim(order_id, ' ')::varchar(50) AS order_id,
            trim(session_id, ' ')::varchar(50) AS session_id,
            trim(page_url, ' ')::varchar(200) AS page_url,
            trim(event_type, ' ')::varchar(50) AS event_type,
            trim(created_at, ' ')::timestamp_ntz AS created_at_pt,
            coalesce(_fivetran_deleted, false) AS was_this_event_row_deleted,
            _fivetran_synced::date AS event_load_date
    FROM src_events
    )

SELECT * FROM stg_events