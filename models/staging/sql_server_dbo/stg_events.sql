WITH src_events AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'events') }}
    ),

stg_events AS (
    SELECT trim(event_id, ' ')::varchar(50) AS event_id,
            (CASE WHEN trim(user_id, ' ') = '' THEN 'not_registered'
                    ELSE trim(user_id, ' ')
                    END)::varchar(50) AS user_id,
            (CASE WHEN trim(product_id, ' ') = '' THEN 'not_registered'
                    ELSE trim(product_id, ' ')
                    END)::varchar(50) AS product_id,
            (CASE WHEN trim(order_id, ' ') = '' THEN 'not_registered'
                    ELSE trim(order_id, ' ')
                    END)::varchar(50) AS order_id,
            (CASE WHEN trim(session_id, ' ') = '' THEN 'not_registered'
                    ELSE trim(session_id, ' ')
                    END)::varchar(50) AS session_id,
            lower(trim(page_url, ' '))::varchar(200) AS event_page_url,
            lower(trim(event_type, ' '))::varchar(50) AS event_type,
            trim(created_at, ' ')::timestamp_ntz AS event_created_at_pt,
            coalesce(_fivetran_deleted, false) AS was_this_event_row_deleted,
            _fivetran_synced::date AS event_load_date
    FROM src_events
    )

SELECT * FROM stg_events