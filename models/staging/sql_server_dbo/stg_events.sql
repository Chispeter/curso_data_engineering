{% set ref_table = source('sql_server_dbo', 'events') %}

WITH src_events AS (
    SELECT * 
    FROM {{ref_table}}
    ),

stg_events AS (
    SELECT trim(event_id, ' ')::varchar(50) AS event_id,
            {{replace_empty_or_null_values_with_tag('ref_table', product_id, 'xD')}} AS product_id,
            (CASE WHEN (trim(user_id, ' ') = '' OR trim(user_id, ' ') IS NULL)  THEN 'not_registered'
                    ELSE trim(user_id, ' ')
                    END)::varchar(50) AS user_id,
            (CASE WHEN (trim(user_id, ' ') = '' OR trim(user_id, ' ') IS NULL)  THEN 'not_registered'
                    ELSE trim(user_id, ' ')
                    END)::varchar(50) AS user_id,
            (CASE WHEN (trim(order_id, ' ') = '' OR trim(order_id, ' ') IS NULL) THEN 'not_registered'
                    ELSE trim(order_id, ' ')
                    END)::varchar(50) AS order_id,
            (CASE WHEN (trim(session_id, ' ') = '' OR trim(session_id, ' ') IS NULL) THEN 'not_registered'
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

