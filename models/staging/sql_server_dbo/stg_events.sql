-- Using macros:
-- replace_empty_and_null_values_with_tag(column, tag)
WITH src_events AS (
    SELECT *
    FROM {{ source('sql_server_dbo', 'events') }}
),

stg_events AS (
    SELECT
        {{ replace_empty_and_null_values_with_tag('event_id', 'not_registered') }}::varchar(50) AS event_id,
        {{ replace_empty_and_null_values_with_tag('user_id', 'not_registered') }}::varchar(50) AS user_id,
        {{ replace_empty_and_null_values_with_tag('session_id', 'not_registered') }}::varchar(50) AS session_id,
        {{ replace_empty_and_null_values_with_tag('product_id', 'not_registered') }}::varchar(50) AS product_id,
        {{ replace_empty_and_null_values_with_tag('order_id', 'not_registered') }}::varchar(50) AS order_id,
        lower(trim(page_url, ' '))::varchar(200) AS event_page_url,
        lower(trim(event_type, ' '))::varchar(50) AS event_type,
        trim(created_at, ' ')::timestamp_ntz AS event_created_at_pt,
        coalesce(_fivetran_deleted, false) AS was_this_event_row_deleted,
        _fivetran_synced::date AS event_load_date
    FROM src_events
)

SELECT * FROM stg_events
