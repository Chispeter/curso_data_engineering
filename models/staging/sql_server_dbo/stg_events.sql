WITH src_events AS (
    SELECT *
    FROM {{ source('sql_server_dbo', 'events') }}
),

stg_events AS (
    SELECT
        {{ replace_empty_and_null_values_with_tag('event_id', 'not registered') }}::varchar(50) AS event_id,
        {{ replace_empty_and_null_values_with_tag('user_id', 'not registered') }}::varchar(50) AS user_id,
        {{ replace_empty_and_null_values_with_tag('session_id', 'not registered') }}::varchar(50) AS session_id,
        {{ replace_empty_and_null_values_with_tag('product_id', 'not registered') }}::varchar(50) AS product_id,
        {{ replace_empty_and_null_values_with_tag('order_id', 'not registered') }}::varchar(50) AS order_id,
        {{ replace_empty_and_null_values_with_tag('created_at', 'not defined') }}::timestamp_ntz AS event_created_at_pt,
        {{ replace_empty_and_null_values_with_tag(get_lowercased_column('page_url'), 'not defined') }}::varchar(200) AS event_page_url,
        {{ replace_empty_and_null_values_with_tag(get_lowercased_column('event_type'), 'not defined') }}::varchar(50) AS event_type,
        coalesce(_fivetran_deleted, false) AS was_this_event_row_deleted,
        _fivetran_synced::date AS event_load_date
    FROM src_events
)

SELECT * FROM stg_events
