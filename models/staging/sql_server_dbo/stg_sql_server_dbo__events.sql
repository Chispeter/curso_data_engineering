WITH src_sql_server_dbo__events AS (
    SELECT *
    FROM {{ source('sql_server_dbo', 'events') }}
    {% if is_incremental() %}
    WHERE _fivetran_synced > (SELECT max(batched_at_utc) FROM {{ this }})
    {% endif %}
),

stg_sql_server_dbo__events AS (
    SELECT
        cast(event_id as varchar(50))                                                                               AS event_id,
        cast(user_id as varchar(50))                                                                                AS customer_id,
        cast(session_id as varchar(50))                                                                             AS session_id,
        cast(decode(product_id, '', {{ dbt_utils.generate_surrogate_key(['null']) }}, product_id) as varchar(50))   AS product_id,
        cast(decode(order_id, '', {{ dbt_utils.generate_surrogate_key(['null']) }}, order_id) as varchar(50))       AS order_id,
        cast(created_at as timestamp_tz(9))                                                                         AS event_created_at_utc,
        cast(page_url as varchar(200))                                                                              AS page_url,
        cast(event_type as varchar(50))                                                                             AS event_type,
        cast(coalesce(_fivetran_deleted, false) as boolean)                                                         AS was_this_row_deleted,
        cast(_fivetran_synced as timestamp_tz(9))                                                                   AS batched_at_utc
    FROM src_sql_server_dbo__events
)

SELECT * FROM stg_sql_server_dbo__events
