WITH src_sql_server_dbo_events AS (
    SELECT *
    FROM {{ source('sql_server_dbo', 'events') }}

    {% if is_incremental() %}

        WHERE _fivetran_synced > (SELECT max(_fivetran_synced) FROM {{ this }})

    {% endif %}
),

base_sql_server_dbo_events AS (
    SELECT event_id,
            page_url,
            event_type,
            user_id,
            decode(product_id, '', {{ dbt_utils.generate_surrogate_key(['null']) }}, product_id) AS product_id,
            session_id,
            created_at,
            decode(order_id, '', {{ dbt_utils.generate_surrogate_key(['null']) }}, order_id) AS order_id,
            _fivetran_deleted,
            _fivetran_synced
FROM src_sql_server_dbo_events
)

SELECT * FROM base_sql_server_dbo_events
