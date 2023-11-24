WITH stg_events AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__events') }}
),

unique_sessions_per_hour AS (
    SELECT COUNT(DISTINCT(event_session_id)) AS number_of_unique_sessions,
            year(date(event_created_at_utc)) AS event_year,
            month(date(event_created_at_utc)) AS event_month,
            day(date(event_created_at_utc)) AS event_day,
            date_trunc('hour', time(event_created_at_utc)) AS event_hour
    FROM stg_events
    GROUP BY 2, 3, 4, 5
    ORDER BY 2 ASC, 3 ASC, 4 ASC, 5 ASC
)

SELECT * FROM unique_sessions_per_hour