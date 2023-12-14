-- 4. 1. En promedio, ¿Cuántas sesiones únicas tenemos por hora?

WITH fct_events AS (
    SELECT
        session_id,
        creation_time
    FROM {{ ref('fct_events') }}
),

total_number_of_unique_sessions_per_hour AS (
    SELECT
        count(distinct(session_id)) AS number_of_unique_sessions,
        date_trunc('hour', creation_time) AS event_creation_hour
    FROM fct_events
    GROUP BY event_creation_hour
    ORDER BY event_creation_hour ASC
)

SELECT * FROM total_number_of_unique_sessions_per_hour