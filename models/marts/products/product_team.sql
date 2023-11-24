WITH products AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__products') }}
),

events AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__events') }}
),

customers AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__customers') }}
),

product_team AS (
    SELECT events.event_session_id AS session_id,
            events.event_customer_id AS customer_id,
            customers.customer_first_name AS first_name,
            customers.customer_email AS email
    FROM events
    JOIN customers ON events.event_customer_id = customers.customer_id
)

SELECT * FROM product_team