WITH stg_customers AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__customers') }}
),

stg_dates AS (
    SELECT
        date_id,
        date_day
    FROM {{ ref('stg_staging__dates') }}
),

dim_customers AS (
    SELECT 
        c.customer_id   AS customer_id,
        d1.date_id      AS creation_date_id,
        d2.date_id      AS update_date_id,
        c.first_name    AS first_name,
        c.last_name     AS last_name,
        c.phone_number  AS phone_number,
        c.email         AS email
    FROM stg_customers AS c
    LEFT JOIN stg_dates AS d1 ON c.creation_date = d1.date_day
    LEFT JOIN stg_dates AS d2 ON c.update_date = d2.date_day
)

SELECT * FROM dim_customers