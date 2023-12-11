WITH dim_customers AS (
    SELECT *
    FROM {{ ref('dim_customers') }}
),

stg_dates AS (
    SELECT
        date_id,
        date_day
    FROM {{ ref('stg_staging__dates') }}
),

int_customers_date__joined AS (
    SELECT 
        c.customer_id    AS customer_id,
        d1.date_day      AS creation_date,
        d2.date_day      AS update_date,
        c.first_name     AS first_name,
        c.last_name      AS last_name,
        c.phone_number   AS phone_number,
        c.email          AS email
    FROM dim_customers AS c
    LEFT JOIN stg_dates AS d1 ON c.creation_date_id = d1.date_id
    LEFT JOIN stg_dates AS d2 ON c.update_date_id = d2.date_id
)

SELECT * FROM int_customers_date__joined