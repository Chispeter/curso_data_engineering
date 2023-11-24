WITH stg_customers AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__customers') }}
),

number_of_total_customers AS (
    SELECT count(*) AS number_of_total_customers
    FROM stg_customers
)

SELECT * FROM number_of_total_customers