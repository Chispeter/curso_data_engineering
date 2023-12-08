WITH stg_customers AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__customers') }}
),

dim_customers AS (
    SELECT 
        customer_id,
        first_name,
        last_name,
        phone_number,
        email,
        created_at_utc,
        updated_at_utc
    FROM stg_customers
)

SELECT * FROM dim_customers