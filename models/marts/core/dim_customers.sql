WITH stg_customers AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__customers') }}
),

dim_customers AS (
    SELECT
        -- STG_CUSTOMERS (customer data of each customer_id)
        -- customer data 
        customer_id,
        customer_first_name,
        customer_last_name,
        customer_phone_number,
        customer_email,
        customer_created_at_utc,
        customer_updated_at_utc
    FROM stg_customers
)

SELECT * FROM dim_customers