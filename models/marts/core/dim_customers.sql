WITH int_customers_dates__joined AS (
    SELECT *
    FROM {{ ref('int_customers_dates__joined') }}
),

dim_customers AS (
    SELECT 
        customer_id,
        address_id,
        creation_date_id,
        update_date_id,
        creation_time,
        update_time,
        first_name,
        last_name,
        phone_number,
        email
    FROM int_customers_dates__joined
)

SELECT * FROM dim_customers