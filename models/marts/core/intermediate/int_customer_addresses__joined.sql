WITH stg_customers AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__customers') }}
),

stg_addresses AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__addresses') }}
),

int_customer_addresses__joined AS (
    SELECT customers.customer_id AS customer_id,
            customers.customer_first_name AS customer_first_name,
            customers.customer_last_name AS customer_last_name,
            customers.customer_phone_number AS customer_phone_number,
            customers.customer_email AS customer_email,
            addresses.address_street_number,
            addresses.address_street_name,
            addresses.address_state_name,
            addresses.address_zipcode,
            addresses.address_country_name
    FROM stg_customers AS customers
    LEFT JOIN stg_addresses AS addresses ON customers.customer_address_id = addresses.address_id
    )

SELECT * FROM int_customer_addresses__joined