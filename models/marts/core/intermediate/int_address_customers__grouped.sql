WITH stg_customers AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__customers') }}
),

stg_addresses AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__addresses') }}
),

int_address_customers__joined AS (
    SELECT addresses.address_id AS address_id,
            addresses.address_street_number,
            addresses.address_street_name,
            addresses.address_state_name,
            addresses.address_zipcode,
            addresses.address_country_name,
            customers.customer_first_name AS customer_first_name,
            customers.customer_last_name AS customer_last_name,
            customers.customer_phone_number AS customer_phone_number,
            customers.customer_email AS customer_email
    FROM stg_addresses AS addresses
    LEFT JOIN stg_customers AS customers ON addresses.address_id = customers.customer_address_id
    )

SELECT * FROM int_address_customers__joined