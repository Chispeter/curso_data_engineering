WITH snapshot_customers AS (
    SELECT * 
    FROM {{ ref('snapshot_customers') }}
    ),
    
stg_addresses AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__addresses') }}
),

int_customer_addresses__joined AS (
    SELECT -- SNAPSHOT_CUSTOMERS
            -- customer data
            customers.customer_id AS customer_id,
            customers.customer_first_name AS customer_first_name,
            customers.customer_last_name AS customer_last_name,
            customers.customer_phone_number AS customer_phone_number,
            customers.customer_email AS customer_email,
            customers.customer_created_at_utc AS customer_created_at_utc,
            customers.customer_updated_at_utc AS customer_updated_at_utc,
            -- INT_CUSTOMER_ADDRESSES__JOINED (address data of each customer_id)
            -- address data
            addresses.address_id AS address_id,
            addresses.address_street_number AS address_street_number,
            addresses.address_street_name AS address_street_name,
            addresses.address_state_name AS address_state_name,
            addresses.address_zipcode AS address_zipcode,
            addresses.address_country_name AS address_country_name
    FROM snapshot_customers AS customers
    LEFT JOIN stg_addresses AS addresses ON customers.customer_address_id = addresses.address_id
    
    )

SELECT * FROM int_customer_addresses__joined