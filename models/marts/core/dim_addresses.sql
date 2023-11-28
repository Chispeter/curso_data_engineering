WITH int_address_customers__grouped AS (
    SELECT * 
    FROM {{ ref('int_address_customers__grouped') }}
),

dim_addresses AS (
    SELECT
        -- INT_ADDRESS_CUSTOMERS__GROUPED (customer data of each address_id)
        -- address data
        address_customers.address_id AS address_id,
        address_customers.address_street_number AS address_street_number,
        address_customers.address_street_name AS address_street_name,
        address_customers.address_state_name AS address_state_name,
        address_customers.address_zipcode AS address_zipcode,
        address_customers.address_country_name AS address_country_name,
        -- total number of customers living in each address_id
        address_customers.total_number_of_customers AS total_number_of_customers

    FROM int_address_customers__grouped AS address_customers
    )

SELECT * FROM dim_addresses