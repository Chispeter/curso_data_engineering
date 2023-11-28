WITH int_customer_addresses__joined AS (
    SELECT * 
    FROM {{ ref('int_customer_addresses__joined') }}
),

int_address_customers__grouped AS (
    SELECT -- INT_CUSTOMER_ADDRESSES__JOINED (address data of each customer_id)
            -- address data
            customer_addresses.address_id AS address_id,
            customer_addresses.address_street_number AS address_street_number,
            customer_addresses.address_street_name AS address_street_name,
            customer_addresses.address_state_name AS address_state_name,
            customer_addresses.address_zipcode AS address_zipcode,
            customer_addresses.address_country_name AS address_country_name,
            -- total number of customers living in each address_id
            cast(count(*) as number(38,0)) AS total_number_of_customers
    FROM int_customer_addresses__joined AS customer_addresses
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT * FROM int_address_customers__grouped