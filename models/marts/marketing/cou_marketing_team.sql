WITH int_customer_addresses__joined AS (
    SELECT *
    FROM {{ ref('int_customer_addresses__joined') }}
),

cou_marketing_team AS (
    SELECT customer_addresses.customer_id,
            customer_addresses.customer_first_name,
            customer_addresses.customer_last_name,
            customer_addresses.customer_email,
            customer_addresses.customer_phone_number,
            customer_addresses.customer_created_at_utc,
            customer_addresses.customer_updated_at_utc,
            customer_addresses.address_street_number,
            customer_addresses.address_street_name,
            customer_addresses.address_state_name,
            customer_addresses.address_zipcode,
            customer_addresses.address_country_name
    FROM int_customer_addresses__joined AS customer_addresses
)

SELECT * FROM cou_marketing_team