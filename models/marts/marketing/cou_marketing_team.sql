WITH customers AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__customers') }}
),

addresses AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__addresses') }}
),

cou_marketing_team AS (
    SELECT customers.customer_id,
            customers.customer_first_name,
            customers.customer_last_name,
            customers.customer_email,
            customers.customer_phone_number,
            customers.customer_created_at_utc,
            customers.customer_updated_at_utc,
            addresses.address_street_number,
            addresses.address_street_name,
            addresses.address_state_name,
            addresses.address_zipcode,
            addresses.address_country_name
    FROM customers
    LEFT JOIN addresses ON customers.customer_address_id = addresses.address_id
)

SELECT * FROM cou_marketing_team