WITH stg_addresses AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__addresses') }}
),


dim_addresses AS (
    SELECT
        -- STG_ADDRESSES
        addresses.address_id AS address_id,
        addresses.address_street_number AS address_street_number,
        addresses.address_street_name AS address_street_name,
        addresses.address_state_name AS address_state_name,
        addresses.address_zipcode AS address_zipcode,
        addresses.address_country_name AS address_country_name,
    FROM stg_addresses AS addresses
    )

SELECT * FROM dim_addresses