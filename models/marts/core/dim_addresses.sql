WITH stg_addresses AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__addresses') }}
),

dim_addresses AS (
    SELECT
        address_id,
        address_street_number,
        address_street_name,
        address_state_name,
        address_zipcode,
        address_country_name
    FROM stg_addresses
    )

SELECT * FROM dim_addresses