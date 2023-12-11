WITH stg_addresses AS (
    SELECT
        address_id,
        street_number,
        street_name,
        state_name,
        zipcode,
        country_name
    FROM {{ ref('stg_sql_server_dbo__addresses') }}
),

dim_addresses AS (
    SELECT
        address_id,
        street_number,
        street_name,
        state_name,
        zipcode,
        country_name
    FROM stg_addresses
)

SELECT * FROM dim_addresses