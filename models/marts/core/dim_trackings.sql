WITH stg_orders AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__orders') }}
),

dim_trackings AS (
    SELECT
        -- STG_ADDRESSES (address data of each address_id)
        -- address data
        address_id,
        address_street_number,
        address_street_name,
        address_state_name,
        address_zipcode,
        address_country_name
    FROM stg_orders
)

SELECT * FROM dim_trackings