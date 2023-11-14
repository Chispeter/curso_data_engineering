WITH src_addresses AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'addresses') }}
    ),

stg_addresses AS (
    SELECT address_id,
            zipcode,
            country,
            address,
            state,
            _fivetran_deleted,
            _fivetran_synced AS date_load
    FROM src_addresses
    )

SELECT * FROM stg_addresses