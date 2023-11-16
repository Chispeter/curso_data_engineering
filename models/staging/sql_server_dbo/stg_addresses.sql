WITH src_addresses AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'addresses') }}
    ),

stg_addresses AS (
    SELECT address_id,
            address AS street_number_and_name,
            state,
            zipcode,
            country,
            _fivetran_synced AS address_batched_at
    FROM src_addresses
    )

SELECT * FROM stg_addresses