WITH src_addresses AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'addresses') }}
    ),

stg_addresses AS (
    SELECT address_id,
            address,
            state,
            zipcode,
            country,
            _fivetran_deleted,
            _fivetran_synced AS batched_at
    FROM src_addresses
    )

SELECT * FROM stg_addresses