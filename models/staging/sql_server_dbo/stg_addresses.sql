WITH src_addresses AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'addresses') }}
    ),

stg_addresses AS (
    SELECT trim(address_id, ' ')::varchar(50) AS address_id,
            left(trim(address, ' '), position(' ', trim(address, ' '), 1)-1)::number(38,0) AS street_number,
            initcap(right(trim(address, ' '), length(trim(address, ' ')) - position(' ', trim(address, ' '), 1)))::varchar(150) AS street_name,
            initcap(trim(state, ' '))::varchar(50) AS state,
            trim(zipcode, ' ')::number(38,0) AS zipcode,
            initcap(trim(country, ' '))::varchar(50) AS country,
            coalesce(_fivetran_deleted, false) AS was_this_address_row_deleted,
            _fivetran_synced::date AS address_load_date
    FROM src_addresses
    )

SELECT * FROM stg_addresses