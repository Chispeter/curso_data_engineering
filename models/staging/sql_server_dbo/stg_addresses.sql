WITH src_addresses AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'addresses') }}
    ),

stg_addresses AS (
    SELECT {{get_trimmed_column('address_id')}}::varchar(50) AS address_id,
            {{get_street_number_column_from_address('address')}}::number(38,0) AS street_number,
            {{get_uppercased_column_each_word(get_street_name_column_from_address('address'))}}::varchar(150) AS street_name,
            {{get_uppercased_column_each_word('state')}}::varchar(50) AS state,
            {{get_trimmed_column('zipcode')}}::number(38,0) AS zipcode,
            {{get_uppercased_column_each_word('country')}}::varchar(50) AS country,
            coalesce(_fivetran_deleted, false) AS was_this_address_row_deleted, -- no need to build CASE due to Fivetran always returns NULL when row was not deleted
            _fivetran_synced::date AS address_load_date
    FROM src_addresses
    )

SELECT * FROM stg_addresses