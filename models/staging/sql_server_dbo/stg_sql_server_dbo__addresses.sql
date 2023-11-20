WITH src_addresses AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'addresses') }}
    ),

stg_addresses AS (
    SELECT {{ replace_empty_and_null_values_with_tag('address_id', 'not registered') }}::varchar(50) AS address_id,
            {{ replace_empty_and_null_values_with_tag(get_street_number_column_from_address('address'), 'not defined') }}::number(38,0) AS street_number,
            {{ replace_empty_and_null_values_with_tag(get_uppercased_column_each_word(get_street_name_column_from_address('address')), 'not defined') }}::varchar(150) AS street_name,
            {{ replace_empty_and_null_values_with_tag(get_uppercased_column_each_word('state'), 'not defined') }}::varchar(50) AS state,
            {{ replace_empty_and_null_values_with_tag('zipcode', 'not defined') }}::number(38,0) AS zipcode,
            {{ replace_empty_and_null_values_with_tag(get_uppercased_column_each_word('country'), 'not defined') }}::varchar(50) AS country,
            coalesce(_fivetran_deleted, false) AS was_this_address_row_deleted, -- no need to build CASE due to Fivetran always returns NULL when row was not deleted
            _fivetran_synced::date AS address_load_date
    FROM src_addresses
    )

SELECT * FROM stg_addresses