WITH src_sql_server_dbo__addresses AS (
    SELECT *
    FROM {{ source('sql_server_dbo', 'addresses') }}
    UNION ALL
    SELECT
        {{ dbt_utils.generate_surrogate_key(['null']) }},
        0,
        'No Country',
        '0 No Address',
        'No State',
        null,
        min(_fivetran_synced)
    FROM {{ source('sql_server_dbo', 'addresses') }}
    {% if is_incremental() %}
    WHERE _fivetran_synced > (SELECT max(_fivetran_synced) FROM {{ this }})
    {% endif %}
),

stg_sql_server_dbo__addresses AS (
    SELECT
        cast(address_id as varchar(50)) AS address_id,
        cast({{ get_street_number_column_from_address('address') }} as number(38,0)) AS address_street_number,
        cast({{ get_street_name_column_from_address('address') }} as varchar(150)) AS address_street_name,
        cast(state as varchar(50)) AS address_state_name,
        cast(zipcode as number(38, 0)) AS address_zipcode,
        cast(country as varchar(50)) AS address_country_name,
        cast(coalesce(_fivetran_deleted, false) AS boolean) AS was_this_address_row_deleted,
        cast(_fivetran_synced as timestamp_tz(9)) AS address_batched_at_utc
    FROM src_sql_server_dbo__addresses
)

SELECT * FROM stg_sql_server_dbo__addresses
