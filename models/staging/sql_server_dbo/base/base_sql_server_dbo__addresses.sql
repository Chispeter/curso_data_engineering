WITH src_sql_server_dbo__addresses AS (
    SELECT *
    FROM {{ source('sql_server_dbo', 'addresses') }}
),

base_sql_server_dbo__addresses AS (
    SELECT
        address_id,
        zipcode,
        country,
        address
        state,
        _fivetran_deleted,
        _fivetran_synced
    FROM src_sql_server_dbo__addresses

    UNION ALL

    SELECT {{ dbt_utils.generate_surrogate_key(['null']) }}, 0, 'No Country', 'No Address', 'No State', null, current_timestamp()
)

SELECT * FROM base_sql_server_dbo__addresses
