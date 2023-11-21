{{ config(
    materialized='incremental',
    unique_key='user_id',
    on_schema_change='fail'
    ) 
    }}

WITH src_users AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}
    {% if is_incremental() %}

        WHERE _fivetran_synced > (SELECT max(f_carga) FROM {{ this }})

    {% endif %}
    ),

stg_users_2 AS (
    SELECT cast(user_id as varchar(50)) AS user_id,
            cast(first_name as varchar(50)) AS first_name,
            cast(last_name as varchar(50)) AS last_name,
            cast(address_id as varchar(50)) AS address_id,
            cast(replace(phone_number, '-', '') as number(38,0)) AS phone_number,
            cast(_fivetran_synced as timestamp_tz(9)) AS f_carga
    FROM src_users
    )

SELECT * FROM stg_users_2