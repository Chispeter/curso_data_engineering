WITH src_users AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}
    ),

stg_users AS (
    SELECT user_id::varchar(50) AS user_id,
            address_id::varchar(50) AS address_id,
            first_name::varchar(50) AS first_name,
            last_name::varchar(50) AS last_name,
            phone_number::varchar(20) AS phone_number,
            total_orders::number(38,0) AS total_orders,
            email::varchar(100) AS email,
            created_at AS user_created_at,
            updated_at AS user_updated_at,
            _fivetran_synced AS batched_at
    FROM src_users
    )

SELECT * FROM stg_users