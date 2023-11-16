WITH src_users AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}
    ),

stg_users AS (
    SELECT user_id,
            address_id,
            first_name,
            last_name,
            phone_number,
            total_orders,
            email,
            created_at,
            updated_at,
            _fivetran_deleted,
            _fivetran_synced AS batched_at
    FROM src_users
    )

SELECT * FROM stg_users