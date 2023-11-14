WITH stg_users AS (
    SELECT * 
    FROM {{ ref('stg_users') }}
    ),

dim_users AS (
    SELECT
        user_id,
        first_name,
        last_name,
        email,
        phone_number,
        created_at AS created_at_utc,
        updated_at AS updated_at_utc,
        address_id,
        date_load
    FROM stg_users
    )

SELECT * FROM dim_users