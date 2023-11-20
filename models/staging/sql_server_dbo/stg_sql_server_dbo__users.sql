WITH src_users AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}
    ),

stg_users AS (
    SELECT {{ replace_empty_and_null_values_with_tag('user_id', 'not registered') }}::varchar(50) AS user_id,
            {{ replace_empty_and_null_values_with_tag('address_id', 'not registered') }}::varchar(50) AS address_id,
            {{ replace_empty_and_null_values_with_tag('first_name', 'not defined') }}::varchar(50) AS first_name,
            {{ replace_empty_and_null_values_with_tag('last_name', 'not defined') }}::varchar(50) AS last_name,
            {{ replace_empty_and_null_values_with_tag('phone_number', 'not defined') }}::varchar(20) AS phone_number,
            {{ get_trimmed_column('total_orders') }}::number(38,0) AS total_orders,
            {{ replace_empty_and_null_values_with_tag('email', 'not defined') }}::varchar(100) AS email,
            {{ get_trimmed_column('created_at') }}::timestamp_tz AS user_created_at_utc,
            {{ get_trimmed_column('updted_at') }}::timestamp_tz AS user_updated_at_utc,
            coalesce(_fivetran_deleted, false) AS was_this_user_row_deleted,
            _fivetran_synced::date AS user_load_date
    FROM src_users
    )

SELECT * FROM stg_users