WITH src_sql_server_dbo__users AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}
),

stg_sql_server_dbo__customers AS (
    SELECT
        cast(user_id as varchar(50))                            AS customer_id,
        cast(address_id as varchar(50))                         AS address_id,
        cast(first_name as varchar(50))                         AS first_name,
        cast(last_name as varchar(50))                          AS last_name,
        cast(replace(phone_number, '-', '') as number(38,0))    AS phone_number,
        cast(email as varchar(100))                             AS email,
        coalesce(cast(total_orders as number(38,0)), 0)         AS number_of_total_orders,
        cast(created_at as timestamp_tz(9))                     AS created_at_utc,
        cast(updated_at as timestamp_tz(9))                     AS updated_at_utc,
        cast(coalesce(_fivetran_deleted, false) as boolean)     AS was_this_row_deleted,
        cast(_fivetran_synced as timestamp_tz(9))               AS batched_at_utc
    FROM src_sql_server_dbo__users
)

SELECT * FROM stg_sql_server_dbo__customers