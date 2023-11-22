{{ config(
    materialized='incremental',
    unique_key='customer_id'
    ) 
    }}


WITH src_sql_server_dbo__users AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}

    {% if is_incremental() %}

        WHERE _fivetran_synced > (SELECT max(customer_batched_at_utc) FROM {{ this }})

    {% endif %}

    ),

stg_sql_server_dbo__customers AS (
    SELECT cast(user_id as varchar(50)) AS customer_id,
            cast({{ replace_empty_and_null_values_with_tag('address_id', 'not registered')}} as varchar(50)) AS customer_address_id,
            cast({{ replace_empty_and_null_values_with_tag('first_name', 'not defined') }} as varchar(50)) AS customer_first_name,
            cast({{ replace_empty_and_null_values_with_tag('last_name', 'not defined') }} as varchar(50)) AS customer_last_name,
            cast(replace(phone_number, '-', '') as number(38,0)) AS customer_phone_number,
            cast({{ replace_empty_and_null_values_with_tag('email', 'not defined') }} as varchar(100)) AS customer_email,
            cast({{ get_trimmed_column('total_orders') }} as number(38,0)) AS customer_number_of_total_orders,
            cast({{ get_trimmed_column('created_at') }} as timestamp_tz(9)) AS customer_created_at_utc,
            cast({{ get_trimmed_column('updated_at') }} as timestamp_tz(9)) AS customer_updated_at_utc,
            cast(coalesce(_fivetran_deleted, false) as boolean) AS was_this_customer_row_deleted,
            cast(_fivetran_synced as timestamp_tz(9)) AS customer_batched_at_utc
    FROM src_sql_server_dbo__users
    )

SELECT * FROM stg_sql_server_dbo__customers