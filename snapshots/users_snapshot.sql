{% snapshot users_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='user_id',

      strategy='check',
      check_cols=['first_name', 'last_name', 'address_id', 'phone_number']

    )
}}

select * from {{ ref('stg_sql_server_dbo__users_2') }}

{% endsnapshot %}