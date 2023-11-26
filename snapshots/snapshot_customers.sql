{% snapshot snapshot_customers %}

{{
    config(
      target_schema='snapshots',
      unique_key='customer_id',

      strategy='check',
      check_cols=['customer_first_name', 'customer_last_name', 'customer_address_id', 'customer_phone_number']

    )
}}

SELECT *
FROM {{ ref('stg_sql_server_dbo__customers') }}
WHERE customer_batched_at_utc = (SELECT max(customer_batched_at_utc) FROM {{ ref('stg_sql_server_dbo__customers') }})

{% endsnapshot %}