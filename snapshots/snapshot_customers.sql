{% snapshot snapshot_customers %}

{{
    config(
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at_utc'
    )
}}

SELECT *
FROM {{ ref('stg_sql_server_dbo__customers') }}

{% endsnapshot %}