{% snapshot snapshot_products %}

{{
    config(
      target_schema='snapshots',
      unique_key='product_id',

      strategy='check',
      check_cols=['product_name', 'product_price_in_usd']

    )
}}

SELECT *
FROM {{ ref('stg_sql_server_dbo__products') }}
WHERE product_batched_at_utc = (SELECT max(product_batched_at_utc) FROM {{ ref('stg_sql_server_dbo__products') }})

{% endsnapshot %}