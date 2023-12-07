{% snapshot snapshot_products %}

{{
    config(
      unique_key='product_id',
      strategy='check',
      check_cols=['name', 'price_in_usd', 'number_of_units_in_inventory']

    )
}}

SELECT *
FROM {{ ref('stg_sql_server_dbo__products') }}
WHERE batched_at_utc = (SELECT max(batched_at_utc) FROM {{ ref('stg_sql_server_dbo__products') }})

{% endsnapshot %}