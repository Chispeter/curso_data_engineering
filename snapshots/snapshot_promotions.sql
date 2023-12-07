{% snapshot snapshot_promotions %}

{{
    config(
      unique_key='promotion_id',
      strategy='check',
      check_cols=['name', 'discount_in_percentage', 'status']
    )
}}

SELECT *
FROM {{ ref('stg_sql_server_dbo__promotions') }}
WHERE batched_at_utc = (SELECT max(batched_at_utc) FROM {{ ref('stg_sql_server_dbo__promotions') }})

{% endsnapshot %}