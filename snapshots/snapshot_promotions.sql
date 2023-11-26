{% snapshot snapshot_promotions %}

{{
    config(
      target_schema='snapshots',
      unique_key='promotion_id',

      strategy='check',
      check_cols=['promotion_name', 'promotion_discount_in_percentage']

    )
}}

SELECT *
FROM {{ ref('stg_sql_server_dbo__promotions') }}
WHERE promotion_batched_at_utc = (SELECT max(promotion_batched_at_utc) FROM {{ ref('stg_sql_server_dbo__promotions') }})

{% endsnapshot %}