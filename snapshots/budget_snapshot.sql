{% snapshot budget_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='budget_id',

      strategy='check',
      check_cols=['quantity'],

    )
}}

SELECT *
FROM {{ ref('stg_google_sheets__budget') }}

{% endsnapshot %}