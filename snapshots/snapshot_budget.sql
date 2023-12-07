{% snapshot snapshot_budget %}

{{
    config(
      unique_key='budget_id',
      strategy='check',
      check_cols=['number_of_units_of_product_expected_to_be_sold']
    )
}}

SELECT *
FROM {{ ref('stg_google_sheets__budget') }}

{% endsnapshot %}