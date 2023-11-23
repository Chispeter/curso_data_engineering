{{ config(
    materialized='incremental',
    unique_key='budget_id'
    ) 
    }}

WITH src_google_sheets__budget AS (
    SELECT * 
    FROM {{ source('google_sheets', 'budget') }}
    {% if is_incremental() %}

        WHERE _fivetran_synced > (SELECT max(budget_batched_at_utc) FROM {{ this }})

    {% endif %}
    ),

stg_google_sheets__budget AS (
    SELECT cast({{ dbt_utils.generate_surrogate_key(['_row']) }} as varchar(50)) AS budget_id,
            cast(product_id as varchar(50)) AS product_id,
            cast(month as date) AS budget_date,
            cast(quantity as number(38,0)) AS number_of_units_of_product_sold,
            cast(_fivetran_synced as timestamp_tz(9))AS budget_batched_at_utc
    FROM src_google_sheets__budget
    )

SELECT * FROM stg_google_sheets__budget