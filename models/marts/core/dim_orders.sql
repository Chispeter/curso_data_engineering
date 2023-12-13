{{
    config(
        materialized='incremental'
    )
}}

WITH int_orders_dates__joined AS (
    SELECT
        order_id,
        customer_id,
        address_id,
        batched_at_utc
    FROM {{ ref('int_orders_dates__joined') }}
    {% if is_incremental() %}
        WHERE batched_at_utc > (SELECT max(batched_at_utc) FROM {{ this }})
    {% endif %}
),

dim_orders AS (
    SELECT
        order_id,
        customer_id,
        address_id,
        batched_at_utc
    FROM int_orders_dates__joined  
)

SELECT * FROM dim_orders