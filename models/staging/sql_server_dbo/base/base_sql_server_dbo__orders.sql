{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

WITH src_sql_server_dbo__orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
    {% if is_incremental() %}
        WHERE _fivetran_synced > (SELECT max(_fivetran_synced) FROM {{ this }})
    {% endif %}
    UNION ALL
    SELECT
        {{ dbt_utils.generate_surrogate_key(['null']) }},
        'not yet defined',
        0,
        {{ dbt_utils.generate_surrogate_key(['null']) }},
        null,
        'no promo',
        null,
        0,
        {{ dbt_utils.generate_surrogate_key(['null']) }},
        0,
        null,
        {{ dbt_utils.generate_surrogate_key(['null']) }},
        'no status',
        null,
        min(_fivetran_synced)
    FROM {{ source('sql_server_dbo', 'orders') }}
),

base_sql_server_dbo__orders AS (
    SELECT
        order_id,
        decode(shipping_service, '', 'not yet defined', shipping_service) AS shipping_service,
        shipping_cost,
        address_id,
        created_at,
        decode(promo_id, '', 'no promo', lower(promo_id)) AS promo_name,
        estimated_delivery_at,
        order_cost,
        user_id,
        order_total,
        delivered_at,
        decode(tracking_id, '', {{ dbt_utils.generate_surrogate_key(['null']) }}, tracking_id) AS tracking_id,
        status,
        _fivetran_deleted,
        _fivetran_synced
    FROM src_sql_server_dbo__orders
)

SELECT * FROM base_sql_server_dbo__orders
    
    
    
