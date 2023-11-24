WITH src_sql_server_dbo__orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
    ),

base_sql_server_dbo__orders AS (
    SELECT order_id,
            decode(shipping_service, '', 'no shipping service', shipping_service) AS shipping_service,
            shipping_cost,
            address_id,
            created_at,
            decode(promo_id, '', 'no promo', {{get_lowercased_column('promo_id')}}) AS promo_name,
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

    UNION ALL

    SELECT {{ dbt_utils.generate_surrogate_key(['null']) }},
            'no shipping service',
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
    FROM src_sql_server_dbo__orders

    )

SELECT * FROM base_sql_server_dbo__orders

