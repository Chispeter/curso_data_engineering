WITH src_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
    ),

base_orders AS (
    SELECT order_id,
            shipping_service,
            shipping_cost,
            address_id,
            created_at,
            {{ replace_empty_and_null_values_with_tag(get_lowercased_column('promo_id'), 'no promo') }} AS promo_name,
            estimated_delivery_at,
            order_cost,
            user_id,
            order_total,
            delivered_at,
            tracking_id,
            status,
            _fivetran_deleted,
            _fivetran_synced
    FROM src_orders

    UNION ALL

    SELECT {{ dbt_utils.generate_surrogate_key(['null']) }}, 0, 'No Product', 0, null, current_timestamp()
    )

SELECT * FROM base_orders

