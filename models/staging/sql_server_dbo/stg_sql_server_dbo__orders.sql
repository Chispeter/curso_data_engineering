WITH base_orders AS (
    SELECT * 
    FROM {{ ref('base_orders') }}
    ),

base_promos AS (
    SELECT * 
    FROM {{ ref('base_promos') }}
    ),

stg_orders AS (
    SELECT {{ replace_empty_and_null_values_with_tag('base_orders.order_id', 'not registered') }}::varchar(50) AS order_id,
            {{ replace_empty_and_null_values_with_tag('base_orders.user_id', 'not registered') }}::varchar(50) AS user_id,
            {{ replace_empty_and_null_values_with_tag('base_orders.address_id', 'not registered') }}::varchar(50) AS address_id,
            {{ replace_empty_and_null_values_with_tag('base_promos.promo_id', 'not registered') }}::varchar(50) AS promo_id,
            {{ replace_empty_and_null_values_with_tag('base_orders.tracking_id', 'not registered') }}::varchar(50) AS tracking_id,
            (CASE WHEN {{get_trimmed_column('base_orders.shipping_service')}} = '' THEN 'not defined'
                    WHEN {{get_trimmed_column('base_orders.shipping_service')}} = 'fedex' THEN 'FedEx'
                    ELSE {{get_uppercased_column('base_orders.shipping_service')}}
                    END)::varchar(20) AS shipping_service_name,
            {{ get_trimmed_column('base_orders.shipping_cost') }}::number(38,2) AS shipping_service_cost_in_usd,
            {{ get_trimmed_column('base_orders.order_cost') }}::number(38,2) AS order_cost_in_usd,
            {{ get_trimmed_column('base_orders.order_total') }}::number(38,2) AS total_order_cost_in_usd,
            {{ get_lowercased_column('base_orders.status') }}::varchar(20) AS order_status,
            {{ get_trimmed_column('base_orders.created_at') }}::timestamp_tz AS order_created_at_utc,
            {{ get_trimmed_column('base_orders.estimated_delivery_at') }}::timestamp_tz AS order_estimated_delivery_at_utc,
            {{ get_trimmed_column('base_orders.delivered_at') }}::timestamp_tz AS order_delivered_at_utc,
            coalesce(base_orders._fivetran_deleted, false) AS was_this_order_row_deleted,
            base_orders._fivetran_synced::date AS order_load_date
    FROM base_orders
    LEFT JOIN base_promos ON base_orders.promo_name = base_promos.name
    )

SELECT * FROM stg_orders

