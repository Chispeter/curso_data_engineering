WITH src_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
    ),

stg_orders AS (
    SELECT trim(order_id, ' ')::varchar(50) AS order_id,
            trim(user_id, ' ')::varchar(50) AS user_id,
            trim(address_id, ' ')::varchar(50) AS address_id,
            (CASE WHEN lower(trim(promo_id, ' ')) = '' THEN 'not_applicable'
                    ELSE lower(trim(promo_id, ' '))
                    END)::varchar(50) AS promo_name,
            trim(tracking_id, ' ')::varchar(50) AS tracking_id,
            (CASE WHEN trim(shipping_service, ' ') = '' THEN 'unknown'
                    WHEN trim(shipping_service, ' ') = 'fedex' THEN 'FedEx'
                    ELSE upper(trim(shipping_service, ' '))
                    END)::varchar(20) AS shipping_service,
            trim(shipping_cost, ' ')::number(38,2) AS shipping_cost_usd,
            trim(order_cost, ' ')::number(38,2) AS order_cost_usd,
            trim(order_total, ' ')::number(38,2) AS order_total_usd,
            trim(created_at, ' ')::timestamp_tz AS order_created_at_utc,
            lower(trim(status, ' '))::varchar(20) AS order_status,
            trim(estimated_delivery_at, ' ')::timestamp_tz AS order_estimated_delivery_at_utc,
            trim(delivered_at, ' ')::timestamp_tz AS order_delivered_at_utc,
            coalesce(_fivetran_deleted, false) AS was_this_order_row_deleted,
            _fivetran_synced::date AS order_load_date
    FROM src_orders
    )

SELECT * FROM stg_orders

