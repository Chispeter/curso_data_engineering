WITH src_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
    ),

stg_orders AS (
    SELECT trim(order_id, ' ')::varchar(50) AS order_id,
            trim(user_id, ' ')::varchar(50) AS user_id,
            trim(address_id, ' ')::varchar(50) AS address_id,
            trim(promo_id, ' ')::varchar(50) AS promo_name,
            trim(tracking_id, ' ')::varchar(50) AS tracking_id,
            trim(shipping_service, ' ')::varchar(20) AS shipping_service,
            trim(shipping_cost, ' ')::float AS shipping_cost,
            trim(order_cost, ' ')::float AS order_cost,
            trim(order_total, ' ')::float AS order_total,
            trim(created_at, ' ')::timestamp_tz AS created_at_utc,
            trim(status, ' ')::varchar(20) AS order_status,
            trim(estimated_delivery_at, ' ')::timestamp_tz AS estimated_delivery_at_utc,
            trim(delivered_at, ' ')::timestamp_tz AS delivered_at_utc,
            coalesce(_fivetran_deleted, false) AS was_this_order_row_deleted,
            _fivetran_synced::date AS order_load_date
    FROM src_orders
    )

SELECT * FROM stg_orders

