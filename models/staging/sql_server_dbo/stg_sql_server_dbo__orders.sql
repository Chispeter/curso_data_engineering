WITH base_sql_server_dbo__orders AS (
    SELECT *
    FROM {{ ref('base_sql_server_dbo__orders') }}
),

base_sql_server_dbo__promos AS (
    SELECT *
    FROM {{ ref('base_sql_server_dbo__promos') }}
),

stg_sql_server_dbo__orders AS (
    SELECT
        cast(base_orders.order_id as varchar(50))                           AS order_id,
        cast(base_orders.user_id as varchar(50))                            AS customer_id,
        cast(base_orders.address_id as varchar(50))                         AS address_id,
        cast(base_promos.promo_id as varchar(50))                           AS promotion_id,
        cast(base_orders.tracking_id as varchar(50))                        AS tracking_id,
        cast(base_orders.shipping_service as varchar(20))                   AS shipping_service_name,
        cast(base_orders.shipping_cost as number(38,2))                     AS shipping_service_cost_in_usd,
        cast(base_orders.order_cost as number(38,2))                        AS order_cost_in_usd,
        cast(base_orders.order_total as number(38,2))                       AS order_total_cost_in_usd,
        cast(base_orders.status as varchar(20))                             AS order_status,
        cast(base_orders.created_at as timestamp_tz(9))                     AS order_created_at_utc,
        cast(base_orders.estimated_delivery_at as timestamp_tz(9))          AS order_estimated_delivery_at_utc,
        cast(base_orders.delivered_at as timestamp_tz(9))                   AS order_delivered_at_utc,
        cast(coalesce(base_orders._fivetran_deleted, false) as boolean)     AS was_this_row_deleted,
        cast(base_orders._fivetran_synced as timestamp_tz(9))               AS batched_at_utc
    FROM base_sql_server_dbo__orders AS base_orders
    INNER JOIN base_sql_server_dbo__promos AS base_promos ON base_orders.promo_name = base_promos.name
)

SELECT * FROM stg_sql_server_dbo__orders order by 5
