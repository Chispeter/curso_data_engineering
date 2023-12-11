WITH stg_orders AS (
    SELECT
        order_status
    FROM {{ ref('stg_sql_server_dbo__orders') }}
),

int_order_status_orders AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['order_status']) }}   AS order_status_id,
        order_status                                               AS order_status
    FROM stg_orders
    GROUP BY order_status_id, order_status
)

SELECT * FROM int_order_status_orders
