WITH int_orders_dates__joined AS (
    SELECT
        order_status
    FROM {{ ref('int_orders_dates__joined') }}
),

int_order_status_orders AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['order_status']) }}   AS order_status_id,
        order_status                                               AS order_status
    FROM int_orders_dates__joined
    GROUP BY order_status_id, order_status
)

SELECT * FROM int_order_status_orders
