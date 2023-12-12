WITH distinct_order_status AS (
    SELECT
        distinct(order_status) AS order_status
    FROM {{ ref('stg_sql_server_dbo__orders') }}
),


dim_order_status AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['order_status']) }}   AS order_status_id,
        order_status                                               AS order_status
    FROM distinct_order_status
)

SELECT * FROM dim_order_status