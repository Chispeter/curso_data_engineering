WITH base_sql_server_dbo__promos AS (
    SELECT *
    FROM {{ ref('base_sql_server_dbo__promos') }}
),

stg_sql_server_dbo__promotions AS (
    SELECT
        cast(promo_id as varchar(50))                           AS promotion_id,
        cast(name as varchar(50))                               AS name,
        cast(discount as number(38,2))                          AS discount_in_usd,
        cast(status as varchar(50))                             AS status,
        cast(coalesce(_fivetran_deleted, false) as boolean)     AS was_this_row_deleted,
        cast(_fivetran_synced as timestamp_tz(9))               AS batched_at_utc
    FROM base_sql_server_dbo__promos
)

SELECT * FROM stg_sql_server_dbo__promotions
