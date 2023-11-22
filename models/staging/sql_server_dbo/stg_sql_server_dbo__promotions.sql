WITH base_promos AS (
    SELECT *
    FROM {{ ref('base_sql_server_dbo__promos') }}
),

stg_sql_server_dbo__promotions AS (
    SELECT
        cast(promo_id as varchar(50)) AS promotion_id,
        cast(name as varchar(50)) AS promotion_name,
        cast(discount as number(38,2)) AS promotion_discount_in_percentage,
        cast(status as varchar(50)) AS promotion_status,
        cast(coalesce(_fivetran_deleted, false) as boolean) AS was_this_promotion_row_deleted,
        cast(_fivetran_synced as timestamp_tz(9)) AS promotion_batched_at_utc
    FROM base_promos
)

SELECT * FROM stg_sql_server_dbo__promotions
