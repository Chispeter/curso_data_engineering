WITH snapshot_customers AS (
    SELECT
        customer_id,
        address_id,
        cast(created_at_utc as date)              AS creation_date,
        cast(created_at_utc as time)              AS creation_time,
        cast(updated_at_utc as date)              AS update_date,
        cast(updated_at_utc as time)              AS update_time,
        first_name,
        last_name,
        phone_number,
        email,
        dbt_valid_to AS valid_to_utc
    FROM {{ ref('snapshot_customers') }}
),

stg_dates AS (
    SELECT
        date_id,
        date_day
    FROM {{ ref('stg_staging__dates') }}
),

dim_customers AS (
    SELECT
        customer_id,
        address_id,
        d1.date_id       AS creation_date_id,
        d2.date_id       AS update_date_id,
        creation_time    AS creation_time,
        update_time      AS update_time,
        first_name,
        last_name,
        phone_number,
        email,
        valid_to_utc
    FROM snapshot_customers AS c
    LEFT JOIN stg_dates AS d1 ON c.creation_date = d1.date_day
    LEFT JOIN stg_dates AS d2 ON c.update_date = d2.date_day
)

SELECT * FROM dim_customers