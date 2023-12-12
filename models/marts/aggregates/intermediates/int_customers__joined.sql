WITH dim_customers AS (
    SELECT *
    FROM {{ ref('dim_customers') }}
),

dim_addresses AS (
    SELECT *
    FROM {{ ref('dim_addresses') }}
),

dim_dates AS (
    SELECT
        date_id,
        date_day
    FROM {{ ref('dim_dates') }}
),

int_customers__joined AS (
    SELECT
        -- customer data with date ids joined
        c.customer_id,
        d1.date_day     AS creation_date,
        d2.date_day     AS update_date,
        c.creation_time,
        c.update_time,
        c.first_name,
        c.last_name,
        c.phone_number,
        c.email,
        -- address data
        a.address_id,
        a.street_number,
        a.street_name,
        a.state_name,
        a.zipcode,
        a.country_name
    FROM dim_customers AS c
    LEFT JOIN dim_dates AS d1 ON c.creation_date_id = d1.date_id
    LEFT JOIN dim_dates AS d2 ON c.update_date_id = d2.date_id
    LEFT JOIN dim_addreses AS a ON c.address_id = a.address_id
)

SELECT * FROM int_customers__joined