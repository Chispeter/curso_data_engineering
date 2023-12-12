WITH dim_orders AS (
    SELECT *
    FROM {{ ref('dim_orders') }}
),

dim_shipping_services AS (
    SELECT *
    FROM {{ ref('dim_shipping_services') }}
),

dim_order_status AS (
    SELECT *
    FROM {{ ref('dim_order_status') }}
),

dim_addresses AS (
    SELECT *
    FROM {{ ref('dim_addresses') }}
),

int_customers__joined AS (
    SELECT *
    FROM {{ ref('int_customers__joined') }}
),

int_orders__joined AS (
    SELECT 
        -- dim_orders (order data)
        o.order_id                  AS order_id,
        o.tracking_id               AS tracking_id,
        ss.shipping_service_id      AS shipping_service_id,
        ss.shipping_service_name    AS shipping_service_name,
        os.order_status_id          AS order_status_id,
        os.order_status             AS order_status,
        -- dim_addresses (address data)
        a.address_id                AS order_address_id,
        a.street_number             AS order_street_number,
        a.street_name               AS order_street_name,
        a.state_name                AS order_state_name,
        a.zipcode                   AS order_zipcode,
        a.country_name              AS order_country_name
        -- int_customers__joined (customer extended data)
        c.customer_id               AS customer_id,
        c.creation_date_id          AS customer_creation_date_id,
        c.creation_date             AS customer_creation_date,
        c.update_date_id            AS customer_update_date_id,
        c.update_date               AS customer_update_date,
        c.creation_time             AS customer_creation_time,
        c.update_time               AS customer_update_time,
        c.first_name                AS customer_first_name,
        c.last_name                 AS customer_last_name,
        c.phone_number              AS customer_phone_number,
        c.email                     AS customer_email,
        c.address_id                AS customer_address_id,
        c.street_number             AS customer_street_number,
        c.street_name               AS customer_street_name,
        c.state_name                AS customer_state_name,
        c.zipcode                   AS customer_zipcode,
        c.country_name              AS customer_country_name
    FROM dim_orders AS o
    LEFT JOIN dim_shipping_services AS ss ON o.shipping_service_id = ss.shipping_service_id
    LEFT JOIN dim_order_status AS os ON o.order_status_id = os.order_status_id
    LEFT JOIN dim_addresses AS a ON o.address_id = a.address_id
    LEFT JOIN int_customers__joined AS c ON o.customer_id = c.customer_id
)

SELECT * FROM int_orders__joined