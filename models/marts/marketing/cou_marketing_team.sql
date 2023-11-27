WITH int_customer_addresses__joined AS (
    SELECT *
    FROM {{ ref('int_customer_addresses__joined') }}
),

stg_orders AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__orders') }}
),

snapshot_promotions AS (
    SELECT *
    FROM {{ ref('snapshot_promotions') }}
),

stg_order_products AS (
    SELECT *
    FROM {{ ref('stg_sql_server_dbo__order_products') }}
),

cou_marketing_team AS (
    SELECT customer_addresses.customer_id,
            customer_addresses.customer_first_name,
            customer_addresses.customer_last_name,
            customer_addresses.customer_email,
            customer_addresses.customer_phone_number,
            customer_addresses.customer_created_at_utc,
            customer_addresses.customer_updated_at_utc,
            customer_addresses.address_street_number,
            customer_addresses.address_street_name,
            customer_addresses.address_state_name,
            customer_addresses.address_zipcode,
            customer_addresses.address_country_name,
            count(orders.order_id) AS total_number_of_orders,
            coalesce(sum(orders.order_cost_in_usd), 0) AS total_order_cost_in_usd,
            coalesce(sum(orders.order_shipping_service_cost_in_usd), 0) AS total_order_shipping_service_cost_in_usd,
            coalesce(cast(sum(orders.order_cost_in_usd * promotions.promotion_discount_in_percentage / 100) as number(38,2)), 0) AS total_promotion_discount_in_usd,
            coalesce(sum(order_products.number_of_units_of_product_sold), 0) AS total_number_of_units_of_product_sold,
            coalesce(count(distinct order_products.product_id), 0) AS total_number_of_different_products_sold
    FROM int_customer_addresses__joined AS customer_addresses
    LEFT JOIN stg_orders AS orders ON customer_addresses.customer_id = orders.order_customer_id
    LEFT JOIN snapshot_promotions AS promotions ON orders.order_promotion_id = promotions.promotion_id
    LEFT JOIN stg_order_products AS order_products ON orders.order_id = order_products.order_id
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12

)

SELECT * FROM cou_marketing_team