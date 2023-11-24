/*
{% set event_types = get_values_from_column(ref('stg_sql_server_dbo__events'), 'event_type') %}
WITH stg_events AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__events') }}
    ),

renamed_casted AS (
    SELECT
        event_customer_id,
        {%- for event_type in event_types   %}
        sum(case when event_type = '{{event_type}}' then 1 end) as {{event_type}}_amount
        {%- if not loop.last %},{% endif -%}
        {% endfor %}
    FROM stg_events
    GROUP BY 1
    )

SELECT * FROM renamed_casted
*/