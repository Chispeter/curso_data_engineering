WITH stg_dates AS (
    {{ dbt_utils.get_date_dimension("2023-01-01", "2023-02-01") }}
),


SELECT {{ dbt_utils.get_base_dates(start_date='2023-01-01', end_date='2023-02-01') }} FROM stg_dates
