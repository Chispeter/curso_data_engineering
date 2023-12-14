{% set start_date_value = "2000-01-01" %} -- 1) company year foundation or 2) create macro that calculates the minimum date in all the source tables
{% set end_date_value = "2050-12-31" %}

{{
    config(
        materialized='table'
    )
}}

WITH src_dates AS (
    {{ dbt_date.get_date_dimension(start_date = start_date_value, end_date = end_date_value) }}
),

stg_staging__dates AS (
    SELECT {{ dbt_utils.generate_surrogate_key(['year_number', 'month_of_year', 'day_of_month']) }} AS date_id,
            date_day,
            --prior_date_day,
            --next_date_day,
            --prior_year_date_day,
            --prior_year_over_year_date_day,
            --day_of_week,
            day_of_week_iso,
            day_of_week_name,
            day_of_week_name_short,
            day_of_month,
            day_of_year,
            week_start_date,
            week_end_date,
            --prior_year_week_start_date,
            --prior_year_week_end_date,
            --week_of_year,
            iso_week_start_date,
            iso_week_end_date,
            --prior_year_iso_week_start_date,
            --prior_year_iso_week_end_date,
            iso_week_of_year,
            --prior_year_week_of_year,
            --prior_year_iso_week_of_year,
            {{ dbt_utils.generate_surrogate_key(['month_of_year']) }} AS month_id,
            month_of_year,
            month_name,
            month_name_short,
            month_start_date,
            month_end_date,
            --prior_year_month_start_date,
            --prior_year_month_end_date,
            quarter_of_year,
            quarter_start_date,
            quarter_end_date,
            {{ dbt_utils.generate_surrogate_key(['year_number']) }} AS year_id,
            year_number
            --year_start_date,
            --year_end_date
    FROM src_dates
)

SELECT * FROM stg_staging__dates
