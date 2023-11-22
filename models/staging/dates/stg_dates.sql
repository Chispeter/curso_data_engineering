{% set start_date_value = "2023-01-01" %} -- find minimum date --> create macro
{% set margin_of_years_of_data_forecast = 1 %} -- given by company especifications (e.g. margin of years of data forecast of the company for the future)
{% set end_date_value = "2023-01-31" %} 

WITH src_sql_server_dbo__dates AS (
    {{ dbt_date.get_date_dimension(start_date = start_date_value, end_date = end_date_value) }}
),

stg_sql_server_dbo__dates AS (
    SELECT {{ dbt_utils.generate_surrogate_key(['date_day']) }} AS date_id,
            date_day,
            prior_date_day,
            next_date_day,
            prior_year_date_day,
            prior_year_over_year_date_day,
            day_of_week,
            day_of_week_iso,
            day_of_week_name,
            day_of_week_name_short,
            day_of_month,
            day_of_year,
            week_start_date,
            week_end_date,
            prior_year_week_start_date,
            prior_year_week_end_date,
            week_of_year,
            iso_week_start_date,
            iso_week_end_date,
            prior_year_iso_week_start_date,
            prior_year_iso_week_end_date,
            iso_week_of_year,
            prior_year_week_of_year,
            prior_year_iso_week_of_year,
            month_of_year,
            month_name,
            month_name_short,
            month_start_date,
            month_end_date,
            prior_year_month_start_date,
            prior_year_month_end_date,
            quarter_of_year,
            quarter_start_date,
            quarter_end_date,
            year_number,
            year_start_date,
            year_end_date
    FROM src_sql_server_dbo__dates
)

SELECT * FROM stg_sql_server_dbo__dates
