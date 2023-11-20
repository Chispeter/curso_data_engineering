{# call as follows:
date_spine(
    "day",
    "to_date('01/01/2016', 'mm/dd/yyyy')",
    "dbt.dateadd(week, 1, current_date)"
) #}
{% set start_date_value = "cast('2023-01-01' as date)" %} -- value based on min_date macro that maps in each source or on company year foundation
{% set end_date_value = "cast('2025-01-01' as date)" %}


{% endif %}

SELECT * FROM ({{ dbt_utils.date_spine(
    datepart="day",
    start_date=start_date_value,
    end_date=end_date_value
   )
}})
