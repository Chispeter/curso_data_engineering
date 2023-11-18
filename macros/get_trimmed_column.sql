-- MACRO DESCRIPTION: removes leading and trailing spaces from 'column'
{% macro get_trimmed_column(column) %}
{% set column_name = column %}
trim({{column_name}}, ' ')
{% endmacro %}