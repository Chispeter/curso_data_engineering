-- MACRO DESCRIPTION: Returns trimmed column in lowercase.
{% macro get_lowercased_column(column) %}
{% set column_name = column %}
lower({{get_trimmed_column(column_name)}})
{% endmacro %}