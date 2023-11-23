-- MACRO DESCRIPTION: searches for the first occurrence of the first argument in the second trimmed argument and,
-- if successful, returns the position (1-based) of the first argument in the second trimmed argument
{% macro get_first_space_index(column) %}
{% set column_name = column %}
position(' ', {{get_trimmed_column(column_name)}}, 1)
{% endmacro %}