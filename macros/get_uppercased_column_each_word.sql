-- MACRO DESCRIPTION: Returns trimmed column with the first letter of each word in uppercase
-- and the subsequent letters in lowercase.
{% macro get_uppercased_column_each_word(column) %}
{% set column_name = column %}
initcap({{get_trimmed_column(column_name)}})
{% endmacro %}