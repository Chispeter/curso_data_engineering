-- MACRO DESCRIPTION: Returns the length of an trimmed input string or binary value. For strings, the length is the number of
-- characters, and UTF-8 characters are counted as a single character. For binary, the length is the number of bytes.
{% macro get_length_of_values(column) %}
{% set column_name = column %}
length({{get_trimmed_column(column_name)}})
{% endmacro %}