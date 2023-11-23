-- MACRO DESCRIPTION: replace empty and null values from a 'column' with a specified 'tag' after column is trimmed
{% macro replace_empty_and_null_values_with_tag(column, tag) %}
{% set column_name = column %}
CASE WHEN ({{get_trimmed_column(column_name)}} = '' OR {{get_trimmed_column(column_name)}} IS NULL) THEN '{{tag}}'
        ELSE {{get_trimmed_column(column_name)}}
        END
{% endmacro %}