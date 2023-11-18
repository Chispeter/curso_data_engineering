-- MACRO DESCRIPTION: replace empty and null values from a 'column' with a specified 'tag' after column values are trimmed
{% macro replace_empty_and_null_values_with_tag(column, tag) %}
CASE WHEN (trim({{column}}, ' ') = '' OR trim({{column}}, ' ') IS NULL) THEN '{{tag}}'
        ELSE trim({{column}}, ' ')
        END
{% endmacro %}