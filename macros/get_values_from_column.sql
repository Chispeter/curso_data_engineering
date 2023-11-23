{% macro get_values_from_column(table, column) %}
  --Preparamos la query
  {% set query_sql %}
    SELECT DISTINCT {{column}} FROM {{table}}
    {% endset %}
{% endmacro %}