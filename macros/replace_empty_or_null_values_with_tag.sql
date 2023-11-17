{% macro replace_empty_or_null_values_with_tag(table, column, tag) %}

  {% set query_sql %}
    SELECT CASE WHEN (trim({{column}}, ' ') = '' OR trim({{column}}, ' ') IS NULL) THEN {{tag}}
                    ELSE trim({{column}}, ' ')
                    END
    FROM {{table}}
    {% endset %}
   
    {% set results = run_query(query_sql) %}

    {% if execute %}

        {% set results_list = results.columns[0].values() %}
    {% else %}
        {% set results_list = [] %}
    {% endif %}

{{ return(results_list) }}
{% endmacro %}