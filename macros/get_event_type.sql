{% macro get_event_type() %}
{{ return(["checkout", "package_shipped", "add_to_cart","page_view"]) }}
{% endmacro %}