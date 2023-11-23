-- MACRO DESCRIPTION: get street name from 'address_column' (street_number street_name) after 'address_column' is trimmed
{% macro get_street_name_column_from_address(address_column) %}
{% set column_name = address_column %}
right({{get_trimmed_column(column_name)}}, {{get_length_of_values(column_name)}} - {{get_first_space_index(column_name)}})
{% endmacro %}