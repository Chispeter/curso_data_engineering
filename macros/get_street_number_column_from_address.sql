-- MACRO DESCRIPTION: get street number from 'address_column' (street_number street_name) after 'address_column' is trimmed
{% macro get_street_number_column_from_address(address_column) %}
{% set column_name = address_column %}
left({{get_trimmed_column(column_name)}}, {{get_first_space_index(column_name)}}-1)
{% endmacro %}