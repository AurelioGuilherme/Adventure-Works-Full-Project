--https://docs.getdbt.com/docs/build/custom-schemas

{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name }}
{%- endmacro %}
