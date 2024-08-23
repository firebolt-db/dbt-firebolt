{% macro firebolt__get_create_materialized_view_as_sql(relation, sql) %}
    {{ exceptions.raise_compiler_error("Firebolt does not support materialized views") }}
{% endmacro %}
