{% macro firebolt__drop_materialized_view(relation) %}
    {{ exceptions.raise_compiler_error("Firebolt does not support materialized views") }}
{% endmacro %}