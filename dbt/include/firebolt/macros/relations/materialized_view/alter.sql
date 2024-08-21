{% macro firebolt__get_alter_materialized_view_as_sql(
    relation,
    configuration_changes,
    sql,
    existing_relation,
    backup_relation,
    intermediate_relation
) %}

    {{ exceptions.raise_compiler_error("Firebolt does not support materialized views") }}
{% endmacro %}
