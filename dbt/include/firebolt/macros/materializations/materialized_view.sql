{#
    All these macros are stubbed with compiler errors because Firebolt does not
    support materialized views.
#}
{% materialization materialized_view, adapter='firebolt' %}

    {{ exceptions.raise_compiler_error("Firebolt does not support materialized views") }}

{% endmaterialization %}

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


{% macro firebolt__get_create_materialized_view_as_sql(relation, sql) %}

    {{ exceptions.raise_compiler_error("Firebolt does not support materialized views") }}

{% endmacro %}


{% macro firebolt__get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) %}
    {{ exceptions.raise_compiler_error("Firebolt does not support materialized views") }}
{% endmacro %}


{% macro firebolt__get_materialized_view_configuration_changes(existing_relation, new_config) %}
    {{ exceptions.raise_compiler_error("Firebolt does not support materialized views") }}
{% endmacro %}


{% macro firebolt__refresh_materialized_view(relation) -%}
    {{ exceptions.raise_compiler_error("Firebolt does not support materialized views") }}
{% endmacro %}


{% macro firebolt__drop_materialized_view(relation) -%}
    {{ exceptions.raise_compiler_error("Firebolt does not support materialized views") }}
{%- endmacro %}
