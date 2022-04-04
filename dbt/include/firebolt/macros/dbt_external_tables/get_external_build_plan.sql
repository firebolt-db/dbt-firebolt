{% macro firebolt__get_external_build_plan(source_node) %}
    {% set build_plan = [] %}
    {% set old_relation = adapter.get_relation(
        database = source_node.database,
        schema = source_node.schema,
        identifier = source_node.identifier
    ) %}
    {# var(variable, Boolean) defaults to Boolean value if variable isnt set.
       Firebolt doesn't do refresh because we don't need to—external tables will always
       pull most recent data from S3, so the default action below is to skip. #}
    {% if old_relation is none or var('ext_full_refresh', false) %}
        {% set build_plan = build_plan + [dropif(source_node),
                                          create_external_table(source_node)] %}
    {% else %}
        {% set build_plan = dbt_external_tables.refresh_external_table(source_node) %}
    {% endif %}
    {% do return(build_plan) %}
{% endmacro %}
