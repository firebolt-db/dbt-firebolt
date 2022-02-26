{% macro firebolt__get_external_build_plan(source_node) %}
    {% set build_plan = [] %}
    {% set old_relation = adapter.get_relation(
        database = source_node.database,
        schema = source_node.schema,
        identifier = source_node.identifier
    ) %}
    {# var(variable, Boolean) if `variable` isn't set defaults to Boolean value.
       So the default action below is to do a full refresh. #}
    {% if old_relation is not none and var('ext_full_refresh', True) %}
        {% set build_plan = build_plan + [dropif(source_node),
                                          create_external_table(source_node)] %}
    {% else %}
        {% set build_plan = build_plan + [create_external_table(source_node)] %}
    {% endif %}
    {% do return(build_plan) %}
{% endmacro %}
