{% macro firebolt__dropif(source_node) %}
    {% set ddl %}
        DROP TABLE IF EXISTS {{ source_node['name'] }}
    {% endset %}
    {{return(ddl)}}
{% endmacro %}
