{% macro firebolt__get_replace_view_sql(relation, sql) %}
    {{ firebolt__create_view_as(relation, sql) }}
{% endmacro %}
