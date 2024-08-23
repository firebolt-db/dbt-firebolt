{% macro firebolt__get_replace_table_sql(relation, sql) %}
    {{ firebolt__create_table_as(False, relation, sql) }}
{% endmacro %}
