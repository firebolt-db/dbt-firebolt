{% macro firebolt__get_drop_view_sql(relation) %}
    DROP VIEW if EXISTS {{ relation }} CASCADE
{% endmacro %}
