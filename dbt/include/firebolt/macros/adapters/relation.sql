{% macro firebolt__rename_relation(from_relation, to_relation) -%}
  {# Create new relation, do CTAS to swap it, remove original relation.
     Must check whether relation is view or table, because process is
     different for the two. #}
  {% set all_relations = (list_relations_without_caching(from_relation)) %}
  {% set views = adapter.filter_table(all_relations, 'type', 'view') %}
  {% if (views.rows | length) > 0 %}
    {% call statement('view_definition', fetch_result=True) %}
      SELECT view_definition FROM information_schema.views
      WHERE table_name = '{{ from_relation.identifier }}'
    {%- endcall %}
    {% if load_result('view_definition')['data'] %}
      {# For some reason this ocassionally returns an empty set.
         In that case do nothing. Don't even ask why I have to jump through
         the stupid hoops to get the value out of the result in the next line. #}
      {% set view_ddl = load_result('view_definition')['data'][0][0] %}
      {% set view_ddl = view_ddl.replace(from_relation.identifier,
                                         to_relation.identifier) %}
      {% call statement('new_view') %}
        DROP VIEW {{ from_relation.identifier }};
        {{ view_ddl }};
      {% endcall %}
    {% endif %}
  {% elif (tables.rows | length) > 0 %}
    {# There were no views, so retrieve table. (There must be a table.) #}
    {% set table_type = adapter.filter_table(all_relations, 'table_type', '') %}
    {% call statement('tables') %}
      CREATE {{ table_type }} TABLE {{ to_relation.identifier }} AS
      SELECT * FROM {{ from_relation.identifier }};
      DROP TABLE {{ from_relation.identifier }};
    {%- endcall %}
  {% endif %}
{% endmacro %}


{# Todo: Figure out why this isn't working when it gets called from incremental.
{% macro drop_relation(relation) %}
  {% if relation is not none %}
    DROP IF EXISTS {{ relation.type }} CASCADE
  {% endif %}
{% endmacro %}
#}
