{% macro firebolt__rename_relation(source, target) -%}
  {# Create new relation, do CTAS to swap it, remove original relation.
     Must check whether relation is view or table, because process is
     different for the two. #}
  {% set all_relations = (list_relations_without_caching(source)) %}
  {% set tables = adapter.filter_table(all_relations, 'type', 'table') %}
  {% set views = adapter.filter_table(all_relations, 'type', 'view') %}
  {% if (views.rows | length) > 0 %}
    {% call statement('view_definition', fetch_result=True) %}

      SELECT view_definition FROM information_schema.views
      WHERE table_name = '{{ source.identifier }}'
    {%- endcall %}
    {% if load_result('view_definition')['data'] %}
      {# For some reason this ocassionally returns an empty set.
         In that case do nothing. Don't even ask why I have to jump through
         the stupid hoops to get the value out of the result in the next line. #}
      {% set view_ddl = load_result('view_definition')['data'][0][0] %}
      {% set view_ddl = view_ddl.replace(source.identifier,
                                         target.identifier) %}
      {% call statement('new_view') %}

        DROP VIEW {{ source.identifier }};
        {{ view_ddl }};
      {% endcall %}
    {% endif %}
  {% elif (tables.rows | length) > 0 %}
    {# There were no views, so retrieve table. (There must be a table.) #}
    {% set table_type = adapter.filter_table(all_relations, 'table_type', '') %}
    {% call statement('tables') %}

      CREATE {{ table_type }} TABLE {{ target.identifier }} AS
      SELECT * FROM {{ source.identifier }};
      DROP TABLE {{ source.identifier }};
    {%- endcall %}
  {% endif %}
{% endmacro %}


{% macro firebolt__drop_relation(relation) -%}
  {#-
  Drop relation. Drop both table and view because relation doesn't always
  have a table_type specified.
  #}
  {% call statement('drop') %}

      DROP {{ relation.type | upper }} IF EXISTS {{ relation }} CASCADE;
  {% endcall %}
{% endmacro %}
