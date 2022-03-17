{% macro firebolt__rename_relation(from_relation, to_relation) -%}
  {# Create new relation, do CTAS to swap it, remove original relation.
     Must check whether relation is view or table, because process is
     different for the two. #}
  {# {% set target_name = adapter.quote_as_configured(to_relation.identifier, 'identifier') %}
  {% set source_name = adapter.quote_as_configured(from_relation.identifier, 'identifier') %} #}
  {# Have to figure out relation type first. #}
  {% set all_relations = (list_relations_without_caching(from_relation)) %}
  {% set views = adapter.filter_table(all_relations, 'type', 'view') %}
  {% if (views.rows | length) > 0 %}
    {% call statement('view_ddl', fetch_result=True) %}
      SELECT view_definition FROM information_schema.views
      WHERE table_name = '{{ from_relation.identifier }}'
    {%- endcall %}
    {{ log("\n\n Swappable view:\n" ~ load_result(view_ddl) ~ "\n" ~ adapter.filter_table(load_result(view_ddl),
                                               'view_definition',
                                               '.*'), True) }}
    {% set this_sql = adapter.get_renamed_view_ddl(
                          adapter.filter_table(load_result(view_ddl),
                                               'view_definition',
                                               '.*'),
                          to_relation) %}
    {% call statement('new_view') %}
    {{ this_sql }}
    {% endcall %}
  {% else %}
    {% set tables = adapter.filter_table(all_relations, 'type', 'table') %}
    {% if (tables.rows | length) > 0 %}
      {% call statement('table_type', fetch_result=True) %}
        SELECT table_type FROM information_schema.tables
        WHERE table_name = '{{ to_relation.identifier }}'
      {% endcall %}
      {% set table_type = load_result('table_type').table.collumns['table_type'] %}
      {% call statement('rename_relation') %}
        CREATE {{ table_type }} TABLE {{ to_relation.identifier }} AS
        SELECT * FROM {{ from_relation.identifier }};
        DROP TABLE {{ from_relation.identifier }};
        --alter table {{ from_relation.identifier }} rename to {{ to_relation.identifier }}
      {%- endcall %}
    {% endif %}
  {% endif %}

  {% for row in load_result('list_views_without_caching').table.rows %}
  {{ log("\n\n** table info table row " ~ row.get('table_type'), True) }}
  {% endfor %}
  {% call statement('table_type', fetch_result=True) %}
    SELECT table_type FROM information_schema.tables
    WHERE table_name = '{{ to_relation.identifier }}'
  {% endcall %}
  {{ log("\n\n** table name " ~ source_name, True) }}
  {{ log("\n\n** table type\n", True) }}
  {% for row in load_result('table_type').table.rows %}
      {{ log(row.get('table_type'), True) }}
  {% endfor %}
  {% set table_type = load_result('table_type').table.columns['table_type'] %}

  {% call statement('rename_relation') %}
    CREATE {{ table_type }} TABLE {{ source_name }}_swap AS
      SELECT * FROM {{ source_name }};
    CREATE {{ table_type }} TABLE {{ target_name }} AS
      SELECT * FROM {{ source_name }}_swap;
    --DROP TABLE {{ source_name }};
    --DROP TABLE {{ source_name }}_swap
    --alter table {{ from_relation }} rename to {{ target_name }}
  {%- endcall %}
{% endmacro %}
