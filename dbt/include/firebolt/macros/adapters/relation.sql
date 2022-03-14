{% macro firebolt__rename_relation(from_relation, to_relation) -%}
  {% set target_name = adapter.quote_as_configured(to_relation.identifier, 'identifier') %}
  {% set source_name = adapter.quote_as_configured(from_relation.identifier, 'identifier') %}
  {% call statement('list_views_without_caching', fetch_result=True) -%}

        SHOW tables
    {% endcall %}
  {% for row in load_result('list_views_without_caching').table.rows %}
  {{ log("\n\n** table info table row " ~ row.get('table_type'), True) }}
  {% endfor %}
  {% call statement('table_type', fetch_result=True) %}
    SELECT table_type FROM information_schema.tables
    WHERE table_name = '{{ source_name }}'
  {% endcall %}
  {{ log("\n\n** table name " ~ source_name, True) }}
  {{ log("\n\n** table type\n", True) }}
  {% for row in load_result('table_type').table.rows %}
      {{ log(row.get('table_type'), True) }}
  {% endfor %}
  {% set table_type = load_result('table_type').table %}

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
