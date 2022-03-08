{% macro firebolt__rename_relation(from_relation, to_relation) -%}
  {% set target_name = adapter.quote_as_configured(to_relation.identifier, 'identifier') %}

  {% call statement('table_type', fetch_result=True) %}
    SELECT table_type FROM information_schema.tables
    WHERE table_name = '{{ from_relation }}'
  {% endcall %}

  {% set table_type = load_result('table_type').table.rows.get('table_name') %}

  {% call statement('rename_relation') -%}
    CREATE {{ table_type }} TABLE {{ from_relation }}_swap AS
      SELECT * FROM {{ from_relation }};
    CREATE {{ table_type }} TABLE {{ target_name }} AS
      SELECT * FROM {{ from_relation }}_swap;
    DROP TABLE {{ from_relation }};
    DROP TABLE {{ from_relation }}_swap
    --alter table {{ from_relation }} rename to {{ target_name }}
  {%- endcall %}
{% endmacro %}
