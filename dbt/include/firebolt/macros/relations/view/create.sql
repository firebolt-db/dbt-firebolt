{% macro firebolt__create_view_as(
    relation,
    select_sql
  ) %}
  CREATE
  OR REPLACE VIEW {{ relation.identifier }}

  {%- set contract_config = config.get('contract') -%}
  {%- if contract_config.enforced -%}
    {{ get_assert_columns_equivalent(select_sql) }}
  {%- endif %}

  AS (
    {{ select_sql }}
  )
{% endmacro %}
