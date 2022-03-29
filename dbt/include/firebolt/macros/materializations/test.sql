{%- materialization test, adapter='firebolt' -%}
  {% set relations = [] %}
  {% if should_store_failures() %}
    {% set identifier = model['alias'] %}
    {% set old_relation = adapter.get_relation(database=database,
                                               schema=schema,
                                               identifier=identifier) %}
    {% set target_relation = api.Relation.create(
        identifier=identifier,
        schema=schema,
        database=database,
        type='table') -%}

    {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
    {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}
    {% if old_relation is not none %}
        {% do adapter.drop_relation(old_relation) %}
    {% endif %}

    {% call statement(auto_begin=True) %}
        {{ create_table_as(False, target_relation, sql) }}
    {% endcall %}

    {% do relations.append(target_relation) %}
    {%- set main_sql -%}
        SELECT *
        FROM {{ target_relation }}
    {%- endset -%}
    {{ adapter.commit() }}
  {% else %}
      {% set main_sql = sql %}
  {% endif %}

  {% set limit = config.get('limit') %}
  {% set fail_calc = config.get('fail_calc') %}
  {% set warn_if = config.get('warn_if') %}
  {% set error_if = config.get('error_if') %}

  {% call statement('main', fetch_result=True) -%}
    {{ get_test_sql(main_sql, fail_calc, warn_if, error_if, limit)}}
  {%- endcall %}
  {{ return({'relations': relations}) }}
{%- endmaterialization -%}


{% macro default__get_test_sql(main_sql, fail_calc, warn_if, error_if, limit) -%}
    SELECT
      CAST({{ fail_calc }} AS INT) AS failures,
      CASE WHEN {{ fail_calc }} {{ warn_if }}
        THEN 'true' ELSE 'false'
      END AS should_warn,
      CASE WHEN {{ fail_calc }} {{ error_if }}
        THEN 'true' ELSE 'false'
      END AS should_error
    FROM (
      {{ main_sql }}
      {{ "LIMIT " ~ limit if limit is not none }}
    ) dbt_internal_test
{%- endmacro %}
