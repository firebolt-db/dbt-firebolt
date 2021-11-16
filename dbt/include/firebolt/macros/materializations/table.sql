{% materialization table, adapter='firebolt' %}
{# Note that a nearly identical materialization appears
   in table.sql #}
  {%- set identifier = model['alias'] -%} {# alias comes from where? #}
  {%- set target_relation = api.Relation.create(database=database,
                                                schema=schema,
                                                identifier=identifier,
                                                type='table') -%}

  {{ run_hooks(pre_hooks) }}

  {% do adapter.drop_relation(target_relation) %}

  -- build model
  {% call statement('main') -%}
    {{ create_table_as(False, target_relation, sql) }}
  {%- endcall %}
  {% do create_indexes(target_relation) %}
  {{ run_hooks(post_hooks) }}

  {% do persist_docs(target_relation, model) %}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
