{% materialization table, adapter='firebolt' %}
{# Note that a nearly identical materialization appears
   in table.sql #}
  {%- set identifier = model['alias'] -%} {# alias comes from where? #}

  {# Temporary workaround to issue with adapter.get_relation() until
     the following are resolved:
     https://github.com/firebolt-db/dbt-firebolt/issues/11
     https://github.com/dbt-labs/dbt-core/issues/4187
     the hack is to always drop the view/table if it already exists rather than
     checking the db first to see if it exists.
   #}
  {%- set target_relation = api.Relation.create(database=database,
                                                schema=schema,
                                                identifier=identifier,
                                                type='table') -%}
  {%- set target_relation_view = api.Relation.create(database=database,
                                                     schema=schema,
                                                     identifier=identifier,
                                                     type='view') -%}

  {{ run_hooks(pre_hooks) }}

  {% do adapter.drop_relation(target_relation) %}
  {% do adapter.drop_relation(target_relation_view) %}

  -- build model
  {% call statement('main') -%}
    {{ create_table_as(False, target_relation, sql) }}
  {%- endcall %}
  {% do create_indexes(target_relation) %}
  {{ run_hooks(post_hooks) }}

  {% do persist_docs(target_relation, model) %}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
