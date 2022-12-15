{% materialization view, adapter='firebolt' %}
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
                                                type='view') -%}
  {%- set target_relation_table = api.Relation.create(database=database,
                                                      schema=schema,
                                                      identifier=identifier,
                                                      type='table') -%}
  {%- set grant_config = config.get('grants') -%}

  {{ run_hooks(pre_hooks) }}

  {% do adapter.drop_relation(target_relation) %}
  {% do adapter.drop_relation(target_relation_table) %}

  -- build model
  {% call statement('main') -%}
    {{ create_view_as(target_relation, sql) }}
  {%- endcall %}
  {{ run_hooks(post_hooks) }}

  {% set should_revoke = should_revoke(old_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke) %}

  {% do persist_docs(target_relation, model) %}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
