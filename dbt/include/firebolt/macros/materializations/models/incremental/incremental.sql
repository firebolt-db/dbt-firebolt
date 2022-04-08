{% materialization incremental, adapter='firebolt' -%}
  {# This is largely copied from dbt-core. create_table_as was changed to
     create_view_as for temp views. #}
  {% set unique_key = config.get('unique_key') %}

  {# incorporate() adds columns to existing relation. #}
  {% set target_relation = this.incorporate(type='table') %}
  {% set source_relation = load_relation(this) %}
  {% set temp_relation = make_temp_relation(target_relation) %}
  {%- set full_refresh_mode = (should_full_refresh()) -%}
  {% set on_schema_change = incremental_validate_on_schema_change(
      config.get('on_schema_change'),
      default='ignore') %}
  {% set temp_identifier = model['name'] + '__dbt_temp' %}
  {% set backup_identifier = model['name'] + "__dbt_backup" %}

  {# The intermediate_ and backup_ relations should not already exist in the
     database; get_relation will return None in that case. Otherwise, we get
     a relation that we can drop later, before we try to use this name for the
     current operation. This has to happen before BEGIN, in a separate transaction.
     For now I'm doing this separately for views and tables, until I figure
     out how to pass a relation type into get_relation. #}
  {% set preexisting_temp_relation = adapter.get_relation(
                                         identifier=temp_identifier,
                                         schema=schema,
                                         database=database) %}
  {% set preexisting_backup_relation = adapter.get_relation(
                                           identifier=backup_identifier,
                                           schema=schema,
                                           database=database) %}
  {{ drop_relation(preexisting_backup_relation) }}
  {{ drop_relation(preexisting_temp_relation) }}
  {{ log('\n\n** dropped backup and temp relations.', True) }}
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {# We'll collect the relations in to_drop that will
     be dropped at the end of the run. #}
  {% set to_drop = [] %}

  {# First check whether we want to full refresh for source view or config reasons #}
  {% set trigger_full_refresh = (full_refresh_mode or source_relation.is_view) %}

  {% if source_relation is none %}
    {# The first argument is actually unused in firebolt__create_table_as,
       but it's describing whether the relation is temporary or not. #}
    {% set build_sql = create_table_as(True, target_relation, sql) %}
  {% elif trigger_full_refresh %}
    {# Make sure the backup doesn't exist so we don't encounter
       issues with the rename below. #}
    {% set temp_relation = source_relation.incorporate(
                              path={"identifier": temp_identifier}) %}
    {% set backup_relation = source_relation.incorporate(
                                 path={"identifier": backup_identifier}) %}
    {% set build_sql = create_table_as(True, temp_relation, sql) %}
    {% set need_swap = true %}
    {% do to_drop.append(temp_relation) %}
    {% do to_drop.append(backup_relation) %}
  {% else %}
    {% do run_query(create_table_as(True, temp_relation, sql)) %}
    {% do adapter.expand_target_column_types(
             from_relation=temp_relation,
             to_relation=target_relation) %}
    {# Look for changes to table schema. Returns dict of changes if successful.
       Use source columns for upserting/merging #}
    {% set dest_columns = process_schema_changes(on_schema_change,
                                                 temp_relation,
                                                 source_relation) %}
    {% if not dest_columns %}
      {% set dest_columns = adapter.get_columns_in_relation(source_relation) %}
    {% endif %}
    {% set build_sql = get_delete_insert_merge_sql(target_relation,
                                                   temp_relation,
                                                   unique_key,
                                                   dest_columns) %}
  {% endif %}
  {% call statement("main") %}
    {{ build_sql }}
  {% endcall %}

  {% if need_swap %}
    {% do adapter.rename_relation(target_relation, backup_relation) %}
    {% do adapter.rename_relation(temp_relation, target_relation) %}
  {% endif %}

  {% do persist_docs(target_relation, model) %}

  {% if source_relation is none
        or source_relation.is_view
        or should_full_refresh() %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
    {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
