{% materialization incremental, adapter='firebolt' -%}
  {#
  Incremental implementation. Actual strategy SQL is determined
  outside this function, in strategies.sql.
  #}
  {# Not yet used
    {% set unique_key = config.get('unique_key') %}
  #}
  {% set strategy = config.get('incremental_strategy', default='append') %}
  {% set source = this %}
  {% set target = load_relation(this) %}
  {{ log("\n\n** source: " ~ source, True) }}
  {{ log("\n** sql: " ~ sql, True) }}
  {%- set full_refresh_mode = (should_full_refresh()) -%}
  {# Todo: figure out how to fail or fix if table schema changes.
     for now just ignore it.
     {% set on_schema_change = incremental_validate_on_schema_change(
      config.get('on_schema_change'),
      default='ignore') %} #}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {# First check whether we want to full refresh for source view or config reasons. #}
  {% if (full_refresh_mode or source.is_view) %}
    {% do adapter.drop_relation(source) %}
    {% set build_sql = create_table_as(False, target, sql) %}
  {% else %}
    {# Actually do the incremental query here. #}
    {% set new_records = create_view_as(False, model['name'] + '__dbt_temp', sql) %}
    {# Once we allow table schema changes the following will be uncommented.
       Will need to rewrite process_schema_changes.
       {% do adapter.expand_target_column_types(
                from_relation=temp_relation,
                to_relation=target) %}
       {% set dest_columns = process_schema_changes(on_schema_change,
                                                    new_records,
                                                    source) %}
       {% if not dest_columns %}
         {% set dest_columns = adapter.get_columns_in_relation(source) %}
       {% endif %}
    #}
    {% set dest_columns = adapter.get_columns_in_relation(source) %}
    {% set build_sql = get_incremental_sql(strategy,
                                           new_records,
                                           target,
                                           unique_key,
                                           dest_columns) %}
  {% endif %}
  {% call statement("main") %}
    {{ build_sql }}
  {% endcall %}

  {% do adapter.drop_relation(source) %}
  {% do adapter.rename_relation(new_records, source) %}
  {% do adapter.drop_relation(new_records) %}

  {% do persist_docs(target, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ return({'relations': [target]}) }}

{%- endmaterialization %}
