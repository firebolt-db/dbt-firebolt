{% materialization incremental, adapter='firebolt' -%}
  {#
  Incremental implementation. Actual strategy SQL is determined
  outside this materialization, in strategies.sql.

  This function works by first creating a view, `new_records`, of type `BaseRelation`,
  then using a CTE defined in the project model to populate `new_records` with
  any records that ought to be inserted into the original table (views aren't currently
  supported), `source`, using an INSERT. This process is agnostic to the incremental
  strategy actually used: the specific INSERT is defined in `strategies.sql`.

  Note that all new relations must first be instantiated as BaseRelation objects,
  and that those objects are used to create and query actual relations in the DB.
  Care must be taken to correctly define each BaseRelation as either a view or
  a table, lest subsequent operations on the relations (be the they objects or
  the DB tables the objects abstract) fail.
  #}

  {# Not yet used
    {% set unique_key = config.get('unique_key') %}
  #}
  {% set strategy = config.get('incremental_strategy', default='append') %}
  -- load_relation() returns the BaseRelation associated with `this`, if it exists.
  {% set source = load_relation(this) %}
  -- incorporate() returns a new BaseRelation, an altered copy of `this`.
  {% set target = this.incorporate(type='table') %}
  {{ log("\n\n** source: " ~ source, True) }}
  {{ log("\n\n** source type: " ~ source.type, True) }}
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
  {% if source is none %}
    {% set build_sql = create_table_as(False, target, sql) %}
  {% elif (full_refresh_mode or source.is_view) %}
    {{ log('\n\n** drop source: ' ~ source, True) }}
    {{ log('\n\n** source type: ' ~ source.__dict__, True) }}
    {% do adapter.drop_relation(source) %}
    {% set build_sql = create_table_as(False, target, sql) %}
  {% else %}
    {# Actually do the incremental query here. #}
    {% set build_sql = create_view_as(new_records, sql) %}
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
    {% do adapter.drop_relation(new_records) %}
    {% do adapter.drop_relation(source) %}
  {% endif %}
  {% call statement("main") %}
    {{ build_sql }}
  {% endcall %}

  {% do adapter.rename_relation(target, source) %}

  {% do persist_docs(target, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ return({'relations': [target]}) }}

{%- endmaterialization %}
