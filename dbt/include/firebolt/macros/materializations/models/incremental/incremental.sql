{% materialization incremental, adapter='firebolt' -%}
  {#
  Incremental implementation. Actual strategy SQL is determined
  outside this materialization, in strategies.sql.

  This function works by first creating a view, `new_records`, of type `BaseRelation`,
  then using a CTE defined in the project model to populate `new_records` with
  any records that ought to be inserted into the original table (views aren't currently
  supported), `existing`, using an INSERT. This process is agnostic to the incremental
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
  {# incorporate() returns a new BaseRelation, an altered copy of `this`.
     If `this` is None, returns None.
     Note that this does *not* create a table in the DB.
     We're doing this in case `this` is a view. Now we have a relation
     with all the same field values, but as a table. #}
  {% set target = this.incorporate(type='table') %}
  {# If `this` exists, load_relation() returns BaseRelation with the dictionary
     values of `this`, else None. Note that, as with incorporate(),
     this does *not* create a table in the DB. #}
  {% set existing = load_relation(this) %}
  {% set new_records = make_temp_relation(target) %}
  {% set new_records = new_records.incorporate(type='view') %}

  {# Todo: For now we don't allow table schema changes; just ignore them.
     {% set on_schema_change = incremental_validate_on_schema_change(
      config.get('on_schema_change'),
      default='ignore') %}
  #}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {# First check whether we want to full refresh for existing view or config reasons. #}
  {% set do_full_refresh = (full_refresh_mode or existing.is_view) %}
  {% if existing is none %}
    {# I'm unclear how this would work: if `existing` is None it's because
       `this` is None, so `target` will also be None. #}
    {% set build_sql = create_table_as(False, target, sql) %}
  {% elif should_full_refresh() or existing.is_view %}
    {{ drop_relation_if_exists(existing) }}
    {% set build_sql = create_table_as(False, target, sql) %}
  {% else %}
    {# Actually do the incremental query here. #}
    {# Instantiate new objects in dbt's internal list #}
    {% do run_query(create_view_as(new_records, sql)) %}
    {# Once we allow table schema changes the following will be uncommented.
       Will need to rewrite process_schema_changes.
       {% do adapter.expand_target_column_types(
                from_relation=temp_relation,
                to_relation=target) %}
       {% set dest_columns = process_schema_changes(on_schema_change,
                                                    new_records,
                                                    existing) %}
       {% if not dest_columns %}
         {% set dest_columns = adapter.get_columns_in_relation(existing) %}
       {% endif %}
    #}
    {% set dest_columns = adapter.get_columns_in_relation(existing) %}
    {% set build_sql = get_incremental_sql(strategy,
                                           new_records,
                                           target,
                                           unique_key,
                                           dest_columns) %}
  {% endif %}
  {% call statement("main") %}
    {{ build_sql }}
  {% endcall %}


  {# Todo: figure out what persist_docs and create_indexes do. #}
  {% do persist_docs(target, model) %}
  {% if existing is none
      or existing.is_view
      or should_full_refresh() %}
  {% do create_indexes(target) %}
  {% endif %}

  {{ drop_relation_if_exists(new_records) }}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ return({'relations': [target]}) }}

{%- endmaterialization %}
