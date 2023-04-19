{% macro dbt_firebolt_get_tmp_relation_type(strategy) %}
  {#
    Determine whether to use a temporary view or a table based on
    specified strategy and config. Users can override the behaviour
    by setting tmp_replation_type. Default is to use a view (more efficient).

    Arguments:
      strategy: incremental strategy from config. e.g. "append"
  #}
  {%- set tmp_relation_type = config.get('tmp_relation_type') -%}

  {% if tmp_relation_type == "table" %}
    {{ return("table") }}
  {% elif tmp_relation_type == "view" %}
    {{ return("view") }}
  {% elif strategy in ("default", "append", "insert_overwrite") %}
    {{ return("view") }}
  {% else %}
    {{ return("table") }}
  {% endif %}
{% endmacro %}

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

  {% set unique_key = config.get('unique_key') %}

  {% set grant_config = config.get('grants') %}

  {%- set strategy = config.get('incremental_strategy') -%}
  {%- if is_incremental() and strategy is none -%}
    {{ log('Model %s is set to incremental, but no incremental strategy is set. '
           'Defaulting to append' % this, True) }}
    {%- set strategy = 'append' -%}
  {%- endif -%}
  {# In following lines:

     `target` is just a dbt `BaseRelation` object, not a DB table.

     We're only retrieving `existing` to check for existence; `load_relation()`
     returns a `BaseRelation` with the dictionary values of any relations that exist
     in dbt's cache that share the identifier of the passed relation. If none
     exist, returns None. Note that this does *not* create a table in the DB.

     `target` is a relation with all the same fields as `existing`, but guaranteed
     to be an actual `BaseRelation` object. #}
  {%- set target = this.incorporate(type='table') -%}
  {%- set existing = load_relation(this) -%}
  {%- set tmp_relation_type = dbt_firebolt_get_tmp_relation_type(strategy) -%}
  {%- set new_records = make_temp_relation(target) -%}
  {{ drop_relation_if_exists(new_records) }}
  {%- set new_records = new_records.incorporate(type=tmp_relation_type) -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(
                                config.get('on_schema_change'),
                                default='ignore') -%}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}
  {%- set partition_by = config.get('partition_by') -%}
  {# First check whether we want to full refresh for existing view or config reasons. #}
  {%- if strategy == 'insert_overwrite' and not partition_by -%}
    {% do exceptions.raise_compiler_error('`insert_overwrite` is specified for model %s, '
                                          'but `partition_by` is not. `insert_overwrite` '
                                          'requires a partition.' % (target)) %}
  {%- endif -%}
  {%- set do_full_refresh = (should_full_refresh()
                             or existing.is_view) %}
  {% if existing is none %}
    {%- set build_sql = create_table_as(False, target, sql) -%}
  {%- elif do_full_refresh -%}
    {{ drop_relation_if_exists(existing) }}
    {%- set build_sql = create_table_as(False, target, sql) -%}
  {%- else -%}
    {# Actually do the incremental query here. #}
    {# Instantiate new objects in dbt's internal list. Have to
       run this query so dbt can query the DB to get the columns in
       new_records. #}
    {%- if tmp_relation_type=='view' -%}
      {%- do run_query(create_view_as(new_records, sql)) -%}
    {%- else -%}
      {%- do run_query(create_table_as(True, new_records, sql)) -%}
    {%- endif -%}
    {# All errors involving schema changes are dealt with in `process_schema_changes`. #}
    {%- set dest_columns = process_schema_changes(on_schema_change,
                                                  new_records,
                                                  existing) -%}
    {%- set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) -%}
    {%- set build_sql = get_incremental_sql(strategy,
                                            new_records,
                                            target,
                                            unique_key,
                                            dest_columns,
                                            incremental_predicates) -%}
  {%- endif -%}
  {%- call statement("main") -%}
    {{ build_sql }}
  {%- endcall -%}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke) %}

  {# Todo: figure out what persist_docs and create_indexes do. #}
  {%- do persist_docs(target, model) -%}
  {%- if existing is none
      or existing.is_view
      or should_full_refresh() -%}
  {%- do create_indexes(target) -%}
  {%- endif %}

  {{ drop_relation_if_exists(new_records) }}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ return({'relations': [target]}) }}

{%- endmaterialization %}
