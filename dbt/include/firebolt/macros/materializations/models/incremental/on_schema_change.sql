{% macro incremental_validate_on_schema_change(on_schema_change, default='ignore') %}
  {#
  Ensure that the value of `on_schema_change` in dbt_project models is valid.
  Firebolt doesnt currently support `sync_all_columns`, so this is excluded
  from valid columns.
  #}
  {% if on_schema_change == 'sync_all_columns' %}
    {% do exceptions.raise_compiler_error(
              'Firebolt does not support the on_schema_change value "sync_all_columns."') %}
    {{ return(default) }}
  {% else %}
    {{ return(on_schema_change) }}
  {% endif %}
{% endmacro %}


{% macro check_for_schema_changes(source_relation, target_relation) %}
  {#
  Return a dict = {
     'schema_changed': schema_changed,
     'source_not_in_target': source_not_in_target (List[FireboltColumn]),
     'target_not_in_source': target_not_in_source (List[FireboltColumn]),
     'source_columns': source_columns (List[FireboltColumn]),
     'target_columns': target_columns (List[FireboltColumn]),
     'new_target_types': new_target_types,
     'common_columns': common_colums
  }
  Args:
      source_relation: dbt Relation
      target_relation: dbt Relation
  #}
  {%- set schema_changed = False -%}
  {%- set source_columns = adapter.get_columns_in_relation(source_relation) -%}
  {%- set target_columns = adapter.get_columns_in_relation(target_relation) -%}
  {%- set source_not_in_target = diff_columns(source_columns, target_columns) -%}
  {%- set target_not_in_source = diff_columns(target_columns, source_columns) -%}
  {%- set new_target_types = diff_column_data_types(source_columns, target_columns) -%}
  {%- set common_columns = intersect_columns(source_columns, target_columns) -%}
  {% if source_not_in_target != [] %}
    {%- set schema_changed = True -%}
  {% elif target_not_in_source != [] or new_target_types != [] %}
    {%- set schema_changed = True -%}
  {% elif new_target_types != [] %}
    {%- set schema_changed = True -%}
  {% endif %}
  {% set changes_dict = {
    'schema_changed': schema_changed,
    'source_not_in_target': source_not_in_target,
    'target_not_in_source': target_not_in_source,
    'source_columns': source_columns,
    'target_columns': target_columns,
    'new_target_types': new_target_types,
    'common_columns': common_columns
  } %}
  {{ return(changes_dict) }}
{% endmacro %}


{% macro process_schema_changes(on_schema_change, source_relation, target_relation) %}
  {#
  Check for schema changes. Error out if appropriate, else return the list of columns
  that will be transferred from source to target, i.e. the intersection of the columns.
  #}
  {% if on_schema_change == 'sync_all_columns' %}
     {% do exceptions.raise_compiler_error(
             'Firebolt does not allow an `on_schema_change` value of `sync_all_columns`.') %}
  {% endif %}
  {% set schema_changes_dict = check_for_schema_changes(source_relation, target_relation) %}
  {% if schema_changes_dict['schema_changed'] %}
    {% if on_schema_change == 'fail' %}
      {% do exceptions.raise_compiler_error(
          'A schema change was detected and `on_schema_change` was set to "fail".') %}
    {% else %}
      {% set fail_msg %}
          Schema changes detected on this incremental!
          Either revert the change or run the model with the --full-refresh flag
          on the command line to recreate the model with the new schema definition.
          Running with the --full-refresh flag drops and recreates the database object.

          Additional troubleshooting context:
              Source columns not in target: {{ schema_changes_dict['source_not_in_target'] }}
              Target columns not in source: {{ schema_changes_dict['target_not_in_source'] }}
              New column types: {{ schema_changes_dict['new_target_types'] }}
      {% endset %}

      {% do exceptions.raise_compiler_error(fail_msg) %}
    {% endif %}
  {% endif %}
  {{ return(schema_changes_dict['common_columns']) }}
{% endmacro %}
