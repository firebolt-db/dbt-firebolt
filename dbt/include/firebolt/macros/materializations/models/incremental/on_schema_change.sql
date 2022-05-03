{% macro incremental_validate_on_schema_change(on_schema_change, default='ignore') %}
  {#
  Ensure that the value of `on_schema_change` in dbt_project models is valid.
  Firebolt doesnt currently support `sync_all_columns`, so this is excluded
  from valid columns.
  #}
  {% if on_schema_change == 'sync_all_columns' %}
    {% do exceptions.raise_compiler_error(
              'Firebolt does not support the on_schema_change value "sync_all_columns."') %}
    {% set log_message = 'Invalid value for on_schema_change (%s) specified. Setting default value of %s.' %
                        (on_schema_change, default) %}
     {% do log(log_message, True) %}
    {{ return(default) }}
  {% else %}
    {{ return(on_schema_change) }}
  {% endif %}
{% endmacro %}


{% macro check_for_schema_changes(source_relation, target_relation) %}
  {% set schema_changed = False %}
  {%- set source_columns = adapter.get_columns_in_relation(source_relation) -%}
  {%- set target_columns = adapter.get_columns_in_relation(target_relation) -%}
  {%- set source_not_in_target = diff_columns(source_columns, target_columns) -%}
  {%- set target_not_in_source = diff_columns(target_columns, source_columns) -%}
  {% set new_target_types = diff_column_data_types(source_columns, target_columns) %}
  {% set common_columns = intersect_columns(source_columns, target_columns) %}
  {{ log('\n\n** source_columns: ' ~ source_columns ~ '\n** target_columns: ' ~ target_columns, True) }}
  {% if source_not_in_target != [] %}
    {% set schema_changed = True %}
  {% elif target_not_in_source != [] or new_target_types != [] %}
    {% set schema_changed = True %}
  {% elif new_target_types != [] %}
    {% set schema_changed = True %}
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
  {% set msg %}
    In {{ target_relation }}:
        Schema changed: {{ schema_changed }}
        Source columns not in target: {{ source_not_in_target }}
        Target columns not in source: {{ target_not_in_source }}
        New column types: {{ new_target_types }}
  {% endset %}
  {% do log(msg) %}
  {{ return(changes_dict) }}
{% endmacro %}


{% macro process_schema_changes(on_schema_change, source_relation, target_relation) %}
  {#
  Check for schema changes. Error out if appropriate, else return the list of columns
  that will be transferred from source to target, i.e. the intersection of the columns.
  #}
  {% if on_schema_change == 'append_new_columns' or on_schema_change == 'sync_all_columns' %}
     {% do exceptions.raise_compiler_error(
             'Firebolt does not allow table schema changes.') %}
  {% endif %}
    {# check_for_schema_changes returns a dict = {
       'schema_changed': schema_changed,
       'source_not_in_target': source_not_in_target,
       'target_not_in_source': target_not_in_source,
       'source_columns': source_columns,
       'target_columns': target_columns,
       'new_target_types': new_target_types
    } %} #}
  {% set schema_changes_dict = check_for_schema_changes(source_relation, target_relation) %}
  {% if schema_changes_dict['schema_changed'] %}
    {% if on_schema_change == 'fail' %}
      {% do exceptions.raise_compiler_error(
          'on_schema_change was set to "fail" and a schema change was detected.') %}
    {% endif %}
    {% if schema_changes_dict['source_not_in_target'] and schema_changes_dict['target_not_in_source'] %}
      {# Columns have been added *and* removed. #}
      {% do exceptions.raise_compiler_error(
        'Firebolt does not allow table schema changes involving both column additions and removals.') %}
      }
    {% endif %}
    {% if schema_changes_dict['new_target_types'] %}
      {% do exceptions.raise_compiler_error(
        'Firebolt, does not allow column types to change.') %}
      }
    {% endif %}
    {# Todo: Should I log changes here? #}
    {% if schema_changes_dict['source_columns'] < len(schema_changes_dict['target_columns']) %}
      {{ return(schema_changes_dict['source_columns']) }}
    {% endif %}
    {{ return(schema_changes_dict['target_columns']) }}
  {% endif %}
{% endmacro %}
