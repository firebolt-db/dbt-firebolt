{# Todo: Should this be moved to impl.py? #}
{% macro intersect_columns(source_columns, target_columns) %}
  {# Return a List[FireboltColumn] of columns that appear in both source and target.
     Args:
       source_columns: List[FireboltColumn]
       target_columns: List[FireboltColumn]
  #}
  {% set result = [] %}
  {% set source_names = source_columns | map(attribute = 'column') | list %}
  {% set target_names = target_columns | map(attribute = 'column') | list %}
  {# Check whether the name attribute exists in the target - this does
     not perform a data type check. This is O(m•n), but cardinality
     of columns is probably low enough that it doesn't matter. #}
  {% for sc in source_columns %}
    {% if sc.name in target_names %}
       {{ result.append(sc) }}
    {% endif %}
  {% endfor %}
  {{ return(result) }}
{% endmacro %}

{#
{% macro diff_column_data_types(source_columns, target_columns) %}
  {% set result = [] %}
  {% for sc in source_columns %}
    {% set tc = target_columns | selectattr('column', 'equalto', sc.column) | list | first %}
    {% if tc %}
      {% if tc.dtype != sc.dtype %}
      {{ result.append({'column_name': tc.column, 'new_type': sc.dtype}) }}
      {% endif %}
    {% endif %}
  {% endfor %}
  {{ return(result) }}
{% endmacro %}
#}
