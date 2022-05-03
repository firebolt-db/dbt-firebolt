{% macro intersect_columns(source_columns, target_columns) %}
  {# Return list of columns that appear in both source and target. #}
  {% set source_names = source_columns | map(attribute = 'column') | list %}
  {% set target_names = target_columns | map(attribute = 'column') | list %}
  {# check whether the name attribute exists in the target - this does
     not perform a data type check. This is really inefficient, but cardinality
     of columns is probably low enough that it doesn't matter. #}
  {{ return([value for value in target_names if value in source_names]) }}
{% endmacro %}
