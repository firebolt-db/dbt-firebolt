{% macro intersect_columns(source_columns, target_columns) %}
  {# Return list of columns that appear in both source and target. #}
  {% set result = [] %}
  {% set source_names = source_columns | map(attribute = 'column') | list %}
  {% set target_names = target_columns | map(attribute = 'column') | list %}
   {# check whether the name attribute exists in the target - this does
      not perform a data type check. This is really inefficient, but cardinality
      of columns is probably low enough that it doesn't matter. #}
   {% for sc in source_columns %}
     {% if sc.name in target_names %}
        {{ result.append(sc) }}
     {% endif %}
   {% endfor %}
  {{ return(result) }}
{% endmacro %}
