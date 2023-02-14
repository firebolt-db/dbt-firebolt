{% macro firebolt__array_append(array, new_element) -%}
    {{ array_concat(array, [new_element]) }}
{%- endmacro %}
