{% macro firebolt__bool_or(expression) -%}

    MAX({{ expression }})

{%- endmacro %}
