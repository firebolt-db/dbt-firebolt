{% macro firebolt__bool_or(expression) -%}

    ANY({{ expression }})

{%- endmacro %}
