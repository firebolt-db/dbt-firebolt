{% macro firebolt__position(substring_text, string_text) %}
    STRPOS({{ string_text }}, {{ substring_text }})
{%- endmacro -%}
