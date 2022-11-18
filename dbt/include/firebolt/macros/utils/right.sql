{% macro firebolt__right(string_text, length_expression) %}

    CASE WHEN {{ length_expression }} = 0
        THEN ''
    ELSE
        SUBSTR(
            {{ string_text }},
            -1 * ({{ length_expression }})
        )
    END

{%- endmacro -%}
