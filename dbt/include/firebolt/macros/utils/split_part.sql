{% macro firebolt__split_part(string_text, delimiter_text, part_number) %}
    SPLIT_PART(
        {{ string_text }},
        {{ delimiter_text }},
        {{ part_number }}
    )
{% endmacro %}