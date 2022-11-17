{% macro firebolt__split_part(string_text, delimiter_text, part_number) %}

    {% if delimiter_text.startswith("'") and delimiter_text.endswith("'") -%}
    SPLIT_PART(
        {{ string_text }},
        {{ delimiter_text }},
        {{ part_number }}
    )
    {%- else %}
    {{ exceptions.raise_compiler_error("Firebolt does not currently support "
                                       "reading delimiter from a column.") }}

    {%- endif %}
{% endmacro %}
