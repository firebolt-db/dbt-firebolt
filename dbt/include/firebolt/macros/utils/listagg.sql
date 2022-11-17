{% macro firebolt__listagg(measure, delimiter_text, order_by_clause, limit_num) -%}

    {% if order_by_clause -%}

    {{ exceptions.raise_compiler_error("Firebolt does not currently support "
                                       "ARRAY_AGG with ORDER BY clause.") }}

    {%- endif %}

    {% if limit_num -%}

    ARRAY_JOIN(
        SLICE(
            ARRAY_AGG({{ measure }})
            0,
            {{ limit_num }}
        ),
        {{ delimiter_text }}
    )

    {%- else %}

    ARRAY_JOIN(ARRAY_AGG({{ measure }}), {{ delimiter_text }})

    {%- endif %}

{%- endmacro %}
