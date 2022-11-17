{% macro firebolt__datediff(first_date, second_date, datepart) -%}

    datediff(
        '{{ datepart }}',
        {{ first_date }} :: TIMESTAMP,
        {{ second_date }} :: TIMESTAMP
        )

{%- endmacro %}
