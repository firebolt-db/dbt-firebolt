{% macro firebolt__datediff(first_date, second_date, datepart) -%}

    date_diff(
        '{{ datepart }}',
        {{ first_date }} :: TIMESTAMP,
        {{ second_date }} :: TIMESTAMP
        )

{%- endmacro %}
