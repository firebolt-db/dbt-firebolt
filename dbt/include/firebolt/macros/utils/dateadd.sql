{% macro firebolt__dateadd(datepart, interval, from_date_or_timestamp) %}

    DATE_ADD(
        '{{ datepart }}',
        {{ interval }},
        {{ from_date_or_timestamp }}
        )

{% endmacro %}
