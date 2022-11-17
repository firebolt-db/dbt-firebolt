{% macro firebolt__cast_bool_to_text(field) %}
    CASE
        WHEN {{ field }} = 0 THEN 'false'
        WHEN {{ field }} = 1 THEN 'true'
        ELSE NULL
    END
{% endmacro %}
