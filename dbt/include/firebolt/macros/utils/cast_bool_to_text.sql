{% macro firebolt__cast_bool_to_text(field) %}
    CASE
        WHEN {{ field }} = false THEN 'false'
        WHEN {{ field }} = true THEN 'true'
        ELSE NULL
    END
{% endmacro %}
