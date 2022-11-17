{% macro firebolt__intersect() %}

    {{ exceptions.raise_compiler_error("Firebolt does not currently support "
                                       "INTERSECT clause.") }}

{% endmacro %}
