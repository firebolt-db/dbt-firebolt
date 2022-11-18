{% macro firebolt__except() %}

    {{ exceptions.raise_compiler_error("Firebolt does not currently support "
                                       "EXCEPT clause.") }}

{% endmacro %}
