{% macro firebolt__get_show_grant_sql(relation) %}
    {{ adapter.raise_grant_error() }}
{% endmacro %}


{%- macro firebolt__get_grant_sql(relation, privilege, grantee) -%}
    {{ adapter.raise_grant_error() }}
{%- endmacro -%}


{%- macro firebolt__get_revoke_sql(relation, privilege, grantee) -%}
    {{ adapter.raise_grant_error() }}
{%- endmacro -%}


{% macro firebolt__copy_grants() %}
    {{ adapter.raise_grant_error() }}
{% endmacro %}


{% macro firebolt__apply_grants(relation, grant_config, should_revoke) %}
    {% if grant_config %}
        {{ adapter.raise_grant_error() }}
    {% endif %}
{% endmacro %}
