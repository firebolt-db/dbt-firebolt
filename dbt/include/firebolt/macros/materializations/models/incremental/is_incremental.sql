{% macro is_incremental() %}
    {#- Return True if model is incremental, otherwise False. -#}
    {% if not execute %}
        {{ return(False) }}
    {% else %}
        {% set relation = adapter.get_relation(this.database,
                                               this.schema,
                                               this.table) %}
        {{ return(relation is not none
                  and relation.type == 'table'
                  and model.config.materialized == 'incremental'
                  and not should_full_refresh()) }}
    {% endif %}
{% endmacro %}
