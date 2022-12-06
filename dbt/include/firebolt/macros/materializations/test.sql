{% macro default__get_test_sql(main_sql, fail_calc, warn_if, error_if, limit) %}

    SELECT
      CAST({{ fail_calc }} AS INT) AS failures,
      CASE WHEN {{ fail_calc }} {{ warn_if }}
        THEN 'true' ELSE 'false'
      END AS should_warn,
      CASE WHEN {{ fail_calc }} {{ error_if }}
        THEN 'true' ELSE 'false'
      END AS should_error
    FROM (
      {{ main_sql }}
      {{ "LIMIT " ~ limit if limit is not none }}
    ) dbt_internal_test
{%- endmacro %}
