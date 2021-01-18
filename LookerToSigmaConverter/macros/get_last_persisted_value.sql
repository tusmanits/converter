{% macro get_last_persisted_value(model_name, persist_for) %}
    
    {%- set log_table = get_persisted_relation() -%}

    {%- set sql -%}
    SELECT LAST_LOADED_PERSISTED_VALUE from {{ log_table }}
    WHERE LOWER(PERSISTED_MODEL) = LOWER('{{ model_name }}')
    ORDER BY PERSISTED_TIMESTAMP DESC
    LIMIT 1
    {%- endset -%}

{% endmacro %}