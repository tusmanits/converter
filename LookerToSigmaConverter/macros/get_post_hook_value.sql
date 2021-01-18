{% macro get_post_hook_value(persist_for, value) %}
    {% if persist_for == "PERSIST_FOR"%}

    {% if value == "true"%}
    {{ return("log_persisted_load('PERSIST_FOR','SKIPPED','')")}}
    {% else %}
    {{ return("log_persisted_load('PERSIST_FOR','LOADED','')")}}
    {%- endif -%}

    {% else %}

    {% if value == "true"%}
    {{ return("log_persisted_load('SQL_TRIGGER_VALUE','SKIPPED','')")}}
    {% else %}
    {{ return("log_persisted_load('SQL_TRIGGER_VALUE','LOADED','')")}}
    {%- endif -%}

    {%- endif -%}

{% endmacro %}


{% macro get_final_hook(persist_for, value) %}
    {%- set var = get_post_hook_value(persist_for, value) -%}

    {%- set final_hook -%}
    config( post_hook=after_commit('{var}')
    {%- endset -%}

    {{return(final_hook)}}

{% endmacro %}


