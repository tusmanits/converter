        {{ 
            config(
                materialized='incremental',
                incremental_strategy='delete+insert',
                unique_key='1'
                ) 
        }}

        --these need to change by converter

        {{ config(schema = "sigma_its_sig") }}
        {{ config(alias = "clean_events") }}

        {% set persisted_type = 'SQL_TRIGGER_VALUE' %}

        {% set alias = "clean_events" %}

        {% set persisted_sql = "SELECT CURRENT_DATE()" %}

        {{ config( post_hook=after_commit ("{{log_persisted_event_completed(\"SQL_TRIGGER_VALUE\",\"SELECT CURRENT_DATE()\")}}")) }}

        --

        
        {% set ns = namespace(skipped="") %}
        {% set bs = namespace(status="") %}      
        

        {%- set sourceSQL -%}

        
            SELECT
            *
            FROM (SELECT * FROM PUBLIC.events WHERE EVENT_TYPE NOT IN ('test', 'staff'))
            

        {%- endset -%}

        {%- set condition_ = get_persisted_regenration_flag(persisted_type, persisted_sql) -%}

        {{log(condition_, info = True)}}               

        {%- if condition_ == 1 -%}

        {%- set finalSQL -%}
        SELECT * FROM {{ this }}
        WHERE FALSE
        {%- endset -%}

        {% set ns.skipped = "true" %}
    
        {%- else -%}

        {%- set finalSQL = sourceSQL -%}

        {%- endif-%}        

        {%- if is_incremental() -%}

        {{ finalSQL }}

        {%- else -%}

        {{ sourceSQL }}

        {%- endif -%}

        {%- set bs.status = get_status_value(ns.skipped) -%}

        {%- set log_transition = log_persisted_event_transition(persisted_type, persisted_sql, bs.status) -%}