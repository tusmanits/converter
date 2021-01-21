        {{ 
            config(
                materialized='incremental',
                incremental_strategy='delete+insert',
                unique_key='1'
                ) 
        }}

        --these need to change by converter

        {{ config(schema = "sigma_its_sig") }}
        {{ config(alias = "events_summary") }}

        {% set persisted_type = 'SQL_TRIGGER_VALUE' %}

        {% set alias = "events_summary" %}

        {% set persisted_sql = "SELECT MAX(id) FROM SIGMA_ITS_SIG.clean_events" %}

        {{ config( post_hook=after_commit ("{{log_persisted_event_completed(\"SQL_TRIGGER_VALUE\",\"SELECT MAX(id) FROM SIGMA_ITS_SIG.clean_events\")}}")) }}

        --

        
        {% set ns = namespace(skipped="") %}
        {% set bs = namespace(status="") %}      
        

        {%- set sourceSQL -%}

        
            SELECT
            *
            FROM (SELECT EVENT_TYPE, created_at::date as date, COUNT(*) AS num_events FROM {{ref('sigma_its_sig_clean_events')}} AS clean_events GROUP BY 1, 2)
            

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