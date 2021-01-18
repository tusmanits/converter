        {{ 
            config(
                materialized='incremental',
                incremental_strategy='delete+insert',
                unique_key='1'
                ) 
        }}

        --these need to change by converter

        {{ config(schema = "sigma_its_sig") }}
        {{ config(alias = "repeat_purchase_orders1") }}

        {% set persisted_type = 'SQL_TRIGGER_VALUE' %}

        {% set alias = "repeat_purchase_orders1" %}

        {% set persisted_sql = "SELECT 'LOADED'" %}

        {{ config( post_hook=after_commit ("{{log_persisted_event_completed(\"SQL_TRIGGER_VALUE\",\"SELECT 'LOADED'\")}}")) }}

        --

        
        {% set ns = namespace(skipped="") %}
        {% set bs = namespace(status="") %}      
        

        {%- set sourceSQL -%}

        SELECT * FROM {{ref('rp')}}

        {%- endset -%}

        {%- set condition_ = get_persisted_regenration_flag(persisted_type, persisted_sql) -%}

        {{log(condition_, info = True)}}               

        {%- if condition_ == 1 -%}

        {%- set finalSQL -%}
        SELECT * FROM {{ this }}
        WHERE FALSE
        {%- endset -%}

        {% set ns.skipped = "true" %}

        {{log(ns.skipped, info = True)}}
    
        {%- else -%}

        {%- set finalSQL = sourceSQL -%}

        {%- endif-%}        

        {%- if is_incremental() -%}

        {{ finalSQL }}

        {%- else -%}

        {{ sourceSQL }}

        {%- endif -%}

        {{log(ns.skipped, info = True)}}

        {%- set bs.status = get_status_value(ns.skipped) -%}

        {{log(bs.status, info = True)}}

        {%- set log_transition = log_persisted_event_transition(persisted_type, persisted_sql, bs.status) -%}