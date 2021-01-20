        {{ 
            config(
                materialized='incremental',
                incremental_strategy='delete+insert',
                unique_key='1'
                ) 
        }}

        --these need to change by converter

        {{ config(schema = "@@SCHEMA@@") }}
        {{ config(alias = "@@ALIAS@@") }}

        {% set persisted_type = '@@PERSISTED_TYPE@@' %}

        {% set alias = "@@ALIAS@@" %}

        {% set persisted_sql = "@@PERSISTED_SQL@@" %}

        {{ config( post_hook=after_commit ("{{log_persisted_event_completed(\"@@PERSISTED_TYPE@@\",\"@@PERSISTED_SQL@@\")}}")) }}

        --

        
        {% set ns = namespace(skipped="") %}
        {% set bs = namespace(status="") %}      
        

        {%- set sourceSQL -%}

        @@SQL@@

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