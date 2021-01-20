        {{ 
            config(
                materialized='incremental',
                incremental_strategy='delete+insert',
                unique_key='1'
                ) 
        }}

        --these need to change by converter

        {{ config(schema = "sigma_its_sig") }}
        {{ config(alias = "repeat_purchase_facts") }}

        {% set persisted_type = 'PERSIST_FOR' %}

        {% set alias = "repeat_purchase_facts" %}

        {% set persisted_sql = "24 hours" %}

        {{ config( post_hook=after_commit ("{{log_persisted_event_completed(\"PERSIST_FOR\",\"24 hours\")}}")) }}

        --

        
        {% set ns = namespace(skipped="") %}
        {% set bs = namespace(status="") %}      
        

        {%- set sourceSQL -%}

        SELECT
        order_items.order_id
        , COUNT(DISTINCT repeat_order_items.id) AS number_subsequent_orders
        , MIN(repeat_order_items.created_at) AS next_order_date
        , MIN(repeat_order_items.order_id) AS next_order_id
      FROM order_items
      LEFT JOIN order_items repeat_order_items
        ON order_items.user_id = repeat_order_items.user_id
        AND order_items.created_at < repeat_order_items.created_at
      GROUP BY 1

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