-- depends on: {{ ref('rp') }}

        {{ config(materialized = "ephemeral") }}

        {{ config(schema = "SIGMA_ITS_SIG") }}
                
        {%- set presist_query_sql-%}
        SELECT 0  
        {%- endset-%}

        {%- set presist_result = run_query(presist_query_sql)-%}

        {%- if execute -%}

        {%- set result = presist_result.columns[0].values()|first -%}

        {%- if result|int != 1 %}

        {%- set execute_var -%}
        Executing Model :  {{ this }} {{ result }} Added Result

        {%- endset-%}

        {%- set sql%}

        CREATE OR REPLACE TABLE SIGMA_ITS_SIG.RP

        AS

        {{ ref('rp') }}             

        {%- endset -%}

        {%- do run_query(sql)-%}

        {{ log( execute_var , info = True) }}

        {%- else -%}

        {%- set skip_var -%}
        Skip Model :  {{ this }} {{ result }} Added Result

        {%- endset -%}

        {{ log( skip_var , info = True) }}
        
        {%- endif -%}
        
        {%- else -%}

        {%- set execute_var -%}
        Executing Model :  {{ this }} Added Result
        {%- endset -%}

        {{ log( execute_var , info = True) }}        
        
        {%- endif -%}



        