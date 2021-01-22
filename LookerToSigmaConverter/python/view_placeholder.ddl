        {{ 
            config(
                materialized='view',
                ) 
        }}

        --these need to change by converter

        {{ config(schema = "@@SCHEMA@@") }}
        {{ config(alias = "@@ALIAS@@") }}


        @@SQL@@