{{
    config(
        materialized='view',
        schema = 'SIGMA'
    )
}}


SELECT
* 
FROM PUBLIC.ORDER_ITEMS