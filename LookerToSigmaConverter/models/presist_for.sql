
        {{ config(materialized = "table") }}

        {{ config(schema = "SIGMA_ITS_SIG") }}

        SELECT CURRENT_TIMESTAMP, '' AS MODEL, '' AS SCHEMA WHERE 1 != 0