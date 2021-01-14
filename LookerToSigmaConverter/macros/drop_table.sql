{% macro drop_table(table_name) %}

{% set sql %}
    BEGIN;
    DROP TABLE IF EXISTS {{ table_name }};
    COMMIT;
{% endset %}

{% do run_query(sql) %}
{% do log(table_name+" Table Droped", info=True) %}
{% endmacro %}