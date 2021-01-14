{% macro star_model(from, except=[]) -%}

  {%- call statement('empty_results', fetch_result=True) -%}
   select
   *
   from {{ref(from)}}
   where false

  {%- endcall -%}
  
  {% set results = load_result('empty_results') %}
  
  {% if results %} {# this handles the error on compilation #}
    {%- set cols = load_result('empty_results').table.column_names -%}
  {% else %}
    {%- set cols = [] -%}
  {% endif %}
  
  {%- set include_cols = [] %}
  
  {%- for col in cols -%}
    {%- if col.column not in except -%}
      {% set _ = include_cols.append(col.column) %}
    {%- endif %}
  {%- endfor %}

  {%- for col in include_cols %}
    "{{ col }}" {% if not loop.last %},
    {% endif %}
  {%- endfor -%}

{%- endmacro %}