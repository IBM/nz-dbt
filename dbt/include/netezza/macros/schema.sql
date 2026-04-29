{% macro netezza__create_schema(relation) -%}
  {%- set schema_check %}
    select count(1) as cnt
    from {{ relation.database }}.._v_schema
    where upper(schema) = upper('{{ relation.without_identifier().schema }}')
  {%- endset -%}

  {%- set schema_exists = (run_query(schema_check).columns[0].values() | first) | int -%}

  {%- if schema_exists == 0 -%}
    {%- call statement('create_schema') -%}
      create schema {{ relation.without_identifier() }}
    {%- endcall -%}
  {%- endif -%}
{%- endmacro %}
