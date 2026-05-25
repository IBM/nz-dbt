{% macro netezza__create_schema(relation) -%}
  {%- set schema_check %}
    select count(1) as cnt
    from {{ netezza_database_ref(relation.database) }}.._v_schema
    where {{ netezza_schema_match('schema', relation.without_identifier().schema) }}
  {%- endset -%}

  {%- set schema_exists = (run_query(schema_check).columns[0].values() | first) | int -%}

  {%- if schema_exists == 0 -%}
    {%- call statement('create_schema') -%}
      create schema {{ netezza_database_ref(relation.database) }}.{{ netezza_schema_ref(relation.without_identifier().schema) }}
    {%- endcall -%}
  {%- endif -%}
{%- endmacro %}
