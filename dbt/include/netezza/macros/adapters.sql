{% macro dist(dist) %}
  {%- if dist is not none -%}
      {%- if dist is string -%}
        {%- if dist in ['random'] -%}
          distribute on {{ dist }}
        {%- else -%}
          distribute on ({{ dist }})
        {%- endif -%}
      {%- else -%}
        distribute on (
          {%- for item in dist -%}
            {{ item }}
            {%- if not loop.last -%},{%- endif -%}
          {%- endfor -%}
        )  
      {%- endif -%}
  {%- endif -%}
{%- endmacro -%}

{% macro create_table_with_constraints(relation, sql, _dist) %}
    {%- set _dist = config.get('dist') -%}
    create table 
    {{ relation }}
    {{ get_assert_columns_equivalent(sql) }}
    {{ get_table_columns_and_constraints() }}
    {%- set sql = get_select_subquery(sql) %}
    {{ dist(_dist) }}
    ;

    insert into {{ relation }} 
    {{ sql }};
{% endmacro %}

{% macro create_table_no_constraints(temporary, relation, sql, _dist) %}
    create {% if temporary -%}temporary{%- endif %} table
    {{ relation }}
    as (
        {{ sql }}
    )
    {{ dist(_dist) }}
    ;
{% endmacro %}

{% macro netezza__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set _dist = config.get('dist') -%}
  {{ sql_header if sql_header is not none }}

  {% set contract_config = config.get('contract') %}
  {% if contract_config.enforced and (not temporary) %}
    {{ create_table_with_constraints(relation, sql, _dist) }}
  {% else %}
    {{ create_table_no_constraints(temporary, relation, sql, _dist) }}
  {% endif %}
{%- endmacro %}

{% macro netezza__list_schemas(database) -%}
  {% set sql %}
    select distinct schema_name
    from {{ information_schema_name(database) }}.SCHEMATA
    where catalog_name ilike '{{ database.strip("\"") }}'
  {% endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}

{% macro netezza__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True, auto_begin=False) -%}
    select
      '{{ schema_relation.database }}' as database,
      tablename as name,
      schema as schema,
      'table' as type
    from {{ schema_relation.database }}.._v_table
    where schema ilike '{{ schema_relation.schema }}'
    union all
    select
      '{{ schema_relation.database }}' as database,
      viewname as name,
      schema as schema,
      'view' as type
    from {{ schema_relation.database }}.._v_view
    where schema ilike '{{ schema_relation.schema }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro netezza__get_empty_subquery_sql(select_sql, select_sql_header=none) %}
    {%- if select_sql_header is not none -%}
    {{ select_sql_header }}
    {%- endif -%}
    select * from (
        {{ select_sql }}
    ) __dbt_sbq
    where false
    limit 0
{% endmacro %}

{% macro netezza__drop_schema(relation) -%}
  {# Netezza does not support DROP SCHEMA IF EXISTS.
     Check _v_schema first to avoid errors on non-existing schemas. #}
  {%- set schema_check %}
    select count(1) as cnt
    from {{ relation.database }}.._v_schema
    where upper(schema) = upper('{{ relation.without_identifier().schema }}')
  {%- endset -%}

  {%- set schema_exists = (run_query(schema_check).columns[0].values() | first) | int -%}

  {%- if schema_exists > 0 -%}
    {%- call statement('drop_schema') -%}
      drop schema {{ relation.without_identifier() }} cascade
    {%- endcall -%}
  {%- endif -%}
{%- endmacro %}

{% macro netezza__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    {% if relation.type == 'view' %}
        drop {{ relation.type }} {{ relation }}
    {% else %}
        drop {{ relation.type }} {{ relation }} if exists
    {% endif %}
  {%- endcall %}
{% endmacro %}

{% macro netezza__rename_relation(from_relation, to_relation) -%}
  {#-- Look up the actual object type from the database because the relation
       cache may have an incorrect type (e.g. incremental materialization always
       sets target_relation.type='table' even when the existing object is a view). --#}
  {% set objtype_query %}
    select objtype from {{ from_relation.database }}.._v_objects
    where objname = upper('{{ from_relation.identifier }}')
    and schema = upper('{{ from_relation.schema }}')
  {% endset %}
  {% set results = run_query(objtype_query) %}
  {% if results and results.rows | length > 0 %}
    {% set actual_type = results.rows[0][0] | lower %}
  {% else %}
    {% set actual_type = from_relation.type %}
  {% endif %}
  {% call statement('rename_relation') -%}
    alter {{ actual_type }} {{ from_relation }} rename to {{ to_relation }}
  {%- endcall %}
{% endmacro %}

{% macro netezza__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          column_name,
          data_type,
          character_maximum_length,
          numeric_precision,
          numeric_scale
      from {{ relation.information_schema('columns') }}
      where table_name ilike '{{ relation.identifier }}'
        {% if relation.schema %}
        and table_schema ilike '{{ relation.schema }}'
        {% endif %}
      order by ordinal_position
  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}

{% macro netezza__alter_relation_comment(relation, comment) %}
  {% set escaped_comment = netezza_escape_comment(comment) %}
  comment on {{ relation.type }} {{ relation }} is {{ escaped_comment }};
{% endmacro %}

{% macro netezza__alter_column_comment(relation, column_dict) %}
  {% set existing_columns = adapter.get_columns_in_relation(relation) | map(attribute="name") | list %}
  {% for column_name in column_dict if (column_name if column_dict[column_name]['quote'] else column_name | upper in existing_columns) %}
    {% set comment = column_dict[column_name]['description'] %}
    {% set escaped_comment = netezza_escape_comment(comment) %}
    comment on column {{ relation }}.{{ adapter.quote(column_name) if column_dict[column_name]['quote'] else column_name }} is {{ escaped_comment }};
  {% endfor %}
{% endmacro %}

{% macro netezza__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}
  {# Run the default ALTER TABLE ADD/DROP COLUMN statements first. #}
  {% do default__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}
  {# Netezza requires GROOM TABLE VERSIONS after DDL produces versioned rows so
     that subsequent MERGE statements can run. Use the safe adapter helper so
     we don't fail when no versioned rows exist. #}
  {% if (add_columns and add_columns | length > 0) or (remove_columns and remove_columns | length > 0) %}
    {% do adapter.groom_table_versions(relation) %}
  {% endif %}
{% endmacro %}

{% macro netezza_escape_comment(comment) -%}
  {% if comment is not string %}
    {% do exceptions.raise_compiler_error('cannot escape a non-string: ' ~ comment) %}
  {% endif %}
  '{{ comment | replace("'", "''")}}'
{%- endmacro %}

{% macro netezza__can_clone_table() %}
  {{ return(True) }}
{% endmacro %}

{% macro netezza__create_or_replace_clone(this_relation, defer_relation) %}
  {% do adapter.drop_relation(this_relation) %}
  create table {{ this_relation }} as select * from {{ defer_relation }}
{% endmacro %}
