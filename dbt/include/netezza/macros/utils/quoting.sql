{#
  Quoting-aware equality predicate helpers.

  Each macro emits the correct SQL WHERE fragment for an identifier component
  based on the project quoting configuration:

    quoting.<component> = true  (quoted)   ->  column = 'value'
    quoting.<component> = false (unquoted) ->  lower(column) = lower('value')

  Netezza stores unquoted identifiers as UPPERCASE in its system catalogs,
  so case-insensitive comparison is required when quoting is disabled.
  Quoted identifiers are stored exactly as written, so an exact (case-sensitive)
  match is both correct and necessary.

  Surrounding double-quotes are stripped from value before embedding in SQL so
  callers can pass relation.schema / relation.identifier directly without
  needing to pre-clean the value.

  Usage (WHERE predicates):
    where {{ netezza_schema_match('schema', relation.schema) }}
    and   {{ netezza_identifier_match('tablename', relation.identifier) }}

  Usage (FROM clause database prefix):
    from {{ netezza_database_ref(relation.database) }}.._v_table
#}

{% macro netezza_database_match(column, value) -%}
  {%- set clean = value | replace('"', '') | replace("'", "''") -%}
  {%- if adapter.config.quoting.get('database', true) is not false -%}
    {{ column }} = '{{ clean }}'
  {%- else -%}
    lower({{ column }}) = lower('{{ clean }}')
  {%- endif -%}
{%- endmacro %}

{% macro netezza_schema_match(column, value) -%}
  {%- set clean = value | replace('"', '') | replace("'", "''") -%}
  {%- if adapter.config.quoting.get('schema', true) is not false -%}
    {{ column }} = '{{ clean }}'
  {%- else -%}
    lower({{ column }}) = lower('{{ clean }}')
  {%- endif -%}
{%- endmacro %}

{% macro netezza_identifier_match(column, value) -%}
  {%- set clean = value | replace('"', '') | replace("'", "''") -%}
  {%- if adapter.config.quoting.get('identifier', true) is not false -%}
    {{ column }} = '{{ clean }}'
  {%- else -%}
    lower({{ column }}) = lower('{{ clean }}')
  {%- endif -%}
{%- endmacro %}

{% macro netezza_database_ref(value) -%}
  {%- set clean = value | replace('"', '') -%}
  {%- if adapter.config.quoting.get('database', true) is not false -%}
    "{{ clean }}"
  {%- else -%}
    {{ clean }}
  {%- endif -%}
{%- endmacro %}

{% macro netezza_schema_ref(value) -%}
  {%- set clean = value | replace('"', '') -%}
  {%- if adapter.config.quoting.get('schema', true) is not false -%}
    "{{ clean }}"
  {%- else -%}
    {{ clean }}
  {%- endif -%}
{%- endmacro %}

{% macro netezza_identifier_ref(value) -%}
  {%- set clean = value | replace('"', '') -%}
  {%- if adapter.config.quoting.get('identifier', true) is not false -%}
    "{{ clean }}"
  {%- else -%}
    {{ clean }}
  {%- endif -%}
{%- endmacro %}
