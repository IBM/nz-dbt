import os
from pathlib import Path

import pytest

from dbt.tests.adapter.hooks.test_model_hooks import BaseTestPrePost
from dbt.tests.adapter.hooks.test_run_hooks import BasePrePostRunHooks
from dbt.adapters.netezza.relation import NetezzaQuotePolicy


def _format_relation(schema_expr: str, table: str) -> str:
    """Format schema.table according to active quote policy."""
    policy = NetezzaQuotePolicy()
    if policy.schema and policy.identifier:
        return f'"{schema_expr}"."{table}"'
    elif policy.schema:
        return f'"{schema_expr}".{table}'
    elif policy.identifier:
        return f'{schema_expr}."{table}"'
    return f'{schema_expr}.{table}'


class TestPrePost(BaseTestPrePost):
    @pytest.fixture(scope="class", autouse=True)
    def setup_policy(self, quote_policy_override):
        """Use parametrized quote policy."""
        pass


class TestPrePostRunHooks(BasePrePostRunHooks):
    @pytest.fixture(scope="class", autouse=True)
    def setup_policy(self, quote_policy_override):
        """Use parametrized quote policy."""
        pass
    
    @pytest.fixture(scope="function", autouse=True)
    def cleanup_after_test(self, project):
        """Cleanup database schema after each parametrized test.
        
        NOTE: Quoted and unquoted policies create DIFFERENT schemas in Netezza:
        - Quoted: "test_schema" (stored as lowercase)
        - Unquoted: TEST_SCHEMA (stored as uppercase)
        
        We clean both to prevent relation cache conflicts.
        """
        # Only cleanup AFTER test to avoid interfering with setUp
        yield
        
        # Cleanup after test to prevent cache conflicts for next parametrized run
        for schema in [f'"{project.test_schema}"', project.test_schema]:
            try:
                project.run_sql(f'drop schema {schema} CASCADE')
            except:
                pass

    @pytest.fixture(scope="function")
    def setUp(self, project):
        # Import policy dynamically to get monkeypatched version
        import dbt.adapters.netezza.relation
        policy = dbt.adapters.netezza.relation.NetezzaQuotePolicy()
        schema_fmt = f'"{project.test_schema}"' if policy.schema else project.test_schema
        ident_fmt = lambda t: f'"{t}"' if policy.identifier else t
        
        # Create schema if it doesn't exist (needed after cleanup drops it)
        try:
            project.run_sql(f'create schema {schema_fmt}')
        except Exception as e:
            # Ignore if schema already exists
            if "already exists" not in str(e) and "duplicate" not in str(e).lower():
                raise
        
        # Run seed SQL (creates on_run_hook table)
        project.run_sql_file(Path(project.test_data_dir) / "seed_run.sql")
        
        # Drop tables (ignore errors if they don't exist)
        try:
            project.run_sql(f'drop table {schema_fmt}.{ident_fmt("schemas")} if exists')
        except:
            pass
        try:
            project.run_sql(f'drop table {schema_fmt}.{ident_fmt("db_schemas")} if exists')
        except:
            pass
        os.environ["TERM_TEST"] = "TESTING"

    @pytest.fixture(scope="class")
    def macros(self):
        # Override base class macro with Jinja-time policy-aware formatting
        return {
            "before-and-after.sql": """
{% macro format_test_relation(schema_expr, table_name) %}
    {%- set rel = api.Relation.create(database=target.database, schema=schema_expr, identifier=table_name) -%}
    {{ rel.render() }}
{%- endmacro %}

{% macro custom_run_hook(state, target, run_started_at, invocation_id) %}

   insert into {{ format_test_relation(target.schema, "on_run_hook") }} (
        test_state,
        target_dbname,
        target_host,
        target_name,
        target_schema,
        target_type,
        target_user,
        target_pass,
        target_threads,
        run_started_at,
        invocation_id,
        thread_id
   ) VALUES (
    '{{ state }}',
    '{{ target.dbname }}',
    '{{ target.host }}',
    '{{ target.name }}',
    '{{ target.schema }}',
    '{{ target.type }}',
    '{{ target.user }}',
    '{{ target.get("pass", "") }}',
    {{ target.threads }},
    '{{ run_started_at }}',
    '{{ invocation_id }}',
    '{{ thread_id }}'
   )

{% endmacro %}
"""
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # Use Jinja-time policy-aware formatting for all SQL statements
        return {
            "on-run-start": [
                "{{ custom_run_hook('start', target, run_started_at, invocation_id) }}",
                'create table {{ format_test_relation(target.schema, "start_hook_order_test") }} ( id int )',
                'drop table {{ format_test_relation(target.schema, "start_hook_order_test") }}',
                "{{ log(env_var('TERM_TEST'), info=True) }}",
            ],
            "on-run-end": [
                "{{ custom_run_hook('end', target, run_started_at, invocation_id) }}",
                'create table {{ format_test_relation(target.schema, "end_hook_order_test") }} ( id int )',
                'drop table {{ format_test_relation(target.schema, "end_hook_order_test") }}',
                'create table {{ format_test_relation(target.schema, "schemas") }} ( schema varchar(256) )',
                'insert into {{ format_test_relation(target.schema, "schemas") }} (schema) values {% for schema in schemas %}( \'{{ schema }}\' ){% if not loop.last %},{% endif %}{% endfor %}',
                'create table {{ format_test_relation(target.schema, "db_schemas") }} ( db varchar(256), schema varchar(256) )',
                'insert into {{ format_test_relation(target.schema, "db_schemas") }} (db, schema) values {% for db, schema in database_schemas %}(\'{{ db }}\', \'{{ schema }}\' ){% if not loop.last %},{% endif %}{% endfor %}',
            ],
            "seeds": {
                "quote_columns": False,
            },
        }

    def get_ctx_vars(self, state, project):
        fields = [
            "test_state",
            "target_dbname",
            "target_host",
            "target_name",
            "target_schema",
            "target_threads",
            "target_type",
            "target_user",
            "target_pass",
            "run_started_at",
            "invocation_id",
            "thread_id",
        ]
        field_list = ", ".join(fields)
        
        # Dynamically format based on current policy
        import dbt.adapters.netezza.relation
        policy = dbt.adapters.netezza.relation.NetezzaQuotePolicy()
        schema_fmt = f'"{project.test_schema}"' if policy.schema else project.test_schema
        ident_fmt = lambda t: f'"{t}"' if policy.identifier else t
        on_run_hook_table = f'{schema_fmt}.{ident_fmt("on_run_hook")}'
        
        query = f'select {field_list} from {on_run_hook_table} where test_state = \'{state}\''

        vals = project.run_sql(query, fetch="all")
        assert len(vals) != 0, "nothing inserted into on_run_hook table"
        assert len(vals) == 1, "too many rows in hooks table"
        ctx = dict([(k, v) for (k, v) in zip(fields, vals[0])])

        return ctx

    def check_hooks(self, state, project, host):
        ctx = self.get_ctx_vars(state, project)

        assert ctx["test_state"] == state
        assert ctx["target_dbname"] == project.database
        assert ctx["target_host"] == host
        assert ctx["target_name"] == "default"
        assert ctx["target_schema"] == project.test_schema
        assert ctx["target_threads"] == 4
        assert ctx["target_type"] == "netezza"
        assert ctx["target_pass"] == ""

    def assert_used_schemas(self, project):
        # Dynamically format based on current policy
        import dbt.adapters.netezza.relation
        policy = dbt.adapters.netezza.relation.NetezzaQuotePolicy()
        schema_fmt = f'"{project.test_schema}"' if policy.schema else project.test_schema
        ident_fmt = lambda t: f'"{t}"' if policy.identifier else t
        
        schemas_table = f'{schema_fmt}.{ident_fmt("schemas")}'
        db_schemas_table = f'{schema_fmt}.{ident_fmt("db_schemas")}'
        
        schemas_query = f'select * from {schemas_table}'
        results = project.run_sql(schemas_query, fetch="all")
        assert len(results) == 1
        assert results[0][0] == project.test_schema

        db_schemas_query = f'select * from {db_schemas_table}'
        results = project.run_sql(db_schemas_query, fetch="all")
        assert len(results) == 1
        assert results[0][0] == project.database
        assert results[0][1] == project.test_schema
