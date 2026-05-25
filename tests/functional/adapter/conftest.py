import os
import platform
import tempfile

import pytest
import yaml

from dbt.adapters.netezza.et_options_parser import ETOptions, etoptions_representer
from dbt.adapters.netezza.relation import NetezzaQuotePolicy


# Parametrize quote policy for all tests to test both quoted and unquoted strategies
@pytest.fixture(scope="class", params=[
    {"schema": True, "identifier": True, "database": True},
    {"schema": False, "identifier": False, "database": True},
], ids=["quoted", "unquoted"])
def quote_policy_override(request):
    """Override NetezzaQuotePolicy to test both quoted and unquoted strategies.
    
    This fixture parametrizes all tests to run twice:
    - Once with quoted identifiers (schema=True, identifier=True)
    - Once with unquoted identifiers (schema=False, identifier=False)
    """
    policy_config = request.param
    
    # Create a custom policy class with the test parameters
    from dataclasses import dataclass
    from dbt.adapters.base.relation import Policy
    
    @dataclass
    class TestNetezzaQuotePolicy(Policy):
        database: bool = policy_config["database"]
        schema: bool = policy_config["schema"]
        identifier: bool = policy_config["identifier"]
    
    # Directly replace the class in the module (works with class scope)
    import dbt.adapters.netezza.relation
    original_policy = dbt.adapters.netezza.relation.NetezzaQuotePolicy
    dbt.adapters.netezza.relation.NetezzaQuotePolicy = TestNetezzaQuotePolicy
    
    yield policy_config
    
    # Restore original policy after test
    dbt.adapters.netezza.relation.NetezzaQuotePolicy = original_policy


@pytest.fixture(autouse=True, scope="class")
def create_et_options_file(project):
    """Auto-generate et_options.yml with proper options for every adapter test.

    The inherited dbt-tests-adapter base classes use run_dbt() directly,
    which doesn't call create_et_options(). Netezza seeds require
    et_options.yml to exist, and the default seed CSVs use ISO 8601
    datetime format (with 'T' separator) that needs explicit options.
    """
    yaml.add_representer(ETOptions, etoptions_representer)
    options = {
        "SkipRows": "1",
        "Delimiter": "','",
        "DateDelim": "'-'",
        "DateStyle": "YMD",
        "TimeStyle": "24HOUR",
        "TimeDelim": "':'",
        "DateTimeDelim": "'T'",
        "BoolStyle": "TRUE_FALSE",
        "QuotedValue": "Double",
        "MaxErrors": "1",
    }
    if platform.system() == "Windows":
        logdir = os.path.join(tempfile.gettempdir(), "DBT")
        os.makedirs(logdir, exist_ok=True)
        options["LogDir"] = f"'{logdir}'"
    et_options = ETOptions(options=options)
    with open(os.path.join(project.project_root, "et_options.yml"), "w") as f:
        yaml.dump([et_options], f, default_flow_style=False)


@pytest.fixture(autouse=True, scope="function")
def patch_run_sql_for_quoting(project, monkeypatch):
    """Patch project.run_sql to quote schema and identifier names if quote policy is enabled.
    
    This fixture is policy-aware:
    - If NetezzaQuotePolicy has schema=True or identifier=True: applies quoting patches
    - If NetezzaQuotePolicy has schema=False and identifier=False: no patching (unquoted mode)
    
    With identifier quoting enabled, relations are created as "schema_name"."table_name" (quoted, lowercase).
    But raw test SQL may use unquoted names, causing Netezza to uppercase them, resulting in 
    "Schema/relation does not exist" errors.
    
    This fixture wraps run_sql to:
    1. Replace {schema} placeholder with "{schema}" before substitution
    2. Quote actual schema names that have already been substituted
    3. Quote identifiers that follow a quoted schema (e.g., "schema".table -> "schema"."table")
    """
    import re
    from dbt.tests.util import run_sql_with_adapter
    
    original_run_sql = project.run_sql
    
    def quoted_run_sql(sql, fetch="None"):
        # Check the active quote policy DYNAMICALLY on each call
        # Import from module to get the potentially monkeypatched class
        import dbt.adapters.netezza.relation
        quote_policy = dbt.adapters.netezza.relation.NetezzaQuotePolicy()
        schema_quoting = quote_policy.schema
        identifier_quoting = quote_policy.identifier
        
        # If both schema and identifier quoting are disabled, no patching needed
        if not schema_quoting and not identifier_quoting:
            return original_run_sql(sql, fetch)
        
        if schema_quoting:
            # Add quotes around {schema} placeholder if not already quoted
            # Match {schema} that is NOT preceded by " and NOT followed by "
            sql = re.sub(r'(?<!")(\{schema\})(?!")', r'"\1"', sql)
            
            # Also quote the actual schema name if it appears unquoted in the SQL
            # This handles cases where f-strings have already substituted the schema name
            schema_name = project.test_schema
            # Match schema_name that is NOT preceded by " and NOT followed by "
            # Use word boundaries to avoid partial matches
            if schema_name:
                pattern = r'(?<!")\b(' + re.escape(schema_name) + r')\b(?!")'
                sql = re.sub(pattern, r'"\1"', sql)
        
        if identifier_quoting:
            # Quote identifiers that follow a quoted schema: "schema".identifier -> "schema"."identifier"
            # Match: "anything".(word characters not already quoted)
            sql = re.sub(r'"([^"]+)"\.(?!")([a-zA-Z_][a-zA-Z0-9_]*)\b', r'"\1"."\2"', sql)
        
        return original_run_sql(sql, fetch)
    
    monkeypatch.setattr(project, "run_sql", quoted_run_sql)


@pytest.fixture(scope="class")
def project_root(tmpdir_factory, quote_policy_override):
    """Override project_root so each quote_policy_override variant gets its own
    temp directory.  Without this, both [quoted] and [unquoted] share project0
    and project_files errors with FileExistsError when it tries to create the
    'models' subdirectory a second time."""
    return tmpdir_factory.mktemp("project")


@pytest.fixture(scope="class")
def unique_schema(request, prefix, quote_policy_override) -> str:
    """Override unique_schema to include a quoting-mode suffix so each
    quote_policy_override variant operates in its own Netezza schema namespace.

    Both [quoted] and [unquoted] use the same base schema name by default.
    Custom schemas created by models (e.g. _schema_a/_schema_b) are not
    tracked in project.created_schemas and therefore not dropped at teardown.
    Whichever variant runs first leaves behind schemas; when the second variant
    runs, create_schema's case-insensitive lower/lower check finds them and
    skips recreation, then the DDL fails because the casing doesn't match.

    Adding _q / _u suffix gives each variant its own namespace.
    """
    test_file = request.module.__name__.split(".")[-1]
    suffix = "q" if quote_policy_override.get("schema", True) else "u"
    return f"{prefix}_{test_file}_{suffix}"


@pytest.fixture(scope="class")
def project_config_update(quote_policy_override):
    """Propagate the quote policy into dbt_project.yml so that adapter.config.quoting
    matches the parameterized test policy.  Without this, adapter.config.quoting is
    always the adapter default (all True), meaning macros always create schemas quoted
    (lowercase) regardless of the test parameter."""
    return {
        "quoting": {
            "database": quote_policy_override["database"],
            "schema": quote_policy_override["schema"],
            "identifier": quote_policy_override["identifier"],
        }
    }
