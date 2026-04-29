import pytest
from dbt.tests.adapter.constraints.test_constraints import BaseTableConstraintsColumnsEqual
from dbt.tests.adapter.constraints import fixtures


# Override schema to replace 'text' with 'varchar(256)' for Netezza
NETEZZA_MODEL_SCHEMA_YML = fixtures.model_schema_yml.replace("data_type: text", "data_type: varchar(256)")


class TestConstraintsColumnsEqual(BaseTableConstraintsColumnsEqual):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": fixtures.my_model_wrong_order_sql,
            "my_model_wrong_name.sql": fixtures.my_model_wrong_name_sql,
            "constraints_schema.yml": NETEZZA_MODEL_SCHEMA_YML,
        }

    @pytest.fixture
    def string_type(self):
        return "CHARACTER VARYING"

    @pytest.fixture
    def schema_string_type(self):
        # Must include size for valid Netezza DDL
        return "CHARACTER VARYING(256)"

    @pytest.fixture
    def int_type(self):
        return "INTEGER"

    @pytest.fixture
    def data_types(self, schema_int_type, int_type, string_type):
        # Netezza-compatible types only (no arrays, json, timestamptz, bool)
        # Netezza OID mapping reports: INTEGER, CHARACTER VARYING, TIMESTAMP, NUMERIC
        # schema_data_type must be valid DDL (CHARACTER VARYING needs a size)
        # error_data_type is what appears in the dbt error log (from OID mapping)
        return [
            # [sql_value, schema_data_type, error_data_type]
            ["1", schema_int_type, int_type],
            ["'1'", "CHARACTER VARYING(256)", string_type],
            ["'2013-11-03 00:00:00'::timestamp", "timestamp", "TIMESTAMP"],
            ["'1'::numeric", "numeric", "NUMERIC"],
        ]
