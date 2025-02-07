import pytest

from dbt.tests.adapter.aliases import fixtures
from dbt.tests.adapter.aliases.test_aliases import (
    BaseAliases,
    BaseAliasErrors,
    BaseSameAliasDifferentSchemas,
    BaseSameAliasDifferentDatabases,
)


class BaseAliasesNetezza:
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "cast.sql": fixtures.MACROS__CAST_SQL.replace("text", "varchar(100)"),
            "expect_value.sql": fixtures.MACROS__EXPECT_VALUE_SQL,
        }


class TestAliasesNetezza(BaseAliasesNetezza, BaseAliases):
    pass


class TestAliasErrorsNetezza(BaseAliasesNetezza, BaseAliasErrors):
    pass


@pytest.mark.skip("Adapter does not support multiple schemas.")
class TestSameAliasDifferentSchemasNetezza(
    BaseAliasesNetezza, BaseSameAliasDifferentSchemas
):
    pass


@pytest.mark.skip("Adapter does not support multiple databases.")
class TestSameAliasDifferentDatabasesNetezza(
    BaseAliasesNetezza, BaseSameAliasDifferentDatabases
):
    pass
