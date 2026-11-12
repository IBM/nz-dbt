import pytest
from dbt.tests.adapter.aliases import fixtures
from dbt.tests.adapter.aliases.test_aliases import (
    BaseAliases,
    BaseAliasErrors,
    BaseSameAliasDifferentSchemas,
)

# Netezza doesn't support the 'text' type or '::text' cast syntax.
MACROS__CAST_SQL = """
{% macro string_literal(s) -%}
  {{ adapter.dispatch('string_literal', macro_namespace='test')(s) }}
{%- endmacro %}

{% macro default__string_literal(s) %}
    cast('{{ s }}' as varchar(256))
{% endmacro %}
"""


class TestAliases(BaseAliases):
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "cast.sql": MACROS__CAST_SQL,
            "expect_value.sql": fixtures.MACROS__EXPECT_VALUE_SQL,
        }


class TestAliasErrors(BaseAliasErrors):
    pass


class TestSameAliasDifferentSchemas(BaseSameAliasDifferentSchemas):
    pass
