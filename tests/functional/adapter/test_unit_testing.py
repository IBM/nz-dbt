"""
Inherited dbt-tests-adapter unit testing tests for the Netezza adapter.

These tests validate unit testing support including:
- Data type handling in unit tests
- Case insensitivity
- Invalid input handling
- Quoted reserved word column names (new in dbt-tests-adapter 1.18+)
"""
import pytest

from dbt.tests.adapter.unit_testing.test_types import BaseUnitTestingTypes
from dbt.tests.adapter.unit_testing.test_case_insensitivity import BaseUnitTestCaseInsensivity
from dbt.tests.adapter.unit_testing.test_invalid_input import BaseUnitTestInvalidInput
from dbt.tests.adapter.unit_testing.test_quoted_reserved_word_column_names import (
    BaseUnitTestQuotedReservedWordColumnNames,
)


class TestUnitTestingTypes(BaseUnitTestingTypes):
    @pytest.fixture
    def data_types(self):
        # Netezza: no TIMESTAMPTZ, no ::json cast, no ::numeric cast
        return [
            ["1", "1"],
            ["'1'", "1"],
            ["true", "true"],
            ["DATE '2020-01-02'", "2020-01-02"],
            ["TIMESTAMP '2013-11-03 00:00:00'", "2013-11-03 00:00:00"],
            ["cast('1' as numeric)", "1"],
        ]


class TestUnitTestCaseInsensitivity(BaseUnitTestCaseInsensivity):
    pass


class TestUnitTestInvalidInput(BaseUnitTestInvalidInput):
    pass


class TestUnitTestQuotedReservedWordColumnNames(BaseUnitTestQuotedReservedWordColumnNames):
    pass
