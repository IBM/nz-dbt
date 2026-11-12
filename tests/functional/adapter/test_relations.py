import pytest

from dbt.tests.adapter.relations.test_changing_relation_type import (
    BaseChangeRelationTypeValidator,
)


class TestChangeRelationType(BaseChangeRelationTypeValidator):
    @pytest.fixture(scope="class", autouse=True)
    def setup_policy(self, quote_policy_override):
        """Use parametrized quote policy."""
        pass
