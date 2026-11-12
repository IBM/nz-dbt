import pytest

from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants


@pytest.mark.skip(reason="Requires DBT_TEST_USER_1/2/3 environment variables")
class TestModelGrants(BaseModelGrants):
    pass
