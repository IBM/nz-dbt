from dbt.tests.adapter.caching.test_caching import (
    BaseCachingLowercaseModel,
    BaseCachingUppercaseModel,
    BaseCachingSelectedSchemaOnly,
    BaseNoPopulateCache,
)
import pytest


class TestCachingLowercaseModel(BaseCachingLowercaseModel):
    @pytest.fixture(scope="class", autouse=True)
    def setup_policy(self, quote_policy_override):
        """Use parametrized quote policy."""
        pass
    
    @pytest.fixture(scope="class")
    def project_config_update(self):
        # Import policy dynamically to get monkeypatched version
        import dbt.adapters.netezza.relation
        policy = dbt.adapters.netezza.relation.NetezzaQuotePolicy()
        return {
            "config-version": 2,
            "quoting": {
                "identifier": policy.identifier,
                "schema": policy.schema,
                "database": policy.database,
            },
        }


class TestCachingUppercaseModel(BaseCachingUppercaseModel):
    @pytest.fixture(scope="class", autouse=True)
    def setup_policy(self, quote_policy_override):
        """Use parametrized quote policy."""
        pass
    
    @pytest.fixture(scope="class")
    def project_config_update(self):
        # Import policy dynamically to get monkeypatched version
        import dbt.adapters.netezza.relation
        policy = dbt.adapters.netezza.relation.NetezzaQuotePolicy()
        return {
            "config-version": 2,
            "quoting": {
                "identifier": policy.identifier,
                "schema": policy.schema,
                "database": policy.database,
            },
        }


class TestCachingSelectedSchemaOnly(BaseCachingSelectedSchemaOnly):
    @pytest.fixture(scope="class", autouse=True)
    def setup_policy(self, quote_policy_override):
        """Use parametrized quote policy."""
        pass
    
    @pytest.fixture(scope="class")
    def project_config_update(self):
        # Import policy dynamically to get monkeypatched version
        import dbt.adapters.netezza.relation
        policy = dbt.adapters.netezza.relation.NetezzaQuotePolicy()
        return {
            "config-version": 2,
            "quoting": {
                "identifier": policy.identifier,
                "schema": policy.schema,
                "database": policy.database,
            },
        }


class TestNoPopulateCache(BaseNoPopulateCache):
    @pytest.fixture(scope="class", autouse=True)
    def setup_policy(self, quote_policy_override):
        """Use parametrized quote policy."""
        pass
    
    @pytest.fixture(scope="class")
    def project_config_update(self):
        # Import policy dynamically to get monkeypatched version
        import dbt.adapters.netezza.relation
        policy = dbt.adapters.netezza.relation.NetezzaQuotePolicy()
        return {
            "config-version": 2,
            "quoting": {
                "identifier": policy.identifier,
                "schema": policy.schema,
                "database": policy.database,
            },
        }
