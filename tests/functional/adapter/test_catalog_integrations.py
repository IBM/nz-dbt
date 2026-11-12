"""
New in dbt-tests-adapter 1.16+ (dbt-core 1.10+):
Tests catalog integration validation.
"""
from dbt.tests.adapter.catalog_integrations.test_catalog_integration import (
    BaseCatalogIntegrationValidation,
)


class TestCatalogIntegrationValidation(BaseCatalogIntegrationValidation):
    pass
