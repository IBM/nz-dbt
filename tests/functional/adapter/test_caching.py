import pytest
from dbt.tests.adapter.caching.test_caching import (
    BaseCachingTest,
    BaseCachingLowercaseModel,
    BaseCachingUppercaseModel,
    BaseCachingSelectedSchemaOnly,
    TestNoPopulateCache as BaseNoPopulateCache,
)
from dbt.adapters.netezza.util import run_dbt

# Updating the BaseCachingTest class to be used in all caching tests for Netezza
class NetezzaBaseCachingTest(BaseCachingTest):
    def run_and_inspect_cache(self, project, run_args=None):
        print(f"The run args sent are : {run_args}")
        run_dbt(run_args)

        # the cache was empty at the start of the run.
        # the model materialization returned an unquoted relation and added to the cache.
        adapter = project.adapter
        print(f"The adapter.cache.relations presenr are : {adapter.cache.relations}")
        assert len(adapter.cache.relations) == 1
        relation = list(adapter.cache.relations).pop()
        # assert relation.schema == project.test_schema
        assert relation.schema == project.test_schema.lower()

        # on the second run, dbt will find a relation in the database during cache population.
        # this relation will be quoted, because list_relations_without_caching (by default) uses
        # quote_policy = {"database": True, "schema": True, "identifier": True}
        # when adding relations to the cache.
        run_dbt(run_args)
        adapter = project.adapter
        assert len(adapter.cache.relations) == 1
        second_relation = list(adapter.cache.relations).pop()

        # perform a case-insensitive + quote-insensitive comparison
        for key in ["database", "schema", "identifier"]:
            assert getattr(relation, key).lower() == getattr(second_relation, key).lower()


class NetezzaBaseNoPopulateCache(BaseNoPopulateCache, NetezzaBaseCachingTest):
    pass

class TestNoPopulateCacheNetezza(NetezzaBaseNoPopulateCache):
    pass

class NetezzaBaseCachingLowercaseModel(BaseCachingLowercaseModel, NetezzaBaseCachingTest):
    pass

class TestCachingLowerCaseModelNetezza(NetezzaBaseCachingLowercaseModel):
    pass

class NetezzaBaseCachingUppercaseModel(BaseCachingUppercaseModel, NetezzaBaseCachingTest):
    pass

class TestCachingUppercaseModelNetezza(NetezzaBaseCachingUppercaseModel):
    pass

class NetezzaBaseCachingSelectedSchemaOnly(BaseCachingSelectedSchemaOnly, NetezzaBaseCachingTest):
    pass

class TestCachingSelectedSchemaOnlyNetezza(NetezzaBaseCachingSelectedSchemaOnly):
    pass
