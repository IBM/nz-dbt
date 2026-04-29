import pytest

from dbt.tests.adapter.empty.test_empty import (
    BaseTestEmpty,
    BaseTestEmptyInlineSourceRef,
)
from dbt.tests.adapter.empty._models import schema_sources_yml


class TestEmpty(BaseTestEmpty):
    pass


# The base model uses "as raw_source" inline alias, but with require_alias=True
# the adapter already appends an alias to subqueries in --empty mode, causing a
# double-alias syntax error on Netezza.  Override the model to drop the inline alias.
model_inline_no_alias_sql = """
select * from {{ source('seed_sources', 'raw_source') }}
"""


class TestEmptyInlineSourceRef(BaseTestEmptyInlineSourceRef):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": model_inline_no_alias_sql,
            "sources.yml": schema_sources_yml,
        }
