from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChangeSetup,
)
from dbt.tests.adapter.incremental.test_incremental_merge_exclude_columns import (
    BaseMergeExcludeColumns,
)


class TestIncrementalOnSchemaChange(BaseIncrementalOnSchemaChangeSetup):
    pass

class TestMergeExcludeColumns(BaseMergeExcludeColumns):
    pass
