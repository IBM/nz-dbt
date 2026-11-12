"""
Inherited dbt-tests-adapter basic tests for the Netezza adapter.

These tests validate core adapter functionality:
- Simple materializations (table, view, incremental, ephemeral)
- Generic and singular tests
- Snapshot check cols and timestamp
- Connection validation
- Catalog retrieval
"""
import pytest

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import BaseSingularTestsEphemeral
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_validate_connection import BaseValidateConnection


class TestSimpleMaterializations(BaseSimpleMaterializations):
    @pytest.fixture(scope="class", autouse=True)
    def setup_policy(self, quote_policy_override):
        """Use parametrized quote policy."""
        pass


class TestEmpty(BaseEmpty):
    @pytest.fixture(scope="class", autouse=True)
    def setup_policy(self, quote_policy_override):
        """Use parametrized quote policy."""
        pass


class TestEphemeral(BaseEphemeral):
    @pytest.fixture(scope="class", autouse=True)
    def setup_policy(self, quote_policy_override):
        """Use parametrized quote policy."""
        pass


class TestGenericTests(BaseGenericTests):
    @pytest.fixture(scope="class", autouse=True)
    def setup_policy(self, quote_policy_override):
        """Use parametrized quote policy."""
        pass


class TestIncremental(BaseIncremental):
    @pytest.fixture(scope="class", autouse=True)
    def setup_policy(self, quote_policy_override):
        """Use parametrized quote policy."""
        pass


class TestSingularTests(BaseSingularTests):
    @pytest.fixture(scope="class", autouse=True)
    def setup_policy(self, quote_policy_override):
        """Use parametrized quote policy."""
        pass


class TestSingularTestsEphemeral(BaseSingularTestsEphemeral):
    @pytest.fixture(scope="class", autouse=True)
    def setup_policy(self, quote_policy_override):
        """Use parametrized quote policy."""
        pass


class TestSnapshotCheckCols(BaseSnapshotCheckCols):
    @pytest.fixture(scope="class", autouse=True)
    def setup_policy(self, quote_policy_override):
        """Use parametrized quote policy."""
        pass


class TestSnapshotTimestamp(BaseSnapshotTimestamp):
    @pytest.fixture(scope="class", autouse=True)
    def setup_policy(self, quote_policy_override):
        """Use parametrized quote policy."""
        pass


class TestValidateConnection(BaseValidateConnection):
    @pytest.fixture(scope="class", autouse=True)
    def setup_policy(self, quote_policy_override):
        """Use parametrized quote policy."""
        pass
