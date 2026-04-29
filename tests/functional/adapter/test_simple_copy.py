import os

import pytest

from dbt.tests.adapter.simple_copy.test_simple_copy import SimpleCopyBase, EmptyModelsArentRunBase
from dbt.tests.adapter.simple_copy.test_copy_uppercase import BaseSimpleCopyUppercase


class TestSimpleCopy(SimpleCopyBase):
    pass


class TestEmptyModelsArentRun(EmptyModelsArentRunBase):
    pass


class TestSimpleCopyUppercase(BaseSimpleCopyUppercase):
    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return {
            "type": "netezza",
            "host": os.getenv("NZ_TEST_HOST", "hostname"),
            "port": int(os.getenv("NZ_TEST_PORT", 5480)),
            "user": os.getenv("NZ_TEST_USER", "ADMIN"),
            "pass": os.getenv("NZ_TEST_PASS", "password"),
            "dbname": os.getenv("NZ_TEST_DATABASE", "TESTDBTINTEGRATION"),
            "threads": int(os.getenv("NZ_TEST_THREADS", 4)),
        }
