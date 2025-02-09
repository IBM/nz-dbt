import os

import pytest

from tests.functional.projects import dbt_integration


@pytest.fixture(scope="class")
def dbt_integration_project():
    return dbt_integration()


@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        "type": "netezza",
        "host": os.getenv("NZ_TEST_HOST", "hostname"),
        "port": int(os.getenv("NZ_TEST_PORT", 5480)),
        "user": os.getenv("NZ_TEST_USER", "ADMIN"),
        "pass": os.getenv("NZ_TEST_PASS", "password"),
        "dbname": os.getenv("NZ_TEST_DATABASE", "TESTDBTINTEGRATION"),
        "threads": int(os.getenv("NZ_TEST_THREADS", 4)),
    }
