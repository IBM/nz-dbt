from dbt.tests.adapter.dbt_show.test_dbt_show import (
    BaseShowLimit,
    BaseShowSqlHeader,
    BaseShowDoesNotHandleDoubleLimit,
)


class TestShowLimit(BaseShowLimit):
    pass


class TestShowSqlHeader(BaseShowSqlHeader):
    pass


class TestShowDoesNotHandleDoubleLimit(BaseShowDoesNotHandleDoubleLimit):
    DATABASE_ERROR_MESSAGE = 'found "LIMIT"'
