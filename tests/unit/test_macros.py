"""Unit tests for the `dist`/`organize` Jinja macros (no live Netezza connection needed)."""
import re
from pathlib import Path

import jinja2
import pytest

MACRO_FILE = Path(__file__).resolve().parents[2] / "dbt" / "include" / "netezza" / "macros" / "adapters.sql"


def _normalize(s):
    return re.sub(r"\s+", "", s).lower()


@pytest.fixture(scope="module")
def macro_module():
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(str(MACRO_FILE.parent)),
        extensions=["jinja2.ext.do"],
    )
    return env.get_template(MACRO_FILE.name).module


@pytest.fixture(scope="module")
def organize_macro(macro_module):
    return macro_module.organize


@pytest.fixture(scope="module")
def dist_macro(macro_module):
    return macro_module.dist


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, ""),
        ("random", "distribute on random"),
        ("col1", "distribute on (col1)"),
        (["col1", "col2"], "distribute on (col1,col2)"),
    ],
)
def test_dist_macro(dist_macro, value, expected):
    assert _normalize(dist_macro(value)) == _normalize(expected)


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, ""),
        ("col1", "organize on (col1)"),
        (["col1", "col2"], "organize on (col1,col2)"),
        ("none", "organize on none"),
        ("NONE", "organize on none"),
    ],
)
def test_organize_macro(organize_macro, value, expected):
    assert _normalize(organize_macro(value)) == _normalize(expected)
