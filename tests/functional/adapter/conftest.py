import os
import platform
import tempfile

import pytest
import yaml

from dbt.adapters.netezza.et_options_parser import ETOptions, etoptions_representer


@pytest.fixture(autouse=True, scope="class")
def create_et_options_file(project):
    """Auto-generate et_options.yml with proper options for every adapter test.

    The inherited dbt-tests-adapter base classes use run_dbt() directly,
    which doesn't call create_et_options(). Netezza seeds require
    et_options.yml to exist, and the default seed CSVs use ISO 8601
    datetime format (with 'T' separator) that needs explicit options.
    """
    yaml.add_representer(ETOptions, etoptions_representer)
    options = {
        "SkipRows": "1",
        "Delimiter": "','",
        "DateDelim": "'-'",
        "DateStyle": "YMD",
        "TimeStyle": "24HOUR",
        "TimeDelim": "':'",
        "DateTimeDelim": "'T'",
        "BoolStyle": "TRUE_FALSE",
        "QuotedValue": "Double",
        "MaxErrors": "1",
    }
    if platform.system() == "Windows":
        logdir = os.path.join(tempfile.gettempdir(), "DBT")
        os.makedirs(logdir, exist_ok=True)
        options["LogDir"] = f"'{logdir}'"
    et_options = ETOptions(options=options)
    with open(os.path.join(project.project_root, "et_options.yml"), "w") as f:
        yaml.dump([et_options], f, default_flow_style=False)
