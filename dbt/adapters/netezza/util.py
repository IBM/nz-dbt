import os
import yaml
from typing import Callable, Dict, List, Optional
import warnings

from dbt.cli.main import dbtRunner
from dbt_common.events.functions import reset_metadata_vars
from dbt_common.events.base_types import EventLevel, EventMsg

from typing import Any, Callable, Dict, List, Optional
from io import StringIO
from dbt_common.events.functions import (
    capture_stdout_logs,
    fire_event,
    reset_metadata_vars,
    stop_capture_stdout_logs,
)


class ETOptions:
    def __init__(self, SkipRows, Delimiter, DateDelim, MaxErrors, BoolStyle):
        self.SkipRows = SkipRows
        self.Delimiter = Delimiter
        self.DateDelim = DateDelim
        self.MaxErrors = MaxErrors
        self.BoolStyle = BoolStyle

def etoptions_representer(dumper, data):
    return dumper.represent_mapping('!ETOptions', {
        'SkipRows': data.SkipRows,
        'Delimiter': data.Delimiter,
        'DateDelim': data.DateDelim,
        'MaxErrors': data.MaxErrors,
        'BoolStyle': data.BoolStyle
    })

def create_et_options(project_path):
    yaml.add_representer(ETOptions, etoptions_representer)
    et_options = ETOptions('0', "','", "'-'", ' 0 ', ' TRUE_FALSE ')
    with open(f"{project_path}/et_options.yml", "w") as file:
        yaml.dump([et_options], file, default_flow_style=False)
    print("YAML file generated successfully.")

def update_seed_file_names(seeds_path):
    if os.path.exists(seeds_path):
        for i in os.listdir(seeds_path):
            print(f'The name of the seed file is : {i}')
            if i[-4:] == '.csv':
                new_name = i.split('.csv')[0].upper() + '.csv'
                print(f'updeting {seeds_path}/{i} to : {seeds_path}/{new_name}')
                os.rename(seeds_path + '/' + i,seeds_path + '/' + new_name)
    else:
        pass



# 'run_dbt' is used in pytest tests to run dbt commands. It will return
# different objects depending on the command that is executed.
# For a run command (and most other commands) it will return a list
# of results. For the 'docs generate' command it returns a CatalogArtifact.
# The first parameter is a list of dbt command line arguments, such as
#   run_dbt(["run", "--vars", "seed_name: base"])
# If the command is expected to fail, pass in "expect_pass=False"):
#   run_dbt("test"], expect_pass=False)
def run_dbt(args: List[str] = None, expect_pass=True, callbacks: Optional[List[Callable[[EventMsg], None]]] = None,):
    # Ignore logbook warnings
    # warnings.filterwarnings("ignore", category=DeprecationWarning, module="logbook")
    # warnings.filterwarnings("ignore", category=NotOpenSSLWarning)
    # reset global vars
    reset_metadata_vars()

    # The logger will complain about already being initialized if
    # we don't do this.
    # log_manager.reset_handlers()
    if args is None:
        args = ["run"]
    print("Caleed from the Netezza directory!!")
    print("\n\nInvoking dbt with {}".format(args))
    from dbt.flags import get_flags

    flags = get_flags()
    project_dir = getattr(flags, "PROJECT_DIR", None)
    profiles_dir = getattr(flags, "PROFILES_DIR", None)
    print(f"The project_dir is : {project_dir}")
    print(f"The list of directory is : {os.listdir(project_dir)}")
    print(f"The profiles_dir is : {profiles_dir}")
    create_et_options(project_dir)
    update_seed_file_names(project_dir + '/seeds')
    print(f"The list of directory afte yaml generation : {os.listdir(project_dir)}")

    # print(f"The list of seeds directory afte update : {os.listdir(project_dir + '/seeds')}")
    if project_dir and "--project-dir" not in args:
        args.extend(["--project-dir", project_dir])
    if profiles_dir and "--profiles-dir" not in args:
        args.extend(["--profiles-dir", profiles_dir])
    print(f"the value of callbacks : {callbacks}")
    print(f"the value of args  : {args}")
    dbt = dbtRunner(callbacks=callbacks)

    print(f'The args for dbt.invoke(args) : {args}')
    res = dbt.invoke(args)

    print(f'the value of res = {res} and the value of res.exp : {res.exception}')

    # the exception is immediately raised to be caught in tests
    # using a pattern like `with pytest.raises(SomeException):`
    if res.exception is not None:
        print(f"the exception is : {res.exception}")
        raise res.exception

    if expect_pass is not None:
        print(f"The value res.success is : {res.success}")
        print(f"The value expect_pass is : {expect_pass}")

        assert res.success == expect_pass, "dbt exit state did not match expected"
    print(f"The cvalue of tge result is res.result : {res.result}")
    return res.result

# from typing import Callable, Dict, Optional, TextIO, Union
# CAPTURE_STREAM: Optional[TextIO] = None
# # used for integration tests
# def capture_stdout_logs(stream: TextIO) -> None:
#     global CAPTURE_STREAM
#     CAPTURE_STREAM = stream

# Use this if you need to capture the command logs in a test.
# If you want the logs that are normally written to a file, you must
# start with the "--debug" flag. The structured schema log CI test
# will turn the logs into json, so you have to be prepared for that.
def run_dbt_and_capture(
    args: Optional[List[str]] = None,
    expect_pass: bool = True,
):
    try:
        print(f"inside the run_dbt_and_capture and the value of args : {args} and expect_pass : {expect_pass}")

        stringbuf = StringIO()
        print(f"here inside stringbuf : {stringbuf}")
        capture_stdout_logs(stringbuf)
        print(f"here after capture_stdout_logs : ")
        res = run_dbt(args, expect_pass=expect_pass)
        print(f"here after res = run_dbt( : {res}")
        stdout = stringbuf.getvalue()
        print(f"the value of stdout is : {stdout}")

    finally:
        stop_capture_stdout_logs()

    return res, stdout
