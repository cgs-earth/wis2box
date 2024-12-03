import os
import click
import pytest
import debugpy
from pathlib import Path
from wis2box import cli_helpers
from wis2box.oregon.xlsx.lib import parse_xlsx
import logging
LOGGER = logging.getLogger(__name__)

@click.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@cli_helpers.OPTION_VERBOSITY
@click.argument('file', type=click.Path(exists=True))
def load(ctx, verbosity, file):
    """Load and process an xlsx file, parsing and sending to Frost."""
    path = Path(file)
    parsed = parse_xlsx(path)
    sta = parsed.to_sta()
    parsed.send_to_frost(sta)
    LOGGER.info(f"Finished uploading {path} to FROST")
    click.echo("Done")

@click.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@cli_helpers.OPTION_VERBOSITY
@click.argument('pytest_args', nargs=-1, type=click.UNPROCESSED)
def test(ctx, verbosity, pytest_args):
    """Run all pytest tests associated with this module. Pass in additional arguments to pytest if needed."""
    dir_path = os.path.dirname(os.path.realpath(__file__))
    test_dir = os.path.join(dir_path, "tests")
    pytest.main([test_dir, "-vvvx", *pytest_args])

@click.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@cli_helpers.OPTION_VERBOSITY
@click.argument('pytest_args', nargs=-1, type=click.UNPROCESSED)
def test_debug(ctx, verbosity, pytest_args):
    """Run tests with debugpy for debugging. Requires an external debugger to connect to the port"""
    debugpy.listen(("0.0.0.0", 5678))
    print("Waiting for debugger attach... If you are using vscode, use the Attach Debugger configuration in this repo")
    debugpy.wait_for_client()
    print("Debugger attached.")
    dir_path = os.path.dirname(os.path.realpath(__file__))
    test_dir = os.path.join(dir_path, "tests")
    pytest.main([test_dir, "-vvvx", *pytest_args])

@click.group()
def xlsx():
    """Station metadata management via xlsx files which represent a STA data model."""
    pass

xlsx.add_command(test)
xlsx.add_command(test_debug)
xlsx.add_command(load)

if __name__ == '__main__':
    xlsx()
