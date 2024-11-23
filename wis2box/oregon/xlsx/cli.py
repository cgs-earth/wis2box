import os
import click
import pytest
import debugpy

from wis2box import cli_helpers

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
    """Run tests with debugpy for debugging."""
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

if __name__ == '__main__':
    xlsx()
