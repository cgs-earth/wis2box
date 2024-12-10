from datetime import date
import datetime
import os
from typing import Optional

import click
import pytest
import debugpy
from wis2box import cli_helpers
from wis2box.api import remove_collection, setup_collection
from wis2box.oregon.odwr.helper_classes import UpdateMetadata, save_metadata
from wis2box.oregon.odwr.lib import to_oregon_datetime
from wis2box.oregon.odwr.main import load_data_into_frost, update_data
from wis2box.oregon.odwr.types import (
    ALL_RELEVANT_STATIONS,
    DATASTREAM_COLLECTION_METADATA,
    OBSERVATION_COLLECTION_METADATA,
    THINGS_COLLECTION,
)

import logging
LOGGER = logging.getLogger(__name__)

@click.command()
@click.pass_context
@click.option("--stations", "-s", default="all", help="station identifier", callback=lambda _,__,x: x.split(',') if x else [])
@click.option("--begin", "-b", help="data start date in Oregon timezone", type=str)
@click.option("--end", "-e", help="data end date in Oregon timezone", type=str)
@cli_helpers.OPTION_VERBOSITY
def load(ctx, verbosity, stations: list[int] , begin: Optional[str] , end: Optional[str]):
    """Loads stations into sensorthings backend"""
    if stations == ["all"]:
        load_data_into_frost(ALL_RELEVANT_STATIONS, begin, end)
    else:
        stations = list(map(int, stations))
        load_data_into_frost(stations, begin, end)
    

@click.command()
@click.pass_context
@cli_helpers.OPTION_VERBOSITY
@click.option("--stations", "-s", default="all", help="station identifier", callback=lambda _,__,x: x.split(',') if x else [])
def update(ctx, verbosity, stations: list[int]):
    """Update the data to include new data since the last crawl"""
    from wis2box.oregon.odwr.helper_classes import metadata_file_path

    if not metadata_file_path.exists():
        LOGGER.error("No metadata file found! Skipping updates. Please create a metadata file first on run an initial crawl.")
        return

    if stations == ["all"]:
        update_data(ALL_RELEVANT_STATIONS, None)
    else:
        stations = list(map(int, stations))
        update_data(stations, None)

@click.command()
@click.pass_context
@cli_helpers.OPTION_VERBOSITY
def delete(ctx, verbosity):
    """Delete all oregon observations"""
    remove_collection(THINGS_COLLECTION)


@click.command()
@click.pass_context
@cli_helpers.OPTION_VERBOSITY
def publish(ctx, verbosity):
    """Publishes the observations and datastreams to the API config and backend"""
    setup_collection(meta=OBSERVATION_COLLECTION_METADATA)
    setup_collection(meta=DATASTREAM_COLLECTION_METADATA)
    click.echo("Done")

@click.command(context_settings=dict(ignore_unknown_options=True))
@click.pass_context
@cli_helpers.OPTION_VERBOSITY
@click.argument('pytest_args', nargs=-1, type=click.UNPROCESSED)
def test(ctx, verbosity, pytest_args):
    """Run all pytest tests in the oregon tests/ folder. Pass in additional arguments to pytest if needed."""
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

@click.command()
@click.pass_context
@cli_helpers.OPTION_VERBOSITY
def setup_cron(ctx, verbosity):
    """Sets up a cronjob to run the update command every day. Gets rid of other cronjobs"""
    cronjob = "0 0 * * * /usr/local/bin/wis2box oregon odwr update  > /proc/1/fd/1 2>/proc/1/fd/2"
    os.system(f'crontab -l | grep "{cronjob}" || echo "{cronjob}" | crontab -')
    # get the new value of the crontab
    cronjob = os.popen('crontab -l').read()
    click.echo("Cronjob set to:")
    click.echo(cronjob)

@click.command()
@click.pass_context
@cli_helpers.OPTION_VERBOSITY
def generate_test_metadata(ctx, verbosity):
    """Create a sample metadata file with dates in the Oregon format for testing purposes"""
    start = to_oregon_datetime(datetime.datetime.now())
    end = to_oregon_datetime(datetime.datetime.now())
    metadata = UpdateMetadata(start, end, [], [])
    save_metadata(metadata)

@click.group()
def odwr():
    """Station metadata management for Oregon Water Resources"""
    pass

odwr.add_command(generate_test_metadata)
odwr.add_command(setup_cron)
odwr.add_command(publish)
odwr.add_command(load)
odwr.add_command(delete)
odwr.add_command(update)
odwr.add_command(test)
odwr.add_command(test_debug)
