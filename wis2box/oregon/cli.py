import os
import click
from typing import Optional
from wis2box import cli_helpers
from wis2box.api import remove_collection, setup_collection
from wis2box.oregon.main import load_data_into_frost, update_data
from wis2box.oregon.types import ALL_RELEVANT_STATIONS, DATASTREAM_COLLECTION_METADATA, OBSERVATION_COLLECTION_METADATA
from wis2box.oregon.types import THINGS_COLLECTION
import pytest

@click.command()
@click.pass_context
@click.option("--station", "-s", default="*", help="station identifier")
@click.option("--begin", "-b", help="data start date", type=str)
@click.option("--end", "-e", help="data end date", type=str)
@cli_helpers.OPTION_VERBOSITY
def load(ctx, verbosity, station: int , begin: Optional[str] , end: Optional[str]):
    """Loads stations into sensorthings backend"""
    load_data_into_frost(station, begin, end)
    

@click.command()
@click.pass_context
@cli_helpers.OPTION_VERBOSITY
@click.option("--station", "-s", default="*", help="station identifier", type=str)
def update(ctx, verbosity, station: int):
    """Update the data to include new data since the last crawl"""
    if station == "*":
        update_data(ALL_RELEVANT_STATIONS, None)
    else:
        update_data([int(station)], None)

@click.command()
@click.pass_context
@cli_helpers.OPTION_VERBOSITY
def delete(ctx, verbosity):
    """Delete a collection of stations from the API config and backend"""
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


@click.group()
def oregon():
    """Station metadata management for Oregon Water Resources"""
    pass

oregon.add_command(publish)
oregon.add_command(load)
oregon.add_command(delete)
oregon.add_command(update)
oregon.add_command(test)
