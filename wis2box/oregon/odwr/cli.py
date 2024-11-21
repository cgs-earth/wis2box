import os
from typing import Optional

import click
import pytest

from wis2box import cli_helpers
from wis2box.api import remove_collection, setup_collection
from wis2box.oregon.odwr.main import load_data_into_frost, update_data
from wis2box.oregon.odwr.types import (
    ALL_RELEVANT_STATIONS,
    DATASTREAM_COLLECTION_METADATA,
    OBSERVATION_COLLECTION_METADATA,
    THINGS_COLLECTION,
)


@click.command()
@click.pass_context
@click.option("--stations", "-s", default="all", help="station identifier", callback=lambda _,__,x: x.split(',') if x else [])
@click.option("--begin", "-b", help="data start date", type=str)
@click.option("--end", "-e", help="data end date", type=str)
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
@click.option("--stations", "-s", default=["all"], help="station identifier", callback=lambda _,__,x: x.split(',') if x else [])
def update(ctx, verbosity, stations: list[int]):
    """Update the data to include new data since the last crawl"""
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


@click.group()
def odwr():
    """Station metadata management for Oregon Water Resources"""
    pass

odwr.add_command(publish)
odwr.add_command(load)
odwr.add_command(delete)
odwr.add_command(update)
odwr.add_command(test)
