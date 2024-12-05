###############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
###############################################################################

__version__ = '0.6.dev1'

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import click
import debugpy
import httpx
from wis2box import cli_helpers
import os
import pytest
import requests
import threading

from wis2box.api import remove_collection
from wis2box.pitt.lib import parse_csv, parse_geojson, post_to_things, to_sta
from wis2box.pitt.types import PredictionsCSV
from frost_sta_client import utils
import json
import frost_sta_client as fsc
import queue

@click.group()
@click.version_option(version=__version__)
def pitt():
    """Oregon data management"""
    pass


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

@click.command()
@click.pass_context
@cli_helpers.OPTION_VERBOSITY
def delete(ctx, verbosity):
    """Delete all pitt observations"""
    remove_collection("Things")



@click.command()
@click.pass_context
@cli_helpers.OPTION_VERBOSITY
def load_sample(ctx, verbosity):
    """Load all pitt observations. Requires files to be mounted inside the container"""
    geometry = Path(__file__).parent / "tests" / "nhd_centerlines.geojson"
    geometry = parse_geojson(geometry)
    observations = Path(__file__).parent / "tests" / "rs_chla_predictions.csv"
    observations = parse_csv(observations, PredictionsCSV)

    for thing in to_sta(geometry, observations):
        post_to_things(thing)
        

pitt.add_command(load_sample)
pitt.add_command(delete)
pitt.add_command(test)
pitt.add_command(test_debug)

