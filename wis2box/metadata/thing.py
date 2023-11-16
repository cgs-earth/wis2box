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

import click
from csv import DictReader
import logging
from io import TextIOWrapper, BytesIO
import re
import requests
from zipfile import ZipFile

from wis2box import cli_helpers
from wis2box.api import (setup_collection, upsert_collection_item,
                         delete_collection_item)
from wis2box.env import (THINGS, STATIONS, GEOCONNEX, RESULTS_URL, STATION_URL)
from wis2box.metadata.datastream import load_datastreams
from wis2box.util import extract_coord, url_join, clean_word

LOGGER = logging.getLogger(__name__)


def gcm() -> dict:
    """
    Gets collection metadata for API provisioning

    :returns: `dict` of collection metadata
    """

    return {
        'id': THINGS,
        'title': THINGS,
        'description': 'SensorThings API Things',
        'keywords': ['thing', 'dam'],
        'links': ['https://www.waterqualitydata.us'],
        'bbox': [-180, -90, 180, 90],
        'id_field': '@iot.id',
        'title_field': 'name'
    }


@click.group()
def thing():
    """Station metadata management"""
    pass


def publish_station_collection() -> None:
    """
    Publishes station collection to API config and backend

    :returns: `None`
    """
    with STATIONS.open() as fh:
        reader = DictReader(fh)

        for row in reader:
            station_identifier = row['MonitoringLocationIdentifier']
            try:
                datastreams = load_datastreams(station_identifier)
            except Exception:
                LOGGER.warning(station_identifier)
                continue
            # oname = row['OrganizationFormalName']
            # _ = re.findall(r'\b[\da-sA-Z]+', row['OrganizationIdentifier'])
            # oid = '-'.join(['WQX', *_]).upper()
            # if upsert_collection_item('Parties', {
            #     'authId': oid,
            #     'displayName': oname,
            #     'description': oname,
            #     'role': 'institutional'
            # }):
            #     LOGGER.debug('Party created')

            location_name = clean_word(row['MonitoringLocationName'])

            feature = {
                '@iot.id': station_identifier,
                'name': location_name,
                'description': location_name,
                'Locations': [{
                    'name': row['MonitoringLocationName'],
                    'description': row['MonitoringLocationName'],
                    'encodingType': 'application/geo+json',
                    'location': {
                        'type': 'Point',
                        'coordinates': [
                            extract_coord(row['LongitudeMeasure']),
                            extract_coord(row['LatitudeMeasure'])
                        ]
                    }
                }],
                'properties': {
                    'hu08': url_join(GEOCONNEX, 'ref/hu08', row['HUCEightDigitCode']),  # noqa
                    'state': url_join(GEOCONNEX, 'ref/states', row['StateCode']),  # noqa
                    'county': url_join(GEOCONNEX, 'ref/counties', f"{row['StateCode']}{row['CountyCode']}"),  # noqa
                    'provider': row['ProviderName']
                },
                'Datastreams': list(datastreams)
            }

            if not upsert_collection_item(THINGS, feature):
                LOGGER.error(f'Failed to publish {station_identifier}')
                break

    setup_collection(meta=gcm())

    return


@click.command()
@click.pass_context
@click.argument('counties', nargs=-1)
@cli_helpers.OPTION_VERBOSITY
def cache_stations(ctx, counties, verbosity):
    """Publishes collection of stations to API config and backend"""
    _counties = []
    pattern = r'^(https://geoconnex.us/ref/counties/(\d{2})(\d{3})|(\d{2})(\d{3}))$' # noqa
    for county in counties:
        match = re.match(pattern, county)
        if match:
            statecode = match.group(2) or match.group(4)
            countycode = match.group(3) or match.group(5)
            _counties.append(f'US:{statecode}:{countycode}')
        else:
            click.echo(f'Invalid county: {county}')

    params = {
        'countycode': _counties,
        'zip': 'yes',
        'mimeType': 'csv',
        'startDateLo': '01-01-2020',
        'dataProfile': 'resultPhysChem'
    }
    r = requests.get(RESULTS_URL, params=params)

    if not r.ok:
        LOGGER.error(f'Bad response at {r.url}')
        return

    LOGGER.debug(f'Unzipping {r.url}')
    zipfiles = ZipFile(BytesIO(r.content))
    [zipfile] = zipfiles.namelist()
    sites = set()
    with zipfiles.open(zipfile) as fh:
        reader = DictReader(TextIOWrapper(fh, 'utf-8'))
        for row in reader:
            sites.add(row['MonitoringLocationIdentifier'])

    LOGGER.debug(f'Sites mapped: {sites}')
    params = {
        'siteid': list(sites),
        'zip': 'yes',
        'mimeType': 'csv'
    }
    LOGGER.debug('Retrieving site metadata')
    r = requests.post(STATION_URL, data=params)
    if not r.ok:
        LOGGER.error(f'Bad response at {r.url}')
        return

    LOGGER.info('Writing site metadata')
    zipfiles = ZipFile(BytesIO(r.content))
    [zipfile] = zipfiles.namelist()
    with (zipfiles.open(zipfile) as fh_in,
          STATIONS.open('wb') as fh_out):
        fh_out.write(fh_in.read())

    click.echo('Done')


@click.command()
@click.pass_context
@cli_helpers.OPTION_VERBOSITY
def publish_collection(ctx, verbosity):
    """Publishes collection of stations to API config and backend"""

    publish_station_collection()
    click.echo('Done')


@click.command()
@click.pass_context
@cli_helpers.OPTION_VERBOSITY
def delete_collection(ctx, verbosity):
    """Publishes collection of stations to API config and backend"""

    with STATIONS.open() as fh:
        reader = DictReader(fh)

        for row in reader:
            delete_collection_item(THINGS, row['MonitoringLocationIdentifier'])

    click.echo('Done')


thing.add_command(cache_stations)
thing.add_command(publish_collection)
thing.add_command(delete_collection)
