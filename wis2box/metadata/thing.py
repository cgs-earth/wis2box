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
import csv
import logging
from requests import Session

from wis2box import cli_helpers
from wis2box.api import (setup_collection, upsert_collection_item,
                         delete_collection_item)
from wis2box.env import DATADIR
from wis2box.metadata.datastream import load_datastreams, gcm
from wis2box.util import get_typed_value, url_join

LOGGER = logging.getLogger(__name__)

STATION_METADATA = DATADIR / 'metadata' / 'station'
STATIONS = STATION_METADATA / 'location_data.csv'
THINGS = 'Things'

USBR_URL = 'https://data.usbr.gov'
RISE_URL = f'{USBR_URL}/rise/api'


def gcm_() -> dict:
    """
    Gets collection metadata for API provisioning

    :returns: `dict` of collection metadata
    """

    return {
        'id': THINGS,
        'title': THINGS,
        'description': 'SensorThings API Things',
        'keywords': ['thing', 'dam'],
        'links': ['https://data.usbr.gov/rise-api'],
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

    setup_collection(meta=gcm_())
    setup_collection(meta=gcm())

    with STATIONS.open() as fh:
        reader = csv.DictReader(fh)

        for row in reader:
            station_identifier = row.pop('station_identifier')
            try:
                datastreams = load_datastreams(station_identifier)
            except Exception:
                LOGGER.error(station_identifier)
                continue
            feature = {
                '@iot.id': station_identifier,
                'name': row['station_name'],
                'description': row['station_name'],
                'Locations': [{
                    'name': row['station_name'],
                    'description': row['station_name'],
                    'encodingType': 'application/vnd.geo+json',
                    'location': {
                        'type': 'Point',
                        'coordinates': [
                            get_typed_value(row.pop('longitude')),
                            get_typed_value(row.pop('latitude')),
                            get_typed_value(row.pop('elevation'))
                        ]}
                }],
                'Datastreams': list(datastreams),
                'properties': {
                    'RISE.selfLink': f'{RISE_URL}/location/{station_identifier}', # noqa
                    **row
                }
            }

            LOGGER.debug('Publishing to backend')
            upsert_collection_item(THINGS, feature)

    return


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
def cache_stations(ctx, verbosity):
    """Caches collection of stations to API config and backend"""
    http = Session()
    all_stations = []
    params = {
        'hasCatalogItems': 'true',
        'order[id]': 'asc'
    }
    _ = url_join(RISE_URL, 'location')
    url = http.get(_, params=params).url

    while url:
        r = http.get(url)
        response = r.json()
        
        # Extract station data
        for station in response.get('data', []):
            attributes = station['attributes']
            if attributes['locationCoordinates']['type'] != 'Point':
                continue

            coordinates = attributes['locationCoordinates']['coordinates']
            try:
                station_data = {
                    'station_identifier': attributes['_id'],
                    'station_name': attributes['locationName'],
                    'description': attributes.get('locationDescription', ''),
                    'latitude': coordinates[1],
                    'longitude': coordinates[0],
                    'elevation': attributes['elevation'],
                    'create_date': attributes['createDate'],
                    'update_date': attributes['updateDate'],
                    'timezone': attributes['timezone'],
                    'type': attributes['locationTypeName'],
                    'region': ','.join(attributes.get('locationRegionNames', [])),
                }
            except IndexError as err:
                click.echo(err)
                click.echo(station)
                continue
            all_stations.append(station_data)
        
        # Get the next URL from the response
        links = response.get('links', {})
        url = url_join(USBR_URL, links.get('next')) if 'next' in links else None
    
    # Write station data to CSV
    fieldnames = ['station_identifier', 'station_name', 'description', 'latitude', 'longitude', 'elevation', 'create_date', 'update_date', 'timezone', 'type', 'region']
    with open(STATIONS, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_stations)

@click.command()
@click.pass_context
@cli_helpers.OPTION_VERBOSITY
def delete_collection(ctx, verbosity):
    """Publishes collection of stations to API config and backend"""

    with STATIONS.open() as fh:
        reader = csv.DictReader(fh)

        for row in reader:
            delete_collection_item(THINGS, row['station_identifier'])

    click.echo('Done')


thing.add_command(publish_collection)
thing.add_command(cache_stations)
thing.add_command(delete_collection)
