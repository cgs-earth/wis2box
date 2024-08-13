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
from datetime import datetime, timedelta
from json.decoder import JSONDecodeError
import logging
from pathlib import Path
from requests import Session, RequestException
from typing import Union

from wis2box import cli_helpers
from wis2box.api import setup_collection
from wis2box.data.base import BaseAbstractData
from wis2box.env import (DATADIR, DOCKER_API_URL, STORAGE_INCOMING,
                         USBR_URL, RESULT_URL)
from wis2box.storage import put_data
from wis2box.topic_hierarchy import validate_and_load

LOGGER = logging.getLogger(__name__)


STATION_METADATA = DATADIR / 'metadata' / 'station'
STATIONS = STATION_METADATA / 'location_data.csv'


def gcm() -> dict:
    """
    Gets collection metadata for API provisioning

    :returns: `dict` of collection metadata
    """

    return {
        'id': 'Observations',
        'title': 'Observations',
        'description': 'SensorThings API Observations',
        'keywords': ['observation', 'dam'],
        'links': [f'{USBR_URL}/rise-api'],
        'bbox': [-180, -90, 180, 90],
        'time_field': 'resultTime',
        'id_field': '@iot.id'
    }


class ObservationDataDownload(BaseAbstractData):
    """Observation data"""

    def __init__(self, defs: dict) -> None:
        """
        USBR Observations data initializer

        :param def: `dict` object of resource mappings

        :returns: `None`
        """
        super().__init__(defs)

        self.http = Session()
        self._end = datetime.utcnow()
        self._begin = self._end - timedelta(days=1)

    @property
    def begin(self):
        return self._begin.strftime('%Y-%m-%dT')

    @property
    def end(self):
        return self._end.strftime('%Y-%m-%dT')

    def set_date(self, begin: str = '', end: str = '') -> None:
        """
        Set date parametetrs

        :param begin: `str` data search date begin
        :param end: `str` data search date end

        :returns: `None`
        """
        if begin != '':
            self._begin = datetime.strptime(begin, '%Y-%m-%dT')

        if end != '':
            self._end = datetime.strptime(end, '%Y-%m-%dT')

    def _get_response(self, url: str, params: dict = {}):
        """
        Private function: Get STA response

        :param url: request url
        :param params: query parameters

        :returns: STA response
        """
        r = self.http.get(url, params=params)

        if r.ok:
            try:
                response = r.json()
            except JSONDecodeError:
                response = r.content
        else:
            msg = f'Bad http response code: {r.url}'
            LOGGER.error(msg)
            raise RequestException(msg)

        return response

    def transform(
        self, input_data: Union[Path, bytes], filename: str = ''
    ) -> bool:
        rmk = f'{input_data}_{self.begin}_{self.end}'
        params = {
            'type': 'csv',
            'after': self.begin,
            'before': self.end,
            'itemId': input_data,
            'filename': f'{rmk}.csv'
        }
        data = self._get_response(RESULT_URL, params)
        bytes = self.as_bytes(data)

        if 'No data' in str(bytes):
            LOGGER.info(f'No data for {rmk}')
        else:
            path = f'{STORAGE_INCOMING}/{self.local_filepath(self.end)}/{rmk}.csv'  # noqa

            put_data(data, path)

            LOGGER.debug('Finished processing subset')

    def local_filepath(self, date_):
        yyyymmdd = date_[0:10]  # date_.strftime('%Y-%m-%d')
        return Path(yyyymmdd) / self.topic_hierarchy.dirpath

    def __repr__(self):
        return '<ObservationDataDownload>'


def sync_datastreams(station_id, begin, end):
    url = DOCKER_API_URL + '/collections/datastreams/items'

    _, plugins = validate_and_load('iow.demo.Observations')
    [plugin] = [p for p in plugins
                if isinstance(p, ObservationDataDownload)]

    if begin:
        plugin.set_date(begin=begin)
    if end:
        plugin.set_date(end=end)

    # params = {'Thing': station_id, 'resulttype': 'hits'}
    # response = plugin._get_response(url=url, params=params)
    # hits = response.get('numberMatched')

    params = {'Thing': station_id, 'limit': 10000}
    datastreams = plugin._get_response(url=url, params=params)

    for datastream in datastreams['features']:
        try:
            plugin.transform(datastream['id'], datastream['id'])
        except Exception as err:
            LOGGER.error(datastream['id'])
            LOGGER.error(err)


@click.group()
def observation():
    """Observation metadata management"""
    pass


@click.command()
@click.pass_context
@cli_helpers.OPTION_VERBOSITY
def publish_collection(ctx, verbosity):
    """Publishes collection of datastreams to API config and backend"""

    setup_collection(meta=gcm())
    click.echo('Done')


@click.command()
@click.pass_context
@click.option('--station', '-s', default='*', help='station identifier')
@click.option('--begin', '-b', help='data start date')
@click.option('--end', '-e', help='data end date')
@cli_helpers.OPTION_VERBOSITY
def ingest(ctx, station, begin, end, verbosity):
    """Ingest all data from a station"""
    click.echo('Ingesting observations')

    if station == '*':
        with STATIONS.open() as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                station = row['station_identifier']
                try:
                    sync_datastreams(station, begin, end)
                except Exception as err:
                    click.echo(f'{err} with {station}')
    else:
        sync_datastreams(station, begin, end)

    click.echo('Done')


observation.add_command(publish_collection)
observation.add_command(ingest)
