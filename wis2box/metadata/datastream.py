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
from io import StringIO
import logging
import re
from requests import Session


from wis2box import cli_helpers
from wis2box.api import setup_collection
from wis2box.env import RESULTS_URL, WQP_URL
from wis2box.resources.code_mapping import MAPPING
from wis2box.util import make_uuid, url_join

LOGGER = logging.getLogger(__name__)


def gcm() -> dict:
    """
    Gets collection metadata for API provisioning

    :returns: `dict` of collection metadata
    """

    return {
        'id': 'Datastreams',
        'title': 'Datastreams',
        'description': 'SensorThings API Datastreams',
        'keywords': ['datastream', 'wqp'],
        'links': [WQP_URL],
        'bbox': [-180, -90, 180, 90],
        'id_field': '@iot.id',
        'title_field': 'name'
    }


def fetch_datastreams(station_id: str):
    """
    Load datasets from USBR RISE API

    :returns: `list`, of link relations for all datasets
    """
    http = Session()

    params = {
        'siteid': station_id,
        'mimeType': 'csv',
        'startDateLo': '01-01-2020',
        'dataProfile': 'resultPhysChem'
    }

    r = http.get(RESULTS_URL, params=params)
    if len(r.content) <= 2278:
        # LOGGER.warning(f'No data found at {r.url}')
        return {}

    datastreams = {}
    with StringIO(r.text) as fh:
        reader = DictReader(fh)
        for row in reader:
            code = row['ResultMeasure/MeasureUnitCode'] or row['DetectionQuantitationLimitMeasure/MeasureUnitCode']  # noqa
            c_string = f"{row['CharacteristicName']}-{row['MonitoringLocationIdentifier']}-{code}"  # noqa
            _uuid = make_uuid(c_string)
            datastreams[_uuid] = dict(row)

    return datastreams


def yield_datastreams(datasets: dict) -> list:
    """
    Yield datasets from USBR RISE API

    :returns: `list`, of link relations for all datasets
    """
    for id, dataset in datasets.items():
        sensor_ResultAnalyticalMethodMethodIdentifier = dataset[
            'ResultAnalyticalMethod/MethodIdentifier']
        sensor_ResultAnalyticalMethodMethodIdentifierContext = dataset[
            'ResultAnalyticalMethod/MethodIdentifierContext']
        if sensor_ResultAnalyticalMethodMethodIdentifier and sensor_ResultAnalyticalMethodMethodIdentifierContext:  # noqa
            sensor_identifier = f"{sensor_ResultAnalyticalMethodMethodIdentifierContext}-{sensor_ResultAnalyticalMethodMethodIdentifier}"  # noqa
            sensor_description = dataset[
                'ResultAnalyticalMethod/MethodDescriptionText']
        else:
            sensor_identifier = f"{dataset['SampleCollectionMethod/MethodIdentifierContext'] or dataset['OrganizationIdentifier']}-{dataset['SampleCollectionMethod/MethodIdentifier']}"  # noqa
            sensor_description = dataset[
                'SampleCollectionMethod/MethodDescriptionText']

        observed_property_name = ' '.join([dataset['ResultSampleFractionText'],
                                          dataset['CharacteristicName'],
                                          dataset['MethodSpeciationName']]
                                          ).strip()

        observed_property_definition = ''
        _url = 'https://cdxapps.epa.gov/oms-substance-registry-services/substance-details'  # noqa
        _ = dataset['CharacteristicName'].replace(' ', '').lower()
        __ = observed_property_name.replace(' ', '').lower()
        if _ in MAPPING:
            observed_property_definition = url_join(_url, MAPPING[_])
        elif __ in MAPPING:
            observed_property_definition = url_join(_url, MAPPING[__])
        else:
            pattern = r'[a-zA-Z0-9()\[\].-]+'
            for word in re.findall(pattern, observed_property_name):
                try:
                    inner_id = MAPPING[word.lower()]
                    observed_property_definition = url_join(_url, inner_id)
                    break
                except KeyError:
                    continue

        unitOfMeasurement = dataset['ResultMeasure/MeasureUnitCode'] or dataset['DetectionQuantitationLimitMeasure/MeasureUnitCode']  # noqa
        yield {
            '@iot.id': id,
            'name': observed_property_name + ' at ' + dataset['MonitoringLocationIdentifier'],  # noqa
            'description': observed_property_name + ' at ' + dataset['MonitoringLocationIdentifier'],  # noqa
            'observationType': 'http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement',  # noqa
            'properties': {
                'ActivityIdentifier': dataset['ActivityIdentifier'],
                'ActivityTypeCode': dataset['ActivityTypeCode'],
                'ActivityMediaName': dataset['ActivityMediaName']
            },
            'unitOfMeasurement': {
                'name': unitOfMeasurement,
                'symbol': unitOfMeasurement,
                'definition': unitOfMeasurement
            },
            'ObservedProperty': {
                'name': observed_property_name,
                'description': observed_property_name,
                'definition': observed_property_definition,
                'properties': {
                    'USGSPCode': dataset['USGSPCode'],
                    'MethodSpeciationName': dataset['MethodSpeciationName'],
                    'iop': dataset['ResultSampleFractionText']
                }
            },
            'Sensor': {
                'name': dataset['SampleCollectionMethod/MethodName'],
                'description': sensor_description,
                'metadata': dataset['ResultAnalyticalMethod/MethodUrl'],
                'encodingType': 'text/html',
                'properties': {
                    'identifier': sensor_identifier,
                    'EquipmentName': dataset['SampleCollectionEquipmentName'],
                    'ResultValueTypeName': dataset['ResultValueTypeName'],
                    'ResultAnalyticalMethod.MethodUrl': dataset['ResultAnalyticalMethod/MethodUrl']  # noqa
                }
            }
        }


def load_datastreams(station_id: str):
    """
    Load datasets from USBR RISE API

    :returns: `list`, of link relations for all datasets
    """
    return yield_datastreams(fetch_datastreams(station_id))


@click.group()
def datastream():
    """Datastream metadata management"""
    pass


@click.command()
@click.pass_context
@cli_helpers.OPTION_VERBOSITY
def publish_collection(ctx, verbosity):
    """Publishes collection of datastreams to API config and backend"""

    setup_collection(meta=gcm())
    click.echo('Done')


datastream.add_command(publish_collection)
