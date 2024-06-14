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
from datetime import datetime
from io import StringIO
import logging
from pytz import timezone
import re
from requests import Session
from typing import Iterable


from wis2box import cli_helpers
from wis2box.api import setup_collection
from wis2box.env import RESULTS_URL, WQP_URL, NLDI_URL
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


def yield_datastreams(station_identifier: str,
                      datasets: dict) -> Iterable:
    """
    Yield datasets from USBR RISE API

    :returns: `Iterable`, of link relations for all datasets
    """
    http = Session()

    for id, dataset in datasets.items():
        kwargs = {}
        monitoring_location_identifier = \
            dataset['MonitoringLocationIdentifier']
        url = url_join(NLDI_URL, monitoring_location_identifier)
        try:
            result = http.get(url)
            feature = result.json()['features'][0]
            mainstem = http.get(feature['properties']['mainstem']).json()
            kwargs['UltimateFeatureOfInterest'] = {
                '@iot.id': make_uuid(feature['properties']['mainstem']),
                'name': mainstem['properties']['name_at_outlet'],
                'description': mainstem['properties']['name_at_outlet'],
                'encodingType': 'application/geo+json',
                'feature': mainstem['geometry'],
                'properties': {
                    'uri': feature['properties']['mainstem']
                }
            }
        except KeyError:
            LOGGER.info(f'Could not discover {monitoring_location_identifier}')
            continue

        sensor_kwargs = {}
        sensor_name = ' '.join([
            dataset['ResultAnalyticalMethod/MethodName'],
            'applied', 'by', dataset['OrganizationFormalName']])
        sensor_ResultAnalyticalMethodMethodIdentifier = dataset[
            'ResultAnalyticalMethod/MethodIdentifier']
        sensor_ResultAnalyticalMethodMethodIdentifierContext = dataset[
            'ResultAnalyticalMethod/MethodIdentifierContext']
        if sensor_ResultAnalyticalMethodMethodIdentifier and sensor_ResultAnalyticalMethodMethodIdentifierContext:  # noqa
            sensor_identifier = f"{sensor_ResultAnalyticalMethodMethodIdentifierContext}-{sensor_ResultAnalyticalMethodMethodIdentifier}"  # noqa
            sensor_description = ' '.join([
                dataset['ResultAnalyticalMethod/MethodName'],
                'applied', 'by', dataset['OrganizationFormalName'],
                'analyzed', 'by', dataset['LaboratoryName']])
        else:
            sensor_identifier = f"{dataset['SampleCollectionMethod/MethodIdentifierContext'] or dataset['OrganizationIdentifier']}-{dataset['SampleCollectionMethod/MethodIdentifier']}"  # noqa
            sensor_description = ' '.join([
                dataset['SampleCollectionMethod/MethodName'],
                'applied', 'by', dataset['OrganizationFormalName'],
                'analyzed', 'by', dataset['LaboratoryName']])

        observed_property_name = ' '.join([
            dataset['ResultSampleFractionText'],
            dataset['CharacteristicName'],
            dataset['MethodSpeciationName']
        ]).strip()

        observing_procedure_id = '-'.join([
            dataset['ResultAnalyticalMethod/MethodIdentifierContext'],
            dataset['ResultAnalyticalMethod/MethodIdentifier']])

        deployment_info = dataset['ActivityTypeCode'] in (
            'Field Msr/Obs-Portable Data Logger', 'Field Msr/Obs')
        if deployment_info:
            _ = ' '.join([dataset['ActivityStartDate'],
                          dataset['ActivityStartTime/Time']])
            try:
                isodate = datetime.strptime(_, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                isodate = datetime.strptime(_, '%Y-%m-%d ')
            try:
                isodate = isodate.replace(
                    tzinfo=timezone(dataset['ActivityStartTime/TimeZoneCode']))
            except Exception:
                LOGGER.info('Could not apply time zone information')
            deployment_ActivityStartTime = isodate.strftime(
                '%Y-%m-%dT%H:%M:%SZ')
            deployment_description = ' '.join([
                dataset['OrganizationFormalName'],
                dataset['ActivityTypeCode'], 'at',
                dataset['MonitoringLocationName'], 'at',
                dataset['ActivityStartDate']
            ])
            deployment_id = '-'.join([dataset['ActivityIdentifier'],
                                      dataset['MonitoringLocationIdentifier']])
            sensor_kwargs['Deployments'] = [{
                '@iot.id': make_uuid(deployment_id),
                'name': deployment_id,
                'deploymentTime': deployment_ActivityStartTime,
                'depthUom': dataset['ActivityDepthHeightMeasure/MeasureUnitCode'], # noqa
                'description': deployment_description,
                'reason': dataset['ProjectName'],
                'Host': {'@iot.id': station_identifier},
                'properties': {
                    'orgName': dataset['OrganizationFormalName']
                }
            }]
            if dataset['ActivityDepthHeightMeasure/MeasureValue']:
                sensor_kwargs['Deployments'][0]['atDepth'] = \
                    dataset['ActivityDepthHeightMeasure/MeasureValue']

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
                    'speciation': dataset['MethodSpeciationName'],
                    'iop': dataset['ResultSampleFractionText']
                }
            },
            'ObservingProcedure': {
                '@iot.id': observing_procedure_id,
                'name': dataset['ResultAnalyticalMethod/MethodName']
            },
            'Sensor': {
                '@iot.id': sensor_identifier,
                'name': sensor_name,
                'description': sensor_description,
                'metadata': dataset['ResultAnalyticalMethod/MethodDescriptionText'], # noqa
                'encodingType': 'text/html',
                'properties': {
                    'identifier': sensor_identifier,
                    'EquipmentName': dataset['SampleCollectionEquipmentName'],
                    'ResultValueTypeName': dataset['ResultValueTypeName'],
                    'ResultAnalyticalMethod.MethodUrl': dataset['ResultAnalyticalMethod/MethodUrl']  # noqa
                },
                **sensor_kwargs
            },
            **kwargs
        }


def load_datastreams(station_id: str):
    """
    Load datasets from USBR RISE API

    :returns: `list`, of link relations for all datasets
    """
    return yield_datastreams(station_id, fetch_datastreams(station_id))


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
