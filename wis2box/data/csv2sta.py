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

from csv import DictReader
from datetime import datetime
from pytz import timezone
from io import StringIO
import logging
from pathlib import Path
from requests import Session
from typing import Union

from wis2box.env import NLDI_URL
from wis2box.data.geojson import ObservationDataGeoJSON
from wis2box.util import make_uuid, url_join

LOGGER = logging.getLogger(__name__)


class ObservationDataCSV(ObservationDataGeoJSON):
    """Observation data"""

    def transform(self, input_data: Union[Path, bytes],
                  filename: str = '') -> bool:

        LOGGER.debug('Procesing data')
        input_bytes = self.as_bytes(input_data)

        fh = StringIO(input_bytes.decode())
        reader = DictReader(fh)

        http = Session()

        for row in reader:
            monitoring_location_identifier = \
                row['MonitoringLocationIdentifier']
            url = url_join(NLDI_URL, monitoring_location_identifier)
            try:
                result = http.get(url)
                feature = result.json()['features'][0]
            except KeyError:
                msg = f'Could not discover {monitoring_location_identifier}'
                LOGGER.info(msg)
                continue

            identifier = row['ResultIdentifier']
            unitOfMeasurement = row['ResultMeasure/MeasureUnitCode'] or row['DetectionQuantitationLimitMeasure/MeasureUnitCode']  # noqa
            datastream = make_uuid(f"{row['CharacteristicName']}-{row['MonitoringLocationIdentifier']}-{unitOfMeasurement}")  # noqa

            _ = ' '.join([row['ActivityStartDate'], row['ActivityStartTime/Time']]) # noqa
            try:
                isodate = datetime.strptime(_, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                isodate = datetime.strptime(_, '%Y-%m-%d ')
            try:
                isodate = isodate.replace(
                    tzinfo=timezone(row['ActivityStartTime/TimeZoneCode']))
            except Exception:
                LOGGER.info('Could not apply time zone information')

            rowdate = isodate.strftime('%Y-%m-%dT%H:%M:%SZ')
            isodate = isodate.strftime('%Y%m%dT%H%M%S')

            try:
                analysisStartDate = datetime.strptime(
                    row['AnalysisStartDate'], '%Y-%m-%d'
                ).strftime('%Y-%m-%dT%H:%M:%SZ')
            except ValueError:
                analysisStartDate = rowdate

            try:
                LongitudeMeasure = float(row['ActivityLocation/LongitudeMeasure']) # noqa
                LatitudeMeasure = float(row['ActivityLocation/LatitudeMeasure']) # noqa
                geom = {
                    'type': 'Point',
                    'coordinates': [LongitudeMeasure, LatitudeMeasure]
                }
            except ValueError:
                geom = feature['geometry']

            try:
                result = float(row['ResultMeasureValue'])
            except ValueError:
                result = row['ResultDetectionConditionText']

            resultQuality = {
                'detectionCondition': row['ResultDetectionConditionText'],
                'precision': row['DataQuality/PrecisionValue'],
                'accuracy': row['DataQuality/BiasValue'],
                'detectionLimit': {
                    'value': row['DetectionQuantitationLimitMeasure/MeasureValue'], # noqa
                    'unit': row['DetectionQuantitationLimitMeasure/MeasureUnitCode'] # noqa
                }
            }
            resultQuality = (row['MeasureQualifierCode'] or row['ResultStatusIdentifier']) or ' '.join([  # noqa
                row['ResultDetectionQuantitationLimitUrl'],
                row['DetectionQuantitationLimitMeasure/MeasureValue'],
                row['DetectionQuantitationLimitMeasure/MeasureUnitCode']
            ])
            LOGGER.debug(f'Publishing with ID {identifier}')
            self.output_data[identifier] = {
                '_meta': {
                    'identifier': identifier,
                    'rowdate': rowdate,
                    'relative_filepath': self.get_local_filepath(rowdate)
                },
                'geojson': {
                    'phenomenonTime': rowdate,
                    'resultTime': analysisStartDate,
                    'result': result,
                    'resultQuality': resultQuality,
                    'parameters': {
                        'hydrologicCondition': row['HydrologicCondition'],
                        'hydrologicEvent': row['HydrologicEvent'],
                        'modified': row['LastUpdated'],
                        'status': row['ResultStatusIdentifier'],
                        'publisher': row['ProviderName'],
                        'valueType': row['ResultValueTypeName'],
                        'comment': row['ResultCommentText']
                    },
                    'Datastream': {'@iot.id': datastream},
                    'FeatureOfInterest': {
                        '@iot.id': datastream,
                        'name': row['MonitoringLocationName'],
                        'description': row['MonitoringLocationName'],
                        'encodingType': 'application/geo+json',
                        'feature': geom,
                    },
                }
            }

            deployment_info = row['ActivityTypeCode'] in (
                'Field Msr/Obs-Portable Data Logger', 'Field Msr/Obs')
            if not deployment_info:
                LOGGER.info('Adding Sampling Entity')
                sampling_name = '-'.join([
                    row['MonitoringLocationIdentifier'],
                    row['ActivityIdentifier']
                ])
                samplingProcedure_id = '-'.join([
                    row['SampleCollectionMethod/MethodIdentifierContext'],
                    row['SampleCollectionMethod/MethodIdentifier']
                ])
                featureOfInterest = self.output_data[identifier]['geojson']['FeatureOfInterest']  # noqa

                try:
                    featureOfInterest['Samplings'] = [{
                        'name': sampling_name,
                        'description': row['ActivityTypeCode'] + row['ActivityRelativeDepthName'],  # noqa
                        'depthUom': row['ResultDepthHeightMeasure/MeasureUnitCode'], # noqa
                        'encodingType': 'application/geo+json',
                        # 'samplingLocation': geom,
                        'Thing': {
                            '@iot.id': row['MonitoringLocationIdentifier']
                        },
                        'Sampler': {
                            'name': row['OrganizationFormalName'],
                            'SamplingProcedure': {
                                '@iot.id': make_uuid(samplingProcedure_id),
                                'name': row['SampleCollectionMethod/MethodName'], # noqa
                                'definition': row['SampleCollectionMethod/MethodDescriptionText'], # noqa
                                'description': row['SampleCollectionMethod/MethodDescriptionText'] # noqa
                            }
                        }
                    }]
                    if row['ActivityDepthHeightMeasure/MeasureValue']:
                        featureOfInterest['Samplings'][0]['atDepth'] = \
                            row['ActivityDepthHeightMeasure/MeasureValue']

                except (TypeError, ValueError):
                    LOGGER.error('No Sampling detected')

    def __repr__(self):
        return '<ObservationDataCSV>'
