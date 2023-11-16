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
from typing import Union

from wis2box.data.geojson import ObservationDataGeoJSON
from wis2box.util import make_uuid

LOGGER = logging.getLogger(__name__)


class ObservationDataCSV(ObservationDataGeoJSON):
    """Observation data"""

    def transform(self, input_data: Union[Path, bytes],
                  filename: str = '') -> bool:

        LOGGER.debug('Procesing data')
        input_bytes = self.as_bytes(input_data)

        fh = StringIO(input_bytes.decode())
        reader = DictReader(fh)

        for row in reader:
            identifier = row['ResultIdentifier']
            unitOfMeasurement = row['ResultMeasure/MeasureUnitCode'] or row['DetectionQuantitationLimitMeasure/MeasureUnitCode']  # noqa
            datastream = make_uuid(f"{row['CharacteristicName']}-{row['MonitoringLocationIdentifier']}-{unitOfMeasurement}")  # noqa

            _ = f"{row['ActivityStartDate']} {row['ActivityStartTime/Time']}"
            isodate = datetime.strptime(
                _, '%Y-%m-%d %H:%M:%S'
            ).replace(tzinfo=timezone(row['ActivityStartTime/TimeZoneCode']))
            rowdate = isodate.strftime('%Y-%m-%dT%H:%M:%SZ')
            isodate = isodate.strftime('%Y%m%dT%H%M%S')

            LongitudeMeasure = row['ActivityLocation/LongitudeMeasure'] # noqa
            LatitudeMeasure = row['ActivityLocation/LatitudeMeasure'] # noqa
            try:
                result = float(row['ResultMeasureValue'])
            except ValueError:
                result = row['ResultDetectionConditionText']

            if not result:
                LOGGER.warning(f'No results for {identifier}')
                continue

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
                    'resultTime': rowdate,
                    'result': result,
                    'resultQuality': resultQuality,
                    'parameters': {
                        'ResultCommentText': row['ResultCommentText'],
                        'HydrologicCondition': row['HydrologicCondition'],
                        'HydrologicEvent': row['HydrologicEvent']
                    },
                    'Datastream': {'@iot.id': datastream},
                    'FeatureOfInterest': {
                        '@iot.id': datastream,
                        'name': row['MonitoringLocationName'],
                        'description': row['MonitoringLocationName'],
                        'encodingType': 'application/vnd.geo+json',
                        'feature': {
                            'type': 'Point',
                            'coordinates': [LongitudeMeasure, LatitudeMeasure]
                        },
                    },
                }
            }

            try:
                depth = float(row['ActivityDepthHeightMeasure/MeasureValue'])
                LOGGER.info('Adding samplings')
                featureOfInterest = self.output_data[identifier]['geojson']['FeatureOfInterest']  # noqa
                featureOfInterest['Samplings'] = [{
                    'name': row['ActivityTypeCode'],
                    'description': row['ActivityTypeCode'] + row['ActivityRelativeDepthName'],  # noqa
                    'atDepth': depth,  # noqa
                    'depthUom': row['ActivityDepthHeightMeasure/MeasureUnitCode'],  # noqa
                    'encodingType': 'application/vnd.geo+json',
                    'samplingLocation': {
                        'type': 'Point',
                        'coordinates': [LongitudeMeasure, LatitudeMeasure]
                    },
                    'Thing': {
                        '@iot.id': row['MonitoringLocationIdentifier']
                    },
                    'Sampler': {
                        'name': row['OrganizationFormalName'],
                        'SamplingProcedure': {
                            'name': row['ActivityTypeCode']
                        }
                    },
                    'SamplingProcedure': {
                        'name': row['ActivityTypeCode']
                    }
                }]
            except (TypeError, ValueError):
                LOGGER.info('No Sampling detected')

    def __repr__(self):
        return '<ObservationDataCSV>'
