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

import csv
from datetime import datetime
from io import StringIO
import json
import logging
from pathlib import Path
import requests
from typing import Union

from wis2box.data.geojson import ObservationDataGeoJSON
from wis2box.util import to_json
from wis2box.env import API_BACKEND_URL

LOGGER = logging.getLogger(__name__)


class ObservationDataCSV(ObservationDataGeoJSON):
    """Observation data"""

    def transform(self, input_data: Union[Path, bytes],
                  filename: str = '') -> bool:

        LOGGER.debug('Procesing data')
        input_bytes = self.as_bytes(input_data)

        fh = StringIO(input_bytes.decode())
        reader = csv.reader(fh, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)

        # read in header rows
        rows_read = 0
        skip = 7
        while rows_read <= skip:
            row = next(reader)
            if rows_read == 3:
                loc_names = row
            elif rows_read == 4:
                loc = row
            elif rows_read == skip:
                col_names = row
            rows_read += 1

        location = dict(zip(loc_names, loc))
        location['Coordinates'] = location.get('Coordinates (long, lat)', loc[2])  # noqa
        location['Coordinates'] = location['Coordinates'].replace('(', '[')
        location['Coordinates'] = location['Coordinates'].replace(')', ']')
        LOGGER.debug(location['Coordinates'])
        location['Coordinates'] = json.loads(location['Coordinates'])
        LOGGER.debug('Processing data from ' + location['Location'])

        for row in reader:
            data_dict = dict(zip(col_names, row))

            datastream = filename.split('_').pop(0)
            isodate = datetime.strptime(
                data_dict.get('Datetime (UTC)'), '%Y-%m-%d %H:%M:%S'
            )
            data_date = isodate.strftime('%Y-%m-%dT%H:%M:%SZ')
            isodate = isodate.strftime('%Y%m%dT%H%M%S')

            try:
                result = float(data_dict['Result'])
            except ValueError:
                result = data_dict['Result']

            identifier = f'{datastream}_{isodate}'
            LOGGER.debug(f'Publishing with ID {identifier}')
            self.output_data[identifier] = {
                '_meta': {
                    'identifier': identifier,
                    'data_date': data_date,
                    'relative_filepath': self.get_local_filepath(data_date)
                },
                'geojson': {
                    'phenomenonTime': data_date,
                    'resultTime': data_date,
                    'result': result,
                    'Datastream': {'@iot.id': datastream},
                    'FeatureOfInterest': {
                        '@iot.id': datastream,
                        'name': location.get('Location'),
                        'description': data_dict.get('Parameter'),
                        'encodingType': 'application/vnd.geo+json',
                        'feature': {
                            'type': 'Point',
                            'coordinates': location['Coordinates']
                        }
                    },
                }
            }

    def publish(self) -> bool:
        LOGGER.info('Publishing output data')

        batch_request = {'requests': []}
        request_id = 0
        for identifier, item in self.output_data.items():
            for format_, the_data in item.items():
                if format_ == '_meta':
                    continue

                LOGGER.debug(f'Processing format: {format_}')

                if the_data is None:
                    msg = f'Empty data for {identifier}-{format_}; not publishing' # noqa
                    LOGGER.warning(msg)
                    continue

                LOGGER.debug('Preparing data for batch request')

                batch_request['requests'].append({
                    'id': str(request_id),
                    'method': 'post',
                    'url': 'Observations',
                    'body': the_data
                })
                request_id += 1

        try:
            r = requests.post(
                f'{API_BACKEND_URL}/$batch',
                data=to_json(batch_request),
                headers={'Content-Type': 'application/json'}
            )

            # Check the response status
            if r.status_code == 200:
                LOGGER.info('Successfully published all data in batch.')
                return True
            else:
                msg = f'Failed to publish data: {r.status_code}, {r.text}'
                LOGGER.error(msg)
                return False
        except Exception as e:
            LOGGER.error(f'Exception occurred: {e}')
            return False

    def __repr__(self):
        return '<ObservationDataCSV>'
