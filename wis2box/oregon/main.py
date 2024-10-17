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

import asyncio
from datetime import datetime
import json
import aiohttp
from typing import Tuple, Optional, List
import logging
import requests
from wis2box.api import remove_collection, setup_collection, upsert_collection_item
from wis2box.env import API_BACKEND_URL, STORAGE_INCOMING
from wis2box.oregon.lib import (
    DataUpdateHelper,
    OregonHttpClient,
    assert_valid_date,
    generate_FROST_batch_data,
    generate_oregon_tsv_url,
    generate_phenomenon_time,
    parse_oregon_tsv,
    to_oregon_datetime,
)

from wis2box.oregon.sta_generation import (
    to_sensorthings_datastream,
    to_sensorthings_observation,
    to_sensorthings_station
)

from wis2box.oregon.types import (
    ALL_RELEVANT_STATIONS,
    POTENTIAL_DATASTREAMS,
    START_OF_DATA,
    THINGS_COLLECTION,
    FrostBatchRequest,
    OregonHttpResponse,
    ParsedTSVData,
    StationData,
    Datastream,
)
from wis2box.storage import put_data

LOGGER = logging.getLogger(__name__)


class OregonStaRequestBuilder:
    """
    Helper class for constructing the sensorthings API requests for
    inserting oregon data into the sensorthings FROST server
    """

    relevant_stations: List[int]
    data_start: str
    data_end: str

    def __init__(
        self, relevant_stations: List[int], data_start: str, data_end: str
    ) -> None:
        self.relevant_stations = relevant_stations
        self.data_start = data_start
        self.data_end = data_end

    def _get_upstream_data(self) -> list[StationData]:
        """Get the upstream metadata from the Oregon API for all relevant stations."""
        client = OregonHttpClient()

        # Split the ALL_RELEVANT_STATIONS into two halves since the oregon api can't handle all of them at once
        if len(self.relevant_stations) > 1:
            half_index = len(self.relevant_stations) // 2
            first_half_stations = self.relevant_stations[:half_index]
            second_half_stations = self.relevant_stations[half_index:]

            # Fetch and process the first half of the stations
            first_station_set: OregonHttpResponse = client.fetch_stations(
                first_half_stations
            )
            second_station_set: OregonHttpResponse = client.fetch_stations(
                second_half_stations
            )
            # create one larger dictionary that merges the two
            return first_station_set["features"] + second_station_set["features"]

        # If there's only one station, just fetch it directly since we can't split it
        else:
            return client.fetch_stations(self.relevant_stations)["features"]

    def _generate_datastreams_and_observations(
        self,
        station: StationData,
    ) -> Tuple[list[Datastream], list[dict]]:
        """Given a station, return the datastreams and observations associated with it."""
        attr = station["attributes"]
        datastreams: list[Datastream] = []
        observations: list[dict] = []

        async def __fetch_datastream(stream: str, id: int):
            async with aiohttp.ClientSession() as session:
                LOGGER.debug(f"Fetching {stream} data for station {attr['station_nbr']}")
                tsv_url = generate_oregon_tsv_url(stream, int(attr["station_nbr"]), self.data_start, self.data_end)
                
                response = await session.get(tsv_url)
                tsvParse: ParsedTSVData = parse_oregon_tsv(await response.read())

                for datapoint, obs_time in zip(tsvParse.data, tsvParse.dates):
                    sta_formatted_data = to_sensorthings_observation(
                        attr, datapoint, obs_time, id
                    )
                    observations.append(sta_formatted_data)

                phenom_time = generate_phenomenon_time(tsvParse.dates)
                datastream = to_sensorthings_datastream(attr, tsvParse.units, phenom_time, stream, id)
                datastreams.append(datastream)
            
        tasks = []

        for id, stream in enumerate(POTENTIAL_DATASTREAMS):
            no_stream_available = str(attr[stream]) != "1" or stream not in attr
            if no_stream_available:
                continue
            tasks.append(__fetch_datastream(stream, id))

        async def main():
            return await asyncio.gather(*tasks)

        datastreams = asyncio.run(main())

        LOGGER.info(f"Finished downloading {len(datastreams)} datastreams and {len(observations)} observations")
        return datastreams, observations

    def send(self) -> None:
        batch_body: list[FrostBatchRequest] = []
        for station in self._get_upstream_data():
            LOGGER.info(f"Generating data for station {station['attributes']['station_nbr']}")
            datastreams, sta_observations = self._generate_datastreams_and_observations(
                station
            )
            batch_body.extend(generate_FROST_batch_data(sta_observations))
            stations = to_sensorthings_station(station["attributes"], datastreams)
            upsert_collection_item(THINGS_COLLECTION, stations)

        bytes = json.dumps(batch_body).encode("utf-8")
        put_data(bytes, f"{STORAGE_INCOMING}/frost_batch_request.json")
        r = requests.post(
            f"{API_BACKEND_URL}/$batch",
            data=json.dumps({"requests": batch_body}),
            headers={"Content-Type": "application/json"},
        )
        # Proper status code to check is 201 for POST but sometimes the server returns 200 to signify success
        if r.status_code != 201 and r.status_code != 200:
            raise RuntimeError(
                f"Failed to insert observation into FROST. Got {r.status_code} with content: {r.content}"
            )


def load_data_into_frost(station: int, begin: Optional[str], end: Optional[str]):
    remove_collection(THINGS_COLLECTION)

    METADATA = {
        "id": THINGS_COLLECTION,
        "title": THINGS_COLLECTION,
        "description": "Oregon Water Resource SensorThings",
        "keywords": ["thing", "oregon"],
        "links": [
            "https://gis.wrd.state.or.us/server/rest/services",
            "https://gis.wrd.state.or.us/server/sdk/rest/index.html#/02ss00000029000000",
        ],
        "bbox": [-180, -90, 180, 90],
        "id_field": "@iot.id",
        "title_field": "name",
    }
    setup_collection(meta=METADATA)

    data_range_setter = DataUpdateHelper()

    if not begin:
        begin = START_OF_DATA
    if not end:
        end = to_oregon_datetime(datetime.now())

    data_range_setter.update_range(begin, end)

    if station == "*":
        relevant_stations: list[int] = ALL_RELEVANT_STATIONS
    else:
        relevant_stations: list[int] = [int(station)]

    builder = OregonStaRequestBuilder(
        relevant_stations=relevant_stations, data_start=begin, data_end=end
    )
    builder.send()


def update_data(stations: list[int], new_end: Optional[str]):
    """Update the data in FROST"""
    update_helper = DataUpdateHelper()
    _, end = update_helper.get_range()
    # make sure the start and end are valid dates
    assert_valid_date(end)
    new_start = (
        end  # new start should be set to the previous end in order to only get new data
    )

    if not new_end:
        # Get the current date and time
        new_end = to_oregon_datetime(datetime.now())
        assert_valid_date(new_end)

    builder = OregonStaRequestBuilder(
        relevant_stations=stations, data_start=new_start, data_end=new_end
    )
    LOGGER.error(f"Updating data from {new_start} to {new_end}")
    builder.send()

    update_helper.update_range(new_start, new_end)
