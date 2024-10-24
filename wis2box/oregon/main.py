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
from collections import deque
from datetime import datetime
import itertools
import json
from typing import Coroutine, Optional, List
import logging
import httpx
import requests
from wis2box.api import remove_collection, setup_collection, upsert_collection_item
from wis2box.env import API_BACKEND_URL
from wis2box.oregon.lib import (
    DataUpdateHelper,
    OregonHttpClient,
    assert_valid_date,
    generate_oregon_tsv_url,
    generate_phenomenon_time,
    parse_oregon_tsv,
    to_oregon_datetime,
)

from wis2box.oregon.sta_generation import (
    to_sensorthings_datastream,
    to_sensorthings_observation,
    to_sensorthings_station,
)

from wis2box.oregon.types import (
    ALL_RELEVANT_STATIONS,
    POTENTIAL_DATASTREAMS,
    START_OF_DATA,
    THINGS_COLLECTION,
    Attributes,
    FrostBatchRequest,
    Observation,
    OregonHttpResponse,
    ParsedTSVData,
    StationData,
    Datastream,
)

LOGGER = logging.getLogger(__name__)


def batched_it(iterable, n):
    "Batch data into iterators of length n. The last batch may be shorter."
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while True:
        chunk_it = itertools.islice(it, n)
        try:
            first_el = next(chunk_it)
        except StopIteration:
            return
        yield itertools.chain((first_el,), chunk_it)


# We have to implement this ourselves since wis2box is using py 3.9 and doesn't have this in itertools yet
def batched(iterable, batch_size):
    length = len(iterable)
    for ndx in range(0, length, batch_size):
        yield iterable[ndx : min(ndx + batch_size, length)]


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
            stations = first_station_set["features"] + second_station_set["features"]

        # If there's only one station, just fetch it directly since we can't split it
        else:
            stations = client.fetch_stations(self.relevant_stations)["features"]

        assert len(stations) == len(self.relevant_stations)
        return stations

    def _get_datastreams(
        self,
        station: StationData,
    ) -> list[Datastream]:
        """Given a station, return the datastreams associated with it."""
        attr = station["attributes"]

        datastreams: list[Datastream] = []
        for id, stream in enumerate(POTENTIAL_DATASTREAMS):
            no_stream_available = str(attr[stream]) != "1" or stream not in attr
            if no_stream_available:
                continue

            dummy_start = to_oregon_datetime(datetime.now())
            dummy_end = to_oregon_datetime(datetime.now())
            tsv_url = generate_oregon_tsv_url(
                stream, int(attr["station_nbr"]), dummy_start, dummy_end
            )
            response = requests.get(tsv_url)
            tsvParse: ParsedTSVData = parse_oregon_tsv(response.content)
            phenom_time = generate_phenomenon_time(tsvParse.dates)
            datastreams.append(
                to_sensorthings_datastream(
                    attr, tsvParse.units, phenom_time, stream, id
                )
            )

        return datastreams

    async def _get_observations(self, station: StationData, session: httpx.AsyncClient):
        assert isinstance(station, dict)
        for id, datastream in enumerate(POTENTIAL_DATASTREAMS):
            attr: Attributes = station["attributes"]

            if str(attr[datastream]) != "1" or datastream not in attr:
                continue

            tsv_url = generate_oregon_tsv_url(
                datastream, int(attr["station_nbr"]), self.data_start, self.data_end
            )

            LOGGER.debug(f"Fetching {tsv_url}")
            response = await session.get(tsv_url)
            tsvParse: ParsedTSVData = parse_oregon_tsv(response.read())

            all_observations: list[Observation] = [
                to_sensorthings_observation(attr, datapoint, date, date, id)
                for datapoint, date in zip(tsvParse.data, tsvParse.dates)
            ]
            yield all_observations

    async def send(self) -> None:
        """Send the data to the FROST server"""
        stations = self._get_upstream_data()
        for station in stations:
            LOGGER.info(
                f"Generating data for station {station['attributes']['station_nbr']}"
            )
            datastreams: list[Datastream] = self._get_datastreams(station)
            sta_station = to_sensorthings_station(station, datastreams)
            upsert_collection_item(THINGS_COLLECTION, sta_station)

        async with httpx.AsyncClient(timeout=None) as http_session:
            upload_tasks: list[Coroutine] = []
            for station in stations:

                async def upload_observations(station: StationData) -> None:
                    observation_list = self._get_observations(station, http_session)
                    id: int = 0
                    async for observation_dataset in observation_list:
                        request = {"requests": []}

                        for single_observation in observation_dataset:
                            request_encoded: FrostBatchRequest = {
                                "id": f"{single_observation['Datastream']['@iot.id']}{id}",
                                "method": "post",
                                "url": "Observations",
                                "body": single_observation,
                            }

                            request["requests"].append(request_encoded)

                        LOGGER.info(
                            f"Sending batch observations for {station['attributes']['station_name']} to FROST"
                        )

                        resp = await http_session.post(
                            f"{API_BACKEND_URL}/$batch",
                            json=request,
                            headers={"Content-Type": "application/json"},
                        )

                        # Proper status code to check is 201 for POST but sometimes the server returns 200 to signify success
                        if resp.status_code != 200 and resp.status_code != 201:
                            raise RuntimeError(
                                f"Failed to insert observation into FROST. Got {resp.status_code} with content: {resp.content}"
                            )

                        id += 1

                upload_tasks.append(upload_observations(station))
            await asyncio.gather(*upload_tasks)
            LOGGER.info("Done uploading to FROST")


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

    start_time = datetime.now()
    async def main():
        await builder.send()

    asyncio.run(main())
    end_time = datetime.now()
    duration = round((end_time - start_time).total_seconds() / 60, 3)

    LOGGER.info(f"Data loaded into FROST for stations: {relevant_stations} after {duration} minutes")


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
        new_end = to_oregon_datetime(datetime.now())

    builder = OregonStaRequestBuilder(
        relevant_stations=stations, data_start=new_start, data_end=new_end
    )
    LOGGER.info(f"Updating data from {new_start} to {new_end}")

    async def main():
        await builder.send()

    asyncio.run(main())

    update_helper.update_range(new_start, new_end)
