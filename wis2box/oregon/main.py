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

import json
import requests
import io
from typing import Tuple, TypedDict, Optional, List
import click
import logging
from wis2box import cli_helpers, data
from wis2box.api import remove_collection, setup_collection, upsert_collection_item
from urllib.parse import parse_qs, urlencode, urlparse
from requests import Session

from wis2box.api.backend import load_backend
from wis2box.api.backend.sensorthings import SensorthingsBackend
from wis2box.env import API_BACKEND_URL, STORAGE_INCOMING
from wis2box.oregon.cache import ShelveCache
from wis2box.oregon.lib import OregonHttpClient, download_oregon_tsv, generate_phenomenon_time, parse_date, parse_oregon_tsv
from wis2box.oregon.types import ALL_RELEVANT_STATIONS, POTENTIAL_DATASTREAMS, Attributes, OregonHttpResponse, StationData, Datastream
from wis2box.storage import put_data

LOGGER = logging.getLogger(__name__)

THINGS_COLLECTION = "Things"


class OregonStaRequestBuilder():

    relevant_stations: List[int]

    def __init__(self, relevant_stations: List[int]) -> None:
        self.relevant_stations = relevant_stations

    def _get_upstream_data(self) -> list[StationData]:
        """Get metadata for all relevant stations."""
        client = OregonHttpClient()
        # Split the ALL_RELEVANT_STATIONS into two halves since the oregon api can't handle all of them at once
        half_index = len(self.relevant_stations) // 2
        first_half_stations = self.relevant_stations[:half_index]
        second_half_stations = self.relevant_stations[half_index:]

        # Fetch and process the first half of the stations
        first_station_set: OregonHttpResponse = client.fetch_stations(first_half_stations)
        second_station_set: OregonHttpResponse = client.fetch_stations(second_half_stations)
        # create one big dict

        all_stations = first_station_set["features"] + second_station_set["features"]
        return all_stations

    def _get_data_associated_with_station(
        self, station_nbr, start_date, end_date, dataset
    ) -> Tuple[list[float], list[str]]:
        """Ingest all data from a station and return the third column."""
        start_date = "09/25/2024 12:00:00"
        end_date = "09/30/2024 12:00:00"
        response: bytes = download_oregon_tsv(dataset, station_nbr, start_date, end_date)
        # put_data(response, f"{STORAGE_INCOMING}/oregon/{station_nbr}_{dataset}.tsv")
        sensor_values, dates = parse_oregon_tsv(response)
        return (sensor_values, dates)

    def _generate_datastreams_and_observations(
        self, attr: Attributes,
    ) -> Tuple[list[Datastream], list[dict]]:
        datastreams = []
        observations: list[dict] = []
        id = 0
        for stream in POTENTIAL_DATASTREAMS:
            if stream not in attr or str(attr[stream]) != "1":
                continue

            datapoints, observation_times = self._get_data_associated_with_station(
                station_nbr=attr["station_nbr"],
                start_date=attr["period_of_record_start_date"],
                end_date=attr["period_of_record_end_date"],
                dataset=stream,
            )

            for datapoint, obs_time in zip(datapoints, observation_times):
                sta_formatted_data = {
                    "resultTime": obs_time,
                    "Datastream": {"@iot.id": f"{attr['station_nbr']}{id}"},
                    "result": datapoint,
                    "FeatureOfInterest": {
                        "name": attr["station_name"],
                        "description": attr["station_name"],
                        "encodingType": "application/vnd.geo+json",
                        "feature": {
                            "type": "Point",
                            "coordinates": [
                                attr["longitude_dec"],
                                attr["latitude_dec"],
                                attr["elevation"],
                            ],
                        },
                    },
                }
                observations.append(sta_formatted_data)


            time = generate_phenomenon_time(attr)
            property = stream.removesuffix("_available").removesuffix("_avail")
            datastream = {
                "@iot.id": f"{attr['station_nbr']}{id}",
                "name": property,
                "description": property,
                "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
                "unitOfMeasurement": {
                    "name": "Unknown",
                    "symbol": "Unknown",
                    "definition": "Unknown",
                },
                "ObservedProperty": {
                    "name": property,
                    "description": property,
                    "definition": "Unknown",
                },
                "Sensor": {
                    "name": "Unkown",
                    "description": "Unknown",
                    "encodingType": "Unknown",
                    "metadata": "Unknown",
                },
            }
            if time:
                datastream["phenomenonTime"] = time

            datastreams.append(datastream)
            id += 1

        return datastreams, observations


    def send(self) -> None:
        response = self._get_upstream_data()
        self._insert_to_FROST(response)

    def _generate_station_data(self, attr: Attributes, datastreams: list[Datastream]) -> dict:
        """Generate data for the body of a POST request for Locations/ in FROST"""
        return  {
                "name": attr["station_name"],
                "@iot.id": f"{attr['station_nbr']}",
                "description": attr["station_name"],
                "Locations": [
                    {
                        "name": attr["station_name"],
                        "description": attr["station_name"],
                        "encodingType": "application/vnd.geo+json",
                        "location": {
                            "type": "Point",
                            "coordinates": [
                                attr["longitude_dec"],
                                attr["latitude_dec"],
                                attr["elevation"],
                            ],
                        },
                    }
                ],
                "Datastreams": datastreams,
                "properties": {
                    **attr,
                },
            }

    def _insert_to_FROST(self, all_stations: list[StationData]) -> None:

        for station in all_stations:
            attr = station["attributes"]

            sta_datastreams, sta_observations = self._generate_datastreams_and_observations(attr)

            station_data = self._generate_station_data(attr, sta_datastreams)
            upsert_collection_item(THINGS_COLLECTION, station_data)

            if not sta_datastreams and not sta_observations:
                continue

            batch_request = {"requests": []}

            for obs in sta_observations:
                batch_request["requests"].append({
                    "id": obs["Datastream"]["@iot.id"],
                    "method": "post",
                    "url": "Observations",
                    "body": obs
                })

            r = requests.post(
                f"{API_BACKEND_URL}/$batch",
                data=json.dumps(batch_request),
                headers={"Content-Type": "application/json"},
            )

            if r.status_code != 201:
                LOGGER.error(f"Failed to create observation: {r.content}")


# In the load_stations function
@click.command()
@click.pass_context
@click.option("--station", "-s", default="*", help="station identifier")
@click.option("--begin", "-b", help="data start date")
@click.option("--end", "-e", help="data end date")
@cli_helpers.OPTION_VERBOSITY
def load(ctx, verbosity, station, begin, end):
    """Loads stations into sensorthings backend"""
    remove_collection(THINGS_COLLECTION)

    METADATA = {
        "id": THINGS_COLLECTION,
        "title": THINGS_COLLECTION,
        "description": "Oregon Water Resource SensorThings",
        "keywords": ["thing", "oregons"],
        "links": [
            "https://gis.wrd.state.or.us/server/rest/services",
            "https://gis.wrd.state.or.us/server/sdk/rest/index.html#/02ss00000029000000",
        ],
        "bbox": [-180, -90, 180, 90],
        "id_field": "@iot.id",
        "title_field": "name",
    }
    setup_collection(meta=METADATA)

    builder = OregonStaRequestBuilder(relevant_stations=ALL_RELEVANT_STATIONS)
    builder.send()



@click.command()
@click.pass_context
@cli_helpers.OPTION_VERBOSITY
def delete(ctx, verbosity):
    """Delete a collection of stations from the API config and backend"""
    remove_collection(THINGS_COLLECTION)


@click.command()
@click.pass_context
@cli_helpers.OPTION_VERBOSITY
def publish(ctx, verbosity):
    """Publishes the observations and datastreams to the API config and backend"""
    obs_metadata = {
        "id": "Observations",
        "title": "Observations",
        "description": "SensorThings API Observations",
        "keywords": ["observation", "dam"],
        "links": ["https://data.usbr.gov/rise-api"],
        "bbox": [-180, -90, 180, 90],
        "time_field": "resultTime",
        "id_field": "@iot.id",
    }
    setup_collection(meta=obs_metadata)

    datastream_metadata = {
        "id": "Datastreams",
        "title": "Datastreams",
        "description": "SensorThings API Datastreams",
        "keywords": ["datastream", "dam"],
        "links": [
            "https://gis.wrd.state.or.us/server/rest/services",
            "https://gis.wrd.state.or.us/server/sdk/rest/index.html#/02ss00000029000000",
        ],
        "bbox": [-180, -90, 180, 90],
        "id_field": "@iot.id",
        "title_field": "name",
    }

    setup_collection(meta=datastream_metadata)

    click.echo("Done")

@click.group()
def oregon():
    """Station metadata management"""
    pass

oregon.add_command(publish)
oregon.add_command(load)
oregon.add_command(delete)

