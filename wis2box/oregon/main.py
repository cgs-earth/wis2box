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
from wis2box.oregon.lib import OregonClient, generate_phenomenon_time, parse_date
from wis2box.oregon.types import ALL_RELEVANT_STATIONS, POTENTIAL_DATASTREAMS, Attributes, StationData, Datastream
from wis2box.storage import put_data

LOGGER = logging.getLogger(__name__)

THINGS_COLLECTION = "Things"


def get_data_associated_with_station(
    station_nbr, start_date, end_date, dataset
) -> Tuple[list[float], list[str]]:
    """Ingest all data from a station and return the third column."""
    start_date = "8/24/2024 12:00:00"
    end_date = "9/30/2024 12:00:00"
    dataset_param_name = POTENTIAL_DATASTREAMS[dataset]
    base_url = (
        "https://apps.wrd.state.or.us/apps/sw/hydro_near_real_time/hydro_download.aspx"
    )
    tsv_url = f"{base_url}?{urlencode({'station_nbr': station_nbr, 'start_date': start_date, 'end_date': end_date, 'dataset': dataset_param_name, 'format': 'tsv'})}"
    print(tsv_url)
    cache = ShelveCache()
    response, status_code = cache.get_or_fetch(tsv_url, force_fetch=True)

    if status_code != 200:
        raise RuntimeError(f"Request to {tsv_url} failed with status {status_code}")

    put_data(response, f"{STORAGE_INCOMING}/oregon/{station_nbr}_{dataset}.tsv")

    # we just use the third column since the name of the dataset in the
    # url does not match the name in the result column. However,
    # it consistently is returned in the third column
    third_column_data = []
    date_data: list[str] = []
    tsv_data = io.StringIO(response.decode("utf-8"))
    reader = csv.reader(tsv_data, delimiter="\t")
    try:
        # Skip the header row if it exists
        header = next(reader, None)
        if header is not None:
            for row in reader:
                if len(row) >= 3:
                    if row[2] == "":
                        third_column_data.append(None)
                    else:
                        third_column_data.append(float(row[2]))

                date_data.append(parse_date(row[1]))
    except IndexError:
        LOGGER.error(f"Failed to parse {tsv_url}")
        return ([], [])

    return (third_column_data, date_data)


def generate_datastreams_and_observations(
    attr: Attributes,
) -> Tuple[list[Datastream], list[dict]]:
    datastreams = []
    observations: list[dict] = []
    id = 0
    for stream in POTENTIAL_DATASTREAMS:
        if stream not in attr or str(attr[stream]) != "1":
            continue

        datapoints, time_for_datapoints = get_data_associated_with_station(
            station_nbr=attr["station_nbr"],
            start_date=attr["period_of_record_start_date"],
            end_date=attr["period_of_record_end_date"],
            dataset=stream,
        )

        for datapoint, time_for_datapoint in zip(datapoints, time_for_datapoints):
            sta_formatted_data = {
                "resultTime": time_for_datapoint,
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


def process_stations(result: dict):
    features: list[StationData] = result.get("features", [])

    for station in features:
        attr = station["attributes"]

        sta_datastreams, sta_observations = generate_datastreams_and_observations(attr)
        if not sta_datastreams and not sta_observations:
            continue

        station_data = {
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
            "Datastreams": sta_datastreams,
            "properties": {
                **attr,
            },
        }
        upsert_collection_item(THINGS_COLLECTION, station_data)

        for observation in sta_observations:
            r = requests.post(
                f"{API_BACKEND_URL}/Observations",
                data=json.dumps(observation),
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

    client = OregonClient()
    setup_collection(meta=METADATA)

    # Split the ALL_RELEVANT_STATIONS into two halves since the oregon api can't handle all of them at once
    half_index = len(ALL_RELEVANT_STATIONS) // 2
    first_half_stations = ALL_RELEVANT_STATIONS[:half_index]
    second_half_stations = ALL_RELEVANT_STATIONS[half_index:]

    # Fetch and process the first half of the stations
    first_station_set = client.fetch_stations(first_half_stations)
    process_stations(first_station_set)

    # Fetch and process the second half of the stations
    second_station_set = client.fetch_stations(second_half_stations)
    process_stations(second_station_set)


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

