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
import sys
import requests
import io
from typing import Tuple, TypedDict, Optional, List
import click
import logging
from requests import Session
import datetime
from wis2box import cli_helpers, data
from wis2box.api import remove_collection, setup_collection, upsert_collection_item
from urllib.parse import parse_qs, urlencode, urlparse
from requests import Session

from wis2box.api.backend import load_backend
from wis2box.api.backend.sensorthings import SensorthingsBackend
from wis2box.env import API_BACKEND_URL, STORAGE_INCOMING
from wis2box.storage import put_data
from wis2box.util import to_json

LOGGER = logging.getLogger(__name__)

THINGS_COLLECTION = "Things"

ALL_RELEVANT_STATIONS = [
    10378500,
    10392400,
    11491400,
    11494000,
    11494510,
    11495900,
    11497500,
    11497550,
    11500400,
    11500500,
    11502550,
    11503500,
    11504103,
    11504109,
    11504120,
    11510000,
    13214000,
    13215000,
    13216500,
    13217500,
    13269450,
    13273000,
    13275105,
    13275300,
    13281200,
    13282550,
    13317850,
    13318060,
    13318210,
    13318920,
    13325500,
    13329100,
    13329765,
    13330000,
    13330300,
    13330500,
    13331450,
    14010000,
    14010800,
    14021000,
    14022500,
    14023500,
    14024300,
    14025000,
    14026000,
    14029900,
    14031050,
    14031600,
    14032000,
    14032400,
    14039500,
    14054000,
    14056500,
    14060000,
    14063000,
    14064500,
    14070920,
    14070980,
    14073520,
    14074900,
    14075000,
    14076020,
    14076100,
    14079800,
    14080500,
    14081500,
    14082550,
    14083400,
    14085700,
    14087300,
    14088500,
    14095250,
    14095255,
    14104125,
    14104190,
    14104700,
    14104800,
    14105545,
    14105550,
    14192500,
    14193000,
    14202510,
    14202850,
    14306820,
    14306900,
    14320700,
    14327120,
    14327122,
    14327137,
    14327300,
    14335200,
    14335230,
    14335235,
    14335250,
    14335300,
    14335500,
    14336700,
    14337000,
    14340800,
    14341610,
    14342500,
    14343000,
    14346700,
    14346900,
    14347800,
    14348080,
    14348150,
    14348400,
    14350900,
    14352000,
    14352001,
    14354100,
    14354950,
    14355875,
    14357000,
    14357503,
    14358610,
    14358680,
    14358725,
    14358750,
    14358800,
    14360500,
    14363450,
    14365500,
    14368300,
    14375200,
    14400200,
]


class Attributes(TypedDict):
    OBJECTID: int
    lkp_gaging_station_id: int
    station_nbr: str
    station_name: str
    station_status: str
    streamflow_type: str
    source_type: str
    streamcode: str
    longitude_dec: float
    latitude_dec: float
    county_name: str
    state_name: str
    owrd_region: str
    wm_district: int
    hydrologic_unit_code: int
    meridian: Optional[str]
    township: int
    township_char: str
    range: int
    range_char: str
    sctn: int
    qtr160: str
    qtr40: str
    elevation: int
    elevation_datum: Optional[str]
    current_operation_mode: str
    most_recent_operator: str
    cooperators: Optional[str]
    published_area: int
    owrd_area: int
    ws_characteristic: int
    flood_region: Optional[str]
    basin_name: str
    streamflow_type_name: str
    source_type_name: str
    station_status_name: str
    current_operation_mode_name: str
    period_of_record_start_date: int
    period_of_record_end_date: int
    nbr_of_complete_water_years: int
    nbr_of_peak_flow_values: int
    peak_flow_record_start_wy: int
    peak_flow_record_end_wy: int
    near_real_time_web_link: str
    near_real_time_processing: int
    daily_processing: int
    stage_instantaneous_available: int
    flow_instantaneous_available: int
    mean_daily_flow_available: int
    measured_flow_available: int
    volume_midnight_available: int
    stage_midnight_available: int
    mean_daily_volume_available: int
    mean_daily_stage_available: int
    rating_curve_available: int
    water_temp_instantaneous_avail: int
    water_temp_measurement_avail: int
    water_temp_mean_available: int
    water_temp_max_available: int
    water_temp_min_available: int
    air_temp_instantaneous_avail: int
    air_temp_mean_available: int
    air_temp_max_available: int
    air_temp_min_available: int
    precipitation_available: int


class StationData(TypedDict):
    attributes: Attributes
    geometry: dict[str, float]


class UnitOfMeasurement(TypedDict):
    name: str
    symbol: str
    definition: str


class Period(TypedDict):
    EndTime: str
    StartTime: str
    SuppressData: bool
    ReferenceValue: float
    ReferenceValueToTriggerDisplay: Optional[float]


class Threshold(TypedDict):
    Name: str
    Type: str
    Periods: List[Period]
    ReferenceCode: str


class Properties(TypedDict, total=False):
    Thresholds: List[Threshold]
    ParameterCode: Optional[str]
    StatisticCode: Optional[str]
    # Add other optional properties here if needed


Datastream = TypedDict(
    "Datastream",
    {
        # "@iot.selfLink": str,
        "@iot.id": str,
        "name": str,
        "description": str,
        "observationType": str,
        "unitOfMeasurement": UnitOfMeasurement,
        "ObservedProperty": dict[str, str],
        "phenomenonTime": Optional[str],
        "Sensor": dict[str, str],
    },
)


class OregonClient:
    BASE_URL: str = "https://gis.wrd.state.or.us/server/rest/services/dynamic/Gaging_Stations_WGS84/FeatureServer/2/query?"

    def __init__(self):
        self.session = Session()

    def fetch_stations(self, station_numbers: List[int]) -> dict:
        """Fetches stations given a list of station numbers."""
        params = {
            "where": self.format_where_param(station_numbers),
            "outFields": "*",
            "f": "json",
        }
        url = self.BASE_URL + urlencode(params)
        response = self.session.get(url)
        if response.ok:
            return response.json()
        else:
            raise RuntimeError(response.url)

    def format_where_param(self, station_numbers: List[int]) -> str:
        wrapped_with_quotes = [f"'{station}'" for station in station_numbers]
        formatted_stations = " , ".join(wrapped_with_quotes)
        query = f"station_nbr IN ({formatted_stations})"
        return query


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


@click.command()
@click.pass_context
@cli_helpers.OPTION_VERBOSITY
def publish_collection(ctx, verbosity):
    """Publishes collection of stations to API config and backend"""
    setup_collection(meta=METADATA)
    click.echo("Done")


def convert_to_tsv_url(original_url, start_date, end_date, dataset="MDF"):
    # Parse the original URL to get the query parameters
    parsed_url = urlparse(original_url)
    query_params = parse_qs(parsed_url.query)

    # Extract the station number from the original URL
    station_nbr = query_params.get("station_nbr", [None])[0]

    if not station_nbr:
        raise ValueError("station_nbr parameter not found in the original URL")

    # Construct the new URL with TSV format
    base_tsv_url = (
        "https://apps.wrd.state.or.us/apps/sw/hydro_near_real_time/hydro_download.aspx"
    )
    tsv_params = {
        "station_nbr": station_nbr,
        "start_date": start_date,
        "end_date": end_date,
        "dataset": dataset,
        "format": "tsv",
    }

    tsv_url = f"{base_tsv_url}?{urlencode(tsv_params)}"
    return tsv_url


POTENTIAL_DATASTREAMS = [
    "stage_instantaneous_available",
    "flow_instantaneous_available",
    "mean_daily_flow_available",
    "measured_flow_available",
    "volume_midnight_available",
    "stage_midnight_available",
    "mean_daily_volume_available",
    "mean_daily_stage_available",
    "rating_curve_available",
    "water_temp_instantaneous_avail",
    "water_temp_measurement_avail",
    "water_temp_mean_available",
    "water_temp_max_available",
    "water_temp_min_available",
    "air_temp_instantaneous_avail",
    "air_temp_mean_available",
    "air_temp_max_available",
    "air_temp_min_available",
    "precipitation_available",
]


def generate_phenomenon_time(attributes: Attributes) -> Optional[str]:
    if attributes["period_of_record_start_date"] is not None:
        start = datetime.datetime.fromtimestamp(
            attributes["period_of_record_start_date"] / 1000
        ).isoformat()
    else:
        start = ".."  # Default value if start date is None

    if attributes["period_of_record_end_date"] is not None:
        end = datetime.datetime.fromtimestamp(
            attributes["period_of_record_end_date"] / 1000
        ).isoformat()
    else:
        end = ".."  # Default value if end date is None

    phenomenonTime = start + "/" + end if start != ".." and end == ".." else None
    assert phenomenonTime != ""
    return phenomenonTime


def get_data_associated_with_station(
    station_nbr, start_date, end_date, dataset
) -> Tuple[list[float], list[str]]:
    """Ingest all data from a station and return the third column."""
    backend: SensorthingsBackend = load_backend()
    start_date = "9/24/2024 12:00:00"
    end_date = "9/30/2024 12:00:00"
    dataset = "MDF"
    base_url = (
        "https://apps.wrd.state.or.us/apps/sw/hydro_near_real_time/hydro_download.aspx"
    )
    # TODO change MDF to the dataset name; these don't exactly match up however
    tsv_url = f"{base_url}?{urlencode({'station_nbr': station_nbr, 'start_date': start_date, 'end_date': end_date, 'dataset': dataset, 'format': 'tsv'})}"

    # Fetch the URL and download the data
    response = backend.http.get(tsv_url).content
    put_data(response, f"{STORAGE_INCOMING}/oregon/{station_nbr}_{dataset}.tsv")

    # we just use the third column since the name of the dataset in the
    # url does not match the name in the result column. However,
    # it consistently is returned in the third column
    third_column_data = []
    date_data: list[str] = []
    tsv_data = io.StringIO(response.decode("utf-8"))
    reader = csv.reader(tsv_data, delimiter="\t")

    # Skip the header row if it exists
    header = next(reader, None)
    if header is not None:
        for row in reader:
            if len(row) >= 3:
                if row[2] == "":
                    third_column_data.append(1.90)
                else:
                    third_column_data.append(float(row[2]))
            date_data.append(row[1])

    assert any(third_column_data)
    assert any(date_data)
    return (third_column_data, date_data)


def generate_datastreams(attr: Attributes) -> list[Datastream]:
    datastreams = []
    for stream in POTENTIAL_DATASTREAMS:
        if stream not in attr or str(attr[stream]) != "1":
            continue

        time = generate_phenomenon_time(attr)
        property = stream.removesuffix("_available").removesuffix("_avail")
        datastream = {
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
    return datastreams


def publish_datasets_for_station(station: StationData):
    for stream in POTENTIAL_DATASTREAMS:
        attr = station["attributes"]
        if stream not in attr or str(attr[stream]) != "1":
            continue

        datapoints, time_for_datapoints = get_data_associated_with_station(
            station_nbr=station["attributes"]["station_name"],
            start_date=attr["period_of_record_start_date"],
            end_date=attr["period_of_record_end_date"],
            dataset=stream,
        )

        sta_formatted_data = {
            "phenomenonTime": generate_phenomenon_time(attr),
            "resultTime": time_for_datapoints,
            "Datastream": {"@iot.id": stream},
            "result": 1.0,

            "FeatureOfInterest": {
                "@iot.id": stream,
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
        LOGGER.error(f"added data for {attr['station_name']}")
        r = requests.post(
            f"{API_BACKEND_URL}/Observations",
            data=json.dumps(sta_formatted_data),
            headers={"Content-Type": "application/json"},
        )

        if r.status_code != 200:
            raise Exception(r.text)


def process_stations(result: dict):
    features: list[StationData] = result.get("features", [])

    for station in features:
        attr = station["attributes"]
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
            "Datastreams": generate_datastreams(attr),
            "properties": {
                **attr,
            },
        }
        upsert_collection_item(THINGS_COLLECTION, station_data)

        publish_datasets_for_station(station)


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
    """Publishes collection of stations to API config and backend"""
    remove_collection(THINGS_COLLECTION)


@click.group()
def oregon():
    """Station metadata management"""
    pass


oregon.add_command(publish_collection)
oregon.add_command(load)
oregon.add_command(delete)
