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
import os
from typing import TypedDict, Optional, List
import click
import logging
from requests import Session
import datetime
from wis2box import cli_helpers
from wis2box.api import remove_collection, setup_collection, upsert_collection_item
from urllib.parse import parse_qs, urlencode, urlparse
from requests import Session

from wis2box.auth import delete_token

LOGGER = logging.getLogger(__name__)

THINGS_COLLECTION = "Things"

ALL_RELEVANT_STATIONS = [
    "10378500",
    "10392400",
    "11491400",
    "11494000",
    "11494510",
    "11495900",
    "11497500",
    "11497550",
    "11500400",
    "11500500",
    "11502550",
    "11503500",
    "11504103",
    "11504109",
    "11504120",
    "11510000",
    "13214000",
    "13215000",
    "13216500",
    "13217500",
    "13269450",
    "13273000",
    "13275105",
    "13275300",
    "13281200",
    "13282550",
    "13317850",
    "13318060",
    "13318210",
    "13318920",
    "13325500",
    "13329100",
    "13329765",
    "13330000",
    "13330300",
    "13330500",
    "13331450",
    "14010000",
    "14010800",
    "14021000",
    "14022500",
    "14023500",
    "14024300",
    "14025000",
    "14026000",
    "14029900",
    "14031050",
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
        self.params = {
            "where": self.format_where_param(),
            "outFields": "*",
            "f": "json",
        }
        self.session = Session()

    def format_where_param(self) -> str:
        wrapped_with_quotes = [f"'{station}'" for station in ALL_RELEVANT_STATIONS]
        formatted_stations = ",".join(wrapped_with_quotes)
        return f"station_nbr IN ({formatted_stations})"

    def change_param(self, param: str, value: str):
        self.params[param] = value

    def fetch(self) -> dict:
        url = self.BASE_URL + urlencode(self.params)
        print(url)
        response = self.session.get(url).json()
        return response


METADATA = {
    "id": THINGS_COLLECTION,
    "title": THINGS_COLLECTION,
    "description": "SensorThings API Things",
    "keywords": ["thing", "dam"],
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
    "water_temp_instantaneous_available",
    "water_temp_measurement_available",
    "water_temp_mean_available",
    "water_temp_max_available",
    "water_temp_min_available",
    "air_temp_instantaneous_available",
    "air_temp_mean_available",
    "air_temp_max_available",
    "air_temp_min_available",
    "precipitation_available",
]

# geometry = station["geometry"]
# points = [float(value) for value in geometry.values()]
ESRI_TO_GEOJSON_SPECIFIER = {
    "esriGeometryPoint": "Point",
}

def generate_phenomenon_time(attributes: Attributes) -> str:
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

    phenomenonTime = (
        start + "/" + end if start != ".." and end == ".." else ""
    )
    return phenomenonTime


@click.command()
@click.pass_context
@cli_helpers.OPTION_VERBOSITY
def load_stations(ctx, verbosity):
    """Caches collection of stations to API config and backend"""
    remove_collection(THINGS_COLLECTION)

    if os.path.exists("stations.json"):
        print("Using cached stations")
        with open("stations.json", "r") as fh:
            result = json.load(fh)
    else: 
        client = OregonClient()
        result = client.fetch()

        with open("stations.json", "w") as fh:
            json.dump(result, fh)

    features: list[StationData] = result.get("features", [])
    setup_collection(meta=METADATA)

    for station in features:
        attr = station["attributes"]

        # example datastream located at
        # https://gis.wrd.state.or.us/server/rest/services/dynamic/Gaging_Stations_WGS84/FeatureServer/2/query?where=station_nbr+IN+%28%2714211814%27%2C+%2713331450%27%29&outFields=*&f=json

        datastreams = []
        for stream in POTENTIAL_DATASTREAMS:
            if stream not in attr:
                continue
            
            if not str(attr[stream]) == "1":
                continue 

            # datastream: Datastream = {
            #     "description": stream,
            #     "name": stream,
            #     "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
            #     "unitOfMeasurement": {
            #         "name": "Degree Celsius",
            #         "symbol": "degC",
            #         "definition": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Observation",
            #     },
            #     "ObservedProperty": {
            #         "name": "Area Temperature",
            #         "description": "The degree or intensity of heat present in the area",
            #         "definition": "http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html#AreaTemperature"
            #     },
            #     "phenomenonTime": generate_phenomenon_time(attr),
            #     "Sensor": {
            #         "name": "Unknown",
            #         "description": "Unknown",
            #         "encodingType": "application/vnd.geo+json",
            #         "metadata": "Unknown",
            #     },
            # }
            datastream =  {
                    "name": stream.removesuffix("_available").removesuffix("_avail"),
                    "description": stream.removesuffix("_available").removesuffix("_avail"),
                    "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
                    "unitOfMeasurement": {
                    "name": "Degree Celsius",
                    "symbol": "degC",
                    "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html#DegreeCelsius"
                    },
                    "ObservedProperty": {
                    "name": "Area Temperature",
                    "description": "The degree or intensity of heat present in the area",
                    "definition": "http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html#AreaTemperature"
                    },
                    "Sensor": {
                    "name": "DHT22",
                    "description": "DHT22 temperature sensor",
                    "encodingType": "application/pdf",
                    "metadata": "https://cdn-shop.adafruit.com/datasheets/DHT22.pdf"
                    }
                }

            datastreams.append(datastream)

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
            "Datastreams": datastreams,
            "properties": {
                "Deployment Condition": "test",
                "Deployment Method": "test",
            },
        }
        LOGGER.debug(f"Publishing feature for {attr['station_name']} to backend")
        upsert_collection_item(THINGS_COLLECTION, station_data, method="POST")

        


@click.command()
@click.pass_context
@cli_helpers.OPTION_VERBOSITY
def delete_collection(ctx, verbosity):
    """Publishes collection of stations to API config and backend"""
    remove_collection(THINGS_COLLECTION)


@click.group()
def thing():
    """Station metadata management"""
    pass


thing.add_command(publish_collection)
thing.add_command(load_stations)
thing.add_command(delete_collection)
