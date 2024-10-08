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

from typing import ClassVar
import click
import logging
from requests import Session

from wis2box import cli_helpers
from wis2box.api import setup_collection, upsert_collection_item
from wis2box.metadata.datastream import load_datastreams
from wis2box.util import get_typed_value

LOGGER = logging.getLogger(__name__)

THINGS_COLLECTION = "Things"

OREGON_STATIONS = [
    '10378500'
    '10392400'
    '11491400'
    '11494000'
    '11494510'
    '11495900'
    '11497500'
    '11497550'
    '11500400'
    '11500500'
    '11502550'
    '11503500'
    '11504103'
    '11504109'
    '11504120'
    '11510000'
    '13214000'
    '13215000'
    '13216500'
    '13217500'
    '13269450'
    '13273000'
    '13275105'
    '13275300'
    '13281200'
    '13282550'
    '13317850'
    '13318060'
    '13318210'
    '13318920'
    '13325500'
    '13329100'
    '13329765'
    '13330000'
    '13330300'
    '13330500'
    '13331450'
    '14010000'
    '14010800'
    '14021000'
    '14022500'
    '14023500'
    '14024300'
    '14025000'
    '14026000'
    '14029900'
    '14031050'
]

class OregonClient():
    BASE_URL: ClassVar[str] = "https://gis.wrd.state.or.us/server/rest/services/dynamic/Gaging_Stations_WGS84/FeatureServer/2/query?"
    params: dict[str, str] = {
        "where": f"station_nbr IN ({','.join(OREGON_STATIONS)})",
        "outFields": "*",
        "f": "json"
    }

    def __init__(self):
        self.session = Session()
    
    def change_param(self, param: str, value: str):
        self.params[param] = value

    def fetch(self):
        response = self.session.get(self.BASE_URL, params=self.params).json()
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


def handle_location(row) -> None:
    station_identifier = row.pop("station_identifier")
    try:
        datastreams = list(load_datastreams(station_identifier))
        datastreams[0]
    except Exception as err:
        LOGGER.error(f"Unable to publish {station_identifier} - {err}")
        return

    feature = {
        "@iot.id": station_identifier,
        "name": row["station_name"],
        "description": row["station_name"],
        "Locations": [
            {
                "@iot.id": station_identifier,
                "name": row["station_name"],
                "description": row["station_name"],
                "encodingType": "application/vnd.geo+json",
                "location": {
                    "type": "Point",
                    "coordinates": [
                        get_typed_value(row.pop("longitude")),
                        get_typed_value(row.pop("latitude")),
                        get_typed_value(row.pop("elevation")),
                    ],
                },
            }
        ],
        "Datastreams": datastreams,
        "properties": {
            "RISE.selfLink": f"{RISE_URL}/location/{station_identifier}",  # noqa
            **row,
        },
    }

    LOGGER.debug("Publishing to backend")
    upsert_collection_item(THINGS_COLLECTION, feature)


@click.command()
@click.pass_context
@cli_helpers.OPTION_VERBOSITY
def publish_collection(ctx, verbosity):
    """Publishes collection of stations to API config and backend"""
    setup_collection(meta=METADATA)
    click.echo("Done")


@click.command()
@click.pass_context
@cli_helpers.OPTION_VERBOSITY
def load_stations(ctx, verbosity):
    """Caches collection of stations to API config and backend"""
    client = OregonClient()
    print(client.fetch())
        # r = http.get(url)
        # response = r.json()

        # # Extract station data
        # for station in response.get("data", []):
        #     attributes = station["attributes"]
        #     if attributes["locationCoordinates"]["type"] != "Point":
        #         continue

        #     coordinates = attributes["locationCoordinates"]["coordinates"]
        #     try:
        #         station_data = {
        #             "station_identifier": attributes["_id"],
        #             "station_name": attributes["locationName"],
        #             "description": attributes.get("locationDescription", ""),
        #             "latitude": coordinates[1],
        #             "longitude": coordinates[0],
        #             "elevation": attributes["elevation"],
        #             "create_date": attributes["createDate"],
        #             "update_date": attributes["updateDate"],
        #             "timezone": attributes["timezone"],
        #             "type": attributes["locationTypeName"],
        #             "region": ",".join(attributes.get("locationRegionNames", [])),
        #         }
        #     except IndexError as err:
        #         click.echo(err)
        #         click.echo(station)
        #         continue
        #     all_stations.append(station_data)

        # # Get the next URL from the response
        # links = response.get("links", {})
        # url = url_join(USBR_URL, links.get("next")) if "next" in links else None


@click.command()
@click.pass_context
@cli_helpers.OPTION_VERBOSITY
def delete_collection(ctx, verbosity):
    """Publishes collection of stations to API config and backend"""
    pass


@click.group()
def thing():
    """Station metadata management"""
    pass


thing.add_command(publish_collection)
thing.add_command(load_stations)
thing.add_command(delete_collection)
