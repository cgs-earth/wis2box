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
import click
import logging
import httpx
from requests import Session
from typing import AsyncGenerator, Dict, Any

from wis2box import cli_helpers
from wis2box.api import setup_collection

LOGGER = logging.getLogger(__name__)


def gcm() -> dict:
    """
    Gets collection metadata for API provisioning

    :returns: `dict` of collection metadata
    """

    return {
        "id": "Datastreams",
        "title": "Datastreams",
        "description": "SensorThings API Datastreams",
        "keywords": ["datastream", "dam"],
        "links": ["https://data.usbr.gov/rise-api"],
        "bbox": [-180, -90, 180, 90],
        "id_field": "@iot.id",
        "title_field": "name",
    }


def fetch_datastreams(station_id: str):
    """
    Load datasets from USBR RISE API

    :returns: `list`, of link relations for all datasets
    """
    http = Session()
    http.headers.update({"accept": "application/vnd.api+json"})

    location = http.get(f"{RISE_URL}/location/{station_id}").json()

    return location["data"]["relationships"]["catalogItems"]["data"]


async def fetch_catalog_item(client: httpx.AsyncClient, dataset_id: str) -> dict:
    """
    Fetch a catalog item from the USBR RISE API asynchronously.

    :param client: An instance of httpx.AsyncClient.
    :param dataset_id: The ID of the dataset.
    :return: The JSON response as a dictionary.
    """
    response = await client.get(f"{USBR_URL}{dataset_id}")
    return response.json()


async def yield_datastreams(datasets: dict) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Yield datasets from USBR RISE API asynchronously.

    :returns: An iterable of link relations for all datasets.
    """
    async with httpx.AsyncClient() as client:
        tasks = [fetch_catalog_item(client, dataset["id"]) for dataset in datasets]
        catalog_items = await asyncio.gather(*tasks)

        for catalog_item in catalog_items:
            attrs = catalog_item["data"]["attributes"]
            if not attrs.get("parameterUnit"):
                continue

            parameter_id = attrs["parameterId"]
            yield {
                "@iot.id": attrs["_id"],
                "name": attrs["itemTitle"],
                "description": attrs["itemDescription"],
                "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",  # noqa
                "properties": {
                    "RISE.selfLink": f"{USBR_URL}{catalog_item['data']['id']}"
                },
                "unitOfMeasurement": {
                    "name": attrs["parameterUnit"],
                    "symbol": attrs["parameterUnit"],
                    "definition": attrs["parameterUnit"],
                },
                "ObservedProperty": {
                    "@iot.id": parameter_id,
                    "name": attrs["parameterName"],
                    "description": attrs["parameterName"],
                    "definition": f"{RISE_URL}/parameter/{parameter_id}",
                },
                "Sensor": {
                    "@iot.id": 0,
                    "name": "Unknown",
                    "description": "Unknown",
                    "encodingType": "Unknown",
                    "metadata": "Unknown",
                },
            }


def load_datastreams(station_id: str):
    """
    Load datasets from USBR RISE API

    :returns: `list`, of link relations for all datasets
    """

    async def get_datastreams():
        return [
            datastream
            async for datastream in yield_datastreams(fetch_datastreams(station_id))
        ]

    return asyncio.run(get_datastreams())


@click.group()
def datastream():
    """Datastream metadata management"""
    pass


@click.command()
@click.pass_context
@cli_helpers.OPTION_VERBOSITY
def publish_collection(ctx, verbosity):
    """Publishes collection of datastreams to API config and backend"""

    setup_collection(meta=gcm())
    click.echo("Done")


datastream.add_command(publish_collection)
