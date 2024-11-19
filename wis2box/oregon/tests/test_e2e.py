import asyncio
import datetime
import os

import httpx
from wis2box.api import remove_collection
from wis2box.oregon.helper_classes import CrawlResultStore
from wis2box.oregon.lib import to_oregon_datetime
from wis2box.oregon.main import THINGS_COLLECTION, OregonStaRequestBuilder, load_data_into_frost, update_data
import requests
import logging
from wis2box.oregon.types import ALL_RELEVANT_STATIONS

LOGGER = logging.getLogger(__name__)


def test_deletion():
    remove_collection(THINGS_COLLECTION)
    api_url= os.environ.get('WIS2BOX_DOCKER_API_URL')
    assert requests.get(f"{api_url}/collections/{THINGS_COLLECTION.lower()}/items?f=json").json()["numberReturned"] == 0

def test_load_one_station_fully():
    """Try loading in https://apps.wrd.state.or.us/apps/sw/hydro_near_real_time/display_hydro_graph.aspx?station_nbr=10378500"""
    remove_collection(THINGS_COLLECTION) # teardown
    api_url= os.environ.get('WIS2BOX_DOCKER_API_URL')
    inserted_data_url = f"{api_url}/collections/things/items/10378500"
    assert requests.get(inserted_data_url).status_code == 500
    remove_collection(THINGS_COLLECTION)
    load_data_into_frost([10378500], None, None)
    # make sure you can ping  http://localhost:8999/oapi/collections/things/items/10378500
    assert requests.get(inserted_data_url).status_code == 200
    
def test_load_one_station_partially():
    """Try loading in https://apps.wrd.state.or.us/apps/sw/hydro_near_real_time/display_hydro_graph.aspx?station_nbr=14026000"""
    remove_collection(THINGS_COLLECTION) # teardown
    api_url= os.environ.get('WIS2BOX_DOCKER_API_URL')
    item = 14026000
    assert item in ALL_RELEVANT_STATIONS
    inserted_data_url = f"{api_url}/collections/things/items/{item}"

    assert requests.get(f"{api_url}/collections/things/items").json()["numberReturned"] == 0

    load_data_into_frost([item], "01/01/2023 12:00:00 AM", "01/15/2023 12:00:00 AM")
    assert requests.get(inserted_data_url).status_code == 200
    date_range = CrawlResultStore().get_range()
    assert date_range == ("01/01/2023 12:00:00 AM", "01/15/2023 12:00:00 AM")


def test_load_partially_then_update():
    remove_collection(THINGS_COLLECTION) # teardown
    api_url= os.environ.get('WIS2BOX_DOCKER_API_URL')
    item = 14026000
    inserted_data_url = f"{api_url}/collections/things/items/{item}"

    load_data_into_frost([item], "01/01/2024 12:00:00 AM", "01/15/2024 12:00:00 AM")
    assert requests.get(inserted_data_url).status_code == 200
    date_range = CrawlResultStore().get_range()
    assert date_range == ("01/01/2024 12:00:00 AM", "01/15/2024 12:00:00 AM")

    update_time = to_oregon_datetime(datetime.datetime.now())


    LOGGER.info(f"Updating data to contain data from {update_time}")
    update_data([item],update_time)

    assert CrawlResultStore().get_range() == ("01/01/2024 12:00:00 AM", update_time)
    new_data= requests.get(f"{api_url}/collections/observations/items/158205")
    assert new_data.status_code == 200
    new_result: str = new_data.json()["properties"]["resultTime"]
    date = datetime.datetime.fromisoformat(new_result.replace("Z", "+00:00"))
    # make sure the date is within the last month; rough estimate. just making sure the update got new data
    assert date > datetime.datetime.now() - datetime.timedelta(days=30)


def test_get_many_observations_in_async():
    """Check to make sure that the server doesn't kick us out if we try to make too many requests to observation endpoints asynchronously"""
    async def main():
        builder = OregonStaRequestBuilder(ALL_RELEVANT_STATIONS, "01/01/1850 12:00:00 AM", "01/15/2024 12:00:00 AM")
        stationMetadata = builder._get_upstream_data()
        tasks = []
        async with httpx.AsyncClient(timeout=None) as client:
            for station in stationMetadata:

                async def get_observations():
                    LOGGER.info(f"Processing {station['attributes']['station_nbr']}")
                    res = builder._get_observations(station, client)
                    async for _ in res:
                        pass # consume the generator

                tasks.append(asyncio.create_task(get_observations()))
            await asyncio.gather(*tasks)

    asyncio.run(main())