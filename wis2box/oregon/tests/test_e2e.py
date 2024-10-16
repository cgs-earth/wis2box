import os
from wis2box.api import remove_collection
from wis2box.oregon.lib import DataUpdateHelper
from wis2box.oregon.main import THINGS_COLLECTION, load_data_into_frost, update_data
import requests

from wis2box.oregon.types import ALL_RELEVANT_STATIONS

def test_deletion():
    remove_collection(THINGS_COLLECTION)
    api_url= os.environ.get('WIS2BOX_DOCKER_API_URL')
    assert requests.get(f"{api_url}/collections/{THINGS_COLLECTION.lower()}/items?f=json").json()["numberReturned"] == 0


# def test_load_one_station_fully():
#     """Try loading in https://apps.wrd.state.or.us/apps/sw/hydro_near_real_time/display_hydro_graph.aspx?station_nbr=10378500"""
#     remove_collection(THINGS_COLLECTION) # teardown
#     api_url= os.environ.get('WIS2BOX_DOCKER_API_URL')
#     inserted_data_url = f"{api_url}/collections/things/items/10378500"
#     assert requests.get(inserted_data_url).status_code == 500
#     remove_collection(THINGS_COLLECTION)
#     load_data_into_frost(10378500, None, None)
#     # make sure you can ping  http://localhost:8999/oapi/collections/things/items/10378500
#     assert requests.get(inserted_data_url).status_code == 200
    
#     data_insert_helper = DataUpdateHelper()
#     data_insert_helper.get_range()
    
def test_load_one_station_partially():
    """Try loading in https://apps.wrd.state.or.us/apps/sw/hydro_near_real_time/display_hydro_graph.aspx?station_nbr=14026000"""
    remove_collection(THINGS_COLLECTION) # teardown
    api_url= os.environ.get('WIS2BOX_DOCKER_API_URL')
    item = 14026000
    assert item in ALL_RELEVANT_STATIONS
    inserted_data_url = f"{api_url}/collections/things/items/{item}"

    assert requests.get(f"{api_url}/collections/things/items").json()["numberReturned"] == 0

    load_data_into_frost(item, "01/01/2023 12:00:00 AM", "01/15/2023 12:00:00 AM")
    assert requests.get(inserted_data_url).status_code == 200
    date_range = DataUpdateHelper().get_range()
    assert date_range == ("01/01/2023 12:00:00 AM", "01/15/2023 12:00:00 AM")


def test_load_partially_then_update():
    remove_collection(THINGS_COLLECTION) # teardown
    api_url= os.environ.get('WIS2BOX_DOCKER_API_URL')
    item = 14026000
    inserted_data_url = f"{api_url}/collections/things/items/{item}"
    load_data_into_frost(item, "01/01/2024 12:00:00 AM", "01/15/2024 12:00:00 AM")
    assert requests.get(inserted_data_url).status_code == 200
    date_range = DataUpdateHelper().get_range()
    assert date_range == ("01/01/2024 12:00:00 AM", "01/15/2024 12:00:00 AM")
    update_data([item], None)
    assert DataUpdateHelper().get_range() == ("01/01/2024 12:00:00 AM", "01/15/2024 12:00:00 AM")

    