import os
from wis2box.pitt.lib import assert_in_db, parse_csv, parse_geojson, send_to_frost, to_sta
from wis2box.pitt.types import InsituCSV, PredictionsCSV
from pathlib import Path
import logging
import frost_sta_client as fsc
import requests
from frost_sta_client import utils 
import json

LOGGER = logging.getLogger(__name__)

def test_parse_csv():

    file = Path(__file__).parent / "insitu_chla.csv"
    insitu_chla = parse_csv(
        file, InsituCSV
    )
    for chunk in insitu_chla:
        assert insitu_chla
        columns = len(chunk.keys())
        assert columns == 10
        assert "SiteID" in chunk.keys()
        # sanity checks on a few of the data points
        assert chunk["ID"] == 1
        assert chunk["longitude"] == 36.09989
        break # Just test the first row 


def test_parse_geojson_version_of_gpkg():

    file = Path(__file__).parent / "nhd_centerlines.geojson"
    insitu_chla = parse_geojson(file)
    assert insitu_chla

def test_can_ping_service():
    resp = requests.get(os.getenv("WIS2BOX_API_BACKEND_URL"))
    assert resp.ok

def test_sta():

    geometry = Path(__file__).parent / "nhd_centerlines.geojson"
    geometry = parse_geojson(geometry)
    observations = Path(__file__).parent / "abbreviated_predictions.csv"
    observations = parse_csv(observations, PredictionsCSV, sort=True)

    things = to_sta(geometry, observations)
    assert things
    for thing in things:
        assert thing.datastreams
        for datastream in thing.datastreams:
            assert datastream.observations, f"Datastream {datastream.name} has no observations"

def test_e2e():

    geometry = Path(__file__).parent / "nhd_centerlines.geojson"
    geometry = parse_geojson(geometry)
    observations = Path(__file__).parent / "rs_chla_predictions.csv"
    observations = parse_csv(observations, PredictionsCSV)

    url = os.getenv("WIS2BOX_API_BACKEND_URL")
    service = fsc.SensorThingsService(url) 
    for thing in to_sta(geometry, observations):
        assert thing.datastreams

        jsonVersion = fsc.utils.transform_entity_to_json_dict(thing)
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }
        resp = requests.post(
            f"{url}/Things",
            json= jsonVersion,
            headers= headers
        )

        if not resp.ok:
            file_name = f"failed_{thing.id}.json"  # You can customize the filename
            with open(file_name, 'w') as file:
                file.write(json.dumps(jsonVersion))
            raise Exception(resp.text)


        # service.things().create(thing)