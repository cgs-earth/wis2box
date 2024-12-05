import os
from wis2box.pitt.lib import assert_in_db, parse_csv, parse_geojson, send_to_frost, to_sta
from wis2box.pitt.types import InsituCSV, PredictionsCSV
from pathlib import Path
import logging
import requests

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
    keys = insitu_chla.keys()
    for key in keys:
        assert insitu_chla[key].is_valid

def test_can_ping_service():
    url = os.getenv("WIS2BOX_API_BACKEND_URL")
    assert url
    resp = requests.get(url)
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
