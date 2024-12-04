import os
from wis2box.pitt.lib import assert_in_db, parse_csv, parse_geojson, send_to_frost, to_sta
from wis2box.pitt.types import InsituCSV, PredictionsCSV
from pathlib import Path
import logging
import frost_sta_client as fsc

LOGGER = logging.getLogger(__name__)


def test_parse_geojson_version_of_gpkg():

    file = Path(__file__).parent / "nhd_centerlines.geojson"
    insitu_chla = parse_geojson(file)
    assert insitu_chla



def test_parse_predictions_csv():

    nhd_centerlines = Path(__file__).parent / "nhd_centerlines.geojson"
    nhd_centerlines = parse_geojson(nhd_centerlines)

    file = Path(__file__).parent / "abbreviated_predictions.csv"
    predicCSV = parse_csv(
        file, nhd_centerlines, PredictionsCSV
    )

    

def test_sta():

    geometry = Path(__file__).parent / "nhd_centerlines.geojson"
    geometry = parse_geojson(geometry)
    observations = Path(__file__).parent / "abbreviated_predictions.csv"
    observations = parse_csv(observations, PredictionsCSV)

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
    observations = parse_csv(observations, PredictionsCSV, sort_by="COMID")

    things = to_sta(geometry, observations)
    assert things
    send_to_frost(things)
    assert_in_db(things)