from wis2box.pitt.lib import parse_csv, parse_geojson
from wis2box.pitt.types import InsituCSV, PredictionsCSV
from pathlib import Path
import logging

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


def test_parse_predictions_csv():

    file = Path(__file__).parent / "rs_chla_predictions.csv"
    predicCSV = parse_csv(
        file, PredictionsCSV
    )
    assert predicCSV

    for chunk in predicCSV:
        assert chunk
        columns = len(chunk.keys())
        assert columns == 3
        assert chunk["COMID"] == 8969898
        assert chunk["date"] == "2022-04-29"
        break # Just test the first row 


def test_parse_geojson_version_of_gpkg():

    file = Path(__file__).parent / "nhd_centerlines.geojson"
    insitu_chla = parse_geojson(file)
    assert insitu_chla