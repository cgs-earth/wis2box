from pathlib import Path
from typing import Generator, TypeVar, Type, Union
import geojson.utils
import pandas as pd
import geojson
from wis2box.data import observation
from  wis2box.pitt.types import InsituCSV, PredictionsCSV
import frost_sta_client as fsc

T = TypeVar("T", bound=Union[InsituCSV, PredictionsCSV])

def validate_columns(sheet: list, typedDict: Type[T]):
    """Make sure that the sheet has the same columns as the typedDict"""
    expectedCols = list(typedDict.__annotations__.keys())
    gotCols = list(sheet[0].keys())
    # take the set difference
    missingCols = set(expectedCols) - set(gotCols)
    assert len(missingCols) == 0, f"Validation failed: file '{typedDict.__name__}' is missing columns: {missingCols}"


def parse_csv(input_file: Path, schema: Type[T]) -> Generator[T, None, None]:
    csv = pd.read_csv(input_file, chunksize=1000)

    for chunk in csv:
        parsed_data = chunk.to_dict(orient='records')
        # make sure that parsed_data has the same column names as the associated typedict
        validate_columns(parsed_data, schema)

        yield from parsed_data

def get_list_of_features(fc: geojson.FeatureCollection) -> list[geojson.Feature]:
    features = []
    for f in fc["features"]:
        feature = geojson.Feature(f)
        assert feature.is_valid
        features.append(feature)
    return features
    
def parse_geojson(input_file: Path) -> list[geojson.Feature]:
    with open(input_file, 'r') as f:
        fc = geojson.FeatureCollection(f)
        features = get_list_of_features(fc)
        return features

def to_sta(geometry: list[geojson.Feature], observations: list[PredictionsCSV]) -> list[fsc.Thing]:

    # map the COMID to the thing
    things: dict[str, fsc.Thing] = {}

    for site in geometry:
        thing = fsc.Thing(
            name=f"COMID {site["properties"]["COMID"]}",
            description=site["properties"]["COMID"],
        )

        location = fsc.Location(
            encoding_type="application/json",
            name=f"COMID {site["properties"]["COMID"]}",
            description=site["properties"]["COMID"],
            location=site["geometry"]
        )
        thing.locations = [location]
        things[site["properties"]["COMID"]] = thing


        datastream = fsc.Datastream(
            name=f"Chlorophyll a prediction at COMID {observation['COMID']}",
            description=f"Chlorophyll a prediction at COMID {observation['COMID']}",
            observed_property= fsc.ObservedProperty(
                name="Chlorophyll a prediction",
                definition="Chlorophyll a prediction",
                description="Chlorophyll a prediction",
            ),
            sensor=fsc.Sensor(
                name="Chlorophyll a prediction based on Landsat",
                description="Chlorophyll a prediction based on Landsat",
                encoding_type="Unknown",
                metadata=''
            )
        )

        thing.datastreams = [datastream]
        

        obs = fsc.Observation(
            result_time=observation["resultTime"],