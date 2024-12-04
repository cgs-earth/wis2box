import json
import os
from pathlib import Path
import re
from typing import TypeVar, Type, Union
import geojson.utils
import pandas as pd
import geojson
import requests
from wis2box import data
from wis2box.pitt.types import InsituCSV, PredictionsCSV
import frost_sta_client as fsc
import logging
from frost_sta_client import utils
from dask import dataframe as dd


LOGGER = logging.getLogger(__name__)

T = TypeVar("T", bound=Union[InsituCSV, PredictionsCSV])


def validate_columns(cols: list, typedDict: Type[T]):
    """Make sure that the sheet has the same columns as the typedDict"""
    expectedCols = list(typedDict.__annotations__.keys())
    # take the set difference
    missingCols = set(expectedCols) - set(cols)
    assert (
        len(missingCols) == 0
    ), f"Validation failed: file '{typedDict.__name__}' is missing columns: {missingCols}"


def get_list_of_features(fc: dict) -> dict[int, geojson.Feature]:
    """Take in a dict serialization of a GeoJSON FeatureCollection and return a mapping of the COMID to the feature"""
    features = {}
    for f in fc["features"]:
        feature = geojson.Feature(
            id=f["properties"]["COMID"],
            geometry=f["geometry"],
            properties=f["properties"],
        )
        assert feature.is_valid
        features[f["properties"]["COMID"]] = feature
    return features


def get_first_point_from_feature(feature: geojson.Feature) -> geojson.Point:
    geometry = feature["geometry"]
    assert geometry["type"] == "LineString"
    coords = geometry["coordinates"][0]
    point = geojson.Point((coords[0], coords[1], 0))
    assert point.is_valid
    return point


def parse_geojson(input_file: Path) -> dict[int, geojson.Feature]:
    with open(input_file, "r") as f:
        fc: dict = json.load(f)
        features = get_list_of_features(fc)
        return features


def parse_csv(
    input_file: Path,
    geometry: dict[int, geojson.Feature],
    schema: Type[T],
):
    # Read CSV using Dask
    csv = dd.read_csv(input_file)

    # Validate columns
    validate_columns(csv.columns, schema)

    def get_associated_geometry(df):
        # Get the first value from the COMID column
        comid = int(df["COMID"].iloc[0])
        associated = geometry.get(comid)
        if not associated:
            LOGGER.error(f"COMID {comid} not found in geojson file")
        return associated

    def process_group(comid_group):
        associated_geometry = get_associated_geometry(comid_group)
        if associated_geometry is not None:
            return to_frost(to_sta(associated_geometry, comid_group))
        return None

    # Apply the function to each group by COMID
    results = csv.groupby("COMID").apply(process_group, meta='object')

    # Since apply is lazy, trigger computation to actually process the results
    results.compute()



def to_sta(geometry: geojson.Feature, observations: pd.DataFrame) -> fsc.Thing:
    if not geometry:
        return
    
    COMID = observations[0]["COMID"]
    obsList = []

    for obs in observations.iterrows():
        fscObservation = fsc.Observation(
            result_quality="Prediction",
            feature_of_interest=fsc.FeatureOfInterest(
                name=(COMID),
                description=(COMID),
                encoding_type="application/json",
                # feature=geometry[obs["COMID"]], # associate the full geometry with the feature
                feature="Chlorophyll a prediction at COMID " + str(obs["COMID"]),
            ),
            result=obs["pred"],
            phenomenon_time=f"{obs['date']} 00:00:00Z",
            result_time=f"{obs['date']} 00:00:00Z",
        )

        obsList.append(fscObservation)

    datastream = fsc.Datastream(
        name=f"Chlorophyll a prediction at COMID {COMID}",
        description=f"Chlorophyll a prediction at COMID {COMID}",
        observed_property=fsc.ObservedProperty(
            name="Chlorophyll a prediction",
            definition="Chlorophyll a prediction",
            description="Chlorophyll a prediction",
        ),
        # required
        unit_of_measurement=fsc.UnitOfMeasurement(
            name="micrograms per liter",
            symbol="µg/L",
            definition="micrograms per liter",
        ),
        observation_type="Chlorophyll a prediction",
        observed_area=get_first_point_from_feature(
            geometry[COMID]
        ),  # for the time being we can only display the first point since the map doesnt support line strings
        sensor=fsc.Sensor(
            name="Chlorophyll a prediction based on Landsat",
            description="Chlorophyll a prediction based on Landsat",
            encoding_type="Unknown",
            metadata="Unknown",
        ),
        observations=obsList,
        properties={},
    )

    thing = fsc.Thing(
        name=f"COMID {COMID}",
        description=f"COMID {COMID}",
        locations=[
            fsc.Location(
                name=f"COMID {COMID}",
                encoding_type="application/json",
                description=f"COMID {COMID}",
                location=get_first_point_from_feature(geometry),
                properties={},
            )
        ],
        datastreams=[datastream],
    )
    return thing


def send_to_frost(things: list[fsc.Thing], postAsBatch=False):
    frost_url = os.getenv("WIS2BOX_API_BACKEND_URL")
    if not frost_url:
        raise Exception(
            "Frost API backend env var 'WIS2BOX_API_BACKEND_URL' not defined in env vars"
        )
    service = fsc.SensorThingsService(frost_url)

    if not service:
        raise Exception("Can't connect to FROST API backend")

    if postAsBatch:
        batch = []
        for thing in things:
            jsonVersion = utils.transform_entity_to_json_dict(thing)
            batch.append(
                {
                    "id": thing.name,
                    "method": "post",
                    "url": "Things",
                    "body": jsonVersion,
                }
            )

        payload = json.dumps({"requests": batch})

        resp = requests.post(
            f"{frost_url}/$batch",
            json=payload,
            headers={"Content-Type": "application/json"},
        )

        if resp.status_code != 200:
            raise Exception(resp.text)
    else:
        for thing in things:
            service.things().create(thing)


def assert_in_db(things: list[fsc.Thing]):
    frost_url = os.getenv("WIS2BOX_API_BACKEND_URL")
    service = fsc.SensorThingsService(frost_url)
    thingsInDB = service.locations().query().list().entities
    assert thingsInDB
    existingThings = set(thing.name for thing in thingsInDB)

    assert len(existingThings) >= len(things)

    for thing in things:
        # we use description since the name field returns the name of nativeid in the db
        # not the natural language description
        assert thing._description in existingThings

def to_frost(thing: fsc.Thing):
    if not thing:
        return 
    service = fsc.SensorThingsService(os.getenv("WIS2BOX_API_BACKEND_URL"))
    service.create(thing)