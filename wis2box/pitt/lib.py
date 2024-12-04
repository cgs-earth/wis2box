import json
import os
from pathlib import Path
import re
from typing import Generator, TypeVar, Type, Union
from attr import assoc
import geojson.utils
import pandas as pd
import geojson
import requests
from wis2box import data
from wis2box.pitt.types import InsituCSV, PredictionsCSV
import frost_sta_client as fsc
import logging
from frost_sta_client import utils 

LOGGER = logging.getLogger(__name__)

T = TypeVar("T", bound=Union[InsituCSV, PredictionsCSV])


def validate_columns(sheet: list, typedDict: Type[T]):
    """Make sure that the sheet has the same columns as the typedDict"""
    expectedCols = list(typedDict.__annotations__.keys())
    gotCols = list(sheet[0].keys())
    # take the set difference
    missingCols = set(expectedCols) - set(gotCols)
    assert (
        len(missingCols) == 0
    ), f"Validation failed: file '{typedDict.__name__}' is missing columns: {missingCols}"


def parse_csv(input_file: Path, schema: Type[T], sort: bool = True) -> Generator[T, None, None]:
    if sort:
        # check if we have it cached
        if Path(f"/tmp/{input_file.name}_sorted.csv").is_file():
            input_file = Path(f"/tmp/{input_file.name}_sorted.csv")
        else:
            # Use Unix `sort` with --header option to keep the first row (header) in place
            os.system(f"tail -n +2 {input_file} | sort -t, -k1,1 > /tmp/sorted_body.csv && head -n 1 {input_file} > /tmp/sorted_header.csv && cat /tmp/sorted_header.csv /tmp/sorted_body.csv > /tmp/{input_file.name}_sorted.csv")
            input_file = Path(f"/tmp/{input_file.name}_sorted.csv")

    csv = pd.read_csv(input_file, chunksize=3000)

    for chunk in csv:
        parsed_data = chunk.to_dict(orient="records")
        # Validate parsed data against schema
        validate_columns(parsed_data, schema)

        yield from parsed_data


def get_list_of_features(fc: dict) -> dict[str, geojson.Feature]:
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

def parse_geojson(input_file: Path) -> dict[str, geojson.Feature]:
    with open(input_file, "r") as f:
        fc: dict = json.load(f)
        features = get_list_of_features(fc)
        return features


def create_thing(obs: list[fsc.Observation], geometry, comid: str) -> fsc.Thing:

    datastream = fsc.Datastream(
        name=f"Chlorophyll a prediction at COMID {comid}",
        description=f"Chlorophyll a prediction at COMID {comid}",
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
        observed_area=get_first_point_from_feature(geometry), # for the time being we can only display the first point since the map doesnt support line strings
        sensor=fsc.Sensor(
            name="Chlorophyll a prediction based on Landsat",
            description="Chlorophyll a prediction based on Landsat",
            encoding_type="Unknown",
            metadata="Unknown",
        ),
        observations=obs,
        properties={},
    )

    return fsc.Thing(
        name=f"COMID {comid}",
        description=f"COMID {comid}",
        locations=[
            fsc.Location(
                name=f"COMID {comid}",
                encoding_type="application/json",
                description=f"COMID {comid}",
                location=get_first_point_from_feature(geometry),
                properties={},
            )
        ],
        datastreams=[datastream],
    )


def to_sta(
    geometry: dict[str, geojson.Feature],
    observations: Generator[PredictionsCSV, None, None],
) -> Generator[fsc.Thing, None, None]:
    
    lastCOMID = None
    observationsForCurrentCOMID = []

    for obs in observations:
        
        currentCOMID = obs["COMID"]
        
        if currentCOMID != lastCOMID:
            associatedGeometry = geometry.get(currentCOMID)
            if not associatedGeometry:
                LOGGER.warning(f"Could not find geometry for COMID {currentCOMID}")
                continue
            thing = create_thing(observationsForCurrentCOMID, associatedGeometry, currentCOMID)
            yield thing 
            observationsForCurrentCOMID = []

        fscObservation = fsc.Observation(
            result_quality="Prediction",
            feature_of_interest=fsc.FeatureOfInterest(
                name=str(obs["COMID"]),
                description=str(obs["COMID"]),
                encoding_type="application/json",
                # feature=geometry[obs["COMID"]], # associate the full geometry with the feature
                feature="Chlorophyll a prediction at COMID " + str(obs["COMID"]),
            ),
            result=obs["pred"],
            phenomenon_time=f"{obs['date']} 00:00:00Z",
            result_time=f"{obs['date']} 00:00:00Z",
        )

        observationsForCurrentCOMID.append(fscObservation)

        lastCOMID = currentCOMID


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

        resp = requests.post(f"{frost_url}/$batch", json=payload, headers={"Content-Type": "application/json"})

        if resp.status_code != 200:
            raise Exception(resp.text)
    else:
        for thing in things:
            service.create(thing)

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
