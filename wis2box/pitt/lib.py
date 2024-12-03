import json
import os
from pathlib import Path
from typing import Generator, TypeVar, Type, Union
from attr import assoc
import geojson.utils
import pandas as pd
import geojson
from wis2box import data
from wis2box.pitt.types import InsituCSV, PredictionsCSV
import frost_sta_client as fsc
import logging

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


def parse_csv(input_file: Path, schema: Type[T]) -> Generator[T, None, None]:
    csv = pd.read_csv(input_file, chunksize=1000)

    for chunk in csv:
        parsed_data = chunk.to_dict(orient="records")
        # make sure that parsed_data has the same column names as the associated typedict
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


def parse_geojson(input_file: Path) -> dict[str, geojson.Feature]:
    with open(input_file, "r") as f:
        fc: dict = json.load(f)
        features = get_list_of_features(fc)
        return features


def to_sta(
    geometry: dict[str, geojson.Feature],
    observations: Generator[PredictionsCSV, None, None],
) -> list[fsc.Thing]:
    observationMapping: dict[str, list[fsc.Observation]] = {}
    # map the COMID to the datastream
    datastreamMapping: dict[str, fsc.Datastream] = {}

    observations = list(observations)

    for obs in observations:
        fscObservation = fsc.Observation(
            result_quality="Prediction",
            feature_of_interest=fsc.FeatureOfInterest(
                name=str(obs["COMID"]),
                description=str(obs["COMID"]),
                encoding_type="application/json",
                feature="Chlorophyll a prediction",
            ),
            result=obs["pred"],
            phenomenon_time=f"{obs['date']} 00:00:00Z",
            result_time=f"{obs['date']} 00:00:00Z",
        )

        associatedObservations = observationMapping.get(obs["COMID"])
        if associatedObservations:
            associatedObservations.append(fscObservation)
        else:
            observationMapping[obs["COMID"]] = [fscObservation]



    for obs in observations:
        # If it is already a datastream, skip it
        if obs["COMID"] in datastreamMapping.keys():
            continue
        elif obs["COMID"] is None:
            continue

        associatedObservation = observationMapping[obs["COMID"]]

        datastream = fsc.Datastream(
            name=f"Chlorophyll a prediction at COMID {obs['COMID']}",
            description=f"Chlorophyll a prediction at COMID {obs['COMID']}",
            observed_property=fsc.ObservedProperty(
                name="Chlorophyll a prediction",
                definition="Chlorophyll a prediction",
                description="Chlorophyll a prediction",
            ),
            # required
            unit_of_measurement=fsc.UnitOfMeasurement(
                name="Chlorophyll a prediction",
                symbol="Chlorophyll a prediction",
                definition="Chlorophyll a prediction",
            ),
            observation_type="Chlorophyll a prediction",
            observed_area=geometry[obs["COMID"]],
            sensor=fsc.Sensor(
                name="Chlorophyll a prediction based on Landsat",
                description="Chlorophyll a prediction based on Landsat",
                encoding_type="Unknown",
                metadata="Unknown",
            ),
            observations=associatedObservation,
            properties={},
        )
        datastreamMapping[obs["COMID"]] = datastream

    # map the COMID to the thing
    thingsMapping: dict[str, fsc.Thing] = {}
    # Create the things objects based on the geojson
    for comid, geo in geometry.items():
        associatedDatastream = datastreamMapping.get(comid)
        if not associatedDatastream:
            continue
        thingsMapping[comid] = fsc.Thing(
            name=f"{comid}",
            description=f"COMID {comid}",
            locations=[
                fsc.Location(
                    name=f"COMID {comid}",
                    encoding_type="application/json",
                    description=f"COMID {comid}",
                    location=geo,
                    properties={},
                )
            ],
            datastreams=[associatedDatastream],
        )

    return list(thingsMapping.values())


def send_to_frost(things: list[fsc.Thing]):
    frost_url = os.getenv("WIS2BOX_API_BACKEND_URL")
    if not frost_url:
        raise Exception(
            "Frost API backend env var 'WIS2BOX_API_BACKEND_URL' not defined in env vars"
        )
    service = fsc.SensorThingsService(frost_url)

    if not service:
        raise Exception("Can't connect to FROST API backend")

    for thing in things:
        if not thing.datastreams:
            LOGGER.warning(f"Thing {thing.name} has no datastreams. Skipping...")
            continue

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
