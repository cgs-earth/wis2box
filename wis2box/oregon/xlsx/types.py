# This file represents the potential fields inside an Oregon XLSX file
# The column data can be used to create a STA data model

import os
from typing import TypedDict, Union
from dataclasses import dataclass
import frost_sta_client as fsc
import geojson

SiteData = TypedDict(
    "SiteData",
    {
        "Site Name": str,
        "Latitude": float,
        "Longitude": float,
        "Elevation": str, # i.e. 305m
        "Operating/Monitoring/Sampling Organization": str,
        "Native ID": str,
        "URI for Site": str,
        "Facility Type": str,
        "Feature of Interest Type": str,
        "Feature of Interest Name": str,
        "Feature of Interest URI": str
    },
)

Metadata = TypedDict(
    "Metadata",
    {
        "Metadata Indetifier": str,
        "Associated Site Identifier": str,
        "Observed Property Name": str,
        "Observed Property URI": str,
        "Value Type": str,
        "Units Name": str,
        "Units URI": str,
        "Sampling Method Name": str,
        "Sample Fraction": str,
        "Sampling Method URI": str,
        "Analytical Method Name": str,
        "Analytical Method URI": str,
        "Detection Limits": str,
        "Accuracy Bounds": str,
        "Data Status": str
    }
)

Data = TypedDict(
    "Data",
    {
        "Associated Metadata Identifier": str,
        "Sample Timestamp": str,
        "Result Timestamp": str,
        "Data Value": Union[float, int, str],
        "Comments": str,
        "Data Quality": str
    }
)

@dataclass
class OregonXLSX:
    siteDataSheet: list[SiteData]  # data about the site/location/station
    metadataSheet: list[Metadata] # data about datastreams
    dataSheet: list[Data] # timeseries data

    def to_sta(self) -> list[fsc.Thing]:
        
        # Map the Native ID to the thing
        # use hashtable for quick lookup when merging
        things: dict[str, fsc.Thing] = {}

        for site in self.siteDataSheet:

            # elevation is in meters above sea level
            # but FROST doesn't allow the extra m so we remove it
            elevation = int(site["Elevation"].removesuffix("m"))
            point = geojson.Point((site["Longitude"], site["Latitude"], elevation))

            location = fsc.Location(
                name=site["Site Name"],
                location=point,
                properties={
                    "Operating/Monitoring/Sampling Organization": site["Operating/Monitoring/Sampling Organization"],
                    "Native ID": site["Native ID"],
                    "URI for Site": site["URI for Site"],
                    "Facility Type": site["Facility Type"],
                    "Feature of Interest Type": site["Feature of Interest Type"],
                    "Feature of Interest Name": site["Feature of Interest Name"],
                    "Feature of Interest URI": site["Feature of Interest URI"]
                }
            )
            # Need to create a dummy thing only for associating the location
            thing = fsc.Thing(name=site["Native ID"])
            thing.locations = [location]
            things[site["Native ID"]] = thing
        
        datastreams: dict[str, fsc.Datastream] = {}
    
        for metadata in self.metadataSheet:

            associatedThing = things[metadata["Associated Site Identifier"]]
            if not associatedThing:
                raise Exception(f"Associated thing {metadata['Associated Site Identifier']} not found")

            def getObservedArea(thing: fsc.Thing):
                """Get the geojson representation of a thing's location. Gets the 0th location since it is 
                assumed that there is only one location per thing."""
                if thing.locations:
                    res =  thing.locations.get(0).location
                    return res
                else:
                    return None

            datastream = fsc.Datastream(
                name=metadata["Observed Property Name"],
                # Use the metadata identifier to get the associated observations
                observation_type=metadata["Value Type"],
                unit_of_measurement=fsc.UnitOfMeasurement(name=metadata["Units Name"], definition=metadata["Units URI"]),
                observed_area=getObservedArea(associatedThing),
                observed_property= fsc.ObservedProperty(
                    name=metadata["Observed Property Name"],
                    definition=metadata["Observed Property URI"],
                ),
                properties={
                    "Accuracy Bounds": metadata["Accuracy Bounds"],
                    "Data Status": metadata["Data Status"],
                    "Detection Limits": metadata["Detection Limits"],
                    "Analytical Method Name": metadata["Analytical Method Name"],
                    "Analytical Method URI": metadata["Analytical Method URI"],
                    "Sampling Method Name": metadata["Sampling Method Name"],
                    "Sample Fraction": metadata["Sample Fraction"],
                    "Value Type": metadata["Value Type"],
                }
            )

            datastreams[metadata["Metadata Indetifier"]] = datastream

            if not associatedThing.datastreams:
                associatedThing.datastreams = [datastream]
            else:
                associatedThing.datastreams = [*associatedThing.datastreams, datastream]

        for data in self.dataSheet:

            associatedDatastream = datastreams[data["Associated Metadata Identifier"]]

            obs = fsc.Observation(
                result_time=data["Result Timestamp"],
                phenomenon_time=data["Sample Timestamp"],
                datastream=associatedDatastream,
                result=data["Data Value"],
                result_quality=data["Data Quality"],
            )

            if not associatedDatastream.observations:
                associatedDatastream.observations = [obs]
            else:
                associatedDatastream.observations = [*associatedDatastream.observations, obs]



        return list(things.values())
    
    def send_to_frost(self, things: list[fsc.Thing]):
        frost_url = os.getenv("WIS2BOX_API_BACKEND_URL")
        if not frost_url:
            raise Exception("Frost API backend env var 'WIS2BOX_API_BACKEND_URL' not defined in env vars")
        service = fsc.SensorThingsService(frost_url)

        if not service:
            raise Exception("Can't connect to FROST API backend")
        
        for thing in things:
            service.create(thing)