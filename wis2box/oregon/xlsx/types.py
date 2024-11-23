# This file represents the potential fields inside an Oregon XLSX file
# The column data can be used to create a STA data model

from typing import TypedDict, Union
from dataclasses import dataclass
import frost_sta_client

SiteData = TypedDict(
    "SiteData",
    {
        "Site Name": str,
        "Latitude": float,
        "Longitude": float,
        "Elevation": str, # i.e. 305m
        "Operating/Monitoring/Sampling Organization": str,
        "URI for Site": str,
        "Facililty Type": str,
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

    def extract_sta_datastreams(self):
        pass

    def extract_sta_observations(self):
        observations: list[frost_sta_client.Observation]  = []
        for data in self.dataSheet:

            datastream = frost_sta_client.Datastream(
            )

            obs = frost_sta_client.Observation(
                result_time=data["Result Timestamp"],
                phenomenon_time=data["Sample Timestamp"],
                datastream=data["Associated Metadata Identifier"],
                result=data["Data Value"],
                result_quality=data["Data Quality"],
            )
            observations.append(obs)

    def extract_sta_stations(self):
        locations: list[frost_sta_client.Location] = []
        for site in self.siteDataSheet:
            pass