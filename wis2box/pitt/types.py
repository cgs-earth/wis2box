import os
from typing import TypedDict, Union
from dataclasses import dataclass
import frost_sta_client as fsc
import geojson

class PredictionsCSV(TypedDict):
    COMID: str
    pred: float
    date: str # YYYY-MM-DD

class InsituCSV(TypedDict):
    ID: str
    Date: str
    Method: str 
    Value: int 
    SiteID: str
    geometry: str
    latitude: float 
    longitude: float