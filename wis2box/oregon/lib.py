import csv
import datetime
import io
import json
import logging
import os
from requests import Session
from urllib.parse import urlencode
from typing import ClassVar, List, Optional, Tuple, TypedDict

from wis2box.oregon.cache import ShelveCache
from wis2box.oregon.types import POTENTIAL_DATASTREAMS, Attributes, OregonHttpResponse

LOGGER = logging.getLogger(__name__)

def parse_oregon_tsv(response: bytes) -> Tuple[list[float], str, list[str]]:
    """Return the data column and the date column for a given tsv response"""
    # we just use the third column since the name of the dataset in the
    # url does not match the name in the result column. However,
    # it consistently is returned in the third column
    third_column_data = []
    date_data: list[str] = []
    units = "Unknown"
    tsv_data = io.StringIO(response.decode("utf-8"))
    reader = csv.reader(tsv_data, delimiter="\t")
    # Skip the header row if it exists
    header = next(reader, None)
        
    if header is not None:
        units = header[2].split("_")[-1]
        for row in reader:
            if len(row) >= 3:
                if row[2] == "":
                    third_column_data.append(None)
                else:
                    third_column_data.append(float(row[2]))

            date_data.append(parse_date(row[1]))

    return (third_column_data, units, date_data)

def generate_phenomenon_time(attributes: Attributes) -> Optional[str]:
    if attributes["period_of_record_start_date"] is not None:
        start = datetime.datetime.fromtimestamp(
            attributes["period_of_record_start_date"] / 1000
        ).isoformat()
    else:
        start = ".."  # Default value if start date is None

    if attributes["period_of_record_end_date"] is not None:
        end = datetime.datetime.fromtimestamp(
            attributes["period_of_record_end_date"] / 1000
        ).isoformat()
    else:
        end = ".."  # Default value if end date is None

    phenomenonTime = start + "/" + end if start != ".." and end == ".." else None
    assert phenomenonTime != ""
    return phenomenonTime


def parse_date(date_str: str) -> str:
    formats = ["%m-%d-%Y %H:%M", "%m-%d-%Y"]
    for fmt in formats:
        try:
            return f"{datetime.datetime.strptime(date_str, fmt).isoformat()}Z"
        except ValueError:
            continue
    raise ValueError(f"Date {date_str} does not match any known formats")

def download_oregon_tsv(dataset: str, station_nbr: str, start_date: str, end_date: str) -> bytes:
    """Get the tsv data for a specific dataset for a specific station in a given date range"""
    dataset_param_name = POTENTIAL_DATASTREAMS[dataset]
    base_url = (
        "https://apps.wrd.state.or.us/apps/sw/hydro_near_real_time/hydro_download.aspx"
    )
    params = {
        "station_nbr": station_nbr,
        "start_date": start_date,
        "end_date": end_date,
        "dataset": dataset_param_name,
        "format": "tsv",
        "units": "" # this is required
    }
    encoded_params = urlencode(params)
    tsv_url = f"{base_url}?{encoded_params}"

    cache = ShelveCache()
    response, status_code = cache.get_or_fetch(tsv_url, force_fetch=False)

    if status_code != 200 or "An Error Has Occured" in response.decode("utf-8"):
        raise RuntimeError(f"Request to {tsv_url} failed with status {status_code} with params {params}")

    return response


class OregonHttpClient:
    BASE_URL: str = "https://gis.wrd.state.or.us/server/rest/services/dynamic/Gaging_Stations_WGS84/FeatureServer/2/query?"

    def __init__(self):
        self.session = Session()

    def fetch_stations(self, station_numbers: List[int]) -> OregonHttpResponse:
        """Fetches stations given a list of station numbers."""
        params = {
            "where": self.format_where_param(station_numbers),
            "outFields": "*",
            "f": "json",
        }
        url = self.BASE_URL + urlencode(params)
        response = self.session.get(url)
        if response.ok:
            return response.json()
        else:
            raise RuntimeError(response.url)

    def format_where_param(self, station_numbers: List[int]) -> str:
        wrapped_with_quotes = [f"'{station}'" for station in station_numbers]
        formatted_stations = " , ".join(wrapped_with_quotes)
        query = f"station_nbr IN ({formatted_stations})"
        return query


def assert_valid_date(date_str: Optional[str]) -> None:
    """defensively assert that a date string is in the proper format for the Oregon API"""
    if not date_str:
        return
    try:
        datetime.datetime.strptime(date_str, "%m/%d/%Y %I:%M:%S %p")
    except ValueError:
        raise ValueError(f"Date string '{date_str}' could not be parsed into the format that the Oregon API expects")

def to_oregon_datetime(date_str: datetime.datetime) -> str:
    """Convert a datetime into the format that the Oregon API expects"""
    return datetime.datetime.strftime(date_str, "%m/%d/%Y %I:%M:%S %p")

def from_oregon_datetime(date_str: str) -> datetime.datetime:
    """Convert a datetime string into a datetime object"""
    return datetime.datetime.strptime(date_str, "%m/%d/%Y %I:%M:%S %p")

class UpdateMetadata(TypedDict):
    data_start: str
    data_end: str

class DataUpdateHelper():
    """Helper class to determine what to download based on a local metadata file"""

    metadata_file: ClassVar[str] = "oregon_metadata.json"

    def __init__(self):
        # check if metadata.json exists if not create it
        if not os.path.exists(self.metadata_file):
            with open(self.metadata_file, "w") as f:
                json.dump({"data_start": "", "data_end": ""}, f)

    def get_range(self) -> Tuple[str, str]:
        """Get the range of data that has been downloaded"""
        with open(self.metadata_file, "r") as f:
            metadata: UpdateMetadata = json.load(f)
        assert_valid_date(metadata["data_start"])
        assert_valid_date(metadata["data_end"])
        return (metadata["data_start"], metadata["data_end"])

    def update_range(self, start: str, end: str):
        """Update the range of dates of data that has been downloaded"""
        # make sure that start and end are valid dates
        assert_valid_date(start)
        assert_valid_date(end)

        with open(self.metadata_file, "r") as f:
            metadata: UpdateMetadata = json.load(f)

        metadata["data_start"] = start
        metadata["data_end"] = end
        with open(self.metadata_file, "w") as f:
            json.dump(metadata, f)
