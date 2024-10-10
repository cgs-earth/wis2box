import datetime
from requests import Session
from urllib.parse import urlencode
from typing import List, Optional

from wis2box.oregon.types import Attributes



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

class OregonClient:
    BASE_URL: str = "https://gis.wrd.state.or.us/server/rest/services/dynamic/Gaging_Stations_WGS84/FeatureServer/2/query?"

    def __init__(self):
        self.session = Session()

    def fetch_stations(self, station_numbers: List[int]) -> dict:
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
