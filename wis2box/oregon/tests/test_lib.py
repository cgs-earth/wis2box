from wis2box.oregon.lib import (
    assert_valid_date,
    download_oregon_tsv,
    parse_oregon_tsv,
    from_oregon_datetime,
)
import datetime
import pytest
from wis2box.oregon.main import OregonStaRequestBuilder
from wis2box.oregon.types import ALL_RELEVANT_STATIONS, POTENTIAL_DATASTREAMS, StationData

def test_download():
    response: bytes = download_oregon_tsv(
        "mean_daily_flow_available",
        "10371500",
        "9/29/2023 12:00:00 AM",
        "10/7/2024 12:00:00 AM",
    )
    assert len(response.decode("utf-8")) > 0

def test_parse_tsv():
    start, end = "9/29/2023 12:00:00 AM",  "10/7/2024 12:00:00 AM"
    response: bytes = download_oregon_tsv(
        "mean_daily_flow_available", "10371500", start, end
    )
    assert len(response.decode("utf-8")) > 0

    dataset, units, dates = parse_oregon_tsv(response)
    assert units == "cfs"
    assert len(dates) == len(dataset)

    # convert to standardized iso format for comparison
    dates = [date.replace("Z", "+00:00") for date in dates]

    # make sure the dates are in the specified range; we need to remove the timezone information from the dates to compare
    assert from_oregon_datetime(start) <= datetime.datetime.fromisoformat(dates[0]).replace(tzinfo=None)
    assert from_oregon_datetime(end) >=  datetime.datetime.fromisoformat(dates[-1]).replace(tzinfo=None)

def test_oregon_dates():
    assert_valid_date("09/25/2024 12:00:00 AM")
    with pytest.raises(ValueError):
        assert_valid_date("09/25/2024")

def test_station_metadata_has_expected_datastreams():
    builder = OregonStaRequestBuilder(ALL_RELEVANT_STATIONS, None, None)
    data: list[StationData] = builder._get_upstream_data()
    assert len(data) == len(ALL_RELEVANT_STATIONS)
    for station in data:
        assert int(station["attributes"]["station_nbr"]) in ALL_RELEVANT_STATIONS
        for attribute in station["attributes"]:
            # the API does not return a list of datastreams explicitly but rather 
            # returns certain datastream names with the "avail" or "available" suffix mapped to 0/1
            if "avail" in attribute:
                assert attribute in POTENTIAL_DATASTREAMS
        