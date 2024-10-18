from datetime import datetime
from wis2box.oregon.lib import download_oregon_tsv, parse_oregon_tsv, to_oregon_datetime
import pytest
from collections import Counter

from wis2box.oregon.types import START_OF_DATA

@pytest.mark.parametrize("end_date", ["10/7/2022 12:00:00 AM", "10/7/2024 12:00:00 AM", "4/7/2000 11:00:00 AM"])
def test_no_data_with_no_beginning_date(end_date):
    """It appears that if no beginning date is specified, the data will always be empty"""
    response: bytes = download_oregon_tsv(
        "mean_daily_flow_available", 10371500, start_date="", end_date=end_date 
    )
    assert len(response.decode("utf-8")) > 0

    result = parse_oregon_tsv(response)
    assert len(result.dates) == len(result.data) == 0

@pytest.mark.parametrize("start_date", ["10/7/2023 12:00:00 AM", "10/7/2024 12:00:00 AM", "4/7/2000 11:00:00 AM"])
def test_no_data_with_no_end_date(start_date):
    """If the end date is not specified, the data WILL return up to the current date"""
    response: bytes = download_oregon_tsv(
        "mean_daily_flow_available", 10371500, start_date=start_date, end_date=""
    )
    assert len(response.decode("utf-8")) > 0
    result = parse_oregon_tsv(response)
    assert len(result.dates) == len(result.data) != 0
    assert len(result.dates) == len(result.data) 

@pytest.mark.parametrize("start_date", ["10/7/2023 12:00:00 AM", "10/7/2024 12:00:00 AM", "4/7/2000 11:00:00 AM"])
def test_today_not_same_as_no_end_date(start_date):
    no_end_response: bytes = download_oregon_tsv(
        "mean_daily_flow_available", 10371500, start_date=start_date, end_date=""
    )
    no_end_result = parse_oregon_tsv(no_end_response)

    response: bytes = download_oregon_tsv(
        "mean_daily_flow_available", 10371500, start_date=start_date, end_date=to_oregon_datetime(datetime.now())
    )
    assert len(response.decode("utf-8")) > 0

    today_result = parse_oregon_tsv(response)


    assert today_result.dates != no_end_result.dates
    assert today_result.data != no_end_result.data
    assert today_result.units == no_end_result.units
    assert len(today_result.dates) > len(no_end_result.data)

    isSubset = not (Counter(no_end_result.data) - Counter(today_result.data))
    assert isSubset
    isSubset = not (Counter(no_end_result.dates) - Counter(today_result.dates))
    assert isSubset

def test_very_old_date_same_as_no_start_date():
    response: bytes = download_oregon_tsv(
        "mean_daily_flow_available", 10371500, start_date="", end_date="4/7/2000 11:00:00 AM"
    )
    assert len(response.decode("utf-8")) > 0
    no_start_result = parse_oregon_tsv(response)

    response: bytes = download_oregon_tsv(
        "mean_daily_flow_available", 10371500, start_date="4/7/1800 11:00:00 AM", end_date=""
    )
    assert len(response.decode("utf-8")) > 0
    very_old_result = parse_oregon_tsv(response)
    assert len(very_old_result.dates) == len(very_old_result.data) != 0
    assert len(very_old_result.dates) == len(very_old_result.data)

    assert no_start_result.dates != very_old_result.dates
    assert no_start_result.data != very_old_result.data
    assert no_start_result.units == very_old_result.units
    assert len(no_start_result.dates) < len(very_old_result.data)

    
def test_very_old_dates_are_the_same():
    response: bytes = download_oregon_tsv(
        "mean_daily_flow_available", 10371500, start_date="4/7/1800 11:00:00 AM", end_date="4/7/2000 11:00:00 AM"
    )
    very_old_result_1 = parse_oregon_tsv(response)

    response: bytes = download_oregon_tsv(
        "mean_daily_flow_available", 10371500, start_date="4/7/1850 11:00:00 AM", end_date="4/7/2000 11:00:00 AM" 
    )
    very_old_result_2 = parse_oregon_tsv(response)

    assert very_old_result_1.dates == very_old_result_2.dates
    assert very_old_result_1.data == very_old_result_2.data
    assert very_old_result_1.units == very_old_result_2.units

def test_how_many_observations_in_full_station():
    begin = START_OF_DATA
    end = to_oregon_datetime(datetime.now())

    response: bytes = download_oregon_tsv(
        "mean_daily_flow_available", 10371500, start_date=begin, end_date=end
    )
    result = parse_oregon_tsv(response)
    length = len(result.dates)
    assert length == 56540 == len(result.dates)