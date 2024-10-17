import json
from wis2box.api import  remove_collection, upsert_collection_item
from wis2box.env import STORAGE_INCOMING
from wis2box.oregon.main import OregonStaRequestBuilder
from wis2box.oregon.types import ALL_RELEVANT_STATIONS, THINGS_COLLECTION
from wis2box.storage import put_data


def test_upsert():
    remove_collection(THINGS_COLLECTION)
    data = {
        "name": "HONEY CR NR PLUSH, OR",
        "@iot.id": 10378500,
        "description": "HONEY CR NR PLUSH, OR",
        "Locations": [
            {
                "name": "HONEY CR NR PLUSH, OR",
                "description": "HONEY CR NR PLUSH, OR",
                "encodingType": "application/vnd.geo+json",
                "location": {
                    "type": "Point",
                    "coordinates": [-119.922309, 42.424893, 4550.0],
                },
            }
        ],
        "Datastreams": [
            {
                "@iot.id": 93,
                "name": "HONEY CR NR PLUSH, OR stage_instantaneous",
                "description": "stage_instantaneous",
                "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
                "unitOfMeasurement": {"name": "ft", "symbol": "ft", "definition": "ft"},
                "ObservedProperty": {
                    "name": "stage_instantaneous",
                    "description": "stage_instantaneous",
                    "definition": "Unknown",
                },
                "Sensor": {
                    "@iot.id": 700,
                    "name": "Unknown",
                    "description": "Unknown",
                    "encodingType": "Unknown",
                    "metadata": "Unknown",
                },
                "phenomenonTime": "2023-10-01T00:00:00+00:00/2023-10-02T23:45:00+00:00",
                "resultTime": "2023-10-01T00:00:00+00:00/2023-10-02T23:45:00+00:00",
            }
        ],
        "properties": {
            "OBJECTID": 39,
            "lkp_gaging_station_id": 100,
            "station_nbr": "10378500",
            "station_name": "HONEY CR NR PLUSH, OR",
            "station_status": "A",
            "streamflow_type": "R",
            "source_type": "S",
            "streamcode": "1300800700",
            "longitude_dec": -119.922309,
            "latitude_dec": 42.424893,
            "county_name": "Lake",
            "state_name": "Oregon                        ",
            "owrd_region": "SC",
            "wm_district": 12,
            "hydrologic_unit_code": 17120007,
            "meridian": "null",
            "township": 36.0,
            "township_char": "S",
            "range": 24.0,
            "range_char": "E",
            "sctn": 20,
            "qtr160": "null",
            "qtr40": "null",
            "elevation": 4550.0,
            "elevation_datum": 4552.8,
            "current_operation_mode": "YR",
            "most_recent_operator": "OWRD",
            "cooperators": "null",
            "published_area": 170.0,
            "owrd_area": 168.0,
            "ws_characteristic": 1,
            "flood_region": 35,
            "basin_name": "Goose & Summer Lake ",
            "streamflow_type_name": "Runoff",
            "source_type_name": "Stream",
            "station_status_name": "Active",
            "current_operation_mode_name": "Year-round",
            "period_of_record_start_date": -1869868800000,
            "period_of_record_end_date": 1412035200000,
            "nbr_of_complete_water_years": 87,
            "nbr_of_peak_flow_values": 93,
            "peak_flow_record_start_wy": 1910,
            "peak_flow_record_end_wy": 2014,
            "near_real_time_web_link": "https://apps.wrd.state.or.us/apps/sw/hydro_near_real_time/display_hydro_graph.aspx?station_nbr=10378500",
            "near_real_time_processing": 1,
            "daily_processing": 1,
            "stage_instantaneous_available": 1,
            "flow_instantaneous_available": 1,
            "mean_daily_flow_available": 1,
            "measured_flow_available": 1,
            "volume_midnight_available": 0,
            "stage_midnight_available": 0,
            "mean_daily_volume_available": 0,
            "mean_daily_stage_available": 0,
            "rating_curve_available": 1,
            "water_temp_instantaneous_avail": 1,
            "water_temp_measurement_avail": 1,
            "water_temp_mean_available": 1,
            "water_temp_max_available": 1,
            "water_temp_min_available": 1,
            "air_temp_instantaneous_avail": 0,
            "air_temp_mean_available": 0,
            "air_temp_max_available": 0,
            "air_temp_min_available": 0,
            "precipitation_available": 0,
        },
    }
    upsert_collection_item(THINGS_COLLECTION, data)


def test_batch_insert():
    builder = OregonStaRequestBuilder(
        ALL_RELEVANT_STATIONS, "10/1/2023 12:00:00 AM", "10/2/2023 12:00:00 AM"
    )
    stations = builder._get_upstream_data()

    for s in stations:
        datastreams, sta_observations = builder._generate_datastreams_and_observations(
            s
        )
        station_metadata = to_station_metadata(s["attributes"], datastreams)
        # put_data(
        #     json.dumps(station_metadata).encode("utf-8"),
        #     f"{STORAGE_INCOMING}/{station_metadata['name']}",
        # )
        upsert_collection_item(THINGS_COLLECTION, station_metadata)

    observation = [
        {
            "id": 1402600000,
            "method": "post",
            "url": "Observations",
            "body": {
                "resultTime": "2023-01-01T00:00:00Z",
                "Datastream": {"@iot.id": 140260000},
                "result": 2.67,
                "FeatureOfInterest": {
                    "name": "UMATILLA R AT YOAKUM, OR",
                    "description": "UMATILLA R AT YOAKUM, OR",
                    "encodingType": "application/vnd.geo+json",
                    "feature": {
                        "type": "Point",
                        "coordinates": [-119.036847, 45.677157, 770.0],
                    },
                },
            },
        },
        {
            "id": 1402600001,
            "method": "post",
            "url": "Observations",
            "body": {
                "resultTime": "2023-01-01T00:15:00Z",
                "Datastream": {"@iot.id": 140260000},
                "result": 2.67,
                "FeatureOfInterest": {
                    "name": "UMATILLA R AT YOAKUM, OR",
                    "description": "UMATILLA R AT YOAKUM, OR",
                    "encodingType": "application/vnd.geo+json",
                    "feature": {
                        "type": "Point",
                        "coordinates": [-119.036847, 45.677157, 770.0],
                    },
                },
            },
        },
    ]
