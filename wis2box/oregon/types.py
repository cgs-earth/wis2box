from typing import List, Optional, TypedDict

POTENTIAL_DATASTREAMS = [
    "stage_instantaneous_available",
    "flow_instantaneous_available",
    "mean_daily_flow_available",
    "measured_flow_available",
    "volume_midnight_available",
    "stage_midnight_available",
    "mean_daily_volume_available",
    "mean_daily_stage_available",
    "rating_curve_available",
    "water_temp_instantaneous_avail",
    "water_temp_measurement_avail",
    "water_temp_mean_available",
    "water_temp_max_available",
    "water_temp_min_available",
    "air_temp_instantaneous_avail",
    "air_temp_mean_available",
    "air_temp_max_available",
    "air_temp_min_available",
    "precipitation_available",
]

DATASTREAM_TO_DOWNLOAD_NAME = {
    "water_temp_max_available": "WTEMP_MAX",
    "water_temp_min_available": "WTEMP_MIN",
    "water_temp_mean_available": "WTEMP_MEAN",
    "water_temp_instantaneous_avail": "WTEMP15",
    "water_temp_measurement_avail": "WTEMP_MEASURE",
}

ALL_RELEVANT_STATIONS = [
    10378500,
    10392400,
    11491400,
    11494000,
    11494510,
    11495900,
    11497500,
    11497550,
    11500400,
    11500500,
    11502550,
    11503500,
    11504103,
    11504109,
    11504120,
    11510000,
    13214000,
    13215000,
    13216500,
    13217500,
    13269450,
    13273000,
    13275105,
    13275300,
    13281200,
    13282550,
    13317850,
    13318060,
    13318210,
    13318920,
    13325500,
    13329100,
    13329765,
    13330000,
    13330300,
    13330500,
    13331450,
    14010000,
    14010800,
    14021000,
    14022500,
    14023500,
    14024300,
    14025000,
    14026000,
    14029900,
    14031050,
    14031600,
    14032000,
    14032400,
    14039500,
    14054000,
    14056500,
    14060000,
    14063000,
    14064500,
    14070920,
    14070980,
    14073520,
    14074900,
    14075000,
    14076020,
    14076100,
    14079800,
    14080500,
    14081500,
    14082550,
    14083400,
    14085700,
    14087300,
    14088500,
    14095250,
    14095255,
    14104125,
    14104190,
    14104700,
    14104800,
    14105545,
    14105550,
    14192500,
    14193000,
    14202510,
    14202850,
    14306820,
    14306900,
    14320700,
    14327120,
    14327122,
    14327137,
    14327300,
    14335200,
    14335230,
    14335235,
    14335250,
    14335300,
    14335500,
    14336700,
    14337000,
    14340800,
    14341610,
    14342500,
    14343000,
    14346700,
    14346900,
    14347800,
    14348080,
    14348150,
    14348400,
    14350900,
    14352000,
    14352001,
    14354100,
    14354950,
    14355875,
    14357000,
    14357503,
    14358610,
    14358680,
    14358725,
    14358750,
    14358800,
    14360500,
    14363450,
    14365500,
    14368300,
    14375200,
    14400200,
]


class Attributes(TypedDict):
    OBJECTID: int
    lkp_gaging_station_id: int
    station_nbr: str
    station_name: str
    station_status: str
    streamflow_type: str
    source_type: str
    streamcode: str
    longitude_dec: float
    latitude_dec: float
    county_name: str
    state_name: str
    owrd_region: str
    wm_district: int
    hydrologic_unit_code: int
    meridian: Optional[str]
    township: int
    township_char: str
    range: int
    range_char: str
    sctn: int
    qtr160: str
    qtr40: str
    elevation: int
    elevation_datum: Optional[str]
    current_operation_mode: str
    most_recent_operator: str
    cooperators: Optional[str]
    published_area: int
    owrd_area: int
    ws_characteristic: int
    flood_region: Optional[str]
    basin_name: str
    streamflow_type_name: str
    source_type_name: str
    station_status_name: str
    current_operation_mode_name: str
    period_of_record_start_date: int
    period_of_record_end_date: int
    nbr_of_complete_water_years: int
    nbr_of_peak_flow_values: int
    peak_flow_record_start_wy: int
    peak_flow_record_end_wy: int
    near_real_time_web_link: str
    near_real_time_processing: int
    daily_processing: int
    stage_instantaneous_available: int
    flow_instantaneous_available: int
    mean_daily_flow_available: int
    measured_flow_available: int
    volume_midnight_available: int
    stage_midnight_available: int
    mean_daily_volume_available: int
    mean_daily_stage_available: int
    rating_curve_available: int
    water_temp_instantaneous_avail: int
    water_temp_measurement_avail: int
    water_temp_mean_available: int
    water_temp_max_available: int
    water_temp_min_available: int
    air_temp_instantaneous_avail: int
    air_temp_mean_available: int
    air_temp_max_available: int
    air_temp_min_available: int
    precipitation_available: int


class StationData(TypedDict):
    attributes: Attributes
    geometry: dict[str, float]


class UnitOfMeasurement(TypedDict):
    name: str
    symbol: str
    definition: str


class Period(TypedDict):
    EndTime: str
    StartTime: str
    SuppressData: bool
    ReferenceValue: float
    ReferenceValueToTriggerDisplay: Optional[float]


class Threshold(TypedDict):
    Name: str
    Type: str
    Periods: List[Period]
    ReferenceCode: str


class Properties(TypedDict, total=False):
    Thresholds: List[Threshold]
    ParameterCode: Optional[str]
    StatisticCode: Optional[str]
    # Add other optional properties here if needed


Datastream = TypedDict(
    "Datastream",
    {
        # "@iot.selfLink": str,
        "@iot.id": str,
        "name": str,
        "description": str,
        "observationType": str,
        "unitOfMeasurement": UnitOfMeasurement,
        "ObservedProperty": dict[str, str],
        "phenomenonTime": Optional[str],
        "Sensor": dict[str, str],
    },
)