import logging
import os
from pathlib import Path
from wis2box.oregon.xlsx.lib import parse_xlsx
import frost_sta_client
from frost_sta_client.model.ext import data_array_value


logger = logging.getLogger(__name__)

def test_parse_xlsx():
    file = Path(__file__).parent / "IoW_Reccomended_Obs_Data_Elements.xlsx"
    xlsx = parse_xlsx(file)
    assert xlsx.dataSheet
    assert xlsx.metadataSheet
    assert xlsx.siteDataSheet
    logger.error(xlsx.dataSheet)
    assert xlsx.dataSheet[0]["Data Quality"] == "Good"
    assert xlsx.metadataSheet[1]["Metadata Indetifier"] == "MD67890"
    assert xlsx.siteDataSheet[0]["Operating/Monitoring/Sampling Organization"] == "Environmental Agency"


def test_frost_connection():
    url = os.getenv("WIS2BOX_API_BACKEND_URL")
    assert url
    service = frost_sta_client.SensorThingsService(url)
    dav = data_array_value.DataArrayValue()
    assert dav
    # foi = service.features_of_interest().find(1)
    # components = {dav.Property.PHENOMENON_TIME, dav.Property.RESULT, dav.Property.FEATURE_OF_INTEREST}
    # dav.components = components
    # dav.datastream = datastream
    # obs1 = fsc.Observation(result=3,
    #                        phenomenon_time='2022-12-19T10:00:00Z',
    #                        datastream=datastream,
    #                        feature_of_interest=foi)
    # obs2 = fsc.Observation(result=5,
    #                        phenomenon_time='2022-12-19T10:00:00Z/2022-12-19T11:00:00Z',
    #                        datastream=datastream,
    #                        feature_of_interest=foi)
    # dav.add_observation(obs1)
    # dav.add_observation(obs2)
    # dad = fsc.model.ext.data_array_document.DataArrayDocument()
    # dad.add_data_array_value(dav)
    # result_list = service.observations().create(dad)