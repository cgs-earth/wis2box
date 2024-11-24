import logging
import os
from pathlib import Path
from wis2box.oregon.xlsx.lib import parse_xlsx
import frost_sta_client as fsc
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
    service = fsc.SensorThingsService(url)
    res = service.things().query()
    assert res
   
def test_parse_xlsx_and_generate_sta():
    file = Path(__file__).parent / "IoW_Reccomended_Obs_Data_Elements.xlsx"
    xlsx = parse_xlsx(file)
    sta_representation = xlsx.to_sta()
    assert sta_representation

def test_insert_data_into_frost():
    xlsx = parse_xlsx(Path(__file__).parent / "IoW_Reccomended_Obs_Data_Elements.xlsx")
    sta_representation = xlsx.to_sta()
    xlsx.send_to_frost(sta_representation)