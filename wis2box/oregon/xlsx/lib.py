from pathlib import Path
import pandas as pd
from wis2box.oregon.xlsx.types import OregonXLSX, Metadata

# Reads a workbook sheet and returns a list of dictionaries
def read_sheet(sheet: pd.DataFrame) -> list:
    return sheet.to_dict(orient='records')

def parse_xlsx(input_file: Path) -> OregonXLSX:
    workbook = pd.read_excel(input_file, sheet_name=None)
    site_data = read_sheet(workbook["Site Data"])
    metadata = read_sheet(workbook["Metadata"])
    data = read_sheet(workbook["Data"])
    return OregonXLSX(site_data, metadata, data)
