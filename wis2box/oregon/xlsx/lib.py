from pathlib import Path
import pandas as pd
from wis2box.oregon.xlsx.types import OregonXLSX, Metadata, SiteData, Data

# Reads a workbook sheet and returns a list of dictionaries
def read_sheet(sheet: pd.DataFrame) -> list:
    return sheet.to_dict(orient='records')


def validate_columns(sheet: list, typedDict: type):
    """Make sure that the sheet has the same columns as the typedDict"""
    expectedCols = list(typedDict.__annotations__.keys())
    gotCols = list(sheet[0].keys())
    # take the set difference
    missingCols = set(expectedCols) - set(gotCols)
    assert len(missingCols) == 0, f"XLSX Validation failed: Sheet {typedDict.__name__} is missing columns: {missingCols}"


def parse_xlsx(input_file: Path) -> OregonXLSX:
    workbook = pd.read_excel(input_file, sheet_name=None)
    assert len(workbook) == 3 # make sure there are 3 sheets

    site_data: list[SiteData] = read_sheet(workbook["Site Data"])
    # make sure that site_data has the same column names as the SiteData typedict
    validate_columns(site_data, SiteData)

    metadata: list[Metadata] = read_sheet(workbook["Metadata"])
    validate_columns(metadata, Metadata)
    
    data: list[Data] = read_sheet(workbook["Data"])
    validate_columns(data, Data)

    return OregonXLSX(site_data, metadata, data)
