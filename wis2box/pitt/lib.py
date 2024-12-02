from pathlib import Path
import pandas as pd
from wis2box.oregon.xlsx.types import OregonXLSX, Metadata, SiteData, Data

def validate_columns(sheet: list, typedDict: type):
    """Make sure that the sheet has the same columns as the typedDict"""
    expectedCols = list(typedDict.__annotations__.keys())
    gotCols = list(sheet[0].keys())
    # take the set difference
    missingCols = set(expectedCols) - set(gotCols)
    assert len(missingCols) == 0, f"XLSX Validation failed: Sheet {typedDict.__name__} is missing columns: {missingCols}"


def parse_csv(input_file: Path, schema: type):
    workbook = pd.read_csv(input_file)

    parsed_data = workbook.to_dict(orient='records')
    # make sure that parsed_data has the same column names as the associated typedict
    validate_columns(parsed_data, schema)

    return parsed_data