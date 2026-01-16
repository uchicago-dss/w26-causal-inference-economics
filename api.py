"""
USITC DataWeb API Query Script
For W26 Causal Inference Economics Project
"""

import pandas as pd
import requests
import json
import time
import os
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# CONFIGURATION
# =============================================================================

api_key = os.getenv("TRADE_API_KEY").strip()
if not api_key:
    raise ValueError("TRADE_API_KEY not found in environment or .env file")

TOKEN = api_key
BASE_URL = 'https://datawebws.usitc.gov/dataweb'
HEADERS = {
    "Content-Type": "application/json; charset=utf-8",
    "Authorization": "Bearer " + TOKEN
}

requests.packages.urllib3.disable_warnings()

# =============================================================================
# DATA MEASURE CODES - UPDATE THESE FROM THE WEB INTERFACE
# =============================================================================

DATA_MEASURES = [
    "REPLACE_WITH_CUSTOMS_VALUE_CODE",  # customs value
    "REPLACE_WITH_QUANTITY_CODE",       # first unit of quantity
    "REPLACE_WITH_DUTY_CODE"            # calculated duties
]
CHINA_CODE = "5700"

# =============================================================================
# HELPER FUNCTIONS  
# =============================================================================

def get_columns(column_groups, prev_cols=None):
    if prev_cols is None:
        columns = []
    else:
        columns = prev_cols
    for group in column_groups:
        if isinstance(group, dict) and 'columns' in group.keys():
            get_columns(group['columns'], columns)
        elif isinstance(group, dict) and 'label' in group.keys():
            columns.append(group['label'])
        elif isinstance(group, list):
            get_columns(group, columns)
    return columns


def get_data(data_groups):
    data = []
    for row in data_groups:
        row_data = []
        for field in row['rowEntries']:
            row_data.append(field['value'])
        data.append(row_data)
    return data


def run_query(query_data: dict) -> Optional[pd.DataFrame]:
    try:
        response = requests.post(
            f"{BASE_URL}/api/v2/report2/runReport",
            headers=HEADERS,
            json=query_data,
            verify=False
        )
        response.raise_for_status()
        result = response.json()
        
        if 'dto' not in result:
            print(f"ERROR: {json.dumps(result, indent=2)}")
            return None
        if 'errors' in result['dto'] and result['dto']['errors']:
            print(f"API ERRORS: {result['dto']['errors']}")
            return None
        if 'tables' not in result['dto'] or len(result['dto']['tables']) == 0:
            return None
            
        table = result['dto']['tables'][0]
        if 'row_groups' not in table or len(table['row_groups']) == 0:
            return None
            
        columns = get_columns(table['column_groups'])
        data = get_data(table['row_groups'][0]['rowsNew'])
        
        return pd.DataFrame(data, columns=columns) if data else None
        
    except Exception as e:
        print(f"Error: {e}")
        return None


def build_query(years, countries, data_measures, granularity="10"):
    return {
        "savedQueryName": "",
        "savedQueryDesc": "",
        "isOwner": True,
        "runMonthly": False,
        "reportOptions": {
            "tradeType": "Import",
            "classificationSystem": "HTS"
        },
        "searchOptions": {
            "MiscGroup": {
                "districts": {
                    "aggregation": "Aggregate District",
                    "districtGroups": {"userGroups": []},
                    "districts": [],
                    "districtsExpanded": [{"name": "All Districts", "value": "all"}],
                    "districtsSelectType": "all"
                },
                "importPrograms": {
                    "aggregation": None,
                    "importPrograms": [],
                    "programsSelectType": "all"
                },
                "extImportPrograms": {
                    "aggregation": "Aggregate CSC",
                    "extImportPrograms": [],
                    "extImportProgramsExpanded": [],
                    "programsSelectType": "all"
                },
                "provisionCodes": {
                    "aggregation": "Aggregate RPCODE",
                    "provisionCodesSelectType": "all",
                    "rateProvisionCodes": [],
                    "rateProvisionCodesExpanded": []
                }
            },
            "commodities": {
                "aggregation": "Break Out Commodities",
                "codeDisplayFormat": "YES",
                "commodities": [],
                "commoditiesExpanded": [],
                "commoditiesManual": "",
                "commodityGroups": {"systemGroups": [], "userGroups": []},
                "commoditySelectType": "all",
                "granularity": granularity,
                "groupGranularity": None,
                "searchGranularity": None
            },
            "componentSettings": {
                "dataToReport": data_measures,
                "scale": "1",
                "timeframeSelectType": "fullYears",
                "years": years,
                "startDate": None,
                "endDate": None,
                "startMonth": None,
                "endMonth": None,
                "yearsTimeline": "Monthly"
            },
            "countries": {
                "aggregation": "Break Out Countries",
                "countries": countries,
                "countriesExpanded": [],
                "countriesSelectType": "list" if countries else "all",
                "countryGroups": {"systemGroups": [], "userGroups": []}
            }
        },
        "sortingAndDataFormat": {
            "DataSort": {
                "columnOrder": [],
                "fullColumnOrder": [],
                "sortOrder": []
            },
            "reportCustomizations": {
                "exportCombineTables": False,
                "showAllSubtotal": True,
                "subtotalRecords": "",
                "totalRecords": "50000",
                "exportRawData": False
            }
        }
    }


def query_imports(years, countries, data_measures, granularity="10"):
    query = build_query(years, countries, data_measures, granularity)
    return run_query(query)


# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    # Test query
    df_test = query_imports(
        years=["2005"],
        countries=[CHINA_CODE],
        data_measures=DATA_MEASURES,
        granularity="2"
    )

    if df_test is not None:
        print(f"Success! Shape: {df_test.shape}")
        print(df_test.head())
        df_test.to_csv("test_query_result.csv", index=False)
    else:
        print("Query failed - check data measure codes")