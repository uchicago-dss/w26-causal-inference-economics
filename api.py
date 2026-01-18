import pandas as pd
import requests
import urllib3
import os
from dotenv import load_dotenv

# 1. SETUP
load_dotenv()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

token = os.getenv("TRADE_API_KEY")
if not token:
    raise SystemExit("Environment variable TRADE_API_KEY is not set.")
token = token.strip()

base_url = "https://datawebws.usitc.gov/dataweb"
headers = {
    "Content-Type": "application/json; charset=utf-8",
    "Authorization": "Bearer " + token,
}

# 2. COLLECT DATA ACROSS ALL YEARS AND METRICS - CHINA IMPORTS
all_data = []

metrics = [
    ("CONS_CUSTOMS_VALUE", "Customs Value"),
    ("CONS_FIR_UNIT_QUANT", "First Unit of Quantity"),
    ("CONS_CALC_DUTY", "Calculated Duties"),
]

for year in range(1995, 2006):  # 1995 to 2005 inclusive
    for metric_code, metric_name in metrics:
        print(f"Fetching {metric_name} for {year}...")
        
        requestData = {
            "savedQueryType": "",
            "isOwner": True,
            "unitConversion": "0",
            "manualConversions": [],
            "reportOptions": {"tradeType": "Import", "classificationSystem": "HTS"},
            "searchOptions": {
                "MiscGroup": {
                    "districts": {
                        "aggregation": "Aggregate District",
                        "districtGroups": {},
                        "districts": [],
                        "districtsExpanded": [{"name": "All Districts", "value": "all"}],
                        "districtsSelectType": "all",
                    },
                    "importPrograms": {
                        "aggregation": None,
                        "importPrograms": [],
                        "programsSelectType": "all",
                    },
                    "extImportPrograms": {
                        "aggregation": "Aggregate CSC",
                        "extImportPrograms": [],
                        "extImportProgramsExpanded": [],
                        "programsSelectType": "all",
                    },
                    "provisionCodes": {
                        "aggregation": "Aggregate RPCODE",
                        "provisionCodesSelectType": "all",
                        "rateProvisionCodes": [],
                        "rateProvisionCodesExpanded": [],
                        "rateProvisionGroups": {"systemGroups": []},
                    },
                },
                "commodities": {
                    "aggregation": "Break Out Commodities",
                    "codeDisplayFormat": "NO",
                    "commodities": [],
                    "commoditiesExpanded": [],
                    "commoditiesManual": "",
                    "commodityGroups": {"systemGroups": [], "userGroups": []},
                    "commoditySelectType": "all",
                    "granularity": "10",
                    "groupGranularity": None,
                    "searchGranularity": None,
                    "showHTSValidDetails": "",
                },
                "componentSettings": {
                    "dataToReport": [metric_code],
                    "scale": "1",
                    "timeframeSelectType": "fullYears",
                    "years": [str(year)],  # Single year per request
                    "startDate": None,
                    "endDate": None,
                    "startMonth": None,
                    "endMonth": None,
                    "yearsTimeline": "Monthly",
                },
                "countries": {
                    "aggregation": "Break Out Countries",
                    "countries": ["5700"],
                    "countriesExpanded": [{"name": "China - CN - CHN", "value": "5700"}],
                    "countriesSelectType": "list",
                    "countryGroups": {"systemGroups": [], "userGroups": []},
                },
            },
            "sortingAndDataFormat": {
                "DataSort": {
                    "columnOrder": ["COUNTRY", "YEAR"],
                    "fullColumnOrder": [],
                    "sortOrder": [
                        {"sortData": "Countries", "orderBy": "asc"},
                        {"sortData": "Year", "orderBy": "asc"},
                    ],
                },
                "reportCustomizations": {
                    "exportCombineTables": False,
                    "totalRecords": "20000",
                    "exportRawData": True,
                },
            },
            "deletedCountryUserGroups": [],
            "deletedCommodityUserGroups": [],
            "deletedDistrictUserGroups": [],
        }

        try:
            response = requests.post(
                base_url + "/api/v2/report2/runReport",
                headers=headers,
                json=requestData,
                verify=False,
            )

            if response.status_code == 200:
                resp_json = response.json()
                
                if resp_json.get("dto") and resp_json["dto"].get("tables"):
                    tables = resp_json["dto"]["tables"]

                    for table in tables:
                        if table.get("row_groups"):
                            for row_group in table.get("row_groups", []):
                                for row in row_group.get("rowsNew", []):
                                    row_entries = row.get("rowEntries", [])
                                    row_values = [entry.get("value") for entry in row_entries]
                                    all_data.append([year, metric_name] + row_values)
                    
                    print(f"  ✓ {year} - {metric_name} complete")
                else:
                    print(f"  ✗ {year} - {metric_name}: No tables in response")
            else:
                print(f"  ✗ {year} - {metric_name}: Status {response.status_code}")

        except Exception as e:
            print(f"  ✗ {year} - {metric_name}: Error - {e}")

# 3. CREATE FINAL DATAFRAME - CHINA IMPORTS
if all_data:
    # Determine column count from max row length (rows may vary)
    max_cols = max(len(row) for row in all_data)
    n_cols = max_cols - 2  # Subtract year and metric columns
    columns = ["Year", "Data Type"] + [f"Col_{i}" for i in range(n_cols)]
    
    # Pad rows to match max length with None values
    all_data = [row + [None] * (max_cols - len(row)) for row in all_data]
    
    df_combined = pd.DataFrame(all_data, columns=columns)
    print(f"\nSuccess! Retrieved {len(df_combined)} total rows.")
    print(df_combined.head(10))
    
    df_combined.to_csv("China_1995_2005.csv", index=False)
    print("Saved to China_1995_2005.csv")
else:
    print("No data retrieved.")

# 4. COLLECT DATA FOR IMPORTS FROM ALL COUNTRIES
all_data_imports = []

for year in range(1995, 2006):  # 1995 to 2005 inclusive
    print(f"Fetching imports for {year} (All Countries)...")
    
    requestData = {
        "savedQueryType": "",
        "isOwner": True,
        "unitConversion": "0",
        "manualConversions": [],
        "reportOptions": {
            "tradeType": "Import",
            "classificationSystem": "HTS"
        },
        "searchOptions": {
            "MiscGroup": {
                "districts": {
                    "aggregation": "Aggregate District",
                    "districtGroups": {},
                    "districts": [],
                    "districtsExpanded": [
                        {
                            "name": "All Districts",
                            "value": "all"
                        }
                    ],
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
                    "rateProvisionCodesExpanded": [],
                    "rateProvisionGroups": {
                        "systemGroups": []
                    }
                }
            },
            "commodities": {
                "aggregation": "Break Out Commodities",
                "codeDisplayFormat": "YES",
                "commodities": [],
                "commoditiesExpanded": [],
                "commoditiesManual": "",
                "commodityGroups": {
                    "systemGroups": [],
                    "userGroups": []
                },
                "commoditySelectType": "all",
                "granularity": "10",
                "groupGranularity": None,
                "searchGranularity": None,
                "showHTSValidDetails": ""
            },
            "componentSettings": {
                "dataToReport": [
                    "CONS_FIR_UNIT_QUANT",
                    "CONS_CUSTOMS_VALUE",
                    "CONS_CALC_DUTY"
                ],
                "scale": "1",
                "timeframeSelectType": "fullYears",
                "years": [str(year)],
                "startDate": None,
                "endDate": None,
                "startMonth": None,
                "endMonth": None,
                "yearsTimeline": "Monthly"
            },
            "countries": {
                "aggregation": "Break Out Countries",
                "countries": [],
                "countriesExpanded": [
                    {
                        "name": "All Countries",
                        "value": "all"
                    }
                ],
                "countriesSelectType": "all",
                "countryGroups": {
                    "systemGroups": [],
                    "userGroups": []
                }
            }
        },
        "sortingAndDataFormat": {
            "DataSort": {
                "columnOrder": [
                    "COUNTRY",
                    "HTS10 & DESCRIPTION",
                    "YEAR"
                ],
                "fullColumnOrder": [
                    {
                        "checked": False,
                        "disabled": False,
                        "hasChildren": False,
                        "name": "Countries",
                        "value": "COUNTRY",
                        "classificationSystem": "",
                        "groupUUID": "",
                        "items": [],
                        "tradeType": ""
                    },
                    {
                        "checked": False,
                        "disabled": False,
                        "hasChildren": False,
                        "name": "HTS10 & DESCRIPTION",
                        "value": "HTS10 & DESCRIPTION",
                        "classificationSystem": "",
                        "groupUUID": "",
                        "items": [],
                        "tradeType": ""
                    },
                    {
                        "checked": False,
                        "disabled": False,
                        "hasChildren": False,
                        "name": "Year",
                        "value": "YEAR",
                        "classificationSystem": "",
                        "groupUUID": "",
                        "items": [],
                        "tradeType": ""
                    }
                ],
                "sortOrder": [
                    {
                        "sortData": "Countries",
                        "orderBy": "asc",
                        "year": ""
                    },
                    {
                        "sortData": "HTS10 & DESCRIPTION",
                        "orderBy": "asc",
                        "year": ""
                    },
                    {
                        "sortData": "Year",
                        "orderBy": "asc",
                        "year": ""
                    }
                ]
            },
            "reportCustomizations": {
                "exportCombineTables": False,
                "totalRecords": "20000",
                "exportRawData": True
            }
        },
        "deletedCountryUserGroups": [],
        "deletedCommodityUserGroups": [],
        "deletedDistrictUserGroups": []
    }
    
    try:
        response = requests.post(
            base_url + "/api/v2/report2/runReport",
            headers=headers,
            json=requestData,
            verify=False,
        )

        if response.status_code == 200:
            resp_json = response.json()
            
            if resp_json.get("dto") and resp_json["dto"].get("tables"):
                tables = resp_json["dto"]["tables"]

                for table in tables:
                    if table.get("row_groups"):
                        for row_group in table.get("row_groups", []):
                            for row in row_group.get("rowsNew", []):
                                row_entries = row.get("rowEntries", [])
                                row_values = [entry.get("value") for entry in row_entries]
                                all_data_imports.append([year] + row_values)
                
                print(f"  ✓ {year} complete")
            else:
                print(f"  ✗ {year}: No tables in response")
        else:
            print(f"  ✗ {year}: Status {response.status_code}")
    except Exception as e:
        print(f"  ✗ {year}: Error - {e}")
        
# 5. CREATE FINAL DATAFRAME FOR IMPORTS ALL COUNTRIES
if all_data_imports:
    # Determine column count from max row length (rows may vary)
    max_cols = max(len(row) for row in all_data_imports)
    n_cols = max_cols - 1  # Subtract year column
    columns = ["Year"] + [f"Col_{i}" for i in range(n_cols)]

    # Pad rows to match max length with None values
    all_data_imports = [row + [None] * (max_cols - len(row)) for row in all_data_imports]

    df_combined_imports = pd.DataFrame(all_data_imports, columns=columns)
    print(f"\nSuccess! Retrieved {len(df_combined_imports)} total rows for Imports All Countries.")
    print(df_combined_imports.head(10))

    df_combined_imports.to_csv("Imports_All_Countries_1995_2005.csv", index=False)
    print("Saved to Imports_All_Countries_1995_2005.csv")
else:
    print("No data retrieved for Imports All Countries.")
